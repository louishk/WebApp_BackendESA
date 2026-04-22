"""
IglooPipeline — fetch Igloo properties + devices from the Igloo API and
write into esa_middleware. Reuses the legacy datalayer transforms so
behaviour is identical to the scheduler job, just with a different target DB.

Legacy scheduler script (datalayer/igloo_to_sql.py) continues to run and
populates esa_backend. This orchestrator pipeline is the parallel middleware
copy and leaves esa_backend untouched.
"""

import logging
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from sync_service.pipelines.base import BasePipeline, RunResult
from sync_service.config import get_engine
from common.models import IglooProperty, IglooDevice

logger = logging.getLogger(__name__)


class IglooPipeline(BasePipeline):
    """Sync Igloo properties + devices → esa_middleware."""

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        # Reuse legacy transforms — they have the full site_id resolution logic
        from datalayer.igloo_to_sql import (
            IglooAuth, fetch_paginated, fetch_device_detail,
            transform_property, transform_device,
            sync_devices_to_smart_locks,
        )
        import datalayer.igloo_to_sql as legacy_igloo
        from common.http_client import HTTPClient
        from common.secrets_vault import vault_config

        property_site_map: Dict[str, int] = scope.get('property_site_map') or {}

        client_id = vault_config('IGLOO_CLIENT_ID')
        client_secret = vault_config('IGLOO_CLIENT_SECRET')
        if not client_id or not client_secret:
            return RunResult(status='failed', scope=scope,
                             error='Igloo credentials not in vault')

        # Populate legacy helper's site-lookup caches from mw_siteinfo
        # (not PBI siteinfo — orchestrator is middleware-tier)
        mw_engine = get_engine('middleware')
        with mw_engine.connect() as conn:
            rows = conn.execute(text(
                'SELECT "SiteID", "SiteCode", igloo_property_id, igloo_department_id '
                'FROM mw_siteinfo'
            )).fetchall()
        legacy_igloo._site_code_to_id = {r[1]: r[0] for r in rows if r[1]}
        legacy_igloo._igloo_prop_to_site = {r[2]: r[0] for r in rows if r[2]}
        legacy_igloo._new_igloo_mappings.clear()

        http = HTTPClient(default_timeout=60)
        auth = IglooAuth(client_id, client_secret, http)

        raw_properties = fetch_paginated(http, auth, 'properties')
        raw_departments = fetch_paginated(http, auth, 'departments')
        raw_devices = fetch_paginated(http, auth, 'devices')

        # Fetch device detail (expanded property info)
        detailed_devices: List[Dict[str, Any]] = []
        for d in raw_devices:
            did = d.get('deviceId')
            if did:
                detail = fetch_device_detail(http, auth, did)
                detailed_devices.append(detail or d)
            else:
                detailed_devices.append(d)

        departments = {d.get('id'): d for d in raw_departments if d.get('id')}

        property_records = [
            transform_property(p, property_site_map) for p in raw_properties
        ]
        device_records = [
            transform_device(d, departments, property_site_map) for d in detailed_devices
        ]

        # Dedup
        property_records = list({r['propertyId']: r for r in property_records}.values())
        device_records = list({r['deviceId']: r for r in device_records}.values())

        # Write to middleware via SQLAlchemy Core on_conflict
        with mw_engine.begin() as conn:
            if property_records:
                stmt = pg_insert(IglooProperty.__table__).values(property_records)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['propertyId'],
                    set_={c: getattr(stmt.excluded, c) for c in property_records[0].keys()
                          if c != 'propertyId'},
                )
                conn.execute(stmt)

            if device_records:
                stmt = pg_insert(IglooDevice.__table__).values(device_records)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['deviceId'],
                    set_={c: getattr(stmt.excluded, c) for c in device_records[0].keys()
                          if c != 'deviceId'},
                )
                conn.execute(stmt)

        # Persist any auto-discovered igloo_property_id / igloo_department_id back
        # to mw_siteinfo (middleware-tier enrichment).
        enriched = 0
        if legacy_igloo._new_igloo_mappings:
            with mw_engine.begin() as conn:
                for m in legacy_igloo._new_igloo_mappings:
                    conn.execute(text("""
                        UPDATE mw_siteinfo
                        SET igloo_property_id = COALESCE(igloo_property_id, :prop_id),
                            igloo_department_id = COALESCE(igloo_department_id, :dept_id)
                        WHERE "SiteID" = :site_id
                    """), m)
            enriched = len(legacy_igloo._new_igloo_mappings)

        # Auto-populate mw_smart_lock_keypads / mw_smart_lock_padlocks for newly
        # discovered Igloo devices (parity with the retired legacy scheduler job).
        # Models are bound to middleware tables, so pass the middleware engine.
        kp_added, pl_added = sync_devices_to_smart_locks(mw_engine, device_records)

        return RunResult(
            status='refreshed',
            records=len(property_records) + len(device_records),
            scope=scope,
            metadata={
                'properties': len(property_records),
                'devices': len(device_records),
                'siteinfo_enriched': enriched,
                'keypads_seen': kp_added,
                'padlocks_seen': pl_added,
            },
        )
