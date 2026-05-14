"""
ZoomContactsSyncPipeline — push SugarCRM contacts/leads to Zoom Phone External Contacts.

Maintains bidirectional mapping in esa_pbi.zoom_contact_sync; tracks watermarks
in esa_pbi.zoom_sync_state. Writes external_contact_id back to SugarCRM custom
field es_zoom_contact_id_c when a contact is created.

Modes:
  - auto (default): incremental since last successful sync
  - backfill:       process all CRM records; also runs handle_deletions

Scope keys honoured (all optional):
  - mode:  'auto' | 'backfill'
  - limit: int  (cap records per module — testing only)
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)

BATCH_COMMIT_SIZE = 50

COUNTRY_CODES = {
    'SG': '65', 'KR': '82', 'MY': '60', 'HK': '852', 'JP': '81', 'TW': '886',
}
DEFAULT_COUNTRY_CODE = '65'

PHONE_FIELDS = ['phone_mobile', 'phone_work', 'phone_home', 'phone_other', 'phone_fax']


def normalize_phone(raw: str, country_hint: Optional[str] = None) -> Optional[str]:
    if not raw:
        return None
    has_plus = raw.strip().startswith('+')
    digits = re.sub(r'\D', '', raw)
    if not digits or len(digits) < 4:
        return None
    if has_plus and len(digits) >= 7:
        return f'+{digits}'
    for code_val in sorted(COUNTRY_CODES.values(), key=len, reverse=True):
        if digits.startswith(code_val) and len(digits) >= len(code_val) + 4:
            return f'+{digits}'
    cc = COUNTRY_CODES.get(country_hint, DEFAULT_COUNTRY_CODE)
    if digits.startswith('0'):
        digits = digits[1:]
    if len(digits) < 4:
        return None
    return f'+{cc}{digits}'


def extract_phone_numbers(record: Dict[str, Any], country_hint: Optional[str] = None) -> List[str]:
    phones: List[str] = []
    seen: Set[str] = set()
    for field in PHONE_FIELDS:
        raw = record.get(field)
        if not raw:
            continue
        normalized = normalize_phone(str(raw), country_hint)
        if normalized and normalized not in seen:
            seen.add(normalized)
            phones.append(normalized)
    return phones


def infer_country_hint(record: Dict[str, Any]) -> Optional[str]:
    country_name_map = {
        'SINGAPORE': 'SG', 'KOREA': 'KR', 'SOUTH KOREA': 'KR',
        'MALAYSIA': 'MY', 'HONG KONG': 'HK', 'JAPAN': 'JP', 'TAIWAN': 'TW',
    }
    for field in ['primary_address_country', 'billing_address_country', 'country']:
        val = record.get(field, '')
        if not val:
            continue
        val_upper = str(val).upper().strip()
        if val_upper in COUNTRY_CODES:
            return val_upper
        if val_upper in country_name_map:
            return country_name_map[val_upper]
    return None


def fetch_crm_records(pbi_engine, module: str, since: Optional[datetime] = None) -> List[Dict[str, Any]]:
    table = 'sugarcrm_contacts' if module == 'Contacts' else 'sugarcrm_leads'
    phone_filter = ' OR '.join(f'({f} IS NOT NULL AND {f} != \'\')' for f in PHONE_FIELDS)

    query = f"SELECT * FROM {table} WHERE ({phone_filter})"
    params: Dict[str, Any] = {}
    if since:
        query += " AND date_modified > :since"
        params['since'] = since

    with pbi_engine.connect() as conn:
        result = conn.execute(text(query), params)
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]


def build_zoom_payload(record: Dict[str, Any], module: str, phones: List[str]) -> Dict[str, Any]:
    first = record.get('first_name', '') or ''
    last = record.get('last_name', '') or ''
    name = f"{first} {last}".strip() or 'Unknown'
    email = record.get('email1', '') or record.get('email', '') or ''
    description = f"SugarCRM {module}: {record.get('sugar_id', 'N/A')}"

    payload = {
        'name': name[:200],
        'phone_numbers': phones,
        'description': description[:500],
        'auto_call_recorded': True,
    }
    if email:
        payload['email'] = email
    return payload


def sync_contacts_for_module(
    zoom_client, backend_session, pbi_engine, module: str,
    since: Optional[datetime] = None, limit: Optional[int] = None,
    sugar_client=None,
) -> Tuple[int, int, int, int]:
    from common.models import ZoomContactSync
    from common.zoom_client import ZoomAPIError

    crm_records = fetch_crm_records(pbi_engine, module, since)
    if limit:
        crm_records = crm_records[:limit]
    logger.info("zoom_contacts: %s fetched=%d", module, len(crm_records))

    created = updated = skipped = errors = 0
    processed = 0

    for record in crm_records:
        sugar_id = record.get('sugar_id')
        if not sugar_id:
            skipped += 1
            continue

        country_hint = infer_country_hint(record)
        phones = extract_phone_numbers(record, country_hint)
        if not phones:
            skipped += 1
            continue

        payload = build_zoom_payload(record, module, phones)

        existing = backend_session.query(ZoomContactSync).filter_by(
            sugar_id=sugar_id, sugar_module=module,
        ).first()

        try:
            if existing and existing.zoom_contact_id:
                old_phones = existing.phone_numbers or []
                old_name = existing.name_pushed or ''
                new_phones_set = set(phones)
                old_phones_set: Set[str] = set()
                if isinstance(old_phones, list):
                    for p in old_phones:
                        if isinstance(p, str):
                            old_phones_set.add(p)
                        elif isinstance(p, dict) and 'number' in p:
                            old_phones_set.add(p['number'])

                if new_phones_set == old_phones_set and payload['name'] == old_name:
                    skipped += 1
                    continue

                zoom_client.update_external_contact(existing.zoom_contact_id, payload)
                existing.phone_numbers = phones
                existing.name_pushed = payload['name']
                existing.sync_status = 'synced'
                existing.error_message = None
                existing.last_synced_at = datetime.now(timezone.utc)
                updated += 1
            else:
                result = zoom_client.create_external_contact(payload)
                zoom_contact_id = result.get('external_contact_id') or result.get('id', '')

                if existing:
                    existing.zoom_contact_id = zoom_contact_id
                    existing.phone_numbers = phones
                    existing.name_pushed = payload['name']
                    existing.sync_status = 'synced'
                    existing.error_message = None
                    existing.last_synced_at = datetime.now(timezone.utc)
                else:
                    backend_session.add(ZoomContactSync(
                        sugar_id=sugar_id,
                        sugar_module=module,
                        zoom_contact_id=zoom_contact_id,
                        phone_numbers=phones,
                        name_pushed=payload['name'],
                        sync_status='synced',
                        last_synced_at=datetime.now(timezone.utc),
                    ))
                created += 1

                if sugar_client and zoom_contact_id:
                    try:
                        _, sugar_err = sugar_client.update_record(
                            module, sugar_id, {'es_zoom_contact_id_c': zoom_contact_id},
                        )
                        if sugar_err:
                            logger.warning("SugarCRM writeback failed for %s/%s: %s",
                                           module, sugar_id, sugar_err)
                    except Exception:
                        logger.exception("SugarCRM writeback exception for %s/%s",
                                         module, sugar_id)

        except ZoomAPIError:
            logger.warning("zoom_contacts: sync failed for %s %s", module, sugar_id)
            if existing:
                existing.sync_status = 'error'
                existing.error_message = 'Zoom API error'
            else:
                backend_session.add(ZoomContactSync(
                    sugar_id=sugar_id, sugar_module=module,
                    sync_status='error', error_message='Zoom API error',
                ))
            errors += 1

        processed += 1
        if processed % BATCH_COMMIT_SIZE == 0:
            backend_session.commit()

    backend_session.commit()
    return created, updated, skipped, errors


def handle_deletions(zoom_client, backend_session, pbi_engine) -> int:
    from common.models import ZoomContactSync
    from common.zoom_client import ZoomAPIError

    synced = backend_session.query(ZoomContactSync).filter(
        ZoomContactSync.sync_status == 'synced',
        ZoomContactSync.zoom_contact_id.isnot(None),
    ).all()
    if not synced:
        return 0

    valid_ids: Set[Tuple[str, str]] = set()
    for module in ['Contacts', 'Leads']:
        records = fetch_crm_records(pbi_engine, module)
        for rec in records:
            sid = rec.get('sugar_id')
            if sid:
                valid_ids.add((sid, module))

    deleted = 0
    for mapping in synced:
        if (mapping.sugar_id, mapping.sugar_module) not in valid_ids:
            try:
                zoom_client.delete_external_contact(mapping.zoom_contact_id)
                mapping.sync_status = 'deleted'
                mapping.zoom_contact_id = None
                deleted += 1
            except ZoomAPIError:
                logger.warning("zoom_contacts: delete failed for %s", mapping.zoom_contact_id)
                mapping.sync_status = 'error'
                mapping.error_message = 'Delete failed'
            if deleted % BATCH_COMMIT_SIZE == 0:
                backend_session.commit()

    backend_session.commit()
    return deleted


def get_last_sync(backend_session, sync_name: str) -> Optional[datetime]:
    from common.models import ZoomSyncState
    state = backend_session.query(ZoomSyncState).filter_by(sync_name=sync_name).first()
    if state and state.last_success_at:
        return state.last_success_at
    return None


def update_sync_state(backend_session, sync_name: str, records_processed: int):
    from common.models import ZoomSyncState
    now = datetime.now(timezone.utc)
    state = backend_session.query(ZoomSyncState).filter_by(sync_name=sync_name).first()
    if state:
        state.last_sync_at = now
        state.last_success_at = now
        state.records_processed = records_processed
        state.updated_at = now
    else:
        backend_session.add(ZoomSyncState(
            sync_name=sync_name, last_sync_at=now, last_success_at=now,
            records_processed=records_processed,
        ))
    backend_session.commit()


def run(mode: str = 'auto', limit: Optional[int] = None) -> Dict[str, int]:
    from common.config_loader import get_database_url
    from common.models import Base, ZoomContactSync, ZoomSyncState
    from common.sugarcrm_client import SugarCRMClient
    from common.zoom_client import ZoomClient

    pbi_engine = create_engine(get_database_url('pbi'))
    Base.metadata.create_all(pbi_engine, tables=[
        ZoomContactSync.__table__, ZoomSyncState.__table__,
    ])
    Session = sessionmaker(bind=pbi_engine)
    session = Session()
    sugar_client = None

    try:
        since = None
        if mode == 'auto':
            since = get_last_sync(session, 'contacts_push')
            logger.info("zoom_contacts auto: since=%s", since.isoformat() if since else None)

        zoom_client = ZoomClient()

        sugar_client = SugarCRMClient.from_env()
        if not sugar_client.authenticate():
            logger.warning("SugarCRM auth failed — writeback disabled")
            sugar_client = None

        c_created, c_updated, c_skipped, c_errors = sync_contacts_for_module(
            zoom_client, session, pbi_engine, 'Contacts', since,
            limit=limit, sugar_client=sugar_client,
        )
        l_created, l_updated, l_skipped, l_errors = sync_contacts_for_module(
            zoom_client, session, pbi_engine, 'Leads', since,
            limit=limit, sugar_client=sugar_client,
        )

        deleted = 0
        if mode == 'backfill' and not limit:
            deleted = handle_deletions(zoom_client, session, pbi_engine)

        total_processed = c_created + c_updated + l_created + l_updated + deleted
        update_sync_state(session, 'contacts_push', total_processed)

        return {
            'created': c_created + l_created,
            'updated': c_updated + l_updated,
            'skipped': c_skipped + l_skipped,
            'errors': c_errors + l_errors,
            'deleted': deleted,
        }
    finally:
        try:
            if sugar_client:
                sugar_client.logout()
        except Exception:
            pass
        session.close()
        pbi_engine.dispose()


class ZoomContactsSyncPipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode = scope.get('mode', 'auto')
        limit = scope.get('limit')
        result = run(mode=mode, limit=limit)
        records = (result.get('created', 0) + result.get('updated', 0)
                   + result.get('deleted', 0))
        return RunResult(
            status='refreshed',
            records=records,
            scope=scope,
            metadata=result,
        )
