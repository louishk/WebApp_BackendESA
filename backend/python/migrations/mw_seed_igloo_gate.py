"""
Create middleware tables + seed mw_sync_pipelines rows for:
  - ccws_gate_access (SOAP GateAccessData, every 30 min)
  - igloo            (Igloo REST API, every 4 hours)
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url
from common.models import (
    Base as CommonBase,
    CcwsGateAccess,
    IglooProperty,
    IglooDevice,
)

LOCATION_CODES = [
    "L001", "L002", "L003", "L004", "L005", "L006", "L007", "L008",
    "L009", "L010", "L011", "L013", "L015", "L017", "L018", "L019",
    "L020", "L021", "L022", "L023", "L024", "L025", "L026", "L028",
    "L029", "L030", "L031", "LSETUP",
]

PIPELINES = [
    {
        'name': 'ccws_gate_access',
        'model': CcwsGateAccess,
        'display': 'CCWS Gate Access',
        'desc': 'Fernet-encrypted gate access codes from CallCenterWs GateAccessData',
        'cls': 'sync_service.pipelines.ccws_gate_access.CcwsGateAccessPipeline',
        'ttl': 1800,
        'fresh_table': 'ccws_gate_access',
        'scope_col': 'site_id',
        'cron': '*/30 * * * *',
        'args': {'location_codes': LOCATION_CODES},
    },
    {
        'name': 'igloo',
        'model': None,  # creates both IglooProperty + IglooDevice
        'display': 'Igloo Properties + Devices',
        'desc': 'Igloo Works API — properties, devices (battery, last sync)',
        'cls': 'sync_service.pipelines.igloo.IglooPipeline',
        'ttl': 7200,
        'fresh_table': 'igloo_devices',
        'scope_col': 'site_id',
        'cron': '0 */4 * * *',
        'args': {},  # property_site_map can be overridden via UI
    },
]


def main():
    mw_engine = create_engine(get_database_url('middleware'))

    print('[1] Creating tables in esa_middleware...')
    CommonBase.metadata.create_all(mw_engine, tables=[
        CcwsGateAccess.__table__,
        IglooProperty.__table__,
        IglooDevice.__table__,
    ])
    print('    done')

    print('[2] Seeding mw_sync_pipelines rows...')
    with mw_engine.begin() as conn:
        for p in PIPELINES:
            conn.execute(text("""
                INSERT INTO mw_sync_pipelines (
                    pipeline_name, display_name, description, pipeline_class,
                    enabled, schedule_type, schedule_config,
                    freshness_table, freshness_column, freshness_scope_column,
                    freshness_ttl_seconds, freshness_database,
                    max_concurrency, resource_group, max_db_connections,
                    timeout_seconds, max_retries, retry_delay_seconds,
                    default_args
                ) VALUES (
                    :name, :display, :desc, :cls,
                    TRUE, 'cron', CAST(:sched AS jsonb),
                    :ft, 'updated_at', :sc,
                    :ttl, 'middleware',
                    2, :rg, 2,
                    1800, 3, 300,
                    CAST(:args AS jsonb)
                )
                ON CONFLICT (pipeline_name) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    description = EXCLUDED.description,
                    pipeline_class = EXCLUDED.pipeline_class,
                    schedule_type = EXCLUDED.schedule_type,
                    schedule_config = EXCLUDED.schedule_config,
                    freshness_table = EXCLUDED.freshness_table,
                    freshness_scope_column = EXCLUDED.freshness_scope_column,
                    freshness_ttl_seconds = EXCLUDED.freshness_ttl_seconds,
                    default_args = EXCLUDED.default_args,
                    updated_at = NOW()
            """), {
                'name': p['name'], 'display': p['display'], 'desc': p['desc'],
                'cls': p['cls'],
                'sched': json.dumps({'cron': p['cron']}),
                'ft': p['fresh_table'], 'sc': p['scope_col'], 'ttl': p['ttl'],
                'rg': 'http_api' if p['name'] == 'igloo' else 'soap_api',
                'args': json.dumps(p['args']),
            })
            print(f"    seeded {p['name']} (cron={p['cron']})")
    print('    done')


if __name__ == '__main__':
    main()
