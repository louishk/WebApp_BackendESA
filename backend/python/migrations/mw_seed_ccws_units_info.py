"""Create ccws_units_info in esa_middleware + register pipeline."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url
from common.models import Base as CommonBase, CcwsUnitInfo

LOCATION_CODES = [
    "L001", "L002", "L003", "L004", "L005", "L006", "L007", "L008",
    "L009", "L010", "L011", "L013", "L015", "L017", "L018", "L019",
    "L020", "L021", "L022", "L023", "L024", "L025", "L026", "L028",
    "L029", "L030", "L031", "LSETUP",
]


def main():
    mw = create_engine(get_database_url('middleware'))

    print('[1] Creating ccws_units_info in esa_middleware...')
    CommonBase.metadata.create_all(mw, tables=[CcwsUnitInfo.__table__])
    print('    done')

    print('[2] Seeding mw_sync_pipelines row for ccws_units_info...')
    with mw.begin() as conn:
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
                :ft, 'updated_at', 'SiteID',
                :ttl, 'middleware',
                5, 'soap_api', 2,
                1800, 3, 300,
                CAST(:args AS jsonb)
            )
            ON CONFLICT (pipeline_name) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                description = EXCLUDED.description,
                pipeline_class = EXCLUDED.pipeline_class,
                schedule_type = EXCLUDED.schedule_type,
                schedule_config = EXCLUDED.schedule_config,
                freshness_ttl_seconds = EXCLUDED.freshness_ttl_seconds,
                default_args = EXCLUDED.default_args,
                updated_at = NOW()
        """), {
            'name': 'ccws_units_info',
            'display': 'CCWS Units Info',
            'desc': 'Full unit catalog from UnitsInformation_v3 — used by Smart Lock tool',
            'cls': 'sync_service.pipelines.ccws_units_info.CcwsUnitsInfoPipeline',
            'sched': json.dumps({'cron': '0 */6 * * *'}),
            'ft': 'ccws_units_info',
            'ttl': 21600,
            'args': json.dumps({'location_codes': LOCATION_CODES}),
        })
    print('    done')


if __name__ == '__main__':
    main()
