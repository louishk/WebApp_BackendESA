"""
Register sync_service.pipelines.rentroll.RentRollPipeline in mw_sync_pipelines.

This replaces the legacy APScheduler entry (rentroll in config/pipelines.yaml).
Drop the YAML block and the scheduler_pipeline_config row after running this.

Run from backend/python:
    python3 migrations/mw_seed_rentroll.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    mw_engine = create_engine(get_database_url('middleware'))

    print('[1] Seeding mw_sync_pipelines row for rentroll...')
    with mw_engine.begin() as conn:
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
                'rentroll', 'extract_date', NULL,
                :ttl, 'pbi',
                1, 'soap_api', 3,
                3600, 3, 300,
                CAST(:args AS jsonb)
            )
            ON CONFLICT (pipeline_name) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                description = EXCLUDED.description,
                pipeline_class = EXCLUDED.pipeline_class,
                schedule_type = EXCLUDED.schedule_type,
                schedule_config = EXCLUDED.schedule_config,
                freshness_table = EXCLUDED.freshness_table,
                freshness_column = EXCLUDED.freshness_column,
                freshness_database = EXCLUDED.freshness_database,
                resource_group = EXCLUDED.resource_group,
                max_db_connections = EXCLUDED.max_db_connections,
                timeout_seconds = EXCLUDED.timeout_seconds,
                max_retries = EXCLUDED.max_retries,
                retry_delay_seconds = EXCLUDED.retry_delay_seconds,
                default_args = EXCLUDED.default_args,
                enabled = TRUE,
                updated_at = NOW()
        """), {
            'name': 'rentroll',
            'display': 'RentRoll',
            'desc': 'Fetch unit inventory + occupancy snapshots from SMD SOAP '
                    '(GetRentRoll) for all locations and upsert into esa_pbi.rentroll '
                    'keyed on (extract_date, SiteID, UnitID). Current month overwrites; '
                    'closed months use EOM extract_date.',
            'cls': 'sync_service.pipelines.rentroll.RentRollPipeline',
            'sched': '{"cron": "0 6 * * *"}',
            'ttl': 23 * 3600,
            'args': '{"mode": "auto"}',
        })
    print('    done')


if __name__ == '__main__':
    main()
