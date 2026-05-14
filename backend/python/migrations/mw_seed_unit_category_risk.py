"""
Register sync_service.pipelines.unit_category_risk.UnitCategoryRiskPipeline
in mw_sync_pipelines so the orchestrator picks it up.

This replaces the legacy APScheduler entry (unit_category_risk in
config/pipelines.yaml). Drop the YAML block and the scheduler_pipeline_config
row after running this.

Run from backend/python:
    python3 migrations/mw_seed_unit_category_risk.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    mw_engine = create_engine(get_database_url('middleware'))

    print('[1] Seeding mw_sync_pipelines row for unit_category_risk...')
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
                'unit_category_risk_baseline', 'computed_at', NULL,
                :ttl, 'pbi',
                1, 'db_pool', 2,
                1800, 2, 600,
                '{}'::jsonb
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
                enabled = TRUE,
                updated_at = NOW()
        """), {
            'name': 'unit_category_risk',
            'display': 'Unit-Category Risk',
            'desc': 'Compute country-scoped move-out risk factors per '
                    'dimension/value. Reads rentroll + mimo from esa_pbi; '
                    'upserts unit_category_risk_baseline/factors/history.',
            'cls': 'sync_service.pipelines.unit_category_risk.UnitCategoryRiskPipeline',
            'sched': '{"cron": "0 3 1 * *"}',
            'ttl': 27 * 24 * 3600,  # ~monthly cadence
        })
    print('    done')


if __name__ == '__main__':
    main()
