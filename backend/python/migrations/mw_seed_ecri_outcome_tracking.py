"""
Register sync_service.pipelines.ecri_outcome_tracking.EcriOutcomeTrackingPipeline
in mw_sync_pipelines so the orchestrator picks it up.

Run from backend/python:
    python3 migrations/mw_seed_ecri_outcome_tracking.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    mw_engine = create_engine(get_database_url('middleware'))

    print('[1] Seeding mw_sync_pipelines row for ecri_outcome_tracking...')
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
                TRUE, 'cron', :sched::jsonb,
                'ecri_outcomes', 'created_at', NULL,
                :ttl, 'pbi',
                1, 'db_only', 2,
                900, 3, 300,
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
                enabled = TRUE,
                updated_at = NOW()
        """), {
            'name': 'ecri_outcome_tracking',
            'display': 'ECRI Outcome Tracking',
            'desc': 'Track stay/move-out/scheduled-out outcomes for executed ECRI '
                    'batches by joining ecri_batch_ledgers against vw_ecri_eligible_ledgers '
                    'in one bulk query per batch.',
            'cls': 'sync_service.pipelines.ecri_outcome_tracking.EcriOutcomeTrackingPipeline',
            'sched': '{"cron": "0 8 * * *"}',
            'ttl': 23 * 3600,
        })
    print('    done')


if __name__ == '__main__':
    main()
