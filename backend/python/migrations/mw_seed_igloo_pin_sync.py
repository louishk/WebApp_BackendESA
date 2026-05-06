"""
Seed mw_sync_pipelines row for the igloo_pin_sync pipeline.

Run once from dev machine (or VM) after deploying igloo_pin_sync.py:

    cd backend/python
    python3 migrations/mw_seed_igloo_pin_sync.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


PIPELINE = {
    'pipeline_name': 'igloo_pin_sync',
    'display_name': 'Igloo PIN Auto-Sync',
    'description': (
        'Reconcile Igloo keypad PINs with SiteLink gate access codes. '
        'Pushes new PINs for rented units, revokes on move-out.'
    ),
    'pipeline_class': 'sync_service.pipelines.igloo_pin_sync.IglooPinSyncPipeline',
    'schedule_type': 'cron',
    'schedule_config': json.dumps({'cron': '*/15 * * * *'}),
    'freshness_table': 'mw_smart_lock_audit_log',
    'freshness_column': 'created_at',
    'freshness_scope_column': 'site_id',
    'freshness_ttl_seconds': 1800,
    'freshness_database': 'middleware',
    'resource_group': 'http_api',
    'timeout_seconds': 900,
    'max_concurrency': 1,
    'max_db_connections': 2,
    'max_retries': 3,
    'retry_delay_seconds': 300,
    'default_args': json.dumps({}),
}


def main():
    mw_engine = create_engine(get_database_url('middleware'))

    print('[1] Seeding mw_sync_pipelines row for igloo_pin_sync...')
    with mw_engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO mw_sync_pipelines (
                pipeline_name, display_name, description, pipeline_class,
                enabled, schedule_type, schedule_config,
                freshness_table, freshness_column, freshness_scope_column,
                freshness_ttl_seconds, freshness_database,
                resource_group,
                timeout_seconds, max_concurrency, max_db_connections,
                max_retries, retry_delay_seconds,
                default_args
            ) VALUES (
                :pipeline_name, :display_name, :description, :pipeline_class,
                TRUE, :schedule_type, CAST(:schedule_config AS jsonb),
                :freshness_table, :freshness_column, :freshness_scope_column,
                :freshness_ttl_seconds, :freshness_database,
                :resource_group,
                :timeout_seconds, :max_concurrency, :max_db_connections,
                :max_retries, :retry_delay_seconds,
                CAST(:default_args AS jsonb)
            )
            ON CONFLICT (pipeline_name) DO UPDATE SET
                display_name              = EXCLUDED.display_name,
                description               = EXCLUDED.description,
                pipeline_class            = EXCLUDED.pipeline_class,
                schedule_type             = EXCLUDED.schedule_type,
                schedule_config           = EXCLUDED.schedule_config,
                freshness_table           = EXCLUDED.freshness_table,
                freshness_column          = EXCLUDED.freshness_column,
                freshness_scope_column    = EXCLUDED.freshness_scope_column,
                freshness_ttl_seconds     = EXCLUDED.freshness_ttl_seconds,
                freshness_database        = EXCLUDED.freshness_database,
                resource_group            = EXCLUDED.resource_group,
                timeout_seconds           = EXCLUDED.timeout_seconds,
                max_concurrency           = EXCLUDED.max_concurrency,
                max_db_connections        = EXCLUDED.max_db_connections,
                max_retries               = EXCLUDED.max_retries,
                retry_delay_seconds       = EXCLUDED.retry_delay_seconds,
                default_args              = EXCLUDED.default_args,
                updated_at                = NOW()
        """), PIPELINE)

    print('    seeded igloo_pin_sync (cron=*/15 * * * *)')
    print('[2] Done.')


if __name__ == '__main__':
    main()
