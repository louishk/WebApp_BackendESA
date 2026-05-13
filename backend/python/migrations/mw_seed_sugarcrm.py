"""
Register sync_service.pipelines.sugarcrm.SugarCrmPipeline in
mw_sync_pipelines so the orchestrator picks it up.

Replaces the legacy APScheduler entry `sugarcrm` from config/pipelines.yaml.
Watermark/checkpoint config from the YAML sync: block is intentionally NOT
forwarded — the datalayer module manages its own per-module watermark.

Run from backend/python:
    python3 migrations/mw_seed_sugarcrm.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    mw_engine = create_engine(get_database_url('middleware'))

    print('[1] Seeding mw_sync_pipelines row for sugarcrm...')
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
                'sugarcrm_leads', 'synced_at', NULL,
                :ttl, 'pbi',
                1, 'http_api', 3,
                14400, 5, 600,
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
            'name': 'sugarcrm',
            'display': 'SugarCRM',
            'desc': 'Sync CRM data (Leads, Contacts, Accounts, etc.) from SugarCRM. '
                    'Wraps datalayer.sugarcrm_to_sql via subprocess. The datalayer '
                    'module manages its own per-module watermark, so checkpoint '
                    'config from the legacy YAML is not forwarded.',
            'cls': 'sync_service.pipelines.sugarcrm.SugarCrmPipeline',
            'sched': '{"cron": "0 */3 * * *"}',
            'ttl': 4 * 3600,
            'args': '{"mode": "auto"}',
        })
    print('    done')


if __name__ == '__main__':
    main()
