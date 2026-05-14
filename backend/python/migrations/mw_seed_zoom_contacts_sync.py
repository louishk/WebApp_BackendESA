"""
Register sync_service.pipelines.zoom_contacts_sync.ZoomContactsSyncPipeline
in mw_sync_pipelines.

Seeded DISABLED to match the legacy yaml flag — flip enabled=TRUE in the
mw_sync_pipelines row when ready to roll out.

Run from backend/python:
    python3 migrations/mw_seed_zoom_contacts_sync.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    mw_engine = create_engine(get_database_url('middleware'))

    print('[1] Seeding mw_sync_pipelines row for zoom_contacts_sync (disabled)...')
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
                FALSE, 'cron', CAST(:sched AS jsonb),
                'zoom_contact_sync', 'last_synced_at', NULL,
                :ttl, 'pbi',
                1, 'http_api', 2,
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
                freshness_column = EXCLUDED.freshness_column,
                freshness_database = EXCLUDED.freshness_database,
                resource_group = EXCLUDED.resource_group,
                max_db_connections = EXCLUDED.max_db_connections,
                timeout_seconds = EXCLUDED.timeout_seconds,
                max_retries = EXCLUDED.max_retries,
                retry_delay_seconds = EXCLUDED.retry_delay_seconds,
                default_args = EXCLUDED.default_args,
                updated_at = NOW()
        """), {
            'name': 'zoom_contacts_sync',
            'display': 'Zoom Contacts Sync',
            'desc': 'Push SugarCRM contacts and leads (with phone numbers) to Zoom '
                    'Phone External Contacts. Maintains zoom_contact_sync mapping; '
                    'writes back external_contact_id to SugarCRM es_zoom_contact_id_c. '
                    'Auto mode is incremental via zoom_sync_state watermark.',
            'cls': 'sync_service.pipelines.zoom_contacts_sync.ZoomContactsSyncPipeline',
            'sched': '{"cron": "30 */3 * * *"}',
            'ttl': 4 * 3600,
            'args': '{"mode": "auto"}',
        })
    print('    done')


if __name__ == '__main__':
    main()
