"""
Register sync_service.pipelines.outlook_calendar.OutlookCalendarPipeline
in mw_sync_pipelines so the orchestrator picks it up.

Replaces the legacy APScheduler entry `calendar` from config/pipelines.yaml.
Legacy schedule was manual (no cron) — registered here as on_demand so the
orchestrator API/CLI can trigger it but no auto-fire.

Run from backend/python:
    python3 migrations/mw_seed_outlook_calendar.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    mw_engine = create_engine(get_database_url('middleware'))

    print('[1] Seeding mw_sync_pipelines row for outlook_calendar...')
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
                TRUE, 'on_demand', CAST('{}' AS jsonb),
                'calendar_events', 'synced_at', NULL,
                :ttl, 'pbi',
                1, 'http_api', 2,
                1200, 3, 300,
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
            'name': 'outlook_calendar',
            'display': 'Outlook Calendar',
            'desc': 'Extract calendar events from configured Outlook mailboxes '
                    'via Microsoft Graph and write to calendar_events. '
                    'On-demand only (no cron) — trigger via orchestrator API/CLI.',
            'cls': 'sync_service.pipelines.outlook_calendar.OutlookCalendarPipeline',
            'ttl': 24 * 3600,
            'args': '{"mode": "auto"}',
        })
    print('    done')


if __name__ == '__main__':
    main()
