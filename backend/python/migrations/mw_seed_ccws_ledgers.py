"""
Register sync_service.pipelines.ccws_ledgers.CcwsLedgersPipeline in
mw_sync_pipelines so the orchestrator picks it up.

The legacy APScheduler entry (config/pipelines.yaml) was enabled but its
APScheduler jobs never registered, so ccws_ledgers data has been frozen
at 2026-04-20 since then. This makes the orchestrator the source of truth.

Run from backend/python:
    python3 migrations/mw_seed_ccws_ledgers.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    mw_engine = create_engine(get_database_url('middleware'))

    print('[1] Seeding mw_sync_pipelines row for ccws_ledgers...')
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
                'ccws_ledgers', 'extract_date', NULL,
                :ttl, 'pbi',
                1, 'soap_api', 3,
                7200, 3, 600,
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
                default_args = EXCLUDED.default_args,
                enabled = TRUE,
                updated_at = NOW()
        """), {
            'name': 'ccws_ledgers',
            'display': 'CCWS Tenant Ledgers & Charges',
            'desc': 'Sync ccws_ledgers + ccws_charges from CallCenterWs '
                    '(LedgersByTenantID_v3 + ChargesAllByLedgerID). '
                    'Wraps datalayer.tenant_ledger_charges_to_sql via subprocess. '
                    'Required upstream feed for ECRI eligibility view and outcome tracking.',
            'cls': 'sync_service.pipelines.ccws_ledgers.CcwsLedgersPipeline',
            'sched': '{"cron": "0 7 * * *"}',
            'ttl': 23 * 3600,
            'args': (
                '{"mode": "incremental", "days_back": 7, "location_codes": ['
                '"L001","L002","L003","L004","L005","L006","L007","L008","L009","L010",'
                '"L011","L013","L015","L017","L018","L019","L020","L021","L022","L023",'
                '"L024","L025","L026","L028","L029","L030","L031","LSETUP"]}'
            ),
        })
    print('    done')


if __name__ == '__main__':
    main()
