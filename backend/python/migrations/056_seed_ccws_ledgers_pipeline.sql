-- Seed scheduler_pipeline_config row for ccws_ledgers
-- Run against esa_backend DB (database=backend)
INSERT INTO scheduler_pipeline_config (
    pipeline_name, display_name, description, module_path,
    schedule_type, schedule_config, enabled, priority,
    conflicts_with, resource_group, max_db_connections, estimated_duration_seconds,
    max_retries, retry_delay_seconds, retry_backoff_multiplier, timeout_seconds,
    default_args, data_freshness_config, managed_by
) VALUES (
    'ccws_ledgers',
    'CCWS Tenant Ledgers & Charges',
    'Sync tenants, ledgers and charges from CallCenterWs (LedgersByTenantID_v3 + ChargesAllByLedgerID). Incremental refresh; required for fresh ECRI eligibility.',
    'datalayer.tenant_ledger_charges_to_sql',
    'cron',
    '{"type":"cron","cron":"0 7 * * *"}'::jsonb,
    true,
    2,
    ARRAY['rentroll','discount','mimo'],
    'soap_api',
    3,
    1800,
    3,
    300,
    2.0,
    7200,
    '{"mode":"incremental","days-back":7}'::jsonb,
    '{"table":"ccws_ledgers","date_column":"extract_date","database":"pbi"}'::jsonb,
    'scheduler'
)
ON CONFLICT (pipeline_name) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    description = EXCLUDED.description,
    module_path = EXCLUDED.module_path,
    schedule_type = EXCLUDED.schedule_type,
    schedule_config = EXCLUDED.schedule_config,
    enabled = EXCLUDED.enabled,
    priority = EXCLUDED.priority,
    conflicts_with = EXCLUDED.conflicts_with,
    resource_group = EXCLUDED.resource_group,
    max_db_connections = EXCLUDED.max_db_connections,
    estimated_duration_seconds = EXCLUDED.estimated_duration_seconds,
    max_retries = EXCLUDED.max_retries,
    retry_delay_seconds = EXCLUDED.retry_delay_seconds,
    retry_backoff_multiplier = EXCLUDED.retry_backoff_multiplier,
    timeout_seconds = EXCLUDED.timeout_seconds,
    default_args = EXCLUDED.default_args,
    data_freshness_config = EXCLUDED.data_freshness_config,
    updated_at = NOW();
