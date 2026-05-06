"""
Create ccws_* tables in esa_middleware and seed mw_sync_pipelines rows
for the 4 new orchestrator pipelines.

Run from backend/python:
    python3 migrations/mw_seed_ccws_pipelines.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url
from common.models import (
    Base as CommonBase,
    CcwsReservation,
    CcwsChargeDescription,
    CcwsInsuranceCoverage,
    CcwsSiteBillingConfig,
)

LOCATION_CODES = [
    "L001", "L002", "L003", "L004", "L005", "L006", "L007", "L008",
    "L009", "L010", "L011", "L013", "L015", "L017", "L018", "L019",
    "L020", "L021", "L022", "L023", "L024", "L025", "L026", "L028",
    "L029", "L030", "L031", "LSETUP",
]

# pipeline_name -> (model, display, desc, class_path, ttl_s, freshness_table, cron)
PIPELINES = [
    (
        'ccws_reservations',
        CcwsReservation,
        'CCWS Reservations',
        'Raw reservation records from ReservationList_v3',
        'sync_service.pipelines.ccws_reservations.CcwsReservationsPipeline',
        1800,  # 30 min TTL
        'ccws_reservations',
        '*/30 * * * *',  # every 30 min (matches legacy reservations_sync)
    ),
    (
        'ccws_charge_descriptions',
        CcwsChargeDescription,
        'CCWS Charge Descriptions',
        'Per-site charge type config (tax rates, prices) from ChargeDescriptionsRetrieve',
        'sync_service.pipelines.ccws_charge_descriptions.CcwsChargeDescriptionsPipeline',
        3600,
        'ccws_charge_descriptions',
        '15 5 * * 0',  # weekly Sunday 05:15
    ),
    (
        'ccws_insurance_coverage',
        CcwsInsuranceCoverage,
        'CCWS Insurance Coverage',
        'Per-site insurance plans from InsuranceCoverageRetrieve_V2',
        'sync_service.pipelines.ccws_insurance_coverage.CcwsInsuranceCoveragePipeline',
        3600,
        'ccws_insurance_coverage',
        '30 5 * * 0',  # weekly Sunday 05:30
    ),
    (
        'ccws_site_billing_config',
        CcwsSiteBillingConfig,
        'CCWS Site Billing Config',
        'Per-site proration config derived from MoveInCostRetrieveWithDiscount_v4',
        'sync_service.pipelines.ccws_site_billing_config.CcwsSiteBillingConfigPipeline',
        86400,
        'ccws_site_billing_config',
        '45 5 * * 0',  # weekly Sunday 05:45
    ),
]


def main():
    mw_engine = create_engine(get_database_url('middleware'))

    print('[1] Creating ccws_* tables in esa_middleware (if missing)...')
    tables = [m.__table__ for (_n, m, *_rest) in PIPELINES]
    CommonBase.metadata.create_all(mw_engine, tables=tables)
    print('    done')

    print('[2] Seeding mw_sync_pipelines rows...')
    import json
    with mw_engine.begin() as conn:
        for (name, _model, display, desc, cls, ttl, fresh_table, cron) in PIPELINES:
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
                    freshness_table = EXCLUDED.freshness_table,
                    freshness_column = EXCLUDED.freshness_column,
                    freshness_scope_column = EXCLUDED.freshness_scope_column,
                    freshness_ttl_seconds = EXCLUDED.freshness_ttl_seconds,
                    freshness_database = EXCLUDED.freshness_database,
                    default_args = EXCLUDED.default_args,
                    updated_at = NOW()
            """), {
                'name': name, 'display': display, 'desc': desc, 'cls': cls,
                'sched': json.dumps({'cron': cron}),
                'ft': fresh_table, 'ttl': ttl,
                'args': json.dumps({'location_codes': LOCATION_CODES}),
            })
            print(f'    seeded {name} (cron={cron})')
    print('    done')


if __name__ == '__main__':
    main()
