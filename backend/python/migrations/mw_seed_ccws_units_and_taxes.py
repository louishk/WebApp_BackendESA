"""
Create ccws_rent_tax_rates + ccws_available_units in esa_middleware and
seed mw_sync_pipelines rows for the 2 new orchestrator pipelines.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url
from common.models import (
    Base as CommonBase,
    CcwsRentTaxRate,
    CcwsAvailableUnit,
)

LOCATION_CODES = [
    "L001", "L002", "L003", "L004", "L005", "L006", "L007", "L008",
    "L009", "L010", "L011", "L013", "L015", "L017", "L018", "L019",
    "L020", "L021", "L022", "L023", "L024", "L025", "L026", "L028",
    "L029", "L030", "L031", "LSETUP",
]

PIPELINES = [
    (
        'ccws_rent_tax_rates',
        CcwsRentTaxRate,
        'CCWS Rent Tax Rates',
        'Per-site rent tax rates from RentTaxRatesRetrieve',
        'sync_service.pipelines.ccws_rent_tax_rates.CcwsRentTaxRatesPipeline',
        86400,  # TTL 1 day (tax rates change rarely)
        'ccws_rent_tax_rates',
        '0 6 * * 0',  # weekly Sunday 06:00 UTC
        'SiteCode',
    ),
    (
        'ccws_available_units',
        CcwsAvailableUnit,
        'CCWS Available Units',
        'Snapshot of available units from UnitsInformationAvailableUnitsOnly_v2',
        'sync_service.pipelines.ccws_available_units.CcwsAvailableUnitsPipeline',
        900,  # 15 min TTL (availability changes frequently)
        'ccws_available_units',
        '*/15 * * * *',  # every 15 minutes
        'SiteID',
    ),
]


def main():
    mw_engine = create_engine(get_database_url('middleware'))

    print('[1] Creating tables in esa_middleware...')
    tables = [m.__table__ for (_n, m, *_rest) in PIPELINES]
    CommonBase.metadata.create_all(mw_engine, tables=tables)
    print('    done')

    print('[2] Seeding mw_sync_pipelines rows...')
    with mw_engine.begin() as conn:
        for (name, _m, display, desc, cls, ttl, fresh_table, cron, scope_col) in PIPELINES:
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
                    :ft, 'updated_at', :sc,
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
                'ft': fresh_table, 'sc': scope_col, 'ttl': ttl,
                'args': json.dumps({'location_codes': LOCATION_CODES}),
            })
            print(f'    seeded {name} (cron={cron})')
    print('    done')


if __name__ == '__main__':
    main()
