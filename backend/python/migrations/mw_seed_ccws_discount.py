"""
Create esa_middleware.ccws_discount table (via CcwsDiscount model) and seed
the orchestrator registry row in mw_sync_pipelines.

Does NOT copy data from esa_pbi — the new orchestrator pipeline will populate
on its first run.

Run from backend/python:
    python3 migrations/mw_seed_ccws_discount.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url
from common.models import CcwsDiscount, Base as CommonBase


def main():
    mw_engine = create_engine(get_database_url('middleware'))

    print('[1] Creating ccws_discount in esa_middleware (if missing)...')
    CommonBase.metadata.create_all(mw_engine, tables=[CcwsDiscount.__table__])
    print('    done')

    print('[2] Seeding mw_sync_pipelines row for ccws_discount...')
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
                TRUE, 'on_demand', '{}'::jsonb,
                'ccws_discount', 'updated_at', 'SiteID',
                :ttl, 'middleware',
                :mc, 'soap_api', 2,
                1200, 3, 300,
                CAST(:args AS jsonb)
            )
            ON CONFLICT (pipeline_name) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                description = EXCLUDED.description,
                pipeline_class = EXCLUDED.pipeline_class,
                freshness_table = EXCLUDED.freshness_table,
                freshness_column = EXCLUDED.freshness_column,
                freshness_scope_column = EXCLUDED.freshness_scope_column,
                freshness_database = EXCLUDED.freshness_database,
                default_args = EXCLUDED.default_args,
                updated_at = NOW()
        """), {
            'name': 'ccws_discount',
            'display': 'CCWS Discount Plans',
            'desc': 'Fetch discount/concession plans from CallCenterWs '
                    'DiscountPlansRetrieveIncludingDisabled → esa_middleware.ccws_discount',
            'cls': 'sync_service.pipelines.ccws_discount.CcwsDiscountPipeline',
            'ttl': 3600,
            'mc': 5,
            'args': '{"location_codes": ["L001","L002","L003","L004","L005","L006","L007","L008","L009","L010","L011","L013","L015","L017","L018","L019","L020","L021","L022","L023","L024","L025","L026","L028","L029","L030","L031","LSETUP"]}',
        })
    print('    done')


if __name__ == '__main__':
    main()
