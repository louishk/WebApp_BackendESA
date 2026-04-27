"""
Create esa_middleware.mw_unit_discount_candidates (via UnitDiscountCandidate
model) and seed the orchestrator registry row in mw_sync_pipelines.

Does NOT compute candidates — the UnitDiscountCandidatesPipeline populates
the table on its first run.

Run from backend/python:
    python3 migrations/mw_seed_unit_discount_candidates.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url
from common.models import UnitDiscountCandidate, Base as CommonBase


def main():
    mw_engine = create_engine(get_database_url('middleware'))

    print('[1] Creating mw_unit_discount_candidates in esa_middleware (if missing)...')
    CommonBase.metadata.create_all(
        mw_engine, tables=[UnitDiscountCandidate.__table__],
    )
    print('    done')

    print('[2] Seeding mw_sync_pipelines row for mw_unit_discount_candidates...')
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
                'mw_unit_discount_candidates', 'computed_at', 'site_id',
                :ttl, 'middleware',
                :mc, 'db_pool', 4,
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
            'name': 'mw_unit_discount_candidates',
            'display': 'MW Unit Discount Candidates',
            'desc': 'Recombine ccws_units × ccws_discount × '
                    'mw_discount_plans.linked_concessions into a per-unit '
                    'candidate table for the recommendation engine. '
                    'sTypeName decomposed per SOP COM01.',
            'cls': 'sync_service.pipelines.mw_unit_discount_candidates.UnitDiscountCandidatesPipeline',
            'ttl': 14400,  # 4h
            'mc': 5,
            'args': '{"location_codes": ["L001","L002","L003","L004","L005",'
                    '"L006","L007","L008","L009","L010","L011","L013","L015",'
                    '"L017","L018","L019","L020","L021","L022","L023","L024",'
                    '"L025","L026","L028","L029","L030","L031","LSETUP"]}',
        })
    print('    done')


if __name__ == '__main__':
    main()
