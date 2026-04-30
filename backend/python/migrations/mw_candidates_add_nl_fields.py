"""
Phase 3.6 — add NL-friendly fields to mw_unit_discount_candidates so the
recommender can return human-readable labels and full plan terms in one
response without secondary lookups.

  concession_name      ccws_discount.sPlanName        (e.g. "Moving Season 2026")
  size_sqft            ccws_available_units            dcWidth × dcLength
  lock_in_months       integer parsed from mw_discount_plans.lock_in_period
  promo_valid_until    earliest of plan_end / promo_period_end / booking_period_end

`payment_terms` is already on the candidate table (added Phase 1) so it
does not need a column add — the pipeline just needs to keep populating
it (no change there).

Run from backend/python:
    python3 migrations/mw_candidates_add_nl_fields.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        print('[1] Adding NL fields to mw_unit_discount_candidates...')
        conn.execute(text("""
            ALTER TABLE mw_unit_discount_candidates
                ADD COLUMN IF NOT EXISTS concession_name   TEXT,
                ADD COLUMN IF NOT EXISTS size_sqft         NUMERIC(10, 2),
                ADD COLUMN IF NOT EXISTS lock_in_months    INTEGER,
                ADD COLUMN IF NOT EXISTS promo_valid_until DATE
        """))
        print('    done')


if __name__ == '__main__':
    main()
