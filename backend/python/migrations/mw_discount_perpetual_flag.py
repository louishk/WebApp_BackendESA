"""
Phase 4 Part 1 — `discount_perpetual` flag.

Background: SiteLink concessions with iInMonth=1 only show a discount
line on month 1, but operations staff often follow up by clicking the
"Apply Tenant's Rate" button at move-in to bake the discounted price
into the unit's recurring rate. The customer ends up paying the
discounted amount every month, but our calculator (faithful to the
SOAP API) was quoting the full rate from month 2 onward — over-quoting
by the discount % across the lease.

This flag lets a plan owner declare the perpetual intent. When set,
the calculator applies the same discount to every month in the
breakdown, matching what the operator-applied Tenant's Rate will
actually charge. Part 2 will eventually automate the rate write so the
manual click goes away.

Adds two columns:
  mw_discount_plans.discount_perpetual          BOOLEAN DEFAULT FALSE
  mw_unit_discount_candidates.discount_perpetual BOOLEAN

Run from backend/python:
    python3 migrations/mw_discount_perpetual_flag.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        print('[1] mw_discount_plans.discount_perpetual ...')
        conn.execute(text("""
            ALTER TABLE mw_discount_plans
            ADD COLUMN IF NOT EXISTS discount_perpetual BOOLEAN NOT NULL DEFAULT FALSE
        """))
        print('    done')
        print('[2] mw_unit_discount_candidates.discount_perpetual ...')
        conn.execute(text("""
            ALTER TABLE mw_unit_discount_candidates
            ADD COLUMN IF NOT EXISTS discount_perpetual BOOLEAN
        """))
        print('    done')


if __name__ == '__main__':
    main()
