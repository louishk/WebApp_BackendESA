"""
Add is_stdrate_override column to mw_discount_plans.

When TRUE, the plan represents a "Standard Rate" pseudo-plan: the booking
flow should send ConcessionID=0 to SOAP (no discount applied). The
candidates pipeline emits one row per applicable site/unit with all
concession-derived fields nulled and effective_rate = std_rate.

Run from backend/python:
    python3 migrations/mw_discount_plans_add_stdrate_override.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        print('[1] Adding is_stdrate_override column...')
        conn.execute(text("""
            ALTER TABLE mw_discount_plans
            ADD COLUMN IF NOT EXISTS is_stdrate_override BOOLEAN NOT NULL DEFAULT FALSE
        """))
        print('    done')


if __name__ == '__main__':
    main()
