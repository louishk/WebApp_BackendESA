"""
Add the `restrictions` JSONB column to mw_discount_plans.

Stores the per-dim restriction map from the SOP COM01 naming convention, e.g.
    {
        "size_category": ["M", "L"],
        "climate_type": ["A", "AD"],
        "unit_type": ["W", "E"]
    }

Empty list or missing key = no restriction on that dim. Idempotent.

Run from backend/python:
    python3 migrations/mw_discount_plans_add_restrictions.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        print('[1] Adding mw_discount_plans.restrictions (JSONB) if missing...')
        conn.execute(text("""
            ALTER TABLE mw_discount_plans
            ADD COLUMN IF NOT EXISTS restrictions JSONB DEFAULT '{}'::jsonb
        """))
        # Back-fill nulls so every row has a valid object — simplifies API reads.
        conn.execute(text("""
            UPDATE mw_discount_plans
            SET restrictions = '{}'::jsonb
            WHERE restrictions IS NULL
        """))
        print('    done')


if __name__ == '__main__':
    main()
