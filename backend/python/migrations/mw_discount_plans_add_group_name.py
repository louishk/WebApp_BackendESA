"""
Add group_name column to mw_discount_plans so plans can be bucketed
(e.g. a promo campaign that covers SG Self + SG Wine + MY + KR all under
"Moving Season 2026-Q2").

Run from backend/python:
    python3 migrations/mw_discount_plans_add_group_name.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        print('[1] Adding group_name column...')
        conn.execute(text("""
            ALTER TABLE mw_discount_plans
            ADD COLUMN IF NOT EXISTS group_name VARCHAR(255)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_mw_discount_plans_group_name
            ON mw_discount_plans (group_name)
        """))
        print('    done')


if __name__ == '__main__':
    main()
