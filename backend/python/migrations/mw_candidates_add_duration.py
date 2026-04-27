"""
Add min_duration_months + max_duration_months columns to
mw_unit_discount_candidates. Metadata only — pipeline writes them from
plan.restrictions but doesn't filter on them.

Run from backend/python:
    python3 migrations/mw_candidates_add_duration.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        print('[1] Adding duration columns to mw_unit_discount_candidates...')
        conn.execute(text("""
            ALTER TABLE mw_unit_discount_candidates
            ADD COLUMN IF NOT EXISTS min_duration_months INTEGER,
            ADD COLUMN IF NOT EXISTS max_duration_months INTEGER
        """))
        print('    done')


if __name__ == '__main__':
    main()
