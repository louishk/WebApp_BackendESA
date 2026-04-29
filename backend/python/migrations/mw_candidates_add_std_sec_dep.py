"""
Add std_sec_dep column to mw_unit_discount_candidates so the recommender
can quote with the unit's actual deposit (which can differ from std_rate
on premium / discounted-deposit / no-deposit units).

Run from backend/python:
    python3 migrations/mw_candidates_add_std_sec_dep.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        print('[1] Adding std_sec_dep column...')
        conn.execute(text("""
            ALTER TABLE mw_unit_discount_candidates
            ADD COLUMN IF NOT EXISTS std_sec_dep NUMERIC(14, 4)
        """))
        print('    done')


if __name__ == '__main__':
    main()
