"""
Add smart_lock JSONB column to mw_unit_discount_candidates so each
candidate row carries the keypad(s) + padlock id assigned to that unit
(NULL when no assignment exists).

Shape: {"keypad_ids": [12345, 67890], "padlock_id": 999}

Run from backend/python:
    python3 migrations/mw_candidates_add_smart_lock.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        print('[1] Adding smart_lock JSONB column...')
        conn.execute(text("""
            ALTER TABLE mw_unit_discount_candidates
            ADD COLUMN IF NOT EXISTS smart_lock JSONB
        """))
        print('    done')


if __name__ == '__main__':
    main()
