"""
Add booked_concession_id column to mw_recommendations_served.

The outcome write-back now stores the concession id the booking actually
used. Combined with booked_plan_id (auto-derived from the matched slot),
this lets analytics distinguish:
  - "Customer accepted slot 1's plan + concession exactly"
    (booked_plan_id == slot1_plan_id AND booked_concession_id == slot1_concession_id)
  - "Customer accepted the unit but chose a different deal"
    (booked_unit_id matches a slot but booked_concession_id doesn't)

Run from backend/python:
    python3 migrations/mw_recs_add_booked_concession_id.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        print('[1] Adding booked_concession_id...')
        conn.execute(text("""
            ALTER TABLE mw_recommendations_served
            ADD COLUMN IF NOT EXISTS booked_concession_id INTEGER
        """))
        print('    done')


if __name__ == '__main__':
    main()
