"""
Add distribution_channel + hidden_rate columns to mw_unit_discount_candidates
so the recommender can filter/shape candidates by publish constraints.

Run from backend/python:
    python3 migrations/mw_candidates_add_distribution_channel.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        print('[1] Adding distribution_channel + hidden_rate columns...')
        conn.execute(text("""
            ALTER TABLE mw_unit_discount_candidates
            ADD COLUMN IF NOT EXISTS distribution_channel VARCHAR(255),
            ADD COLUMN IF NOT EXISTS hidden_rate BOOLEAN
        """))
        print('    done')


if __name__ == '__main__':
    main()
