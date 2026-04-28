"""
Add coupon_code support to discount plans + candidates.

Behaviour: when a plan has hidden_rate=TRUE, it must NOT surface to public
channels (chatbot/web) unless the booking flow provides a matching
coupon_code. Non-hidden plans ignore the coupon field entirely.

The pipeline copies coupon_code from plan → candidate row so the
recommender can do its filtering with one table read.

Run from backend/python:
    python3 migrations/mw_add_coupon_code.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        print('[1] Adding coupon_code to mw_discount_plans...')
        conn.execute(text("""
            ALTER TABLE mw_discount_plans
            ADD COLUMN IF NOT EXISTS coupon_code VARCHAR(100)
        """))
        print('    done')

        print('[2] Adding coupon_code to mw_unit_discount_candidates...')
        conn.execute(text("""
            ALTER TABLE mw_unit_discount_candidates
            ADD COLUMN IF NOT EXISTS coupon_code VARCHAR(100)
        """))
        print('    done')

        print('[3] Adding partial index on candidates.coupon_code...')
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_mudc_coupon_code
            ON mw_unit_discount_candidates (coupon_code)
            WHERE coupon_code IS NOT NULL
        """))
        print('    done')


if __name__ == '__main__':
    main()
