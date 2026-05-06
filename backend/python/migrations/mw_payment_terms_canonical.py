"""
Replace mw_discount_plan_config payment_terms options with the canonical
set: Flexible / Prepaid / Fixed.

Idempotent: existing payment_terms rows are soft-deactivated (is_active=false)
rather than deleted, so historical plans keep a valid FK to their original
value. New rows upsert on (field_name, option_value).

Run from backend/python:
    python3 migrations/mw_payment_terms_canonical.py
"""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


CANONICAL = [
    ('Flexible', 1),
    ('Prepaid', 2),
    ('Fixed', 3),
]


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        print('[1] Deactivating non-canonical payment_terms options...')
        canonical_values = [v for v, _ in CANONICAL]
        res = conn.execute(text("""
            UPDATE mw_discount_plan_config
            SET is_active = FALSE,
                updated_at = NOW()
            WHERE field_name = 'payment_terms'
              AND option_value <> ALL(:keep)
              AND is_active = TRUE
        """), {'keep': canonical_values})
        print(f'    deactivated {res.rowcount} row(s)')

        print('[2] Upserting canonical payment_terms options...')
        for value, sort_order in CANONICAL:
            # No unique constraint on (field_name, option_value) — do a manual
            # check-and-update, insert if missing.
            exists = conn.execute(text("""
                SELECT id FROM mw_discount_plan_config
                WHERE field_name = 'payment_terms' AND option_value = :val
                LIMIT 1
            """), {'val': value}).scalar()
            if exists:
                conn.execute(text("""
                    UPDATE mw_discount_plan_config
                    SET is_active = TRUE,
                        sort_order = :sort,
                        updated_at = :now
                    WHERE id = :id
                """), {'id': exists, 'sort': sort_order, 'now': datetime.utcnow()})
            else:
                conn.execute(text("""
                    INSERT INTO mw_discount_plan_config (
                        field_name, option_value, translations,
                        sort_order, is_active, created_at, updated_at
                    ) VALUES (
                        'payment_terms', :val, '{}'::jsonb,
                        :sort, TRUE, :now, :now
                    )
                """), {'val': value, 'sort': sort_order, 'now': datetime.utcnow()})
        print(f'    upserted {len(CANONICAL)} canonical row(s)')

    print('Done.')


if __name__ == '__main__':
    main()
