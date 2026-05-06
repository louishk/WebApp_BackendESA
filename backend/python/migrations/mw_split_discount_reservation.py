"""
Migrate discount_plans, discount_plan_config, reservation_fees from
esa_backend → esa_middleware (renamed with mw_ prefix).

- Creates mw_* tables in esa_middleware via SQLAlchemy metadata.
- Copies rows from esa_backend old tables.
- Leaves old esa_backend tables intact for rollback. Drop manually later.

Run from backend/python:
    python3 migrations/mw_split_discount_reservation.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url
from web.models.base import Base
from web.models.discount_plan import DiscountPlan
from web.models.discount_plan_config import DiscountPlanConfig
from web.models.reservation_fee import ReservationFee


PAIRS = [
    # (old_table_in_backend, model_with_new_mw_tablename)
    ('discount_plans', DiscountPlan),
    ('discount_plan_config', DiscountPlanConfig),
    ('reservation_fees', ReservationFee),
]


def main():
    backend_engine = create_engine(get_database_url('backend'))
    mw_engine = create_engine(get_database_url('middleware'))

    # 1. Create tables in esa_middleware
    print('[1] Creating tables in esa_middleware...')
    tables = [m.__table__ for _, m in PAIRS]
    Base.metadata.create_all(mw_engine, tables=tables)
    print('    done')

    # 2. Copy rows
    for old_name, model in PAIRS:
        new_name = model.__tablename__
        cols = [c.name for c in model.__table__.columns]
        col_list = ', '.join(f'"{c}"' for c in cols)
        placeholders = ', '.join(f':{c}' for c in cols)

        print(f'[2] Copy {old_name} -> {new_name} ...')
        with backend_engine.connect() as src:
            rows = src.execute(text(f'SELECT {col_list} FROM "{old_name}"')).mappings().all()

        if not rows:
            print(f'    (0 rows, skipped)')
            continue

        with mw_engine.begin() as dst:
            # Clear target to be idempotent
            dst.execute(text(f'TRUNCATE TABLE "{new_name}" RESTART IDENTITY CASCADE'))
            # Use Core insert() so SQLAlchemy applies column type bindings
            # (JSONB dict/list → JSON encoded, etc.)
            dst.execute(
                model.__table__.insert(),
                [dict(r) for r in rows],
            )
            # Fix identity sequence so new inserts don't clash with copied ids
            dst.execute(text(
                f"SELECT setval(pg_get_serial_sequence('\"{new_name}\"', 'id'), "
                f"COALESCE((SELECT MAX(id) FROM \"{new_name}\"), 1))"
            ))
        print(f'    copied {len(rows)} rows')

    print('\nDone. Old tables in esa_backend are untouched — drop manually after verification.')


if __name__ == '__main__':
    main()
