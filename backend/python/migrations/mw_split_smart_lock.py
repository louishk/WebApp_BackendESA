"""
Migrate smart_lock_* tables from esa_backend → esa_middleware (mw_ prefix).

Tables copied (preserving IDs + FKs):
  smart_lock_keypads           → mw_smart_lock_keypads
  smart_lock_padlocks          → mw_smart_lock_padlocks
  smart_lock_unit_assignments  → mw_smart_lock_unit_assignments
  smart_lock_site_config       → mw_smart_lock_site_config
  smart_lock_audit_log         → mw_smart_lock_audit_log
  igloo_access_codes           → mw_igloo_access_codes

Old esa_backend tables are LEFT INTACT for rollback; drop manually after
verification. Legacy scheduler pipeline for gate_access_data keeps writing
to esa_backend.gate_access_data (the orchestrator equivalent is ccws_gate_access
in middleware).

Run from backend/python:
    python3 migrations/mw_split_smart_lock.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url

# Import models so Base.metadata knows the new tables
from web.models.base import Base
from web.models.smart_lock import (
    SmartLockKeypad, SmartLockPadlock, SmartLockUnitAssignment,
    SmartLockAuditLog, SmartLockSiteConfig, IglooAccessCode,
)


# (old_table_in_backend, model_with_new_mw_tablename)
# Order matters — parents before children (FK targets first).
PAIRS = [
    ('smart_lock_keypads', SmartLockKeypad),
    ('smart_lock_padlocks', SmartLockPadlock),
    ('smart_lock_unit_assignments', SmartLockUnitAssignment),
    ('smart_lock_site_config', SmartLockSiteConfig),
    ('smart_lock_audit_log', SmartLockAuditLog),
    ('igloo_access_codes', IglooAccessCode),
]


def main():
    backend_engine = create_engine(get_database_url('backend'))
    mw_engine = create_engine(get_database_url('middleware'))

    print('[1] Creating mw_smart_lock_* / mw_igloo_access_codes in esa_middleware...')
    tables = [m.__table__ for _, m in PAIRS]
    Base.metadata.create_all(mw_engine, tables=tables)
    print('    done')

    for old_name, model in PAIRS:
        new_name = model.__tablename__
        cols = [c.name for c in model.__table__.columns]
        col_list = ', '.join(f'"{c}"' for c in cols)

        print(f'[2] Copy {old_name} -> {new_name} ...')
        with backend_engine.connect() as src:
            rows = src.execute(text(
                f'SELECT {col_list} FROM "{old_name}"'
            )).mappings().all()

        if not rows:
            print('    (0 rows)')
            continue

        with mw_engine.begin() as dst:
            dst.execute(text(f'TRUNCATE TABLE "{new_name}" RESTART IDENTITY CASCADE'))
            # Use Core insert() so SQLAlchemy applies per-column type adapters
            dst.execute(model.__table__.insert(), [dict(r) for r in rows])

            # Reset identity seq so future inserts don't collide with copied ids
            pk_col = None
            for c in model.__table__.columns:
                if c.primary_key and c.autoincrement is True:
                    pk_col = c.name
                    break
            if pk_col:
                dst.execute(text(
                    f"SELECT setval(pg_get_serial_sequence('\"{new_name}\"', '{pk_col}'), "
                    f"COALESCE((SELECT MAX(\"{pk_col}\") FROM \"{new_name}\"), 1))"
                ))
        print(f'    copied {len(rows)} rows')

    print('\nDone. Old tables in esa_backend are UNTOUCHED — drop manually after verification.')


if __name__ == '__main__':
    main()
