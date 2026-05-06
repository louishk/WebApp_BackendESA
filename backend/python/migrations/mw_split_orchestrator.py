"""
Migrate sync_service orchestrator tables from esa_backend → esa_middleware
with mw_ prefix.

Source (esa_backend): sync_pipelines, sync_runs, sync_state, sync_service_state
Target (esa_middleware): mw_sync_pipelines, mw_sync_runs, mw_sync_state,
                         mw_sync_service_state

Run from backend/python:
    python3 migrations/mw_split_orchestrator.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url
from sync_service.models import (
    Base, SyncPipeline, SyncRun, SyncStateEntry, SyncServiceState,
)


# (old_name_in_backend, model_with_new_mw_tablename)
PAIRS = [
    ('sync_pipelines', SyncPipeline),
    ('sync_runs', SyncRun),
    ('sync_state', SyncStateEntry),
    ('sync_service_state', SyncServiceState),
]


def main():
    backend_engine = create_engine(get_database_url('backend'))
    mw_engine = create_engine(get_database_url('middleware'))

    print('[1] Creating mw_sync_* tables in esa_middleware...')
    tables = [m.__table__ for _, m in PAIRS]
    Base.metadata.create_all(mw_engine, tables=tables)
    print('    done')

    for old_name, model in PAIRS:
        new_name = model.__tablename__
        cols = [c.name for c in model.__table__.columns]
        col_list = ', '.join(f'"{c}"' for c in cols)

        print(f'[2] Copy {old_name} -> {new_name} ...')
        try:
            with backend_engine.connect() as src:
                rows = src.execute(
                    text(f'SELECT {col_list} FROM "{old_name}"')
                ).mappings().all()
        except Exception as e:
            print(f'    source table missing or empty ({e}); skipped')
            continue

        if not rows:
            print('    (0 rows)')
            continue

        with mw_engine.begin() as dst:
            dst.execute(text(f'TRUNCATE TABLE "{new_name}" CASCADE'))
            dst.execute(model.__table__.insert(), [dict(r) for r in rows])
        print(f'    copied {len(rows)} rows')

    print('\nDone. Old esa_backend tables untouched — drop manually after verification.')


if __name__ == '__main__':
    main()
