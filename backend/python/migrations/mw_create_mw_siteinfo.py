"""Create mw_siteinfo in esa_middleware as a mirror of esa_pbi.siteinfo.

Transitional: admin/siteinfo dual-writes to both DBs until the split completes.

- Introspects PBI siteinfo via information_schema (tracks dynamic columns).
- Creates mw_siteinfo with matching column types, PK on "SiteID", UNIQUE on "SiteCode".
- Seeds from PBI with ON CONFLICT DO NOTHING (idempotent; re-runs won't clobber).

Re-running this after admin adds a column on PBI is safe — new columns are
added to middleware via ALTER TABLE ADD COLUMN IF NOT EXISTS.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


TABLE_NAME = 'mw_siteinfo'


def _pg_type_expr(col: dict) -> str:
    """Build a PG type expression from an information_schema row."""
    dt = col['data_type']
    maxlen = col['character_maximum_length']
    prec = col['numeric_precision']
    scale = col['numeric_scale']

    if dt == 'character varying':
        return f'VARCHAR({maxlen})' if maxlen else 'VARCHAR'
    if dt == 'character':
        return f'CHAR({maxlen})' if maxlen else 'CHAR'
    if dt == 'numeric':
        if prec is not None and scale is not None:
            return f'NUMERIC({prec},{scale})'
        if prec is not None:
            return f'NUMERIC({prec})'
        return 'NUMERIC'
    if dt == 'timestamp without time zone':
        return 'TIMESTAMP'
    if dt == 'timestamp with time zone':
        return 'TIMESTAMPTZ'
    if dt == 'double precision':
        return 'DOUBLE PRECISION'
    # integer, bigint, smallint, boolean, text, date, real — pass-through
    return dt.upper()


def _introspect_pbi_columns(pbi_engine) -> list:
    sql = text("""
        SELECT column_name, data_type, character_maximum_length,
               numeric_precision, numeric_scale, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'siteinfo' AND table_schema = 'public'
        ORDER BY ordinal_position
    """)
    with pbi_engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(sql).fetchall()]


def _middleware_columns(mw_engine) -> set:
    sql = text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = :t AND table_schema = 'public'
    """)
    with mw_engine.connect() as conn:
        return {r[0] for r in conn.execute(sql, {'t': TABLE_NAME}).fetchall()}


def _build_create_ddl(columns: list) -> str:
    parts = []
    for col in columns:
        name = col['column_name']
        type_expr = _pg_type_expr(col)
        nullable = '' if col['is_nullable'] == 'YES' else ' NOT NULL'
        parts.append(f'  "{name}" {type_expr}{nullable}')
    cols_sql = ',\n'.join(parts)
    return (
        f'CREATE TABLE IF NOT EXISTS {TABLE_NAME} (\n'
        f'{cols_sql},\n'
        f'  PRIMARY KEY ("SiteID"),\n'
        f'  UNIQUE ("SiteCode")\n'
        f')'
    )


def _add_missing_columns(mw_engine, columns: list) -> list:
    existing = _middleware_columns(mw_engine)
    added = []
    with mw_engine.begin() as conn:
        for col in columns:
            name = col['column_name']
            if name in existing:
                continue
            type_expr = _pg_type_expr(col)
            ddl = f'ALTER TABLE {TABLE_NAME} ADD COLUMN IF NOT EXISTS "{name}" {type_expr}'
            conn.execute(text(ddl))
            added.append(name)
    return added


def _seed_rows(pbi_engine, mw_engine, columns: list) -> tuple:
    col_names = [c['column_name'] for c in columns]
    col_list = ', '.join(f'"{c}"' for c in col_names)

    with pbi_engine.connect() as conn:
        rows = conn.execute(text(f'SELECT {col_list} FROM siteinfo')).fetchall()

    if not rows:
        return 0, 0

    placeholders = ', '.join(f':{c}' for c in col_names)
    insert_sql = text(
        f'INSERT INTO {TABLE_NAME} ({col_list}) VALUES ({placeholders}) '
        f'ON CONFLICT ("SiteID") DO NOTHING'
    )
    payload = [dict(r._mapping) for r in rows]
    with mw_engine.begin() as conn:
        result = conn.execute(insert_sql, payload)
        inserted = result.rowcount if result.rowcount is not None else 0
    return len(rows), inserted


def main():
    pbi = create_engine(get_database_url('pbi'))
    mw = create_engine(get_database_url('middleware'))

    print('[1] Introspecting esa_pbi.siteinfo...')
    columns = _introspect_pbi_columns(pbi)
    print(f'    {len(columns)} columns found')

    print(f'[2] Creating {TABLE_NAME} in esa_middleware (if missing)...')
    ddl = _build_create_ddl(columns)
    with mw.begin() as conn:
        conn.execute(text(ddl))
    print('    done')

    print('[3] Reconciling columns (add missing)...')
    added = _add_missing_columns(mw, columns)
    if added:
        print(f'    added: {", ".join(added)}')
    else:
        print('    no new columns')

    print('[4] Seeding rows from esa_pbi (ON CONFLICT DO NOTHING)...')
    total, inserted = _seed_rows(pbi, mw, columns)
    skipped = total - inserted
    print(f'    read {total} rows from PBI → inserted {inserted}, skipped {skipped}')

    pbi.dispose()
    mw.dispose()
    print('done.')


if __name__ == '__main__':
    main()
