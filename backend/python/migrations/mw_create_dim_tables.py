"""Create mw_dim_* tables in esa_middleware as mirrors of esa_pbi.dim_* tables.

These are static reference tables (climate type, pillar, size category/range,
unit shape/type). One-shot seed — no ongoing sync. PBI stays canonical.

Pattern follows mw_create_mw_siteinfo.py:
- Introspects each source table via information_schema.
- Creates mw_dim_* with matching column types and PK on the natural key.
- Seeds from PBI with ON CONFLICT DO NOTHING (idempotent).
- Re-running after a column is added on PBI reconciles via
  ALTER TABLE ADD COLUMN IF NOT EXISTS.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


# (source_table_in_pbi, pk_column)
TABLES = [
    ('dim_climate_type', 'code'),
    ('dim_pillar', 'code'),
    ('dim_size_category', 'code'),
    ('dim_size_range', 'range_code'),
    ('dim_unit_shape', 'code'),
    ('dim_unit_type', 'code'),
]


def _pg_type_expr(col: dict) -> str:
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
    return dt.upper()


def _introspect_columns(pbi_engine, source_table: str) -> list:
    sql = text("""
        SELECT column_name, data_type, character_maximum_length,
               numeric_precision, numeric_scale, is_nullable
        FROM information_schema.columns
        WHERE table_name = :t AND table_schema = 'public'
        ORDER BY ordinal_position
    """)
    with pbi_engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(sql, {'t': source_table}).fetchall()]


def _middleware_columns(mw_engine, target_table: str) -> set:
    sql = text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = :t AND table_schema = 'public'
    """)
    with mw_engine.connect() as conn:
        return {r[0] for r in conn.execute(sql, {'t': target_table}).fetchall()}


def _build_create_ddl(target_table: str, columns: list, pk_column: str) -> str:
    parts = []
    for col in columns:
        name = col['column_name']
        type_expr = _pg_type_expr(col)
        nullable = '' if col['is_nullable'] == 'YES' else ' NOT NULL'
        parts.append(f'  "{name}" {type_expr}{nullable}')
    cols_sql = ',\n'.join(parts)
    return (
        f'CREATE TABLE IF NOT EXISTS {target_table} (\n'
        f'{cols_sql},\n'
        f'  PRIMARY KEY ("{pk_column}")\n'
        f')'
    )


def _add_missing_columns(mw_engine, target_table: str, columns: list) -> list:
    existing = _middleware_columns(mw_engine, target_table)
    added = []
    with mw_engine.begin() as conn:
        for col in columns:
            name = col['column_name']
            if name in existing:
                continue
            type_expr = _pg_type_expr(col)
            ddl = f'ALTER TABLE {target_table} ADD COLUMN IF NOT EXISTS "{name}" {type_expr}'
            conn.execute(text(ddl))
            added.append(name)
    return added


def _seed_rows(pbi_engine, mw_engine, source_table: str, target_table: str,
               columns: list, pk_column: str) -> tuple:
    col_names = [c['column_name'] for c in columns]
    col_list = ', '.join(f'"{c}"' for c in col_names)

    with pbi_engine.connect() as conn:
        rows = conn.execute(text(f'SELECT {col_list} FROM {source_table}')).fetchall()

    if not rows:
        return 0, 0

    placeholders = ', '.join(f':{c}' for c in col_names)
    insert_sql = text(
        f'INSERT INTO {target_table} ({col_list}) VALUES ({placeholders}) '
        f'ON CONFLICT ("{pk_column}") DO NOTHING'
    )
    payload = [dict(r._mapping) for r in rows]
    with mw_engine.begin() as conn:
        result = conn.execute(insert_sql, payload)
        inserted = result.rowcount if result.rowcount is not None else 0
    return len(rows), inserted


def _mirror_table(pbi_engine, mw_engine, source_table: str, pk_column: str) -> None:
    target_table = f'mw_{source_table}'
    print(f'\n=== {source_table} -> {target_table} ===')

    print(f'[1] Introspecting esa_pbi.{source_table}...')
    columns = _introspect_columns(pbi_engine, source_table)
    if not columns:
        print(f'    ! source table {source_table} not found in esa_pbi — skipping')
        return
    print(f'    {len(columns)} columns found')

    print(f'[2] Creating {target_table} in esa_middleware (if missing)...')
    ddl = _build_create_ddl(target_table, columns, pk_column)
    with mw_engine.begin() as conn:
        conn.execute(text(ddl))
    print('    done')

    print('[3] Reconciling columns (add missing)...')
    added = _add_missing_columns(mw_engine, target_table, columns)
    if added:
        print(f'    added: {", ".join(added)}')
    else:
        print('    no new columns')

    print('[4] Seeding rows from esa_pbi (ON CONFLICT DO NOTHING)...')
    total, inserted = _seed_rows(pbi_engine, mw_engine, source_table, target_table, columns, pk_column)
    skipped = total - inserted
    print(f'    read {total} rows from PBI -> inserted {inserted}, skipped {skipped}')


def main():
    pbi = create_engine(get_database_url('pbi'))
    mw = create_engine(get_database_url('middleware'))

    try:
        for source_table, pk_column in TABLES:
            _mirror_table(pbi, mw, source_table, pk_column)
    finally:
        pbi.dispose()
        mw.dispose()

    print('\ndone.')


if __name__ == '__main__':
    main()
