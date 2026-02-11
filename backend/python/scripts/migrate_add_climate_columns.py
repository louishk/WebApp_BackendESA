"""
Migration: Add climate columns to inventory tables.

Splits the old combined "Unit Type" into two independent dimensions:
- inventory_type_mappings.mapped_climate_code  (Type mapping climate)
- inventory_unit_overrides.climate_code         (Per-unit override climate)

Usage:
    python -m scripts.migrate_add_climate_columns
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from common.config_loader import get_database_url
from sqlalchemy import create_engine


NEW_COLUMNS = [
    ("inventory_type_mappings", "mapped_climate_code", "VARCHAR(5)"),
    ("inventory_unit_overrides", "climate_code", "VARCHAR(5)"),
]


def column_exists(conn, table_name: str, column_name: str) -> bool:
    """Check if a column exists in a table."""
    result = conn.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = :table_name
        AND column_name = :column_name
    """), {"table_name": table_name, "column_name": column_name})
    return result.fetchone() is not None


def run_migration():
    db_url = get_database_url('backend')
    engine = create_engine(db_url)

    print("=" * 60)
    print("Migration: Add Climate Columns to Inventory Tables")
    print("=" * 60)

    with engine.connect() as conn:
        added = 0
        skipped = 0
        for table_name, col_name, col_type in NEW_COLUMNS:
            if column_exists(conn, table_name, col_name):
                print(f"  SKIP  {table_name}.{col_name} (already exists)")
                skipped += 1
            else:
                conn.execute(text(f'ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}'))
                print(f"  ADDED {table_name}.{col_name} ({col_type})")
                added += 1
        conn.commit()

    print(f"\nDone: {added} added, {skipped} already existed.")
    print("=" * 60)


if __name__ == "__main__":
    run_migration()
