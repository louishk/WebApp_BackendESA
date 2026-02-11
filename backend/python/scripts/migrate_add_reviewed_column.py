"""
Migration: Add reviewed column to inventory_unit_overrides.

Allows staff to mark units as "reviewed" in the inventory checker
so they can filter to show only unreviewed units.

Usage:
    python -m scripts.migrate_add_reviewed_column
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from common.config_loader import get_database_url
from sqlalchemy import create_engine


NEW_COLUMNS = [
    ("inventory_unit_overrides", "reviewed", "BOOLEAN DEFAULT FALSE"),
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
    print("Migration: Add Reviewed Column to inventory_unit_overrides")
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
