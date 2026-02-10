"""
Migration: Add Microsoft 365 profile fields to users table.

Adds columns for data pulled from Microsoft Graph API /me endpoint:
- department, job_title, office_location, employee_id

These are populated automatically on each OAuth login.

Usage:
    python -m scripts.migrate_add_o365_profile_fields
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from common.config_loader import get_database_url
from sqlalchemy import create_engine


NEW_COLUMNS = [
    ("department", "VARCHAR(255)"),
    ("job_title", "VARCHAR(255)"),
    ("office_location", "VARCHAR(255)"),
    ("employee_id", "VARCHAR(255)"),
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
    print("Migration: Add O365 Profile Fields to Users Table")
    print("=" * 60)

    with engine.connect() as conn:
        added = 0
        skipped = 0
        for col_name, col_type in NEW_COLUMNS:
            if column_exists(conn, 'users', col_name):
                print(f"  SKIP  {col_name} (already exists)")
                skipped += 1
            else:
                conn.execute(text(f'ALTER TABLE users ADD COLUMN {col_name} {col_type}'))
                print(f"  ADDED {col_name} ({col_type})")
                added += 1
        conn.commit()

    print(f"\nDone: {added} added, {skipped} already existed.")
    print("=" * 60)


if __name__ == "__main__":
    run_migration()
