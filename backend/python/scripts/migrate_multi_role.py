"""
Migration: Add multi-role support for users.

Creates a user_roles join table for many-to-many relationship between users and roles.
Migrates existing role_id data into the join table.

Usage:
    python scripts/migrate_multi_role.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from common.config_loader import get_database_url
from sqlalchemy import create_engine


def run_migration():
    """Create user_roles join table and migrate existing data."""
    db_url = get_database_url('backend')
    engine = create_engine(db_url)

    print("=" * 60)
    print("Migration: Multi-role support for users")
    print("=" * 60)
    print()

    migration_sql = Path(__file__).parent.parent / 'migrations' / '005_multi_role.sql'

    with engine.connect() as conn:
        # Check if table already exists
        check_query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_name = 'user_roles'
        """)
        result = conn.execute(check_query)
        exists = result.fetchone() is not None

        if exists:
            print("  [user_roles] Table already exists - checking for missing data")
            # Still run the INSERT to catch any users not yet migrated
            migrate_query = text("""
                INSERT INTO user_roles (user_id, role_id)
                SELECT id, role_id FROM users WHERE role_id IS NOT NULL
                ON CONFLICT DO NOTHING
            """)
            result = conn.execute(migrate_query)
            conn.commit()
            print(f"  [user_roles] Migrated {result.rowcount} additional user-role mapping(s)")
        else:
            print("  [user_roles] Creating join table...")
            create_query = text("""
                CREATE TABLE user_roles (
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    role_id INTEGER NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
                    PRIMARY KEY (user_id, role_id)
                )
            """)
            conn.execute(create_query)
            conn.commit()
            print("  [user_roles] Table created")

            # Migrate existing data
            print("  [user_roles] Migrating existing role assignments...")
            migrate_query = text("""
                INSERT INTO user_roles (user_id, role_id)
                SELECT id, role_id FROM users WHERE role_id IS NOT NULL
                ON CONFLICT DO NOTHING
            """)
            result = conn.execute(migrate_query)
            conn.commit()
            print(f"  [user_roles] Migrated {result.rowcount} user-role mapping(s)")

    print()
    print("Migration complete!")
    print("Note: users.role_id column is preserved but no longer used by the application.")


if __name__ == '__main__':
    run_migration()
