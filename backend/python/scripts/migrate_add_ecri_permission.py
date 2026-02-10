"""
Migration: Add ECRI permission columns to roles table.

This adds two ECRI permissions:
- can_access_ecri: View ECRI dashboards, eligibility lists, analytics
- can_manage_ecri: Create batches, execute pushes to SiteLink

Usage:
    python scripts/migrate_add_ecri_permission.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from common.config_loader import get_database_url
from sqlalchemy import create_engine


def run_migration():
    """Add ECRI permission columns to roles table."""
    db_url = get_database_url('backend')
    engine = create_engine(db_url)

    print("=" * 60)
    print("Migration: Add ECRI permissions to roles")
    print("=" * 60)
    print()

    columns = [
        ('can_access_ecri', 'ECRI view access'),
        ('can_manage_ecri', 'ECRI management'),
    ]

    with engine.connect() as conn:
        for col_name, description in columns:
            # Check if column already exists
            check_query = text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'roles'
                AND column_name = :col_name
            """)
            result = conn.execute(check_query, {'col_name': col_name})
            exists = result.fetchone() is not None

            if exists:
                print(f"  [roles] {col_name} column already exists - skipping")
            else:
                alter_query = text(f"""
                    ALTER TABLE roles
                    ADD COLUMN {col_name} BOOLEAN DEFAULT FALSE
                """)
                conn.execute(alter_query)
                conn.commit()
                print(f"  [roles] Added {col_name} column ({description})")

        # Update admin role to have both ECRI permissions
        update_query = text("""
            UPDATE roles
            SET can_access_ecri = TRUE,
                can_manage_ecri = TRUE
            WHERE name = 'admin'
        """)
        result = conn.execute(update_query)
        conn.commit()
        print(f"  [roles] Updated {result.rowcount} role(s) to have ECRI permissions (admin)")

    print()
    print("Migration complete!")


if __name__ == '__main__':
    run_migration()
