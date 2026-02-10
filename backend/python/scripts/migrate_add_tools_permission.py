"""
Migration: Add can_access_billing_tools column to roles table.

This adds the Billing Tools permission for controlling access to billing
management tools like the Billing Date Changer.

Usage:
    python scripts/migrate_add_tools_permission.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from common.config_loader import get_database_url
from sqlalchemy import create_engine


def run_migration():
    """Add can_access_billing_tools column to roles table."""
    db_url = get_database_url('backend')
    engine = create_engine(db_url)

    print("=" * 60)
    print("Migration: Add can_access_billing_tools permission to roles")
    print("=" * 60)
    print()

    with engine.connect() as conn:
        # Check if column already exists
        check_query = text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'roles'
            AND column_name = 'can_access_billing_tools'
        """)
        result = conn.execute(check_query)
        exists = result.fetchone() is not None

        if exists:
            print("  [roles] can_access_billing_tools column already exists - skipping")
        else:
            # Add the column with default FALSE
            alter_query = text("""
                ALTER TABLE roles
                ADD COLUMN can_access_billing_tools BOOLEAN DEFAULT FALSE
            """)
            conn.execute(alter_query)
            conn.commit()
            print("  [roles] Added can_access_billing_tools column")

        # Update admin and scheduler_admin roles to have billing tools access
        update_query = text("""
            UPDATE roles
            SET can_access_billing_tools = TRUE
            WHERE name IN ('admin', 'scheduler_admin')
        """)
        result = conn.execute(update_query)
        conn.commit()
        print(f"  [roles] Updated {result.rowcount} role(s) to have billing tools access (admin, scheduler_admin)")

    print()
    print("Migration complete!")


if __name__ == '__main__':
    run_migration()
