"""
Migration: Create inventory checker tables.

Creates two tables in the backend database:
- inventory_type_mappings: Maps sTypeName to SOP unit type codes
- inventory_unit_overrides: Per-unit overrides for auto-calculated fields

Usage:
    python scripts/migrate_add_inventory_tables.py
"""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from common.config_loader import get_database_url
from sqlalchemy import create_engine


def run_migration():
    """Create inventory checker tables."""
    db_url = get_database_url('backend')
    engine = create_engine(db_url)

    print("=" * 60)
    print("Migration: Create inventory checker tables")
    print("=" * 60)
    print()

    sql_path = Path(__file__).parent.parent / 'migrations' / '003_inventory_checker_tables.sql'

    with open(sql_path) as f:
        sql = f.read()

    with engine.connect() as conn:
        # Check if tables already exist
        for table_name in ['inventory_type_mappings', 'inventory_unit_overrides']:
            check = text("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :tbl
            """)
            result = conn.execute(check, {'tbl': table_name})
            if result.fetchone():
                print(f"  [{table_name}] already exists - will skip via IF NOT EXISTS")
            else:
                print(f"  [{table_name}] will be created")

        conn.execute(text(sql))
        conn.commit()

    print()
    print("Migration complete!")


if __name__ == '__main__':
    run_migration()
