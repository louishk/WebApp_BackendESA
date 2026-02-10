"""
Migration: Add data_source column to tenants, ledgers, and charges tables.

This adds tracking for where the data originated from ('api' or 'local_sql').
Run this once before using the unified sync script.

Usage:
    python migrate_add_data_source.py
"""

from sqlalchemy import text
from common import DataLayerConfig, create_engine_from_config


def run_migration():
    """Add data_source column to tenants, ledgers, and charges tables."""
    config = DataLayerConfig.from_env()
    db_config = config.databases.get('postgresql')

    if not db_config:
        print("ERROR: PostgreSQL configuration not found in .env")
        return

    engine = create_engine_from_config(db_config)

    print("=" * 60)
    print("Migration: Add data_source column")
    print("=" * 60)
    print(f"Database: {db_config.database}")
    print()

    tables = ['cc_tenants', 'cc_ledgers', 'cc_charges']

    with engine.connect() as conn:
        for table in tables:
            # Check if column already exists
            check_query = text(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name
                AND column_name = 'data_source'
            """)
            result = conn.execute(check_query, {"table_name": table})
            exists = result.fetchone() is not None

            if exists:
                print(f"  [{table}] data_source column already exists - skipping")
            else:
                # Add the column
                alter_query = text(f"""
                    ALTER TABLE {table}
                    ADD COLUMN data_source VARCHAR(20) DEFAULT NULL
                """)
                conn.execute(alter_query)
                print(f"  [{table}] Added data_source column")

        conn.commit()

    print()
    print("Migration completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    run_migration()
