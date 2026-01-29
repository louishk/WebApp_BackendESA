"""
Inspect Local SQL Server Schema

Dumps the actual column structure of Tenants, Ledgers, and Charges tables
from the local SQL Server database (sldbclnt).

Run this on Windows where SQL Server is accessible:
    python inspect_local_sql_schema.py
"""

import urllib.parse
from sqlalchemy import create_engine, text, inspect


def create_local_sql_engine(
    server: str = r"LOUISVER-T14\VSDOTNET",
    database: str = "sldbclnt",
    driver: str = "SQL Server"
):
    """Create SQLAlchemy engine for local SQL Server."""
    driver_encoded = urllib.parse.quote_plus(driver)
    connection_url = (
        f"mssql+pyodbc://@{server}/{database}"
        f"?driver={driver_encoded}"
        f"&Trusted_Connection=yes"
        f"&TrustServerCertificate=yes"
    )
    return create_engine(connection_url)


def get_table_columns(engine, table_name: str):
    """Get column info for a table using INFORMATION_SCHEMA."""
    query = text("""
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = :table_name
        ORDER BY ORDINAL_POSITION
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"table_name": table_name})
        return list(result.fetchall())


def print_table_schema(engine, table_name: str):
    """Print schema for a table."""
    columns = get_table_columns(engine, table_name)

    if not columns:
        print(f"  Table '{table_name}' not found or has no columns")
        return

    print(f"\n{'='*70}")
    print(f"TABLE: {table_name} ({len(columns)} columns)")
    print(f"{'='*70}")
    print(f"{'Column Name':<30} {'Type':<20} {'Nullable':<10}")
    print(f"{'-'*30} {'-'*20} {'-'*10}")

    for col in columns:
        col_name = col[0]
        data_type = col[1]
        max_len = col[2]
        nullable = col[3]

        # Format type with length if applicable
        if max_len and data_type in ('varchar', 'nvarchar', 'char', 'nchar'):
            type_str = f"{data_type}({max_len})"
        else:
            type_str = data_type

        print(f"{col_name:<30} {type_str:<20} {nullable:<10}")

    return columns


def main():
    print("Connecting to local SQL Server...")
    try:
        engine = create_local_sql_engine()
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Connected successfully!\n")
    except Exception as e:
        print(f"ERROR: Failed to connect: {e}")
        return

    # Tables to inspect
    tables = ['Tenants', 'Ledgers', 'Charges', 'ChargeDesc', 'Units']

    all_columns = {}
    for table in tables:
        cols = print_table_schema(engine, table)
        if cols:
            all_columns[table] = [c[0] for c in cols]

    # Summary
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    for table, cols in all_columns.items():
        print(f"{table}: {len(cols)} columns")

    # Export to file for reference
    output_file = "local_sql_schema.txt"
    with open(output_file, 'w') as f:
        for table in tables:
            cols = get_table_columns(engine, table)
            if cols:
                f.write(f"\n{'='*70}\n")
                f.write(f"TABLE: {table} ({len(cols)} columns)\n")
                f.write(f"{'='*70}\n")
                for col in cols:
                    f.write(f"{col[0]:<30} {col[1]:<20} {col[3]:<10}\n")

    print(f"\nSchema exported to: {output_file}")

    engine.dispose()


if __name__ == "__main__":
    main()
