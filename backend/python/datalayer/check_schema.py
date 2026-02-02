"""
Check SQL Server table schema to understand column types and sizes.
Run from PyCharm with local interpreter.
"""

import urllib.parse
from sqlalchemy import create_engine, text


def create_local_sql_engine():
    """Create connection to local SQL Server."""
    server = r"LOUISVER-T14\VSDOTNET"
    database = "sldbclnt"
    driver = "SQL Server"

    driver_encoded = urllib.parse.quote_plus(driver)
    connection_url = (
        f"mssql+pyodbc://@{server}/{database}"
        f"?driver={driver_encoded}&Trusted_Connection=yes&TrustServerCertificate=yes&Connection Timeout=60"
    )

    return create_engine(connection_url)


def get_table_schema(engine, table_name: str):
    """Get column definitions for a table."""
    query = text("""
        SELECT
            c.name AS column_name,
            t.name AS data_type,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable
        FROM sys.columns c
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE c.object_id = OBJECT_ID(:table_name)
        ORDER BY c.column_id
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {'table_name': table_name})
        return list(result)


def main():
    print("Connecting to local SQL Server...")
    engine = create_local_sql_engine()

    for table in ['Tenants', 'Ledgers', 'Charges']:
        print(f"\n{'=' * 70}")
        print(f"Table: {table}")
        print("=" * 70)

        columns = get_table_schema(engine, table)

        # Group by type for summary
        varchar_cols = []
        nvarchar_cols = []
        text_cols = []
        other_cols = []

        for col in columns:
            name, dtype, max_len, prec, scale, nullable = col

            if dtype in ('varchar', 'char'):
                # varchar max_length is actual length
                varchar_cols.append((name, max_len))
            elif dtype in ('nvarchar', 'nchar'):
                # nvarchar max_length is 2x actual length
                actual_len = max_len // 2 if max_len > 0 else -1
                nvarchar_cols.append((name, actual_len))
            elif dtype in ('text', 'ntext'):
                text_cols.append(name)
            else:
                other_cols.append((name, dtype, max_len, prec, scale))

        # Print VARCHAR columns sorted by length
        if varchar_cols:
            print(f"\nVARCHAR columns ({len(varchar_cols)}):")
            for name, length in sorted(varchar_cols, key=lambda x: -x[1]):
                len_str = 'MAX' if length == -1 else str(length)
                print(f"  {name}: VARCHAR({len_str})")

        if nvarchar_cols:
            print(f"\nNVARCHAR columns ({len(nvarchar_cols)}):")
            for name, length in sorted(nvarchar_cols, key=lambda x: -x[1]):
                len_str = 'MAX' if length == -1 else str(length)
                print(f"  {name}: NVARCHAR({len_str})")

        if text_cols:
            print(f"\nTEXT columns ({len(text_cols)}):")
            for name in text_cols:
                print(f"  {name}: TEXT")

        # Print other columns
        if other_cols:
            print(f"\nOther columns ({len(other_cols)}):")
            for name, dtype, max_len, prec, scale in other_cols:
                if dtype in ('decimal', 'numeric'):
                    print(f"  {name}: {dtype.upper()}({prec},{scale})")
                elif dtype in ('int', 'bigint', 'smallint', 'tinyint', 'bit', 'datetime', 'datetime2', 'date'):
                    print(f"  {name}: {dtype.upper()}")
                else:
                    print(f"  {name}: {dtype.upper()}({max_len})")

    engine.dispose()
    print("\nDone!")


if __name__ == "__main__":
    main()
