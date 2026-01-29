"""
Data Processing and Cleaning Module

Handles column detection, data type processing, value cleaning and validation
for SQL Server synchronization operations.
"""

import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine


class DataProcessor:
    """Handles data processing and cleaning operations"""

    def __init__(self):
        """Initialize data processor"""
        self.logger = logging.getLogger('DataProcessor')

    def get_syncable_columns(self, engine: Engine, table_name: str) -> List[str]:
        """
        Get columns that can be synced (excluding binary/problematic columns)

        Args:
            engine: SQLAlchemy engine for database connection
            table_name: Name of the table to analyze

        Returns:
            List[str]: List of column names that can be safely synced
        """
        with engine.connect() as conn:
            # Get ALL columns to analyze
            all_columns_query = text("""
                                     SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                                     FROM INFORMATION_SCHEMA.COLUMNS
                                     WHERE TABLE_NAME = :table_name
                                     ORDER BY ORDINAL_POSITION
                                     """)

            all_result = conn.execute(all_columns_query, {"table_name": table_name})
            all_columns = all_result.fetchall()

            # Filter out problematic column types
            excluded_data_types = {
                'varbinary', 'image', 'timestamp', 'rowversion',
                'sql_variant', 'xml', 'geometry', 'geography', 'hierarchyid'
            }

            # Process each column
            safe_columns = []
            excluded_columns = []

            for col_info in all_columns:
                col_name = col_info[0]
                data_type = col_info[1]
                is_nullable = col_info[2]

                should_exclude, exclude_reason = self._should_exclude_column(
                    col_name, data_type, is_nullable, excluded_data_types
                )

                if should_exclude:
                    excluded_columns.append(f"{col_name} ({exclude_reason})")
                else:
                    safe_columns.append(col_name)

            # Log results
            if safe_columns:
                self.logger.debug(f"Table {table_name}: syncing {len(safe_columns)} columns")
            else:
                self.logger.warning(f"No syncable columns found for {table_name}")

            if excluded_columns:
                self.logger.debug(
                    f"Excluded {len(excluded_columns)} columns from {table_name}: "
                    f"{', '.join(excluded_columns[:5])}"
                    f"{'...' if len(excluded_columns) > 5 else ''}"
                )

            return safe_columns

    def _should_exclude_column(self, col_name: str, data_type: str, is_nullable: str,
                               excluded_types: set) -> tuple[bool, str]:
        """
        Determine if a column should be excluded from sync

        Args:
            col_name: Column name
            data_type: SQL Server data type
            is_nullable: Whether column allows NULL
            excluded_types: Set of data types to exclude

        Returns:
            tuple: (should_exclude: bool, reason: str)
        """
        col_name_lower = col_name.lower()
        data_type_lower = data_type.lower()

        # Exclude problematic data types (except binary for uTS)
        if data_type_lower in excluded_types:
            return True, data_type

        # Exclude timestamp-like column names (case insensitive, but allow uTS)
        timestamp_names = {'timestamp', 'rowversion', 'ts'}
        if col_name_lower in timestamp_names:
            return True, f"timestamp-like name ({data_type})"

        # Exclude columns with timestamp/rowversion in the name
        if ('timestamp' in col_name_lower or 'rowversion' in col_name_lower):
            return True, f"contains timestamp ({data_type})"

        return False, ""

    def get_column_types(self, engine: Engine, table_name: str) -> Dict[str, str]:
        """
        Get column data types from table for proper casting

        Args:
            engine: SQLAlchemy engine for database connection
            table_name: Name of the table to analyze

        Returns:
            Dict[str, str]: Mapping of column names to SQL Server data types
        """
        try:
            with engine.connect() as conn:
                query = text("""
                             SELECT COLUMN_NAME,
                                    DATA_TYPE,
                                    CHARACTER_MAXIMUM_LENGTH,
                                    NUMERIC_PRECISION,
                                    NUMERIC_SCALE
                             FROM INFORMATION_SCHEMA.COLUMNS
                             WHERE TABLE_NAME = :table_name
                             ORDER BY ORDINAL_POSITION
                             """)

                result = conn.execute(query, {"table_name": table_name})
                column_types = {}

                for row in result.fetchall():
                    col_name = row[0]
                    data_type = row[1]
                    max_length = row[2]
                    precision = row[3]
                    scale = row[4]

                    # Build proper SQL Server data type string
                    sql_type = self._build_sql_type(data_type, max_length, precision, scale)
                    column_types[col_name] = sql_type

                return column_types

        except Exception as e:
            self.logger.warning(f"Could not get column types for {table_name}: {e}")
            return {}

    def _build_sql_type(self, data_type: str, max_length: Optional[int],
                        precision: Optional[int], scale: Optional[int]) -> str:
        """
        Build proper SQL Server data type string with parameters

        Args:
            data_type: Base data type name
            max_length: Maximum length for string types
            precision: Numeric precision
            scale: Numeric scale

        Returns:
            str: Complete SQL Server data type string
        """
        data_type_lower = data_type.lower()

        # String types with length
        if data_type_lower in ['varchar', 'nvarchar', 'char', 'nchar']:
            if max_length and max_length > 0:
                return f"{data_type}({max_length})"
            else:
                return f"{data_type}(MAX)"

        # Numeric types with precision/scale
        elif data_type_lower in ['decimal', 'numeric'] and precision:
            if scale is not None:
                return f"{data_type}({precision},{scale})"
            else:
                return f"{data_type}({precision})"

        # All other types
        else:
            return data_type

    def clean_value_for_sql(self, value: Any, col_type: str, col_name: str = None,
                            expected_binary_length: Optional[int] = None) -> Any:
        """
        Clean a value for SQL insertion with proper type handling

        Args:
            value: Raw value from source database
            col_type: Target SQL Server column type
            col_name: Column name for logging
            expected_binary_length: Expected length for binary data

        Returns:
            Any: Cleaned value ready for SQL insertion
        """
        if value is None:
            return None

        col_type_lower = col_type.lower()

        # Handle binary data (especially uTS columns)
        if 'binary' in col_type_lower:
            return self._clean_binary_value(value, col_name, expected_binary_length)

        # Handle datetime values
        elif any(dt in col_type_lower for dt in ['datetime', 'date']):
            return self._clean_datetime_value(value)

        # Handle boolean values
        elif 'bit' in col_type_lower:
            return self._clean_boolean_value(value)

        # Handle numeric values
        elif any(nt in col_type_lower for nt in ['int', 'decimal', 'numeric', 'float', 'real']):
            return self._clean_numeric_value(value)

        # Handle string values
        else:
            return self._clean_string_value(value)

    def _clean_binary_value(self, value: Any, col_name: str,
                            expected_length: Optional[int]) -> Optional[bytes]:
        """Clean binary data values"""
        if isinstance(value, bytes):
            if expected_length and len(value) != expected_length:
                self.logger.warning(
                    f"Binary value length mismatch for {col_name}: "
                    f"{value.hex()}, length: {len(value)}, expected: {expected_length}"
                )
                return None
            return value

        elif isinstance(value, str):
            if value.strip() == '':
                self.logger.warning(f"Empty string for binary column {col_name}: returning NULL")
                return None

            try:
                # Handle hex string with or without '0x' prefix
                hex_value = value[2:] if value.startswith('0x') else value
                binary_value = bytes.fromhex(hex_value.strip())

                if expected_length and len(binary_value) != expected_length:
                    self.logger.warning(
                        f"Converted binary value length mismatch for {col_name}: "
                        f"{binary_value.hex()}, length: {len(binary_value)}, expected: {expected_length}"
                    )
                    return None

                return binary_value

            except (ValueError, TypeError) as e:
                self.logger.warning(f"Failed to convert string to binary for {col_name}: {value}, error: {str(e)}")
                return None

        elif isinstance(value, bytearray):
            binary_value = bytes(value)
            if expected_length and len(binary_value) != expected_length:
                self.logger.warning(
                    f"Converted bytearray length mismatch for {col_name}: "
                    f"{binary_value.hex()}, length: {len(binary_value)}, expected: {expected_length}"
                )
                return None
            return binary_value

        else:
            self.logger.warning(f"Unsupported binary value type for {col_name}: {type(value)} for value: {value}")
            return None

    def _clean_datetime_value(self, value: Any) -> Optional[str]:
        """Clean datetime values"""
        if hasattr(value, 'isoformat'):
            try:
                iso_string = value.isoformat()
                # Remove microseconds and ensure proper format
                if '.' in iso_string:
                    iso_string = iso_string.split('.')[0]
                # SQL Server prefers space instead of T
                return iso_string.replace('T', ' ')
            except:
                return None

        elif isinstance(value, str):
            if value.lower() in ['nat', 'none', '']:
                return None
            return value

        else:
            return str(value) if value else None

    def _clean_boolean_value(self, value: Any) -> Optional[int]:
        """Clean boolean values"""
        if isinstance(value, bool):
            return 1 if value else 0

        elif str(value).lower() in ['true', '1', 'yes']:
            return 1

        elif str(value).lower() in ['false', '0', 'no']:
            return 0

        else:
            return None

    def _clean_numeric_value(self, value: Any) -> Optional[float]:
        """Clean numeric values"""
        try:
            if isinstance(value, (int, float)):
                return value
            return float(value) if value else None
        except:
            return None

    def _clean_string_value(self, value: Any) -> Optional[str]:
        """Clean string values"""
        return str(value) if value is not None else None

    def validate_batch_data(self, batch_data: List[tuple], columns: List[str],
                            column_types: Dict[str, str]) -> tuple[List[Dict[str, Any]], int]:
        """
        Validate and clean a batch of data

        Args:
            batch_data: List of row tuples from database
            columns: List of column names
            column_types: Column type mappings

        Returns:
            tuple: (cleaned_data_list, invalid_count)
        """
        cleaned_params = []
        invalid_count = 0

        for row in batch_data:
            row_params = {}
            is_valid_row = True

            for i, value in enumerate(row):
                col_name = columns[i]
                col_type = column_types.get(col_name, '')

                # Special handling for uTS binary columns
                expected_binary_length = 8 if col_name.lower() == 'uts' else None

                cleaned_value = self.clean_value_for_sql(
                    value, col_type, col_name=col_name,
                    expected_binary_length=expected_binary_length
                )

                # Check for critical validation failures (like invalid uTS)
                if (col_name.lower() == 'uts' and cleaned_value is None and value is not None):
                    is_valid_row = False
                    invalid_count += 1
                    break

                row_params[f'col{i}'] = cleaned_value

            if is_valid_row:
                cleaned_params.append(row_params)

        return cleaned_params, invalid_count

    def get_table_info(self, engine: Engine, table_name: str) -> Dict[str, Any]:
        """
        Get comprehensive table information

        Args:
            engine: SQLAlchemy engine for database connection
            table_name: Name of the table to analyze

        Returns:
            Dict: Table information including columns, types, constraints
        """
        try:
            with engine.connect() as conn:
                # Check if table exists
                table_exists = conn.execute(text("""
                                                 SELECT COUNT(*)
                                                 FROM INFORMATION_SCHEMA.TABLES
                                                 WHERE TABLE_NAME = :table_name
                                                 """), {"table_name": table_name}).fetchone()[0] > 0

                if not table_exists:
                    return {"exists": False, "error": "Table not found"}

                # Get column information
                columns_info = conn.execute(text("""
                                                 SELECT COLUMN_NAME,
                                                        DATA_TYPE,
                                                        IS_NULLABLE,
                                                        CHARACTER_MAXIMUM_LENGTH,
                                                        NUMERIC_PRECISION,
                                                        NUMERIC_SCALE
                                                 FROM INFORMATION_SCHEMA.COLUMNS
                                                 WHERE TABLE_NAME = :table_name
                                                 ORDER BY ORDINAL_POSITION
                                                 """), {"table_name": table_name}).fetchall()

                # Check for SiteID column
                has_siteid = any(col[0] == 'SiteID' for col in columns_info)

                # Check for dUpdated column
                has_dupdated = any(col[0] == 'dUpdated' for col in columns_info)

                # Get row count
                row_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()[0]

                # Get SiteID information if available
                site_info = {}
                if has_siteid and row_count > 0:
                    site_data = conn.execute(text(f"""
                        SELECT COUNT(*) as RowCount, 
                               COUNT(DISTINCT SiteID) as UniqueSites,
                               MIN(SiteID) as MinSiteID,
                               MAX(SiteID) as MaxSiteID
                        FROM {table_name} WHERE SiteID IS NOT NULL
                    """)).fetchone()

                    site_info = {
                        "row_count": site_data[0],
                        "unique_sites": site_data[1],
                        "min_site_id": site_data[2],
                        "max_site_id": site_data[3]
                    }

                return {
                    "exists": True,
                    "row_count": row_count,
                    "column_count": len(columns_info),
                    "has_siteid": has_siteid,
                    "has_dupdated": has_dupdated,
                    "site_info": site_info,
                    "syncable_columns": len(self.get_syncable_columns(engine, table_name))
                }

        except Exception as e:
            return {"exists": False, "error": str(e)}


# Factory function for creating DataProcessor instances
def create_data_processor() -> DataProcessor:
    """
    Factory function to create a DataProcessor instance

    Returns:
        DataProcessor: Configured data processor
    """
    return DataProcessor()


# For testing the module independently
if __name__ == "__main__":
    print("Data Handler Module Test")
    print("=" * 40)

    # Test data cleaning functions
    processor = DataProcessor()

    # Test datetime cleaning
    from datetime import datetime

    test_datetime = datetime.now()
    cleaned_dt = processor._clean_datetime_value(test_datetime)
    print(f"Datetime cleaning: {test_datetime} -> {cleaned_dt}")

    # Test binary cleaning
    test_binary = "0x1234567890ABCDEF"
    cleaned_binary = processor._clean_binary_value(test_binary, "test_col", 8)
    print(f"Binary cleaning: {test_binary} -> {cleaned_binary.hex() if cleaned_binary else None}")

    # Test boolean cleaning
    test_bool = "true"
    cleaned_bool = processor._clean_boolean_value(test_bool)
    print(f"Boolean cleaning: {test_bool} -> {cleaned_bool}")

    # Test numeric cleaning
    test_num = "123.45"
    cleaned_num = processor._clean_numeric_value(test_num)
    print(f"Numeric cleaning: {test_num} -> {cleaned_num}")

    print("\n✅ Data Handler module loaded successfully!")