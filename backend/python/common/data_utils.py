"""
Data conversion and transformation utilities.

Provides type conversion functions for API response data and
record deduplication for batch database operations.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, List, Dict, Optional

import dateutil.parser


@dataclass
class AdaptiveBatchParams:
    """Batch parameters auto-tuned based on dataset size."""
    api_batch_size: int
    sql_chunk_size: int
    push_threshold: int    # records to buffer before DB write
    client_timeout: int    # API request timeout in seconds
    is_large: bool         # whether large-dataset mode activated


LARGE_DATASET_THRESHOLD = 50_000


def adaptive_batch_params(
    record_count,
    base_api_batch: int = 200,
    base_sql_chunk: int = 500,
    base_push_threshold: int = 5000,
    base_timeout: int = 120,
    threshold: int = LARGE_DATASET_THRESHOLD,
) -> AdaptiveBatchParams:
    """
    Return batch parameters scaled to dataset size.

    Below threshold: returns base values unchanged.
    Above threshold: scales up for throughput.
    """
    if not isinstance(record_count, int):
        # Unknown count — use large-mode push_threshold to avoid OOM,
        # but keep base API/SQL sizes since we can't confirm volume
        return AdaptiveBatchParams(
            api_batch_size=base_api_batch,
            sql_chunk_size=base_sql_chunk,
            push_threshold=2000,
            client_timeout=base_timeout,
            is_large=False,
        )
    if record_count <= threshold:
        return AdaptiveBatchParams(
            api_batch_size=base_api_batch,
            sql_chunk_size=base_sql_chunk,
            push_threshold=base_push_threshold,
            client_timeout=base_timeout,
            is_large=False,
        )
    return AdaptiveBatchParams(
        api_batch_size=max(base_api_batch, 1000),
        sql_chunk_size=max(base_sql_chunk, 2000),
        push_threshold=2000,
        client_timeout=max(base_timeout, 180),
        is_large=True,
    )


def convert_to_bool(value: Any) -> bool:
    """
    Convert string boolean value to Python bool.

    Handles various input types including None, empty strings,
    string "true"/"false", and actual bool values.

    Args:
        value: Value to convert (string, bool, or other)

    Returns:
        bool: Converted boolean value (defaults to False for None/empty)
    """
    if value is None or value == "":
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return bool(value)


def convert_to_int(value: Any) -> Optional[int]:
    """
    Convert value to integer, handling None and empty strings.

    Safely converts numeric strings, floats, and integers.
    Returns None for unconvertible values.

    Args:
        value: Value to convert

    Returns:
        int or None: Converted integer or None if conversion fails
    """
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def convert_to_decimal(value: Any) -> Optional[Decimal]:
    """
    Convert value to Decimal, handling None and empty strings.

    Uses string conversion to preserve precision for monetary
    and measurement values.

    Args:
        value: Value to convert

    Returns:
        Decimal or None: Converted Decimal or None if conversion fails
    """
    if value is None or value == "":
        return None
    try:
        result = Decimal(str(value))
        if not result.is_finite():
            return None
        return result
    except (ValueError, TypeError, InvalidOperation):
        return None


def convert_to_datetime(value: Any) -> Optional[datetime]:
    """
    Convert value to Python datetime.

    Handles multiple input types:
    - None/empty string: returns None
    - datetime object: returns as-is (from SQL Server native types)
    - string: parses using dateutil.parser

    Args:
        value: Datetime string, datetime object, or None

    Returns:
        datetime or None: Parsed datetime or None if parsing fails
    """
    if value is None or value == "":
        return None
    # Already a datetime object (from SQL Server)
    if isinstance(value, datetime):
        return value
    try:
        return dateutil.parser.parse(value)
    except (ValueError, TypeError):
        return None


def deduplicate_records(
    data: List[Dict[str, Any]],
    key_columns: List[str]
) -> List[Dict[str, Any]]:
    """
    Deduplicate records by composite key, keeping last occurrence.

    Useful for batch upsert operations where duplicate records
    in the same batch would cause ON CONFLICT errors.

    Args:
        data: List of record dictionaries
        key_columns: List of column names that form the unique key

    Returns:
        Deduplicated list of records (preserves order, keeps last duplicate)

    Example:
        >>> records = [
        ...     {'id': 1, 'site': 'A', 'value': 100},
        ...     {'id': 1, 'site': 'A', 'value': 200},  # duplicate
        ...     {'id': 2, 'site': 'B', 'value': 300},
        ... ]
        >>> deduplicate_records(records, ['id', 'site'])
        [{'id': 1, 'site': 'A', 'value': 200}, {'id': 2, 'site': 'B', 'value': 300}]
    """
    seen = {}
    for record in data:
        key = tuple(record.get(col) for col in key_columns)
        seen[key] = record  # Later records overwrite earlier ones
    return list(seen.values())
