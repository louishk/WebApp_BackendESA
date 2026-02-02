"""
Date utilities for data pipeline operations.

Provides functions for date range generation and extract_date calculation
commonly needed in data extraction pipelines.
"""

from datetime import date, timedelta
from typing import List, Tuple


def get_first_day_of_month(year: int, month: int) -> date:
    """
    Get the first day of a given month.

    Args:
        year: Target year
        month: Target month (1-12)

    Returns:
        date: First day of the specified month
    """
    return date(year, month, 1)


def get_last_day_of_month(year: int, month: int) -> date:
    """
    Get the last day of a given month.

    Args:
        year: Target year
        month: Target month (1-12)

    Returns:
        date: Last day of the specified month
    """
    if month == 12:
        return date(year, 12, 31)
    else:
        return date(year, month + 1, 1) - timedelta(days=1)


def is_current_month(target_date: date) -> bool:
    """
    Check if the given date is in the current month.

    Args:
        target_date: Date to check

    Returns:
        bool: True if the date is in the current month, False otherwise
    """
    today = date.today()
    return target_date.year == today.year and target_date.month == today.month


def get_extract_date(year: int, month: int) -> Tuple[date, str]:
    """
    Determine the extract_date based on whether the month is closed or current.

    For data extraction pipelines:
    - Closed months: use last day of month (final snapshot)
    - Current month: use today's date (live snapshot)

    Args:
        year: Target year
        month: Target month (1-12)

    Returns:
        Tuple of (extract_date, status_label)
        - Closed month: (last day of month, "closed")
        - Current month: (today's date, "current")
        - Future month: (last day of month, "future")
    """
    today = date.today()
    last_day = get_last_day_of_month(year, month)

    if (year, month) < (today.year, today.month):
        # Past/closed month - use last day of month
        return last_day, "closed"
    elif (year, month) == (today.year, today.month):
        # Current month - use today's date
        return today, "current"
    else:
        # Future month (edge case) - use last day
        return last_day, "future"


def get_date_range_manual(start_str: str, end_str: str) -> List[Tuple[int, int]]:
    """
    Parse YYYY-MM strings and return list of (year, month) tuples.

    Args:
        start_str: Start month in YYYY-MM format (e.g., "2025-01")
        end_str: End month in YYYY-MM format (e.g., "2025-12")

    Returns:
        List of (year, month) tuples covering the range inclusive

    Example:
        >>> get_date_range_manual("2025-01", "2025-03")
        [(2025, 1), (2025, 2), (2025, 3)]
    """
    start_parts = start_str.split('-')
    end_parts = end_str.split('-')

    start_year, start_month = int(start_parts[0]), int(start_parts[1])
    end_year, end_month = int(end_parts[0]), int(end_parts[1])

    months = []
    current_year, current_month = start_year, start_month

    while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
        months.append((current_year, current_month))

        if current_month == 12:
            current_month = 1
            current_year += 1
        else:
            current_month += 1

    return months


def get_date_range_auto() -> List[Tuple[int, int]]:
    """
    Get date range for automatic mode: previous month + current month.

    Useful for scheduled pipelines that need to:
    - Re-extract previous month (in case of late updates)
    - Extract current month (live snapshot)

    Returns:
        List of (year, month) tuples for [previous_month, current_month]

    Example (if today is 2026-01-08):
        >>> get_date_range_auto()
        [(2025, 12), (2026, 1)]
    """
    today = date.today()

    # Previous month
    if today.month == 1:
        prev_year, prev_month = today.year - 1, 12
    else:
        prev_year, prev_month = today.year, today.month - 1

    # Current month
    curr_year, curr_month = today.year, today.month

    return [(prev_year, prev_month), (curr_year, curr_month)]


def get_date_range_days_back(days_back: int = 60, days_forward: int = 365) -> Tuple[date, date]:
    """
    Get date range for cumulative data auto mode.

    Used for pipelines that need to:
    - Delete and re-extract recent data (e.g., last 60 days)
    - Include future bookings (e.g., next 365 days)

    Args:
        days_back: Number of days to look back (default: 60)
        days_forward: Number of days to look forward for future bookings (default: 365)

    Returns:
        Tuple of (start_date, end_date)

    Example (if today is 2026-01-08):
        >>> get_date_range_days_back(60, 365)
        (date(2025, 11, 9), date(2027, 1, 8))
    """
    today = date.today()
    start_date = today - timedelta(days=days_back)
    end_date = today + timedelta(days=days_forward)
    return start_date, end_date


def parse_date_string(date_str: str) -> date:
    """
    Parse a date string in YYYY-MM-DD format.

    Args:
        date_str: Date string in YYYY-MM-DD format (e.g., "2025-01-15")

    Returns:
        date object

    Example:
        >>> parse_date_string("2025-01-15")
        date(2025, 1, 15)
    """
    parts = date_str.split('-')
    return date(int(parts[0]), int(parts[1]), int(parts[2]))
