"""
Input validation utilities.

Provides validation functions for passwords, usernames, and other user input.
Also provides shared helpers for parsing and bounding API query parameters.
"""

import logging
import re
from datetime import datetime

logger = logging.getLogger(__name__)

# Shared bounds for API route validation
MAX_SITE_IDS = 50
MAX_PAGE_SIZE = 500
DEFAULT_PAGE_SIZE = 50
MAX_ARRAY_SIZE = 1000


def parse_site_ids(param: str) -> list:
    """Parse a comma-separated site_ids string into a list of ints.

    Raises ValueError with a user-facing message if:
    - any token is not a valid integer
    - the list exceeds MAX_SITE_IDS entries
    - the result is empty
    """
    tokens = [t.strip() for t in param.split(',') if t.strip()]
    if not tokens:
        raise ValueError('At least one site_id is required')
    if len(tokens) > MAX_SITE_IDS:
        raise ValueError(f'Too many site IDs: maximum is {MAX_SITE_IDS}')
    try:
        return [int(t) for t in tokens]
    except ValueError:
        raise ValueError('site_ids must be comma-separated integers')


def parse_pagination(args) -> tuple:
    """Extract and bound limit/offset from request.args.

    Returns (limit, offset). Clamps limit to MAX_PAGE_SIZE; defaults to
    DEFAULT_PAGE_SIZE when not supplied. Raises ValueError on non-integer input.
    """
    try:
        limit = int(args.get('limit', DEFAULT_PAGE_SIZE))
    except (ValueError, TypeError):
        raise ValueError('limit must be an integer')
    try:
        offset = int(args.get('offset', 0))
    except (ValueError, TypeError):
        raise ValueError('offset must be an integer')

    if limit < 1:
        limit = DEFAULT_PAGE_SIZE
    elif limit > MAX_PAGE_SIZE:
        limit = MAX_PAGE_SIZE

    if offset < 0:
        offset = 0

    return limit, offset


def parse_date_param(param: str, name: str) -> datetime:
    """Parse an ISO 8601 date/datetime string.

    Raises ValueError with the parameter name if the string is not a valid
    ISO format date (e.g. '2024-01-15' or '2024-01-15T00:00:00').
    """
    try:
        return datetime.fromisoformat(param)
    except (ValueError, TypeError):
        raise ValueError(f"'{name}' must be a valid ISO date (e.g. 2024-01-15)")


def validate_array_size(data: list, name: str, max_size: int = MAX_ARRAY_SIZE):
    """Raise ValueError if the array exceeds max_size entries."""
    if len(data) > max_size:
        raise ValueError(f"'{name}' array exceeds maximum allowed size of {max_size}")


def validate_password(password, min_length=8):
    """
    Validate password meets security requirements.

    Requirements:
    - Minimum length (default 8 characters)
    - At least one uppercase letter
    - At least one lowercase letter
    - At least one digit
    - At least one special character

    Args:
        password: Password string to validate
        min_length: Minimum required length (default 8)

    Returns:
        tuple: (is_valid: bool, message: str)
    """
    if not password:
        return False, "Password is required"

    if len(password) < min_length:
        return False, f"Password must be at least {min_length} characters"

    if not re.search(r'[A-Z]', password):
        return False, "Password must contain at least one uppercase letter"

    if not re.search(r'[a-z]', password):
        return False, "Password must contain at least one lowercase letter"

    if not re.search(r'\d', password):
        return False, "Password must contain at least one digit"

    if not re.search(r'[!@#$%^&*(),.?":{}|<>\-_=+\[\]\\;\'`~]', password):
        return False, "Password must contain at least one special character"

    return True, "Password is valid"


def validate_username(username):
    """
    Validate username format.

    Requirements:
    - 3-50 characters
    - Alphanumeric, underscores, hyphens only
    - Must start with a letter

    Args:
        username: Username string to validate

    Returns:
        tuple: (is_valid: bool, message: str)
    """
    if not username:
        return False, "Username is required"

    if len(username) < 3:
        return False, "Username must be at least 3 characters"

    if len(username) > 50:
        return False, "Username must be at most 50 characters"

    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_-]*$', username):
        return False, "Username must start with a letter and contain only letters, numbers, underscores, and hyphens"

    return True, "Username is valid"
