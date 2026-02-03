"""
Input validation utilities.

Provides validation functions for passwords, usernames, and other user input.
"""

import re


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
