"""
Authentication and authorization decorators.
"""

from functools import wraps
from flask import abort, redirect, url_for, request
from flask_login import current_user


def login_required(f):
    """
    Decorator to require user login.
    Redirects to login page if not authenticated.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('auth.login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function


def require_roles(roles):
    """
    Decorator factory to require specific roles.
    Returns 403 if user doesn't have the required role.

    Args:
        roles: String or list of allowed roles

    Usage:
        @require_roles(['admin', 'editor'])
        def admin_page():
            ...
    """
    if isinstance(roles, str):
        roles = [roles]

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for('auth.login', next=request.url))

            if current_user.role not in roles:
                abort(403)

            return f(*args, **kwargs)
        return decorated_function
    return decorator


def admin_required(f):
    """Decorator to require admin role."""
    return require_roles(['admin'])(f)


def scheduler_access_required(f):
    """Decorator to require scheduler access (admin or scheduler_admin)."""
    return require_roles(['admin', 'scheduler_admin'])(f)


def editor_required(f):
    """Decorator to require editor or admin role."""
    return require_roles(['admin', 'editor'])(f)
