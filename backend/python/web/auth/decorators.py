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


def require_permission(permission_check):
    """
    Decorator factory to require specific permission.
    Returns 403 if user doesn't have the required permission.

    Args:
        permission_check: String name of permission method on User model
                         (e.g., 'can_access_scheduler', 'can_manage_users')

    Usage:
        @require_permission('can_manage_users')
        def admin_page():
            ...
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for('auth.login', next=request.url))

            # Get the permission method from the user object
            check_method = getattr(current_user, permission_check, None)
            if check_method is None or not callable(check_method):
                abort(500)  # Invalid permission check configured

            if not check_method():
                abort(403)

            return f(*args, **kwargs)
        return decorated_function
    return decorator


def require_roles(role_names):
    """
    Decorator factory to require specific role names.
    Returns 403 if user's role is not in the allowed list.

    Args:
        role_names: String or list of allowed role names

    Usage:
        @require_roles(['admin', 'editor'])
        def admin_page():
            ...
    """
    if isinstance(role_names, str):
        role_names = [role_names]

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for('auth.login', next=request.url))

            if not current_user.has_role(role_names):
                abort(403)

            return f(*args, **kwargs)
        return decorated_function
    return decorator


def admin_required(f):
    """Decorator to require user management permission."""
    return require_permission('can_manage_users')(f)


def scheduler_access_required(f):
    """Decorator to require scheduler access permission."""
    return require_permission('can_access_scheduler')(f)


def billing_tools_access_required(f):
    """Decorator to require billing tools access permission."""
    return require_permission('can_access_billing_tools')(f)


def editor_required(f):
    """Decorator to require page management permission."""
    return require_permission('can_manage_pages')(f)


def roles_required(f):
    """Decorator to require role management permission."""
    return require_permission('can_manage_roles')(f)


def config_required(f):
    """Decorator to require config management permission."""
    return require_permission('can_manage_configs')(f)


def ecri_access_required(f):
    """Decorator to require ECRI view access permission."""
    return require_permission('can_access_ecri')(f)


def ecri_manage_required(f):
    """Decorator to require ECRI management permission."""
    return require_permission('can_manage_ecri')(f)
