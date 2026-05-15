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
                         (e.g., 'can_access_sync', 'can_manage_users')

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


def sync_access_required(f):
    """Decorator to require sync orchestrator access permission."""
    return require_permission('can_access_sync')(f)


def billing_tools_access_required(f):
    """Decorator to require billing tools access permission."""
    return require_permission('can_access_billing_tools')(f)


def inventory_tools_access_required(f):
    """Decorator to require inventory tools access permission."""
    return require_permission('can_access_inventory_tools')(f)


def discount_tools_access_required(f):
    """Decorator to require discount plan tools access permission."""
    return require_permission('can_access_discount_tools')(f)


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


def statistics_access_required(f):
    """Decorator to require statistics access permission."""
    return require_permission('can_access_statistics')(f)


def smart_lock_access_required(f):
    """Decorator to require smart lock tools access permission.

    Grants entry to the assignment + refresh workflow. For inventory
    management (bridges/keypads/padlocks/site config) use
    smart_lock_admin_required instead.
    """
    return require_permission('can_access_smart_lock')(f)


def smart_lock_admin_required(f):
    """Decorator to require smart lock ADMIN permission (inventory + config)."""
    return require_permission('can_admin_smart_lock')(f)


def revenue_tools_access_required(f):
    """Decorator to require revenue management tools access permission."""
    return require_permission('can_access_revenue_tools')(f)


def ecri_exclusion_required(f):
    """Decorator to require ECRI exclusion-request permission (ops site-review)."""
    return require_permission('can_request_ecri_exclusion')(f)


def ecri_objection_required(f):
    """Decorator to require ECRI objection-create permission."""
    return require_permission('can_create_ecri_objection')(f)


def ecri_objection_approve_required(f):
    """Decorator to require ECRI objection-approve permission."""
    return require_permission('can_approve_ecri_objection')(f)


def ecri_finalize_required(f):
    """Decorator to require ECRI batch-finalize permission (Revenue)."""
    return require_permission('can_finalize_ecri_batch')(f)


def ecri_execute_required(f):
    """Decorator to require ECRI batch-execute permission (Revenue)."""
    return require_permission('can_execute_ecri_batch')(f)


def ecri_reasons_manage_required(f):
    """Decorator to require ECRI reasons admin permission."""
    return require_permission('can_manage_ecri_reasons')(f)


def pricing_anomalies_tools_access_required(f):
    """Decorator to require pricing anomalies tools access permission."""
    return require_permission('can_access_pricing_anomalies_tools')(f)


def risk_admin_access_required(f):
    """Decorator to require risk-factor administration permission.

    Aliased to revenue tools access — risk overrides drive pricing decisions
    and live alongside revenue-management features.
    """
    return require_permission('can_access_revenue_tools')(f)
