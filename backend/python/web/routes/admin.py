"""
Admin routes - user, role, and page management.
"""

import re
import logging
import bcrypt
from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify, current_app
from flask_login import login_required, current_user
from web.utils.audit import audit_log, AuditEvent
from web.utils.validators import validate_password

logger = logging.getLogger(__name__)

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')


def get_session():
    """Get database session from app context."""
    return current_app.get_db_session()


def _permission_required(check_fn, deny_message):
    """Factory for permission-checking decorators. Redirects with flash on denial."""
    from functools import wraps
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for('auth.login'))
            if not check_fn(current_user):
                flash(deny_message, 'error')
                return redirect(url_for('main.dashboard'))
            return f(*args, **kwargs)
        return decorated
    return decorator


admin_required = _permission_required(lambda u: u.can_manage_users(), 'Admin access required.')
roles_required = _permission_required(lambda u: u.can_manage_roles(), 'Role management access required.')
editor_required = _permission_required(lambda u: u.can_manage_pages(), 'Editor access required.')
config_required = _permission_required(lambda u: u.can_manage_configs(), 'Config management access required.')


# =============================================================================
# Role Management
# =============================================================================

@admin_bp.route('/roles')
@login_required
@roles_required
def list_roles():
    """List all roles."""
    from web.models.role import Role
    from web.models.user import User

    db_session = get_session()
    try:
        roles = db_session.query(Role).order_by(Role.name).all()
        # Get user count for each role via the join table
        from web.models.user import user_roles
        from sqlalchemy import func
        role_user_counts = {}
        counts = db_session.query(user_roles.c.role_id, func.count(user_roles.c.user_id)).group_by(user_roles.c.role_id).all()
        for role_id, count in counts:
            role_user_counts[role_id] = count
        # Ensure all roles have an entry
        for role in roles:
            role_user_counts.setdefault(role.id, 0)
        return render_template('admin/roles/list.html', roles=roles, role_user_counts=role_user_counts)
    finally:
        db_session.close()


@admin_bp.route('/roles/create', methods=['GET', 'POST'])
@login_required
@roles_required
def create_role():
    """Create a new role."""
    from web.models.role import Role

    if request.method == 'POST':
        name = request.form.get('name', '').strip().lower()
        description = request.form.get('description', '').strip()

        if not name:
            flash('Role name is required.', 'error')
            return render_template('admin/roles/edit.html', role=None)

        # Validate name format (alphanumeric and underscores only)
        if not name.replace('_', '').isalnum():
            flash('Role name can only contain letters, numbers, and underscores.', 'error')
            return render_template('admin/roles/edit.html', role=None)

        db_session = get_session()
        try:
            # Check for duplicate name
            if db_session.query(Role).filter_by(name=name).first():
                flash('Role name already exists.', 'error')
                return render_template('admin/roles/edit.html', role=None)

            role = Role(
                name=name,
                description=description,
                can_access_scheduler=request.form.get('can_access_scheduler') == 'on',
                can_access_billing_tools=request.form.get('can_access_billing_tools') == 'on',
                can_access_inventory_tools=request.form.get('can_access_inventory_tools') == 'on',
                can_access_discount_tools=request.form.get('can_access_discount_tools') == 'on',
                can_manage_users=request.form.get('can_manage_users') == 'on',
                can_manage_pages=request.form.get('can_manage_pages') == 'on',
                can_manage_roles=request.form.get('can_manage_roles') == 'on',
                can_manage_configs=request.form.get('can_manage_configs') == 'on',
                can_access_ecri=request.form.get('can_access_ecri') == 'on',
                can_manage_ecri=request.form.get('can_manage_ecri') == 'on',
                can_request_ecri_exclusion=request.form.get('can_request_ecri_exclusion') == 'on',
                can_create_ecri_objection=request.form.get('can_create_ecri_objection') == 'on',
                can_approve_ecri_objection=request.form.get('can_approve_ecri_objection') == 'on',
                can_finalize_ecri_batch=request.form.get('can_finalize_ecri_batch') == 'on',
                can_execute_ecri_batch=request.form.get('can_execute_ecri_batch') == 'on',
                can_manage_ecri_reasons=request.form.get('can_manage_ecri_reasons') == 'on',
                can_access_statistics=request.form.get('can_access_statistics') == 'on',
                can_access_smart_lock=request.form.get('can_access_smart_lock') == 'on',
                can_access_revenue_tools=request.form.get('can_access_revenue_tools') == 'on',
                is_system=False
            )
            db_session.add(role)
            db_session.commit()

            audit_log(AuditEvent.ROLE_CREATED, f"Created role '{name}' with permissions: {role.get_permissions_list()}")
            flash(f'Role "{name}" created successfully.', 'success')
            return redirect(url_for('admin.list_roles'))
        except Exception as e:
            db_session.rollback()
            current_app.logger.error(f"Error creating role: {e}")
            flash('An error occurred.', 'error')
        finally:
            db_session.close()

    return render_template('admin/roles/edit.html', role=None)


@admin_bp.route('/roles/<int:role_id>/edit', methods=['GET', 'POST'])
@login_required
@roles_required
def edit_role(role_id):
    """Edit an existing role."""
    from web.models.role import Role

    db_session = get_session()
    try:
        role = db_session.query(Role).get(role_id)
        if not role:
            flash('Role not found.', 'error')
            return redirect(url_for('admin.list_roles'))

        if request.method == 'POST':
            role.description = request.form.get('description', '').strip()
            role.can_access_scheduler = request.form.get('can_access_scheduler') == 'on'
            role.can_access_billing_tools = request.form.get('can_access_billing_tools') == 'on'
            role.can_access_inventory_tools = request.form.get('can_access_inventory_tools') == 'on'
            role.can_access_discount_tools = request.form.get('can_access_discount_tools') == 'on'
            role.can_manage_users = request.form.get('can_manage_users') == 'on'
            role.can_manage_pages = request.form.get('can_manage_pages') == 'on'
            role.can_manage_roles = request.form.get('can_manage_roles') == 'on'
            role.can_manage_configs = request.form.get('can_manage_configs') == 'on'
            role.can_access_ecri = request.form.get('can_access_ecri') == 'on'
            role.can_manage_ecri = request.form.get('can_manage_ecri') == 'on'
            role.can_access_statistics = request.form.get('can_access_statistics') == 'on'
            role.can_access_smart_lock = request.form.get('can_access_smart_lock') == 'on'
            role.can_access_revenue_tools = request.form.get('can_access_revenue_tools') == 'on'

            db_session.commit()
            audit_log(AuditEvent.ROLE_UPDATED, f"Updated role '{role.name}' (id={role_id}), permissions: {role.get_permissions_list()}")
            flash('Role updated successfully.', 'success')
            return redirect(url_for('admin.list_roles'))

        return render_template('admin/roles/edit.html', role=role)
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Error editing role: {e}")
        flash('An error occurred.', 'error')
        return redirect(url_for('admin.list_roles'))
    finally:
        db_session.close()


@admin_bp.route('/roles/<int:role_id>/delete', methods=['POST'])
@login_required
@roles_required
def delete_role(role_id):
    """Delete a role."""
    from web.models.role import Role
    from web.models.user import User

    db_session = get_session()
    try:
        role = db_session.query(Role).get(role_id)
        if not role:
            flash('Role not found.', 'error')
            return redirect(url_for('admin.list_roles'))

        if role.is_system:
            flash('Cannot delete system roles.', 'error')
            return redirect(url_for('admin.list_roles'))

        # Check if any users have this role via the join table
        from web.models.user import user_roles
        user_count = db_session.query(user_roles).filter(user_roles.c.role_id == role_id).count()
        if user_count > 0:
            flash(f'Cannot delete role. {user_count} user(s) are assigned to this role.', 'error')
            return redirect(url_for('admin.list_roles'))

        role_name = role.name
        db_session.delete(role)
        db_session.commit()
        audit_log(AuditEvent.ROLE_DELETED, f"Deleted role '{role_name}' (id={role_id})")
        flash(f'Role "{role_name}" deleted.', 'success')
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Error deleting role: {e}")
        flash('An error occurred.', 'error')
    finally:
        db_session.close()

    return redirect(url_for('admin.list_roles'))


# =============================================================================
# User Management (Admin only)
# =============================================================================

@admin_bp.route('/users/ecri-limits')
@login_required
@admin_required
def ecri_user_limits():
    """Per-user ECRI approval limits + site restrictions (lives under User Management)."""
    return render_template('admin/ecri_user_limits.html')


@admin_bp.route('/api/users')
@login_required
@admin_required
def api_list_users():
    """JSON list of users with roles + ECRI settings (used by ECRI admin pages)."""
    from web.models.user import User
    db = get_session()
    try:
        users = db.query(User).order_by(User.username).all()
        return jsonify({
            'users': [
                {
                    'id': u.id,
                    'username': u.username,
                    'email': u.email,
                    'department': getattr(u, 'department', None),
                    'roles': [{'id': r.id, 'name': r.name} for r in u.roles],
                    'ecri_max_pct_reduction': float(u.ecri_max_pct_reduction) if getattr(u, 'ecri_max_pct_reduction', None) is not None else 0,
                    'ecri_max_abs_reduction': float(u.ecri_max_abs_reduction) if getattr(u, 'ecri_max_abs_reduction', None) is not None else 0,
                    'allowed_site_ids': list(getattr(u, 'allowed_site_ids', None) or []),
                }
                for u in users
            ]
        })
    finally:
        db.close()


@admin_bp.route('/users')
@login_required
@admin_required
def list_users():
    """List all users with optional filtering and sorting."""
    from web.models.user import User
    from sqlalchemy import or_, asc, desc

    # Get filter parameters
    search = request.args.get('search', '').strip()
    dept_filter = request.args.get('department', '').strip()
    office_filter = request.args.get('office', '').strip()

    # Get sort parameters
    SORTABLE_COLUMNS = {
        'id': User.id,
        'username': User.username,
        'email': User.email,
        'department': User.department,
        'job_title': User.job_title,
        'office': User.office_location,
        'role': User.username,  # Role sorting not meaningful with multi-role; fallback to username
        'auth': User.auth_provider,
        'created': User.created_at,
    }
    sort_by = request.args.get('sort', 'username')
    sort_dir = request.args.get('dir', 'asc')
    if sort_by not in SORTABLE_COLUMNS:
        sort_by = 'username'
    if sort_dir not in ('asc', 'desc'):
        sort_dir = 'asc'

    db_session = get_session()
    try:
        query = db_session.query(User)

        # Text search across name and email
        if search:
            safe_q = search.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
            query = query.filter(or_(
                User.username.ilike(f'%{safe_q}%', escape='\\'),
                User.email.ilike(f'%{safe_q}%', escape='\\'),
                User.job_title.ilike(f'%{safe_q}%', escape='\\'),
            ))

        # Department filter (exact match from dropdown)
        if dept_filter:
            query = query.filter(User.department == dept_filter)

        # Office location filter (exact match from dropdown)
        if office_filter:
            query = query.filter(User.office_location == office_filter)

        # Apply sort
        col = SORTABLE_COLUMNS[sort_by]
        order_fn = desc if sort_dir == 'desc' else asc
        query = query.order_by(order_fn(col))

        # Pagination
        PER_PAGE = 15
        page = request.args.get('page', 1, type=int)
        if page < 1:
            page = 1
        total = query.count()
        total_pages = max(1, (total + PER_PAGE - 1) // PER_PAGE)
        if page > total_pages:
            page = total_pages
        users = query.offset((page - 1) * PER_PAGE).limit(PER_PAGE).all()

        # Get distinct departments and offices for filter dropdowns
        all_users = db_session.query(User).all()
        departments = sorted(set(u.department for u in all_users if u.department))
        offices = sorted(set(u.office_location for u in all_users if u.office_location))

        return render_template('admin/users/list.html',
                               users=users,
                               total=total,
                               page=page,
                               total_pages=total_pages,
                               per_page=PER_PAGE,
                               departments=departments,
                               offices=offices,
                               search=search,
                               dept_filter=dept_filter,
                               office_filter=office_filter,
                               sort_by=sort_by,
                               sort_dir=sort_dir)
    finally:
        db_session.close()


@admin_bp.route('/users/sync-o365', methods=['POST'])
@login_required
@admin_required
def sync_o365_profiles():
    """Backfill O365 profile fields for all Microsoft users via Graph API."""
    import requests as http_requests
    from web.models.user import User
    from common.config_loader import get_config

    config = get_config()
    ms = config.oauth.microsoft

    if not ms or not ms.enabled:
        flash('Microsoft OAuth is not configured.', 'error')
        return redirect(url_for('admin.list_users'))

    # Get app-only token via client credentials flow
    token_url = f"https://login.microsoftonline.com/{ms.tenant_id}/oauth2/v2.0/token"
    try:
        token_resp = http_requests.post(token_url, data={
            'grant_type': 'client_credentials',
            'client_id': ms.client_id,
            'client_secret': ms.client_secret_vault,
            'scope': 'https://graph.microsoft.com/.default',
        }, timeout=10)
        token_resp.raise_for_status()
        token = token_resp.json()['access_token']
    except Exception as e:
        current_app.logger.error(f"O365 sync: token error: {e}")
        flash('Failed to get Graph API token. Check OAuth config.', 'error')
        return redirect(url_for('admin.list_users'))

    # Fetch all users from Graph
    headers = {'Authorization': f'Bearer {token}'}
    graph_users = []
    url = 'https://graph.microsoft.com/v1.0/users'
    params = {'$select': 'mail,userPrincipalName,department,jobTitle,officeLocation,employeeId', '$top': '999'}
    try:
        while url:
            resp = http_requests.get(url, headers=headers, params=params, timeout=15)
            if resp.status_code == 403:
                flash('Graph API permission denied. Ask your Azure AD admin to grant User.Read.All application permission.', 'error')
                return redirect(url_for('admin.list_users'))
            resp.raise_for_status()
            data = resp.json()
            graph_users.extend(data.get('value', []))
            url = data.get('@odata.nextLink')
            params = None
    except Exception as e:
        current_app.logger.error(f"O365 sync: Graph API error: {e}")
        flash('Graph API error. Check server logs for details.', 'error')
        return redirect(url_for('admin.list_users'))

    # Build lookup by email
    graph_lookup = {}
    for gu in graph_users:
        email = (gu.get('mail') or gu.get('userPrincipalName') or '').lower()
        if email:
            graph_lookup[email] = gu

    # Update DB
    db_session = get_session()
    try:
        ms_users = db_session.query(User).filter_by(auth_provider='microsoft').all()
        updated = 0
        not_found = 0

        for user in ms_users:
            gu = graph_lookup.get((user.email or '').lower())
            if not gu:
                not_found += 1
                continue

            dept = gu.get('department') or None
            title = gu.get('jobTitle') or None
            office = gu.get('officeLocation') or None
            emp_id = gu.get('employeeId') or None

            if (user.department != dept or user.job_title != title
                    or user.office_location != office or user.employee_id != emp_id):
                user.department = dept
                user.job_title = title
                user.office_location = office
                user.employee_id = emp_id
                updated += 1

        db_session.commit()
        audit_log(AuditEvent.USER_UPDATED, f"O365 profile sync: {updated} updated, {not_found} not found in Graph, {len(ms_users)} total")
        flash(f'O365 sync complete: {updated} users updated, {len(ms_users) - updated - not_found} unchanged, {not_found} not found in Azure AD.', 'success')
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"O365 sync DB error: {e}")
        flash('Database error during sync. Check server logs.', 'error')
    finally:
        db_session.close()

    return redirect(url_for('admin.list_users'))


@admin_bp.route('/users/create', methods=['GET', 'POST'])
@login_required
@admin_required
def create_user():
    """Create a new user."""
    from web.models.user import User
    from web.models.role import Role

    db_session = get_session()
    try:
        roles = db_session.query(Role).order_by(Role.name).all()

        if request.method == 'POST':
            username = request.form.get('username', '').strip()
            email = request.form.get('email', '').strip() or None
            password = request.form.get('password', '')
            role_ids = request.form.getlist('role_ids', type=int)

            if not username:
                flash('Username is required.', 'error')
                return render_template('admin/users/edit.html', user=None, roles=roles)

            if not role_ids:
                flash('At least one role is required.', 'error')
                return render_template('admin/users/edit.html', user=None, roles=roles)

            # Verify all roles exist
            selected_roles = db_session.query(Role).filter(Role.id.in_(role_ids)).all()
            if len(selected_roles) != len(role_ids):
                flash('One or more invalid roles.', 'error')
                return render_template('admin/users/edit.html', user=None, roles=roles)

            # Check for duplicate username
            if db_session.query(User).filter_by(username=username).first():
                flash('Username already exists.', 'error')
                return render_template('admin/users/edit.html', user=None, roles=roles)

            # Validate and hash password
            hashed_password = None
            if password:
                is_valid, message = validate_password(password)
                if not is_valid:
                    flash(message, 'error')
                    return render_template('admin/users/edit.html', user=None, roles=roles)
                hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

            user = User(
                username=username,
                email=email,
                password=hashed_password,
                auth_provider='local'
            )
            user.roles = selected_roles
            db_session.add(user)
            db_session.commit()

            role_names = [r.name for r in selected_roles]
            audit_log(AuditEvent.USER_CREATED, f"Created user '{username}' with roles: {role_names}")
            flash(f'User "{username}" created successfully.', 'success')
            return redirect(url_for('admin.list_users'))

        return render_template('admin/users/edit.html', user=None, roles=roles)
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Error creating user: {e}")
        flash('An error occurred.', 'error')
        return redirect(url_for('admin.list_users'))
    finally:
        db_session.close()


@admin_bp.route('/users/<int:user_id>/edit', methods=['GET', 'POST'])
@login_required
@admin_required
def edit_user(user_id):
    """Edit an existing user."""
    from web.models.user import User
    from web.models.role import Role

    db_session = get_session()
    try:
        user = db_session.query(User).get(user_id)
        if not user:
            flash('User not found.', 'error')
            return redirect(url_for('admin.list_users'))

        roles = db_session.query(Role).order_by(Role.name).all()

        if request.method == 'POST':
            user.email = request.form.get('email', '').strip() or None
            role_ids = request.form.getlist('role_ids', type=int)
            old_role_names = sorted(r.name for r in user.roles)

            if not role_ids:
                flash('At least one role is required.', 'error')
                return render_template('admin/users/edit.html', user=user, roles=roles)

            # Verify all roles exist
            selected_roles = db_session.query(Role).filter(Role.id.in_(role_ids)).all()
            if len(selected_roles) != len(role_ids):
                flash('One or more invalid roles.', 'error')
                return render_template('admin/users/edit.html', user=user, roles=roles)

            user.roles = selected_roles

            # Update password if provided
            new_password = request.form.get('password', '')
            password_changed = False
            if new_password:
                is_valid, message = validate_password(new_password)
                if not is_valid:
                    flash(message, 'error')
                    return render_template('admin/users/edit.html', user=user, roles=roles)
                user.password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                password_changed = True

            db_session.commit()

            # Audit logging
            new_role_names = sorted(r.name for r in selected_roles)
            changes = []
            if old_role_names != new_role_names:
                audit_log(AuditEvent.USER_ROLE_CHANGED, f"User '{user.username}' roles changed from {old_role_names} to {new_role_names}")
                changes.append(f"roles: {old_role_names}->{new_role_names}")
            if password_changed:
                changes.append("password changed")
            audit_log(AuditEvent.USER_UPDATED, f"Updated user '{user.username}' (id={user_id}): {', '.join(changes) if changes else 'email updated'}")

            flash('User updated successfully.', 'success')
            return redirect(url_for('admin.list_users'))

        return render_template('admin/users/edit.html', user=user, roles=roles)
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Error editing user: {e}")
        flash('An error occurred.', 'error')
        return redirect(url_for('admin.list_users'))
    finally:
        db_session.close()


@admin_bp.route('/users/<int:user_id>/delete', methods=['POST'])
@login_required
@admin_required
def delete_user(user_id):
    """Delete a user."""
    from web.models.user import User

    if user_id == current_user.id:
        flash('You cannot delete yourself.', 'error')
        return redirect(url_for('admin.list_users'))

    db_session = get_session()
    try:
        user = db_session.query(User).get(user_id)
        if user:
            username = user.username
            db_session.delete(user)
            db_session.commit()
            audit_log(AuditEvent.USER_DELETED, f"Deleted user '{username}' (id={user_id})")
            flash(f'User "{username}" deleted.', 'success')
        else:
            flash('User not found.', 'error')
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Error deleting user: {e}")
        flash('An error occurred.', 'error')
    finally:
        db_session.close()

    return redirect(url_for('admin.list_users'))


# =============================================================================
# User Search API (for Select2 in page access control)
# =============================================================================

@admin_bp.route('/api/users/search')
@login_required
def search_users():
    """Search users for Select2 dropdown."""
    from web.models.user import User

    q = request.args.get('q', '').strip()[:50]

    db_session = get_session()
    try:
        query = db_session.query(User)
        if q:
            # Escape SQL LIKE wildcard characters in user input
            safe_q = q.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
            query = query.filter(User.username.ilike(f'%{safe_q}%', escape='\\'))
        users = query.order_by(User.username).limit(20).all()
        return jsonify({
            'results': [{'id': u.id, 'text': u.username} for u in users]
        })
    finally:
        db_session.close()


# =============================================================================
# Page Management (Editor permission)
# =============================================================================

@admin_bp.route('/pages')
@login_required
@editor_required
def list_pages():
    """List all pages."""
    from web.models.page import Page

    db_session = get_session()
    try:
        pages = db_session.query(Page).order_by(Page.slug).all()
        return render_template('admin/pages/list.html', pages=pages)
    finally:
        db_session.close()


@admin_bp.route('/pages/create', methods=['GET', 'POST'])
@login_required
@editor_required
def create_page():
    """Create a new page."""
    from web.models.page import Page
    from web.models.role import Role
    from web.models.user import User

    db_session = get_session()
    try:
        roles = db_session.query(Role).order_by(Role.name).all()

        if request.method == 'POST':
            title = request.form.get('title', '').strip()
            slug = request.form.get('slug', '').strip()
            content = request.form.get('content', '')
            extension = request.form.get('extension', 'html')
            is_public = request.form.get('is_public') == 'on'

            # Access control lists
            view_roles = ','.join(request.form.getlist('view_roles'))
            view_users = ','.join(request.form.getlist('view_users'))
            edit_roles = ','.join(request.form.getlist('edit_roles'))
            edit_users = ','.join(request.form.getlist('edit_users'))

            if not title or not slug:
                flash('Title and slug are required.', 'error')
                return render_template('admin/pages/edit.html', page=None, extensions=Page.ALLOWED_EXTENSIONS, roles=roles)

            if extension not in Page.ALLOWED_EXTENSIONS:
                flash('Invalid extension.', 'error')
                return render_template('admin/pages/edit.html', page=None, extensions=Page.ALLOWED_EXTENSIONS, roles=roles)

            # Check for duplicate slug
            if db_session.query(Page).filter_by(slug=slug).first():
                flash('Slug already exists.', 'error')
                return render_template('admin/pages/edit.html', page=None, extensions=Page.ALLOWED_EXTENSIONS, roles=roles)

            page = Page(
                title=title,
                slug=slug,
                content=content,
                extension=extension,
                is_public=is_public,
                view_roles=view_roles,
                view_users=view_users,
                edit_roles=edit_roles,
                edit_users=edit_users
            )
            db_session.add(page)
            db_session.commit()

            audit_log(AuditEvent.PAGE_CREATED, f"Created page '{title}' ({slug}.{extension}), public={is_public}")
            flash(f'Page "{title}" created successfully.', 'success')
            return redirect(url_for('admin.list_pages'))

        return render_template('admin/pages/edit.html', page=None, extensions=Page.ALLOWED_EXTENSIONS, roles=roles)
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Error creating page: {e}")
        flash('An error occurred.', 'error')
        return redirect(url_for('admin.list_pages'))
    finally:
        db_session.close()


@admin_bp.route('/pages/<int:page_id>/edit', methods=['GET', 'POST'])
@login_required
@editor_required
def edit_page(page_id):
    """Edit an existing page."""
    from web.models.page import Page
    from web.models.role import Role
    from web.models.user import User

    db_session = get_session()
    try:
        page = db_session.query(Page).get(page_id)
        if not page:
            flash('Page not found.', 'error')
            return redirect(url_for('admin.list_pages'))

        # Check edit permissions
        if not page.can_edit(current_user):
            flash('You do not have permission to edit this page.', 'error')
            return redirect(url_for('admin.list_pages'))

        roles = db_session.query(Role).order_by(Role.name).all()

        # Get selected users for display
        view_user_ids = page.get_view_users_list()
        edit_user_ids = page.get_edit_users_list()
        view_users = db_session.query(User).filter(User.id.in_(view_user_ids)).all() if view_user_ids else []
        edit_users = db_session.query(User).filter(User.id.in_(edit_user_ids)).all() if edit_user_ids else []

        if request.method == 'POST':
            page.title = request.form.get('title', '').strip()
            page.content = request.form.get('content', '')
            extension = request.form.get('extension', page.extension)
            page.is_public = request.form.get('is_public') == 'on'

            # Access control lists
            page.view_roles = ','.join(request.form.getlist('view_roles'))
            page.view_users = ','.join(request.form.getlist('view_users'))
            page.edit_roles = ','.join(request.form.getlist('edit_roles'))
            page.edit_users = ','.join(request.form.getlist('edit_users'))

            if extension in Page.ALLOWED_EXTENSIONS:
                page.extension = extension

            db_session.commit()
            audit_log(AuditEvent.PAGE_UPDATED, f"Updated page '{page.title}' ({page.slug}.{page.extension})")
            flash('Page updated successfully.', 'success')
            return redirect(url_for('admin.list_pages'))

        return render_template('admin/pages/edit.html',
                             page=page,
                             extensions=Page.ALLOWED_EXTENSIONS,
                             roles=roles,
                             view_users=view_users,
                             edit_users=edit_users)
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Error editing page: {e}")
        flash('An error occurred.', 'error')
        return redirect(url_for('admin.list_pages'))
    finally:
        db_session.close()


@admin_bp.route('/pages/<int:page_id>/delete', methods=['POST'])
@login_required
@admin_required
def delete_page(page_id):
    """Delete a page (admin only)."""
    from web.models.page import Page

    db_session = get_session()
    try:
        page = db_session.query(Page).get(page_id)
        if page:
            page_title = page.title
            page_slug = page.slug
            db_session.delete(page)
            db_session.commit()
            audit_log(AuditEvent.PAGE_DELETED, f"Deleted page '{page_title}' ({page_slug})")
            flash(f'Page "{page_title}" deleted.', 'success')
        else:
            flash('Page not found.', 'error')
    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Error deleting page: {e}")
        flash('An error occurred.', 'error')
    finally:
        db_session.close()

    return redirect(url_for('admin.list_pages'))


# =============================================================================
# Configuration Management (Config permission required)
# =============================================================================

@admin_bp.route('/config')
@login_required
@config_required
def list_configs():
    """List all configuration files."""
    from common.config_loader import get_config

    config = get_config()
    config_files = config.get_config_files()

    configs = []
    for name in config_files:
        raw_data = config.get_raw_config(name)
        key_count = len(raw_data) if raw_data else 0
        configs.append({
            'name': name,
            'file': f"{name}.yaml",
            'section_count': key_count
        })

    return render_template('admin/config/list.html', configs=configs)


@admin_bp.route('/config/<name>', methods=['GET', 'POST'])
@login_required
@config_required
def edit_config(name):
    """Edit a configuration file."""
    import yaml
    from common.config_loader import get_config

    config = get_config()

    if request.method == 'POST':
        try:
            yaml_content = request.form.get('content', '')
            # Parse YAML to validate
            data = yaml.safe_load(yaml_content)
            if config.update_config(name, data):
                flash(f'Configuration "{name}" updated successfully.', 'success')
                return redirect(url_for('admin.list_configs'))
            else:
                flash('Failed to save configuration.', 'error')
        except yaml.YAMLError as e:
            flash(f'Invalid YAML: {e}', 'error')
        except Exception as e:
            flash(f'Error: {e}', 'error')

    # Get current content
    raw_data = config.get_raw_config(name)
    content = yaml.dump(raw_data, default_flow_style=False, sort_keys=False) if raw_data else ''

    return render_template('admin/config/edit.html', name=name, content=content)


# =============================================================================
# Secrets Management (Config permission required)
# =============================================================================

@admin_bp.route('/secrets')
@login_required
@config_required
def list_secrets():
    """List all secrets in vault with environment metadata."""
    from common.config_loader import get_config

    config = get_config()

    if not config.vault_available:
        flash('Vault is not available.', 'error')
        return render_template('admin/secrets/list.html', secrets=[], vault_available=False)

    secrets = config.list_secrets_detail()
    return render_template('admin/secrets/list.html', secrets=secrets, vault_available=True)


_SECRET_KEY_PATTERN = re.compile(r'^[A-Z0-9_]{1,100}$')
_VALID_SECRET_ENVS = {'all', 'production', 'development'}


@admin_bp.route('/secrets/add', methods=['GET', 'POST'])
@login_required
@config_required
def add_secret():
    """Add a new secret to vault."""
    from common.config_loader import get_config

    config = get_config()

    if not config.vault_available:
        flash('Vault is not available.', 'error')
        return redirect(url_for('admin.list_secrets'))

    if request.method == 'POST':
        key = request.form.get('key', '').strip().upper()
        value = request.form.get('value', '')
        environment = request.form.get('environment', 'all').strip().lower()
        description = request.form.get('description', '').strip() or None

        if not key or not _SECRET_KEY_PATTERN.match(key):
            flash('Invalid secret key. Use UPPERCASE letters, digits, and underscores (max 100 chars).', 'error')
            return render_template('admin/secrets/edit.html', secret=None)

        if environment not in _VALID_SECRET_ENVS:
            flash('Invalid environment. Must be: all, production, or development.', 'error')
            return render_template('admin/secrets/edit.html', secret=None)

        if config.set_secret(key, value, environment=environment,
                             description=description, updated_by=current_user.username):
            audit_log(AuditEvent.CONFIG_UPDATED, f"Secret '{key}' added (env={environment})")
            flash(f'Secret "{key}" added successfully.', 'success')
            return redirect(url_for('admin.list_secrets'))
        else:
            flash('Failed to add secret.', 'error')

    return render_template('admin/secrets/edit.html', secret=None)


@admin_bp.route('/secrets/<key>/edit', methods=['GET', 'POST'])
@login_required
@config_required
def edit_secret(key):
    """Edit an existing secret."""
    from common.config_loader import get_config

    config = get_config()

    if not config.vault_available:
        flash('Vault is not available.', 'error')
        return redirect(url_for('admin.list_secrets'))

    if not _SECRET_KEY_PATTERN.match(key):
        flash('Invalid secret key format.', 'error')
        return redirect(url_for('admin.list_secrets'))

    if request.method == 'POST':
        value = request.form.get('value', '')
        environment = request.form.get('environment', 'all').strip().lower()
        description = request.form.get('description', '').strip() or None

        if environment not in _VALID_SECRET_ENVS:
            flash('Invalid environment.', 'error')
            return render_template('admin/secrets/edit.html', secret={'key': key, 'has_value': True})

        if config.set_secret(key, value, environment=environment,
                             description=description, updated_by=current_user.username):
            audit_log(AuditEvent.CONFIG_UPDATED, f"Secret '{key}' updated (env={environment})")
            flash(f'Secret "{key}" updated successfully.', 'success')
            return redirect(url_for('admin.list_secrets'))
        else:
            flash('Failed to update secret.', 'error')

    # Check existence without decrypting
    has_value = config.has_secret(key)

    return render_template('admin/secrets/edit.html', secret={'key': key, 'has_value': has_value})


@admin_bp.route('/secrets/<key>/delete', methods=['POST'])
@login_required
@config_required
def delete_secret(key):
    """Delete a secret from vault."""
    from common.config_loader import get_config

    config = get_config()

    if not config.vault_available:
        flash('Vault is not available.', 'error')
        return redirect(url_for('admin.list_secrets'))

    if not _SECRET_KEY_PATTERN.match(key):
        flash('Invalid secret key format.', 'error')
        return redirect(url_for('admin.list_secrets'))

    if config.delete_secret(key):
        audit_log(AuditEvent.CONFIG_UPDATED, f"Secret '{key}' deleted")
        flash(f'Secret "{key}" deleted.', 'success')
    else:
        flash('Failed to delete secret.', 'error')

    return redirect(url_for('admin.list_secrets'))


# =============================================================================
# Type Mappings (Config permission required)
# =============================================================================

@admin_bp.route('/type-mappings')
@login_required
@config_required
def type_mappings():
    """Manage inventory type mappings."""
    return render_template('admin/type_mappings/manage.html')


# =============================================================================
# Services (Config permission required)
# =============================================================================

@admin_bp.route('/services')
@login_required
@config_required
def services_page():
    """Service management page."""
    return render_template('admin/services.html')


# =============================================================================
# Site Billing Config — proration / billing-mode per site
# =============================================================================

def _get_pbi_session():
    """Get PBI database session for site billing config (Azure-hosted)."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from common.config_loader import get_database_url
    engine = create_engine(get_database_url('pbi'))
    return sessionmaker(bind=engine)()


@admin_bp.route('/site-billing-config')
@login_required
@config_required
def site_billing_config_list():
    """View per-site proration and billing-mode configuration."""
    from common.models import CcwsSiteBillingConfig
    session = _get_pbi_session()
    try:
        configs = session.query(CcwsSiteBillingConfig).order_by(
            CcwsSiteBillingConfig.SiteCode).all()
        rows = [{
            'id': c.id,
            'SiteCode': c.SiteCode,
            'SiteID': c.SiteID,
            'b_anniv_date_leasing': c.b_anniv_date_leasing,
            'i_day_strt_prorating': c.i_day_strt_prorating,
            'i_day_strt_prorate_plus_next': c.i_day_strt_prorate_plus_next,
            'synced_from_soap_at': c.synced_from_soap_at,
            'overridden_by': c.overridden_by,
            'overridden_at': c.overridden_at,
            'notes': c.notes,
        } for c in configs]
    finally:
        session.close()
    return render_template('admin/site_billing_config/list.html', configs=rows)


@admin_bp.route('/site-billing-config/<int:config_id>/edit',
                methods=['GET', 'POST'])
@login_required
@config_required
def site_billing_config_edit(config_id):
    """Edit one site's billing config (creates an override)."""
    from common.models import CcwsSiteBillingConfig
    from datetime import datetime, timezone
    session = _get_pbi_session()
    try:
        cfg = session.query(CcwsSiteBillingConfig).get(config_id)
        if not cfg:
            flash('Config not found', 'error')
            return redirect(url_for('admin.site_billing_config_list'))

        if request.method == 'POST':
            try:
                anniv = request.form.get('b_anniv_date_leasing') == 'on'
                day_start = int(request.form.get('i_day_strt_prorating', 1))
                day_plus_next = int(
                    request.form.get('i_day_strt_prorate_plus_next', 17))
                notes = (request.form.get('notes') or '').strip()[:1000]
            except (TypeError, ValueError):
                flash('Invalid numeric input', 'error')
                return redirect(url_for(
                    'admin.site_billing_config_edit', config_id=config_id))

            if not (1 <= day_start <= 31):
                flash('iDayStrtProrating must be 1-31', 'error')
                return redirect(url_for(
                    'admin.site_billing_config_edit', config_id=config_id))
            if not (1 <= day_plus_next <= 31):
                flash('iDayStrtProratePlusNext must be 1-31', 'error')
                return redirect(url_for(
                    'admin.site_billing_config_edit', config_id=config_id))

            cfg.b_anniv_date_leasing = anniv
            cfg.i_day_strt_prorating = day_start
            cfg.i_day_strt_prorate_plus_next = day_plus_next
            cfg.notes = notes
            cfg.overridden_by = current_user.email or current_user.username
            cfg.overridden_at = datetime.now(timezone.utc).replace(tzinfo=None)
            session.commit()

            audit_log(AuditEvent.CONFIG_UPDATED,
                      f"site_billing_config site={cfg.SiteCode} "
                      f"anniv={anniv} prorate_start={day_start} "
                      f"prorate_plus_next={day_plus_next}")
            flash(f'Updated billing config for {cfg.SiteCode}', 'success')
            return redirect(url_for('admin.site_billing_config_list'))

        return render_template(
            'admin/site_billing_config/edit.html', config=cfg)
    finally:
        session.close()


@admin_bp.route('/site-billing-config/<int:config_id>/clear-override',
                methods=['POST'])
@login_required
@config_required
def site_billing_config_clear_override(config_id):
    """Clear manual override so the next pipeline run resyncs from SOAP."""
    from common.models import CcwsSiteBillingConfig
    session = _get_pbi_session()
    try:
        cfg = session.query(CcwsSiteBillingConfig).get(config_id)
        if not cfg:
            flash('Config not found', 'error')
            return redirect(url_for('admin.site_billing_config_list'))
        site = cfg.SiteCode
        cfg.overridden_by = None
        cfg.overridden_at = None
        session.commit()
        audit_log(AuditEvent.CONFIG_UPDATED,
                  f"site_billing_config override cleared site={site}")
        flash(f'Override cleared for {site}. Next sync will refresh from SOAP.',
              'success')
    finally:
        session.close()
    return redirect(url_for('admin.site_billing_config_list'))


# =============================================================================
# API Key Management (Admin — under User Management section)
# =============================================================================
# Admins manage: scopes, rate limits, quotas per user's API key.
# Users can only view/regenerate their own key (separate route).

@admin_bp.route('/api-keys')
@login_required
@admin_required
def list_api_keys():
    """List all users and their API key status."""
    from web.models.user import User
    from web.models.api_key import ApiKey, API_SCOPES

    db = get_session()
    try:
        users = db.query(User).order_by(User.username).all()
        # Build a map of user_id -> ApiKey
        keys = db.query(ApiKey).all()
        key_map = {k.user_id: k for k in keys}

        return render_template('admin/api_keys/list.html',
                               users=users,
                               key_map=key_map,
                               all_scopes=API_SCOPES)
    finally:
        db.close()


def _get_mcp_db_presets():
    """Get available MCP database presets for the admin UI.
    Returns dict of {preset_name: 'type — database'} for display."""
    try:
        from common.config_loader import get_config
        config = get_config()
        raw = config.get_raw_config('mcp')
        databases = raw.get('databases', {})
        result = {}
        for name, db in databases.items():
            db_type = db.get('type', 'postgresql')
            if db_type == 'bigquery':
                result[name] = f"BigQuery — {db.get('project_id', '?')}"
            else:
                result[name] = f"{db_type} — {db.get('database', '?')}"
        return result
    except Exception:
        return {}


@admin_bp.route('/api-keys/preset-tables/<preset_name>', methods=['GET'])
@login_required
@admin_required
def get_preset_tables(preset_name):
    """AJAX: Return list of tables in a database preset (for table access control UI)."""
    try:
        from common.config_loader import get_config
        config = get_config()
        raw = config.get_raw_config('mcp')
        databases = raw.get('databases', {})

        if preset_name not in databases:
            return jsonify({"error": "Preset not found"}), 404

        db_config = databases[preset_name]
        db_type = db_config.get('type', 'postgresql')

        # Resolve password from vault
        password = None
        pw_key = db_config.get('password_vault')
        if pw_key:
            from common.secrets_vault import vault_config
            password = vault_config(pw_key, default=None)
        else:
            password = db_config.get('password')

        tables = []

        if db_type == 'bigquery':
            # BigQuery: use google-cloud-bigquery
            creds_key = db_config.get('credentials_json_vault')
            if creds_key:
                import json as json_mod
                from common.secrets_vault import vault_config
                from google.cloud import bigquery
                from google.oauth2 import service_account
                creds_json = vault_config(creds_key, default=None)
                if creds_json:
                    info = json_mod.loads(creds_json)
                    credentials = service_account.Credentials.from_service_account_info(info)
                    client = bigquery.Client(credentials=credentials, project=db_config.get('project_id'))
                    dataset = db_config.get('dataset')
                    if dataset:
                        for t in client.list_tables(dataset):
                            tables.append(t.table_id)
        else:
            # PostgreSQL / MySQL / MariaDB / MSSQL — use SQLAlchemy
            from sqlalchemy import create_engine, text as sa_text
            from sqlalchemy.engine import URL

            driver_map = {
                'postgresql': 'postgresql',
                'mysql': 'mysql+pymysql',
                'mariadb': 'mysql+pymysql',
                'mssql': 'mssql+pyodbc',
            }
            driver = driver_map.get(db_type)
            if not driver:
                return jsonify({"error": f"Unsupported DB type: {db_type}"}), 400

            port_defaults = {'postgresql': 5432, 'mysql': 3306, 'mariadb': 3306, 'mssql': 1433}
            query_params = {}
            if db_type == 'postgresql' and db_config.get('ssl'):
                query_params['sslmode'] = 'require'
            elif db_type == 'mssql':
                query_params['driver'] = 'ODBC Driver 17 for SQL Server'

            url = URL.create(
                drivername=driver,
                username=db_config['user'],
                password=password,
                host=db_config['host'],
                port=db_config.get('port', port_defaults.get(db_type, 5432)),
                database=db_config['database'],
                query=query_params,
            )

            engine = create_engine(url)
            with engine.connect() as conn:
                if db_type == 'postgresql':
                    rows = conn.execute(sa_text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = 'public' AND table_type IN ('BASE TABLE', 'VIEW') "
                        "ORDER BY table_name"
                    ))
                elif db_type in ('mysql', 'mariadb'):
                    rows = conn.execute(sa_text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = :db ORDER BY table_name"
                    ), {"db": db_config['database']})
                elif db_type == 'mssql':
                    rows = conn.execute(sa_text(
                        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                        "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_TYPE = 'BASE TABLE' "
                        "ORDER BY TABLE_NAME"
                    ))
                tables = [row[0] for row in rows]
            engine.dispose()

        # Sanitize: only return valid identifier names
        safe_tables = [t for t in tables if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]{0,127}$', t)]
        return jsonify({"tables": sorted(safe_tables)})

    except Exception as e:
        logger.error(f"Failed to list tables for preset {preset_name}: {e}")
        return jsonify({"error": "Failed to connect to database"}), 500


def _get_mcp_tools_grouped():
    """Get available MCP tools grouped by category for the admin UI."""
    from collections import OrderedDict
    tools = OrderedDict()
    tools['Health'] = ['health_check', 'echo', 'ping']
    tools['Database'] = [
        'DB_list_database_presets', 'DB_connect_preset', 'DB_connect_multiple_presets',
        'DB_execute_query', 'DB_list_tables', 'DB_describe_table',
        'DB_list_connections', 'DB_disconnect_database',
    ]
    tools['Google Ads - Account'] = [
        'GA_test_connection', 'GA_list_accessible_customers', 'GA_get_account_info',
    ]
    tools['Google Ads - Campaigns'] = [
        'GA_list_campaigns', 'GA_get_campaign', 'GA_create_campaign',
        'GA_update_campaign', 'GA_set_campaign_status',
    ]
    tools['Google Ads - Ad Groups'] = [
        'GA_list_ad_groups', 'GA_create_ad_group', 'GA_update_ad_group',
    ]
    tools['Google Ads - Reporting'] = [
        'GA_query', 'GA_get_campaign_performance', 'GA_get_account_performance',
        'GA_get_keyword_performance',
    ]
    tools['Google Ads - AI Analysis'] = [
        'GA_audit_account', 'GA_analyze_keywords', 'GA_analyze_search_terms',
        'GA_suggest_negative_keywords', 'GA_analyze_competitors',
        'GA_analyze_quality_scores', 'GA_analyze_trends',
        'GA_analyze_audiences', 'GA_optimize_budget', 'GA_generate_report',
    ]
    tools['Google Ads - Token'] = ['GA_start_token_refresh']
    tools['Google Analytics 4 - Account'] = [
        'GA4_test_connection', 'GA4_list_properties', 'GA4_get_metadata',
    ]
    tools['Google Analytics 4 - Reports'] = [
        'GA4_run_report', 'GA4_run_realtime',
    ]
    tools['Google Analytics 4 - Pre-built'] = [
        'GA4_top_pages', 'GA4_traffic_sources', 'GA4_user_acquisition',
        'GA4_conversions', 'GA4_device_breakdown', 'GA4_geo_breakdown',
    ]
    tools['Naver Search Ad - Account'] = [
        'NSA_test_connection', 'NSA_list_business_channels',
    ]
    tools['Naver Search Ad - Campaigns'] = [
        'NSA_list_campaigns', 'NSA_get_campaign', 'NSA_create_campaign',
        'NSA_set_campaign_status', 'NSA_delete_campaign',
    ]
    tools['Naver Search Ad - Ad Groups & Keywords'] = [
        'NSA_list_ad_groups', 'NSA_get_ad_group', 'NSA_list_keywords',
        'NSA_update_keyword_bid', 'NSA_list_ads',
    ]
    tools['Naver Search Ad - Reporting'] = [
        'NSA_get_stats', 'NSA_create_stat_report', 'NSA_get_stat_report',
        'NSA_keyword_tool',
    ]
    tools['Naver Search Ad - Billing'] = [
        'NSA_get_bizmoney_balance', 'NSA_get_bizmoney_cost',
    ]
    tools['Naver Search Ad - AI Analysis'] = [
        'NSA_audit_account', 'NSA_analyze_keywords', 'NSA_analyze_trends',
        'NSA_suggest_negative_keywords', 'NSA_optimize_budget', 'NSA_generate_report',
    ]
    tools['Revenue - Data'] = [
        'RM_get_portfolio_snapshot', 'RM_get_site_performance', 'RM_get_budget_variance',
        'RM_get_occupancy_trends', 'RM_get_movement_analysis', 'RM_get_rate_analysis',
        'RM_get_customer_segments',
    ]
    tools['Revenue - AI Analysis'] = [
        'RM_analyze_revenue', 'RM_detect_anomalies', 'RM_generate_executive_report',
    ]
    tools['SugarCRM - Read'] = [
        'SC_get_record', 'SC_list_records', 'SC_search', 'SC_get_related',
        'SC_list_modules', 'SC_list_fields', 'SC_get_field',
        'SC_list_dropdowns', 'SC_get_dropdown', 'SC_get_layout',
        'SC_get_lead', 'SC_get_contact', 'SC_get_account', 'SC_search_by_email',
    ]
    tools['SugarCRM - Write'] = [
        'SC_create_record', 'SC_update_record', 'SC_delete_record',
        'SC_link_records', 'SC_unlink_records',
        'SC_create_lead', 'SC_convert_lead', 'SC_log_call',
    ]
    tools['SugarCRM - Admin (Studio)'] = [
        'SC_create_field', 'SC_update_field', 'SC_delete_field',
        'SC_update_dropdown',
        'SC_create_relationship', 'SC_delete_relationship',
        'SC_update_layout', 'SC_studio_deploy',
        'SC_list_fields_admin',
    ]
    tools['SugarCRM - Module Loader'] = [
        'SC_list_packages', 'SC_get_package',
        'SC_upload_package', 'SC_install_package', 'SC_uninstall_package',
    ]
    tools['Google Search Console'] = [
        'GSC_test_connection', 'GSC_list_sites', 'GSC_analyze_keywords',
        'GSC_inspect_url', 'GSC_list_sitemaps', 'GSC_submit_sitemap',
        'GSC_delete_sitemap', 'GSC_get_coverage',
    ]
    return tools


@admin_bp.route('/api-keys/<int:user_id>/edit', methods=['GET', 'POST'])
@login_required
@admin_required
def edit_api_key(user_id):
    """Edit scopes, rate limit, and quota for a user's API key."""
    from web.models.user import User
    from web.models.api_key import ApiKey, API_SCOPES, DEFAULT_RATE_LIMIT, DEFAULT_DAILY_QUOTA

    db = get_session()
    try:
        user = db.query(User).get(user_id)
        if not user:
            flash('User not found.', 'error')
            return redirect(url_for('admin.list_api_keys'))

        api_key = db.query(ApiKey).filter_by(user_id=user_id).first()

        if request.method == 'POST':
            if not api_key:
                flash('This user has no API key yet. They must generate one first.', 'error')
                return redirect(url_for('admin.list_api_keys'))

            # Update scopes
            scopes = request.form.getlist('scopes')
            scopes = [s for s in scopes if s in API_SCOPES]
            api_key.scopes = scopes

            # Update rate limit
            rl = request.form.get('rate_limit', str(DEFAULT_RATE_LIMIT)).strip()
            try:
                api_key.rate_limit = max(0, int(rl))
            except (ValueError, TypeError):
                api_key.rate_limit = DEFAULT_RATE_LIMIT

            # Update daily quota
            dq = request.form.get('daily_quota', str(DEFAULT_DAILY_QUOTA)).strip()
            try:
                api_key.daily_quota = max(0, int(dq))
            except (ValueError, TypeError):
                api_key.daily_quota = DEFAULT_DAILY_QUOTA

            # Update active status
            api_key.is_active = request.form.get('is_active') == 'on'

            # Update MCP access
            api_key.mcp_enabled = request.form.get('mcp_enabled') == 'on'

            # Update MCP tool restrictions
            mcp_tools = request.form.getlist('mcp_tools')
            api_key.mcp_tools = mcp_tools if mcp_tools else []

            # Update MCP DB preset restrictions
            mcp_db_presets = request.form.getlist('mcp_db_presets')
            api_key.mcp_db_presets = mcp_db_presets if mcp_db_presets else []

            # Update MCP DB table restrictions
            # Form sends: mcp_db_table_rules__esa_pbi=rent_rolls&mcp_db_table_rules__esa_pbi=site_info&...
            mcp_db_table_rules = {}
            for key in request.form:
                if key.startswith('mcp_db_table_rules__'):
                    preset = key[len('mcp_db_table_rules__'):]
                    tables = request.form.getlist(key)
                    if tables:
                        mcp_db_table_rules[preset] = tables
            api_key.mcp_db_table_rules = mcp_db_table_rules

            db.commit()
            audit_log(AuditEvent.CONFIG_UPDATED,
                      f"Updated API key config for user '{user.username}': scopes={scopes}, "
                      f"rate_limit={api_key.rate_limit}, daily_quota={api_key.daily_quota}, "
                      f"mcp_enabled={api_key.mcp_enabled}, mcp_tools={len(mcp_tools)} selected, "
                      f"mcp_db_presets={len(mcp_db_presets)} selected, "
                      f"mcp_db_table_rules={len(mcp_db_table_rules)} preset(s) restricted")
            flash(f'API key settings updated for {user.username}.', 'success')
            return redirect(url_for('admin.list_api_keys'))

        # Build grouped MCP tool list for the UI
        mcp_tools_available = _get_mcp_tools_grouped()
        mcp_db_presets_available = _get_mcp_db_presets()

        return render_template('admin/api_keys/edit.html',
                               user=user,
                               api_key=api_key,
                               all_scopes=API_SCOPES,
                               default_rate_limit=DEFAULT_RATE_LIMIT,
                               default_daily_quota=DEFAULT_DAILY_QUOTA,
                               mcp_tools_available=mcp_tools_available,
                               mcp_db_presets_available=mcp_db_presets_available)
    finally:
        db.close()


@admin_bp.route('/api-keys/<int:user_id>/revoke', methods=['POST'])
@login_required
@admin_required
def admin_revoke_api_key(user_id):
    """Admin revoke a user's API key."""
    from web.models.api_key import ApiKey

    db = get_session()
    try:
        api_key = db.query(ApiKey).filter_by(user_id=user_id).first()
        if api_key:
            api_key.is_active = False
            db.commit()
            audit_log(AuditEvent.CONFIG_UPDATED, f"Admin revoked API key for user_id={user_id}")
            flash('API key revoked.', 'success')
        else:
            flash('No API key found for this user.', 'error')
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"Error revoking API key: {e}")
        flash('An error occurred.', 'error')
    finally:
        db.close()

    return redirect(url_for('admin.list_api_keys'))


@admin_bp.route('/api-keys/<int:user_id>/delete', methods=['POST'])
@login_required
@admin_required
def admin_delete_api_key(user_id):
    """Admin delete a user's API key entirely."""
    from web.models.api_key import ApiKey

    db = get_session()
    try:
        api_key = db.query(ApiKey).filter_by(user_id=user_id).first()
        if api_key:
            db.delete(api_key)
            db.commit()
            audit_log(AuditEvent.CONFIG_UPDATED, f"Admin deleted API key for user_id={user_id}")
            flash('API key deleted. User can generate a new one.', 'success')
        else:
            flash('No API key found for this user.', 'error')
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"Error deleting API key: {e}")
        flash('An error occurred.', 'error')
    finally:
        db.close()

    return redirect(url_for('admin.list_api_keys'))


# =============================================================================
# Site Distance Editor (Config permission required)
# =============================================================================

def _get_middleware_session():
    """Get middleware DB session (mw_site_distance lives in esa_middleware)."""
    return current_app.get_middleware_session()


@admin_bp.route('/site-distance')
@login_required
@config_required
def site_distance():
    """Render the mw_site_distance grid editor, grouped by country."""
    from sqlalchemy import text

    db = _get_middleware_session()
    try:
        # Pull all site-distance rows + country name via mw_siteinfo join.
        rows = db.execute(text("""
            SELECT
                d.from_site_code,
                d.to_site_code,
                d.distance_km,
                d.same_country,
                d.notes,
                d.updated_at,
                d.updated_by,
                si_from."Country" AS country
            FROM mw_site_distance d
            JOIN mw_siteinfo si_from ON si_from."SiteCode" = d.from_site_code
            WHERE d.same_country = true
            ORDER BY si_from."Country", d.from_site_code, d.to_site_code
        """)).fetchall()

        # Collect per-country site code lists (ordered) and build a nested dict.
        # Structure: { country: { from_code: { to_code: row_dict } } }
        countries = {}  # ordered by first appearance
        for row in rows:
            country = row.country
            if country not in countries:
                countries[country] = {}
            if row.from_site_code not in countries[country]:
                countries[country][row.from_site_code] = {}
            countries[country][row.from_site_code][row.to_site_code] = {
                'distance_km': str(row.distance_km),
                'notes': row.notes or '',
                'updated_at': row.updated_at.strftime('%Y-%m-%d %H:%M') if row.updated_at else '',
                'updated_by': row.updated_by or '',
            }

        # For each country, build an ordered list of site codes (union of from + to).
        country_site_codes = {}
        for country, from_map in countries.items():
            codes = set()
            for from_code, to_map in from_map.items():
                codes.add(from_code)
                codes.update(to_map.keys())
            country_site_codes[country] = sorted(codes)

    finally:
        db.close()

    return render_template(
        'admin/site_distance.html',
        countries=countries,
        country_site_codes=country_site_codes,
    )


@admin_bp.route('/site-distance/save', methods=['POST'])
@login_required
@config_required
def site_distance_save():
    """Bulk update mw_site_distance rows submitted from the editor grid."""
    from sqlalchemy import text
    from decimal import Decimal, InvalidOperation

    db = _get_middleware_session()
    try:
        # Parse all distance_km_<from>_<to> fields from the form.
        updates = []
        errors = []
        for key, val in request.form.items():
            if not key.startswith('distance_km_'):
                continue
            # key format: distance_km_<from_site_code>_<to_site_code>
            # Site codes like L017, MY001 contain no underscore; safe to split on first two underscores.
            suffix = key[len('distance_km_'):]
            # suffix is "<from>_<to>" — site codes may contain digits/letters only, no underscores
            parts = suffix.split('_', 1)
            if len(parts) != 2:
                continue
            from_code, to_code = parts[0], parts[1]
            notes_key = f'notes_{from_code}_{to_code}'
            notes_val = request.form.get(notes_key, '').strip()[:500]

            val = val.strip()
            if not val:
                continue
            try:
                km = Decimal(val)
            except InvalidOperation:
                errors.append(f"Invalid distance value for {from_code} to {to_code}: '{val}'")
                continue
            if km < 0 or km > 9999.99:
                errors.append(f"Distance for {from_code} to {to_code} must be between 0 and 9999.99.")
                continue
            updates.append({
                'from_code': from_code,
                'to_code': to_code,
                'km': float(km),
                'notes': notes_val or None,
            })

        if errors:
            for err in errors:
                flash(err, 'error')
            return redirect(url_for('admin.site_distance'))

        if not updates:
            flash('No distances submitted.', 'error')
            return redirect(url_for('admin.site_distance'))

        # Fetch current DB values in one query for change-detection.
        from_codes = list({u['from_code'] for u in updates})
        current_rows = db.execute(text("""
            SELECT from_site_code, to_site_code, distance_km, notes
            FROM mw_site_distance
            WHERE from_site_code = ANY(:froms)
        """), {'froms': from_codes}).fetchall()

        current_map = {
            (r.from_site_code, r.to_site_code): {'km': float(r.distance_km), 'notes': r.notes}
            for r in current_rows
        }

        changed = []
        for u in updates:
            cur = current_map.get((u['from_code'], u['to_code']))
            if cur is None:
                continue  # row not found — skip silently (no orphan inserts)
            km_changed = round(cur['km'], 2) != round(u['km'], 2)
            notes_changed = (cur['notes'] or '') != (u['notes'] or '')
            if km_changed or notes_changed:
                changed.append(u)

        if not changed:
            flash('No changes detected — nothing saved.', 'info')
            return redirect(url_for('admin.site_distance'))

        username = current_user.username
        for u in changed:
            db.execute(text("""
                UPDATE mw_site_distance
                SET distance_km = :km,
                    notes       = :notes,
                    updated_at  = now(),
                    updated_by  = :user
                WHERE from_site_code = :from_code
                  AND to_site_code   = :to_code
            """), {
                'km': u['km'],
                'notes': u['notes'],
                'user': username,
                'from_code': u['from_code'],
                'to_code': u['to_code'],
            })

        db.commit()
        n = len(changed)
        audit_log(AuditEvent.CONFIG_UPDATED, f"Updated site distances: {n} pair(s)")
        flash(f'{n} distance pair(s) saved successfully.', 'success')
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"Error saving site distances: {e}")
        flash('An error occurred while saving. Please try again.', 'error')
    finally:
        db.close()

    return redirect(url_for('admin.site_distance'))
