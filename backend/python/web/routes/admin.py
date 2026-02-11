"""
Admin routes - user, role, and page management.
"""

import bcrypt
from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify, current_app
from flask_login import login_required, current_user
from web.utils.audit import audit_log, AuditEvent
from web.utils.validators import validate_password

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
                can_manage_users=request.form.get('can_manage_users') == 'on',
                can_manage_pages=request.form.get('can_manage_pages') == 'on',
                can_manage_roles=request.form.get('can_manage_roles') == 'on',
                can_manage_configs=request.form.get('can_manage_configs') == 'on',
                can_access_ecri=request.form.get('can_access_ecri') == 'on',
                can_manage_ecri=request.form.get('can_manage_ecri') == 'on',
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
            role.can_manage_users = request.form.get('can_manage_users') == 'on'
            role.can_manage_pages = request.form.get('can_manage_pages') == 'on'
            role.can_manage_roles = request.form.get('can_manage_roles') == 'on'
            role.can_manage_configs = request.form.get('can_manage_configs') == 'on'
            role.can_access_ecri = request.form.get('can_access_ecri') == 'on'
            role.can_manage_ecri = request.form.get('can_manage_ecri') == 'on'

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
        'role': User.role_id,
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
        flash(f'Graph API error: {e}', 'error')
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
        flash(f'Database error during sync: {e}', 'error')
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
    """List all secrets in vault (keys only, not values)."""
    from common.config_loader import get_config

    config = get_config()

    if not config.vault_available:
        flash('Vault is not available.', 'error')
        return render_template('admin/secrets/list.html', secrets=[], vault_available=False)

    secrets = config.list_secrets()
    return render_template('admin/secrets/list.html', secrets=secrets, vault_available=True)


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

        if not key:
            flash('Secret key is required.', 'error')
            return render_template('admin/secrets/edit.html', secret=None)

        if config.set_secret(key, value):
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

    if request.method == 'POST':
        value = request.form.get('value', '')

        if config.set_secret(key, value):
            flash(f'Secret "{key}" updated successfully.', 'success')
            return redirect(url_for('admin.list_secrets'))
        else:
            flash('Failed to update secret.', 'error')

    # Don't show actual value for security
    current_value = config.get_secret(key)
    has_value = current_value is not None and current_value != ''

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

    if config.delete_secret(key):
        flash(f'Secret "{key}" deleted.', 'success')
    else:
        flash('Failed to delete secret.', 'error')

    return redirect(url_for('admin.list_secrets'))
