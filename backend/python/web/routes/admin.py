"""
Admin routes - user and page management.
"""

import bcrypt
from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify, current_app
from flask_login import login_required, current_user

admin_bp = Blueprint('admin', __name__, url_prefix='/admin')


def get_session():
    """Get database session from app context."""
    return current_app.get_db_session()


def admin_required(f):
    """Decorator to require admin role."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))
        if current_user.role != 'admin':
            flash('Admin access required.', 'error')
            return redirect(url_for('main.dashboard'))
        return f(*args, **kwargs)
    return decorated


def editor_required(f):
    """Decorator to require admin or editor role."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))
        if current_user.role not in ['admin', 'editor']:
            flash('Editor access required.', 'error')
            return redirect(url_for('main.dashboard'))
        return f(*args, **kwargs)
    return decorated


# =============================================================================
# User Management (Admin only)
# =============================================================================

@admin_bp.route('/users')
@login_required
@admin_required
def list_users():
    """List all users."""
    from web.models.user import User

    db_session = get_session()
    try:
        users = db_session.query(User).order_by(User.username).all()
        return render_template('admin/users/list.html', users=users)
    finally:
        db_session.close()


@admin_bp.route('/users/create', methods=['GET', 'POST'])
@login_required
@admin_required
def create_user():
    """Create a new user."""
    from web.models.user import User

    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        email = request.form.get('email', '').strip() or None
        password = request.form.get('password', '')
        role = request.form.get('role', 'viewer')

        if not username:
            flash('Username is required.', 'error')
            return render_template('admin/users/edit.html', user=None, roles=User.ROLES)

        if role not in User.ROLES:
            flash('Invalid role.', 'error')
            return render_template('admin/users/edit.html', user=None, roles=User.ROLES)

        db_session = get_session()
        try:
            # Check for duplicate username
            if db_session.query(User).filter_by(username=username).first():
                flash('Username already exists.', 'error')
                return render_template('admin/users/edit.html', user=None, roles=User.ROLES)

            # Hash password if provided
            hashed_password = None
            if password:
                hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

            user = User(
                username=username,
                email=email,
                password=hashed_password,
                role=role,
                auth_provider='local'
            )
            db_session.add(user)
            db_session.commit()

            flash(f'User "{username}" created successfully.', 'success')
            return redirect(url_for('admin.list_users'))
        except Exception as e:
            db_session.rollback()
            current_app.logger.error(f"Error creating user: {e}")
            flash('An error occurred.', 'error')
        finally:
            db_session.close()

    from web.models.user import User
    return render_template('admin/users/edit.html', user=None, roles=User.ROLES)


@admin_bp.route('/users/<int:user_id>/edit', methods=['GET', 'POST'])
@login_required
@admin_required
def edit_user(user_id):
    """Edit an existing user."""
    from web.models.user import User

    db_session = get_session()
    try:
        user = db_session.query(User).get(user_id)
        if not user:
            flash('User not found.', 'error')
            return redirect(url_for('admin.list_users'))

        if request.method == 'POST':
            user.email = request.form.get('email', '').strip() or None
            role = request.form.get('role', user.role)

            if role not in User.ROLES:
                flash('Invalid role.', 'error')
                return render_template('admin/users/edit.html', user=user, roles=User.ROLES)

            user.role = role

            # Update password if provided
            new_password = request.form.get('password', '')
            if new_password:
                user.password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

            db_session.commit()
            flash('User updated successfully.', 'success')
            return redirect(url_for('admin.list_users'))

        return render_template('admin/users/edit.html', user=user, roles=User.ROLES)
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
            db_session.delete(user)
            db_session.commit()
            flash(f'User "{user.username}" deleted.', 'success')
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
# Page Management (Admin and Editor)
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

    if request.method == 'POST':
        title = request.form.get('title', '').strip()
        slug = request.form.get('slug', '').strip()
        content = request.form.get('content', '')
        extension = request.form.get('extension', 'html')
        is_secure = request.form.get('is_secure') == 'on'
        edit_restricted = request.form.get('edit_restricted') == 'on'

        if not title or not slug:
            flash('Title and slug are required.', 'error')
            return render_template('admin/pages/edit.html', page=None, extensions=Page.ALLOWED_EXTENSIONS)

        if extension not in Page.ALLOWED_EXTENSIONS:
            flash('Invalid extension.', 'error')
            return render_template('admin/pages/edit.html', page=None, extensions=Page.ALLOWED_EXTENSIONS)

        db_session = get_session()
        try:
            # Check for duplicate slug
            if db_session.query(Page).filter_by(slug=slug).first():
                flash('Slug already exists.', 'error')
                return render_template('admin/pages/edit.html', page=None, extensions=Page.ALLOWED_EXTENSIONS)

            page = Page(
                title=title,
                slug=slug,
                content=content,
                extension=extension,
                is_secure=is_secure,
                edit_restricted=edit_restricted
            )
            db_session.add(page)
            db_session.commit()

            flash(f'Page "{title}" created successfully.', 'success')
            return redirect(url_for('admin.list_pages'))
        except Exception as e:
            db_session.rollback()
            current_app.logger.error(f"Error creating page: {e}")
            flash('An error occurred.', 'error')
        finally:
            db_session.close()

    from web.models.page import Page
    return render_template('admin/pages/edit.html', page=None, extensions=Page.ALLOWED_EXTENSIONS)


@admin_bp.route('/pages/<int:page_id>/edit', methods=['GET', 'POST'])
@login_required
@editor_required
def edit_page(page_id):
    """Edit an existing page."""
    from web.models.page import Page

    db_session = get_session()
    try:
        page = db_session.query(Page).get(page_id)
        if not page:
            flash('Page not found.', 'error')
            return redirect(url_for('admin.list_pages'))

        # Check edit restrictions
        if page.edit_restricted and current_user.role != 'admin':
            flash('This page can only be edited by admins.', 'error')
            return redirect(url_for('admin.list_pages'))

        if request.method == 'POST':
            page.title = request.form.get('title', '').strip()
            page.content = request.form.get('content', '')
            extension = request.form.get('extension', page.extension)
            page.is_secure = request.form.get('is_secure') == 'on'

            # Only admin can change edit_restricted
            if current_user.role == 'admin':
                page.edit_restricted = request.form.get('edit_restricted') == 'on'

            if extension in Page.ALLOWED_EXTENSIONS:
                page.extension = extension

            db_session.commit()
            flash('Page updated successfully.', 'success')
            return redirect(url_for('admin.list_pages'))

        return render_template('admin/pages/edit.html', page=page, extensions=Page.ALLOWED_EXTENSIONS)
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
            db_session.delete(page)
            db_session.commit()
            flash(f'Page "{page.title}" deleted.', 'success')
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
# Configuration Management (Admin only)
# =============================================================================

@admin_bp.route('/config')
@login_required
@admin_required
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
@admin_required
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
# Secrets Management (Admin only)
# =============================================================================

@admin_bp.route('/secrets')
@login_required
@admin_required
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
@admin_required
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
@admin_required
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
@admin_required
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
