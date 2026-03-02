"""
API Key management routes.
Users can generate, view, and revoke their own API keys.
"""

from flask import Blueprint, render_template, redirect, url_for, request, flash, jsonify, current_app
from flask_login import login_required, current_user

from web.utils.audit import audit_log, AuditEvent

api_keys_bp = Blueprint('api_keys', __name__, url_prefix='/api-keys')


def get_session():
    return current_app.get_db_session()


@api_keys_bp.route('/')
@login_required
def list_keys():
    """List the current user's API keys."""
    from web.models.api_key import ApiKey, API_SCOPES

    db = get_session()
    try:
        keys = (db.query(ApiKey)
                .filter_by(user_id=current_user.id)
                .order_by(ApiKey.created_at.desc())
                .all())
        return render_template('api_keys/list.html',
                               keys=keys,
                               all_scopes=API_SCOPES)
    finally:
        db.close()


@api_keys_bp.route('/create', methods=['POST'])
@login_required
def create_key():
    """Generate a new API key for the current user."""
    from web.models.api_key import ApiKey, API_SCOPES, generate_api_key, hash_api_secret

    name = request.form.get('key_name', '').strip()
    if not name:
        flash('Key name is required.', 'error')
        return redirect(url_for('api_keys.list_keys'))

    # Collect scopes from checkboxes
    scopes = request.form.getlist('scopes')
    # Validate scopes
    scopes = [s for s in scopes if s in API_SCOPES]
    if not scopes:
        flash('Select at least one API scope.', 'error')
        return redirect(url_for('api_keys.list_keys'))

    db = get_session()
    try:
        # Limit: max 5 active keys per user
        active_count = db.query(ApiKey).filter_by(user_id=current_user.id, is_active=True).count()
        if active_count >= 5:
            flash('Maximum 5 active API keys per user. Revoke an existing key first.', 'error')
            return redirect(url_for('api_keys.list_keys'))

        key_id, raw_secret, full_key = generate_api_key()

        api_key = ApiKey(
            user_id=current_user.id,
            name=name,
            key_id=key_id,
            key_hash=hash_api_secret(raw_secret),
            scopes=scopes,
            is_active=True,
        )
        db.add(api_key)
        db.commit()

        audit_log(AuditEvent.CONFIG_UPDATED, f"Created API key '{name}' (key_id={key_id})")

        # Show the key once — it can't be retrieved later
        flash(f'API key created. Copy it now — it will not be shown again:', 'success')
        return render_template('api_keys/created.html',
                               key_name=name,
                               full_key=full_key,
                               scopes=scopes,
                               all_scopes=API_SCOPES)
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"Error creating API key: {e}")
        flash('An error occurred while creating the key.', 'error')
        return redirect(url_for('api_keys.list_keys'))
    finally:
        db.close()


@api_keys_bp.route('/<int:key_id>/revoke', methods=['POST'])
@login_required
def revoke_key(key_id):
    """Revoke (deactivate) an API key."""
    from web.models.api_key import ApiKey

    db = get_session()
    try:
        api_key = db.query(ApiKey).filter_by(id=key_id, user_id=current_user.id).first()
        if not api_key:
            flash('API key not found.', 'error')
            return redirect(url_for('api_keys.list_keys'))

        api_key.is_active = False
        db.commit()

        audit_log(AuditEvent.CONFIG_UPDATED, f"Revoked API key '{api_key.name}' (key_id={api_key.key_id})")
        flash(f'API key "{api_key.name}" has been revoked.', 'success')
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"Error revoking API key: {e}")
        flash('An error occurred.', 'error')
    finally:
        db.close()

    return redirect(url_for('api_keys.list_keys'))


@api_keys_bp.route('/<int:key_id>/delete', methods=['POST'])
@login_required
def delete_key(key_id):
    """Permanently delete an API key."""
    from web.models.api_key import ApiKey

    db = get_session()
    try:
        api_key = db.query(ApiKey).filter_by(id=key_id, user_id=current_user.id).first()
        if not api_key:
            flash('API key not found.', 'error')
            return redirect(url_for('api_keys.list_keys'))

        name = api_key.name
        db.delete(api_key)
        db.commit()

        audit_log(AuditEvent.CONFIG_UPDATED, f"Deleted API key '{name}' (key_id={api_key.key_id})")
        flash(f'API key "{name}" has been deleted.', 'success')
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"Error deleting API key: {e}")
        flash('An error occurred.', 'error')
    finally:
        db.close()

    return redirect(url_for('api_keys.list_keys'))
