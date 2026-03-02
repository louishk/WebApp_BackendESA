"""
API Key management routes — user-facing.
Each user can have one API key. Scopes are managed by admins under User Management.
Users can generate, view, and regenerate their key here.
"""

from flask import Blueprint, render_template, redirect, url_for, request, flash, current_app
from flask_login import login_required, current_user

from web.utils.audit import audit_log, AuditEvent

api_keys_bp = Blueprint('api_keys', __name__, url_prefix='/api-keys')


def get_session():
    return current_app.get_db_session()


@api_keys_bp.route('/')
@login_required
def my_key():
    """Show the current user's single API key (or prompt to generate one)."""
    from web.models.api_key import ApiKey, API_SCOPES

    db = get_session()
    try:
        api_key = db.query(ApiKey).filter_by(user_id=current_user.id).first()
        return render_template('api_keys/my_key.html',
                               api_key=api_key,
                               all_scopes=API_SCOPES)
    finally:
        db.close()


@api_keys_bp.route('/generate', methods=['POST'])
@login_required
def generate_key():
    """Generate a new API key for the current user (one per user)."""
    from web.models.api_key import ApiKey, API_SCOPES, generate_api_key, hash_api_secret

    db = get_session()
    try:
        # Check if the user already has a key
        existing = db.query(ApiKey).filter_by(user_id=current_user.id).first()
        if existing:
            flash('You already have an API key. Use "Regenerate" to create a new one.', 'error')
            return redirect(url_for('api_keys.my_key'))

        key_id, raw_secret, full_key = generate_api_key()

        api_key = ApiKey(
            user_id=current_user.id,
            name='Default',
            key_id=key_id,
            key_hash=hash_api_secret(raw_secret),
            scopes=[],  # Empty — admin will assign scopes
            is_active=True,
        )
        db.add(api_key)
        db.commit()

        audit_log(AuditEvent.CONFIG_UPDATED, f"Generated API key (key_id={key_id})")

        return render_template('api_keys/created.html',
                               full_key=full_key,
                               key_name='Default',
                               scopes=[])
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"Error generating API key: {e}")
        flash('An error occurred while generating the key.', 'error')
        return redirect(url_for('api_keys.my_key'))
    finally:
        db.close()


@api_keys_bp.route('/regenerate', methods=['POST'])
@login_required
def regenerate_key():
    """Delete the existing key and create a new one."""
    from web.models.api_key import ApiKey, generate_api_key, hash_api_secret

    db = get_session()
    try:
        existing = db.query(ApiKey).filter_by(user_id=current_user.id).first()

        # Preserve scopes and settings from old key
        old_scopes = existing.scopes if existing and existing.scopes else []
        old_rate_limit = existing.rate_limit if existing else 60
        old_daily_quota = existing.daily_quota if existing else 10000

        if existing:
            db.delete(existing)
            db.flush()

        key_id, raw_secret, full_key = generate_api_key()

        api_key = ApiKey(
            user_id=current_user.id,
            name='Default',
            key_id=key_id,
            key_hash=hash_api_secret(raw_secret),
            scopes=old_scopes,
            rate_limit=old_rate_limit,
            daily_quota=old_daily_quota,
            is_active=True,
        )
        db.add(api_key)
        db.commit()

        audit_log(AuditEvent.CONFIG_UPDATED, f"Regenerated API key (new key_id={key_id})")

        return render_template('api_keys/created.html',
                               full_key=full_key,
                               key_name='Default',
                               scopes=old_scopes)
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"Error regenerating API key: {e}")
        flash('An error occurred while regenerating the key.', 'error')
        return redirect(url_for('api_keys.my_key'))
    finally:
        db.close()
