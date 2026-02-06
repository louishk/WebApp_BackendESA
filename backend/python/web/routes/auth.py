"""
Authentication routes - login, logout, OAuth callbacks.
"""

import secrets
import bcrypt
from flask import Blueprint, render_template, redirect, url_for, request, flash, session, current_app

# Pre-computed bcrypt hash for constant-time comparison when user not found
_DUMMY_BCRYPT_HASH = b'$2b$12$LJ3m4ys3Lg2VBe8jOObnzOqN0MR/XhMGHTLQEQ1ek5gNkb1M1FgC6'
from flask_login import login_user, logout_user, login_required, current_user
from web.utils.audit import audit_log, AuditEvent
from web.utils.rate_limit import rate_limit_login, record_failed_login, reset_login_attempts

auth_bp = Blueprint('auth', __name__)


def get_session():
    """Get database session from app context."""
    return current_app.get_db_session()


@auth_bp.route('/login', methods=['GET', 'POST'])
@rate_limit_login(max_attempts=5, window_seconds=300)
def login():
    """Local username/password login."""
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))

    if request.method == 'POST':
        from web.models.user import User

        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')

        if not username or not password:
            flash('Please enter username and password.', 'error')
            return render_template('login.html')

        db_session = get_session()
        try:
            user = db_session.query(User).filter_by(username=username).first()

            valid = False
            if user and user.password:
                # TECHNICAL DEBT: PHP bcrypt migration
                # Legacy passwords were hashed with PHP's password_hash() which uses
                # the $2y$ prefix. Python's bcrypt uses $2b$. Both are functionally
                # identical (same algorithm, same security). We convert the prefix at
                # runtime for compatibility. No security impact.
                # TODO: Rehash to $2b$ on successful login to eliminate this conversion
                #       once done, remove the prefix swap below.
                stored_hash = user.password
                if stored_hash.startswith('$2y$'):
                    stored_hash = '$2b$' + stored_hash[4:]
                valid = bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8'))
            else:
                # Dummy bcrypt check to prevent timing-based user enumeration
                bcrypt.checkpw(password.encode('utf-8'), _DUMMY_BCRYPT_HASH)

            if valid:
                # Opportunistic rehash: migrate legacy $2y$ hashes to $2b$ on login
                if user.password.startswith('$2y$'):
                    user.password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                    db_session.commit()

                login_user(user)
                reset_login_attempts(username)
                audit_log(AuditEvent.LOGIN_SUCCESS, f"Local login for user '{username}'", user=username)
                next_page = request.args.get('next', '')
                if not next_page or not next_page.startswith('/') or next_page.startswith('//'):
                    next_page = url_for('main.dashboard')
                return redirect(next_page)

            record_failed_login(username)
            audit_log(AuditEvent.LOGIN_FAILED, f"Failed login attempt for username '{username}'", user=username, level='WARNING')
            flash('Invalid username or password.', 'error')
        finally:
            db_session.close()

    return render_template('login.html')


@auth_bp.route('/login/microsoft')
def microsoft_login():
    """Initiate Microsoft OAuth login."""
    from web.auth.oauth import oauth
    from common.config_loader import get_config

    config = get_config()
    ms_config = config.oauth.microsoft if config.oauth else None

    # Use config redirect_uri, or generate from current request
    if ms_config and ms_config.redirect_uri:
        redirect_uri = ms_config.redirect_uri
    else:
        redirect_uri = url_for('auth.oauth_callback', _external=True)

    current_app.logger.info(f"OAuth redirect URI: {redirect_uri}")
    state = secrets.token_urlsafe(32)
    session['oauth_state'] = state
    return oauth.microsoft.authorize_redirect(redirect_uri, state=state)


@auth_bp.route('/oauth_callback')
@auth_bp.route('/oauth_callback.php')  # Compatibility with Azure AD config
def oauth_callback():
    """Handle Microsoft OAuth callback."""
    from web.auth.oauth import oauth
    from web.models.user import User
    from web.models.role import Role

    # Validate OAuth state parameter to prevent CSRF
    state = request.args.get('state')
    expected_state = session.pop('oauth_state', None)
    if not state or state != expected_state:
        current_app.logger.warning("OAuth callback with invalid state parameter")
        flash('Invalid authentication state. Please try again.', 'error')
        return redirect(url_for('auth.login'))

    try:
        token = oauth.microsoft.authorize_access_token()
    except Exception as e:
        current_app.logger.error(f"OAuth error: {e}")
        flash('Authentication failed. Please try again.', 'error')
        return redirect(url_for('auth.login'))

    # Get user info from Microsoft Graph
    user_info = oauth.microsoft.get('https://graph.microsoft.com/v1.0/me', token=token).json()

    email = user_info.get('mail') or user_info.get('userPrincipalName')
    display_name = user_info.get('displayName', '')

    if not email:
        flash('Could not retrieve email from Microsoft account.', 'error')
        return redirect(url_for('auth.login'))

    # Restrict OAuth to allowed email domains
    ALLOWED_DOMAINS = {'extraspaceasia.com'}
    email_domain = email.rsplit('@', 1)[-1].lower()
    if email_domain not in ALLOWED_DOMAINS:
        current_app.logger.warning(f"OAuth login rejected for unauthorized domain: {email}")
        audit_log(AuditEvent.LOGIN_FAILED, f"OAuth rejected: unauthorized domain '{email_domain}' for {email}", user=email, level='WARNING')
        flash('Access is restricted to authorized organization accounts.', 'error')
        return redirect(url_for('auth.login'))

    db_session = get_session()
    try:
        # Find or create user
        user = db_session.query(User).filter_by(email=email).first()

        if not user:
            # Get the default viewer role
            viewer_role = db_session.query(Role).filter_by(name='viewer').first()
            if not viewer_role:
                current_app.logger.error("Viewer role not found in database")
                flash('System configuration error. Please contact administrator.', 'error')
                return redirect(url_for('auth.login'))

            # Create new user with default viewer role
            username = email.split('@')[0]
            # Ensure unique username
            base_username = username
            counter = 1
            while db_session.query(User).filter_by(username=username).first():
                username = f"{base_username}{counter}"
                counter += 1

            user = User(
                username=username,
                email=email,
                role_id=viewer_role.id,
                auth_provider='microsoft'
            )
            db_session.add(user)
            db_session.commit()
            audit_log(AuditEvent.USER_CREATED, f"New OAuth user created: {username} ({email})", user=username)

        login_user(user)
        audit_log(AuditEvent.OAUTH_SUCCESS, f"OAuth login for user '{user.username}' ({email})", user=user.username)
        return redirect(url_for('main.dashboard'))

    except Exception as e:
        db_session.rollback()
        current_app.logger.error(f"Database error during OAuth: {e}")
        flash('An error occurred. Please try again.', 'error')
        return redirect(url_for('auth.login'))
    finally:
        db_session.close()


@auth_bp.route('/logout')
@login_required
def logout():
    """Log out the current user."""
    username = current_user.username
    logout_user()
    audit_log(AuditEvent.LOGOUT, f"User logged out", user=username)
    flash('You have been logged out.', 'info')
    return redirect(url_for('auth.login'))
