"""
Authentication routes - login, logout, OAuth callbacks.
"""

import os
import bcrypt
from flask import Blueprint, render_template, redirect, url_for, request, flash, session, current_app
from flask_login import login_user, logout_user, login_required, current_user

auth_bp = Blueprint('auth', __name__)


def get_session():
    """Get database session from app context."""
    return current_app.get_db_session()


@auth_bp.route('/login', methods=['GET', 'POST'])
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

            if user and user.password:
                # Check password with bcrypt
                # Convert PHP bcrypt $2y$ to Python bcrypt $2b$ for compatibility
                stored_hash = user.password
                if stored_hash.startswith('$2y$'):
                    stored_hash = '$2b$' + stored_hash[4:]
                if bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8')):
                    login_user(user)
                    next_page = request.args.get('next')
                    return redirect(next_page or url_for('main.dashboard'))

            flash('Invalid username or password.', 'error')
        finally:
            db_session.close()

    return render_template('login.html')


@auth_bp.route('/login/microsoft')
def microsoft_login():
    """Initiate Microsoft OAuth login."""
    from web.auth.oauth import oauth

    redirect_uri = os.getenv('MS_OAUTH_REDIRECT_URI') or url_for('auth.oauth_callback', _external=True)
    return oauth.microsoft.authorize_redirect(redirect_uri)


@auth_bp.route('/oauth_callback')
def oauth_callback():
    """Handle Microsoft OAuth callback."""
    from web.auth.oauth import oauth
    from web.models.user import User

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

    db_session = get_session()
    try:
        # Find or create user
        user = db_session.query(User).filter_by(email=email).first()

        if not user:
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
                role='viewer',
                auth_provider='microsoft'
            )
            db_session.add(user)
            db_session.commit()

        login_user(user)
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
    logout_user()
    flash('You have been logged out.', 'info')
    return redirect(url_for('auth.login'))
