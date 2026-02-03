"""
Flask Web Application for ESA Backend.
Provides REST API, scheduler dashboard, user/page management.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from flask import Flask, g, request, jsonify
from flask_cors import CORS
from flask_login import LoginManager
from flask_wtf.csrf import CSRFProtect
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

from common.config_loader import get_config, get_database_url, get_flask_config


def create_app(config=None, db_url=None):
    """
    Create Flask application with all blueprints registered.

    Args:
        config: SchedulerConfig instance (optional)
        db_url: Database URL (optional, will use config loader if not provided)

    Returns:
        Flask application
    """
    app = Flask(__name__)

    # Load Flask configuration from unified config
    flask_config = get_flask_config()
    app.config.update(flask_config)

    # Ensure secure session cookie defaults
    # HTTPONLY should always be True to prevent XSS access to session
    app.config.setdefault('SESSION_COOKIE_HTTPONLY', True)
    # SAMESITE should be at least 'Lax' to prevent CSRF
    if app.config.get('SESSION_COOKIE_SAMESITE') not in ('Lax', 'Strict'):
        app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    # SECURE should be True in production (when not debug)
    if not app.config.get('DEBUG', False):
        app.config['SESSION_COOKIE_SECURE'] = True

    # Store scheduler config
    app.scheduler_config = config

    # Store app config for access by blueprints
    app.app_config = get_config()

    # Build database URL from unified config
    if not db_url:
        db_url = get_database_url('backend')

    app.db_url = db_url

    # Database session factory
    _db_engine = None
    _session_factory = None

    def get_db_session():
        nonlocal _db_engine, _session_factory
        if _db_engine is None:
            _db_engine = create_engine(app.db_url)
            _session_factory = sessionmaker(bind=_db_engine)
        return _session_factory()

    app.get_db_session = get_db_session

    # Initialize CORS
    CORS(app, supports_credentials=True)

    # Initialize Flask-Login
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Please log in to access this page.'
    login_manager.login_message_category = 'warning'

    # Initialize CSRF protection
    csrf = CSRFProtect()
    csrf.init_app(app)

    # Store csrf on app for blueprint exemptions
    app.csrf = csrf

    @login_manager.user_loader
    def load_user(user_id):
        from web.models.user import User
        from sqlalchemy.orm import joinedload
        session = get_db_session()
        try:
            # Eagerly load the role relationship to avoid DetachedInstanceError
            user = session.query(User).options(joinedload(User.role)).get(int(user_id))
            if user:
                # Expunge from session so it can be used after session closes
                session.expunge(user)
            return user
        finally:
            session.close()

    # Initialize JWT auth for API routes
    from web.auth.jwt_auth import init_auth
    init_auth(app)

    # Initialize Microsoft OAuth
    from web.auth.oauth import init_oauth
    init_oauth(app)

    # Track web UI start time
    app.web_started_at = datetime.now()

    # Initialize audit logging
    from web.utils.audit import setup_audit_logging
    setup_audit_logging(app)

    # Add security headers and prevent caching of API responses
    @app.after_request
    def add_security_headers(response):
        # Cache control for API endpoints
        if '/api/' in request.path:
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'

        # Security headers for all responses
        response.headers['X-Content-Type-Options'] = 'nosniff'
        # X-Frame-Options removed; CSP frame-ancestors handles this
        # and supports multiple origins (X-Frame-Options ALLOW-FROM is deprecated)
        response.headers['X-XSS-Protection'] = '1; mode=block'
        response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
        response.headers['Permissions-Policy'] = 'geolocation=(), microphone=(), camera=()'

        # Content Security Policy
        # Note: 'unsafe-inline' needed for inline styles in templates
        # Consider moving to external CSS files for stricter CSP
        csp_directives = [
            "default-src 'self'",
            "script-src 'self' https://cdn.jsdelivr.net",  # For select2
            "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net",  # For select2 and inline styles
            "img-src 'self' data:",
            "font-src 'self'",
            "connect-src 'self'",
            "frame-ancestors 'self' https://app.powerbi.com",
            "form-action 'self'",
            "base-uri 'self'"
        ]
        response.headers['Content-Security-Policy'] = '; '.join(csp_directives)

        # HSTS - only enable in production with HTTPS
        # Uncomment when deployed with HTTPS:
        # response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'

        return response

    # Make current_user available in templates
    @app.context_processor
    def inject_user():
        from flask_login import current_user
        return dict(current_user=current_user)

    # Register blueprints
    from web.routes.main import main_bp
    from web.routes.auth import auth_bp
    from web.routes.admin import admin_bp
    from web.routes.scheduler import scheduler_bp
    from web.routes.api import api_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(scheduler_bp)
    app.register_blueprint(api_bp)

    # Exempt API routes from CSRF (they use JWT authentication)
    csrf.exempt(api_bp)

    # Backward compatibility: also mount health check at root
    @app.route('/health')
    def health():
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat()
        })

    return app


def run_app(host='0.0.0.0', port=5000, debug=False, db_url=None):
    """Run the Flask application."""
    # Initialize unified config (loads YAML + vault)
    app_config = get_config()

    from scheduler.config import SchedulerConfig
    config = SchedulerConfig.from_yaml()

    app = create_app(config, db_url)

    # Get server settings from config
    flask_settings = app_config.app.flask
    if flask_settings:
        host = flask_settings.host or host
        port = flask_settings.port or port
        debug = flask_settings.debug if flask_settings.debug is not None else debug

    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    run_app(debug=True)
