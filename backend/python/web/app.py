"""
Flask Web Application for ESA Backend.
Provides REST API, scheduler dashboard, user/page management.
"""

import logging
import os
import sys
import uuid
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

    # Trust one layer of reverse proxy (nginx) for correct client IP and scheme
    from werkzeug.middleware.proxy_fix import ProxyFix
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)

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

    # Pipeline config is now loaded from DB per-request (see _get_scheduler_config in api.py)
    app.scheduler_config = None

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
            _db_engine = create_engine(
                app.db_url,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=300,
            )
            _session_factory = sessionmaker(bind=_db_engine)
        return _session_factory()

    app.get_db_session = get_db_session

    # Middleware database session factory (esa_middleware — discount plans,
    # reservation fees, and other middleware-tier tables)
    _mw_engine = None
    _mw_session_factory = None

    def get_middleware_session():
        nonlocal _mw_engine, _mw_session_factory
        if _mw_engine is None:
            _mw_engine = create_engine(
                get_database_url('middleware'),
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=300,
            )
            _mw_session_factory = sessionmaker(bind=_mw_engine)
        return _mw_session_factory()

    app.get_middleware_session = get_middleware_session

    # Initialize CORS with restricted origins
    cors_origins = app.config.get('CORS_ORIGINS', [
        'https://esa-backend.extraspaceasia.com',
    ])
    CORS(app, supports_credentials=True, origins=cors_origins)

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
            # Eagerly load the roles relationship to avoid DetachedInstanceError
            user = session.query(User).options(joinedload(User.roles)).get(int(user_id))
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

    # Initialize rate limiter (Redis if available, in-memory fallback)
    from web.utils.rate_limit import init_rate_limiter
    init_rate_limiter(app)

    # Initialize audit logging
    from web.utils.audit import setup_audit_logging
    setup_audit_logging(app)

    # Initialize API statistics tracking
    from web.utils.api_stats import init_api_stats
    init_api_stats(app)

    # Initialize outbound (external) API statistics tracking
    from common.outbound_stats import init_outbound_stats
    init_outbound_stats(app)

    # -----------------------------------------------------------------------
    # Request correlation IDs
    # -----------------------------------------------------------------------
    class _RequestIDFilter(logging.Filter):
        """Inject g.request_id into every log record emitted during a request."""
        def filter(self, record):
            try:
                record.request_id = g.request_id
            except RuntimeError:
                # Outside of a request context
                record.request_id = '-'
            return True

    _request_id_filter = _RequestIDFilter()

    # Attach filter once at startup so all log records get request_id
    _root_logger = logging.getLogger()
    if _request_id_filter not in _root_logger.filters:
        _root_logger.addFilter(_request_id_filter)

    @app.before_request
    def assign_request_id():
        request_id = request.headers.get('X-Request-ID')
        if not request_id:
            request_id = str(uuid.uuid4())
        g.request_id = request_id

    # Add security headers and prevent caching of API responses
    @app.after_request
    def add_security_headers(response):
        # Echo the correlation ID back to the caller
        request_id = getattr(g, 'request_id', None)
        if request_id:
            response.headers['X-Request-ID'] = request_id

        # Cache control for API endpoints
        if '/api/' in request.path:
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'

        # Security headers for all responses
        response.headers['X-Robots-Tag'] = 'noindex, nofollow'
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
            "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdn.sheetjs.com",  # unsafe-inline needed for template scripts; sheetjs for Excel export
            "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net",  # For select2 and inline styles
            "img-src 'self' data:",
            "font-src 'self'",
            "connect-src 'self'",
            "frame-src 'self' https://app.powerbi.com",  # Allow embedding Power BI iframes
            "frame-ancestors 'self' https://app.powerbi.com",
            "form-action 'self'",
            "base-uri 'self'"
        ]
        response.headers['Content-Security-Policy'] = '; '.join(csp_directives)

        # HSTS - enforce HTTPS in production
        if not app.config.get('DEBUG', False):
            response.headers['Strict-Transport-Security'] = 'max-age=31536000'

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
    from web.routes.tools import tools_bp
    from web.routes.ecri import ecri_bp
    from web.routes.statistics import statistics_bp
    from web.routes.discount_plans import discount_plans_bp
    from web.routes.api_keys import api_keys_bp
    from web.routes.admin_siteinfo import admin_siteinfo_bp
    from web.routes.reservations import reservations_bp
    from web.routes.crm import crm_bp
    from web.routes.stripe_payments import stripe_bp
    from web.routes.visits import visits_bp
    from web.routes.revenue import revenue_bp
    from web.routes.tenants import tenants_bp
    from web.routes.billing import billing_bp
    from web.routes.units import units_bp
    from web.routes.reservation_fees import reservation_fees_bp, reservation_fees_api_bp
    from web.routes.orchestrator_ui import orchestrator_ui_bp
    from web.routes.recommendation_engine import recommendation_engine_bp
    from web.routes.recommendations import recommendations_bp
    from sync_service.api import sync_service_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(scheduler_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(tools_bp)
    app.register_blueprint(ecri_bp)
    app.register_blueprint(statistics_bp)
    app.register_blueprint(discount_plans_bp)
    app.register_blueprint(api_keys_bp)
    app.register_blueprint(admin_siteinfo_bp)
    app.register_blueprint(reservations_bp)
    app.register_blueprint(crm_bp)
    app.register_blueprint(stripe_bp)
    app.register_blueprint(visits_bp)
    app.register_blueprint(revenue_bp)
    app.register_blueprint(tenants_bp)
    app.register_blueprint(billing_bp)
    app.register_blueprint(units_bp)
    app.register_blueprint(reservation_fees_bp)
    app.register_blueprint(reservation_fees_api_bp)
    app.register_blueprint(orchestrator_ui_bp)
    app.register_blueprint(recommendation_engine_bp)
    app.register_blueprint(recommendations_bp)
    app.register_blueprint(sync_service_bp)

    # Exempt API routes from CSRF (they use JWT authentication, not session cookies)
    csrf.exempt(api_bp)
    csrf.exempt(reservations_bp)
    csrf.exempt(tenants_bp)
    csrf.exempt(reservation_fees_api_bp)
    csrf.exempt(billing_bp)
    csrf.exempt(sync_service_bp)
    csrf.exempt(recommendations_bp)
    # stripe_bp uses Stripe signature verification on webhook; other routes use JWT
    csrf.exempt(stripe_bp)
    # crm_bp and visits_bp use session auth — CSRF protection stays enabled
    # (the frontend sends X-CSRFToken header via apiHeaders())

    # Backward compatibility: also mount health check at root (unauthenticated — used by load balancers)
    @app.route('/health')
    def health():
        from web.utils.rate_limit import api_limiter, get_client_ip
        ip = get_client_ip()
        is_limited, retry_after = api_limiter.is_rate_limited(f"api:{ip}:health", 30, 60)
        if is_limited:
            resp = jsonify({'error': 'Rate limit exceeded', 'retry_after': retry_after})
            resp.status_code = 429
            resp.headers['Retry-After'] = str(retry_after)
            return resp
        api_limiter.record_attempt(f"api:{ip}:health", 60)
        from web.utils.health import run_health_checks
        body, status = run_health_checks()
        return jsonify(body), status

    # Global error handlers — prevent stack trace leaks
    _error_logger = logging.getLogger(__name__)

    @app.errorhandler(404)
    def not_found(e):
        if request.path.startswith('/api/'):
            return jsonify({'error': 'Not found'}), 404
        from flask import render_template
        try:
            return render_template('errors/404.html'), 404
        except Exception:
            return jsonify({'error': 'Not found'}), 404

    @app.errorhandler(405)
    def method_not_allowed(e):
        return jsonify({'error': 'Method not allowed'}), 405

    @app.errorhandler(500)
    def internal_error(e):
        _error_logger.error(f"Internal server error: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

    @app.errorhandler(Exception)
    def unhandled_exception(e):
        _error_logger.error(f"Unhandled exception: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

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
