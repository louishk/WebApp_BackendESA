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

    @login_manager.user_loader
    def load_user(user_id):
        from web.models.user import User
        session = get_db_session()
        try:
            return session.query(User).get(int(user_id))
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

    # Prevent caching of API responses
    @app.after_request
    def add_no_cache_headers(response):
        if '/api/' in request.path:
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'
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
