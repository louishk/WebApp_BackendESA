"""
Flask-Login session-based authentication.
"""

from flask_login import LoginManager

login_manager = LoginManager()
login_manager.login_view = 'auth.login'
login_manager.login_message = 'Please log in to access this page.'
login_manager.login_message_category = 'warning'


def init_login_manager(app, get_session):
    """
    Initialize Flask-Login for the app.

    Args:
        app: Flask application
        get_session: Function that returns a database session
    """
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        """Load user by ID for Flask-Login."""
        from web.models.user import User
        session = get_session()
        try:
            return session.query(User).get(int(user_id))
        finally:
            session.close()


def load_user(user_id, get_session):
    """
    Load user by ID.

    Args:
        user_id: User ID
        get_session: Function that returns a database session

    Returns:
        User instance or None
    """
    from web.models.user import User
    session = get_session()
    try:
        return session.query(User).get(int(user_id))
    finally:
        session.close()
