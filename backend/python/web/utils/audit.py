"""
Audit logging for security-relevant events.

Logs important actions like user management, authentication events,
and permission changes to a dedicated audit log.
"""

import logging
from datetime import datetime
from functools import wraps
from flask import request, current_app
from flask_login import current_user


# Configure audit logger
audit_logger = logging.getLogger('security.audit')


def setup_audit_logging(app):
    """
    Set up audit logging for the application.

    Creates a dedicated log file for security audit events.
    """
    import os
    from logging.handlers import RotatingFileHandler

    # Create logs directory if it doesn't exist
    log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # Set up rotating file handler (10MB max, keep 5 backups)
    log_file = os.path.join(log_dir, 'audit.log')
    handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    handler.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))

    audit_logger.addHandler(handler)
    audit_logger.setLevel(logging.INFO)

    # Also add to app logger if in debug mode
    if app.debug:
        audit_logger.addHandler(logging.StreamHandler())


def get_client_ip():
    """Get the real client IP, handling proxies."""
    if request.headers.get('X-Forwarded-For'):
        return request.headers.get('X-Forwarded-For').split(',')[0].strip()
    return request.remote_addr or 'unknown'


def get_current_username():
    """Get current username or 'anonymous'."""
    if current_user and current_user.is_authenticated:
        return current_user.username
    return 'anonymous'


def audit_log(event_type, details, user=None, level='INFO'):
    """
    Log a security audit event.

    Args:
        event_type: Type of event (e.g., 'LOGIN_SUCCESS', 'USER_CREATED')
        details: Description of what happened
        user: Username (defaults to current user)
        level: Log level ('INFO', 'WARNING', 'ERROR')
    """
    username = user or get_current_username()
    ip_address = get_client_ip()

    message = f"{event_type} | User: {username} | IP: {ip_address} | {details}"

    if level == 'WARNING':
        audit_logger.warning(message)
    elif level == 'ERROR':
        audit_logger.error(message)
    else:
        audit_logger.info(message)


# Common event types
class AuditEvent:
    """Audit event type constants."""
    # Authentication
    LOGIN_SUCCESS = 'LOGIN_SUCCESS'
    LOGIN_FAILED = 'LOGIN_FAILED'
    LOGOUT = 'LOGOUT'
    OAUTH_SUCCESS = 'OAUTH_SUCCESS'
    OAUTH_FAILED = 'OAUTH_FAILED'

    # User management
    USER_CREATED = 'USER_CREATED'
    USER_UPDATED = 'USER_UPDATED'
    USER_DELETED = 'USER_DELETED'
    USER_ROLE_CHANGED = 'USER_ROLE_CHANGED'

    # Role management
    ROLE_CREATED = 'ROLE_CREATED'
    ROLE_UPDATED = 'ROLE_UPDATED'
    ROLE_DELETED = 'ROLE_DELETED'

    # Page management
    PAGE_CREATED = 'PAGE_CREATED'
    PAGE_UPDATED = 'PAGE_UPDATED'
    PAGE_DELETED = 'PAGE_DELETED'

    # Access control
    ACCESS_DENIED = 'ACCESS_DENIED'
    PERMISSION_DENIED = 'PERMISSION_DENIED'

    # Configuration
    CONFIG_UPDATED = 'CONFIG_UPDATED'
    SECRET_CREATED = 'SECRET_CREATED'
    SECRET_UPDATED = 'SECRET_UPDATED'
    SECRET_DELETED = 'SECRET_DELETED'
