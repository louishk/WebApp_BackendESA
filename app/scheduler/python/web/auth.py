"""
JWT Authentication Middleware for Scheduler Web API.
Validates tokens from PHP backend for API access.
"""

import os
import jwt
from functools import wraps
from flask import request, jsonify, g
from decouple import config


# JWT Configuration
JWT_SECRET = config('JWT_SECRET', default='')
JWT_ALGORITHM = config('JWT_ALGORITHM', default='HS256')

# Allowed roles for scheduler access
SCHEDULER_ROLES = ['admin', 'scheduler_admin']


class AuthError(Exception):
    """Authentication error with status code."""
    def __init__(self, message, status_code=401):
        self.message = message
        self.status_code = status_code


def get_token_from_header():
    """
    Extract JWT token from Authorization header.

    Returns:
        str: Token string or None
    """
    auth_header = request.headers.get('Authorization', '')

    if auth_header.startswith('Bearer '):
        return auth_header[7:]

    # Also check query parameter as fallback
    return request.args.get('token')


def decode_token(token):
    """
    Decode and validate JWT token.

    Args:
        token: JWT token string

    Returns:
        dict: Decoded payload

    Raises:
        AuthError: If token is invalid
    """
    if not JWT_SECRET:
        raise AuthError('JWT secret not configured', 500)

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise AuthError('Token has expired')
    except jwt.InvalidTokenError as e:
        raise AuthError(f'Invalid token: {str(e)}')


def require_auth(f):
    """
    Decorator to require JWT authentication for a route.
    Sets g.current_user with the decoded token payload.

    Usage:
        @app.route('/api/protected')
        @require_auth
        def protected_route():
            user = g.current_user
            ...
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        token = get_token_from_header()

        if not token:
            return jsonify({
                'error': 'Unauthorized',
                'message': 'Missing authentication token'
            }), 401

        try:
            payload = decode_token(token)

            # Check role
            user_role = payload.get('role', '')
            if user_role not in SCHEDULER_ROLES:
                return jsonify({
                    'error': 'Forbidden',
                    'message': f'Role "{user_role}" does not have scheduler access'
                }), 403

            # Store user info in Flask's g object
            g.current_user = payload

        except AuthError as e:
            return jsonify({
                'error': 'Authentication failed',
                'message': e.message
            }), e.status_code

        return f(*args, **kwargs)

    return decorated


def require_role(allowed_roles):
    """
    Decorator factory to require specific roles.

    Usage:
        @app.route('/api/admin-only')
        @require_role(['admin'])
        def admin_route():
            ...
    """
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            token = get_token_from_header()

            if not token:
                return jsonify({
                    'error': 'Unauthorized',
                    'message': 'Missing authentication token'
                }), 401

            try:
                payload = decode_token(token)
                user_role = payload.get('role', '')

                if user_role not in allowed_roles:
                    return jsonify({
                        'error': 'Forbidden',
                        'message': f'This endpoint requires one of: {", ".join(allowed_roles)}'
                    }), 403

                g.current_user = payload

            except AuthError as e:
                return jsonify({
                    'error': 'Authentication failed',
                    'message': e.message
                }), e.status_code

            return f(*args, **kwargs)

        return decorated
    return decorator


def optional_auth(f):
    """
    Decorator for routes that work with or without authentication.
    If token is present and valid, sets g.current_user.
    If token is missing or invalid, g.current_user is None.

    Usage:
        @app.route('/api/public')
        @optional_auth
        def public_route():
            if g.current_user:
                # User is authenticated
            else:
                # Anonymous access
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        token = get_token_from_header()
        g.current_user = None

        if token:
            try:
                g.current_user = decode_token(token)
            except AuthError:
                pass  # Ignore auth errors for optional auth

        return f(*args, **kwargs)

    return decorated


def init_auth(app):
    """
    Initialize authentication for Flask app.
    Adds error handlers and before_request hooks.

    Args:
        app: Flask application
    """
    @app.errorhandler(AuthError)
    def handle_auth_error(error):
        return jsonify({
            'error': 'Authentication error',
            'message': error.message
        }), error.status_code

    # Log authentication info
    @app.before_request
    def log_auth_info():
        token = get_token_from_header()
        if token:
            try:
                payload = decode_token(token)
                app.logger.debug(f"Authenticated request from user {payload.get('sub')} ({payload.get('role')})")
            except AuthError:
                app.logger.debug("Request with invalid token")
