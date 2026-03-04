"""
JWT Authentication Middleware for Scheduler Web API.
Validates tokens from PHP backend for API access.
"""

import os
import jwt
from functools import wraps
from flask import request, jsonify, g


def _get_jwt_secret():
    """Get JWT secret from unified config system. Raises ValueError if not configured."""
    try:
        from common.config_loader import get_config
        config = get_config()
        secret = config.get_secret('JWT_SECRET')
        if secret:
            return secret
    except Exception:
        pass
    # Fallback to environment variable
    secret = os.environ.get('JWT_SECRET')
    if secret:
        return secret
    raise ValueError(
        'JWT_SECRET is not configured. Set it in vault or JWT_SECRET environment variable.'
    )


def _get_jwt_algorithm():
    """Get JWT algorithm from config."""
    try:
        from common.config_loader import get_config
        config = get_config()
        if config.app.jwt and config.app.jwt.algorithm:
            return config.app.jwt.algorithm
        return 'HS256'
    except Exception:
        return 'HS256'


# JWT Configuration (lazy loaded)
JWT_SECRET = None
JWT_ALGORITHM = None


def _ensure_jwt_config():
    """Ensure JWT config is loaded."""
    global JWT_SECRET, JWT_ALGORITHM
    if JWT_SECRET is None:
        JWT_SECRET = _get_jwt_secret()
        JWT_ALGORITHM = _get_jwt_algorithm()

# Allowed roles for API access (scheduler and tools)
API_ACCESS_ROLES = ['admin', 'scheduler_admin']


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

    return None


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
    _ensure_jwt_config()

    if not JWT_SECRET:
        raise AuthError('JWT secret not configured', 500)

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise AuthError('Token has expired')
    except jwt.InvalidTokenError as e:
        raise AuthError(f'Invalid token: {str(e)}')


def _authenticate_api_key():
    """
    Authenticate via API key (X-API-Key header).
    Key format: esa_<key_id>.<secret>

    Returns dict of user info if valid, else None.
    Also sets g.api_key_scopes for scope checking and
    g.api_key_rate_limit for rate limiting.

    Returns (dict, error_tuple) — if error_tuple is not None,
    return it as the response (quota/rate exceeded).
    """
    from flask import current_app

    api_key_header = request.headers.get('X-API-Key', '')
    if not api_key_header or not api_key_header.startswith('esa_'):
        return None, None

    try:
        without_prefix = api_key_header[4:]  # strip "esa_"
        key_id, raw_secret = without_prefix.split('.', 1)
    except ValueError:
        return None, None

    try:
        from web.models.api_key import ApiKey
        from sqlalchemy import text
        session = current_app.get_db_session()
        try:
            api_key = session.query(ApiKey).filter_by(key_id=key_id).first()
            if not api_key or not api_key.is_valid() or not api_key.verify_secret(raw_secret):
                return None, None

            # Atomic quota check + increment + date-reset + last_used update
            result = session.execute(
                text("""
                    UPDATE api_keys
                    SET daily_usage = CASE WHEN quota_reset_date != CURRENT_DATE
                                          THEN 1
                                          ELSE daily_usage + 1 END,
                        quota_reset_date = CURRENT_DATE,
                        last_used_at = NOW()
                    WHERE id = :id AND is_active = true
                      AND (daily_quota = 0 OR
                           CASE WHEN quota_reset_date != CURRENT_DATE THEN 0 ELSE daily_usage END < daily_quota)
                    RETURNING daily_usage, daily_quota
                """),
                {"id": api_key.id}
            )
            row = result.fetchone()
            session.commit()

            if not row:
                # Quota exceeded (UPDATE matched no rows)
                return None, (jsonify({
                    'error': 'Quota exceeded',
                    'message': f'Daily API quota of {api_key.daily_quota} requests exceeded. Resets at midnight.',
                    'daily_quota': api_key.daily_quota,
                }), 429)

            new_usage, daily_quota = row
            remaining = (daily_quota - new_usage) if daily_quota > 0 else -1

            g.api_key_scopes = api_key.scopes or []
            g.api_key_rate_limit = api_key.rate_limit

            user_info = {
                'sub': api_key.user.username if api_key.user else f'key:{key_id}',
                'user_id': api_key.user_id,
                'roles': [r.name for r in api_key.user.roles] if api_key.user else [],
                'role': api_key.user.roles[0].name if api_key.user and api_key.user.roles else 'api_key',
                'auth_method': 'api_key',
                'key_id': key_id,
            }

            # Add quota info to response headers later
            g.api_key_quota_remaining = remaining
            g.api_key_daily_quota = daily_quota

            return user_info, None
        finally:
            session.close()
    except Exception:
        return None, None


def require_auth(f):
    """
    Decorator to require authentication for a route.
    Accepts API keys (X-API-Key), JWT tokens (Bearer), and session auth.
    Sets g.current_user with the user info.

    Usage:
        @app.route('/api/protected')
        @require_auth
        def protected_route():
            user = g.current_user
            ...
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        from flask_login import current_user

        # 1. Session-based authentication (web UI)
        # Session users only need to be authenticated; route-level decorators handle RBAC.
        if current_user and current_user.is_authenticated:
            g.current_user = {
                'sub': current_user.username,
                'roles': [r.name for r in current_user.roles],
                'role': current_user.roles[0].name if current_user.roles else 'unknown',
                'user_id': current_user.id,
                'auth_method': 'session',
            }
            g.api_key_scopes = None  # session users bypass scope checks
            return f(*args, **kwargs)

        # 2. API key authentication (X-API-Key header)
        api_key_user, api_key_error = _authenticate_api_key()
        if api_key_error:
            return api_key_error  # quota exceeded
        if api_key_user:
            g.current_user = api_key_user

            # Enforce per-key rate limit (uses the global rate limiter)
            rate = getattr(g, 'api_key_rate_limit', 0)
            if rate and rate > 0:
                from web.utils.rate_limit import api_limiter, get_client_ip
                ip = get_client_ip()
                rl_key = f"apikey:{api_key_user.get('key_id')}:{ip}"
                is_limited, retry_after = api_limiter.is_rate_limited(rl_key, rate, 60)
                if is_limited:
                    return jsonify({
                        'error': 'Rate limit exceeded',
                        'message': f'API key rate limit: {rate} req/min. Retry after {retry_after}s.',
                        'retry_after': retry_after,
                    }), 429
                api_limiter.record_attempt(rl_key)

            # Call the endpoint, then add quota headers to response
            response = f(*args, **kwargs)

            # Add quota info headers if available
            quota_remaining = getattr(g, 'api_key_quota_remaining', None)
            daily_quota = getattr(g, 'api_key_daily_quota', None)
            if quota_remaining is not None and daily_quota:
                # Handle both Response objects and tuples
                if hasattr(response, 'headers'):
                    response.headers['X-RateLimit-Limit'] = str(rate or 'unlimited')
                    response.headers['X-Quota-Limit'] = str(daily_quota)
                    response.headers['X-Quota-Remaining'] = str(max(0, quota_remaining))
            return response

        # 3. JWT authentication (Bearer token)
        token = get_token_from_header()

        if not token:
            return jsonify({
                'error': 'Unauthorized',
                'message': 'Missing authentication. Use X-API-Key, Bearer JWT, or session cookie.'
            }), 401

        try:
            payload = decode_token(token)

            user_roles = payload.get('roles', [])
            if not user_roles:
                single_role = payload.get('role', '')
                user_roles = [single_role] if single_role else []
            if not any(r in API_ACCESS_ROLES for r in user_roles):
                return jsonify({
                    'error': 'Forbidden',
                    'message': f'Role(s) "{", ".join(user_roles)}" do not have API access'
                }), 403

            payload['auth_method'] = 'jwt'
            g.current_user = payload
            g.api_key_scopes = None  # JWT users bypass scope checks

        except AuthError as e:
            return jsonify({
                'error': 'Authentication failed',
                'message': e.message
            }), e.status_code

        return f(*args, **kwargs)

    return decorated


def require_api_scope(scope):
    """
    Decorator to enforce a specific API scope on an endpoint.

    - Session and JWT users: always pass (they use RBAC roles instead).
    - API key users: must have the scope in their key's scopes list.

    Usage:
        @app.route('/api/discount-plans')
        @require_auth
        @require_api_scope('discount_plans:read')
        def list_plans():
            ...
    """
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            # g.api_key_scopes is None for session/JWT (bypass)
            # or a list of scopes for API key users
            scopes = getattr(g, 'api_key_scopes', None)
            if scopes is not None:
                if scope not in scopes:
                    return jsonify({
                        'error': 'Forbidden',
                        'message': f'API key missing required scope: {scope}'
                    }), 403
            return f(*args, **kwargs)
        return decorated
    return decorator


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
