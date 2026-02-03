"""
Simple rate limiting for authentication and API endpoints.

Provides in-memory rate limiting to prevent brute force and abuse.
Can be upgraded to Redis-based limiting in production.
"""

import time
from collections import defaultdict
from functools import wraps
from flask import request, jsonify, current_app
from threading import Lock


class RateLimiter:
    """
    Simple in-memory rate limiter.

    Tracks request counts per key (IP or username) within time windows.
    Thread-safe for multi-threaded Flask applications.
    """

    def __init__(self):
        self._attempts = defaultdict(list)
        self._lock = Lock()

    def _cleanup_old_attempts(self, key, window_seconds):
        """Remove attempts older than the window."""
        cutoff = time.time() - window_seconds
        self._attempts[key] = [t for t in self._attempts[key] if t > cutoff]

    def is_rate_limited(self, key, max_attempts, window_seconds):
        """
        Check if a key has exceeded the rate limit.

        Args:
            key: Identifier (e.g., IP address, username)
            max_attempts: Maximum allowed attempts in the window
            window_seconds: Time window in seconds

        Returns:
            tuple: (is_limited: bool, retry_after_seconds: int or None)
        """
        with self._lock:
            self._cleanup_old_attempts(key, window_seconds)
            attempts = len(self._attempts[key])

            if attempts >= max_attempts:
                # Calculate when the oldest attempt will expire
                oldest = min(self._attempts[key]) if self._attempts[key] else time.time()
                retry_after = int((oldest + window_seconds) - time.time()) + 1
                return True, max(retry_after, 1)

            return False, None

    def record_attempt(self, key):
        """Record an attempt for a key."""
        with self._lock:
            self._attempts[key].append(time.time())

    def reset(self, key):
        """Reset attempts for a key (e.g., on successful login)."""
        with self._lock:
            if key in self._attempts:
                del self._attempts[key]


# Global rate limiter instances
login_limiter = RateLimiter()
api_limiter = RateLimiter()


def get_client_ip():
    """Get client IP, handling proxies."""
    if request.headers.get('X-Forwarded-For'):
        return request.headers.get('X-Forwarded-For').split(',')[0].strip()
    return request.remote_addr or 'unknown'


def rate_limit_login(max_attempts=5, window_seconds=300):
    """
    Decorator to rate limit login attempts.

    Limits by both IP address and username to prevent:
    - Brute force attacks from single IP
    - Credential stuffing against single user

    Args:
        max_attempts: Max attempts per window (default 5)
        window_seconds: Window duration in seconds (default 300 = 5 minutes)
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if request.method != 'POST':
                return f(*args, **kwargs)

            ip = get_client_ip()
            username = request.form.get('username', '').strip().lower()

            # Check IP-based rate limit
            ip_key = f"ip:{ip}"
            is_limited, retry_after = login_limiter.is_rate_limited(
                ip_key, max_attempts * 2, window_seconds  # More lenient for IP
            )
            if is_limited:
                current_app.logger.warning(f"Rate limit exceeded for IP {ip}")
                from flask import flash
                flash(f'Too many login attempts. Please try again in {retry_after} seconds.', 'error')
                from flask import render_template
                return render_template('login.html'), 429

            # Check username-based rate limit (if username provided)
            if username:
                user_key = f"user:{username}"
                is_limited, retry_after = login_limiter.is_rate_limited(
                    user_key, max_attempts, window_seconds
                )
                if is_limited:
                    current_app.logger.warning(f"Rate limit exceeded for user '{username}'")
                    from flask import flash
                    flash(f'Too many login attempts for this account. Please try again in {retry_after} seconds.', 'error')
                    from flask import render_template
                    return render_template('login.html'), 429

            return f(*args, **kwargs)
        return decorated_function
    return decorator


def rate_limit_api(max_requests=30, window_seconds=60):
    """
    Decorator to rate limit API endpoints by IP + endpoint.

    Args:
        max_requests: Max requests per window (default 30)
        window_seconds: Window duration in seconds (default 60)
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            ip = get_client_ip()
            key = f"api:{ip}:{request.endpoint}"

            is_limited, retry_after = api_limiter.is_rate_limited(
                key, max_requests, window_seconds
            )
            if is_limited:
                current_app.logger.warning(
                    f"API rate limit exceeded for {ip} on {request.endpoint}"
                )
                response = jsonify({
                    'error': 'Rate limit exceeded',
                    'retry_after': retry_after
                })
                response.status_code = 429
                response.headers['Retry-After'] = str(retry_after)
                return response

            api_limiter.record_attempt(key)
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def record_failed_login(username=None):
    """Record a failed login attempt."""
    ip = get_client_ip()
    login_limiter.record_attempt(f"ip:{ip}")
    if username:
        login_limiter.record_attempt(f"user:{username.lower()}")


def reset_login_attempts(username=None):
    """Reset login attempts on successful login."""
    ip = get_client_ip()
    login_limiter.reset(f"ip:{ip}")
    if username:
        login_limiter.reset(f"user:{username.lower()}")
