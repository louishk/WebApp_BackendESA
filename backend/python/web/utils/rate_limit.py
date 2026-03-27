"""
Rate limiting for authentication and API endpoints.

Primary store: Redis sorted-set sliding window (accurate across all Gunicorn workers).
Fallback: in-memory defaultdict(list) when Redis is unavailable.

The public API is unchanged — all existing callers work without modification.
"""

import logging
import time
from collections import defaultdict
from functools import wraps
from threading import Lock, Thread

from flask import request, jsonify, current_app

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Redis connection state (module-level, shared across RateLimiter instances)
# ---------------------------------------------------------------------------

_redis_client = None          # redis.Redis instance, or None
_redis_available = False      # True only when a PING succeeds
_redis_lock = Lock()          # guards _redis_client / _redis_available
_redis_log_warned = False     # emit the "falling back" warning only once
_RETRY_INTERVAL = 60          # seconds between reconnect attempts
_last_retry_at = 0.0


def _try_connect_redis(url: str) -> bool:
    """
    Attempt to create and PING a Redis connection.
    Returns True on success, False on any error.
    Updates module-level _redis_client and _redis_available.
    """
    global _redis_client, _redis_available, _redis_log_warned
    try:
        import redis as redis_lib
        client = redis_lib.Redis.from_url(
            url,
            socket_connect_timeout=2,
            socket_timeout=2,
            decode_responses=False,
        )
        client.ping()
        with _redis_lock:
            _redis_client = client
            _redis_available = True
            _redis_log_warned = False   # reset so we re-warn if it drops again
        logger.info("Rate limiter connected to Redis at %s", url)
        return True
    except Exception as exc:
        with _redis_lock:
            _redis_client = None
            _redis_available = False
        if not _redis_log_warned:
            logger.warning(
                "Redis unavailable for rate limiter (%s: %s) — falling back to in-memory",
                type(exc).__name__, exc,
            )
            # Don't set _redis_log_warned here; set it after first failed request
        return False


def _get_redis():
    """
    Return the Redis client if available, else None.
    Periodically retries the connection in the background.
    """
    global _redis_available, _last_retry_at, _redis_log_warned

    with _redis_lock:
        if _redis_available:
            return _redis_client

    # Not available — check whether it's time to retry
    now = time.time()
    if now - _last_retry_at >= _RETRY_INTERVAL and _rl_redis_url:
        _last_retry_at = now
        # Retry in a daemon thread so requests aren't blocked
        t = Thread(target=_try_connect_redis, args=(_rl_redis_url,), daemon=True)
        t.start()

    return None


# URL stored at init time so the retry loop can use it
_rl_redis_url: str = ""


def init_rate_limiter(app) -> None:
    """
    Read Redis config and attempt an initial connection.
    Call this once from create_app(), after config is loaded.
    Falls back to in-memory silently if Redis is disabled or unreachable.
    """
    global _rl_redis_url, _last_retry_at

    try:
        from common.config_loader import get_config
        cfg = get_config()
        redis_cfg = cfg.database.redis if cfg.database else None

        enabled = getattr(redis_cfg, 'enabled', False) if redis_cfg else False
        url = getattr(redis_cfg, 'url', 'redis://localhost:6379/0') if redis_cfg else 'redis://localhost:6379/0'

        if not enabled:
            logger.info("Rate limiter: Redis disabled in config — using in-memory store")
            return

        _rl_redis_url = url or 'redis://localhost:6379/0'
        _last_retry_at = time.time()
        _try_connect_redis(_rl_redis_url)

    except Exception as exc:
        logger.warning("Rate limiter init failed (%s) — using in-memory store", exc)


# ---------------------------------------------------------------------------
# RateLimiter class
# ---------------------------------------------------------------------------

class RateLimiter:
    """
    Sliding-window rate limiter with Redis primary store and in-memory fallback.

    Redis backend: sorted sets keyed as  rl:{key}
      - Score  = timestamp (float seconds)
      - Member = unique request token (timestamp + counter suffix)
    Accurate across all Gunicorn workers because operations are atomic.

    In-memory fallback: defaultdict(list) of timestamps, guarded by Lock.
    Per-process only — workers don't share state, so effective limit is
    max_attempts × num_workers in the worst case. Acceptable for degraded mode.
    """

    def __init__(self):
        self._attempts: defaultdict = defaultdict(list)
        self._lock = Lock()
        self._counters: defaultdict = defaultdict(int)  # tie-break for same-timestamp members

    # ------------------------------------------------------------------
    # Redis helpers
    # ------------------------------------------------------------------

    def _redis_key(self, key: str) -> str:
        return f"rl:{key}"

    def _redis_is_limited(self, key: str, max_attempts: int, window_seconds: int):
        """
        Sliding window check via Redis sorted set.
        Returns (is_limited: bool, retry_after: int | None).
        Raises on Redis error so the caller can fall back.
        """
        client = _get_redis()
        if client is None:
            raise RuntimeError("Redis not available")

        rk = self._redis_key(key)
        now = time.time()
        cutoff = now - window_seconds

        pipe = client.pipeline()
        # Remove expired entries
        pipe.zremrangebyscore(rk, '-inf', cutoff)
        # Count remaining
        pipe.zcard(rk)
        # Fetch oldest score (needed for retry_after calculation)
        pipe.zrange(rk, 0, 0, withscores=True)
        # Set TTL so keys clean themselves up
        pipe.expire(rk, window_seconds + 10)
        results = pipe.execute()

        count = results[1]
        if count >= max_attempts:
            oldest_entries = results[2]
            if oldest_entries:
                oldest_score = oldest_entries[0][1]
                retry_after = int((oldest_score + window_seconds) - now) + 1
            else:
                retry_after = 1
            return True, max(retry_after, 1)

        return False, None

    def _redis_record(self, key: str, window_seconds: int) -> None:
        """
        Record one attempt in the Redis sorted set.
        Raises on Redis error.
        """
        client = _get_redis()
        if client is None:
            raise RuntimeError("Redis not available")

        rk = self._redis_key(key)
        now = time.time()

        # Use counter suffix to keep members unique at the same timestamp
        with self._lock:
            self._counters[key] += 1
            suffix = self._counters[key]

        member = f"{now:.6f}:{suffix}"
        pipe = client.pipeline()
        pipe.zadd(rk, {member: now})
        pipe.expire(rk, window_seconds + 10)
        pipe.execute()

    def _redis_reset(self, key: str) -> None:
        """Delete the sorted set for key. Raises on Redis error."""
        client = _get_redis()
        if client is None:
            raise RuntimeError("Redis not available")
        client.delete(self._redis_key(key))

    # ------------------------------------------------------------------
    # In-memory helpers (existing logic, unchanged)
    # ------------------------------------------------------------------

    def _mem_cleanup(self, key: str, window_seconds: int) -> None:
        cutoff = time.time() - window_seconds
        self._attempts[key] = [t for t in self._attempts[key] if t > cutoff]

    def _mem_is_limited(self, key: str, max_attempts: int, window_seconds: int):
        with self._lock:
            self._mem_cleanup(key, window_seconds)
            attempts = len(self._attempts[key])
            if attempts >= max_attempts:
                oldest = min(self._attempts[key]) if self._attempts[key] else time.time()
                retry_after = int((oldest + window_seconds) - time.time()) + 1
                return True, max(retry_after, 1)
            return False, None

    def _mem_record(self, key: str) -> None:
        with self._lock:
            self._attempts[key].append(time.time())

    def _mem_reset(self, key: str) -> None:
        with self._lock:
            self._attempts.pop(key, None)

    # ------------------------------------------------------------------
    # Public API (same as before)
    # ------------------------------------------------------------------

    def is_rate_limited(self, key: str, max_attempts: int, window_seconds: int):
        """
        Check if a key has exceeded the rate limit.

        Returns:
            tuple: (is_limited: bool, retry_after_seconds: int | None)
        """
        global _redis_log_warned
        try:
            return self._redis_is_limited(key, max_attempts, window_seconds)
        except Exception:
            if not _redis_log_warned:
                logger.warning("Rate limiter Redis check failed — using in-memory fallback")
                _redis_log_warned = True
            return self._mem_is_limited(key, max_attempts, window_seconds)

    def record_attempt(self, key: str, window_seconds: int = 3600) -> None:
        """
        Record an attempt for a key.

        window_seconds is used only by the Redis backend to set TTL;
        the in-memory path ignores it (cleanup happens on next is_rate_limited call).
        """
        try:
            self._redis_record(key, window_seconds)
        except Exception:
            self._mem_record(key)

    def reset(self, key: str) -> None:
        """Reset attempts for a key (e.g. on successful login)."""
        try:
            self._redis_reset(key)
        except Exception:
            pass
        # Always clear in-memory too — both stores may have entries
        self._mem_reset(key)


# ---------------------------------------------------------------------------
# Global instances (unchanged names)
# ---------------------------------------------------------------------------

login_limiter = RateLimiter()
api_limiter = RateLimiter()


# ---------------------------------------------------------------------------
# Helper functions (public API, unchanged)
# ---------------------------------------------------------------------------

def get_client_ip() -> str:
    """Get client IP (ProxyFix middleware resolves the correct IP from trusted proxy)."""
    return request.remote_addr or 'unknown'


def rate_limit_login(max_attempts: int = 5, window_seconds: int = 300):
    """
    Decorator to rate limit login attempts.

    Limits by both IP address and username to prevent:
    - Brute force attacks from a single IP
    - Credential stuffing against a single user account
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if request.method != 'POST':
                return f(*args, **kwargs)

            ip = get_client_ip()
            username = request.form.get('username', '').strip().lower()

            # Check IP-based rate limit (2× headroom for shared IPs)
            ip_key = f"ip:{ip}"
            is_limited, retry_after = login_limiter.is_rate_limited(
                ip_key, max_attempts * 2, window_seconds
            )
            if is_limited:
                current_app.logger.warning("Rate limit exceeded for IP %s", ip)
                from flask import flash, render_template
                flash(
                    f'Too many login attempts. Please try again in {retry_after} seconds.',
                    'error',
                )
                return render_template('login.html'), 429

            # Check username-based rate limit (if username provided)
            if username:
                user_key = f"user:{username}"
                is_limited, retry_after = login_limiter.is_rate_limited(
                    user_key, max_attempts, window_seconds
                )
                if is_limited:
                    current_app.logger.warning(
                        "Rate limit exceeded for user '%s'", username
                    )
                    from flask import flash, render_template
                    flash(
                        f'Too many login attempts for this account. Please try again in {retry_after} seconds.',
                        'error',
                    )
                    return render_template('login.html'), 429

            return f(*args, **kwargs)
        return decorated_function
    return decorator


def rate_limit_api(max_requests: int = 30, window_seconds: int = 60):
    """
    Decorator to rate limit API endpoints by IP + endpoint.
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
                    "API rate limit exceeded for %s on %s", ip, request.endpoint
                )
                response = jsonify({
                    'error': 'Rate limit exceeded',
                    'retry_after': retry_after,
                })
                response.status_code = 429
                response.headers['Retry-After'] = str(retry_after)
                return response

            api_limiter.record_attempt(key, window_seconds)
            return f(*args, **kwargs)
        return decorated_function
    return decorator


def record_failed_login(username: str = None) -> None:
    """Record a failed login attempt (IP and optionally username)."""
    ip = get_client_ip()
    login_limiter.record_attempt(f"ip:{ip}", window_seconds=300)
    if username:
        login_limiter.record_attempt(f"user:{username.lower()}", window_seconds=300)


def reset_login_attempts(username: str = None) -> None:
    """Reset login attempts on successful login."""
    ip = get_client_ip()
    login_limiter.reset(f"ip:{ip}")
    if username:
        login_limiter.reset(f"user:{username.lower()}")
