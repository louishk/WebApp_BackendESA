"""
MCP API Key Authentication Middleware

Validates X-API-Key headers against the ESA Backend's api_keys table.
Key format: esa_<key_id>.<secret> — same as the Flask API.
"""

import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional, Tuple

import bcrypt
import jwt
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from mcp_esa.config.settings import get_settings

logger = logging.getLogger(__name__)

# Lazy-initialized DB engine for api_key lookups
_engine = None
_SessionFactory = None

# Dummy bcrypt hash for constant-time comparison when key not found
_DUMMY_HASH = bcrypt.hashpw(b"dummy", bcrypt.gensalt()).decode("utf-8")

# In-memory rate limiting (per IP)
_rate_limit_window: dict = defaultdict(list)
_RATE_LIMIT_MAX = 60  # max requests per minute
_RATE_LIMIT_WINDOW = 60  # seconds


def _get_session():
    """Get a database session for the esa_backend DB."""
    global _engine, _SessionFactory
    if _engine is None:
        settings = get_settings()
        _engine = create_engine(
            settings.get_backend_db_url(),
            pool_size=2,
            max_overflow=3,
            pool_pre_ping=True,
        )
        _SessionFactory = sessionmaker(bind=_engine)
    return _SessionFactory()


class ApiKeyAuthMiddleware(BaseHTTPMiddleware):
    """
    Validates X-API-Key header against the backend's api_keys table.

    Public endpoints (/, /health) bypass auth.
    Protected endpoints require a valid, active, non-expired API key.
    """

    PUBLIC_PATHS = {
        "/", "/health",
        "/.well-known/oauth-authorization-server",
        "/.well-known/oauth-protected-resource",
        "/oauth/register",
        "/oauth/authorize",
        "/oauth/token",
    }

    def __init__(self, app):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Public endpoints (includes OAuth discovery/flow)
        if path in self.PUBLIC_PATHS:
            return await call_next(request)

        # CORS preflight
        if request.method == "OPTIONS":
            return await call_next(request)

        # Rate limiting by IP (with cleanup to prevent memory leak)
        client_ip = request.client.host if request.client else "unknown"
        now = time.time()
        hits = [t for t in _rate_limit_window.get(client_ip, []) if now - t < _RATE_LIMIT_WINDOW]
        if len(hits) >= _RATE_LIMIT_MAX:
            logger.warning(f"Rate limit exceeded for {client_ip}")
            return JSONResponse(
                {
                    "jsonrpc": "2.0",
                    "error": {"code": -32003, "message": "Rate limit exceeded"},
                    "id": None,
                },
                status_code=429,
            )
        hits.append(now)
        _rate_limit_window[client_ip] = hits

        # Periodic cleanup: evict IPs with no recent hits (every ~100 requests)
        if len(_rate_limit_window) > 100:
            stale_ips = [ip for ip, ts in _rate_limit_window.items() if not ts or now - ts[-1] > _RATE_LIMIT_WINDOW]
            for ip in stale_ips:
                del _rate_limit_window[ip]

        # Try X-API-Key first, then Bearer token (OAuth)
        api_key_header = request.headers.get("X-API-Key", "")
        token = self._extract_token(request)

        if api_key_header:
            user_info, error = _authenticate_api_key(api_key_header)
            if error:
                logger.warning(f"MCP auth failed from {client_ip}: {error}")
                status_code = 429 if error == "Daily quota exceeded" else 401
                public_msg = "Rate limit exceeded" if status_code == 429 else "Invalid or unauthorized API key"
                return JSONResponse(
                    {
                        "jsonrpc": "2.0",
                        "error": {"code": -32002, "message": public_msg},
                        "id": None,
                    },
                    status_code=status_code,
                )
            request.state.user = user_info["username"]
            request.state.user_id = user_info["user_id"]
            request.state.key_id = user_info["key_id"]
            request.state.scopes = user_info["scopes"]
            request.state.mcp_tools = user_info["mcp_tools"]
            logger.info(f"MCP auth OK: {user_info['username']} (key: {user_info['key_id']}) from {client_ip}")
            return await call_next(request)

        if token:
            user_info, error = _authenticate_bearer_token(token)
            if error:
                logger.warning(f"MCP OAuth auth failed from {client_ip}: {error}")
                return JSONResponse(
                    {
                        "jsonrpc": "2.0",
                        "error": {"code": -32002, "message": "Invalid or expired token"},
                        "id": None,
                    },
                    status_code=401,
                )
            request.state.user = user_info["username"]
            request.state.user_id = user_info["user_id"]
            request.state.key_id = user_info["key_id"]
            request.state.scopes = user_info["scopes"]
            request.state.mcp_tools = user_info["mcp_tools"]
            logger.info(f"MCP OAuth auth OK: {user_info['username']} from {client_ip}")
            return await call_next(request)

        # No credentials — return 401 with OAuth discovery per RFC 6750
        resource_metadata_url = self._get_resource_metadata_url(request)
        return JSONResponse(
            {
                "jsonrpc": "2.0",
                "error": {"code": -32001, "message": "Authentication required."},
                "id": None,
            },
            status_code=401,
            headers={
                "WWW-Authenticate": f'Bearer resource_metadata="{resource_metadata_url}"'
            },
        )

    @staticmethod
    def _extract_token(request: Request) -> Optional[str]:
        """Extract JWT token from Authorization header."""
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            return auth_header[7:]
        return None

    @staticmethod
    def _get_resource_metadata_url(request: Request) -> str:
        """Build the OAuth protected resource metadata URL from request headers."""
        scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
        host = request.headers.get("x-forwarded-host", request.headers.get("host", "localhost"))
        base_url = f"{scheme}://{host}"
        prefix = request.headers.get("x-forwarded-prefix", "")
        if prefix:
            base_url = f"{base_url}{prefix}"
        return f"{base_url}/.well-known/oauth-protected-resource"


def _authenticate_api_key(api_key_header: str) -> Tuple[Optional[dict], Optional[str]]:
    """
    Validate an API key against the esa_backend database.

    Returns:
        (user_info_dict, None) on success
        (None, error_message) on failure
    """
    # Parse key format: esa_<key_id>.<secret>
    if not api_key_header.startswith("esa_"):
        return None, "Invalid API key format"

    try:
        without_prefix = api_key_header[4:]
        key_id, raw_secret = without_prefix.split(".", 1)
    except ValueError:
        return None, "Invalid API key format"

    session = _get_session()
    try:
        # Fetch key + user in one query (include MCP access fields)
        row = session.execute(
            text("""
                SELECT ak.id, ak.key_hash, ak.scopes, ak.is_active,
                       ak.expires_at, ak.daily_quota, ak.daily_usage, ak.quota_reset_date,
                       ak.mcp_enabled, ak.mcp_tools,
                       u.username, u.id as user_id
                FROM api_keys ak
                JOIN users u ON u.id = ak.user_id
                WHERE ak.key_id = :key_id
            """),
            {"key_id": key_id},
        ).fetchone()

        if not row:
            # Constant-time: always run bcrypt even when key not found
            bcrypt.checkpw(raw_secret.encode("utf-8"), _DUMMY_HASH.encode("utf-8"))
            return None, "Invalid API key"

        # Verify secret first (bcrypt) — before checking other conditions
        # so all code paths run bcrypt for constant-time behavior
        if not bcrypt.checkpw(raw_secret.encode("utf-8"), row.key_hash.encode("utf-8")):
            return None, "Invalid API key"

        # Check active
        if not row.is_active:
            return None, "API key is inactive"

        # Check MCP access enabled
        if not row.mcp_enabled:
            return None, "MCP access not enabled for this API key"

        # Check expiry
        if row.expires_at:
            if datetime.now(timezone.utc) > row.expires_at.replace(tzinfo=timezone.utc):
                return None, "API key has expired"

        # Atomic quota check + increment (same pattern as jwt_auth.py)
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
            {"id": row.id},
        )
        quota_row = result.fetchone()
        session.commit()

        if not quota_row:
            return None, "Daily quota exceeded"

        return {
            "username": row.username,
            "user_id": row.user_id,
            "key_id": key_id,
            "scopes": row.scopes or [],
            "mcp_tools": row.mcp_tools or [],
        }, None

    except Exception as e:
        logger.error(f"API key authentication error: {e}")
        session.rollback()
        return None, "Authentication error"
    finally:
        session.close()


def _validate_client_id(client_id: str) -> bool:
    """Check if a client_id (API key_id) exists in the DB and has MCP enabled."""
    session = _get_session()
    try:
        row = session.execute(
            text("SELECT is_active, mcp_enabled FROM api_keys WHERE key_id = :key_id"),
            {"key_id": client_id},
        ).fetchone()
        return bool(row and row.is_active and row.mcp_enabled)
    except Exception as e:
        logger.error(f"Client ID validation error: {e}")
        return False
    finally:
        session.close()


def _get_jwt_secret() -> str:
    """Get JWT secret from the backend's vault. Fails hard if not configured."""
    try:
        from common.secrets_vault import vault_config
        secret = vault_config('JWT_SECRET', default=None)
        if secret:
            return secret
    except Exception as e:
        logger.error(f"Could not load JWT_SECRET from vault: {e}")
    import os
    secret = os.environ.get('JWT_SECRET')
    if not secret:
        raise RuntimeError("JWT_SECRET is not configured — refusing to validate tokens")
    return secret


def _authenticate_bearer_token(token: str) -> Tuple[Optional[dict], Optional[str]]:
    """
    Validate an OAuth Bearer JWT token.

    Returns:
        (user_info_dict, None) on success
        (None, error_message) on failure
    """
    jwt_secret = _get_jwt_secret()
    try:
        payload = jwt.decode(token, jwt_secret, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        return None, "Token expired"
    except jwt.InvalidTokenError:
        return None, "Invalid token"

    if payload.get("type") != "access":
        return None, "Not an access token"

    # Look up the API key's mcp_tools restriction from DB
    client_id = payload.get("client_id")
    mcp_tools = None  # None = no restriction (all tools)
    if client_id:
        session = _get_session()
        try:
            row = session.execute(
                text("SELECT mcp_tools FROM api_keys WHERE key_id = :key_id AND is_active = true AND mcp_enabled = true"),
                {"key_id": client_id},
            ).fetchone()
            if not row:
                return None, "API key no longer active"
            mcp_tools = row.mcp_tools or None
        except Exception as e:
            logger.error(f"OAuth token key lookup error: {e}")
            return None, "Authentication error"
        finally:
            session.close()

    return {
        "username": payload.get("sub", "oauth_client"),
        "user_id": None,
        "key_id": client_id or "unknown",
        "scopes": payload.get("scope", "mcp:*").split(),
        "mcp_tools": mcp_tools,
    }, None
