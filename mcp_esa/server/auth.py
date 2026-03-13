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

    PUBLIC_PATHS = {"/", "/health"}

    def __init__(self, app):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Public endpoints
        if path in self.PUBLIC_PATHS:
            return await call_next(request)

        # CORS preflight
        if request.method == "OPTIONS":
            return await call_next(request)

        # Rate limiting by IP
        client_ip = request.client.host if request.client else "unknown"
        now = time.time()
        hits = [t for t in _rate_limit_window[client_ip] if now - t < _RATE_LIMIT_WINDOW]
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

        # Validate API key
        api_key_header = request.headers.get("X-API-Key", "")
        if not api_key_header:
            return JSONResponse(
                {
                    "jsonrpc": "2.0",
                    "error": {"code": -32001, "message": "Authentication required. Provide X-API-Key header."},
                    "id": None,
                },
                status_code=401,
            )

        user_info, error = _authenticate_api_key(api_key_header)
        if error:
            logger.warning(f"MCP auth failed from {client_ip}: {error}")
            status_code = 429 if error == "Daily quota exceeded" else 401
            # Generic message — don't reveal specific failure reason
            public_msg = "Rate limit exceeded" if status_code == 429 else "Invalid or unauthorized API key"
            return JSONResponse(
                {
                    "jsonrpc": "2.0",
                    "error": {"code": -32002, "message": public_msg},
                    "id": None,
                },
                status_code=status_code,
            )

        # Attach user info to request state for tools to access
        request.state.user = user_info["username"]
        request.state.user_id = user_info["user_id"]
        request.state.key_id = user_info["key_id"]
        request.state.scopes = user_info["scopes"]
        request.state.mcp_tools = user_info["mcp_tools"]  # empty = all tools allowed

        logger.info(f"MCP auth OK: {user_info['username']} (key: {user_info['key_id']}) from {client_ip}")
        return await call_next(request)


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
