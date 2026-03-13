"""
MCP OAuth 2.0 Authentication for ESA Backend
Implements OAuth 2.0 endpoints required by claude.ai MCP integration.

Endpoints:
- GET  /.well-known/oauth-authorization-server  - OAuth metadata
- GET  /.well-known/oauth-protected-resource     - Protected resource metadata
- POST /oauth/register                           - Dynamic client registration
- GET  /oauth/authorize                          - Authorization code grant
- POST /oauth/token                              - Token exchange
"""

import logging
import os
import secrets
import hashlib
import base64
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from urllib.parse import urlencode, urlparse

import jwt
from starlette.requests import Request
from starlette.responses import Response, JSONResponse, RedirectResponse
from starlette.routing import Route

logger = logging.getLogger(__name__)

# In-memory storage (single-process uvicorn, acceptable for this deployment)
_oauth_clients: Dict[str, Dict[str, Any]] = {}
_authorization_codes: Dict[str, Dict[str, Any]] = {}

# Limits
_MAX_CLIENTS = 50
_MAX_PENDING_CODES = 500


def _get_base_url(request: Request) -> str:
    """Determine public base URL from request headers (handles nginx proxy)."""
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.headers.get("host", "localhost"))
    base_url = f"{scheme}://{host}"
    forwarded_prefix = request.headers.get("x-forwarded-prefix", "")
    if forwarded_prefix:
        base_url = f"{base_url}{forwarded_prefix}"
    return base_url


def _get_jwt_secret() -> str:
    """Get JWT secret from the backend's vault. Fails hard if not configured."""
    try:
        from common.secrets_vault import vault_config
        secret = vault_config('JWT_SECRET', default=None)
        if secret:
            return secret
    except Exception as e:
        logger.error(f"Could not load JWT_SECRET from vault: {e}")
    secret = os.environ.get('JWT_SECRET')
    if not secret:
        raise RuntimeError("JWT_SECRET is not configured — refusing to issue/validate tokens")
    return secret


def _prune_expired_codes():
    """Remove expired authorization codes to prevent memory accumulation."""
    now = datetime.now()
    expired = [k for k, v in _authorization_codes.items() if now > v["expires_at"]]
    for k in expired:
        del _authorization_codes[k]
    if expired:
        logger.debug(f"Pruned {len(expired)} expired authorization codes")


async def oauth_metadata_endpoint(request: Request) -> Response:
    """OAuth 2.0 Authorization Server Metadata (RFC 8414)"""
    base_url = _get_base_url(request)
    return JSONResponse({
        "issuer": base_url,
        "authorization_endpoint": f"{base_url}/oauth/authorize",
        "token_endpoint": f"{base_url}/oauth/token",
        "registration_endpoint": f"{base_url}/oauth/register",
        "token_endpoint_auth_methods_supported": ["none"],
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "scopes_supported": ["mcp:tools", "mcp:resources", "mcp:prompts", "mcp:*"],
        "service_documentation": f"{base_url}/",
    })


async def oauth_protected_resource_endpoint(request: Request) -> Response:
    """OAuth 2.0 Protected Resource Metadata (RFC 9728)"""
    base_url = _get_base_url(request)
    return JSONResponse({
        "resource": f"{base_url}/mcp",
        "authorization_servers": [base_url],
        "scopes_supported": ["mcp:tools", "mcp:resources", "mcp:prompts", "mcp:*"],
        "bearer_methods_supported": ["header"],
    })


async def oauth_register_endpoint(request: Request) -> Response:
    """OAuth 2.0 Dynamic Client Registration (RFC 7591)"""
    # Cap total registered clients
    if len(_oauth_clients) >= _MAX_CLIENTS:
        return JSONResponse(
            {"error": "server_error", "error_description": "Registration limit reached"},
            status_code=503,
        )

    try:
        body = await request.json()
    except Exception:
        return JSONResponse(
            {"error": "invalid_request", "error_description": "Invalid JSON body"},
            status_code=400,
        )

    # Require at least one valid redirect_uri (must be https, or http://localhost for dev)
    redirect_uris = body.get("redirect_uris", [])
    if not redirect_uris:
        return JSONResponse(
            {"error": "invalid_request", "error_description": "redirect_uris required"},
            status_code=400,
        )
    for uri in redirect_uris:
        try:
            p = urlparse(uri)
            if p.scheme == "https":
                continue
            if p.scheme == "http" and p.hostname in ("localhost", "127.0.0.1"):
                continue
            return JSONResponse(
                {"error": "invalid_request", "error_description": "redirect_uris must use https"},
                status_code=400,
            )
        except Exception:
            return JSONResponse(
                {"error": "invalid_request", "error_description": "Invalid redirect_uri format"},
                status_code=400,
            )

    client_id = f"claude_{secrets.token_hex(16)}"
    client_secret = secrets.token_urlsafe(32)

    # Sanitize client_name for log safety
    client_name = str(body.get("client_name", "Claude")).replace("\n", " ").replace("\r", " ")[:128]

    client_info = {
        "client_id": client_id,
        "client_secret": client_secret,
        "client_name": client_name,
        "redirect_uris": redirect_uris,
        "grant_types": body.get("grant_types", ["authorization_code"]),
        "response_types": body.get("response_types", ["code"]),
        "scope": body.get("scope", "mcp:*"),
        "token_endpoint_auth_method": body.get("token_endpoint_auth_method", "none"),
        "created_at": datetime.now().isoformat(),
    }
    _oauth_clients[client_id] = client_info
    logger.info(f"Registered OAuth client: {client_id} ({client_name})")

    return JSONResponse({
        "client_id": client_id,
        "client_secret": client_secret,
        "client_id_issued_at": int(datetime.now().timestamp()),
        "client_secret_expires_at": 0,
        "redirect_uris": client_info["redirect_uris"],
        "grant_types": client_info["grant_types"],
        "response_types": client_info["response_types"],
        "token_endpoint_auth_method": client_info["token_endpoint_auth_method"],
    }, status_code=201)


async def oauth_authorize_endpoint(request: Request) -> Response:
    """OAuth 2.0 Authorization Endpoint — issues auth code and redirects."""
    params = dict(request.query_params)

    redirect_uri = params.get("redirect_uri")
    response_type = params.get("response_type")
    state = params.get("state")
    client_id = params.get("client_id")
    code_challenge = params.get("code_challenge")
    code_challenge_method = params.get("code_challenge_method", "S256")
    scope = params.get("scope", "mcp:*")

    if response_type != "code":
        return JSONResponse({"error": "unsupported_response_type"}, status_code=400)

    if not redirect_uri:
        return JSONResponse(
            {"error": "invalid_request", "error_description": "redirect_uri required"},
            status_code=400,
        )

    # Validate client_id is registered
    if not client_id or client_id not in _oauth_clients:
        return JSONResponse(
            {"error": "invalid_client", "error_description": "Unknown client_id"},
            status_code=400,
        )

    # Validate redirect_uri matches registered URIs
    client = _oauth_clients[client_id]
    if redirect_uri not in client["redirect_uris"]:
        return JSONResponse(
            {"error": "invalid_request", "error_description": "redirect_uri mismatch"},
            status_code=400,
        )

    # PKCE is mandatory (S256 only)
    if not code_challenge:
        return JSONResponse(
            {"error": "invalid_request", "error_description": "PKCE code_challenge is required"},
            status_code=400,
        )
    if code_challenge_method != "S256":
        return JSONResponse(
            {"error": "invalid_request", "error_description": "Only S256 code_challenge_method is supported"},
            status_code=400,
        )

    # Prune expired codes and check cap
    _prune_expired_codes()
    if len(_authorization_codes) >= _MAX_PENDING_CODES:
        return JSONResponse(
            {"error": "server_error", "error_description": "Too many pending authorization requests"},
            status_code=503,
        )

    auth_code = secrets.token_urlsafe(32)
    _authorization_codes[auth_code] = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": scope,
        "code_challenge": code_challenge,
        "code_challenge_method": code_challenge_method,
        "created_at": datetime.now(),
        "expires_at": datetime.now() + timedelta(minutes=10),
    }

    logger.info(f"Generated authorization code for client: {client_id}")

    redirect_params = {"code": auth_code}
    if state:
        redirect_params["state"] = state
    return RedirectResponse(f"{redirect_uri}?{urlencode(redirect_params)}", status_code=302)


async def oauth_token_endpoint(request: Request) -> Response:
    """OAuth 2.0 Token Endpoint — exchanges code/credentials for JWT."""
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            body = await request.json()
        except Exception:
            body = {}
    else:
        form = await request.form()
        body = dict(form)

    grant_type = body.get("grant_type")

    if grant_type == "authorization_code":
        return await _handle_auth_code_grant(body)
    elif grant_type == "refresh_token":
        return await _handle_refresh_grant(body)
    else:
        return JSONResponse(
            {"error": "unsupported_grant_type"},
            status_code=400,
        )


async def _handle_auth_code_grant(body: dict) -> Response:
    code = body.get("code")
    code_verifier = body.get("code_verifier")
    client_id = body.get("client_id")

    if code not in _authorization_codes:
        return JSONResponse(
            {"error": "invalid_grant", "error_description": "Invalid or expired authorization code"},
            status_code=400,
        )

    auth_data = _authorization_codes[code]

    if datetime.now() > auth_data["expires_at"]:
        del _authorization_codes[code]
        return JSONResponse(
            {"error": "invalid_grant", "error_description": "Authorization code expired"},
            status_code=400,
        )

    # Validate client_id matches the one used at authorization
    if client_id != auth_data["client_id"]:
        return JSONResponse(
            {"error": "invalid_grant", "error_description": "client_id mismatch"},
            status_code=400,
        )

    # PKCE verification (mandatory — code_challenge is always present)
    if not code_verifier:
        return JSONResponse(
            {"error": "invalid_grant", "error_description": "code_verifier required"},
            status_code=400,
        )
    computed = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode()).digest()
    ).decode().rstrip("=")
    if computed != auth_data["code_challenge"]:
        return JSONResponse(
            {"error": "invalid_grant", "error_description": "Invalid code_verifier"},
            status_code=400,
        )

    del _authorization_codes[code]
    scope = auth_data.get("scope", "mcp:*")
    return _generate_token_response(scope, client_id)


async def _handle_refresh_grant(body: dict) -> Response:
    refresh_token = body.get("refresh_token")
    jwt_secret = _get_jwt_secret()
    try:
        payload = jwt.decode(refresh_token, jwt_secret, algorithms=["HS256"])
    except Exception:
        return JSONResponse(
            {"error": "invalid_grant", "error_description": "Invalid refresh token"},
            status_code=400,
        )

    # Must be a refresh token, not an access token
    if payload.get("type") != "refresh":
        return JSONResponse(
            {"error": "invalid_grant", "error_description": "Not a refresh token"},
            status_code=400,
        )

    client_id = payload.get("client_id")
    # Use the scope from the original grant, not from the request
    original_scope = payload.get("scope", "mcp:*")
    return _generate_token_response(original_scope, client_id)


def _generate_token_response(scope: str, client_id: Optional[str] = None) -> Response:
    """Generate JWT access + refresh tokens."""
    jwt_secret = _get_jwt_secret()
    now = datetime.now()
    expires_in = 3600 * 8  # 8 hours

    access_payload = {
        "sub": "oauth_client",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=expires_in)).timestamp()),
        "type": "access",
        "scope": scope,
        "client_id": client_id,
        "jti": secrets.token_hex(16),
    }
    access_token = jwt.encode(access_payload, jwt_secret, algorithm="HS256")

    refresh_payload = {
        "sub": "oauth_client",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(days=30)).timestamp()),
        "type": "refresh",
        "scope": scope,
        "client_id": client_id,
        "jti": secrets.token_hex(16),
    }
    refresh_token = jwt.encode(refresh_payload, jwt_secret, algorithm="HS256")

    logger.info(f"Issued OAuth tokens for client: {client_id}")

    return JSONResponse({
        "access_token": access_token,
        "token_type": "Bearer",
        "expires_in": expires_in,
        "refresh_token": refresh_token,
        "scope": scope,
    })


def get_oauth_routes() -> list:
    """Get all OAuth route definitions."""
    return [
        Route("/.well-known/oauth-authorization-server", endpoint=oauth_metadata_endpoint, methods=["GET"]),
        Route("/.well-known/oauth-protected-resource", endpoint=oauth_protected_resource_endpoint, methods=["GET"]),
        Route("/oauth/register", endpoint=oauth_register_endpoint, methods=["POST"]),
        Route("/oauth/authorize", endpoint=oauth_authorize_endpoint, methods=["GET", "POST"]),
        Route("/oauth/token", endpoint=oauth_token_endpoint, methods=["POST"]),
    ]
