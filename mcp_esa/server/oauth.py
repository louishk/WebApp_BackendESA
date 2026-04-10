"""
MCP OAuth 2.0 Authentication for ESA Backend
Implements OAuth 2.0 endpoints required by claude.ai MCP integration.

Authentication is tied to the ESA API key system:
- client_id  = API key_id (e.g. "bd26e494")
- client_secret = full API key (e.g. "esa_bd26e494.BKbc...")

No dynamic registration needed — claude.ai enters client_id + client_secret
in its MCP settings, and the server validates against the api_keys DB table.

Endpoints:
- GET  /.well-known/oauth-authorization-server  - OAuth metadata
- GET  /.well-known/oauth-protected-resource     - Protected resource metadata
- POST /oauth/register                           - Dynamic client registration (API key required)
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
from starlette.responses import Response, JSONResponse, RedirectResponse, HTMLResponse
from starlette.routing import Route

logger = logging.getLogger(__name__)

# In-memory storage
_authorization_codes: Dict[str, Dict[str, Any]] = {}

# Pending MS SSO sessions: ms_state → {claude.ai OAuth params}
_pending_sso_sessions: Dict[str, Dict[str, Any]] = {}
_MAX_PENDING_SSO = 200

# Limits
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


def _validate_client(client_id: str, client_secret: str) -> tuple:
    """
    Validate OAuth client credentials against the API keys table.
    client_id = key_id, client_secret = full API key (esa_<key_id>.<secret>).

    Returns:
        (user_info_dict, None) on success
        (None, error_message) on failure
    """
    from mcp_esa.server.auth import _authenticate_api_key

    # The client_secret IS the full API key
    if not client_secret:
        return None, "client_secret required"

    # Validate the API key
    user_info, error = _authenticate_api_key(client_secret)
    if error:
        return None, error

    # Verify client_id matches the key_id from the API key
    if client_id and client_id != user_info["key_id"]:
        return None, "client_id does not match API key"

    return user_info, None


def _prune_expired_codes():
    """Remove expired authorization codes to prevent memory accumulation."""
    now = datetime.now()
    expired = [k for k, v in _authorization_codes.items() if now > v["expires_at"]]
    for k in expired:
        del _authorization_codes[k]
    if expired:
        logger.debug(f"Pruned {len(expired)} expired authorization codes")


def _prune_expired_sso():
    """Remove expired SSO sessions."""
    now = datetime.now()
    expired = [k for k, v in _pending_sso_sessions.items() if now > v["expires_at"]]
    for k in expired:
        del _pending_sso_sessions[k]


def _build_ms_authorize_url(settings, ms_state: str) -> str:
    """Build Microsoft OAuth 2.0 authorization URL."""
    tenant = settings.ms_oauth_tenant_id
    params = {
        "client_id": settings.ms_oauth_client_id,
        "response_type": "code",
        "redirect_uri": settings.ms_oauth_redirect_uri,
        "response_mode": "query",
        "scope": "openid profile email User.Read",
        "state": ms_state,
    }
    return f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/authorize?{urlencode(params)}"


async def oauth_metadata_endpoint(request: Request) -> Response:
    """OAuth 2.0 Authorization Server Metadata (RFC 8414)"""
    base_url = _get_base_url(request)
    return JSONResponse({
        "issuer": base_url,
        "authorization_endpoint": f"{base_url}/oauth/authorize",
        "token_endpoint": f"{base_url}/oauth/token",
        "registration_endpoint": f"{base_url}/oauth/register",
        "token_endpoint_auth_methods_supported": ["client_secret_post", "none"],
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
    """OAuth 2.0 Dynamic Client Registration (RFC 7591)
    Validates the API key passed as Bearer token, then returns it back
    as client_id/client_secret so claude.ai can use it for the OAuth flow.
    """
    # Extract API key from Authorization header
    auth_header = request.headers.get("Authorization", "")
    api_key = None
    if auth_header.startswith("Bearer "):
        api_key = auth_header[7:]

    if not api_key:
        # Enterprise SSO: no API key needed at registration
        try:
            body = await request.json()
        except Exception:
            body = {}

        from mcp_esa.config.settings import get_settings
        settings = get_settings()
        if not settings.ms_oauth_enabled:
            return JSONResponse(
                {"error": "invalid_token", "error_description": "API key required as Bearer token"},
                status_code=401,
            )

        logger.info("OAuth client registered for Enterprise SSO (no API key)")
        return JSONResponse({
            "client_id": "enterprise_sso",
            "client_id_issued_at": int(datetime.now().timestamp()),
            "client_secret_expires_at": 0,
            "redirect_uris": body.get("redirect_uris", []),
            "grant_types": ["authorization_code"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
        }, status_code=201)

    from mcp_esa.server.auth import _authenticate_api_key
    user_info, error = _authenticate_api_key(api_key)
    if error:
        logger.warning(f"OAuth registration denied: {error}")
        return JSONResponse(
            {"error": "invalid_token", "error_description": "Invalid or unauthorized API key"},
            status_code=401,
        )

    try:
        body = await request.json()
    except Exception:
        body = {}

    # Return the API key credentials as OAuth client credentials
    # client_id = key_id, client_secret = full API key
    logger.info(f"OAuth client registered via API key: {user_info['username']} (key: {user_info['key_id']})")

    return JSONResponse({
        "client_id": user_info["key_id"],
        "client_secret": api_key,
        "client_id_issued_at": int(datetime.now().timestamp()),
        "client_secret_expires_at": 0,
        "redirect_uris": body.get("redirect_uris", []),
        "grant_types": ["authorization_code"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "client_secret_post",
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

    # Determine auth flow: direct (API key known) vs SSO (Enterprise connector)
    from mcp_esa.server.auth import _validate_client_id
    is_direct_key = client_id and _validate_client_id(client_id)

    if not is_direct_key:
        # Enterprise SSO flow: redirect to Microsoft login
        from mcp_esa.config.settings import get_settings
        settings = get_settings()

        if not settings.ms_oauth_enabled:
            return JSONResponse(
                {"error": "invalid_client", "error_description": "SSO is not enabled"},
                status_code=400,
            )

        _prune_expired_sso()
        if len(_pending_sso_sessions) >= _MAX_PENDING_SSO:
            return JSONResponse(
                {"error": "server_error", "error_description": "Too many pending SSO sessions"},
                status_code=503,
            )

        ms_state = secrets.token_urlsafe(32)
        _pending_sso_sessions[ms_state] = {
            "redirect_uri": redirect_uri,
            "scope": scope,
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": code_challenge_method,
            "client_id_hint": client_id,
            "expires_at": datetime.now() + timedelta(minutes=10),
        }

        ms_url = _build_ms_authorize_url(settings, ms_state)
        logger.info(f"Redirecting to MS login for Enterprise SSO (ms_state={ms_state[:8]}...)")
        return RedirectResponse(ms_url, status_code=302)

    # Validate redirect_uri is https and from a trusted domain
    _ALLOWED_REDIRECT_DOMAINS = {"claude.ai", "localhost", "127.0.0.1"}
    try:
        p = urlparse(redirect_uri)
        if p.scheme != "https" and not (p.scheme == "http" and p.hostname in ("localhost", "127.0.0.1")):
            return JSONResponse(
                {"error": "invalid_request", "error_description": "redirect_uri must use https"},
                status_code=400,
            )
        if p.hostname not in _ALLOWED_REDIRECT_DOMAINS:
            return JSONResponse(
                {"error": "invalid_request", "error_description": "redirect_uri domain not allowed"},
                status_code=400,
            )
    except Exception:
        return JSONResponse(
            {"error": "invalid_request", "error_description": "Invalid redirect_uri"},
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


async def oauth_ms_callback_endpoint(request: Request) -> Response:
    """Handle Microsoft OAuth callback — complete Enterprise SSO flow."""
    params = dict(request.query_params)
    ms_code = params.get("code")
    ms_state = params.get("state")
    ms_error = params.get("error")

    if ms_error:
        error_desc = params.get("error_description", "Microsoft login failed")
        logger.warning(f"MS OAuth error: {ms_error} - {error_desc}")
        return HTMLResponse(
            "<html><body><h2>Login Failed</h2>"
            "<p>Microsoft authentication failed. Please close this window and try again.</p></body></html>",
            status_code=400,
        )

    if not ms_state or ms_state not in _pending_sso_sessions:
        return HTMLResponse(
            "<html><body><h2>Session Expired</h2>"
            "<p>Your login session has expired. Please try connecting again from Claude.</p></body></html>",
            status_code=400,
        )

    session = _pending_sso_sessions.pop(ms_state)

    if datetime.now() > session["expires_at"]:
        return HTMLResponse(
            "<html><body><h2>Session Expired</h2>"
            "<p>Your login session has expired. Please try connecting again from Claude.</p></body></html>",
            status_code=400,
        )

    if not ms_code:
        return HTMLResponse(
            "<html><body><h2>Login Failed</h2>"
            "<p>No authorization code received from Microsoft.</p></body></html>",
            status_code=400,
        )

    from mcp_esa.config.settings import get_settings
    settings = get_settings()

    try:
        email, display_name = await _exchange_ms_code_for_user(settings, ms_code)
    except Exception as e:
        logger.error(f"MS token exchange failed: {e}")
        return HTMLResponse(
            "<html><body><h2>Authentication Failed</h2>"
            "<p>Could not verify your Microsoft identity. Please try again.</p></body></html>",
            status_code=500,
        )

    email_domain = email.rsplit('@', 1)[-1].lower()
    if email_domain not in settings.ms_oauth_allowed_domains:
        logger.warning(f"SSO rejected: unauthorized domain '{email_domain}' for {email}")
        return HTMLResponse(
            "<html><body><h2>Access Denied</h2>"
            "<p>Your organization is not authorized to access this service.</p></body></html>",
            status_code=403,
        )

    key_id, error_msg = _lookup_user_api_key(email)
    if error_msg:
        logger.warning(f"SSO denied for {email}: {error_msg}")
        return HTMLResponse(
            f"<html><body><h2>Access Denied</h2><p>{error_msg}</p></body></html>",
            status_code=403,
        )

    _prune_expired_codes()
    if len(_authorization_codes) >= _MAX_PENDING_CODES:
        return HTMLResponse(
            "<html><body><h2>Server Busy</h2><p>Too many pending requests. Please try again.</p></body></html>",
            status_code=503,
        )

    auth_code = secrets.token_urlsafe(32)
    _authorization_codes[auth_code] = {
        "client_id": key_id,
        "redirect_uri": session["redirect_uri"],
        "scope": session["scope"],
        "code_challenge": session["code_challenge"],
        "code_challenge_method": session["code_challenge_method"],
        "sso_authenticated": True,
        "created_at": datetime.now(),
        "expires_at": datetime.now() + timedelta(minutes=10),
    }

    logger.info(f"SSO auth code issued for {email} (key: {key_id})")

    redirect_params = {"code": auth_code}
    if session.get("state"):
        redirect_params["state"] = session["state"]
    return RedirectResponse(
        f"{session['redirect_uri']}?{urlencode(redirect_params)}",
        status_code=302,
    )


async def _exchange_ms_code_for_user(settings, ms_code: str) -> tuple:
    """Exchange MS authorization code for token, then fetch user email."""
    import httpx

    tenant = settings.ms_oauth_tenant_id
    token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"

    async with httpx.AsyncClient(timeout=15.0) as client:
        token_resp = await client.post(token_url, data={
            "client_id": settings.ms_oauth_client_id,
            "client_secret": settings.ms_oauth_client_secret,
            "code": ms_code,
            "redirect_uri": settings.ms_oauth_redirect_uri,
            "grant_type": "authorization_code",
            "scope": "openid profile email User.Read",
        })

        if token_resp.status_code != 200:
            raise Exception(f"MS token exchange failed: {token_resp.status_code} {token_resp.text[:200]}")

        token_data = token_resp.json()
        ms_access_token = token_data.get("access_token")
        if not ms_access_token:
            raise Exception("No access_token in MS token response")

        graph_resp = await client.get(
            "https://graph.microsoft.com/v1.0/me",
            headers={"Authorization": f"Bearer {ms_access_token}"},
        )

        if graph_resp.status_code != 200:
            raise Exception(f"MS Graph API failed: {graph_resp.status_code}")

        user_data = graph_resp.json()
        email = user_data.get("mail") or user_data.get("userPrincipalName")
        display_name = user_data.get("displayName", "")

        if not email:
            raise Exception("Could not retrieve email from Microsoft account")

        return email.lower(), display_name


def _lookup_user_api_key(email: str) -> tuple:
    """Look up a user by email and find their MCP-enabled API key."""
    from mcp_esa.server.auth import _get_session
    from sqlalchemy import text

    session = _get_session()
    try:
        user_row = session.execute(
            text("SELECT id, username FROM users WHERE LOWER(email) = :email"),
            {"email": email.lower()},
        ).fetchone()

        if not user_row:
            return None, "No account found for this email. Contact your administrator."

        key_row = session.execute(
            text("""
                SELECT key_id FROM api_keys
                WHERE user_id = :user_id AND is_active = true AND mcp_enabled = true
                ORDER BY created_at DESC LIMIT 1
            """),
            {"user_id": user_row.id},
        ).fetchone()

        if not key_row:
            any_key = session.execute(
                text("SELECT is_active, mcp_enabled FROM api_keys WHERE user_id = :uid"),
                {"uid": user_row.id},
            ).fetchone()
            if not any_key:
                return None, "No API key configured for your account. Contact your administrator."
            if not any_key.is_active:
                return None, "Your API key is inactive. Contact your administrator."
            if not any_key.mcp_enabled:
                return None, "MCP access is not enabled for your account. Contact your administrator."

        logger.info(f"SSO user lookup: {email} -> {user_row.username} (key: {key_row.key_id})")
        return key_row.key_id, None

    except Exception as e:
        logger.error(f"User lookup failed for {email}: {e}")
        return None, "An error occurred during authentication. Please try again."
    finally:
        session.close()


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
    client_secret = body.get("client_secret")

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

    # Validate client_id matches — for SSO flows, use the key_id from auth_data
    effective_client_id = auth_data["client_id"]
    if auth_data.get("sso_authenticated"):
        pass  # SSO: client_id may differ from key_id (e.g. "enterprise_sso" vs actual key_id)
    elif client_id != effective_client_id:
        return JSONResponse(
            {"error": "invalid_grant", "error_description": "client_id mismatch"},
            status_code=400,
        )

    # Validate client credentials
    # Two paths: (1) client_secret provided → validate API key (existing flow)
    #            (2) SSO-authenticated code → client_secret not needed
    if client_secret:
        user_info, error = _validate_client(client_id, client_secret)
        if error:
            logger.warning(f"OAuth token exchange denied: {error}")
            return JSONResponse(
                {"error": "invalid_client", "error_description": "Invalid client credentials"},
                status_code=401,
            )
    elif auth_data.get("sso_authenticated"):
        from mcp_esa.server.auth import _validate_client_id
        if not _validate_client_id(auth_data["client_id"]):
            return JSONResponse(
                {"error": "invalid_client", "error_description": "API key is no longer active"},
                status_code=401,
            )
    else:
        return JSONResponse(
            {"error": "invalid_client", "error_description": "client_secret required"},
            status_code=401,
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
    return _generate_token_response(scope, effective_client_id)


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

    # Re-validate the API key is still active and MCP-enabled
    if client_id:
        from mcp_esa.server.auth import _validate_client_id
        if not _validate_client_id(client_id):
            logger.warning(f"OAuth refresh denied: API key {client_id} no longer valid")
            return JSONResponse(
                {"error": "invalid_grant", "error_description": "API key is no longer active"},
                status_code=400,
            )

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
        Route("/oauth/callback", endpoint=oauth_ms_callback_endpoint, methods=["GET"]),
        Route("/oauth/token", endpoint=oauth_token_endpoint, methods=["POST"]),
    ]
