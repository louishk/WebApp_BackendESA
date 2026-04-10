# MCP OAuth MS 365 SSO — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Microsoft 365 SSO login to the MCP OAuth flow so Claude.ai Enterprise connector users authenticate via MS login instead of needing API keys upfront.

**Architecture:** When `/oauth/authorize` receives a request without a known `client_id`, it redirects to Microsoft login. After MS auth, `/oauth/callback` looks up the user by email → finds their API key → stores an auth code tied to that key_id → redirects back to Claude.ai. The existing API key auth and client_secret OAuth paths are untouched.

**Tech Stack:** Python, Starlette, httpx (async HTTP client), Microsoft Identity Platform (OAuth 2.0), JWT

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `mcp_esa/server/oauth.py` | Modify | Add MS SSO redirect in authorize, new callback endpoint, make client_secret optional at token exchange |
| `mcp_esa/config/settings.py` | Modify | Add MS OAuth config properties |
| `backend/python/config/mcp.yaml` | Modify | Add `microsoft_oauth` config section |
| `backend/python/config/oauth.yaml` | Read-only reference | Existing MS OAuth config (client_id, tenant_id, redirect_uri) |

---

### Task 1: Add MS OAuth Config to MCP Settings

**Files:**
- Modify: `backend/python/config/mcp.yaml`
- Modify: `mcp_esa/config/settings.py`

- [ ] **Step 1: Add microsoft_oauth section to mcp.yaml**

Add after the `server` block (after line 11):

```yaml
# =============================================================================
# Microsoft OAuth (for claude.ai Enterprise connector SSO)
# =============================================================================
# Reuses the same Azure AD app registration as the Flask app.
# The MCP redirect_uri must be registered in Azure AD as an additional redirect.
microsoft_oauth:
  enabled: true
  client_id: "a95a430f-94c8-4c9d-8522-f5e26cebe8ec"
  client_secret_vault: "MS_OAUTH_CLIENT_SECRET"
  tenant_id: "45d0dc96-5f92-4a48-93c1-edea9332dd55"
  # MCP server's callback URL — must be registered in Azure AD app
  redirect_uri: "https://backend.extraspace.com.sg/mcp/oauth/callback"
  allowed_domains:
    - "extraspaceasia.com"
```

- [ ] **Step 2: Add MS OAuth properties to Settings class**

In `mcp_esa/config/settings.py`, add to the `__init__` method after `self._gads`:

```python
        self._ms_oauth = self._mcp.get('microsoft_oauth', {})
```

Add these properties after the `revenue_enabled` property:

```python
    # Microsoft OAuth (for claude.ai Enterprise SSO)
    @property
    def ms_oauth_enabled(self) -> bool:
        return self._ms_oauth.get('enabled', False)

    @property
    def ms_oauth_client_id(self) -> str:
        return self._ms_oauth.get('client_id', '')

    @property
    def ms_oauth_client_secret(self) -> str:
        vault_key = self._ms_oauth.get('client_secret_vault', 'MS_OAUTH_CLIENT_SECRET')
        return self._config.get_secret(vault_key) or ''

    @property
    def ms_oauth_tenant_id(self) -> str:
        return self._ms_oauth.get('tenant_id', 'common')

    @property
    def ms_oauth_redirect_uri(self) -> str:
        return self._ms_oauth.get('redirect_uri', '')

    @property
    def ms_oauth_allowed_domains(self) -> list:
        return self._ms_oauth.get('allowed_domains', ['extraspaceasia.com'])
```

- [ ] **Step 3: Commit**

```bash
git add backend/python/config/mcp.yaml mcp_esa/config/settings.py
git commit -m "feat: add MS OAuth config for MCP Enterprise SSO"
```

---

### Task 2: Modify /oauth/authorize — MS SSO Redirect

**Files:**
- Modify: `mcp_esa/server/oauth.py`

The current `oauth_authorize_endpoint` requires a valid `client_id` (API key_id). For Enterprise connectors, Claude.ai sends a `client_id` from dynamic registration — which may be a generic placeholder, not an API key_id.

The change: if `client_id` is not a valid API key_id, treat this as an Enterprise SSO flow and redirect to Microsoft login. If it IS a valid key_id, proceed with the existing flow unchanged.

- [ ] **Step 1: Add in-memory storage for pending SSO sessions**

At the top of `oauth.py`, after `_authorization_codes` (line 37), add:

```python
# Pending MS SSO sessions: ms_state → {claude.ai OAuth params}
_pending_sso_sessions: Dict[str, Dict[str, Any]] = {}
_MAX_PENDING_SSO = 200
```

- [ ] **Step 2: Add helper to build MS authorize URL**

Add after `_prune_expired_codes()` function:

```python
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
```

- [ ] **Step 3: Modify oauth_authorize_endpoint**

Replace the `client_id` validation block (lines 202-214) with a branching check. The existing code:

```python
    if not client_id:
        return JSONResponse(
            {"error": "invalid_request", "error_description": "client_id required"},
            status_code=400,
        )

    # Validate client_id exists in API keys table
    from mcp_esa.server.auth import _validate_client_id
    if not _validate_client_id(client_id):
        return JSONResponse(
            {"error": "invalid_client", "error_description": "Unknown client_id"},
            status_code=400,
        )
```

Replace with:

```python
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

        # Store the Claude.ai OAuth params so we can resume after MS login
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
            "client_id_hint": client_id,  # May be generic placeholder
            "expires_at": datetime.now() + timedelta(minutes=10),
        }

        ms_url = _build_ms_authorize_url(settings, ms_state)
        logger.info(f"Redirecting to MS login for Enterprise SSO (ms_state={ms_state[:8]}...)")
        return RedirectResponse(ms_url, status_code=302)
```

Then keep the rest of the function (redirect_uri validation, PKCE, code generation) as-is — it only runs for the `is_direct_key` path.

**IMPORTANT:** The redirect_uri and PKCE validation currently happens BEFORE the client_id check. We need to move the SSO branch AFTER redirect_uri and PKCE validation so those checks still apply. The actual insertion point is: replace only the `client_id` validation block (lines 202-214), keeping everything before (response_type, redirect_uri, PKCE checks) and after (code generation) intact.

- [ ] **Step 4: Commit**

```bash
git add mcp_esa/server/oauth.py
git commit -m "feat: redirect to MS login when client_id is not a known API key"
```

---

### Task 3: Add /oauth/callback — MS SSO Completion

**Files:**
- Modify: `mcp_esa/server/oauth.py`

- [ ] **Step 1: Add the MS callback endpoint**

Add this function after `oauth_authorize_endpoint`:

```python
async def oauth_ms_callback_endpoint(request: Request) -> Response:
    """Handle Microsoft OAuth callback — complete Enterprise SSO flow.

    Flow: MS login → callback with code → exchange for MS token →
    get user email → look up API key → issue auth code → redirect to Claude.ai
    """
    params = dict(request.query_params)
    ms_code = params.get("code")
    ms_state = params.get("state")
    ms_error = params.get("error")

    # Handle MS login errors
    if ms_error:
        error_desc = params.get("error_description", "Microsoft login failed")
        logger.warning(f"MS OAuth error: {ms_error} - {error_desc}")
        # Static error message — never reflect MS error_description in HTML (XSS risk)
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

    # Exchange MS code for token and get user info
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

    # Validate email domain
    email_domain = email.rsplit('@', 1)[-1].lower()
    if email_domain not in settings.ms_oauth_allowed_domains:
        logger.warning(f"SSO rejected: unauthorized domain '{email_domain}' for {email}")
        return HTMLResponse(
            "<html><body><h2>Access Denied</h2>"
            "<p>Your organization is not authorized to access this service.</p></body></html>",
            status_code=403,
        )

    # Look up user → API key
    key_id, error_msg = _lookup_user_api_key(email)
    if error_msg:
        logger.warning(f"SSO denied for {email}: {error_msg}")
        return HTMLResponse(
            f"<html><body><h2>Access Denied</h2><p>{error_msg}</p></body></html>",
            status_code=403,
        )

    # Generate auth code tied to the user's API key_id
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
        "sso_authenticated": True,  # Flag: user proved identity via MS SSO
        "created_at": datetime.now(),
        "expires_at": datetime.now() + timedelta(minutes=10),
    }

    logger.info(f"SSO auth code issued for {email} (key: {key_id})")

    # Redirect back to Claude.ai with the auth code
    redirect_params = {"code": auth_code}
    if session.get("state"):
        redirect_params["state"] = session["state"]
    return RedirectResponse(
        f"{session['redirect_uri']}?{urlencode(redirect_params)}",
        status_code=302,
    )
```

- [ ] **Step 2: Add the MS token exchange helper**

Add after the callback endpoint:

```python
async def _exchange_ms_code_for_user(settings, ms_code: str) -> tuple:
    """Exchange MS authorization code for token, then fetch user email.

    Returns:
        (email, display_name) tuple

    Raises:
        Exception on any failure
    """
    import httpx

    tenant = settings.ms_oauth_tenant_id
    token_url = f"https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token"

    async with httpx.AsyncClient(timeout=15.0) as client:
        # Exchange code for token
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

        # Fetch user info from Graph API
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
```

- [ ] **Step 3: Add user → API key lookup helper**

Add after the token exchange helper:

```python
def _lookup_user_api_key(email: str) -> tuple:
    """Look up a user by email and find their MCP-enabled API key.

    Returns:
        (key_id, None) on success
        (None, error_message) on failure
    """
    from mcp_esa.server.auth import _get_session
    from sqlalchemy import text

    session = _get_session()
    try:
        # First check if user exists at all
        user_row = session.execute(
            text("SELECT id, username FROM users WHERE LOWER(email) = :email"),
            {"email": email.lower()},
        ).fetchone()

        if not user_row:
            return None, "No account found for this email. Contact your administrator."

        # Then check for an active, MCP-enabled API key
        key_row = session.execute(
            text("""
                SELECT key_id FROM api_keys
                WHERE user_id = :user_id AND is_active = true AND mcp_enabled = true
                ORDER BY created_at DESC LIMIT 1
            """),
            {"user_id": user_row.id},
        ).fetchone()

        if not key_row:
            # Check why — no key at all, inactive, or not MCP-enabled?
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

        logger.info(f"SSO user lookup: {email} → {user_row.username} (key: {key_row.key_id})")
        return key_row.key_id, None

    except Exception as e:
        logger.error(f"User lookup failed for {email}: {e}")
        return None, "An error occurred during authentication. Please try again."
    finally:
        session.close()
```

- [ ] **Step 4: Add HTMLResponse import**

At the top of `oauth.py`, update the starlette imports (line 31):

```python
from starlette.responses import Response, JSONResponse, RedirectResponse, HTMLResponse
```

- [ ] **Step 5: Register the callback route**

In `get_oauth_routes()`, add the callback route:

```python
        Route("/oauth/callback", endpoint=oauth_ms_callback_endpoint, methods=["GET"]),
```

- [ ] **Step 6: Commit**

```bash
git add mcp_esa/server/oauth.py
git commit -m "feat: add /oauth/callback for MS SSO Enterprise flow"
```

---

### Task 4: Make client_secret Optional at Token Exchange

**Files:**
- Modify: `mcp_esa/server/oauth.py`

The current `_handle_auth_code_grant` requires `client_secret`. For SSO-authenticated codes, the user already proved their identity via MS login — no client_secret needed.

- [ ] **Step 1: Modify _handle_auth_code_grant**

Replace the client_secret validation block (lines 328-340):

```python
    # Validate client credentials (API key) — mandatory
    if not client_secret:
        return JSONResponse(
            {"error": "invalid_client", "error_description": "client_secret required"},
            status_code=401,
        )
    user_info, error = _validate_client(client_id, client_secret)
    if error:
        logger.warning(f"OAuth token exchange denied: {error}")
        return JSONResponse(
            {"error": "invalid_client", "error_description": "Invalid client credentials"},
            status_code=401,
        )
```

With:

```python
    # Validate client credentials
    # Two paths: (1) client_secret provided → validate API key (existing flow)
    #            (2) SSO-authenticated code → client_secret not needed
    if client_secret:
        # Existing path: validate API key
        user_info, error = _validate_client(client_id, client_secret)
        if error:
            logger.warning(f"OAuth token exchange denied: {error}")
            return JSONResponse(
                {"error": "invalid_client", "error_description": "Invalid client credentials"},
                status_code=401,
            )
    elif auth_data.get("sso_authenticated"):
        # SSO path: user proved identity via MS login, client_id is the key_id
        # Re-verify the API key is still valid
        from mcp_esa.server.auth import _validate_client_id
        if not _validate_client_id(auth_data["client_id"]):
            return JSONResponse(
                {"error": "invalid_client", "error_description": "API key is no longer active"},
                status_code=401,
            )
    else:
        # No client_secret and not SSO → reject
        return JSONResponse(
            {"error": "invalid_client", "error_description": "client_secret required"},
            status_code=401,
        )
```

- [ ] **Step 2: Update client_id handling for SSO path**

In `_handle_auth_code_grant`, the `client_id` from Claude.ai may be a generic placeholder for SSO flows. The real key_id is in `auth_data["client_id"]` (set by the callback). Update the client_id mismatch check (lines 321-326):

Replace:

```python
    # Validate client_id matches the one used at authorization
    if client_id != auth_data["client_id"]:
        return JSONResponse(
            {"error": "invalid_grant", "error_description": "client_id mismatch"},
            status_code=400,
        )
```

With:

```python
    # Validate client_id matches — for SSO flows, use the key_id from auth_data
    effective_client_id = auth_data["client_id"]
    if auth_data.get("sso_authenticated"):
        pass  # SSO: client_id may differ from key_id (e.g. "enterprise_sso" vs actual key_id)
    elif client_id != effective_client_id:
        return JSONResponse(
            {"error": "invalid_grant", "error_description": "client_id mismatch"},
            status_code=400,
        )
```

And update the token generation at the end of the function to use `effective_client_id`:

```python
    del _authorization_codes[code]
    scope = auth_data.get("scope", "mcp:*")
    return _generate_token_response(scope, effective_client_id)
```

- [ ] **Step 3: Update oauth_register_endpoint for Enterprise connectors**

For Enterprise connectors, `/oauth/register` is called without an API key. It should return a generic client_id (since per-user identity comes from the SSO step, not from registration).

Add an early return at the top of `oauth_register_endpoint`, before the API key check:

```python
    # If no API key provided, return a generic registration for Enterprise SSO
    if not api_key:
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
```

- [ ] **Step 4: Update metadata to advertise "none" auth method**

In `oauth_metadata_endpoint`, the `token_endpoint_auth_methods_supported` already includes `"none"`. No change needed.

- [ ] **Step 5: Commit**

```bash
git add mcp_esa/server/oauth.py
git commit -m "feat: make client_secret optional for SSO-authenticated token exchange"
```

---

### Task 5: Nginx Route & Azure AD Configuration

**Files:**
- Documentation only — manual steps

- [ ] **Step 1: Add /mcp/oauth/callback route to nginx**

The MCP server runs on port 8002 behind nginx. The callback URL must be routable. Check if the existing nginx config already proxies `/mcp/*` or `/oauth/*` paths to the MCP server. If `/oauth/callback` is already covered by the MCP proxy block, no change needed.

If not, add to the nginx config:

```nginx
location /mcp/oauth/callback {
    proxy_pass http://127.0.0.1:8002/oauth/callback;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-Host $host;
}
```

- [ ] **Step 2: Register redirect URI in Azure AD**

Go to Azure Portal → App Registrations → select the ESA app (`a95a430f-94c8-4c9d-8522-f5e26cebe8ec`) → Authentication → Add platform (Web) or add redirect URI:

```
https://backend.extraspace.com.sg/mcp/oauth/callback
```

This is required for Microsoft to redirect back to the MCP server after login.

- [ ] **Step 3: Commit plan doc**

```bash
git add docs/superpowers/plans/2026-04-10-mcp-oauth-ms-sso.md
git commit -m "docs: MCP OAuth MS SSO implementation plan"
```

---

### Task 6: Smoke Test

- [ ] **Step 1: Test existing API key flow still works**

Verify direct API key auth and client_secret OAuth flow are unaffected:
1. Call MCP with `X-API-Key` header → should work as before
2. Call `/oauth/authorize` with a valid API key_id as `client_id` → should issue code directly (no MS redirect)

- [ ] **Step 2: Test Enterprise SSO flow**

1. Call `/oauth/register` without Bearer token → should return generic `enterprise_sso` client_id
2. Call `/oauth/authorize` with `client_id=enterprise_sso` → should redirect to Microsoft login
3. Complete MS login → should redirect to `/oauth/callback`
4. Callback should look up user → find API key → redirect back to Claude.ai with auth code
5. Call `/oauth/token` with the code (no client_secret) → should return JWT

- [ ] **Step 3: Test error cases**

1. User not in users table → "No account found" page
2. User exists, no API key → "No API key configured" page
3. User has API key, mcp_enabled=false → "MCP access not enabled" page
4. Email from wrong domain → "Access Denied" page
