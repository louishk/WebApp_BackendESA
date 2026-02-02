"""
Microsoft OAuth Authentication using Authlib.
"""

from authlib.integrations.flask_client import OAuth

oauth = OAuth()


def init_oauth(app):
    """
    Initialize Microsoft OAuth for Flask app.

    Configuration loaded from:
    - backend/config/oauth.yaml (client_id, tenant_id, redirect_uri, scopes)
    - Vault secret: MS_OAUTH_CLIENT_SECRET

    Args:
        app: Flask application
    """
    from common.config_loader import get_config

    config = get_config()
    ms_config = config.oauth.microsoft

    if not ms_config or not ms_config.enabled:
        app.logger.warning("Microsoft OAuth is disabled or not configured")
        return

    oauth.init_app(app)

    tenant = ms_config.tenant_id or 'common'
    client_id = ms_config.client_id
    client_secret = ms_config.client_secret_vault  # Auto-resolved from vault

    if not client_id:
        app.logger.error("Microsoft OAuth client_id not configured")
        return

    if not client_secret:
        app.logger.error("Microsoft OAuth client_secret not found in vault")
        return

    # Build scope string from list
    scopes = ' '.join(ms_config.scopes) if ms_config.scopes else 'openid profile email User.Read'

    oauth.register(
        name='microsoft',
        client_id=client_id,
        client_secret=client_secret,
        server_metadata_url=f"https://login.microsoftonline.com/{tenant}/v2.0/.well-known/openid-configuration",
        client_kwargs={
            'scope': scopes
        }
    )

    app.logger.info(f"Microsoft OAuth initialized for tenant {tenant}")


def get_microsoft_user_info(token):
    """
    Get user info from Microsoft Graph API.

    Args:
        token: OAuth access token

    Returns:
        dict: User info from Microsoft Graph
    """
    resp = oauth.microsoft.get('https://graph.microsoft.com/v1.0/me', token=token)
    return resp.json()
