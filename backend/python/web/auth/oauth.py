"""
Microsoft OAuth Authentication using Authlib.
"""

import os
from authlib.integrations.flask_client import OAuth

oauth = OAuth()


def init_oauth(app):
    """
    Initialize Microsoft OAuth for Flask app.

    Required environment variables:
    - MS_OAUTH_CLIENT_ID: Azure AD application client ID
    - MS_OAUTH_CLIENT_SECRET: Azure AD application client secret
    - MS_OAUTH_TENANT: Azure AD tenant ID (or 'common' for multi-tenant)
    - MS_OAUTH_REDIRECT_URI: OAuth callback URL

    Args:
        app: Flask application
    """
    oauth.init_app(app)

    tenant = os.getenv('MS_OAUTH_TENANT', 'common')

    oauth.register(
        name='microsoft',
        client_id=os.getenv('MS_OAUTH_CLIENT_ID'),
        client_secret=os.getenv('MS_OAUTH_CLIENT_SECRET'),
        server_metadata_url=f"https://login.microsoftonline.com/{tenant}/v2.0/.well-known/openid-configuration",
        client_kwargs={
            'scope': 'openid profile email User.Read'
        }
    )


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
