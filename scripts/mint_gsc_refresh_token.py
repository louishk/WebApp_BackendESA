#!/usr/bin/env python3
"""
Mint a Google Search Console OAuth refresh token.

One-time helper to obtain a refresh token scoped for the Search Console API.
Reuses the existing Google Ads OAuth client_id/client_secret (same Cloud project),
but the resulting refresh token is scope-locked to webmasters and stored
separately in the vault as GOOGLE_SEARCHCONSOLE_REFRESH_TOKEN.

Usage:
    python scripts/mint_gsc_refresh_token.py

After consenting in the browser, the refresh token is printed. Save it via the
/admin/secrets UI under the key GOOGLE_SEARCHCONSOLE_REFRESH_TOKEN.

Requirements:
    pip install google-auth-oauthlib
"""

import os
import sys
from pathlib import Path

# Make backend/python importable for vault access
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "backend" / "python"))

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    print("ERROR: google-auth-oauthlib not installed.")
    print("Run: pip install google-auth-oauthlib")
    sys.exit(1)

SCOPES = ["https://www.googleapis.com/auth/webmasters"]
DEFAULT_CLIENT_ID = "387700327426-cmsrklgau8vqh9mnjvl95cc4vvenqj77.apps.googleusercontent.com"


def _load_secret_from_vault(key: str) -> str:
    try:
        from dotenv import load_dotenv
        load_dotenv(REPO_ROOT / ".env")
        from common.config_loader import get_config
        return get_config().get_secret(key) or ""
    except Exception as e:
        print(f"WARN: vault lookup for {key} failed: {e}")
        return ""


def main() -> int:
    client_id = (
        os.environ.get("GSC_OAUTH_CLIENT_ID")
        or DEFAULT_CLIENT_ID
    )
    client_secret = (
        os.environ.get("GSC_OAUTH_CLIENT_SECRET")
        or _load_secret_from_vault("GOOGLE_ADS_CLIENT_SECRET")
        or input("OAuth client_secret: ").strip()
    )

    if not client_id or not client_secret:
        print("client_id and client_secret are required")
        return 1

    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["http://localhost"],
        }
    }

    flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
    creds = flow.run_local_server(port=0, prompt="consent", access_type="offline")

    if not creds.refresh_token:
        print("ERROR: No refresh token returned. Make sure you consented with prompt=consent.")
        return 1

    print("\n" + "=" * 70)
    print("SUCCESS — Search Console refresh token obtained.")
    print("=" * 70)
    print(f"\nRefresh token:\n{creds.refresh_token}\n")
    print("Next steps:")
    print("  1. Open the ESA Backend admin UI: /admin/secrets")
    print("  2. Add a new secret with key: GOOGLE_SEARCHCONSOLE_REFRESH_TOKEN")
    print("  3. Paste the refresh token above as the value")
    print("  4. Restart the MCP server")
    return 0


if __name__ == "__main__":
    sys.exit(main())
