"""
One-off script: Backfill O365 profile fields for existing users.

Uses Microsoft Graph client credentials flow to fetch department, jobTitle,
officeLocation, employeeId for all Microsoft-auth users and updates the DB.

Requires: User.Read.All APPLICATION permission on the Azure AD app registration.
(Go to Azure Portal > App registrations > API permissions > Add > Microsoft Graph >
 Application permissions > User.Read.All > Grant admin consent)

Usage:
    cd backend/python
    python -m scripts.backfill_o365_profiles
"""

import sys
import requests
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.config_loader import get_config, get_database_url
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def get_graph_token(tenant_id, client_id, client_secret):
    """Get an app-only token via client credentials flow."""
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    resp = requests.post(url, data={
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://graph.microsoft.com/.default',
    })
    resp.raise_for_status()
    return resp.json()['access_token']


def fetch_all_graph_users(token):
    """Fetch all users from Microsoft Graph with profile fields."""
    headers = {'Authorization': f'Bearer {token}'}
    url = 'https://graph.microsoft.com/v1.0/users'
    params = {
        '$select': 'mail,userPrincipalName,displayName,department,jobTitle,officeLocation,employeeId',
        '$top': '999',
    }

    users = []
    while url:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        users.extend(data.get('value', []))
        url = data.get('@odata.nextLink')
        params = None  # nextLink already includes params

    return users


def run():
    config = get_config()
    ms = config.oauth.microsoft

    if not ms or not ms.enabled:
        print("ERROR: Microsoft OAuth not configured")
        return

    tenant_id = ms.tenant_id
    client_id = ms.client_id
    client_secret = ms.client_secret_vault

    print("=" * 60)
    print("Backfill O365 Profile Fields")
    print("=" * 60)

    # Get app-only token
    print("\n[1] Getting Graph API token (client credentials)...")
    try:
        token = get_graph_token(tenant_id, client_id, client_secret)
        print("    Token acquired.")
    except Exception as e:
        print(f"    FAILED: {e}")
        print("\n    Make sure User.Read.All APPLICATION permission is granted:")
        print("    Azure Portal > App registrations > API permissions > Add permission")
        print("    > Microsoft Graph > Application permissions > User.Read.All")
        print("    > Grant admin consent")
        return

    # Fetch all users from Graph
    print("[2] Fetching users from Microsoft Graph...")
    graph_users = fetch_all_graph_users(token)
    print(f"    Got {len(graph_users)} users from Azure AD.")

    # Build lookup by email (lowercase)
    graph_lookup = {}
    for gu in graph_users:
        email = (gu.get('mail') or gu.get('userPrincipalName') or '').lower()
        if email:
            graph_lookup[email] = gu

    # Update database
    print("[3] Updating database...")
    db_url = get_database_url('backend')
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    from web.models.user import User
    ms_users = session.query(User).filter_by(auth_provider='microsoft').all()
    print(f"    Found {len(ms_users)} Microsoft users in DB.")

    updated = 0
    skipped = 0
    not_found = 0

    for user in ms_users:
        email_key = (user.email or '').lower()
        gu = graph_lookup.get(email_key)

        if not gu:
            print(f"    MISS  {user.username} ({user.email}) - not found in Graph")
            not_found += 1
            continue

        dept = gu.get('department') or None
        title = gu.get('jobTitle') or None
        office = gu.get('officeLocation') or None
        emp_id = gu.get('employeeId') or None

        if (user.department == dept and user.job_title == title
                and user.office_location == office and user.employee_id == emp_id):
            skipped += 1
            continue

        user.department = dept
        user.job_title = title
        user.office_location = office
        user.employee_id = emp_id
        updated += 1
        print(f"    OK    {user.username}: dept={dept}, title={title}, office={office}")

    session.commit()
    session.close()

    print(f"\nDone: {updated} updated, {skipped} unchanged, {not_found} not found in Graph.")
    print("=" * 60)


if __name__ == '__main__':
    run()
