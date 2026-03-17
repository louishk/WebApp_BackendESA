"""
Outlook Calendar Events to SQL Pipeline

Extracts calendar events from configured Outlook mailboxes via Microsoft Graph API
(client credentials flow) and upserts into the calendar_events table in esa_pbi.

Requires: Calendars.Read APPLICATION permission on the Azure AD app registration.

Usage:
    cd backend/python
    python -m datalayer.calendar_to_sql --mode auto
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.config_loader import get_config, get_database_url

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

GRAPH_BASE = 'https://graph.microsoft.com/v1.0'
EVENT_SELECT = ','.join([
    'id', 'subject', 'organizer', 'start', 'end', 'location',
    'isAllDay', 'isCancelled', 'responseStatus', 'sensitivity',
    'showAs', 'categories', 'attendees', 'recurrence',
    'bodyPreview', 'webLink', 'createdDateTime', 'lastModifiedDateTime',
])


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


def fetch_calendar_events(token, mailbox, modified_since=None):
    """
    Fetch all calendar events for a mailbox, with pagination.

    Args:
        token: Graph API access token
        mailbox: Email address of the mailbox
        modified_since: Optional ISO timestamp for incremental sync

    Returns:
        List of event dicts from Graph API
    """
    headers = {'Authorization': f'Bearer {token}'}
    url = f"{GRAPH_BASE}/users/{mailbox}/calendar/events"
    params = {
        '$select': EVENT_SELECT,
        '$top': '100',
        '$orderby': 'lastModifiedDateTime desc',
    }

    if modified_since:
        params['$filter'] = f"lastModifiedDateTime ge {modified_since}"

    events = []
    page = 0
    while url:
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get('value', [])
        events.extend(batch)
        page += 1
        print(f"      Page {page}: {len(batch)} events (total: {len(events)})", flush=True)
        url = data.get('@odata.nextLink')
        params = None  # nextLink already includes params

    return events


def transform_event(event, mailbox):
    """Transform a Graph API event into a database record dict."""
    organizer = event.get('organizer', {})
    organizer_email = organizer.get('emailAddress', {}).get('address') if organizer else None

    start = event.get('start', {})
    end = event.get('end', {})

    location = event.get('location', {})
    location_str = location.get('displayName') if location else None

    categories = event.get('categories') or []
    categories_str = ', '.join(categories) if categories else None

    attendees = event.get('attendees') or []

    response_status = event.get('responseStatus', {})
    response_str = response_status.get('response') if response_status else None

    return {
        'event_id': event.get('id'),
        'mailbox': mailbox,
        'subject': event.get('subject'),
        'organizer': organizer_email,
        'start_time': _parse_graph_datetime(start),
        'end_time': _parse_graph_datetime(end),
        'location': location_str,
        'is_all_day': event.get('isAllDay', False),
        'is_cancelled': event.get('isCancelled', False),
        'response_status': response_str,
        'sensitivity': event.get('sensitivity'),
        'show_as': event.get('showAs'),
        'categories': categories_str,
        'attendees_count': len(attendees),
        'is_recurring': event.get('recurrence') is not None,
        'body_preview': event.get('bodyPreview'),
        'web_link': event.get('webLink'),
        'created_at': _parse_iso(event.get('createdDateTime')),
        'updated_at': _parse_iso(event.get('lastModifiedDateTime')),
        'synced_at': datetime.now(timezone.utc),
    }


def _parse_graph_datetime(dt_obj):
    """Parse Graph API dateTime object {dateTime, timeZone} to timezone-aware datetime."""
    if not dt_obj or not dt_obj.get('dateTime'):
        return None
    try:
        dt_str = dt_obj['dateTime']
        # Graph returns ISO format without timezone suffix; timeZone field indicates zone
        # For simplicity, parse as-is (usually UTC or local)
        if dt_str.endswith('Z'):
            dt_str = dt_str[:-1] + '+00:00'
        return datetime.fromisoformat(dt_str)
    except (ValueError, TypeError):
        return None


def _parse_iso(iso_str):
    """Parse ISO 8601 datetime string."""
    if not iso_str:
        return None
    try:
        return datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


def get_last_updated(engine, mailbox):
    """Get the max updated_at timestamp for a mailbox from the DB."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT MAX(updated_at) FROM calendar_events WHERE mailbox = :mailbox"),
            {'mailbox': mailbox}
        )
        row = result.fetchone()
        if row and row[0]:
            return row[0].isoformat()
    return None


def upsert_events(engine, records):
    """Upsert event records into calendar_events table."""
    if not records:
        return 0

    upsert_sql = text("""
        INSERT INTO calendar_events (
            event_id, mailbox, subject, organizer, start_time, end_time,
            location, is_all_day, is_cancelled, response_status, sensitivity,
            show_as, categories, attendees_count, is_recurring, body_preview,
            web_link, created_at, updated_at, synced_at
        ) VALUES (
            :event_id, :mailbox, :subject, :organizer, :start_time, :end_time,
            :location, :is_all_day, :is_cancelled, :response_status, :sensitivity,
            :show_as, :categories, :attendees_count, :is_recurring, :body_preview,
            :web_link, :created_at, :updated_at, :synced_at
        )
        ON CONFLICT (event_id, mailbox) DO UPDATE SET
            subject = EXCLUDED.subject,
            organizer = EXCLUDED.organizer,
            start_time = EXCLUDED.start_time,
            end_time = EXCLUDED.end_time,
            location = EXCLUDED.location,
            is_all_day = EXCLUDED.is_all_day,
            is_cancelled = EXCLUDED.is_cancelled,
            response_status = EXCLUDED.response_status,
            sensitivity = EXCLUDED.sensitivity,
            show_as = EXCLUDED.show_as,
            categories = EXCLUDED.categories,
            attendees_count = EXCLUDED.attendees_count,
            is_recurring = EXCLUDED.is_recurring,
            body_preview = EXCLUDED.body_preview,
            web_link = EXCLUDED.web_link,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            synced_at = EXCLUDED.synced_at
    """)

    count = 0
    with engine.begin() as conn:
        for i in range(0, len(records), 100):
            chunk = records[i:i + 100]
            for record in chunk:
                conn.execute(upsert_sql, record)
            count += len(chunk)

    return count


def parse_args():
    parser = argparse.ArgumentParser(
        description='Outlook Calendar Events to SQL Pipeline',
    )
    parser.add_argument(
        '--mode',
        choices=['backfill', 'auto'],
        default='auto',
        help='backfill = full history, auto = incremental from last sync'
    )
    parser.add_argument(
        '--mailbox',
        help='Process only this mailbox (must be in oauth.yaml list)'
    )
    return parser.parse_args()


def main():
    args = parse_args()

    config = get_config()
    ms = config.oauth.microsoft

    if not ms or not ms.enabled:
        print("ERROR: Microsoft OAuth not configured")
        sys.exit(1)

    tenant_id = ms.tenant_id
    client_id = ms.client_id
    client_secret = ms.client_secret_vault

    cal_config = ms.calendar_extract
    if not cal_config or not cal_config.mailboxes:
        print("ERROR: No mailboxes configured in oauth.yaml > microsoft > calendar_extract")
        sys.exit(1)

    mailboxes = cal_config.mailboxes

    if args.mailbox:
        if args.mailbox not in mailboxes:
            print(f"ERROR: {args.mailbox} not in configured mailbox list")
            sys.exit(1)
        mailboxes = [args.mailbox]

    print("=" * 70)
    print("Outlook Calendar Events to SQL Pipeline")
    print("=" * 70)
    print(f"Mode: {args.mode.upper()}")
    print(f"Mailboxes: {len(mailboxes)}")
    print("=" * 70)
    print("[STAGE:INIT] CalendarEvents")

    # Get Graph token
    print("\n[1] Acquiring Graph API token...")
    try:
        token = get_graph_token(tenant_id, client_id, client_secret)
        print("    Token acquired.")
    except Exception as e:
        logger.error(f"Failed to get Graph token: {e}")
        print("    FAILED to acquire token.")
        print("    Ensure Calendars.Read APPLICATION permission is granted + admin consent.")
        sys.exit(1)

    # Connect to esa_pbi
    db_url = get_database_url('pbi')
    engine = create_engine(db_url)

    total_count = 0

    print("[STAGE:FETCH] Fetching calendar events from Graph API")
    for idx, mailbox in enumerate(mailboxes, 1):
        print(f"\n[{idx + 1}] Processing: {mailbox}", flush=True)

        # Determine incremental filter
        modified_since = None
        if args.mode == 'auto':
            modified_since = get_last_updated(engine, mailbox)
            if modified_since:
                print(f"    Incremental since: {modified_since}", flush=True)
            else:
                print("    No previous data — full fetch", flush=True)

        # Fetch events
        try:
            events = fetch_calendar_events(token, mailbox, modified_since)
            print(f"    Fetched {len(events)} events from Graph API", flush=True)
        except requests.exceptions.HTTPError as e:
            logger.error(f"Failed to fetch events for {mailbox}: {e}")
            print(f"    FAILED: HTTP {e.response.status_code}")
            try:
                print(f"    Response: {e.response.json()}")
            except Exception:
                print(f"    Response: {e.response.text[:500]}")
            continue

        if not events:
            print("    No events to process")
            continue

        # Transform
        records = [transform_event(ev, mailbox) for ev in events]

        # Upsert
        print("[STAGE:PUSH] Upserting to PostgreSQL")
        count = upsert_events(engine, records)
        print(f"    Upserted {count} records")
        total_count += count

    engine.dispose()

    print(f"[STAGE:COMPLETE] {total_count} records")
    print("\n" + "=" * 70)
    print(f"TOTAL: {total_count} records")
    print("=" * 70)


if __name__ == "__main__":
    main()
