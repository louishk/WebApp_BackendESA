"""
OutlookCalendarPipeline — extract calendar events from Outlook mailboxes
via Microsoft Graph (client credentials flow) and upsert to esa_pbi.calendar_events.

Requires: Calendars.Read APPLICATION permission on the Azure AD app registration.

Modes:
  - auto (default): incremental sync from last updated_at per mailbox
  - backfill:       full history fetch regardless of prior sync state

Scope keys honoured (all optional):
  - mode:    'auto' | 'backfill'   default 'auto'
  - mailbox: 'user@domain'         restrict to a single mailbox
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from sqlalchemy import text

from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)

GRAPH_BASE = 'https://graph.microsoft.com/v1.0'
EVENT_SELECT = ','.join([
    'id', 'subject', 'organizer', 'start', 'end', 'location',
    'isAllDay', 'isCancelled', 'responseStatus', 'sensitivity',
    'showAs', 'categories', 'attendees', 'recurrence',
    'bodyPreview', 'webLink', 'createdDateTime', 'lastModifiedDateTime',
])


def get_graph_token(tenant_id: str, client_id: str, client_secret: str) -> str:
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


def fetch_calendar_events(token: str, mailbox: str, modified_since: Optional[str] = None) -> List[Dict]:
    """Fetch all calendar events for a mailbox, with pagination.

    Args:
        token: Graph API access token
        mailbox: Email address of the mailbox
        modified_since: Optional ISO timestamp for incremental sync

    Returns:
        List of event dicts from Graph API
    """
    headers = {'Authorization': f'Bearer {token}'}
    url = f"{GRAPH_BASE}/users/{mailbox}/calendar/events"
    params: Optional[Dict] = {
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
        logger.info("outlook_calendar %s page %d: %d events (total: %d)", mailbox, page, len(batch), len(events))
        url = data.get('@odata.nextLink')
        params = None  # nextLink already includes params

    return events


def _parse_graph_datetime(dt_obj: Optional[Dict]) -> Optional[datetime]:
    """Parse Graph API dateTime object {dateTime, timeZone} to datetime."""
    if not dt_obj or not dt_obj.get('dateTime'):
        return None
    try:
        dt_str = dt_obj['dateTime']
        if dt_str.endswith('Z'):
            dt_str = dt_str[:-1] + '+00:00'
        return datetime.fromisoformat(dt_str)
    except (ValueError, TypeError):
        return None


def _parse_iso(iso_str: Optional[str]) -> Optional[datetime]:
    """Parse ISO 8601 datetime string."""
    if not iso_str:
        return None
    try:
        return datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


def transform_event(event: Dict, mailbox: str) -> Dict:
    """Transform a Graph API event into a database record dict."""
    organizer = event.get('organizer', {})
    organizer_email = organizer.get('emailAddress', {}).get('address') if organizer else None

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
        'start_time': _parse_graph_datetime(event.get('start', {})),
        'end_time': _parse_graph_datetime(event.get('end', {})),
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


def get_last_updated(engine, mailbox: str) -> Optional[str]:
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


def upsert_events(engine, records: List[Dict]) -> int:
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


def run(mode: str = 'auto', mailbox: Optional[str] = None) -> Dict[str, Any]:
    """Fetch calendar events from Graph API and upsert to esa_pbi.calendar_events.

    Returns {'records': int, 'mailboxes': [...]}
    """
    from common.config_loader import get_config
    from common.db import get_engine

    config = get_config()
    ms = config.oauth.microsoft

    if not ms or not ms.enabled:
        raise ValueError("Microsoft OAuth not configured or not enabled")

    cal_config = ms.calendar_extract
    if not cal_config or not cal_config.mailboxes:
        raise ValueError("No mailboxes configured in oauth.yaml > microsoft > calendar_extract")

    mailboxes = cal_config.mailboxes

    if mailbox:
        if mailbox not in mailboxes:
            raise ValueError(f"{mailbox} not in configured mailbox list")
        mailboxes = [mailbox]

    logger.info("outlook_calendar mode=%s mailboxes=%d", mode, len(mailboxes))

    token = get_graph_token(ms.tenant_id, ms.client_id, ms.client_secret_vault)
    logger.info("outlook_calendar: Graph token acquired")

    engine = get_engine('pbi')

    total_count = 0
    mailboxes_summary = []

    for mb in mailboxes:
        logger.info("outlook_calendar processing: %s", mb)

        modified_since = None
        if mode == 'auto':
            modified_since = get_last_updated(engine, mb)
            if modified_since:
                logger.info("outlook_calendar %s incremental since %s", mb, modified_since)
            else:
                logger.info("outlook_calendar %s no prior data — full fetch", mb)

        try:
            events = fetch_calendar_events(token, mb, modified_since)
            logger.info("outlook_calendar %s fetched %d events", mb, len(events))
        except requests.exceptions.HTTPError as e:
            logger.error("outlook_calendar failed to fetch events for %s: %s", mb, e)
            mailboxes_summary.append({'mailbox': mb, 'records': 0, 'error': str(e.response.status_code)})
            continue

        if not events:
            logger.info("outlook_calendar %s no events to process", mb)
            mailboxes_summary.append({'mailbox': mb, 'records': 0})
            continue

        records = [transform_event(ev, mb) for ev in events]
        count = upsert_events(engine, records)
        logger.info("outlook_calendar %s upserted %d records", mb, count)
        total_count += count
        mailboxes_summary.append({'mailbox': mb, 'records': count})

    return {'records': total_count, 'mailboxes': mailboxes_summary, 'mode': mode}


class OutlookCalendarPipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode = scope.get('mode', 'auto')
        mailbox = scope.get('mailbox')

        result = run(mode=mode, mailbox=mailbox)

        return RunResult(
            status='refreshed',
            records=result['records'],
            scope=scope,
            metadata={'mode': mode, 'mailboxes': result['mailboxes']},
        )
