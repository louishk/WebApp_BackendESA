"""
Zoom Call Log Sync Pipeline

Three-phase pipeline:
  A) Fetch call logs from Zoom Phone API
  B) Match calls to SugarCRM contacts/leads by phone number
  C) Push matched calls to SugarCRM as Call records with transcripts

Modes:
- backfill: Pull last 6 months of call logs
- auto: Pull since last sync (default: last 24 hours if first run)

Usage:
    python -m datalayer.zoom_call_log_sync --mode backfill
    python -m datalayer.zoom_call_log_sync --mode auto
"""

import argparse
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from common.call_scorer import score_call
from common.config_loader import get_database_url
from common.models import Base, ZoomCallLog, ZoomContactSync, ZoomSyncState
from common.scoring_config import get_active_config
from common.sugarcrm_client import SugarCRMClient
from common.transcript_formatter import format_as_conversation
from common.zoom_agent_resolver import (
    refresh_agent_mapping,
    get_sugar_user_for_zoom,
    derive_agent_zoom_user_id,
)
from common.zoom_client import ZoomClient, ZoomAPIError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

BATCH_COMMIT_SIZE = 50
BACKFILL_DAYS = 180  # 6 months
DEFAULT_LOOKBACK_HOURS = 24


# =============================================================================
# Phone Normalization (shared with contacts sync)
# =============================================================================

def strip_to_digits(phone: Optional[str]) -> str:
    """Strip a phone string down to digits only (drop leading +)."""
    if not phone:
        return ''
    return re.sub(r'\D', '', phone)


# =============================================================================
# Phase A: Fetch Call Logs from Zoom
# =============================================================================

def fetch_call_logs(
    zoom_client: ZoomClient,
    from_date: datetime,
    to_date: datetime,
) -> List[Dict[str, Any]]:
    """Fetch call logs from Zoom API for the given date range."""
    logger.info("Fetching Zoom call logs from %s to %s",
                from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'))
    return zoom_client.get_all_call_logs(from_date, to_date)


def parse_zoom_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse a Zoom API datetime string to Python datetime."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


def ingest_call_logs(
    pbi_session,
    raw_logs: List[Dict[str, Any]],
) -> Tuple[int, int]:
    """Insert new call logs into zoom_call_logs, dedup by zoom_call_id.

    Returns:
        Tuple of (inserted, skipped).
    """
    inserted = 0
    skipped = 0

    for log in tqdm(raw_logs, desc="  Ingesting call logs", unit="log"):
        call_id = log.get('id', '')
        if not call_id:
            skipped += 1
            continue

        # Skip internal (staff-to-staff) calls — no CRM relevance
        if (log.get('connect_type') or '').lower() == 'internal':
            skipped += 1
            continue

        # Check for existing
        existing = pbi_session.query(ZoomCallLog).filter_by(
            zoom_call_id=call_id,
        ).first()
        if existing:
            skipped += 1
            continue

        # Determine external number (the one to match against CRM)
        # For outbound: external is callee_did_number (caller is internal ext)
        # For inbound:  external is caller_did_number (callee is internal ext)
        direction = (log.get('direction') or '').lower()
        caller_did = log.get('caller_did_number') or log.get('caller_ext_number') or ''
        callee_did = log.get('callee_did_number') or log.get('callee_ext_number') or ''

        # Recording status from API: 'recorded' / 'non_recorded'
        has_recording = log.get('recording_status') == 'recorded'

        pbi_session.add(ZoomCallLog(
            zoom_call_id=call_id,
            direction=direction[:10] if direction else None,
            caller_number=caller_did[:30] if caller_did else None,
            callee_number=callee_did[:30] if callee_did else None,
            caller_name=(log.get('caller_name', '') or '')[:200],
            callee_name=(log.get('callee_name', '') or '')[:200],
            duration=log.get('duration'),
            answer_start=parse_zoom_datetime(log.get('start_time')),
            call_end=parse_zoom_datetime(log.get('end_time')),
            has_recording=has_recording,
            recording_id=None,  # populated separately by joining with /phone/recordings
            sync_status='pending',
            raw_json=log,
        ))
        inserted += 1

        if inserted % BATCH_COMMIT_SIZE == 0:
            pbi_session.commit()

    pbi_session.commit()
    return inserted, skipped


# =============================================================================
# Phase A.5: Enrich with recording metadata
# =============================================================================

def enrich_with_recordings(
    pbi_session,
    zoom_client: ZoomClient,
    from_date: datetime,
    to_date: datetime,
) -> int:
    """Fetch recordings for the date window and join to zoom_call_logs.

    Sets recording_id, download_url, has_recording on matching rows.
    Recordings are joined by call_log_id (Zoom's internal call_log_id field)
    which corresponds to our zoom_call_id (the call history id).

    Returns:
        Number of rows updated with recording metadata.
    """
    logger.info(
        "Fetching Zoom recordings %s to %s",
        from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'),
    )
    # Walk recordings paginated using the same client
    recordings: List[Dict[str, Any]] = []
    next_token = None
    page = 0
    while True:
        page += 1
        params = {
            'from': from_date.strftime('%Y-%m-%d'),
            'to': to_date.strftime('%Y-%m-%d'),
            'page_size': 100,
        }
        if next_token:
            params['next_page_token'] = next_token
        try:
            resp = zoom_client._request('GET', 'phone/recordings', params=params)
            data = resp.json()
        except Exception:
            logger.exception("Failed to fetch recordings page %d", page)
            break
        chunk = data.get('recordings', [])
        if not chunk:
            break
        recordings.extend(chunk)
        next_token = data.get('next_page_token') or ''
        if not next_token:
            break
    logger.info("Fetched %d recordings", len(recordings))

    if not recordings:
        return 0

    # Build lookup by call_id (the underlying call number, shared across legs)
    # Multiple recordings can map to the same call_id; keep the longest one.
    by_call_id: Dict[str, Dict[str, Any]] = {}
    for r in recordings:
        cid = r.get('call_id')
        if not cid:
            continue
        existing = by_call_id.get(cid)
        if not existing or (r.get('duration') or 0) > (existing.get('duration') or 0):
            by_call_id[cid] = r

    logger.info("Indexed %d recordings by call_id", len(by_call_id))

    # Update matching zoom_call_logs rows by call_id stored in raw_json
    updated = 0
    rows = pbi_session.query(ZoomCallLog).filter(
        ZoomCallLog.has_recording == True,
        ZoomCallLog.download_url.is_(None),
    ).all()
    for row in tqdm(rows, desc="  Joining recordings", unit="row"):
        cid = (row.raw_json or {}).get('call_id') if row.raw_json else None
        if not cid:
            continue
        rec = by_call_id.get(cid)
        if not rec:
            continue
        row.recording_id = (rec.get('id') or '')[:100]
        row.download_url = rec.get('download_url')
        updated += 1
        if updated % BATCH_COMMIT_SIZE == 0:
            pbi_session.commit()
    pbi_session.commit()
    return updated


# =============================================================================
# Phase B.5: Transcribe recordings via Azure Whisper
# =============================================================================

def transcribe_recordings(
    pbi_session,
    zoom_client: ZoomClient,
    limit: Optional[int] = None,
) -> Tuple[int, int]:
    """Download audio + run STT for matched call logs that have recordings.

    Updates: transcript_original, transcript_en, detected_language,
    transcript_status, transcript_model, transcript_processed_at.

    Returns:
        Tuple of (transcribed, errors).
    """
    from common.speech_client import WhisperClient, WhisperAPIError
    import requests

    q = pbi_session.query(ZoomCallLog).filter(
        ZoomCallLog.has_recording == True,
        ZoomCallLog.download_url.isnot(None),
        ZoomCallLog.transcript_status.in_(('none', 'pending', 'error')),
        ZoomCallLog.sync_status.in_(('matched', 'pending')),
    )
    if limit:
        q = q.limit(limit)
    targets = q.all()

    if not targets:
        logger.info("No recordings to transcribe")
        return 0, 0

    logger.info("Transcribing %d recordings (3 req/min rate limit)", len(targets))

    whisper = WhisperClient()
    transcribed = 0
    errors = 0

    # Need a Bearer token for downloading audio from Zoom
    zoom_client._ensure_auth()
    zoom_token = zoom_client._token

    for row in tqdm(targets, desc="  Transcribing", unit="call"):
        try:
            row.transcript_status = 'processing'
            pbi_session.commit()

            # Download audio file from Zoom
            dl = requests.get(
                row.download_url,
                headers={'Authorization': f'Bearer {zoom_token}'},
                timeout=120,
            )
            if dl.status_code != 200 or not dl.content:
                logger.warning(
                    "Audio download failed for call %s: HTTP %d",
                    row.zoom_call_id, dl.status_code,
                )
                row.transcript_status = 'error'
                row.error_message = f"Audio download HTTP {dl.status_code}"
                errors += 1
                pbi_session.commit()
                continue

            # Run STT (Whisper handles its own rate limiting internally)
            result = whisper.transcribe_and_translate(
                dl.content, filename=f"{row.zoom_call_id}.mp3",
            )
            row.transcript_original = (result.get('text_original') or '')[:65535]
            row.transcript_en = (result.get('text_en') or '')[:65535]
            row.detected_language = (result.get('language') or '')[:20]
            row.transcript_model = 'whisper'
            row.transcript_status = 'done'
            row.transcript_processed_at = datetime.now(timezone.utc)
            row.error_message = None
            transcribed += 1

        except WhisperAPIError as e:
            logger.warning("Whisper failed for call %s: %s", row.zoom_call_id, e)
            row.transcript_status = 'error'
            row.error_message = 'Whisper API error'
            errors += 1
        except Exception:
            logger.exception("Transcription exception for call %s", row.zoom_call_id)
            row.transcript_status = 'error'
            row.error_message = 'Transcription exception'
            errors += 1

        pbi_session.commit()

    return transcribed, errors


# =============================================================================
# Phase B.6: Score Calls (LLM-based quality + categorization)
# =============================================================================

# Set of dimension keys that have dedicated columns on zoom_call_logs (mirrored
# for fast queries). All other dimensions live only in scores_json.
_FLAT_SCORE_COLUMNS = {
    'quality_overall', 'call_category', 'call_subcategory', 'sentiment',
}


def score_pending_calls(
    pbi_session,
    limit: Optional[int] = None,
) -> Tuple[int, int]:
    """Score calls whose transcript is done but score_status is none/error.

    Reads the current rubric via `scoring_config.get_active_config()` (cached
    for 5 min) and writes the result into both flat columns and scores_json.

    Returns:
        Tuple of (scored, errors).
    """
    q = pbi_session.query(ZoomCallLog).filter(
        ZoomCallLog.transcript_status == 'done',
        ZoomCallLog.transcript_en.isnot(None),
        ZoomCallLog.score_status.in_(('none', 'error')),
    )
    if limit:
        q = q.limit(limit)
    targets = q.all()

    if not targets:
        logger.info("No transcribed calls awaiting scoring")
        return 0, 0

    logger.info("Scoring %d transcribed calls via Grok", len(targets))

    scored = 0
    errors = 0

    for row in tqdm(targets, desc="  Scoring", unit="call"):
        try:
            row.score_status = 'processing'
            pbi_session.commit()

            # Pick agent/customer based on direction
            if row.direction == 'outbound':
                agent_name = row.caller_name or ''
                customer_name = row.callee_name or ''
            else:
                agent_name = row.callee_name or ''
                customer_name = row.caller_name or ''

            result = score_call(
                transcript=row.transcript_en,
                direction=row.direction or 'outbound',
                agent_name=agent_name,
                customer_name=customer_name,
                duration_sec=row.duration or 0,
            )

            if result.get('score_status') == 'error':
                row.score_status = 'error'
                row.score_error = (result.get('score_error') or '')[:1000]
                row.score_model = result.get('score_model')
                errors += 1
                pbi_session.commit()
                continue

            # Store flat columns for fast querying
            row.quality_overall = result.get('quality_overall')
            row.call_category = (result.get('call_category') or '')[:30] or None
            row.call_subcategory = (result.get('call_subcategory') or '')[:100] or None
            row.sentiment = (result.get('sentiment') or '')[:20] or None
            row.score_confidence = result.get('score_confidence')

            # Store the full result (sans status/model) as the audit JSON
            row.scores_json = {k: v for k, v in result.items() if k not in (
                'score_status', 'score_model'
            )}
            row.score_model = result.get('score_model')
            row.score_status = 'done'
            row.score_processed_at = datetime.now(timezone.utc)
            row.score_error = None
            scored += 1

        except Exception:
            logger.exception("Scoring exception for call %s", row.zoom_call_id)
            row.score_status = 'error'
            row.score_error = 'Scoring exception'
            errors += 1

        pbi_session.commit()

    return scored, errors


# =============================================================================
# Phase B: Match Call Logs to CRM Records
# =============================================================================

def match_by_zoom_contacts(
    pbi_session,
    phone: str,
) -> Optional[Tuple[str, str]]:
    """Fast match: look up phone in zoom_contact_sync JSONB phone_numbers.

    Returns (sugar_id, sugar_module) or None.
    """
    if not phone:
        return None

    digits = strip_to_digits(phone)
    if len(digits) < 7:
        return None

    # phone_numbers is a JSONB array of E.164 strings, e.g. ["+6596314175"]
    # Use @> containment with a JSON-encoded array of one string.
    import json
    candidate_e164 = phone if phone.startswith('+') else f'+{digits}'

    result = pbi_session.execute(
        text("""
            SELECT sugar_id, sugar_module
            FROM zoom_contact_sync
            WHERE sync_status = 'synced'
              AND phone_numbers @> :pattern
            LIMIT 1
        """),
        {'pattern': json.dumps([candidate_e164])},
    ).fetchone()

    if result:
        return (result[0], result[1])

    # Fuzzy fallback: match on last-8 digits as a substring of the JSONB text
    result = pbi_session.execute(
        text("""
            SELECT sugar_id, sugar_module
            FROM zoom_contact_sync
            WHERE sync_status = 'synced'
              AND phone_numbers::text LIKE :pattern
            LIMIT 1
        """),
        {'pattern': f'%{digits[-8:]}%'},
    ).fetchone()
    if result:
        return (result[0], result[1])

    return None


def match_by_crm_tables(
    pbi_engine,
    phone: str,
) -> Optional[Tuple[str, str]]:
    """Fallback match: search sugarcrm_contacts and sugarcrm_leads phone columns.

    Returns (sugar_id, module) or None.
    """
    if not phone:
        return None

    digits = strip_to_digits(phone)
    if len(digits) < 6:
        return None

    # Use last 8 digits for matching (handles country code differences)
    search_suffix = digits[-8:]

    phone_fields = ['phone_mobile', 'phone_work', 'phone_home', 'phone_other']

    for module, table in [('Contacts', 'sugarcrm_contacts'), ('Leads', 'sugarcrm_leads')]:
        conditions = ' OR '.join(
            f"REPLACE(REPLACE(REPLACE({f}, '+', ''), ' ', ''), '-', '') LIKE :pattern"
            for f in phone_fields
        )
        query = f"SELECT sugar_id FROM {table} WHERE {conditions} LIMIT 1"

        try:
            with pbi_engine.connect() as conn:
                result = conn.execute(
                    text(query),
                    {'pattern': f'%{search_suffix}'},
                ).fetchone()
                if result:
                    return (result[0], module)
        except Exception:
            logger.warning("Failed to search %s for phone match", table)

    return None


def match_call_logs(
    pbi_session,
    pbi_engine,
    limit: Optional[int] = None,
) -> Tuple[int, int]:
    """Match pending call logs to CRM records.

    Returns:
        Tuple of (matched, no_match).
    """
    q = pbi_session.query(ZoomCallLog).filter_by(sync_status='pending')
    if limit:
        q = q.limit(limit)
    pending = q.all()

    if not pending:
        return 0, 0

    matched = 0
    no_match = 0

    for log in tqdm(pending, desc="  Matching call logs", unit="log"):
        # Try both caller and callee numbers
        numbers_to_try = []
        if log.direction == 'inbound' and log.caller_number:
            numbers_to_try.append(log.caller_number)
        elif log.direction == 'outbound' and log.callee_number:
            numbers_to_try.append(log.callee_number)
        else:
            # Try both
            if log.caller_number:
                numbers_to_try.append(log.caller_number)
            if log.callee_number:
                numbers_to_try.append(log.callee_number)

        found = None
        for number in numbers_to_try:
            # Fast path: zoom_contact_sync JSONB lookup
            found = match_by_zoom_contacts(pbi_session, number)
            if found:
                break
            # Fallback: CRM table scan
            found = match_by_crm_tables(pbi_engine, number)
            if found:
                break

        if found:
            log.matched_sugar_id = found[0]
            log.matched_sugar_module = found[1]
            log.sync_status = 'matched'
            matched += 1
        else:
            log.sync_status = 'no_match'
            no_match += 1

        if (matched + no_match) % BATCH_COMMIT_SIZE == 0:
            pbi_session.commit()

    pbi_session.commit()
    return matched, no_match


# =============================================================================
# Phase C helpers
# =============================================================================

def _add_scores_to_call_data(
    call_data: Dict[str, Any],
    scores_json: Dict[str, Any],
    score_model: Optional[str],
    score_processed_at: Optional[datetime],
) -> None:
    """Map scores_json keys onto SugarCRM custom field names from the rubric.

    Reads the active scoring config to know which dimension keys map to which
    sugar_field. Skips null values so partially-scored rows don't overwrite
    manually-edited fields in SugarCRM. Mutates `call_data` in place.
    """
    try:
        cfg = get_active_config()
    except Exception:
        logger.warning("Could not load scoring config for field mapping; skipping")
        return

    for dim in cfg.get('dimensions', []):
        if not dim.get('enabled', True):
            continue
        key = dim.get('key')
        sugar_field = dim.get('sugar_field')
        if not key or not sugar_field:
            continue
        value = scores_json.get(key)
        if value is None or value == '':
            continue
        # Truncate text fields to declared max_length to be safe
        if dim.get('type') == 'text':
            ml = dim.get('max_length')
            if ml and isinstance(value, str):
                value = value[:ml]
        call_data[sugar_field] = value

    # Metadata fields that aren't part of the rubric dimensions
    if 'quality_overall' in scores_json and scores_json['quality_overall'] is not None:
        call_data['es_zoom_quality_overall_c'] = scores_json['quality_overall']
    if score_model:
        call_data['es_zoom_score_model_c'] = score_model[:50]
    if score_processed_at:
        call_data['es_zoom_score_processed_at_c'] = score_processed_at.strftime(
            '%Y-%m-%dT%H:%M:%S+00:00'
        )


# =============================================================================
# Phase C: Push Matched Calls to SugarCRM
# =============================================================================

def push_to_sugarcrm(
    pbi_session,
    zoom_client: ZoomClient,
    sugar_client: SugarCRMClient,
    limit: Optional[int] = None,
) -> Tuple[int, int]:
    """Push matched call logs to SugarCRM as Call records.

    Downloads transcripts for recordings before pushing.

    Returns:
        Tuple of (pushed, errors).
    """
    q = pbi_session.query(ZoomCallLog).filter_by(sync_status='matched')
    if limit:
        q = q.limit(limit)
    matched_logs = q.all()

    if not matched_logs:
        return 0, 0

    pushed = 0
    errors = 0

    for log in tqdm(matched_logs, desc="  Pushing to SugarCRM", unit="call"):
        # Build SugarCRM Call record
        direction_map = {'inbound': 'Inbound', 'outbound': 'Outbound'}
        sugar_direction = direction_map.get(log.direction, 'Inbound')

        # Format duration
        dur_seconds = log.duration or 0
        hours, remainder = divmod(dur_seconds, 3600)
        minutes, _ = divmod(remainder, 60)

        # Description: clean metadata only (transcript goes in dedicated field)
        description_lines = [
            f"Zoom Phone {log.direction or 'call'}",
            f"From: {log.caller_name or log.caller_number or 'Unknown'}",
            f"To:   {log.callee_name or log.callee_number or 'Unknown'}",
        ]
        if log.has_recording and log.transcript_status == 'pending':
            description_lines.append("(Recording transcription pending)")
        elif log.has_recording and log.transcript_status == 'error':
            description_lines.append("(Recording available — transcription failed)")
        description = '\n'.join(description_lines)

        # Transcript: format as conversation with speaker labels (LLM-powered)
        transcript_field = ''
        if log.transcript_en:
            lang = log.detected_language or 'unknown'
            # Pick agent name based on direction
            if log.direction == 'outbound':
                agent_name = log.caller_name or ''
                customer_name = log.callee_name or ''
            else:
                agent_name = log.callee_name or ''
                customer_name = log.caller_name or ''

            conversation_en = format_as_conversation(
                text=log.transcript_en,
                direction=log.direction or 'outbound',
                agent_name=agent_name,
                customer_name=customer_name,
                language='English',
            )
            transcript_field = f"[Language: {lang}]\n\n=== English (formatted) ===\n{conversation_en}"

            # Include original-language version (raw, not re-formatted to save LLM cost)
            if log.transcript_original and log.transcript_original != log.transcript_en:
                transcript_field += f"\n\n=== Original ({lang}) ===\n{log.transcript_original}"
        elif log.transcript:
            transcript_field = log.transcript

        call_data = {
            'name': f"ZoomSync - {log.caller_name or log.caller_number or 'Unknown'} → "
                    f"{log.callee_name or log.callee_number or 'Unknown'}",
            'direction': sugar_direction,
            'status': 'Held',
            'description': description,
            'duration_hours': hours,
            'duration_minutes': minutes,
        }
        if transcript_field:
            call_data['transcript'] = transcript_field[:65535]

        # Assignment: look up the internal Zoom agent and find their SugarCRM user
        agent_zoom_id = derive_agent_zoom_user_id(log)
        if agent_zoom_id:
            sugar_user_id = get_sugar_user_for_zoom(pbi_session, agent_zoom_id)
            if sugar_user_id:
                call_data['assigned_user_id'] = sugar_user_id

        # Set date_start from answer_start or call_end
        if log.answer_start:
            call_data['date_start'] = log.answer_start.strftime('%Y-%m-%dT%H:%M:%S+00:00')
        elif log.call_end:
            call_data['date_start'] = log.call_end.strftime('%Y-%m-%dT%H:%M:%S+00:00')

        # Link to the matched CRM record
        if log.matched_sugar_module == 'Contacts':
            call_data['parent_type'] = 'Contacts'
            call_data['parent_id'] = log.matched_sugar_id
        elif log.matched_sugar_module == 'Leads':
            call_data['parent_type'] = 'Leads'
            call_data['parent_id'] = log.matched_sugar_id

        # Map LLM scores onto SugarCRM custom fields (rubric-driven)
        if log.score_status == 'done' and log.scores_json:
            _add_scores_to_call_data(call_data, log.scores_json, log.score_model,
                                     log.score_processed_at)

        try:
            result, error = sugar_client.create_record('Calls', call_data)
            if error:
                logger.warning("SugarCRM create_record error for call %s: %s",
                               log.zoom_call_id, error)
                log.sync_status = 'error'
                log.error_message = 'SugarCRM create failed'
                errors += 1
                if (pushed + errors) % BATCH_COMMIT_SIZE == 0:
                    pbi_session.commit()
                continue

            sugar_call_id = result.get('id') if result else None
            log.sugar_call_id = sugar_call_id
            log.sync_status = 'pushed'
            log.error_message = None
            pushed += 1

        except Exception:
            logger.exception("Failed to push call %s to SugarCRM", log.zoom_call_id)
            log.sync_status = 'error'
            log.error_message = 'SugarCRM push exception'
            errors += 1

        if (pushed + errors) % BATCH_COMMIT_SIZE == 0:
            pbi_session.commit()

    pbi_session.commit()
    return pushed, errors


# =============================================================================
# Sync State Management
# =============================================================================

def get_last_sync(pbi_session, sync_name: str) -> Optional[datetime]:
    """Read last successful sync time from zoom_sync_state."""
    state = pbi_session.query(ZoomSyncState).filter_by(sync_name=sync_name).first()
    if state and state.last_success_at:
        return state.last_success_at
    return None


def update_sync_state(pbi_session, sync_name: str, records_processed: int):
    """Update sync state after a successful run."""
    now = datetime.now(timezone.utc)
    state = pbi_session.query(ZoomSyncState).filter_by(sync_name=sync_name).first()
    if state:
        state.last_sync_at = now
        state.last_success_at = now
        state.records_processed = records_processed
        state.updated_at = now
    else:
        pbi_session.add(ZoomSyncState(
            sync_name=sync_name,
            last_sync_at=now,
            last_success_at=now,
            records_processed=records_processed,
        ))
    pbi_session.commit()


# =============================================================================
# Pipeline Runner
# =============================================================================

def run_pipeline(
    mode: str,
    limit: Optional[int] = None,
    days: Optional[int] = None,
    no_push: bool = False,
    skip_transcribe: bool = False,
    transcribe_limit: Optional[int] = None,
    skip_score: bool = False,
    score_limit: Optional[int] = None,
    rescore_all: bool = False,
) -> Dict[str, int]:
    """Run the Zoom call log sync pipeline (fetch, match, push).

    Args:
        mode: 'backfill' (last 6 months) or 'auto' (since last sync).
        limit: Optional cap on records per phase (test mode).
        days: Override fetch window in days.
        no_push: Skip Phase C (push to SugarCRM) — useful for testing fetch+match.

    Returns:
        Summary dict with phase counts.
    """
    pbi_url = get_database_url('pbi')
    pbi_engine = create_engine(pbi_url)

    # Ensure tables exist (in esa_pbi)
    Base.metadata.create_all(pbi_engine, tables=[
        ZoomCallLog.__table__,
        ZoomSyncState.__table__,
    ])

    Session = sessionmaker(bind=pbi_engine)
    session = Session()

    try:
        # Determine time window
        now = datetime.now(timezone.utc)

        window_days = days if days else (BACKFILL_DAYS if mode == 'backfill' else None)
        if window_days:
            from_date = now - timedelta(days=window_days)
            to_date = now
        else:
            last_sync = get_last_sync(session, 'call_log_fetch')
            if last_sync:
                from_date = last_sync
            else:
                from_date = now - timedelta(hours=DEFAULT_LOOKBACK_HOURS)
            to_date = now

        logger.info("Call log window: %s to %s",
                     from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'))

        # Initialize clients
        zoom_client = ZoomClient()

        # --- Phase A: Fetch ---
        print("[STAGE:FETCH] Fetching call logs from Zoom Phone API")
        raw_logs = fetch_call_logs(zoom_client, from_date, to_date)
        print(f"  Fetched {len(raw_logs)} raw call logs")
        if limit:
            raw_logs = raw_logs[:limit]
            print(f"  TEST MODE: limited to first {limit} logs")

        inserted, fetch_skipped = ingest_call_logs(session, raw_logs)
        print(f"  Ingested: {inserted} new, {fetch_skipped} duplicates skipped")

        update_sync_state(session, 'call_log_fetch', inserted)

        # --- Phase A.5: Enrich with recording metadata ---
        print("[STAGE:RECORDINGS] Joining recordings to call logs")
        rec_updated = enrich_with_recordings(session, zoom_client, from_date, to_date)
        print(f"  Recording metadata updated on {rec_updated} rows")

        # --- Phase B: Match ---
        print("[STAGE:MATCH] Matching call logs to CRM records")
        matched, no_match = match_call_logs(session, pbi_engine, limit=limit)
        print(f"  Matched: {matched}, No match: {no_match}")

        # --- Phase B.5: Transcribe matched recordings ---
        transcribed = 0
        transcribe_errors = 0
        if skip_transcribe:
            print("[STAGE:TRANSCRIBE] Skipped (--skip-transcribe)")
        else:
            print("[STAGE:TRANSCRIBE] Running Whisper on matched recordings")
            transcribed, transcribe_errors = transcribe_recordings(
                session, zoom_client, limit=transcribe_limit,
            )
            print(f"  Transcribed: {transcribed}, Errors: {transcribe_errors}")

        # --- Phase B.6: Score transcribed calls ---
        scored = 0
        score_errors = 0
        if skip_score:
            print("[STAGE:SCORE] Skipped (--skip-score)")
        else:
            if rescore_all:
                print("[STAGE:SCORE] --rescore-all: resetting score_status to 'none'")
                session.execute(text(
                    "UPDATE zoom_call_logs SET score_status='none' "
                    "WHERE transcript_status='done' AND score_status='done'"
                ))
                session.commit()

            print("[STAGE:SCORE] Scoring transcribed calls via Grok")
            scored, score_errors = score_pending_calls(session, limit=score_limit)
            print(f"  Scored: {scored}, Errors: {score_errors}")

        pushed = 0
        push_errors = 0

        if no_push:
            print("[STAGE:PUSH] Skipped (--no-push)")
        else:
            # --- Phase C: Push ---
            print("[STAGE:PUSH] Pushing matched calls to SugarCRM")
            sugar_client = SugarCRMClient.from_env()
            if not sugar_client.authenticate():
                logger.error("Failed to authenticate with SugarCRM")
            else:
                # Refresh the Zoom <-> SugarCRM agent mapping (self-healing)
                print("  Refreshing zoom_agent_mapping")
                map_stats = refresh_agent_mapping(pbi_engine, sugar_client=sugar_client)
                print(f"    new={map_stats['new']} updated={map_stats['updated']} "
                      f"unmatched={map_stats['unmatched']} total={map_stats['total']}")

                pushed, push_errors = push_to_sugarcrm(
                    session, zoom_client, sugar_client, limit=limit,
                )
                print(f"  Pushed: {pushed}, Errors: {push_errors}")
                try:
                    sugar_client.logout()
                except Exception:
                    pass

            update_sync_state(session, 'call_log_push', pushed)

        return {
            'fetched': inserted,
            'fetch_skipped': fetch_skipped,
            'matched': matched,
            'no_match': no_match,
            'transcribed': transcribed,
            'transcribe_errors': transcribe_errors,
            'scored': scored,
            'score_errors': score_errors,
            'pushed': pushed,
            'push_errors': push_errors,
        }

    finally:
        session.close()
        pbi_engine.dispose()


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description='Zoom Call Log Sync Pipeline — fetch, match, push to SugarCRM',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m datalayer.zoom_call_log_sync --mode backfill
  python -m datalayer.zoom_call_log_sync --mode auto
        """,
    )
    parser.add_argument(
        '--mode',
        choices=['backfill', 'auto'],
        required=True,
        help='Extraction mode: backfill (last 6 months), auto (since last sync)',
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Cap records per phase (test mode)',
    )
    parser.add_argument(
        '--days',
        type=int,
        default=None,
        help='Override fetch window (days). Default: 180 for backfill, since-last-sync for auto.',
    )
    parser.add_argument(
        '--no-push',
        action='store_true',
        help='Skip Phase C — do not push to SugarCRM (test fetch+match only)',
    )
    parser.add_argument(
        '--skip-transcribe',
        action='store_true',
        help='Skip Phase B.5 — do not call Whisper on recordings',
    )
    parser.add_argument(
        '--transcribe-limit',
        type=int,
        default=None,
        help='Cap the number of recordings to transcribe per run (rate-limit safety)',
    )
    parser.add_argument(
        '--skip-score',
        action='store_true',
        help='Skip Phase B.6 — do not run LLM scoring on transcribed calls',
    )
    parser.add_argument(
        '--score-limit',
        type=int,
        default=None,
        help='Cap the number of calls to score per run',
    )
    parser.add_argument(
        '--rescore-all',
        action='store_true',
        help='Reset score_status=none on all already-scored rows so they get re-scored '
             'with the current rubric',
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 70)
    print("Zoom Call Log Sync Pipeline")
    print("=" * 70)
    print(f"Mode: {args.mode.upper()}")
    print(f"Phases: Fetch -> Match -> Push to SugarCRM")
    print("=" * 70)
    print("[STAGE:INIT] Zoom Call Log Sync")

    if args.limit:
        print(f"Limit: {args.limit} per phase (TEST MODE)")
    if args.no_push:
        print("Push: SKIPPED (--no-push)")
    results = run_pipeline(
        args.mode, limit=args.limit, days=args.days, no_push=args.no_push,
        skip_transcribe=args.skip_transcribe, transcribe_limit=args.transcribe_limit,
        skip_score=args.skip_score, score_limit=args.score_limit,
        rescore_all=args.rescore_all,
    )

    total = sum(results.values())
    print(f"[STAGE:COMPLETE] {total} records processed")
    print("\n" + "=" * 70)
    print("Pipeline completed!")
    print(f"  Fetched:       {results['fetched']} new ({results['fetch_skipped']} dupes skipped)")
    print(f"  Matched:       {results['matched']} ({results['no_match']} unmatched)")
    print(f"  Transcribed:   {results['transcribed']} ({results['transcribe_errors']} errors)")
    print(f"  Scored:        {results['scored']} ({results['score_errors']} errors)")
    print(f"  Pushed:        {results['pushed']} ({results['push_errors']} errors)")
    print("=" * 70)


if __name__ == "__main__":
    main()
