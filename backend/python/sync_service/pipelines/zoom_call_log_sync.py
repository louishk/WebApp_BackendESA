"""
ZoomCallLogSyncPipeline — fetch Zoom Phone call logs, match to SugarCRM, push as Calls.

Phases per run:
  A   Fetch call logs (Zoom Phone API)
  A.5 Enrich with recording metadata (Zoom /phone/recordings)
  B   Match calls to SugarCRM contacts/leads by phone
  B.5 Transcribe recordings (Azure Whisper)
  B.6 Score transcripts (LLM rubric)
  C   Push matched calls into SugarCRM (Calls module)

Modes:
  - auto (default): window since last successful sync, fallback 24h
  - backfill:       last BACKFILL_DAYS (180) days

Scope keys honoured (all optional):
  - mode: 'auto' | 'backfill'
  - limit: int                   cap records per phase
  - days: int                    override fetch window
  - no_push: bool
  - skip_transcribe: bool
  - transcribe_limit: int
  - skip_score: bool
  - score_limit: int
  - rescore_all: bool
"""

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)

BATCH_COMMIT_SIZE = 50
BACKFILL_DAYS = 180
DEFAULT_LOOKBACK_HOURS = 24


def strip_to_digits(phone: Optional[str]) -> str:
    if not phone:
        return ''
    return re.sub(r'\D', '', phone)


def parse_zoom_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Phase A — fetch + ingest
# ---------------------------------------------------------------------------

def fetch_call_logs(zoom_client, from_date: datetime, to_date: datetime) -> List[Dict[str, Any]]:
    logger.info("zoom_call_log: fetching %s → %s",
                from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'))
    return zoom_client.get_all_call_logs(from_date, to_date)


def ingest_call_logs(pbi_session, raw_logs: List[Dict[str, Any]]) -> Tuple[int, int]:
    from common.models import ZoomCallLog

    inserted = 0
    skipped = 0

    for log in raw_logs:
        call_id = log.get('id', '')
        if not call_id:
            skipped += 1
            continue
        if (log.get('connect_type') or '').lower() == 'internal':
            skipped += 1
            continue

        existing = pbi_session.query(ZoomCallLog).filter_by(zoom_call_id=call_id).first()
        if existing:
            skipped += 1
            continue

        direction = (log.get('direction') or '').lower()
        caller_did = log.get('caller_did_number') or log.get('caller_ext_number') or ''
        callee_did = log.get('callee_did_number') or log.get('callee_ext_number') or ''
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
            recording_id=None,
            sync_status='pending',
            raw_json=log,
        ))
        inserted += 1
        if inserted % BATCH_COMMIT_SIZE == 0:
            pbi_session.commit()

    pbi_session.commit()
    return inserted, skipped


# ---------------------------------------------------------------------------
# Phase A.5 — recording metadata enrichment
# ---------------------------------------------------------------------------

def enrich_with_recordings(pbi_session, zoom_client, from_date: datetime, to_date: datetime) -> int:
    from common.models import ZoomCallLog

    logger.info("zoom_call_log: fetching recordings %s → %s",
                from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'))

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
            logger.exception("zoom_call_log: failed recordings page %d", page)
            break
        chunk = data.get('recordings', [])
        if not chunk:
            break
        recordings.extend(chunk)
        next_token = data.get('next_page_token') or ''
        if not next_token:
            break
    logger.info("zoom_call_log: fetched %d recordings", len(recordings))

    if not recordings:
        return 0

    by_call_id: Dict[str, Dict[str, Any]] = {}
    for r in recordings:
        cid = r.get('call_id')
        if not cid:
            continue
        existing = by_call_id.get(cid)
        if not existing or (r.get('duration') or 0) > (existing.get('duration') or 0):
            by_call_id[cid] = r

    updated = 0
    rows = pbi_session.query(ZoomCallLog).filter(
        ZoomCallLog.has_recording == True,
        ZoomCallLog.download_url.is_(None),
    ).all()
    for row in rows:
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


# ---------------------------------------------------------------------------
# Phase B — match call logs to CRM
# ---------------------------------------------------------------------------

def match_by_zoom_contacts(pbi_session, phone: str) -> Optional[Tuple[str, str]]:
    if not phone:
        return None
    digits = strip_to_digits(phone)
    if len(digits) < 7:
        return None

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


def match_by_crm_tables(pbi_engine, phone: str) -> Optional[Tuple[str, str]]:
    if not phone:
        return None
    digits = strip_to_digits(phone)
    if len(digits) < 6:
        return None
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
                    text(query), {'pattern': f'%{search_suffix}'},
                ).fetchone()
                if result:
                    return (result[0], module)
        except Exception:
            logger.warning("zoom_call_log: failed to search %s", table)
    return None


def match_call_logs(pbi_session, pbi_engine, limit: Optional[int] = None) -> Tuple[int, int]:
    from common.models import ZoomCallLog

    q = pbi_session.query(ZoomCallLog).filter_by(sync_status='pending')
    if limit:
        q = q.limit(limit)
    pending = q.all()
    if not pending:
        return 0, 0

    matched = 0
    no_match = 0
    for log in pending:
        numbers_to_try: List[str] = []
        if log.direction == 'inbound' and log.caller_number:
            numbers_to_try.append(log.caller_number)
        elif log.direction == 'outbound' and log.callee_number:
            numbers_to_try.append(log.callee_number)
        else:
            if log.caller_number:
                numbers_to_try.append(log.caller_number)
            if log.callee_number:
                numbers_to_try.append(log.callee_number)

        found = None
        for number in numbers_to_try:
            found = match_by_zoom_contacts(pbi_session, number)
            if found:
                break
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


# ---------------------------------------------------------------------------
# Phase B.5 — transcribe recordings
# ---------------------------------------------------------------------------

def transcribe_recordings(pbi_session, zoom_client, limit: Optional[int] = None) -> Tuple[int, int]:
    from common.models import ZoomCallLog
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
        logger.info("zoom_call_log: no recordings to transcribe")
        return 0, 0

    logger.info("zoom_call_log: transcribing %d recordings", len(targets))
    whisper = WhisperClient()
    transcribed = 0
    errors = 0

    zoom_client._ensure_auth()
    zoom_token = zoom_client._token

    for row in targets:
        try:
            row.transcript_status = 'processing'
            pbi_session.commit()

            dl = requests.get(
                row.download_url,
                headers={'Authorization': f'Bearer {zoom_token}'},
                timeout=120,
            )
            if dl.status_code != 200 or not dl.content:
                logger.warning("zoom_call_log: audio dl failed call=%s http=%d",
                               row.zoom_call_id, dl.status_code)
                row.transcript_status = 'error'
                row.error_message = f"Audio download HTTP {dl.status_code}"
                errors += 1
                pbi_session.commit()
                continue

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
            logger.warning("zoom_call_log: whisper failed call=%s err=%s", row.zoom_call_id, e)
            row.transcript_status = 'error'
            row.error_message = 'Whisper API error'
            errors += 1
        except Exception:
            logger.exception("zoom_call_log: transcribe exception call=%s", row.zoom_call_id)
            row.transcript_status = 'error'
            row.error_message = 'Transcription exception'
            errors += 1

        pbi_session.commit()

    return transcribed, errors


# ---------------------------------------------------------------------------
# Phase B.6 — LLM scoring
# ---------------------------------------------------------------------------

_FLAT_SCORE_COLUMNS = {
    'quality_overall', 'call_category', 'call_subcategory', 'sentiment',
}


def score_pending_calls(pbi_session, limit: Optional[int] = None) -> Tuple[int, int]:
    from common.models import ZoomCallLog
    from common.call_scorer import score_call

    q = pbi_session.query(ZoomCallLog).filter(
        ZoomCallLog.transcript_status == 'done',
        ZoomCallLog.transcript_en.isnot(None),
        ZoomCallLog.score_status.in_(('none', 'error')),
    )
    if limit:
        q = q.limit(limit)
    targets = q.all()
    if not targets:
        logger.info("zoom_call_log: no calls awaiting scoring")
        return 0, 0

    logger.info("zoom_call_log: scoring %d calls", len(targets))
    scored = 0
    errors = 0

    for row in targets:
        try:
            row.score_status = 'processing'
            pbi_session.commit()

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

            row.quality_overall = result.get('quality_overall')
            row.call_category = (result.get('call_category') or '')[:30] or None
            row.call_subcategory = (result.get('call_subcategory') or '')[:100] or None
            row.sentiment = (result.get('sentiment') or '')[:20] or None
            row.score_confidence = result.get('score_confidence')

            row.scores_json = {k: v for k, v in result.items()
                               if k not in ('score_status', 'score_model')}
            row.score_model = result.get('score_model')
            row.score_status = 'done'
            row.score_processed_at = datetime.now(timezone.utc)
            row.score_error = None
            scored += 1

        except Exception:
            logger.exception("zoom_call_log: scoring exception call=%s", row.zoom_call_id)
            row.score_status = 'error'
            row.score_error = 'Scoring exception'
            errors += 1

        pbi_session.commit()
    return scored, errors


# ---------------------------------------------------------------------------
# Phase C — push to SugarCRM
# ---------------------------------------------------------------------------

def _add_scores_to_call_data(call_data: Dict[str, Any], scores_json: Dict[str, Any],
                             score_model: Optional[str],
                             score_processed_at: Optional[datetime]) -> None:
    from common.scoring_config import get_active_config

    try:
        cfg = get_active_config()
    except Exception:
        logger.warning("zoom_call_log: scoring config load failed; skipping field map")
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
        if dim.get('type') == 'text':
            ml = dim.get('max_length')
            if ml and isinstance(value, str):
                value = value[:ml]
        call_data[sugar_field] = value

    if 'quality_overall' in scores_json and scores_json['quality_overall'] is not None:
        call_data['es_zoom_quality_overall_c'] = scores_json['quality_overall']
    if score_model:
        call_data['es_zoom_score_model_c'] = score_model[:50]
    if score_processed_at:
        call_data['es_zoom_score_processed_at_c'] = score_processed_at.strftime(
            '%Y-%m-%dT%H:%M:%S+00:00'
        )


def push_to_sugarcrm(pbi_session, zoom_client, sugar_client, limit: Optional[int] = None) -> Tuple[int, int]:
    from common.models import ZoomCallLog
    from common.transcript_formatter import format_as_conversation
    from common.zoom_agent_resolver import get_sugar_user_for_zoom, derive_agent_zoom_user_id

    q = pbi_session.query(ZoomCallLog).filter_by(sync_status='matched')
    if limit:
        q = q.limit(limit)
    matched_logs = q.all()
    if not matched_logs:
        return 0, 0

    pushed = 0
    errors = 0
    direction_map = {'inbound': 'Inbound', 'outbound': 'Outbound'}

    for log in matched_logs:
        sugar_direction = direction_map.get(log.direction, 'Inbound')
        dur_seconds = log.duration or 0
        hours, remainder = divmod(dur_seconds, 3600)
        minutes, _ = divmod(remainder, 60)

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

        transcript_field = ''
        if log.transcript_en:
            lang = log.detected_language or 'unknown'
            if log.direction == 'outbound':
                agent_name = log.caller_name or ''
                customer_name = log.callee_name or ''
            else:
                agent_name = log.callee_name or ''
                customer_name = log.caller_name or ''

            conversation_en = format_as_conversation(
                text=log.transcript_en, direction=log.direction or 'outbound',
                agent_name=agent_name, customer_name=customer_name, language='English',
            )
            transcript_field = (
                f"[Language: {lang}]\n\n=== English (formatted) ===\n{conversation_en}"
            )
            if log.transcript_original and log.transcript_original != log.transcript_en:
                transcript_field += f"\n\n=== Original ({lang}) ===\n{log.transcript_original}"
        elif getattr(log, 'transcript', None):
            transcript_field = log.transcript

        call_data: Dict[str, Any] = {
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

        agent_zoom_id = derive_agent_zoom_user_id(log)
        if agent_zoom_id:
            sugar_user_id = get_sugar_user_for_zoom(pbi_session, agent_zoom_id)
            if sugar_user_id:
                call_data['assigned_user_id'] = sugar_user_id

        if log.answer_start:
            call_data['date_start'] = log.answer_start.strftime('%Y-%m-%dT%H:%M:%S+00:00')
        elif log.call_end:
            call_data['date_start'] = log.call_end.strftime('%Y-%m-%dT%H:%M:%S+00:00')

        if log.matched_sugar_module in ('Contacts', 'Leads'):
            call_data['parent_type'] = log.matched_sugar_module
            call_data['parent_id'] = log.matched_sugar_id

        if log.score_status == 'done' and log.scores_json:
            _add_scores_to_call_data(call_data, log.scores_json, log.score_model,
                                     log.score_processed_at)

        try:
            result, error = sugar_client.create_record('Calls', call_data)
            if error:
                logger.warning("zoom_call_log: SugarCRM create failed call=%s err=%s",
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
            logger.exception("zoom_call_log: SugarCRM push exception call=%s", log.zoom_call_id)
            log.sync_status = 'error'
            log.error_message = 'SugarCRM push exception'
            errors += 1

        if (pushed + errors) % BATCH_COMMIT_SIZE == 0:
            pbi_session.commit()

    pbi_session.commit()
    return pushed, errors


# ---------------------------------------------------------------------------
# Sync state
# ---------------------------------------------------------------------------

def get_last_sync(pbi_session, sync_name: str) -> Optional[datetime]:
    from common.models import ZoomSyncState
    state = pbi_session.query(ZoomSyncState).filter_by(sync_name=sync_name).first()
    if state and state.last_success_at:
        return state.last_success_at
    return None


def update_sync_state(pbi_session, sync_name: str, records_processed: int):
    from common.models import ZoomSyncState
    now = datetime.now(timezone.utc)
    state = pbi_session.query(ZoomSyncState).filter_by(sync_name=sync_name).first()
    if state:
        state.last_sync_at = now
        state.last_success_at = now
        state.records_processed = records_processed
        state.updated_at = now
    else:
        pbi_session.add(ZoomSyncState(
            sync_name=sync_name, last_sync_at=now, last_success_at=now,
            records_processed=records_processed,
        ))
    pbi_session.commit()


# ---------------------------------------------------------------------------
# Top-level runner
# ---------------------------------------------------------------------------

def run(mode: str = 'auto', limit: Optional[int] = None, days: Optional[int] = None,
        no_push: bool = False, skip_transcribe: bool = False,
        transcribe_limit: Optional[int] = None, skip_score: bool = False,
        score_limit: Optional[int] = None, rescore_all: bool = False) -> Dict[str, int]:
    from common.db import get_engine, get_session
    from common.models import Base, ZoomCallLog, ZoomSyncState
    from common.sugarcrm_client import SugarCRMClient
    from common.zoom_agent_resolver import refresh_agent_mapping
    from common.zoom_client import ZoomClient

    pbi_engine = get_engine('pbi')
    Base.metadata.create_all(pbi_engine, tables=[
        ZoomCallLog.__table__, ZoomSyncState.__table__,
    ])
    session = get_session('pbi')

    try:
        now = datetime.now(timezone.utc)
        window_days = days if days else (BACKFILL_DAYS if mode == 'backfill' else None)
        if window_days:
            from_date = now - timedelta(days=window_days)
            to_date = now
        else:
            last_sync = get_last_sync(session, 'call_log_fetch')
            from_date = last_sync if last_sync else now - timedelta(hours=DEFAULT_LOOKBACK_HOURS)
            to_date = now

        logger.info("zoom_call_log: window %s → %s",
                    from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'))

        zoom_client = ZoomClient()

        raw_logs = fetch_call_logs(zoom_client, from_date, to_date)
        if limit:
            raw_logs = raw_logs[:limit]
        inserted, fetch_skipped = ingest_call_logs(session, raw_logs)
        logger.info("zoom_call_log: ingested=%d skipped=%d", inserted, fetch_skipped)
        update_sync_state(session, 'call_log_fetch', inserted)

        rec_updated = enrich_with_recordings(session, zoom_client, from_date, to_date)
        logger.info("zoom_call_log: recordings_enriched=%d", rec_updated)

        matched, no_match = match_call_logs(session, pbi_engine, limit=limit)
        logger.info("zoom_call_log: matched=%d no_match=%d", matched, no_match)

        transcribed = 0
        transcribe_errors = 0
        if not skip_transcribe:
            transcribed, transcribe_errors = transcribe_recordings(
                session, zoom_client, limit=transcribe_limit,
            )

        scored = 0
        score_errors = 0
        if not skip_score:
            if rescore_all:
                session.execute(text(
                    "UPDATE zoom_call_logs SET score_status='none' "
                    "WHERE transcript_status='done' AND score_status='done'"
                ))
                session.commit()
            scored, score_errors = score_pending_calls(session, limit=score_limit)

        pushed = 0
        push_errors = 0
        if not no_push:
            sugar_client = SugarCRMClient.from_env()
            if not sugar_client.authenticate():
                logger.error("zoom_call_log: SugarCRM auth failed")
            else:
                map_stats = refresh_agent_mapping(pbi_engine, sugar_client=sugar_client)
                logger.info("zoom_call_log: agent_map new=%d updated=%d unmatched=%d total=%d",
                            map_stats['new'], map_stats['updated'],
                            map_stats['unmatched'], map_stats['total'])
                pushed, push_errors = push_to_sugarcrm(
                    session, zoom_client, sugar_client, limit=limit,
                )
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


class ZoomCallLogSyncPipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode = scope.get('mode', 'auto')
        result = run(
            mode=mode,
            limit=scope.get('limit'),
            days=scope.get('days'),
            no_push=bool(scope.get('no_push', False)),
            skip_transcribe=bool(scope.get('skip_transcribe', False)),
            transcribe_limit=scope.get('transcribe_limit'),
            skip_score=bool(scope.get('skip_score', False)),
            score_limit=scope.get('score_limit'),
            rescore_all=bool(scope.get('rescore_all', False)),
        )
        records = (result.get('fetched', 0) + result.get('matched', 0)
                   + result.get('transcribed', 0) + result.get('scored', 0)
                   + result.get('pushed', 0))
        return RunResult(
            status='refreshed',
            records=records,
            scope=scope,
            metadata=result,
        )
