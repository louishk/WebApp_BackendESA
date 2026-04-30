"""
Lease Follow-up Job Queue — DLQ + executor for post-MoveIn SOAP calls.

Two callers:
  - /api/reservations/move-in handler — enqueues jobs after SOAP MoveIn
    succeeds, then attempts to execute pending jobs for THIS lease inline
    before responding to the bot. Bot waits ~2-3s on the happy path.
  - backend-scheduler worker — every 10s, drains pending jobs across all
    leases, retrying with exponential backoff. After 5 attempts, marks
    status='failed_permanent' and alerts ops via alert_manager.

SOAP calls are dispatched via _execute_action(), keyed on action_type:
  - 'prepayment'             → PaymentSimpleCash
  - 'schedule_rate_change'   → ScheduleTenantRateChange_v2

Backoff: 30s, 2min, 10min, 30min, 1h between retries.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text

from common.config import DataLayerConfig
from common.soap_client import SOAPClient

logger = logging.getLogger(__name__)

CC_NS = "http://tempuri.org/CallCenterWs/CallCenterWs"

_BACKOFF_SECONDS: List[int] = [30, 120, 600, 1800, 3600]
_MAX_ATTEMPTS = 5


# ---------------------------------------------------------------------------
# Public — used by /move-in handler
# ---------------------------------------------------------------------------

def enqueue(jobs: List[Dict[str, Any]], db_session) -> List[int]:
    """Insert job rows into mw_lease_followup_jobs. Returns the new ids.

    `jobs` is the output of perpetual_orchestrator.determine_followups().
    Caller is responsible for committing the session.
    """
    if not jobs:
        return []
    ids: List[int] = []
    for job in jobs:
        result = db_session.execute(
            text("""
                INSERT INTO mw_lease_followup_jobs
                    (ledger_id, site_code, tenant_id, unit_id, action_type,
                     payload, status, attempts, next_attempt_at,
                     related_request_id, related_session_id, related_customer_id,
                     created_at, updated_at)
                VALUES
                    (:ledger_id, :site_code, :tenant_id, :unit_id, :action_type,
                     CAST(:payload AS jsonb), 'pending', 0, NOW(),
                     :rrid, :rsid, :rcid,
                     NOW(), NOW())
                RETURNING id
            """),
            {
                'ledger_id':   job['ledger_id'],
                'site_code':   job['site_code'],
                'tenant_id':   job.get('tenant_id'),
                'unit_id':     job.get('unit_id'),
                'action_type': job['action_type'],
                'payload':     json.dumps(job['payload']),
                'rrid':        job.get('related_request_id'),
                'rsid':        job.get('related_session_id'),
                'rcid':        job.get('related_customer_id'),
            },
        )
        new_id = result.scalar()
        if new_id:
            ids.append(int(new_id))
    return ids


def execute_pending_for_ledger(ledger_id: int, db_session) -> Dict[str, int]:
    """Best-effort inline execution of all pending jobs for one lease.

    Called by the /move-in handler immediately after enqueue() so the
    happy path completes in a single request. Returns {'ok': N, 'failed': N,
    'pending_retry': N} for the response payload.
    """
    rows = db_session.execute(
        text("""
            SELECT id FROM mw_lease_followup_jobs
            WHERE ledger_id = :lid
              AND status = 'pending'
              AND next_attempt_at <= NOW()
            ORDER BY id
            FOR UPDATE SKIP LOCKED
        """),
        {'lid': ledger_id},
    ).fetchall()
    counts = {'ok': 0, 'failed': 0, 'pending_retry': 0}
    for (job_id,) in rows:
        outcome = _run_single(job_id, db_session)
        counts[outcome] = counts.get(outcome, 0) + 1
    return counts


def execute_pending_batch(db_session, batch_size: int = 10) -> Dict[str, int]:
    """Worker entry point — drain N pending jobs across all leases."""
    rows = db_session.execute(
        text(f"""
            SELECT id FROM mw_lease_followup_jobs
            WHERE status = 'pending'
              AND next_attempt_at <= NOW()
            ORDER BY next_attempt_at, id
            LIMIT {int(batch_size)}
            FOR UPDATE SKIP LOCKED
        """),
    ).fetchall()
    counts = {'ok': 0, 'failed': 0, 'pending_retry': 0, 'failed_permanent': 0}
    for (job_id,) in rows:
        outcome = _run_single(job_id, db_session)
        counts[outcome] = counts.get(outcome, 0) + 1
    return counts


def retry_job(job_id: int, db_session) -> str:
    """Manual retry — used by /admin/recommendation-engine/lease-followups.

    Resets status to 'pending' and next_attempt_at to NOW, then runs once.
    Returns the outcome string.
    """
    db_session.execute(
        text("""
            UPDATE mw_lease_followup_jobs
            SET status='pending', next_attempt_at=NOW(), last_error=NULL
            WHERE id = :id
        """),
        {'id': job_id},
    )
    return _run_single(job_id, db_session)


# ---------------------------------------------------------------------------
# Internal — run one job, update its row
# ---------------------------------------------------------------------------

def _run_single(job_id: int, db_session) -> str:
    """Run one job; update its status/attempts/error in-place. Returns
    'ok' | 'pending_retry' | 'failed_permanent' | 'failed'.
    """
    row = db_session.execute(
        text("""
            UPDATE mw_lease_followup_jobs
            SET status='running', last_attempt_at=NOW(), updated_at=NOW()
            WHERE id = :id AND status='pending'
            RETURNING id, action_type, payload, attempts, ledger_id, site_code
        """),
        {'id': job_id},
    ).fetchone()
    if not row:
        return 'failed'   # someone else grabbed it

    _, action_type, payload, attempts, ledger_id, site_code = row
    if isinstance(payload, str):
        payload = json.loads(payload)

    try:
        soap_response = _execute_action(action_type, payload)
        # Treat negative Ret_Code as a failure
        if isinstance(soap_response, list) and soap_response:
            ret_code = soap_response[0].get('Ret_Code')
            if ret_code is not None:
                try:
                    if int(ret_code) < 0:
                        raise SoapBusinessFault(soap_response[0].get('Ret_Msg', f'Ret_Code={ret_code}'))
                except ValueError:
                    pass
        db_session.execute(
            text("""
                UPDATE mw_lease_followup_jobs
                SET status='success',
                    attempts=attempts+1,
                    soap_response=CAST(:resp AS jsonb),
                    last_error=NULL,
                    updated_at=NOW()
                WHERE id=:id
            """),
            {'id': job_id, 'resp': json.dumps(soap_response, default=str)},
        )
        return 'ok'
    except Exception as exc:
        new_attempts = (attempts or 0) + 1
        if new_attempts >= _MAX_ATTEMPTS:
            db_session.execute(
                text("""
                    UPDATE mw_lease_followup_jobs
                    SET status='failed_permanent',
                        attempts=:n, last_error=:err, updated_at=NOW()
                    WHERE id=:id
                """),
                {'id': job_id, 'n': new_attempts, 'err': str(exc)[:1000]},
            )
            try:
                _alert_ops(action_type, ledger_id, site_code, str(exc))
            except Exception:
                logger.exception("alert_ops failed for job %s", job_id)
            return 'failed_permanent'
        # exponential backoff
        backoff = _BACKOFF_SECONDS[min(new_attempts - 1, len(_BACKOFF_SECONDS) - 1)]
        next_at = datetime.now(timezone.utc) + timedelta(seconds=backoff)
        db_session.execute(
            text("""
                UPDATE mw_lease_followup_jobs
                SET status='pending',
                    attempts=:n,
                    next_attempt_at=:nat,
                    last_error=:err,
                    updated_at=NOW()
                WHERE id=:id
            """),
            {'id': job_id, 'n': new_attempts, 'nat': next_at, 'err': str(exc)[:1000]},
        )
        return 'pending_retry'


class SoapBusinessFault(Exception):
    """SOAP returned a negative Ret_Code — semantic error, not transport."""


def _execute_action(action_type: str, payload: Dict[str, Any]) -> Any:
    """Dispatch to the right SOAP op."""
    cfg = DataLayerConfig.from_env()
    cc_url = cfg.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
    client = SOAPClient(
        base_url=cc_url, corp_code=cfg.soap.corp_code,
        corp_user=cfg.soap.corp_user, api_key=cfg.soap.api_key,
        corp_password=cfg.soap.corp_password, timeout=60, retries=1,
    )

    if action_type == 'prepayment':
        return client.call(
            operation='PaymentSimpleCash',
            parameters=payload,
            soap_action=f"{CC_NS}/PaymentSimpleCash",
            namespace=CC_NS,
            result_tag='RT',
        )
    elif action_type == 'schedule_rate_change':
        return client.call(
            operation='ScheduleTenantRateChange_v2',
            parameters=payload,
            soap_action=f"{CC_NS}/ScheduleTenantRateChange_v2",
            namespace=CC_NS,
            result_tag='RT',
        )
    else:
        raise ValueError(f"Unknown action_type: {action_type}")


def _alert_ops(action_type: str, ledger_id: int, site_code: str, err: str) -> None:
    """Best-effort alert via the existing alert manager."""
    try:
        from scheduler.alert_manager import alert_manager
        alert_manager.alert(
            level='error',
            source='lease_followup_queue',
            title=f'Lease follow-up failed permanently: {action_type}',
            message=(
                f"Action {action_type!r} for ledger {ledger_id} at {site_code} "
                f"failed after {_MAX_ATTEMPTS} attempts. Manual review required.\n"
                f"Last error: {err[:500]}"
            ),
            metadata={'ledger_id': ledger_id, 'site_code': site_code, 'action_type': action_type},
        )
    except ImportError:
        # Alert manager not available — log only
        logger.error(
            "Lease follow-up failed permanently action=%s ledger=%s site=%s err=%s",
            action_type, ledger_id, site_code, err[:200],
        )
