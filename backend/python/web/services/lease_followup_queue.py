"""
Lease Follow-up Job Queue — DLQ + executor for post-MoveIn SOAP calls.

Two callers:
  - /api/reservations/move-in handler — enqueues jobs after SOAP MoveIn
    succeeds, then attempts to execute pending jobs for THIS lease inline
    before responding to the bot. Bot waits ~2-3s on the happy path.
  - A background worker — every 10s, drains pending jobs across all leases,
    retrying with exponential backoff. After 5 attempts marks
    status='failed_permanent' and logs the failure for ops review.

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

    if not rows:
        return counts

    # P5: shared SOAP client across the inline batch (typically 1-2 jobs
    # per lease, but the pattern is consistent with the worker's batch
    # path).
    shared_client: Optional[SOAPClient] = None
    try:
        shared_client = _build_cc_soap_client()
    except Exception as exc:
        logger.warning("inline followups: SOAP client init failed: %s", exc)
    try:
        for (job_id,) in rows:
            outcome = _run_single(job_id, db_session, soap_client=shared_client)
            counts[outcome] = counts.get(outcome, 0) + 1
    finally:
        if shared_client is not None:
            try:
                shared_client.close()
            except Exception:
                pass
    return counts


def recover_stuck_running(db_session, stale_after_minutes: int = 5) -> int:
    """
    M4 watchdog: reset jobs left in `status='running'` for too long back to
    `pending`. Happens when the scheduler is killed mid-SOAP (VM restart,
    OOM, deploy). Without this, the row would never retry and never alert.

    Called once at startup AND on every batch tick (cheap query).
    Returns the number of rows recovered.
    """
    result = db_session.execute(
        text("""
            UPDATE mw_lease_followup_jobs
            SET status = 'pending',
                last_error = COALESCE(last_error, '') ||
                             ' [watchdog: recovered from stuck-running]',
                updated_at = NOW()
            WHERE status = 'running'
              AND last_attempt_at IS NOT NULL
              AND last_attempt_at < NOW() - make_interval(mins => :mins)
        """),
        {'mins': int(stale_after_minutes)},
    )
    recovered = result.rowcount or 0
    if recovered:
        logger.warning(
            "lease_followup watchdog: recovered %d stuck-running job(s)",
            recovered,
        )
    return recovered


# P7: watchdog cadence — runs once every Nth tick instead of every tick.
# Worker tick is 10s; the stale threshold is 5min, so a 6-tick cadence
# (≈ 60s detection latency) is well within the recovery SLA and cuts
# the no-op UPDATE rate from 360/h to 60/h. Module-level counter is
# safe because the worker is single-instance per process and the
# scheduler runs with max_instances=1.
_WATCHDOG_TICK_INTERVAL = 6
_watchdog_tick_counter = 0


def execute_pending_batch(db_session, batch_size: int = 10) -> Dict[str, int]:
    """Worker entry point — drain N pending jobs across all leases."""
    # P7: throttled watchdog. Runs every Nth tick (default 6 ≈ 60s).
    # A stuck-running row gets re-enqueued within ~60s, well inside the
    # 5-minute stale threshold, while the no-op UPDATE per tick is
    # eliminated on the common (zero stuck rows) path.
    global _watchdog_tick_counter
    _watchdog_tick_counter = (_watchdog_tick_counter + 1) % _WATCHDOG_TICK_INTERVAL
    if _watchdog_tick_counter == 0:
        try:
            recover_stuck_running(db_session)
        except Exception as exc:
            logger.warning("lease_followup watchdog query failed: %s", exc)

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

    if not rows:
        return counts

    # P5: one SOAP client for the whole batch. With ~10 jobs, this saves
    # ~9 TLS handshakes when there's a backlog.
    shared_client: Optional[SOAPClient] = None
    try:
        shared_client = _build_cc_soap_client()
    except Exception as exc:
        logger.warning(
            "lease_followup batch: could not pre-build SOAP client; falling "
            "back to per-job clients: %s", exc,
        )
    try:
        for (job_id,) in rows:
            outcome = _run_single(job_id, db_session, soap_client=shared_client)
            counts[outcome] = counts.get(outcome, 0) + 1
    finally:
        if shared_client is not None:
            try:
                shared_client.close()
            except Exception:
                pass
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

def _run_single(
    job_id: int,
    db_session,
    soap_client: Optional[SOAPClient] = None,
) -> str:
    """Run one job; update its status/attempts/error in-place. Returns
    'ok' | 'pending_retry' | 'failed_permanent' | 'failed'.

    P5: callers can pass a shared `soap_client` to amortise TLS handshake
    cost across a batch. When None, _execute_action builds a one-shot
    client.
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
        soap_response = _execute_action(action_type, payload, client=soap_client)
        # M6: empty SOAP response list = no RT element returned. For these
        # action types (PaymentSimpleCash, ScheduleTenantRateChange_v2) every
        # success path returns at least one RT row; treat empty as a
        # retryable fault so the worker doesn't silently mark it 'success'.
        if isinstance(soap_response, list) and not soap_response:
            raise SoapBusinessFault(
                f"{action_type}: empty SOAP response (no RT element)"
            )
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


def _build_cc_soap_client() -> SOAPClient:
    """Construct a fresh CallCenterWs SOAP client. Caller closes."""
    cfg = DataLayerConfig.from_env()
    cc_url = cfg.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
    return SOAPClient(
        base_url=cc_url, corp_code=cfg.soap.corp_code,
        corp_user=cfg.soap.corp_user, api_key=cfg.soap.api_key,
        corp_password=cfg.soap.corp_password, timeout=60, retries=1,
    )


def _execute_action(
    action_type: str,
    payload: Dict[str, Any],
    client: Optional[SOAPClient] = None,
) -> Any:
    """
    Dispatch to the right SOAP op.

    P5: when `client` is provided, reuse it (saves the TLS handshake
    cost across a batch). When None, build a one-shot client and close
    it before returning. Batch entry-points (execute_pending_batch,
    execute_pending_for_ledger) build one client and pass it down.
    """
    owns_client = client is None
    if owns_client:
        client = _build_cc_soap_client()

    try:
        return _dispatch_soap_call(action_type, payload, client)
    finally:
        if owns_client:
            try:
                client.close()
            except Exception:
                pass


def _dispatch_soap_call(action_type: str, payload: Dict[str, Any], client: SOAPClient) -> Any:
    """Inner dispatch — single op call, no client-lifecycle concerns."""
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
    """Log permanent failure for ops review. Wire to an alerter when one exists."""
    logger.error(
        "Lease follow-up failed permanently action=%s ledger=%s site=%s err=%s",
        action_type, ledger_id, site_code, err[:200],
    )
