"""
Idempotency-Key support for booking endpoints.

Caller passes `Idempotency-Key: <opaque-string>` on POST. Within the 24h
window, replays of the same (api_key_id, key, endpoint) tuple return the
cached response without re-running the handler. Defends against bot
retry-on-network-blip double-MoveIn.

H4 — body-hash mismatch detection
---------------------------------
A replay with the SAME key but a DIFFERENT request body is a caller bug
(the bot reused the key for a different booking attempt). Silently
replaying the prior response in that case would book the wrong unit. We
now SHA-256 the canonical (sorted-keys) request body and compare on
lookup; mismatch is signalled to the caller for a HTTP 422.

Storage: mw_idempotency_keys table (esa_middleware).
"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import text

logger = logging.getLogger(__name__)


# Sentinel returned when the cached entry exists but body_hash differs.
BODY_MISMATCH = 'body_mismatch'


def canonical_body_hash(body: Optional[Dict[str, Any]]) -> Optional[str]:
    """SHA-256 hex of the request body serialized with sorted keys.

    Returns None for empty/None body — those callers don't get hash
    comparison (legacy GET-style or empty-body POSTs).
    """
    if not body:
        return None
    try:
        canonical = json.dumps(body, sort_keys=True, default=str, separators=(',', ':'))
    except Exception:
        return None
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


def lookup(
    key: str,
    endpoint: str,
    api_key_id: Optional[int],
    db_session,
    request_body: Optional[Dict[str, Any]] = None,
):
    """Return one of:
        - None                              — no cached entry, proceed
        - (status:int, body:dict)           — cached fresh entry, replay it
        - (BODY_MISMATCH, body_hash_str)    — entry exists but body differs

    Pre-existing rows with body_hash=NULL are treated as legacy and
    replayed without comparison (no migration required for in-flight
    keys).
    """
    if not key:
        return None
    try:
        row = db_session.execute(
            text("""
                SELECT response_status, response_json, body_hash
                FROM mw_idempotency_keys
                WHERE idempotency_key = :k
                  AND endpoint = :ep
                  AND (api_key_id = :aid OR (api_key_id IS NULL AND :aid IS NULL))
                  AND expires_at > NOW()
                LIMIT 1
            """),
            {'k': key, 'ep': endpoint, 'aid': api_key_id},
        ).fetchone()
        if not row:
            return None
        status = int(row[0])
        body = row[1]
        if isinstance(body, str):
            body = json.loads(body)
        stored_hash = row[2]
        # Body-hash check (only if we stored a hash AND caller passed a body)
        if stored_hash and request_body is not None:
            incoming_hash = canonical_body_hash(request_body)
            if incoming_hash and incoming_hash != stored_hash:
                return BODY_MISMATCH, stored_hash
        return status, body
    except Exception as exc:
        logger.warning("idempotency lookup failed: %s", exc)
        return None


def store(
    key: str,
    endpoint: str,
    api_key_id: Optional[int],
    status: int,
    body: Dict[str, Any],
    db_session,
    request_body: Optional[Dict[str, Any]] = None,
) -> None:
    """Cache a successful response. Called by the route handler after a
    handler completes (any 2xx — replay returns the same outcome).
    Stores a SHA-256 hash of the request body so future replays with a
    different body are detected and rejected (see lookup)."""
    if not key:
        return
    body_hash = canonical_body_hash(request_body) if request_body is not None else None
    try:
        db_session.execute(
            text("""
                INSERT INTO mw_idempotency_keys
                    (api_key_id, idempotency_key, endpoint,
                     response_json, response_status, body_hash,
                     created_at, expires_at)
                VALUES
                    (:aid, :k, :ep, CAST(:body AS jsonb), :status, :bhash,
                     NOW(), NOW() + INTERVAL '24 hours')
                ON CONFLICT (api_key_id, idempotency_key, endpoint) DO NOTHING
            """),
            {
                'aid': api_key_id, 'k': key, 'ep': endpoint,
                'body': json.dumps(body, default=str), 'status': status,
                'bhash': body_hash,
            },
        )
        db_session.commit()
    except Exception as exc:
        logger.warning("idempotency store failed: %s", exc)
        try: db_session.rollback()
        except Exception: pass
