"""
Idempotency-Key support for booking endpoints.

Caller passes `Idempotency-Key: <opaque-string>` on POST. Within the 24h
window, replays of the same (api_key_id, key, endpoint) tuple return the
cached response without re-running the handler. Defends against bot
retry-on-network-blip double-MoveIn.

Storage: mw_idempotency_keys table (esa_middleware).
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Tuple

from sqlalchemy import text

logger = logging.getLogger(__name__)


def lookup(key: str, endpoint: str, api_key_id: Optional[int], db_session) -> Optional[Tuple[int, Dict[str, Any]]]:
    """Return (status, response_json) if a fresh cached entry exists, else None."""
    if not key:
        return None
    try:
        row = db_session.execute(
            text("""
                SELECT response_status, response_json
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
        return status, body
    except Exception as exc:
        logger.warning("idempotency lookup failed: %s", exc)
        return None


def store(key: str, endpoint: str, api_key_id: Optional[int],
          status: int, body: Dict[str, Any], db_session) -> None:
    """Cache a successful response. Called by the route handler after a
    handler completes (any 2xx — replay returns the same outcome)."""
    if not key:
        return
    try:
        db_session.execute(
            text("""
                INSERT INTO mw_idempotency_keys
                    (api_key_id, idempotency_key, endpoint,
                     response_json, response_status,
                     created_at, expires_at)
                VALUES
                    (:aid, :k, :ep, CAST(:body AS jsonb), :status,
                     NOW(), NOW() + INTERVAL '24 hours')
                ON CONFLICT (api_key_id, idempotency_key, endpoint) DO NOTHING
            """),
            {
                'aid': api_key_id, 'k': key, 'ep': endpoint,
                'body': json.dumps(body, default=str), 'status': status,
            },
        )
        db_session.commit()
    except Exception as exc:
        logger.warning("idempotency store failed: %s", exc)
        try: db_session.rollback()
        except Exception: pass
