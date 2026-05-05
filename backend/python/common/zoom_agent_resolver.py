"""
Zoom Agent Resolver

Maintains the `zoom_agent_mapping` table that links Zoom Phone users
(by zoom_user_id) to SugarCRM Users (by sugar_user_id). The mapping is
used by the call log sync pipeline to assign pushed Call records to the
correct SugarCRM user.

Self-healing: every pipeline run discovers any new Zoom users from
zoom_call_logs.raw_json and attempts to match them against active
SugarCRM users. Existing rows are preserved (manual mappings won't be
overwritten unless --force-refresh is used).

Matching strategy (in order):
  1. user_name == zoom_email (OIDC: user_name is the email local part)
  2. user_name == local-part of zoom_email (e.g. "jamesoh")
  3. squished full_name match (e.g. "James Oh" -> "jamesoh")

Usage:
    from common.zoom_agent_resolver import refresh_agent_mapping, get_sugar_user_for_zoom

    stats = refresh_agent_mapping(pbi_engine)
    sugar_user_id = get_sugar_user_for_zoom(pbi_session, zoom_user_id)
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _norm(s: Optional[str]) -> str:
    return (s or '').strip().lower()


def _build_sugar_index(active_users: List[Dict[str, Any]]) -> Dict[str, Tuple[str, str]]:
    """Build a multi-key lookup from SugarCRM user list.

    Returns dict keyed by various normalized identifiers:
      - email1 (full + local part)
      - user_name (full + local part)
      - squished full_name ("james oh" -> "jamesoh")

    Each value is (sugar_user_id, display_name).
    """
    idx: Dict[str, Tuple[str, str]] = {}
    for u in active_users:
        sid = u.get('id')
        if not sid:
            continue
        sname = (u.get('full_name')
                 or f"{u.get('first_name','')} {u.get('last_name','')}".strip()).strip()
        keys = set()
        for k in (u.get('email1'), u.get('user_name')):
            if k:
                keys.add(_norm(k))
                if '@' in k:
                    keys.add(_norm(k.split('@')[0]))
        if sname:
            keys.add(_norm(sname).replace(' ', ''))
        for key in keys:
            if key and key not in idx:
                idx[key] = (sid, sname)
    return idx


def _lookup(idx: Dict[str, Tuple[str, str]],
            email: Optional[str],
            name: Optional[str]) -> Optional[Tuple[str, str]]:
    """Look up a Zoom user against the SugarCRM index using multiple strategies."""
    if email:
        hit = idx.get(_norm(email))
        if hit:
            return hit
        if '@' in email:
            hit = idx.get(_norm(email.split('@')[0]))
            if hit:
                return hit
    if name:
        hit = idx.get(_norm(name).replace(' ', ''))
        if hit:
            return hit
    return None


def _fetch_active_sugar_users(sugar_client) -> List[Dict[str, Any]]:
    """Fetch all active SugarCRM users (paginated)."""
    all_users: List[Dict[str, Any]] = []
    offset = 0
    while True:
        data, err = sugar_client.filter_records(
            'Users',
            filter_expr=[{'status': 'Active'}],
            fields=['id', 'first_name', 'last_name', 'user_name', 'email1', 'full_name'],
            max_num=200,
            offset=offset,
        )
        if err or not data or not data.get('records'):
            break
        all_users.extend(data['records'])
        offset = data.get('next_offset', -1)
        if offset == -1:
            break
    return all_users


def _discover_zoom_users(pbi_engine: Engine) -> List[Tuple[str, Optional[str], Optional[str]]]:
    """Find all distinct Zoom users referenced in zoom_call_logs.raw_json."""
    with pbi_engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT zoom_user_id, zoom_email, zoom_name FROM (
                SELECT raw_json->>'caller_user_id' AS zoom_user_id,
                       raw_json->>'caller_email'   AS zoom_email,
                       raw_json->>'caller_name'    AS zoom_name
                FROM zoom_call_logs
                WHERE raw_json->>'caller_user_id' IS NOT NULL
                  AND raw_json->>'caller_user_id' != ''
                UNION
                SELECT raw_json->>'callee_user_id',
                       raw_json->>'callee_email',
                       raw_json->>'callee_name'
                FROM zoom_call_logs
                WHERE raw_json->>'callee_user_id' IS NOT NULL
                  AND raw_json->>'callee_user_id' != ''
            ) sub
        """)).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def refresh_agent_mapping(
    pbi_engine: Engine,
    sugar_client=None,
    force: bool = False,
) -> Dict[str, int]:
    """Refresh the zoom_agent_mapping table.

    Discovers any new Zoom users from call logs, fetches active SugarCRM users,
    and inserts new mappings. By default only new (zoom_user_id not yet in the
    table) users are added — manual mappings are preserved.

    Args:
        pbi_engine: SQLAlchemy engine for esa_pbi.
        sugar_client: Optional pre-authenticated SugarCRMClient. If None,
                      one is created and authenticated for this call.
        force: If True, also re-resolve already-mapped rows (useful after
               OIDC config changes or SugarCRM user roster updates).

    Returns:
        Dict with counts: {discovered, new, updated, unmatched, total}.
    """
    own_client = False
    if sugar_client is None:
        from common.sugarcrm_client import SugarCRMClient
        sugar_client = SugarCRMClient.from_env()
        if not sugar_client.authenticate():
            logger.error("SugarCRM auth failed — agent mapping refresh skipped")
            return {'discovered': 0, 'new': 0, 'updated': 0, 'unmatched': 0, 'total': 0}
        own_client = True

    try:
        zoom_users = _discover_zoom_users(pbi_engine)
        logger.info("Agent resolver: discovered %d distinct Zoom users", len(zoom_users))

        # Find which ones are already mapped
        with pbi_engine.connect() as conn:
            existing = {row[0] for row in conn.execute(
                text("SELECT zoom_user_id FROM zoom_agent_mapping")
            ).fetchall()}

        to_resolve = [
            (zid, email, name) for (zid, email, name) in zoom_users
            if force or zid not in existing
        ]
        if not to_resolve:
            with pbi_engine.connect() as conn:
                total = conn.execute(text("SELECT COUNT(*) FROM zoom_agent_mapping")).scalar()
            logger.info("Agent resolver: no new Zoom users (mapping table has %d entries)", total)
            return {'discovered': len(zoom_users), 'new': 0, 'updated': 0,
                    'unmatched': 0, 'total': total}

        # Build SugarCRM index once
        all_sugar = _fetch_active_sugar_users(sugar_client)
        idx = _build_sugar_index(all_sugar)
        logger.info("Agent resolver: %d active SugarCRM users indexed", len(all_sugar))

        new_count = 0
        updated_count = 0
        unmatched_count = 0

        with pbi_engine.begin() as conn:
            for zoom_user_id, email, name in to_resolve:
                hit = _lookup(idx, email, name)
                if hit:
                    sid, sname = hit
                    is_existing = zoom_user_id in existing
                    conn.execute(text("""
                        INSERT INTO zoom_agent_mapping
                            (zoom_user_id, zoom_email, zoom_name, sugar_user_id, sugar_user_name)
                        VALUES (:zid, :ze, :zn, :sid, :sn)
                        ON CONFLICT (zoom_user_id) DO UPDATE
                        SET zoom_email = :ze,
                            zoom_name = :zn,
                            sugar_user_id = :sid,
                            sugar_user_name = :sn,
                            updated_at = NOW()
                    """), {
                        'zid': zoom_user_id, 'ze': email, 'zn': name,
                        'sid': sid, 'sn': sname,
                    })
                    if is_existing:
                        updated_count += 1
                    else:
                        new_count += 1
                else:
                    unmatched_count += 1
                    # Insert a stub row so we know about it (sugar_user_id NULL)
                    conn.execute(text("""
                        INSERT INTO zoom_agent_mapping
                            (zoom_user_id, zoom_email, zoom_name, sugar_user_id, sugar_user_name, enabled)
                        VALUES (:zid, :ze, :zn, NULL, NULL, FALSE)
                        ON CONFLICT (zoom_user_id) DO NOTHING
                    """), {'zid': zoom_user_id, 'ze': email, 'zn': name})

        with pbi_engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM zoom_agent_mapping")).scalar()

        logger.info(
            "Agent resolver: new=%d updated=%d unmatched=%d total=%d",
            new_count, updated_count, unmatched_count, total,
        )
        return {
            'discovered': len(zoom_users),
            'new': new_count,
            'updated': updated_count,
            'unmatched': unmatched_count,
            'total': total,
        }

    finally:
        if own_client:
            try:
                sugar_client.logout()
            except Exception:
                pass


def get_sugar_user_for_zoom(pbi_session: Session, zoom_user_id: str) -> Optional[str]:
    """Look up the SugarCRM user_id assigned to a Zoom user.

    Returns None if no mapping exists or the mapping is disabled.
    """
    if not zoom_user_id:
        return None
    row = pbi_session.execute(
        text("""
            SELECT sugar_user_id FROM zoom_agent_mapping
            WHERE zoom_user_id = :zid AND enabled = TRUE
            LIMIT 1
        """),
        {'zid': zoom_user_id},
    ).fetchone()
    return row[0] if row and row[0] else None


def derive_agent_zoom_user_id(call_log) -> Optional[str]:
    """Given a ZoomCallLog row, return the zoom_user_id of the *internal agent*.

    For outbound calls the agent is the caller; for inbound calls it's the callee.
    Reads from raw_json.
    """
    if not call_log or not call_log.raw_json:
        return None
    raw = call_log.raw_json
    direction = (call_log.direction or '').lower()
    if direction == 'outbound':
        return raw.get('caller_user_id') or None
    if direction == 'inbound':
        return raw.get('callee_user_id') or None
    # Unknown direction — try caller first, then callee
    return raw.get('caller_user_id') or raw.get('callee_user_id') or None
