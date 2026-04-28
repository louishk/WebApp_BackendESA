"""
Outcome write-back helper for the recommendation engine.

When a reservation is successfully created, call link_booking_to_recommendation()
to find the mw_recommendations_served row that produced the unit recommendation
and stamp the booked_* columns on it. This closes the conversion feedback loop
for analytics without blocking the booking flow (all failures are caught and
logged).

Reconciliation strategy
-----------------------
Since both calls (recommendation request + reservation) flow through this
middleware, we have full visibility — the bot doesn't need to echo session_id /
customer_id / plan_id back. We reconcile from middleware-side data:

  1. Match by (unit_id, concession_id) against slot1/2/3 — when the booking's
     unit + discount line up exactly with one of the offered slots, we know
     which slot the customer accepted and we automatically derive the
     plan_id from that slot. Cleanest attribution.

  2. If only unit_id matches (the bot booked a unit we recommended but with
     a different concession the customer must have negotiated outside the
     standard flow), still record the slot but leave booked_plan_id NULL —
     we don't know which plan they actually used.

Within each strategy we narrow further when extra context is available:
  - session_id (passed by the bot in the booking request) → best precision,
    no time window
  - customer_id → 24 h window
  - unit_id alone → 4 h window

IMPORTANT — DB session ownership:
    This module operates on the esa_middleware database. The caller is
    responsible for obtaining a middleware session via
    current_app.get_middleware_session() and passing it as db_session.
    Failures are caught and logged; never raise.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text

logger = logging.getLogger(__name__)

# How far back we search when we have no session_id anchor
_CUSTOMER_WINDOW_HOURS = 24
_UNIT_WINDOW_HOURS = 4


def link_booking_to_recommendation(
    *,
    unit_id: int,
    concession_id: Optional[int] = None,
    customer_id: Optional[str] = None,
    session_id: Optional[str] = None,
    plan_id_override: Optional[int] = None,
    booked_at: datetime,
    db_session,
) -> Optional[int]:
    """Best-effort match a fresh booking to a recent recommendation.

    Match priority (most specific → least), each layer prefers exact
    (unit_id, concession_id) match over unit_id-only:

      1. session_id provided → most recent row in this session whose
         slot1/2/3 unit_id matches; prefer the slot whose concession_id
         also matches.
      2. customer_id provided → most recent row for this customer in last
         24 h, same slot logic.
      3. unit_id alone → most recent row in last 4 h, same slot logic.

    Updates the matched row:
      booked_unit_id      = unit_id
      booked_plan_id      = matched slot's plan_id when concession matched;
                            else `plan_id_override` arg; else NULL
      booked_concession_id = concession_id of the booking
      booked_at           = booked_at
      booked_slot         = which slot (1|2|3) the unit was in

    The UPDATE includes WHERE booked_unit_id IS NULL so an already-booked
    recommendation row is never overwritten; a later booking of the same
    unit in a different conversation will match a newer row instead.

    Returns the updated row's id, or None when no match found.
    Never raises — any failure is logged as a warning.

    Args:
        unit_id:           The unit that was just booked.
        concession_id:     Concession applied to the booking (0 for stdrate).
                           Used to pick the right slot when the same unit
                           appeared with multiple offers across slots.
        customer_id:       PandaAI customer id (optional but recommended —
                           tightens matching across the 24h window).
        session_id:        Conversation session id (optional — tightest
                           match when the bot remembers it from the
                           recommendation turn).
        plan_id_override:  Force a specific plan_id when no slot matches
                           the booking's concession. Most callers should
                           leave this None — we derive from the slot.
        booked_at:         Timestamp of the booking (UTC preferred).
        db_session:        Live SQLAlchemy Session bound to esa_middleware.
    """
    try:
        return _find_and_update(
            unit_id=unit_id,
            concession_id=concession_id,
            customer_id=customer_id,
            session_id=session_id,
            plan_id_override=plan_id_override,
            booked_at=booked_at,
            db_session=db_session,
        )
    except Exception as exc:
        logger.warning(
            "link_booking_to_recommendation failed unit_id=%s concession_id=%s "
            "session_id=%s customer_id=%s — recommendation row not stamped: %s",
            unit_id, concession_id, session_id, customer_id, exc,
        )
        try:
            db_session.rollback()
        except Exception:
            pass
        return None


# ---------------------------------------------------------------------------
# Internal implementation
# ---------------------------------------------------------------------------

# Common SELECT fragment — pulls (id, slot_n, slot_plan_id, slot_concession_id)
# for whichever slot the unit appears in. Concession-match flag is computed
# inline so we can ORDER BY concession_match DESC (exact match first).
_SLOT_PROJECTION = """
    SELECT
        id,
        CASE
            WHEN slot1_unit_id = :uid THEN 1
            WHEN slot2_unit_id = :uid THEN 2
            WHEN slot3_unit_id = :uid THEN 3
        END AS matched_slot,
        CASE
            WHEN slot1_unit_id = :uid THEN slot1_plan_id
            WHEN slot2_unit_id = :uid THEN slot2_plan_id
            WHEN slot3_unit_id = :uid THEN slot3_plan_id
        END AS matched_plan_id,
        CASE
            WHEN slot1_unit_id = :uid THEN slot1_concession_id
            WHEN slot2_unit_id = :uid THEN slot2_concession_id
            WHEN slot3_unit_id = :uid THEN slot3_concession_id
        END AS matched_concession_id,
        CASE
            WHEN (slot1_unit_id = :uid AND slot1_concession_id = :cid)
              OR (slot2_unit_id = :uid AND slot2_concession_id = :cid)
              OR (slot3_unit_id = :uid AND slot3_concession_id = :cid)
            THEN 1 ELSE 0
        END AS concession_match
"""


def _find_and_update(
    *,
    unit_id: int,
    concession_id: Optional[int],
    customer_id: Optional[str],
    session_id: Optional[str],
    plan_id_override: Optional[int],
    booked_at: datetime,
    db_session,
) -> Optional[int]:
    """Find the best-matching recommendation row and stamp it."""
    candidate = _find_candidate(
        unit_id=unit_id,
        concession_id=concession_id,
        customer_id=customer_id,
        session_id=session_id,
        db_session=db_session,
    )
    if candidate is None:
        return None

    rec_id, booked_slot, slot_plan_id, slot_concession_id, concession_match = candidate

    # Pick the most authoritative plan_id available:
    #   1. The slot's plan_id IF the booking's concession matched the slot's
    #      concession exactly (we know which offer they accepted)
    #   2. Caller's plan_id_override (rare)
    #   3. NULL — we recorded the slot but can't attribute the plan
    if concession_match:
        booked_plan_id: Optional[int] = slot_plan_id
    else:
        booked_plan_id = plan_id_override

    result = db_session.execute(
        text("""
            UPDATE mw_recommendations_served
            SET
                booked_unit_id       = :unit_id,
                booked_plan_id       = COALESCE(:plan_id, booked_plan_id),
                booked_concession_id = :concession_id,
                booked_at            = :booked_at,
                booked_slot          = :booked_slot
            WHERE id = :rec_id
              AND booked_unit_id IS NULL
        """),
        {
            "unit_id": unit_id,
            "plan_id": booked_plan_id,
            "concession_id": concession_id,
            "booked_at": booked_at,
            "booked_slot": booked_slot,
            "rec_id": rec_id,
        },
    )
    db_session.commit()

    if result.rowcount == 0:
        logger.info(
            "recommendation row id=%s already booked; unit_id=%s not linked",
            rec_id, unit_id,
        )
        return None

    logger.info(
        "linked booking unit=%s concession=%s plan=%s → recommendation id=%s slot=%s "
        "(concession_match=%s)",
        unit_id, concession_id, booked_plan_id, rec_id, booked_slot, bool(concession_match),
    )
    return rec_id


def _find_candidate(
    *,
    unit_id: int,
    concession_id: Optional[int],
    customer_id: Optional[str],
    session_id: Optional[str],
    db_session,
) -> Optional[tuple]:
    """
    Return (rec_id, booked_slot, slot_plan_id, slot_concession_id, concession_match)
    for the best-matching recommendation row, or None.

    Within each priority level, ORDER BY concession_match DESC, served_at DESC
    so a row whose slot's concession_id matches the booking's exactly wins
    over a row that only matches the unit.
    """
    now_utc = datetime.now(timezone.utc)
    # When concession_id is unknown, NULL won't equal anything so concession_match
    # will always be 0 — that's fine, we just skip the exact-attribution logic.
    cid = concession_id if concession_id is not None else -1

    # Priority 1 — session_id match (no time window; session scopes it)
    if session_id:
        row = db_session.execute(
            text(f"""
                {_SLOT_PROJECTION}
                FROM mw_recommendations_served
                WHERE session_id = :session_id
                  AND booked_unit_id IS NULL
                  AND (slot1_unit_id = :uid
                       OR slot2_unit_id = :uid
                       OR slot3_unit_id = :uid)
                ORDER BY concession_match DESC, served_at DESC
                LIMIT 1
            """),
            {"uid": unit_id, "cid": cid, "session_id": session_id},
        ).fetchone()
        if row:
            return tuple(row)

    # Priority 2 — customer_id within 24h
    if customer_id:
        cutoff = now_utc - timedelta(hours=_CUSTOMER_WINDOW_HOURS)
        row = db_session.execute(
            text(f"""
                {_SLOT_PROJECTION}
                FROM mw_recommendations_served
                WHERE customer_id = :customer_id
                  AND booked_unit_id IS NULL
                  AND served_at >= :cutoff
                  AND (slot1_unit_id = :uid
                       OR slot2_unit_id = :uid
                       OR slot3_unit_id = :uid)
                ORDER BY concession_match DESC, served_at DESC
                LIMIT 1
            """),
            {"uid": unit_id, "cid": cid, "customer_id": customer_id, "cutoff": cutoff},
        ).fetchone()
        if row:
            return tuple(row)

    # Priority 3 — unit_id alone within 4h
    cutoff = now_utc - timedelta(hours=_UNIT_WINDOW_HOURS)
    row = db_session.execute(
        text(f"""
            {_SLOT_PROJECTION}
            FROM mw_recommendations_served
            WHERE booked_unit_id IS NULL
              AND served_at >= :cutoff
              AND (slot1_unit_id = :uid
                   OR slot2_unit_id = :uid
                   OR slot3_unit_id = :uid)
            ORDER BY concession_match DESC, served_at DESC
            LIMIT 1
        """),
        {"uid": unit_id, "cid": cid, "cutoff": cutoff},
    ).fetchone()
    if row:
        return tuple(row)

    return None
