"""
Outcome write-back helper for the recommendation engine.

When a reservation is successfully created, call link_booking_to_recommendation()
to find the mw_recommendations_served row that produced the unit recommendation and
stamp the booked_* columns on it. This closes the conversion feedback loop for
analytics without blocking the booking flow (all failures are caught and logged).

IMPORTANT — DB session ownership:
    This module operates on the esa_middleware database. The caller is responsible
    for obtaining a middleware session via current_app.get_middleware_session() and
    passing it as db_session. The session is NOT committed here; the caller must
    commit or the function handles its own mini-transaction via db_session.commit()
    inside a try/except so that a failure here does not corrupt the caller's
    transaction state.
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
    plan_id: Optional[int],
    concession_id: Optional[int],
    customer_id: Optional[str],
    session_id: Optional[str],
    booked_at: datetime,
    db_session,
) -> Optional[int]:
    """Best-effort match a fresh booking to a recent recommendation.

    Match priority (most specific → least):
      1. session_id provided → most recent row in this session whose
         slot1/2/3 unit_id == unit_id
      2. customer_id provided → most recent row for this customer in last
         24 h whose slot1/2/3 unit_id == unit_id
      3. unit_id alone → most recent row in last 4 h whose slot1/2/3
         unit_id == unit_id

    Updates the matched row:
      booked_unit_id = unit_id
      booked_plan_id = plan_id  (if provided, else left as-is / NULL)
      booked_at      = booked_at
      booked_slot    = which slot (1|2|3) the unit was in (lowest slot wins
                       if the unit appears in multiple slots)

    The UPDATE includes WHERE booked_unit_id IS NULL so that an already-
    booked recommendation row is never overwritten; a later booking of the
    same unit in a different conversation will match a newer row instead.

    Returns the updated row's id, or None when no match found.
    Never raises — any failure is logged as a warning and treated as "no link".

    Args:
        unit_id:       The unit that was just booked.
        plan_id:       The discount plan used, if known.
        concession_id: The concession used, if known.
        customer_id:   PandaAI / bot customer identifier (may be None for
                       anonymous bookings).
        session_id:    Conversation session identifier passed by the bot in
                       the original recommendation request (may be None).
        booked_at:     Timestamp of the booking (UTC preferred).
        db_session:    A live SQLAlchemy Session bound to esa_middleware.
                       The caller owns the session lifecycle; this function
                       commits its own sub-transaction on success.
    """
    try:
        row_id = _find_and_update(
            unit_id=unit_id,
            plan_id=plan_id,
            concession_id=concession_id,
            customer_id=customer_id,
            session_id=session_id,
            booked_at=booked_at,
            db_session=db_session,
        )
        return row_id
    except Exception as exc:
        logger.warning(
            "link_booking_to_recommendation failed unit_id=%s session_id=%s "
            "customer_id=%s — recommendation row not stamped: %s",
            unit_id, session_id, customer_id, exc,
        )
        try:
            db_session.rollback()
        except Exception:
            pass
        return None


# ---------------------------------------------------------------------------
# Internal implementation
# ---------------------------------------------------------------------------

def _find_and_update(
    *,
    unit_id: int,
    plan_id: Optional[int],
    concession_id: Optional[int],
    customer_id: Optional[str],
    session_id: Optional[str],
    booked_at: datetime,
    db_session,
) -> Optional[int]:
    """Find the best-matching recommendation row and stamp it. Returns id or None."""

    # --- Step 1: find the candidate row id ---
    candidate = _find_candidate(
        unit_id=unit_id,
        customer_id=customer_id,
        session_id=session_id,
        db_session=db_session,
    )
    if candidate is None:
        return None

    rec_id, booked_slot = candidate

    # --- Step 2: stamp the row (only if not already booked) ---
    result = db_session.execute(
        text("""
            UPDATE mw_recommendations_served
            SET
                booked_unit_id  = :unit_id,
                booked_plan_id  = COALESCE(:plan_id, booked_plan_id),
                booked_at       = :booked_at,
                booked_slot     = :booked_slot
            WHERE id = :rec_id
              AND booked_unit_id IS NULL
        """),
        {
            "unit_id": unit_id,
            "plan_id": plan_id,
            "booked_at": booked_at,
            "booked_slot": booked_slot,
            "rec_id": rec_id,
        },
    )
    db_session.commit()

    if result.rowcount == 0:
        # Row was already booked by a concurrent update or prior booking
        logger.info(
            "recommendation row id=%s already booked; unit_id=%s not linked",
            rec_id, unit_id,
        )
        return None

    return rec_id


def _find_candidate(
    *,
    unit_id: int,
    customer_id: Optional[str],
    session_id: Optional[str],
    db_session,
) -> Optional[tuple]:
    """
    Return (rec_id, booked_slot) for the best-matching recommendation row,
    or None if no match.

    The slot number is the lowest-numbered slot where the unit appears
    (defensive: unit should only appear in one slot per row, but lowest wins).
    """
    now_utc = datetime.now(timezone.utc)

    # Priority 1 — session_id match (no time window needed; session already scopes it)
    if session_id:
        row = db_session.execute(
            text("""
                SELECT id,
                    CASE
                        WHEN slot1_unit_id = :uid THEN 1
                        WHEN slot2_unit_id = :uid THEN 2
                        WHEN slot3_unit_id = :uid THEN 3
                    END AS matched_slot
                FROM mw_recommendations_served
                WHERE session_id = :session_id
                  AND booked_unit_id IS NULL
                  AND (
                      slot1_unit_id = :uid OR
                      slot2_unit_id = :uid OR
                      slot3_unit_id = :uid
                  )
                ORDER BY served_at DESC
                LIMIT 1
            """),
            {"uid": unit_id, "session_id": session_id},
        ).fetchone()
        if row:
            return row[0], row[1]

    # Priority 2 — customer_id match within last 24 h
    if customer_id:
        cutoff = now_utc - timedelta(hours=_CUSTOMER_WINDOW_HOURS)
        row = db_session.execute(
            text("""
                SELECT id,
                    CASE
                        WHEN slot1_unit_id = :uid THEN 1
                        WHEN slot2_unit_id = :uid THEN 2
                        WHEN slot3_unit_id = :uid THEN 3
                    END AS matched_slot
                FROM mw_recommendations_served
                WHERE customer_id = :customer_id
                  AND booked_unit_id IS NULL
                  AND served_at >= :cutoff
                  AND (
                      slot1_unit_id = :uid OR
                      slot2_unit_id = :uid OR
                      slot3_unit_id = :uid
                  )
                ORDER BY served_at DESC
                LIMIT 1
            """),
            {"uid": unit_id, "customer_id": customer_id, "cutoff": cutoff},
        ).fetchone()
        if row:
            return row[0], row[1]

    # Priority 3 — unit_id alone within last 4 h
    cutoff = now_utc - timedelta(hours=_UNIT_WINDOW_HOURS)
    row = db_session.execute(
        text("""
            SELECT id,
                CASE
                    WHEN slot1_unit_id = :uid THEN 1
                    WHEN slot2_unit_id = :uid THEN 2
                    WHEN slot3_unit_id = :uid THEN 3
                END AS matched_slot
            FROM mw_recommendations_served
            WHERE booked_unit_id IS NULL
              AND served_at >= :cutoff
              AND (
                  slot1_unit_id = :uid OR
                  slot2_unit_id = :uid OR
                  slot3_unit_id = :uid
              )
            ORDER BY served_at DESC
            LIMIT 1
        """),
        {"uid": unit_id, "cutoff": cutoff},
    ).fetchone()
    if row:
        return row[0], row[1]

    return None
