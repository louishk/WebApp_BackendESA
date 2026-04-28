"""
Tests for web/services/booking_outcomes.py

Uses an in-memory SQLite database that mirrors the columns of
mw_recommendations_served that the helper reads/writes.  No live DB or Flask
app required.

Run with:
    cd backend/python
    python -m pytest test_booking_outcomes.py -v
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ---------------------------------------------------------------------------
# Minimal schema (only the columns the helper touches)
# ---------------------------------------------------------------------------
_DDL = """
CREATE TABLE IF NOT EXISTS mw_recommendations_served (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    served_at       TIMESTAMP NOT NULL,
    request_id      TEXT UNIQUE NOT NULL,
    session_id      TEXT NOT NULL,
    customer_id     TEXT,
    channel         TEXT NOT NULL DEFAULT 'chatbot',
    mode            TEXT NOT NULL DEFAULT 'recommendation',
    request_payload TEXT NOT NULL DEFAULT '{}',
    filters_applied TEXT NOT NULL DEFAULT '{}',

    slot1_unit_id   INTEGER,
    slot1_plan_id   INTEGER,
    slot1_concession_id INTEGER,
    slot1_first_month   REAL,
    slot1_total_contract REAL,

    slot2_unit_id   INTEGER,
    slot2_plan_id   INTEGER,
    slot2_concession_id INTEGER,
    slot2_first_month   REAL,
    slot2_total_contract REAL,

    slot3_unit_id   INTEGER,
    slot3_plan_id   INTEGER,
    slot3_concession_id INTEGER,
    slot3_first_month   REAL,
    slot3_total_contract REAL,

    booked_unit_id  INTEGER,
    booked_plan_id  INTEGER,
    booked_at       TIMESTAMP,
    booked_slot     INTEGER
)
"""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def db_session():
    """Fresh in-memory SQLite session per test."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    with engine.connect() as conn:
        conn.execute(text(_DDL))
        conn.commit()
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    engine.dispose()


def _now():
    return datetime.now(timezone.utc)


def _insert_rec(
    session,
    *,
    session_id: str,
    customer_id: str | None = None,
    slot1_unit_id: int | None = None,
    slot2_unit_id: int | None = None,
    slot3_unit_id: int | None = None,
    served_at: datetime | None = None,
    request_id: str | None = None,
) -> int:
    """Insert a fake mw_recommendations_served row; returns new id."""
    if served_at is None:
        served_at = _now()
    if request_id is None:
        import uuid
        request_id = str(uuid.uuid4())

    result = session.execute(
        text("""
            INSERT INTO mw_recommendations_served
                (served_at, request_id, session_id, customer_id,
                 slot1_unit_id, slot2_unit_id, slot3_unit_id)
            VALUES
                (:served_at, :request_id, :session_id, :customer_id,
                 :s1, :s2, :s3)
        """),
        {
            "served_at": served_at.replace(tzinfo=None) if served_at.tzinfo else served_at,
            "request_id": request_id,
            "session_id": session_id,
            "customer_id": customer_id,
            "s1": slot1_unit_id,
            "s2": slot2_unit_id,
            "s3": slot3_unit_id,
        },
    )
    session.commit()
    return result.lastrowid


def _fetch(session, row_id: int) -> dict:
    row = session.execute(
        text("SELECT * FROM mw_recommendations_served WHERE id = :id"),
        {"id": row_id},
    ).fetchone()
    return dict(row._mapping) if row else {}


# ---------------------------------------------------------------------------
# Import the helper (adjust sys.path so the relative import works when running
# from the backend/python directory)
# ---------------------------------------------------------------------------

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from web.services.booking_outcomes import link_booking_to_recommendation


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestSessionMatch:
    """Priority 1: session_id-anchored match."""

    def test_slot1_match(self, db_session):
        rec_id = _insert_rec(db_session, session_id="abc", slot1_unit_id=12345)

        result = link_booking_to_recommendation(
            unit_id=12345,
            plan_id=7,
            concession_id=3,
            customer_id=None,
            session_id="abc",
            booked_at=_now(),
            db_session=db_session,
        )

        assert result == rec_id
        row = _fetch(db_session, rec_id)
        assert row["booked_unit_id"] == 12345
        assert row["booked_slot"] == 1
        assert row["booked_plan_id"] == 7

    def test_slot2_match(self, db_session):
        rec_id = _insert_rec(
            db_session,
            session_id="s2",
            slot1_unit_id=111,
            slot2_unit_id=12345,
        )

        result = link_booking_to_recommendation(
            unit_id=12345,
            plan_id=None,
            concession_id=None,
            customer_id=None,
            session_id="s2",
            booked_at=_now(),
            db_session=db_session,
        )

        assert result == rec_id
        assert _fetch(db_session, rec_id)["booked_slot"] == 2

    def test_slot3_match(self, db_session):
        rec_id = _insert_rec(
            db_session,
            session_id="s3",
            slot1_unit_id=1,
            slot2_unit_id=2,
            slot3_unit_id=12345,
        )

        result = link_booking_to_recommendation(
            unit_id=12345,
            plan_id=None,
            concession_id=None,
            customer_id=None,
            session_id="s3",
            booked_at=_now(),
            db_session=db_session,
        )

        assert result == rec_id
        assert _fetch(db_session, rec_id)["booked_slot"] == 3

    def test_no_matching_row_returns_none(self, db_session):
        # Row exists but for a different unit
        _insert_rec(db_session, session_id="abc", slot1_unit_id=99999)

        result = link_booking_to_recommendation(
            unit_id=12345,
            plan_id=None,
            concession_id=None,
            customer_id=None,
            session_id="abc",
            booked_at=_now(),
            db_session=db_session,
        )

        assert result is None

    def test_most_recent_row_wins(self, db_session):
        """Two rows in the same session — most recent one is stamped."""
        older_at = _now() - timedelta(minutes=10)
        newer_at = _now() - timedelta(minutes=2)

        older_id = _insert_rec(
            db_session,
            session_id="abc",
            slot1_unit_id=12345,
            served_at=older_at,
            request_id="req-old",
        )
        newer_id = _insert_rec(
            db_session,
            session_id="abc",
            slot1_unit_id=12345,
            served_at=newer_at,
            request_id="req-new",
        )

        result = link_booking_to_recommendation(
            unit_id=12345,
            plan_id=None,
            concession_id=None,
            customer_id=None,
            session_id="abc",
            booked_at=_now(),
            db_session=db_session,
        )

        assert result == newer_id
        # Older row untouched
        assert _fetch(db_session, older_id)["booked_unit_id"] is None


class TestCustomerIdMatch:
    """Priority 2: customer_id match within 24h."""

    def test_customer_match_within_window(self, db_session):
        served = _now() - timedelta(hours=2)
        rec_id = _insert_rec(
            db_session,
            session_id="different-session",
            customer_id="cust-42",
            slot1_unit_id=12345,
            served_at=served,
        )

        result = link_booking_to_recommendation(
            unit_id=12345,
            plan_id=None,
            concession_id=None,
            customer_id="cust-42",
            session_id=None,           # no session_id → falls to priority 2
            booked_at=_now(),
            db_session=db_session,
        )

        assert result == rec_id

    def test_customer_match_outside_window_returns_none(self, db_session):
        served = _now() - timedelta(hours=25)
        _insert_rec(
            db_session,
            session_id="s",
            customer_id="cust-42",
            slot1_unit_id=12345,
            served_at=served,
        )

        result = link_booking_to_recommendation(
            unit_id=12345,
            plan_id=None,
            concession_id=None,
            customer_id="cust-42",
            session_id=None,
            booked_at=_now(),
            db_session=db_session,
        )

        assert result is None


class TestUnitOnlyMatch:
    """Priority 3: bare unit_id match within 4h."""

    def test_unit_match_within_4h(self, db_session):
        served = _now() - timedelta(hours=3)
        rec_id = _insert_rec(
            db_session,
            session_id="s-anon",
            customer_id=None,
            slot2_unit_id=12345,
            served_at=served,
        )

        result = link_booking_to_recommendation(
            unit_id=12345,
            plan_id=None,
            concession_id=None,
            customer_id=None,          # no customer
            session_id=None,           # no session → priority 3
            booked_at=_now(),
            db_session=db_session,
        )

        assert result == rec_id

    def test_unit_match_outside_4h_returns_none(self, db_session):
        served = _now() - timedelta(hours=5)
        _insert_rec(
            db_session,
            session_id="s-anon",
            slot1_unit_id=12345,
            served_at=served,
        )

        result = link_booking_to_recommendation(
            unit_id=12345,
            plan_id=None,
            concession_id=None,
            customer_id=None,
            session_id=None,
            booked_at=_now(),
            db_session=db_session,
        )

        assert result is None


class TestIdempotencyAndEdgeCases:
    """Ensure the WHERE booked_unit_id IS NULL guard prevents double-stamping."""

    def test_already_booked_row_not_overwritten(self, db_session):
        rec_id = _insert_rec(db_session, session_id="abc", slot1_unit_id=12345)

        # First booking stamps the row
        r1 = link_booking_to_recommendation(
            unit_id=12345,
            plan_id=1,
            concession_id=None,
            customer_id=None,
            session_id="abc",
            booked_at=_now(),
            db_session=db_session,
        )
        assert r1 == rec_id

        # Second call with same session — row is already booked, nothing to update
        r2 = link_booking_to_recommendation(
            unit_id=12345,
            plan_id=99,
            concession_id=None,
            customer_id=None,
            session_id="abc",
            booked_at=_now(),
            db_session=db_session,
        )
        # No new unbooked row exists → None
        assert r2 is None
        # Original stamp unchanged
        assert _fetch(db_session, rec_id)["booked_plan_id"] == 1

    def test_db_error_returns_none_no_raise(self, db_session):
        """A DB-level failure must be swallowed and return None."""
        bad_session = MagicMock()
        bad_session.execute.side_effect = Exception("simulated DB failure")
        bad_session.rollback = MagicMock()

        result = link_booking_to_recommendation(
            unit_id=12345,
            plan_id=None,
            concession_id=None,
            customer_id=None,
            session_id="abc",
            booked_at=_now(),
            db_session=bad_session,
        )

        assert result is None
        bad_session.rollback.assert_called_once()

    def test_db_error_logs_warning(self, db_session, caplog):
        bad_session = MagicMock()
        bad_session.execute.side_effect = Exception("boom")
        bad_session.rollback = MagicMock()

        with caplog.at_level(logging.WARNING, logger="web.services.booking_outcomes"):
            link_booking_to_recommendation(
                unit_id=99,
                plan_id=None,
                concession_id=None,
                customer_id=None,
                session_id=None,
                booked_at=_now(),
                db_session=bad_session,
            )

        assert any("recommendation row not stamped" in r.message for r in caplog.records)

    def test_no_rows_at_all_returns_none(self, db_session):
        result = link_booking_to_recommendation(
            unit_id=12345,
            plan_id=None,
            concession_id=None,
            customer_id=None,
            session_id="no-such-session",
            booked_at=_now(),
            db_session=db_session,
        )
        assert result is None

    def test_session_priority_beats_customer_id(self, db_session):
        """session_id match should be used even when customer_id also has a match."""
        # Customer-matched row (older)
        cust_id = _insert_rec(
            db_session,
            session_id="other-session",
            customer_id="cust-X",
            slot1_unit_id=12345,
            served_at=_now() - timedelta(hours=1),
            request_id="req-cust",
        )
        # Session-matched row (newer)
        sess_id = _insert_rec(
            db_session,
            session_id="my-session",
            customer_id="cust-X",
            slot1_unit_id=12345,
            served_at=_now() - timedelta(minutes=5),
            request_id="req-sess",
        )

        result = link_booking_to_recommendation(
            unit_id=12345,
            plan_id=None,
            concession_id=None,
            customer_id="cust-X",
            session_id="my-session",
            booked_at=_now(),
            db_session=db_session,
        )

        # Should pick the session-matched row, not the customer-id row
        assert result == sess_id
        assert _fetch(db_session, cust_id)["booked_unit_id"] is None
