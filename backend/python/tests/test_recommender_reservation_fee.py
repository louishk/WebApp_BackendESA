"""
Tests for recommender._resolve_reservation_fee.

Run with:
    cd backend/python
    python -m pytest tests/test_recommender_reservation_fee.py -v
"""
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from web.services.recommender import _resolve_reservation_fee


def _make_rf(fee_value):
    """Return a mock ReservationFee ORM object."""
    rf = MagicMock()
    rf.reservation_fee = fee_value
    return rf


def _make_db(query_result):
    """Return a mock db_session whose .query(...).filter_by(...).first() returns query_result."""
    db = MagicMock()
    db.query.return_value.filter_by.return_value.first.return_value = query_result
    return db


class TestResolveReservationFee:
    def test_returns_override_when_row_exists(self):
        rf = _make_rf(Decimal('150.00'))
        db = _make_db(rf)

        fee, source = _resolve_reservation_fee(
            site_id=42, std_rate=Decimal('200.00'), db_session=db
        )

        assert source == 'override'
        assert fee == Decimal('150.00')

    def test_returns_std_rate_default_when_no_row(self):
        db = _make_db(None)  # no override row

        fee, source = _resolve_reservation_fee(
            site_id=42, std_rate=Decimal('200.00'), db_session=db
        )

        assert source == 'default'
        assert fee == Decimal('200.00')

    def test_returns_none_default_when_no_row_and_no_std_rate(self):
        db = _make_db(None)

        fee, source = _resolve_reservation_fee(
            site_id=42, std_rate=None, db_session=db
        )

        assert source == 'default'
        assert fee is None

    def test_cache_hit_on_second_call_with_same_site_id(self):
        rf = _make_rf(Decimal('99.00'))
        db = _make_db(rf)
        cache: dict = {}

        # First call — should hit DB.
        fee1, src1 = _resolve_reservation_fee(
            site_id=7, std_rate=Decimal('100.00'), db_session=db, site_cache=cache
        )
        # Second call — same site_id, should use cache.
        fee2, src2 = _resolve_reservation_fee(
            site_id=7, std_rate=Decimal('100.00'), db_session=db, site_cache=cache
        )

        # DB should only have been queried once across both calls.
        assert db.query.call_count == 1
        assert fee1 == fee2 == Decimal('99.00')
        assert src1 == src2 == 'override'

    def test_cache_miss_for_different_site_ids(self):
        rf_a = _make_rf(Decimal('80.00'))
        rf_b = _make_rf(Decimal('120.00'))

        db = MagicMock()
        db.query.return_value.filter_by.return_value.first.side_effect = [rf_a, rf_b]
        cache: dict = {}

        fee_a, _ = _resolve_reservation_fee(site_id=1, std_rate=None, db_session=db, site_cache=cache)
        fee_b, _ = _resolve_reservation_fee(site_id=2, std_rate=None, db_session=db, site_cache=cache)

        assert db.query.call_count == 2
        assert fee_a == Decimal('80.00')
        assert fee_b == Decimal('120.00')

    def test_db_error_falls_back_to_std_rate(self):
        db = MagicMock()
        db.query.side_effect = Exception("DB unavailable")

        fee, source = _resolve_reservation_fee(
            site_id=99, std_rate=Decimal('175.00'), db_session=db
        )

        assert source == 'default'
        assert fee == Decimal('175.00')
