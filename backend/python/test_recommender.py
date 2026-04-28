"""
Unit tests for web/services/recommender.py

Covers:
  - normalise_request: coercion, required-field validation, defaults
  - relax_strategy: every (picked_slot, action) pair
  - build_slot1: cheapest exact-match; None when no site match
  - build_slot3: None when rate >= slot1; None when same unit_id; picks cheapest ±20%
  - resume_session: no-op when previous_request_id=None; exclude_unit_ids populated, strategy set
  - log_served: verifies INSERT is called with the right shape (mocked DB)

No live DB required — all DB calls are mocked via unittest.mock.
"""

import json
import sys
import os
import unittest
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, call, patch

# ---------------------------------------------------------------------------
# Path setup — allows running from backend/python or from the repo root
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

# Stub out the sync_service import that size_range_window would trigger so it
# works without a real DB in test mode.
_sync_service_stub = MagicMock()
sys.modules.setdefault('sync_service', _sync_service_stub)
sys.modules.setdefault('sync_service.config', _sync_service_stub)

from web.services.recommender import (
    CandidateRow,
    RecommendationRequest,
    ValidationError,
    build_slot1,
    build_slot2,
    build_slot3,
    log_served,
    normalise_request,
    relax_strategy,
    resume_session,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_req(**overrides) -> RecommendationRequest:
    """Build a minimal valid RecommendationRequest."""
    base = RecommendationRequest(
        mode='recommendation',
        level='standard',
        filters={'location': ['L017']},
        duration_months=6,
        constraints={
            'max_distance_km': 50,
            'include_legacy': True,
            'max_results': 3,
            'exclude_unit_ids': [],
        },
        context={
            'channel': 'api',
            'request_id': 'req-001',
            'session_id': 'sess-abc',
            'customer_id': None,
            'previous_request_id': None,
            'picked_slot': None,
            'action': None,
        },
    )
    for k, v in overrides.items():
        setattr(base, k, v)
    return base


def _make_row(
    site_code='L017',
    unit_id=100,
    plan_id=1,
    concession_id=10,
    std_rate='200.00',
    effective_rate='190.00',
    size_range='30-35',
    unit_type='W',
    climate_type='A',
    site_id=1,
    **kwargs,
) -> CandidateRow:
    """Build a minimal CandidateRow for testing."""
    return CandidateRow(
        site_id=site_id,
        site_code=site_code,
        unit_id=unit_id,
        plan_id=plan_id,
        concession_id=concession_id,
        unit_type=unit_type,
        climate_type=climate_type,
        size_range=size_range,
        std_rate=Decimal(std_rate),
        effective_rate=Decimal(effective_rate) if effective_rate else None,
        smart_lock=None,
        parse_ok=True,
        legacy_mapped=False,
        plan_name='Test Plan',
        min_duration_months=None,
        max_duration_months=None,
        distribution_channel=None,
        hidden_rate=None,
        amt_type=1,
        pct_discount=Decimal('5'),
        fixed_discount=Decimal('0'),
        max_amount_off=None,
        in_month=1,
        prepay=False,
        prepaid_months=0,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Tests: normalise_request
# ---------------------------------------------------------------------------

class TestNormaliseRequest(unittest.TestCase):

    def _raw(self, **overrides):
        base = {
            'duration_months': 6,
            'filters': {'location': 'L017'},
            'context': {'request_id': 'req-001'},
        }
        base.update(overrides)
        return base

    def test_scalar_location_coerced_to_list(self):
        req = normalise_request(self._raw())
        self.assertEqual(req.filters['location'], ['L017'])

    def test_list_location_passes_through(self):
        raw = self._raw(filters={'location': ['L017', 'L018']})
        req = normalise_request(raw)
        self.assertEqual(req.filters['location'], ['L017', 'L018'])

    def test_unit_type_scalar_coerced(self):
        raw = self._raw(filters={'location': 'L017', 'unit_type': 'W'})
        req = normalise_request(raw)
        self.assertEqual(req.filters['unit_type'], ['W'])

    def test_unit_type_list_passes_through(self):
        raw = self._raw(filters={'location': ['L017'], 'unit_type': ['W', 'L']})
        req = normalise_request(raw)
        self.assertEqual(req.filters['unit_type'], ['W', 'L'])

    def test_climate_type_coerced(self):
        raw = self._raw(filters={'location': 'L017', 'climate_type': 'A'})
        req = normalise_request(raw)
        self.assertEqual(req.filters['climate_type'], ['A'])

    def test_size_range_coerced(self):
        raw = self._raw(filters={'location': 'L017', 'size_range': '30-35'})
        req = normalise_request(raw)
        self.assertEqual(req.filters['size_range'], ['30-35'])

    def test_missing_duration_months_raises(self):
        raw = {'filters': {'location': 'L017'}, 'context': {'request_id': 'x'}}
        with self.assertRaises(ValidationError):
            normalise_request(raw)

    def test_missing_location_raises(self):
        raw = {'duration_months': 6, 'filters': {}, 'context': {'request_id': 'x'}}
        with self.assertRaises(ValidationError):
            normalise_request(raw)

    def test_missing_request_id_raises(self):
        raw = {'duration_months': 6, 'filters': {'location': 'L017'}, 'context': {}}
        with self.assertRaises(ValidationError):
            normalise_request(raw)

    def test_bad_duration_type_raises(self):
        raw = self._raw(duration_months='banana')
        with self.assertRaises(ValidationError):
            normalise_request(raw)

    def test_zero_duration_raises(self):
        raw = self._raw(duration_months=0)
        with self.assertRaises(ValidationError):
            normalise_request(raw)

    def test_negative_duration_raises(self):
        raw = self._raw(duration_months=-3)
        with self.assertRaises(ValidationError):
            normalise_request(raw)

    def test_defaults_populated(self):
        req = normalise_request(self._raw())
        self.assertEqual(req.mode, 'recommendation')
        self.assertEqual(req.level, 'standard')
        self.assertEqual(req.constraints['max_distance_km'], 50)
        self.assertEqual(req.constraints['max_results'], 3)
        self.assertTrue(req.constraints['include_legacy'])
        self.assertEqual(req.constraints['exclude_unit_ids'], [])

    def test_session_id_auto_generated_when_absent(self):
        req = normalise_request(self._raw())
        self.assertIsNotNone(req.context['session_id'])
        self.assertGreater(len(req.context['session_id']), 5)

    def test_session_id_preserved_when_given(self):
        raw = self._raw()
        raw['context']['session_id'] = 'my-session'
        req = normalise_request(raw)
        self.assertEqual(req.context['session_id'], 'my-session')

    def test_context_customer_id_optional(self):
        req = normalise_request(self._raw())
        self.assertIsNone(req.context['customer_id'])

    def test_mode_override(self):
        raw = self._raw()
        raw['mode'] = 'availability'
        req = normalise_request(raw)
        self.assertEqual(req.mode, 'availability')

    def test_not_a_dict_raises(self):
        with self.assertRaises(ValidationError):
            normalise_request("not a dict")

    def test_exclude_unit_ids_parsed(self):
        raw = self._raw()
        raw['constraints'] = {'exclude_unit_ids': [1, 2, 3]}
        req = normalise_request(raw)
        self.assertEqual(req.constraints['exclude_unit_ids'], [1, 2, 3])


# ---------------------------------------------------------------------------
# Tests: relax_strategy
# ---------------------------------------------------------------------------

class TestRelaxStrategy(unittest.TestCase):

    def test_none_when_no_args(self):
        self.assertEqual(relax_strategy(None, None), 'none')

    def test_none_when_empty_action(self):
        self.assertEqual(relax_strategy(None, ''), 'none')

    def test_slot1_more_like_this(self):
        self.assertEqual(relax_strategy(1, 'more_like_this'), 'size_plus_one')

    def test_slot2_more_like_this(self):
        self.assertEqual(relax_strategy(2, 'more_like_this'), 'next_nearest_site')

    def test_slot3_more_like_this(self):
        self.assertEqual(relax_strategy(3, 'more_like_this'), 'size_plus_one')

    def test_different_options(self):
        self.assertEqual(relax_strategy(1, 'different_options'), 'different_options')
        self.assertEqual(relax_strategy(None, 'different_options'), 'different_options')

    def test_expand_size(self):
        self.assertEqual(relax_strategy(1, 'expand_size'), 'wider_size_band')
        self.assertEqual(relax_strategy(None, 'expand_size'), 'wider_size_band')

    def test_different_type(self):
        self.assertEqual(relax_strategy(None, 'different_type'), 'expand_unit_type')

    def test_slot_only_no_action(self):
        # picked_slot without a recognised action defaults to size_plus_one
        self.assertEqual(relax_strategy(1, None), 'size_plus_one')
        self.assertEqual(relax_strategy(2, None), 'size_plus_one')

    def test_unknown_action_with_slot(self):
        # Unknown action with a slot → default to size_plus_one (slot-based)
        self.assertEqual(relax_strategy(1, 'bogus_action'), 'size_plus_one')


# ---------------------------------------------------------------------------
# Tests: build_slot1
# ---------------------------------------------------------------------------

class TestBuildSlot1(unittest.TestCase):

    def test_returns_cheapest_matching_site(self):
        req = _make_req()
        pool = [
            _make_row(unit_id=1, effective_rate='200.00'),
            _make_row(unit_id=2, effective_rate='180.00'),
            _make_row(unit_id=3, effective_rate='210.00'),
        ]
        result = build_slot1(pool, req)
        self.assertIsNotNone(result)
        self.assertEqual(result.unit_id, 2)

    def test_returns_none_when_no_site_match(self):
        req = _make_req(filters={'location': ['L017']})
        pool = [
            _make_row(site_code='L018', unit_id=1),
            _make_row(site_code='L018', unit_id=2),
        ]
        result = build_slot1(pool, req)
        self.assertIsNone(result)

    def test_returns_none_when_pool_empty(self):
        req = _make_req()
        result = build_slot1([], req)
        self.assertIsNone(result)

    def test_handles_none_effective_rate(self):
        req = _make_req()
        pool = [
            _make_row(unit_id=1, effective_rate=None, std_rate='200.00'),
            _make_row(unit_id=2, effective_rate='180.00'),
        ]
        result = build_slot1(pool, req)
        # None effective_rate sorts last → unit 2 wins
        self.assertEqual(result.unit_id, 2)

    def test_single_matching_unit(self):
        req = _make_req()
        pool = [_make_row(unit_id=5)]
        result = build_slot1(pool, req)
        self.assertIsNotNone(result)
        self.assertEqual(result.unit_id, 5)

    def test_multiple_locations(self):
        req = _make_req(filters={'location': ['L017', 'L018']})
        pool = [
            _make_row(site_code='L018', unit_id=10, effective_rate='150.00'),
            _make_row(site_code='L017', unit_id=11, effective_rate='160.00'),
        ]
        result = build_slot1(pool, req)
        self.assertEqual(result.unit_id, 10)  # cheapest across both sites


# ---------------------------------------------------------------------------
# Tests: build_slot3
# ---------------------------------------------------------------------------

class TestBuildSlot3(unittest.TestCase):

    def setUp(self):
        # build_slot3 lazy-imports from common.size_range_window so patch there.
        patcher = patch(
            'common.size_range_window.size_range_neighbours',
            return_value=['25-30', '30-35', '35-40'],
        )
        self.mock_neighbours = patcher.start()
        self.addCleanup(patcher.stop)

    def _mock_db(self):
        return MagicMock()

    def test_returns_none_when_slot1_is_none(self):
        req = _make_req()
        result = build_slot3([], req, slot1=None, db_session=self._mock_db())
        self.assertIsNone(result)

    def test_returns_none_when_slot3_rate_equal_to_slot1(self):
        req = _make_req(filters={'location': ['L017'], 'size_range': ['30-35']})
        slot1 = _make_row(unit_id=1, effective_rate='180.00')
        pool = [
            _make_row(unit_id=2, effective_rate='180.00', size_range='25-30'),
        ]
        result = build_slot3(pool, req, slot1=slot1, db_session=self._mock_db())
        self.assertIsNone(result)

    def test_returns_none_when_slot3_rate_more_than_slot1(self):
        req = _make_req(filters={'location': ['L017'], 'size_range': ['30-35']})
        slot1 = _make_row(unit_id=1, effective_rate='180.00')
        pool = [
            _make_row(unit_id=2, effective_rate='200.00', size_range='25-30'),
        ]
        result = build_slot3(pool, req, slot1=slot1, db_session=self._mock_db())
        self.assertIsNone(result)

    def test_returns_none_when_same_unit_id_as_slot1(self):
        req = _make_req(filters={'location': ['L017'], 'size_range': ['30-35']})
        slot1 = _make_row(unit_id=1, effective_rate='180.00')
        pool = [
            _make_row(unit_id=1, effective_rate='150.00', size_range='25-30'),
        ]
        result = build_slot3(pool, req, slot1=slot1, db_session=self._mock_db())
        self.assertIsNone(result)

    def test_returns_cheapest_neighbour_cheaper_than_slot1(self):
        req = _make_req(filters={'location': ['L017'], 'size_range': ['30-35']})
        slot1 = _make_row(unit_id=1, effective_rate='180.00')
        pool = [
            _make_row(unit_id=2, effective_rate='160.00', size_range='25-30'),
            _make_row(unit_id=3, effective_rate='150.00', size_range='35-40'),
            _make_row(unit_id=4, effective_rate='170.00', size_range='25-30'),
        ]
        result = build_slot3(pool, req, slot1=slot1, db_session=self._mock_db())
        self.assertIsNotNone(result)
        self.assertEqual(result.unit_id, 3)  # cheapest

    def test_ignores_units_at_different_site(self):
        req = _make_req(filters={'location': ['L017'], 'size_range': ['30-35']})
        slot1 = _make_row(unit_id=1, effective_rate='180.00', site_code='L017')
        pool = [
            _make_row(unit_id=2, effective_rate='120.00', size_range='25-30',
                      site_code='L018'),  # different site — excluded
        ]
        result = build_slot3(pool, req, slot1=slot1, db_session=self._mock_db())
        self.assertIsNone(result)

    def test_handles_none_effective_rate_on_slot1(self):
        """slot1 with None effective_rate falls back to std_rate for comparison."""
        req = _make_req(filters={'location': ['L017'], 'size_range': ['30-35']})
        slot1 = _make_row(unit_id=1, effective_rate=None, std_rate='200.00')
        pool = [
            _make_row(unit_id=2, effective_rate='150.00', size_range='25-30'),
        ]
        result = build_slot3(pool, req, slot1=slot1, db_session=self._mock_db())
        self.assertIsNotNone(result)
        self.assertEqual(result.unit_id, 2)

    def test_no_size_filter_includes_all_sizes(self):
        """When no size_range in filters, slot3 considers all sizes at the site."""
        req = _make_req(filters={'location': ['L017']})
        slot1 = _make_row(unit_id=1, effective_rate='200.00', size_range='30-35')
        pool = [
            _make_row(unit_id=2, effective_rate='150.00', size_range='50-60'),
        ]
        result = build_slot3(pool, req, slot1=slot1, db_session=self._mock_db())
        self.assertIsNotNone(result)
        self.assertEqual(result.unit_id, 2)


# ---------------------------------------------------------------------------
# Tests: resume_session
# ---------------------------------------------------------------------------

class TestResumeSession(unittest.TestCase):

    def _mock_db_no_prior(self):
        """DB that returns no prior row."""
        db = MagicMock()
        db.execute.return_value.mappings.return_value.first.return_value = None
        return db

    def _mock_db_with_prior(self, prior_filters=None, served_units=None):
        """DB that returns a fake prior row + session rows."""
        db = MagicMock()

        prior_row = {
            'filters_applied': json.dumps(prior_filters or {'location': ['L017']}),
            'slot1_unit_id': 100,
            'slot2_unit_id': 200,
            'slot3_unit_id': None,
            'session_id': 'sess-abc',
        }

        # Mock the mappings().first() call for the prior row lookup
        mock_first_result = MagicMock()
        mock_first_result.__getitem__ = lambda self, k: prior_row[k]
        mock_first_result.get = lambda k, d=None: prior_row.get(k, d)

        # Build a proper mock mapping
        prior_mapping = MagicMock()
        prior_mapping.__getitem__ = lambda self_, k: prior_row[k]
        prior_mapping.get = lambda k, d=None: prior_row.get(k, d)
        prior_mapping.__bool__ = lambda self_: True

        # Session rows for exclude_unit_ids
        session_row1 = (100, 200, None)
        session_rows_result = served_units if served_units is not None else [session_row1]

        call_count = [0]

        def side_effect(sql, params=None):
            mock_result = MagicMock()
            sql_str = str(sql)
            if 'mw_recommendations_served' in sql_str and 'request_id' in sql_str and call_count[0] == 0:
                call_count[0] += 1
                mock_result.mappings.return_value.first.return_value = prior_mapping
            else:
                mock_result.fetchall.return_value = session_rows_result
            return mock_result

        db.execute.side_effect = side_effect
        return db

    def test_noop_when_no_previous_request_id(self):
        req = _make_req()
        req.context['previous_request_id'] = None
        db = MagicMock()
        result = resume_session(req, db)
        db.execute.assert_not_called()
        self.assertEqual(result.constraints['exclude_unit_ids'], [])

    def test_exclude_unit_ids_populated_from_session(self):
        req = _make_req()
        req.context['previous_request_id'] = 'req-000'
        req.context['session_id'] = 'sess-abc'

        db = self._mock_db_with_prior()
        result = resume_session(req, db)

        # Units 100 and 200 were served in prior turns
        excluded = set(result.constraints['exclude_unit_ids'])
        self.assertIn(100, excluded)
        self.assertIn(200, excluded)

    def test_relax_strategy_set_on_context(self):
        req = _make_req()
        req.context['previous_request_id'] = 'req-000'
        req.context['picked_slot'] = 1
        req.context['action'] = 'more_like_this'

        db = self._mock_db_with_prior()
        result = resume_session(req, db)

        self.assertEqual(result.context.get('_relax_strategy'), 'size_plus_one')

    def test_prior_filters_merged_with_current_override(self):
        req = _make_req()
        req.context['previous_request_id'] = 'req-000'
        # Current request has unit_type filter — should override prior
        req.filters['unit_type'] = ['L']

        prior_filters = {'location': ['L017'], 'unit_type': ['W'], 'climate_type': ['A']}
        db = self._mock_db_with_prior(prior_filters=prior_filters)
        result = resume_session(req, db)

        # location comes from prior (since current has it too, current wins)
        self.assertIn('climate_type', result.filters)  # carried from prior
        self.assertEqual(result.filters['unit_type'], ['L'])  # current override

    def test_prior_not_found_returns_req_unchanged(self):
        req = _make_req()
        req.context['previous_request_id'] = 'req-999'

        db = self._mock_db_no_prior()
        result = resume_session(req, db)
        # Should return req unchanged
        self.assertEqual(result.constraints['exclude_unit_ids'], [])

    def test_db_error_handled_gracefully(self):
        req = _make_req()
        req.context['previous_request_id'] = 'req-000'

        db = MagicMock()
        db.execute.side_effect = Exception("DB error")
        # Should not raise
        result = resume_session(req, db)
        self.assertIsNotNone(result)


# ---------------------------------------------------------------------------
# Tests: log_served
# ---------------------------------------------------------------------------

class TestLogServed(unittest.TestCase):

    def _make_quote(self, unit_id=1, plan_id=1, concession_id=0,
                    first_month='250.00', total_contract='1500.00'):
        """Build a minimal DurationQuote-like object."""
        q = MagicMock()
        q.unit_id = unit_id
        q.plan_id = plan_id
        q.concession_id = concession_id
        q.first_month_total = Decimal(first_month)
        q.total_contract = Decimal(total_contract)
        return q

    def test_insert_called_with_returning_id(self):
        req = _make_req()
        slot1_row = _make_row(unit_id=101, plan_id=1, concession_id=10)
        slot1_quote = self._make_quote(unit_id=101)

        db = MagicMock()
        db.execute.return_value.fetchone.return_value = (42,)

        tracking_id = log_served(
            req=req,
            slots_with_quotes=[(slot1_row, slot1_quote), None, None],
            pool_size=15,
            total_matches=5,
            relax_strategy_used='none',
            response={'slots': []},
            db_session=db,
        )

        self.assertEqual(tracking_id, 42)
        db.execute.assert_called_once()

    def test_none_slots_written_as_null(self):
        req = _make_req()
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = (7,)

        tracking_id = log_served(
            req=req,
            slots_with_quotes=[None, None, None],
            pool_size=0,
            total_matches=0,
            relax_strategy_used='none',
            response={},
            db_session=db,
        )

        self.assertEqual(tracking_id, 7)
        # Verify the params passed to execute contain None slot values.
        call_args = db.execute.call_args
        params = call_args[0][1] if call_args[0] and len(call_args[0]) > 1 else call_args[1]
        if isinstance(params, dict):
            self.assertIsNone(params.get('slot1_unit_id'))
            self.assertIsNone(params.get('slot2_unit_id'))
            self.assertIsNone(params.get('slot3_unit_id'))

    def test_slot_values_mapped_correctly(self):
        req = _make_req()
        row = _make_row(unit_id=55, plan_id=2, concession_id=20)
        quote = self._make_quote(unit_id=55, first_month='300.00', total_contract='1800.00')

        db = MagicMock()
        db.execute.return_value.fetchone.return_value = (99,)

        log_served(
            req=req,
            slots_with_quotes=[(row, quote), None, None],
            pool_size=10,
            total_matches=3,
            relax_strategy_used='none',
            response={},
            db_session=db,
        )

        call_args = db.execute.call_args
        params = call_args[0][1] if call_args[0] and len(call_args[0]) > 1 else call_args[1]
        if isinstance(params, dict):
            self.assertEqual(params.get('slot1_unit_id'), 55)
            self.assertEqual(params.get('slot1_plan_id'), 2)
            self.assertEqual(params.get('slot1_concession_id'), 20)
            self.assertAlmostEqual(params.get('slot1_first_month'), 300.0)
            self.assertAlmostEqual(params.get('slot1_total_contract'), 1800.0)

    def test_request_id_in_params(self):
        req = _make_req()
        db = MagicMock()
        db.execute.return_value.fetchone.return_value = (1,)

        log_served(
            req=req,
            slots_with_quotes=[],
            pool_size=0,
            total_matches=0,
            relax_strategy_used='none',
            response={},
            db_session=db,
        )

        call_args = db.execute.call_args
        params = call_args[0][1] if call_args[0] and len(call_args[0]) > 1 else call_args[1]
        if isinstance(params, dict):
            self.assertEqual(params.get('request_id'), 'req-001')
            self.assertEqual(params.get('session_id'), 'sess-abc')
            self.assertEqual(params.get('mode'), 'recommendation')


# ---------------------------------------------------------------------------
# Tests: fetch_candidate_pool (SQL shape only — no live DB)
# ---------------------------------------------------------------------------

class TestFetchCandidatePool(unittest.TestCase):
    """
    We can't run a real query, but we can verify that the function handles
    an empty result gracefully and that a bad row doesn't crash the whole call.
    """

    def test_returns_empty_list_on_db_error(self):
        from web.services.recommender import fetch_candidate_pool
        req = _make_req()
        db = MagicMock()
        db.execute.side_effect = Exception("connection lost")
        result = fetch_candidate_pool(req, db)
        self.assertEqual(result, [])

    def test_returns_empty_list_when_no_locations(self):
        from web.services.recommender import fetch_candidate_pool
        req = _make_req(filters={})
        db = MagicMock()
        result = fetch_candidate_pool(req, db)
        self.assertEqual(result, [])
        db.execute.assert_not_called()

    def test_returns_rows_when_db_succeeds(self):
        from web.services.recommender import fetch_candidate_pool
        req = _make_req()
        db = MagicMock()

        fake_row = {
            'site_id': 1, 'site_code': 'L017', 'unit_id': 10,
            'plan_id': 1, 'concession_id': 5,
            'unit_type': 'W', 'climate_type': 'A', 'size_range': '30-35',
            'std_rate': '200.00', 'effective_rate': '190.00',
            'smart_lock': None, 'parse_ok': True, 'legacy_mapped': False,
            'plan_name': 'Test', 'min_duration_months': None,
            'max_duration_months': None, 'distribution_channel': None,
            'hidden_rate': None, 'amt_type': 1,
            'pct_discount': '5', 'fixed_discount': '0',
            'max_amount_off': None, 'in_month': 1,
            'prepay': False, 'prepaid_months': 0,
        }
        fake_mapping = MagicMock()
        fake_mapping.__getitem__ = lambda self_, k: fake_row[k]
        fake_mapping.get = lambda k, d=None: fake_row.get(k, d)

        db.execute.return_value.mappings.return_value.all.return_value = [fake_mapping]
        result = fetch_candidate_pool(req, db)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].unit_id, 10)
        self.assertEqual(result[0].site_code, 'L017')


# ---------------------------------------------------------------------------
# Tests: build_slot2 (mocked distance query)
# ---------------------------------------------------------------------------

class TestBuildSlot2(unittest.TestCase):

    def test_returns_none_when_no_other_sites_in_pool(self):
        req = _make_req(filters={'location': ['L017']})
        pool = [_make_row(site_code='L017', unit_id=1)]
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = []
        result = build_slot2(pool, req, db)
        self.assertIsNone(result)

    def test_returns_closest_site_with_candidates(self):
        # build_slot2 now re-runs fetch_candidate_pool per neighbour.
        # Patch it to simulate L018 + L019 inventories.
        req = _make_req(filters={'location': ['L017']})
        pool = [_make_row(site_code='L017', unit_id=1, effective_rate='200.00')]
        db = MagicMock()
        db.execute.return_value.fetchall.return_value = [
            ('L018', 5.0),
            ('L019', 10.0),
        ]
        l018_row = _make_row(site_code='L018', unit_id=2, effective_rate='190.00')
        l019_row = _make_row(site_code='L019', unit_id=3, effective_rate='180.00')
        with patch('web.services.recommender.fetch_candidate_pool',
                   side_effect=lambda r, _db: {'L018': [l018_row], 'L019': [l019_row]}.get(
                       r.filters['location'][0], [])):
            result = build_slot2(pool, req, db)
        self.assertIsNotNone(result)
        self.assertEqual(result.site_code, 'L018')

    def test_skips_site_with_no_candidates_in_pool(self):
        req = _make_req(filters={'location': ['L017']})
        pool = [_make_row(site_code='L017', unit_id=1)]
        db = MagicMock()
        # L018 is closer but no inventory; L019 farther, has inventory
        db.execute.return_value.fetchall.return_value = [
            ('L018', 5.0),
            ('L019', 10.0),
        ]
        l019_row = _make_row(site_code='L019', unit_id=3, effective_rate='180.00')
        with patch('web.services.recommender.fetch_candidate_pool',
                   side_effect=lambda r, _db: {'L019': [l019_row]}.get(
                       r.filters['location'][0], [])):
            result = build_slot2(pool, req, db)
        self.assertIsNotNone(result)
        self.assertEqual(result.site_code, 'L019')

    def test_db_error_returns_none(self):
        req = _make_req(filters={'location': ['L017']})
        pool = [_make_row(site_code='L018', unit_id=2)]
        db = MagicMock()
        db.execute.side_effect = Exception("db error")
        result = build_slot2(pool, req, db)
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    unittest.main()
