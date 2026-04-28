"""
Integration tests for POST /api/recommendations.

Uses the live middleware DB (Azure Postgres, accessible from dev machine).
Auth decorators are patched so no real JWT/API-key is needed.

Targeting plan #9 "Opening Offer YC" at L031 — the only active plan in the
middleware DB during development.

Run:
    cd backend/python && python3 -m pytest test_recommendations_api.py -q
"""
from __future__ import annotations

import json
import sys
import os
import uuid
import unittest
from unittest.mock import patch, MagicMock
from functools import wraps

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

# Stub missing optional dependencies before any app import
def _stub(name):
    m = MagicMock()
    m.__name__ = name
    sys.modules.setdefault(name, m)
    return m

from flask import Blueprint as _Blueprint  # noqa: E402  (imported before create_app)

_sync_stub = _stub('sync_service')
_stub('sync_service.config')

# sync_service.api must expose a real Blueprint so csrf.exempt works
_sync_api_stub = _stub('sync_service.api')
_real_sync_bp = _Blueprint('sync_service', __name__, url_prefix='/sync')
_sync_api_stub.sync_service_bp = _real_sync_bp
_sync_stub.sync_service_bp = _real_sync_bp

_stub('stripe')
_stub('twilio')
_stub('twilio.rest')


# ---------------------------------------------------------------------------
# Auth bypass — patch decorators before the blueprint is imported
# ---------------------------------------------------------------------------

def _passthrough(f):
    """Identity decorator — used to replace require_auth / require_api_scope."""
    @wraps(f)
    def wrapper(*args, **kwargs):
        from flask import g
        g.current_user = {'sub': 'test', 'auth_method': 'api_key', 'key_id': 'test'}
        g.api_key_scopes = ['recommender']
        return f(*args, **kwargs)
    return wrapper


def _scope_passthrough(scope):
    def decorator(f):
        return f
    return decorator


# Apply patches before importing the routes module
_auth_patch = patch('web.auth.jwt_auth.require_auth', side_effect=lambda f: _passthrough(f))
_scope_patch = patch('web.auth.jwt_auth.require_api_scope', side_effect=_scope_passthrough)
_auth_patch.start()
_scope_patch.start()


# ---------------------------------------------------------------------------
# Now create the app
# ---------------------------------------------------------------------------

from web.app import create_app  # noqa: E402

app = create_app()
app.config['TESTING'] = True
app.config['WTF_CSRF_ENABLED'] = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _uid() -> str:
    return str(uuid.uuid4())


def _base_payload(**overrides) -> dict:
    """Minimal valid payload targeting L031 / Opening Offer YC."""
    payload = {
        'mode': 'recommendation',
        'level': 'standard',
        'duration_months': 6,
        'filters': {
            'location': ['L031'],
            'unit_type': ['W'],
            'size_range': ['30-35'],
        },
        'context': {
            'channel': 'chatbot',
            'request_id': _uid(),
            'session_id': _uid(),
        },
        'constraints': {},
    }
    payload.update(overrides)
    return payload


def _post(client, payload: dict) -> tuple:
    resp = client.post(
        '/api/recommendations',
        data=json.dumps(payload),
        content_type='application/json',
    )
    return resp, resp.get_json()


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestRecommendationsAPI(unittest.TestCase):

    def setUp(self):
        self.client = app.test_client()

    # ------------------------------------------------------------------
    # TC-1: Valid request → 200 + envelope shape
    # ------------------------------------------------------------------
    def test_valid_request_returns_200_and_envelope_shape(self):
        resp, body = _post(self.client, _base_payload())
        self.assertEqual(resp.status_code, 200, body)

        # Top-level envelope keys
        for key in ('mode', 'level', 'request_id', 'served_at', 'ttl_seconds',
                    'stats', 'slots', 'tracking_id'):
            self.assertIn(key, body, f"Missing key: {key}")

        self.assertEqual(body['mode'], 'recommendation')
        self.assertEqual(body['level'], 'standard')
        self.assertEqual(body['ttl_seconds'], 60)

        stats = body['stats']
        self.assertIn('total_matches_before_slotting', stats)
        self.assertIn('candidates_pool_size', stats)
        self.assertIn('filter_applied', stats)

        slots = body['slots']
        self.assertIsInstance(slots, list)
        self.assertEqual(len(slots), 3)

        # tracking_id must be a positive integer
        self.assertIsNotNone(body['tracking_id'])
        self.assertGreater(body['tracking_id'], 0)

    # ------------------------------------------------------------------
    # TC-2: Slot structure when slot 1 is populated
    # ------------------------------------------------------------------
    def test_slot1_has_correct_structure(self):
        resp, body = _post(self.client, _base_payload())
        self.assertEqual(resp.status_code, 200, body)

        slot1 = body['slots'][0]
        if slot1 is None:
            self.skipTest("No slot1 returned — check L031 candidates")

        for key in ('slot', 'label', 'unit_id', 'facility', 'unit_type',
                    'climate_type', 'size_range', 'price', 'plan_id',
                    'concession_id', 'pricing'):
            self.assertIn(key, slot1, f"Slot1 missing key: {key}")

        self.assertEqual(slot1['slot'], 1)
        self.assertEqual(slot1['label'], 'Best Match')
        self.assertIsInstance(slot1['price'], float)
        self.assertGreater(slot1['price'], 0)

        pricing = slot1['pricing']
        for pk in ('first_month_total', 'monthly_average', 'total_contract', 'breakdown'):
            self.assertIn(pk, pricing, f"pricing missing key: {pk}")

        breakdown = pricing['breakdown']
        self.assertEqual(len(breakdown), 6,
                         f"Expected 6 months, got {len(breakdown)}")

        for mb in breakdown:
            for mk in ('month_index', 'billing_date', 'rent', 'total'):
                self.assertIn(mk, mb)

        # sum of breakdown totals ≈ total_contract
        total_sum = sum(mb['total'] for mb in breakdown)
        self.assertAlmostEqual(total_sum, pricing['total_contract'], places=1)

    # ------------------------------------------------------------------
    # TC-3: Missing location → 400 with field name
    # ------------------------------------------------------------------
    def test_missing_location_returns_400(self):
        payload = _base_payload()
        payload['filters'] = {}
        resp, body = _post(self.client, payload)
        self.assertEqual(resp.status_code, 400, body)
        self.assertIn('error', body)
        # Should identify the missing field
        self.assertTrue(
            'location' in str(body.get('error', '')) or
            'location' in str(body.get('field', '')),
            body,
        )

    # ------------------------------------------------------------------
    # TC-4: mode='availability' → 501 with supported list
    # ------------------------------------------------------------------
    def test_unsupported_mode_returns_501(self):
        payload = _base_payload()
        payload['mode'] = 'availability'
        resp, body = _post(self.client, payload)
        self.assertEqual(resp.status_code, 501, body)
        self.assertIn('supported', body)
        self.assertIn('recommendation', body['supported'])
        self.assertEqual(body.get('mode'), 'availability')

    # ------------------------------------------------------------------
    # TC-5: level='top_n' → 501
    # ------------------------------------------------------------------
    def test_unsupported_level_returns_501(self):
        payload = _base_payload()
        payload['level'] = 'top_n'
        resp, body = _post(self.client, payload)
        self.assertEqual(resp.status_code, 501, body)
        self.assertIn('supported', body)
        self.assertIn('standard', body['supported'])
        self.assertEqual(body.get('level'), 'top_n')

    # ------------------------------------------------------------------
    # TC-6: Bad channel → 400
    # ------------------------------------------------------------------
    def test_bad_channel_returns_400(self):
        payload = _base_payload()
        payload['context']['channel'] = 'unknown'
        resp, body = _post(self.client, payload)
        self.assertEqual(resp.status_code, 400, body)
        self.assertIn('error', body)
        self.assertIn('allowed', body)

    # ------------------------------------------------------------------
    # TC-7: Duplicate request_id → 409
    # ------------------------------------------------------------------
    def test_duplicate_request_id_returns_409(self):
        payload = _base_payload()
        request_id = _uid()
        payload['context']['request_id'] = request_id

        resp1, _ = _post(self.client, payload)
        self.assertEqual(resp1.status_code, 200, "First call should succeed")

        # Second call with same request_id (different session_id to avoid
        # session dedup colliding) — duplicate detection is on request_id
        payload2 = dict(payload)
        payload2['context'] = dict(payload['context'])
        payload2['context']['session_id'] = _uid()
        resp2, body2 = _post(self.client, payload2)
        self.assertEqual(resp2.status_code, 409, body2)
        self.assertIn('request_id', body2)

    # ------------------------------------------------------------------
    # TC-8: Two-turn flow — second turn excludes prior unit_ids
    # ------------------------------------------------------------------
    def test_two_turn_flow_excludes_prior_units(self):
        session_id = _uid()

        # Turn 1
        payload1 = _base_payload()
        payload1['context']['session_id'] = session_id
        rid1 = _uid()
        payload1['context']['request_id'] = rid1
        resp1, body1 = _post(self.client, payload1)
        self.assertEqual(resp1.status_code, 200, body1)

        # Collect slot unit_ids from turn 1
        turn1_unit_ids = set()
        for s in body1['slots']:
            if s and s.get('unit_id'):
                turn1_unit_ids.add(s['unit_id'])

        if not turn1_unit_ids:
            self.skipTest("Turn 1 returned no slots — cannot verify exclusion")

        # Turn 2 — more_like_this referencing turn 1
        payload2 = _base_payload()
        payload2['context']['session_id'] = session_id
        payload2['context']['request_id'] = _uid()
        payload2['context']['previous_request_id'] = rid1
        payload2['context']['picked_slot'] = 1
        payload2['context']['action'] = 'more_like_this'

        resp2, body2 = _post(self.client, payload2)
        self.assertEqual(resp2.status_code, 200, body2)

        turn2_unit_ids = set()
        for s in body2['slots']:
            if s and s.get('unit_id'):
                turn2_unit_ids.add(s['unit_id'])

        # None of turn 1's units should appear in turn 2
        overlap = turn1_unit_ids & turn2_unit_ids
        self.assertEqual(
            overlap, set(),
            f"Turn 2 returned unit_ids also in turn 1: {overlap}",
        )

    # ------------------------------------------------------------------
    # TC-9: Booking outcome column is null before hook runs
    # ------------------------------------------------------------------
    def test_booked_unit_id_is_null_before_outcome_hook(self):
        """
        After a recommendation is served, booked_unit_id should be NULL
        (the outcome write-back hook is W4, not this task).
        """
        payload = _base_payload()
        resp, body = _post(self.client, payload)
        self.assertEqual(resp.status_code, 200, body)

        tracking_id = body.get('tracking_id')
        self.assertIsNotNone(tracking_id)

        slot1 = body['slots'][0]
        if slot1 is None:
            self.skipTest("No slot1; skipping outcome null check")

        # Query the DB directly to confirm booked_unit_id is null
        from common.config_loader import get_database_url
        from sqlalchemy import create_engine, text as sa_text
        engine = create_engine(get_database_url('middleware'))
        with engine.connect() as conn:
            row = conn.execute(sa_text(
                "SELECT booked_unit_id FROM mw_recommendations_served WHERE id = :id"
            ), {'id': tracking_id}).first()
        self.assertIsNotNone(row, "Row not found in mw_recommendations_served")
        self.assertIsNone(row[0], "booked_unit_id should be NULL before outcome hook")

    # ------------------------------------------------------------------
    # TC-10: Missing body → 400
    # ------------------------------------------------------------------
    def test_empty_body_returns_400(self):
        resp = self.client.post(
            '/api/recommendations',
            data='',
            content_type='application/json',
        )
        self.assertEqual(resp.status_code, 400)

    # ------------------------------------------------------------------
    # TC-11: duration_months missing → 400
    # ------------------------------------------------------------------
    def test_missing_duration_months_returns_400(self):
        payload = _base_payload()
        del payload['duration_months']
        resp, body = _post(self.client, payload)
        self.assertEqual(resp.status_code, 400, body)
        self.assertIn('duration_months', body.get('error', ''))

    # ------------------------------------------------------------------
    # TC-12: Slot unit_ids are distinct when populated
    # ------------------------------------------------------------------
    def test_slot_unit_ids_are_distinct(self):
        resp, body = _post(self.client, _base_payload())
        self.assertEqual(resp.status_code, 200, body)

        unit_ids = [s['unit_id'] for s in body['slots'] if s is not None]
        self.assertEqual(len(unit_ids), len(set(unit_ids)),
                         f"Duplicate unit_ids across slots: {unit_ids}")


if __name__ == '__main__':
    unittest.main()
