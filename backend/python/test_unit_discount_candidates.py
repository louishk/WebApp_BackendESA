"""Unit tests for the pure composition helpers in
sync_service.pipelines.mw_unit_discount_candidates — no DB required.

Run:
    cd backend/python && python test_unit_discount_candidates.py
"""
from datetime import datetime, timedelta
from decimal import Decimal
import unittest

from sync_service.pipelines.mw_unit_discount_candidates import (
    _compose_candidates, _effective_rate, _site_applies,
)


PLAN = {
    'id': 42,
    'plan_name': 'Test Plan',
    'plan_type': 'Evergreen',
    'applicable_sites': {'L017': True, 'L018': False},
    'linked_concessions': [
        {'site_id': 100, 'concession_id': 900},
    ],
    'promo_period_start': None,
    'promo_period_end': None,
    'booking_period_start': None,
    'booking_period_end': None,
    'period_start': None,
    'period_end': None,
    'move_in_range': 'Flexible',
    'lock_in_period': '6 months',
    'payment_terms': 'Monthly',
    'discount_type': 'percentage',
    'discount_numeric': Decimal('10'),
    'discount_segmentation': '>=10% < 20%',
    'is_active': True,
}

CONC_ACTIVE = {
    'SiteID': 100,
    'ConcessionID': 900,
    'sPlanName': '10%',
    'iAmtType': 2,
    'dcFixedDiscount': None,
    'dcPCDiscount': Decimal('10'),
    'dcMaxAmountOff': None,
    'dPlanStrt': None,
    'dPlanEnd': None,
    'bNeverExpires': True,
    'iInMonth': 1,
    'bPrepay': False,
    'iPrePaidMonths': None,
    'bForAllUnits': True,
    'bForCorp': None,
    'iRestrictionFlags': 0,
    'iExcludeIfLessThanUnitsTotal': None,
    'iExcludeIfMoreThanUnitsTotal': None,
    'dcMaxOccPct': None,
}

UNIT_A = {
    'SiteID': 100,
    'UnitID': 1,
    'UnitTypeID': 5,
    'sLocationCode': 'L017',
    'sTypeName': 'M/30-35/W/A/SS/NP',
    'bCorporate': False,
    'dcStdRate': Decimal('100'),
    'dcWebRate': Decimal('95'),
    'dcPushRate': Decimal('90'),
    'dcBoardRate': Decimal('100'),
    'dcPreferredRate': Decimal('90'),
}

UNIT_B_LEGACY = dict(UNIT_A, UnitID=2, sTypeName='AC Walk-In')
UNIT_C_CORP = dict(UNIT_A, UnitID=3, bCorporate=True)


class ComposeTests(unittest.TestCase):
    def test_happy_path(self):
        rows, parse_fail, _excluded, _restr, _legacy = _compose_candidates(
            plan_rows=[PLAN],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_A],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
        )
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r['site_id'], 100)
        self.assertEqual(r['unit_id'], 1)
        self.assertEqual(r['plan_id'], 42)
        self.assertEqual(r['concession_id'], 900)
        self.assertEqual(r['climate_type'], 'A')
        self.assertEqual(r['size_category'], 'M')
        self.assertTrue(r['parse_ok'])
        self.assertEqual(r['effective_rate'], Decimal('90.0'))
        self.assertEqual(parse_fail, 0)

    def test_legacy_stype_flags_parse_fail_but_still_emits(self):
        rows, parse_fail, _excluded, _restr, _legacy = _compose_candidates(
            plan_rows=[PLAN],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_B_LEGACY],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
        )
        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]['parse_ok'])
        self.assertIsNone(rows[0]['climate_type'])
        self.assertEqual(parse_fail, 1)

    def test_legacy_type_map_fills_unit_and_climate_type(self):
        # parse_ok stays False but unit_type/climate_type get populated from
        # the legacy lookup so per-plan dim restrictions can still match.
        rows, parse_fail, _excluded, _restr, legacy = _compose_candidates(
            plan_rows=[PLAN],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_B_LEGACY],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
            legacy_type_map={'AC Walk-In': ('W', 'A')},
        )
        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]['parse_ok'])
        self.assertEqual(rows[0]['unit_type'], 'W')
        self.assertEqual(rows[0]['climate_type'], 'A')
        self.assertEqual(parse_fail, 1)
        self.assertEqual(legacy, 1)

    def test_site_not_in_applicable_sites_skipped(self):
        rows, _, _excluded, _restr, _legacy = _compose_candidates(
            plan_rows=[PLAN],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_A],
            code_to_site_id={'L018': 100},  # wrong code mapping
            computed_at=datetime(2026, 4, 22),
        )
        self.assertEqual(rows, [])

    def test_applicable_site_false_skipped(self):
        plan = dict(PLAN, applicable_sites={'L017': False})
        rows, _, _excluded, _restr, _legacy = _compose_candidates(
            plan_rows=[plan],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_A],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
        )
        self.assertEqual(rows, [])

    def test_concession_missing_skipped(self):
        rows, _, _excluded, _restr, _legacy = _compose_candidates(
            plan_rows=[PLAN],
            conc_rows=[],
            unit_rows=[UNIT_A],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
        )
        self.assertEqual(rows, [])

    def test_smart_lock_attached_when_assignment_present(self):
        rows, _, _excluded, _restr, _legacy = _compose_candidates(
            plan_rows=[PLAN],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_A],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
            smart_lock_map={(100, 1): {'keypad_ids': [12345], 'padlock_id': 999}},
        )
        self.assertEqual(len(rows), 1)
        # Pre-serialised JSON string (cast to ::jsonb at INSERT time).
        self.assertIn('"keypad_ids"', rows[0]['smart_lock'])
        self.assertIn('"padlock_id"', rows[0]['smart_lock'])

    def test_smart_lock_null_when_no_assignment(self):
        rows, _, _excluded, _restr, _legacy = _compose_candidates(
            plan_rows=[PLAN],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_A],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
        )
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]['smart_lock'])

    def test_corporate_only_concession_skips_noncorp_unit(self):
        conc = dict(CONC_ACTIVE, bForCorp=True)
        rows, _, _excluded, _restr, _legacy = _compose_candidates(
            plan_rows=[PLAN],
            conc_rows=[conc],
            unit_rows=[UNIT_A],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
        )
        self.assertEqual(rows, [])

    def test_non_corp_concession_emits_for_corp_unit_when_flag_null(self):
        # bForCorp=None → no explicit mismatch, emit.
        rows, _, _excluded, _restr, _legacy = _compose_candidates(
            plan_rows=[PLAN],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_C_CORP],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
        )
        self.assertEqual(len(rows), 1)

    def test_dedup_linked_concession_duplicates(self):
        plan = dict(PLAN, linked_concessions=[
            {'site_id': 100, 'concession_id': 900},
            {'site_id': 100, 'concession_id': 900},  # duplicate
        ])
        rows, _, _excluded, _restr, _legacy = _compose_candidates(
            plan_rows=[plan],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_A],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
        )
        self.assertEqual(len(rows), 1)

    def test_excluded_unit_type_dropped(self):
        # Unit type 'W' is in the parsed output; exclude it globally.
        rows, _, excluded, _restr, _legacy = _compose_candidates(
            plan_rows=[PLAN],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_A],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
            excluded_unit_types={'W'},
        )
        self.assertEqual(rows, [])
        self.assertEqual(excluded, 1)

    def test_exclusion_does_not_affect_other_types(self):
        unit_u = dict(UNIT_A, UnitID=99, sTypeName='S/10-12/U/NC/SS/NP')
        rows, _, excluded, _restr, _legacy = _compose_candidates(
            plan_rows=[PLAN],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_A, unit_u],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
            excluded_unit_types={'U'},  # exclude locker upper only
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['unit_type'], 'W')
        self.assertEqual(excluded, 1)

    def test_plan_restriction_drops_unit_when_dim_mismatch(self):
        # Unit A parses as climate_type='A'; restrict to climate 'RF' only.
        plan = dict(PLAN, restrictions={'climate_type': ['RF']})
        rows, _pf, _exc, restr, _legacy = _compose_candidates(
            plan_rows=[plan],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_A],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
        )
        self.assertEqual(rows, [])
        self.assertEqual(restr, 1)

    def test_plan_restriction_allows_unit_when_dim_matches(self):
        # climate_type=A → restrict to ['A', 'AD'] → unit passes.
        plan = dict(PLAN, restrictions={'climate_type': ['A', 'AD']})
        rows, _pf, _exc, restr, _legacy = _compose_candidates(
            plan_rows=[plan],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_A],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(restr, 0)

    def test_plan_restriction_multi_dim_all_must_match(self):
        # Unit A: size_category=M, climate=A. Restrict size to ['L','XL'] → fail.
        plan = dict(PLAN, restrictions={
            'size_category': ['L', 'XL'],
            'climate_type': ['A'],
        })
        rows, _pf, _exc, restr, _legacy = _compose_candidates(
            plan_rows=[plan],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_A],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
        )
        self.assertEqual(rows, [])
        self.assertEqual(restr, 1)

    def test_plan_restriction_empty_dim_is_ignored(self):
        # Empty dim list = no restriction on that dim; climate is untouched.
        plan = dict(PLAN, restrictions={'size_category': ['M'], 'climate_type': []})
        rows, _pf, _exc, restr, _legacy = _compose_candidates(
            plan_rows=[plan],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_A],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(restr, 0)

    def test_plan_restriction_blocks_legacy_unparsed_unit(self):
        # parse_ok=False → every parsed field is None → any restriction excludes it.
        plan = dict(PLAN, restrictions={'climate_type': ['A']})
        rows, pf, _exc, restr, _legacy = _compose_candidates(
            plan_rows=[plan],
            conc_rows=[CONC_ACTIVE],
            unit_rows=[UNIT_B_LEGACY],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
        )
        self.assertEqual(rows, [])
        self.assertEqual(pf, 1)  # legacy still counted as parse fail
        self.assertEqual(restr, 1)

    def test_effective_rate_fixed_discount(self):
        conc = dict(CONC_ACTIVE, dcPCDiscount=None, dcFixedDiscount=Decimal('20'))
        rows, _, _excluded, _restr, _legacy = _compose_candidates(
            plan_rows=[PLAN],
            conc_rows=[conc],
            unit_rows=[UNIT_A],
            code_to_site_id={'L017': 100},
            computed_at=datetime(2026, 4, 22),
        )
        self.assertEqual(rows[0]['effective_rate'], Decimal('80'))


class SiteAppliesTests(unittest.TestCase):
    def test_true_value(self):
        self.assertTrue(_site_applies({'L017': True}, 'L017'))

    def test_false_value(self):
        self.assertFalse(_site_applies({'L017': False}, 'L017'))

    def test_missing_key(self):
        self.assertFalse(_site_applies({'L017': True}, 'L018'))

    def test_none_applicable(self):
        self.assertFalse(_site_applies(None, 'L017'))

    def test_empty_dict(self):
        self.assertFalse(_site_applies({}, 'L017'))

    def test_empty_code(self):
        self.assertFalse(_site_applies({'L017': True}, None))


if __name__ == '__main__':
    unittest.main()
