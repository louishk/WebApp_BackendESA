"""
COM01 decoder tests — exercises common.stype_name_parser against every SOP
COM01 code permutation and edge case.

These tests are the acceptance gate for the pricing-tool Phase 5.5 requirement.
The decoder lives in stype_name_parser (no separate com01_decoder module was
needed — the parser already implements the full SOP).

Run:
    cd backend/python
    PYTHONPATH=. pytest tests/test_com01_decoder.py -v
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from common.stype_name_parser import (
    parse_stype_name,
    is_wine_type,
    all_size_ranges,
    all_climate_codes,
    all_type_codes,
    StypeNameParts,
    WINE_TYPES,
    SIZE_RANGE_LIST,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ok(s: str, **expected):
    """Assert parse_ok=True and every expected field matches."""
    p = parse_stype_name(s)
    assert p.parse_ok, f"Expected parse_ok=True for {s!r}, invalid_tokens={p.invalid_tokens}"
    for attr, val in expected.items():
        got = getattr(p, attr)
        assert got == val, (
            f"Field '{attr}': expected {val!r}, got {got!r}  (input={s!r})"
        )
    return p


def _bad(s):
    """Assert parse_ok=False."""
    p = parse_stype_name(s)
    assert not p.parse_ok, f"Expected parse_ok=False for {s!r}"
    return p


# ---------------------------------------------------------------------------
# 1. Full-form parsing — size / range / type / climate / shape / pillar
# ---------------------------------------------------------------------------

class TestFullForm:

    def test_standard_walk_in_nc_ss_np(self):
        _ok('M/30-35/W/NC/SS/NP',
            size_category='M', size_range='30-35', unit_type='W',
            climate_type='NC', unit_shape='SS', pillar='NP')

    def test_executive_walk_in_aircon(self):
        _ok('L/60-70/E/A/WR/P',
            size_category='L', unit_type='E', climate_type='A', pillar='P')

    def test_smart_walk_in_dehumidifier(self):
        _ok('S/14-16/S/D/NR/NP', unit_type='S', climate_type='D', unit_shape='NR')

    def test_xl_drive_up_ad(self):
        _ok('XL/250+/DV/AD/SS/NP',
            size_category='XL', size_range='250+', unit_type='DV', climate_type='AD')

    def test_locker_upper_rf(self):
        _ok('S/6-8/U/RF/SS/NP', unit_type='U', climate_type='RF')

    def test_locker_middle(self):
        _ok('S/8-10/M/NC/SS/NP', unit_type='M')

    def test_locker_lower(self):
        _ok('S/10-12/L/NC/SS/NP', unit_type='L')

    def test_smart_locker_upper(self):
        _ok('S/12-14/SU/A/SS/NP', unit_type='SU')

    def test_smart_locker_middle(self):
        _ok('S/14-16/SM/A/SS/NP', unit_type='SM')

    def test_smart_locker_lower(self):
        _ok('S/16-18/SL/A/SS/NP', unit_type='SL')

    def test_executive_locker_upper(self):
        _ok('M/30-35/EU/A/SS/NP', unit_type='EU')

    def test_executive_locker_middle(self):
        _ok('M/30-35/EM/A/SS/NP', unit_type='EM')

    def test_executive_locker_lower(self):
        _ok('M/30-35/EL/A/SS/NP', unit_type='EL')

    def test_wardrobe_rb(self):
        _ok('M/35-40/RB/NC/WR/NP', unit_type='RB')

    def test_pillar_p(self):
        _ok('M/30-35/W/A/SS/P', pillar='P')

    def test_odd_shape(self):
        _ok('L/70-80/W/NC/OS/NP', unit_shape='OS')

    def test_narrow_rect(self):
        _ok('S/0-6/U/NC/NR/NP', unit_shape='NR')

    def test_wide_rect(self):
        _ok('M/30-35/W/NC/WR/NP', unit_shape='WR')

    def test_all_climate_codes_round_trip(self):
        for code in ['NC', 'A', 'D', 'AD', 'RF']:
            _ok(f'M/30-35/W/{code}/SS/NP', climate_type=code)

    def test_all_size_cats(self):
        for cat, rng in [('S', '0-6'), ('M', '30-35'), ('L', '60-70'), ('XL', '90-110')]:
            _ok(f'{cat}/{rng}/W/NC/SS/NP', size_category=cat)

    def test_every_canonical_size_range(self):
        """All 28 SOP ranges parse without error."""
        for rng in SIZE_RANGE_LIST:
            p = parse_stype_name(f'M/{rng}/W/NC/SS/NP')
            assert p.parse_ok and p.size_range == rng, f"Range {rng!r} failed"

    # Size-category / type-code name collisions
    def test_size_m_type_m_collision(self):
        p = _ok('M/30-35/M/NC/SS/NP')
        assert p.size_category == 'M'
        assert p.unit_type == 'M'

    def test_size_s_type_s_collision(self):
        p = _ok('S/14-16/S/NC/SS/NP')
        assert p.size_category == 'S'
        assert p.unit_type == 'S'

    def test_size_l_type_l_collision(self):
        p = _ok('L/60-70/L/NC/SS/NP')
        assert p.size_category == 'L'
        assert p.unit_type == 'L'


# ---------------------------------------------------------------------------
# 2. Wine types — 7-segment with case count
# ---------------------------------------------------------------------------

class TestWineTypes:

    def test_wine_walk_in(self):
        p = _ok('S/12-14/WN/RF/SS/NP/18', unit_type='WN', case_count=18)
        assert is_wine_type(p) is True

    def test_wine_locker_upper(self):
        _ok('S/6-8/WNU/RF/SS/NP/6', unit_type='WNU', case_count=6)

    def test_wine_locker_middle(self):
        _ok('S/8-10/WNM/RF/SS/NP/12', unit_type='WNM', case_count=12)

    def test_wine_locker_lower(self):
        _ok('S/10-12/WNL/RF/SS/NP/24', unit_type='WNL', case_count=24)

    def test_smart_wine_walk_in(self):
        p = _ok('M/30-35/SWN/RF/SS/NP/36', unit_type='SWN', case_count=36)
        assert is_wine_type(p) is True

    def test_smart_wine_locker_upper(self):
        _ok('S/12-14/SWNU/RF/SS/NP/8', unit_type='SWNU', case_count=8)

    def test_smart_wine_locker_middle(self):
        _ok('S/14-16/SWNM/RF/SS/NP/10', unit_type='SWNM', case_count=10)

    def test_smart_wine_locker_lower(self):
        _ok('S/16-18/SWNL/RF/SS/NP/16', unit_type='SWNL', case_count=16)

    def test_wine_missing_case_count_is_invalid(self):
        p = _bad('S/12-14/WN/RF/SS/NP')
        assert p.unit_type == 'WN'
        assert p.climate_type == 'RF'
        assert 'case_count=missing' in p.invalid_tokens

    def test_wine_non_integer_case_count_is_invalid(self):
        p = _bad('S/12-14/WN/RF/SS/NP/abc')
        assert p.unit_type == 'WN'
        assert any("case_count='abc'" in t for t in p.invalid_tokens)

    def test_non_wine_with_7th_token_is_invalid(self):
        p = _bad('M/30-35/W/NC/SS/NP/12')
        assert any(t.startswith('unexpected_7th=') for t in p.invalid_tokens)

    def test_is_wine_type_false_for_walk_in(self):
        p = parse_stype_name('M/30-35/W/NC/SS/NP')
        assert is_wine_type(p) is False

    def test_is_wine_type_false_for_null_result(self):
        p = parse_stype_name('garbage')
        assert is_wine_type(p) is False


# ---------------------------------------------------------------------------
# 3. Short-code-only forms (MB / BZ / SC / SB / PR)
# ---------------------------------------------------------------------------

class TestShortCodeOnly:

    @pytest.mark.parametrize("code", ['MB', 'BZ', 'SC', 'SB', 'PR'])
    def test_all_short_codes(self, code):
        p = parse_stype_name(code)
        assert p.parse_ok is True
        assert p.unit_type == code
        assert p.size_category is None
        assert p.climate_type is None

    def test_mailbox_raw_preserved(self):
        p = parse_stype_name('MB')
        assert p.raw == 'MB'


# ---------------------------------------------------------------------------
# 4. BW deprecated — parse_ok=False, recognised as legacy
# ---------------------------------------------------------------------------

class TestBWDeprecated:

    def test_bw_is_invalid(self):
        """BW is deprecated/merged; stype_name_parser correctly rejects it."""
        p = _bad('BW')
        assert p.raw == 'BW'

    def test_bw_type_not_set(self):
        p = parse_stype_name('BW')
        assert p.unit_type is None  # Not in UNIT_TYPE_CODES


# ---------------------------------------------------------------------------
# 5. Whitespace tolerance
# ---------------------------------------------------------------------------

class TestWhitespace:

    def test_leading_trailing(self):
        p = _ok('  M/30-35/W/NC/SS/NP  ', unit_type='W')
        assert p.size_category == 'M'

    def test_spaces_around_separators(self):
        _ok('M / 30-35 / W / NC / SS / NP', unit_type='W', climate_type='NC')

    def test_tab_whitespace(self):
        _ok('\tM/30-35/W/NC/SS/NP\t', unit_type='W')

    def test_whitespace_only_is_bad(self):
        _bad('   ')

    def test_short_code_with_spaces(self):
        p = parse_stype_name('  MB  ')
        assert p.parse_ok is True
        assert p.unit_type == 'MB'


# ---------------------------------------------------------------------------
# 6. Malformed / garbage inputs — all-None, no raise
# ---------------------------------------------------------------------------

class TestMalformed:

    def test_empty_string(self):
        p = _bad('')
        assert p.raw == ''

    def test_none_input(self):
        p = parse_stype_name(None)
        assert p.raw == ''
        assert p.unit_type is None
        assert not p.parse_ok

    def test_legacy_walk_in(self):
        _bad('Walk-In')

    def test_legacy_ac_walk_in(self):
        _bad('AC Walk-In')

    def test_five_tokens(self):
        _bad('M/30-35/W/NC/SS')

    def test_eight_tokens(self):
        _bad('M/30-35/W/NC/SS/NP/6/EXTRA')

    def test_unknown_climate_code(self):
        p = _bad('M/30-35/W/ZZ/SS/NP')
        assert p.size_category == 'M'
        assert p.unit_type == 'W'
        assert p.climate_type is None
        assert any("climate_type='ZZ'" in t for t in p.invalid_tokens)

    def test_unknown_size_cat(self):
        p = _bad('XX/30-35/W/NC/SS/NP')
        assert p.size_category is None

    def test_unknown_type_code(self):
        p = _bad('M/30-35/ZZZ/NC/SS/NP')
        assert p.unit_type is None
        assert p.size_category == 'M'

    def test_empty_segment_yields_invalid(self):
        p = _bad('M//W/NC/SS/NP')
        assert 'size_range' in p.invalid_tokens

    def test_single_slash(self):
        _bad('/')

    def test_numeric_only(self):
        _bad('123456')

    def test_raw_always_preserved(self):
        raw = 'some garbage string'
        p = parse_stype_name(raw)
        assert p.raw == raw


# ---------------------------------------------------------------------------
# 7. Utility-function contracts
# ---------------------------------------------------------------------------

class TestUtilityFunctions:

    def test_all_size_ranges_count(self):
        # The SOP COM01 Jan 2026 defines 29 canonical size ranges (plan doc says 28 — docx is authoritative).
        assert len(all_size_ranges()) == 29

    def test_all_size_ranges_includes_open_ended(self):
        assert '250+' in all_size_ranges()

    def test_all_size_ranges_includes_smallest(self):
        assert '0-6' in all_size_ranges()

    def test_all_size_ranges_order_matches_sop(self):
        ranges = all_size_ranges()
        assert ranges[0] == '0-6'
        assert ranges[-1] == '250+'

    def test_all_climate_codes_count(self):
        codes = all_climate_codes()
        assert set(codes) == {'NC', 'A', 'D', 'AD', 'RF'}

    def test_all_type_codes_returns_dict(self):
        codes = all_type_codes()
        assert isinstance(codes, dict)
        assert codes['W'] == 'Walk-In'

    def test_all_type_codes_includes_wine(self):
        codes = all_type_codes()
        assert 'WN' in codes
        assert 'SWNU' in codes

    def test_all_type_codes_includes_short_codes(self):
        codes = all_type_codes()
        for c in ['MB', 'BZ', 'SC', 'SB', 'PR']:
            assert c in codes, f"Short code {c!r} missing"

    def test_stypeparts_is_frozen(self):
        p = parse_stype_name('M/30-35/W/NC/SS/NP')
        with pytest.raises((AttributeError, TypeError)):
            p.unit_type = 'E'  # type: ignore[misc]
