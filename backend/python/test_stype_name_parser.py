"""Unit tests for common.stype_name_parser.

Run:
    cd backend/python && python -m pytest test_stype_name_parser.py -v
or directly:
    cd backend/python && python test_stype_name_parser.py
"""
import sys
import unittest

from common.stype_name_parser import parse_stype_name


class ParserTests(unittest.TestCase):
    def test_full_valid_six_token(self):
        r = parse_stype_name('M/30-35/W/A/SS/NP')
        self.assertTrue(r.parse_ok)
        self.assertEqual(r.size_category, 'M')
        self.assertEqual(r.size_range, '30-35')
        self.assertEqual(r.unit_type, 'W')
        self.assertEqual(r.climate_type, 'A')
        self.assertEqual(r.unit_shape, 'SS')
        self.assertEqual(r.pillar, 'NP')
        self.assertIsNone(r.case_count)

    def test_full_valid_xl_with_open_range(self):
        r = parse_stype_name('XL/250+/DV/NC/WR/P')
        self.assertTrue(r.parse_ok)
        self.assertEqual(r.size_category, 'XL')
        self.assertEqual(r.size_range, '250+')
        self.assertEqual(r.unit_type, 'DV')

    def test_wine_seven_token_with_case_count(self):
        r = parse_stype_name('S/12-14/WN/RF/SS/NP/18')
        self.assertTrue(r.parse_ok)
        self.assertEqual(r.unit_type, 'WN')
        self.assertEqual(r.case_count, 18)

    def test_wine_missing_case_count_is_invalid(self):
        r = parse_stype_name('S/12-14/WN/RF/SS/NP')
        self.assertFalse(r.parse_ok)
        self.assertIn('case_count=missing', r.invalid_tokens)
        # Everything else still parsed cleanly.
        self.assertEqual(r.unit_type, 'WN')
        self.assertEqual(r.climate_type, 'RF')

    def test_non_wine_with_unexpected_case_count_is_invalid(self):
        r = parse_stype_name('M/30-35/W/A/SS/NP/12')
        self.assertFalse(r.parse_ok)
        self.assertTrue(any(t.startswith('unexpected_7th=') for t in r.invalid_tokens))

    def test_short_code_only_mailbox(self):
        r = parse_stype_name('MB')
        self.assertTrue(r.parse_ok)
        self.assertEqual(r.unit_type, 'MB')
        self.assertIsNone(r.size_category)
        self.assertIsNone(r.climate_type)

    def test_short_code_only_all_variants(self):
        for code in ('MB', 'BZ', 'SC', 'SB', 'PR'):
            r = parse_stype_name(code)
            self.assertTrue(r.parse_ok, f'{code} should parse')
            self.assertEqual(r.unit_type, code)

    def test_legacy_free_form_walk_in(self):
        r = parse_stype_name('Walk-In')
        self.assertFalse(r.parse_ok)
        self.assertEqual(r.raw, 'Walk-In')

    def test_legacy_free_form_ac_walk_in(self):
        r = parse_stype_name('AC Walk-In')
        self.assertFalse(r.parse_ok)

    def test_unknown_token_flags_invalid(self):
        # ZZ is not a valid climate code.
        r = parse_stype_name('M/30-35/W/ZZ/SS/NP')
        self.assertFalse(r.parse_ok)
        self.assertTrue(any("climate_type='ZZ'" in t for t in r.invalid_tokens))
        # Valid tokens still populate.
        self.assertEqual(r.size_category, 'M')
        self.assertEqual(r.unit_type, 'W')

    def test_whitespace_trimmed(self):
        r = parse_stype_name('  M / 30-35 / W / A / SS / NP  ')
        self.assertTrue(r.parse_ok)
        self.assertEqual(r.unit_type, 'W')

    def test_empty_string(self):
        r = parse_stype_name('')
        self.assertFalse(r.parse_ok)
        self.assertEqual(r.raw, '')

    def test_none_input(self):
        r = parse_stype_name(None)
        self.assertFalse(r.parse_ok)

    def test_wrong_token_count(self):
        # 5 tokens — not valid under SOP.
        r = parse_stype_name('M/30-35/W/A/SS')
        self.assertFalse(r.parse_ok)

    def test_size_type_code_collision(self):
        # 'M' is both a size category and a locker-middle unit type.
        # Position-based decoding must resolve this without collision.
        r = parse_stype_name('M/30-35/M/A/SS/NP')
        self.assertTrue(r.parse_ok)
        self.assertEqual(r.size_category, 'M')
        self.assertEqual(r.unit_type, 'M')

    def test_smart_wine_seven_token(self):
        r = parse_stype_name('M/30-35/SWNU/AD/WR/P/24')
        self.assertTrue(r.parse_ok)
        self.assertEqual(r.unit_type, 'SWNU')
        self.assertEqual(r.case_count, 24)

    def test_empty_token_in_middle(self):
        r = parse_stype_name('M//W/A/SS/NP')
        self.assertFalse(r.parse_ok)
        self.assertIn('size_range', r.invalid_tokens)

    def test_non_integer_case_count(self):
        r = parse_stype_name('S/12-14/WN/RF/SS/NP/abc')
        self.assertFalse(r.parse_ok)
        self.assertTrue(any("case_count='abc'" in t for t in r.invalid_tokens))


if __name__ == '__main__':
    unittest.main()
