"""
Tests for common/size_range_window.py

DB is never hit — _DIM_CACHE is populated directly in setUp().

Real SOP buckets from mw_dim_size_range (queried 2026-04-27):
  sort_order | range_code
  -----------+-----------
   1         | 0-6
   2         | 6-8
   3         | 8-10
   4         | 10-12
   5         | 12-14
   6         | 14-16
   7         | 16-18
   8         | 18-20
   9         | 20-22
  10         | 22-24
  11         | 24-26
  12         | 26-28
  13         | 28-30
  14         | 30-35
  15         | 35-40
  16         | 40-45
  17         | 45-50
  18         | 50-60
  19         | 60-70
  20         | 70-80
  21         | 80-90
  22         | 90-110
  23         | 110-130
  24         | 130-150
  25         | 150-175
  26         | 175-200
  27         | 200-225
  28         | 225-250
  29         | 250+
"""

import unittest

import common.size_range_window as srw


# Canonical SOP buckets — (sort_order, range_code)
_SOP_BUCKETS = [
    (1,  '0-6'),
    (2,  '6-8'),
    (3,  '8-10'),
    (4,  '10-12'),
    (5,  '12-14'),
    (6,  '14-16'),
    (7,  '16-18'),
    (8,  '18-20'),
    (9,  '20-22'),
    (10, '22-24'),
    (11, '24-26'),
    (12, '26-28'),
    (13, '28-30'),
    (14, '30-35'),
    (15, '35-40'),
    (16, '40-45'),
    (17, '45-50'),
    (18, '50-60'),
    (19, '60-70'),
    (20, '70-80'),
    (21, '80-90'),
    (22, '90-110'),
    (23, '110-130'),
    (24, '130-150'),
    (25, '150-175'),
    (26, '175-200'),
    (27, '200-225'),
    (28, '225-250'),
    (29, '250+'),
]


def _seed_cache():
    """Populate _DIM_CACHE directly, bypassing DB load."""
    srw.clear_cache()
    for sort_order, range_code in _SOP_BUCKETS:
        srw._DIM_CACHE[range_code] = (sort_order, srw._parse_midpoint(range_code))
    srw._CACHE_LOADED = True


class TestParseMidpoint(unittest.TestCase):
    def test_normal_range(self):
        self.assertEqual(srw._parse_midpoint('30-35'), 32.5)

    def test_open_ended_returns_none(self):
        self.assertIsNone(srw._parse_midpoint('250+'))

    def test_malformed_returns_none(self):
        self.assertIsNone(srw._parse_midpoint('UNKNOWN'))
        self.assertIsNone(srw._parse_midpoint(''))

    def test_small_range(self):
        self.assertEqual(srw._parse_midpoint('6-8'), 7.0)

    def test_wide_range(self):
        self.assertEqual(srw._parse_midpoint('90-110'), 100.0)


class TestSizeRangeNeighbours(unittest.TestCase):
    def setUp(self):
        _seed_cache()

    # ------------------------------------------------------------------ #
    # Core percentage cases                                                #
    # ------------------------------------------------------------------ #

    def test_30_35_at_20pct(self):
        """
        center='30-35', midpoint=32.5, ±20% → [26.0, 39.0].
        Buckets whose midpoints fall in [26.0, 39.0]:
          26-28 → 27.0  ✓
          28-30 → 29.0  ✓
          30-35 → 32.5  ✓
          35-40 → 37.5  ✓
        """
        result = srw.size_range_neighbours('30-35', 20)
        self.assertEqual(result, ['26-28', '28-30', '30-35', '35-40'])

    def test_30_35_at_50pct(self):
        """
        center='30-35', midpoint=32.5, ±50% → [16.25, 48.75].
        Buckets:
          16-18 → 17.0  ✓
          18-20 → 19.0  ✓
          20-22 → 21.0  ✓
          22-24 → 23.0  ✓
          24-26 → 25.0  ✓
          26-28 → 27.0  ✓
          28-30 → 29.0  ✓
          30-35 → 32.5  ✓
          35-40 → 37.5  ✓
          40-45 → 42.5  ✓
          45-50 → 47.5  ✓
          (50-60 → 55.0 > 48.75  ✗)
          (14-16 → 15.0 < 16.25  ✗)
        """
        result = srw.size_range_neighbours('30-35', 50)
        expected = [
            '16-18', '18-20', '20-22', '22-24', '24-26',
            '26-28', '28-30', '30-35', '35-40', '40-45', '45-50',
        ]
        self.assertEqual(result, expected)

    def test_unknown_center_returns_fallback(self):
        result = srw.size_range_neighbours('UNKNOWN', 20)
        self.assertEqual(result, ['UNKNOWN'])

    def test_open_ended_bucket_returns_self_only(self):
        """'250+' has no midpoint — percentage math not applicable."""
        result = srw.size_range_neighbours('250+', 20)
        self.assertEqual(result, ['250+'])

    def test_includes_center_bucket(self):
        result = srw.size_range_neighbours('50-60', 20)
        self.assertIn('50-60', result)

    def test_sorted_by_sort_order(self):
        result = srw.size_range_neighbours('50-60', 30)
        self.assertEqual(result, sorted(result, key=lambda c: srw._DIM_CACHE[c][0]))

    def test_zero_pct_returns_only_center(self):
        result = srw.size_range_neighbours('30-35', 0)
        self.assertEqual(result, ['30-35'])

    def test_edge_bottom_small_window(self):
        """'0-6' midpoint=3.0, ±20% → [2.4, 3.6]. Only '0-6' (3.0) itself qualifies."""
        result = srw.size_range_neighbours('0-6', 20)
        self.assertEqual(result, ['0-6'])


class TestSizeRangeNeighboursStep(unittest.TestCase):
    def setUp(self):
        _seed_cache()

    def test_30_35_step_1(self):
        """sort_order=14, ±1 → orders 13,14,15 → ['28-30','30-35','35-40']."""
        result = srw.size_range_neighbours_step('30-35', 1)
        self.assertEqual(result, ['28-30', '30-35', '35-40'])

    def test_30_35_step_2(self):
        """sort_order=14, ±2 → orders 12..16 → 5 buckets."""
        result = srw.size_range_neighbours_step('30-35', 2)
        self.assertEqual(result, ['26-28', '28-30', '30-35', '35-40', '40-45'])

    def test_lower_edge_step_1(self):
        """'0-6' is sort_order=1; ±1 → orders 0..2. Order 0 doesn't exist."""
        result = srw.size_range_neighbours_step('0-6', 1)
        self.assertEqual(result, ['0-6', '6-8'])

    def test_upper_edge_step_1(self):
        """'250+' is sort_order=29; ±1 → orders 28..30. Order 30 doesn't exist."""
        result = srw.size_range_neighbours_step('250+', 1)
        self.assertEqual(result, ['225-250', '250+'])

    def test_unknown_center_returns_fallback(self):
        result = srw.size_range_neighbours_step('UNKNOWN', 1)
        self.assertEqual(result, ['UNKNOWN'])

    def test_step_0_returns_only_center(self):
        result = srw.size_range_neighbours_step('30-35', 0)
        self.assertEqual(result, ['30-35'])

    def test_large_step_covers_whole_table(self):
        """n_steps=50 from middle covers the entire 29-bucket list."""
        result = srw.size_range_neighbours_step('30-35', 50)
        self.assertEqual(len(result), 29)

    def test_sorted_by_sort_order(self):
        result = srw.size_range_neighbours_step('50-60', 3)
        orders = [srw._DIM_CACHE[c][0] for c in result]
        self.assertEqual(orders, sorted(orders))


class TestClearCache(unittest.TestCase):
    def test_clear_resets_state(self):
        _seed_cache()
        self.assertGreater(len(srw._DIM_CACHE), 0)
        self.assertTrue(srw._CACHE_LOADED)
        srw.clear_cache()
        self.assertEqual(len(srw._DIM_CACHE), 0)
        self.assertFalse(srw._CACHE_LOADED)


if __name__ == '__main__':
    unittest.main()
