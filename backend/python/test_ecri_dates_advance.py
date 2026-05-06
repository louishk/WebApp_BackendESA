"""Unit tests for common.ecri_dates.compute_advance_effective_date.

Run:
    cd backend/python && python -m pytest test_ecri_dates_advance.py -v
or directly:
    cd backend/python && python test_ecri_dates_advance.py
"""
import unittest
from datetime import date, timedelta

from common.ecri_dates import compute_advance_effective_date


class AdvanceEffectiveDateTests(unittest.TestCase):
    """Invariants for the Pre-Load Batch effective-date helper."""

    def setUp(self):
        self.today = date(2026, 5, 1)
        self.anniv = date(2025, 1, 15)  # only .day (15) is used
        self.buffer = 7

    def test_heavy_prepayer_60d_out(self):
        """paid_thru 60d out: effective must be strictly after paid_thru + 7."""
        pt = self.today + timedelta(days=60)
        eff, notice, bucket = compute_advance_effective_date(
            self.anniv, pt, self.today, prepay_buffer_days=self.buffer
        )
        self.assertGreater(eff, pt + timedelta(days=self.buffer))
        self.assertEqual(eff.day, 15)  # LAD constraint preserved
        self.assertEqual(bucket, 'red')
        self.assertLess(notice, eff)

    def test_heavy_prepayer_180d_out(self):
        pt = self.today + timedelta(days=180)
        eff, _, _ = compute_advance_effective_date(
            self.anniv, pt, self.today, prepay_buffer_days=self.buffer
        )
        self.assertGreater(eff, pt + timedelta(days=self.buffer))
        self.assertEqual(eff.day, 15)

    def test_heavy_prepayer_365d_out(self):
        pt = self.today + timedelta(days=365)
        eff, _, _ = compute_advance_effective_date(
            self.anniv, pt, self.today, prepay_buffer_days=self.buffer
        )
        self.assertGreater(eff, pt + timedelta(days=self.buffer))
        self.assertEqual(eff.day, 15)

    def test_heavy_prepayer_400d_out(self):
        pt = self.today + timedelta(days=400)
        eff, _, _ = compute_advance_effective_date(
            self.anniv, pt, self.today, prepay_buffer_days=self.buffer
        )
        self.assertGreater(eff, pt + timedelta(days=self.buffer))

    def test_no_projected_paid_thru_falls_back_to_today(self):
        """None projected_paid_thru behaves like a near-immediate change."""
        eff, _, bucket = compute_advance_effective_date(
            self.anniv, None, self.today, prepay_buffer_days=self.buffer
        )
        # With paid_thru=None, the helper treats it as today — so the first
        # eligible LAD should be picked (bucket likely green or amber, not red).
        self.assertEqual(eff.day, 15)
        self.assertGreaterEqual(eff, self.today)
        self.assertIn(bucket, ('green', 'amber', 'red'))

    def test_no_anniv_unknown_bucket(self):
        """anniv=None → 'unknown' bucket, date computed from today + buffer."""
        pt = self.today + timedelta(days=60)
        eff, _, bucket = compute_advance_effective_date(
            None, pt, self.today, prepay_buffer_days=self.buffer
        )
        self.assertEqual(bucket, 'unknown')
        # unknown path uses max(today + notice_days, pt_inflated + 1)
        self.assertGreater(eff, pt)

    def test_custom_buffer_increases_effective(self):
        """Larger buffer pushes the effective date at least as far out."""
        pt = self.today + timedelta(days=60)
        eff_small, _, _ = compute_advance_effective_date(
            self.anniv, pt, self.today, prepay_buffer_days=0
        )
        eff_large, _, _ = compute_advance_effective_date(
            self.anniv, pt, self.today, prepay_buffer_days=30
        )
        self.assertGreaterEqual(eff_large, eff_small)
        self.assertGreater(eff_large, pt + timedelta(days=30))


if __name__ == '__main__':
    unittest.main()
