"""Tests for common.risk_lookup."""
import pytest
from common.risk_lookup import (
    bucket_band, compute_risk, RiskResult, FactorRow,
)

BANDS = [
    {"max_composite": 0.70, "label": "very_low",  "color": "#1b5e20"},
    {"max_composite": 0.90, "label": "low",       "color": "#2e7d32"},
    {"max_composite": 1.10, "label": "average",   "color": "#9e9e9e"},
    {"max_composite": 1.30, "label": "high",      "color": "#f57c00"},
    {"max_composite": None, "label": "very_high", "color": "#c62828"},
]


def test_bucket_band_low():
    assert bucket_band(0.80, BANDS)["label"] == "low"


def test_bucket_band_edge_inclusive_upper():
    # max_composite is inclusive upper bound: 0.90 → 'low', 0.9001 → 'average'
    assert bucket_band(0.90, BANDS)["label"] == "low"
    assert bucket_band(0.9001, BANDS)["label"] == "average"


def test_bucket_band_very_high():
    assert bucket_band(2.0, BANDS)["label"] == "very_high"


def test_compute_risk_missing_dim_treated_as_one():
    factors = {
        "size": FactorRow(value="S", effective=0.80, source="empirical",
                          is_thin=False, sample_size=100),
    }
    result = compute_risk(baseline_rate=0.025, factors=factors, bands=BANDS)
    assert result.composite_factor == pytest.approx(0.80)
    assert result.risk_pct == pytest.approx(0.025 * 0.80)
    assert result.band["label"] == "low"
    assert result.delta_vs_baseline_pct == pytest.approx(-20.0)


def test_compute_risk_multiple_dims():
    factors = {
        "size": FactorRow("S", 0.80, "empirical", False, 100),
        "type": FactorRow("W", 0.92, "empirical", False, 100),
        "climate": FactorRow("A", 1.05, "override", False, 50),
    }
    result = compute_risk(0.025, factors, BANDS)
    expected = 0.80 * 0.92 * 1.05
    assert result.composite_factor == pytest.approx(expected)


from common.risk_lookup import resolve_effective_factor


def test_resolve_override_wins():
    eff, source = resolve_effective_factor(
        empirical=1.20, override=0.95, is_thin=False)
    assert eff == 0.95 and source == "override"


def test_resolve_thin_data_pinned_to_one():
    eff, source = resolve_effective_factor(
        empirical=1.40, override=None, is_thin=True)
    assert eff == 1.0 and source == "thin_neutral"


def test_resolve_empirical_normal():
    eff, source = resolve_effective_factor(
        empirical=0.85, override=None, is_thin=False)
    assert eff == 0.85 and source == "empirical"


def test_resolve_no_data_neutral():
    eff, source = resolve_effective_factor(
        empirical=None, override=None, is_thin=True)
    assert eff == 1.0 and source == "thin_neutral"


def test_resolve_override_overrides_thin():
    eff, source = resolve_effective_factor(
        empirical=None, override=1.10, is_thin=True)
    assert eff == 1.10 and source == "override"
