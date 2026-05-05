"""Pure functions for risk-factor composition and gradient bucketing."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class FactorRow:
    value: str
    effective: float
    source: str            # 'empirical' | 'override' | 'thin_neutral' | 'missing'
    is_thin: bool
    sample_size: int


@dataclass(frozen=True)
class RiskResult:
    baseline_rate: float
    composite_factor: float
    risk_pct: float
    delta_vs_baseline_pct: float
    band: dict
    factors: dict          # {dim: FactorRow}


def bucket_band(composite: float, bands: list[dict]) -> dict:
    """Pick the band whose max_composite >= composite. Last band (max=None) is +inf."""
    for b in bands:
        ceiling = b.get("max_composite")
        if ceiling is None or composite <= float(ceiling):
            return b
    return bands[-1]


def compute_risk(baseline_rate: float,
                 factors: dict[str, FactorRow],
                 bands: list[dict]) -> RiskResult:
    composite = 1.0
    for f in factors.values():
        composite *= float(f.effective)
    risk_pct = baseline_rate * composite
    delta = (composite - 1.0) * 100.0
    return RiskResult(
        baseline_rate=baseline_rate,
        composite_factor=composite,
        risk_pct=risk_pct,
        delta_vs_baseline_pct=delta,
        band=bucket_band(composite, bands),
        factors=factors,
    )


def resolve_effective_factor(empirical: Optional[float],
                             override: Optional[float],
                             is_thin: bool) -> tuple[float, str]:
    """Spec §5.4: override > thin-neutral > empirical."""
    if override is not None:
        return float(override), "override"
    if is_thin or empirical is None:
        return 1.0, "thin_neutral"
    return float(empirical), "empirical"
