"""Unit-category risk pipeline — computes country baseline + per-dimension factors.

Reads from esa_pbi: rentroll (occupancy), mimo (move-outs), siteinfo (country).
Writes to esa_pbi: unit_category_risk_baseline, unit_category_risk_factor,
unit_category_risk_history.

Spec: docs/superpowers/specs/2026-05-04-unit-category-risk-design.md
"""
from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

DAYS_PER_MONTH = Decimal("30.4375")


@dataclass
class BaselineResult:
    country_name: str
    window_start: dt.date
    window_end: dt.date
    moveout_count: int
    unit_months_occupied: Decimal
    baseline_rate: Decimal


def compute_country_baseline(session: Session,
                             country_name: str,
                             window_start: dt.date,
                             window_end: dt.date) -> BaselineResult:
    """Compute monthly move-out rate for a country over the given window."""
    occupied_days = session.execute(text("""
        SELECT COUNT(*) FROM rentroll r
        JOIN siteinfo s ON s.SiteID = r.SiteID
        WHERE r.bRented = :rented
          AND r.extract_date BETWEEN :ws AND :we
          AND s.Country = :country
    """), {"rented": True, "ws": window_start, "we": window_end,
           "country": country_name}).scalar() or 0

    moveout_count = session.execute(text("""
        SELECT COUNT(*) FROM mimo m
        JOIN siteinfo s ON s.SiteID = m.SiteID
        WHERE m.MoveOut = 1
          AND m.MoveDate >= :ws AND m.MoveDate < :we_next
          AND s.Country = :country
    """), {"ws": window_start,
           "we_next": dt.datetime.combine(window_end + dt.timedelta(days=1), dt.time.min),
           "country": country_name}).scalar() or 0

    unit_months = (Decimal(occupied_days) / DAYS_PER_MONTH) if occupied_days else Decimal(0)
    rate = (Decimal(moveout_count) / unit_months) if unit_months > 0 else Decimal(0)
    return BaselineResult(
        country_name=country_name,
        window_start=window_start, window_end=window_end,
        moveout_count=moveout_count,
        unit_months_occupied=unit_months.quantize(Decimal("0.01")),
        baseline_rate=rate.quantize(Decimal("0.000001")),
    )
