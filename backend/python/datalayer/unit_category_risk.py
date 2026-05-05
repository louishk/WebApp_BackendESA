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


from common.stype_name_parser import parse_stype_name

DIMENSIONS = ("size", "range", "type", "climate", "shape", "pillar")


@dataclass
class CellFactor:
    dimension: str
    value: str
    sample_size: int
    unit_months_occupied: Decimal
    empirical_factor: Optional[Decimal]
    is_thin_data: bool


def _explode_dims(s_type_name: str) -> dict[str, str]:
    """Parse sTypeName via SOP parser; return {dim: value}, empty on failure."""
    if not s_type_name:
        return {}
    parsed = parse_stype_name(s_type_name)
    if not parsed.parse_ok:
        return {}
    return {
        "size":    parsed.size_category or "",
        "range":   parsed.size_range or "",
        "type":    parsed.unit_type or "",
        "climate": parsed.climate_type or "",
        "shape":   parsed.unit_shape or "",
        "pillar":  parsed.pillar or "",
    }


def compute_cell_factors(session: Session,
                         country_name: str,
                         window_start: dt.date,
                         window_end: dt.date,
                         baseline_rate: float,
                         sample_size_threshold: int) -> list[CellFactor]:
    """Aggregate occupancy + moveouts per (dimension, value) for one country."""
    occ_rows = session.execute(text("""
        SELECT r.sTypeName, COUNT(*) AS days
        FROM rentroll r
        JOIN siteinfo s ON s.SiteID = r.SiteID
        WHERE r.bRented = :rented
          AND r.extract_date BETWEEN :ws AND :we
          AND s.Country = :country
        GROUP BY r.sTypeName
    """), {"rented": True, "ws": window_start, "we": window_end,
           "country": country_name}).fetchall()

    mo_rows = session.execute(text("""
        SELECT m.sUnitType, COUNT(*) AS n
        FROM mimo m
        JOIN siteinfo s ON s.SiteID = m.SiteID
        WHERE m.MoveOut = 1
          AND m.MoveDate >= :ws AND m.MoveDate < :we_next
          AND s.Country = :country
        GROUP BY m.sUnitType
    """), {"ws": window_start,
           "we_next": dt.datetime.combine(window_end + dt.timedelta(days=1), dt.time.min),
           "country": country_name}).fetchall()

    occ_by_cell: dict[tuple[str, str], Decimal] = {}
    for stype, days in occ_rows:
        dims = _explode_dims(stype or "")
        for dim, val in dims.items():
            if not val:
                continue
            key = (dim, val)
            occ_by_cell[key] = occ_by_cell.get(key, Decimal(0)) + Decimal(days)

    mo_by_cell: dict[tuple[str, str], int] = {}
    for stype, n in mo_rows:
        dims = _explode_dims(stype or "")
        for dim, val in dims.items():
            if not val:
                continue
            key = (dim, val)
            mo_by_cell[key] = mo_by_cell.get(key, 0) + int(n)

    cells: list[CellFactor] = []
    for key, days in occ_by_cell.items():
        dim, val = key
        unit_months = (days / DAYS_PER_MONTH) if days else Decimal(0)
        sample = mo_by_cell.get(key, 0)
        if unit_months > 0 and baseline_rate > 0:
            cell_rate = Decimal(sample) / unit_months
            empirical = (cell_rate / Decimal(str(baseline_rate))).quantize(Decimal("0.000001"))
        else:
            empirical = None
        cells.append(CellFactor(
            dimension=dim, value=val,
            sample_size=sample,
            unit_months_occupied=unit_months.quantize(Decimal("0.01")),
            empirical_factor=empirical,
            is_thin_data=sample < sample_size_threshold,
        ))
    return cells
