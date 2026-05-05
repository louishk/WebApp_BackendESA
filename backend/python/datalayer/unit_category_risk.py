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


from common.risk_lookup import resolve_effective_factor


def upsert_baseline(session: Session, country_code: str, baseline: BaselineResult) -> None:
    session.execute(text("""
        INSERT INTO unit_category_risk_baseline
            (country_code, window_start, window_end, moveout_count,
             unit_months_occupied, baseline_rate, computed_at)
        VALUES (:cc, :ws, :we, :mo, :um, :rate, CURRENT_TIMESTAMP)
        ON CONFLICT (country_code) DO UPDATE SET
            window_start = EXCLUDED.window_start,
            window_end = EXCLUDED.window_end,
            moveout_count = EXCLUDED.moveout_count,
            unit_months_occupied = EXCLUDED.unit_months_occupied,
            baseline_rate = EXCLUDED.baseline_rate,
            computed_at = EXCLUDED.computed_at
    """), {"cc": country_code,
           "ws": baseline.window_start, "we": baseline.window_end,
           "mo": baseline.moveout_count,
           "um": float(baseline.unit_months_occupied),
           "rate": float(baseline.baseline_rate)})
    session.commit()


def upsert_factors(session: Session, country_code: str, cells: list[CellFactor]) -> None:
    """UPSERT cells; preserve override_*; recompute effective_factor.

    Reads existing override (if any) per cell, computes effective via
    resolve_effective_factor, and writes that. Override columns themselves
    are not touched in the UPDATE branch.
    """
    for c in cells:
        existing = session.execute(text("""
            SELECT override_factor FROM unit_category_risk_factor
             WHERE country_code=:cc AND dimension=:d AND value=:v
        """), {"cc": country_code, "d": c.dimension, "v": c.value}).fetchone()
        existing_override = float(existing[0]) if existing and existing[0] is not None else None
        empirical = float(c.empirical_factor) if c.empirical_factor is not None else None
        effective, _src = resolve_effective_factor(
            empirical=empirical, override=existing_override, is_thin=c.is_thin_data)

        session.execute(text("""
            INSERT INTO unit_category_risk_factor
                (country_code, dimension, value, sample_size,
                 unit_months_occupied, empirical_factor,
                 effective_factor, is_thin_data, computed_at)
            VALUES (:cc, :d, :v, :ss, :um, :emp, :eff, :thin, CURRENT_TIMESTAMP)
            ON CONFLICT (country_code, dimension, value) DO UPDATE SET
                sample_size = EXCLUDED.sample_size,
                unit_months_occupied = EXCLUDED.unit_months_occupied,
                empirical_factor = EXCLUDED.empirical_factor,
                effective_factor = EXCLUDED.effective_factor,
                is_thin_data = EXCLUDED.is_thin_data,
                computed_at = EXCLUDED.computed_at
        """), {"cc": country_code, "d": c.dimension, "v": c.value,
               "ss": c.sample_size, "um": float(c.unit_months_occupied),
               "emp": empirical, "eff": effective, "thin": c.is_thin_data})
    session.commit()


def snapshot_history(session: Session, country_code: str,
                     baseline: BaselineResult,
                     cells: list[CellFactor],
                     month: dt.date) -> None:
    for c in cells:
        session.execute(text("""
            INSERT INTO unit_category_risk_history
                (snapshot_month, country_code, dimension, value,
                 empirical_factor, sample_size, baseline_rate)
            VALUES (:m, :cc, :d, :v, :emp, :ss, :rate)
            ON CONFLICT (country_code, dimension, value, snapshot_month)
            DO UPDATE SET
                empirical_factor = EXCLUDED.empirical_factor,
                sample_size = EXCLUDED.sample_size,
                baseline_rate = EXCLUDED.baseline_rate
        """), {"m": month, "cc": country_code, "d": c.dimension, "v": c.value,
               "emp": float(c.empirical_factor) if c.empirical_factor is not None else None,
               "ss": c.sample_size, "rate": float(baseline.baseline_rate)})
    session.commit()
