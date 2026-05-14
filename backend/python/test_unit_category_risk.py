"""Tests for unit-category risk pipeline.

Uses an in-memory SQLite engine seeded with a tiny fixture: 1 country,
365 days of rentroll occupancy, 12 moveouts in window.
"""
import datetime as dt
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, Column, Integer, Date, Boolean, String, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base

from sync_service.pipelines.unit_category_risk import compute_country_baseline

Base = declarative_base()


class _RentRoll(Base):
    __tablename__ = 'rentroll'
    extract_date = Column(Date, primary_key=True)
    SiteID = Column(Integer, primary_key=True)
    UnitID = Column(Integer, primary_key=True)
    sUnit = Column(String(50))
    bRented = Column(Boolean)
    sTypeName = Column(String(100))


class _MIMO(Base):
    __tablename__ = 'mimo'
    SiteID = Column(Integer, primary_key=True)
    TenantID = Column(Integer, primary_key=True)
    MoveDate = Column(DateTime, primary_key=True)
    MoveOut = Column(Integer)
    UnitName = Column(String(100))
    sUnitType = Column(String(100))


class _SiteInfo(Base):
    __tablename__ = 'siteinfo'
    SiteID = Column(Integer, primary_key=True)
    Country = Column(String(100))


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    s = Session()
    s.add(_SiteInfo(SiteID=1, Country="Korea"))
    today = dt.date(2026, 5, 5)
    for d in range(365):
        date = today - dt.timedelta(days=d)
        for u in range(100):
            s.add(_RentRoll(extract_date=date, SiteID=1, UnitID=u,
                            sUnit=f"U{u:04d}",
                            bRented=True, sTypeName="S/8-10/W/A/SS/NP"))
    for m in range(12):
        s.add(_MIMO(SiteID=1, TenantID=m, MoveOut=1,
                    MoveDate=dt.datetime(2026, m + 1 if m < 4 else 1, 15),
                    UnitName=f"U{m:04d}",
                    sUnitType="legacy-name-pre-SOP"))
    s.commit()
    return s


def test_baseline_rate_korea(session):
    window_end = dt.date(2026, 5, 5)
    window_start = window_end - dt.timedelta(days=365 * 2)
    result = compute_country_baseline(
        session, country_name="Korea",
        window_start=window_start, window_end=window_end)
    # Sparse-snapshot extrapolation: avg_occupied_per_snapshot * window_months.
    # Fixture: 365 snapshots × 100 occupied units, window = 730 days ≈ 23.98 months.
    # Expected unit-months = (36500 / 365) * (730 / 30.4375) = 100 * 23.98 = 2398.36
    expected_um = (36500 / 365) * (730 / 30.4375)
    assert float(result.unit_months_occupied) == pytest.approx(expected_um, rel=1e-3)
    assert result.moveout_count == 12
    assert float(result.baseline_rate) == pytest.approx(12 / expected_um, rel=1e-3)


from sync_service.pipelines.unit_category_risk import compute_cell_factors


def test_cell_factors_size_S(session):
    we = dt.date(2026, 5, 5)
    ws = we - dt.timedelta(days=365 * 2)
    baseline = compute_country_baseline(session, "Korea", ws, we)
    cells = compute_cell_factors(
        session, country_name="Korea",
        window_start=ws, window_end=we,
        baseline_rate=float(baseline.baseline_rate),
        sample_size_threshold=5,
    )
    # All units are S/8-10/W/A/SS/NP, so size=S cell rate == baseline → factor 1.0
    size_s = next(c for c in cells if c.dimension == "size" and c.value == "S")
    assert float(size_s.empirical_factor) == pytest.approx(1.0, rel=1e-3)
    assert size_s.sample_size == 12
    assert size_s.is_thin_data is False  # 12 >= 5

    # No units of size M → cell shouldn't appear
    assert not any(c for c in cells if c.dimension == "size" and c.value == "M")


from sync_service.pipelines.unit_category_risk import upsert_factors, upsert_baseline
from sqlalchemy import text


def _ensure_risk_tables(session):
    """SQLite-friendly create of the 3 risk tables for tests."""
    session.execute(text("""
        CREATE TABLE IF NOT EXISTS unit_category_risk_baseline (
            country_code VARCHAR(2) PRIMARY KEY,
            window_start DATE, window_end DATE,
            moveout_count INTEGER, unit_months_occupied NUMERIC,
            baseline_rate NUMERIC, computed_at DATETIME)"""))
    session.execute(text("""
        CREATE TABLE IF NOT EXISTS unit_category_risk_factor (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_code VARCHAR(2), dimension VARCHAR(16), value VARCHAR(16),
            sample_size INTEGER, unit_months_occupied NUMERIC,
            empirical_factor NUMERIC, override_factor NUMERIC,
            effective_factor NUMERIC, is_thin_data BOOLEAN,
            override_reason TEXT, override_by VARCHAR(64),
            override_at DATETIME, computed_at DATETIME,
            UNIQUE (country_code, dimension, value))"""))
    session.execute(text("""
        CREATE TABLE IF NOT EXISTS unit_category_risk_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_month DATE, country_code VARCHAR(2),
            dimension VARCHAR(16), value VARCHAR(16),
            empirical_factor NUMERIC, sample_size INTEGER,
            baseline_rate NUMERIC,
            UNIQUE (country_code, dimension, value, snapshot_month))"""))
    session.commit()


def test_override_preserved_across_recompute(session):
    _ensure_risk_tables(session)
    we = dt.date(2026, 5, 5)
    ws = we - dt.timedelta(days=365 * 2)
    baseline = compute_country_baseline(session, "Korea", ws, we)
    upsert_baseline(session, country_code="KR", baseline=baseline)
    cells = compute_cell_factors(
        session, "Korea", ws, we, float(baseline.baseline_rate), 5)
    upsert_factors(session, country_code="KR", cells=cells)

    # Set an admin override on size=S
    session.execute(text("""
        UPDATE unit_category_risk_factor
           SET override_factor = 1.25, override_by = 'tester',
               override_reason = 'manual', override_at = CURRENT_TIMESTAMP,
               effective_factor = 1.25
         WHERE country_code='KR' AND dimension='size' AND value='S'"""))
    session.commit()

    # Re-run upsert (simulated recompute)
    upsert_factors(session, country_code="KR", cells=cells)
    row = session.execute(text("""
        SELECT override_factor, effective_factor FROM unit_category_risk_factor
         WHERE country_code='KR' AND dimension='size' AND value='S'""")).fetchone()
    assert float(row[0]) == 1.25
    # Effective should still be the override
    assert float(row[1]) == 1.25
