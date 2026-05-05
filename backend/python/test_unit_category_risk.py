"""Tests for unit-category risk pipeline.

Uses an in-memory SQLite engine seeded with a tiny fixture: 1 country,
365 days of rentroll occupancy, 12 moveouts in window.
"""
import datetime as dt
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, Column, Integer, Date, Boolean, String, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base

from datalayer.unit_category_risk import compute_country_baseline

Base = declarative_base()


class _RentRoll(Base):
    __tablename__ = 'rentroll'
    extract_date = Column(Date, primary_key=True)
    SiteID = Column(Integer, primary_key=True)
    UnitID = Column(Integer, primary_key=True)
    bRented = Column(Boolean)
    sTypeName = Column(String(100))


class _MIMO(Base):
    __tablename__ = 'mimo'
    SiteID = Column(Integer, primary_key=True)
    TenantID = Column(Integer, primary_key=True)
    MoveDate = Column(DateTime, primary_key=True)
    MoveOut = Column(Integer)
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
                            bRented=True, sTypeName="S/8-10/W/A/SS/NP"))
    for m in range(12):
        s.add(_MIMO(SiteID=1, TenantID=m, MoveOut=1,
                    MoveDate=dt.datetime(2026, m + 1 if m < 4 else 1, 15),
                    sUnitType="S/8-10/W/A/SS/NP"))
    s.commit()
    return s


def test_baseline_rate_korea(session):
    window_end = dt.date(2026, 5, 5)
    window_start = window_end - dt.timedelta(days=365 * 2)
    result = compute_country_baseline(
        session, country_name="Korea",
        window_start=window_start, window_end=window_end)
    assert float(result.unit_months_occupied) == pytest.approx(36500 / 30.4375, rel=1e-3)
    assert result.moveout_count == 12
    assert float(result.baseline_rate) == pytest.approx(12 / (36500 / 30.4375), rel=1e-3)


from datalayer.unit_category_risk import compute_cell_factors


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
