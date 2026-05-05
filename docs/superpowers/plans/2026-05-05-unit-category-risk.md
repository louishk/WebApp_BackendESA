# Unit-Category Risk Scoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a country-scoped, multiplicative move-out risk model derived from a 2-year rolling window, exposed via API + admin UI, ready for pricing consumption.

**Architecture:** APScheduler pipeline computes empirical per-(country, dimension, value) factors from `rentroll` (occupancy denominator) and `mimo` (move-out numerator) joined to `siteinfo`. Three new `esa_pbi` tables store baseline + factors + monthly history. Admin overrides survive recompute. Lookup is on-demand (`baseline × Π factor`).

**Tech Stack:** Python 3, Flask, SQLAlchemy ORM, APScheduler, PostgreSQL (esa_pbi), Jinja2 + vanilla JS, pytest.

**Spec:** `docs/superpowers/specs/2026-05-04-unit-category-risk-design.md`

## File Structure

| File | Purpose | New / Modify |
|---|---|---|
| `sql/2026-05-05-unit-category-risk.sql` | Migration: 3 new tables in esa_pbi | New |
| `backend/python/common/models.py` | Add 3 SQLAlchemy models | Modify |
| `backend/python/config/risk.yaml` | Window, threshold, gradient, country code↔name map | New |
| `backend/python/web/utils/audit.py` | Add 3 AuditEvent constants | Modify |
| `backend/python/web/auth/jwt_auth.py` | Add `risk_read` / `risk_admin` scopes | Modify |
| `backend/python/web/auth/decorators.py` | Add `risk_admin_access_required` | Modify |
| `backend/python/datalayer/unit_category_risk.py` | Pipeline: compute baseline + factors | New |
| `backend/python/common/risk_lookup.py` | Pure-function lookup + gradient bucketing | New |
| `backend/python/config/pipelines.yaml` | Register `unit_category_risk` | Modify |
| `backend/python/web/routes/risk.py` | Blueprint with 4 API endpoints | New |
| `backend/python/web/routes/admin_risk.py` | Admin UI route | New |
| `backend/python/web/templates/admin/risk_factors.html` | Admin UI page | New |
| `backend/python/web/__init__.py` (or app factory) | Register the new blueprints | Modify |
| `tests/datalayer/test_unit_category_risk.py` | Pipeline tests | New |
| `tests/common/test_risk_lookup.py` | Lookup tests | New |
| `tests/web/routes/test_risk_api.py` | API tests | New |

---

## Task 1: Migration — create the three tables

**Files:**
- Create: `sql/2026-05-05-unit-category-risk.sql`

- [ ] **Step 1: Write the migration SQL**

```sql
-- sql/2026-05-05-unit-category-risk.sql
-- Tables for unit-category risk scoring (esa_pbi)

CREATE TABLE IF NOT EXISTS unit_category_risk_baseline (
    country_code        VARCHAR(2)    PRIMARY KEY,
    window_start        DATE          NOT NULL,
    window_end          DATE          NOT NULL,
    moveout_count       INTEGER       NOT NULL DEFAULT 0,
    unit_months_occupied NUMERIC(14,2) NOT NULL DEFAULT 0,
    baseline_rate       NUMERIC(8,6)  NOT NULL DEFAULT 0,
    computed_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS unit_category_risk_factor (
    id                   SERIAL        PRIMARY KEY,
    country_code         VARCHAR(2)    NOT NULL,
    dimension            VARCHAR(16)   NOT NULL,
    value                VARCHAR(16)   NOT NULL,
    sample_size          INTEGER       NOT NULL DEFAULT 0,
    unit_months_occupied NUMERIC(14,2) NOT NULL DEFAULT 0,
    empirical_factor     NUMERIC(8,6),
    override_factor      NUMERIC(8,6),
    effective_factor     NUMERIC(8,6)  NOT NULL DEFAULT 1.0,
    is_thin_data         BOOLEAN       NOT NULL DEFAULT TRUE,
    override_reason      TEXT,
    override_by          VARCHAR(64),
    override_at          TIMESTAMPTZ,
    computed_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_risk_factor UNIQUE (country_code, dimension, value),
    CONSTRAINT ck_risk_factor_dim CHECK (dimension IN
        ('size','range','type','climate','shape','pillar')),
    CONSTRAINT ck_risk_factor_override_range CHECK
        (override_factor IS NULL OR (override_factor >= 0.1 AND override_factor <= 5.0))
);
CREATE INDEX IF NOT EXISTS idx_risk_factor_country
    ON unit_category_risk_factor(country_code);

CREATE TABLE IF NOT EXISTS unit_category_risk_history (
    id               SERIAL        PRIMARY KEY,
    snapshot_month   DATE          NOT NULL,
    country_code     VARCHAR(2)    NOT NULL,
    dimension        VARCHAR(16)   NOT NULL,
    value            VARCHAR(16)   NOT NULL,
    empirical_factor NUMERIC(8,6),
    sample_size      INTEGER       NOT NULL DEFAULT 0,
    baseline_rate    NUMERIC(8,6)  NOT NULL DEFAULT 0,
    CONSTRAINT uq_risk_history UNIQUE
        (country_code, dimension, value, snapshot_month)
);
CREATE INDEX IF NOT EXISTS idx_risk_history_lookup
    ON unit_category_risk_history(country_code, dimension, value);
```

- [ ] **Step 2: Run the migration against esa_pbi**

```bash
DB_PW=$(python3 -c "from dotenv import load_dotenv; load_dotenv('.env'); import os; print(os.environ['PBI_DB_PASSWORD'])")
PGPASSWORD="$DB_PW" psql -h esapbi.postgres.database.azure.com -U esa_pbi_admin -d esa_pbi \
    -f sql/2026-05-05-unit-category-risk.sql
```

Expected: three `CREATE TABLE` and two `CREATE INDEX` notices, no errors.

- [ ] **Step 3: Verify tables exist**

```bash
PGPASSWORD="$DB_PW" psql -h esapbi.postgres.database.azure.com -U esa_pbi_admin -d esa_pbi \
    -c "\d unit_category_risk_baseline" \
    -c "\d unit_category_risk_factor" \
    -c "\d unit_category_risk_history"
```

Expected: all three tables described with the columns from Step 1.

- [ ] **Step 4: Commit**

```bash
git add sql/2026-05-05-unit-category-risk.sql
git commit -m "feat(risk): migration for unit-category risk tables"
```

---

## Task 2: SQLAlchemy models

**Files:**
- Modify: `backend/python/common/models.py` (append at end of file)

- [ ] **Step 1: Append model definitions to models.py**

```python
# === Unit-Category Risk Scoring ===

class UnitCategoryRiskBaseline(Base, BaseModel):
    __tablename__ = 'unit_category_risk_baseline'

    country_code = Column(String(2), primary_key=True)
    window_start = Column(Date, nullable=False)
    window_end = Column(Date, nullable=False)
    moveout_count = Column(Integer, nullable=False, default=0)
    unit_months_occupied = Column(Numeric(14, 2), nullable=False, default=0)
    baseline_rate = Column(Numeric(8, 6), nullable=False, default=0)
    computed_at = Column(DateTime(timezone=True), nullable=False)


class UnitCategoryRiskFactor(Base, BaseModel):
    __tablename__ = 'unit_category_risk_factor'

    id = Column(Integer, primary_key=True)
    country_code = Column(String(2), nullable=False, index=True)
    dimension = Column(String(16), nullable=False)
    value = Column(String(16), nullable=False)
    sample_size = Column(Integer, nullable=False, default=0)
    unit_months_occupied = Column(Numeric(14, 2), nullable=False, default=0)
    empirical_factor = Column(Numeric(8, 6), nullable=True)
    override_factor = Column(Numeric(8, 6), nullable=True)
    effective_factor = Column(Numeric(8, 6), nullable=False, default=1.0)
    is_thin_data = Column(Boolean, nullable=False, default=True)
    override_reason = Column(Text, nullable=True)
    override_by = Column(String(64), nullable=True)
    override_at = Column(DateTime(timezone=True), nullable=True)
    computed_at = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint('country_code', 'dimension', 'value', name='uq_risk_factor'),
    )


class UnitCategoryRiskHistory(Base, BaseModel):
    __tablename__ = 'unit_category_risk_history'

    id = Column(Integer, primary_key=True)
    snapshot_month = Column(Date, nullable=False)
    country_code = Column(String(2), nullable=False)
    dimension = Column(String(16), nullable=False)
    value = Column(String(16), nullable=False)
    empirical_factor = Column(Numeric(8, 6), nullable=True)
    sample_size = Column(Integer, nullable=False, default=0)
    baseline_rate = Column(Numeric(8, 6), nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint('country_code', 'dimension', 'value', 'snapshot_month',
                         name='uq_risk_history'),
    )
```

If `Text` or `UniqueConstraint` aren't already imported, add them to the imports at top of `models.py`.

- [ ] **Step 2: Smoke-test model imports**

```bash
cd backend/python && python3 -c "from common.models import (
    UnitCategoryRiskBaseline, UnitCategoryRiskFactor, UnitCategoryRiskHistory)
print('ok')"
```

Expected: `ok`.

- [ ] **Step 3: Commit**

```bash
git add backend/python/common/models.py
git commit -m "feat(risk): add SQLAlchemy models for risk tables"
```

---

## Task 3: Add `risk.yaml` config + AuditEvent constants

**Files:**
- Create: `backend/python/config/risk.yaml`
- Modify: `backend/python/web/utils/audit.py`

- [ ] **Step 1: Create risk.yaml**

```yaml
# backend/python/config/risk.yaml
window_years: 2
sample_size_threshold: 30
recompute_cron: "0 3 1 * *"

# 2-letter code → siteinfo.Country full name match
countries:
  KR: Korea
  SG: Singapore
  JP: Japan
  MY: Malaysia
  HK: Hong Kong

# Bands ordered ascending by `max_composite`. Last entry uses null = +inf.
gradient_bands:
  - {max_composite: 0.70, label: very_low,  color: "#1b5e20"}
  - {max_composite: 0.90, label: low,       color: "#2e7d32"}
  - {max_composite: 1.10, label: average,   color: "#9e9e9e"}
  - {max_composite: 1.30, label: high,      color: "#f57c00"}
  - {max_composite: null, label: very_high, color: "#c62828"}
```

- [ ] **Step 2: Add AuditEvent constants**

In `backend/python/web/utils/audit.py`, find the `class AuditEvent:` block and append new constants near the existing `# Configuration` section:

```python
    # Risk scoring
    RISK_RECOMPUTE = 'RISK_RECOMPUTE'
    RISK_OVERRIDE_SET = 'RISK_OVERRIDE_SET'
    RISK_OVERRIDE_CLEARED = 'RISK_OVERRIDE_CLEARED'
```

- [ ] **Step 3: Verify config loads**

```bash
cd backend/python && python3 -c "
from common.config_loader import load_yaml_config
cfg = load_yaml_config('risk.yaml')
print(cfg['countries'], cfg['sample_size_threshold'])"
```

Expected: prints country dict and `30`. (If `load_yaml_config` is named differently, adjust to match the actual loader function used in other config calls.)

- [ ] **Step 4: Commit**

```bash
git add backend/python/config/risk.yaml backend/python/web/utils/audit.py
git commit -m "feat(risk): add risk.yaml config and audit events"
```

---

## Task 4: Pure lookup module (TDD)

**Files:**
- Create: `backend/python/common/risk_lookup.py`
- Create: `tests/common/test_risk_lookup.py`

The lookup is pure: takes a baseline rate, a dict of `{dim: factor_row}`, applies missing-dim-as-1.0, computes composite, picks band. No DB inside.

- [ ] **Step 1: Write failing tests**

```python
# tests/common/test_risk_lookup.py
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
    b = bucket_band(0.80, BANDS)
    assert b["label"] == "low"


def test_bucket_band_edge_average():
    # 0.90 lands in 'low' (max_composite is exclusive upper bound by spec? -> use <=)
    # We define max_composite as inclusive upper.
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
    # Only size contributes — composite = 0.80
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
```

- [ ] **Step 2: Run tests, verify failure**

```bash
cd backend/python && pytest ../../tests/common/test_risk_lookup.py -v
```

Expected: FAIL — module not found.

- [ ] **Step 3: Implement risk_lookup.py**

```python
# backend/python/common/risk_lookup.py
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
```

- [ ] **Step 4: Run tests, verify pass**

```bash
cd backend/python && pytest ../../tests/common/test_risk_lookup.py -v
```

Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add backend/python/common/risk_lookup.py tests/common/test_risk_lookup.py
git commit -m "feat(risk): pure lookup + gradient bucketing with tests"
```

---

## Task 5: Effective-factor resolution (TDD)

This is the small but critical rule from spec §5.4. Pure function; no DB.

**Files:**
- Modify: `backend/python/common/risk_lookup.py`
- Modify: `tests/common/test_risk_lookup.py`

- [ ] **Step 1: Append failing tests**

```python
# Append to tests/common/test_risk_lookup.py
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
    # An admin override should win even if cell is thin.
    eff, source = resolve_effective_factor(
        empirical=None, override=1.10, is_thin=True)
    assert eff == 1.10 and source == "override"
```

- [ ] **Step 2: Run, expect failure (function not defined)**

```bash
cd backend/python && pytest ../../tests/common/test_risk_lookup.py -v
```

Expected: 5 new tests fail with ImportError.

- [ ] **Step 3: Add resolver**

Append to `backend/python/common/risk_lookup.py`:

```python
def resolve_effective_factor(empirical: Optional[float],
                             override: Optional[float],
                             is_thin: bool) -> tuple[float, str]:
    """Spec §5.4: override > thin-neutral > empirical."""
    if override is not None:
        return float(override), "override"
    if is_thin or empirical is None:
        return 1.0, "thin_neutral"
    return float(empirical), "empirical"
```

- [ ] **Step 4: Run, verify pass**

```bash
cd backend/python && pytest ../../tests/common/test_risk_lookup.py -v
```

Expected: 10 passed.

- [ ] **Step 5: Commit**

```bash
git add backend/python/common/risk_lookup.py tests/common/test_risk_lookup.py
git commit -m "feat(risk): effective-factor resolver"
```

---

## Task 6: Pipeline — country baseline computation (TDD)

**Files:**
- Create: `backend/python/datalayer/unit_category_risk.py`
- Create: `tests/datalayer/test_unit_category_risk.py`

We build the pipeline incrementally: Task 6 = baseline only, Task 7 = per-cell factors, Task 8 = persistence + override preservation, Task 9 = orchestration entry point.

- [ ] **Step 1: Write failing test for baseline calc**

```python
# tests/datalayer/test_unit_category_risk.py
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
    # Site 1 in Korea
    s.add(_SiteInfo(SiteID=1, Country="Korea"))
    today = dt.date(2026, 5, 5)
    # 100 units, all rented every day for a year
    for d in range(365):
        date = today - dt.timedelta(days=d)
        for u in range(100):
            s.add(_RentRoll(extract_date=date, SiteID=1, UnitID=u,
                            bRented=True, sTypeName="S/8-10/W/A/SS/NP"))
    # 12 moveouts in last 12 months
    for m in range(12):
        s.add(_MIMO(SiteID=1, TenantID=m, MoveOut=1,
                    MoveDate=dt.datetime(2026, m + 1 if m < 5 else 1, 15),
                    sUnitType="S/8-10/W/A/SS/NP"))
    s.commit()
    return s


def test_baseline_rate_korea(session):
    window_end = dt.date(2026, 5, 5)
    window_start = window_end - dt.timedelta(days=365 * 2)
    result = compute_country_baseline(
        session, country_name="Korea",
        window_start=window_start, window_end=window_end)
    # 100 units × 365 days = 36500 unit-days = ~1199.4 unit-months
    # 12 moveouts → rate ≈ 12/1199.4 ≈ 0.01
    assert result.unit_months_occupied == pytest.approx(36500 / 30.4375, rel=1e-3)
    assert result.moveout_count == 12
    assert float(result.baseline_rate) == pytest.approx(12 / (36500 / 30.4375), rel=1e-3)
```

- [ ] **Step 2: Run, expect failure**

```bash
cd backend/python && pytest ../../tests/datalayer/test_unit_category_risk.py -v
```

Expected: ImportError on `compute_country_baseline`.

- [ ] **Step 3: Implement baseline calc**

```python
# backend/python/datalayer/unit_category_risk.py
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
```

- [ ] **Step 4: Run, verify pass**

```bash
cd backend/python && pytest ../../tests/datalayer/test_unit_category_risk.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/python/datalayer/unit_category_risk.py tests/datalayer/test_unit_category_risk.py
git commit -m "feat(risk): pipeline — country baseline computation"
```

---

## Task 7: Pipeline — per-cell factor computation (TDD)

**Files:**
- Modify: `backend/python/datalayer/unit_category_risk.py`
- Modify: `tests/datalayer/test_unit_category_risk.py`

For each `(dimension, value)` cell, compute occupancy, moveouts, empirical factor, thin flag. Uses `stype_name_parser` to break `sTypeName` into 6 dims.

- [ ] **Step 1: Append failing test**

```python
# Append to tests/datalayer/test_unit_category_risk.py
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
```

- [ ] **Step 2: Run, expect ImportError**

```bash
cd backend/python && pytest ../../tests/datalayer/test_unit_category_risk.py::test_cell_factors_size_S -v
```

- [ ] **Step 3: Implement**

Append to `backend/python/datalayer/unit_category_risk.py`:

```python
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
    """Parse sTypeName via SOP parser; fall back to empty dict on failure."""
    if not s_type_name:
        return {}
    parsed = parse_stype_name(s_type_name)
    if not parsed.parse_ok:
        return {}
    return {
        "size":    parsed.size or "",
        "range":   parsed.size_range or "",
        "type":    parsed.unit_type or "",
        "climate": parsed.climate or "",
        "shape":   parsed.shape or "",
        "pillar":  parsed.pillar or "",
    }


def compute_cell_factors(session: Session,
                         country_name: str,
                         window_start: dt.date,
                         window_end: dt.date,
                         baseline_rate: float,
                         sample_size_threshold: int) -> list[CellFactor]:
    """Aggregate occupancy + moveouts per (dimension, value) for one country."""
    # Pull occupied unit-days with sTypeName
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
```

- [ ] **Step 4: Run, verify pass**

```bash
cd backend/python && pytest ../../tests/datalayer/test_unit_category_risk.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add backend/python/datalayer/unit_category_risk.py tests/datalayer/test_unit_category_risk.py
git commit -m "feat(risk): pipeline — per-cell factor computation"
```

---

## Task 8: Persistence layer (override preservation) — TDD

**Files:**
- Modify: `backend/python/datalayer/unit_category_risk.py`
- Modify: `tests/datalayer/test_unit_category_risk.py`

UPSERTs that preserve override columns and recompute `effective_factor` via `resolve_effective_factor`.

- [ ] **Step 1: Append failing test**

```python
# Append to tests/datalayer/test_unit_category_risk.py
from datalayer.unit_category_risk import upsert_factors, upsert_baseline
from common.risk_lookup import resolve_effective_factor


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
```

- [ ] **Step 2: Run, expect ImportError**

```bash
cd backend/python && pytest ../../tests/datalayer/test_unit_category_risk.py::test_override_preserved_across_recompute -v
```

- [ ] **Step 3: Implement upsert helpers**

Append to `backend/python/datalayer/unit_category_risk.py`:

```python
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

    Uses ON CONFLICT (country_code, dimension, value) — touches only the
    refreshable columns. The trick: read the existing override (if any),
    compute effective via resolve_effective_factor, and write that.
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
```

> **Note:** SQLite supports `ON CONFLICT` with the same syntax as Postgres for UNIQUE constraint upserts (since SQLite 3.24), so the same SQL works in tests.

- [ ] **Step 4: Run, verify pass**

```bash
cd backend/python && pytest ../../tests/datalayer/test_unit_category_risk.py -v
```

Expected: all 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add backend/python/datalayer/unit_category_risk.py tests/datalayer/test_unit_category_risk.py
git commit -m "feat(risk): persistence with override preservation + history"
```

---

## Task 9: Pipeline orchestrator entry point + register

**Files:**
- Modify: `backend/python/datalayer/unit_category_risk.py`
- Modify: `backend/python/config/pipelines.yaml`

- [ ] **Step 1: Add `run()` orchestrator**

Append to `backend/python/datalayer/unit_category_risk.py`:

```python
import datetime as _dt
from common.config_loader import load_yaml_config


def run(country_code: Optional[str] = None) -> dict:
    """Pipeline entry point. Called by APScheduler and by /api/risk/recompute.

    If country_code is given, only that country is recomputed; otherwise all
    countries listed in risk.yaml are processed.
    """
    from common.config_loader import get_database_url
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from web.utils.audit import audit_log, AuditEvent

    cfg = load_yaml_config('risk.yaml')
    countries: dict[str, str] = cfg['countries']     # {code: name}
    threshold = int(cfg['sample_size_threshold'])
    window_years = int(cfg['window_years'])

    engine = create_engine(get_database_url('pbi'))
    Session = sessionmaker(bind=engine)
    session = Session()

    targets = ({country_code: countries[country_code]}
               if country_code else countries)

    today = _dt.date.today()
    window_end = today
    window_start = today.replace(year=today.year - window_years)
    snapshot_month = today.replace(day=1)
    summary = {"countries": [], "errors": []}

    try:
        for code, name in targets.items():
            try:
                baseline = compute_country_baseline(session, name, window_start, window_end)
                upsert_baseline(session, code, baseline)
                if baseline.moveout_count < 100:
                    logger.warning(
                        "Country %s has only %d moveouts in window — factors will be thin",
                        code, baseline.moveout_count)
                cells = compute_cell_factors(
                    session, name, window_start, window_end,
                    float(baseline.baseline_rate), threshold)
                upsert_factors(session, code, cells)
                snapshot_history(session, code, baseline, cells, snapshot_month)
                audit_log(AuditEvent.RISK_RECOMPUTE,
                          country=code, factors=len(cells),
                          baseline_rate=float(baseline.baseline_rate))
                summary["countries"].append({
                    "country_code": code,
                    "moveout_count": baseline.moveout_count,
                    "factors_written": len(cells),
                    "baseline_rate": float(baseline.baseline_rate),
                })
            except Exception as exc:
                logger.exception("Risk recompute failed for %s", code)
                summary["errors"].append({"country_code": code, "error": str(exc)})
    finally:
        session.close()

    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run())
```

- [ ] **Step 2: Register in `pipelines.yaml`**

Insert under the `pipelines:` mapping (alphabetical-ish — find a good neighbour like `units_info`):

```yaml
  unit_category_risk:
    display_name: Unit-Category Risk
    description: Compute country-scoped move-out risk factors per dimension/value
    module_path: datalayer.unit_category_risk
    enabled: true
    schedule:
      type: cron
      cron: 0 3 1 * *
    priority: 8
    resource_group: db_only
    max_db_connections: 2
    estimated_duration_seconds: 600
    retry:
      max_attempts: 2
      delay_seconds: 600
      backoff_multiplier: 2
    timeout_seconds: 1800
    data_freshness:
      table: unit_category_risk_baseline
      date_column: computed_at
```

- [ ] **Step 3: Smoke test orchestrator wiring**

```bash
cd backend/python && python3 -c "
from datalayer.unit_category_risk import run
print(run.__doc__)
print('module ok')"
```

Expected: prints docstring and `module ok` (no DB call).

- [ ] **Step 4: Commit**

```bash
git add backend/python/datalayer/unit_category_risk.py backend/python/config/pipelines.yaml
git commit -m "feat(risk): pipeline orchestrator + scheduler registration"
```

---

## Task 10: Auth scopes + admin permission decorator

**Files:**
- Modify: `backend/python/web/auth/jwt_auth.py`
- Modify: `backend/python/web/auth/decorators.py`

- [ ] **Step 1: Add scopes to JWT scope list**

In `jwt_auth.py`, find the existing list of allowed scopes (search for `'visits_write'` or `'crm_read'`). Add `'risk_read'` and `'risk_admin'` to that list. If scopes are validated against a constant set, add to that set; if validated against role-permission mappings, add `risk_read` to a sensible read role and `risk_admin` to admin.

- [ ] **Step 2: Add admin decorator**

Append to `backend/python/web/auth/decorators.py`:

```python
def risk_admin_access_required(f):
    """Permission gate for /admin/risk-factors and write APIs."""
    from functools import wraps
    from flask import abort
    from flask_login import current_user

    @wraps(f)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated:
            abort(401)
        if not (current_user.is_admin or current_user.has_permission('risk_admin')):
            abort(403)
        return f(*args, **kwargs)
    return wrapper
```

> Match the call shape of existing decorators in this file (e.g. `billing_tools_access_required`). If `has_permission` is named differently in `User` (e.g. `can('risk_admin')`), use that.

- [ ] **Step 3: Quick import test**

```bash
cd backend/python && python3 -c "
from web.auth.decorators import risk_admin_access_required
print('ok')"
```

- [ ] **Step 4: Commit**

```bash
git add backend/python/web/auth/jwt_auth.py backend/python/web/auth/decorators.py
git commit -m "feat(risk): auth scopes + admin decorator"
```

---

## Task 11: API blueprint — read endpoints (TDD)

**Files:**
- Create: `backend/python/web/routes/risk.py`
- Create: `tests/web/routes/test_risk_api.py`
- Modify: app factory to register the blueprint

- [ ] **Step 1: Write failing tests**

```python
# tests/web/routes/test_risk_api.py
"""Tests for /api/risk/* endpoints. Uses Flask test client + sqlite fixture."""
import datetime as dt
import pytest
from flask import Flask
from sqlalchemy import text

# Reuse the in-memory fixture from the pipeline tests by monkeypatching
# get_pbi_session(). Adjust import path as your project structure dictates.


@pytest.fixture
def app(monkeypatch, populated_pbi_session):
    from web.routes.risk import bp as risk_bp
    app = Flask(__name__)
    app.config["TESTING"] = True
    monkeypatch.setattr("web.routes.risk._pbi_session",
                        lambda: populated_pbi_session)
    monkeypatch.setattr("web.routes.risk._require_scope",
                        lambda scope: (lambda f: f))   # bypass auth in tests
    app.register_blueprint(risk_bp)
    return app


@pytest.fixture
def client(app):
    return app.test_client()


def test_get_unit_category_returns_risk(client):
    resp = client.get("/api/risk/unit-category?country=KR&size=S&type=W")
    assert resp.status_code == 200
    data = resp.get_json()["data"]
    assert data["country"] == "KR"
    assert "risk_pct" in data
    assert "composite_factor" in data
    assert "band" in data
    assert data["factors"]["size"]["value"] == "S"


def test_get_unit_category_missing_country_400(client):
    resp = client.get("/api/risk/unit-category?size=S")
    assert resp.status_code == 400
    assert resp.get_json()["error"]


def test_get_factors_dump(client):
    resp = client.get("/api/risk/factors?country=KR")
    assert resp.status_code == 200
    payload = resp.get_json()["data"]
    assert payload["baseline_rate"] >= 0
    assert isinstance(payload["factors"], list)
```

(`populated_pbi_session` should be a session fixture similar to Task 8's, with the 3 risk tables created and at least one country populated. Put it in `tests/conftest.py` or import from the pipeline test module.)

- [ ] **Step 2: Run, expect failure**

```bash
cd backend/python && pytest ../../tests/web/routes/test_risk_api.py -v
```

- [ ] **Step 3: Implement the read endpoints**

```python
# backend/python/web/routes/risk.py
"""Read + write API for unit-category risk scoring.

GET  /api/risk/unit-category   — single-unit lookup
GET  /api/risk/factors         — full matrix dump for one country
PUT  /api/risk/factors/<id>    — set / clear override
POST /api/risk/recompute       — admin-trigger pipeline run
"""
from __future__ import annotations

import logging
from datetime import datetime
from flask import Blueprint, current_app, jsonify, request
from sqlalchemy import text

from common.config_loader import get_database_url, load_yaml_config
from common.risk_lookup import (
    FactorRow, bucket_band, compute_risk, resolve_effective_factor,
)
from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.rate_limit import rate_limit_api

logger = logging.getLogger(__name__)
bp = Blueprint("risk", __name__, url_prefix="/api/risk")

DIMENSIONS = ("size", "range", "type", "climate", "shape", "pillar")


def _pbi_session():
    """Lazy session — overridden in tests."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    engine = create_engine(get_database_url("pbi"))
    return sessionmaker(bind=engine)()


def _require_scope(scope):
    """Indirection so tests can swap to a no-op."""
    return require_api_scope(scope)


def _bands():
    return load_yaml_config("risk.yaml")["gradient_bands"]


def _load_factor_row(session, country_code: str, dim: str, value: str) -> FactorRow:
    row = session.execute(text("""
        SELECT empirical_factor, override_factor, is_thin_data,
               sample_size, effective_factor
          FROM unit_category_risk_factor
         WHERE country_code=:cc AND dimension=:d AND value=:v
    """), {"cc": country_code, "d": dim, "v": value}).fetchone()
    if not row:
        return FactorRow(value=value, effective=1.0, source="missing",
                         is_thin=True, sample_size=0)
    emp = float(row[0]) if row[0] is not None else None
    ovr = float(row[1]) if row[1] is not None else None
    eff, src = resolve_effective_factor(emp, ovr, bool(row[2]))
    return FactorRow(value=value, effective=eff, source=src,
                     is_thin=bool(row[2]), sample_size=int(row[3]))


def _load_baseline(session, country_code: str):
    row = session.execute(text("""
        SELECT baseline_rate, computed_at FROM unit_category_risk_baseline
         WHERE country_code = :cc
    """), {"cc": country_code}).fetchone()
    if not row:
        return None, None
    return float(row[0]), row[1]


@bp.route("/unit-category", methods=["GET"])
@require_auth
@_require_scope("risk_read")
@rate_limit_api(max_per_minute=120)
def get_unit_category():
    country = (request.args.get("country") or "").strip().upper()
    if not country:
        return jsonify({"error": "country is required"}), 400

    session = _pbi_session()
    try:
        baseline_rate, computed_at = _load_baseline(session, country)
        if baseline_rate is None:
            return jsonify({"error": "country not configured"}), 404

        factors: dict[str, FactorRow] = {}
        for dim in DIMENSIONS:
            value = (request.args.get(dim) or "").strip()
            if not value:
                continue
            factors[dim] = _load_factor_row(session, country, dim, value)

        result = compute_risk(baseline_rate, factors, _bands())
        return jsonify({"status": "success", "data": {
            "country": country,
            "baseline_rate": result.baseline_rate,
            "composite_factor": round(result.composite_factor, 6),
            "risk_pct": round(result.risk_pct, 6),
            "delta_vs_baseline_pct": round(result.delta_vs_baseline_pct, 2),
            "band": result.band["label"],
            "band_color": result.band["color"],
            "factors": {
                dim: {"value": f.value, "factor": round(f.effective, 4),
                      "source": f.source, "is_thin": f.is_thin,
                      "sample_size": f.sample_size}
                for dim, f in factors.items()
            },
            "computed_at": computed_at.isoformat() if computed_at else None,
        }})
    except Exception:
        logger.exception("risk lookup failed")
        return jsonify({"error": "lookup failed"}), 500
    finally:
        session.close()


@bp.route("/factors", methods=["GET"])
@require_auth
@_require_scope("risk_read")
@rate_limit_api(max_per_minute=60)
def get_factors():
    country = (request.args.get("country") or "").strip().upper()
    if not country:
        return jsonify({"error": "country is required"}), 400
    session = _pbi_session()
    try:
        baseline_rate, computed_at = _load_baseline(session, country)
        if baseline_rate is None:
            return jsonify({"error": "country not configured"}), 404
        rows = session.execute(text("""
            SELECT id, dimension, value, sample_size, empirical_factor,
                   override_factor, effective_factor, is_thin_data,
                   override_reason, override_by, override_at
              FROM unit_category_risk_factor
             WHERE country_code = :cc
          ORDER BY dimension, value
        """), {"cc": country}).fetchall()
        return jsonify({"status": "success", "data": {
            "country": country,
            "baseline_rate": baseline_rate,
            "computed_at": computed_at.isoformat() if computed_at else None,
            "factors": [
                {"id": r[0], "dimension": r[1], "value": r[2],
                 "sample_size": r[3],
                 "empirical_factor": float(r[4]) if r[4] is not None else None,
                 "override_factor": float(r[5]) if r[5] is not None else None,
                 "effective_factor": float(r[6]) if r[6] is not None else 1.0,
                 "is_thin_data": bool(r[7]),
                 "override_reason": r[8], "override_by": r[9],
                 "override_at": r[10].isoformat() if r[10] else None}
                for r in rows
            ],
        }})
    except Exception:
        logger.exception("factor dump failed")
        return jsonify({"error": "lookup failed"}), 500
    finally:
        session.close()
```

- [ ] **Step 4: Register the blueprint**

In the Flask app factory (search for existing `app.register_blueprint(...)` calls in `web/__init__.py` or wherever `create_app` lives):

```python
from web.routes.risk import bp as risk_bp
app.register_blueprint(risk_bp)
```

- [ ] **Step 5: Run tests, verify pass**

```bash
cd backend/python && pytest ../../tests/web/routes/test_risk_api.py -v
```

Expected: 3 passed.

- [ ] **Step 6: Commit**

```bash
git add backend/python/web/routes/risk.py tests/web/routes/test_risk_api.py backend/python/web/__init__.py
git commit -m "feat(risk): GET /api/risk/unit-category and /factors"
```

---

## Task 12: API — override + recompute endpoints (TDD)

**Files:**
- Modify: `backend/python/web/routes/risk.py`
- Modify: `tests/web/routes/test_risk_api.py`

- [ ] **Step 1: Append failing tests**

```python
# Append to tests/web/routes/test_risk_api.py
def test_put_override_set(client):
    # Look up a known factor id from /factors
    resp = client.get("/api/risk/factors?country=KR")
    fid = resp.get_json()["data"]["factors"][0]["id"]
    r = client.put(f"/api/risk/factors/{fid}",
                   json={"override_factor": 1.10, "reason": "manual tune"})
    assert r.status_code == 200
    assert r.get_json()["data"]["override_factor"] == 1.10


def test_put_override_invalid_range_400(client):
    resp = client.get("/api/risk/factors?country=KR")
    fid = resp.get_json()["data"]["factors"][0]["id"]
    r = client.put(f"/api/risk/factors/{fid}",
                   json={"override_factor": 99.0, "reason": "x"})
    assert r.status_code == 400


def test_put_override_clear(client):
    resp = client.get("/api/risk/factors?country=KR")
    fid = resp.get_json()["data"]["factors"][0]["id"]
    r = client.put(f"/api/risk/factors/{fid}",
                   json={"override_factor": None, "reason": "revert"})
    assert r.status_code == 200
    assert r.get_json()["data"]["override_factor"] is None
```

- [ ] **Step 2: Run, expect 404 (route not defined yet)**

- [ ] **Step 3: Append handlers to `risk.py`**

```python
@bp.route("/factors/<int:factor_id>", methods=["PUT"])
@require_auth
@_require_scope("risk_admin")
@rate_limit_api(max_per_minute=20)
def put_factor_override(factor_id: int):
    from web.utils.audit import audit_log, AuditEvent
    from flask_login import current_user

    body = request.get_json(silent=True) or {}
    reason = (body.get("reason") or "").strip()
    if "override_factor" not in body:
        return jsonify({"error": "override_factor is required"}), 400
    raw = body["override_factor"]

    if raw is None:
        new_override = None
    else:
        try:
            new_override = float(raw)
        except (TypeError, ValueError):
            return jsonify({"error": "override_factor must be a number or null"}), 400
        if not (0.1 <= new_override <= 5.0):
            return jsonify({"error": "override_factor must be between 0.1 and 5.0"}), 400
        if not reason:
            return jsonify({"error": "reason is required when setting override"}), 400

    session = _pbi_session()
    try:
        existing = session.execute(text("""
            SELECT country_code, dimension, value, empirical_factor,
                   is_thin_data, override_factor
              FROM unit_category_risk_factor WHERE id = :id
        """), {"id": factor_id}).fetchone()
        if not existing:
            return jsonify({"error": "factor not found"}), 404
        emp = float(existing[3]) if existing[3] is not None else None
        is_thin = bool(existing[4])
        eff, _ = resolve_effective_factor(emp, new_override, is_thin)

        username = getattr(current_user, "username", None) or "system"
        session.execute(text("""
            UPDATE unit_category_risk_factor
               SET override_factor = :ovr,
                   override_reason = :rsn,
                   override_by = :usr,
                   override_at = CURRENT_TIMESTAMP,
                   effective_factor = :eff
             WHERE id = :id
        """), {"ovr": new_override, "rsn": reason if new_override is not None else None,
               "usr": username if new_override is not None else None,
               "eff": eff, "id": factor_id})
        session.commit()
        audit_log(
            AuditEvent.RISK_OVERRIDE_SET if new_override is not None
            else AuditEvent.RISK_OVERRIDE_CLEARED,
            factor_id=factor_id, country=existing[0],
            dimension=existing[1], value=existing[2],
            old=float(existing[5]) if existing[5] is not None else None,
            new=new_override, reason=reason,
        )
        return jsonify({"status": "success", "data": {
            "id": factor_id, "override_factor": new_override,
            "effective_factor": eff,
        }})
    except Exception:
        logger.exception("override update failed")
        return jsonify({"error": "update failed"}), 500
    finally:
        session.close()


@bp.route("/recompute", methods=["POST"])
@require_auth
@_require_scope("risk_admin")
@rate_limit_api(max_per_minute=4)
def post_recompute():
    body = request.get_json(silent=True) or {}
    country = body.get("country")
    if country and not isinstance(country, str):
        return jsonify({"error": "country must be a string"}), 400
    try:
        from datalayer.unit_category_risk import run as run_pipeline
        summary = run_pipeline(country_code=country.upper() if country else None)
        return jsonify({"status": "success", "data": summary})
    except Exception:
        logger.exception("recompute failed")
        return jsonify({"error": "recompute failed"}), 500
```

- [ ] **Step 4: Run tests, verify pass**

```bash
cd backend/python && pytest ../../tests/web/routes/test_risk_api.py -v
```

Expected: all 6 pass.

- [ ] **Step 5: Commit**

```bash
git add backend/python/web/routes/risk.py tests/web/routes/test_risk_api.py
git commit -m "feat(risk): PUT override + POST recompute"
```

---

## Task 13: Admin UI route + permission

**Files:**
- Create: `backend/python/web/routes/admin_risk.py`
- Modify: app factory (register blueprint)

- [ ] **Step 1: Create the route module**

```python
# backend/python/web/routes/admin_risk.py
from flask import Blueprint, render_template
from flask_login import login_required

from web.auth.decorators import risk_admin_access_required
from common.config_loader import load_yaml_config

bp = Blueprint("admin_risk", __name__, url_prefix="/admin")


@bp.route("/risk-factors", methods=["GET"])
@login_required
@risk_admin_access_required
def risk_factors_page():
    cfg = load_yaml_config("risk.yaml")
    return render_template(
        "admin/risk_factors.html",
        countries=cfg["countries"],
        bands=cfg["gradient_bands"],
    )
```

- [ ] **Step 2: Register the blueprint** (same app factory file as Task 11)

```python
from web.routes.admin_risk import bp as admin_risk_bp
app.register_blueprint(admin_risk_bp)
```

- [ ] **Step 3: Smoke-test route registration**

Run the dev server, log in as admin, hit `http://localhost:5000/admin/risk-factors`. Expect a 500 referencing the missing template (template comes in Task 14). That confirms routing + auth work.

- [ ] **Step 4: Commit**

```bash
git add backend/python/web/routes/admin_risk.py backend/python/web/__init__.py
git commit -m "feat(risk): admin UI route gated by risk_admin_access"
```

---

## Task 14: Admin UI template — baseline + factor matrix

**Files:**
- Create: `backend/python/web/templates/admin/risk_factors.html`

- [ ] **Step 1: Write the template**

```html
{# backend/python/web/templates/admin/risk_factors.html #}
{% extends "base.html" %}
{% block title %}Risk Factors{% endblock %}

{% block content %}
<div class="container my-4">
  <h1>Unit-Category Risk Factors</h1>

  <ul class="nav nav-tabs" id="countryTabs">
    {% for code, name in countries.items() %}
    <li class="nav-item">
      <button class="nav-link {% if loop.first %}active{% endif %}"
              data-country="{{ code }}">{{ code }} — {{ name }}</button>
    </li>
    {% endfor %}
  </ul>

  <div id="baselinePanel" class="card my-3 p-3">
    <div class="d-flex justify-content-between align-items-start">
      <div>
        <h4>Country baseline</h4>
        <p>Move-out rate: <strong id="baselineRate">—</strong></p>
        <p>Window: <span id="baselineWindow">—</span></p>
        <p>Move-outs: <span id="baselineMoveouts">—</span> · Unit-months: <span id="baselineUM">—</span></p>
        <p>Computed: <span id="baselineComputed">—</span></p>
      </div>
      <button id="btnRecompute" class="btn btn-warning">Recompute this country</button>
    </div>
  </div>

  <div id="factorMatrix"></div>
</div>

<script>
const BANDS = {{ bands | tojson }};
const DIMENSIONS = ["size","range","type","climate","shape","pillar"];

function bandFor(composite) {
  for (const b of BANDS) {
    if (b.max_composite === null || composite <= b.max_composite) return b;
  }
  return BANDS[BANDS.length - 1];
}

let currentCountry = "{{ countries.keys()|list|first }}";

async function loadCountry(code) {
  currentCountry = code;
  const r = await fetch(`/api/risk/factors?country=${code}`);
  if (!r.ok) { alert("Load failed"); return; }
  const { data } = await r.json();
  document.getElementById("baselineRate").textContent =
    (data.baseline_rate * 100).toFixed(2) + "%";
  document.getElementById("baselineComputed").textContent = data.computed_at || "—";
  // moveouts/window/UM aren't in this endpoint yet — stays as "—" until we add
  // them or pull from a separate /baseline call (left as future polish).
  renderMatrix(data.factors);
}

function renderMatrix(factors) {
  const root = document.getElementById("factorMatrix");
  root.innerHTML = "";
  for (const dim of DIMENSIONS) {
    const rows = factors.filter(f => f.dimension === dim);
    if (!rows.length) continue;
    const card = document.createElement("div");
    card.className = "card my-2";
    card.innerHTML = `
      <div class="card-header"><strong>${dim.toUpperCase()}</strong></div>
      <table class="table table-sm mb-0">
        <thead><tr>
          <th>Value</th><th>Sample</th><th>Empirical</th>
          <th>Override</th><th>Effective</th><th>Thin?</th><th></th>
        </tr></thead>
        <tbody>${rows.map(rowHtml).join("")}</tbody>
      </table>`;
    root.appendChild(card);
  }
}

function rowHtml(f) {
  const band = bandFor(f.effective_factor);
  const bg = `style="background:${band.color}22"`;
  const emp = f.empirical_factor !== null ? f.empirical_factor.toFixed(3) : "—";
  const ovr = f.override_factor !== null ? f.override_factor.toFixed(3) : "—";
  return `
    <tr ${bg}>
      <td>${f.value}</td>
      <td>${f.sample_size}</td>
      <td>${emp}</td>
      <td>${ovr}</td>
      <td><strong>${f.effective_factor.toFixed(3)}</strong></td>
      <td>${f.is_thin_data ? "✓" : ""}</td>
      <td><button class="btn btn-sm btn-outline-secondary"
                  onclick="editOverride(${f.id}, ${f.override_factor})">Edit</button></td>
    </tr>`;
}

async function editOverride(id, current) {
  const raw = prompt("Override factor (0.1–5.0), blank to clear:",
                     current ?? "");
  if (raw === null) return;
  const reason = raw.trim() === "" ? "cleared via UI" :
                 prompt("Reason for override:") || "";
  if (raw.trim() !== "" && !reason) { alert("Reason required."); return; }
  const body = raw.trim() === ""
    ? { override_factor: null, reason }
    : { override_factor: parseFloat(raw), reason };
  const r = await fetch(`/api/risk/factors/${id}`, {
    method: "PUT", headers: {"Content-Type": "application/json"},
    body: JSON.stringify(body),
  });
  if (!r.ok) { alert((await r.json()).error || "Save failed"); return; }
  loadCountry(currentCountry);
}

document.getElementById("btnRecompute").addEventListener("click", async () => {
  if (!confirm(`Recompute risk factors for ${currentCountry}?`)) return;
  const r = await fetch("/api/risk/recompute", {
    method: "POST", headers: {"Content-Type":"application/json"},
    body: JSON.stringify({ country: currentCountry }),
  });
  alert(r.ok ? "Recompute complete" : "Recompute failed");
  loadCountry(currentCountry);
});

document.querySelectorAll("#countryTabs .nav-link").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll("#countryTabs .nav-link")
      .forEach(b => b.classList.remove("active"));
    btn.classList.add("active");
    loadCountry(btn.dataset.country);
  });
});

loadCountry(currentCountry);
</script>
{% endblock %}
```

- [ ] **Step 2: Verify the page renders**

Start dev server, visit `/admin/risk-factors`. Expected:
- Country tabs render
- Switching tabs triggers `/api/risk/factors?country=…` calls
- Each dimension card shows its values with bands tinting the rows
- Edit button opens prompt → updates → reloads

- [ ] **Step 3: Commit**

```bash
git add backend/python/web/templates/admin/risk_factors.html
git commit -m "feat(risk): admin UI — baseline + factor matrix"
```

---

## Task 15: Admin UI — inventory preview with risk %

**Files:**
- Modify: `backend/python/web/templates/admin/risk_factors.html`
- Modify: `backend/python/web/routes/admin_risk.py` (add a small JSON endpoint for the inventory list)

For each unit in the selected country, parse its `sTypeName`, look up factors via the same logic as `/api/risk/unit-category`, return risk % + breakdown.

- [ ] **Step 1: Add the inventory endpoint**

In `admin_risk.py`:

```python
from sqlalchemy import text
from common.config_loader import get_database_url, load_yaml_config
from common.risk_lookup import (
    FactorRow, bucket_band, compute_risk, resolve_effective_factor,
)
from common.stype_name_parser import parse_stype_name


@bp.route("/risk-factors/inventory", methods=["GET"])
@login_required
@risk_admin_access_required
def risk_inventory_json():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from flask import request, jsonify

    country_code = (request.args.get("country") or "").strip().upper()
    cfg = load_yaml_config("risk.yaml")
    name = cfg["countries"].get(country_code)
    if not name:
        return jsonify({"error": "country not configured"}), 404

    engine = create_engine(get_database_url("pbi"))
    session = sessionmaker(bind=engine)()
    try:
        baseline_row = session.execute(text("""
            SELECT baseline_rate FROM unit_category_risk_baseline
             WHERE country_code = :cc"""), {"cc": country_code}).fetchone()
        if not baseline_row:
            return jsonify({"error": "baseline missing"}), 404
        baseline_rate = float(baseline_row[0])

        # Factor cache: {(dim, value): (empirical, override, is_thin, sample)}
        factor_rows = session.execute(text("""
            SELECT dimension, value, empirical_factor, override_factor,
                   is_thin_data, sample_size
              FROM unit_category_risk_factor
             WHERE country_code = :cc"""), {"cc": country_code}).fetchall()
        cache = {}
        for r in factor_rows:
            emp = float(r[2]) if r[2] is not None else None
            ovr = float(r[3]) if r[3] is not None else None
            eff, src = resolve_effective_factor(emp, ovr, bool(r[4]))
            cache[(r[0], r[1])] = FactorRow(value=r[1], effective=eff,
                                            source=src, is_thin=bool(r[4]),
                                            sample_size=int(r[5]))

        units = session.execute(text("""
            SELECT DISTINCT r.SiteID, r.UnitID, r.sUnit, r.sTypeName, s.SiteCode
              FROM rentroll r
              JOIN siteinfo s ON s.SiteID = r.SiteID
             WHERE s.Country = :country
               AND r.extract_date = (SELECT MAX(extract_date) FROM rentroll
                                      WHERE SiteID = r.SiteID)
          ORDER BY s.SiteCode, r.sUnit
             LIMIT 1000
        """), {"country": name}).fetchall()

        bands = cfg["gradient_bands"]
        out = []
        for site_id, unit_id, s_unit, s_type, site_code in units:
            parsed = parse_stype_name(s_type or "")
            if not parsed.parse_ok:
                out.append({"site_code": site_code, "unit": s_unit,
                            "s_type": s_type, "parsed_ok": False})
                continue
            dims = {
                "size": parsed.size, "range": parsed.size_range,
                "type": parsed.unit_type, "climate": parsed.climate,
                "shape": parsed.shape, "pillar": parsed.pillar,
            }
            factors = {}
            for dim, val in dims.items():
                if not val:
                    continue
                factors[dim] = cache.get((dim, val), FactorRow(
                    value=val, effective=1.0, source="missing",
                    is_thin=True, sample_size=0))
            result = compute_risk(baseline_rate, factors, bands)
            out.append({
                "site_code": site_code, "unit": s_unit, "s_type": s_type,
                "parsed_ok": True,
                "dims": dims,
                "risk_pct": round(result.risk_pct * 100, 3),
                "composite": round(result.composite_factor, 3),
                "delta_pct": round(result.delta_vs_baseline_pct, 1),
                "band": result.band["label"],
                "color": result.band["color"],
                "factor_breakdown": [
                    {"dim": d, "value": f.value, "factor": round(f.effective, 3),
                     "is_thin": f.is_thin}
                    for d, f in factors.items()
                ],
            })
        return jsonify({"status": "success", "data": {
            "baseline_rate_pct": round(baseline_rate * 100, 3),
            "units": out,
        }})
    finally:
        session.close()
```

- [ ] **Step 2: Append inventory section to the template**

Just before `{% endblock %}`, add:

```html
<div class="card my-3">
  <div class="card-header">Inventory preview (max 1000 units)</div>
  <div class="card-body">
    <input type="text" id="invFilter" class="form-control mb-2"
           placeholder="Filter by site / unit / sType…">
    <table class="table table-sm">
      <thead><tr>
        <th>Site</th><th>Unit</th><th>sTypeName</th>
        <th>Risk %</th><th>vs baseline</th><th>Band</th><th>Breakdown</th>
      </tr></thead>
      <tbody id="invBody"></tbody>
    </table>
  </div>
</div>
```

And append to the `<script>` block:

```javascript
async function loadInventory(code) {
  const r = await fetch(`/admin/risk-factors/inventory?country=${code}`);
  if (!r.ok) return;
  const { data } = await r.json();
  const body = document.getElementById("invBody");
  body.innerHTML = data.units.map(u => {
    if (!u.parsed_ok) {
      return `<tr class="text-muted">
        <td>${u.site_code}</td><td>${u.unit}</td><td>${u.s_type}</td>
        <td colspan="4">unparseable</td></tr>`;
    }
    const breakdown = u.factor_breakdown
      .map(f => `${f.dim}=${f.value} (${f.factor})`).join(" × ");
    return `<tr style="background:${u.color}22">
      <td>${u.site_code}</td><td>${u.unit}</td><td>${u.s_type}</td>
      <td><strong>${u.risk_pct.toFixed(2)}%</strong></td>
      <td>${u.delta_pct > 0 ? "+" : ""}${u.delta_pct}%</td>
      <td>${u.band}</td>
      <td><small>${breakdown}</small></td></tr>`;
  }).join("");
}

// Hook into the existing loadCountry to also pull inventory
const _origLoad = loadCountry;
loadCountry = async (code) => { await _origLoad(code); await loadInventory(code); };

document.getElementById("invFilter").addEventListener("input", (e) => {
  const q = e.target.value.toLowerCase();
  document.querySelectorAll("#invBody tr").forEach(tr => {
    tr.style.display = tr.textContent.toLowerCase().includes(q) ? "" : "none";
  });
});

loadCountry(currentCountry);
```

- [ ] **Step 3: Manual smoke test**

Visit `/admin/risk-factors` after running the pipeline once for at least one country. Confirm:
- Each unit row tinted by band
- Risk % shown alongside delta vs baseline
- Filter input narrows the list
- Unparseable units render as muted "unparseable"

- [ ] **Step 4: Commit**

```bash
git add backend/python/web/routes/admin_risk.py backend/python/web/templates/admin/risk_factors.html
git commit -m "feat(risk): admin UI — inventory preview with band tinting"
```

---

## Task 16: First production run + sanity check

**Files:**
- (none)

- [ ] **Step 1: Trigger an initial recompute via the API on dev**

```bash
curl -X POST http://localhost:5000/api/risk/recompute \
     -H "Authorization: Bearer $JWT_ADMIN" \
     -H "Content-Type: application/json" \
     -d '{"all": true}'
```

Expected: `{"status":"success","data":{"countries":[…],"errors":[]}}`.

- [ ] **Step 2: Spot-check one country in psql**

```bash
PGPASSWORD="$PBI_DB_PW" psql -h esapbi.postgres.database.azure.com -U esa_pbi_admin -d esa_pbi -c "
SELECT country_code, baseline_rate, moveout_count, unit_months_occupied
  FROM unit_category_risk_baseline ORDER BY country_code;"

PGPASSWORD="$PBI_DB_PW" psql -h esapbi.postgres.database.azure.com -U esa_pbi_admin -d esa_pbi -c "
SELECT dimension, value, sample_size, empirical_factor, effective_factor
  FROM unit_category_risk_factor
 WHERE country_code='KR' ORDER BY dimension, value LIMIT 30;"
```

Sanity rules to eyeball:
- Baseline rates between 0.005 and 0.05 (0.5%–5% monthly)
- Empirical factors mostly in 0.5–2.0 range
- Cells with `sample_size < 30` should have `effective_factor = 1.0` (unless override)

- [ ] **Step 3: Eyeball the admin UI**

Load `/admin/risk-factors`, click each country tab, confirm:
- Baseline panel populated
- Each dimension card shows its values
- Inventory table loads, bands look reasonable

- [ ] **Step 4: Commit no code; record findings if anything weird**

If the sanity check surfaces oddities (e.g. one dimension entirely thin), note them in `docs/superpowers/specs/2026-05-04-unit-category-risk-design.md` "Open Questions" section and decide whether they need a follow-up plan.

---

## Self-review notes

- All spec sections (§3–§12) have implementing tasks.
- All TDD tasks have test code shown.
- No "TBD"/"TODO" placeholders.
- Type names consistent across tasks (`FactorRow`, `RiskResult`, `CellFactor`, `BaselineResult`).
- One spec deviation: `siteinfo.Country` is a full string, so `risk.yaml.countries` provides the code↔name mapping. Documented in Task 3.
- Pipeline test uses SQLite in-memory; Postgres-only `ON CONFLICT` syntax works on SQLite ≥3.24, but if your CI runs older SQLite the upsert tests need an integration-test path against a real Postgres (acceptable trade-off — flagged here, not blocking).
