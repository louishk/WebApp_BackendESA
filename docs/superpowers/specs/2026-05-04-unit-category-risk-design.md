# Unit-Category Risk Scoring — Design

**Date:** 2026-05-04
**Author:** brainstorm session (Louis + Claude)
**Status:** approved, awaiting implementation plan

## 1. Purpose

Produce a per-unit "move-out risk %" derived from historical churn behaviour broken down by unit-category naming-convention parameters (the six dimensions defined by SOP COM01 Jan 2026). The score feeds **pricing/discount decisions** — riskier categories get less aggressive discounting.

## 2. Scope

In scope:
- Country-level model (KR, SG, JP, MY, HK)
- All 6 SOP COM01 dimensions: Size, Range, Type, Climate, Shape, Pillar
- Single risk metric: **move-out rate** (2-year rolling window)
- Hybrid factor source: empirical + admin override
- Multiplicative aggregation: `risk = baseline × Π factor[dim_i]`
- Monthly auto-recompute + on-demand recompute
- Read API + admin UI; consumption by pricing logic is out of scope of this spec (separate work)

Out of scope (future):
- Site-level overrides
- Multi-metric composite (occupancy, days-vacant, discount-take-rate)
- Bayesian shrinkage for thin cells
- Time-of-year / seasonal factors

## 3. Architecture

```
[rentroll + moveinmoveout + siteinfo in esa_pbi]
        │
        ▼  (monthly + on-demand)
   datalayer/unit_category_risk.py
        │
        ├──► unit_category_risk_baseline      (country-level baseline)
        ├──► unit_category_risk_factor        (country × dimension × value)
        └──► unit_category_risk_history       (monthly snapshots)
        │
        ▼  (request time)
   risk_pct = baseline × Π effective_factor[dim_i, value_i]
        │
        ├──► GET /api/risk/unit-category    (single-unit lookup)
        ├──► GET /api/risk/factors          (matrix dump)
        ├──► PUT /api/risk/factors/<id>     (set override)
        ├──► POST /api/risk/recompute       (admin-trigger)
        └──► /admin/risk-factors            (admin UI)
```

## 4. Data Model (new tables in `esa_pbi`)

### 4.1 `unit_category_risk_baseline`
| Column | Type | Notes |
|---|---|---|
| country_code | VARCHAR(2) | PK — 'KR','SG','JP','MY','HK' |
| window_start | DATE | inclusive |
| window_end | DATE | inclusive |
| moveout_count | INTEGER | total move-outs in window |
| unit_months_occupied | NUMERIC(14,2) | denominator |
| baseline_rate | NUMERIC(8,6) | monthly move-out rate |
| computed_at | TIMESTAMPTZ | |

### 4.2 `unit_category_risk_factor`
| Column | Type | Notes |
|---|---|---|
| id | SERIAL | PK |
| country_code | VARCHAR(2) | |
| dimension | VARCHAR(16) | one of: size, range, type, climate, shape, pillar |
| value | VARCHAR(16) | dimension value (e.g. 'S', '8-10', 'W', 'A', 'SS', 'NP') |
| sample_size | INTEGER | move-outs in this cell |
| unit_months_occupied | NUMERIC(14,2) | denominator for cell |
| empirical_factor | NUMERIC(8,6) | cell_rate / baseline_rate (NULL if no occupancy data) |
| override_factor | NUMERIC(8,6) | admin-set, nullable |
| effective_factor | NUMERIC(8,6) | written by job, see §5.4 |
| is_thin_data | BOOLEAN | sample_size < threshold |
| override_reason | TEXT | nullable |
| override_by | VARCHAR(64) | username |
| override_at | TIMESTAMPTZ | |
| computed_at | TIMESTAMPTZ | |
| UNIQUE | (country_code, dimension, value) | |

### 4.3 `unit_category_risk_history`
| Column | Type | Notes |
|---|---|---|
| id | SERIAL | PK |
| snapshot_month | DATE | first-of-month |
| country_code | VARCHAR(2) | |
| dimension | VARCHAR(16) | |
| value | VARCHAR(16) | |
| empirical_factor | NUMERIC(8,6) | |
| sample_size | INTEGER | |
| baseline_rate | NUMERIC(8,6) | denormalised for trending |
| UNIQUE | (country_code, dimension, value, snapshot_month) | required for §5.6 ON CONFLICT |

## 5. Computation Pipeline

**File:** `backend/python/datalayer/unit_category_risk.py`
**Schedule:** monthly, 1st @ 03:00 local, registered in `config/pipelines.yaml`
**On-demand:** `POST /api/risk/recompute` (admin-only)
**Lock:** advisory lock on `unit_category_risk_baseline` to prevent concurrent runs

### 5.1 Window
- `window_end = today`
- `window_start = today - INTERVAL '2 years'`

### 5.2 Inputs (esa_pbi)
- `rentroll` — daily occupancy snapshots; `bRented` is occupancy flag
- `moveinmoveout` — move-out events with `dMoveOut`, joined to `rentroll` for unit attributes (use latest pre-moveout snapshot for `sTypeName`)
- `siteinfo` — `country_code` per `SiteID`
- Parser: `common.stype_name_parser.parse_stype_name(sTypeName)` → 6 dim values; fallback to `inventory_type_mappings` for legacy names

### 5.3 Country baseline
```
unit_months_occupied(country, window) =
    COUNT(rentroll rows
          WHERE bRented = TRUE
            AND extract_date BETWEEN window_start AND window_end
            AND siteinfo.country_code = :country)
    / 30.4375
moveout_count(country, window) =
    COUNT(moveinmoveout rows
          WHERE dMoveOut BETWEEN window_start AND window_end
            AND siteinfo.country_code = :country)
baseline_rate = moveout_count / unit_months_occupied
```

### 5.4 Per-cell factors
For each `(country, dimension, value)` cell present in inventory:
```
cell_unit_months  = same as baseline but filtered on dim=value
cell_moveout_count = same as baseline but filtered on dim=value
cell_rate         = cell_moveout_count / NULLIF(cell_unit_months, 0)
empirical_factor  = cell_rate / baseline_rate
sample_size       = cell_moveout_count
is_thin_data      = sample_size < threshold   -- default 30
```
**Effective factor** (computed in the job, persisted):
```
if   override_factor IS NOT NULL  → effective = override_factor
elif is_thin_data OR empirical_factor IS NULL → effective = 1.0
else → effective = empirical_factor
```

### 5.5 Override preservation
Recompute UPDATEs `empirical_factor`, `sample_size`, `unit_months_occupied`, `is_thin_data`, `effective_factor`, `computed_at`. It does NOT touch `override_factor`, `override_reason`, `override_by`, `override_at`.

### 5.6 History snapshot
At end of run, INSERT one row per cell into `unit_category_risk_history` with `snapshot_month = date_trunc('month', today)`. ON CONFLICT (country, dimension, value, snapshot_month) DO UPDATE — re-runs in the same month overwrite, not duplicate.

### 5.7 Edge cases
| Case | Handling |
|---|---|
| Country with <100 moveouts in window | Log WARNING; write baseline; all cells likely thin → factors default 1.0 |
| Unparseable `sTypeName` | Excluded from numerator and denominator; count surfaced in admin UI |
| Dimension value in inventory but zero moveouts | Row with `empirical_factor=NULL`, `is_thin_data=TRUE`, `effective=1.0` |
| Dimension value with zero unit-months (no inventory) | Row not created |
| New country added | Picked up automatically on next run |

### 5.8 Audit
`audit_log(AuditEvent.RISK_RECOMPUTE, country=..., factors_changed=N, baseline_delta=...)` per country.

## 6. Configuration — `backend/python/config/risk.yaml` (new)
```yaml
window_years: 2
sample_size_threshold: 30          # below = thin data, factor pinned to 1.0
recompute_cron: "0 3 1 * *"        # monthly, 1st @ 03:00
gradient_bands:
  - {max: 0.70, label: very_low,  color: "#1b5e20"}
  - {max: 0.90, label: low,       color: "#2e7d32"}
  - {max: 1.10, label: average,   color: "#9e9e9e"}
  - {max: 1.30, label: high,      color: "#f57c00"}
  - {max: null, label: very_high, color: "#c62828"}
countries: [KR, SG, JP, MY, HK]
```

## 7. Risk Lookup Logic
Computed at request time (no per-unit table):
```
def lookup(country, dims: dict[str,str]) -> RiskResult:
    baseline = get_baseline(country)
    factors = {}
    composite = 1.0
    for dim, value in dims.items():
        f = get_factor(country, dim, value)        # effective_factor or 1.0 if missing
        factors[dim] = f
        composite *= f.effective
    risk_pct = baseline.rate * composite
    band = bucket(composite, gradient_bands)
    return RiskResult(risk_pct, baseline.rate, composite, factors, band)
```
Missing dimension in input → factor 1.0 (don't penalise).

## 8. API

### 8.1 `GET /api/risk/unit-category`
- Auth: `@require_auth` + `@require_api_scope('risk_read')`
- Query: `country` (required), any of `size`, `range`, `type`, `climate`, `shape`, `pillar`
- 200:
  ```json
  {"status":"success","data":{
    "country":"KR",
    "baseline_rate":0.0250,
    "composite_factor":0.736,
    "risk_pct":0.0184,
    "delta_vs_baseline_pct":-26.4,
    "band":"low",
    "band_color":"#2e7d32",
    "factors":{
      "size":{"value":"S","factor":0.80,"is_thin":false,"sample_size":412,"source":"empirical"},
      "type":{"value":"W","factor":0.92,"is_thin":false,"sample_size":511,"source":"empirical"}
    },
    "computed_at":"2026-05-01T03:00:00Z"
  }}
  ```
- 400: missing `country` → generic error message
- 404: country not found → generic error message

### 8.2 `GET /api/risk/factors?country=KR`
- Auth: `risk_read` scope
- Returns baseline + full list of factor rows for the country

### 8.3 `PUT /api/risk/factors/<id>`
- Auth: `risk_admin` scope; rate-limited
- Body: `{"override_factor": 1.15, "reason": "..."}` or `{"override_factor": null}`
- Validates `0.1 <= override_factor <= 5.0`
- Audit-logged (`AuditEvent.RISK_OVERRIDE_SET` / `RISK_OVERRIDE_CLEARED`)

### 8.4 `POST /api/risk/recompute`
- Auth: `risk_admin` scope
- Body: `{"country":"KR"}` or `{"all": true}`
- Returns `{job_id, started_at}`; runs in background; result observable via `GET /api/risk/factors`
- Audit-logged

## 9. Admin UI — `/admin/risk-factors`

Permission: `risk_admin_access_required` decorator (new).
Template: `web/templates/admin/risk_factors.html`
Route: `web/routes/admin.py` (or new `admin_risk.py`)

**Layout:**
1. **Country tabs** — KR / SG / JP / MY / HK
2. **Baseline panel** — rate, window range, moveout count, unit-months, computed_at, [Recompute] button
3. **Factor matrix** — one collapsible card per dimension. Inside each card a table:
   `Value | Sample | Empirical | Override | Effective | Thin? | Actions`
   - "Effective" cell tinted by gradient band
   - [Edit override] modal: numeric input + reason
   - [Clear override] action
4. **Inventory preview** — paginated table of units in selected country:
   `Site | Unit | sTypeName | Parsed dims | Risk % | Δ vs baseline | Band`
   - Risk cell tinted by band
   - Click row → factor breakdown popover (`base 2.50% × 0.80 × 0.92 × 1.05 × 1.00 × 1.00 × 1.10 = 1.84%`)
5. **Unparseable inventory counter** — count + "view list" link (rows that failed parser, blocked from contributing)

## 10. Risk Gradient

Bands defined in `risk.yaml` (§6). Applied to `composite_factor` (country-relative) so the same band thresholds work across countries.

UI surfaces:
- Per-cell tint in factor matrix
- Per-unit tint in inventory preview
- Comparative phrasing: `Risk: 1.84% — 26% below KR average (2.50%)`

API surfaces band + color hex so consuming clients render consistently.

## 11. Security

- All write endpoints behind `risk_admin` scope and rate-limited
- Override reason required (non-empty) on set
- Validate override range to prevent absurd factors poisoning pricing
- Audit log every override change with old/new values
- No `str(e)` in responses — generic messages, real error to logger
- All SQL via SQLAlchemy ORM or parameterised text

## 12. Testing

- Unit: factor math (baseline, empirical, override, thin-data, effective resolution)
- Unit: gradient bucketing edges (0.70, 0.90, 1.10, 1.30)
- Unit: stype_name_parser fallback path with mapping tables
- Integration: full pipeline against fixture rentroll + moveinmoveout for one country
- Integration: override preserved across recompute
- API: each endpoint, auth/scope failures, validation
- UI: smoke test via existing tool-page test pattern

## 13. Open Questions / Future Work

- Multi-metric composite (occupancy, days-vacant) — design supports adding metric columns later
- Site-level override layer (`risk_factor_site_override` table)
- Pricing/discount integration — separate spec; this spec only produces the score
- Exposing risk in inventory checker tool — easy win once API is live
