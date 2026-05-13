# ECRI Pre-Load Mode — Standard Parity

**Date:** 2026-05-13
**Status:** Approved (design)
**Files:** `backend/python/web/routes/ecri.py`, `backend/python/web/templates/ecri/eligibility.html`

## Problem

Pre-Load mode on `/ecri/eligibility` shows only basic columns (Site, Unit, Tenant, Current Rent, Moved In, Disc Expires, Paid Thru, Projected PT, Tenure). With 3,600+ tenants across segments and no filtering / no benchmark context, selecting the right ledgers for a Pre-Load batch is impractical. Standard mode already has the missing capabilities — Pre-Load should reach feature parity.

## Goal

Pre-Load mode reaches column + filter parity with Standard for both segments (Recent Move-ins, Heavy Prepayers), keeping its segment-specific columns intact.

## Backend — `ecri.py`

### Extract shared benchmark helper
Both `api_eligible_tenants` and `api_advance_eligible` need the same country/Top-N benchmark math. Extract a module-level helper:

```python
def _country_size_climate_benchmarks(session, site_ids) -> dict[tuple[str, str], dict]:
    """Return {(size_range, climate_code): {country_rent_med, country_psf_med,
    top1_rent, top3_rent, top1_psf, top3_psf, n_sites}} for the country
    most represented in site_ids."""
```

The body is the existing CTE block from `api_eligible_tenants` (country lookup + benchmark SQL). `api_eligible_tenants` is refactored to call it.

Also extract `_variance_pct(actual, ref)` (currently a nested function in `api_eligible_tenants`) so both endpoints share it.

### Extend `api_advance_eligible`

1. **SQL** — JOIN `units_info_enriched u ON u."UnitID" = "UnitID"` in the row query to pull `dcarea_fixed AS sqft`, `label_size_range AS sz`, `label_climate_code AS cc`.
2. **Site medians** — extend the existing rent-median map to also compute a `$/sqft` median per `(site_id, unit_type)` (mirrors `site_type_psf_medians` in Standard).
3. **Country benchmarks** — call `_country_size_climate_benchmarks(session, site_ids)`.
4. **Per-row enrichment** — for each ledger, compute and emit:
   - `sqft`, `size_range`, `climate_code`, `current_psf`
   - `in_place_psf_site`, `variance_psf_vs_site`
   - `market_psf`, `variance_psf_vs_market`
   - `country_rent_med`, `country_psf_med`, `top3_rent`, `top3_psf`, `top1_rent`, `top1_psf`
   - `variance_vs_country`, `variance_psf_vs_country`, `variance_vs_top3`, `variance_psf_vs_top3`, `variance_vs_top1`, `variance_psf_vs_top1`

The response shape is otherwise unchanged (segments dict + counts + parameters).

## Frontend — `eligibility.html`

### Filter panel — lift to shared location
Move the existing `#filter-panel` (variance filters with AND/OR conditions) out of `#standard-results` to a new shared block placed above the results panels, visible in both modes. State lives at script-level (already does).

`applyFilters()` becomes mode-aware:
- **Standard:** unchanged — filters `eligibleData`, re-renders flat table.
- **Pre-Load:** filters `advData.recent_movein` and `advData.heavy_prepayer` independently using the same condition list; re-renders both segment tables; filter-result count shows `X+Y of M+N shown` (segment totals).

`clearFilters()` clears and re-renders whatever mode is active.

### Pre-Load table columns
Both `#tbody-movein` and `#tbody-prepayer` adopt these columns after the existing identity columns (Site/Unit/Tenant):

| Column | Source |
|---|---|
| Current Rent ($/sf) | `benchCell(current_rent, null, current_psf, null)` |
| Site Median | `benchCell(in_place_median_site, variance_vs_site, in_place_psf_site, variance_psf_vs_site)` |
| Market Rate | `benchCell(market_rate, variance_vs_market, market_psf, variance_psf_vs_market)` |
| Country Med | `benchCell(country_rent_med, variance_vs_country, country_psf_med, variance_psf_vs_country)` |
| Top-3 | `benchCell(top3_rent, variance_vs_top3, top3_psf, variance_psf_vs_top3)` |
| Top-1 | `benchCell(top1_rent, variance_vs_top1, top1_psf, variance_psf_vs_top1)` |

Pre-Load-specific cols (Moved In, [Disc Expires], Paid Thru, Projected PT, Tenure) retained at the end so the segment-specific context stays visible. Existing horizontal scroll on the segment `<div>` handles the extra width.

## Out of Scope

- No DB migration / view change — all required fields already exist in `units_info_enriched`.
- Risk-score columns kept as `—` placeholder, same as Standard.
- Append-to-draft flow in Pre-Load mode (Standard-only today) — not changed.

## Risks

- **Performance:** advance-eligible now runs the same country benchmark CTE that Standard runs (one extra query per request). Standard already does it; cost is bounded.
- **Wide tables:** Pre-Load tables grow ~6 columns. Existing `overflow:auto` on the wrapping `<div>` handles horizontal scroll.

## Testing

- Manual: load `/ecri/eligibility`, switch to Pre-Load, pick a multi-site selection, confirm benchmark columns populated and variance % colored.
- Manual: apply a filter (e.g. `variance_vs_market < -10`) in Pre-Load mode, confirm both segment tables filter and result-count badge updates.
- Manual: create a Pre-Load batch from a filtered selection, confirm `/ecri/api/advance-batch` succeeds and the batch lands in draft with the expected ledgers.
