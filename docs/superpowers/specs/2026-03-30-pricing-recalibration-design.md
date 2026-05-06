# Pricing Recalibration Script — Design Spec

## Purpose

Generate a CSV that shows every occupied self-storage unit's current pricing vs a recalibrated target, where the target is derived from:
- Site-level budget ARR/sqft
- A -log yield curve (smaller units -> higher $/sqft)
- A climate multiplier matrix (ascending: NC < D < A < AD < RF)
- A uniform 30% discount assumption

The CSV enables pricing review and feeds into future ECRI batch creation.

## Scope

- **Sites**: All sites with budget data in `vw_budget_monthly` (SG, MY, KR)
- **Units**: Occupied, self-storage only (exclude unit_type_code P, ST, SC)
- **Labels**: Only units published in `unit_category_labels` (units without labels are excluded — they have no climate_code or size_range to price against)
- **Snapshot**: Uses the latest `extract_date` available in `rentroll_enriched`
- **Output**: Single CSV file, one row per tenant-unit (multi-tenant units produce multiple rows)

## Configurable Parameters

```python
# Current market discounts (for "book rate" = std x (1 - disc%))
CURRENT_DISCOUNT = {
    'Singapore': 0.35,
    'Malaysia': 0.35,
    'South Korea': 0.50,
}

# Future uniform discount (new_std x (1 - this) = target ARR)
FUTURE_DISCOUNT = 0.30

# Climate multipliers (NC baseline = 1.0, ascending)
CLIMATE_MATRIX = {
    'NC': 1.00,
    'D': 1.15,
    'A': 1.20,
    'AD': 1.25,
    'RF': 1.60,
}
```

All three are defined at the top of the script for easy amendment.

## CSV Output Columns

### Identity
| Column | Source |
|--------|--------|
| site_id | rentroll_enriched.SiteID |
| site_code | siteinfo.SiteCode |
| site_name | siteinfo.Name |
| country | siteinfo.Country |
| currency | siteinfo currency (SGD/MYR/KRW) |
| unit_id | rentroll_enriched.UnitID |
| sUnit | rentroll_enriched.sUnit |

### Unit Attributes
| Column | Source |
|--------|--------|
| area_sqft | rentroll_enriched.dcarea_fixed |
| floor | rentroll_enriched.iFloor |
| sTypeName | rentroll_enriched.sTypeName |

### Final Label (split)
| Column | Source |
|--------|--------|
| final_label | unit_category_labels.final_label |
| size_category | unit_category_labels.size_category |
| size_range | unit_category_labels.size_range |
| unit_type_code | unit_category_labels.unit_type_code |
| climate_code | unit_category_labels.climate_code |
| shape | unit_category_labels.shape |
| pillar | unit_category_labels.pillar |

### Tenant Info
| Column | Source |
|--------|--------|
| ledger_id | rentroll_enriched.LedgerID |
| tenant_name | rentroll_enriched.sTenant |
| moved_in_date | rentroll_enriched.dLeaseDate |
| tenure_months | floor(days_rented / 30.44) |
| los_range | rentroll_enriched.los_range |

### Multi-tenancy
| Column | Derivation |
|--------|------------|
| is_multi_tenant | true if >1 active ledger on same UnitID at same SiteID |
| tenant_count | count of active ledgers on unit |
| tenant_share_pct | 100 / tenant_count (equal split assumption) |

Note: Pricing columns (current_std_rate, new_std_rate, etc.) are **unit-level** rates, repeated per tenant row. Actual rent (revenue_effective) is **tenant-level** (per ledger). Multi-tenant units will show the full unit std rate alongside each tenant's individual rent.

### Current Pricing
| Column | Derivation |
|--------|------------|
| current_std_rate | rentroll_enriched.dcStdRate (unit-level rack rate, independent of tenant discounts) |
| current_std_sqft | dcStdRate / dcarea_fixed |
| current_disc_pct | CURRENT_DISCOUNT[country] |
| current_book_rate | dcStdRate x (1 - current_disc_pct) |
| current_book_sqft | current_book_rate / dcarea_fixed |

### Actual Rent
| Column | Derivation |
|--------|------------|
| actual_rent | rentroll_enriched.revenue_effective (per ledger — tenant-level) |
| actual_rent_sqft | revenue_effective / dcarea_fixed |
| actual_vs_budget_pct | actual_rent_sqft / budget_arr_sqft x 100 |

### Budget
| Column | Source |
|--------|--------|
| budget_arr_sqft | vw_budget_monthly.avr_rental_rate (current month via date_trunc('month', CURRENT_DATE), joined through siteinfo.SiteCode = vw_budget_monthly.site_code) |

### Recalibrated Pricing
| Column | Derivation |
|--------|------------|
| new_target_arr_sqft | from yield curve x climate matrix, rescaled to budget |
| new_std_rate | (new_target_arr_sqft / (1 - FUTURE_DISCOUNT)) x dcarea_fixed |
| new_std_sqft | new_target_arr_sqft / (1 - FUTURE_DISCOUNT) |
| change_vs_current_std_pct | (new_std_sqft - current_std_sqft) / current_std_sqft x 100 |

### Rounding

- SGD/MYR: round to 2 decimal places
- KRW: round to 0 decimal places (whole won)

## Yield Curve Algorithm

Per site:

1. **Extract current shape**: Group occupied storage units by `size_range`. For each size_range, compute NLA-weighted average `current_book_sqft` (= std_rate/sqft x (1 - market_discount)). Exclude units with zero or NULL dcStdRate.

2. **Fit -log curve**: Regress `current_book_sqft` against `ln(avg_area)` for each size_range bucket, weighted by total NLA in that bucket. This produces coefficients `a, b` where `rate = a - b x ln(area)`. Use stdlib `math` module — no numpy needed:
   ```
   x = ln(area), y = rate, w = NLA weight
   b = (sum(w*x*y) - sum(w*x)*sum(w*y)/sum(w)) / (sum(w*x^2) - (sum(w*x))^2/sum(w))
   a = (sum(w*y) - b*sum(w*x)) / sum(w)
   ```

3. **Apply climate**: Multiply the fitted rate by the climate multiplier for each unit's climate_code. NC units keep the base rate; others get scaled up.

4. **Rescale to budget**: Compute the NLA-weighted average of all `fitted_rate x climate_multiplier` across the site. Scale all rates by `budget_arr_sqft / weighted_average` so the site total matches budget.

5. **Derive new std rate**: `new_std_sqft = rescaled_rate / (1 - FUTURE_DISCOUNT)`, then `new_std_rate = new_std_sqft x area_sqft`.

### Validation output

Per site, log to console:
- Site code, total NLA, unit count
- Weighted average of new_target_arr_sqft vs budget_arr_sqft (should match)
- Curve coefficients (a, b)
- Residual error after rescaling

## Data Sources

All queries against `esa_pbi` database:
- `rentroll_enriched` — unit + tenant + pricing data (filter to latest extract_date)
- `unit_category_labels` — published inventory labels (INNER JOIN — unlabeled units excluded)
- `siteinfo` — site metadata (SiteCode, Name, Country)
- `vw_budget_monthly` — budget ARR/sqft targets (join via siteinfo.SiteCode = budget.site_code)

## Join Path

```
rentroll_enriched rr
  INNER JOIN unit_category_labels ucl ON ucl.site_id = rr.SiteID AND ucl.unit_id = rr.UnitID
  INNER JOIN siteinfo si ON si.SiteID = rr.SiteID
  LEFT JOIN vw_budget_monthly b ON b.site_code = si.SiteCode AND b.date = date_trunc('month', CURRENT_DATE)
```

## Script Location

`backend/python/scripts/pricing_recalibration.py`

## Output Location

`output/pricing_recalibration_YYYY-MM-DD.csv`

## Dependencies

- `sqlalchemy` (already in project)
- `math` (stdlib — for log regression)
- `csv` (stdlib)

No external dependencies beyond what the project already has.

## Future Extensions (not in this pass)

- Unit type differentiation (W vs L vs U multipliers)
- Web tool page with interactive climate matrix editor
- Direct ECRI batch creation from recalibrated output
- HK sites (once budget data available)
