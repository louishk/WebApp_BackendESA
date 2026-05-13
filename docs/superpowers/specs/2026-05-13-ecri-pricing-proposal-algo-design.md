# ECRI Pricing Tool — Increase Proposal Algorithm

**Date:** 2026-05-13
**Status:** Approved (design)
**Files (new):** `backend/python/migrations/059_ecri_pricing_config.sql`, `backend/python/web/routes/ecri.py` (additions), `backend/python/web/templates/ecri/pricing.html`
**Files (modified):** `backend/python/web/templates/ecri/eligibility.html`, `backend/python/common/models.py`

## Problem

Operators currently set a single global `target_increase_pct` per batch. There is no way to differentiate "below market, low-risk" tenants (should get a larger increase) from "above top-1, high-risk" tenants (should get less or none). We need a per-ledger proposed % driven by configurable weighted factors.

## Goal

A new `/ecri/pricing` admin page lets a manager define a gradient (min/max %) and per-factor toggle + weight. Eligibility tables in both Standard and Pre-Load show a **Proposed %** column per ledger computed from those weights against the same benchmark/risk fields already on each row. The factors that fired are surfaced via tooltip.

## Pricing Page — `/ecri/pricing`

Route: `GET /ecri/pricing` (HTML), `GET /ecri/api/pricing-config`, `PUT /ecri/api/pricing-config` (manage role).

Layout — single card with:
1. **Gradient** — two number inputs: Min %, Max % (default 3, 18). Validation: 0 ≤ min < max ≤ 50.
2. **Factor table** — one row per factor (15 rows):

   | Factor | Enabled | Weight | Description |
   |---|---|---|---|
   | below_market | ☑ | 1.0 | current_rent < market_rate |
   | below_site_median | ☑ | 0.7 | current_rent < in_place_median_site |
   | below_country_median | ☑ | 0.5 | current_rent < country_rent_med |
   | below_top3 | ☑ | 0.6 | current_rent < top3_rent |
   | below_top1 | ☑ | 0.4 | current_rent < top1_rent |
   | above_market | ☑ | -0.6 | current_rent > market_rate |
   | above_site_median | ☑ | -0.4 | current_rent > in_place_median_site |
   | above_country_median | ☑ | -0.3 | current_rent > country_rent_med |
   | above_top3 | ☑ | -0.5 | current_rent > top3_rent |
   | above_top1 | ☑ | -0.7 | current_rent > top1_rent |
   | high_unit_risk | ☑ | -1.0 | unit_risk_factor > 1.10 |
   | very_high_unit_risk | ☑ | -1.5 | unit_risk_factor > 1.30 |
   | tenure_under_24mo | ☑ | -0.3 | tenure_months < 24 |
   | red_bucket | ☑ | -0.4 | effective_bucket == 'red' |

   Weights are floats in [-3, +3]. Negative factors are stored with negative weights so the algorithm logic stays uniform.

3. **Preview panel** — small "what-if" widget below the table: numeric inputs for current_rent / market_rate / site_median / country_med / top3 / top1 / unit_risk_factor / tenure_months / effective_bucket. Live-updates a "Proposed: X% — fired: factor_a, factor_b" line as you adjust inputs/weights/gradient. Pure client-side, no extra API call.

4. **Save** — PUT the config back. Visible only to users with `ecri_manage_required`.

## Storage

New table in esa_pbi (consistent with other ECRI tables):

```sql
-- migrations/059_ecri_pricing_config.sql
CREATE TABLE IF NOT EXISTS ecri_pricing_config (
    id          INTEGER PRIMARY KEY,
    config      JSONB    NOT NULL,
    updated_by  VARCHAR(255),
    updated_at  TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed singleton row (id=1) with defaults from the design spec.
INSERT INTO ecri_pricing_config (id, config, updated_by)
VALUES (1, '{"gradient_min_pct":3,"gradient_max_pct":18,"factors":{...}}'::jsonb, 'system')
ON CONFLICT (id) DO NOTHING;
```

Singleton pattern: only id=1 row is ever read/written. Audit trail is the `updated_by` + `updated_at` columns plus an existing app-level `audit_log()` call on save.

Model: `ECRIPricingConfig` in `common/models.py` (mirrors the other ECRI models).

## Algorithm — `compute_proposed_pct(row, config)`

Implemented client-side in JS (and a tiny Python equivalent in `common/ecri_pricing.py` for tests / potential server-side use later):

```
Inputs:
  row    = one tenant row (already has all benchmark + risk + bucket fields)
  config = {gradient_min_pct, gradient_max_pct, factors: {name: {enabled, weight}}}

1. fired = []
2. For each factor in config.factors where enabled:
     if predicate(factor.name, row) is true: fired.push({name, weight: factor.weight})
3. positive = sum(weight for w in fired if weight > 0)
4. negative = sum(-weight for w in fired if weight < 0)
5. max_pos  = sum(weight    for f in enabled factors if weight > 0)
6. max_neg  = sum(-weight   for f in enabled factors if weight < 0)
7. pos_norm = positive / max_pos   if max_pos > 0 else 0   # [0, 1]
8. neg_norm = negative / max_neg   if max_neg > 0 else 0   # [0, 1]
9. raw     = pos_norm - neg_norm                           # [-1, 1]
10. proposed_pct = gradient_min_pct + (gradient_max_pct - gradient_min_pct) * max(0, raw)
11. return { proposed_pct: round(proposed_pct, 1), fired: fired.map(f => f.name) }
```

**Interpretation:**
- All positive factors fire, no negatives → `raw = 1` → `proposed = max_pct`
- No factors fire → `raw = 0` → `proposed = min_pct`
- Negative factors dominate → `raw < 0` → clamped to `min_pct` (floor is min, not zero, by design — we don't propose decreases)

**Predicate details:**
- `below_market`: `current_rent < market_rate` (both must be non-null)
- `above_market`: `current_rent > market_rate`
- ...similar for site_median, country_median, top3, top1
- `high_unit_risk`: `unit_risk_factor != null && unit_risk_factor > 1.10`
- `very_high_unit_risk`: `unit_risk_factor != null && unit_risk_factor > 1.30`
- `tenure_under_24mo`: `tenure_months != null && tenure_months < 24`
- `red_bucket`: `effective_bucket == 'red'`

Predicates with missing inputs do NOT fire (a row missing market_rate just contributes nothing for that factor; it doesn't subtract from max_pos/max_neg).

## Eligibility UI — Proposed % column

- On `runStandard()` / `runAdvance()`, also fetch `/ecri/api/pricing-config` once and cache in script-level `PRICING_CONFIG`.
- New column **Proposed %** in all three tables (Standard + 2 Pre-Load segments) rendered between Next Effective and Unit Risk.
- Cell renders `<span title="fired: factor_a, factor_b">12.5%</span>`. When no factors fire, render the `gradient_min_pct` value.
- Add `proposed_pct` to FILTER_FIELDS so users can `proposed_pct >= 10` etc.

The existing batch-create flow still uses the single `target_increase_pct` input from the inline form — the Proposed % column is **informational for v1**. A follow-up can change batch creation to use per-row proposed values; out of scope for this spec.

## Out of Scope (v1)

- Per-row override of proposed % in the table (no inline edit).
- Using proposed_pct as the actual batch increase (still `target_increase_pct` for now — flagged for follow-up).
- Per-site / per-country pricing variations (single global config).
- Versioned config history (just last-write-wins on id=1 row).

## Risks

- **Default weights are guesses.** Operator should tune via the preview before relying on it.
- **Predicate gaps:** if `country_rent_med` is null for a small site, `below_country_median` doesn't fire — that's fine semantically (no signal) but could surprise users who don't notice the dimension is silently inactive. The fired-list tooltip exposes which fired, so silence is visible.

## Testing

- Manual: set min=5, max=10, enable only `below_market` at weight 1.0; confirm tenants with `current_rent < market_rate` show 10%, others show 5%.
- Manual: enable a positive + negative; confirm the cancellation math (raw ≈ 0) lands close to min%.
- Manual: load on a site with high-risk units; confirm the negative-only path floors to min%.
- Persistence: save config in one browser tab, reload `/ecri/eligibility` in another, confirm new defaults take effect.
