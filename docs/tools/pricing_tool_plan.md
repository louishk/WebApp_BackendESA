# Pricing Tool — Implementation Plan

**Status:** Draft, pending review
**Author:** louis
**Last updated:** 2026-04-21
**Scope:** Bring SiteLink unit-rate-update SOAP endpoints into the backend middleware, expose them through audited REST routes, and build a Revenue → Pricing Tool with rule-based pricing engine + AI review layer (closed-loop with backtesting).

---

## 1. Background

Verified during the 2026-04-21 SOAP probe (`Project Documentation/Documentation/Endpoints/CallCenterWs/Units/`):

| Endpoint | CCTST/Demo | C234/LSETUP |
|---|---|---|
| `UnitStandardRateUpdate` / `_v2` / `_v3` | ✅ working with `sUsagePassword="UnitStandardRateP@SS"` | ✅ working |
| `UnitWebRateUpdate` | ✅ working with `sUsagePassword="UnitWebRateP@SS"` | ✅ working |
| `UnitPushRateUpdate` / `_v2` | ⚙️ facility-blocked (`Ret_Code=-75`) | ❌ corp not licensed (`Ret_Code=-95`) |

Key takeaways:

- Each unit-rate family is gated by its own per-site **`sUsagePassword`** named `Unit<Type>RateP@SS`.
- A successful `UnitStandardRateUpdate*` automatically mirrors `dcStdRate` into `dcPushRate`, so direct push writes are usually unnecessary.
- The `EXTRASPAJYOSS3SPJDOW` key SiteLink issued is a **test/dev key** scoped to `CCTST/Demo`. Production writes use the existing **`SOAP_API_KEY`** in vault (whitelisted against C234).

---

## 2. Phases

### Phase 1 — Vault & env hygiene (≈ 0.5d)

#### 2.1.1 New vault entries

Stored encrypted in `app_secrets` (esa_backend), managed via `/admin/secrets`:

| Vault key | Value | Notes |
|---|---|---|
| `SITELINK_USAGE_PW_STANDARD_RATE` | `UnitStandardRateP@SS` | Gates `UnitStandardRateUpdate` v1/v2/v3 |
| `SITELINK_USAGE_PW_WEB_RATE` | `UnitWebRateP@SS` | Gates `UnitWebRateUpdate` |
| `SITELINK_USAGE_PW_PUSH_RATE` | `UnitPushRateP@SS` | Stored even though corp-blocked on C234 |
| `SITELINK_API_KEY_TEST` | `EXTRASPAJYOSS3SPJDOW` | Sandbox key for CCTST/Demo only |
| `AZURE_CLAUDE_ENDPOINT` | `https://louis-mmsp9ee1-eastus2.cognitiveservices.azure.com/` | Azure Cognitive Services base URL |
| `AZURE_CLAUDE_KEY` | (long token in `.env`) | Azure auth |
| `AZURE_CLAUDE_DEPLOYMENT_PRIMARY` | `claude-sonnet-4-6` | Default model for AI reviews |
| `AZURE_CLAUDE_DEPLOYMENT_FAST` | `claude-haiku-4-5` | Backtest replays + low-context overrides |

`SOAP_API_KEY` already in vault — reused for production calls; no change.

#### 2.1.2 Env clean-up

Move from `.env` into vault (loose secrets shouldn't sit on disk):

- `Username`, `Password` (test creds)
- `Backend_apikey`
- `ZOOM_*` (4 entries)
- `AZURE_CLAUDE_ENDPOINT`, `AZURE_CLAUDE_KEY`
- Delete `Sitelink_Pricing_Api_Key` (replaced by `SITELINK_API_KEY_TEST`)

**Stays in `.env`** (bootstrap-only): `VAULT_MASTER_KEY`, `DB_PASSWORD`, `PBI_DB_PASSWORD`, `VM_*`.

#### 2.1.3 Migration file

`backend/python/migrations/<date>_pricing_tool_secrets.sql` — placeholder INSERTs (real values pasted in via admin UI, never committed).

**Acceptance:** `vault_config('SITELINK_USAGE_PW_STANDARD_RATE')` returns the right value in Flask shell on dev + VM.

---

### Phase 2 — Middleware: `SiteLinkPricingClient` (≈ 1d)

New high-level client at `backend/python/common/sitelink_pricing_client.py`. Auto-injects auth + usage password; returns typed result; tracks outbound stats.

```python
class SiteLinkPricingClient:
    def __init__(self, corp_code: str, location_code: str):
        # Picks SOAP_API_KEY (prod) or SITELINK_API_KEY_TEST (CCTST) automatically
        # Picks usage password from vault per operation

    def update_standard_rate(unit_ids, std_rate, tax_inclusive=False) -> RateUpdateResult
    def update_monthly_weekly(unit_ids, monthly, weekly, tax_inclusive=False) -> RateUpdateResult  # v3
    def update_web_rate(unit_ids, web_rate, tax_inclusive=False) -> RateUpdateResult
    def update_push_rate(unit_ids, push_rate, tax_inclusive=False) -> RateUpdateResult
```

`RateUpdateResult`:

```python
@dataclass
class RateUpdateResult:
    success: bool
    ret_code: int
    ret_msg: Optional[str]
    unit_ids: list[int]
    error_kind: Literal['ok', 'bad_usage_password', 'corp_not_licensed',
                        'facility_disabled', 'auth', 'unknown']
```

- Corp-routing: `corp_code in {"CCTST"}` ⇒ test key; else prod key.
- Tracks every call via `common/outbound_stats.py`.
- Pytest live tests opt-in via `RUN_LIVE_SITELINK_TESTS=1`, restore baseline at teardown.

**Acceptance:** end-to-end update of unit 106073 on LSETUP from a Python REPL; `error_kind='corp_not_licensed'` correctly returned for `update_push_rate`.

---

### Phase 3 — REST API (≈ 3d)

#### 3.1 New blueprint `pricing.py` mounted at `/api/pricing`

| Method | Path | Purpose |
|---|---|---|
| GET | `/sites` | Sites the caller can edit |
| GET | `/types?site_id=` | One row per `sTypeName` at the site, decoded per COM01 |
| GET | `/types/metrics?site_id=` | Type-level analytics (see §3.2) |
| POST | `/types/standard-rate` | `{site_id, s_type_names[], monthly, weekly?, weekly_ratio?, tax_inclusive}` — expands to all units of those types and calls `update_monthly_weekly` |
| POST | `/types/web-rate` | `{site_id, s_type_names[], web_rate, tax_inclusive}` |
| POST | `/units/override` | Per-unit override path |
| POST | `/bulk` | Bulk upload — `{site_id, rows[], dry_run}` |
| GET | `/recommendations?site_id=` | Returns baseline + AI overlay per type |
| GET | `/ai-review/{review_id}/explain` | Drawer breakdown (inputs + reasoning) |
| POST | `/ai-review/{review_id}/decide` | `{decision, applied_monthly?}` — accept/reject |
| GET | `/ai-performance?site_id=&from=&to=` | Win-rate dashboard |
| GET | `/config?site_id=` / PUT `/config/site` / PUT `/config/ranges` / PUT `/config/modifiers` | Pricing engine configuration |
| POST | `/recompute?site_id=` | Manual recompute trigger |

All write endpoints: `@require_auth + @require_api_scope('pricing.write')` and rate-limited (30/min writes, 120/min reads). Every write produces an `audit_log(AuditEvent.PRICING_RATE_CHANGE, ...)` row.

#### 3.2 Type-level metrics (`/types/metrics`)

Per `sTypeName` row returns:

- `total_units`, `vacant_units`, `occupied_units`
- `occ_pct_now`, `occ_pct_7d`, `occ_pct_30d`, `occ_pct_90d` and Δs
- `movein_7d / 30d / 90d`
- `moveout_7d / 30d / 90d`
- `net_movein_7d / 30d / 90d`
- `current_std_rate`, `current_web_rate`, `current_monthly`, `current_weekly`
- `actual_avg_price_per_sqft_after_discount` (rent roll: `sum(actual_rent_after_discount) / sum(sqft)` over occupied units)
- `comp_price_per_sqft` (nullable — placeholder for future scraper)

Backed by materialized view `esa_pbi.vw_pricing_type_metrics`, refreshed nightly. **Prereq:** confirm rent-roll snapshot retention covers D-90; if not, add a daily snapshot pipeline as Phase 0.

---

### Phase 4 — Frontend: Revenue → Pricing Tool (≈ 5d)

Lives at `backend/python/web/templates/tools/pricing/`. Vanilla JS + Jinja per project convention.

Top-level tabs:

#### 4.1 Recommendations

Site dropdown → table grouped by `sTypeName`. Columns:

| Column | Source |
|---|---|
| Type code (`M/30-35/W/NC/SS/NP`) | `sTypeName` |
| Decoded labels (Size / Range / Climate / etc.) | COM01 decoder |
| # units (vacant / total) | metrics |
| Occ% (now, Δ7d, Δ30d, Δ90d) | metrics |
| Move-ins (7/30/90) | metrics |
| Net move-in (7/30/90) | metrics |
| Actual $/sqft (after discount) | metrics |
| Current monthly / weekly / web | metrics |
| Baseline recommended (rule engine) | recommendations |
| AI action chip + Δ% | AI review |
| Confidence bar | AI review |
| Effective $/sqft (recommended) | computed |
| Comp $/sqft | placeholder |
| Action | ✓ / ✗ / Manual |

Click row → drawer with: side-by-side Current vs Baseline vs AI Final, AI reasoning text, last 5 outcomes for `(site, type)` with green/red labels, and the four buttons:

- `Accept AI`
- `Accept Baseline only`
- `Reject both`
- `Manual override` (inline numeric input)

Footer: aggregate impact ("Δ revenue if all accepted: +$X/mo"). Anything over per-site `confirmation_threshold_pct` triggers an extra confirmation modal.

#### 4.2 Price Upload (sub-tabs)

##### 4.2.1 By Type (default)

Editable inline table: Monthly (primary), Weekly (auto-derived from `monthly × weekly_ratio`), Web rate. Header has the `weekly_ratio` input (default 1.25); changing it recomputes every Weekly cell that hasn't been manually overridden.

`Apply` → confirmation modal with exact $ deltas → POST to `/types/standard-rate` + `/types/web-rate`.

##### 4.2.2 By Unit (override)

Single-unit form OR drag-drop CSV with columns `unit_id, std_rate, web_rate, std_monthly_rate, std_weekly_rate`. Pre-flight `dry_run=true` returns row-by-row diff; user reviews; `Confirm & apply`. Push-rate columns rejected gracefully with per-row error.

#### 4.3 Config

Three editor cards:

1. **Site basics** — `rate_per_sqft`, `weekly_ratio`, `rounding_step` (default 5), `ai_max_adjustment_pct` (default ±10), `ai_model_tier` (`primary` | `fast`), `confirmation_threshold_pct`.
2. **Size ranges** — table of all 28 COM01 ranges with: `ref_sqft_strategy` (lowest/highest/midpoint/manual), `ref_sqft_value`, `floor_step`, `min_floor_sqft`. "Reset to corp default" per row.
3. **Modifiers** — accordion per component (Climate / Type / Shape / Pillar / Size Cat / Case Count). Each opens a code → % table. Defaults pre-loaded from COM01 SOP (Climate: NC -5, A 0, D 0, AD +10, RF +15).

`Preview impact` button calls `/recompute?dry_run=1` and shows: # types whose recommended price would change, % delta distribution, top 10 movers.

#### 4.4 AI Performance tab

- Per-site / per-type win rate (T+30 positive outcomes ÷ accepted recommendations).
- Distribution of `ai_action` over time.
- Trailing 30/60/90-day `outcome_label` mix.
- "Mute AI for this type" toggle — writes to `pricing_ai_mute`.

---

### Phase 5 — Rule-based pricing engine (≈ 4.5d)

#### 5.1 Formula (additive composition)

```
total_modifier_pct = sum of all applicable modifier % (climate, type, shape, pillar, size_cat, case_count)
multiplier         = 1 + total_modifier_pct / 100

raw_monthly        = base_rate_per_sqft × ref_sqft × multiplier
recommended_monthly = round_to(raw_monthly, site.rounding_step)         # default $5
recommended_weekly  = round_to(recommended_monthly × site.weekly_ratio, site.rounding_step)
recommended_web     = recommended_monthly                                # default
```

#### 5.2 Range flooring

Per `Unit Size Range` (COM01: 28 ranges):

- `ref_sqft_strategy ∈ {lowest, highest, midpoint, manual}` (default `lowest`)
- `floor_step` (optional) — splits range into sub-bands of N sqft, each anchored on its own lower bound
- Example: range `90-110`, `floor_step=10` → 90-100 (ref 90) + 100-110 (ref 100); a 104 sqft unit prices on 100

#### 5.3 Modifier defaults (COM01)

| Component | Code | Default % |
|---|---|---|
| Climate | NC / A / D / AD / RF | -5 / 0 / 0 / +10 / +15 |
| Type | W (Walk-In) | 0 (rest configurable) |
| Shape | SS / WR / NR / OS | all 0 by default |
| Pillar | P / NP | both 0 |
| Size cat | S / M / L / XL | all 0 (range drives pricing) |
| Case count (wine) | coefficient `+x% per case`, default 0% | |

Negative values allowed (malus). Site-level overrides fall back to corp defaults.

#### 5.4 Data model (esa_backend)

| Table | Columns |
|---|---|
| `pricing_site_config` | `site_id` PK, `rate_per_sqft`, `weekly_ratio`, `rounding_step`, `ai_max_adjustment_pct`, `ai_autoapply_threshold`, `ai_autoapply_max_pct`, `ai_confidence_weights` (JSONB), `ai_model_tier`, `confirmation_threshold_pct`, `updated_by`, `updated_at` |
| `pricing_range_config` | PK `(site_id NULLABLE, range_code)`. NULL site_id = corp default. Cols: `ref_sqft_strategy`, `ref_sqft_value`, `floor_step`, `min_floor_sqft` |
| `pricing_modifier_config` | PK `(site_id NULLABLE, component, code)`. Cols: `pct_modifier` |
| `pricing_recommendations` | `site_id`, `s_type_name`, `unit_id` (nullable for type rollup), `recommended_monthly`, `recommended_weekly`, `recommended_web`, `inputs_json`, `algo_version`, `generated_at`, `status` |
| `units_pricing_override` | `unit_id` PK, `std`, `web`, `monthly`, `weekly`, `is_manual`, `set_by`, `set_at` — recomputer skips manual rows |

#### 5.5 COM01 decoder

`backend/python/common/com01_decoder.py` — pure-function parser of `sTypeName` → component dict. Fully unit-tested across all COM01 codes.

#### 5.6 Nightly recompute

APScheduler job `pricing_recommendations_refresh` (config: `pipelines.yaml`):

1. For each site → each unit:
   - Decode `sTypeName` via `com01_decoder`.
   - Resolve effective config (range → modifiers).
   - Compute `recommended_monthly` (engine) → store baseline.
2. Skips units flagged `units_pricing_override.is_manual=TRUE`.

---

### Phase 6 — AI review layer (Claude via Azure) (≈ 6.75d)

#### 6.1 Inputs

For each `(site, sTypeName)`:

1. Type metadata + decoded COM01 components.
2. Current rates + effective $/sqft.
3. Rule-based baseline + breakdown JSON.
4. Performance signals: occ% now, Δocc% at D-7/D-30/D-90, move-in & net move-in 7/30/90, days-vacant distribution.
5. Active discount plans only (filter `is_active=TRUE` AND date window covers today).
6. Memory: last 10 AI reviews on this `(site, sTypeName)` plus their realized T+7 / T+30 outcomes.

#### 6.2 Output (strict JSON)

```json
{
  "ai_action": "hold | nudge_up | push_up_strong | nudge_down | push_down_strong",
  "ai_adjustment_pct": -8.0,
  "ai_self_confidence": 0.65,
  "ai_reasoning": "Free-text rationale, 2-4 sentences."
}
```

Adjustment is on top of the baseline; final = `baseline × (1 + ai_adjustment_pct/100)`, clamped to `±site.ai_max_adjustment_pct`, then rounded.

#### 6.3 Confidence (composite, 0–1)

```
confidence = 0.40·signal_strength
           + 0.30·data_quality
           + 0.20·historical_accuracy
           + 0.10·model_self_report
```

| Sub-score | Computation |
|---|---|
| `signal_strength` | normalized magnitude of Δocc%_30d × sign-agreement with net_movein_30d |
| `data_quality` | `min(1, n_history_points / 30)` — 0 if D-90 retention missing |
| `historical_accuracy` | `precision_at_T+30` over trailing 90 days of decisions; default 0.5 if <5 prior decisions |
| `model_self_report` | LLM's `ai_self_confidence` field |

Weights tunable per site via `pricing_site_config.ai_confidence_weights`.

#### 6.4 Closed-loop jobs

- **Outcome backfill** (daily): for every review at age T+7 or T+30, compute realized `occ_pct`, `net_movein`, `revenue_actual`, derive `outcome_label ∈ {positive, neutral, negative}`. T+30 is authoritative when it conflicts with T+7.
- **Memory refresh** (daily): aggregate per `(site, sTypeName)` the last 10 reviews + their outcomes into a fast-read table for prompt assembly.
- **Nightly review run**: regenerate baseline (if config changed) → call AI per type → persist to `pricing_ai_reviews`.

#### 6.5 Data model additions

| Table | Purpose |
|---|---|
| `pricing_ai_reviews` | One row per AI call. Cols include `ai_raw_adjustment_pct`, `ai_clamped`, `confidence_signal`, `confidence_data`, `confidence_history`, `confidence_model`, `ai_reasoning_text`, `inputs_snapshot_json`, `model_version` (e.g. `claude-sonnet-4-6`) |
| `pricing_ai_outcomes` | T+7 / T+30 realized metrics + `outcome_label` |
| `pricing_decisions` | Captures human accept/reject: `decision ∈ {accept_ai, accept_baseline, reject_both, manual_override}`, `applied_monthly`, `decided_by`, `decided_at` |
| `pricing_ai_mute` | Per-site / per-type opt-out |

#### 6.6 Client wrapper

`backend/python/common/azure_claude_client.py`:

```python
class AzureClaudeClient:
    def __init__(self):
        self.endpoint = vault_config('AZURE_CLAUDE_ENDPOINT')
        self.api_key  = vault_config('AZURE_CLAUDE_KEY')
        self._client = anthropic.Anthropic(
            base_url=f"{self.endpoint.rstrip('/')}/anthropic/v1",
            auth_token=self.api_key,
        )

    def review_pricing(self, system_prompt, user_prompt,
                       tier: Literal["primary", "fast"] = "primary",
                       max_tokens: int = 1024) -> dict:
        deployment = vault_config(
            "AZURE_CLAUDE_DEPLOYMENT_PRIMARY" if tier == "primary"
            else "AZURE_CLAUDE_DEPLOYMENT_FAST"
        )
        resp = self._client.messages.create(
            model=deployment,
            system=[{"type": "text", "text": system_prompt,
                     "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": user_prompt}],
            max_tokens=max_tokens,
        )
        return parse_pricing_review_json(resp.content[0].text)
```

- **Prompt caching** on the system prompt (COM01 decoder + formula explanation + few-shot examples) — ~90% cost reduction on the nightly batch.
- **Strict JSON parse**, fail closed: skip AI overlay for that type and use baseline only on parse error.

#### 6.7 Cost envelope

≈ 50 sites × 30 types/site = 1,500 type-rows/night on Sonnet 4.6 with caching: ≈ **$0.90/night ≈ $27/month**. Backtest replays on Haiku ≈ 10× cheaper.

#### 6.8 Backtest harness

`scripts/pricing_ai_backtest.py` — replays last N months of `pricing_ai_reviews` against a candidate prompt or model version; compares simulated `outcome_label` distribution against actual. Used to A/B test before promoting prompt changes.

---

### Phase 7 — Future: comp-price scraper (out of scope)

Placeholder column already in metrics. When scraper lands:

- Drops rows into `pricing_comp_observations` (esa_pbi).
- Metrics view joins on `(geo, type_code)` and fills `comp_price_per_sqft`.
- Recommendation engine + AI prompt can incorporate as a comp-anchor term.

---

## 3. Effort summary

| Phase | Effort |
|---|---|
| 1. Vault + env hygiene | 0.5d |
| 2. `SiteLinkPricingClient` middleware | 1d |
| 3. REST API + metrics view | 3d |
| 4. UI (Recommendations + Price Upload + Config + AI Performance) | 5d |
| 5. Rule-based pricing engine | 4.5d |
| 6. AI review layer (Claude via Azure) | 6.75d |
| 7. Rollout / docs / feature flag | 0.5d |
| **Total** | **≈ 21.25d** |

---

## 4. Rollout

1. **Dev validation** — verified working on LSETUP during 2026-04-21 probe.
2. **Feature flag** `pricing_tool_enabled` in `app_settings`; default off; only `pricing_team` role sees the menu item.
3. **Audit alerts** (existing `alert_manager`): >50 pricing changes/hour from a single user, OR >5 AI overlays clamped at max in a single run.
4. **Rollback**: every API write stamps `pricing_change_history.before_value`; admin "Revert change" action calls the inverse update through the same client.

---

## 5. Open questions before kickoff

1. **D-90 snapshot retention** — does our rent roll history cover 90 days at daily granularity? If not, daily snapshot pipeline is a prereq (Phase 0).
2. **Auto-apply mode** — when do we enable `confidence ≥ X AND |adj| ≤ Y` auto-apply? Suggest leaving off until win-rate ≥ 70% over 60 days.
3. **Korean sites** — confirm same `sUsagePassword` convention applies across all corps; KR may need separate vault entries.
4. **`pricing_team` role** — exists already? If not, add a migration to seed it.
5. **Discount-plan join shape** — `discount_plan_config` is per-unit; need to verify the exact link from a discount plan to a `(site, sTypeName)` for the AI context.

---

## 6. Files to create / touch

### New
- `backend/python/migrations/<date>_pricing_tool_secrets.sql`
- `backend/python/migrations/<date>_pricing_tool_tables.sql` — 7+ new tables
- `backend/python/common/sitelink_pricing_client.py`
- `backend/python/common/azure_claude_client.py`
- `backend/python/common/com01_decoder.py`
- `backend/python/common/pricing_engine.py`
- `backend/python/common/prompts/pricing_ai_review.md` — version-controlled system prompt
- `backend/python/web/routes/pricing.py`
- `backend/python/web/templates/tools/pricing/` (4 HTML files)
- `backend/python/datalayer/pricing_recommendations_refresh.py` — nightly recompute
- `backend/python/datalayer/pricing_ai_outcome_backfill.py` — daily T+7 / T+30 backfill
- `scripts/pricing_ai_backtest.py`

### Touch
- `backend/python/web/app.py` — register `pricing_bp`
- `backend/python/web/auth/decorators.py` — `@pricing_tools_access_required`
- `backend/python/web/templates/base.html` — nav item under Revenue
- `backend/python/config/pipelines.yaml` — register the two new pipelines
- `.env` — strip moved-to-vault entries
- `CLAUDE.md` — update "Vault Secrets" section with new keys
