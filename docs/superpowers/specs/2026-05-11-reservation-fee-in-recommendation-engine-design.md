# Reservation Fee in Recommendation Engine — Design

**Date:** 2026-05-11
**Status:** Draft for review

## Background

The chatbot and other third-party consumers of the Recommendation Engine need
to know how much to charge as a reservation fee to confirm a booking. Today
they default to one month of the unit's standard rate, which is also the
business default. We already maintain per-site overrides in
`mw_reservation_fees` (managed at `/tools/reservation-fees`), but the
recommendation API does not surface this value, so third parties cannot honour
it.

## Goal

Expose a `reservation_fee` (and its source) on every quoted slot returned by
the Recommendation Engine, derived from `mw_reservation_fees` when a row
exists for the slot's site, and falling back to one month of the unit's
`std_rate` otherwise.

## Non-goals

- No change to the reservation-fees admin tool or `mw_reservation_fees` schema
- No change to deposit calculation in `quote_slot`; reservation fee is a
  separate value
- No currency conversion — fee is in the site's local currency, consistent
  with the rest of the quote
- No per-unit override; site-level only

## Data sources

| Source | Used when | Field |
|---|---|---|
| `mw_reservation_fees.reservation_fee` | a row exists for the slot's `site_id` | override |
| `CandidateRow.std_rate` | no override row | default |

Source label values in the API: `"override"` or `"default"`.

## Implementation

### 1. Per-slot resolution

Add a private helper in `backend/python/web/services/recommender.py`:

```python
def _resolve_reservation_fee(
    site_id: int,
    std_rate: Optional[Decimal],
    db_session,
    site_cache: Optional[Dict[Any, Dict[str, Any]]] = None,
) -> Tuple[Optional[Decimal], str]:
    """Return (fee, source) for a slot. source is 'override' or 'default'.

    Cached per-site in site_cache so multi-slot, same-site requests hit the
    DB once. Falls back to std_rate when no mw_reservation_fees row exists.
    """
```

- Reads `mw_reservation_fees` once per site per request (memoised in the
  existing `site_cache` dict that `quote_slot` already threads)
- Returns `(Decimal(std_rate), 'default')` when no row exists and `std_rate`
  is not None
- Returns `(None, 'default')` only if `std_rate` is also None (defensive; the
  result is then dropped — see slot 2/3 rule below)

Wire it into `quote_slot` after `security_deposit` is resolved. Attach two
new attributes to the returned `DurationQuote` (or alongside it in the slot
dict the route builds — whichever is the lightest touch in the current
recommender flow):

- `reservation_fee: Decimal`
- `reservation_fee_source: str`  (`"override"` | `"default"`)

### 2. API response shape

Each slot object in `/api/recommendations` responses gains two fields:

```json
{
  "slot": 1,
  "unit_id": 12345,
  "first_month": 198.00,
  "security_deposit": 150.00,
  "reservation_fee": 150.00,
  "reservation_fee_source": "override",
  ...
}
```

When a slot has no quote (slot 2 or 3 empty), the fields are omitted along
with the rest of the quote — no change to current empty-slot behaviour.

### 3. Persistence — `mw_recommendations_served`

Add six nullable columns (migration `mw_add_reservation_fee_to_served.py` +
matching SQL):

```sql
ALTER TABLE mw_recommendations_served
  ADD COLUMN slot1_reservation_fee NUMERIC(12,2),
  ADD COLUMN slot1_reservation_fee_source VARCHAR(20),
  ADD COLUMN slot2_reservation_fee NUMERIC(12,2),
  ADD COLUMN slot2_reservation_fee_source VARCHAR(20),
  ADD COLUMN slot3_reservation_fee NUMERIC(12,2),
  ADD COLUMN slot3_reservation_fee_source VARCHAR(20);
```

`log_served()` writes the values from each slot's quote; nulls when a slot
has no quote. Existing rows remain valid.

### 4. Feed UI

In `web/templates/admin/recommendation_engine/feed.html`, render a small
"Res. fee" cell beside each slot's first-month value:

- `$150.00 (override)` — neutral colour
- `$198.00 (default)` — muted/grey colour, signalling the site has no
  configured override

`feed_detail` already returns the full JSON response, so the detail drawer
picks up the new fields automatically once the quote carries them.

## Test plan

- **Unit (`recommender._resolve_reservation_fee`)**
  - returns `(override_value, 'override')` when row exists
  - returns `(std_rate, 'default')` when no row
  - second call with same `site_id` hits cache (one DB query for N slots)
- **Integration (`/api/recommendations`)**
  - response contains `reservation_fee` + `reservation_fee_source` on every
    populated slot
  - empty slots have no reservation fee fields
  - multi-site recommendation returns the correct override per slot
- **Persistence**
  - `mw_recommendations_served` row has `slotN_reservation_fee*` columns
    populated after a served request; nulls for empty slots
- **Feed UI**
  - Override row renders with neutral chip; fallback row renders muted

## Rollout

1. Run migration (`ALTER TABLE` is additive, safe with running app)
2. Deploy new recommender + API + feed template
3. Spot-check `/api/recommendations` and feed UI in prod
