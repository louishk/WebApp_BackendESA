# Smart Lock — Hourly Cron, Per-Site Manual Refresh, Site-Level RBAC

**Status:** Approved (design phase)
**Date:** 2026-05-13
**Owner:** louisver@extraspaceasia.com

## Summary

Three coordinated changes to the smart lock subsystem:

1. **Cron rebalance** — all 4 SOAP/Igloo pipelines move to an hourly cadence: igloo every 15 min; in the last 15 min of each hour the SOAP/sync chain (units → gate → pin_sync) runs.
2. **Manual per-site refresh** — on the assignments page, two new buttons let an authorised user trigger the full data chain on demand for the currently selected site, or for all sites they can access. SOAP cost is bounded by a 15-minute per-site cooldown.
3. **Site-level RBAC** — reuse `User.allowed_site_ids` (already used by ECRI) to scope smart lock pages, and merge the standalone `/admin/users/ecri-limits` panel into the main `/admin/users` user edit page so site access and ECRI limits are configured in one place.

## Goals / Non-goals

**Goals**
- Fresher Igloo data without increasing SOAP API cost.
- A clear "force a refresh now" path for site staff/admins when data looks stale.
- Per-user site allow-list applied consistently to smart lock pages and APIs.
- Single admin surface for per-user site access + ECRI limits.

**Non-goals**
- No new role flags beyond what already exists.
- No changes to the smart lock data model (keypads, padlocks, bridges, assignments).
- No changes to the audit subsystem.
- No backfill of `allowed_site_ids` (empty = unrestricted).

## Section 1 — Cron schedule (`mw_sync_pipelines`)

All four pipelines are seeded into `esa_middleware.mw_sync_pipelines`. Per the project memory note on pipeline ownership, schedule changes MUST be applied via SQL on that table (not YAML).

| Pipeline | Old cron | New cron | Runs at |
|---|---|---|---|
| `igloo` | `0 */4 * * *` | `*/15 * * * *` | :00, :15, :30, :45 |
| `ccws_units_info` | `0 */6 * * *` | `45 * * * *` | :45 |
| `ccws_gate_access` | `*/30 * * * *` | `50 * * * *` | :50 |
| `igloo_pin_sync` | `*/15 * * * *` | `55 * * * *` | :55 |

Rationale: igloo is free (REST API, no SOAP cost) and is the source of device/property data the rest of the chain depends on, so it stays fast. The SOAP chain (units → gate) runs once per hour with `pin_sync` last so the reconciliation step sees the freshest units, gate codes, and igloo devices.

**Migration script:** `backend/python/migrations/mw_update_smartlock_cron.py`

```python
# Idempotent UPSERT-style UPDATE
UPDATE mw_sync_pipelines
SET schedule_config = :sched, updated_at = NOW()
WHERE pipeline_name = :name
```

Applied to all 4 rows. No code changes to pipeline classes. Orchestrator reconciler picks up new cron on next loop; deploy step restarts `backend-scheduler` for immediate effect.

## Section 2 — Manual per-site refresh

### UI

In `backend/python/web/templates/tools/smart_lock_assignments.html`, add a toolbar near the site selector with two buttons:

- **Refresh This Site** — full chain for the currently selected site.
- **Refresh All My Sites** — full chain for every site in the user's allowed list.

Both buttons show:
- Status badge (`idle` / `running` / `done` / `failed`) per pipeline in the chain.
- Last-refreshed-at timestamp for the selected site.
- A countdown tooltip when the button is disabled by cooldown ("Available in 12:34").

### Auth

Single rule for both buttons (server-enforced):

- `can_access_smart_lock` permission.
- Every target `site_id` must pass `user.can_see_site(site_id)`.
- "Refresh All My Sites" expands to the user's effective allowed sites (or all sites if `allowed_site_ids` is empty/NULL).

### Endpoints

New blueprint: `backend/python/web/routes/smart_lock_refresh.py`

**`POST /api/smart-lock/refresh`**

Body:
```json
{ "site_ids": [123, 456] }
```

Behavior:
1. Validate auth (login + smart lock permission).
2. For each `site_id`: verify `user.can_see_site(site_id)`. Reject whole request on any failure.
3. For each `site_id`: check cooldown row `smart_lock_refresh_cooldowns`. Reject whole request if any site is on cooldown; response includes which sites and when they unlock.
4. Insert chain rows into `mw_sync_runs` with `triggered_by='manual'`, common `chain_id` (uuid), `scope_filter={"site_ids": [...]}`, in order: `igloo`, `ccws_units_info`, `ccws_gate_access`, `igloo_pin_sync`.
5. Upsert cooldown row for each site with `last_refresh_at = NOW()`.
6. Return `{ "chain_id": "...", "pipelines": ["igloo", ...] }`.

**`GET /api/smart-lock/refresh/<chain_id>`**

Returns:
```json
{
  "chain_id": "...",
  "status": "pending|running|completed|failed",
  "pipelines": [
    {"name": "igloo", "status": "completed", "started_at": "...", "finished_at": "..."},
    ...
  ]
}
```

Frontend polls every 2 s, stops on terminal state.

### Cooldown table (esa_backend)

```sql
CREATE TABLE smart_lock_refresh_cooldowns (
    site_id          INTEGER PRIMARY KEY,
    last_refresh_at  TIMESTAMP NOT NULL,
    last_refresh_by  INTEGER REFERENCES users(id),
    updated_at       TIMESTAMP NOT NULL DEFAULT NOW()
);
```

Cooldown duration: **15 minutes per site**. Cooldown is updated on **enqueue** (not on completion) so spam-clicking can't double-fire while a chain is in flight.

Igloo-only refresh is not exposed separately — both buttons run the full chain because the user wants a single "refresh everything" UX. (If the user later wants a cheap igloo-only refresh, the endpoint can accept a `pipelines` array.)

### Pipeline `site_ids` argument

Each pipeline's `run()` accepts an optional `site_ids: list[int] | None`:

- `igloo` — filter `property_site_map` to the matching sites.
- `igloo_pin_sync` — constrain DB queries to those site_ids.
- `ccws_units_info` — pass single/multi-site `location_codes` (translate site_id → site_code via `siteinfo`).
- `ccws_gate_access` — same site_code translation.

When `site_ids` is `None`, behavior is unchanged (full run).

## Section 3 — Site-level RBAC + admin merge

### Data model

Reuse existing `User.allowed_site_ids: ARRAY(Integer)`. Empty/NULL = unrestricted. Same semantics as `User.can_see_site(site_id)`.

### Admin UI consolidation

Today:
- `/admin/users` — user list + edit (basic fields + roles).
- `/admin/users/ecri-limits` — site allow-list + ECRI pct/abs caps. (Owned by `admin_bp.ecri_user_limits`.)

After:
- `/admin/users/<id>/edit` (or the equivalent existing route) gains an **Access** section with:
  - **Allowed sites** — multi-select dropdown of all sites (label `<SiteCode> — <SiteName>`). Empty = all sites.
  - **ECRI max % reduction** — existing field, moved.
  - **ECRI max abs reduction** — existing field, moved.
- `/admin/users/ecri-limits` route + template removed; 302 redirect to `/admin/users` preserves bookmarks.
- New endpoint `POST /admin/api/users/<id>/access` — body `{allowed_site_ids: [int], ecri_max_pct_reduction: float, ecri_max_abs_reduction: float}`. Atomic update. `@admin_required`.

### Site list source

`GET /api/sites` returning `[{site_id, site_code, site_name, country}]` from the `siteinfo` table. Used by:
- Admin user edit page (full list).
- Smart lock pages (filtered to the caller's `allowed_site_ids`).

If `/api/sites` already exists in some form, reuse it; otherwise add it.

### Smart lock enforcement (3 layers)

1. **Page guard** (already in place): `@smart_lock_access_required`.
2. **Site dropdown filter** (new): `GET /api/me/sites` returns the caller's effective allowed sites. The assignments page populates its selector from this endpoint, so a restricted user never sees sites they cannot access.
3. **API guard** (new): every smart lock API endpoint that accepts a `site_id` calls `user.can_see_site(site_id)` and returns 403 otherwise. Applies to:
   - `/api/smart-lock/assignments` (GET, POST)
   - `/api/smart-lock/keypads`
   - `/api/smart-lock/padlocks`
   - `/api/smart-lock/bridges`
   - `/api/smart-lock/config`
   - `/api/smart-lock/refresh` (new)

The 403 path uses the generic error pattern (no `str(e)` leak).

## Section 4 — Testing, deploy, rollout

### Testing

- **Cron migration**: run on staging; verify `mw_sync_pipelines.schedule_config` updated for all 4 rows; confirm orchestrator reconcile loop picks up new schedule (check next scheduled run timestamps in `mw_sync_runs`).
- **Pipeline `site_ids` arg**: unit tests for each pipeline with single-site, multi-site, and `None`; assert SOAP call count matches site count for single-site case.
- **Refresh enqueue**: happy path; reject when missing permission; reject when target site not in `allowed_site_ids`; reject when any target site on cooldown; verify cooldown row upserted; verify chain rows inserted in order.
- **Refresh polling**: returns correct aggregated status (pending/running/completed/failed) across the chain.
- **Admin merge**: edit a user, set `allowed_site_ids` + ECRI fields, save; reload; both fields persist. `/admin/users/ecri-limits` 302s to `/admin/users`.
- **Smart lock filtering**: as a user with `allowed_site_ids=[L001 site_id]`, the assignments page selector shows only L001; direct `GET /api/smart-lock/assignments?site_id=<L002>` returns 403.

### Deploy order

Single deploy via `scripts/deploy_to_vm.py`:

1. SQL migration on `esa_backend`: create `smart_lock_refresh_cooldowns` table.
2. Python migration on `esa_middleware`: `mw_update_smartlock_cron.py`.
3. Rsync code.
4. `systemctl restart esa-backend backend-scheduler`.

### Rollout notes

- All current users have `allowed_site_ids` empty → keep full access. No backfill.
- `smart_lock_refresh_cooldowns` starts empty → first refresh per site succeeds.
- Cron migration is idempotent (UPDATE on stable PK `pipeline_name`).
- `/admin/users/ecri-limits` redirect prevents broken bookmarks.

### Out of scope

- No new role flags.
- No changes to audit logging beyond existing patterns.
- No change to the SOAP client, the orchestrator scheduler engine, or the data model.
- No igloo-only refresh button (can be added later by extending the endpoint with a `pipelines` array).

## File touch list

**New**
- `backend/python/migrations/mw_update_smartlock_cron.py`
- `backend/python/migrations/059_smart_lock_refresh_cooldowns.sql` (or next free number)
- `backend/python/web/routes/smart_lock_refresh.py`

**Modified**
- `backend/python/web/routes/admin.py` — add `POST /admin/api/users/<id>/access`, remove/redirect `ecri-limits` route.
- `backend/python/web/templates/admin/<user-edit>.html` — add Access section.
- `backend/python/web/templates/admin/ecri_user_limits.html` — delete after redirect lands.
- `backend/python/web/templates/tools/smart_lock_assignments.html` — refresh buttons, polling JS, site selector now hits `/api/me/sites`.
- `backend/python/web/routes/api.py` — add `GET /api/me/sites`, add `can_see_site` guards on smart lock endpoints; add `GET /api/sites` if not present.
- `backend/python/sync_service/pipelines/igloo.py` — accept `site_ids`.
- `backend/python/sync_service/pipelines/igloo_pin_sync.py` — accept `site_ids`.
- `backend/python/sync_service/pipelines/ccws_units_info.py` — accept `site_ids` → `location_codes`.
- `backend/python/sync_service/pipelines/ccws_gate_access.py` — accept `site_ids` → `location_codes`.
- `backend/python/app.py` — register new blueprint.
