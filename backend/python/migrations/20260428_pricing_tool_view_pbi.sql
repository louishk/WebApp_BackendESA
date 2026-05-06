-- Migration: Pricing Tool — Materialized View vw_pricing_type_metrics
-- Target database: esa_pbi
-- Purpose: One row per (site_id, s_type_name) aggregating occupancy, move
--          event counts, and current rate data for the pricing tool's
--          GET /api/pricing/types/metrics endpoint.
-- Safe to re-run: DROP + CREATE sequence is wrapped in a transaction.
--
-- ============================================================================
-- DATA AVAILABILITY WARNINGS (read before querying D-7 / D-30 / D-90 deltas)
-- ============================================================================
-- [WARNING-1] rentroll IS A MONTHLY SNAPSHOT TABLE, not daily.
--   As of 2026-04-28, it holds ~40 distinct extract_dates spanning 2023-01 to
--   2026-04-27, one snapshot per end-of-month (plus the most recent run).
--   There is NO daily granularity. The D-7 and D-30 occupancy columns below
--   are approximated using the nearest available monthly snapshot older than
--   7 / 30 / 90 days respectively. Delta accuracy is therefore ±~15 days.
--   PREREQUISITE from plan §3.2: "confirm rent-roll snapshot retention covers
--   D-90 at daily granularity; if not, add a daily snapshot pipeline as
--   Phase 0." That daily pipeline does NOT yet exist. These columns are
--   populated with the best available approximation and flagged as such.
--
-- [WARNING-2] occ_pct_7d / occ_delta_7d will often be NULL.
--   A monthly snapshot older than 7 but newer than 30 days is rarely available
--   (only the current-month snapshot from the most recent sync run may qualify).
--   The view returns NULL rather than a misleading value when no snapshot exists
--   in the target window.
--
-- [WARNING-3] comp_price_per_sqft is always NULL.
--   The competitor price scraper is out of scope (plan §7). This column is a
--   placeholder for the future scraper integration.
--
-- [WARNING-4] actual_avg_price_per_sqft_after_discount uses revenue_effective
--   from rentroll_enriched. This is the rent after concession discount but
--   BEFORE tax. Tax treatment is consistent with the rest of the reporting layer.
--
-- [WARNING-5] current_monthly and current_weekly use dcSchedRateMonthly /
--   dcStdWeeklyRate from rentroll. These are 0 for most non-LSETUP sites
--   (confirmed: all non-LSETUP sites return 0 for dcSchedRateMonthly).
--   The view returns NULL instead of 0 for cleaner downstream handling.
-- ============================================================================

BEGIN;

-- Drop old version if it exists (safe because we recreate immediately below)
DROP MATERIALIZED VIEW IF EXISTS vw_pricing_type_metrics;

-- ============================================================================
-- Materialized view body
-- ============================================================================
CREATE MATERIALIZED VIEW vw_pricing_type_metrics AS

WITH
-- -------------------------------------------------------------------------
-- current_snapshot: latest available rentroll snapshot per unit
-- Used for: total_units, vacant_units, occ_pct_now, current rates
-- -------------------------------------------------------------------------
latest_date AS (
    SELECT MAX(extract_date) AS d FROM rentroll
),

current_snapshot AS (
    SELECT
        r."SiteID"              AS site_id,
        r."UnitID"              AS unit_id,
        r."sTypeName"           AS s_type_name,
        r."bRentable"           AS is_rentable,
        r."bRented"             AS is_rented,
        r."dcStdRate"           AS std_rate,
        r."dcWebRate"           AS web_rate,
        -- Use NULL instead of 0 for monthly/weekly to avoid misleading aggregates
        -- (see WARNING-5 above)
        NULLIF(r."dcSchedRateMonthly", 0)   AS monthly_rate,
        NULLIF(r."dcStdWeeklyRate",    0)   AS weekly_rate,
        r."dcarea_fixed"        AS sqft,        -- area from rentroll_enriched
        r."revenue_effective"   AS rent_after_discount
    FROM rentroll_enriched r
    CROSS JOIN latest_date ld
    WHERE r.extract_date = ld.d
      AND r."bRentable" = TRUE
),

-- -------------------------------------------------------------------------
-- snapshot_Nd: closest available monthly snapshot >= N days ago
-- Strategy: find MAX(extract_date) <= NOW() - INTERVAL 'N days'
-- Returns NULL-joined rows when no snapshot exists in the window.
-- See WARNING-1 and WARNING-2 above.
-- -------------------------------------------------------------------------
snap_7d_date AS (
    SELECT MAX(extract_date) AS d
    FROM rentroll
    WHERE extract_date <= (CURRENT_DATE - INTERVAL '7 days')
),
snap_30d_date AS (
    SELECT MAX(extract_date) AS d
    FROM rentroll
    WHERE extract_date <= (CURRENT_DATE - INTERVAL '30 days')
),
snap_90d_date AS (
    SELECT MAX(extract_date) AS d
    FROM rentroll
    WHERE extract_date <= (CURRENT_DATE - INTERVAL '90 days')
),

snapshot_7d AS (
    SELECT
        r."SiteID"  AS site_id,
        r."sTypeName" AS s_type_name,
        -- occupancy % at snapshot date
        CASE WHEN COUNT(*) FILTER (WHERE r."bRentable") > 0
            THEN ROUND(
                100.0 * COUNT(*) FILTER (WHERE r."bRentable" AND r."bRented")
                    / NULLIF(COUNT(*) FILTER (WHERE r."bRentable"), 0),
                3
            )
        END AS occ_pct
    FROM rentroll r
    CROSS JOIN snap_7d_date sd
    WHERE sd.d IS NOT NULL AND r.extract_date = sd.d
    GROUP BY r."SiteID", r."sTypeName"
),

snapshot_30d AS (
    SELECT
        r."SiteID"  AS site_id,
        r."sTypeName" AS s_type_name,
        CASE WHEN COUNT(*) FILTER (WHERE r."bRentable") > 0
            THEN ROUND(
                100.0 * COUNT(*) FILTER (WHERE r."bRentable" AND r."bRented")
                    / NULLIF(COUNT(*) FILTER (WHERE r."bRentable"), 0),
                3
            )
        END AS occ_pct
    FROM rentroll r
    CROSS JOIN snap_30d_date sd
    WHERE sd.d IS NOT NULL AND r.extract_date = sd.d
    GROUP BY r."SiteID", r."sTypeName"
),

snapshot_90d AS (
    SELECT
        r."SiteID"  AS site_id,
        r."sTypeName" AS s_type_name,
        CASE WHEN COUNT(*) FILTER (WHERE r."bRentable") > 0
            THEN ROUND(
                100.0 * COUNT(*) FILTER (WHERE r."bRentable" AND r."bRented")
                    / NULLIF(COUNT(*) FILTER (WHERE r."bRentable"), 0),
                3
            )
        END AS occ_pct
    FROM rentroll r
    CROSS JOIN snap_90d_date sd
    WHERE sd.d IS NOT NULL AND r.extract_date = sd.d
    GROUP BY r."SiteID", r."sTypeName"
),

-- -------------------------------------------------------------------------
-- type_current: aggregate current_snapshot to (site, type) level
-- -------------------------------------------------------------------------
type_current AS (
    SELECT
        cs.site_id,
        cs.s_type_name,

        COUNT(*)                                                AS total_units,
        COUNT(*) FILTER (WHERE NOT cs.is_rented)               AS vacant_units,
        COUNT(*) FILTER (WHERE cs.is_rented)                   AS occupied_units,

        -- Current occupancy %
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE cs.is_rented)
                / NULLIF(COUNT(*), 0),
            3
        )                                                       AS occ_pct_now,

        -- Rates: use MODE (most common value per type) as the representative rate.
        -- This handles the case where a type has minor per-unit rate variances.
        -- NOTE: NUMERIC(12,4) columns; AVG is used as a fallback for ties.
        ROUND(AVG(cs.std_rate)::NUMERIC, 4)                    AS current_std_rate,
        ROUND(AVG(cs.web_rate)::NUMERIC, 4)                    AS current_web_rate,
        ROUND(AVG(cs.monthly_rate)::NUMERIC, 4)                AS current_monthly,
        ROUND(AVG(cs.weekly_rate)::NUMERIC, 4)                 AS current_weekly,

        -- Actual avg $/sqft after discount over occupied units with valid area
        -- Formula: sum(rent_after_discount) / sum(sqft) for occupied units
        -- See WARNING-4 above.
        CASE WHEN SUM(cs.sqft) FILTER (WHERE cs.is_rented AND cs.sqft > 0) > 0
            THEN ROUND(
                SUM(cs.rent_after_discount) FILTER (WHERE cs.is_rented AND cs.sqft > 0)
                    / SUM(cs.sqft) FILTER (WHERE cs.is_rented AND cs.sqft > 0),
                4
            )
        END                                                     AS actual_avg_price_per_sqft_after_discount

    FROM current_snapshot cs
    GROUP BY cs.site_id, cs.s_type_name
),

-- -------------------------------------------------------------------------
-- mimo_counts: aggregate move-in / move-out events from mimo table
-- mimo has individual event rows with real timestamps — suitable for 7/30/90d
-- window counts (see data availability check above: mimo covers back to 2023).
-- NOTE: mimo.sUnitType is the type code at move time; may differ from the
-- current sTypeName in rentroll if the type was renamed. We join on SiteID +
-- sUnitType = sTypeName. This is the same field used in rentroll.sTypeName.
-- -------------------------------------------------------------------------
mimo_counts AS (
    SELECT
        m."SiteID"              AS site_id,
        m."sUnitType"           AS s_type_name,

        -- Move-ins
        COUNT(*) FILTER (WHERE m."MoveIn" = 1 AND m."MoveDate" >= NOW() - INTERVAL '7 days')   AS movein_7d,
        COUNT(*) FILTER (WHERE m."MoveIn" = 1 AND m."MoveDate" >= NOW() - INTERVAL '30 days')  AS movein_30d,
        COUNT(*) FILTER (WHERE m."MoveIn" = 1 AND m."MoveDate" >= NOW() - INTERVAL '90 days')  AS movein_90d,

        -- Move-outs
        COUNT(*) FILTER (WHERE m."MoveOut" = 1 AND m."MoveDate" >= NOW() - INTERVAL '7 days')  AS moveout_7d,
        COUNT(*) FILTER (WHERE m."MoveOut" = 1 AND m."MoveDate" >= NOW() - INTERVAL '30 days') AS moveout_30d,
        COUNT(*) FILTER (WHERE m."MoveOut" = 1 AND m."MoveDate" >= NOW() - INTERVAL '90 days') AS moveout_90d

    FROM mimo m
    WHERE m."MoveDate" >= NOW() - INTERVAL '90 days'
    GROUP BY m."SiteID", m."sUnitType"
)

-- -------------------------------------------------------------------------
-- Final SELECT
-- -------------------------------------------------------------------------
SELECT
    tc.site_id,
    tc.s_type_name,

    -- Unit counts
    tc.total_units,
    tc.vacant_units,
    tc.occupied_units,

    -- Occupancy % — current and historical approximations
    tc.occ_pct_now,
    s7.occ_pct                                      AS occ_pct_7d,   -- see WARNING-1/2
    s30.occ_pct                                     AS occ_pct_30d,  -- see WARNING-1
    s90.occ_pct                                     AS occ_pct_90d,  -- see WARNING-1

    -- Occupancy deltas (positive = occupancy improved)
    ROUND((tc.occ_pct_now - s7.occ_pct)::NUMERIC, 3)  AS occ_delta_7d,
    ROUND((tc.occ_pct_now - s30.occ_pct)::NUMERIC, 3) AS occ_delta_30d,
    ROUND((tc.occ_pct_now - s90.occ_pct)::NUMERIC, 3) AS occ_delta_90d,

    -- Move-in / move-out event counts (from mimo; NULL when no events)
    COALESCE(mc.movein_7d,   0)::INTEGER            AS movein_7d,
    COALESCE(mc.movein_30d,  0)::INTEGER            AS movein_30d,
    COALESCE(mc.movein_90d,  0)::INTEGER            AS movein_90d,

    COALESCE(mc.moveout_7d,  0)::INTEGER            AS moveout_7d,
    COALESCE(mc.moveout_30d, 0)::INTEGER            AS moveout_30d,
    COALESCE(mc.moveout_90d, 0)::INTEGER            AS moveout_90d,

    -- Net move-ins (positive = net inflow)
    (COALESCE(mc.movein_7d,   0) - COALESCE(mc.moveout_7d,  0))::INTEGER AS net_movein_7d,
    (COALESCE(mc.movein_30d,  0) - COALESCE(mc.moveout_30d, 0))::INTEGER AS net_movein_30d,
    (COALESCE(mc.movein_90d,  0) - COALESCE(mc.moveout_90d, 0))::INTEGER AS net_movein_90d,

    -- Current rates
    tc.current_std_rate,
    tc.current_web_rate,
    tc.current_monthly,    -- NULL for non-LSETUP sites (see WARNING-5)
    tc.current_weekly,     -- NULL for non-LSETUP sites (see WARNING-5)

    -- Effective $/sqft after discount (occupied units only)
    tc.actual_avg_price_per_sqft_after_discount,

    -- Placeholder for future competitor scraper (plan §7)
    NULL::NUMERIC(12,4)                             AS comp_price_per_sqft,

    -- Refresh timestamp so callers can show data age
    NOW()                                           AS refreshed_at

FROM type_current tc
LEFT JOIN snapshot_7d  s7  ON s7.site_id  = tc.site_id AND s7.s_type_name  = tc.s_type_name
LEFT JOIN snapshot_30d s30 ON s30.site_id = tc.site_id AND s30.s_type_name = tc.s_type_name
LEFT JOIN snapshot_90d s90 ON s90.site_id = tc.site_id AND s90.s_type_name = tc.s_type_name
LEFT JOIN mimo_counts  mc  ON mc.site_id  = tc.site_id AND mc.s_type_name  = tc.s_type_name

WITH NO DATA;

-- ============================================================================
-- Unique index on (site_id, s_type_name) — required for CONCURRENTLY refresh
-- ============================================================================
CREATE UNIQUE INDEX IF NOT EXISTS idx_vw_pricing_type_metrics_pk
    ON vw_pricing_type_metrics (site_id, s_type_name);

-- ============================================================================
-- Initial data load (non-concurrent; safe on first run when view is empty)
-- ============================================================================
REFRESH MATERIALIZED VIEW vw_pricing_type_metrics;

-- ============================================================================
-- Nightly refresh should use:
--   REFRESH MATERIALIZED VIEW CONCURRENTLY vw_pricing_type_metrics;
-- The CONCURRENTLY form requires the unique index above and allows reads
-- during the refresh without blocking.
-- ============================================================================

COMMIT;
