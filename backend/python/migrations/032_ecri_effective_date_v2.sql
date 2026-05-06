-- Migration 032: ECRI effective date v2 — add billing-cycle columns + backfill Batch1
-- Target database: esa_pbi
--
-- Adds paid_thru_date, next_lad, bucket to ecri_batch_ledgers, then backfills
-- Batch1 (the only existing batch) using the billing-cycle-aware algorithm.
--
-- Algorithm per tenant:
--   earliest_effective = MAX(today + 14, COALESCE(dPaidThru::date, today) + 1)
--   next_lad           = next date >= today where day-of-month == dAnniv.day (month-end clamped)
--   next_bgd           = next_lad - 14
--   IF earliest_effective < next_bgd → green  (use earliest_effective)
--   ELSE push one more month          → red    (target_bgd - 1)

BEGIN;

ALTER TABLE ecri_batch_ledgers
    ADD COLUMN IF NOT EXISTS paid_thru_date DATE,
    ADD COLUMN IF NOT EXISTS next_lad       DATE,
    ADD COLUMN IF NOT EXISTS bucket         VARCHAR(10);

-- Backfill Batch1
WITH today AS (
    SELECT CURRENT_DATE AS d
),
anniv_data AS (
    SELECT
        bl.id                                               AS bl_id,
        cw."dPaidThru"::date                               AS paid_thru,
        cw."dAnniv"::date                                  AS anniv,
        EXTRACT(DAY FROM cw."dAnniv")::int                 AS anniv_day
    FROM ecri_batch_ledgers bl
    JOIN ccws_ledgers cw
      ON cw."SiteID"   = bl.site_id
     AND cw."LedgerID" = bl.ledger_id
    WHERE bl.batch_id = '5d3f4e45-f6ae-4232-a544-8b44f5750a4c'
),
computed AS (
    SELECT
        ad.bl_id,
        ad.paid_thru,
        ad.anniv,
        ad.anniv_day,
        (SELECT d FROM today)                                                           AS today,

        -- earliest_effective = MAX(today + 14, COALESCE(paid_thru, today) + 1)
        GREATEST(
            (SELECT d FROM today) + INTERVAL '14 days',
            COALESCE(ad.paid_thru, (SELECT d FROM today)) + INTERVAL '1 day'
        )::date                                                                         AS earliest_effective,

        -- this_month_lad: anniv_day clamped to last day of current month
        make_date(
            EXTRACT(YEAR FROM (SELECT d FROM today))::int,
            EXTRACT(MONTH FROM (SELECT d FROM today))::int,
            LEAST(
                ad.anniv_day,
                DATE_PART('days', DATE_TRUNC('month', (SELECT d FROM today)) + INTERVAL '1 month - 1 day')::int
            )
        )                                                                               AS this_month_lad
    FROM anniv_data ad
),
with_next_lad AS (
    SELECT
        c.*,
        -- If this_month_lad >= today use it, else advance to next month (clamped)
        CASE
            WHEN c.this_month_lad >= c.today THEN c.this_month_lad
            ELSE make_date(
                EXTRACT(YEAR FROM (c.today + INTERVAL '1 month'))::int,
                EXTRACT(MONTH FROM (c.today + INTERVAL '1 month'))::int,
                LEAST(
                    c.anniv_day,
                    DATE_PART('days',
                        DATE_TRUNC('month', c.today + INTERVAL '1 month')
                        + INTERVAL '1 month - 1 day'
                    )::int
                )
            )
        END                                                                             AS next_lad
    FROM computed c
),
with_dates AS (
    SELECT
        w.*,
        (w.next_lad - INTERVAL '14 days')::date                                        AS next_bgd,

        -- next_next_lad = next_lad + 1 month (month-end clamped)
        make_date(
            EXTRACT(YEAR FROM (w.next_lad + INTERVAL '1 month'))::int,
            EXTRACT(MONTH FROM (w.next_lad + INTERVAL '1 month'))::int,
            LEAST(
                w.anniv_day,
                DATE_PART('days',
                    DATE_TRUNC('month', w.next_lad + INTERVAL '1 month')
                    + INTERVAL '1 month - 1 day'
                )::int
            )
        )                                                                               AS next_next_lad
    FROM with_next_lad w
),
final AS (
    SELECT
        d.bl_id,
        d.paid_thru                                                                     AS paid_thru_date,
        d.next_lad,
        CASE
            WHEN d.anniv IS NULL THEN 'unknown'
            WHEN d.earliest_effective < d.next_bgd THEN
                CASE
                    WHEN d.paid_thru IS NOT NULL AND d.paid_thru > d.today THEN 'amber'
                    ELSE 'green'
                END
            ELSE 'red'
        END                                                                             AS bucket,
        CASE
            WHEN d.anniv IS NULL THEN d.earliest_effective
            WHEN d.earliest_effective < d.next_bgd THEN d.earliest_effective
            -- red: push to next cycle: target_bgd - 1
            ELSE ((d.next_next_lad - INTERVAL '14 days')::date - INTERVAL '1 day')::date
        END                                                                             AS effective_date,
        CASE
            WHEN d.anniv IS NULL THEN (d.earliest_effective - INTERVAL '14 days')::date
            WHEN d.earliest_effective < d.next_bgd THEN (d.earliest_effective - INTERVAL '14 days')::date
            ELSE (((d.next_next_lad - INTERVAL '14 days')::date - INTERVAL '1 day') - INTERVAL '14 days')::date
        END                                                                             AS notice_date
    FROM with_dates d
)
UPDATE ecri_batch_ledgers bl
SET
    paid_thru_date = f.paid_thru_date,
    next_lad       = f.next_lad,
    bucket         = f.bucket,
    effective_date = f.effective_date,
    notice_date    = f.notice_date
FROM final f
WHERE bl.id = f.bl_id;

COMMIT;
