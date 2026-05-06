-- Migration 055: ECRI advance-scheduling ("Pre-Load Batch") support
-- Target database: esa_pbi
--
-- Adds a batch_type discriminator to ecri_batches and three per-ledger columns
-- that are only populated on advance batches (recent_movein / heavy_prepayer).
-- Also creates the eligibility view feeding the new UI + API.
--
-- See plan: /home/louis/.claude/plans/possibliyl-some-manula-action-vectorized-willow.md

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. ecri_batches: discriminator
-- ---------------------------------------------------------------------------
ALTER TABLE ecri_batches
    ADD COLUMN IF NOT EXISTS batch_type VARCHAR(16) NOT NULL DEFAULT 'standard';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE table_name = 'ecri_batches' AND constraint_name = 'ecri_batches_batch_type_check'
    ) THEN
        ALTER TABLE ecri_batches
            ADD CONSTRAINT ecri_batches_batch_type_check
            CHECK (batch_type IN ('standard', 'advance'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_ecri_batches_batch_type ON ecri_batches(batch_type);

-- ---------------------------------------------------------------------------
-- 2. ecri_batch_ledgers: advance-scheduling snapshot columns
-- ---------------------------------------------------------------------------
ALTER TABLE ecri_batch_ledgers
    ADD COLUMN IF NOT EXISTS segment             VARCHAR(20),
    ADD COLUMN IF NOT EXISTS projected_paid_thru DATE,
    ADD COLUMN IF NOT EXISTS discount_expires    DATE;

CREATE INDEX IF NOT EXISTS idx_ecri_bl_segment ON ecri_batch_ledgers(batch_id, segment);

-- ---------------------------------------------------------------------------
-- 3. Advance-eligibility view (UNION of segment A + B)
--
--    Segment A — recent move-ins (tenure < 12mo) whose move-in discount
--    expires within the next 90 days. Discount expiration is derived from
--    discount.dChgStrt + ccws_discount.iExpirMonths (only rows where the
--    plan has bNeverExpires = FALSE and iExpirMonths > 0).
--
--    Segment B — heavy prepayers whose dPaidThru is > 60 days from today.
--
--    A ledger may appear in BOTH segments (the batch-creation code must
--    dedupe before insert). The unique (batch_id, site_id, ledger_id)
--    constraint on ecri_batch_ledgers enforces dedup at write time.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_ecri_advance_eligible_ledgers AS
WITH base AS (
    -- Reuse the existing eligibility view so active/sched-out/pending-increase
    -- filters stay consistent with the standard batch flow. Intentionally do
    -- NOT filter by tenure here — recent move-ins are the point of segment A.
    SELECT
        l."SiteID",
        l."LedgerID",
        l."TenantID",
        l."UnitID",
        l."TenantName",
        l."sUnit"        AS unit_name,
        l."sTypeName"    AS unit_type,
        l."dMovedIn",
        l."dPaidThru"::date AS paid_thru,
        l."dAnniv",
        l."dcRent"       AS current_rent,
        l."dcStdRate"    AS std_rate,
        l."dRentLastChanged",
        l."dSchedRentStrt",
        l."dSchedOut"
    FROM vw_ecri_eligible_ledgers l
    WHERE l."dMovedIn" IS NOT NULL
      AND (l."dSchedOut" IS NULL OR l."dSchedOut" > CURRENT_DATE + INTERVAL '30 days')
      AND (l."dSchedRentStrt" IS NULL OR l."dSchedRentStrt" < CURRENT_DATE)
),
disc_live AS (
    -- Latest-snapshot discount rows; strip " : Non-Expiring"-style suffixes so
    -- we can match sPlanName against ccws_discount. Exclude plans explicitly
    -- tagged as non-expiring/permanent in their display string.
    SELECT
        d."SiteID",
        d."sUnitName",
        d."dChgStrt",
        TRIM(SPLIT_PART(d."sConcessionPlan", ':', 1)) AS plan_name_guess
    FROM discount d
    WHERE d.extract_date = (SELECT MAX(extract_date) FROM discount)
      AND d."sConcessionPlan" IS NOT NULL
      AND d."sConcessionPlan" <> ''
      AND d."sConcessionPlan" NOT ILIKE '%Non-Expiring%'
      AND d."sConcessionPlan" NOT ILIKE '%Permanent%'
      AND d."dChgStrt" IS NOT NULL
),
disc_resolved AS (
    SELECT DISTINCT ON (dl."SiteID", dl."sUnitName")
        dl."SiteID",
        dl."sUnitName",
        dl."dChgStrt",
        cd."iExpirMonths",
        (dl."dChgStrt"::date + (cd."iExpirMonths" || ' months')::INTERVAL)::date
            AS discount_expires
    FROM disc_live dl
    JOIN ccws_discount cd
      ON cd."SiteID"        = dl."SiteID"
     AND cd."sPlanName"     = dl.plan_name_guess
     AND cd."bNeverExpires" = FALSE
     AND COALESCE(cd."iExpirMonths", 0) > 0
    ORDER BY dl."SiteID", dl."sUnitName", dl."dChgStrt" DESC
),
segment_a AS (
    SELECT
        b.*,
        dr.discount_expires,
        'recent_movein'::text AS segment,
        GREATEST(
            b.paid_thru,
            dr.discount_expires + INTERVAL '30 days'
        )::date AS projected_paid_thru
    FROM base b
    JOIN disc_resolved dr
      ON dr."SiteID"    = b."SiteID"
     AND dr."sUnitName" = b.unit_name
    WHERE b."dMovedIn" > CURRENT_DATE - INTERVAL '12 months'
      AND dr.discount_expires BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '90 days'
),
segment_b AS (
    SELECT
        b.*,
        NULL::date        AS discount_expires,
        'heavy_prepayer'::text AS segment,
        b.paid_thru       AS projected_paid_thru
    FROM base b
    WHERE b.paid_thru > CURRENT_DATE + INTERVAL '60 days'
)
SELECT * FROM segment_a
UNION ALL
SELECT * FROM segment_b;

COMMENT ON VIEW vw_ecri_advance_eligible_ledgers IS
    'Advance-scheduling eligibility for ECRI Pre-Load batches. '
    'UNION of segment_a (recent move-ins with expiring discount) and '
    'segment_b (heavy prepayers paid_thru > 60d). A ledger may appear in '
    'both segments; batch-creation must dedupe on (SiteID, LedgerID).';

COMMIT;
