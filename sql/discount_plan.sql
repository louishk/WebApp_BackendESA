-- View: discount_plan
-- Normalizes discount plan names from mimo.sDiscountPlanAtMoveIn into unified categories.
-- Target database: esa_pbi

CREATE OR REPLACE VIEW discount_plan AS
SELECT DISTINCT mimo."sDiscountPlanAtMoveIn" AS discount_plan_name,
    CASE
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%$1 move%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%$1move%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) = '$ move in' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '$1 %' OR mimo."sDiscountPlanAtMoveIn"::text ~~ '$1 dollar%' OR mimo."sDiscountPlanAtMoveIn"::text ~~ '$1 speical%' THEN '$1 Move-In'::character varying
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ 'rm $1%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ 'rm1 move%' THEN 'RM1 Move-In'::character varying
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%₩1,000 move%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%kw1,000%' THEN 'KRW 1,000 Move-In'::character varying
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%kr10,000%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%₩10,000%' THEN 'KRW 10,000 Move-In'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '^[0-9]+% discount - 12' THEN regexp_replace(mimo."sDiscountPlanAtMoveIn"::text, '^([0-9]+)%.*', '\1% Discount - 12 Months')::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '^[0-9]+% discount - 6' THEN regexp_replace(mimo."sDiscountPlanAtMoveIn"::text, '^([0-9]+)%.*', '\1% Discount - 6 Months')::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '^[0-9]+% discount - 3' THEN regexp_replace(mimo."sDiscountPlanAtMoveIn"::text, '^([0-9]+)%.*', '\1% Discount - 3 Months')::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '^[0-9]+% discount - 1' THEN regexp_replace(mimo."sDiscountPlanAtMoveIn"::text, '^([0-9]+)%.*', '\1% Discount - 1 Month')::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '^[0-9]+% discount$' THEN regexp_replace(mimo."sDiscountPlanAtMoveIn"::text, '^([0-9]+)%.*', '\1% Discount')::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '^[0-9]+% recurring' THEN regexp_replace(mimo."sDiscountPlanAtMoveIn"::text, '^([0-9]+)%.*', '\1% Recurring Discount')::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '^[0-9]+% off' THEN regexp_replace(mimo."sDiscountPlanAtMoveIn"::text, '^([0-9]+)%.*', '\1% Discount')::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '^enjoy [0-9]+%' THEN regexp_replace(mimo."sDiscountPlanAtMoveIn"::text, '.*?([0-9]+)%.*', '\1% Discount')::character varying
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%1 month free%' THEN '1 Month Free'::character varying
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%3 months free%' THEN '3 Months Free'::character varying
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%6 months free%' THEN '6 Months Free'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text = '12th Month Free' THEN '12th Month Free'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text = '13th Month Free' THEN '13th Month Free'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text = '5th Month Free' THEN '5th Month Free'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ 'Free 3rd Month' THEN '3rd Month Free'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ '2 months + 1 month free' THEN '1 Month Free'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '^[0-9]+th month 50%' THEN regexp_replace(mimo."sDiscountPlanAtMoveIn"::text, '^([0-9]+)th.*', '\1th Month 50%')::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ '13th month 50%' THEN '13th Month 50%'::character varying
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%complementary%' THEN 'Complementary 1 Month'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ 'Chuseok 1 Month Free' THEN 'Chuseok 1 Month Free'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ 'Chuseok 3 Months Free' THEN 'Chuseok 3 Months Free'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ 'Prepaied 12mths%' OR mimo."sDiscountPlanAtMoveIn"::text ~~ 'Prepaid 12%' THEN 'Prepaid 12 Months 15%'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ 'Prepayment 6 Months%' THEN 'Prepaid 6 Months 4%'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ 'Dec Promo 12m%' THEN 'Dec Promo 12M 50%'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ 'Dec Promo 6m%' THEN 'Dec Promo 6M 25%'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ 'Dec Promo 3m%' THEN 'Dec Promo 3M 12%'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ 'Nov Promo%' THEN 'Nov Promo 11%'::character varying
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '[0-9]+% covid' THEN regexp_replace(mimo."sDiscountPlanAtMoveIn"::text, '^([0-9]+)%.*', '\1% Covid Rebate')::character varying
        ELSE mimo."sDiscountPlanAtMoveIn"
    END AS discount_plan_unified,
    CASE
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~ '^[0-9]+%' THEN "substring"(mimo."sDiscountPlanAtMoveIn"::text, '^([0-9]+)%')::numeric
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%$1 move%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%$1move%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) = '$ move in' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ 'rm $1%' OR mimo."sDiscountPlanAtMoveIn"::text ~~ '$1 dollar%' OR mimo."sDiscountPlanAtMoveIn"::text ~~ '$1 speical%' OR mimo."sDiscountPlanAtMoveIn"::text ~~ '$1 New%' THEN 100.00
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ 'rm1 move%' THEN 100.00
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%month free%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%months free%' THEN 100.00
        WHEN mimo."sDiscountPlanAtMoveIn"::text = 'Dec Promo 12m-50%' THEN 50.00
        WHEN mimo."sDiscountPlanAtMoveIn"::text = 'Dec Promo 3m-12%' THEN 12.00
        WHEN mimo."sDiscountPlanAtMoveIn"::text = 'Dec Promo 6m-25%' THEN 25.00
        WHEN mimo."sDiscountPlanAtMoveIn"::text = 'Nov Promo 11%' THEN 11.00
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '13th month 50%' THEN 50.00
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%complementary%' THEN 100.00
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ 'enjoy %' THEN "substring"(mimo."sDiscountPlanAtMoveIn"::text, '([0-9]+)%')::numeric
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ '%50% OFF%' OR mimo."sDiscountPlanAtMoveIn"::text ~~ '%50% off%' THEN 50.00
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '^[0-9]+th month 50%' THEN 50.00
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ '%Free 3rd Month%' OR mimo."sDiscountPlanAtMoveIn"::text ~~ '%free%month%' THEN 100.00
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ 'Prepaied 12mths (15%)%' THEN 15.00
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ 'Prepayment 6 Months - 4%%' THEN 4.00
        ELSE NULL::numeric
    END AS discount_pct,
    CASE
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '12 month|12m|12mth' THEN 12
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '6 month|6m' THEN 6
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '3 month|3m' THEN 3
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '1 month|1m' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%complementary%' THEN 1
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%$1 move%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) = '$ move in' THEN 1
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~* '^[0-9]+th month' THEN 1
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~~ '%Free 3rd Month%' THEN 1
        ELSE NULL::integer
    END AS discount_months,
    CASE
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%$1 move%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) = '$ move in' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ 'rm%$1%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ 'rm1 move%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%₩1,000 move%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%kw1,000%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%kr10,000%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%₩10,000%' OR mimo."sDiscountPlanAtMoveIn"::text ~~ '$1 dollar%' OR mimo."sDiscountPlanAtMoveIn"::text ~~ '$1 speical%' OR mimo."sDiscountPlanAtMoveIn"::text ~~ '$1 New%' THEN 'First Month'
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%month% free%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%free%month%' THEN 'Months Free'
        WHEN mimo."sDiscountPlanAtMoveIn"::text ~ '^[0-9]+%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ 'enjoy %' OR mimo."sDiscountPlanAtMoveIn"::text ~~ '%Recurring%' THEN 'Percentage'
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%rebate%' THEN 'Rebate'
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%referral%' THEN 'Referral'
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%waiver%' THEN 'Waiver'
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%promo%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%ndp%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%chuseok%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%merdeka%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%hari raya%' THEN 'Promo'
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%voucher%' THEN 'Voucher'
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) = 'manual' THEN 'Manual'
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%complementary%' THEN 'Complementary'
        WHEN lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%prepay%' OR lower(mimo."sDiscountPlanAtMoveIn"::text) ~~ '%prepaied%' THEN 'Prepayment'
        ELSE 'Other'
    END AS discount_type
FROM mimo
WHERE mimo."sDiscountPlanAtMoveIn" IS NOT NULL AND mimo."sDiscountPlanAtMoveIn"::text <> ''
UNION ALL
SELECT 'No Discount'::character varying AS discount_plan_name,
    'No Discount'::character varying AS discount_plan_unified,
    0.00 AS discount_pct,
    NULL::integer AS discount_months,
    'None' AS discount_type;
