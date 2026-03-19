-- Migration 036: mimo_enriched — add UnitName_clean, is_transfer, fix mimo_id
-- Target database: esa_pbi
-- Adds:
--   UnitName_clean: strips * and trailing X for clean joins to units_info
--   is_transfer: flags transfer journal entries (MoveIn=0 AND MoveOut=0)
--   mimo_id: now uses cleaned UnitName for accurate joins

DROP VIEW IF EXISTS mimo_enriched;

CREATE OR REPLACE VIEW mimo_enriched AS
 SELECT m."SiteID",
    m."TenantID",
    m."MoveDate",
    m.extract_date,
    m."MoveIn",
    m."MoveOut",
    m."Transfer",
    m."UnitName",
    m."UnitSize",
    m."Width",
    m."Length",
    m."sUnitType",
    m."TenantName",
    m."sCompany",
    m."sEmail",
    lower(TRIM(BOTH FROM m."sEmail")) AS tenant_email,
    m."Address",
    m."City",
    m."Region",
    m."PostalCode",
    m."Country",
    m."StandardRate",
    m."MovedInRentalRate",
    m."MovedInVariance",
    m."MovedInDaysVacant",
    m."MovedOutRentalRate",
    m."MovedOutVariance",
    m."MovedOutDaysRented",
    m."iLeaseNum",
    m."dRentLastChanged",
    m."sLicPlate",
    m."sEmpInitials",
    m."sPlanTerm",
    m."dcInsurPremium",
    m."dcDiscount",
    m."sDiscountPlan",
    m."iAuctioned",
    m."sAuctioned",
    m."iDaysSinceMoveOut",
    m."dcAmtPaid",
    m."sSource",
    m."bPower",
    m."bClimate",
    m."bAlarm",
    m."bInside",
    m."dcPushRateAtMoveIn",
    m."dcStdRateAtMoveIn",
    m."dcInsurPremiumAtMoveIn",
    m."sDiscountPlanAtMoveIn",
    m."WaitingID",
    m."InquiryEmployeeID",
    m."sInquiryPlacedBy",
    m."CorpUserID_Placed",
    m."CorpUserID_ConvertedToMoveIn",
    m.created_at,
    m.updated_at,
    m."MoveDate"::date AS date_fixed,
        CASE
            WHEN m."SiteID" = ANY (ARRAY[2276, 24411, 25675, 29064, 33881, 39284, 40100]) THEN m."MovedInArea" * 10.7639
            ELSE m."MovedInArea"
        END AS "MovedInArea_fixed",
        CASE
            WHEN m."SiteID" = ANY (ARRAY[2276, 24411, 25675, 29064, 33881, 39284, 40100]) THEN m."MovedOutArea" * 10.7639
            ELSE m."MovedOutArea"
        END AS "MovedOutArea_fixed",
        CASE
            WHEN m."SiteID" = ANY (ARRAY[26710, 32663]) THEN m."MovedInRentalRate" / COALESCE(fx_hkd.avg_rate, 1::numeric)
            WHEN m."SiteID" = ANY (ARRAY[4183, 10419, 10777, 44449]) THEN m."MovedInRentalRate" / COALESCE(fx_myr.avg_rate, 1::numeric)
            WHEN m."SiteID" = ANY (ARRAY[2276, 24411, 25675, 29064, 33881, 39284, 40100]) THEN m."MovedInRentalRate" / COALESCE(fx_krw.avg_rate, 1::numeric)
            ELSE m."MovedInRentalRate"
        END AS "MovedInRentalRate_SGD",
        CASE
            WHEN m."SiteID" = ANY (ARRAY[26710, 32663]) THEN m."MovedOutRentalRate" / COALESCE(fx_hkd.avg_rate, 1::numeric)
            WHEN m."SiteID" = ANY (ARRAY[4183, 10419, 10777, 44449]) THEN m."MovedOutRentalRate" / COALESCE(fx_myr.avg_rate, 1::numeric)
            WHEN m."SiteID" = ANY (ARRAY[2276, 24411, 25675, 29064, 33881, 39284, 40100]) THEN m."MovedOutRentalRate" / COALESCE(fx_krw.avg_rate, 1::numeric)
            ELSE m."MovedOutRentalRate"
        END AS "MovedOutRentalRate_SGD",
    m."MovedInArea",
    m."MovedOutArea",
    REGEXP_REPLACE(REPLACE(m."UnitName", '*', ''), 'X$', '') AS "UnitName_clean",
    (m."MoveIn" = 0 AND m."MoveOut" = 0) AS is_transfer,
    (m."SiteID"::text || '_'::text) || REGEXP_REPLACE(REPLACE(m."UnitName", '*', ''), 'X$', '')::text AS mimo_id
   FROM mimo m
     LEFT JOIN fx_rates_monthly fx_hkd ON to_char(m."MoveDate", 'YYYY-MM'::text) = fx_hkd.year_month::text AND fx_hkd.target_currency::text = 'HKD'::text
     LEFT JOIN fx_rates_monthly fx_myr ON to_char(m."MoveDate", 'YYYY-MM'::text) = fx_myr.year_month::text AND fx_myr.target_currency::text = 'MYR'::text
     LEFT JOIN fx_rates_monthly fx_krw ON to_char(m."MoveDate", 'YYYY-MM'::text) = fx_krw.year_month::text AND fx_krw.target_currency::text = 'KRW'::text;
