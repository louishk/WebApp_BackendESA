-- Recreate rentroll_enriched view (dropped by CASCADE when updating units_info_enriched)
-- Per user request: unit_range and unit_type columns removed

CREATE OR REPLACE VIEW rentroll_enriched AS
WITH rent_discounts AS (
    SELECT DISTINCT ON (discount.extract_date, discount."SiteID", discount."sUnitName")
        discount.extract_date,
        discount."SiteID",
        discount."sUnitName",
        discount."dcPrice",
        discount."dcAmt",
        discount."sDiscMemo",
        discount."dcDiscount",
        discount."sConcessionPlan"
    FROM discount
    WHERE discount."sChgDesc"::text = 'Rent'::text
    ORDER BY discount.extract_date, discount."SiteID", discount."sUnitName", discount."ChargeID" DESC
)
SELECT r.extract_date,
    r."SiteID",
    r."UnitID",
    r."LedgerID",
    r."sUnit",
    r."sSize",
    r."Area",
    r."sUnitName",
    r."UnitTypeID",
    r."sTypeName",
    r."iFloor",
    r."dcWidth",
    r."dcLength",
    r."iWalkThruOrder",
    r."iDoorType",
    r."dcMapTop",
    r."dcMapLeft",
    r."dcMapTheta",
    r."bMapReversWL",
    r."iEntryLoc",
    r."dcPushRate",
    r."dcStdRate",
    r."dcStdWeeklyRate",
    r."dcStdSecDep",
    r."dcStdLateFee",
    r."dcWebRate",
    r."dcWebPushRate",
    r."dcWebRateDated",
    r."dcSchedRateMonthly",
    r."dcSchedRateWeekly",
    r."bPower",
    r."bClimate",
    r."bInside",
    r."bAlarm",
    r."bRentable",
    r."bRented",
    r."bCorporate",
    r."bMobile",
    r."bDamaged",
    r."bCollapsible",
    r."bPermanent",
    r."bExcludeFromSqftReports",
    r."bExcludeFromWebsite",
    r."bNotReadyToRent",
    r."bExcludeFromInsurance",
    r."iMobileStatus",
    r."iADA",
    r."iVehicleStorageAllowed",
    r."iDaysVacant",
    r."EmployeeID",
    r."dCreated",
    r."dUpdated",
    r."dUnitNote",
    r."dLeaseDate",
    r."dPaidThru",
    r."dRentLastChanged",
    r."dSchedRentStrt",
    r."TenantID",
    r."sTenant",
    r."sCompany",
    r."sEmail",
    r."iAnnivDays",
    r."sTaxExempt",
    r."dcSecDep",
    r."dcStandardRate",
    r."dcRent",
    r."dcVar",
    r."dcSchedRent",
    r."dcPrePaidRentLiability",
    r."dcInsurPremium",
    r."iAutoBillType",
    r."DaysSame",
    r."SiteID1",
    r."Area1",
    r."OldPK",
    r."uTS",
    r."sUnitNote",
    r.created_at,
    r.updated_at,
    rd."dcPrice" AS disc_dcprice,
    rd."dcAmt" AS disc_dcamt,
    rd."sDiscMemo" AS disc_sdiscmemo,
    rd."dcDiscount" AS disc_dcdiscount,
    rd."sConcessionPlan" AS disc_sconcessionplan,
    COALESCE(rd."dcAmt", r."dcRent") AS disc_dcamt_adjusted,
        CASE
            WHEN r."bRented" = true AND (r."LedgerID" IS NULL OR r."LedgerID" = 0) AND r."dLeaseDate" IS NULL THEN 1
            ELSE 0
        END AS "FutureMoveins",
        CASE
            WHEN r."SiteID" = ANY (ARRAY[2276, 24411, 25675, 29064, 33881, 39284, 40100]) THEN r."Area" * 10.7639
            ELSE r."Area"
        END AS dcarea_fixed,
    r.extract_date - r."dLeaseDate"::date AS days_rented,
    los."RangeLabel" AS los_range,
        CASE
            WHEN r."SiteID" = ANY (ARRAY[26710, 32663]) THEN COALESCE(rd."dcAmt", r."dcRent") / fx_hkd.avg_rate
            WHEN r."SiteID" = ANY (ARRAY[4183, 10419, 10777, 44449]) THEN COALESCE(rd."dcAmt", r."dcRent") / fx_myr.avg_rate
            WHEN r."SiteID" = ANY (ARRAY[2276, 24411, 25675, 29064, 33881, 39284, 40100]) THEN COALESCE(rd."dcAmt", r."dcRent") / fx_krw.avg_rate
            ELSE COALESCE(rd."dcAmt", r."dcRent")
        END AS disc_dcamt_adjusted_samecurrency,
    1 AS "UnitInventory",
        CASE
            WHEN r."LedgerID" > 0 THEN 1
            ELSE 0
        END AS "Unitrented",
        CASE
            WHEN r."LedgerID" > 0 THEN
            CASE
                WHEN r."SiteID" = ANY (ARRAY[2276, 24411, 25675, 29064, 33881, 39284, 40100]) THEN r."Area" * 10.7639
                ELSE r."Area"
            END
            ELSE 0::numeric
        END AS "Arearented",
        CASE
            WHEN r."LedgerID" > 0 AND COALESCE(r."dcInsurPremium", 0::numeric) > 0::numeric THEN 1
            ELSE 0
        END AS "Unitrented_withinsurance",
        CASE
            WHEN r."LedgerID" > 0 AND COALESCE(r."iAutoBillType", 0) = 1 THEN 1
            ELSE 0
        END AS "Unitrented_withautobill",
        CASE
            WHEN r."LedgerID" > 0 AND COALESCE(r."dcInsurPremium", 0::numeric) > 0::numeric THEN r."dcInsurPremium"
            ELSE 0::numeric
        END AS "InsuranceRevenue",
        CASE
            WHEN r."LedgerID" > 0 AND COALESCE(r."dcInsurPremium", 0::numeric) > 0::numeric THEN
            CASE
                WHEN r."SiteID" = ANY (ARRAY[26710, 32663]) THEN r."dcInsurPremium" / fx_hkd.avg_rate
                WHEN r."SiteID" = ANY (ARRAY[4183, 10419, 10777, 44449]) THEN r."dcInsurPremium" / fx_myr.avg_rate
                WHEN r."SiteID" = ANY (ARRAY[2276, 24411, 25675, 29064, 33881, 39284, 40100]) THEN r."dcInsurPremium" / fx_krw.avg_rate
                ELSE r."dcInsurPremium"
            END
            ELSE 0::numeric
        END AS "InsuranceRevenue_sgd",
    COALESCE(rd."dcAmt", r."dcRent") AS revenue_effective,
        CASE
            WHEN r."SiteID" = ANY (ARRAY[26710, 32663]) THEN COALESCE(rd."dcAmt", r."dcRent") / fx_hkd.avg_rate
            WHEN r."SiteID" = ANY (ARRAY[4183, 10419, 10777, 44449]) THEN COALESCE(rd."dcAmt", r."dcRent") / fx_myr.avg_rate
            WHEN r."SiteID" = ANY (ARRAY[2276, 24411, 25675, 29064, 33881, 39284, 40100]) THEN COALESCE(rd."dcAmt", r."dcRent") / fx_krw.avg_rate
            ELSE COALESCE(rd."dcAmt", r."dcRent")
        END AS revenue_effective_sgd
   FROM rentroll r
     LEFT JOIN rent_discounts rd ON r.extract_date = rd.extract_date AND r."SiteID" = rd."SiteID" AND r."sUnit"::text = rd."sUnitName"::text
     LEFT JOIN fx_rates_monthly fx_hkd ON to_char(r.extract_date::timestamp with time zone, 'YYYY-MM') = fx_hkd.year_month::text AND fx_hkd.target_currency::text = 'HKD'
     LEFT JOIN fx_rates_monthly fx_myr ON to_char(r.extract_date::timestamp with time zone, 'YYYY-MM') = fx_myr.year_month::text AND fx_myr.target_currency::text = 'MYR'
     LEFT JOIN fx_rates_monthly fx_krw ON to_char(r.extract_date::timestamp with time zone, 'YYYY-MM') = fx_krw.year_month::text AND fx_krw.target_currency::text = 'KRW'
     LEFT JOIN site_revenue_config cfg ON r."SiteID" = cfg.siteid
     LEFT JOIN losrange los ON (r.extract_date - r."dLeaseDate"::date) >= los."RangeMin" AND (r.extract_date - r."dLeaseDate"::date) < los."RangeMax";
