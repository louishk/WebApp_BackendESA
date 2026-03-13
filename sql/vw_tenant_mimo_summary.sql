-- View: vw_tenant_mimo_summary
-- Target database: esa_pbi
-- Auto-exported from pg_get_viewdef on 2026-03-05

CREATE OR REPLACE VIEW vw_tenant_mimo_summary AS
 SELECT lower(TRIM(BOTH FROM "sEmail")) AS tenant_email,
    min("TenantID") AS first_tenant_id,
    max("TenantID") AS last_tenant_id,
    count(DISTINCT "TenantID") AS unique_tenant_ids,
    sum("MoveIn") AS total_move_ins,
    sum("MoveOut") AS total_move_outs,
    sum("Transfer") AS total_transfers,
    count(*) AS total_mimo_records,
    min("MoveDate") FILTER (WHERE "MoveIn" = 1) AS first_move_in_date,
    max("MoveDate") FILTER (WHERE "MoveIn" = 1) AS last_move_in_date,
    min("MoveDate") FILTER (WHERE "MoveOut" = 1) AS first_move_out_date,
    max("MoveDate") FILTER (WHERE "MoveOut" = 1) AS last_move_out_date,
        CASE
            WHEN max("MoveDate") FILTER (WHERE "MoveIn" = 1) > COALESCE(max("MoveDate") FILTER (WHERE "MoveOut" = 1), '1900-01-01 00:00:00'::timestamp without time zone) THEN 'Active Tenant'::text
            WHEN max("MoveDate") FILTER (WHERE "MoveOut" = 1) IS NOT NULL THEN 'Former Tenant'::text
            ELSE 'Unknown'::text
        END AS tenant_status,
    array_agg(DISTINCT "SiteID") AS site_ids,
    count(DISTINCT "SiteID") AS unique_sites,
    array_agg(DISTINCT "UnitName") FILTER (WHERE "UnitName" IS NOT NULL) AS units_rented,
    count(DISTINCT "UnitName") AS unique_units,
    array_agg(DISTINCT "UnitSize") FILTER (WHERE "UnitSize" IS NOT NULL) AS unit_sizes,
    avg("MovedInRentalRate") FILTER (WHERE "MoveIn" = 1) AS avg_move_in_rate,
    min("MovedInRentalRate") FILTER (WHERE "MoveIn" = 1) AS min_move_in_rate,
    max("MovedInRentalRate") FILTER (WHERE "MoveIn" = 1) AS max_move_in_rate,
    sum("MovedInRentalRate") FILTER (WHERE "MoveIn" = 1) AS total_move_in_revenue,
    bool_or("bClimate") AS ever_had_climate,
    bool_or("bPower") AS ever_had_power,
    bool_or("bAlarm") AS ever_had_alarm,
    (array_agg("TenantName" ORDER BY "MoveDate" DESC))[1] AS tenant_name,
    (array_agg("sCompany" ORDER BY "MoveDate" DESC NULLS LAST) FILTER (WHERE "sCompany" IS NOT NULL AND TRIM(BOTH FROM "sCompany") <> ''::text))[1] AS company
   FROM mimo
  WHERE "sEmail" IS NOT NULL AND TRIM(BOTH FROM "sEmail") <> ''::text
  GROUP BY (lower(TRIM(BOTH FROM "sEmail")));
