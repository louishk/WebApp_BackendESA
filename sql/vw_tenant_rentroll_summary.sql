-- View: vw_tenant_rentroll_summary
-- Target database: esa_pbi
-- Auto-exported from pg_get_viewdef on 2026-03-05

CREATE OR REPLACE VIEW vw_tenant_rentroll_summary AS
 SELECT lower(TRIM(BOTH FROM "sEmail")) AS tenant_email,
    min("TenantID") AS first_tenant_id,
    max("TenantID") AS last_tenant_id,
    count(DISTINCT "TenantID") AS unique_tenant_ids,
    min(extract_date) AS first_seen_date,
    max(extract_date) AS last_seen_date,
    count(DISTINCT extract_date) AS months_in_system,
        CASE
            WHEN max(extract_date) >= (CURRENT_DATE - '60 days'::interval) THEN 'Active Tenant'::text
            ELSE 'Former Tenant'::text
        END AS tenant_status,
    array_agg(DISTINCT "SiteID") AS site_ids,
    count(DISTINCT "SiteID") AS unique_sites,
    array_agg(DISTINCT "sUnit") FILTER (WHERE "sUnit" IS NOT NULL) AS units_rented,
    count(DISTINCT "UnitID") AS unique_units,
    array_agg(DISTINCT "sTypeName") FILTER (WHERE "sTypeName" IS NOT NULL) AS unit_types,
    avg("dcRent") AS avg_rent,
    min("dcRent") FILTER (WHERE "dcRent" > 0::numeric) AS min_rent,
    max("dcRent") AS max_rent,
    avg("Area") AS avg_unit_area,
    sum(DISTINCT "Area") AS total_area_rented,
    (array_agg("sTenant" ORDER BY extract_date DESC NULLS LAST) FILTER (WHERE "sTenant" IS NOT NULL))[1] AS tenant_name,
    (array_agg("sCompany" ORDER BY extract_date DESC NULLS LAST) FILTER (WHERE "sCompany" IS NOT NULL AND TRIM(BOTH FROM "sCompany") <> ''::text))[1] AS company
   FROM rentroll
  WHERE "sEmail" IS NOT NULL AND TRIM(BOTH FROM "sEmail") <> ''::text AND "TenantID" IS NOT NULL
  GROUP BY (lower(TRIM(BOTH FROM "sEmail")));
