-- View: vw_tenant_combined
-- Target database: esa_pbi
-- Auto-exported from pg_get_viewdef on 2026-03-05

CREATE OR REPLACE VIEW vw_tenant_combined AS
 SELECT COALESCE(m.tenant_email, r.tenant_email) AS tenant_email,
        CASE
            WHEN m.tenant_email IS NOT NULL AND r.tenant_email IS NOT NULL THEN 'Both'::text
            WHEN m.tenant_email IS NOT NULL THEN 'MIMO Only'::text
            ELSE 'RentRoll Only'::text
        END AS data_source,
    COALESCE(m.first_tenant_id, r.first_tenant_id) AS tenant_id,
    COALESCE(m.unique_tenant_ids, 0::bigint) + COALESCE(r.unique_tenant_ids, 0::bigint) AS total_tenant_ids,
    COALESCE(m.tenant_status, r.tenant_status) AS tenant_status,
    COALESCE(m.total_move_ins, 0::bigint) AS total_move_ins,
    COALESCE(m.total_move_outs, 0::bigint) AS total_move_outs,
    COALESCE(m.total_transfers, 0::bigint) AS total_transfers,
    LEAST(m.first_move_in_date, r.first_seen_date::timestamp without time zone) AS first_tenant_date,
    GREATEST(COALESCE(m.last_move_in_date, m.last_move_out_date), r.last_seen_date::timestamp without time zone) AS last_tenant_date,
    m.first_move_in_date,
    m.last_move_in_date,
    m.first_move_out_date,
    m.last_move_out_date,
    r.first_seen_date AS rentroll_first_seen,
    r.last_seen_date AS rentroll_last_seen,
    COALESCE(r.months_in_system, 0::bigint) AS months_in_system,
    COALESCE(m.unique_sites, 0::bigint) + COALESCE(r.unique_sites, 0::bigint) AS total_unique_sites,
    m.site_ids AS mimo_site_ids,
    r.site_ids AS rentroll_site_ids,
    COALESCE(m.unique_units, 0::bigint) AS mimo_unique_units,
    COALESCE(r.unique_units, 0::bigint) AS rentroll_unique_units,
    m.units_rented AS mimo_units,
    r.units_rented AS rentroll_units,
    m.unit_sizes,
    r.unit_types,
    m.avg_move_in_rate,
    m.total_move_in_revenue,
    r.avg_rent AS rentroll_avg_rent,
    r.avg_unit_area,
    COALESCE(m.ever_had_climate, false) AS ever_had_climate,
    COALESCE(m.ever_had_power, false) AS ever_had_power,
    COALESCE(m.ever_had_alarm, false) AS ever_had_alarm,
    COALESCE(m.tenant_name, r.tenant_name) AS tenant_name,
    COALESCE(m.company, r.company) AS company,
    COALESCE(m.total_mimo_records, 0::bigint) AS mimo_record_count,
    COALESCE(r.months_in_system, 0::bigint) AS rentroll_record_count
   FROM vw_tenant_mimo_summary m
     FULL JOIN vw_tenant_rentroll_summary r ON m.tenant_email = r.tenant_email;
