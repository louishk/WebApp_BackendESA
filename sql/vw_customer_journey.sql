-- View: vw_customer_journey
-- Target database: esa_pbi
-- Auto-exported from pg_get_viewdef on 2026-03-05

CREATE OR REPLACE VIEW vw_customer_journey AS
 SELECT customer_id,
    identifier_type,
    sugar_id,
    full_name,
    unified_email,
    unified_mobile,
    is_internal_email,
    is_test_email,
    is_competitor_email,
    is_dummy_phone,
    is_same_day_duplicate,
    email_type,
    data_quality_category,
    data_completeness_score,
    exclusion_reason,
    is_valid_for_analysis,
    exclusion_category,
    date_entered,
    date_modified,
    lead_year,
    lead_month,
    lead_year_month,
    lead_day_of_week,
    lead_age_days,
    status,
    lead_source,
    source_of_call_c,
    source_of_call_others_c,
    source_channel,
    source_subchannel,
    source_name_clean,
    facility_location_c,
    type_of_customer_c,
    type_of_goods_c,
    reason_for_storing_c,
    utm_source_c,
    utm_medium_c,
    utm_campaign_c,
    converted,
    contact_id,
    row_number() OVER (PARTITION BY customer_id ORDER BY date_entered) AS lead_sequence,
    count(*) OVER (PARTITION BY customer_id) AS customer_total_leads,
    EXTRACT(day FROM date_entered - first_value(date_entered) OVER (PARTITION BY customer_id ORDER BY date_entered))::integer AS days_since_first_lead,
    EXTRACT(day FROM date_entered - lag(date_entered) OVER (PARTITION BY customer_id ORDER BY date_entered))::integer AS days_since_previous_lead,
    lag(status) OVER (PARTITION BY customer_id ORDER BY date_entered) AS previous_status,
    lag(source_of_call_c) OVER (PARTITION BY customer_id ORDER BY date_entered) AS previous_lead_source,
    lag(source_channel) OVER (PARTITION BY customer_id ORDER BY date_entered) AS previous_source_channel,
    lag(facility_location_c) OVER (PARTITION BY customer_id ORDER BY date_entered) AS previous_facility,
        CASE
            WHEN row_number() OVER (PARTITION BY customer_id ORDER BY date_entered) = 1 THEN true
            ELSE false
        END AS is_first_lead,
        CASE
            WHEN row_number() OVER (PARTITION BY customer_id ORDER BY date_entered DESC) = 1 THEN true
            ELSE false
        END AS is_latest_lead,
        CASE
            WHEN status <> lag(status) OVER (PARTITION BY customer_id ORDER BY date_entered) THEN true
            WHEN lag(status) OVER (PARTITION BY customer_id ORDER BY date_entered) IS NULL THEN false
            ELSE false
        END AS status_changed,
        CASE
            WHEN source_of_call_c <> lag(source_of_call_c) OVER (PARTITION BY customer_id ORDER BY date_entered) THEN true
            WHEN lag(source_of_call_c) OVER (PARTITION BY customer_id ORDER BY date_entered) IS NULL THEN false
            ELSE false
        END AS source_changed,
        CASE
            WHEN source_channel::text <> lag(source_channel) OVER (PARTITION BY customer_id ORDER BY date_entered)::text THEN true
            WHEN lag(source_channel) OVER (PARTITION BY customer_id ORDER BY date_entered) IS NULL THEN false
            ELSE false
        END AS channel_changed,
        CASE
            WHEN facility_location_c <> lag(facility_location_c) OVER (PARTITION BY customer_id ORDER BY date_entered) THEN true
            WHEN lag(facility_location_c) OVER (PARTITION BY customer_id ORDER BY date_entered) IS NULL THEN false
            ELSE false
        END AS facility_changed,
        CASE
            WHEN row_number() OVER (PARTITION BY customer_id ORDER BY date_entered) = 1 THEN 'First Touch'::text
            WHEN converted = true THEN 'Conversion'::text
            WHEN row_number() OVER (PARTITION BY customer_id ORDER BY date_entered DESC) = 1 AND status = 'Dead'::text THEN 'Churned'::text
            WHEN row_number() OVER (PARTITION BY customer_id ORDER BY date_entered DESC) = 1 THEN 'Current'::text
            ELSE 'Re-engagement'::text
        END AS journey_stage,
        CASE
            WHEN row_number() OVER (PARTITION BY customer_id ORDER BY date_entered) = 1 THEN 'First Touch'::text
            WHEN EXTRACT(day FROM date_entered - lag(date_entered) OVER (PARTITION BY customer_id ORDER BY date_entered)) <= 7::numeric THEN 'Quick Follow-up (<=7d)'::text
            WHEN EXTRACT(day FROM date_entered - lag(date_entered) OVER (PARTITION BY customer_id ORDER BY date_entered)) <= 30::numeric THEN 'Monthly (8-30d)'::text
            WHEN EXTRACT(day FROM date_entered - lag(date_entered) OVER (PARTITION BY customer_id ORDER BY date_entered)) <= 90::numeric THEN 'Quarterly (31-90d)'::text
            WHEN EXTRACT(day FROM date_entered - lag(date_entered) OVER (PARTITION BY customer_id ORDER BY date_entered)) <= 365::numeric THEN 'Annual (91-365d)'::text
            ELSE 'Long Gap (>1yr)'::text
        END AS re_engagement_timing
   FROM vw_customer_master
  WHERE customer_id IS NOT NULL
  ORDER BY customer_id, date_entered;
