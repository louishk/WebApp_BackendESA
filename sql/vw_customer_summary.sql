-- View: vw_customer_summary
-- Target database: esa_pbi
-- Auto-exported from pg_get_viewdef on 2026-03-05

CREATE OR REPLACE VIEW vw_customer_summary AS
 SELECT customer_id,
    identifier_type,
    max(full_name) AS customer_name,
    max(first_name) AS first_name,
    max(last_name) AS last_name,
    max(unified_email) AS primary_email,
    max(unified_mobile) AS primary_mobile,
    max(phone_work) AS work_phone,
    max(email_domain) AS email_domain,
    bool_or(is_internal_email) AS is_internal_email,
    bool_or(is_test_email) AS is_test_email,
    max(email_type) AS email_type,
    bool_or(has_mobile) AS has_mobile,
    bool_or(has_work_phone) AS has_work_phone,
    bool_or(has_both_contacts) AS has_both_contacts,
    bool_or(has_name) AS has_name,
    bool_or(has_address) AS has_address,
    max(data_completeness_score) AS max_data_completeness,
    avg(data_completeness_score)::numeric(4,2) AS avg_data_completeness,
    mode() WITHIN GROUP (ORDER BY data_quality_category) AS primary_quality_category,
    bool_or(is_competitor_email) AS is_competitor_email,
    bool_or(is_dummy_phone) AS is_dummy_phone,
    bool_or(is_same_day_duplicate) AS has_same_day_duplicates,
    count(*) FILTER (WHERE is_same_day_duplicate = true) AS same_day_duplicate_count,
    mode() WITHIN GROUP (ORDER BY exclusion_reason) AS primary_exclusion_reason,
    mode() WITHIN GROUP (ORDER BY exclusion_category) AS primary_exclusion_category,
    bool_or(is_valid_for_analysis) AS has_valid_leads,
    bool_and(is_valid_for_analysis) AS all_leads_valid,
    count(*) FILTER (WHERE is_valid_for_analysis = true) AS valid_lead_count,
    count(*) FILTER (WHERE is_valid_for_analysis = false) AS excluded_lead_count,
    (array_agg(primary_address_street ORDER BY date_entered DESC NULLS LAST) FILTER (WHERE primary_address_street IS NOT NULL))[1] AS address_street,
    (array_agg(primary_address_city ORDER BY date_entered DESC NULLS LAST) FILTER (WHERE primary_address_city IS NOT NULL))[1] AS address_city,
    (array_agg(primary_address_postalcode ORDER BY date_entered DESC NULLS LAST) FILTER (WHERE primary_address_postalcode IS NOT NULL))[1] AS address_postal,
    count(*) AS total_leads,
    count(DISTINCT sugar_id) AS unique_lead_records,
        CASE
            WHEN count(*) = 1 THEN 'Single'::text
            WHEN count(*) = 2 THEN 'Two'::text
            WHEN count(*) >= 3 AND count(*) <= 5 THEN '3-5'::text
            WHEN count(*) >= 6 AND count(*) <= 10 THEN '6-10'::text
            ELSE '10+'::text
        END AS lead_count_category,
        CASE
            WHEN count(*) > 1 THEN true
            ELSE false
        END AS is_repeat_customer,
    min(date_entered) AS first_lead_date,
    max(date_entered) AS last_lead_date,
    max(date_modified) AS last_activity_date,
    EXTRACT(day FROM max(date_entered) - min(date_entered))::integer AS journey_duration_days,
    EXTRACT(day FROM CURRENT_DATE::timestamp without time zone - max(date_entered))::integer AS days_since_last_lead,
    EXTRACT(year FROM min(date_entered))::integer AS first_lead_year,
    EXTRACT(year FROM max(date_entered))::integer AS last_lead_year,
    to_char(min(date_entered), 'YYYY-MM'::text) AS first_lead_year_month,
    to_char(max(date_entered), 'YYYY-MM'::text) AS last_lead_year_month,
        CASE
            WHEN max(date_entered) >= (CURRENT_DATE - '30 days'::interval) THEN 'Active (30d)'::text
            WHEN max(date_entered) >= (CURRENT_DATE - '90 days'::interval) THEN 'Recent (90d)'::text
            WHEN max(date_entered) >= (CURRENT_DATE - '365 days'::interval) THEN 'This Year'::text
            WHEN max(date_entered) >= (CURRENT_DATE - '730 days'::interval) THEN 'Last Year'::text
            ELSE 'Dormant (2y+)'::text
        END AS recency_category,
    count(*) FILTER (WHERE status = 'New'::text) AS status_new,
    count(*) FILTER (WHERE status = 'Assigned'::text) AS status_assigned,
    count(*) FILTER (WHERE status = 'In Process'::text) AS status_in_process,
    count(*) FILTER (WHERE status = 'Converted'::text) AS status_converted,
    count(*) FILTER (WHERE status = 'Recycled'::text) AS status_recycled,
    count(*) FILTER (WHERE status = 'Dead'::text) AS status_dead,
    (array_agg(status ORDER BY date_entered DESC))[1] AS current_status,
    bool_or(converted) AS ever_converted,
    min(date_entered) FILTER (WHERE converted = true) AS conversion_date,
    (array_agg(contact_id ORDER BY date_entered DESC) FILTER (WHERE contact_id IS NOT NULL))[1] AS converted_contact_id,
        CASE
            WHEN bool_or(converted) THEN EXTRACT(day FROM min(date_entered) FILTER (WHERE converted = true) - min(date_entered))::integer
            ELSE NULL::integer
        END AS days_to_conversion,
    array_agg(DISTINCT source_of_call_c) FILTER (WHERE source_of_call_c IS NOT NULL AND TRIM(BOTH FROM source_of_call_c) <> ''::text) AS lead_sources_used,
    count(DISTINCT source_of_call_c) FILTER (WHERE source_of_call_c IS NOT NULL AND TRIM(BOTH FROM source_of_call_c) <> ''::text) AS unique_lead_sources,
    (array_agg(source_of_call_c ORDER BY date_entered) FILTER (WHERE source_of_call_c IS NOT NULL AND TRIM(BOTH FROM source_of_call_c) <> ''::text))[1] AS first_lead_source,
    (array_agg(source_of_call_c ORDER BY date_entered DESC NULLS LAST) FILTER (WHERE source_of_call_c IS NOT NULL AND TRIM(BOTH FROM source_of_call_c) <> ''::text))[1] AS last_lead_source,
        CASE
            WHEN count(DISTINCT source_of_call_c) FILTER (WHERE source_of_call_c IS NOT NULL AND TRIM(BOTH FROM source_of_call_c) <> ''::text) > 1 THEN true
            ELSE false
        END AS is_multi_channel,
    array_agg(DISTINCT source_channel) FILTER (WHERE source_channel IS NOT NULL) AS source_channels_used,
    count(DISTINCT source_channel) FILTER (WHERE source_channel IS NOT NULL) AS unique_source_channels,
    (array_agg(source_channel ORDER BY date_entered) FILTER (WHERE source_channel IS NOT NULL))[1] AS first_source_channel,
    (array_agg(source_channel ORDER BY date_entered DESC NULLS LAST) FILTER (WHERE source_channel IS NOT NULL))[1] AS last_source_channel,
    array_agg(DISTINCT facility_location_c) FILTER (WHERE facility_location_c IS NOT NULL) AS facilities_inquired,
    count(DISTINCT facility_location_c) AS unique_facilities,
        CASE
            WHEN count(DISTINCT facility_location_c) > 1 THEN true
            ELSE false
        END AS is_multi_facility,
    (array_agg(utm_source_c ORDER BY date_entered) FILTER (WHERE utm_source_c IS NOT NULL))[1] AS first_utm_source,
    (array_agg(utm_medium_c ORDER BY date_entered) FILTER (WHERE utm_medium_c IS NOT NULL))[1] AS first_utm_medium,
    (array_agg(utm_campaign_c ORDER BY date_entered) FILTER (WHERE utm_campaign_c IS NOT NULL))[1] AS first_utm_campaign,
    (array_agg(utm_source_c ORDER BY date_entered DESC NULLS LAST) FILTER (WHERE utm_source_c IS NOT NULL))[1] AS last_utm_source,
    (array_agg(utm_medium_c ORDER BY date_entered DESC NULLS LAST) FILTER (WHERE utm_medium_c IS NOT NULL))[1] AS last_utm_medium,
    (array_agg(utm_campaign_c ORDER BY date_entered DESC NULLS LAST) FILTER (WHERE utm_campaign_c IS NOT NULL))[1] AS last_utm_campaign,
    mode() WITHIN GROUP (ORDER BY type_of_customer_c) AS primary_customer_type,
    mode() WITHIN GROUP (ORDER BY type_of_goods_c) AS primary_goods_type,
    mode() WITHIN GROUP (ORDER BY reason_for_storing_c) AS primary_storage_reason,
    (array_agg(company_c ORDER BY date_entered DESC NULLS LAST) FILTER (WHERE company_c IS NOT NULL))[1] AS company,
    bool_or(has_dupe_flag) AS has_manual_dupe_flag,
    (array_agg(assigned_user_name ORDER BY date_entered DESC NULLS LAST) FILTER (WHERE assigned_user_name IS NOT NULL))[1] AS current_assigned_user
   FROM vw_customer_master
  WHERE customer_id IS NOT NULL
  GROUP BY customer_id, identifier_type;
