-- View: vw_customer_master
-- Target database: esa_pbi
-- Auto-exported from pg_get_viewdef on 2026-03-05

CREATE OR REPLACE VIEW vw_customer_master AS
 WITH base_data AS (
         SELECT l.sugar_id,
            l.synced_at,
            l.name,
            l.date_entered,
            l.date_modified,
            l.modified_user_id,
            l.modified_by_name,
            l.created_by,
            l.created_by_name,
            l.description,
            l.deleted,
            l.salutation,
            l.first_name,
            l.last_name,
            l.full_name,
            l.title,
            l.facebook,
            l.twitter,
            l.googleplus,
            l.department,
            l.do_not_call,
            l.phone_home,
            l.phone_mobile,
            l.phone_work,
            l.phone_other,
            l.phone_fax,
            l.primary_address_street,
            l.primary_address_street_2,
            l.primary_address_street_3,
            l.primary_address_city,
            l.primary_address_state,
            l.primary_address_postalcode,
            l.primary_address_country,
            l.alt_address_street,
            l.alt_address_street_2,
            l.alt_address_street_3,
            l.alt_address_city,
            l.alt_address_state,
            l.alt_address_postalcode,
            l.alt_address_country,
            l.assistant,
            l.assistant_phone,
            l.picture,
            l.converted,
            l.refered_by,
            l.lead_source,
            l.lead_source_description,
            l.status,
            l.status_description,
            l.reports_to_id,
            l.report_to_name,
            l.dp_business_purpose,
            l.dp_consent_last_updated,
            l.dnb_principal_id,
            l.account_name,
            l.account_to_lead,
            l.account_description,
            l.contact_id,
            l.contact_name,
            l.account_id,
            l.opportunity_id,
            l.converted_opp_name,
            l.opportunity_name,
            l.opportunity_amount,
            l.campaign_id,
            l.campaign_name,
            l.c_accept_status_fields,
            l.m_accept_status_fields,
            l.accept_status_id,
            l.accept_status_name,
            l.accept_status_calls,
            l.accept_status_meetings,
            l.accept_status_messages,
            l.webtolead_email1,
            l.webtolead_email2,
            l.webtolead_email_opt_out,
            l.webtolead_invalid_email,
            l.birthdate,
            l.portal_name,
            l.portal_app,
            l.business_center_name,
            l.business_center_id,
            l.website,
            l.preferred_language,
            l.mkto_sync,
            l.mkto_id,
            l.mkto_lead_score,
            l.ai_conv_score_classification,
            l.ai_icp_fit_score_classification,
            l.market_interest_prediction_score,
            l.market_score,
            l.ai_conv_score_absolute,
            l.ai_conv_bin_accuracy,
            l.ai_conv_multiplier,
            l.ai_icp_fit_score_absolute,
            l.ai_icp_fit_bin_accuracy,
            l.ai_icp_fit_multiplier,
            l.hint_account_logo,
            l.hint_contact_pic,
            l.hint_photo,
            l.hint_account_website,
            l.sf_lastactivity_default,
            l.following,
            l.tag,
            l.sync_key,
            l.assigned_user_id,
            l.assigned_user_name,
            l.team_set_id,
            l.acl_team_set_id,
            l.team_count,
            l.team_name,
            l.acl_team_names,
            l.email,
            l.email1,
            l.email2,
            l.invalid_email,
            l.email_opt_out,
            l.email_addresses_non_primary,
            l.external_user_id,
            l.dri_workflow_template_id,
            l.dri_workflow_template_name,
            l.perform_sugar_action,
            l.promo_remarks,
            l.company_industry,
            l.items_to_store_c,
            l.reason_for_storing_c,
            l.type_of_storage_c,
            l.ref_no_c,
            l.alt_salutation_c,
            l.alt_email_c,
            l.promo_code_c,
            l.discount,
            l.date_of_enquiry_c,
            l.tenant_id_c,
            l.source_of_call_c,
            l.type_of_customer_c,
            l.utm_content_c,
            l.utm_source_c,
            l.utm_campaign_c,
            l.utm_medium_c,
            l.signed_up_date_c,
            l.ref_number,
            l.set_appointment_c,
            l.facility_location_c,
            l.initial_commitment_period_c,
            l.chatbot_note_c,
            l.ai_inferred_type_of_goods_c,
            l.alt_first_name_c,
            l.alt_last_name_c,
            l.alt_phone_work_c,
            l.comments_c,
            l.company_c,
            l.discpromo1_c,
            l.discpromo2_c,
            l.discpromo3_c,
            l.dollar_promo_c,
            l.follow_up_action_c,
            l.helpfulness_c,
            l.item_to_store_others_c,
            l.move_in_c,
            l.nric_c,
            l.possible_dupe_c,
            l.price_quoted1_c,
            l.price_quoted2_c,
            l.price_quoted3_c,
            l.primary_address_street1_c,
            l.primary_address_street2_c,
            l.reason_for_storing_others_c,
            l.seen_bus_c,
            l.seen_tv_c,
            l.source_of_call_others_c,
            l.storage_period_c,
            l.storage_required_on_c,
            l.store_before_c,
            l.suburb_c,
            l.suburb_others_c,
            l.type_of_goods_c,
            l.uen_c,
            l.unit_nbr1_c,
            l.unit_nbr2_c,
            l.unit_nbr3_c,
            l.unit_size1_c,
            l.unit_size2_c,
            l.unit_size3_c,
            lsm.source_channel,
            lsm.source_subchannel,
            lsm.source_name_clean,
            lower(TRIM(BOTH FROM COALESCE(NULLIF(TRIM(BOTH FROM l.email1), ''::text), NULLIF(TRIM(BOTH FROM l.email), ''::text), NULLIF(TRIM(BOTH FROM l.email2), ''::text), NULLIF(TRIM(BOTH FROM l.webtolead_email1), ''::text), NULLIF(TRIM(BOTH FROM l.webtolead_email2), ''::text)))) AS _unified_email,
                CASE
                    WHEN l.phone_mobile IS NOT NULL AND length(regexp_replace(l.phone_mobile, '[^0-9]'::text, ''::text, 'g'::text)) >= 8 THEN regexp_replace(l.phone_mobile, '[^0-9+]'::text, ''::text, 'g'::text)
                    ELSE NULL::text
                END AS _unified_mobile,
            row_number() OVER (PARTITION BY (COALESCE(lower(TRIM(BOTH FROM COALESCE(NULLIF(TRIM(BOTH FROM l.email1), ''::text), NULLIF(TRIM(BOTH FROM l.email), ''::text), NULLIF(TRIM(BOTH FROM l.email2), ''::text), NULLIF(TRIM(BOTH FROM l.webtolead_email1), ''::text), NULLIF(TRIM(BOTH FROM l.webtolead_email2), ''::text)))), '__no_email__'::text)), (l.date_entered::date) ORDER BY l.date_entered) AS _same_day_sequence
           FROM sugarcrm_leads l
             LEFT JOIN lead_source_mapping lsm ON l.source_of_call_c = lsm.source_of_call_c::text
          WHERE l.deleted = false
        )
 SELECT COALESCE(_unified_email,
        CASE
            WHEN _unified_mobile IS NOT NULL THEN 'MOBILE:'::text || _unified_mobile
            ELSE NULL::text
        END) AS customer_id,
        CASE
            WHEN _unified_email IS NOT NULL THEN 'email'::text
            WHEN _unified_mobile IS NOT NULL THEN 'mobile'::text
            ELSE 'unidentifiable'::text
        END AS identifier_type,
    _unified_email AS unified_email,
    _unified_mobile AS unified_mobile,
        CASE
            WHEN _unified_email IS NOT NULL AND _unified_email ~~ '%@%'::text THEN split_part(_unified_email, '@'::text, 2)
            ELSE NULL::text
        END AS email_domain,
        CASE
            WHEN _unified_email ~~ '%@extraspaceasia.com'::text THEN true
            WHEN _unified_email ~~ '%@extraspace.com.sg'::text THEN true
            WHEN _unified_email ~~ '%@aboroadlink.com'::text THEN true
            ELSE false
        END AS is_internal_email,
        CASE
            WHEN _unified_email ~~ 'test%@%'::text THEN true
            WHEN _unified_email ~~ '%test@%'::text THEN true
            WHEN _unified_email ~~ 'tba@%'::text THEN true
            WHEN _unified_email ~~ 'dummy%@%'::text THEN true
            WHEN _unified_email ~~ 'fake%@%'::text THEN true
            WHEN _unified_email ~~ 'sample%@%'::text THEN true
            WHEN _unified_email ~~ 'demo%@%'::text THEN true
            WHEN _unified_email ~~ 'na@%'::text THEN true
            WHEN _unified_email ~~ 'n/a@%'::text THEN true
            WHEN _unified_email ~~ 'none@%'::text THEN true
            WHEN _unified_email ~~ 'noemail@%'::text THEN true
            WHEN _unified_email = 'test@gmail.com'::text THEN true
            WHEN _unified_email = 'tba@gmail.com'::text THEN true
            ELSE false
        END AS is_test_email,
        CASE
            WHEN _unified_email ~~ '%@storhub.com'::text THEN true
            WHEN _unified_email ~~ '%@store-friendly.com'::text THEN true
            WHEN _unified_email ~~ '%@storefriendly.com'::text THEN true
            WHEN _unified_email ~~ '%@lockandstore.com'::text THEN true
            WHEN _unified_email ~~ '%@lock-and-store.com'::text THEN true
            WHEN _unified_email ~~ '%@redboxstorage.%'::text THEN true
            WHEN _unified_email ~~ '%@spaceship.com.sg'::text THEN true
            WHEN _unified_email ~~ '%@workstore.com'::text THEN true
            WHEN _unified_email ~~ '%@work-store.com'::text THEN true
            WHEN _unified_email ~~ '%@pti.com.sg'::text THEN true
            WHEN _unified_email ~~ '%@safehouse.com.sg'::text THEN true
            WHEN _unified_email ~~ '%@boxful.%'::text THEN true
            WHEN _unified_email ~~ '%@beam.storage'::text THEN true
            WHEN _unified_email ~~ '%@beamstorage.%'::text THEN true
            ELSE false
        END AS is_competitor_email,
        CASE
            WHEN _unified_email ~~ '%@gmail.com'::text THEN 'Consumer'::text
            WHEN _unified_email ~~ '%@yahoo.%'::text THEN 'Consumer'::text
            WHEN _unified_email ~~ '%@hotmail.%'::text THEN 'Consumer'::text
            WHEN _unified_email ~~ '%@outlook.%'::text THEN 'Consumer'::text
            WHEN _unified_email ~~ '%@live.%'::text THEN 'Consumer'::text
            WHEN _unified_email ~~ '%@icloud.com'::text THEN 'Consumer'::text
            WHEN _unified_email ~~ '%@me.com'::text THEN 'Consumer'::text
            WHEN _unified_email ~~ '%@aol.%'::text THEN 'Consumer'::text
            WHEN _unified_email ~~ '%@msn.%'::text THEN 'Consumer'::text
            WHEN _unified_email ~~ '%@singnet.com.sg'::text THEN 'Consumer'::text
            WHEN _unified_email ~~ '%@starhub.%'::text THEN 'Consumer'::text
            WHEN _unified_email IS NULL THEN NULL::text
            ELSE 'Business'::text
        END AS email_type,
        CASE
            WHEN _unified_email IS NULL THEN false
            WHEN _unified_email ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'::text THEN true
            ELSE false
        END AS has_valid_email_format,
        CASE
            WHEN _unified_mobile IS NOT NULL THEN true
            ELSE false
        END AS has_mobile,
        CASE
            WHEN phone_work IS NOT NULL AND TRIM(BOTH FROM phone_work) <> ''::text AND length(regexp_replace(phone_work, '[^0-9]'::text, ''::text, 'g'::text)) >= 8 THEN true
            ELSE false
        END AS has_work_phone,
        CASE
            WHEN _unified_mobile IS NOT NULL THEN length(regexp_replace(_unified_mobile, '[^0-9]'::text, ''::text, 'g'::text))
            ELSE NULL::integer
        END AS mobile_digit_count,
        CASE
            WHEN _unified_mobile IS NOT NULL AND (_unified_mobile ~ '^[89][0-9]{7}$'::text OR _unified_mobile ~ '^\+?65[89][0-9]{7}$'::text) THEN true
            ELSE false
        END AS is_sg_mobile_format,
        CASE
            WHEN _unified_mobile IS NULL THEN false
            WHEN _unified_mobile ~ '^0{8,}$'::text THEN true
            WHEN _unified_mobile ~ '^1{8,}$'::text THEN true
            WHEN _unified_mobile ~ '^9{8,}$'::text THEN true
            WHEN _unified_mobile ~ '^8{8,}$'::text THEN true
            WHEN _unified_mobile ~ '^12345'::text THEN true
            WHEN _unified_mobile ~ '^0{5}'::text THEN true
            WHEN _unified_mobile = '99999999'::text THEN true
            WHEN _unified_mobile = '88888888'::text THEN true
            WHEN _unified_mobile = '11111111'::text THEN true
            WHEN _unified_mobile = '12345678'::text THEN true
            WHEN _unified_mobile = '87654321'::text THEN true
            ELSE false
        END AS is_dummy_phone,
        CASE
            WHEN _same_day_sequence > 1 THEN true
            ELSE false
        END AS is_same_day_duplicate,
        CASE
            WHEN _unified_email IS NOT NULL OR _unified_mobile IS NOT NULL THEN true
            ELSE false
        END AS is_identifiable,
        CASE
            WHEN _unified_email IS NOT NULL AND _unified_mobile IS NOT NULL THEN true
            ELSE false
        END AS has_both_contacts,
        CASE
            WHEN full_name IS NOT NULL AND TRIM(BOTH FROM full_name) <> ''::text THEN true
            WHEN first_name IS NOT NULL AND TRIM(BOTH FROM first_name) <> ''::text THEN true
            WHEN last_name IS NOT NULL AND TRIM(BOTH FROM last_name) <> ''::text THEN true
            ELSE false
        END AS has_name,
        CASE
            WHEN primary_address_street IS NOT NULL AND TRIM(BOTH FROM primary_address_street) <> ''::text THEN true
            WHEN primary_address_postalcode IS NOT NULL AND TRIM(BOTH FROM primary_address_postalcode) <> ''::text THEN true
            ELSE false
        END AS has_address,
        CASE
            WHEN _unified_email IS NOT NULL THEN 2
            ELSE 0
        END +
        CASE
            WHEN _unified_mobile IS NOT NULL THEN 2
            ELSE 0
        END +
        CASE
            WHEN full_name IS NOT NULL AND TRIM(BOTH FROM full_name) <> ''::text THEN 1
            ELSE 0
        END +
        CASE
            WHEN first_name IS NOT NULL AND TRIM(BOTH FROM first_name) <> ''::text THEN 1
            ELSE 0
        END +
        CASE
            WHEN last_name IS NOT NULL AND TRIM(BOTH FROM last_name) <> ''::text THEN 1
            ELSE 0
        END +
        CASE
            WHEN primary_address_postalcode IS NOT NULL AND TRIM(BOTH FROM primary_address_postalcode) <> ''::text THEN 1
            ELSE 0
        END +
        CASE
            WHEN facility_location_c IS NOT NULL AND TRIM(BOTH FROM facility_location_c) <> ''::text THEN 1
            ELSE 0
        END +
        CASE
            WHEN source_of_call_c IS NOT NULL AND TRIM(BOTH FROM source_of_call_c) <> ''::text THEN 1
            ELSE 0
        END AS data_completeness_score,
    date_entered::date AS lead_date,
    CURRENT_DATE - date_entered::date AS lead_age_days,
    EXTRACT(year FROM date_entered)::integer AS lead_year,
    EXTRACT(month FROM date_entered)::integer AS lead_month,
    to_char(date_entered, 'YYYY-MM'::text) AS lead_year_month,
        CASE
            WHEN date_entered >= (CURRENT_DATE - '30 days'::interval) THEN true
            ELSE false
        END AS is_last_30_days,
        CASE
            WHEN date_entered >= (CURRENT_DATE - '90 days'::interval) THEN true
            ELSE false
        END AS is_last_90_days,
        CASE
            WHEN date_entered >= (CURRENT_DATE - '365 days'::interval) THEN true
            ELSE false
        END AS is_last_365_days,
    EXTRACT(dow FROM date_entered)::integer AS lead_day_of_week,
    to_char(date_entered, 'Day'::text) AS lead_day_name,
        CASE
            WHEN possible_dupe_c IS NOT NULL AND TRIM(BOTH FROM possible_dupe_c) <> ''::text THEN true
            ELSE false
        END AS has_dupe_flag,
        CASE
            WHEN _unified_email IS NULL AND _unified_mobile IS NULL THEN 'No Contact Info'::text
            WHEN _unified_email ~~ '%@extraspaceasia.com'::text THEN 'Internal'::text
            WHEN _unified_email ~~ 'test%@%'::text OR _unified_email = 'tba@gmail.com'::text THEN 'Test/Dummy'::text
            WHEN _unified_email IS NOT NULL AND _unified_mobile IS NOT NULL THEN 'Complete'::text
            WHEN _unified_email IS NOT NULL THEN 'Email Only'::text
            WHEN _unified_mobile IS NOT NULL THEN 'Mobile Only'::text
            ELSE 'Unknown'::text
        END AS data_quality_category,
        CASE
            WHEN _unified_email ~~ '%@extraspaceasia.com'::text THEN 'Internal Staff Email'::text
            WHEN _unified_email ~~ '%@extraspace.com.sg'::text THEN 'Internal Staff Email'::text
            WHEN _unified_email ~~ '%@aboroadlink.com'::text THEN 'Internal Staff Email'::text
            WHEN _unified_email ~~ 'test%@%'::text THEN 'Test/Dummy Email'::text
            WHEN _unified_email ~~ '%test@%'::text THEN 'Test/Dummy Email'::text
            WHEN _unified_email ~~ 'tba@%'::text THEN 'Test/Dummy Email'::text
            WHEN _unified_email ~~ 'dummy%@%'::text THEN 'Test/Dummy Email'::text
            WHEN _unified_email ~~ 'fake%@%'::text THEN 'Test/Dummy Email'::text
            WHEN _unified_email ~~ 'sample%@%'::text THEN 'Test/Dummy Email'::text
            WHEN _unified_email ~~ 'demo%@%'::text THEN 'Test/Dummy Email'::text
            WHEN _unified_email ~~ 'na@%'::text THEN 'Test/Dummy Email'::text
            WHEN _unified_email ~~ 'n/a@%'::text THEN 'Test/Dummy Email'::text
            WHEN _unified_email ~~ 'none@%'::text THEN 'Test/Dummy Email'::text
            WHEN _unified_email ~~ 'noemail@%'::text THEN 'Test/Dummy Email'::text
            WHEN _unified_email = 'test@gmail.com'::text THEN 'Test/Dummy Email'::text
            WHEN _unified_email = 'tba@gmail.com'::text THEN 'Test/Dummy Email'::text
            WHEN _unified_email ~~ '%@storhub.com'::text THEN 'Competitor Email'::text
            WHEN _unified_email ~~ '%@store-friendly.com'::text THEN 'Competitor Email'::text
            WHEN _unified_email ~~ '%@storefriendly.com'::text THEN 'Competitor Email'::text
            WHEN _unified_email ~~ '%@lockandstore.com'::text THEN 'Competitor Email'::text
            WHEN _unified_email ~~ '%@lock-and-store.com'::text THEN 'Competitor Email'::text
            WHEN _unified_email ~~ '%@redboxstorage.%'::text THEN 'Competitor Email'::text
            WHEN _unified_email ~~ '%@spaceship.com.sg'::text THEN 'Competitor Email'::text
            WHEN _unified_email ~~ '%@workstore.com'::text THEN 'Competitor Email'::text
            WHEN _unified_email ~~ '%@work-store.com'::text THEN 'Competitor Email'::text
            WHEN _unified_email ~~ '%@pti.com.sg'::text THEN 'Competitor Email'::text
            WHEN _unified_email ~~ '%@safehouse.com.sg'::text THEN 'Competitor Email'::text
            WHEN _unified_email ~~ '%@boxful.%'::text THEN 'Competitor Email'::text
            WHEN _unified_email ~~ '%@beam.storage'::text THEN 'Competitor Email'::text
            WHEN _unified_email ~~ '%@beamstorage.%'::text THEN 'Competitor Email'::text
            WHEN _unified_email IS NULL AND _unified_mobile IS NULL THEN 'No Contact Info'::text
            WHEN _unified_email IS NULL AND (_unified_mobile ~ '^0{8,}$'::text OR _unified_mobile ~ '^1{8,}$'::text OR _unified_mobile ~ '^9{8,}$'::text OR _unified_mobile ~ '^8{8,}$'::text OR _unified_mobile ~ '^12345'::text OR _unified_mobile ~ '^0{5}'::text OR (_unified_mobile = ANY (ARRAY['99999999'::text, '88888888'::text, '11111111'::text, '12345678'::text, '87654321'::text]))) THEN 'Dummy Phone Number'::text
            WHEN _same_day_sequence > 1 THEN 'Same-Day Duplicate'::text
            ELSE 'Valid for Analysis'::text
        END AS exclusion_reason,
        CASE
            WHEN _unified_email ~~ '%@extraspaceasia.com'::text THEN false
            WHEN _unified_email ~~ '%@extraspace.com.sg'::text THEN false
            WHEN _unified_email ~~ '%@aboroadlink.com'::text THEN false
            WHEN _unified_email ~~ 'test%@%'::text THEN false
            WHEN _unified_email ~~ '%test@%'::text THEN false
            WHEN _unified_email ~~ 'tba@%'::text THEN false
            WHEN _unified_email ~~ 'dummy%@%'::text THEN false
            WHEN _unified_email ~~ 'fake%@%'::text THEN false
            WHEN _unified_email ~~ 'sample%@%'::text THEN false
            WHEN _unified_email ~~ 'demo%@%'::text THEN false
            WHEN _unified_email ~~ 'na@%'::text THEN false
            WHEN _unified_email ~~ 'n/a@%'::text THEN false
            WHEN _unified_email ~~ 'none@%'::text THEN false
            WHEN _unified_email ~~ 'noemail@%'::text THEN false
            WHEN _unified_email = 'test@gmail.com'::text THEN false
            WHEN _unified_email = 'tba@gmail.com'::text THEN false
            WHEN _unified_email ~~ '%@storhub.com'::text THEN false
            WHEN _unified_email ~~ '%@store-friendly.com'::text THEN false
            WHEN _unified_email ~~ '%@storefriendly.com'::text THEN false
            WHEN _unified_email ~~ '%@lockandstore.com'::text THEN false
            WHEN _unified_email ~~ '%@lock-and-store.com'::text THEN false
            WHEN _unified_email ~~ '%@redboxstorage.%'::text THEN false
            WHEN _unified_email ~~ '%@spaceship.com.sg'::text THEN false
            WHEN _unified_email ~~ '%@workstore.com'::text THEN false
            WHEN _unified_email ~~ '%@work-store.com'::text THEN false
            WHEN _unified_email ~~ '%@pti.com.sg'::text THEN false
            WHEN _unified_email ~~ '%@safehouse.com.sg'::text THEN false
            WHEN _unified_email ~~ '%@boxful.%'::text THEN false
            WHEN _unified_email ~~ '%@beam.storage'::text THEN false
            WHEN _unified_email ~~ '%@beamstorage.%'::text THEN false
            WHEN _unified_email IS NULL AND _unified_mobile IS NULL THEN false
            WHEN _unified_email IS NULL AND (_unified_mobile ~ '^0{8,}$'::text OR _unified_mobile ~ '^1{8,}$'::text OR _unified_mobile ~ '^9{8,}$'::text OR _unified_mobile ~ '^8{8,}$'::text OR _unified_mobile ~ '^12345'::text OR _unified_mobile ~ '^0{5}'::text OR (_unified_mobile = ANY (ARRAY['99999999'::text, '88888888'::text, '11111111'::text, '12345678'::text, '87654321'::text]))) THEN false
            WHEN _same_day_sequence > 1 THEN false
            ELSE true
        END AS is_valid_for_analysis,
        CASE
            WHEN _unified_email ~~ '%@extraspaceasia.com'::text THEN 'Test/Internal'::text
            WHEN _unified_email ~~ '%@extraspace.com.sg'::text THEN 'Test/Internal'::text
            WHEN _unified_email ~~ '%@aboroadlink.com'::text THEN 'Test/Internal'::text
            WHEN _unified_email ~~ 'test%@%'::text THEN 'Test/Internal'::text
            WHEN _unified_email ~~ '%test@%'::text THEN 'Test/Internal'::text
            WHEN _unified_email ~~ 'tba@%'::text THEN 'Test/Internal'::text
            WHEN _unified_email ~~ 'dummy%@%'::text THEN 'Test/Internal'::text
            WHEN _unified_email ~~ 'fake%@%'::text THEN 'Test/Internal'::text
            WHEN _unified_email ~~ 'sample%@%'::text THEN 'Test/Internal'::text
            WHEN _unified_email ~~ 'demo%@%'::text THEN 'Test/Internal'::text
            WHEN _unified_email ~~ 'na@%'::text THEN 'Test/Internal'::text
            WHEN _unified_email ~~ 'n/a@%'::text THEN 'Test/Internal'::text
            WHEN _unified_email ~~ 'none@%'::text THEN 'Test/Internal'::text
            WHEN _unified_email ~~ 'noemail@%'::text THEN 'Test/Internal'::text
            WHEN _unified_email = 'test@gmail.com'::text THEN 'Test/Internal'::text
            WHEN _unified_email = 'tba@gmail.com'::text THEN 'Test/Internal'::text
            WHEN _unified_email ~~ '%@storhub.com'::text THEN 'Competitor'::text
            WHEN _unified_email ~~ '%@store-friendly.com'::text THEN 'Competitor'::text
            WHEN _unified_email ~~ '%@storefriendly.com'::text THEN 'Competitor'::text
            WHEN _unified_email ~~ '%@lockandstore.com'::text THEN 'Competitor'::text
            WHEN _unified_email ~~ '%@lock-and-store.com'::text THEN 'Competitor'::text
            WHEN _unified_email ~~ '%@redboxstorage.%'::text THEN 'Competitor'::text
            WHEN _unified_email ~~ '%@spaceship.com.sg'::text THEN 'Competitor'::text
            WHEN _unified_email ~~ '%@workstore.com'::text THEN 'Competitor'::text
            WHEN _unified_email ~~ '%@work-store.com'::text THEN 'Competitor'::text
            WHEN _unified_email ~~ '%@pti.com.sg'::text THEN 'Competitor'::text
            WHEN _unified_email ~~ '%@safehouse.com.sg'::text THEN 'Competitor'::text
            WHEN _unified_email ~~ '%@boxful.%'::text THEN 'Competitor'::text
            WHEN _unified_email ~~ '%@beam.storage'::text THEN 'Competitor'::text
            WHEN _unified_email ~~ '%@beamstorage.%'::text THEN 'Competitor'::text
            WHEN _unified_email IS NULL AND _unified_mobile IS NULL THEN 'Incomplete Data'::text
            WHEN _unified_email IS NULL AND (_unified_mobile ~ '^0{8,}$'::text OR _unified_mobile ~ '^1{8,}$'::text OR _unified_mobile ~ '^9{8,}$'::text OR _unified_mobile ~ '^8{8,}$'::text OR _unified_mobile ~ '^12345'::text OR _unified_mobile ~ '^0{5}'::text OR (_unified_mobile = ANY (ARRAY['99999999'::text, '88888888'::text, '11111111'::text, '12345678'::text, '87654321'::text]))) THEN 'Incomplete Data'::text
            WHEN _same_day_sequence > 1 THEN 'Duplicate'::text
            ELSE 'Valid'::text
        END AS exclusion_category,
    sugar_id,
    full_name,
    first_name,
    last_name,
    salutation,
    title,
    email1,
    email,
    email2,
    webtolead_email1,
    phone_mobile,
    phone_work,
    phone_home,
    primary_address_street,
    primary_address_city,
    primary_address_state,
    primary_address_postalcode,
    primary_address_country,
    status,
    lead_source,
    source_of_call_c,
    source_of_call_others_c,
    source_channel,
    source_subchannel,
    source_name_clean,
    converted,
    contact_id,
    date_entered,
    date_modified,
    facility_location_c,
        CASE facility_location_c
            WHEN 'Eunos Link'::text THEN 'L003'::text
            WHEN 'Boon Keng Road'::text THEN 'L002'::text
            WHEN 'Boon Keng'::text THEN 'L002'::text
            WHEN 'IMM Building'::text THEN 'L001'::text
            WHEN 'Marymount'::text THEN 'L005'::text
            WHEN 'Marymount Road'::text THEN 'L005'::text
            WHEN 'Chan Sow Lin'::text THEN 'L007'::text
            WHEN 'Ang Mo Kio'::text THEN 'L018'::text
            WHEN 'Kallang Way'::text THEN 'L008'::text
            WHEN 'Section 51A'::text THEN 'L010'::text
            WHEN 'Segambut'::text THEN 'L009'::text
            WHEN 'Woodlands'::text THEN 'L017'::text
            WHEN 'ES Yangjae'::text THEN 'L006'::text
            WHEN 'Toh Guan Road'::text THEN 'OLD_SITE'::text
            WHEN 'Kota Damansara'::text THEN 'L026'::text
            WHEN 'West Coast'::text THEN 'L004'::text
            WHEN 'Toa Payoh'::text THEN 'L022'::text
            WHEN 'Tai Seng'::text THEN 'L030'::text
            WHEN 'ES Yatap'::text THEN 'L011'::text
            WHEN 'ES Bundang'::text THEN 'L011'::text
            WHEN 'Commonwealth'::text THEN 'L028'::text
            WHEN 'Sai Wan'::text THEN 'L015'::text
            WHEN 'Hung Hom'::text THEN 'L020'::text
            WHEN 'Hillview'::text THEN 'L025'::text
            WHEN 'Yeongdeungpo'::text THEN 'L021'::text
            WHEN 'Apgujeong'::text THEN 'L019'::text
            WHEN 'Yongsan'::text THEN 'L023'::text
            WHEN 'Gasan'::text THEN 'L013'::text
            WHEN 'Banpo'::text THEN 'L024'::text
            WHEN 'Ampang'::text THEN 'OLD_SITE'::text
            WHEN 'Penang Times Square'::text THEN 'OLD_SITE'::text
            WHEN 'Tsuen Wan'::text THEN 'OLD_SITE'::text
            ELSE NULL::text
        END AS site_code,
    type_of_customer_c,
    company_c,
    type_of_goods_c,
    reason_for_storing_c,
    storage_period_c,
    unit_size1_c,
    utm_source_c,
    utm_medium_c,
    utm_campaign_c,
    campaign_id,
    campaign_name,
    date_of_enquiry_c,
    signed_up_date_c,
    possible_dupe_c,
    assigned_user_id,
    assigned_user_name,
    created_by,
    created_by_name
   FROM base_data;
