# Database Schema Reference

## esa_backend (app database — `backend` on Azure PostgreSQL)

### Core App Tables
**users** — id, username, email, password, auth_provider, department, job_title, office_location, employee_id, created_at, updated_at
**roles** — id, name, description, is_system, can_access_scheduler, can_manage_users, can_manage_pages, can_manage_roles, can_manage_configs, can_access_billing_tools, can_access_ecri, can_manage_ecri, can_access_inventory_tools, can_access_statistics, can_access_discount_tools
**user_roles** — user_id, role_id (join table)
**pages** — id, title, slug, content, is_secure, is_public, extension, edit_restricted, view_roles, view_users, edit_roles, edit_users

### API & Audit
**api_keys** — id, user_id, name, key_id, key_hash, scopes(jsonb), rate_limit, daily_quota, daily_usage, quota_reset_date, is_active, last_used_at, expires_at
**api_statistics** — id, endpoint, method, status_code, response_time_ms, client_ip, user_agent, request_size, response_size, called_at
**external_api_statistics** — id, service_name, endpoint, method, status_code, response_time_ms, success, error_message, caller, called_at
**audit_log** — id, user_id, action, resource, details(jsonb), ip_address, created_at

### Discount Plans
**discount_plans** — id, plan_type, plan_name, sitelink_discount_name, notes, objective, period_range, period_start, period_end, move_in_range, applicable_sites(jsonb), discount_value, discount_type, discount_numeric, discount_segmentation, clawback_condition, offers(jsonb), deposit, payment_terms, termination_notice, extra_offer, terms_conditions(jsonb), terms_conditions_cn(jsonb), terms_conditions_translations(jsonb), hidden_rate, available_for_chatbot, chatbot_notes, switch_to_us, referral_program, distribution_channel, rate_rules, promotion_codes(jsonb), collateral_url, registration_flow, department_notes(jsonb), custom_fields(jsonb), is_active, sort_order, lock_in_period, linked_concessions(jsonb), promo_period_start/end, booking_period_start/end, created_by, updated_by
**discount_plan_config** — id, field_name, option_value, translations(jsonb), sort_order, is_active

### Inventory
**inventory_type_mappings** — id, source_type_name, mapped_type_code, mapped_climate_code, created_by, created_at, updated_at
**inventory_unit_overrides** — id, site_id, unit_id, unit_type_code, size_category, size_range, shape, pillar, climate_code, reviewed, updated_by
**schema_markups** — id, name, schema_type, schema_data(jsonb), form_data(jsonb)

### Scheduler
**apscheduler_jobs** — id, next_run_time, job_state(bytea)
**scheduler_state** — id, status, started_at, host_name, pid, last_heartbeat, version, config_hash
**scheduler_pipeline_config** — pipeline_name(PK), display_name, module_path, schedule_type, schedule_config(jsonb), enabled, priority, depends_on, conflicts_with, resource_group, max_db_connections, estimated_duration_seconds, max_retries, retry_delay_seconds, timeout_seconds
**scheduler_job_history** — id, job_id, pipeline_name, execution_id(uuid), status, priority, scheduled_at, started_at, completed_at, duration_seconds, mode, parameters(jsonb), records_processed, attempt_number, error_message, error_traceback, alert_sent
**scheduler_resource_locks** — resource_name(PK), locked_by_job_id, locked_by_execution_id, locked_at, lock_expires_at, max_concurrent, current_count

---

## esa_pbi (analytics database — `esa_pbi` on Azure PostgreSQL)

### Core Dimension Tables
**siteinfo** — SiteID(PK), SiteCode, Name, InternalLabel, Country, CityDistrict, Street, Longitude, Latitude, google_place_id, embedsocial_source_id
**unit_range** — unit_range(PK), unit_range_cat, sort_order
**unit_type** — unit_type(PK), unit_type_base, unit_type_base_name, unit_type_feature, unit_type_feature_name, sort_order, unit_type_base_group
**unit_range_mappings** — id, site_code, facility, unit_prefix, range_start, range_end, suffix_start, suffix_end, storage_type, climate_type, has_dehumidifier, noke_status
**inventory_type_mappings** — id, source_type_name, mapped_type_code, mapped_climate_code
**losrange** — SortOrder, RangeMin, RangeMax, RangeLabel (length-of-stay buckets)
**pricerange** — SortOrder, RangeMin, RangeMax, RangeLabel (price buckets)
**lead_source_mapping** — source_of_call_c, source_channel, source_subchannel, source_name_clean

### Operational Fact Tables
**rentroll** — extract_date, SiteID, UnitID(PK combo), LedgerID, sUnitName, sTypeName, iFloor, dcWidth, dcLength, dcPushRate, dcStdRate, dcWebRate, dcRent, dcVar, dcSchedRent, bClimate, bPower, bInside, bRentable, bRented, TenantID, sTenant, iDaysVacant, dPaidThru, dMovedIn, dRentLastChanged, dcInsurPremium, iAutoBillType, Area, ...
**units_info** — id, SiteID, UnitID, sLocationCode, sUnitName, sTypeName, dcWidth, dcLength, iFloor, bClimate, bPower, bInside, bRentable, bRented, dcStdRate, dcWebRate, dcPushRate, ...
**mimo** — SiteID, TenantID, MoveDate, MoveIn, MoveOut, Transfer, UnitName, UnitSize, StandardRate, MovedInRentalRate, MovedOutRentalRate, sDiscountPlan, sSource, bClimate, ...
**budget** — id, internal_code, site_code, date, currency, metric, type, sub_type, total_available_nla, occupied_nla, rental_revenue, occupancy_pct, ...
**discount** — extract_date, SiteID, ChargeID, sUnitName, dcPrice, dcAmt, dcDiscount, sConcessionPlan, ...
**fx_rates** — rate_date, target_currency, base_currency(SGD), rate, is_trading_day
**fx_rates_monthly** — year_month, target_currency, avg_rate, first_rate, last_rate

### CallCenter (cc_*) Tables — Raw SOAP extracts
**cc_ledgers** — SiteID, LedgerID, TenantID, unitID, sUnitName, dcRent, dcTotalDue, dMovedIn, dPaidThru, ConcessionID, ... (200+ columns — full ledger snapshot)
**cc_charges** — SiteID, ChargeID, LedgerID, dcPmtAmt, sChgCategory, sChgDesc, dcAmt, dcPrice, dChgStrt, dChgEnd, ...
**cc_tenants** — SiteID, TenantID, sFName, sLName, sCompany, sEmail, sMobile, sPhone, ... (full tenant profile)
**ccws_discount** — id, ConcessionID, SiteID, sPlanName, sDescription, dcPCDiscount, dcFixedDiscount, dPlanStrt, dPlanEnd, ...

### ECRI (Existing Customer Rate Increase)
**ecri_batches** — batch_id(uuid PK), name, site_ids, target_increase_pct, control_group_enabled, total_ledgers, status, min_tenure_months, notice_period_days, created_by
**ecri_batch_ledgers** — id, batch_id, site_id, ledger_id, tenant_id, unit_name, old_rent, new_rent, increase_pct, increase_amt, notice_date, effective_date, api_status, api_response
**ecri_outcomes** — id, batch_id, site_id, ledger_id, outcome_date, outcome_type, days_after_notice, months_at_new_rent

### Google Ads
**gads_campaigns** — campaign_id(PK), customer_id, campaign_name, campaign_status, channel_type, bidding_strategy_type, budget_amount_micros
**gads_campaign_daily** — campaign_id, segments_date, device, ad_network_type, impressions, clicks, cost_micros, conversions
**gads_ad_groups** — ad_group_id(PK), campaign_id, customer_id, ad_group_name, ad_group_status
**gads_ad_group_daily** — ad_group_id, campaign_id, segments_date, device, impressions, clicks, cost_micros, conversions
**gads_keywords** — criterion_id(PK), ad_group_id, campaign_id, keyword_text, match_type, quality_score
**gads_keyword_daily** — criterion_id, segments_date, device, impressions, clicks, cost_micros, conversions
**gads_campaign_conversions** — campaign_id, segments_date, conversion_action_name, conversions
**gads_account_map** — customer_id(PK), country, currency

### SugarCRM
**sugarcrm_leads** — sugar_id(PK), name, status, lead_source, source_of_call_c, facility_location_c, type_of_customer_c, utm_source/medium/campaign, converted, date_entered, ... (200+ fields)
**sugarcrm_contacts** — sugar_id(PK), name, email, phone_mobile, lead_source, account_name, converted, ...
**sugarcrm_calls** — sugar_id(PK), name, date_start, duration_hours/minutes, status, direction, sentiment scores, ...
**sugarcrm_meetings** — sugar_id(PK), name, date_start, duration, status, location, ...
**sugarcrm_dim_users** — id, full_name
**sugarcrm_dim_campaigns** — id, name

### Reviews
**embedsocial_reviews** — id, review_id, source_id, source_name, source_address, author_name, rating, caption_text, reply_text, original_created_on

### Revenue Config
**site_revenue_config** — siteid(PK), sitecode, country, use_metric, coverage_pct, dcamt_var_pct, adj_var_pct, notes
**discount_plan** (VIEW) — discount_plan_name, discount_plan_unified, discount_pct, discount_months, discount_type

### Enriched Views (key analytical views)
**rentroll_enriched** — rentroll + discount info + unit_range/type + area fixes + FX conversion + occupancy metrics
**mimo_enriched** — mimo + date fixes + area fixes + FX conversion + unit_range/type
**units_info_enriched** — units_info + area fixes + unit_range/type
**vw_units_inventory** — units with climate/pillar/shape/noke from unit_range_mappings
**vw_budget_daily/monthly** — budget with FX conversion to SGD (variants: _bizplus, _subtenant)
**vw_gads_overview** — campaigns joined with account_map for country/currency + FX to SGD
**vw_customer_master** — leads with data quality scoring, source mapping, dedup flags
**vw_customer_journey** — leads with sequence tracking, channel changes, journey stages
**vw_customer_summary** — one row per customer: aggregated leads, conversion, multi-channel flags
**vw_customer_tenant_enriched** — customer_summary + tenant/rentroll data for full lifecycle
**vw_tenant_combined** — tenants from mimo + rentroll unified by email
**vw_tenant_mimo_summary** — tenant aggregates from move-in/out data
**vw_tenant_rentroll_summary** — tenant aggregates from rent roll snapshots
