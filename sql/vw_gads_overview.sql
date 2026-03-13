-- View: vw_gads_overview
-- Target database: esa_pbi
-- Auto-exported from pg_get_viewdef on 2026-03-05

CREATE OR REPLACE VIEW vw_gads_overview AS
 SELECT d.segments_date,
    c.campaign_id,
    c.campaign_name,
    c.campaign_status,
    c.channel_type,
    am.country,
    am.currency,
    d.device,
    d.ad_network_type,
    d.impressions,
    d.clicks,
    d.cost_micros::numeric / '1000000'::numeric AS cost_local,
    d.conversions,
    d.conversions_value,
    d.interactions,
        CASE am.currency
            WHEN 'SGD'::text THEN d.cost_micros::numeric / '1000000'::numeric
            WHEN 'KRW'::text THEN d.cost_micros::numeric / '1000000'::numeric / COALESCE(fx.avg_rate, 1::numeric)
            ELSE d.cost_micros::numeric / '1000000'::numeric
        END AS cost_sgd
   FROM gads_campaign_daily d
     JOIN gads_campaigns c ON d.campaign_id = c.campaign_id
     JOIN gads_account_map am ON c.customer_id = am.customer_id
     LEFT JOIN fx_rates_monthly fx ON fx.target_currency::text = am.currency::text AND fx.year = EXTRACT(year FROM d.segments_date)::integer AND fx.month = EXTRACT(month FROM d.segments_date)::integer AND am.currency::text <> 'SGD'::text;
