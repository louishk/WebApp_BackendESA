-- View: vw_budget_daily
-- Daily budget view for Self-Storage only
-- Prorates monthly budget values into daily values (÷ days in month)
-- occupied_nla is linearly interpolated across the month using occupancy_growth
-- rental_revenue and occupancy_pct are recalculated from the interpolated NLA
-- Adds _sgd columns for all monetary fields using fx_rates_monthly

CREATE OR REPLACE VIEW vw_budget_daily AS
SELECT
    b.id AS budget_id,
    b.internal_code,
    b.site_code,
    d.day::date AS date,
    b.currency,
    b.metric,
    b.type,
    b.sub_type,
    b.total_available_nla,
    -- Interpolated occupied NLA: centered linear growth so monthly average = occupied_nla
    (b.occupied_nla
        + COALESCE(b.occupancy_growth, 0) * (2 * EXTRACT(DAY FROM d.day::date) - days_in.cnt - 1) / (2 * days_in.cnt)
    )::numeric(18,6) AS occupied_nla,
    (b.occupancy_growth / days_in.cnt)::numeric(18,6) AS occupancy_growth,
    b.avr_rental_rate,
    (CASE WHEN b.currency = 'SGD' THEN b.avr_rental_rate
         ELSE b.avr_rental_rate / fx.avg_rate
    END)::numeric(18,6) AS avr_rental_rate_sgd,
    -- Rental revenue recalculated from daily NLA × rate
    ((b.occupied_nla
        + COALESCE(b.occupancy_growth, 0) * (2 * EXTRACT(DAY FROM d.day::date) - days_in.cnt - 1) / (2 * days_in.cnt))
        * b.avr_rental_rate / days_in.cnt
    )::numeric(18,6) AS rental_revenue,
    (CASE WHEN b.currency = 'SGD' THEN
        (b.occupied_nla
            + COALESCE(b.occupancy_growth, 0) * (2 * EXTRACT(DAY FROM d.day::date) - days_in.cnt - 1) / (2 * days_in.cnt))
            * b.avr_rental_rate / days_in.cnt
         ELSE
        (b.occupied_nla
            + COALESCE(b.occupancy_growth, 0) * (2 * EXTRACT(DAY FROM d.day::date) - days_in.cnt - 1) / (2 * days_in.cnt))
            * b.avr_rental_rate / days_in.cnt / fx.avg_rate
    END)::numeric(18,6) AS rental_revenue_sgd,
    -- Occupancy pct recalculated from daily NLA
    ((b.occupied_nla
        + COALESCE(b.occupancy_growth, 0) * (2 * EXTRACT(DAY FROM d.day::date) - days_in.cnt - 1) / (2 * days_in.cnt))
        / NULLIF(b.total_available_nla, 0)
    )::numeric(18,6) AS occupancy_pct,
    -- Other revenue fields: simple prorate
    (b.maintenance                   / days_in.cnt)::numeric(18,6) AS maintenance,
    (CASE WHEN b.currency = 'SGD' THEN b.maintenance / days_in.cnt
         ELSE b.maintenance / days_in.cnt / fx.avg_rate
    END)::numeric(18,6) AS maintenance_sgd,
    (b.electricity                   / days_in.cnt)::numeric(18,6) AS electricity,
    (CASE WHEN b.currency = 'SGD' THEN b.electricity / days_in.cnt
         ELSE b.electricity / days_in.cnt / fx.avg_rate
    END)::numeric(18,6) AS electricity_sgd,
    (b.carpark_revenue               / days_in.cnt)::numeric(18,6) AS carpark_revenue,
    (CASE WHEN b.currency = 'SGD' THEN b.carpark_revenue / days_in.cnt
         ELSE b.carpark_revenue / days_in.cnt / fx.avg_rate
    END)::numeric(18,6) AS carpark_revenue_sgd,
    (b.insurance_revenue             / days_in.cnt)::numeric(18,6) AS insurance_revenue,
    (CASE WHEN b.currency = 'SGD' THEN b.insurance_revenue / days_in.cnt
         ELSE b.insurance_revenue / days_in.cnt / fx.avg_rate
    END)::numeric(18,6) AS insurance_revenue_sgd,
    (b.copier_revenue                / days_in.cnt)::numeric(18,6) AS copier_revenue,
    (CASE WHEN b.currency = 'SGD' THEN b.copier_revenue / days_in.cnt
         ELSE b.copier_revenue / days_in.cnt / fx.avg_rate
    END)::numeric(18,6) AS copier_revenue_sgd,
    (b.facility_revenue              / days_in.cnt)::numeric(18,6) AS facility_revenue,
    (CASE WHEN b.currency = 'SGD' THEN b.facility_revenue / days_in.cnt
         ELSE b.facility_revenue / days_in.cnt / fx.avg_rate
    END)::numeric(18,6) AS facility_revenue_sgd,
    (b.others_revenue                / days_in.cnt)::numeric(18,6) AS others_revenue,
    (CASE WHEN b.currency = 'SGD' THEN b.others_revenue / days_in.cnt
         ELSE b.others_revenue / days_in.cnt / fx.avg_rate
    END)::numeric(18,6) AS others_revenue_sgd
FROM budget b
CROSS JOIN LATERAL (
    SELECT DATE_PART('days',
        DATE_TRUNC('month', b.date) + INTERVAL '1 month' - INTERVAL '1 day'
    )::numeric AS cnt
) days_in
CROSS JOIN LATERAL (
    SELECT generate_series(
        b.date,
        (b.date + INTERVAL '1 month' - INTERVAL '1 day')::date,
        INTERVAL '1 day'
    ) AS day
) d
LEFT JOIN fx_rates_monthly fx
    ON to_char(b.date, 'YYYY-MM') = fx.year_month
    AND fx.target_currency = b.currency
    AND b.currency <> 'SGD'
WHERE b.type = 'Self-Storage';
