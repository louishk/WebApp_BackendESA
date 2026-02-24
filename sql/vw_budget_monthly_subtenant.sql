-- View: vw_budget_monthly_subtenant
-- Monthly budget view for Subtenant only
-- Raw monthly values with _sgd columns using fx_rates_monthly

CREATE OR REPLACE VIEW vw_budget_monthly_subtenant AS
SELECT
    b.id AS budget_id,
    b.internal_code,
    b.site_code,
    b.date,
    b.currency,
    b.metric,
    b.type,
    b.sub_type,
    b.total_available_nla,
    b.occupied_nla,
    b.occupancy_growth,
    b.avr_rental_rate,
    (CASE WHEN b.currency = 'SGD' THEN b.avr_rental_rate
         ELSE b.avr_rental_rate / fx.avg_rate
    END)::numeric(18,6) AS avr_rental_rate_sgd,
    b.rental_revenue,
    (CASE WHEN b.currency = 'SGD' THEN b.rental_revenue
         ELSE b.rental_revenue / fx.avg_rate
    END)::numeric(18,6) AS rental_revenue_sgd,
    (b.rental_revenue - LAG(b.rental_revenue) OVER (PARTITION BY b.site_code ORDER BY b.date))::numeric(18,6) AS revenue_growth,
    (CASE WHEN b.currency = 'SGD' THEN
        b.rental_revenue - LAG(b.rental_revenue) OVER (PARTITION BY b.site_code ORDER BY b.date)
     ELSE
        (b.rental_revenue - LAG(b.rental_revenue) OVER (PARTITION BY b.site_code ORDER BY b.date)) / fx.avg_rate
    END)::numeric(18,6) AS revenue_growth_sgd,
    b.occupancy_pct,
    b.maintenance,
    (CASE WHEN b.currency = 'SGD' THEN b.maintenance
         ELSE b.maintenance / fx.avg_rate
    END)::numeric(18,6) AS maintenance_sgd,
    b.electricity,
    (CASE WHEN b.currency = 'SGD' THEN b.electricity
         ELSE b.electricity / fx.avg_rate
    END)::numeric(18,6) AS electricity_sgd,
    b.carpark_revenue,
    (CASE WHEN b.currency = 'SGD' THEN b.carpark_revenue
         ELSE b.carpark_revenue / fx.avg_rate
    END)::numeric(18,6) AS carpark_revenue_sgd,
    b.insurance_revenue,
    (CASE WHEN b.currency = 'SGD' THEN b.insurance_revenue
         ELSE b.insurance_revenue / fx.avg_rate
    END)::numeric(18,6) AS insurance_revenue_sgd,
    b.copier_revenue,
    (CASE WHEN b.currency = 'SGD' THEN b.copier_revenue
         ELSE b.copier_revenue / fx.avg_rate
    END)::numeric(18,6) AS copier_revenue_sgd,
    b.facility_revenue,
    (CASE WHEN b.currency = 'SGD' THEN b.facility_revenue
         ELSE b.facility_revenue / fx.avg_rate
    END)::numeric(18,6) AS facility_revenue_sgd,
    b.others_revenue,
    (CASE WHEN b.currency = 'SGD' THEN b.others_revenue
         ELSE b.others_revenue / fx.avg_rate
    END)::numeric(18,6) AS others_revenue_sgd
FROM budget b
LEFT JOIN fx_rates_monthly fx
    ON to_char(b.date, 'YYYY-MM') = fx.year_month
    AND fx.target_currency = b.currency
    AND b.currency <> 'SGD'
WHERE b.type = 'Subtenant';
