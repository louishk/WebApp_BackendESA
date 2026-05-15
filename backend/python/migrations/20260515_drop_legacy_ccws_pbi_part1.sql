-- Drop stale ccws_site_billing_config from esa_pbi
--
-- Context: ccws_site_billing_config is now canonically owned by esa_middleware
-- (pipeline `ccws_site_billing_config` writes there via get_engine('middleware')).
-- The PBI copy is an orphan from a previous pipeline version — pre-flight audit
-- (2026-05-15) confirmed zero readers across:
--   - backend/python/web/routes/*  (only get_middleware_session)
--   - backend/python/web/services/recommender.py (middleware session)
--   - backend/python/common/movein_cost_calculator.py (get_engine('middleware'))
--   - backend/python/scripts/compare_calc_vs_soap.py (middleware engine)
--   - tests + probes (only ccws_discount/insurance/charge_desc — not this table)

BEGIN;
DROP TABLE IF EXISTS ccws_site_billing_config CASCADE;
COMMIT;
