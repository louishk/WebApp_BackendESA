-- Migration 013: Discount Plans v2 enhancements
-- Split periods (promo + booking) and config table for translatable dropdown options

-- 4 new date columns for split periods
ALTER TABLE discount_plans ADD COLUMN IF NOT EXISTS promo_period_start DATE;
ALTER TABLE discount_plans ADD COLUMN IF NOT EXISTS promo_period_end DATE;
ALTER TABLE discount_plans ADD COLUMN IF NOT EXISTS booking_period_start DATE;
ALTER TABLE discount_plans ADD COLUMN IF NOT EXISTS booking_period_end DATE;

-- Migrate existing period_start/end → promo_period
UPDATE discount_plans SET promo_period_start = period_start, promo_period_end = period_end
WHERE period_start IS NOT NULL OR period_end IS NOT NULL;

-- Config table for translatable dropdown options
CREATE TABLE IF NOT EXISTS discount_plan_config (
    id SERIAL PRIMARY KEY,
    field_name VARCHAR(50) NOT NULL,
    option_value VARCHAR(255) NOT NULL,
    translations JSONB DEFAULT '{}',
    sort_order INTEGER DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(field_name, option_value)
);
CREATE INDEX IF NOT EXISTS idx_dpc_field ON discount_plan_config(field_name);

-- Seed defaults
INSERT INTO discount_plan_config (field_name, option_value, sort_order) VALUES
    ('deposit', '1 Month (Refundable)', 1), ('deposit', '2 Months (Refundable)', 2), ('deposit', 'No Deposit', 3),
    ('payment_terms', 'Monthly', 1), ('payment_terms', 'Prepaid (6M)', 2), ('payment_terms', 'Prepaid (12M)', 3),
    ('termination_notice', '1 Month', 1), ('termination_notice', '30 Days', 2), ('termination_notice', '14 Days', 3),
    ('sales_extra_discount', 'Not Eligible', 1), ('sales_extra_discount', 'Eligible', 2),
    ('switch_to_us', 'Not Eligible', 1), ('switch_to_us', 'Eligible', 2),
    ('referral_program', 'Not Eligible', 1), ('referral_program', 'Eligible', 2),
    ('distribution_channel', 'Direct Mailing', 1), ('distribution_channel', 'Online', 2),
    ('distribution_channel', 'WhatsApp', 3), ('distribution_channel', 'Walk-In', 4)
ON CONFLICT (field_name, option_value) DO NOTHING;
