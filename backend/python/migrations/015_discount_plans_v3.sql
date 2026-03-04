-- Migration 015: Discount Plans v3 - seed move_in_range + lock_in_period config options
-- Run against esa_backend database

INSERT INTO discount_plan_config (field_name, option_value, sort_order) VALUES
    ('move_in_range', 'Maximum 14 days after the booking date', 1),
    ('move_in_range', 'Within 14 days of booking confirmation', 2),
    ('move_in_range', 'Within promotion period', 3),
    ('move_in_range', 'No restriction', 4),
    ('lock_in_period', 'No Lock-In', 1),
    ('lock_in_period', 'Minimum 3 months', 2),
    ('lock_in_period', 'Minimum 6 months', 3),
    ('lock_in_period', 'Minimum 12 months', 4)
ON CONFLICT (field_name, option_value) DO NOTHING;
