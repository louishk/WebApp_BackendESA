-- Migration 014: Remove sales_extra_discount column and config rows
ALTER TABLE discount_plans DROP COLUMN IF EXISTS sales_extra_discount;
DELETE FROM discount_plan_config WHERE field_name = 'sales_extra_discount';
