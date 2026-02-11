-- Migration 005: Add reviewed column to inventory_unit_overrides
-- Allows staff to mark units as "reviewed" in the inventory checker

ALTER TABLE inventory_unit_overrides ADD COLUMN IF NOT EXISTS reviewed BOOLEAN DEFAULT FALSE;
