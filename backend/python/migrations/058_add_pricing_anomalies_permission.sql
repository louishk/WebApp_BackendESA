-- Migration 058: Add pricing anomalies tool permission to roles table
ALTER TABLE roles ADD COLUMN can_access_pricing_anomalies_tools BOOLEAN NOT NULL DEFAULT false;
UPDATE roles SET can_access_pricing_anomalies_tools = true WHERE name = 'admin';
