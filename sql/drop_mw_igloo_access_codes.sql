-- Drop orphan mw_igloo_access_codes from esa_middleware.
-- Table was created with intent to cache Igloo PINs but no pipeline was ever wired to populate it.
-- PIN audit calls Igloo API live; no code reads this table.
-- Run with: PGPASSWORD=... psql -h esapbi.postgres.database.azure.com -U esa_pbi_admin -d esa_middleware -f sql/drop_mw_igloo_access_codes.sql

DROP TABLE IF EXISTS mw_igloo_access_codes;
