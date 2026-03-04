-- Migration 017: Widen key_hash column for bcrypt hashes
-- bcrypt hashes are ~60 chars; SHA-256 hex was 64 chars.
-- All existing keys must be regenerated since SHA-256 cannot be reversed to re-hash.

ALTER TABLE api_keys ALTER COLUMN key_hash TYPE VARCHAR(255);
