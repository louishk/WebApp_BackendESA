-- Migration: Pricing Tool — Vault Secret Placeholders
-- Target database: esa_backend
-- Purpose: Insert NULL-valued placeholder rows for the new vault keys required
--          by the pricing tool. Real values are pasted in via /admin/secrets UI
--          after deployment — never committed to source control.
-- Safe to re-run: INSERT ... ON CONFLICT DO NOTHING is fully idempotent.

BEGIN;

INSERT INTO app_secrets (key, value_encrypted, environment, description)
VALUES
    (
        'SITELINK_USAGE_PW_STANDARD_RATE',
        'PLACEHOLDER',
        'all',
        'SiteLink usage password for UnitStandardRateUpdate v1/v2/v3. Actual value: UnitStandardRateP@SS (paste via /admin/secrets).'
    ),
    (
        'SITELINK_USAGE_PW_WEB_RATE',
        'PLACEHOLDER',
        'all',
        'SiteLink usage password for UnitWebRateUpdate. Actual value: UnitWebRateP@SS (paste via /admin/secrets).'
    ),
    (
        'SITELINK_USAGE_PW_PUSH_RATE',
        'PLACEHOLDER',
        'all',
        'SiteLink usage password for UnitPushRateUpdate. Stored even though C234 corp is not licensed (Ret_Code=-95). Actual value: UnitPushRateP@SS (paste via /admin/secrets).'
    ),
    (
        'SITELINK_API_KEY_TEST',
        'PLACEHOLDER',
        'development',
        'SiteLink test/sandbox API key scoped to CCTST/Demo corp only. Do NOT use against production corps. Actual value: EXTRASPAJYOSS3SPJDOW (paste via /admin/secrets).'
    ),
    (
        'AZURE_CLAUDE_ENDPOINT',
        'PLACEHOLDER',
        'all',
        'Azure Cognitive Services base URL for Claude via Azure AI Foundry. Example: https://louis-mmsp9ee1-eastus2.cognitiveservices.azure.com/ (paste via /admin/secrets).'
    ),
    (
        'AZURE_CLAUDE_KEY',
        'PLACEHOLDER',
        'all',
        'Azure auth token for Claude API calls via Azure AI Foundry (paste via /admin/secrets).'
    ),
    (
        'AZURE_CLAUDE_DEPLOYMENT_PRIMARY',
        'PLACEHOLDER',
        'all',
        'Azure deployment name for the primary Claude model (Sonnet-class). Used for nightly AI pricing reviews. Example: claude-sonnet-4-6 (paste via /admin/secrets).'
    ),
    (
        'AZURE_CLAUDE_DEPLOYMENT_FAST',
        'PLACEHOLDER',
        'all',
        'Azure deployment name for the fast/cheap Claude model (Haiku-class). Used for backtest replays and low-context overrides. Example: claude-haiku-4-5 (paste via /admin/secrets).'
    )
ON CONFLICT (key) DO NOTHING;

COMMIT;
