-- Migration 054: Stripe webhook idempotency table
-- Prevents duplicate processing when Stripe retries webhooks (up to 3 days).
-- event_id (evt_xxx) is the natural idempotency key — UNIQUE constraint is
-- the enforcement mechanism; the app catches IntegrityError on INSERT.

CREATE TABLE IF NOT EXISTS stripe_webhook_events (
    id              bigserial       PRIMARY KEY,
    event_id        text            NOT NULL UNIQUE,
    event_type      text            NOT NULL,
    payment_intent_id text          NULL,
    received_at     timestamptz     NOT NULL DEFAULT now(),
    processed_at    timestamptz     NULL,
    status          text            NOT NULL DEFAULT 'received',
    error_message   text            NULL
);

CREATE INDEX IF NOT EXISTS idx_swe_payment_intent_id
    ON stripe_webhook_events (payment_intent_id);
