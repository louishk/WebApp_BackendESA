"""
Phase 4 Part 2 — perpetual + prepay orchestration schema.

Adds:
  mw_discount_plans
    + prepayment_months          INTEGER         (NULL = no prepay required)
    + post_prepay_ecri_pct       NUMERIC(5,2)    (NULL = use global setting)

  mw_unit_discount_candidates    (mirror of plan fields)
    + prepayment_months          INTEGER
    + post_prepay_ecri_pct       NUMERIC(5,2)

  mw_lease_followup_jobs         (DLQ for post-move-in SOAP follow-ups)
    A durable record of every SOAP call we need to fire after a successful
    MoveIn — PaymentSimpleCash for prepay surplus, ScheduleTenantRateChange
    for the future ECRI. The /move-in handler enqueues them; a worker in
    backend-scheduler drains the queue every 10 s with exponential backoff.

Settings keys (registered in code, persisted via mw_recommender_settings):
    ecri_default_offset_months          12     fallback offset when no plan-level value
    ecri_default_pct                    5.0    default ECRI uplift %
    ecri_min_offset_months              6      floor; never schedule earlier than this
    ecri_auto_schedule_enabled          false  master switch — schedule rate changes at move-in
    perpetual_auto_payment_enabled      false  master switch — push prepay surplus to advance dPaidThru
    move_in_cost_use_soap_fallback      true   /move-in/cost calls SOAP for SOAP-truth confirmation

Run from backend/python:
    python3 migrations/mw_perpetual_orchestration.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        print('[1] Adding plan-level perpetual+prepay fields ...')
        conn.execute(text("""
            ALTER TABLE mw_discount_plans
                ADD COLUMN IF NOT EXISTS prepayment_months    INTEGER,
                ADD COLUMN IF NOT EXISTS post_prepay_ecri_pct NUMERIC(5, 2)
        """))
        print('    done')

        print('[2] Mirroring fields onto mw_unit_discount_candidates ...')
        conn.execute(text("""
            ALTER TABLE mw_unit_discount_candidates
                ADD COLUMN IF NOT EXISTS prepayment_months    INTEGER,
                ADD COLUMN IF NOT EXISTS post_prepay_ecri_pct NUMERIC(5, 2)
        """))
        print('    done')

        print('[3] Creating mw_lease_followup_jobs (DLQ) ...')
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS mw_lease_followup_jobs (
                id                  BIGSERIAL PRIMARY KEY,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                ledger_id           INTEGER NOT NULL,
                site_code           VARCHAR(20) NOT NULL,
                tenant_id           INTEGER,
                unit_id             INTEGER,
                action_type         VARCHAR(40) NOT NULL,
                payload             JSONB NOT NULL,
                status              VARCHAR(20) NOT NULL DEFAULT 'pending',
                attempts            INTEGER NOT NULL DEFAULT 0,
                next_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_attempt_at     TIMESTAMPTZ,
                last_error          TEXT,
                soap_response       JSONB,
                related_request_id  VARCHAR(64),
                related_session_id  VARCHAR(64),
                related_customer_id VARCHAR(120)
            )
        """))
        # Worker query: pick up pending jobs whose next_attempt has arrived
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_lfj_pending
                ON mw_lease_followup_jobs (status, next_attempt_at)
                WHERE status IN ('pending', 'running')
        """))
        # Reverse lookup by lease
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_lfj_ledger
                ON mw_lease_followup_jobs (ledger_id)
        """))
        # Time-based admin queries
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_lfj_created
                ON mw_lease_followup_jobs (created_at DESC)
        """))
        print('    done')


if __name__ == '__main__':
    main()
