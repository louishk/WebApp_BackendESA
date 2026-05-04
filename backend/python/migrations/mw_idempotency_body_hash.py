"""
H4 — body-hash mismatch detection for Idempotency-Key replay protection.

Adds:
  mw_idempotency_keys
    + body_hash TEXT   SHA-256 of the canonical request body (sorted keys),
                       so a same-key replay with a DIFFERENT body returns
                       HTTP 422 instead of silently replaying the wrong unit.

Pre-existing rows: body_hash stays NULL — lookup() treats NULL as "legacy
entry, no body comparison" and replays as before. New stores always populate
the column going forward.

Run from backend/python:
    python3 migrations/mw_idempotency_body_hash.py
"""
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def main():
    url = get_database_url('middleware')
    engine = create_engine(url)
    with engine.begin() as conn:
        logger.info("Adding body_hash column to mw_idempotency_keys ...")
        conn.execute(text("""
            ALTER TABLE mw_idempotency_keys
            ADD COLUMN IF NOT EXISTS body_hash TEXT
        """))
        logger.info("Done.")


if __name__ == '__main__':
    main()
