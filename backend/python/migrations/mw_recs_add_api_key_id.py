"""
S6 — attribution scope: stamp api_key_id on every recommendation row so
the booking matcher can refuse cross-key attribution attempts.

Adds:
  mw_recommendations_served.api_key_id   INTEGER  (nullable; legacy rows)

Pre-existing rows: api_key_id stays NULL — the matcher treats NULL as
"legacy / pre-S6" and falls back to today's session_id-only behaviour
for those rows. New rows always populate the column.

Run from backend/python:
    python3 migrations/mw_recs_add_api_key_id.py
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
        logger.info("Adding api_key_id column to mw_recommendations_served ...")
        conn.execute(text("""
            ALTER TABLE mw_recommendations_served
            ADD COLUMN IF NOT EXISTS api_key_id INTEGER
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_mw_recs_served_api_key_id
            ON mw_recommendations_served (api_key_id)
        """))
        logger.info("Done.")


if __name__ == '__main__':
    main()
