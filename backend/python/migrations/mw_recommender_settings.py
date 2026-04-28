"""
Add mw_recommender_settings — admin-editable global tunables for the
recommendation engine. Key/value with in-process caching.

Settings live here (not in YAML) so admins can tweak without redeploying.
The recommender reads with a short TTL cache so a save propagates within
~60s without a service restart.

Run from backend/python:
    python3 migrations/mw_recommender_settings.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        print('[1] Creating mw_recommender_settings...')
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS mw_recommender_settings (
                key         VARCHAR(80) PRIMARY KEY,
                value       TEXT,
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_by  VARCHAR(120)
            )
        """))
        print('    done')


if __name__ == '__main__':
    main()
