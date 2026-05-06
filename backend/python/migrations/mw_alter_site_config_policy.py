"""
Add per-site revoke policy flag columns to mw_smart_lock_site_config.

New columns (all BOOLEAN NOT NULL DEFAULT TRUE):
  - revoke_on_gate_locked
  - revoke_on_overlocked
  - revoke_on_not_rentable

Safe to run multiple times (IF NOT EXISTS).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url

SQL = """
ALTER TABLE mw_smart_lock_site_config
    ADD COLUMN IF NOT EXISTS revoke_on_gate_locked  BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS revoke_on_overlocked   BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS revoke_on_not_rentable BOOLEAN NOT NULL DEFAULT TRUE;
"""


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        conn.execute(text(SQL))
    print('[mw_alter_site_config_policy] Added revoke_on_gate_locked, '
          'revoke_on_overlocked, revoke_on_not_rentable to '
          'mw_smart_lock_site_config (DEFAULT TRUE, idempotent).')


if __name__ == '__main__':
    main()
