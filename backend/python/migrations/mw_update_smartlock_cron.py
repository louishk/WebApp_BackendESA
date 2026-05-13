"""
Smart-lock cron rebalance (Section 1 of the 2026-05-13 design spec).

Moves the SOAP/sync chain to hourly cadence while keeping the (free) igloo
REST poll at 15-minute granularity:

    igloo            → */15 * * * *   (:00, :15, :30, :45)
    ccws_units       → 45 * * * *     (:45)            [spec calls it ccws_units_info]
    ccws_gate_access → 50 * * * *     (:50)
    igloo_pin_sync   → 55 * * * *     (:55)

Idempotent UPDATE on a stable primary key. Run from backend/python:
    python3 migrations/mw_update_smartlock_cron.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url

SCHEDULES = [
    ('igloo',            '{"cron": "*/15 * * * *"}'),
    ('ccws_units',       '{"cron": "45 * * * *"}'),
    ('ccws_gate_access', '{"cron": "50 * * * *"}'),
    ('igloo_pin_sync',   '{"cron": "55 * * * *"}'),
]


def main():
    eng = create_engine(get_database_url('middleware'))
    with eng.begin() as conn:
        for name, sched in SCHEDULES:
            res = conn.execute(text("""
                UPDATE mw_sync_pipelines
                SET schedule_config = CAST(:sched AS jsonb),
                    updated_at = NOW()
                WHERE pipeline_name = :name
            """), {'name': name, 'sched': sched})
            print(f"  {name}: rows updated = {res.rowcount} → {sched}")
    print("done")


if __name__ == '__main__':
    main()
