"""
Create Phase 3 recommendation engine tables in esa_middleware:
  - mw_recommendations_served  (request/response log + outcome tracking)
  - mw_site_distance           (curated same-country proximity table)

Idempotent — uses IF NOT EXISTS throughout. Single transaction.

Run from backend/python:
    python3 migrations/mw_recommendation_engine_phase3.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    engine = create_engine(get_database_url('middleware'))

    with engine.begin() as conn:
        # ------------------------------------------------------------------
        # 1. mw_recommendations_served
        # ------------------------------------------------------------------
        print('[1] Creating mw_recommendations_served...')
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS mw_recommendations_served (
              id              BIGSERIAL PRIMARY KEY,
              served_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
              request_id      VARCHAR(64) UNIQUE NOT NULL,
              session_id      VARCHAR(64) NOT NULL,
              customer_id     VARCHAR(120),
              channel         VARCHAR(40) NOT NULL,
              mode            VARCHAR(40) NOT NULL,
              level           VARCHAR(40),

              previous_request_id VARCHAR(64),
              picked_slot         SMALLINT,
              action              VARCHAR(40),

              request_payload   JSONB NOT NULL,
              filters_applied   JSONB NOT NULL,
              relax_strategy    VARCHAR(60),

              candidates_pool_size  INTEGER,
              total_matches         INTEGER,

              slot1_unit_id INTEGER, slot1_plan_id INTEGER, slot1_concession_id INTEGER,
              slot1_first_month NUMERIC(14,4), slot1_total_contract NUMERIC(14,4),
              slot2_unit_id INTEGER, slot2_plan_id INTEGER, slot2_concession_id INTEGER,
              slot2_first_month NUMERIC(14,4), slot2_total_contract NUMERIC(14,4),
              slot3_unit_id INTEGER, slot3_plan_id INTEGER, slot3_concession_id INTEGER,
              slot3_first_month NUMERIC(14,4), slot3_total_contract NUMERIC(14,4),

              full_response   JSONB,

              booked_unit_id  INTEGER,
              booked_plan_id  INTEGER,
              booked_at       TIMESTAMPTZ,
              booked_slot     SMALLINT
            )
        """))
        print('    done')

        print('[2] Creating indexes on mw_recommendations_served...')
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS recs_served_at_idx
                ON mw_recommendations_served (served_at DESC)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS recs_session_idx
                ON mw_recommendations_served (session_id, served_at DESC)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS recs_customer_idx
                ON mw_recommendations_served (customer_id)
                WHERE customer_id IS NOT NULL
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS recs_channel_mode_idx
                ON mw_recommendations_served (channel, mode, level)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS recs_outcome_idx
                ON mw_recommendations_served (booked_at)
                WHERE booked_at IS NOT NULL
        """))
        print('    done')

        # ------------------------------------------------------------------
        # 2. mw_site_distance
        # ------------------------------------------------------------------
        print('[3] Creating mw_site_distance...')
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS mw_site_distance (
              from_site_code VARCHAR(20) NOT NULL,
              to_site_code   VARCHAR(20) NOT NULL,
              distance_km    NUMERIC(8,2) NOT NULL,
              same_country   BOOLEAN NOT NULL,
              notes          TEXT,
              updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
              updated_by     VARCHAR(120),
              PRIMARY KEY (from_site_code, to_site_code)
            )
        """))
        print('    done')

        print('[4] Creating indexes on mw_site_distance...')
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS site_dist_from_idx
                ON mw_site_distance (from_site_code, distance_km ASC)
        """))
        print('    done')

    print('Phase 3 schema migration complete.')


if __name__ == '__main__':
    main()
