"""
Create esa_middleware.mw_recommender_excluded_unit_types via the
RecommenderExcludedUnitType model. Idempotent — safe to re-run.

Does NOT pre-seed any exclusions. Admins manage the list from the
Recommendation Engine UI (/recommendation-engine/unit-availability).

Run from backend/python:
    python3 migrations/mw_seed_recommender_exclusions.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine
from common.config_loader import get_database_url
from common.models import RecommenderExcludedUnitType, Base as CommonBase


def main():
    mw_engine = create_engine(get_database_url('middleware'))
    print('[1] Creating mw_recommender_excluded_unit_types (if missing)...')
    CommonBase.metadata.create_all(
        mw_engine, tables=[RecommenderExcludedUnitType.__table__],
    )
    print('    done')


if __name__ == '__main__':
    main()
