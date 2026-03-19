"""
Backfill deleted units from SiteLink CSV export into units_info.

Reads 'temp/unit del jan 2026.csv' (no headers, 70 columns) and inserts
into units_info with ON CONFLICT DO NOTHING to preserve active units.

Usage:
    cd /home/louis/PycharmProjects/WebApp_BackendESA
    python scripts/backfill_deleted_units.py
"""

import csv
import sys
from pathlib import Path
from datetime import datetime
from decimal import Decimal, InvalidOperation

# Add backend path
sys.path.insert(0, str(Path(__file__).parent.parent / 'backend' / 'python'))

from common import create_engine_from_config, DataLayerConfig, SessionManager, Base, UnitsInfo
from sqlalchemy.dialects.postgresql import insert as pg_insert

CSV_PATH = Path(__file__).parent.parent / 'temp' / 'unit del jan 2026.csv'

# CSV column indices (0-based) → UnitsInfo field name
COLUMN_MAP = {
    0: 'UnitID',
    1: 'SiteID',
    3: 'UnitTypeID',
    14: 'sUnitName',
    16: 'iFloor',
    17: 'dcWidth',
    18: 'dcLength',
    21: 'dcMapTheta',
    22: 'bMapReversWL',
    23: 'dcPushRate',
    24: 'dcStdRate',
    25: 'dcStdWeeklyRate',
    26: 'dcStdSecDep',
    28: 'bPower',
    29: 'bClimate',
    30: 'bInside',
    31: 'bAlarm',
    32: 'bRentable',
    33: 'bRented',
    35: 'deleted_at',  # dDeleted
    36: 'sUnitNote',
    39: 'bCorporate',
    40: 'bMobile',
    48: 'sUnitDesc',
    49: 'iEntryLoc',
    51: 'bExcludeFromWebsite',
    53: 'iADA',
    54: 'iDoorType',
    65: 'iDaysVacant',
    66: 'dcWebRate',
}

# Fields that need type conversion
INT_FIELDS = {'UnitID', 'SiteID', 'UnitTypeID', 'iFloor', 'iEntryLoc', 'iADA', 'iDoorType', 'iDaysVacant'}
DECIMAL_FIELDS = {'dcWidth', 'dcLength', 'dcMapTheta', 'dcPushRate', 'dcStdRate',
                  'dcStdWeeklyRate', 'dcStdSecDep', 'dcWebRate'}
BOOL_FIELDS = {'bMapReversWL', 'bPower', 'bClimate', 'bInside', 'bAlarm',
               'bRentable', 'bRented', 'bCorporate', 'bMobile', 'bExcludeFromWebsite'}
DATE_FIELDS = {'deleted_at'}
STRING_FIELDS = {'sUnitName': 100, 'sUnitNote': 500, 'sUnitDesc': 500}


def parse_value(field_name, raw_value):
    """Convert raw CSV value to appropriate Python type."""
    raw = raw_value.strip() if raw_value else ''

    if raw in ('NULL', '', 'null'):
        return None

    if field_name in INT_FIELDS:
        try:
            return int(float(raw))
        except (ValueError, TypeError):
            return None

    if field_name in DECIMAL_FIELDS:
        try:
            return Decimal(raw)
        except (InvalidOperation, ValueError):
            return None

    if field_name in BOOL_FIELDS:
        return raw in ('1', 'True', 'true')

    if field_name in DATE_FIELDS:
        try:
            return datetime.strptime(raw[:10], '%Y-%m-%d').date()
        except (ValueError, TypeError):
            return None

    # String fields — cap to model max length
    max_len = STRING_FIELDS.get(field_name, 500)
    return raw[:max_len] if raw else None


def main():
    if not CSV_PATH.exists():
        print(f"CSV not found: {CSV_PATH}")
        sys.exit(1)

    config = DataLayerConfig.from_env()
    db_config = config.databases.get('postgresql')
    if not db_config:
        print("PostgreSQL configuration not found")
        sys.exit(1)

    engine = create_engine_from_config(db_config)

    # Read and transform CSV
    records = []
    with open(CSV_PATH, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        for row_num, row in enumerate(reader, 1):
            if len(row) < 67:
                print(f"  Row {row_num}: skipping (only {len(row)} columns)")
                continue

            record = {}
            for col_idx, field_name in COLUMN_MAP.items():
                record[field_name] = parse_value(field_name, row[col_idx])

            # Must have UnitID and SiteID
            if record.get('UnitID') is None or record.get('SiteID') is None:
                print(f"  Row {row_num}: skipping (missing UnitID/SiteID)")
                continue

            records.append(record)

    print(f"Parsed {len(records)} records from CSV")

    # Upsert with ON CONFLICT DO NOTHING
    session_mgr = SessionManager(engine)
    with session_mgr.session_scope() as session:
        chunk_size = 500
        inserted = 0
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            stmt = pg_insert(UnitsInfo).values(chunk)
            stmt = stmt.on_conflict_do_nothing(index_elements=['SiteID', 'UnitID'])
            result = session.execute(stmt)
            inserted += result.rowcount
            print(f"  Chunk {i // chunk_size + 1}: {result.rowcount} new rows inserted")

    print(f"\nDone! Inserted {inserted} deleted units into units_info (skipped {len(records) - inserted} existing)")


if __name__ == '__main__':
    main()
