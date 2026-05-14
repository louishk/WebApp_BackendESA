"""
RentRollPipeline — fetch RentRoll snapshots from SMD SOAP API and upsert to esa_pbi.

Reads via SOAP (CallCenterWs GetRentRoll). Writes to esa_pbi.rentroll keyed on
(extract_date, SiteID, UnitID).

Modes:
  - auto (default): previous month + current month
  - manual:         arbitrary YYYY-MM start..end range

Behaviour by month status:
  - current month: delete prior current-month rows before upsert (no accumulation)
  - closed month:  clean stale non-EOM rows then upsert against EOM extract_date

Scope keys honoured (all optional):
  - mode:  'auto' | 'manual'                 (default 'auto')
  - start: 'YYYY-MM'                         (manual only)
  - end:   'YYYY-MM'                         (manual only)
"""

import logging
from datetime import datetime, date
from typing import Any, Dict, List

from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)


def _get_pipeline_config(pipeline_name: str, key: str, default=None):
    try:
        from common.config_loader import get_config
        cfg = get_config()
        pipeline_cfg = getattr(cfg.scheduler.pipelines, pipeline_name, None)
        if pipeline_cfg:
            return getattr(pipeline_cfg, key, default)
    except Exception:
        pass
    return default


def transform_record(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    from common import (
        convert_to_bool, convert_to_int, convert_to_decimal, convert_to_datetime,
    )
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'UnitID': convert_to_int(record.get('UnitID')),

        'LedgerID': convert_to_int(record.get('LedgerID')),
        'sUnit': record.get('sUnit'),
        'sSize': record.get('sSize'),
        'Area': convert_to_decimal(record.get('Area')),
        'sUnitName': record.get('sUnitName'),
        'UnitTypeID': convert_to_int(record.get('UnitTypeID')),
        'sTypeName': record.get('sTypeName'),

        'iFloor': convert_to_int(record.get('iFloor')),
        'dcWidth': convert_to_decimal(record.get('dcWidth')),
        'dcLength': convert_to_decimal(record.get('dcLength')),
        'iWalkThruOrder': convert_to_int(record.get('iWalkThruOrder')),
        'iDoorType': convert_to_int(record.get('iDoorType')),

        'dcMapTop': convert_to_decimal(record.get('dcMapTop')),
        'dcMapLeft': convert_to_decimal(record.get('dcMapLeft')),
        'dcMapTheta': convert_to_decimal(record.get('dcMapTheta')),
        'bMapReversWL': convert_to_bool(record.get('bMapReversWL')),
        'iEntryLoc': convert_to_int(record.get('iEntryLoc')),

        'dcPushRate': convert_to_decimal(record.get('dcPushRate')),
        'dcStdRate': convert_to_decimal(record.get('dcStdRate')),
        'dcStdWeeklyRate': convert_to_decimal(record.get('dcStdWeeklyRate')),
        'dcStdSecDep': convert_to_decimal(record.get('dcStdSecDep')),
        'dcStdLateFee': convert_to_decimal(record.get('dcStdLateFee')),
        'dcWebRate': convert_to_decimal(record.get('dcWebRate')),
        'dcWebPushRate': convert_to_decimal(record.get('dcWebPushRate')),
        'dcWebRateDated': convert_to_decimal(record.get('dcWebRateDated')),
        'dcSchedRateMonthly': convert_to_decimal(record.get('dcSchedRateMonthly')),
        'dcSchedRateWeekly': convert_to_decimal(record.get('dcSchedRateWeekly')),

        'bPower': convert_to_bool(record.get('bPower')),
        'bClimate': convert_to_bool(record.get('bClimate')),
        'bInside': convert_to_bool(record.get('bInside')),
        'bAlarm': convert_to_bool(record.get('bAlarm')),
        'bRentable': convert_to_bool(record.get('bRentable')),
        'bRented': convert_to_bool(record.get('bRented')),
        'bCorporate': convert_to_bool(record.get('bCorporate')),
        'bMobile': convert_to_bool(record.get('bMobile')),
        'bDamaged': convert_to_bool(record.get('bDamaged')),
        'bCollapsible': convert_to_bool(record.get('bCollapsible')),
        'bPermanent': convert_to_bool(record.get('bPermanent')),
        'bExcludeFromSqftReports': convert_to_bool(record.get('bExcludeFromSqftReports')),
        'bExcludeFromWebsite': convert_to_bool(record.get('bExcludeFromWebsite')),
        'bNotReadyToRent': convert_to_bool(record.get('bNotReadyToRent')),
        'bExcludeFromInsurance': convert_to_bool(record.get('bExcludeFromInsurance')),

        'iMobileStatus': convert_to_int(record.get('iMobileStatus')),
        'iADA': convert_to_int(record.get('iADA')),
        'iVehicleStorageAllowed': convert_to_int(record.get('iVehicleStorageAllowed')),
        'iDaysVacant': convert_to_int(record.get('iDaysVacant')),
        'EmployeeID': convert_to_int(record.get('EmployeeID')),

        'dCreated': convert_to_datetime(record.get('dCreated')),
        'dUpdated': convert_to_datetime(record.get('dUpdated')),
        'dUnitNote': convert_to_datetime(record.get('dUnitNote')),
        'dLeaseDate': convert_to_datetime(record.get('dLeaseDate')),
        'dPaidThru': convert_to_datetime(record.get('dPaidThru')),
        'dRentLastChanged': convert_to_datetime(record.get('dRentLastChanged')),
        'dSchedRentStrt': convert_to_datetime(record.get('dSchedRentStrt')),

        'TenantID': convert_to_int(record.get('TenantID')),
        'sTenant': record.get('sTenant'),
        'sCompany': record.get('sCompany'),
        'sEmail': record.get('sEmail'),
        'iAnnivDays': convert_to_int(record.get('iAnnivDays')),
        'sTaxExempt': record.get('sTaxExempt'),

        'dcSecDep': convert_to_decimal(record.get('dcSecDep')),
        'dcStandardRate': convert_to_decimal(record.get('dcStandardRate')),
        'dcRent': convert_to_decimal(record.get('dcRent')),
        'dcVar': convert_to_decimal(record.get('dcVar')),
        'dcSchedRent': convert_to_decimal(record.get('dcSchedRent')),
        'dcPrePaidRentLiability': convert_to_decimal(record.get('dcPrePaidRentLiability')),
        'dcInsurPremium': convert_to_decimal(record.get('dcInsurPremium')),

        'iAutoBillType': convert_to_int(record.get('iAutoBillType')),
        'DaysSame': convert_to_int(record.get('DaysSame')),

        'SiteID1': convert_to_int(record.get('SiteID1')),
        'Area1': convert_to_decimal(record.get('Area1')),
        'OldPK': record.get('OldPK'),
        'uTS': record.get('uTS'),
        'sUnitNote': record.get('sUnitNote'),
    }


def fetch_rentroll_data(report_client, location_codes: List[str],
                        start_date: datetime, end_date: datetime,
                        extract_date: date) -> List[Dict[str, Any]]:
    from common import deduplicate_records
    all_data: List[Dict[str, Any]] = []

    for location_code in location_codes:
        try:
            results = report_client.get_rent_roll(
                location_code=location_code,
                start_date=start_date,
                end_date=end_date,
            )
            for record in results:
                all_data.append(transform_record(record, extract_date))
            logger.info("rentroll fetched %s: %d records", location_code, len(results))
        except Exception as e:
            logger.exception("rentroll fetch failed for %s: %s", location_code, e)
            continue

    original_count = len(all_data)
    all_data = deduplicate_records(all_data, ['extract_date', 'SiteID', 'UnitID'])
    if len(all_data) < original_count:
        logger.info("rentroll deduplicated: %d → %d", original_count, len(all_data))
    return all_data


def push_to_database(data: List[Dict[str, Any]], config, year: int, month: int, status: str) -> int:
    from common import (
        create_engine_from_config, SessionManager, UpsertOperations, Base,
        RentRoll, delete_current_month_records, delete_non_eom_records,
    )

    if not data:
        logger.warning("rentroll: no data to push for %d-%02d", year, month)
        return 0

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found")

    engine = create_engine_from_config(db_config)
    Base.metadata.create_all(engine, tables=[RentRoll.__table__])

    session_manager = SessionManager(engine)
    chunk_size = _get_pipeline_config('rentroll', 'sql_chunk_size', 500)

    with session_manager.session_scope() as session:
        if status == "current":
            deleted = delete_current_month_records(session, RentRoll, year, month)
            logger.info("rentroll deleted %d prior current-month rows for %d-%02d",
                        deleted, year, month)
        elif status == "closed":
            cleaned = delete_non_eom_records(session, RentRoll, year, month)
            if cleaned:
                logger.info("rentroll cleaned %d stale non-EOM rows for %d-%02d",
                            cleaned, year, month)

        upsert_ops = UpsertOperations(session, db_config.db_type)
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            upsert_ops.upsert_batch(
                model=RentRoll, records=chunk,
                constraint_columns=['extract_date', 'SiteID', 'UnitID'],
                chunk_size=chunk_size,
            )

    logger.info("rentroll upserted %d records for %d-%02d", len(data), year, month)
    return len(data)


def run(mode: str = 'auto', start: str = None, end: str = None) -> Dict[str, Any]:
    """Fetch RentRoll for the given month range and upsert to esa_pbi.rentroll.

    Returns {'records': int, 'months': [...]}
    """
    from common import (
        DataLayerConfig, SOAPClient, SOAPReportClient,
        get_last_day_of_month, get_extract_date,
        get_date_range_manual, get_date_range_auto,
    )

    config = DataLayerConfig.from_env()
    if not config.soap:
        raise ValueError("SOAP configuration not found")

    location_codes = _get_pipeline_config('rentroll', 'location_codes', [])
    if not location_codes:
        raise ValueError("rentroll.location_codes not configured")

    if mode == 'manual':
        months = get_date_range_manual(start, end)
    else:
        months = get_date_range_auto()

    soap_client = SOAPClient(
        base_url=config.soap.base_url,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=config.soap.timeout,
        retries=config.soap.retries,
    )
    report_client = SOAPReportClient(soap_client)

    total = 0
    months_summary = []
    try:
        for year, month in months:
            first_day = datetime(year, month, 1)
            last_day_dt = datetime.combine(get_last_day_of_month(year, month), datetime.min.time())
            extract_date, status = get_extract_date(year, month)

            logger.info("rentroll %d-%02d extract_date=%s status=%s", year, month, extract_date, status)
            all_data = fetch_rentroll_data(
                report_client=report_client,
                location_codes=location_codes,
                start_date=first_day,
                end_date=last_day_dt,
                extract_date=extract_date,
            )
            written = push_to_database(all_data, config, year, month, status) if all_data else 0
            total += written
            months_summary.append({
                'year': year, 'month': month,
                'extract_date': str(extract_date), 'status': status,
                'records': written,
            })
    finally:
        soap_client.close()

    return {'records': total, 'months': months_summary, 'mode': mode}


class RentRollPipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode = scope.get('mode', 'auto')
        start = scope.get('start')
        end = scope.get('end')

        result = run(mode=mode, start=start, end=end)

        return RunResult(
            status='refreshed',
            records=result['records'],
            scope=scope,
            metadata={'mode': mode, 'months': result['months']},
        )
