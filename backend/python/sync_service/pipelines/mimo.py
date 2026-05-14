"""
MimoPipeline — fetch Move-In/Move-Out records from SMD SOAP and upsert to esa_pbi.mimo.

Cumulative table (no extract_date in PK). Auto mode: delete rows where
MoveDate >= today - days_back, then repush a [today - days_back, today + days_forward]
window. Manual mode: explicit start/end (YYYY-MM-DD).

PK: (SiteID, TenantID, MoveDate)

Scope keys honoured (all optional):
  - mode:  'auto' | 'manual'      (default 'auto')
  - start: 'YYYY-MM-DD'           (manual only)
  - end:   'YYYY-MM-DD'           (manual only)
"""

import logging
from datetime import datetime, date
from typing import Any, Dict, List

from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)


def transform_record(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    from common import convert_to_bool, convert_to_int, convert_to_decimal, convert_to_datetime
    return {
        'SiteID': convert_to_int(record.get('SiteID')),
        'TenantID': convert_to_int(record.get('TenantID')),
        'MoveDate': convert_to_datetime(record.get('MoveDate')),

        'extract_date': extract_date,

        'MoveIn': convert_to_int(record.get('MoveIn')),
        'MoveOut': convert_to_int(record.get('MoveOut')),
        'Transfer': convert_to_int(record.get('Transfer')),

        'UnitName': record.get('UnitName'),
        'UnitSize': record.get('UnitSize'),
        'Width': convert_to_decimal(record.get('Width')),
        'Length': convert_to_decimal(record.get('Length')),
        'sUnitType': record.get('sUnitType'),

        'TenantName': record.get('TenantName'),
        'sCompany': record.get('sCompany'),
        'sEmail': record.get('sEmail'),
        'Address': record.get('Address'),
        'City': record.get('City'),
        'Region': record.get('Region'),
        'PostalCode': record.get('PostalCode'),
        'Country': record.get('Country'),

        'StandardRate': convert_to_decimal(record.get('StandardRate')),
        'MovedInArea': convert_to_decimal(record.get('MovedInArea')),
        'MovedInRentalRate': convert_to_decimal(record.get('MovedInRentalRate')),
        'MovedInVariance': convert_to_decimal(record.get('MovedInVariance')),
        'MovedInDaysVacant': convert_to_int(record.get('MovedInDaysVacant')),
        'MovedOutArea': convert_to_decimal(record.get('MovedOutArea')),
        'MovedOutRentalRate': convert_to_decimal(record.get('MovedOutRentalRate')),
        'MovedOutVariance': convert_to_decimal(record.get('MovedOutVariance')),
        'MovedOutDaysRented': convert_to_int(record.get('MovedOutDaysRented')),

        'iLeaseNum': convert_to_int(record.get('iLeaseNum')),
        'dRentLastChanged': convert_to_datetime(record.get('dRentLastChanged')),
        'sLicPlate': record.get('sLicPlate'),
        'sEmpInitials': record.get('sEmpInitials'),
        'sPlanTerm': record.get('sPlanTerm'),
        'dcInsurPremium': convert_to_decimal(record.get('dcInsurPremium')),
        'dcDiscount': convert_to_decimal(record.get('dcDiscount')),
        'sDiscountPlan': record.get('sDiscountPlan'),
        'iAuctioned': convert_to_int(record.get('iAuctioned')),
        'sAuctioned': record.get('sAuctioned'),
        'iDaysSinceMoveOut': convert_to_int(record.get('iDaysSinceMoveOut')),
        'dcAmtPaid': convert_to_decimal(record.get('dcAmtPaid')),
        'sSource': record.get('sSource'),

        'bPower': convert_to_bool(record.get('bPower')),
        'bClimate': convert_to_bool(record.get('bClimate')),
        'bAlarm': convert_to_bool(record.get('bAlarm')),
        'bInside': convert_to_bool(record.get('bInside')),

        'dcPushRateAtMoveIn': convert_to_decimal(record.get('dcPushRateAtMoveIn')),
        'dcStdRateAtMoveIn': convert_to_decimal(record.get('dcStdRateAtMoveIn')),
        'dcInsurPremiumAtMoveIn': convert_to_decimal(record.get('dcInsurPremiumAtMoveIn')),
        'sDiscountPlanAtMoveIn': record.get('sDiscountPlanAtMoveIn'),

        'WaitingID': convert_to_int(record.get('WaitingID')),
        'InquiryEmployeeID': convert_to_int(record.get('InquiryEmployeeID')),
        'sInquiryPlacedBy': record.get('sInquiryPlacedBy'),
        'CorpUserID_Placed': convert_to_int(record.get('CorpUserID_Placed')),
        'CorpUserID_ConvertedToMoveIn': convert_to_int(record.get('CorpUserID_ConvertedToMoveIn')),
    }


def fetch_mimo_data(report_client, location_codes: List[str],
                    start_date: date, end_date: date,
                    extract_date: date) -> List[Dict[str, Any]]:
    from common import deduplicate_records
    all_data: List[Dict[str, Any]] = []

    for location_code in location_codes:
        try:
            results = report_client.call_report(
                report_name='move_ins_and_move_outs',
                parameters={
                    'sLocationCode': location_code,
                    'dReportDateStart': start_date.strftime('%Y-%m-%dT00:00:00'),
                    'dReportDateEnd': end_date.strftime('%Y-%m-%dT23:59:59'),
                },
            )
            for record in results:
                all_data.append(transform_record(record, extract_date))
            logger.info("mimo fetched %s: %d records", location_code, len(results))
        except Exception as e:
            logger.exception("mimo fetch failed for %s: %s", location_code, e)
            continue

    original_count = len(all_data)
    all_data = deduplicate_records(all_data, ['SiteID', 'TenantID', 'MoveDate'])
    if len(all_data) < original_count:
        logger.info("mimo deduplicated: %d → %d", original_count, len(all_data))
    return all_data


def delete_recent_records(config, delete_from_date: date) -> int:
    from common import create_engine_from_config, SessionManager, MoveInsAndMoveOuts

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found")

    engine = create_engine_from_config(db_config)
    session_manager = SessionManager(engine)

    with session_manager.session_scope() as session:
        delete_from_datetime = datetime.combine(delete_from_date, datetime.min.time())
        deleted = session.query(MoveInsAndMoveOuts).filter(
            MoveInsAndMoveOuts.MoveDate >= delete_from_datetime
        ).delete()
    return deleted


def push_to_database(data: List[Dict[str, Any]], config) -> int:
    from common import (
        create_engine_from_config, SessionManager, UpsertOperations, Base,
        MoveInsAndMoveOuts,
    )
    from common.config import get_pipeline_config

    if not data:
        logger.warning("mimo: no data to push")
        return 0

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found")

    engine = create_engine_from_config(db_config)
    Base.metadata.create_all(engine, tables=[MoveInsAndMoveOuts.__table__])

    session_manager = SessionManager(engine)
    chunk_size = get_pipeline_config('mimo', 'sql_chunk_size', 500)

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            upsert_ops.upsert_batch(
                model=MoveInsAndMoveOuts, records=chunk,
                constraint_columns=['SiteID', 'TenantID', 'MoveDate'],
                chunk_size=chunk_size,
            )

    logger.info("mimo upserted %d records", len(data))
    return len(data)


def run(mode: str = 'auto', start: str = None, end: str = None) -> Dict[str, Any]:
    from common import (
        DataLayerConfig, SOAPClient, SOAPReportClient,
        get_date_range_days_back, parse_date_string,
    )
    from common.config import get_pipeline_config

    config = DataLayerConfig.from_env()
    if not config.soap:
        raise ValueError("SOAP configuration not found")

    location_codes = get_pipeline_config('mimo', 'location_codes', [])
    if not location_codes:
        raise ValueError("mimo.location_codes not configured")

    days_back = get_pipeline_config('mimo', 'days_back', 60)
    days_forward = get_pipeline_config('mimo', 'days_forward', 365)

    if mode == 'manual':
        if not start or not end:
            raise ValueError("manual mode requires start and end (YYYY-MM-DD)")
        start_date = parse_date_string(start)
        end_date = parse_date_string(end)
        delete_before_push = False
    else:
        start_date, end_date = get_date_range_days_back(days_back, days_forward)
        delete_before_push = True

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

    deleted_count = 0
    try:
        if delete_before_push:
            try:
                deleted_count = delete_recent_records(config, start_date)
                logger.info("mimo deleted %d existing rows from %s onwards",
                            deleted_count, start_date)
            except Exception:
                logger.exception("mimo: delete-before-push failed (table may not exist)")

        extract_date = date.today()
        all_data = fetch_mimo_data(
            report_client=report_client,
            location_codes=location_codes,
            start_date=start_date,
            end_date=end_date,
            extract_date=extract_date,
        )
        written = push_to_database(all_data, config) if all_data else 0
    finally:
        soap_client.close()

    return {
        'records': written,
        'mode': mode,
        'start_date': str(start_date),
        'end_date': str(end_date),
        'deleted_before_push': deleted_count,
    }


class MimoPipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode = scope.get('mode', 'auto')
        start = scope.get('start')
        end = scope.get('end')

        result = run(mode=mode, start=start, end=end)

        return RunResult(
            status='refreshed',
            records=result['records'],
            scope=scope,
            metadata=result,
        )
