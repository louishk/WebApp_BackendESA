"""
DiscountPipeline — fetch Discount/concession records from SMD SOAP and upsert to esa_pbi.

Reads via SOAP report 'discounts'. Writes to esa_pbi.discount keyed on
(extract_date, SiteID, ChargeID).

Modes:
  - auto (default): previous month + current month
  - manual:         arbitrary YYYY-MM start..end range

Scope keys honoured (all optional):
  - mode:  'auto' | 'manual'   (default 'auto')
  - start: 'YYYY-MM'           (manual only)
  - end:   'YYYY-MM'           (manual only)
"""

import logging
from datetime import datetime, date
from typing import Any, Dict, List

from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)


def transform_record(record: Dict[str, Any], extract_date: date) -> Dict[str, Any]:
    from common import convert_to_int, convert_to_decimal, convert_to_datetime
    return {
        'extract_date': extract_date,
        'SiteID': convert_to_int(record.get('SiteID')),
        'ChargeID': convert_to_int(record.get('ChargeID')),

        'sUnitName': record.get('sUnitName'),
        'sTypeName': record.get('sTypeName'),
        'sName': record.get('sName'),
        'sCompany': record.get('sCompany'),

        'sChgDesc': record.get('sChgDesc'),
        'dChgStrt': convert_to_datetime(record.get('dChgStrt')),
        'dcPrice': convert_to_decimal(record.get('dcPrice')),
        'dcAmt': convert_to_decimal(record.get('dcAmt')),
        'dcDiscount': convert_to_decimal(record.get('dcDiscount')),

        'sDiscMemo': record.get('sDiscMemo'),
        'sConcessionPlan': record.get('sConcessionPlan'),
        'sBy': record.get('sBy'),
        'sPlanTerm': record.get('sPlanTerm'),
        'dcPercentDiscount': convert_to_decimal(record.get('dcPercentDiscount')),

        'dMovedIn': convert_to_datetime(record.get('dMovedIn')),
        'dMovedOut': convert_to_datetime(record.get('dMovedOut')),
        'dPaidThru': convert_to_datetime(record.get('dPaidThru')),
        'dcInsurPremium': convert_to_decimal(record.get('dcInsurPremium')),

        'dcSchedRent': convert_to_decimal(record.get('dcSchedRent')),
        'dSchedRentStrt': convert_to_datetime(record.get('dSchedRentStrt')),
        'dRentLastChanged': convert_to_datetime(record.get('dRentLastChanged')),
        'dcStdRateAtMoveIn': convert_to_decimal(record.get('dcStdRateAtMoveIn')),
        'dcVariance': convert_to_decimal(record.get('dcVariance')),
    }


def fetch_discount_data(report_client, location_codes: List[str],
                        start_date: datetime, end_date: datetime,
                        extract_date: date) -> List[Dict[str, Any]]:
    from common import deduplicate_records
    all_data: List[Dict[str, Any]] = []

    for location_code in location_codes:
        try:
            results = report_client.call_report(
                report_name='discounts',
                parameters={
                    'sLocationCode': location_code,
                    'dReportDateStart': start_date.strftime('%Y-%m-%dT00:00:00'),
                    'dReportDateEnd': end_date.strftime('%Y-%m-%dT23:59:59'),
                },
            )
            for record in results:
                all_data.append(transform_record(record, extract_date))
            logger.info("discount fetched %s: %d records", location_code, len(results))
        except Exception as e:
            logger.exception("discount fetch failed for %s: %s", location_code, e)
            continue

    original_count = len(all_data)
    all_data = deduplicate_records(all_data, ['extract_date', 'SiteID', 'ChargeID'])
    if len(all_data) < original_count:
        logger.info("discount deduplicated: %d → %d", original_count, len(all_data))
    return all_data


def push_to_database(data: List[Dict[str, Any]], config, year: int, month: int, status: str) -> int:
    from common import (
        create_engine_from_config, SessionManager, UpsertOperations, Base,
        Discount, delete_current_month_records, delete_non_eom_records,
    )
    from common.config import get_pipeline_config

    if not data:
        logger.warning("discount: no data to push for %d-%02d", year, month)
        return 0

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found")

    engine = create_engine_from_config(db_config)
    Base.metadata.create_all(engine, tables=[Discount.__table__])

    session_manager = SessionManager(engine)
    chunk_size = get_pipeline_config('discount', 'sql_chunk_size', 500)

    with session_manager.session_scope() as session:
        if status == "current":
            deleted = delete_current_month_records(session, Discount, year, month)
            logger.info("discount deleted %d prior current-month rows for %d-%02d",
                        deleted, year, month)
        elif status == "closed":
            cleaned = delete_non_eom_records(session, Discount, year, month)
            if cleaned:
                logger.info("discount cleaned %d stale non-EOM rows for %d-%02d",
                            cleaned, year, month)

        upsert_ops = UpsertOperations(session, db_config.db_type)
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            upsert_ops.upsert_batch(
                model=Discount, records=chunk,
                constraint_columns=['extract_date', 'SiteID', 'ChargeID'],
                chunk_size=chunk_size,
            )

    logger.info("discount upserted %d records for %d-%02d", len(data), year, month)
    return len(data)


def run(mode: str = 'auto', start: str = None, end: str = None) -> Dict[str, Any]:
    from common import (
        DataLayerConfig, SOAPClient, SOAPReportClient,
        get_last_day_of_month, get_extract_date,
        get_date_range_manual, get_date_range_auto,
    )
    from common.config import get_pipeline_config

    config = DataLayerConfig.from_env()
    if not config.soap:
        raise ValueError("SOAP configuration not found")

    location_codes = get_pipeline_config('discount', 'location_codes', [])
    if not location_codes:
        raise ValueError("discount.location_codes not configured")

    months = get_date_range_manual(start, end) if mode == 'manual' else get_date_range_auto()

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

            logger.info("discount %d-%02d extract_date=%s status=%s",
                        year, month, extract_date, status)
            all_data = fetch_discount_data(
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


class DiscountPipeline(BasePipeline):

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
