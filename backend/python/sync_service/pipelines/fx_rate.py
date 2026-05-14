"""
FxRatePipeline — fetch foreign exchange rates from Yahoo Finance and upsert
to esa_pbi.fx_rates + esa_pbi.fx_rates_monthly.

Modes:
  - auto (default):      incremental daily update from latest DB date
  - backfill:            historical load (default: 2010-01-01 to today)
  - refresh-monthly:     recalculate monthly averages from existing daily data

Scope keys honoured (all optional):
  - mode:  'auto' | 'backfill' | 'refresh-monthly'   (default 'auto')
  - start: 'YYYY-MM-DD'   (backfill only)
  - end:   'YYYY-MM-DD'   (backfill only)
"""

import logging
from collections import defaultdict
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

try:
    import yfinance as yf
except ImportError:
    raise ImportError("yfinance is required. Install with: pip install yfinance")

from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================

BASE_CURRENCY = 'SGD'
DEFAULT_TARGET_CURRENCIES = ['USD', 'HKD', 'KRW', 'JPY', 'AUD', 'NZD', 'TWD', 'THB', 'MYR', 'CNY']
DEFAULT_HISTORICAL_START = date(2010, 1, 1)

CURRENCY_PAIRS = {
    'USD': {'ticker': 'SGDUSD=X', 'fallback': 'USDSGD=X', 'invert_fallback': True},
    'HKD': {'ticker': 'SGDHKD=X', 'fallback': 'HKDSGD=X', 'invert_fallback': True},
    'KRW': {'ticker': 'SGDKRW=X', 'fallback': 'KRWSGD=X', 'invert_fallback': True},
    'JPY': {'ticker': 'SGDJPY=X', 'fallback': 'JPYSGD=X', 'invert_fallback': True},
    'AUD': {'ticker': 'SGDAUD=X', 'fallback': 'AUDSGD=X', 'invert_fallback': True},
    'NZD': {'ticker': 'SGDNZD=X', 'fallback': 'NZDSGD=X', 'invert_fallback': True},
    'TWD': {'ticker': 'SGDTWD=X', 'fallback': 'TWDSGD=X', 'invert_fallback': True},
    'THB': {'ticker': 'SGDTHB=X', 'fallback': 'THBSGD=X', 'invert_fallback': True},
    'MYR': {'ticker': 'SGDMYR=X', 'fallback': 'MYRSGD=X', 'invert_fallback': True},
    'CNY': {'ticker': 'SGDCNY=X', 'fallback': 'CNYSGD=X', 'invert_fallback': True},
}


# =============================================================================
# FX Data Fetching
# =============================================================================

def fetch_fx_data_yfinance(
    currency: str,
    start_date: date,
    end_date: date,
) -> List[Dict[str, Any]]:
    """Fetch FX data for a single currency pair from Yahoo Finance."""
    pair_config = CURRENCY_PAIRS.get(currency)
    if not pair_config:
        raise ValueError(f"Unknown currency: {currency}")

    ticker = pair_config['ticker']
    invert = False

    data = yf.download(
        ticker,
        start=start_date.strftime('%Y-%m-%d'),
        end=(end_date + timedelta(days=1)).strftime('%Y-%m-%d'),
        progress=False,
        auto_adjust=True,
    )

    if data.empty and pair_config.get('fallback'):
        ticker = pair_config['fallback']
        invert = pair_config.get('invert_fallback', False)

        data = yf.download(
            ticker,
            start=start_date.strftime('%Y-%m-%d'),
            end=(end_date + timedelta(days=1)).strftime('%Y-%m-%d'),
            progress=False,
            auto_adjust=True,
        )

    if data.empty:
        return []

    results = []
    for idx, row in data.iterrows():
        if isinstance(row.index, tuple) or (hasattr(data.columns, 'nlevels') and data.columns.nlevels > 1):
            close_val = row[('Close', ticker)] if ('Close', ticker) in row.index else row['Close']
        else:
            close_val = row['Close']

        if close_val is None or (hasattr(close_val, 'isna') and close_val.isna()):
            continue

        rate = float(close_val)
        if invert and rate != 0:
            rate = 1 / rate

        results.append({
            'rate_date': idx.date() if hasattr(idx, 'date') else idx,
            'rate': Decimal(str(round(rate, 10))),
            'is_trading_day': True,
        })

    return results


def forward_fill_rates(
    rate_data: List[Dict[str, Any]],
    start_date: date,
    end_date: date,
    currency: str,
) -> List[Dict[str, Any]]:
    """Forward-fill rates for non-trading days (weekends, holidays)."""
    if not rate_data:
        return []

    rate_lookup = {d['rate_date']: d['rate'] for d in rate_data}

    filled_data = []
    current_date = start_date
    last_rate = None

    sorted_dates = sorted(rate_lookup.keys())
    if sorted_dates and start_date < sorted_dates[0]:
        current_date = sorted_dates[0]

    while current_date <= end_date:
        if current_date in rate_lookup:
            last_rate = rate_lookup[current_date]
            is_trading = True
        else:
            is_trading = False

        if last_rate is not None:
            filled_data.append({
                'rate_date': current_date,
                'target_currency': currency,
                'year': current_date.year,
                'month': current_date.month,
                'year_month': current_date.strftime('%Y-%m'),
                'base_currency': BASE_CURRENCY,
                'rate': last_rate,
                'is_trading_day': is_trading,
                'data_source': 'yfinance',
            })

        current_date += timedelta(days=1)

    return filled_data


# =============================================================================
# Monthly Average Calculation
# =============================================================================

def calculate_monthly_averages(
    daily_rates: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Calculate monthly average FX rates from daily data."""
    monthly_data: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)

    for rate in daily_rates:
        key = (rate['year_month'], rate['target_currency'])
        monthly_data[key].append(rate)

    monthly_averages = []

    for (year_month, currency), rates in monthly_data.items():
        if not rates:
            continue

        rate_values = [float(r['rate']) for r in rates]
        trading_rates = [r for r in rates if r['is_trading_day']]
        sorted_rates = sorted(rates, key=lambda x: x['rate_date'])

        monthly_averages.append({
            'year_month': year_month,
            'target_currency': currency,
            'year': int(year_month.split('-')[0]),
            'month': int(year_month.split('-')[1]),
            'base_currency': BASE_CURRENCY,
            'avg_rate': Decimal(str(round(sum(rate_values) / len(rate_values), 10))),
            'min_rate': Decimal(str(round(min(rate_values), 10))),
            'max_rate': Decimal(str(round(max(rate_values), 10))),
            'first_rate': sorted_rates[0]['rate'],
            'last_rate': sorted_rates[-1]['rate'],
            'trading_days': len(trading_rates),
            'total_days': len(rates),
        })

    return monthly_averages


# =============================================================================
# Database Operations
# =============================================================================

def push_daily_rates_to_database(
    data: List[Dict[str, Any]],
    config,
    chunk_size: int = 1000,
) -> None:
    """Push daily FX rate data to PostgreSQL database."""
    if not data:
        logger.warning("fxrate: no daily rate data to push")
        return

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found")

    from common.db import get_engine
    from common import Base, FXRate, SessionManager, UpsertOperations

    engine = get_engine('pbi')
    Base.metadata.create_all(engine, tables=[FXRate.__table__])
    logger.info("fxrate: table 'fx_rates' ready")

    session_manager = SessionManager(engine)
    num_chunks = (len(data) + chunk_size - 1) // chunk_size

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)

        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            upsert_ops.upsert_batch(
                model=FXRate,
                records=chunk,
                constraint_columns=['rate_date', 'target_currency'],
                chunk_size=chunk_size,
            )
            logger.info("fxrate: upserted chunk %d/%d (%d recs)",
                        i // chunk_size + 1, num_chunks, len(chunk))

    logger.info("fxrate: upserted %d daily rate records", len(data))


def push_monthly_rates_to_database(
    data: List[Dict[str, Any]],
    config,
    chunk_size: int = 500,
) -> None:
    """Push monthly average FX rate data to PostgreSQL database."""
    if not data:
        logger.warning("fxrate: no monthly rate data to push")
        return

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found")

    from common.db import get_engine
    from common import Base, FXRateMonthly, SessionManager, UpsertOperations

    engine = get_engine('pbi')
    Base.metadata.create_all(engine, tables=[FXRateMonthly.__table__])
    logger.info("fxrate: table 'fx_rates_monthly' ready")

    session_manager = SessionManager(engine)

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)
        upsert_ops.upsert_batch(
            model=FXRateMonthly,
            records=data,
            constraint_columns=['year_month', 'target_currency'],
            chunk_size=chunk_size,
        )

    logger.info("fxrate: upserted %d monthly average records", len(data))


def get_latest_date_from_db(config) -> Optional[date]:
    """Get the latest rate_date from the database."""
    from sqlalchemy import func
    from common.db import get_engine
    from common import FXRate, SessionManager

    engine = get_engine('pbi')
    session_manager = SessionManager(engine)

    try:
        with session_manager.session_scope() as session:
            result = session.query(func.max(FXRate.rate_date)).scalar()
            return result
    except Exception:
        return None


# =============================================================================
# Pipeline Mode Functions
# =============================================================================

def run_backfill(
    start_date: date,
    end_date: date,
    target_currencies: List[str],
    config,
    chunk_size: int,
) -> Tuple[int, int]:
    """Fetch historical FX data and push daily + monthly to the DB."""
    all_daily_rates = []

    logger.info("fxrate backfill: fetching %d currencies from %s to %s",
                len(target_currencies), start_date, end_date)

    for currency in target_currencies:
        logger.info("fxrate: processing %s/%s", BASE_CURRENCY, currency)

        raw_data = fetch_fx_data_yfinance(currency, start_date, end_date)

        if not raw_data:
            logger.warning("fxrate: no data found for %s", currency)
            continue

        logger.info("fxrate: fetched %d trading days for %s", len(raw_data), currency)

        filled_data = forward_fill_rates(raw_data, start_date, end_date, currency)
        logger.info("fxrate: forward-filled to %d days for %s", len(filled_data), currency)

        all_daily_rates.extend(filled_data)

    if not all_daily_rates:
        logger.warning("fxrate: no data fetched from Yahoo Finance")
        return 0, 0

    logger.info("fxrate: pushing %d daily rate records to database", len(all_daily_rates))
    push_daily_rates_to_database(all_daily_rates, config, chunk_size)

    monthly_averages = calculate_monthly_averages(all_daily_rates)
    logger.info("fxrate: calculated %d monthly averages", len(monthly_averages))
    push_monthly_rates_to_database(monthly_averages, config)

    return len(all_daily_rates), len(monthly_averages)


def run_auto(
    days_back: int,
    target_currencies: List[str],
    config,
    chunk_size: int,
) -> Tuple[int, int]:
    """Incremental update — fetch from latest DB date or last N days."""
    end_date = date.today()

    latest_db_date = get_latest_date_from_db(config)
    if latest_db_date:
        calculated_start = latest_db_date + timedelta(days=1)
        fallback_start = end_date - timedelta(days=days_back)
        start_date = min(calculated_start, fallback_start)
        logger.info("fxrate auto: latest DB date=%s, fetching from %s", latest_db_date, start_date)
    else:
        start_date = end_date - timedelta(days=days_back)
        logger.info("fxrate auto: no existing data, fetching last %d days", days_back)

    return run_backfill(start_date, end_date, target_currencies, config, chunk_size)


def run_refresh_monthly(config) -> int:
    """Recalculate all monthly averages from existing daily data."""
    from common.db import get_engine
    from common import FXRate, SessionManager

    engine = get_engine('pbi')
    session_manager = SessionManager(engine)

    logger.info("fxrate refresh-monthly: loading all daily rates from database")

    with session_manager.session_scope() as session:
        results = session.query(FXRate).all()

        daily_rates = [
            {
                'rate_date': r.rate_date,
                'target_currency': r.target_currency,
                'year': r.year,
                'month': r.month,
                'year_month': r.year_month,
                'base_currency': r.base_currency,
                'rate': r.rate,
                'is_trading_day': r.is_trading_day,
                'data_source': r.data_source,
            }
            for r in results
        ]

    logger.info("fxrate refresh-monthly: loaded %d daily rate records", len(daily_rates))

    if not daily_rates:
        logger.warning("fxrate refresh-monthly: no daily rates found in database")
        return 0

    monthly_averages = calculate_monthly_averages(daily_rates)
    logger.info("fxrate refresh-monthly: calculated %d monthly averages", len(monthly_averages))

    push_monthly_rates_to_database(monthly_averages, config)

    return len(monthly_averages)


# =============================================================================
# Public API
# =============================================================================

def run(mode: str = 'auto', start: str = None, end: str = None) -> Dict[str, Any]:
    """Fetch FX rates and upsert to esa_pbi.fx_rates + fx_rates_monthly.

    Returns {'records': int, 'daily': int, 'monthly': int, 'mode': str}
    """
    from common import DataLayerConfig
    from common.config import get_pipeline_config

    config = DataLayerConfig.from_env()

    target_currencies = get_pipeline_config('fxrate', 'target_currencies', DEFAULT_TARGET_CURRENCIES)
    historical_start_str = get_pipeline_config('fxrate', 'historical_start', '2010-01-01')
    chunk_size = get_pipeline_config('fxrate', 'sql_chunk_size', 1000)
    incremental_days = get_pipeline_config('fxrate', 'incremental_days', 7)

    historical_start = datetime.strptime(historical_start_str, '%Y-%m-%d').date()

    logger.info("fxrate run: mode=%s currencies=%s", mode, target_currencies)

    if mode == 'backfill':
        start_date = datetime.strptime(start, '%Y-%m-%d').date() if start else historical_start
        end_date = datetime.strptime(end, '%Y-%m-%d').date() if end else date.today()
        logger.info("fxrate backfill range: %s to %s", start_date, end_date)
        daily_count, monthly_count = run_backfill(
            start_date=start_date,
            end_date=end_date,
            target_currencies=target_currencies,
            config=config,
            chunk_size=chunk_size,
        )

    elif mode == 'auto':
        daily_count, monthly_count = run_auto(
            days_back=incremental_days,
            target_currencies=target_currencies,
            config=config,
            chunk_size=chunk_size,
        )

    elif mode == 'refresh-monthly':
        monthly_count = run_refresh_monthly(config)
        daily_count = 0

    else:
        raise ValueError(f"Unknown mode: {mode}")

    total = daily_count + monthly_count
    logger.info("fxrate complete: daily=%d monthly=%d total=%d", daily_count, monthly_count, total)
    return {'records': total, 'daily': daily_count, 'monthly': monthly_count, 'mode': mode}


class FxRatePipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode = scope.get('mode', 'auto')
        start = scope.get('start')
        end = scope.get('end')

        result = run(mode=mode, start=start, end=end)

        return RunResult(
            status='refreshed',
            records=result['records'],
            scope=scope,
            metadata={
                'mode': mode,
                'daily': result['daily'],
                'monthly': result['monthly'],
            },
        )
