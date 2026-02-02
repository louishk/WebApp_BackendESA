"""
FX Rate to SQL Pipeline

Fetches FX rate data from Yahoo Finance and pushes to PostgreSQL database.

Features:
- Three modes: backfill (historical from 2010), auto (incremental daily), refresh-monthly
- SGD as base currency with 10 APAC target currencies
- Forward-fill for weekends/holidays
- Monthly average calculation
- Incremental updates (only fetch new dates)

Usage:
    # Backfill mode - load all historical data from 2010-01-01
    python fxrate_to_sql.py --mode backfill

    # Backfill with specific date range
    python fxrate_to_sql.py --mode backfill --start 2024-01-01 --end 2024-12-31

    # Auto mode - fetch last N days for incremental updates (for scheduler)
    python fxrate_to_sql.py --mode auto

    # Refresh monthly averages only
    python fxrate_to_sql.py --mode refresh-monthly

Configuration (in .env):
    - POSTGRESQL_* : Database connection settings
    - FX_TARGET_CURRENCIES: Comma-separated currency codes (default: USD,HKD,KRW,JPY,AUD,NZD,TWD,THB,MYR,CNY)
    - FX_HISTORICAL_START: Historical start date (default: 2010-01-01)
    - FX_SQL_CHUNK_SIZE: Batch size for upsert (default: 1000)
    - FX_INCREMENTAL_DAYS: Days to look back for auto mode (default: 7)
"""

import argparse
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict

from decouple import config as env_config, Csv
from tqdm import tqdm

try:
    import yfinance as yf
except ImportError:
    raise ImportError("yfinance is required. Install with: pip install yfinance")

from common import (
    DataLayerConfig,
    create_engine_from_config,
    SessionManager,
    UpsertOperations,
    Base,
    FXRate,
    FXRateMonthly,
    convert_to_decimal,
)


# =============================================================================
# Configuration
# =============================================================================

BASE_CURRENCY = 'SGD'
DEFAULT_TARGET_CURRENCIES = ['USD', 'HKD', 'KRW', 'JPY', 'AUD', 'NZD', 'TWD', 'THB', 'MYR', 'CNY']
DEFAULT_HISTORICAL_START = date(2010, 1, 1)

# yfinance currency pair mapping
# Format: SGDXXX=X means 1 SGD = X target currency
# Some pairs may need to be fetched as XXXSGD=X and inverted
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
    end_date: date
) -> List[Dict[str, Any]]:
    """
    Fetch FX data for a single currency pair from Yahoo Finance.

    Args:
        currency: Target currency code (e.g., 'USD')
        start_date: Start date for data fetch
        end_date: End date for data fetch

    Returns:
        List of dictionaries with rate_date, rate, is_trading_day
    """
    pair_config = CURRENCY_PAIRS.get(currency)
    if not pair_config:
        raise ValueError(f"Unknown currency: {currency}")

    # Try primary ticker first
    ticker = pair_config['ticker']
    invert = False

    # Fetch data
    data = yf.download(
        ticker,
        start=start_date.strftime('%Y-%m-%d'),
        end=(end_date + timedelta(days=1)).strftime('%Y-%m-%d'),
        progress=False,
        auto_adjust=True
    )

    # If no data, try fallback ticker
    if data.empty and pair_config.get('fallback'):
        ticker = pair_config['fallback']
        invert = pair_config.get('invert_fallback', False)

        data = yf.download(
            ticker,
            start=start_date.strftime('%Y-%m-%d'),
            end=(end_date + timedelta(days=1)).strftime('%Y-%m-%d'),
            progress=False,
            auto_adjust=True
        )

    if data.empty:
        return []

    results = []
    for idx, row in data.iterrows():
        # Handle both single-level and multi-level column index
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
            'is_trading_day': True
        })

    return results


def forward_fill_rates(
    rate_data: List[Dict[str, Any]],
    start_date: date,
    end_date: date,
    currency: str
) -> List[Dict[str, Any]]:
    """
    Forward-fill rates for non-trading days (weekends, holidays).

    Creates a continuous daily series from start_date to end_date,
    filling gaps with the previous available rate.

    Args:
        rate_data: List of trading day rates
        start_date: Start date for the series
        end_date: End date for the series
        currency: Target currency code

    Returns:
        List of daily rate records with forward-filled values
    """
    if not rate_data:
        return []

    # Create date-indexed lookup
    rate_lookup = {d['rate_date']: d['rate'] for d in rate_data}

    filled_data = []
    current_date = start_date
    last_rate = None

    # Find the first available rate
    sorted_dates = sorted(rate_lookup.keys())
    if sorted_dates:
        # If start_date is before first available rate, start from first rate
        if start_date < sorted_dates[0]:
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
                'data_source': 'yfinance'
            })

        current_date += timedelta(days=1)

    return filled_data


# =============================================================================
# Monthly Average Calculation
# =============================================================================

def calculate_monthly_averages(
    daily_rates: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """
    Calculate monthly average FX rates from daily data.

    Args:
        daily_rates: List of daily rate records

    Returns:
        List of monthly average records
    """
    # Group by (year_month, target_currency)
    monthly_data = defaultdict(list)

    for rate in daily_rates:
        key = (rate['year_month'], rate['target_currency'])
        monthly_data[key].append(rate)

    monthly_averages = []

    for (year_month, currency), rates in monthly_data.items():
        if not rates:
            continue

        rate_values = [float(r['rate']) for r in rates]
        trading_rates = [r for r in rates if r['is_trading_day']]

        # Sort by date to get first and last
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
            'total_days': len(rates)
        })

    return monthly_averages


# =============================================================================
# Database Operations
# =============================================================================

def push_daily_rates_to_database(
    data: List[Dict[str, Any]],
    config: DataLayerConfig,
    chunk_size: int = 1000
) -> None:
    """Push daily FX rate data to PostgreSQL database."""
    if not data:
        print("  No daily rate data to push")
        return

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found in .env")

    engine = create_engine_from_config(db_config)

    # Create table if not exists
    Base.metadata.create_all(engine, tables=[FXRate.__table__])
    tqdm.write("  Table 'fx_rates' ready")

    session_manager = SessionManager(engine)
    num_chunks = (len(data) + chunk_size - 1) // chunk_size

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)

        with tqdm(total=len(data), desc="  Upserting daily rates", unit="rec") as pbar:
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]

                upsert_ops.upsert_batch(
                    model=FXRate,
                    records=chunk,
                    constraint_columns=['rate_date', 'target_currency'],
                    chunk_size=chunk_size
                )

                pbar.update(len(chunk))
                pbar.set_postfix({"chunk": f"{i//chunk_size + 1}/{num_chunks}"})

    tqdm.write(f"  Upserted {len(data)} daily rate records")


def push_monthly_rates_to_database(
    data: List[Dict[str, Any]],
    config: DataLayerConfig,
    chunk_size: int = 500
) -> None:
    """Push monthly average FX rate data to PostgreSQL database."""
    if not data:
        print("  No monthly rate data to push")
        return

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found in .env")

    engine = create_engine_from_config(db_config)

    # Create table if not exists
    Base.metadata.create_all(engine, tables=[FXRateMonthly.__table__])
    tqdm.write("  Table 'fx_rates_monthly' ready")

    session_manager = SessionManager(engine)

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)

        upsert_ops.upsert_batch(
            model=FXRateMonthly,
            records=data,
            constraint_columns=['year_month', 'target_currency'],
            chunk_size=chunk_size
        )

    tqdm.write(f"  Upserted {len(data)} monthly average records")


def get_latest_date_from_db(config: DataLayerConfig) -> Optional[date]:
    """Get the latest rate_date from the database."""
    from sqlalchemy import func

    db_config = config.databases.get('postgresql')
    if not db_config:
        return None

    engine = create_engine_from_config(db_config)
    session_manager = SessionManager(engine)

    try:
        with session_manager.session_scope() as session:
            result = session.query(func.max(FXRate.rate_date)).scalar()
            return result
    except Exception:
        return None


# =============================================================================
# Main Pipeline Functions
# =============================================================================

def run_backfill(
    start_date: date,
    end_date: date,
    target_currencies: List[str],
    config: DataLayerConfig,
    chunk_size: int
) -> Tuple[int, int]:
    """
    Run backfill mode - fetch historical data.

    Returns:
        Tuple of (daily_records_count, monthly_records_count)
    """
    all_daily_rates = []

    print(f"\nFetching FX data for {len(target_currencies)} currencies...")

    for currency in tqdm(target_currencies, desc="Currencies"):
        tqdm.write(f"\n  Processing {BASE_CURRENCY}/{currency}...")

        # Fetch from Yahoo Finance
        raw_data = fetch_fx_data_yfinance(currency, start_date, end_date)

        if not raw_data:
            tqdm.write(f"  No data found for {currency}")
            continue

        tqdm.write(f"  Fetched {len(raw_data)} trading days")

        # Forward-fill for continuous daily series
        filled_data = forward_fill_rates(raw_data, start_date, end_date, currency)
        tqdm.write(f"  Forward-filled to {len(filled_data)} days")

        all_daily_rates.extend(filled_data)

    if not all_daily_rates:
        print("\nNo data fetched from Yahoo Finance")
        return 0, 0

    # Push daily rates to database
    print(f"\nPushing {len(all_daily_rates)} daily rate records to database...")
    push_daily_rates_to_database(all_daily_rates, config, chunk_size)

    # Calculate and push monthly averages
    print("\nCalculating monthly averages...")
    monthly_averages = calculate_monthly_averages(all_daily_rates)
    print(f"Calculated {len(monthly_averages)} monthly averages")

    push_monthly_rates_to_database(monthly_averages, config)

    return len(all_daily_rates), len(monthly_averages)


def run_auto(
    days_back: int,
    target_currencies: List[str],
    config: DataLayerConfig,
    chunk_size: int
) -> Tuple[int, int]:
    """
    Run auto mode - incremental update for recent days.

    Returns:
        Tuple of (daily_records_count, monthly_records_count)
    """
    end_date = date.today()

    # Check latest date in database
    latest_db_date = get_latest_date_from_db(config)
    if latest_db_date:
        # Start from the day after latest, but at least days_back
        calculated_start = latest_db_date + timedelta(days=1)
        fallback_start = end_date - timedelta(days=days_back)
        start_date = min(calculated_start, fallback_start)
        print(f"Latest DB date: {latest_db_date}, fetching from {start_date}")
    else:
        start_date = end_date - timedelta(days=days_back)
        print(f"No existing data, fetching last {days_back} days")

    return run_backfill(start_date, end_date, target_currencies, config, chunk_size)


def run_refresh_monthly(
    config: DataLayerConfig
) -> int:
    """
    Recalculate all monthly averages from existing daily data.

    Returns:
        Number of monthly records updated
    """
    from sqlalchemy import select

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found in .env")

    engine = create_engine_from_config(db_config)
    session_manager = SessionManager(engine)

    print("Fetching all daily rates from database...")

    with session_manager.session_scope() as session:
        # Fetch all daily rates
        results = session.query(FXRate).all()

        daily_rates = []
        for r in results:
            daily_rates.append({
                'rate_date': r.rate_date,
                'target_currency': r.target_currency,
                'year': r.year,
                'month': r.month,
                'year_month': r.year_month,
                'base_currency': r.base_currency,
                'rate': r.rate,
                'is_trading_day': r.is_trading_day,
                'data_source': r.data_source
            })

    print(f"Loaded {len(daily_rates)} daily rate records")

    if not daily_rates:
        print("No daily rates found in database")
        return 0

    # Calculate monthly averages
    print("Calculating monthly averages...")
    monthly_averages = calculate_monthly_averages(daily_rates)
    print(f"Calculated {len(monthly_averages)} monthly averages")

    # Push to database
    push_monthly_rates_to_database(monthly_averages, config)

    return len(monthly_averages)


# =============================================================================
# CLI and Main
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='FX Rate to SQL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill mode - load all historical data from 2010
  python fxrate_to_sql.py --mode backfill

  # Backfill specific date range
  python fxrate_to_sql.py --mode backfill --start 2024-01-01 --end 2024-12-31

  # Auto mode - incremental daily update
  python fxrate_to_sql.py --mode auto

  # Refresh monthly averages only
  python fxrate_to_sql.py --mode refresh-monthly
        """
    )

    parser.add_argument(
        '--mode',
        choices=['backfill', 'auto', 'refresh-monthly'],
        required=True,
        help='Extraction mode: backfill (historical), auto (incremental), refresh-monthly'
    )

    parser.add_argument(
        '--start',
        type=str,
        help='Start date in YYYY-MM-DD format (optional for backfill mode)'
    )

    parser.add_argument(
        '--end',
        type=str,
        help='End date in YYYY-MM-DD format (optional for backfill mode)'
    )

    return parser.parse_args()


def main():
    """Main function to fetch and push FX rate data to SQL."""

    args = parse_args()

    # Load configuration
    config = DataLayerConfig.from_env()

    # Load FX-specific config from .env
    target_currencies = env_config(
        'FX_TARGET_CURRENCIES',
        default=','.join(DEFAULT_TARGET_CURRENCIES),
        cast=Csv()
    )
    historical_start_str = env_config('FX_HISTORICAL_START', default='2010-01-01')
    chunk_size = env_config('FX_SQL_CHUNK_SIZE', default=1000, cast=int)
    incremental_days = env_config('FX_INCREMENTAL_DAYS', default=7, cast=int)

    # Parse historical start date
    historical_start = datetime.strptime(historical_start_str, '%Y-%m-%d').date()

    # Print header
    print("=" * 70)
    print("FX Rate to SQL Pipeline")
    print("=" * 70)
    print(f"Mode: {args.mode.upper()}")
    print(f"Base Currency: {BASE_CURRENCY}")
    print(f"Target Currencies: {', '.join(target_currencies)}")
    print(f"Target: PostgreSQL - {config.databases['postgresql'].database}")
    print("=" * 70)

    if args.mode == 'backfill':
        # Determine date range
        if args.start:
            start_date = datetime.strptime(args.start, '%Y-%m-%d').date()
        else:
            start_date = historical_start

        if args.end:
            end_date = datetime.strptime(args.end, '%Y-%m-%d').date()
        else:
            end_date = date.today()

        print(f"Date Range: {start_date} to {end_date}")

        daily_count, monthly_count = run_backfill(
            start_date=start_date,
            end_date=end_date,
            target_currencies=target_currencies,
            config=config,
            chunk_size=chunk_size
        )

    elif args.mode == 'auto':
        print(f"Looking back {incremental_days} days for updates")

        daily_count, monthly_count = run_auto(
            days_back=incremental_days,
            target_currencies=target_currencies,
            config=config,
            chunk_size=chunk_size
        )

    elif args.mode == 'refresh-monthly':
        monthly_count = run_refresh_monthly(config)
        daily_count = 0

    else:
        raise ValueError(f"Unknown mode: {args.mode}")

    # Print summary
    print("\n" + "=" * 70)
    print("Pipeline completed!")
    if daily_count > 0:
        print(f"  Daily rates: {daily_count} records")
    if monthly_count > 0:
        print(f"  Monthly averages: {monthly_count} records")
    print("=" * 70)


if __name__ == "__main__":
    main()
