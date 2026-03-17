"""
Google Ads BigQuery to PostgreSQL Pipeline

Extracts Google Ads data from BigQuery (esa_google_ads dataset, populated by
Google Ads Data Transfer) and loads into esa_pbi PostgreSQL for PBI reporting.

Source: BigQuery project planar-beach-485003-v9, dataset esa_google_ads
  - 3 child accounts under MCC 3494417856:
    4605031997 (SG, SGD), 5469799452 (MY, SGD), 4855318963 (KR, KRW)

Tables synced:
  Dimensions (full overwrite): gads_campaigns, gads_ad_groups, gads_keywords
  Facts (upsert last 7 days): gads_campaign_daily, gads_campaign_conversions,
                               gads_ad_group_daily, gads_keyword_daily

Usage:
    # Full historical load (first run)
    python google_ads_to_sql.py --mode backfill

    # Incremental daily sync (last 7 days)
    python google_ads_to_sql.py --mode auto

Configuration:
    Vault key: BIGQUERY_SA_JSON (service account JSON string)
    Fallback: GOOGLE_APPLICATION_CREDENTIALS env var (path to JSON file)
"""

import argparse
import json
import logging
import os
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional

from tqdm import tqdm

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import (
    DataLayerConfig,
    create_engine_from_config,
    SessionManager,
    UpsertOperations,
    BatchOperations,
    Base,
    GadsAccountMap,
    GadsCampaign,
    GadsAdGroup,
    GadsKeyword,
    GadsCampaignDaily,
    GadsCampaignConversions,
    GadsAdGroupDaily,
    GadsKeywordDaily,
)
from common.config import get_pipeline_config
from common.secrets_vault import vault_config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# BigQuery project and dataset
BQ_PROJECT = 'planar-beach-485003-v9'
BQ_DATASET = 'esa_google_ads'

# MCC account ID (views are named ads_<Resource>_<MCC_ID>)
MCC_ID = '3494417856'

# Days to look back for incremental sync (catches Google's retroactive adjustments)
LOOKBACK_DAYS = 7


# =============================================================================
# BigQuery Client Setup
# =============================================================================

def get_bq_client():
    """
    Create BigQuery client using vault credentials or env var fallback.

    Priority:
    1. BIGQUERY_SA_JSON vault key (JSON string)
    2. GOOGLE_APPLICATION_CREDENTIALS env var (path to JSON file)
    3. Service account JSON file in repo (dev fallback)
    """
    from google.cloud import bigquery
    from google.oauth2 import service_account

    # Try vault first
    sa_json = vault_config('BIGQUERY_SA_JSON', default=None)
    if sa_json:
        logger.info("Using BigQuery credentials from vault")
        info = json.loads(sa_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=BQ_PROJECT, credentials=credentials)

    # Fallback to env var
    creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if creds_path and os.path.exists(creds_path):
        logger.info(f"Using BigQuery credentials from GOOGLE_APPLICATION_CREDENTIALS")
        credentials = service_account.Credentials.from_service_account_file(creds_path)
        return bigquery.Client(project=BQ_PROJECT, credentials=credentials)

    raise ValueError(
        "BigQuery credentials not found. Set BIGQUERY_SA_JSON in vault, "
        "or GOOGLE_APPLICATION_CREDENTIALS env var."
    )


# =============================================================================
# BigQuery Queries
# =============================================================================

def query_bq(client, sql: str) -> List[Dict[str, Any]]:
    """Execute a BigQuery SQL query and return rows as dicts."""
    result = client.query(sql).result()
    rows = []
    for row in result:
        rows.append(dict(row))
    return rows


def fetch_campaigns(client) -> List[Dict[str, Any]]:
    """Fetch latest campaign snapshot from BigQuery."""
    sql = f"""
    SELECT
        campaign_id,
        customer_id,
        campaign_name,
        campaign_status,
        campaign_advertising_channel_type AS channel_type,
        campaign_advertising_channel_sub_type AS channel_sub_type,
        campaign_bidding_strategy_type AS bidding_strategy_type,
        campaign_budget_amount_micros AS budget_amount_micros,
        campaign_start_date AS start_date,
        campaign_end_date AS end_date,
        _DATA_DATE AS _data_date
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_Campaign_{MCC_ID}`
    WHERE _DATA_DATE = _LATEST_DATE
    """
    rows = query_bq(client, sql)
    logger.info(f"  Fetched {len(rows)} campaigns")
    return rows


def fetch_ad_groups(client) -> List[Dict[str, Any]]:
    """Fetch latest ad group snapshot from BigQuery."""
    sql = f"""
    SELECT
        ad_group_id,
        campaign_id,
        customer_id,
        ad_group_name,
        ad_group_status,
        ad_group_type,
        _DATA_DATE AS _data_date
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_AdGroup_{MCC_ID}`
    WHERE _DATA_DATE = _LATEST_DATE
    """
    rows = query_bq(client, sql)
    logger.info(f"  Fetched {len(rows)} ad groups")
    return rows


def fetch_keywords(client) -> List[Dict[str, Any]]:
    """Fetch latest keyword snapshot from BigQuery."""
    sql = f"""
    SELECT
        ad_group_criterion_criterion_id AS criterion_id,
        ad_group_id,
        campaign_id,
        ad_group_criterion_keyword_text AS keyword_text,
        ad_group_criterion_keyword_match_type AS match_type,
        ad_group_criterion_negative AS is_negative,
        ad_group_criterion_status AS status,
        ad_group_criterion_quality_info_quality_score AS quality_score,
        _DATA_DATE AS _data_date
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_Keyword_{MCC_ID}`
    WHERE _DATA_DATE = _LATEST_DATE
    """
    rows = query_bq(client, sql)
    logger.info(f"  Fetched {len(rows)} keywords")
    return rows


def fetch_campaign_stats(client, date_from: Optional[date] = None) -> List[Dict[str, Any]]:
    """Fetch campaign daily stats from BigQuery (aggregated across click types)."""
    date_filter = ""
    if date_from:
        date_filter = f"AND segments_date >= '{date_from.isoformat()}'"

    sql = f"""
    SELECT
        campaign_id,
        segments_date,
        segments_device AS device,
        segments_ad_network_type AS ad_network_type,
        SUM(metrics_impressions) AS impressions,
        SUM(metrics_clicks) AS clicks,
        SUM(metrics_cost_micros) AS cost_micros,
        SUM(metrics_conversions) AS conversions,
        SUM(metrics_conversions_value) AS conversions_value,
        SUM(metrics_interactions) AS interactions
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_CampaignStats_{MCC_ID}`
    WHERE 1=1 {date_filter}
    GROUP BY campaign_id, segments_date, segments_device, segments_ad_network_type
    """
    rows = query_bq(client, sql)
    logger.info(f"  Fetched {len(rows)} campaign stat rows")
    return rows


def fetch_campaign_conversions(client, date_from: Optional[date] = None) -> List[Dict[str, Any]]:
    """Fetch campaign conversion stats by action from BigQuery."""
    date_filter = ""
    if date_from:
        date_filter = f"AND segments_date >= '{date_from.isoformat()}'"

    sql = f"""
    SELECT
        campaign_id,
        segments_date,
        segments_conversion_action_name AS conversion_action_name,
        segments_conversion_action_category AS conversion_action_category,
        SUM(metrics_conversions) AS conversions,
        SUM(metrics_conversions_value) AS conversions_value,
        SUM(metrics_value_per_conversion) AS value_per_conversion,
        segments_ad_network_type AS ad_network_type
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_CampaignConversionStats_{MCC_ID}`
    WHERE 1=1 {date_filter}
    GROUP BY campaign_id, segments_date, segments_conversion_action_name,
             segments_conversion_action_category, segments_ad_network_type
    """
    rows = query_bq(client, sql)
    logger.info(f"  Fetched {len(rows)} conversion rows")
    return rows


def fetch_ad_group_stats(client, date_from: Optional[date] = None) -> List[Dict[str, Any]]:
    """Fetch ad group daily stats from BigQuery (aggregated across click types)."""
    date_filter = ""
    if date_from:
        date_filter = f"AND segments_date >= '{date_from.isoformat()}'"

    sql = f"""
    SELECT
        ad_group_id,
        campaign_id,
        segments_date,
        segments_device AS device,
        segments_ad_network_type AS ad_network_type,
        SUM(metrics_impressions) AS impressions,
        SUM(metrics_clicks) AS clicks,
        SUM(metrics_cost_micros) AS cost_micros,
        SUM(metrics_conversions) AS conversions,
        SUM(metrics_conversions_value) AS conversions_value,
        SUM(metrics_interactions) AS interactions
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_AdGroupStats_{MCC_ID}`
    WHERE 1=1 {date_filter}
    GROUP BY ad_group_id, campaign_id, segments_date, segments_device, segments_ad_network_type
    """
    rows = query_bq(client, sql)
    logger.info(f"  Fetched {len(rows)} ad group stat rows")
    return rows


def fetch_keyword_stats(client, date_from: Optional[date] = None) -> List[Dict[str, Any]]:
    """Fetch keyword daily stats from BigQuery (aggregated across click types)."""
    date_filter = ""
    if date_from:
        date_filter = f"AND segments_date >= '{date_from.isoformat()}'"

    sql = f"""
    SELECT
        ad_group_criterion_criterion_id AS criterion_id,
        ad_group_id,
        campaign_id,
        segments_date,
        segments_device AS device,
        segments_ad_network_type AS ad_network_type,
        SUM(metrics_impressions) AS impressions,
        SUM(metrics_clicks) AS clicks,
        SUM(metrics_cost_micros) AS cost_micros,
        SUM(metrics_conversions) AS conversions,
        SUM(metrics_conversions_value) AS conversions_value,
        SUM(metrics_interactions) AS interactions
    FROM `{BQ_PROJECT}.{BQ_DATASET}.ads_KeywordStats_{MCC_ID}`
    WHERE 1=1 {date_filter}
    GROUP BY ad_group_criterion_criterion_id, ad_group_id, campaign_id,
             segments_date, segments_device, segments_ad_network_type
    """
    rows = query_bq(client, sql)
    logger.info(f"  Fetched {len(rows)} keyword stat rows")
    return rows


# =============================================================================
# Data Transformation
# =============================================================================

def transform_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert BigQuery row types to Python/PostgreSQL-compatible types.
    Handles date objects, Decimal, None values.
    """
    result = {}
    for k, v in row.items():
        if v is None:
            result[k] = v
        elif hasattr(v, 'isoformat') and callable(v.isoformat):
            # date/datetime objects - keep as-is, SQLAlchemy handles them
            result[k] = v
        else:
            result[k] = v
    result['synced_at'] = datetime.utcnow()
    return result


# =============================================================================
# Database Operations
# =============================================================================

def truncate_and_load(
    records: List[Dict[str, Any]],
    model,
    config: DataLayerConfig,
    table_label: str
) -> int:
    """
    Full overwrite for dimension tables: DELETE ALL then INSERT.
    Used for small tables (campaigns, ad_groups, keywords).
    """
    if not records:
        print(f"  No {table_label} data to push")
        return 0

    db_config = config.databases.get('postgresql')
    engine = create_engine_from_config(db_config)
    Base.metadata.create_all(engine, tables=[model.__table__])

    session_manager = SessionManager(engine)
    with session_manager.session_scope() as session:
        # Delete all existing rows
        deleted = session.query(model).delete()
        session.flush()
        tqdm.write(f"  Deleted {deleted} old {table_label} rows")

        # Batch insert
        batch_ops = BatchOperations(session)
        count = batch_ops.batch_insert(model, records, chunk_size=500)

    tqdm.write(f"  Inserted {count} {table_label} rows")
    return count


def upsert_facts(
    records: List[Dict[str, Any]],
    model,
    constraint_columns: List[str],
    config: DataLayerConfig,
    table_label: str,
    chunk_size: int = 1000
) -> int:
    """
    Upsert fact table records using ON CONFLICT on composite PK.
    """
    if not records:
        print(f"  No {table_label} data to push")
        return 0

    db_config = config.databases.get('postgresql')
    engine = create_engine_from_config(db_config)
    Base.metadata.create_all(engine, tables=[model.__table__])

    session_manager = SessionManager(engine)
    num_chunks = (len(records) + chunk_size - 1) // chunk_size

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)

        with tqdm(total=len(records), desc=f"  Upserting {table_label}", unit="rec") as pbar:
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]

                upsert_ops.upsert_batch(
                    model=model,
                    records=chunk,
                    constraint_columns=constraint_columns,
                    chunk_size=chunk_size
                )

                pbar.update(len(chunk))
                pbar.set_postfix({"chunk": f"{i // chunk_size + 1}/{num_chunks}"})

    tqdm.write(f"  Upserted {len(records)} {table_label} rows")
    return len(records)


# =============================================================================
# Pipeline Functions
# =============================================================================

def sync_dimensions(client, config: DataLayerConfig) -> Dict[str, int]:
    """Sync all dimension tables (full overwrite)."""
    counts = {}

    print("\n--- Campaigns ---")
    rows = fetch_campaigns(client)
    records = [transform_row(r) for r in rows]
    counts['campaigns'] = truncate_and_load(records, GadsCampaign, config, 'campaigns')

    print("\n--- Ad Groups ---")
    rows = fetch_ad_groups(client)
    records = [transform_row(r) for r in rows]
    counts['ad_groups'] = truncate_and_load(records, GadsAdGroup, config, 'ad_groups')

    print("\n--- Keywords ---")
    rows = fetch_keywords(client)
    records = [transform_row(r) for r in rows]
    counts['keywords'] = truncate_and_load(records, GadsKeyword, config, 'keywords')

    return counts


def sync_facts(
    client,
    config: DataLayerConfig,
    date_from: Optional[date] = None,
    chunk_size: int = 1000
) -> Dict[str, int]:
    """Sync all fact tables (upsert)."""
    counts = {}

    print("\n--- Campaign Daily Stats ---")
    rows = fetch_campaign_stats(client, date_from)
    records = [transform_row(r) for r in rows]
    counts['campaign_daily'] = upsert_facts(
        records, GadsCampaignDaily,
        ['campaign_id', 'segments_date', 'device', 'ad_network_type'],
        config, 'campaign_daily', chunk_size
    )

    print("\n--- Campaign Conversions ---")
    rows = fetch_campaign_conversions(client, date_from)
    records = [transform_row(r) for r in rows]
    counts['campaign_conversions'] = upsert_facts(
        records, GadsCampaignConversions,
        ['campaign_id', 'segments_date', 'conversion_action_name', 'ad_network_type'],
        config, 'campaign_conversions', chunk_size
    )

    print("\n--- Ad Group Daily Stats ---")
    rows = fetch_ad_group_stats(client, date_from)
    records = [transform_row(r) for r in rows]
    counts['ad_group_daily'] = upsert_facts(
        records, GadsAdGroupDaily,
        ['ad_group_id', 'segments_date', 'device', 'ad_network_type'],
        config, 'ad_group_daily', chunk_size
    )

    print("\n--- Keyword Daily Stats ---")
    rows = fetch_keyword_stats(client, date_from)
    records = [transform_row(r) for r in rows]
    counts['keyword_daily'] = upsert_facts(
        records, GadsKeywordDaily,
        ['criterion_id', 'ad_group_id', 'segments_date', 'device', 'ad_network_type'],
        config, 'keyword_daily', chunk_size
    )

    return counts


def run_backfill(config: DataLayerConfig, chunk_size: int) -> Dict[str, int]:
    """Full historical load from BigQuery."""
    client = get_bq_client()

    print("[STAGE:FETCH] Querying BigQuery")
    print("\nSyncing dimensions (full overwrite)...")
    dim_counts = sync_dimensions(client, config)

    print("[STAGE:PUSH] Writing to PostgreSQL")
    print("\nSyncing facts (full history)...")
    fact_counts = sync_facts(client, config, date_from=None, chunk_size=chunk_size)

    return {**dim_counts, **fact_counts}


def run_auto(config: DataLayerConfig, chunk_size: int, lookback_days: int) -> Dict[str, int]:
    """Incremental sync: dimensions full overwrite, facts last N days."""
    client = get_bq_client()

    print("[STAGE:FETCH] Querying BigQuery")
    print("\nSyncing dimensions (full overwrite)...")
    dim_counts = sync_dimensions(client, config)

    print("[STAGE:PUSH] Writing to PostgreSQL")
    date_from = date.today() - timedelta(days=lookback_days)
    print(f"\nSyncing facts (from {date_from.isoformat()}, {lookback_days} day lookback)...")
    fact_counts = sync_facts(client, config, date_from=date_from, chunk_size=chunk_size)

    return {**dim_counts, **fact_counts}


# =============================================================================
# CLI and Main
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Google Ads BigQuery to PostgreSQL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full historical load (first run)
  python google_ads_to_sql.py --mode backfill

  # Incremental daily sync (last 7 days)
  python google_ads_to_sql.py --mode auto
        """
    )

    parser.add_argument(
        '--mode',
        choices=['backfill', 'auto'],
        required=True,
        help='Extraction mode: backfill (full history), auto (incremental)'
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    # Load configuration
    config = DataLayerConfig.from_env()

    # Pipeline tuning from scheduler.yaml
    chunk_size = get_pipeline_config('google_ads', 'sql_chunk_size', 1000)
    lookback_days = get_pipeline_config('google_ads', 'lookback_days', LOOKBACK_DAYS)

    # Print header
    print("=" * 70)
    print("Google Ads BigQuery to PostgreSQL Pipeline")
    print("=" * 70)
    print(f"Mode: {args.mode.upper()}")
    print(f"Target: PostgreSQL - {config.databases['postgresql'].database}")
    print(f"BQ Dataset: {BQ_PROJECT}.{BQ_DATASET}")
    print(f"MCC Account: {MCC_ID}")
    if args.mode == 'auto':
        print(f"Lookback: {lookback_days} days")
    print("=" * 70)
    print("[STAGE:INIT] GoogleAds")

    if args.mode == 'backfill':
        counts = run_backfill(config, chunk_size)
    elif args.mode == 'auto':
        counts = run_auto(config, chunk_size, lookback_days)
    else:
        raise ValueError(f"Unknown mode: {args.mode}")

    # Print summary
    total = sum(counts.values())
    print(f"[STAGE:COMPLETE] {total} records")
    print("\n" + "=" * 70)
    print("Pipeline completed!")
    for table, count in counts.items():
        print(f"  {table}: {count:,} records")
    print(f"  TOTAL: {total:,} records")
    print("=" * 70)


if __name__ == "__main__":
    main()
