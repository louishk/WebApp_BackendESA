"""
GoogleAdsPipeline — extract Google Ads data from BigQuery (esa_google_ads)
into PostgreSQL for PBI reporting.

Source: BigQuery project planar-beach-485003-v9, dataset esa_google_ads
  - 3 child accounts under MCC 3494417856:
    4605031997 (SG, SGD), 5469799452 (MY, SGD), 4855318963 (KR, KRW)

Tables synced:
  Dimensions (full overwrite): gads_campaigns, gads_ad_groups, gads_keywords
  Facts (upsert last 7 days): gads_campaign_daily, gads_campaign_conversions,
                               gads_ad_group_daily, gads_keyword_daily

Vault key: BIGQUERY_SA_JSON (service account JSON string)
Fallback:  GOOGLE_APPLICATION_CREDENTIALS env var (path to JSON file)

Scope keys honoured:
  - mode: 'auto' | 'backfill'   default 'auto'
"""

import json
import logging
import os
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional

from sync_service.pipelines.base import BasePipeline, RunResult

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
    """
    from google.cloud import bigquery
    from google.oauth2 import service_account
    from common.secrets_vault import vault_config

    sa_json = vault_config('BIGQUERY_SA_JSON', default=None)
    if sa_json:
        logger.info("google_ads: using BigQuery credentials from vault")
        info = json.loads(sa_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=BQ_PROJECT, credentials=credentials)

    creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if creds_path and os.path.exists(creds_path):
        logger.info("google_ads: using BigQuery credentials from GOOGLE_APPLICATION_CREDENTIALS")
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
    return [dict(row) for row in result]


def fetch_campaigns(client) -> List[Dict[str, Any]]:
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
    logger.info("google_ads: fetched %d campaigns", len(rows))
    return rows


def fetch_ad_groups(client) -> List[Dict[str, Any]]:
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
    logger.info("google_ads: fetched %d ad groups", len(rows))
    return rows


def fetch_keywords(client) -> List[Dict[str, Any]]:
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
    logger.info("google_ads: fetched %d keywords", len(rows))
    return rows


def fetch_campaign_stats(client, date_from: Optional[date] = None) -> List[Dict[str, Any]]:
    date_filter = f"AND segments_date >= '{date_from.isoformat()}'" if date_from else ""
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
    logger.info("google_ads: fetched %d campaign stat rows", len(rows))
    return rows


def fetch_campaign_conversions(client, date_from: Optional[date] = None) -> List[Dict[str, Any]]:
    date_filter = f"AND segments_date >= '{date_from.isoformat()}'" if date_from else ""
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
    logger.info("google_ads: fetched %d conversion rows", len(rows))
    return rows


def fetch_ad_group_stats(client, date_from: Optional[date] = None) -> List[Dict[str, Any]]:
    date_filter = f"AND segments_date >= '{date_from.isoformat()}'" if date_from else ""
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
    logger.info("google_ads: fetched %d ad group stat rows", len(rows))
    return rows


def fetch_keyword_stats(client, date_from: Optional[date] = None) -> List[Dict[str, Any]]:
    date_filter = f"AND segments_date >= '{date_from.isoformat()}'" if date_from else ""
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
    logger.info("google_ads: fetched %d keyword stat rows", len(rows))
    return rows


# =============================================================================
# Data Transformation
# =============================================================================

def transform_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Convert BigQuery row types to Python/PostgreSQL-compatible types."""
    result = dict(row)
    result['synced_at'] = datetime.utcnow()
    return result


# =============================================================================
# Database Operations
# =============================================================================

def truncate_and_load(
    records: List[Dict[str, Any]],
    model,
    table_label: str,
) -> int:
    """Full overwrite for dimension tables: DELETE ALL then INSERT."""
    from common import SessionManager, BatchOperations, Base
    from common.db import get_engine

    if not records:
        logger.warning("google_ads: no %s data to push", table_label)
        return 0

    engine = get_engine('pbi')
    Base.metadata.create_all(engine, tables=[model.__table__])

    session_manager = SessionManager(engine)
    with session_manager.session_scope() as session:
        deleted = session.query(model).delete()
        session.flush()
        logger.info("google_ads: deleted %d old %s rows", deleted, table_label)

        batch_ops = BatchOperations(session)
        count = batch_ops.batch_insert(model, records, chunk_size=500)

    logger.info("google_ads: inserted %d %s rows", count, table_label)
    return count


def upsert_facts(
    records: List[Dict[str, Any]],
    model,
    constraint_columns: List[str],
    table_label: str,
    chunk_size: int = 1000,
) -> int:
    """Upsert fact table records using ON CONFLICT on composite PK."""
    from common import SessionManager, UpsertOperations, Base, DatabaseType
    from common.db import get_engine

    if not records:
        logger.warning("google_ads: no %s data to push", table_label)
        return 0

    engine = get_engine('pbi')
    Base.metadata.create_all(engine, tables=[model.__table__])

    session_manager = SessionManager(engine)
    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, DatabaseType.POSTGRESQL)
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            upsert_ops.upsert_batch(
                model=model,
                records=chunk,
                constraint_columns=constraint_columns,
                chunk_size=chunk_size,
            )

    logger.info("google_ads: upserted %d %s rows", len(records), table_label)
    return len(records)


# =============================================================================
# Pipeline Stages
# =============================================================================

def sync_dimensions(client) -> Dict[str, int]:
    """Sync all dimension tables (full overwrite)."""
    from common import GadsCampaign, GadsAdGroup, GadsKeyword

    counts = {}

    logger.info("google_ads: syncing campaigns")
    counts['campaigns'] = truncate_and_load(
        [transform_row(r) for r in fetch_campaigns(client)],
        GadsCampaign, 'campaigns',
    )

    logger.info("google_ads: syncing ad_groups")
    counts['ad_groups'] = truncate_and_load(
        [transform_row(r) for r in fetch_ad_groups(client)],
        GadsAdGroup, 'ad_groups',
    )

    logger.info("google_ads: syncing keywords")
    counts['keywords'] = truncate_and_load(
        [transform_row(r) for r in fetch_keywords(client)],
        GadsKeyword, 'keywords',
    )

    return counts


def sync_facts(
    client,
    date_from: Optional[date] = None,
    chunk_size: int = 1000,
) -> Dict[str, int]:
    """Sync all fact tables (upsert)."""
    from common import (
        GadsCampaignDaily, GadsCampaignConversions,
        GadsAdGroupDaily, GadsKeywordDaily,
    )

    counts = {}

    logger.info("google_ads: syncing campaign_daily")
    counts['campaign_daily'] = upsert_facts(
        [transform_row(r) for r in fetch_campaign_stats(client, date_from)],
        GadsCampaignDaily,
        ['campaign_id', 'segments_date', 'device', 'ad_network_type'],
        'campaign_daily', chunk_size,
    )

    logger.info("google_ads: syncing campaign_conversions")
    counts['campaign_conversions'] = upsert_facts(
        [transform_row(r) for r in fetch_campaign_conversions(client, date_from)],
        GadsCampaignConversions,
        ['campaign_id', 'segments_date', 'conversion_action_name', 'ad_network_type'],
        'campaign_conversions', chunk_size,
    )

    logger.info("google_ads: syncing ad_group_daily")
    counts['ad_group_daily'] = upsert_facts(
        [transform_row(r) for r in fetch_ad_group_stats(client, date_from)],
        GadsAdGroupDaily,
        ['ad_group_id', 'segments_date', 'device', 'ad_network_type'],
        'ad_group_daily', chunk_size,
    )

    logger.info("google_ads: syncing keyword_daily")
    counts['keyword_daily'] = upsert_facts(
        [transform_row(r) for r in fetch_keyword_stats(client, date_from)],
        GadsKeywordDaily,
        ['criterion_id', 'ad_group_id', 'segments_date', 'device', 'ad_network_type'],
        'keyword_daily', chunk_size,
    )

    return counts


# =============================================================================
# Public run() orchestrator
# =============================================================================

def run(mode: str = 'auto') -> Dict[str, Any]:
    """Fetch Google Ads data from BigQuery and upsert to esa_pbi.

    Returns {'records': int, 'counts': {...}, 'mode': str}
    """
    from common.config import get_pipeline_config

    chunk_size = get_pipeline_config('google_ads', 'sql_chunk_size', 1000)
    lookback_days = get_pipeline_config('google_ads', 'lookback_days', LOOKBACK_DAYS)

    logger.info("google_ads run: mode=%s chunk_size=%d lookback_days=%d",
                mode, chunk_size, lookback_days)

    client = get_bq_client()

    dim_counts = sync_dimensions(client)

    if mode == 'backfill':
        logger.info("google_ads: facts full history (backfill)")
        fact_counts = sync_facts(client, date_from=None, chunk_size=chunk_size)
    else:
        date_from = date.today() - timedelta(days=lookback_days)
        logger.info("google_ads: facts from %s (%d-day lookback)", date_from.isoformat(), lookback_days)
        fact_counts = sync_facts(client, date_from=date_from, chunk_size=chunk_size)

    counts = {**dim_counts, **fact_counts}
    total = sum(counts.values())
    logger.info("google_ads complete: total=%d counts=%s", total, counts)

    return {'records': total, 'counts': counts, 'mode': mode}


class GoogleAdsPipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode = scope.get('mode', 'auto')

        result = run(mode=mode)

        return RunResult(
            status='refreshed',
            records=result['records'],
            scope=scope,
            metadata={'mode': mode, 'counts': result['counts']},
        )
