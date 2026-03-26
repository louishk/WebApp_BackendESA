"""
EmbedSocial Reviews to SQL Pipeline

Fetches review data from EmbedSocial GetItems API and pushes to PostgreSQL.
Tracks Google review scores and review counts over time per location.

Features:
- Two modes: backfill (all reviews), auto (incremental newest-first)
- Upsert by review_id (ON CONFLICT)
- Pagination handling for full review catalogue
- Chunked upsert for large datasets

Usage:
    # Backfill mode - fetch all reviews
    python embedsocial_to_sql.py --mode backfill

    # Auto mode - incremental: fetch recent, stop when existing found
    python embedsocial_to_sql.py --mode auto

    # Diagnose mode - read-only comparison of API vs DB state
    python embedsocial_to_sql.py --mode diagnose

Configuration (in scheduler.yaml):
    pipelines.embedsocial.sql_chunk_size: SQL upsert chunk size (default: 500)
    pipelines.embedsocial.page_size: API page size (default: 50)
    pipelines.embedsocial.incremental_pages: Max pages in auto mode (default: 5)
"""

import argparse
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

from tqdm import tqdm

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from common import (
    DataLayerConfig,
    create_engine_from_config,
    SessionManager,
    UpsertOperations,
    Base,
    HTTPClient,
    EmbedSocialReview,
)
from common.config import get_pipeline_config
from common.secrets_vault import vault_config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

API_BASE_URL = 'https://embedsocial.com/app/api/rest/v1'


# =============================================================================
# EmbedSocial API Client
# =============================================================================

def get_items(
    http_client: HTTPClient,
    api_key: str,
    page: int = 1,
    page_size: int = 50,
    sort: str = '-originalCreatedOn,-id'
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Fetch items from EmbedSocial GetItems API.

    Args:
        http_client: HTTPClient instance
        api_key: EmbedSocial API key
        page: Page number (1-based)
        page_size: Number of items per page
        sort: Sort order

    Returns:
        Tuple of (items list, total count from X-Total-Count header)
    """
    url = f"{API_BASE_URL}/items"
    params = {
        'page': page,
        'pageSize': page_size,
        'sort': sort,
    }
    headers = {
        'Authorization': api_key,
        'Content-Type': 'application/json',
    }

    response = http_client.get(url, headers=headers, params=params)
    response.raise_for_status()

    total_count = int(response.headers.get('X-Total-Count', 0))
    items = response.json()

    return items, total_count


# =============================================================================
# Data Transformation
# =============================================================================

def transform_review(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform an EmbedSocial item into a database record dict.

    Args:
        item: Raw item from EmbedSocial API

    Returns:
        Dict ready for DB upsert
    """
    # Parse original_created_on
    original_created_on = None
    raw_date = item.get('originalCreatedOn') or item.get('original_created_on')
    if raw_date:
        for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S.%fZ'):
            try:
                original_created_on = datetime.strptime(raw_date, fmt)
                break
            except (ValueError, TypeError):
                continue
        if original_created_on is None:
            try:
                original_created_on = datetime.fromisoformat(raw_date.replace('Z', '+00:00')).replace(tzinfo=None)
            except (ValueError, TypeError):
                original_created_on = datetime.utcnow()

    # Parse reply_created_on
    reply_created_on = None
    replies = item.get('replies') or []
    reply_text = None
    if replies and len(replies) > 0:
        first_reply = replies[0]
        reply_text = first_reply.get('text') or first_reply.get('caption')
        raw_reply_date = first_reply.get('createdOn') or first_reply.get('created_on')
        if raw_reply_date:
            try:
                reply_created_on = datetime.fromisoformat(raw_reply_date.replace('Z', '+00:00')).replace(tzinfo=None)
            except (ValueError, TypeError):
                pass

    # Get source info
    source = item.get('source') or {}

    return {
        'review_id': str(item.get('id', '')),
        'source_id': str(source.get('id', '') or item.get('sourceId', '')),
        'source_name': source.get('name') or item.get('sourceName'),
        'source_address': source.get('address') or item.get('sourceAddress'),
        'author_name': item.get('authorName') or item.get('author_name'),
        'rating': int(item.get('rating', 0)),
        'caption_text': item.get('caption') or item.get('captionText'),
        'review_link': item.get('reviewLink') or item.get('review_link'),
        'original_created_on': original_created_on or datetime.utcnow(),
        'reply_text': reply_text,
        'reply_created_on': reply_created_on,
        'synced_at': datetime.utcnow(),
    }


# =============================================================================
# Database Operations
# =============================================================================

def push_reviews_to_database(
    data: List[Dict[str, Any]],
    config: DataLayerConfig,
    chunk_size: int = 500
) -> int:
    """
    Upsert review records to PostgreSQL.

    Returns:
        Number of records upserted
    """
    if not data:
        print("  No review data to push")
        return 0

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found in .env")

    engine = create_engine_from_config(db_config)

    # Ensure table exists
    Base.metadata.create_all(engine, tables=[EmbedSocialReview.__table__])
    tqdm.write("  Table 'embedsocial_reviews' ready")

    session_manager = SessionManager(engine)
    num_chunks = (len(data) + chunk_size - 1) // chunk_size

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)

        with tqdm(total=len(data), desc="  Upserting reviews", unit="rec") as pbar:
            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]

                upsert_ops.upsert_batch(
                    model=EmbedSocialReview,
                    records=chunk,
                    constraint_columns=['review_id'],
                    chunk_size=chunk_size
                )

                pbar.update(len(chunk))
                pbar.set_postfix({"chunk": f"{i // chunk_size + 1}/{num_chunks}"})

    tqdm.write(f"  Upserted {len(data)} review records")
    return len(data)


def get_existing_review_ids(config: DataLayerConfig, review_ids: List[str]) -> set:
    """
    Check which review_ids already exist in the database.

    Args:
        config: DataLayerConfig instance
        review_ids: List of review IDs to check

    Returns:
        Set of existing review_ids
    """
    if not review_ids:
        return set()

    db_config = config.databases.get('postgresql')
    if not db_config:
        return set()

    engine = create_engine_from_config(db_config)
    session_manager = SessionManager(engine)

    try:
        with session_manager.session_scope() as session:
            results = session.query(EmbedSocialReview.review_id).filter(
                EmbedSocialReview.review_id.in_(review_ids)
            ).all()
            return {r[0] for r in results}
    except Exception:
        return set()


# =============================================================================
# Pipeline Functions
# =============================================================================

def run_backfill(
    api_key: str,
    config: DataLayerConfig,
    page_size: int,
    chunk_size: int
) -> int:
    """
    Backfill mode: fetch ALL reviews from the API.

    Returns:
        Total number of records upserted
    """
    http_client = HTTPClient()
    all_reviews = []

    # First request to get total count
    print("[STAGE:FETCH] Fetching reviews from EmbedSocial API")
    print("\nFetching first page to determine total count...")
    items, total_count = get_items(http_client, api_key, page=1, page_size=page_size)
    print(f"Total reviews available: {total_count}")

    if total_count == 0:
        print("No reviews found")
        return 0

    total_pages = (total_count + page_size - 1) // page_size
    print(f"Total pages to fetch: {total_pages}")

    # Transform first page
    for item in items:
        all_reviews.append(transform_review(item))

    # Fetch remaining pages
    for page in tqdm(range(2, total_pages + 1), desc="Fetching pages", initial=1, total=total_pages):
        items, _ = get_items(http_client, api_key, page=page, page_size=page_size)

        if not items:
            tqdm.write(f"  Page {page}: no items returned, stopping")
            break

        for item in items:
            all_reviews.append(transform_review(item))

    # Deduplicate by review_id (API may return overlapping items across pages)
    seen = {}
    for review in all_reviews:
        seen[review['review_id']] = review
    all_reviews = list(seen.values())

    print(f"\nFetched {len(all_reviews)} unique reviews")

    # Push to database
    print("[STAGE:PUSH] Writing to PostgreSQL")
    print(f"\nPushing {len(all_reviews)} reviews to database...")
    count = push_reviews_to_database(all_reviews, config, chunk_size)

    return count


def run_auto(
    api_key: str,
    config: DataLayerConfig,
    page_size: int,
    chunk_size: int,
    max_pages: int
) -> int:
    """
    Auto/incremental mode: fetch newest reviews, stop when all in a page already exist.

    Returns:
        Total number of new records upserted
    """
    http_client = HTTPClient()
    new_reviews = []

    print(f"\nIncremental sync (max {max_pages} pages, newest first)...")

    for page in range(1, max_pages + 1):
        items, total_count = get_items(
            http_client, api_key,
            page=page, page_size=page_size,
            sort='-originalCreatedOn,-id'
        )

        if not items:
            print(f"  Page {page}: no items returned, stopping")
            break

        if page == 1:
            print(f"  Total reviews available: {total_count}")
            # Log newest review date from API for monitoring
            if items:
                newest_date = items[0].get('originalCreatedOn', 'unknown')
                logger.info("API newest review originalCreatedOn: %s", newest_date)

        # Check which reviews already exist
        page_review_ids = [str(item.get('id', '')) for item in items]
        existing_ids = get_existing_review_ids(config, page_review_ids)

        new_count = 0
        for item in items:
            item_id = str(item.get('id', ''))
            if item_id not in existing_ids:
                new_reviews.append(transform_review(item))
                new_count += 1

        print(f"  Page {page}: {new_count} new, {len(items) - new_count} existing")

        # If all reviews in this page already exist, we're caught up
        if new_count == 0:
            print("  All reviews on this page already exist, stopping")
            break

    # Deduplicate by review_id (API may return same review across pages)
    seen = {}
    for review in new_reviews:
        seen[review['review_id']] = review
    new_reviews = list(seen.values())

    if not new_reviews:
        # Safety net: warn if API reports more reviews than DB has
        try:
            db_config = config.databases.get('postgresql')
            if db_config and total_count > 0:
                engine = create_engine_from_config(db_config)
                sm = SessionManager(engine)
                with sm.session_scope() as session:
                    db_count = session.query(EmbedSocialReview).count()
                if total_count > db_count:
                    logger.warning(
                        "Auto found 0 new reviews but API total (%d) > DB count (%d). "
                        "Gap of %d reviews — consider running --mode backfill",
                        total_count, db_count, total_count - db_count
                    )
                else:
                    logger.info("DB count (%d) >= API total (%d), fully synced", db_count, total_count)
        except Exception as e:
            logger.debug("Could not compare API vs DB counts: %s", e)
        print("\nNo new reviews to sync")
        return 0

    print(f"\nPushing {len(new_reviews)} new reviews to database...")
    count = push_reviews_to_database(new_reviews, config, chunk_size)

    return count


def run_diagnose(
    api_key: str,
    config: DataLayerConfig,
    page_size: int
) -> int:
    """
    Diagnose mode: read-only check comparing API state vs DB state.
    No DB writes.
    """
    http_client = HTTPClient()

    print("\n[DIAGNOSE] Fetching page 1 from API (newest first)...")
    items, total_count = get_items(http_client, api_key, page=1, page_size=page_size)
    print(f"  API total_count: {total_count}")
    print(f"  Items on page 1: {len(items)}")

    if items:
        print(f"\n  Newest 5 reviews from API:")
        for item in items[:5]:
            print(f"    id={item.get('id')}  date={item.get('originalCreatedOn')}  "
                  f"rating={item.get('rating')}  author={item.get('authorName', '')[:30]}")

    # Compare with DB
    db_config = config.databases.get('postgresql')
    if db_config:
        engine = create_engine_from_config(db_config)
        sm = SessionManager(engine)
        try:
            with sm.session_scope() as session:
                db_count = session.query(EmbedSocialReview).count()
                newest_db = session.query(EmbedSocialReview.original_created_on).order_by(
                    EmbedSocialReview.original_created_on.desc()
                ).first()
            print(f"\n  DB review count: {db_count}")
            if newest_db and newest_db[0]:
                print(f"  DB newest review date: {newest_db[0].isoformat()}")
            gap = total_count - db_count
            if gap > 0:
                print(f"\n  GAP: {gap} reviews in API not in DB")
            elif gap == 0:
                print(f"\n  DB is in sync with API")
            else:
                print(f"\n  DB has {abs(gap)} more records than API total (possible duplicates or deletions)")
        except Exception as e:
            logger.debug("Could not query DB for diagnose: %s", e)

    # Check if page 1 items exist in DB
    if items and db_config:
        page_ids = [str(item.get('id', '')) for item in items]
        existing = get_existing_review_ids(config, page_ids)
        new_on_page1 = len(page_ids) - len(existing)
        print(f"\n  Page 1: {new_on_page1} new, {len(existing)} already in DB")

    return 0


# =============================================================================
# CLI and Main
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='EmbedSocial Reviews to SQL Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill mode - fetch all reviews
  python embedsocial_to_sql.py --mode backfill

  # Auto mode - incremental sync (newest first, stop when caught up)
  python embedsocial_to_sql.py --mode auto

  # Diagnose mode - read-only check of API vs DB state
  python embedsocial_to_sql.py --mode diagnose
        """
    )

    parser.add_argument(
        '--mode',
        choices=['backfill', 'auto', 'diagnose'],
        required=True,
        help='Extraction mode: backfill (all reviews), auto (incremental), diagnose (read-only check)'
    )

    return parser.parse_args()


def main():
    """Main function to fetch and push EmbedSocial reviews to SQL."""
    args = parse_args()

    # Load configuration
    config = DataLayerConfig.from_env()

    # Get API key from vault (falls back to env var)
    api_key = vault_config('EMBEDSOCIAL_API_KEY')
    if not api_key:
        raise ValueError(
            "EMBEDSOCIAL_API_KEY not found. Set it in the vault or as an environment variable."
        )

    # Load pipeline config from scheduler.yaml
    page_size = get_pipeline_config('embedsocial', 'page_size', 50)
    chunk_size = get_pipeline_config('embedsocial', 'sql_chunk_size', 500)
    incremental_pages = get_pipeline_config('embedsocial', 'incremental_pages', 5)

    # Print header
    print("=" * 70)
    print("EmbedSocial Reviews to SQL Pipeline")
    print("=" * 70)
    print(f"Mode: {args.mode.upper()}")
    print(f"Target: PostgreSQL - {config.databases['postgresql'].database}")
    print(f"Page size: {page_size}")
    print("=" * 70)
    print("[STAGE:INIT] EmbedSocial")

    if args.mode == 'backfill':
        count = run_backfill(
            api_key=api_key,
            config=config,
            page_size=page_size,
            chunk_size=chunk_size
        )

    elif args.mode == 'auto':
        count = run_auto(
            api_key=api_key,
            config=config,
            page_size=page_size,
            chunk_size=chunk_size,
            max_pages=incremental_pages
        )

    elif args.mode == 'diagnose':
        count = run_diagnose(
            api_key=api_key,
            config=config,
            page_size=page_size
        )

    else:
        raise ValueError(f"Unknown mode: {args.mode}")

    # Print summary
    print(f"[STAGE:COMPLETE] {count} records")
    print("\n" + "=" * 70)
    print("Pipeline completed!")
    print(f"  Reviews processed: {count} records")
    print("=" * 70)


if __name__ == "__main__":
    main()
