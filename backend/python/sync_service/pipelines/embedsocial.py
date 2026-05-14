"""
EmbedSocialPipeline — sync Google reviews from EmbedSocial GetItems API to esa_pbi.

Upserts to esa_pbi.embedsocial_reviews keyed on review_id.

Modes:
  - auto (default): incremental — fetch newest pages, stop once a page has 0 new IDs
  - backfill:       paginate through every review available from the API
  - diagnose:       read-only API vs DB gap check (no writes; records=0)

Scope keys honoured (all optional):
  - mode: 'auto' | 'backfill' | 'diagnose'   (default 'auto')
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple

from sync_service.pipelines.base import BasePipeline, RunResult

logger = logging.getLogger(__name__)

API_BASE_URL = 'https://embedsocial.com/app/api/rest/v1'


def get_items(http_client, api_key: str, page: int = 1, page_size: int = 50,
              sort: str = '-originalCreatedOn,-id') -> Tuple[List[Dict[str, Any]], int]:
    url = f"{API_BASE_URL}/items"
    params = {'page': page, 'pageSize': page_size, 'sort': sort}
    headers = {'Authorization': api_key, 'Content-Type': 'application/json'}

    response = http_client.get(url, headers=headers, params=params)
    response.raise_for_status()

    total_count = int(response.headers.get('X-Total-Count', 0))
    items = response.json()
    return items, total_count


def transform_review(item: Dict[str, Any]) -> Dict[str, Any]:
    original_created_on = None
    raw_date = item.get('originalCreatedOn') or item.get('original_created_on')
    if raw_date:
        for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f',
                    '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S.%fZ'):
            try:
                original_created_on = datetime.strptime(raw_date, fmt)
                break
            except (ValueError, TypeError):
                continue
        if original_created_on is None:
            try:
                original_created_on = datetime.fromisoformat(
                    raw_date.replace('Z', '+00:00')
                ).replace(tzinfo=None)
            except (ValueError, TypeError):
                original_created_on = datetime.utcnow()

    reply_created_on = None
    reply_text = None
    replies = item.get('replies') or []
    if replies:
        first_reply = replies[0]
        reply_text = first_reply.get('text') or first_reply.get('caption')
        raw_reply_date = first_reply.get('createdOn') or first_reply.get('created_on')
        if raw_reply_date:
            try:
                reply_created_on = datetime.fromisoformat(
                    raw_reply_date.replace('Z', '+00:00')
                ).replace(tzinfo=None)
            except (ValueError, TypeError):
                pass

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


def push_reviews_to_database(data: List[Dict[str, Any]], config, chunk_size: int = 500) -> int:
    from common import (
        SessionManager, UpsertOperations, Base, EmbedSocialReview,
    )
    from common.db import get_engine
    if not data:
        return 0

    db_config = config.databases.get('postgresql')
    if not db_config:
        raise ValueError("PostgreSQL configuration not found")

    engine = get_engine('pbi')
    Base.metadata.create_all(engine, tables=[EmbedSocialReview.__table__])
    session_manager = SessionManager(engine)

    with session_manager.session_scope() as session:
        upsert_ops = UpsertOperations(session, db_config.db_type)
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            upsert_ops.upsert_batch(
                model=EmbedSocialReview, records=chunk,
                constraint_columns=['review_id'],
                chunk_size=chunk_size,
            )
    logger.info("embedsocial upserted %d review records", len(data))
    return len(data)


def get_existing_review_ids(config, review_ids: List[str]) -> set:
    from common import SessionManager, EmbedSocialReview
    from common.db import get_engine
    if not review_ids:
        return set()

    db_config = config.databases.get('postgresql')
    if not db_config:
        return set()

    engine = get_engine('pbi')
    session_manager = SessionManager(engine)
    try:
        with session_manager.session_scope() as session:
            results = session.query(EmbedSocialReview.review_id).filter(
                EmbedSocialReview.review_id.in_(review_ids)
            ).all()
            return {r[0] for r in results}
    except Exception:
        return set()


def run_backfill(api_key: str, config, page_size: int, chunk_size: int) -> int:
    from common import HTTPClient
    http_client = HTTPClient()
    all_reviews: List[Dict[str, Any]] = []

    items, total_count = get_items(http_client, api_key, page=1, page_size=page_size)
    logger.info("embedsocial backfill: API total_count=%d", total_count)
    if total_count == 0:
        return 0

    total_pages = (total_count + page_size - 1) // page_size
    for item in items:
        all_reviews.append(transform_review(item))

    for page in range(2, total_pages + 1):
        items, _ = get_items(http_client, api_key, page=page, page_size=page_size)
        if not items:
            logger.info("embedsocial backfill: page %d empty, stopping", page)
            break
        for item in items:
            all_reviews.append(transform_review(item))

    seen: Dict[str, Dict[str, Any]] = {}
    for r in all_reviews:
        seen[r['review_id']] = r
    all_reviews = list(seen.values())
    logger.info("embedsocial backfill: %d unique reviews", len(all_reviews))

    return push_reviews_to_database(all_reviews, config, chunk_size)


def run_auto(api_key: str, config, page_size: int, chunk_size: int, max_pages: int) -> int:
    from common import HTTPClient, SessionManager, EmbedSocialReview
    from common.db import get_engine

    http_client = HTTPClient()
    new_reviews: List[Dict[str, Any]] = []
    total_count = 0

    for page in range(1, max_pages + 1):
        items, total_count = get_items(
            http_client, api_key, page=page, page_size=page_size,
            sort='-originalCreatedOn,-id',
        )
        if not items:
            logger.info("embedsocial auto: page %d empty, stopping", page)
            break
        if page == 1:
            logger.info("embedsocial auto: API total_count=%d newest=%s",
                        total_count, items[0].get('originalCreatedOn', 'unknown'))

        page_review_ids = [str(item.get('id', '')) for item in items]
        existing_ids = get_existing_review_ids(config, page_review_ids)

        new_count = 0
        for item in items:
            item_id = str(item.get('id', ''))
            if item_id not in existing_ids:
                new_reviews.append(transform_review(item))
                new_count += 1

        logger.info("embedsocial auto: page %d new=%d existing=%d",
                    page, new_count, len(items) - new_count)

        if new_count == 0:
            break

    seen: Dict[str, Dict[str, Any]] = {}
    for r in new_reviews:
        seen[r['review_id']] = r
    new_reviews = list(seen.values())

    if not new_reviews:
        try:
            db_config = config.databases.get('postgresql')
            if db_config and total_count > 0:
                engine = get_engine('pbi')
                sm = SessionManager(engine)
                with sm.session_scope() as session:
                    db_count = session.query(EmbedSocialReview).count()
                if total_count > db_count:
                    logger.warning(
                        "embedsocial auto: 0 new but API total (%d) > DB count (%d). "
                        "Consider mode=backfill", total_count, db_count
                    )
        except Exception:
            logger.debug("embedsocial: API/DB compare failed", exc_info=True)
        return 0

    return push_reviews_to_database(new_reviews, config, chunk_size)


def run_diagnose(api_key: str, config, page_size: int) -> Dict[str, Any]:
    from common import HTTPClient, SessionManager, EmbedSocialReview
    from common.db import get_engine

    http_client = HTTPClient()
    items, total_count = get_items(http_client, api_key, page=1, page_size=page_size)

    info: Dict[str, Any] = {
        'api_total_count': total_count,
        'page1_items': len(items),
    }

    db_config = config.databases.get('postgresql')
    if db_config:
        engine = get_engine('pbi')
        sm = SessionManager(engine)
        try:
            with sm.session_scope() as session:
                db_count = session.query(EmbedSocialReview).count()
                newest_db = session.query(EmbedSocialReview.original_created_on).order_by(
                    EmbedSocialReview.original_created_on.desc()
                ).first()
            info['db_count'] = db_count
            info['db_newest'] = newest_db[0].isoformat() if newest_db and newest_db[0] else None
            info['gap'] = total_count - db_count
        except Exception:
            logger.debug("embedsocial diagnose: DB query failed", exc_info=True)

    if items and db_config:
        page_ids = [str(item.get('id', '')) for item in items]
        existing = get_existing_review_ids(config, page_ids)
        info['page1_new'] = len(page_ids) - len(existing)
        info['page1_existing'] = len(existing)

    logger.info("embedsocial diagnose: %s", info)
    return info


def run(mode: str = 'auto') -> Dict[str, Any]:
    from common import DataLayerConfig
    from common.config import get_pipeline_config
    from common.secrets_vault import vault_config

    config = DataLayerConfig.from_env()
    api_key = vault_config('EMBEDSOCIAL_API_KEY')
    if not api_key:
        raise ValueError("EMBEDSOCIAL_API_KEY not in vault or env")

    page_size = get_pipeline_config('embedsocial', 'page_size', 50)
    chunk_size = get_pipeline_config('embedsocial', 'sql_chunk_size', 500)
    incremental_pages = get_pipeline_config('embedsocial', 'incremental_pages', 5)

    if mode == 'backfill':
        records = run_backfill(api_key, config, page_size, chunk_size)
        return {'records': records, 'mode': mode}
    if mode == 'diagnose':
        info = run_diagnose(api_key, config, page_size)
        return {'records': 0, 'mode': mode, 'diagnose': info}

    records = run_auto(api_key, config, page_size, chunk_size, incremental_pages)
    return {'records': records, 'mode': mode}


class EmbedSocialPipeline(BasePipeline):

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        mode = scope.get('mode', 'auto')
        result = run(mode=mode)
        return RunResult(
            status='refreshed',
            records=result['records'],
            scope=scope,
            metadata=result,
        )
