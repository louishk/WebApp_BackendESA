"""Shared helpers for CallCenterWs-backed orchestrator pipelines."""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

CALL_CENTER_WS_URL = "https://api.smdservers.net/CCWs_3.5/CallCenterWs.asmx"
NAMESPACE = "http://tempuri.org/CallCenterWs/CallCenterWs"


def build_soap_client(timeout: int = 120, retries: int = 3):
    from common.soap_client import SOAPClient
    from common.config_loader import get_config
    soap_cfg = get_config().apis.soap
    return SOAPClient(
        base_url=CALL_CENTER_WS_URL,
        corp_code=soap_cfg.corp_code,
        corp_user=soap_cfg.corp_user,
        api_key=soap_cfg.api_key_vault,
        corp_password=soap_cfg.corp_password_vault,
        timeout=timeout,
        retries=retries,
    )


def site_ids_to_codes(site_ids: List[int]) -> List[str]:
    """Query siteinfo for SiteCode where SiteID IN (site_ids)."""
    if not site_ids:
        return []
    from sync_service.config import get_engine
    from sqlalchemy import text
    with get_engine('pbi').connect() as conn:
        rows = conn.execute(
            text('SELECT "SiteCode" FROM siteinfo WHERE "SiteID" = ANY(:ids) AND "SiteCode" IS NOT NULL'),
            {'ids': list(site_ids)},
        ).fetchall()
    return [r[0] for r in rows if r[0]]


def resolve_site_codes(scope: Dict[str, Any]) -> List[str]:
    if 'site_codes' in scope:
        v = scope['site_codes']
        return list(v) if isinstance(v, (list, tuple)) else [v]
    if 'site_code' in scope:
        return [scope['site_code']]
    if 'location_codes' in scope:
        return list(scope['location_codes'])
    return []


def to_int(v) -> Optional[int]:
    if v is None or v == '':
        return None
    try:
        return int(str(v).strip())
    except (ValueError, TypeError):
        return None


def to_decimal(v) -> Optional[Decimal]:
    if v is None or v == '':
        return None
    try:
        return Decimal(str(v).strip())
    except (InvalidOperation, TypeError, ValueError):
        return None


def to_bool(v) -> Optional[bool]:
    if v is None or v == '':
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ('true', '1', 'yes', 'y', 't'):
        return True
    if s in ('false', '0', 'no', 'n', 'f'):
        return False
    return None


def to_datetime(v) -> Optional[datetime]:
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


def to_str(v, maxlen: Optional[int] = None) -> Optional[str]:
    if v is None:
        return None
    s = str(v)
    if maxlen is not None:
        s = s[:maxlen]
    return s or None


def parallel_fetch(
    fetch_fn: Callable[[str], List[Dict[str, Any]]],
    site_codes: List[str],
    max_workers: int = 6,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Fetch all sites concurrently via a thread pool.

    fetch_fn(site_code) must be thread-safe and return a list of rows.
    Returns (all_rows, per_site_counts).
    """
    all_rows: List[Dict[str, Any]] = []
    per_site: Dict[str, int] = {}
    workers = max(1, min(max_workers, len(site_codes)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(fetch_fn, sc): sc for sc in site_codes}
        for fut in as_completed(futs):
            sc = futs[fut]
            try:
                rows = fut.result() or []
            except Exception as e:
                logger.warning(f"parallel fetch {sc} raised: {e}")
                rows = []
            per_site[sc] = len(rows)
            all_rows.extend(rows)
    return all_rows, per_site


def build_upsert_sql(table: str, cols: List[str], conflict_cols: List[str], has_created_at: bool = True) -> str:
    """Build an INSERT ... ON CONFLICT upsert statement.

    Assumes the table has `updated_at` (and optionally `created_at`) columns which
    are set by the pipeline to NOW().
    """
    col_list = ', '.join(f'"{c}"' for c in cols)
    placeholders = ', '.join(f':{c}' for c in cols)
    update_list = ', '.join(
        f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in conflict_cols
    )
    conflict_list = ', '.join(f'"{c}"' for c in conflict_cols)
    if has_created_at:
        return (
            f'INSERT INTO {table} ({col_list}, created_at, updated_at) '
            f'VALUES ({placeholders}, NOW(), NOW()) '
            f'ON CONFLICT ({conflict_list}) '
            f'DO UPDATE SET {update_list}, updated_at = NOW()'
        )
    return (
        f'INSERT INTO {table} ({col_list}, updated_at) '
        f'VALUES ({placeholders}, NOW()) '
        f'ON CONFLICT ({conflict_list}) '
        f'DO UPDATE SET {update_list}, updated_at = NOW()'
    )
