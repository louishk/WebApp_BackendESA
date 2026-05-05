"""
SOAP + booking-flow helpers shared by the reservations and booking
routes. These were inlined inside `web/routes/reservations.py` until
commit b4e38eb (2026-04-17) which extracted them — but the new file
was never committed to the repo (only existed on the VM). Today's
deploy with `rsync --delete` exposed the gap.

Restored from the pre-extraction commit's content — symbols are the
public (non-underscored) names that `reservations.py` imports and
aliases to `_*` locally for the existing call sites.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# ---------------------------------------------------------------------------
# Database session — esa_pbi (analytics / SiteInfo lookup target)
# ---------------------------------------------------------------------------

_pbi_engine = None
_pbi_session_factory = None


def get_pbi_session():
    """Return a fresh session bound to esa_pbi. Lazy engine init."""
    global _pbi_engine, _pbi_session_factory
    if _pbi_engine is None:
        from common.config_loader import get_database_url
        pbi_url = get_database_url('pbi')
        _pbi_engine = create_engine(
            pbi_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=300,
        )
        _pbi_session_factory = sessionmaker(bind=_pbi_engine)
    return _pbi_session_factory()


# ---------------------------------------------------------------------------
# SOAP — CallCenterWs (SMD)
# ---------------------------------------------------------------------------

CC_NS = "http://tempuri.org/CallCenterWs/CallCenterWs"


def cc_soap_action(operation: str) -> str:
    """Compose the SOAPAction header for a CallCenterWs operation."""
    return f"{CC_NS}/{operation}"


def get_cc_soap_client():
    """Build a SOAPClient configured for CallCenterWs.asmx."""
    from common.config import DataLayerConfig
    from common.soap_client import SOAPClient

    config = DataLayerConfig.from_env()
    if not config.soap:
        raise RuntimeError("SOAP configuration not available")

    cc_url = config.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
    return SOAPClient(
        base_url=cc_url,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=config.soap.timeout,
        retries=config.soap.retries,
    )


def validate_site_code(site_code: str):
    """Look up SiteInfo by code. Returns the row or None."""
    from common.models import SiteInfo
    pbi_session = get_pbi_session()
    try:
        return pbi_session.query(SiteInfo).filter_by(SiteCode=site_code).first()
    finally:
        pbi_session.close()


# ---------------------------------------------------------------------------
# Date helpers — SMD requires non-empty ISO datetimes; never return ''
# ---------------------------------------------------------------------------

def default_date(offset_days: int) -> str:
    """ISO date string offset from today (UTC). Never empty."""
    return (datetime.now(timezone.utc).date() + timedelta(days=offset_days)).isoformat()


def parse_date(value, fallback_offset_days: int) -> str:
    """
    Validate a YYYY-MM-DD date and return it; fall back to a relative
    default if value is missing or malformed. Never empty.
    """
    if value:
        try:
            datetime.strptime(str(value), '%Y-%m-%d')
            return str(value)
        except (ValueError, TypeError):
            pass
    return default_date(fallback_offset_days)


def require_date(value) -> Tuple[Optional[str], Optional[str]]:
    """
    Validate a required date field. Returns (date_str, None) on success
    or (None, error_msg) on failure.
    """
    if not value:
        return None, 'Date is required'
    try:
        datetime.strptime(str(value), '%Y-%m-%d')
        return str(value), None
    except (ValueError, TypeError):
        return None, 'Date must be YYYY-MM-DD format'


# ---------------------------------------------------------------------------
# Input coercion + bounds
# ---------------------------------------------------------------------------

def safe_int(
    value: Any,
    default: int = 0,
    min_val: Optional[int] = None,
    max_val: Optional[int] = None,
) -> Tuple[Optional[int], Optional[str]]:
    """Coerce to int with optional bounds. Returns (int_val, None) or (None, error)."""
    try:
        result = int(value) if value is not None else default
    except (ValueError, TypeError):
        return None, 'Must be an integer'
    if min_val is not None and result < min_val:
        return None, f'Must be >= {min_val}'
    if max_val is not None and result > max_val:
        return None, f'Must be <= {max_val}'
    return result, None


def safe_rate(value: Any, default: float = 0) -> Tuple[Optional[str], Optional[str]]:
    """
    Coerce a quoted_rate to a 2-decimal string. Returns (str_val, None) or
    (None, error_msg). Range-checked 0–1,000,000.
    """
    try:
        rate = float(value) if value is not None else default
    except (ValueError, TypeError):
        return None, 'Must be a number'
    if rate < 0 or rate > 1_000_000:
        return None, 'Rate out of range (0–1000000)'
    return f"{rate:.2f}", None


def sanitize_log(value: Any) -> str:
    """Strip newlines and pipe chars before audit logging; cap at 200 chars."""
    return str(value).replace('\n', ' ').replace('\r', ' ').replace('|', '-')[:200]


def clamp(value: Any, max_len: int) -> str:
    """Truncate string to max_len. Returns '' for falsy values."""
    return str(value)[:max_len] if value else ''
