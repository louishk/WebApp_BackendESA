"""
REST API routes.
Refactored from app.py to use blueprint pattern.
"""

import os
from datetime import datetime, timedelta
import pytz
from pathlib import Path
import threading

import re

from functools import wraps
from flask import Blueprint, jsonify, request, current_app, g
from sqlalchemy import desc, func, case, text

from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.rate_limit import rate_limit_api
from web.utils.validators import parse_site_ids

api_bp = Blueprint('api', __name__, url_prefix='/api')

# Timezones
SGT = pytz.timezone('Asia/Singapore')
UTC = pytz.UTC


def now_sgt():
    """Get current time in Singapore timezone (for display)."""
    return datetime.now(SGT)


def now_utc():
    """Get current time in UTC (for database storage)."""
    return datetime.now(UTC)


# =============================================================================
# Shared File-Based Response Cache (works across gunicorn workers)
# =============================================================================
import json
import hashlib
import tempfile

_CACHE_DIR = os.path.join(tempfile.gettempdir(), 'esa-api-cache')
os.makedirs(_CACHE_DIR, mode=0o700, exist_ok=True)
_cache_lock = threading.Lock()


def _cache_path(key):
    """Get file path for a cache key."""
    safe_key = hashlib.md5(key.encode()).hexdigest()
    return os.path.join(_CACHE_DIR, f'{safe_key}.json')


def cached(ttl_seconds=30):
    """
    File-based cache decorator for API responses.
    Shared across all gunicorn workers via filesystem.

    Args:
        ttl_seconds: TTL in seconds, or a callable (request) -> int for per-request TTL.
            Use the callable form to tier TTL by query params (e.g. period=30d → longer).
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            ttl = ttl_seconds(request) if callable(ttl_seconds) else ttl_seconds
            cache_key = f"{func.__name__}:{request.path}:{request.query_string.decode()}"
            path = _cache_path(cache_key)

            # Check file cache
            try:
                if os.path.exists(path):
                    mtime = os.path.getmtime(path)
                    if (datetime.now().timestamp() - mtime) < ttl:
                        with open(path, 'r') as f:
                            data = json.load(f)
                        return jsonify(data)
            except (OSError, json.JSONDecodeError):
                pass

            # Call function and cache result
            response = func(*args, **kwargs)

            # Write to file cache (owner-only permissions)
            try:
                response_data = response.get_json()
                if response_data is not None:
                    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
                    with os.fdopen(fd, 'w') as f:
                        json.dump(response_data, f)
            except (OSError, TypeError):
                pass

            return response
        return wrapper
    return decorator


def stats_ttl_by_period(req):
    """TTL for stats endpoints, scaled by period.

    Short window (1d/7d) needs to feel near-real-time. Long windows (30d/90d)
    require expensive scans of hundreds of thousands of rows and the underlying
    data shifts only by a few percent per hour — much longer TTL is fine.
    """
    period = req.args.get('period', '7d')
    return {
        '1d': 180,    # 3 min
        '7d': 600,    # 10 min
        '30d': 1800,  # 30 min
        '90d': 3600,  # 1 hour
    }.get(period, 300)


def clear_cache(pattern=None):
    """Clear cache entries."""
    try:
        for f in os.listdir(_CACHE_DIR):
            if f.endswith('.json'):
                os.remove(os.path.join(_CACHE_DIR, f))
    except OSError:
        pass


def get_session():
    """Get database session from app context."""
    return current_app.get_db_session()



def get_pbi_session():
    """Get PBI database session — delegates to the app-level shared pool."""
    from flask import current_app
    return current_app.get_pbi_session()


# =============================================================================
# Status & Health
# =============================================================================


@api_bp.route('/health')
@rate_limit_api(max_requests=30, window_seconds=60)
def health():
    """Health check endpoint with dependency probes."""
    from web.utils.health import run_health_checks
    body, status = run_health_checks()
    return jsonify(body), status














# =============================================================================
# Service Management
# =============================================================================


@api_bp.route('/services/status')
@require_auth
@require_api_scope('sync:read')
def api_services_status():
    """Get status of running services."""
    import os

    # MCP server status (via health endpoint)
    mcp_port = os.environ.get('MCP_SERVER_PORT', '8002')
    mcp_status = 'stopped'
    mcp_info = {}
    try:
        import urllib.request
        req = urllib.request.Request(f'http://127.0.0.1:{mcp_port}/health', method='GET')
        req.add_header('User-Agent', 'esa-backend-status-check')
        with urllib.request.urlopen(req, timeout=3) as resp:
            if resp.status == 200:
                mcp_data = json.loads(resp.read())
                mcp_status = 'running'
                mcp_info = {
                    'transport': mcp_data.get('transport', 'streamable-http'),
                }
    except Exception:
        mcp_status = 'stopped'

    # Orchestrator status — read directly from sync_service
    orchestrator_status = 'stopped'
    orchestrator_info = {}
    try:
        from sync_service.models import SyncPipeline, SyncServiceState
        from sync_service.config import session_scope as sync_session
        from sync_service.executor import get_executor

        with sync_session() as s:
            pipeline_count = s.query(SyncPipeline).count()
            enabled_count = s.query(SyncPipeline).filter_by(enabled=True).count()

        # In-process API means if Flask is up, orchestrator API is up
        orchestrator_status = 'running'
        exec_stats = get_executor().stats()
        orchestrator_info = {
            'pid': os.getpid(),
            'pipelines_total': pipeline_count,
            'pipelines_enabled': enabled_count,
            'in_flight': exec_stats.get('in_flight', 0),
        }
    except Exception as e:
        current_app.logger.debug(f"Orchestrator status check failed: {e}")
        orchestrator_status = 'stopped'

    # Web UI is always running if we're responding
    result = {
        'web_ui': {
            'service': 'Flask Web UI',
            'active': True,
            'status': 'running',
            'pid': os.getpid(),
        },
        'mcp_server': {
            'service': 'MCP Server',
            'active': mcp_status == 'running',
            'status': mcp_status,
            **mcp_info
        },
        'orchestrator': {
            'service': 'Sync Orchestrator',
            'active': orchestrator_status == 'running',
            'status': orchestrator_status,
            **orchestrator_info,
        },
    }

    return jsonify(result)


# Systemd units exposed for remote control via /api/services/<svc>/<action>.
# esa-backend is deliberately excluded (can't restart yourself); backend-scheduler
# stays in the allowlist only until the daemon is fully retired (PR D).
_SERVICE_ALLOWLIST = {'backend-mcp', 'backend-orchestrator', 'backend-scheduler'}
_ACTION_ALLOWLIST = {'start', 'stop', 'restart'}


@api_bp.route('/services/<service>/<action>', methods=['POST'])
@require_auth
@require_api_scope('sync:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def api_service_action(service, action):
    """Generic systemctl-backed service control.

    POST /api/services/<service>/<action>
      service: backend-mcp | backend-orchestrator | backend-scheduler
      action:  start | stop | restart
    """
    import subprocess

    if service not in _SERVICE_ALLOWLIST:
        return jsonify({'success': False, 'error': f'Unknown service: {service}'}), 400
    if action not in _ACTION_ALLOWLIST:
        return jsonify({'success': False, 'error': f'Unknown action: {action}'}), 400

    try:
        result = subprocess.run(
            ['sudo', 'systemctl', action, service],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            return jsonify({
                'success': True,
                'message': f'{service}: {action} dispatched',
            })
        current_app.logger.error(
            "systemctl %s %s failed: %s", action, service, result.stderr,
        )
        return jsonify({
            'success': False,
            'error': f'Failed to {action} {service}',
        }), 500
    except Exception:
        current_app.logger.exception(
            "Exception running systemctl %s %s", action, service,
        )
        return jsonify({
            'success': False,
            'error': f'Failed to {action} {service}',
        }), 500


# =============================================================================
# Pipeline Management
# =============================================================================


@api_bp.route('/pipelines/ownership')
@require_auth
@require_api_scope('sync:read')
def api_pipelines_ownership():
    """Cross-cutting view: who handles each pipeline (scheduler or orchestrator).

    Used by dashboards to show ownership without two separate API calls.
    """
    from common.pipeline_registry import load_ownership_map

    session = get_session()
    try:
        ownership = load_ownership_map(session)
        scheduler_count = sum(1 for v in ownership.values() if v == 'scheduler')
        sync_count = sum(1 for v in ownership.values() if v == 'orchestrator')
        return jsonify({
            'ownership': ownership,
            'totals': {
                'scheduler': scheduler_count,
                'orchestrator': sync_count,
                'total': len(ownership),
            },
        })
    finally:
        session.close()


@api_bp.route('/pipelines/<name>/transfer', methods=['POST'])
@require_auth
@require_api_scope('sync:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def api_transfer_pipeline(name):
    """Transfer a pipeline between scheduler and sync orchestrator.

    Body: {"managed_by": "scheduler" | "orchestrator"}

    Atomic ownership flip — the engine that loses the pipeline stops running it
    on its next config reload, and the new owner picks it up.
    """
    from common.pipeline_registry import transfer_pipeline, VALID_OWNERS

    data = request.get_json() or {}
    new_owner = data.get('managed_by')
    if new_owner not in VALID_OWNERS:
        return jsonify({'error': f'managed_by must be one of {list(VALID_OWNERS)}'}), 400

    session = get_session()
    try:
        ok = transfer_pipeline(session, name, new_owner)
        if not ok:
            return jsonify({'error': 'Pipeline not found'}), 404
        session.commit()
        current_app.logger.info(
            f"Pipeline transferred: name={name} new_owner={new_owner}"
        )
        return jsonify({
            'success': True,
            'pipeline': name,
            'managed_by': new_owner,
        })
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Failed to transfer pipeline {name}: {e}")
        return jsonify({'error': 'Failed to transfer pipeline'}), 500
    finally:
        session.close()


@api_bp.route('/modules')
@require_auth
@require_api_scope('sync:read')
def api_list_modules():
    """List available Python modules in datalayer."""
    datalayer_path = Path(__file__).parent.parent.parent / 'datalayer'
    modules = []

    if datalayer_path.exists():
        for f in datalayer_path.glob('*.py'):
            if f.name.startswith('_'):
                continue
            module_name = f.stem
            modules.append({
                'name': module_name,
                'path': f'datalayer.{module_name}',
                'file': str(f.name)
            })

    return jsonify({'modules': modules})


# =============================================================================
# Billing Day Management
# =============================================================================

@api_bp.route('/billing-day/<int:site_id>')
@require_auth
@require_api_scope('sync:read')
@cached(ttl_seconds=300)
def api_get_billing_day_status(site_id):
    """
    Get billing day status for all rented units at a site.
    Highlights tenants NOT on 1st of month billing.
    Only includes active tenants (rented, valid ledger, paid within last year).
    """
    from common.models import RentRoll, SiteInfo

    session = get_pbi_session()
    try:
        # Get site info
        site = session.query(SiteInfo).filter_by(SiteID=site_id).first()
        if not site:
            return jsonify({'error': 'Site not found'}), 404

        # Get latest extract_date for this site
        latest_date = session.query(func.max(RentRoll.extract_date)).filter(
            RentRoll.SiteID == site_id
        ).scalar()

        if not latest_date:
            return jsonify({
                'site_id': site_id,
                'site_code': site.SiteCode,
                'site_name': site.Name,
                'extract_date': None,
                'total_tenants': 0,
                'on_first': 0,
                'not_on_first': 0,
                'ledgers': []
            })

        # Calculate cutoff date (1 year ago) to filter out inactive tenants
        one_year_ago = datetime.now() - timedelta(days=365)

        # Query active tenants only:
        # - Unit is rented
        # - Has valid ledger and tenant IDs
        # - PaidThru within last year (excludes moved-out/inactive tenants)
        rentroll_data = session.query(RentRoll).filter(
            RentRoll.extract_date == latest_date,
            RentRoll.SiteID == site_id,
            RentRoll.bRented == True,
            RentRoll.LedgerID.isnot(None),
            RentRoll.TenantID.isnot(None),
            RentRoll.dPaidThru >= one_year_ago
        ).order_by(
            RentRoll.iAnnivDays,
            RentRoll.sUnit
        ).all()

        # Build response
        ledgers = []
        on_first = 0
        not_on_first = 0

        for rr in rentroll_data:
            billing_day = rr.iAnnivDays
            needs_conversion = billing_day != 1 if billing_day is not None else False

            if billing_day == 1:
                on_first += 1
            else:
                not_on_first += 1

            ledgers.append({
                'LedgerID': rr.LedgerID,
                'TenantID': rr.TenantID,
                'UnitID': rr.UnitID,
                'UnitName': rr.sUnit,
                'TenantName': rr.sTenant,
                'Company': rr.sCompany,
                'Rent': float(rr.dcRent) if rr.dcRent else None,
                'BillingDay': billing_day,
                'PaidThruDate': rr.dPaidThru.isoformat() if rr.dPaidThru else None,
                'NeedsConversion': needs_conversion
            })

        return jsonify({
            'site_id': site_id,
            'site_code': site.SiteCode,
            'site_name': site.Name,
            'extract_date': latest_date.isoformat() if latest_date else None,
            'total_tenants': len(ledgers),
            'on_first': on_first,
            'not_on_first': not_on_first,
            'ledgers': ledgers
        })

    finally:
        session.close()


@api_bp.route('/billing-day/update', methods=['POST'])
@require_auth
@require_api_scope('sync:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_update_billing_day():
    """
    Update billing day for a ledger via SOAP API.
    Supports preview (commit: false) and commit (commit: true) modes.
    """
    from common.config import DataLayerConfig
    from common.soap_client import SOAPClient, SOAPFaultError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    # Validate required fields
    site_code = data.get('site_code')
    ledger_id = data.get('ledger_id')
    billing_day = data.get('billing_day')
    commit = data.get('commit', False)

    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if ledger_id is None:
        return jsonify({'error': 'ledger_id is required'}), 400
    if billing_day is None:
        return jsonify({'error': 'billing_day is required'}), 400

    # Validate types
    try:
        ledger_id = int(ledger_id)
    except (ValueError, TypeError):
        return jsonify({'error': 'ledger_id must be an integer'}), 400

    try:
        billing_day = int(billing_day)
    except (ValueError, TypeError):
        return jsonify({'error': 'billing_day must be an integer'}), 400

    # Validate billing_day range
    if billing_day < 1 or billing_day > 31:
        return jsonify({'error': 'billing_day must be between 1 and 31'}), 400

    # Validate commit is boolean
    if not isinstance(commit, bool):
        return jsonify({'error': 'commit must be a boolean'}), 400

    # Validate ledger belongs to an active tenant
    from common.models import RentRoll, SiteInfo
    session = get_pbi_session()
    try:
        # Get site by code
        site = session.query(SiteInfo).filter_by(SiteCode=site_code).first()
        if not site:
            return jsonify({'error': f'Site not found: {site_code}'}), 404

        # Get latest extract_date for this site
        latest_date = session.query(func.max(RentRoll.extract_date)).filter(
            RentRoll.SiteID == site.SiteID
        ).scalar()

        if latest_date:
            one_year_ago = datetime.now() - timedelta(days=365)

            # Check if ledger exists and is active
            ledger_record = session.query(RentRoll).filter(
                RentRoll.extract_date == latest_date,
                RentRoll.SiteID == site.SiteID,
                RentRoll.LedgerID == ledger_id
            ).first()

            if not ledger_record:
                return jsonify({'error': f'Ledger {ledger_id} not found at site {site_code}'}), 404

            if not ledger_record.bRented:
                return jsonify({'error': f'Ledger {ledger_id} is not currently rented'}), 400

            if ledger_record.dPaidThru and ledger_record.dPaidThru < one_year_ago:
                return jsonify({
                    'error': f'Ledger {ledger_id} appears inactive (PaidThru: {ledger_record.dPaidThru.date()})',
                    'hint': 'Cannot update billing day for moved-out or inactive tenants'
                }), 400
    finally:
        session.close()

    # Get SOAP config
    config = DataLayerConfig.from_env()
    if not config.soap:
        current_app.logger.error("SOAP configuration not available")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    # Build CallCenterWs URL from base_url
    cc_url = config.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')

    # Initialize SOAP client
    soap_client = SOAPClient(
        base_url=cc_url,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=config.soap.timeout,
        retries=config.soap.retries
    )

    try:
        # Call LedgerBillingDayUpdate SOAP API
        results = soap_client.call(
            operation="LedgerBillingDayUpdate",
            parameters={
                "sLocationCode": site_code,
                "iLedgerID": ledger_id,
                "iBillingDay": billing_day,
                "bUpdateFlag": str(commit).lower(),
            },
            soap_action="http://tempuri.org/CallCenterWs/CallCenterWs/LedgerBillingDayUpdate",
            namespace="http://tempuri.org/CallCenterWs/CallCenterWs",
            result_tag="Table",
        )

        return jsonify({
            'success': True,
            'mode': 'commit' if commit else 'preview',
            'site_code': site_code,
            'ledger_id': ledger_id,
            'billing_day': billing_day,
            'charges': results
        })

    except SOAPFaultError as e:
        current_app.logger.error(f"SOAP fault during billing day update: {e}")
        return jsonify({
            'success': False,
            'error': 'SOAP API error',
            'details': 'SOAP API error occurred'
        }), 502

    except Exception as e:
        current_app.logger.error(f"Unexpected error during billing day update: {e}")
        return jsonify({
            'success': False,
            'error': 'Unexpected error',
            'details': 'An internal error occurred'
        }), 500

    finally:
        soap_client.close()


# =============================================================================
# Sites List
# =============================================================================

@api_bp.route('/sites')
@require_auth
@require_api_scope('inventory:read')
@cached(ttl_seconds=300)
def api_list_sites():
    """List all sites for dropdown selection."""
    from common.models import SiteInfo

    session = get_pbi_session()
    try:
        sites = session.query(SiteInfo).order_by(SiteInfo.Country, SiteInfo.SiteCode).all()
        return jsonify({
            'sites': [
                {
                    'site_id': s.SiteID,
                    'site_code': s.SiteCode,
                    'name': s.Name,
                    'country': s.Country
                }
                for s in sites
            ]
        })
    finally:
        session.close()


# Process-level cache for the all-sites list. Refreshed every 5 min.
# Keeps the dropdown working when the PBI DB is briefly saturated on
# connection slots (we've seen "remaining connection slots are reserved for
# roles with the SUPERUSER attribute" on Azure under load).
_ALL_SITES_CACHE: dict = {'fetched_at': 0.0, 'sites': None}
_ALL_SITES_TTL = 300


def _get_all_sites_cached():
    import time
    now = time.time()
    if _ALL_SITES_CACHE['sites'] is not None and (now - _ALL_SITES_CACHE['fetched_at']) < _ALL_SITES_TTL:
        return _ALL_SITES_CACHE['sites'], None
    from common.models import SiteInfo
    try:
        session = get_pbi_session()
        try:
            rows = session.query(SiteInfo).order_by(SiteInfo.Country, SiteInfo.SiteCode).all()
            sites = [
                {'site_id': s.SiteID, 'site_code': s.SiteCode, 'name': s.Name, 'country': s.Country}
                for s in rows
            ]
        finally:
            session.close()
        _ALL_SITES_CACHE['sites'] = sites
        _ALL_SITES_CACHE['fetched_at'] = now
        return sites, None
    except Exception as e:
        current_app.logger.warning(f"site list fetch failed: {e}")
        # On failure, fall back to whatever's in the cache. None if cold.
        return _ALL_SITES_CACHE.get('sites'), str(e)


@api_bp.route('/me/sites')
def api_me_sites():
    """Caller's effective allowed sites. Empty allowed_site_ids = all sites."""
    from flask_login import current_user
    if not current_user.is_authenticated:
        return jsonify({'error': 'Authentication required'}), 401

    all_sites, err = _get_all_sites_cached()
    if all_sites is None:
        # Cold cache + DB failure — bubble a real error so the frontend can
        # display something more useful than an empty dropdown.
        return jsonify({'error': 'Site list temporarily unavailable', 'sites': []}), 503

    # Semantics: NULL = unrestricted, [] = blocked, [...] = restricted.
    if current_user.allowed_site_ids is None:
        return jsonify({'sites': all_sites, 'unrestricted': True})
    s = set(current_user.allowed_site_ids)
    return jsonify({
        'sites': [x for x in all_sites if x['site_id'] in s],
        'unrestricted': False,
    })


# =============================================================================
# API Statistics - Consumption Monitoring
# =============================================================================


def _build_volume_timeline(period, db_rows):
    """
    Build a full timeline with all expected time slots for the given period.
    Fills gaps with count=0. All timestamps in Asia/Singapore.

    Args:
        period: '1d', '7d', '30d', '90d'
        db_rows: list of (bucket_datetime, count) from DB query

    Returns:
        (time_unit, list of {'date': iso_str, 'count': int})
    """
    now_sg = datetime.now(SGT)

    if period == '1d':
        unit = 'hour'
        # Start from the current hour, go back 24 hours
        current_hour = now_sg.replace(minute=0, second=0, microsecond=0)
        slots = [current_hour - timedelta(hours=i) for i in range(24)]
        slots.reverse()
    else:
        unit = 'day'
        days_count = {'7d': 7, '30d': 30, '90d': 90}.get(period, 7)
        current_day = now_sg.replace(hour=0, minute=0, second=0, microsecond=0)
        slots = [current_day - timedelta(days=i) for i in range(days_count)]
        slots.reverse()

    # Index DB rows by their bucket (already in SGT from the query)
    data_map = {}
    for row in db_rows:
        if row.bucket:
            b = row.bucket
            # DB returns naive SGT (via double timezone conversion)
            if b.tzinfo is not None:
                b = b.replace(tzinfo=None)
            if unit == 'hour':
                key = b.replace(minute=0, second=0, microsecond=0)
            else:
                key = b.replace(hour=0, minute=0, second=0, microsecond=0)
            data_map[key] = row.count

    # Merge
    result = []
    for slot in slots:
        key = slot.replace(tzinfo=None)
        result.append({
            'date': slot.isoformat(),
            'count': data_map.get(key, 0),
        })

    return unit, result

@api_bp.route('/statistics/summary')
@require_auth
@require_api_scope('statistics:read')
@rate_limit_api(max_requests=30, window_seconds=60)
@cached(ttl_seconds=stats_ttl_by_period)
def api_statistics_summary():
    """
    Overall API consumption summary.
    Query params:
        period: 1d, 7d, 30d, 90d (default 7d)
        classification: internal | probes | all (default internal).
            internal = status_code != 404 (real endpoints)
            probes   = status_code == 404 (bot/scanner traffic on routes that don't exist)
    """
    from web.models.api_statistic import ApiStatistic

    period = request.args.get('period', '7d')
    classification = request.args.get('classification', 'internal')
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    session = get_session()
    try:
        # Single roundtrip: total + avg + error_count in one aggregate query.
        agg_q = session.query(
            func.count(ApiStatistic.id).label('total'),
            func.avg(ApiStatistic.response_time_ms).label('avg_ms'),
            func.sum(case((ApiStatistic.status_code >= 400, 1), else_=0)).label('errors'),
        ).filter(ApiStatistic.called_at >= since)
        agg = _apply_classification(agg_q, classification).one()
        total_calls = int(agg.total or 0)
        avg_response = float(agg.avg_ms or 0)
        error_count = int(agg.errors or 0)

        # Calls per time bucket in SGT (hourly for 24h, daily otherwise)
        trunc_unit = 'hour' if period == '1d' else 'day'
        sgt_time = func.timezone('Asia/Singapore', func.timezone('UTC', ApiStatistic.called_at))
        volume_q = session.query(
            func.date_trunc(trunc_unit, sgt_time).label('bucket'),
            func.count(ApiStatistic.id).label('count')
        ).filter(
            ApiStatistic.called_at >= since
        )
        volume_q = _apply_classification(volume_q, classification)
        volume = volume_q.group_by('bucket').order_by('bucket').all()

        time_unit, timeline = _build_volume_timeline(period, volume)

        return jsonify({
            'period': period,
            'classification': classification,
            'since': since.isoformat(),
            'total_calls': total_calls,
            'avg_response_time_ms': round(avg_response, 2),
            'error_count': error_count,
            'error_rate': round(error_count / total_calls * 100, 2) if total_calls > 0 else 0,
            'time_unit': time_unit,
            'calls_per_day': timeline,
        })
    finally:
        session.close()


def _apply_classification(query, classification):
    """Filter ApiStatistic query by Internal (200/2xx/5xx on real routes) vs Probes (404s)."""
    from web.models.api_statistic import ApiStatistic
    if classification == 'probes':
        return query.filter(ApiStatistic.status_code == 404)
    if classification == 'internal':
        return query.filter(ApiStatistic.status_code != 404)
    return query


@api_bp.route('/statistics/endpoints')
@require_auth
@require_api_scope('statistics:read')
@rate_limit_api(max_requests=30, window_seconds=60)
@cached(ttl_seconds=stats_ttl_by_period)
def api_statistics_endpoints():
    """
    Per-endpoint breakdown of API consumption.
    Query params:
        period: 1d, 7d, 30d, 90d (default 7d)
        sort: calls, avg_time, errors (default calls)
        classification: internal | probes | all (default internal)
    """
    from web.models.api_statistic import ApiStatistic

    period = request.args.get('period', '7d')
    sort_by = request.args.get('sort', 'calls')
    classification = request.args.get('classification', 'internal')
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    session = get_session()
    try:
        q = session.query(
            ApiStatistic.endpoint,
            ApiStatistic.method,
            func.count(ApiStatistic.id).label('total_calls'),
            func.avg(ApiStatistic.response_time_ms).label('avg_response_ms'),
            func.max(ApiStatistic.response_time_ms).label('max_response_ms'),
            func.sum(case((ApiStatistic.status_code >= 400, 1), else_=0)).label('error_count'),
        ).filter(
            ApiStatistic.called_at >= since
        )
        q = _apply_classification(q, classification)
        stats = q.group_by(
            ApiStatistic.endpoint, ApiStatistic.method
        ).all()

        endpoints = []
        for s in stats:
            total = s.total_calls
            errors = int(s.error_count or 0)
            endpoints.append({
                'endpoint': s.endpoint,
                'method': s.method,
                'total_calls': total,
                'avg_response_ms': round(float(s.avg_response_ms or 0), 2),
                'max_response_ms': round(float(s.max_response_ms or 0), 2),
                'error_count': errors,
                'error_rate': round(errors / total * 100, 2) if total > 0 else 0,
            })

        # Sort
        sort_key = {
            'calls': lambda x: x['total_calls'],
            'avg_time': lambda x: x['avg_response_ms'],
            'errors': lambda x: x['error_count'],
        }.get(sort_by, lambda x: x['total_calls'])

        endpoints.sort(key=sort_key, reverse=True)

        return jsonify({
            'period': period,
            'classification': classification,
            'since': since.isoformat(),
            'endpoints': endpoints,
        })
    finally:
        session.close()


@api_bp.route('/statistics/timeline')
@require_auth
@require_api_scope('statistics:read')
@rate_limit_api(max_requests=30, window_seconds=60)
@cached(ttl_seconds=30)
def api_statistics_timeline():
    """
    Hourly call volume timeline for a given period.
    Query params:
        period: 1d, 7d, 30d (default 7d)
        endpoint: filter to specific endpoint (optional)
    """
    from web.models.api_statistic import ApiStatistic

    period = request.args.get('period', '7d')
    endpoint_filter = request.args.get('endpoint')
    days = {'1d': 1, '7d': 7, '30d': 30}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    # Use hourly buckets for <=7d, daily for longer
    bucket = 'hour' if days <= 7 else 'day'

    session = get_session()
    try:
        query = session.query(
            func.date_trunc(bucket, ApiStatistic.called_at).label('bucket'),
            func.count(ApiStatistic.id).label('count'),
            func.avg(ApiStatistic.response_time_ms).label('avg_ms'),
            func.sum(case((ApiStatistic.status_code >= 400, 1), else_=0)).label('errors'),
        ).filter(
            ApiStatistic.called_at >= since
        )

        if endpoint_filter:
            query = query.filter(ApiStatistic.endpoint == endpoint_filter)

        query = query.group_by('bucket').order_by('bucket')
        results = query.all()

        return jsonify({
            'period': period,
            'bucket_size': bucket,
            'since': since.isoformat(),
            'endpoint_filter': endpoint_filter,
            'timeline': [
                {
                    'timestamp': r.bucket.isoformat() if r.bucket else None,
                    'calls': r.count,
                    'avg_response_ms': round(float(r.avg_ms or 0), 2),
                    'errors': int(r.errors or 0),
                }
                for r in results
            ],
        })
    finally:
        session.close()


@api_bp.route('/statistics/top-consumers')
@require_auth
@require_api_scope('statistics:read')
@rate_limit_api(max_requests=30, window_seconds=60)
@cached(ttl_seconds=stats_ttl_by_period)
def api_statistics_top_consumers():
    """
    Top API consumers by client IP.
    Query params:
        period: 1d, 7d, 30d (default 7d)
        limit: number of results (default 20)
        classification: internal | probes | all (default internal)
    """
    from web.models.api_statistic import ApiStatistic

    period = request.args.get('period', '7d')
    classification = request.args.get('classification', 'internal')
    try:
        limit = min(int(request.args.get('limit', 20)), 100)
    except (ValueError, TypeError):
        limit = 20
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    session = get_session()
    try:
        q = session.query(
            ApiStatistic.client_ip,
            func.count(ApiStatistic.id).label('total_calls'),
            func.count(func.distinct(ApiStatistic.endpoint)).label('unique_endpoints'),
            func.avg(ApiStatistic.response_time_ms).label('avg_ms'),
        ).filter(
            ApiStatistic.called_at >= since
        )
        q = _apply_classification(q, classification)
        stats = q.group_by(
            ApiStatistic.client_ip
        ).order_by(
            desc(func.count(ApiStatistic.id))
        ).limit(limit).all()

        return jsonify({
            'period': period,
            'since': since.isoformat(),
            'consumers': [
                {
                    'client_ip': s.client_ip,
                    'total_calls': s.total_calls,
                    'unique_endpoints': s.unique_endpoints,
                    'avg_response_ms': round(float(s.avg_ms or 0), 2),
                }
                for s in stats
            ],
        })
    finally:
        session.close()


@api_bp.route('/statistics/slow-endpoints')
@require_auth
@require_api_scope('statistics:read')
@rate_limit_api(max_requests=30, window_seconds=60)
@cached(ttl_seconds=stats_ttl_by_period)
def api_statistics_slow_endpoints():
    """
    Endpoints ranked by response time (identifies performance bottlenecks).
    Query params:
        period: 1d, 7d, 30d (default 7d)
        min_calls: minimum call count to include (default 5)
        classification: internal | probes | all (default internal)
    """
    from web.models.api_statistic import ApiStatistic

    period = request.args.get('period', '7d')
    classification = request.args.get('classification', 'internal')
    try:
        min_calls = int(request.args.get('min_calls', 5))
    except (ValueError, TypeError):
        min_calls = 5
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    session = get_session()
    try:
        q = session.query(
            ApiStatistic.endpoint,
            ApiStatistic.method,
            func.count(ApiStatistic.id).label('total_calls'),
            func.avg(ApiStatistic.response_time_ms).label('avg_ms'),
            func.percentile_cont(0.95).within_group(
                ApiStatistic.response_time_ms
            ).label('p95_ms'),
            func.max(ApiStatistic.response_time_ms).label('max_ms'),
        ).filter(
            ApiStatistic.called_at >= since
        )
        q = _apply_classification(q, classification)
        stats = q.group_by(
            ApiStatistic.endpoint, ApiStatistic.method
        ).having(
            func.count(ApiStatistic.id) >= min_calls
        ).order_by(
            desc(func.avg(ApiStatistic.response_time_ms))
        ).all()

        return jsonify({
            'period': period,
            'since': since.isoformat(),
            'min_calls': min_calls,
            'endpoints': [
                {
                    'endpoint': s.endpoint,
                    'method': s.method,
                    'total_calls': s.total_calls,
                    'avg_response_ms': round(float(s.avg_ms or 0), 2),
                    'p95_response_ms': round(float(s.p95_ms or 0), 2),
                    'max_response_ms': round(float(s.max_ms or 0), 2),
                }
                for s in stats
            ],
        })
    finally:
        session.close()


# =============================================================================
# External API Statistics - Outbound Call Monitoring
# =============================================================================

@api_bp.route('/statistics/external/summary')
@require_auth
@require_api_scope('statistics:read')
@rate_limit_api(max_requests=30, window_seconds=60)
@cached(ttl_seconds=stats_ttl_by_period)
def api_ext_statistics_summary():
    """
    Outbound API call summary.
    Query params: period (1d, 7d, 30d, 90d)
    """
    from web.models.external_api_statistic import ExternalApiStatistic

    period = request.args.get('period', '7d')
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    # Run the 3 independent aggregates concurrently — they all scan the same
    # rows but produce different groupings. For 30d (~400k rows) this drops
    # wall time from the sum (~18 s) to the slowest single query (~11 s).
    # Use common.db.get_session (thread-safe) — Flask's current_app proxy can't
    # cross thread boundaries.
    from concurrent.futures import ThreadPoolExecutor
    from common.db import get_session as _get_db_session

    trunc_unit = 'hour' if period == '1d' else 'day'

    def _q_aggregate():
        s = _get_db_session('backend')
        try:
            return s.execute(text("""
                SELECT COUNT(*) AS total,
                       AVG(response_time_ms) AS avg_ms,
                       SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS errors
                FROM external_api_statistics
                WHERE called_at >= :since
            """), {'since': since}).fetchone()
        finally:
            s.close()

    def _q_by_service():
        s = _get_db_session('backend')
        try:
            return s.execute(text("""
                SELECT service_name,
                       COUNT(*) AS n,
                       AVG(response_time_ms) AS avg_ms,
                       SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS errors
                FROM external_api_statistics
                WHERE called_at >= :since
                GROUP BY service_name
            """), {'since': since}).fetchall()
        finally:
            s.close()

    def _q_volume():
        s = _get_db_session('backend')
        try:
            return s.execute(text("""
                SELECT date_trunc(:trunc, timezone('Asia/Singapore', timezone('UTC', called_at))) AS bucket,
                       COUNT(*) AS count
                FROM external_api_statistics
                WHERE called_at >= :since
                GROUP BY bucket
                ORDER BY bucket
            """), {'since': since, 'trunc': trunc_unit}).fetchall()
        finally:
            s.close()

    with ThreadPoolExecutor(max_workers=3) as pool:
        f_agg = pool.submit(_q_aggregate)
        f_svc = pool.submit(_q_by_service)
        f_vol = pool.submit(_q_volume)
        agg = f_agg.result()
        by_service = f_svc.result()
        volume = f_vol.result()

    total = int(agg.total or 0)
    avg_ms = float(agg.avg_ms or 0)
    error_count = int(agg.errors or 0)

    time_unit, timeline = _build_volume_timeline(period, volume)

    return jsonify({
        'period': period,
        'since': since.isoformat(),
        'total_calls': total,
        'avg_response_time_ms': round(float(avg_ms), 2),
        'error_count': error_count,
        'error_rate': round(error_count / total * 100, 2) if total > 0 else 0,
        'time_unit': time_unit,
        'by_service': [
            {
                'service_name': s.service_name,
                'total_calls': int(s.n or 0),
                'avg_response_ms': round(float(s.avg_ms or 0), 2),
                'error_count': int(s.errors or 0),
            }
            for s in by_service
        ],
        'calls_per_day': timeline,
    })


@api_bp.route('/statistics/external/services')
@require_auth
@require_api_scope('statistics:read')
@rate_limit_api(max_requests=30, window_seconds=60)
@cached(ttl_seconds=stats_ttl_by_period)
def api_ext_statistics_services():
    """
    Per-service/endpoint breakdown of outbound API calls.
    Query params:
        period: 1d, 7d, 30d, 90d (default 7d)
        service: filter to specific service (optional)
    """
    from web.models.external_api_statistic import ExternalApiStatistic

    period = request.args.get('period', '7d')
    service = request.args.get('service')
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    session = get_session()
    try:
        query = session.query(
            ExternalApiStatistic.service_name,
            ExternalApiStatistic.endpoint,
            ExternalApiStatistic.method,
            func.count(ExternalApiStatistic.id).label('total_calls'),
            func.avg(ExternalApiStatistic.response_time_ms).label('avg_ms'),
            func.max(ExternalApiStatistic.response_time_ms).label('max_ms'),
            func.sum(case((ExternalApiStatistic.success == False, 1), else_=0)).label('errors'),
        ).filter(
            ExternalApiStatistic.called_at >= since
        )

        if service:
            query = query.filter(ExternalApiStatistic.service_name == service)

        query = query.group_by(
            ExternalApiStatistic.service_name,
            ExternalApiStatistic.endpoint,
            ExternalApiStatistic.method,
        ).order_by(func.count(ExternalApiStatistic.id).desc())

        results = query.all()

        return jsonify({
            'period': period,
            'service_filter': service,
            'endpoints': [
                {
                    'service_name': r.service_name,
                    'endpoint': r.endpoint,
                    'method': r.method,
                    'total_calls': r.total_calls,
                    'avg_response_ms': round(float(r.avg_ms or 0), 2),
                    'max_response_ms': round(float(r.max_ms or 0), 2),
                    'error_count': int(r.errors or 0),
                    'error_rate': round(int(r.errors or 0) / r.total_calls * 100, 2) if r.total_calls > 0 else 0,
                }
                for r in results
            ],
        })
    finally:
        session.close()


# =============================================================================
# MCP Tool Statistics - MCP server call monitoring
# =============================================================================

@api_bp.route('/statistics/mcp/summary')
@require_auth
@require_api_scope('statistics:read')
@rate_limit_api(max_requests=30, window_seconds=60)
@cached(ttl_seconds=stats_ttl_by_period)
def api_mcp_statistics_summary():
    """
    MCP tool call summary.
    Query params: period (1d, 7d, 30d, 90d)
    """
    period = request.args.get('period', '7d')
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    session = get_session()
    try:
        trunc_unit = 'hour' if period == '1d' else 'day'
        result = session.execute(text("""
            SELECT
                COUNT(*) AS total_calls,
                AVG(response_time_ms) AS avg_response_ms,
                SUM(CASE WHEN is_error THEN 1 ELSE 0 END) AS error_count
            FROM mcp_tool_statistics
            WHERE called_at >= :since
        """), {'since': since}).fetchone()

        total = int(result.total_calls or 0)
        avg_ms = float(result.avg_response_ms or 0)
        error_count = int(result.error_count or 0)

        volume_rows = session.execute(text("""
            SELECT
                date_trunc(:trunc_unit,
                    timezone('Asia/Singapore', timezone('UTC', called_at))
                ) AS bucket,
                COUNT(*) AS count
            FROM mcp_tool_statistics
            WHERE called_at >= :since
            GROUP BY bucket
            ORDER BY bucket
        """), {'trunc_unit': trunc_unit, 'since': since}).fetchall()

        time_unit, timeline = _build_volume_timeline(period, volume_rows)

        return jsonify({
            'period': period,
            'since': since.isoformat(),
            'total_calls': total,
            'avg_response_time_ms': round(avg_ms, 2),
            'error_count': error_count,
            'error_rate': round(error_count / total * 100, 2) if total > 0 else 0,
            'time_unit': time_unit,
            'calls_per_day': timeline,
        })
    finally:
        session.close()


@api_bp.route('/statistics/mcp/tools')
@require_auth
@require_api_scope('statistics:read')
@rate_limit_api(max_requests=30, window_seconds=60)
@cached(ttl_seconds=stats_ttl_by_period)
def api_mcp_statistics_tools():
    """
    Per-tool breakdown of MCP calls.
    Query params:
        period: 1d, 7d, 30d, 90d (default 7d)
        sort: calls, avg_time, errors (default calls)
    """
    period = request.args.get('period', '7d')
    sort_by = request.args.get('sort', 'calls')
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    session = get_session()
    try:
        rows = session.execute(text("""
            SELECT
                tool_name,
                COUNT(*) AS total_calls,
                AVG(response_time_ms) AS avg_response_ms,
                MAX(response_time_ms) AS max_response_ms,
                SUM(CASE WHEN is_error THEN 1 ELSE 0 END) AS error_count
            FROM mcp_tool_statistics
            WHERE called_at >= :since
            GROUP BY tool_name
        """), {'since': since}).fetchall()

        def _category(name):
            if name.startswith('DB_'):
                return 'Database'
            if name.startswith('GA_'):
                return 'Google Ads'
            return 'Health'

        tools = []
        for r in rows:
            total = int(r.total_calls or 0)
            errors = int(r.error_count or 0)
            tools.append({
                'tool_name': r.tool_name,
                'category': _category(r.tool_name),
                'total_calls': total,
                'avg_response_ms': round(float(r.avg_response_ms or 0), 2),
                'max_response_ms': round(float(r.max_response_ms or 0), 2),
                'error_count': errors,
                'error_rate': round(errors / total * 100, 2) if total > 0 else 0,
            })

        sort_key = {
            'calls': lambda x: x['total_calls'],
            'avg_time': lambda x: x['avg_response_ms'],
            'errors': lambda x: x['error_count'],
        }.get(sort_by, lambda x: x['total_calls'])
        tools.sort(key=sort_key, reverse=True)

        return jsonify({
            'period': period,
            'since': since.isoformat(),
            'tools': tools,
        })
    finally:
        session.close()


@api_bp.route('/statistics/mcp/users')
@require_auth
@require_api_scope('statistics:read')
@rate_limit_api(max_requests=30, window_seconds=60)
@cached(ttl_seconds=stats_ttl_by_period)
def api_mcp_statistics_users():
    """
    Per-user breakdown of MCP calls.
    Query params: period (1d, 7d, 30d, 90d)
    """
    period = request.args.get('period', '7d')
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    session = get_session()
    try:
        rows = session.execute(text("""
            SELECT
                username,
                key_id,
                COUNT(*) AS total_calls,
                COUNT(DISTINCT tool_name) AS tools_used,
                AVG(response_time_ms) AS avg_response_ms,
                SUM(CASE WHEN is_error THEN 1 ELSE 0 END) AS error_count
            FROM mcp_tool_statistics
            WHERE called_at >= :since
            GROUP BY username, key_id
            ORDER BY COUNT(*) DESC
        """), {'since': since}).fetchall()

        return jsonify({
            'period': period,
            'since': since.isoformat(),
            'users': [
                {
                    'username': r.username,
                    'key_id': r.key_id,
                    'total_calls': int(r.total_calls or 0),
                    'tools_used': int(r.tools_used or 0),
                    'avg_response_ms': round(float(r.avg_response_ms or 0), 2),
                    'error_count': int(r.error_count or 0),
                }
                for r in rows
            ],
        })
    finally:
        session.close()


# =============================================================================
# Discount Plans API — External-facing REST endpoints
# =============================================================================
# Usable by external systems (chatbot, website, partner integrations)
# via JWT Bearer token or session auth.
#
# GET  /api/discount-plans            — list all active plans
# GET  /api/discount-plans/<id>       — get single plan by id
# GET  /api/discount-plans/by-name/<name> — get single plan by name
# POST /api/discount-plans            — create a new plan (admin only)
# PUT  /api/discount-plans/<id>       — update a plan (admin only)
# =============================================================================

def _get_discount_plan_model():
    """Lazy import to avoid circular imports."""
    from web.models.discount_plan import DiscountPlan
    return DiscountPlan


def _apply_plan_json(plan, data, is_create=False):
    """
    Apply JSON payload fields to a DiscountPlan instance.
    Only touches fields that are present in the payload (PATCH semantics).
    """
    SIMPLE_STR_FIELDS = [
        'plan_type', 'plan_name',
        'notes', 'objective',
        'period_range', 'move_in_range', 'lock_in_period',
        'discount_value', 'discount_type', 'discount_segmentation',
        'clawback_condition',
        'deposit', 'payment_terms', 'termination_notice', 'extra_offer',
        'chatbot_notes',
        'switch_to_us', 'referral_program',
        'distribution_channel',
        'rate_rules', 'rate_rules_sites',
        'collateral_url', 'registration_flow',
    ]
    BOOL_FIELDS = ['hidden_rate', 'discount_perpetual', 'available_for_chatbot', 'is_active']
    JSONB_FIELDS = [
        'applicable_sites', 'offers', 'terms_conditions', 'terms_conditions_cn',
        'terms_conditions_translations',
        'promotion_codes', 'department_notes',
        'linked_concessions',
    ]

    for field in SIMPLE_STR_FIELDS:
        if field in data:
            setattr(plan, field, data[field])

    for field in BOOL_FIELDS:
        if field in data:
            setattr(plan, field, bool(data[field]))

    for field in JSONB_FIELDS:
        if field in data:
            setattr(plan, field, data[field])

    if 'discount_numeric' in data:
        val = data['discount_numeric']
        plan.discount_numeric = float(val) if val is not None else None

    if 'sort_order' in data:
        plan.sort_order = int(data['sort_order'])

    # Date fields
    from datetime import date as date_cls
    for field in ('period_start', 'period_end', 'promo_period_start', 'promo_period_end',
                  'booking_period_start', 'booking_period_end'):
        if field in data:
            val = data[field]
            if val and isinstance(val, str):
                setattr(plan, field, date_cls.fromisoformat(val))
            else:
                setattr(plan, field, None)


def _validate_discount_plan_data(data, exclude_id=None):
    """
    Validate discount plan JSON payload.
    Returns (errors: list[str]) — empty list means valid.
    """
    errors = []

    # Required fields
    if 'plan_type' in data or exclude_id is None:
        if not data.get('plan_type'):
            errors.append('plan_type is required')
    if 'plan_name' in data or exclude_id is None:
        if not data.get('plan_name'):
            errors.append('plan_name is required')

    # Type checks
    if 'discount_numeric' in data and data['discount_numeric'] is not None:
        try:
            float(data['discount_numeric'])
        except (TypeError, ValueError):
            errors.append('discount_numeric must be a number')

    if 'sort_order' in data and data['sort_order'] is not None:
        try:
            int(data['sort_order'])
        except (TypeError, ValueError):
            errors.append('sort_order must be an integer')

    # JSONB field type checks
    list_fields = ['offers', 'terms_conditions', 'terms_conditions_cn', 'promotion_codes', 'linked_concessions']
    for f in list_fields:
        if f in data and data[f] is not None and not isinstance(data[f], list):
            errors.append(f'{f} must be a JSON array')

    dict_fields = ['applicable_sites', 'department_notes', 'terms_conditions_translations']
    for f in dict_fields:
        if f in data and data[f] is not None and not isinstance(data[f], dict):
            errors.append(f'{f} must be a JSON object')

    # Name uniqueness
    if data.get('plan_name'):
        DiscountPlan = _get_discount_plan_model()
        session = current_app.get_middleware_session()
        try:
            existing = session.query(DiscountPlan).filter_by(plan_name=data['plan_name']).first()
            if existing and (exclude_id is None or existing.id != exclude_id):
                errors.append(f'Plan name "{data["plan_name"]}" already exists')
        finally:
            session.close()

    return errors


@api_bp.route('/discount-plans', methods=['GET'])
@require_auth
@require_api_scope('discount_plans:read')
@rate_limit_api(max_requests=60, window_seconds=60)
def api_discount_plans_list():
    """
    List discount plans.

    Query parameters:
        active_only  — "true" (default) returns only active plans; "false" returns all
        plan_type    — filter by plan_type, e.g. "Evergreen"
        site         — filter by applicable site code, e.g. "L004"
    """
    DiscountPlan = _get_discount_plan_model()
    session = current_app.get_middleware_session()
    try:
        q = session.query(DiscountPlan)

        # Filter: active only (default true)
        active_only = request.args.get('active_only', 'true').lower() != 'false'
        if active_only:
            q = q.filter(DiscountPlan.is_active == True)  # noqa: E712

        # Filter: plan_type
        plan_type = request.args.get('plan_type')
        if plan_type:
            q = q.filter(DiscountPlan.plan_type == plan_type)

        # Filter: applicable site
        site = request.args.get('site')
        if site:
            # JSONB containment: applicable_sites->>'L004' = 'true'
            q = q.filter(
                text("applicable_sites->>:site = 'true'").bindparams(site=site)
            )

        plans = q.order_by(DiscountPlan.sort_order, DiscountPlan.plan_name).all()
        return jsonify({
            'count': len(plans),
            'plans': [p.to_dict() for p in plans],
        })
    finally:
        session.close()


@api_bp.route('/discount-plans/<int:plan_id>', methods=['GET'])
@require_auth
@require_api_scope('discount_plans:read')
@rate_limit_api(max_requests=60, window_seconds=60)
def api_discount_plans_get(plan_id):
    """
    Get a single discount plan by ID.
    Query params: include_concessions=true to resolve linked Sitelink data.
    """
    DiscountPlan = _get_discount_plan_model()
    session = current_app.get_middleware_session()
    try:
        plan = session.query(DiscountPlan).get(plan_id)
        if not plan:
            return jsonify({'error': 'Not found', 'message': f'No discount plan with id {plan_id}'}), 404

        result = plan.to_dict()

        # Optionally resolve linked concession details
        if request.args.get('include_concessions', '').lower() == 'true' and plan.is_stdrate_override:
            from common.models import SiteInfo
            applicable = sorted(c for c, v in (plan.applicable_sites or {}).items() if v)
            site_rows = session.query(SiteInfo.SiteID, SiteInfo.Name, SiteInfo.SiteCode) \
                               .filter(SiteInfo.SiteCode.in_(applicable)).all() if applicable else []
            by_code = {s.SiteCode: s for s in site_rows}
            result['linked_concession_details'] = [{
                'site_id': by_code[c].SiteID if c in by_code else None,
                'site_code': c,
                'site_name': by_code[c].Name if c in by_code else None,
                'concession_id': 0,
                'plan_name': 'Standard Rate (no concession)',
                'discount_pct': None,
                'start': None,
                'end': None,
            } for c in applicable]
        elif request.args.get('include_concessions', '').lower() == 'true' and plan.linked_concessions:
            try:
                from common.models import CcwsDiscount, SiteInfo
                details = []
                for link in plan.linked_concessions:
                    cc = (session.query(CcwsDiscount)
                          .filter_by(SiteID=link.get('site_id'), ConcessionID=link.get('concession_id'))
                          .first())
                    if cc:
                        site = session.query(SiteInfo.Name, SiteInfo.SiteCode).filter_by(SiteID=cc.SiteID).first()
                        details.append({
                            'site_id': cc.SiteID,
                            'site_code': site.SiteCode if site else None,
                            'site_name': site.Name if site else None,
                            'concession_id': cc.ConcessionID,
                            'plan_name': cc.sPlanName or cc.sDefPlanName,
                            'discount_pct': float(cc.dcPCDiscount) if cc.dcPCDiscount else None,
                            'start': cc.dPlanStrt.isoformat() if cc.dPlanStrt else None,
                            'end': cc.dPlanEnd.isoformat() if cc.dPlanEnd else None,
                        })
                result['linked_concession_details'] = details
            except Exception as e:
                current_app.logger.error(f"Error resolving linked concessions for plan {plan_id}: {e}")
                result['linked_concession_details'] = []
                result['_concession_error'] = 'Failed to resolve linked concessions'

        return jsonify(result)
    finally:
        session.close()


@api_bp.route('/discount-plans/by-name/<path:plan_name>', methods=['GET'])
@require_auth
@require_api_scope('discount_plans:read')
@rate_limit_api(max_requests=60, window_seconds=60)
def api_discount_plans_get_by_name(plan_name):
    """Get a single discount plan by name (URL-encoded)."""
    DiscountPlan = _get_discount_plan_model()
    session = current_app.get_middleware_session()
    try:
        plan = session.query(DiscountPlan).filter_by(plan_name=plan_name).first()
        if not plan:
            return jsonify({'error': 'Not found', 'message': f'No discount plan named "{plan_name}"'}), 404
        return jsonify(plan.to_dict())
    finally:
        session.close()


@api_bp.route('/discount-plans', methods=['POST'])
@require_auth
@require_api_scope('discount_plans:write')
@rate_limit_api(max_requests=20, window_seconds=60)
def api_discount_plans_create():
    """
    Create a new discount plan.

    Expects JSON body with at minimum: plan_type, plan_name.
    All other fields are optional.
    """
    DiscountPlan = _get_discount_plan_model()
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Bad request', 'message': 'JSON body required'}), 400

    errors = _validate_discount_plan_data(data, exclude_id=None)
    if errors:
        return jsonify({'error': 'Validation', 'message': '; '.join(errors), 'errors': errors}), 400

    session = current_app.get_middleware_session()
    try:
        plan = DiscountPlan()
        _apply_plan_json(plan, data, is_create=True)

        # Audit
        user = g.current_user.get('sub', 'api') if hasattr(g, 'current_user') and g.current_user else 'api'
        plan.created_by = user

        session.add(plan)
        session.commit()
        session.refresh(plan)

        return jsonify(plan.to_dict()), 201
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"API create discount plan error: {e}")
        return jsonify({'error': 'Server error', 'message': 'An internal error occurred'}), 500
    finally:
        session.close()


@api_bp.route('/discount-plans/<int:plan_id>', methods=['PUT'])
@require_auth
@require_api_scope('discount_plans:write')
@rate_limit_api(max_requests=20, window_seconds=60)
def api_discount_plans_update(plan_id):
    """
    Update an existing discount plan.

    PATCH semantics: only fields present in the JSON body are updated.
    """
    DiscountPlan = _get_discount_plan_model()
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Bad request', 'message': 'JSON body required'}), 400

    errors = _validate_discount_plan_data(data, exclude_id=plan_id)
    if errors:
        return jsonify({'error': 'Validation', 'message': '; '.join(errors), 'errors': errors}), 400

    session = current_app.get_middleware_session()
    try:
        plan = session.query(DiscountPlan).get(plan_id)
        if not plan:
            return jsonify({'error': 'Not found', 'message': f'No discount plan with id {plan_id}'}), 404

        _apply_plan_json(plan, data)

        # Audit
        user = g.current_user.get('sub', 'api') if hasattr(g, 'current_user') and g.current_user else 'api'
        plan.updated_by = user

        session.commit()
        session.refresh(plan)

        return jsonify(plan.to_dict())
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"API update discount plan error: {e}")
        return jsonify({'error': 'Server error', 'message': 'An internal error occurred'}), 500
    finally:
        session.close()


# =============================================================================
# Discount Plan Changer Tool — Sitelink SOAP operations
# =============================================================================
# Read plans from ccws_discount (esa_middleware) and update via CallCenterWs SOAP.
#
# GET  /api/ccws-discount-plans/<site_id>       — list plans for a site
# POST /api/ccws-discount-plans/update-simple   — enable/disable plans (simple)
# POST /api/ccws-discount-plans/update          — full plan update
# =============================================================================

@api_bp.route('/ccws-discount-plans/<int:site_id>')
@require_auth
@require_api_scope('sync:read')
def api_ccws_discount_plans(site_id):
    """
    Get discount/concession plans from ccws_discount table for a site.
    Excludes deleted plans. Returns key fields for the tool UI.
    """
    from common.models import CcwsDiscount, SiteInfo

    session = current_app.get_middleware_session()
    try:
        site = session.query(SiteInfo).filter_by(SiteID=site_id).first()
        if not site:
            return jsonify({'error': 'Site not found'}), 404

        plans = (
            session.query(CcwsDiscount)
            .filter(
                CcwsDiscount.SiteID == site_id,
                CcwsDiscount.dDeleted.is_(None),
            )
            .order_by(CcwsDiscount.sPlanName)
            .all()
        )

        results = []
        for p in plans:
            results.append({
                'ConcessionID': p.ConcessionID,
                'SiteID': p.SiteID,
                'sPlanName': p.sPlanName,
                'sDefPlanName': p.sDefPlanName,
                'sDescription': p.sDescription,
                'dPlanStrt': p.dPlanStrt.isoformat() if p.dPlanStrt else None,
                'dPlanEnd': p.dPlanEnd.isoformat() if p.dPlanEnd else None,
                'iShowOn': p.iShowOn,
                'bNeverExpires': p.bNeverExpires,
                'dcMaxOccPct': float(p.dcMaxOccPct) if p.dcMaxOccPct is not None else None,
                'dcFixedDiscount': float(p.dcFixedDiscount) if p.dcFixedDiscount is not None else None,
                'dcPCDiscount': float(p.dcPCDiscount) if p.dcPCDiscount is not None else None,
                'dcMaxAmountOff': float(p.dcMaxAmountOff) if p.dcMaxAmountOff is not None else None,
                'iAvailableAt': p.iAvailableAt,
                'bForAllUnits': p.bForAllUnits,
                'iExcludeIfLessThanUnitsTotal': p.iExcludeIfLessThanUnitsTotal,
                'iExcludeIfMoreThanUnitsTotal': p.iExcludeIfMoreThanUnitsTotal,
                'dcMaxOccPctExcludeIfMoreThanUnitsTotal': float(p.dcMaxOccPctExcludeIfMoreThanUnitsTotal) if p.dcMaxOccPctExcludeIfMoreThanUnitsTotal is not None else None,
                'isDisabled': p.dDisabled is not None,
                'dDisabled': p.dDisabled.isoformat() if p.dDisabled else None,
                'dCreated': p.dCreated.isoformat() if p.dCreated else None,
                'dUpdated': p.dUpdated.isoformat() if p.dUpdated else None,
            })

        active_count = sum(1 for r in results if not r['isDisabled'])

        return jsonify({
            'site_id': site_id,
            'site_code': site.SiteCode,
            'site_name': site.Name,
            'total_plans': len(results),
            'active_plans': active_count,
            'disabled_plans': len(results) - active_count,
            'plans': results,
        })
    finally:
        session.close()


@api_bp.route('/ccws-discount-plans/update-simple', methods=['POST'])
@require_auth
@require_api_scope('sync:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_ccws_discount_plans_update_simple():
    """
    Enable or disable discount plans via DiscountPlanUpdateSimple SOAP.
    Accepts a list of concession IDs for a single site.
    """
    from flask_login import current_user as flask_current_user
    from common.config import DataLayerConfig
    from common.soap_client import SOAPClient, SOAPFaultError
    from common.models import SiteInfo

    # Enforce discount tools permission for session-authenticated users
    if flask_current_user.is_authenticated and not flask_current_user.can_access_discount_tools():
        return jsonify({'error': 'Forbidden', 'message': 'Discount tools access required'}), 403

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code')
    concession_ids = data.get('concession_ids')
    disabled = data.get('disabled')

    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not concession_ids or not isinstance(concession_ids, list):
        return jsonify({'error': 'concession_ids must be a non-empty array'}), 400
    if len(concession_ids) > 500:
        return jsonify({'error': 'Maximum 500 concession IDs per request'}), 400
    if disabled not in (0, 1):
        return jsonify({'error': 'disabled must be 0 (active) or 1 (disabled)'}), 400

    # Validate all IDs are integers
    try:
        concession_ids = [int(cid) for cid in concession_ids]
    except (ValueError, TypeError):
        return jsonify({'error': 'All concession_ids must be integers'}), 400

    # Validate site_code against database
    mw_session = current_app.get_middleware_session()
    try:
        site = mw_session.query(SiteInfo).filter_by(SiteCode=site_code).first()
        if not site:
            return jsonify({'error': 'Invalid site_code'}), 400
    finally:
        mw_session.close()

    config = DataLayerConfig.from_env()
    if not config.soap:
        current_app.logger.error("SOAP configuration not available")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    cc_url = config.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
    soap_client = SOAPClient(
        base_url=cc_url,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=config.soap.timeout,
        retries=config.soap.retries
    )

    try:
        results = soap_client.call(
            operation="DiscountPlanUpdateSimple",
            parameters={
                "sLocationCode": site_code,
                "sConcessionIDs": "|".join(str(cid) for cid in concession_ids),
                "iDisabled": str(disabled),
                "sConcessionUnitTypeIDs": "",
                "iConcessionUnitTypeOverwrite": "0",
            },
            soap_action="http://tempuri.org/CallCenterWs/CallCenterWs/DiscountPlanUpdateSimple",
            namespace="http://tempuri.org/CallCenterWs/CallCenterWs",
            result_tag="RT",
        )

        ret_code = None
        if results:
            ret_code = results[0].get('Ret_Code')
            if results[0].get('Ret_Msg'):
                current_app.logger.info(f"SOAP DiscountPlanUpdateSimple ret_msg: {results[0].get('Ret_Msg')}")

        return jsonify({
            'success': True,
            'site_code': site_code,
            'concession_ids': concession_ids,
            'disabled': disabled,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        current_app.logger.error(f"SOAP fault during discount plan update-simple: {e}")
        return jsonify({
            'success': False,
            'error': 'SOAP API error',
            'details': 'SOAP API error occurred',
        }), 502

    except Exception as e:
        current_app.logger.error(f"Unexpected error during discount plan update-simple: {e}")
        return jsonify({
            'success': False,
            'error': 'Unexpected error',
            'details': 'An internal error occurred',
        }), 500

    finally:
        soap_client.close()


@api_bp.route('/ccws-discount-plans/update', methods=['POST'])
@require_auth
@require_api_scope('sync:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_ccws_discount_plans_update():
    """
    Full discount plan update via DiscountPlanUpdate SOAP.
    Updates occupancy limits, dates, show-on, exclusion thresholds, etc.
    """
    from flask_login import current_user as flask_current_user
    from common.config import DataLayerConfig
    from common.soap_client import SOAPClient, SOAPFaultError
    from common.models import SiteInfo

    # Enforce discount tools permission for session-authenticated users
    if flask_current_user.is_authenticated and not flask_current_user.can_access_discount_tools():
        return jsonify({'error': 'Forbidden', 'message': 'Discount tools access required'}), 403

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = data.get('site_code')
    concession_ids = data.get('concession_ids')

    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400
    if not concession_ids or not isinstance(concession_ids, list):
        return jsonify({'error': 'concession_ids must be a non-empty array'}), 400
    if len(concession_ids) > 500:
        return jsonify({'error': 'Maximum 500 concession IDs per request'}), 400

    try:
        concession_ids = [int(cid) for cid in concession_ids]
    except (ValueError, TypeError):
        return jsonify({'error': 'All concession_ids must be integers'}), 400

    # Validate site_code against database
    mw_session = current_app.get_middleware_session()
    try:
        site = mw_session.query(SiteInfo).filter_by(SiteCode=site_code).first()
        if not site:
            return jsonify({'error': 'Invalid site_code'}), 400
    finally:
        mw_session.close()

    # Extract and validate update fields
    import re
    DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{4}$')

    try:
        i_show_on = int(data.get('iShowOn', 0))
        dc_max_occ_pct = float(data.get('dcMaxOccPct', 0))
        i_available_at = int(data.get('iAvailableAt', 0))
        i_disabled = int(data.get('iDisabled', 0))
        i_exclude_less = int(data.get('iExcludeIfLessThanUnitsTotal', 0))
        i_exclude_more = int(data.get('iExcludeIfMoreThanUnitsTotal', 0))
        dc_max_occ_exclude = float(data.get('dcMaxOccPctExcludeIfMoreThanUnitsTotal', 0))
        i_unit_type_overwrite = int(data.get('iConcessionUnitTypeOverwrite', 0))
    except (ValueError, TypeError):
        return jsonify({'error': 'Numeric fields must be valid numbers'}), 400

    s_plan_strt = data.get('sPlanStrt', '') or ''
    s_plan_end = data.get('sPlanEnd', '') or ''
    if s_plan_strt and not DATE_RE.match(s_plan_strt):
        return jsonify({'error': 'sPlanStrt must be in MM/DD/YYYY format'}), 400
    if s_plan_end and not DATE_RE.match(s_plan_end):
        return jsonify({'error': 'sPlanEnd must be in MM/DD/YYYY format'}), 400

    s_unit_type_ids = data.get('sConcessionUnitTypeIDs', '') or ''
    if s_unit_type_ids and not re.match(r'^[\d|]+$', s_unit_type_ids):
        return jsonify({'error': 'sConcessionUnitTypeIDs must be pipe-separated integers'}), 400

    config = DataLayerConfig.from_env()
    if not config.soap:
        current_app.logger.error("SOAP configuration not available")
        return jsonify({'error': 'SOAP configuration not available'}), 500

    cc_url = config.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
    soap_client = SOAPClient(
        base_url=cc_url,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=config.soap.timeout,
        retries=config.soap.retries
    )

    try:
        results = soap_client.call(
            operation="DiscountPlanUpdate",
            parameters={
                "sLocationCode": site_code,
                "sConcessionIDs": "|".join(str(cid) for cid in concession_ids),
                "iShowOn": str(i_show_on),
                "dcMaxOccPct": str(dc_max_occ_pct),
                "sPlanStrt": str(s_plan_strt),
                "sPlanEnd": str(s_plan_end),
                "iAvailableAt": str(i_available_at),
                "iDisabled": str(i_disabled),
                "iExcludeIfLessThanUnitsTotal": str(i_exclude_less),
                "iExcludeIfMoreThanUnitsTotal": str(i_exclude_more),
                "dcMaxOccPctExcludeIfMoreThanUnitsTotal": str(dc_max_occ_exclude),
                "sConcessionUnitTypeIDs": str(s_unit_type_ids),
                "iConcessionUnitTypeOverwrite": str(i_unit_type_overwrite),
            },
            soap_action="http://tempuri.org/CallCenterWs/CallCenterWs/DiscountPlanUpdate",
            namespace="http://tempuri.org/CallCenterWs/CallCenterWs",
            result_tag="RT",
        )

        ret_code = None
        if results:
            ret_code = results[0].get('Ret_Code')
            if results[0].get('Ret_Msg'):
                current_app.logger.info(f"SOAP DiscountPlanUpdate ret_msg: {results[0].get('Ret_Msg')}")

        return jsonify({
            'success': True,
            'site_code': site_code,
            'concession_ids': concession_ids,
            'ret_code': ret_code,
        })

    except SOAPFaultError as e:
        current_app.logger.error(f"SOAP fault during discount plan update: {e}")
        return jsonify({
            'success': False,
            'error': 'SOAP API error',
            'details': 'SOAP API error occurred',
        }), 502

    except Exception as e:
        current_app.logger.error(f"Unexpected error during discount plan update: {e}")
        return jsonify({
            'success': False,
            'error': 'Unexpected error',
            'details': 'An internal error occurred',
        }), 500

    finally:
        soap_client.close()


# =============================================================================
# Unit Availability
# =============================================================================
# GET  /api/unit-availability   — available units with pricing & discount plans
# Reservation endpoints moved to reservations.py blueprint (/api/reservations/*)
# =============================================================================

@api_bp.route('/unit-availability')
@require_auth
@require_api_scope('inventory:read')
def api_unit_availability():
    """
    Get available (vacant, rentable) units with enriched label data from
    vw_units_inventory joined back to units_info for full pricing.

    Query parameters:
        site_ids       — comma-separated site IDs (required)
        category       — filter by category_label (enriched) or sTypeName fallback
        type_code      — filter by label_type_code
        climate_code   — filter by label_climate_code
        climate        — filter by bClimate (true/false)
        floor          — filter by iFloor
        shape          — filter by label_shape
        min_size       — minimum area (width * length)
        max_size       — maximum area (width * length)
    """
    site_ids_param = request.args.get('site_ids', '')
    if not site_ids_param:
        return jsonify({'error': 'site_ids parameter is required'}), 400

    try:
        site_ids = parse_site_ids(site_ids_param)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    # Optional filters
    category = request.args.get('category', '').strip()
    type_code = request.args.get('type_code', '').strip()
    climate_code = request.args.get('climate_code', '').strip()
    climate = request.args.get('climate', '').strip().lower()
    floor_param = request.args.get('floor', '').strip()
    shape = request.args.get('shape', '').strip()
    min_size = request.args.get('min_size', '').strip()
    max_size = request.args.get('max_size', '').strip()

    pbi_session = get_pbi_session()
    try:
        placeholders = ', '.join([f':sid{i}' for i in range(len(site_ids))])
        params = {f'sid{i}': sid for i, sid in enumerate(site_ids)}

        where_clauses = [
            f'u."SiteID" IN ({placeholders})',
            'u."bRentable" = true',
            'u."bRented" = false',
            'u."bExcludeFromWebsite" = false',
        ]

        if category:
            params['category'] = category
            where_clauses.append(
                '(v.category_label = :category OR '
                '(v.category_label IS NULL AND u."sTypeName" = :category))'
            )

        if type_code:
            params['type_code'] = type_code
            where_clauses.append('v.label_type_code = :type_code')

        if climate_code:
            params['climate_code'] = climate_code
            where_clauses.append('v.label_climate_code = :climate_code')

        if climate in ('true', 'false'):
            params['climate'] = climate == 'true'
            where_clauses.append('u."bClimate" = :climate')

        if floor_param:
            try:
                params['floor_val'] = int(floor_param)
                where_clauses.append('u."iFloor" = :floor_val')
            except ValueError:
                pass

        if shape:
            params['shape'] = shape
            where_clauses.append('v.label_shape = :shape')

        where_sql = ' AND '.join(where_clauses)

        query = text(f"""
            SELECT
                u."SiteID", u."UnitID", u."sLocationCode", u."sUnitName", u."sTypeName",
                u."dcWidth", u."dcLength", u."bClimate", u."bInside", u."bPower", u."bAlarm",
                u."iFloor", u."UnitTypeID",
                u."dcStdRate", u."dcWebRate", u."dcPushRate", u."dcBoardRate",
                u."dcPreferredRate", u."dcStdWeeklyRate", u."dcStdSecDep",
                u."dcTax1Rate", u."dcTax2Rate",
                u."sUnitNote", u."sUnitDesc",
                u."iDaysVacant", u."bWaitingListReserved", u."bCorporate",
                u."bServiceRequired", u."bMobile",
                (u."dcWidth" * u."dcLength") AS area,
                -- Enriched label fields from vw_units_inventory
                v.site_code,
                v.internal_label,
                v.country,
                v.category_label,
                v.label_type_code,
                v.label_climate_code,
                v.label_size_category,
                v.label_size_range,
                v.label_shape,
                v.label_pillar,
                v.label_published_at,
                v.has_pillar,
                v.pillar_size,
                v.is_odd_shape,
                v.deck_position
            FROM units_info u
            LEFT JOIN vw_units_inventory v
                ON v.site_id = u."SiteID" AND v.unit_id = u."UnitID"
            WHERE {where_sql}
            ORDER BY u."SiteID",
                     COALESCE(v.category_label, u."sTypeName"),
                     u."dcStdRate"
        """)

        result = pbi_session.execute(query, params)
        rows = result.fetchall()
        columns = result.keys()

        units = []
        for row in rows:
            unit = {}
            for col, val in zip(columns, row):
                if hasattr(val, 'isoformat'):
                    unit[col] = val.isoformat()
                elif isinstance(val, (int, float, bool, str)) or val is None:
                    unit[col] = val
                else:
                    unit[col] = float(val) if val is not None else None
            units.append(unit)

        # Apply size filters in Python (computed column)
        if min_size:
            try:
                min_val = float(min_size)
                units = [u for u in units if u.get('area') and u['area'] >= min_val]
            except ValueError:
                pass
        if max_size:
            try:
                max_val = float(max_size)
                units = [u for u in units if u.get('area') and u['area'] <= max_val]
            except ValueError:
                pass

        # Fetch distinct filter values from enriched view for dropdowns
        filter_query = text(f"""
            SELECT
                COALESCE(v.category_label, u."sTypeName") AS cat,
                v.label_type_code,
                v.label_climate_code,
                v.label_shape,
                u."iFloor"
            FROM units_info u
            LEFT JOIN vw_units_inventory v
                ON v.site_id = u."SiteID" AND v.unit_id = u."UnitID"
            WHERE u."SiteID" IN ({placeholders})
              AND u."bRentable" = true
              AND u."bRented" = false
        """)
        filter_result = pbi_session.execute(filter_query, params)
        filter_rows = filter_result.fetchall()

        categories_set = set()
        type_codes_set = set()
        climate_codes_set = set()
        shapes_set = set()
        floors_set = set()
        for r in filter_rows:
            if r[0]: categories_set.add(r[0])
            if r[1]: type_codes_set.add(r[1])
            if r[2]: climate_codes_set.add(r[2])
            if r[3]: shapes_set.add(r[3])
            if r[4] is not None: floors_set.add(r[4])

        # Fetch label lookups from dim tables
        label_lookups = {}
        try:
            for dim_table, key in [('dim_unit_type', 'type_codes'), ('dim_climate_type', 'climate_codes'), ('dim_unit_shape', 'shapes')]:
                dim_rows = pbi_session.execute(
                    text(f'SELECT code, description FROM {dim_table} ORDER BY sort_order')
                ).fetchall()
                label_lookups[key] = {r[0]: r[1] for r in dim_rows}
        except Exception:
            pass  # dim tables may not exist on all environments

        return jsonify({
            'units': units,
            'count': len(units),
            'filters': {
                'categories': sorted(categories_set),
                'type_codes': sorted(type_codes_set),
                'climate_codes': sorted(climate_codes_set),
                'shapes': sorted(shapes_set),
                'floors': sorted(floors_set),
            },
            'label_lookups': label_lookups,
        })

    except Exception as e:
        current_app.logger.error(f"Unit availability query error: {e}")
        return jsonify({'error': 'Failed to fetch available units'}), 500
    finally:
        pbi_session.close()


# =============================================================================
# GET /api/unit-availability/reservations — reservation details for reserved units
# =============================================================================

@api_bp.route('/unit-availability/reservations')
@require_auth
@require_api_scope('inventory:read')
def api_unit_availability_reservations():
    """
    Return active reservation details from api_reservations for given sites.
    Used by the unit availability tool to enrich reserved unit badges.

    Query parameters:
        site_codes — comma-separated site codes (required)
    """
    site_codes_param = request.args.get('site_codes', '')
    if not site_codes_param:
        return jsonify({'error': 'site_codes parameter is required'}), 400

    site_codes = [s.strip() for s in site_codes_param.split(',') if s.strip()]
    if not site_codes or len(site_codes) > 50:
        return jsonify({'error': 'Provide 1-50 site codes'}), 400

    pbi_session = get_pbi_session()
    try:
        placeholders = ', '.join([f':sc{i}' for i in range(len(site_codes))])
        params = {f'sc{i}': sc for i, sc in enumerate(site_codes)}

        rows = pbi_session.execute(text(f"""
            SELECT site_code, unit_id, waiting_id, tenant_id,
                   first_name, last_name, quoted_rate, needed_date,
                   expires_date, status, source, source_name,
                   reserved_at, soap_synced_at,
                   followup_date, inquiry_type, rental_type_id,
                   paid_reserve_fee, reserve_fee_receipt_id, concession_id
            FROM api_reservations
            WHERE site_code IN ({placeholders})
              AND status = 'created'
            ORDER BY site_code, unit_id
        """), params).fetchall()

        reservations = {}
        last_synced = None
        for r in rows:
            key = f"{r[0]}:{r[1]}"
            synced = r[13]
            reservations[key] = {
                'waiting_id': r[2],
                'tenant_id': r[3],
                'first_name': r[4] or '',
                'last_name': r[5] or '',
                'quoted_rate': float(r[6]) if r[6] else 0,
                'needed_date': r[7].isoformat() if r[7] else None,
                'expires_date': r[8].isoformat() if r[8] else None,
                'status': r[9],
                'source': r[10] or '',
                'source_name': r[11] or '',
                'reserved_at': r[12].isoformat() if r[12] else None,
                'soap_synced_at': synced.isoformat() if synced else None,
                'followup_date': r[14].isoformat() if r[14] else None,
                'inquiry_type': r[15] or 0,
                'rental_type_id': r[16] or 0,
                'paid_reserve_fee': float(r[17]) if r[17] else 0,
                'reserve_fee_receipt_id': r[18] or 0,
                'concession_id': r[19] or 0,
            }
            if synced and (last_synced is None or synced > last_synced):
                last_synced = synced

        return jsonify({
            'reservations': reservations,
            'count': len(reservations),
            'last_synced': last_synced.isoformat() if last_synced else None,
        })

    except Exception as e:
        current_app.logger.error(f"Reservation details query error: {e}")
        return jsonify({'error': 'Failed to fetch reservation details'}), 500
    finally:
        pbi_session.close()


# =============================================================================
# Smart Lock Management
# =============================================================================

MAX_SL_BATCH_SIZE = 500
MAX_SL_ID_LEN = 50
MAX_SL_NOTES_LEN = 255


def _sl_username():
    """Get current username for smart lock audit."""
    if hasattr(g, 'current_user') and g.current_user:
        return g.current_user.get('sub', 'unknown')
    return 'unknown'


def _require_sl_session_access(f):
    """Check can_access_smart_lock for session-authenticated users (RBAC guard).

    Auth model: session users are checked here; JWT-only callers are authorized
    exclusively by @require_api_scope('smart_lock:*'). If neither auth method is
    present, @require_auth has already rejected the request.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        from flask_login import current_user
        if current_user and current_user.is_authenticated:
            if not current_user.can_access_smart_lock():
                return jsonify({'error': 'Forbidden'}), 403
        return f(*args, **kwargs)
    return decorated


def _require_sl_session_admin(f):
    """Stricter RBAC guard: requires can_admin_smart_lock (manage bridges /
    keypads / padlocks / site config). Ops users hit this with 403.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        from flask_login import current_user
        if current_user and current_user.is_authenticated:
            if not current_user.can_admin_smart_lock():
                return jsonify({'error': 'Forbidden'}), 403
        return f(*args, **kwargs)
    return decorated


def _sl_audit(session, action, entity_type, entity_id=None, site_id=None, unit_id=None, detail=None):
    """Write a smart lock audit log entry."""
    from web.models.smart_lock import SmartLockAuditLog
    entry = SmartLockAuditLog(
        action=action,
        entity_type=entity_type,
        entity_id=entity_id,
        site_id=site_id,
        unit_id=unit_id,
        detail=detail,
        username=_sl_username(),
    )
    session.add(entry)


# --- Keypads CRUD ---

@api_bp.route('/smart-lock/keypads')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_sl_keypads_list():
    """List keypads, optionally filtered by site_id."""
    site_id = request.args.get('site_id')
    if site_id is not None:
        from flask_login import current_user
        if current_user.is_authenticated and not current_user.can_see_site(int(site_id)):
            return jsonify({'error': 'forbidden'}), 403
    session = current_app.get_middleware_session()
    try:
        from web.models.smart_lock import SmartLockKeypad, SmartLockUnitAssignment
        from common.models import IglooDevice
        q = session.query(SmartLockKeypad)
        if site_id:
            q = q.filter(SmartLockKeypad.site_id == int(site_id))
        keypads = q.order_by(SmartLockKeypad.site_id, SmartLockKeypad.keypad_id).all()

        # Build assignment lookup: keypad pk -> unit info
        keypad_pks = [k.id for k in keypads]
        assignment_map = {}
        if keypad_pks:
            assignments = session.query(SmartLockUnitAssignment).filter(
                SmartLockUnitAssignment.keypad_pk.in_(keypad_pks)
            ).all()
            for a in assignments:
                assignment_map[a.keypad_pk] = {'site_id': a.site_id, 'unit_id': a.unit_id}

        # Build igloo device lookup: deviceId/deviceName -> igloo data
        keypad_ids = [k.keypad_id for k in keypads]
        igloo_map = {}
        if keypad_ids:
            igloo_devs = session.query(IglooDevice).filter(
                (IglooDevice.deviceId.in_(keypad_ids)) | (IglooDevice.deviceName.in_(keypad_ids)),
                IglooDevice.type == 'Keypad'
            ).all()
            for ig in igloo_devs:
                igloo_data = {
                    'batteryLevel': ig.batteryLevel,
                    'lastSync': ig.lastSync.isoformat() if ig.lastSync else None,
                    'deviceId': ig.deviceId,
                    'type': ig.type,
                    'country': ig.departmentName,
                    'country_id': ig.departmentId,
                    'property': ig.propertyName,
                    'property_id': ig.propertyId,
                    'site_id': ig.site_id,
                }
                igloo_map[ig.deviceId] = igloo_data
                igloo_map[ig.deviceName] = igloo_data

        result = []
        for k in keypads:
            d = k.to_dict()
            d['assigned_to'] = assignment_map.get(k.id)
            d['igloo'] = igloo_map.get(k.keypad_id)
            result.append(d)

        return jsonify({'keypads': result})
    except Exception as e:
        current_app.logger.error(f"Smart lock keypads list error: {e}")
        return jsonify({'error': 'Failed to fetch keypads'}), 500
    finally:
        session.close()


@api_bp.route('/smart-lock/keypads', methods=['POST'])
@require_auth
@_require_sl_session_admin
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_sl_keypads_create():
    """Add a single keypad."""
    from web.models.smart_lock import SmartLockKeypad
    data = request.get_json()
    if not data:
        return jsonify({'error': 'JSON body required'}), 400

    keypad_id = (data.get('keypad_id') or '').strip()
    site_id = data.get('site_id')
    notes = (data.get('notes') or '').strip() or None

    if not keypad_id or not site_id:
        return jsonify({'error': 'keypad_id and site_id are required'}), 400
    if len(keypad_id) > MAX_SL_ID_LEN:
        return jsonify({'error': f'keypad_id must be {MAX_SL_ID_LEN} characters or fewer'}), 400
    if notes and len(notes) > MAX_SL_NOTES_LEN:
        return jsonify({'error': f'notes must be {MAX_SL_NOTES_LEN} characters or fewer'}), 400
    try:
        site_id = int(site_id)
    except (ValueError, TypeError):
        return jsonify({'error': 'site_id must be an integer'}), 400

    from flask_login import current_user
    if current_user.is_authenticated and not current_user.can_see_site(site_id):
        return jsonify({'error': 'forbidden'}), 403

    session = current_app.get_middleware_session()
    try:
        existing = session.query(SmartLockKeypad).filter_by(keypad_id=keypad_id).first()
        if existing:
            return jsonify({'error': 'Keypad ID already exists'}), 409

        keypad = SmartLockKeypad(
            keypad_id=keypad_id,
            site_id=site_id,
            notes=notes,
            created_by=_sl_username(),
        )
        session.add(keypad)
        _sl_audit(session, 'keypad_added', 'keypad', keypad_id, site_id=site_id,
                  detail=f'Added keypad {keypad_id} to site {site_id}')
        session.commit()
        return jsonify({'success': True, 'keypad': keypad.to_dict()}), 201
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Smart lock keypad create error: {e}")
        return jsonify({'error': 'Failed to create keypad'}), 500
    finally:
        session.close()


@api_bp.route('/smart-lock/keypads/batch', methods=['POST'])
@require_auth
@_require_sl_session_admin
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_sl_keypads_batch():
    """Batch create keypads from CSV upload."""
    from web.models.smart_lock import SmartLockKeypad
    data = request.get_json()
    if not data or 'items' not in data:
        return jsonify({'error': 'items array required'}), 400

    items = data['items']
    if len(items) > MAX_SL_BATCH_SIZE:
        return jsonify({'error': f'Batch size cannot exceed {MAX_SL_BATCH_SIZE} items'}), 400

    from flask_login import current_user
    if current_user.is_authenticated:
        for item in items:
            sid = item.get('site_id')
            if sid is not None:
                try:
                    sid = int(sid)
                except (ValueError, TypeError):
                    continue
                if not current_user.can_see_site(sid):
                    return jsonify({'error': 'forbidden'}), 403

    username = _sl_username()
    session = current_app.get_middleware_session()
    try:
        created = 0
        skipped = 0
        errors = []
        for i, item in enumerate(items):
            kid = (item.get('keypad_id') or '').strip()
            sid = item.get('site_id')
            notes = (item.get('notes') or '').strip() or None
            if not kid or not sid:
                errors.append(f'Row {i+1}: missing keypad_id or site_id')
                continue
            if len(kid) > MAX_SL_ID_LEN:
                errors.append(f'Row {i+1}: keypad_id too long')
                continue
            existing = session.query(SmartLockKeypad).filter_by(keypad_id=kid).first()
            if existing:
                skipped += 1
                continue
            session.add(SmartLockKeypad(
                keypad_id=kid, site_id=int(sid), notes=notes, created_by=username,
            ))
            created += 1

        if created > 0:
            _sl_audit(session, 'keypad_batch_upload', 'keypad',
                      detail=f'Batch uploaded {created} keypads ({skipped} skipped)')
        session.commit()
        return jsonify({'success': True, 'created': created, 'skipped': skipped, 'errors': errors})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Smart lock keypad batch error: {e}")
        return jsonify({'error': 'Batch upload failed'}), 500
    finally:
        session.close()


@api_bp.route('/smart-lock/keypads/<int:pk>', methods=['PUT'])
@require_auth
@_require_sl_session_admin
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_sl_keypads_update(pk):
    """Update a keypad (notes, site_id)."""
    from web.models.smart_lock import SmartLockKeypad
    data = request.get_json()
    if not data:
        return jsonify({'error': 'JSON body required'}), 400

    session = current_app.get_middleware_session()
    try:
        keypad = session.query(SmartLockKeypad).get(pk)
        if not keypad:
            return jsonify({'error': 'Keypad not found'}), 404

        from flask_login import current_user
        if current_user.is_authenticated and not current_user.can_see_site(keypad.site_id):
            return jsonify({'error': 'forbidden'}), 403

        changes = []
        if 'site_id' in data and int(data['site_id']) != keypad.site_id:
            changes.append(f'site {keypad.site_id}->{data["site_id"]}')
            keypad.site_id = int(data['site_id'])
        if 'notes' in data:
            keypad.notes = (data['notes'] or '').strip() or None
            changes.append('notes updated')
        keypad.updated_at = datetime.utcnow()

        if changes:
            _sl_audit(session, 'keypad_updated', 'keypad', keypad.keypad_id,
                      site_id=keypad.site_id, detail=', '.join(changes))
        session.commit()
        return jsonify({'success': True, 'keypad': keypad.to_dict()})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Smart lock keypad update error: {e}")
        return jsonify({'error': 'Failed to update keypad'}), 500
    finally:
        session.close()


@api_bp.route('/smart-lock/keypads/<int:pk>', methods=['DELETE'])
@require_auth
@_require_sl_session_admin
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_sl_keypads_delete(pk):
    """Delete a keypad."""
    from web.models.smart_lock import SmartLockKeypad
    session = current_app.get_middleware_session()
    try:
        keypad = session.query(SmartLockKeypad).get(pk)
        if not keypad:
            return jsonify({'error': 'Keypad not found'}), 404

        from flask_login import current_user
        if current_user.is_authenticated and not current_user.can_see_site(keypad.site_id):
            return jsonify({'error': 'forbidden'}), 403

        kid = keypad.keypad_id
        sid = keypad.site_id
        session.delete(keypad)
        _sl_audit(session, 'keypad_deleted', 'keypad', kid, site_id=sid,
                  detail=f'Deleted keypad {kid} from site {sid}')
        session.commit()
        return jsonify({'success': True})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Smart lock keypad delete error: {e}")
        return jsonify({'error': 'Failed to delete keypad'}), 500
    finally:
        session.close()


# --- Bridges (read-only — Igloo is the source of truth for site mapping) ---

@api_bp.route('/smart-lock/bridges')
@require_auth
@_require_sl_session_admin
@require_api_scope('smart_lock:read')
def api_sl_bridges_list():
    """List bridges, optionally filtered by site_id. Includes the linked
    keypad/lock device (from igloo_devices.linkedDevices) for each bridge."""
    site_id = request.args.get('site_id')
    if site_id is not None:
        from flask_login import current_user
        if current_user.is_authenticated and not current_user.can_see_site(int(site_id)):
            return jsonify({'error': 'forbidden'}), 403
    session = current_app.get_middleware_session()
    try:
        from web.models.smart_lock import SmartLockBridge
        from common.models import IglooDevice

        q = session.query(SmartLockBridge)
        if site_id:
            q = q.filter(SmartLockBridge.site_id == int(site_id))
        bridges = q.order_by(SmartLockBridge.site_id, SmartLockBridge.bridge_id).all()

        bridge_ids = [b.bridge_id for b in bridges]
        igloo_map = {}
        if bridge_ids:
            igloo_devs = session.query(IglooDevice).filter(
                IglooDevice.deviceId.in_(bridge_ids),
                IglooDevice.type == 'Bridge',
            ).all()
            for ig in igloo_devs:
                linked = []
                for ent in (ig.linkedDevices or []):
                    if isinstance(ent, dict):
                        linked.append({
                            'deviceId': ent.get('deviceId') or ent.get('id'),
                            'type': ent.get('type'),
                            'name': ent.get('name'),
                        })
                igloo_map[ig.deviceId] = {
                    'batteryLevel': ig.batteryLevel,
                    'lastSync': ig.lastSync.isoformat() if ig.lastSync else None,
                    'country': ig.departmentName,
                    'property': ig.propertyName,
                    'linked_devices': linked,
                }

        result = []
        for b in bridges:
            d = b.to_dict()
            d['igloo'] = igloo_map.get(b.bridge_id)
            result.append(d)
        return jsonify({'bridges': result})
    except Exception as e:
        current_app.logger.error(f"Smart lock bridges list error: {e}")
        return jsonify({'error': 'Failed to fetch bridges'}), 500
    finally:
        session.close()


# --- Padlocks CRUD ---

@api_bp.route('/smart-lock/padlocks')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_sl_padlocks_list():
    """List padlocks, optionally filtered by site_id."""
    site_id = request.args.get('site_id')
    if site_id is not None:
        from flask_login import current_user
        if current_user.is_authenticated and not current_user.can_see_site(int(site_id)):
            return jsonify({'error': 'forbidden'}), 403
    session = current_app.get_middleware_session()
    try:
        from web.models.smart_lock import SmartLockPadlock, SmartLockUnitAssignment
        from common.models import IglooDevice
        q = session.query(SmartLockPadlock)
        if site_id:
            q = q.filter(SmartLockPadlock.site_id == int(site_id))
        padlocks = q.order_by(SmartLockPadlock.site_id, SmartLockPadlock.padlock_id).all()

        padlock_pks = [p.id for p in padlocks]
        assignment_map = {}
        if padlock_pks:
            assignments = session.query(SmartLockUnitAssignment).filter(
                SmartLockUnitAssignment.padlock_pk.in_(padlock_pks)
            ).all()
            for a in assignments:
                assignment_map[a.padlock_pk] = {'site_id': a.site_id, 'unit_id': a.unit_id}

        # Build igloo device lookup: deviceId/deviceName -> igloo data
        padlock_ids = [p.padlock_id for p in padlocks]
        igloo_map = {}
        if padlock_ids:
            igloo_devs = session.query(IglooDevice).filter(
                (IglooDevice.deviceId.in_(padlock_ids)) | (IglooDevice.deviceName.in_(padlock_ids)),
                IglooDevice.type == 'Lock'
            ).all()
            for ig in igloo_devs:
                igloo_data = {
                    'batteryLevel': ig.batteryLevel,
                    'lastSync': ig.lastSync.isoformat() if ig.lastSync else None,
                    'deviceId': ig.deviceId,
                    'type': ig.type,
                    'country': ig.departmentName,
                    'country_id': ig.departmentId,
                    'property': ig.propertyName,
                    'property_id': ig.propertyId,
                    'site_id': ig.site_id,
                }
                igloo_map[ig.deviceId] = igloo_data
                igloo_map[ig.deviceName] = igloo_data

        result = []
        for p in padlocks:
            d = p.to_dict()
            d['assigned_to'] = assignment_map.get(p.id)
            d['igloo'] = igloo_map.get(p.padlock_id)
            result.append(d)

        return jsonify({'padlocks': result})
    except Exception as e:
        current_app.logger.error(f"Smart lock padlocks list error: {e}")
        return jsonify({'error': 'Failed to fetch padlocks'}), 500
    finally:
        session.close()


@api_bp.route('/smart-lock/padlocks', methods=['POST'])
@require_auth
@_require_sl_session_admin
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_sl_padlocks_create():
    """Add a single padlock."""
    from web.models.smart_lock import SmartLockPadlock
    data = request.get_json()
    if not data:
        return jsonify({'error': 'JSON body required'}), 400

    padlock_id = (data.get('padlock_id') or '').strip()
    site_id = data.get('site_id')
    notes = (data.get('notes') or '').strip() or None

    if not padlock_id or not site_id:
        return jsonify({'error': 'padlock_id and site_id are required'}), 400
    if len(padlock_id) > MAX_SL_ID_LEN:
        return jsonify({'error': f'padlock_id must be {MAX_SL_ID_LEN} characters or fewer'}), 400
    if notes and len(notes) > MAX_SL_NOTES_LEN:
        return jsonify({'error': f'notes must be {MAX_SL_NOTES_LEN} characters or fewer'}), 400
    try:
        site_id = int(site_id)
    except (ValueError, TypeError):
        return jsonify({'error': 'site_id must be an integer'}), 400

    from flask_login import current_user
    if current_user.is_authenticated and not current_user.can_see_site(site_id):
        return jsonify({'error': 'forbidden'}), 403

    session = current_app.get_middleware_session()
    try:
        existing = session.query(SmartLockPadlock).filter_by(padlock_id=padlock_id).first()
        if existing:
            return jsonify({'error': 'Padlock ID already exists'}), 409

        padlock = SmartLockPadlock(
            padlock_id=padlock_id,
            site_id=site_id,
            notes=notes,
            created_by=_sl_username(),
        )
        session.add(padlock)
        _sl_audit(session, 'padlock_added', 'padlock', padlock_id, site_id=site_id,
                  detail=f'Added padlock {padlock_id} to site {site_id}')
        session.commit()
        return jsonify({'success': True, 'padlock': padlock.to_dict()}), 201
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Smart lock padlock create error: {e}")
        return jsonify({'error': 'Failed to create padlock'}), 500
    finally:
        session.close()


@api_bp.route('/smart-lock/padlocks/batch', methods=['POST'])
@require_auth
@_require_sl_session_admin
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_sl_padlocks_batch():
    """Batch create padlocks from CSV upload."""
    from web.models.smart_lock import SmartLockPadlock
    data = request.get_json()
    if not data or 'items' not in data:
        return jsonify({'error': 'items array required'}), 400

    items = data['items']
    if len(items) > MAX_SL_BATCH_SIZE:
        return jsonify({'error': f'Batch size cannot exceed {MAX_SL_BATCH_SIZE} items'}), 400

    from flask_login import current_user
    if current_user.is_authenticated:
        for item in items:
            sid = item.get('site_id')
            if sid is not None:
                try:
                    sid = int(sid)
                except (ValueError, TypeError):
                    continue
                if not current_user.can_see_site(sid):
                    return jsonify({'error': 'forbidden'}), 403

    username = _sl_username()
    session = current_app.get_middleware_session()
    try:
        created = 0
        skipped = 0
        errors = []
        for i, item in enumerate(items):
            pid = (item.get('padlock_id') or '').strip()
            sid = item.get('site_id')
            notes = (item.get('notes') or '').strip() or None
            if not pid or not sid:
                errors.append(f'Row {i+1}: missing padlock_id or site_id')
                continue
            if len(pid) > MAX_SL_ID_LEN:
                errors.append(f'Row {i+1}: padlock_id too long')
                continue
            existing = session.query(SmartLockPadlock).filter_by(padlock_id=pid).first()
            if existing:
                skipped += 1
                continue
            session.add(SmartLockPadlock(
                padlock_id=pid, site_id=int(sid), notes=notes, created_by=username,
            ))
            created += 1

        if created > 0:
            _sl_audit(session, 'padlock_batch_upload', 'padlock',
                      detail=f'Batch uploaded {created} padlocks ({skipped} skipped)')
        session.commit()
        return jsonify({'success': True, 'created': created, 'skipped': skipped, 'errors': errors})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Smart lock padlock batch error: {e}")
        return jsonify({'error': 'Batch upload failed'}), 500
    finally:
        session.close()


@api_bp.route('/smart-lock/padlocks/<int:pk>', methods=['PUT'])
@require_auth
@_require_sl_session_admin
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_sl_padlocks_update(pk):
    """Update a padlock (notes, site_id)."""
    from web.models.smart_lock import SmartLockPadlock
    data = request.get_json()
    if not data:
        return jsonify({'error': 'JSON body required'}), 400

    session = current_app.get_middleware_session()
    try:
        padlock = session.query(SmartLockPadlock).get(pk)
        if not padlock:
            return jsonify({'error': 'Padlock not found'}), 404

        from flask_login import current_user
        if current_user.is_authenticated and not current_user.can_see_site(padlock.site_id):
            return jsonify({'error': 'forbidden'}), 403

        changes = []
        if 'site_id' in data and int(data['site_id']) != padlock.site_id:
            changes.append(f'site {padlock.site_id}->{data["site_id"]}')
            padlock.site_id = int(data['site_id'])
        if 'notes' in data:
            padlock.notes = (data['notes'] or '').strip() or None
            changes.append('notes updated')
        padlock.updated_at = datetime.utcnow()

        if changes:
            _sl_audit(session, 'padlock_updated', 'padlock', padlock.padlock_id,
                      site_id=padlock.site_id, detail=', '.join(changes))
        session.commit()
        return jsonify({'success': True, 'padlock': padlock.to_dict()})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Smart lock padlock update error: {e}")
        return jsonify({'error': 'Failed to update padlock'}), 500
    finally:
        session.close()


@api_bp.route('/smart-lock/padlocks/<int:pk>', methods=['DELETE'])
@require_auth
@_require_sl_session_admin
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_sl_padlocks_delete(pk):
    """Delete a padlock."""
    from web.models.smart_lock import SmartLockPadlock
    session = current_app.get_middleware_session()
    try:
        padlock = session.query(SmartLockPadlock).get(pk)
        if not padlock:
            return jsonify({'error': 'Padlock not found'}), 404

        from flask_login import current_user
        if current_user.is_authenticated and not current_user.can_see_site(padlock.site_id):
            return jsonify({'error': 'forbidden'}), 403

        pid = padlock.padlock_id
        sid = padlock.site_id
        session.delete(padlock)
        _sl_audit(session, 'padlock_deleted', 'padlock', pid, site_id=sid,
                  detail=f'Deleted padlock {pid} from site {sid}')
        session.commit()
        return jsonify({'success': True})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Smart lock padlock delete error: {e}")
        return jsonify({'error': 'Failed to delete padlock'}), 500
    finally:
        session.close()


# --- Units & Assignments ---

@api_bp.route('/smart-lock/units')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_sl_units():
    """Get units (from esa_middleware.units) merged with smart lock assignments."""
    site_ids_param = request.args.get('site_ids', '')
    if not site_ids_param:
        return jsonify({'error': 'site_ids parameter is required'}), 400

    try:
        site_ids = parse_site_ids(site_ids_param)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    from flask_login import current_user
    if current_user.is_authenticated:
        for sid in site_ids:
            if not current_user.can_see_site(sid):
                return jsonify({'error': 'forbidden'}), 403

    # Fetch units + assignments + keypads/padlocks from middleware (single session)
    from web.models.smart_lock import SmartLockUnitAssignment, SmartLockKeypad, SmartLockPadlock, GateAccessData
    from common.models import IglooDevice
    session = current_app.get_middleware_session()
    try:
        placeholders = ', '.join([f':sid{i}' for i in range(len(site_ids))])
        params = {f'sid{i}': sid for i, sid in enumerate(site_ids)}
        rows = session.execute(text(f"""
            SELECT u."SiteID", u."UnitID", u."sUnitName", u."bRentable", u."bRented"
            FROM ccws_units u
            WHERE u."SiteID" IN ({placeholders})
            ORDER BY u."SiteID", u."sUnitName"
        """), params).fetchall()
        units = [{'SiteID': r[0], 'UnitID': r[1], 'sUnitName': r[2],
                  'bRentable': r[3], 'bRented': r[4]} for r in rows]

        refresh_row = session.execute(text(
            f'SELECT MAX(updated_at) FROM ccws_units WHERE "SiteID" IN ({placeholders})'
        ), params).fetchone()
        last_refresh = refresh_row[0].isoformat() if refresh_row and refresh_row[0] else None
    except Exception as e:
        current_app.logger.error(f"Smart lock units query error: {e}")
        session.close()
        return jsonify({'error': 'Failed to fetch units'}), 500

    try:
        assignments = session.query(SmartLockUnitAssignment).filter(
            SmartLockUnitAssignment.site_id.in_(site_ids)
        ).all()
        assign_map = {}
        for a in assignments:
            assign_map[(a.site_id, a.unit_id)] = a.to_dict()

        # Fetch all keypads and padlocks for these sites (for dropdown population)
        keypads = session.query(SmartLockKeypad).filter(
            SmartLockKeypad.site_id.in_(site_ids)
        ).order_by(SmartLockKeypad.keypad_id).all()

        padlocks = session.query(SmartLockPadlock).filter(
            SmartLockPadlock.site_id.in_(site_ids)
        ).order_by(SmartLockPadlock.padlock_id).all()

        # ── gate access data (encrypted codes, lock status) ──
        gate_data = session.query(GateAccessData).filter(
            GateAccessData.site_id.in_(site_ids)
        ).all()
        gate_map = {(g.site_id, g.unit_id): g.to_dict() for g in gate_data}

        # gate data last refresh
        gate_refresh = None
        if gate_data:
            gate_refresh = max(
                (g.updated_at for g in gate_data if g.updated_at), default=None
            )
            if gate_refresh:
                gate_refresh = gate_refresh.isoformat()

        # ── igloo device data (battery, last sync) ──
        igloo_devs = session.query(IglooDevice).filter(
            IglooDevice.site_id.in_(site_ids)
        ).all()
        # Build lookup by both deviceId and deviceName for compatibility
        igloo_map = {}
        for ig in igloo_devs:
            igloo_data = {
                'batteryLevel': ig.batteryLevel,
                'lastSync': ig.lastSync.isoformat() if ig.lastSync else None,
                'deviceId': ig.deviceId,
                'type': ig.type,
                'country': ig.departmentName,
                'country_id': ig.departmentId,
                'property': ig.propertyName,
                'property_id': ig.propertyId,
                'site_id': ig.site_id,
            }
            igloo_map[ig.deviceId] = igloo_data
            igloo_map[ig.deviceName] = igloo_data

        # Enrich keypads/padlocks with igloo data
        keypads_out = []
        for k in keypads:
            d = k.to_dict()
            d['igloo'] = igloo_map.get(k.keypad_id)
            keypads_out.append(d)

        padlocks_out = []
        for p in padlocks:
            d = p.to_dict()
            d['igloo'] = igloo_map.get(p.padlock_id)
            padlocks_out.append(d)

        # Igloo data last refresh — use lastSync (Igloo's "device last
        # contacted Igloo cloud" timestamp), NOT updated_at (which only
        # bumps when a DB column actually changes — produces stale-looking
        # data for devices that haven't had any field change recently).
        # Strip tzinfo so the format matches last_refresh / gate_refresh:
        # the frontend appends 'Z' to mark UTC, so we must emit naive UTC.
        igloo_refresh = None
        if igloo_devs:
            igloo_refresh = max(
                (ig.lastSync for ig in igloo_devs if ig.lastSync), default=None
            )
            if igloo_refresh:
                if igloo_refresh.tzinfo is not None:
                    igloo_refresh = igloo_refresh.replace(tzinfo=None)
                igloo_refresh = igloo_refresh.isoformat()

        # Pipeline run timestamps — when did each cron last complete?
        # Falls back to these when site-level data is empty (e.g. fresh sites
        # with no gate enrollments). Naive UTC so the frontend's 'Z' append
        # produces a valid Date.
        pipeline_runs: Dict[str, Optional[str]] = {
            'ccws_units': None, 'ccws_gate_access': None,
            'igloo': None, 'igloo_pin_sync': None,
        }
        try:
            run_rows = session.execute(text("""
                SELECT pipeline_name, MAX(completed_at) AS last_run
                FROM mw_sync_runs
                WHERE pipeline_name = ANY(:names) AND status = 'completed'
                GROUP BY pipeline_name
            """), {'names': list(pipeline_runs.keys())}).fetchall()
            for name, ts in run_rows:
                if ts is not None:
                    if getattr(ts, 'tzinfo', None) is not None:
                        ts = ts.replace(tzinfo=None)
                    pipeline_runs[name] = ts.isoformat()
        except Exception:
            current_app.logger.exception("pipeline_runs lookup failed")

        # Merge assignments into units
        for u in units:
            key = (u['SiteID'], u['UnitID'])
            u['assignment'] = assign_map.get(key)
            u['gate_access'] = gate_map.get(key)

        return jsonify({
            'units': units,
            'count': len(units),
            'keypads': keypads_out,
            'padlocks': padlocks_out,
            'last_refresh': last_refresh,
            'gate_refresh': gate_refresh,
            'igloo_refresh': igloo_refresh,
            'pipeline_runs': pipeline_runs,
        })
    except Exception as e:
        current_app.logger.error(f"Smart lock assignments query error: {e}")
        return jsonify({'error': 'Failed to fetch assignments'}), 500
    finally:
        session.close()


@api_bp.route('/smart-lock/assignments', methods=['PUT'])
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_sl_assignments_upsert():
    """Bulk upsert unit assignments. Updates keypad/padlock status accordingly."""
    from web.models.smart_lock import (
        SmartLockUnitAssignment, SmartLockKeypad, SmartLockPadlock,
    )
    data = request.get_json()
    if not data or 'assignments' not in data:
        return jsonify({'error': 'assignments array required'}), 400

    assignments_list = data['assignments']
    if not isinstance(assignments_list, list):
        return jsonify({'error': 'assignments must be an array'}), 400
    if len(assignments_list) > MAX_SL_BATCH_SIZE:
        return jsonify({'error': f'Cannot save more than {MAX_SL_BATCH_SIZE} assignments at once'}), 400

    # Validate and collect site_id/unit_id pairs for cross-DB check
    parsed_items = []
    site_ids_set = set()
    for item in assignments_list:
        sid = item.get('site_id')
        uid = item.get('unit_id')
        if not sid or not uid:
            continue
        try:
            parsed_items.append((int(sid), int(uid), item.get('keypad_pk'), item.get('padlock_pk'), item.get('keypad_2_pk')))
            site_ids_set.add(int(sid))
        except (ValueError, TypeError):
            return jsonify({'error': 'site_id and unit_id must be integers'}), 400

    from flask_login import current_user
    if current_user.is_authenticated:
        for sid in site_ids_set:
            if not current_user.can_see_site(sid):
                return jsonify({'error': 'forbidden'}), 403

    # Validate unit_ids exist in ccws_units (esa_middleware)
    if parsed_items:
        mw_session = current_app.get_middleware_session()
        try:
            placeholders = ', '.join([f':sid{i}' for i in range(len(site_ids_set))])
            params = {f'sid{i}': sid for i, sid in enumerate(site_ids_set)}
            query = text(f'SELECT "SiteID", "UnitID" FROM ccws_units WHERE "SiteID" IN ({placeholders})')
            rows = mw_session.execute(query, params).fetchall()
            valid_units = {(r[0], r[1]) for r in rows}
        finally:
            mw_session.close()

        for sid, uid, _, _, _ in parsed_items:
            if (sid, uid) not in valid_units:
                return jsonify({'error': f'Unit {uid} not found at site {sid}'}), 400

    username = _sl_username()
    session = current_app.get_middleware_session()
    try:
        upserted = 0
        for sid, uid, new_keypad_pk, new_padlock_pk, new_keypad_2_pk in parsed_items:
            # Validate keypad/padlock belong to the same site
            if new_keypad_pk:
                kp = session.query(SmartLockKeypad).get(int(new_keypad_pk))
                if not kp or kp.site_id != sid:
                    return jsonify({'error': 'Keypad does not belong to the specified site'}), 400
            if new_keypad_2_pk:
                kp2 = session.query(SmartLockKeypad).get(int(new_keypad_2_pk))
                if not kp2 or kp2.site_id != sid:
                    return jsonify({'error': 'Keypad 2 does not belong to the specified site'}), 400
            if new_padlock_pk:
                pl = session.query(SmartLockPadlock).get(int(new_padlock_pk))
                if not pl or pl.site_id != sid:
                    return jsonify({'error': 'Padlock does not belong to the specified site'}), 400

            existing = session.query(SmartLockUnitAssignment).filter_by(
                site_id=sid, unit_id=uid
            ).first()

            old_keypad_pk = existing.keypad_pk if existing else None
            old_keypad_2_pk = existing.keypad_2_pk if existing else None
            old_padlock_pk = existing.padlock_pk if existing else None
            resolved_keypad_pk = int(new_keypad_pk) if new_keypad_pk else None
            resolved_keypad_2_pk = int(new_keypad_2_pk) if new_keypad_2_pk else None
            resolved_padlock_pk = int(new_padlock_pk) if new_padlock_pk else None

            if existing:
                existing.keypad_pk = resolved_keypad_pk
                existing.keypad_2_pk = resolved_keypad_2_pk
                existing.padlock_pk = resolved_padlock_pk
                existing.assigned_by = username
                existing.updated_at = datetime.utcnow()
            else:
                assignment = SmartLockUnitAssignment(
                    site_id=sid,
                    unit_id=uid,
                    keypad_pk=resolved_keypad_pk,
                    keypad_2_pk=resolved_keypad_2_pk,
                    padlock_pk=resolved_padlock_pk,
                    assigned_by=username,
                )
                session.add(assignment)

            # Update keypad status (keypads can be assigned to multiple units)
            if old_keypad_pk != resolved_keypad_pk:
                if old_keypad_pk:
                    old_kp = session.query(SmartLockKeypad).get(old_keypad_pk)
                    if old_kp:
                        # Only set not_assigned if no other unit still references this keypad
                        other_ref = session.query(SmartLockUnitAssignment).filter(
                            SmartLockUnitAssignment.keypad_pk == old_keypad_pk,
                            SmartLockUnitAssignment.site_id == sid,
                            SmartLockUnitAssignment.unit_id != uid,
                        ).first()
                        if not other_ref:
                            old_kp.status = 'not_assigned'
                        _sl_audit(session, 'keypad_unassigned', 'assignment', old_kp.keypad_id,
                                  site_id=sid, unit_id=uid,
                                  detail=f'Unassigned keypad {old_kp.keypad_id} from unit {uid}')
                if resolved_keypad_pk:
                    new_kp = session.query(SmartLockKeypad).get(resolved_keypad_pk)
                    if new_kp:
                        new_kp.status = 'assigned'
                        _sl_audit(session, 'keypad_assigned', 'assignment', new_kp.keypad_id,
                                  site_id=sid, unit_id=uid,
                                  detail=f'Assigned keypad {new_kp.keypad_id} to unit {uid}')

            # Update keypad 2 status
            if old_keypad_2_pk != resolved_keypad_2_pk:
                if old_keypad_2_pk:
                    old_kp2 = session.query(SmartLockKeypad).get(old_keypad_2_pk)
                    if old_kp2:
                        other_ref = session.query(SmartLockUnitAssignment).filter(
                            SmartLockUnitAssignment.keypad_2_pk == old_keypad_2_pk,
                            SmartLockUnitAssignment.site_id == sid,
                            SmartLockUnitAssignment.unit_id != uid,
                        ).first()
                        if not other_ref:
                            # Also check keypad_pk slot
                            other_ref_kp1 = session.query(SmartLockUnitAssignment).filter(
                                SmartLockUnitAssignment.keypad_pk == old_keypad_2_pk,
                                SmartLockUnitAssignment.site_id == sid,
                            ).first()
                            if not other_ref_kp1:
                                old_kp2.status = 'not_assigned'
                        _sl_audit(session, 'keypad_unassigned', 'assignment', old_kp2.keypad_id,
                                  site_id=sid, unit_id=uid,
                                  detail=f'Unassigned keypad 2 {old_kp2.keypad_id} from unit {uid}')
                if resolved_keypad_2_pk:
                    new_kp2 = session.query(SmartLockKeypad).get(resolved_keypad_2_pk)
                    if new_kp2:
                        new_kp2.status = 'assigned'
                        _sl_audit(session, 'keypad_assigned', 'assignment', new_kp2.keypad_id,
                                  site_id=sid, unit_id=uid,
                                  detail=f'Assigned keypad 2 {new_kp2.keypad_id} to unit {uid}')

            # Update padlock status
            if old_padlock_pk != resolved_padlock_pk:
                if old_padlock_pk:
                    old_pl = session.query(SmartLockPadlock).get(old_padlock_pk)
                    if old_pl:
                        old_pl.status = 'not_assigned'
                        _sl_audit(session, 'padlock_unassigned', 'assignment', old_pl.padlock_id,
                                  site_id=sid, unit_id=uid,
                                  detail=f'Unassigned padlock {old_pl.padlock_id} from unit {uid}')
                if resolved_padlock_pk:
                    new_pl = session.query(SmartLockPadlock).get(resolved_padlock_pk)
                    if new_pl:
                        new_pl.status = 'assigned'
                        _sl_audit(session, 'padlock_assigned', 'assignment', new_pl.padlock_id,
                                  site_id=sid, unit_id=uid,
                                  detail=f'Assigned padlock {new_pl.padlock_id} to unit {uid}')

            upserted += 1

        session.commit()
        return jsonify({'success': True, 'upserted': upserted})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Smart lock assignments upsert error: {e}")
        return jsonify({'error': 'Failed to save assignments'}), 500
    finally:
        session.close()


# --- Assignments Read API (external) ---

@api_bp.route('/smart-lock/assignments', methods=['GET'])
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_sl_assignments_list():
    """List assignments with human-readable keypad/padlock IDs. Requires site_id."""
    from web.models.smart_lock import (
        SmartLockUnitAssignment, SmartLockKeypad, SmartLockPadlock,
    )
    site_id = request.args.get('site_id')
    if not site_id:
        return jsonify({'error': 'site_id parameter is required'}), 400
    try:
        site_id = int(site_id)
    except (ValueError, TypeError):
        return jsonify({'error': 'site_id must be an integer'}), 400

    from flask_login import current_user
    if current_user.is_authenticated and not current_user.can_see_site(site_id):
        return jsonify({'error': 'forbidden'}), 403

    unit_ids_param = request.args.get('unit_ids', '')

    session = current_app.get_middleware_session()
    try:
        q = session.query(SmartLockUnitAssignment).filter(
            SmartLockUnitAssignment.site_id == site_id
        )
        if unit_ids_param:
            try:
                unit_ids = [int(u.strip()) for u in unit_ids_param.split(',') if u.strip()]
            except ValueError:
                return jsonify({'error': 'unit_ids must be comma-separated integers'}), 400
            q = q.filter(SmartLockUnitAssignment.unit_id.in_(unit_ids))

        assignments = q.order_by(SmartLockUnitAssignment.unit_id).all()

        # Build keypad/padlock ID lookup maps
        keypad_pks = set()
        for a in assignments:
            if a.keypad_pk:
                keypad_pks.add(a.keypad_pk)
            if a.keypad_2_pk:
                keypad_pks.add(a.keypad_2_pk)
        padlock_pks = {a.padlock_pk for a in assignments if a.padlock_pk}

        kp_map = {}
        if keypad_pks:
            for kp in session.query(SmartLockKeypad).filter(SmartLockKeypad.id.in_(keypad_pks)).all():
                kp_map[kp.id] = kp.keypad_id

        pl_map = {}
        if padlock_pks:
            for pl in session.query(SmartLockPadlock).filter(SmartLockPadlock.id.in_(padlock_pks)).all():
                pl_map[pl.id] = pl.padlock_id

        result = []
        for a in assignments:
            result.append({
                'site_id': a.site_id,
                'unit_id': a.unit_id,
                'keypad_pk': a.keypad_pk,
                'keypad_id': kp_map.get(a.keypad_pk),
                'keypad_2_pk': a.keypad_2_pk,
                'keypad_2_id': kp_map.get(a.keypad_2_pk),
                'padlock_pk': a.padlock_pk,
                'padlock_id': pl_map.get(a.padlock_pk),
                'assigned_by': a.assigned_by,
                'updated_at': a.updated_at.isoformat() if a.updated_at else None,
            })

        return jsonify({'assignments': result, 'count': len(result)})
    except Exception as e:
        current_app.logger.error(f"Smart lock assignments list error: {e}")
        return jsonify({'error': 'Failed to fetch assignments'}), 500
    finally:
        session.close()


# --- Audit Log ---

@api_bp.route('/smart-lock/audit-log')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_sl_audit_log():
    """Get recent smart lock audit log entries."""
    from web.models.smart_lock import SmartLockAuditLog
    site_id = request.args.get('site_id')
    if site_id is not None:
        from flask_login import current_user
        if current_user.is_authenticated and not current_user.can_see_site(int(site_id)):
            return jsonify({'error': 'forbidden'}), 403
    try:
        limit = min(int(request.args.get('limit', 100)), 500)
    except (ValueError, TypeError):
        return jsonify({'error': 'limit must be an integer'}), 400

    session = current_app.get_middleware_session()
    try:
        q = session.query(SmartLockAuditLog)
        if site_id:
            q = q.filter(SmartLockAuditLog.site_id == int(site_id))
        entries = q.order_by(desc(SmartLockAuditLog.created_at)).limit(limit).all()
        return jsonify({'entries': [e.to_dict() for e in entries]})
    except Exception as e:
        current_app.logger.error(f"Smart lock audit log error: {e}")
        return jsonify({'error': 'Failed to fetch audit log'}), 500
    finally:
        session.close()


# --- Site Config ---

@api_bp.route('/smart-lock/config')
@require_auth
@_require_sl_session_admin
@require_api_scope('smart_lock:read')
def api_sl_config_list():
    """List smart lock site configurations (admin-only)."""
    from web.models.smart_lock import SmartLockSiteConfig
    sid = request.args.get('site_id', type=int)
    if sid is not None:
        from flask_login import current_user
        if current_user.is_authenticated and not current_user.can_see_site(sid):
            return jsonify({'error': 'forbidden'}), 403
    session = current_app.get_middleware_session()
    try:
        configs = session.query(SmartLockSiteConfig).order_by(
            SmartLockSiteConfig.site_code
        ).all()
        return jsonify({
            'configs': [c.to_dict() for c in configs],
            'count': len(configs),
        })
    except Exception as e:
        current_app.logger.exception("Smart lock config list error")
        return jsonify({'error': 'Failed to fetch config'}), 500
    finally:
        session.close()


@api_bp.route('/smart-lock/config/enabled')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_sl_config_enabled():
    """List only enabled smart lock sites. Lightweight endpoint for external consumers."""
    from web.models.smart_lock import SmartLockSiteConfig
    session = current_app.get_middleware_session()
    try:
        configs = session.query(SmartLockSiteConfig).filter(
            SmartLockSiteConfig.enabled.is_(True)
        ).order_by(SmartLockSiteConfig.site_code).all()
        return jsonify({
            'sites': [{'site_id': c.site_id, 'site_code': c.site_code, 'site_name': c.site_name} for c in configs],
            'count': len(configs),
        })
    except Exception as e:
        current_app.logger.exception("Smart lock enabled sites error")
        return jsonify({'error': 'Failed to fetch enabled sites'}), 500
    finally:
        session.close()


@api_bp.route('/smart-lock/config', methods=['PUT'])
@require_auth
@_require_sl_session_admin
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_sl_config_upsert():
    """Enable or disable smart lock for a site, and update per-site revoke policy flags."""
    from web.models.smart_lock import SmartLockSiteConfig
    data = request.get_json()
    if not data:
        return jsonify({'error': 'JSON body required'}), 400

    site_id = data.get('site_id')
    enabled = data.get('enabled')
    if not site_id or enabled is None:
        return jsonify({'error': 'site_id and enabled required'}), 400
    try:
        site_id = int(site_id)
    except (ValueError, TypeError):
        return jsonify({'error': 'site_id must be an integer'}), 400

    from flask_login import current_user
    if current_user.is_authenticated and not current_user.can_see_site(site_id):
        return jsonify({'error': 'forbidden'}), 403

    session = current_app.get_middleware_session()
    try:
        config = session.query(SmartLockSiteConfig).get(site_id)
        if config:
            config.enabled = bool(enabled)
            if 'notes' in data:
                config.notes = (data['notes'] or '').strip()[:255] or None
            if 'revoke_on_gate_locked' in data:
                config.revoke_on_gate_locked = bool(data['revoke_on_gate_locked'])
            if 'revoke_on_overlocked' in data:
                config.revoke_on_overlocked = bool(data['revoke_on_overlocked'])
            if 'revoke_on_not_rentable' in data:
                config.revoke_on_not_rentable = bool(data['revoke_on_not_rentable'])
            config.updated_by = _sl_username()
        else:
            # Resolve site_code and site_name from esa_pbi
            site_code = None
            site_name = None
            try:
                pbi_session = get_pbi_session()
                row = pbi_session.execute(
                    text('SELECT "SiteCode", "Name" FROM siteinfo WHERE "SiteID" = :sid'),
                    {'sid': site_id}
                ).fetchone()
                pbi_session.close()
                if row:
                    site_code = row[0]
                    site_name = row[1]
            except Exception:
                pass

            config = SmartLockSiteConfig(
                site_id=site_id,
                enabled=bool(enabled),
                site_code=site_code,
                site_name=site_name,
                notes=(data.get('notes') or '').strip()[:255] or None,
                updated_by=_sl_username(),
                revoke_on_gate_locked=bool(data['revoke_on_gate_locked']) if 'revoke_on_gate_locked' in data else True,
                revoke_on_overlocked=bool(data['revoke_on_overlocked']) if 'revoke_on_overlocked' in data else True,
                revoke_on_not_rentable=bool(data['revoke_on_not_rentable']) if 'revoke_on_not_rentable' in data else True,
            )
            session.add(config)

        _sl_audit(session, 'config_updated', 'config', str(site_id),
                  site_id=site_id,
                  detail=f'Smart lock {"enabled" if enabled else "disabled"} for site {site_id}')
        session.commit()
        return jsonify({'success': True, 'config': config.to_dict()})
    except Exception as e:
        session.rollback()
        current_app.logger.exception("Smart lock config upsert error")
        return jsonify({'error': 'Failed to update config'}), 500
    finally:
        session.close()


# --- Gate Code Reveal ---

@api_bp.route('/smart-lock/gate-code')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_sl_gate_code():
    """Decrypt and return a single unit's gate access code."""
    from web.models.smart_lock import GateAccessData
    unit_id = request.args.get('unit_id', type=int)
    location_code = request.args.get('location_code', '')
    site_id = request.args.get('site_id', type=int)

    if not unit_id or not location_code or not site_id:
        return jsonify({'error': 'unit_id, location_code, and site_id are required'}), 400

    from flask_login import current_user
    if current_user.is_authenticated and not current_user.can_see_site(site_id):
        return jsonify({'error': 'forbidden'}), 403

    session = current_app.get_middleware_session()
    try:
        record = session.query(GateAccessData).filter_by(
            location_code=location_code, unit_id=unit_id
        ).first()

        if not record:
            return jsonify({'error': 'No gate access data for this unit'}), 404

        # Site-scoping: site_id is mandatory and must match — fail closed.
        if record.site_id != site_id:
            return jsonify({'error': 'Forbidden'}), 403

        from common.gate_access_crypto import get_gate_crypto
        crypto = get_gate_crypto()

        code1 = crypto.decrypt(record.access_code_enc) if record.access_code_enc else ''
        code2 = crypto.decrypt(record.access_code2_enc) if record.access_code2_enc else ''

        # Audit the reveal
        _sl_audit(
            session, 'gate_code_viewed', 'gate_access', str(unit_id),
            site_id=record.site_id, unit_id=unit_id,
            detail=f"Access code revealed for unit {record.unit_name}",
        )

        return jsonify({
            'access_code': code1,
            'access_code2': code2,
        })
    except Exception as e:
        logger.error("Gate code reveal failed: %s", e)
        return jsonify({'error': 'Failed to decrypt access code'}), 500
    finally:
        session.close()


# --- PIN Audit & Push ---

@api_bp.route('/smart-lock/pin-audit')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
@rate_limit_api(max_requests=10, window_seconds=60)
def api_sl_pin_audit():
    """Reconcile Igloo keypad PINs against SiteLink gate access codes.

    Uses the same reconciliation semantics as igloo_pin_sync:
      synced              — should_have_pin=True + ESA-tagged PIN equals gate code
      push_pending        — should_have_pin=True + no/stale ESA PIN (cron will push)
      revoke_pending      — should_have_pin=False + ESA PIN still on device
      clean               — should_have_pin=False + no ESA PIN (nothing to do)
      no_gate_code        — rented but no SiteLink gate code at all
      invalid_pin_format  — rented, gate code present but not 4-10 digits
      legacy_collision    — should_have_pin=True + non-ESA entry already holds the PIN
                            (we cannot push; manual review needed)
      legacy_revoke_blocked — should_have_pin=False + non-ESA entry holds tenant's old
                              PIN (we cannot revoke; tenant retains access)
      bridge_offline      — keypad's bridge unreachable in the last 30min based on
                            recent pin_push_failed audit rows

    Each (unit × keypad) pair is evaluated independently so secondary keypads
    (keypad_2_pk) are covered.

    Response rows include a `reason` field (not_rentable | overlocked | gate_locked |
    moved_out | null) when status is revoke_pending.
    """
    from web.models.smart_lock import (
        SmartLockUnitAssignment, SmartLockKeypad, GateAccessData, SmartLockSiteConfig,
    )
    from common.igloo_client import IglooClient, IglooAPIError

    site_ids_param = request.args.get('site_ids', '')
    if not site_ids_param:
        return jsonify({'error': 'site_ids parameter is required'}), 400
    try:
        site_ids = parse_site_ids(site_ids_param)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    _PIN_RE_AUDIT = re.compile(r'^\d{4,10}$')
    # Use bRented from ccws_units (authoritative occupancy source) — same fix
    # applied in igloo_pin_sync. ccws_gate_access.is_rented is unreliable at
    # sites where SiteLink deletes enrollments on move-out.

    def _esa_tag_audit_matches(name, sid, uid):
        """Match an ESA-owned access entry name by prefix; the trailing
        unit-name suffix is informational only.
        """
        if not name:
            return False
        base = f"ESA-{sid}-{uid}"
        return name == base or name.startswith(base + ' ')

    class _AuditPolicyDefaults:
        revoke_on_gate_locked = True
        revoke_on_overlocked = True
        revoke_on_not_rentable = True

    def _audit_revoke_reason(is_rented, b_rentable, is_gate_locked, is_overlocked, policy):
        if policy.revoke_on_not_rentable and not b_rentable:
            return 'not_rentable'
        if policy.revoke_on_overlocked and is_overlocked:
            return 'overlocked'
        if policy.revoke_on_gate_locked and is_gate_locked:
            return 'gate_locked'
        return 'moved_out'

    session = current_app.get_middleware_session()
    try:
        # Load per-site policy configs
        site_configs = {}
        for cfg in session.query(SmartLockSiteConfig).filter(
            SmartLockSiteConfig.site_id.in_(site_ids)
        ).all():
            site_configs[cfg.site_id] = cfg

        assignments = session.query(SmartLockUnitAssignment).filter(
            SmartLockUnitAssignment.site_id.in_(site_ids),
            (SmartLockUnitAssignment.keypad_pk.isnot(None)) |
            (SmartLockUnitAssignment.keypad_2_pk.isnot(None)),
        ).all()

        if not assignments:
            return jsonify({'results': [], 'count': 0})

        kp_pks = set()
        for a in assignments:
            if a.keypad_pk:
                kp_pks.add(a.keypad_pk)
            if a.keypad_2_pk:
                kp_pks.add(a.keypad_2_pk)
        keypads = session.query(SmartLockKeypad).filter(SmartLockKeypad.id.in_(kp_pks)).all()
        kp_map = {k.id: k.keypad_id for k in keypads}

        gate_rows = session.query(GateAccessData).filter(
            GateAccessData.site_id.in_(site_ids)
        ).all()
        gate_map = {(g.site_id, g.unit_id): g for g in gate_rows}

        # Load bRentable + bRented from ccws_units (authoritative occupancy)
        rentable_map = {}
        rented_map = {}
        if site_ids:
            rows = session.execute(
                text(
                    'SELECT "SiteID", "UnitID", "bRentable", "bRented" '
                    'FROM ccws_units WHERE "SiteID" = ANY(:sids) '
                    'AND deleted_at IS NULL'
                ),
                {'sids': list(site_ids)},
            ).fetchall()
            for sid, uid, rentable, rented in rows:
                rentable_map[(sid, uid)] = bool(rentable)
                rented_map[(sid, uid)] = bool(rented)

        # Pre-load device_ids that hit a bridge-offline event in the last
        # 30min. We surface those units as bridge_offline status until the
        # next successful push (which will not produce an audit row, but
        # the status will simply revert to push_pending/synced).
        from web.models.smart_lock import SmartLockAuditLog as _SLA
        from datetime import datetime as _dt2, timedelta as _td2
        offline_cutoff = _dt2.utcnow() - _td2(minutes=30)
        offline_devices = {
            row.entity_id
            for row in session.query(_SLA.entity_id).filter(
                _SLA.action == 'bridge_offline',
                _SLA.site_id.in_(site_ids),
                _SLA.created_at >= offline_cutoff,
            ).distinct()
        }

        from common.gate_access_crypto import get_gate_crypto
        crypto = get_gate_crypto()

        # Fetch Igloo state per unique device_id
        unique_device_ids = {kp_map[pk] for pk in kp_pks if pk in kp_map}
        device_access = {}
        device_pending = {}
        try:
            client = IglooClient()
        except IglooAPIError:
            return jsonify({'error': 'Failed to connect to Igloo API'}), 502

        for device_id in unique_device_ids:
            try:
                device_access[device_id] = client.list_device_access(device_id)
            except IglooAPIError:
                device_access[device_id] = []
            try:
                jobs = client.list_device_jobs(device_id)
                pending = set()
                for j in jobs:
                    if j.get('status') == 'pending' and j.get('description') == 'create_bluetooth_pin':
                        cp = (j.get('accessData') or {}).get('customPin')
                        if cp:
                            pending.add(cp)
                device_pending[device_id] = pending
            except IglooAPIError:
                device_pending[device_id] = set()

        results = []
        for a in assignments:
            gate = gate_map.get((a.site_id, a.unit_id))
            plain_pin = None
            if gate and gate.access_code_enc:
                try:
                    plain_pin = crypto.decrypt(gate.access_code_enc)
                except Exception:
                    plain_pin = None

            is_rented = rented_map.get((a.site_id, a.unit_id), False)
            is_gate_locked = bool(gate and gate.is_gate_locked)
            is_overlocked = bool(gate and gate.is_overlocked)
            b_rentable = rentable_map.get((a.site_id, a.unit_id), False)
            has_valid_pin = bool(plain_pin and _PIN_RE_AUDIT.match(plain_pin))
            has_invalid_pin = bool(plain_pin and not _PIN_RE_AUDIT.match(plain_pin))
            policy = site_configs.get(a.site_id, _AuditPolicyDefaults())

            # should_have_pin base (excluding gate code validity)
            should_have_pin_base = (
                is_rented
                and (not policy.revoke_on_not_rentable or b_rentable)
                and (not policy.revoke_on_gate_locked or not is_gate_locked)
                and (not policy.revoke_on_overlocked or not is_overlocked)
            )

            slots = []
            if a.keypad_pk and a.keypad_pk in kp_map:
                slots.append(('primary', a.keypad_pk, kp_map[a.keypad_pk]))
            if a.keypad_2_pk and a.keypad_2_pk in kp_map:
                slots.append(('secondary', a.keypad_2_pk, kp_map[a.keypad_2_pk]))

            for slot_label, keypad_pk, device_id in slots:
                access_list = device_access.get(device_id, [])
                pending_set = device_pending.get(device_id, set())

                esa_entry = next(
                    (e for e in access_list
                     if _esa_tag_audit_matches(e.get('name'), a.site_id, a.unit_id)),
                    None,
                )
                esa_pin_value = esa_entry.get('pin') if esa_entry else None

                # Helper: does any non-ESA entry on this device hold the
                # current SiteLink gate code? Two implications:
                #   - on push side: we cannot push (Igloo PIN-uniqueness)
                #   - on revoke side: we cannot revoke (no accessId we own)
                legacy_holds_pin = bool(plain_pin) and any(
                    e.get('pin') == plain_pin
                    and not _esa_tag_audit_matches(e.get('name'), a.site_id, a.unit_id)
                    for e in access_list
                )

                reason = None
                if device_id in offline_devices:
                    # Bridge offline trumps everything — neither push nor
                    # revoke can succeed until on-site IT restores the bridge.
                    status = 'bridge_offline'
                elif should_have_pin_base and has_invalid_pin:
                    status = 'invalid_pin_format'
                elif should_have_pin_base and not has_valid_pin:
                    status = 'no_gate_code'
                elif should_have_pin_base and has_valid_pin:
                    if esa_entry and esa_pin_value == plain_pin:
                        status = 'synced'
                    elif legacy_holds_pin and not esa_entry:
                        status = 'legacy_collision'
                    else:
                        status = 'push_pending'
                else:
                    # should_have_pin = False
                    if esa_entry:
                        status = 'revoke_pending'
                        reason = _audit_revoke_reason(
                            is_rented, b_rentable, is_gate_locked, is_overlocked, policy
                        )
                    elif legacy_holds_pin:
                        # Vacated unit, no ESA-owned entry, but the gate code
                        # is still on the device under a non-ESA name — we
                        # can't revoke it. Real security gap.
                        status = 'legacy_revoke_blocked'
                    else:
                        status = 'clean'

                results.append({
                    'site_id': a.site_id,
                    'unit_id': a.unit_id,
                    'keypad_pk': keypad_pk,
                    'keypad_slot': slot_label,
                    'device_id': device_id,
                    'status': status,
                    'reason': reason,
                    'is_rented': is_rented,
                    'is_gate_locked': is_gate_locked,
                    'is_overlocked': is_overlocked,
                    'b_rentable': b_rentable,
                    'has_gate_code': has_valid_pin,
                    'has_esa_pin': bool(esa_entry),
                    'pin_type': esa_entry.get('pinType') if esa_entry else None,
                })

        # Persist snapshot — upsert each row, replacing prior audit for the
        # same (site, unit, keypad_slot). Tagged with audited_by + audited_at.
        from sqlalchemy import text as _sql_text
        from datetime import datetime as _dt
        audited_at = _dt.utcnow()
        audited_by = _sl_username()
        if results:
            session.execute(_sql_text("""
                INSERT INTO mw_smart_lock_pin_audit_snapshot (
                    site_id, unit_id, keypad_pk, keypad_slot, device_id,
                    status, reason, is_rented, is_gate_locked, is_overlocked,
                    b_rentable, has_gate_code, has_esa_pin, pin_type,
                    audited_at, audited_by
                ) VALUES (
                    :site_id, :unit_id, :keypad_pk, :keypad_slot, :device_id,
                    :status, :reason, :is_rented, :is_gate_locked, :is_overlocked,
                    :b_rentable, :has_gate_code, :has_esa_pin, :pin_type,
                    :audited_at, :audited_by
                )
                ON CONFLICT (site_id, unit_id, keypad_slot) DO UPDATE SET
                    keypad_pk      = EXCLUDED.keypad_pk,
                    device_id      = EXCLUDED.device_id,
                    status         = EXCLUDED.status,
                    reason         = EXCLUDED.reason,
                    is_rented      = EXCLUDED.is_rented,
                    is_gate_locked = EXCLUDED.is_gate_locked,
                    is_overlocked  = EXCLUDED.is_overlocked,
                    b_rentable     = EXCLUDED.b_rentable,
                    has_gate_code  = EXCLUDED.has_gate_code,
                    has_esa_pin    = EXCLUDED.has_esa_pin,
                    pin_type       = EXCLUDED.pin_type,
                    audited_at     = EXCLUDED.audited_at,
                    audited_by     = EXCLUDED.audited_by
            """), [{**r, 'audited_at': audited_at, 'audited_by': audited_by} for r in results])

        _sl_audit(session, 'pin_audit', 'igloo',
                  detail=f'PIN audit for {len(results)} pair(s) across {len(site_ids)} site(s)')
        session.commit()

        return jsonify({
            'results': results,
            'count': len(results),
            'audited_at': audited_at.isoformat() + 'Z',
            'audited_by': audited_by,
        })
    except Exception:
        session.rollback()
        current_app.logger.exception("PIN audit error")
        return jsonify({'error': 'Failed to run PIN audit'}), 500
    finally:
        session.close()


@api_bp.route('/smart-lock/pin-audit/snapshot')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_sl_pin_audit_snapshot():
    """Return the most recently saved pin-audit results (no fresh Igloo call).

    Reads `mw_smart_lock_pin_audit_snapshot` filtered by site_ids. Returns the
    same row shape as /pin-audit plus per-row `audited_at` and a top-level
    max `audited_at`/`audited_by` for the latest entry seen.
    """
    site_ids_param = request.args.get('site_ids', '')
    if not site_ids_param:
        return jsonify({'error': 'site_ids parameter is required'}), 400
    try:
        site_ids = parse_site_ids(site_ids_param)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    session = current_app.get_middleware_session()
    try:
        from sqlalchemy import text as _sql_text
        rows = session.execute(_sql_text("""
            SELECT site_id, unit_id, keypad_pk, keypad_slot, device_id,
                   status, reason, is_rented, is_gate_locked, is_overlocked,
                   b_rentable, has_gate_code, has_esa_pin, pin_type,
                   audited_at, audited_by
            FROM mw_smart_lock_pin_audit_snapshot
            WHERE site_id = ANY(:site_ids)
            ORDER BY site_id, unit_id, keypad_slot
        """), {'site_ids': site_ids}).mappings().all()

        results = []
        latest_at = None
        latest_by = None
        for r in rows:
            d = dict(r)
            ts = d.pop('audited_at')
            by = d.pop('audited_by')
            d['audited_at'] = ts.isoformat() + 'Z' if ts else None
            d['audited_by'] = by
            results.append(d)
            if ts and (latest_at is None or ts > latest_at):
                latest_at = ts
                latest_by = by

        return jsonify({
            'results': results,
            'count': len(results),
            'audited_at': (latest_at.isoformat() + 'Z') if latest_at else None,
            'audited_by': latest_by,
        })
    except Exception:
        current_app.logger.exception("PIN audit snapshot read error")
        return jsonify({'error': 'Failed to read snapshot'}), 500
    finally:
        session.close()


@api_bp.route('/smart-lock/push-gate-pin', methods=['POST'])
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_sl_push_gate_pin():
    """Push a unit's SiteLink gate access code as a custom PIN to its assigned Igloo keypad."""
    from web.models.smart_lock import SmartLockUnitAssignment, SmartLockKeypad, GateAccessData
    from common.igloo_client import IglooClient, IglooAPIError

    data = request.get_json()
    if not data:
        return jsonify({'error': 'JSON body required'}), 400

    site_id = data.get('site_id')
    unit_id = data.get('unit_id')
    if not site_id or not unit_id:
        return jsonify({'error': 'site_id and unit_id required'}), 400
    try:
        site_id = int(site_id)
        unit_id = int(unit_id)
    except (ValueError, TypeError):
        return jsonify({'error': 'site_id and unit_id must be integers'}), 400

    session = current_app.get_middleware_session()
    try:
        # 1) Get the assignment and its keypad
        assignment = session.query(SmartLockUnitAssignment).filter_by(
            site_id=site_id, unit_id=unit_id
        ).first()
        if not assignment or not assignment.keypad_pk:
            return jsonify({'error': 'No keypad assigned to this unit'}), 404

        keypad = session.query(SmartLockKeypad).get(assignment.keypad_pk)
        if not keypad:
            return jsonify({'error': 'Keypad not found'}), 404
        device_id = keypad.keypad_id

        # 2) Get and decrypt the gate access code
        gate = session.query(GateAccessData).filter_by(
            site_id=site_id, unit_id=unit_id
        ).first()
        if not gate or not gate.access_code_enc:
            return jsonify({'error': 'No gate access code for this unit'}), 404

        from common.gate_access_crypto import get_gate_crypto
        crypto = get_gate_crypto()
        pin = crypto.decrypt(gate.access_code_enc)
        if not pin:
            return jsonify({'error': 'Gate access code is empty'}), 400

        # 3) Push to Igloo as permanent PIN via bridge.
        # Use the canonical ESA-{site}-{unit} tag so igloo_pin_sync recognizes
        # ownership and revokes on move-out. Plain unit_name leaves a dangling
        # PIN forever after the tenant leaves.
        from common.igloo_client import PIN_TYPE_PERMANENT
        from sync_service.pipelines.igloo_pin_sync import _esa_tag
        unit_name = gate.unit_name or f'Unit {unit_id}'
        access_name = _esa_tag(site_id, unit_id, gate.unit_name)
        client = IglooClient()
        result = client.create_pin_via_bridge(
            device_id, pin, access_name, pin_type=PIN_TYPE_PERMANENT,
        )

        # 4) Audit
        _sl_audit(session, 'gate_pin_pushed', 'igloo', device_id,
                  site_id=site_id, unit_id=unit_id,
                  detail=f'Pushed gate code to keypad {device_id} for unit {unit_name}')
        session.commit()

        return jsonify({'success': True, 'device_id': device_id, 'unit_name': unit_name}), 201
    except IglooAPIError:
        session.rollback()
        return jsonify({'error': 'Failed to push PIN to Igloo'}), 502
    except Exception as e:
        session.rollback()
        current_app.logger.exception("Push gate PIN error")
        return jsonify({'error': 'Failed to push gate PIN'}), 500
    finally:
        session.close()


# =============================================================================
# Igloo API Proxy Endpoints
# =============================================================================

_ISO8601_RE = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')


def _validate_iso_dt(value):
    """Validate an ISO-8601 datetime string. Returns None if absent, raises ValueError if malformed."""
    if not value:
        return None
    if not isinstance(value, str) or not _ISO8601_RE.match(value) or len(value) > 40:
        raise ValueError("Invalid ISO-8601 datetime format")
    return value

def _get_igloo_client():
    """Get or create IglooClient for the current request (per-request scope via g)."""
    from common.igloo_client import IglooClient
    if 'igloo_client' not in g:
        g.igloo_client = IglooClient()
    return g.igloo_client


@api_bp.route('/igloo/devices')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_igloo_devices():
    """List all Igloo devices."""
    department_id = request.args.get('department_id')
    try:
        client = _get_igloo_client()
        devices = client.list_devices(department_id=department_id)
        return jsonify({'devices': devices, 'count': len(devices)})
    except Exception as e:
        current_app.logger.exception("Igloo devices list error")
        return jsonify({'error': 'Failed to fetch Igloo devices'}), 500


@api_bp.route('/igloo/devices/<device_id>')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_igloo_device_detail(device_id):
    """Get single Igloo device detail."""
    try:
        client = _get_igloo_client()
        device = client.get_device(device_id)
        return jsonify({'device': device})
    except Exception as e:
        current_app.logger.exception("Igloo device detail error")
        return jsonify({'error': 'Failed to fetch device detail'}), 500


@api_bp.route('/igloo/devices/<device_id>/access')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_igloo_device_access(device_id):
    """List PINs/eKeys on a device."""
    try:
        client = _get_igloo_client()
        access_list = client.list_device_access(device_id)
        return jsonify({'access': access_list, 'count': len(access_list)})
    except Exception as e:
        current_app.logger.exception("Igloo device access error")
        return jsonify({'error': 'Failed to fetch device access codes'}), 500


@api_bp.route('/igloo/devices/<device_id>/activity')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_igloo_device_activity(device_id):
    """Get device unlock audit log."""
    limit = request.args.get('limit', 50, type=int)
    try:
        client = _get_igloo_client()
        activity = client.list_device_activity(device_id, limit=min(limit, 200))
        return jsonify({'activity': activity, 'count': len(activity)})
    except Exception as e:
        current_app.logger.exception("Igloo device activity error")
        return jsonify({'error': 'Failed to fetch device activity'}), 500


@api_bp.route('/igloo/devices/<device_id>/jobs')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_igloo_device_jobs(device_id):
    """Get device job history."""
    try:
        client = _get_igloo_client()
        jobs = client.list_device_jobs(device_id)
        return jsonify({'jobs': jobs, 'count': len(jobs)})
    except Exception as e:
        current_app.logger.exception("Igloo device jobs error")
        return jsonify({'error': 'Failed to fetch device jobs'}), 500


@api_bp.route('/igloo/departments')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_igloo_departments():
    """List Igloo departments (= sites)."""
    try:
        client = _get_igloo_client()
        departments = client.list_departments()
        return jsonify({'departments': departments, 'count': len(departments)})
    except Exception as e:
        current_app.logger.exception("Igloo departments error")
        return jsonify({'error': 'Failed to fetch departments'}), 500


@api_bp.route('/igloo/departments/<dept_id>/access')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_igloo_department_access(dept_id):
    """List all PINs across a department/site."""
    try:
        client = _get_igloo_client()
        access_list = client.list_department_access(dept_id)
        return jsonify({'access': access_list, 'count': len(access_list)})
    except Exception as e:
        current_app.logger.exception("Igloo department access error")
        return jsonify({'error': 'Failed to fetch department access codes'}), 500


@api_bp.route('/igloo/properties')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_igloo_properties():
    """List Igloo properties."""
    try:
        client = _get_igloo_client()
        properties = client.list_properties()
        return jsonify({'properties': properties, 'count': len(properties)})
    except Exception as e:
        current_app.logger.exception("Igloo properties error")
        return jsonify({'error': 'Failed to fetch properties'}), 500


# --- Igloo Write Operations (PIN management) ---

@api_bp.route('/igloo/devices/<device_id>/pin/custom', methods=['POST'])
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_igloo_create_custom_pin(device_id):
    """Create a PIN via the device's paired bridge.

    Body:
        pin (str)         — 4-10 digit PIN
        name (str)        — label, e.g. "ESA-{site}-{unit}"
        pin_type (str)    — "duration" (default), "permanent", or "otp"
        start_datetime    — ISO-8601 with offset (optional, defaults to now)
        end_datetime      — ISO-8601 with offset (required when pin_type=duration)
    """
    from common.igloo_client import (
        PIN_TYPE_DURATION, PIN_TYPE_PERMANENT, PIN_TYPE_OTP, IglooAPIError,
    )
    data = request.get_json()
    if not data or 'pin' not in data:
        return jsonify({'error': 'pin is required'}), 400

    pin = str(data['pin']).strip()
    name = (data.get('name') or '').strip()[:100] or 'Custom PIN'
    pin_type_str = (data.get('pin_type') or 'duration').lower()
    type_map = {
        'duration': PIN_TYPE_DURATION,
        'permanent': PIN_TYPE_PERMANENT,
        'otp': PIN_TYPE_OTP,
    }
    pin_type_int = type_map.get(pin_type_str)
    if pin_type_int is None:
        return jsonify({'error': 'pin_type must be duration, permanent, or otp'}), 400

    try:
        start_dt = _validate_iso_dt(data.get('start_datetime'))
        end_dt = _validate_iso_dt(data.get('end_datetime'))
    except ValueError:
        return jsonify({'error': 'Invalid datetime format — use ISO-8601'}), 400

    if not re.match(r'^\d{4,10}$', pin):
        return jsonify({'error': 'PIN must be 4-10 digits'}), 400

    session = get_session()
    try:
        client = _get_igloo_client()
        result = client.create_pin_via_bridge(
            device_id, pin, name,
            pin_type=pin_type_int,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        _sl_audit(session, 'igloo_pin_created', 'igloo', device_id,
                  detail=f'{pin_type_str} PIN created on {device_id}: {name}')
        session.commit()
        return jsonify({'success': True, 'result': result}), 201
    except IglooAPIError as e:
        session.rollback()
        current_app.logger.warning("Igloo create PIN failed: %s", e)
        return jsonify({'error': str(e)}), 502
    except Exception:
        session.rollback()
        current_app.logger.exception("Igloo create PIN error")
        return jsonify({'error': 'Failed to create PIN'}), 500
    finally:
        session.close()


@api_bp.route('/igloo/devices/<device_id>/pin/permanent', methods=['POST'])
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_igloo_create_permanent_pin(device_id):
    """Create a permanent custom PIN via the device's paired bridge.

    Body: pin (4-10 digits), name (label).
    """
    from common.igloo_client import PIN_TYPE_PERMANENT, IglooAPIError
    data = request.get_json() or {}
    pin = str(data.get('pin') or '').strip()
    name = (data.get('name') or '').strip()[:100] or 'Permanent PIN'
    if not re.match(r'^\d{4,10}$', pin):
        return jsonify({'error': 'pin (4-10 digits) is required'}), 400

    session = get_session()
    try:
        client = _get_igloo_client()
        result = client.create_pin_via_bridge(
            device_id, pin, name, pin_type=PIN_TYPE_PERMANENT,
        )
        _sl_audit(session, 'igloo_pin_created', 'igloo', device_id,
                  detail=f'Permanent PIN created on {device_id}: {name}')
        session.commit()
        return jsonify({'success': True, 'result': result}), 201
    except IglooAPIError as e:
        session.rollback()
        return jsonify({'error': str(e)}), 502
    except Exception:
        session.rollback()
        current_app.logger.exception("Igloo create permanent PIN error")
        return jsonify({'error': 'Failed to create permanent PIN'}), 500
    finally:
        session.close()


@api_bp.route('/igloo/devices/<device_id>/pin/otp', methods=['POST'])
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_igloo_create_otp_pin(device_id):
    """Create a one-time PIN via the device's paired bridge.

    Body: pin (4-10 digits), name (label).
    """
    from common.igloo_client import PIN_TYPE_OTP, IglooAPIError
    data = request.get_json() or {}
    pin = str(data.get('pin') or '').strip()
    name = (data.get('name') or '').strip()[:100] or 'OTP PIN'
    if not re.match(r'^\d{4,10}$', pin):
        return jsonify({'error': 'pin (4-10 digits) is required'}), 400

    session = get_session()
    try:
        client = _get_igloo_client()
        result = client.create_pin_via_bridge(
            device_id, pin, name, pin_type=PIN_TYPE_OTP,
        )
        _sl_audit(session, 'igloo_pin_created', 'igloo', device_id,
                  detail=f'OTP PIN created on {device_id}: {name}')
        session.commit()
        return jsonify({'success': True, 'result': result}), 201
    except IglooAPIError as e:
        session.rollback()
        return jsonify({'error': str(e)}), 502
    except Exception:
        session.rollback()
        current_app.logger.exception("Igloo create OTP PIN error")
        return jsonify({'error': 'Failed to create OTP PIN'}), 500
    finally:
        session.close()


# Algopin (daily/hourly) routes deleted: Igloo bridge does not proxy algorithmic
# PINs. /pin/daily and /pin/hourly were the only consumers.


@api_bp.route('/igloo/devices/<device_id>/ekey', methods=['POST'])
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_igloo_create_ekey(device_id):
    """Create a guest Bluetooth eKey."""
    data = request.get_json()
    if not data or 'name' not in data:
        return jsonify({'error': 'name is required'}), 400

    name = (data.get('name') or '').strip()[:100]
    try:
        start_dt = _validate_iso_dt(data.get('start_datetime'))
        end_dt = _validate_iso_dt(data.get('end_datetime'))
    except ValueError:
        return jsonify({'error': 'Invalid datetime format — use ISO-8601'}), 400

    if not name:
        return jsonify({'error': 'name is required'}), 400

    session = get_session()
    try:
        client = _get_igloo_client()
        result = client.create_ekey(device_id, name, start_dt=start_dt, end_dt=end_dt)
        _sl_audit(session, 'igloo_ekey_created', 'igloo', device_id,
                  detail=f'eKey created on {device_id}: {name}')
        session.commit()
        return jsonify({'success': True, 'result': result}), 201
    except Exception as e:
        session.rollback()
        current_app.logger.exception("Igloo create eKey error")
        return jsonify({'error': 'Failed to create eKey'}), 500
    finally:
        session.close()


@api_bp.route('/igloo/devices/<device_id>/access/<access_id>', methods=['DELETE'])
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_igloo_revoke_access(device_id, access_id):
    """Revoke a PIN via the device's paired bridge (executes immediately on-site)."""
    from common.igloo_client import IglooAPIError
    session = get_session()
    try:
        client = _get_igloo_client()
        result = client.delete_pin_via_bridge(device_id, access_id)
        _sl_audit(session, 'igloo_access_revoked', 'igloo', device_id,
                  detail=f'Revoked access {access_id} on {device_id}')
        session.commit()
        return jsonify({'success': True, 'result': result})
    except IglooAPIError as e:
        session.rollback()
        current_app.logger.warning("Igloo revoke failed: %s", e)
        return jsonify({'error': str(e)}), 502
    except Exception:
        session.rollback()
        current_app.logger.exception("Igloo revoke access error")
        return jsonify({'error': 'Failed to revoke access'}), 500
    finally:
        session.close()


# --- Igloo Data Sync ---
# Moved to orchestrator: POST /api/orchestrator/pipelines/igloo/run


# =============================================================================
# Call Scoring Rubric — admin endpoints
# =============================================================================

@api_bp.route('/call-scoring/config', methods=['GET'])
@require_auth
def api_call_scoring_get():
    """Return the active scoring rubric for the editor UI."""
    try:
        from common.scoring_config import get_active_config, _get_engine
        from sqlalchemy import text as _text
        cfg = get_active_config(force_refresh=True)
        version = cfg.get('_version', 1)

        # Also fetch updated_at / updated_by for the UI header
        engine = _get_engine()
        with engine.connect() as conn:
            row = conn.execute(_text("""
                SELECT updated_at, updated_by
                FROM call_scoring_config
                WHERE name = 'default' AND is_active = TRUE
                LIMIT 1
            """)).fetchone()

        return jsonify({
            'config': cfg,
            'version': version,
            'updated_at': row[0].isoformat() if row and row[0] else None,
            'updated_by': row[1] if row else None,
        })
    except Exception:
        current_app.logger.exception("Failed to load call scoring config")
        return jsonify({'error': 'Failed to load config'}), 500


@api_bp.route('/call-scoring/config', methods=['POST'])
@require_auth
def api_call_scoring_save():
    """Persist a new version of the scoring rubric."""
    try:
        from common.scoring_config import save_config, validate_config

        body = request.get_json(silent=True) or {}
        cfg = body.get('config')
        if not isinstance(cfg, dict):
            return jsonify({'error': 'Body must contain a "config" object'}), 400

        errors = validate_config(cfg)
        if errors:
            return jsonify({'error': 'Validation failed', 'errors': errors}), 400

        username = g.current_user.get('sub') if hasattr(g, 'current_user') else 'unknown'
        new_version = save_config(cfg, updated_by=username)
        return jsonify({'success': True, 'version': new_version})
    except ValueError as ve:
        return jsonify({'error': 'Validation failed', 'errors': [str(ve)]}), 400
    except Exception:
        current_app.logger.exception("Failed to save call scoring config")
        return jsonify({'error': 'Failed to save config'}), 500


@api_bp.route('/call-scoring/test', methods=['POST'])
@require_auth
def api_call_scoring_test():
    """Run the scorer against a pasted transcript using a (possibly draft) config."""
    try:
        from common.call_scorer import score_call

        body = request.get_json(silent=True) or {}
        transcript = (body.get('transcript') or '').strip()
        if not transcript:
            return jsonify({'error': 'transcript is required'}), 400
        if len(transcript) > 50000:
            return jsonify({'error': 'transcript too long (max 50000 chars)'}), 400

        config_override = body.get('config') if isinstance(body.get('config'), dict) else None

        result = score_call(
            transcript=transcript,
            direction=body.get('direction') or 'outbound',
            agent_name=body.get('agent_name') or '',
            customer_name=body.get('customer_name') or '',
            duration_sec=int(body.get('duration_sec') or 0),
            config_override=config_override,
        )
        return jsonify({'success': True, 'result': result})
    except Exception:
        current_app.logger.exception("Call scoring test failed")
        return jsonify({'error': 'Test failed'}), 500
