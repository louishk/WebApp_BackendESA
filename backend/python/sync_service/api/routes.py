"""
sync_service REST API — Flask blueprint mounted at /api/orchestrator.

Endpoints:
    GET  /api/orchestrator/pipelines                       List registered pipelines
    GET  /api/orchestrator/pipelines/<name>                Get pipeline config
    GET  /api/orchestrator/pipelines/<name>/freshness      Check current freshness (no run)
    POST /api/orchestrator/pipelines/<name>/ensure-fresh   Refresh if stale (primary MW endpoint)
    POST /api/orchestrator/pipelines/<name>/run            Force-run regardless of freshness
    GET  /api/orchestrator/runs                            Recent runs
    GET  /api/orchestrator/runs/<execution_id>             Single run detail
    GET  /api/orchestrator/stats                           Executor + pool metrics
    GET  /api/orchestrator/status                          Service health/status
"""

import logging
from datetime import datetime, timedelta, timezone

from flask import Blueprint, jsonify, request, current_app

from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.rate_limit import rate_limit_api

from sync_service.config import session_scope
from sync_service.executor import get_executor
from sync_service.freshness import check_freshness
from sync_service.models import SyncPipeline, SyncRun
from sync_service.registry import get_pipeline, list_pipelines

logger = logging.getLogger(__name__)

sync_service_bp = Blueprint('orchestrator', __name__, url_prefix='/api/orchestrator')


# ----- Validation -----

_MAX_TIMEOUT = 300.0
_MAX_SCOPE_KEYS = 20


def _validate_scope(scope):
    if scope is None:
        return True, None, {}
    if not isinstance(scope, dict):
        return False, 'scope must be an object', None
    if len(scope) > _MAX_SCOPE_KEYS:
        return False, f'scope has too many keys (max {_MAX_SCOPE_KEYS})', None
    for k, v in scope.items():
        if not isinstance(k, str):
            return False, 'scope keys must be strings', None
        if isinstance(v, dict):
            return False, f'scope.{k} cannot be a nested object', None
        if isinstance(v, list) and len(v) > 100:
            return False, f'scope.{k} list too long (max 100)', None
    return True, None, scope


def _parse_timeout(raw, default: float) -> float:
    try:
        t = float(raw) if raw is not None else default
    except (TypeError, ValueError):
        return default
    return max(1.0, min(_MAX_TIMEOUT, t))


# ----- Pipeline registry endpoints -----

@sync_service_bp.route('/pipelines')
@require_auth
@require_api_scope('sync:read')
def list_pipelines_endpoint():
    """List all sync_service pipelines."""
    rows = list_pipelines(enabled_only=False)
    return jsonify({
        'pipelines': [r.to_dict() for r in rows],
        'count': len(rows),
    })


@sync_service_bp.route('/pipelines/<name>')
@require_auth
@require_api_scope('sync:read')
def get_pipeline_endpoint(name):
    row = get_pipeline(name)
    if row is None:
        return jsonify({'error': 'Pipeline not found'}), 404
    return jsonify(row.to_dict())


# ----- Freshness + execution endpoints -----

@sync_service_bp.route('/pipelines/<name>/freshness')
@require_auth
@require_api_scope('sync:read')
def freshness_endpoint(name):
    """Check current data freshness for a pipeline without triggering any run.

    Query params:
        site_code=L017       Single scope value
        site_codes=L017,L018 Comma-separated
    """
    row = get_pipeline(name)
    if row is None:
        return jsonify({'error': 'Pipeline not found'}), 404

    # Build scope from query string
    scope = {}
    if 'site_code' in request.args:
        scope['site_code'] = request.args.get('site_code')
    if 'site_codes' in request.args:
        scope['site_codes'] = [s for s in request.args.get('site_codes', '').split(',') if s]

    try:
        age = check_freshness(row, scope or None)
    except Exception as e:
        logger.warning(f"freshness check failed for {name}: {e}")
        return jsonify({'error': 'Freshness check failed'}), 500

    return jsonify({
        'pipeline_name': name,
        'scope': scope,
        'age_seconds': age,
        'ttl_seconds': row.freshness_ttl_seconds,
        'is_stale': age is None or age > row.freshness_ttl_seconds,
        'is_fresh': age is not None and age <= row.freshness_ttl_seconds,
    })


@sync_service_bp.route('/pipelines/<name>/ensure-fresh', methods=['POST'])
@require_auth
@require_api_scope('sync:write')
@rate_limit_api(max_requests=60, window_seconds=60)
def ensure_fresh_endpoint(name):
    """Refresh data if stale — the primary endpoint for middleware callers.

    Body:
        {
            "scope": {"site_codes": ["L017"]},
            "max_age_seconds": 300,
            "timeout": 30
        }
    """
    data = request.get_json(silent=True) or {}

    scope = data.get('scope')
    ok, err, scope = _validate_scope(scope)
    if not ok:
        return jsonify({'error': err}), 400

    max_age = data.get('max_age_seconds')
    if max_age is not None:
        try:
            max_age = int(max_age)
            if max_age < 0:
                raise ValueError
        except (TypeError, ValueError):
            return jsonify({'error': 'max_age_seconds must be a non-negative integer'}), 400

    timeout = _parse_timeout(data.get('timeout'), default=60.0)

    # Identify caller for audit trail
    caller = None
    if hasattr(request, 'api_key_id'):
        caller = f'api_key:{request.api_key_id}'

    executor = get_executor()
    result = executor.ensure_fresh(
        pipeline_name=name,
        scope=scope,
        max_age_seconds=max_age,
        timeout=timeout,
        triggered_by='api',
        triggered_by_detail=caller,
    )

    http_status = 200 if result.success else 500
    return jsonify(result.to_dict()), http_status


@sync_service_bp.route('/pipelines/<name>/run', methods=['POST'])
@require_auth
@require_api_scope('sync:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def run_endpoint(name):
    """Force-run a pipeline regardless of freshness.

    Body:
        {
            "scope": {"site_codes": ["L017"]},
            "timeout": 120
        }
    """
    data = request.get_json(silent=True) or {}

    scope = data.get('scope')
    ok, err, scope = _validate_scope(scope)
    if not ok:
        return jsonify({'error': err}), 400

    timeout = _parse_timeout(data.get('timeout'), default=300.0)

    caller = None
    if hasattr(request, 'api_key_id'):
        caller = f'api_key:{request.api_key_id}'

    executor = get_executor()
    result = executor.run(
        pipeline_name=name,
        scope=scope,
        timeout=timeout,
        triggered_by='api',
        triggered_by_detail=caller,
    )

    http_status = 200 if result.success else 500
    return jsonify(result.to_dict()), http_status


# ----- Run history -----

@sync_service_bp.route('/runs')
@require_auth
@require_api_scope('sync:read')
def list_runs_endpoint():
    """Recent sync_runs. Query params: pipeline, limit, since_hours."""
    pipeline_name = request.args.get('pipeline')
    try:
        limit = min(500, max(1, int(request.args.get('limit', 50))))
    except (TypeError, ValueError):
        limit = 50
    try:
        since_hours = float(request.args.get('since_hours', 24))
    except (TypeError, ValueError):
        since_hours = 24

    since = datetime.now(timezone.utc) - timedelta(hours=since_hours)

    with session_scope() as session:
        q = session.query(SyncRun).filter(SyncRun.queued_at >= since)
        if pipeline_name:
            q = q.filter(SyncRun.pipeline_name == pipeline_name)
        rows = q.order_by(SyncRun.queued_at.desc()).limit(limit).all()
        return jsonify({
            'runs': [r.to_dict() for r in rows],
            'count': len(rows),
        })


@sync_service_bp.route('/runs/<execution_id>')
@require_auth
@require_api_scope('sync:read')
def get_run_endpoint(execution_id):
    with session_scope() as session:
        row = session.query(SyncRun).filter_by(execution_id=execution_id).first()
        if row is None:
            return jsonify({'error': 'Run not found'}), 404
        return jsonify(row.to_dict())


# ----- Stats -----

@sync_service_bp.route('/stats')
@require_auth
@require_api_scope('sync:read')
def stats_endpoint():
    """Live executor + resource pool metrics."""
    executor = get_executor()
    return jsonify(executor.stats())


@sync_service_bp.route('/status')
def status_endpoint():
    """Public health endpoint for admin/services page (no auth — like /health).

    Returns service status + counts, used by /api/services/status aggregator.
    """
    import os
    from sync_service.models import SyncServiceState

    try:
        executor = get_executor()
        exec_stats = executor.stats()
    except Exception as e:
        logger.warning(f"orchestrator status: executor init failed: {e}")
        exec_stats = {'in_flight': 0, 'registered_pipelines': []}

    daemon_info = {}
    try:
        with session_scope() as session:
            rows = session.query(SyncPipeline).all()
            total = len(rows)
            enabled = sum(1 for r in rows if r.enabled)

            state = session.query(SyncServiceState).filter_by(id=1).first()
            if state:
                daemon_info = {
                    'pid': state.pid,
                    'host': state.host_name,
                    'started_at': state.started_at.isoformat() if state.started_at else None,
                    'last_heartbeat': state.last_heartbeat.isoformat() if state.last_heartbeat else None,
                    'daemon_status': state.status,
                }
    except Exception as e:
        logger.warning(f"orchestrator status: registry read failed: {e}")
        total = enabled = 0

    return jsonify({
        'service': 'Sync Orchestrator',
        'status': 'running',  # API is in-process — if this responds, we're up
        'pid': os.getpid(),
        'pipelines_total': total,
        'pipelines_enabled': enabled,
        'in_flight': exec_stats.get('in_flight', 0),
        'registered': len(exec_stats.get('registered_pipelines', [])),
        'daemon': daemon_info,
        'resources': exec_stats.get('resource_pool', {}),
    })


# ----- Dashboard helpers -----

@sync_service_bp.route('/data-freshness')
@require_auth
@require_api_scope('sync:read')
def data_freshness_endpoint():
    """Return {pipeline_name: {latest_date, age_seconds, ttl_seconds, status}}."""
    from sqlalchemy import text as _text
    from sync_service.config import get_engine
    from datetime import date as _date, time as _time

    out = {}
    now = datetime.now(timezone.utc)
    with session_scope() as session:
        pipelines = session.query(SyncPipeline).all()
        for p in pipelines:
            table = (p.freshness_table or '').strip()
            col = (p.freshness_column or 'updated_at').strip()
            db_name = p.freshness_database or 'middleware'
            entry = {
                'latest_date': None,
                'age_seconds': None,
                'ttl_seconds': p.freshness_ttl_seconds,
                'status': 'unknown',
                'display_name': p.display_name,
                'destination': f"{p.freshness_database or 'middleware'}.{p.freshness_table or ''}".rstrip('.'),
                'freshness_database': p.freshness_database,
                'freshness_table': p.freshness_table,
                'frequency_category': p.frequency_category,
                'resolved_frequency_category': p.resolved_frequency_category,
                'schedule_type': p.schedule_type,
            }

            if not table:
                out[p.pipeline_name] = entry
                continue

            try:
                eng = get_engine(db_name)
                with eng.connect() as conn:
                    row = conn.execute(_text(
                        f'SELECT MAX("{col}") FROM "{table}"'
                    )).first()
                    latest = row[0] if row else None

                if latest is not None:
                    # Normalize date → datetime (some columns are DATE not TIMESTAMP)
                    if not isinstance(latest, datetime) and isinstance(latest, _date):
                        latest = datetime.combine(latest, _time.min)
                    if latest.tzinfo is None:
                        latest = latest.replace(tzinfo=timezone.utc)
                    age = (now - latest).total_seconds()
                    entry['latest_date'] = latest.isoformat()
                    entry['age_seconds'] = int(age)
                    entry['status'] = 'fresh' if age <= p.freshness_ttl_seconds else 'stale'
            except Exception as e:
                logger.warning(f"freshness query failed for {p.pipeline_name}: {e}")

            out[p.pipeline_name] = entry
    return jsonify(out)


@sync_service_bp.route('/upcoming')
@require_auth
@require_api_scope('sync:read')
def upcoming_endpoint():
    """Return upcoming cron-scheduled runs sorted by next_run ascending."""
    from croniter import croniter
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        ZoneInfo = None

    sgt = ZoneInfo('Asia/Singapore') if ZoneInfo else timezone(timedelta(hours=8))
    now_utc = datetime.now(timezone.utc)
    now_sgt = now_utc.astimezone(sgt)
    items = []
    with session_scope() as session:
        pipelines = (session.query(SyncPipeline)
                     .filter(SyncPipeline.enabled.is_(True))
                     .all())
        for p in pipelines:
            stype = (p.schedule_type or 'on_demand').lower()
            cfg = p.schedule_config or {}
            cron_expr = cfg.get('cron') or cfg.get('expression')
            interval_secs = cfg.get('interval_seconds')

            next_run = None
            schedule_human = stype
            if stype == 'cron' and cron_expr:
                try:
                    # Compute next fire in SGT so cron is interpreted in the
                    # same timezone the daemon uses.
                    next_sgt = croniter(cron_expr, now_sgt).get_next(datetime)
                    if next_sgt.tzinfo is None:
                        next_sgt = next_sgt.replace(tzinfo=sgt)
                    next_run = next_sgt.astimezone(timezone.utc)
                    schedule_human = f"cron: {cron_expr} (SGT)"
                except Exception as e:
                    logger.debug(f"bad cron for {p.pipeline_name}: {e}")
                    continue
            elif stype == 'interval' and interval_secs:
                next_run = now_utc + timedelta(seconds=int(interval_secs))
                schedule_human = f"every {int(interval_secs)}s"
            else:
                continue

            items.append({
                'pipeline_name': p.pipeline_name,
                'display_name': p.display_name or p.pipeline_name,
                'schedule_type': stype,
                'schedule_human': schedule_human,
                'next_run': next_run.isoformat(),
                'seconds_until': max(0, int((next_run - now_utc).total_seconds())),
            })

    items.sort(key=lambda x: x['seconds_until'])
    return jsonify({'upcoming': items, 'count': len(items)})


# ----- Edit (Phase A2) -----

@sync_service_bp.route('/pipelines/<name>', methods=['PATCH'])
@require_auth
@require_api_scope('sync:write')
def update_pipeline_endpoint(name):
    """Update pipeline settings — enabled, schedule, default_args, TTL."""
    payload = request.get_json(silent=True) or {}
    if not isinstance(payload, dict):
        return jsonify({'error': 'Body must be a JSON object'}), 400

    allowed = {
        'enabled', 'schedule_type', 'schedule_config',
        'default_args', 'freshness_ttl_seconds', 'display_name', 'description',
        'frequency_category',
        'freshness_table', 'freshness_column', 'freshness_database',
        'max_retries', 'retry_delay_seconds', 'timeout_seconds',
    }
    unknown = set(payload.keys()) - allowed
    if unknown:
        return jsonify({'error': f'Unknown fields: {sorted(unknown)}'}), 400

    # Light validation
    if 'schedule_type' in payload and payload['schedule_type'] not in ('cron', 'interval', 'on_demand'):
        return jsonify({'error': 'schedule_type must be cron|interval|on_demand'}), 400
    if 'schedule_config' in payload:
        cfg = payload['schedule_config']
        if not isinstance(cfg, dict):
            return jsonify({'error': 'schedule_config must be an object'}), 400
        if 'cron' in cfg:
            from croniter import croniter, CroniterBadCronError
            try:
                croniter(cfg['cron'])
            except (CroniterBadCronError, Exception) as e:
                return jsonify({'error': f'Invalid cron: {e}'}), 400
    if 'default_args' in payload and not isinstance(payload['default_args'], dict):
        return jsonify({'error': 'default_args must be an object'}), 400

    # frequency_category: '' or 'auto' → NULL (auto-derive from cron)
    if 'frequency_category' in payload:
        fc = payload['frequency_category']
        if fc in (None, '', 'auto'):
            payload['frequency_category'] = None
        elif fc not in ('high', 'med', 'low'):
            return jsonify({'error': 'frequency_category must be one of: high, med, low, auto'}), 400

    if 'freshness_database' in payload and payload['freshness_database'] not in ('middleware', 'pbi', 'backend'):
        return jsonify({'error': 'freshness_database must be middleware|pbi|backend'}), 400

    import re
    _IDENT = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
    for ident_field in ('freshness_table', 'freshness_column'):
        if ident_field in payload:
            v = payload[ident_field]
            if v in (None, ''):
                payload[ident_field] = None
                continue
            if not isinstance(v, str) or len(v) > 100 or not _IDENT.match(v):
                return jsonify({'error': f'{ident_field} must be a valid SQL identifier (≤100 char)'}), 400

    _NUMERIC_BOUNDS = {
        'freshness_ttl_seconds': (0, 7 * 86400),
        'timeout_seconds': (1, 86400),
        'max_retries': (0, 10),
        'retry_delay_seconds': (0, 3600),
    }
    for f, (lo, hi) in _NUMERIC_BOUNDS.items():
        if f in payload:
            v = payload[f]
            if not isinstance(v, int) or isinstance(v, bool) or v < lo or v > hi:
                return jsonify({'error': f'{f} must be an integer in [{lo}, {hi}]'}), 400

    with session_scope() as session:
        row = session.query(SyncPipeline).filter_by(pipeline_name=name).first()
        if row is None:
            return jsonify({'error': 'Pipeline not found'}), 404
        for k, v in payload.items():
            setattr(row, k, v)
        session.flush()
        result = row.to_dict()
    return jsonify({'status': 'updated', 'pipeline': result})
