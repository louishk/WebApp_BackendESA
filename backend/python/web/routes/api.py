"""
REST API routes for scheduler.
Refactored from app.py to use blueprint pattern.
"""

import os
from datetime import datetime, timedelta
import pytz
from pathlib import Path
from uuid import uuid4, UUID
import threading

import re

from functools import wraps
from flask import Blueprint, jsonify, request, current_app, g
from sqlalchemy import desc, func, case, text

from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.rate_limit import rate_limit_api
from web.utils.validators import (
    parse_site_ids, parse_pagination, parse_date_param, validate_array_size,
    MAX_SITE_IDS, MAX_ARRAY_SIZE,
)

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
        ttl_seconds: Cache time-to-live in seconds (default 30s)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_key = f"{func.__name__}:{request.path}:{request.query_string.decode()}"
            path = _cache_path(cache_key)

            # Check file cache
            try:
                if os.path.exists(path):
                    mtime = os.path.getmtime(path)
                    if (datetime.now().timestamp() - mtime) < ttl_seconds:
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


def _get_scheduler_config():
    """Load scheduler config from DB (source of truth), falling back to YAML."""
    from scheduler.config import SchedulerConfig
    session = current_app.get_db_session()
    try:
        return SchedulerConfig.from_db(session)
    finally:
        session.close()


# PBI database session factory (for RentRoll, SiteInfo, etc.)
_pbi_engine = None
_pbi_session_factory = None


def get_pbi_session():
    """Get PBI database session for RentRoll/SiteInfo queries."""
    global _pbi_engine, _pbi_session_factory
    if _pbi_engine is None:
        from common.config_loader import get_database_url
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
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


# =============================================================================
# Status & Health
# =============================================================================

@api_bp.route('/status')
@require_auth
@require_api_scope('scheduler:read')
@cached(ttl_seconds=10)
def api_status():
    """Get scheduler status."""
    from scheduler import __version__
    from scheduler.models import SchedulerState, JobHistory
    from scheduler.resource_manager import get_resource_manager

    session = get_session()
    try:
        state = session.query(SchedulerState).filter_by(id=1).first()

        rm = get_resource_manager()
        resources = rm.get_all_usage_dict()

        running_count = session.query(JobHistory).filter_by(status='running').count()

        if state and state.status == 'running':
            uptime = None
            if state.started_at:
                uptime = (datetime.now() - state.started_at.replace(tzinfo=None)).total_seconds()

            return jsonify({
                'status': state.status,
                'mode': 'scheduler',
                'started_at': state.started_at.isoformat() if state.started_at else None,
                'uptime_seconds': uptime,
                'host_name': state.host_name,
                'pid': state.pid,
                'last_heartbeat': state.last_heartbeat.isoformat() if state.last_heartbeat else None,
                'version': state.version,
                'resources': resources,
                'running_jobs': running_count,
            })
        else:
            web_uptime = (datetime.now() - current_app.web_started_at).total_seconds()
            return jsonify({
                'status': 'web_ui_only',
                'mode': 'standalone',
                'started_at': current_app.web_started_at.isoformat(),
                'uptime_seconds': web_uptime,
                'version': __version__,
                'resources': resources,
                'running_jobs': running_count,
            })
    finally:
        session.close()


@api_bp.route('/health')
@rate_limit_api(max_requests=30, window_seconds=60)
def health():
    """Health check endpoint with dependency probes."""
    from web.utils.health import run_health_checks
    body, status = run_health_checks()
    return jsonify(body), status


# =============================================================================
# Jobs
# =============================================================================

@api_bp.route('/jobs')
@require_auth
@require_api_scope('scheduler:read')
def api_list_jobs():
    """List all scheduled jobs."""
    from scheduler.utils import cron_to_human

    config = _get_scheduler_config()

    jobs = []
    for name, pipeline in config.pipelines.items():
        cron_expr = pipeline.schedule_config.get('cron', 'N/A')
        jobs.append({
            'pipeline_name': name,
            'display_name': pipeline.display_name,
            'schedule': cron_expr,
            'schedule_human': cron_to_human(cron_expr),
            'enabled': pipeline.enabled,
            'priority': pipeline.priority,
            'resource_group': pipeline.resource_group,
            'timeout_seconds': pipeline.timeout_seconds,
            'freshness_table': pipeline.data_freshness.table,
            'freshness_column': pipeline.data_freshness.date_column,
        })

    jobs.sort(key=lambda x: x['priority'])
    return jsonify({'jobs': jobs})


@api_bp.route('/jobs/<pipeline>')
@require_auth
@require_api_scope('scheduler:read')
def api_get_job(pipeline):
    """Get job details."""
    from scheduler.utils import cron_to_human

    config = _get_scheduler_config()

    if pipeline not in config.pipelines:
        return jsonify({'error': 'Pipeline not found'}), 404

    p = config.pipelines[pipeline]
    cron_expr = p.schedule_config.get('cron', 'N/A')

    return jsonify({
        'pipeline_name': pipeline,
        'display_name': p.display_name,
        'module_path': p.module_path,
        'schedule_type': p.schedule_type,
        'schedule_config': p.schedule_config,
        'schedule_human': cron_to_human(cron_expr),
        'enabled': p.enabled,
        'priority': p.priority,
        'depends_on': p.depends_on,
        'conflicts_with': p.conflicts_with,
        'resource_group': p.resource_group,
        'max_db_connections': p.max_db_connections,
        'max_retries': p.retry.max_attempts,
        'timeout_seconds': p.timeout_seconds,
    })


@api_bp.route('/jobs/<pipeline>', methods=['PUT'])
@require_auth
@require_api_scope('scheduler:write')
@rate_limit_api(max_requests=20, window_seconds=60)
def api_update_job(pipeline):
    """Update pipeline schedule/settings."""
    from scheduler.models import PipelineConfig
    from scheduler.utils import cron_to_human

    session = get_session()
    try:
        row = session.query(PipelineConfig).filter_by(pipeline_name=pipeline).first()
        if not row:
            return jsonify({'error': 'Pipeline not found'}), 404

        data = request.get_json() or {}

        cron = data.get('cron')
        enabled = data.get('enabled')
        priority = data.get('priority')

        if priority is not None:
            try:
                priority = int(priority)
                if not 1 <= priority <= 10:
                    return jsonify({'error': 'Priority must be between 1 and 10'}), 400
            except (ValueError, TypeError):
                return jsonify({'error': 'Priority must be an integer'}), 400

        if cron:
            try:
                from croniter import croniter
                croniter(cron)
            except Exception as e:
                current_app.logger.warning(f"Invalid cron expression '{cron}': {e}")
                return jsonify({'error': 'Invalid cron expression'}), 400

        if cron is not None:
            schedule_config = dict(row.schedule_config or {})
            schedule_config['cron'] = cron
            row.schedule_config = schedule_config
        if enabled is not None:
            row.enabled = enabled
        if priority is not None:
            row.priority = priority

        session.commit()

        cron_expr = (row.schedule_config or {}).get('cron', 'N/A')
        return jsonify({
            'success': True,
            'pipeline_name': pipeline,
            'schedule': cron_expr,
            'schedule_human': cron_to_human(cron_expr),
            'enabled': row.enabled,
            'priority': row.priority,
        })
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Failed to update job {pipeline}: {e}")
        return jsonify({'error': 'Failed to update pipeline'}), 500
    finally:
        session.close()


@api_bp.route('/jobs/<pipeline>/enable', methods=['POST'])
@require_auth
@require_api_scope('scheduler:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def api_enable_job(pipeline):
    """Enable a pipeline."""
    from scheduler.models import PipelineConfig

    session = get_session()
    try:
        row = session.query(PipelineConfig).filter_by(pipeline_name=pipeline).first()
        if not row:
            return jsonify({'error': 'Pipeline not found'}), 404
        row.enabled = True
        session.commit()
        return jsonify({'success': True, 'enabled': True})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Failed to enable job {pipeline}: {e}")
        return jsonify({'error': 'Failed to enable pipeline'}), 500
    finally:
        session.close()


@api_bp.route('/jobs/<pipeline>/disable', methods=['POST'])
@require_auth
@require_api_scope('scheduler:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def api_disable_job(pipeline):
    """Disable a pipeline."""
    from scheduler.models import PipelineConfig

    session = get_session()
    try:
        row = session.query(PipelineConfig).filter_by(pipeline_name=pipeline).first()
        if not row:
            return jsonify({'error': 'Pipeline not found'}), 404
        row.enabled = False
        session.commit()
        return jsonify({'success': True, 'enabled': False})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Failed to disable job {pipeline}: {e}")
        return jsonify({'error': 'Failed to disable pipeline'}), 500
    finally:
        session.close()


@api_bp.route('/schedules/presets')
@require_auth
@require_api_scope('scheduler:read')
def api_schedule_presets():
    """Get available schedule presets."""
    from scheduler.utils import SCHEDULE_PRESETS
    return jsonify({'presets': SCHEDULE_PRESETS})


@api_bp.route('/jobs/upcoming')
@require_auth
@require_api_scope('scheduler:read')
def api_upcoming_jobs():
    """Get upcoming scheduled executions."""
    from scheduler.utils import cron_to_human
    import pytz

    config = _get_scheduler_config()

    sg_tz = pytz.timezone('Asia/Singapore')
    now = datetime.now(sg_tz)

    upcoming = []
    for name, pipeline in config.pipelines.items():
        if not pipeline.enabled:
            continue

        cron_expr = pipeline.schedule_config.get('cron')
        if not cron_expr:
            continue

        try:
            from croniter import croniter
            cron = croniter(cron_expr, now)
            next_run = cron.get_next(datetime)
            seconds_until = (next_run - now).total_seconds()

            upcoming.append({
                'pipeline_name': name,
                'display_name': pipeline.display_name,
                'schedule': cron_expr,
                'schedule_human': cron_to_human(cron_expr),
                'next_run': next_run.isoformat(),
                'seconds_until': int(seconds_until),
            })
        except Exception:
            continue

    upcoming.sort(key=lambda x: x['seconds_until'])
    return jsonify({'upcoming': upcoming})


def _is_valid_sql_identifier(name):
    """Validate that a string is a safe SQL identifier (table/column name)."""
    import re
    if not name or not isinstance(name, str):
        return False
    # Allow alphanumeric, underscores, and dots (for schema.table)
    # Must start with letter or underscore
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$', name))


def _validate_module_path(module_path):
    """Validate module_path is a safe Python module reference within datalayer."""
    if not module_path or not isinstance(module_path, str):
        return False, 'module_path is required'
    if not module_path.startswith('datalayer.'):
        return False, 'module_path must start with "datalayer."'
    if not re.match(r'^datalayer(\.[a-zA-Z_][a-zA-Z0-9_]*)+$', module_path):
        return False, 'module_path contains invalid characters'
    return True, 'valid'


_SYNC_CONFIG_ALLOWED_KEYS = frozenset({
    'strategy', 'watermark_field', 'phases', 'validation',
    'checkpoint_interval', 'lookback_window',
})


def _validate_sync_config(sc):
    """Validate sync_config JSONB shape. Returns (is_valid, error_msg)."""
    if sc is None:
        return True, None
    if not isinstance(sc, dict):
        return False, 'sync_config must be an object'
    unknown = set(sc.keys()) - _SYNC_CONFIG_ALLOWED_KEYS
    if unknown:
        return False, f'Unknown sync_config keys: {", ".join(sorted(unknown))}'
    if 'strategy' in sc and sc['strategy'] not in ('watermark', 'lookback', 'full'):
        return False, 'sync_config.strategy must be watermark, lookback, or full'
    return True, None


def _validate_pipeline_specific_args(args):
    """Validate pipeline_specific_args JSONB — must be a flat dict with scalar/list values."""
    if args is None:
        return True, None
    if not isinstance(args, dict):
        return False, 'pipeline_specific_args must be an object'
    for k, v in args.items():
        if not isinstance(k, str):
            return False, 'pipeline_specific_args keys must be strings'
        if isinstance(v, dict):
            # Allow one level of nesting (e.g., property_site_map)
            for sk, sv in v.items():
                if isinstance(sv, (dict, list)):
                    return False, f'pipeline_specific_args.{k} has too-deep nesting'
        elif not isinstance(v, (str, int, float, bool, list, type(None))):
            return False, f'pipeline_specific_args.{k} has unsupported type'
    return True, None


@api_bp.route('/data-freshness')
@require_auth
@require_api_scope('scheduler:read')
@cached(ttl_seconds=60)
def api_data_freshness():
    """Get latest data dates for all pipelines."""
    from scheduler.config import get_pbi_engine

    config = _get_scheduler_config()
    freshness = {}

    # Tables allowed for freshness queries on the backend DB (allowlist).
    # Empty — all active pipelines read from pbi/middleware. Leave the guard
    # in place so any future backend-DB pipeline must be explicitly allowlisted.
    BACKEND_FRESHNESS_TABLES = frozenset()

    # Separate queries by database
    pbi_queries = []
    backend_queries = []
    for name, pipeline in config.pipelines.items():
        table = pipeline.data_freshness.table
        column = pipeline.data_freshness.date_column
        db = pipeline.data_freshness.database

        if not table:
            freshness[name] = {'latest_date': None, 'error': 'No table configured'}
        elif not _is_valid_sql_identifier(table) or not _is_valid_sql_identifier(column):
            freshness[name] = {'latest_date': None, 'error': 'Invalid table or column name'}
        elif db == 'backend':
            if table not in BACKEND_FRESHNESS_TABLES:
                freshness[name] = {'latest_date': None, 'error': 'Table not permitted'}
            else:
                backend_queries.append((name, table, column))
        else:
            pbi_queries.append((name, table, column))

    def _run_freshness_queries(engine, queries):
        """Run freshness queries against an engine, rolling back on per-query errors."""
        try:
            with engine.connect() as conn:
                for name, table, column in queries:
                    try:
                        query = text(f'SELECT MAX("{column}") as max_date FROM "{table}"')
                        result = conn.execute(query).fetchone()

                        if result and result[0]:
                            max_date = result[0]
                            if hasattr(max_date, 'isoformat'):
                                freshness[name] = {'latest_date': max_date.isoformat()}
                            else:
                                freshness[name] = {'latest_date': str(max_date)}
                        else:
                            freshness[name] = {'latest_date': None}
                    except Exception as e:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        current_app.logger.error(f"Data freshness query error for {name}: {e}")
                        freshness[name] = {'latest_date': None, 'error': 'Query failed'}
        except Exception as e:
            current_app.logger.error(f"Data freshness connection error: {e}")
            for name, _, _ in queries:
                freshness[name] = {'latest_date': None, 'error': 'Database connection failed'}

    # Query PBI database
    if pbi_queries:
        try:
            pbi_engine = get_pbi_engine(config)
            _run_freshness_queries(pbi_engine, pbi_queries)
        except Exception as e:
            current_app.logger.error(f"Could not connect to PBI database: {e}")
            for name, _, _ in pbi_queries:
                freshness[name] = {'latest_date': None, 'error': 'Database connection failed'}

    # Query backend database (allowlisted tables only)
    if backend_queries:
        try:
            from common.config_loader import get_database_url
            from sqlalchemy import create_engine
            backend_engine = create_engine(get_database_url('backend'))
            try:
                _run_freshness_queries(backend_engine, backend_queries)
            finally:
                backend_engine.dispose()
        except Exception as e:
            current_app.logger.error(f"Could not connect to backend database: {e}")
            for name, _, _ in backend_queries:
                freshness[name] = {'latest_date': None, 'error': 'Database connection failed'}

    return jsonify(freshness)


@api_bp.route('/jobs/<pipeline>/run-async', methods=['POST'])
@require_auth
@require_api_scope('scheduler:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def api_run_job_async(pipeline):
    """Trigger job execution asynchronously."""
    from scheduler.executor import PipelineExecutor
    from scheduler.models import JobHistory
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    config = _get_scheduler_config()

    if pipeline not in config.pipelines:
        return jsonify({'error': 'Pipeline not found'}), 404

    data = request.get_json() or {}
    mode = data.get('mode', 'auto')
    args = data.get('args', {})
    args['mode'] = mode

    p = config.pipelines[pipeline]
    execution_id = uuid4()

    final_args = dict(p.default_args)
    final_args.update(args)

    db_url = current_app.db_url

    def run_pipeline():
        from scheduler.resource_manager import get_resource_manager

        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        session = Session()

        job_history = JobHistory(
            job_id=f"{pipeline}_{execution_id}",
            pipeline_name=pipeline,
            execution_id=execution_id,
            status='running',
            priority=p.priority,
            scheduled_at=now_utc(),
            started_at=now_utc(),
            mode=mode,
            parameters=final_args,
            triggered_by='web'
        )
        session.add(job_history)
        session.commit()

        rm = get_resource_manager()
        resource_group = p.resource_group
        db_slots = p.max_db_connections

        try:
            with rm.acquire(resource_group, count=1, timeout=300, job_id=str(execution_id)):
                with rm.acquire('db_pool', count=db_slots, timeout=300, job_id=str(execution_id)):
                    executor = PipelineExecutor()
                    result = executor.execute_streaming(
                        module_path=p.module_path,
                        args=final_args,
                        execution_id=execution_id,
                        timeout_seconds=p.timeout_seconds
                    )
        except TimeoutError as e:
            from scheduler.executor import ExecutionResult
            result = ExecutionResult(
                success=False,
                exit_code=-1,
                stdout='',
                stderr=str(e),
                duration_seconds=0,
                error_message=f"Resource acquisition timeout: {e}"
            )

        job_history.completed_at = now_utc()
        job_history.duration_seconds = result.duration_seconds
        job_history.records_processed = result.records_processed
        job_history.status = 'completed' if result.success else 'failed'
        if not result.success:
            job_history.error_message = result.error_message
            job_history.error_traceback = result.stderr[:5000] if result.stderr else None
        session.commit()
        session.close()

        # Clear cache so dashboard shows fresh data
        clear_cache('data-freshness')
        clear_cache('history')

    thread = threading.Thread(target=run_pipeline, daemon=True)
    thread.start()

    return jsonify({
        'execution_id': str(execution_id),
        'pipeline': pipeline,
        'status': 'started',
    })


@api_bp.route('/executions/<execution_id>/output')
@require_auth
@require_api_scope('scheduler:read')
def api_get_execution_output(execution_id):
    """Get current output for a running execution."""
    from scheduler.executor import get_execution_output
    output, status = get_execution_output(execution_id)
    return jsonify({
        'execution_id': execution_id,
        'output': output,
        'status': status,
    })


@api_bp.route('/executions/<execution_id>/stream')
@require_auth
@require_api_scope('scheduler:read')
def api_stream_execution(execution_id):
    """Server-Sent Events stream of execution output."""
    from scheduler.executor import get_execution_output
    import time

    def generate():
        last_index = 0
        wait_count = 0
        max_wait = 60

        while True:
            output, status = get_execution_output(execution_id)
            current_status = status.get('status', 'unknown')

            if len(output) > last_index:
                for line in output[last_index:]:
                    yield f"data: {line}\n\n"
                last_index = len(output)
                wait_count = 0

            if current_status in ('completed', 'failed', 'error', 'timeout'):
                yield f"event: done\ndata: {current_status}\n\n"
                break

            if current_status == 'unknown':
                wait_count += 1
                if wait_count > max_wait:
                    yield f"data: [ERROR] Timed out waiting for execution\n\n"
                    yield f"event: done\ndata: timeout\n\n"
                    break

            time.sleep(0.5)

    return current_app.response_class(
        generate(),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
        }
    )


@api_bp.route('/jobs/<pipeline>/run', methods=['POST'])
@require_auth
@require_api_scope('scheduler:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def api_run_job(pipeline):
    """Trigger job execution (synchronous)."""
    from scheduler.executor import PipelineExecutor

    config = _get_scheduler_config()

    if pipeline not in config.pipelines:
        return jsonify({'error': 'Pipeline not found'}), 404

    data = request.get_json() or {}
    mode = data.get('mode', 'auto')
    args = data.get('args', {})
    args['mode'] = mode

    p = config.pipelines[pipeline]
    execution_id = uuid4()

    final_args = dict(p.default_args)
    final_args.update(args)

    executor = PipelineExecutor()
    result = executor.execute(
        module_path=p.module_path,
        args=final_args,
        execution_id=execution_id,
        timeout_seconds=p.timeout_seconds
    )

    return jsonify({
        'execution_id': str(execution_id),
        'success': result.success,
        'duration_seconds': result.duration_seconds,
        'records_processed': result.records_processed,
        'error_message': result.error_message,
    })


# =============================================================================
# History
# =============================================================================

@api_bp.route('/history')
@require_auth
@require_api_scope('scheduler:read')
@cached(ttl_seconds=15)
def api_list_history():
    """List execution history with pagination."""
    from scheduler.models import JobHistory

    pipeline = request.args.get('pipeline')
    status = request.args.get('status')

    try:
        limit, offset = parse_pagination(request.args)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    since = request.args.get('since')
    since_date = None
    if since:
        try:
            since_date = parse_date_param(since, 'since')
        except ValueError as e:
            return jsonify({'error': str(e)}), 400

    session = get_session()
    try:
        query = session.query(JobHistory)

        if pipeline:
            query = query.filter(JobHistory.pipeline_name == pipeline)
        if status:
            query = query.filter(JobHistory.status == status)
        if since_date:
            query = query.filter(JobHistory.scheduled_at >= since_date)

        total = query.count()

        results = query.order_by(
            desc(JobHistory.scheduled_at)
        ).offset(offset).limit(limit).all()

        return jsonify({
            'total': total,
            'offset': offset,
            'limit': limit,
            'results': [r.to_dict() for r in results]
        })
    finally:
        session.close()


@api_bp.route('/history/<execution_id>')
@require_auth
@require_api_scope('scheduler:read')
def api_get_execution(execution_id):
    """Get execution details by execution ID."""
    from scheduler.models import JobHistory

    session = get_session()
    try:
        record = session.query(JobHistory).filter_by(
            execution_id=UUID(execution_id)
        ).first()

        if not record:
            return jsonify({'error': 'Execution not found'}), 404

        data = record.to_dict()
        # Redact internal error details for API key consumers
        is_api_key = g.current_user.get('auth_method') == 'api_key'
        if is_api_key:
            if data.get('error_message'):
                data['error_message'] = 'Pipeline execution failed'
        else:
            data['error_traceback'] = record.error_traceback
        return jsonify(data)
    finally:
        session.close()


@api_bp.route('/history/stats')
@require_auth
@require_api_scope('scheduler:read')
@cached(ttl_seconds=30)
def api_history_stats():
    """Get execution statistics."""
    from scheduler.models import JobHistory

    period = request.args.get('period', '7d')
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since_date = now_sgt() - timedelta(days=days)

    session = get_session()
    try:
        stats = session.query(
            JobHistory.pipeline_name,
            func.count(JobHistory.id).label('total'),
            func.sum(case((JobHistory.status == 'completed', 1), else_=0)).label('success'),
            func.sum(case((JobHistory.status == 'failed', 1), else_=0)).label('failed'),
            func.avg(JobHistory.duration_seconds).label('avg_duration'),
            func.avg(JobHistory.records_processed).label('avg_records')
        ).filter(
            JobHistory.scheduled_at >= since_date
        ).group_by(
            JobHistory.pipeline_name
        ).all()

        return jsonify({
            'period': period,
            'since': since_date.isoformat(),
            'pipelines': [
                {
                    'pipeline_name': s.pipeline_name,
                    'total': s.total,
                    'success': int(s.success or 0),
                    'failed': int(s.failed or 0),
                    'success_rate': round(int(s.success or 0) / s.total * 100, 1) if s.total > 0 else 0,
                    'avg_duration': round(float(s.avg_duration or 0), 1),
                    'avg_records': int(s.avg_records or 0),
                }
                for s in stats
            ]
        })
    finally:
        session.close()


@api_bp.route('/history/<int:history_id>')
@require_auth
@require_api_scope('scheduler:read')
def api_get_history_detail(history_id):
    """Get detailed execution record."""
    from scheduler.models import JobHistory

    session = get_session()
    try:
        job = session.query(JobHistory).filter_by(id=history_id).first()
        if not job:
            return jsonify({'error': 'Execution not found'}), 404

        return jsonify({
            'id': job.id,
            'pipeline_name': job.pipeline_name,
            'execution_id': str(job.execution_id),
            'status': job.status,
            'started_at': job.started_at.isoformat() if job.started_at else None,
            'completed_at': job.completed_at.isoformat() if job.completed_at else None,
            'duration_seconds': job.duration_seconds,
            'records_processed': job.records_processed,
            'error_message': ('Pipeline execution failed' if job.error_message else None) if g.current_user.get('auth_method') == 'api_key' else job.error_message,
            'error_traceback': None if g.current_user.get('auth_method') == 'api_key' else job.error_traceback,
            'mode': job.mode,
            'parameters': job.parameters,
            'triggered_by': job.triggered_by,
            'attempt_number': job.attempt_number,
            'max_retries': job.max_retries,
        })
    finally:
        session.close()


@api_bp.route('/history/cleanup-stale', methods=['POST'])
@require_auth
@require_api_scope('scheduler:write')
def api_cleanup_stale():
    """Mark stale running jobs as failed."""
    from scheduler.models import JobHistory

    session = get_session()
    try:
        stale = session.query(JobHistory).filter_by(status='running').all()
        fixed = []
        for job in stale:
            job.status = 'failed'
            job.error_message = 'Interrupted - server restarted'
            job.completed_at = now_utc()
            fixed.append({
                'pipeline_name': job.pipeline_name,
                'execution_id': str(job.execution_id),
            })

        session.commit()
        return jsonify({
            'success': True,
            'fixed_count': len(fixed),
            'fixed_jobs': fixed,
        })
    finally:
        session.close()


# =============================================================================
# Resources
# =============================================================================

@api_bp.route('/resources')
@require_auth
@require_api_scope('scheduler:read')
def api_resources():
    """Get current resource usage."""
    from scheduler.resource_manager import get_resource_manager

    rm = get_resource_manager()
    return jsonify(rm.get_all_usage_dict())


# =============================================================================
# Config
# =============================================================================

@api_bp.route('/config')
@require_auth
@require_api_scope('scheduler:read')
def api_config():
    """Get scheduler configuration."""
    config = _get_scheduler_config()

    return jsonify({
        'timezone': config.timezone,
        'max_workers': config.executor_max_workers,
        'pipeline_count': len(config.pipelines),
        'resources': {
            'db_pool': config.resources.db_pool,
            'soap_api': config.resources.soap_api,
            'http_api': config.resources.http_api,
        },
        'alerts': {
            'slack_enabled': config.alerts.slack.enabled,
            'email_enabled': config.alerts.email.enabled,
        }
    })


# =============================================================================
# Service Management
# =============================================================================

# Store scheduler process reference
_scheduler_process = None


@api_bp.route('/services/status')
@require_auth
@require_api_scope('scheduler:read')
def api_services_status():
    """Get status of scheduler services."""
    from scheduler.models import SchedulerState
    import os

    session = get_session()
    try:
        # Check scheduler daemon status from database
        state = session.query(SchedulerState).filter_by(id=1).first()

        scheduler_status = 'stopped'
        scheduler_info = {}

        if state:
            # Check if the process is actually running
            if state.pid:
                try:
                    os.kill(state.pid, 0)  # Check if process exists
                    if state.status == 'running':
                        # Verify heartbeat is recent (within 2 minutes)
                        if state.last_heartbeat:
                            # Handle timezone-aware and naive datetimes
                            now = datetime.now()
                            hb = state.last_heartbeat
                            if hb.tzinfo is not None:
                                hb = hb.replace(tzinfo=None)
                            heartbeat_age = (now - hb).total_seconds()
                            if heartbeat_age < 120:
                                scheduler_status = 'running'
                            else:
                                scheduler_status = 'stale'
                        else:
                            scheduler_status = 'running'
                except (OSError, ProcessLookupError):
                    scheduler_status = 'stopped'

            scheduler_info = {
                'pid': state.pid,
                'host': state.host_name,
                'started_at': state.started_at.isoformat() if state.started_at else None,
                'last_heartbeat': state.last_heartbeat.isoformat() if state.last_heartbeat else None,
            }

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
            'scheduler': {
                'service': 'Scheduler Daemon',
                'active': scheduler_status == 'running',
                'status': scheduler_status,
                **scheduler_info
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
    finally:
        session.close()


@api_bp.route('/services/scheduler/start', methods=['POST'])
@require_auth
@require_api_scope('scheduler:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def api_start_scheduler():
    """Start the scheduler daemon as a background process."""
    import subprocess
    import os
    from pathlib import Path

    # Check if already running
    from scheduler.models import SchedulerState
    session = get_session()
    try:
        state = session.query(SchedulerState).filter_by(id=1).first()
        if state and state.pid:
            try:
                os.kill(state.pid, 0)
                if state.status == 'running':
                    return jsonify({
                        'success': False,
                        'error': f'Scheduler already running (PID {state.pid})'
                    }), 400
            except (OSError, ProcessLookupError):
                pass  # Process not running, ok to start
    finally:
        session.close()

    # Start scheduler daemon
    working_dir = Path(__file__).parent.parent.parent
    log_file = working_dir / 'logs' / 'scheduler.log'
    log_file.parent.mkdir(exist_ok=True)

    try:
        # Start as background process
        with open(log_file, 'a') as log:
            process = subprocess.Popen(
                ['python3.12', '-m', 'scheduler.cli.main', 'daemon', 'start', '--foreground'],
                cwd=str(working_dir),
                stdout=log,
                stderr=log,
                start_new_session=True,  # Detach from parent
            )

        return jsonify({
            'success': True,
            'message': 'Scheduler daemon starting...',
            'pid': process.pid,
            'log_file': str(log_file)
        })

    except Exception as e:
        current_app.logger.error(f"Failed to start scheduler: {e}")
        return jsonify({
            'success': False,
            'error': 'Failed to start scheduler service'
        }), 500


@api_bp.route('/services/scheduler/stop', methods=['POST'])
@require_auth
@require_api_scope('scheduler:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def api_stop_scheduler():
    """Stop the scheduler daemon."""
    import os
    import signal
    from scheduler.models import SchedulerState

    session = get_session()
    try:
        state = session.query(SchedulerState).filter_by(id=1).first()

        if not state or not state.pid:
            return jsonify({
                'success': False,
                'error': 'No scheduler process found'
            }), 400

        pid = state.pid

        try:
            # Send SIGTERM for graceful shutdown
            os.kill(pid, signal.SIGTERM)

            # Update state
            state.status = 'stopping'
            session.commit()

            return jsonify({
                'success': True,
                'message': f'Stop signal sent to scheduler (PID {pid})',
                'pid': pid
            })

        except ProcessLookupError:
            # Process already dead, clean up state
            state.status = 'stopped'
            session.commit()
            return jsonify({
                'success': True,
                'message': 'Scheduler was not running, state cleaned up'
            })

        except PermissionError:
            current_app.logger.error(f"Permission denied to stop scheduler process {pid}")
            return jsonify({
                'success': False,
                'error': 'Permission denied to stop scheduler'
            }), 403

    finally:
        session.close()


@api_bp.route('/services/mcp/start', methods=['POST'])
@require_auth
@require_api_scope('scheduler:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def api_start_mcp():
    """Start the MCP server via systemctl."""
    import subprocess

    try:
        result = subprocess.run(
            ['sudo', 'systemctl', 'start', 'backend-mcp'],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            return jsonify({'success': True, 'message': 'MCP server starting...'})
        else:
            current_app.logger.error(f"Failed to start MCP: {result.stderr}")
            return jsonify({'success': False, 'error': 'Failed to start MCP server'}), 500
    except Exception as e:
        current_app.logger.error(f"Failed to start MCP: {e}")
        return jsonify({'success': False, 'error': 'Failed to start MCP server'}), 500


@api_bp.route('/services/mcp/stop', methods=['POST'])
@require_auth
@require_api_scope('scheduler:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def api_stop_mcp():
    """Stop the MCP server via systemctl."""
    import subprocess

    try:
        result = subprocess.run(
            ['sudo', 'systemctl', 'stop', 'backend-mcp'],
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            return jsonify({'success': True, 'message': 'MCP server stopped'})
        else:
            current_app.logger.error(f"Failed to stop MCP: {result.stderr}")
            return jsonify({'success': False, 'error': 'Failed to stop MCP server'}), 500
    except Exception as e:
        current_app.logger.error(f"Failed to stop MCP: {e}")
        return jsonify({'success': False, 'error': 'Failed to stop MCP server'}), 500


@api_bp.route('/services/<service>/restart', methods=['POST'])
@require_auth
@require_api_scope('scheduler:write')
@rate_limit_api(max_requests=5, window_seconds=60)
def api_restart_service(service):
    """Restart a scheduler service."""
    if service == 'scheduler':
        # Stop then start
        stop_result = api_stop_scheduler()
        if stop_result[1] if isinstance(stop_result, tuple) else 200 >= 400:
            pass  # Ignore stop errors, try to start anyway

        import time
        time.sleep(2)  # Wait for graceful shutdown

        return api_start_scheduler()

    elif service == 'web_ui':
        return jsonify({
            'success': False,
            'error': 'Cannot restart web UI from within itself. Use systemctl or restart the process manually.'
        }), 400

    return jsonify({'error': f'Unknown service: {service}'}), 400


# =============================================================================
# Pipeline Management
# =============================================================================

@api_bp.route('/pipelines')
@require_auth
@require_api_scope('scheduler:read')
def api_list_pipelines():
    """List scheduler-owned pipelines (managed_by='scheduler').

    Sync orchestrator pipelines are served via /api/sync/pipelines.
    """
    from scheduler.models import PipelineConfig

    session = get_session()
    try:
        rows = (session.query(PipelineConfig)
                .filter(PipelineConfig.managed_by == 'scheduler')
                .order_by(PipelineConfig.priority).all())
        pipelines = [row.to_dict() for row in rows]
        return jsonify({'pipelines': pipelines})
    finally:
        session.close()


@api_bp.route('/pipelines/<name>')
@require_auth
@require_api_scope('scheduler:read')
def api_get_pipeline(name):
    """Get a scheduler-owned pipeline configuration."""
    from scheduler.models import PipelineConfig

    session = get_session()
    try:
        row = (session.query(PipelineConfig)
               .filter_by(pipeline_name=name, managed_by='scheduler').first())
        if not row:
            return jsonify({'error': 'Pipeline not found'}), 404
        return jsonify(row.to_dict())
    finally:
        session.close()


@api_bp.route('/pipelines', methods=['POST'])
@require_auth
@require_api_scope('scheduler:write')
@rate_limit_api(max_requests=20, window_seconds=60)
def api_create_pipeline():
    """Create a new pipeline."""
    from scheduler.models import PipelineConfig

    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    name = data.get('name')
    if not name:
        return jsonify({'error': 'Pipeline name is required'}), 400

    if not name.replace('_', '').replace('-', '').isalnum():
        return jsonify({'error': 'Pipeline name must be alphanumeric with underscores/hyphens'}), 400

    module_path = data.get('module_path', f'datalayer.{name}')
    is_valid, msg = _validate_module_path(module_path)
    if not is_valid:
        return jsonify({'error': msg}), 400

    # This endpoint is scheduler-only. Sync pipelines are created via /api/sync/pipelines.
    managed_by = 'scheduler'

    valid, err = _validate_pipeline_specific_args(data.get('pipeline_specific_args'))
    if not valid:
        return jsonify({'error': err}), 400

    session = get_session()
    try:
        existing = session.query(PipelineConfig).filter_by(pipeline_name=name).first()
        if existing:
            return jsonify({'error': f'Pipeline {name} already exists'}), 400

        row = PipelineConfig(
            pipeline_name=name,
            display_name=data.get('display_name', name.replace('_', ' ').title()),
            description=data.get('description', ''),
            module_path=module_path,
            schedule_type='cron',
            schedule_config={'type': 'cron', 'cron': data.get('cron', '0 6 * * *')},
            enabled=data.get('enabled', False),
            priority=data.get('priority', 5),
            depends_on=data.get('depends_on', []),
            conflicts_with=data.get('conflicts_with', []),
            resource_group=data.get('resource_group', 'http_api'),
            max_db_connections=data.get('max_db_connections', 2),
            estimated_duration_seconds=data.get('estimated_duration_seconds', 600),
            max_retries=data.get('max_retries', 3),
            retry_delay_seconds=data.get('retry_delay', 300),
            retry_backoff_multiplier=data.get('backoff_multiplier', 2),
            timeout_seconds=data.get('timeout_seconds', 3600),
            default_args=data.get('default_args', {'mode': 'auto'}),
            data_freshness_config={
                'table': data.get('freshness_table', ''),
                'date_column': data.get('freshness_column', ''),
            },
            sync_config=None,
            pipeline_specific_args=data.get('pipeline_specific_args'),
            managed_by=managed_by,
        )
        session.add(row)
        session.commit()

        return jsonify({
            'success': True,
            'message': f'Pipeline {name} created',
            'pipeline': row.to_dict()
        })
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Failed to create pipeline {name}: {e}")
        return jsonify({'error': 'Failed to create pipeline'}), 500
    finally:
        session.close()


@api_bp.route('/pipelines/<name>', methods=['PUT'])
@require_auth
@require_api_scope('scheduler:write')
@rate_limit_api(max_requests=20, window_seconds=60)
def api_update_pipeline(name):
    """Update a scheduler-owned pipeline configuration.

    Sync orchestrator pipelines are managed via /api/sync/pipelines/<name>.
    To transfer a pipeline between engines, use /api/pipelines/<name>/transfer.
    """
    from scheduler.models import PipelineConfig

    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    session = get_session()
    try:
        row = (session.query(PipelineConfig)
               .filter_by(pipeline_name=name, managed_by='scheduler').first())
        if not row:
            return jsonify({'error': 'Pipeline not found'}), 404

        if 'display_name' in data:
            row.display_name = data['display_name']
        if 'description' in data:
            row.description = data['description']
        if 'module_path' in data:
            is_valid, msg = _validate_module_path(data['module_path'])
            if not is_valid:
                return jsonify({'error': msg}), 400
            row.module_path = data['module_path']
        if 'enabled' in data:
            row.enabled = data['enabled']
        if 'cron' in data:
            row.schedule_config = {'type': 'cron', 'cron': data['cron']}
        if 'priority' in data:
            row.priority = data['priority']
        if 'depends_on' in data:
            row.depends_on = data['depends_on']
        if 'conflicts_with' in data:
            row.conflicts_with = data['conflicts_with']
        if 'resource_group' in data:
            row.resource_group = data['resource_group']
        if 'max_db_connections' in data:
            row.max_db_connections = data['max_db_connections']
        if 'timeout_seconds' in data:
            row.timeout_seconds = data['timeout_seconds']
        if 'max_retries' in data:
            row.max_retries = data['max_retries']
        if 'freshness_table' in data:
            fc = dict(row.data_freshness_config or {})
            fc['table'] = data['freshness_table']
            row.data_freshness_config = fc
        if 'freshness_column' in data:
            fc = dict(row.data_freshness_config or {})
            fc['date_column'] = data['freshness_column']
            row.data_freshness_config = fc
        if 'pipeline_specific_args' in data:
            valid, err = _validate_pipeline_specific_args(data['pipeline_specific_args'])
            if not valid:
                return jsonify({'error': err}), 400
            row.pipeline_specific_args = data['pipeline_specific_args']

        session.commit()
        return jsonify({'success': True, 'message': f'Pipeline {name} updated'})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Failed to update pipeline {name}: {e}")
        return jsonify({'error': 'Failed to update pipeline'}), 500
    finally:
        session.close()


@api_bp.route('/pipelines/<name>', methods=['DELETE'])
@require_auth
@require_api_scope('scheduler:write')
@rate_limit_api(max_requests=10, window_seconds=60)
def api_delete_pipeline(name):
    """Delete a scheduler-owned pipeline."""
    from scheduler.models import PipelineConfig

    session = get_session()
    try:
        row = (session.query(PipelineConfig)
               .filter_by(pipeline_name=name, managed_by='scheduler').first())
        if not row:
            return jsonify({'error': 'Pipeline not found'}), 404

        session.delete(row)
        session.commit()
        return jsonify({'success': True, 'message': f'Pipeline {name} deleted'})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Failed to delete pipeline {name}: {e}")
        return jsonify({'error': 'Failed to delete pipeline'}), 500
    finally:
        session.close()


@api_bp.route('/pipelines/ownership')
@require_auth
@require_api_scope('scheduler:read')
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
@require_api_scope('scheduler:write')
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
@require_api_scope('scheduler:read')
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
@require_api_scope('scheduler:read')
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
@require_api_scope('scheduler:write')
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


# =============================================================================
# Inventory Checker
# =============================================================================

@api_bp.route('/inventory/units')
@require_auth
@require_api_scope('inventory:read')
def api_inventory_units():
    """Get raw unit data from units_info for selected sites."""
    site_ids_param = request.args.get('site_ids', '')
    if not site_ids_param:
        return jsonify({'error': 'site_ids parameter is required'}), 400

    try:
        site_ids = parse_site_ids(site_ids_param)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    session = get_pbi_session()
    try:
        placeholders = ', '.join([f':sid{i}' for i in range(len(site_ids))])
        params = {f'sid{i}': sid for i, sid in enumerate(site_ids)}

        query = text(f"""
            SELECT u."SiteID", u."UnitID", u."sLocationCode", u."sUnitName", u."sTypeName",
                   u."dcWidth", u."dcLength", u."bClimate", u."bInside", u."bPower", u."bAlarm",
                   u."sUnitNote", u."sUnitDesc", u."bRented", u."bRentable",
                   u."dcStdRate", u."dcWebRate", u."dcPushRate", u."dcBoardRate",
                   u."iFloor", u."UnitTypeID",
                   v.climate_type, v.has_dehumidifier, v.noke_status,
                   v.has_pillar, v.pillar_size, v.is_odd_shape, v.deck_position,
                   v.case_count, v.storage_type
            FROM units_info u
            LEFT JOIN vw_units_inventory v
                ON v.unit_id = u."UnitID"
                AND v.site_id = u."SiteID"
            WHERE u."SiteID" IN ({placeholders})
              AND u."deleted_at" IS NULL
            ORDER BY u."SiteID", u."sUnitName"
        """)

        result = session.execute(query, params)
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

        return jsonify({'units': units, 'count': len(units)})

    except Exception as e:
        current_app.logger.error(f"Inventory units query error: {e}")
        return jsonify({'error': 'Failed to fetch units'}), 500
    finally:
        session.close()


@api_bp.route('/inventory/distinct-types')
@require_auth
@require_api_scope('inventory:read')
@cached(ttl_seconds=300)
def api_inventory_distinct_types():
    """Get all distinct sTypeName values from units_info.

    Optional: ?site_ids=1,2,3 to filter by sites. Without it, returns all types.
    """
    session = get_pbi_session()
    try:
        site_ids_param = request.args.get('site_ids', '')
        if site_ids_param:
            try:
                site_ids = parse_site_ids(site_ids_param)
            except ValueError as e:
                return jsonify({'error': str(e)}), 400

            placeholders = ', '.join([f':sid{i}' for i in range(len(site_ids))])
            params = {f'sid{i}': sid for i, sid in enumerate(site_ids)}

            query = text(f"""
                SELECT DISTINCT "sTypeName"
                FROM units_info
                WHERE "SiteID" IN ({placeholders})
                  AND "sTypeName" IS NOT NULL
                  AND deleted_at IS NULL
                ORDER BY "sTypeName"
            """)
            result = session.execute(query, params)
        else:
            query = text("""
                SELECT DISTINCT "sTypeName"
                FROM units_info
                WHERE "sTypeName" IS NOT NULL
                ORDER BY "sTypeName"
            """)
            result = session.execute(query)

        types = [row[0] for row in result.fetchall()]
        return jsonify({'types': types})

    finally:
        session.close()


@api_bp.route('/inventory/type-mappings')
@require_auth
@require_api_scope('inventory:read')
def api_inventory_get_type_mappings():
    """Get all saved type mappings from backend DB."""
    from web.models.inventory import InventoryTypeMapping

    session = get_session()
    try:
        mappings = session.query(InventoryTypeMapping).order_by(
            InventoryTypeMapping.source_type_name
        ).all()
        return jsonify({
            'mappings': [m.to_dict() for m in mappings]
        })
    finally:
        session.close()


@api_bp.route('/inventory/type-mappings', methods=['PUT'])
@require_auth
@require_api_scope('inventory:write')
@rate_limit_api(max_requests=20, window_seconds=60)
def api_inventory_upsert_type_mappings():
    """Bulk upsert type mappings. Requires config management permission."""
    from flask_login import current_user as session_user
    from web.models.inventory import InventoryTypeMapping

    # Defense-in-depth: only config managers can write type mappings
    if session_user and session_user.is_authenticated:
        if not session_user.can_manage_configs():
            return jsonify({'error': 'Forbidden', 'message': 'Config management permission required'}), 403

    data = request.get_json()
    if not data or 'mappings' not in data:
        return jsonify({'error': 'mappings array is required'}), 400

    mappings_data = data['mappings']
    if not isinstance(mappings_data, list):
        return jsonify({'error': 'mappings must be an array'}), 400
    try:
        validate_array_size(mappings_data, 'mappings')
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    username = g.current_user.get('sub', 'unknown') if hasattr(g, 'current_user') and g.current_user else 'unknown'

    session = get_session()
    try:
        upserted = 0
        for item in mappings_data:
            source = item.get('source_type_name', '').strip()
            code = item.get('mapped_type_code', '').strip() or None
            climate = item.get('mapped_climate_code', '').strip() or None
            if not source or (not code and not climate):
                continue

            existing = session.query(InventoryTypeMapping).filter_by(
                source_type_name=source
            ).first()

            if existing:
                existing.mapped_type_code = code
                existing.mapped_climate_code = climate
                existing.updated_at = datetime.now()
            else:
                mapping = InventoryTypeMapping(
                    source_type_name=source,
                    mapped_type_code=code,
                    mapped_climate_code=climate,
                    created_by=username,
                )
                session.add(mapping)

            upserted += 1

        session.commit()
        return jsonify({'success': True, 'upserted': upserted})

    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Error upserting type mappings: {e}")
        return jsonify({'error': 'Failed to save mappings'}), 500
    finally:
        session.close()


@api_bp.route('/inventory/overrides')
@require_auth
@require_api_scope('inventory:read')
def api_inventory_get_overrides():
    """Get saved per-unit overrides for selected sites."""
    from web.models.inventory import InventoryUnitOverride

    site_ids_param = request.args.get('site_ids', '')
    if not site_ids_param:
        return jsonify({'error': 'site_ids parameter is required'}), 400

    try:
        site_ids = parse_site_ids(site_ids_param)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    session = get_session()
    try:
        overrides = session.query(InventoryUnitOverride).filter(
            InventoryUnitOverride.site_id.in_(site_ids)
        ).all()
        return jsonify({
            'overrides': [o.to_dict() for o in overrides]
        })
    finally:
        session.close()


@api_bp.route('/inventory/overrides', methods=['PUT'])
@require_auth
@require_api_scope('inventory:write')
@rate_limit_api(max_requests=20, window_seconds=60)
def api_inventory_upsert_overrides():
    """Bulk upsert per-unit overrides."""
    from web.models.inventory import InventoryUnitOverride

    data = request.get_json()
    if not data or 'overrides' not in data:
        return jsonify({'error': 'overrides array is required'}), 400

    overrides_data = data['overrides']
    if not isinstance(overrides_data, list):
        return jsonify({'error': 'overrides must be an array'}), 400
    try:
        validate_array_size(overrides_data, 'overrides')
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    username = g.current_user.get('sub', 'unknown') if hasattr(g, 'current_user') and g.current_user else 'unknown'

    session = get_session()
    try:
        upserted = 0
        for item in overrides_data:
            site_id = item.get('site_id')
            unit_id = item.get('unit_id')
            if site_id is None or unit_id is None:
                continue

            existing = session.query(InventoryUnitOverride).filter_by(
                site_id=site_id, unit_id=unit_id
            ).first()

            if existing:
                if 'unit_type_code' in item:
                    existing.unit_type_code = item['unit_type_code'] or None
                if 'size_category' in item:
                    existing.size_category = item['size_category'] or None
                if 'size_range' in item:
                    existing.size_range = item['size_range'] or None
                if 'shape' in item:
                    existing.shape = item['shape'] or None
                if 'pillar' in item:
                    existing.pillar = item['pillar'] or None
                if 'climate_code' in item:
                    existing.climate_code = item['climate_code'] or None
                if 'reviewed' in item:
                    existing.reviewed = bool(item['reviewed'])
                existing.updated_by = username
                existing.updated_at = datetime.now()
            else:
                override = InventoryUnitOverride(
                    site_id=site_id,
                    unit_id=unit_id,
                    unit_type_code=item.get('unit_type_code') or None,
                    size_category=item.get('size_category') or None,
                    size_range=item.get('size_range') or None,
                    shape=item.get('shape') or None,
                    pillar=item.get('pillar') or None,
                    climate_code=item.get('climate_code') or None,
                    reviewed=bool(item.get('reviewed', False)),
                    updated_by=username,
                )
                session.add(override)

            upserted += 1

        session.commit()
        return jsonify({'success': True, 'upserted': upserted})

    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Error upserting overrides: {e}")
        return jsonify({'error': 'Failed to save overrides'}), 500
    finally:
        session.close()


@api_bp.route('/inventory/publish-labels', methods=['POST'])
@require_auth
@require_api_scope('inventory:write')
@rate_limit_api(max_requests=10)
def api_inventory_publish_labels():
    """Publish computed final labels to unit_category_labels on esa_pbi."""
    from flask_login import current_user as session_user

    # Defense-in-depth: session users must have inventory tools access
    if session_user and session_user.is_authenticated:
        if not session_user.can_access_inventory_tools():
            return jsonify({'error': 'Forbidden'}), 403

    data = request.get_json()
    if not data or 'labels' not in data:
        return jsonify({'error': 'labels array is required'}), 400

    labels = data['labels']
    if not labels:
        return jsonify({'error': 'labels array is empty'}), 400
    if len(labels) > 25000:
        return jsonify({'error': 'labels array exceeds maximum of 25000'}), 400

    # Field length limits matching DB schema
    field_limits = {
        'size_category': 5, 'size_range': 10, 'unit_type_code': 10,
        'climate_code': 5, 'shape': 5, 'pillar': 5, 'final_label': 30,
    }

    username = g.current_user.get('sub', 'unknown') if hasattr(g, 'current_user') and g.current_user else 'unknown'

    session = get_pbi_session()
    try:
        published = 0
        for item in labels:
            try:
                site_id = int(item['site_id'])
                unit_id = int(item['unit_id'])
            except (TypeError, ValueError, KeyError):
                continue

            final_label = item.get('final_label')
            if not isinstance(final_label, str) or not final_label.strip():
                continue

            # Truncate string fields to schema limits
            def _trunc(key):
                val = item.get(key) or None
                if val and len(str(val)) > field_limits.get(key, 30):
                    val = str(val)[:field_limits[key]]
                return val

            session.execute(text("""
                INSERT INTO unit_category_labels
                    (site_id, unit_id, size_category, size_range, unit_type_code,
                     climate_code, shape, pillar, final_label, published_by, published_at)
                VALUES
                    (:site_id, :unit_id, :size_category, :size_range, :unit_type_code,
                     :climate_code, :shape, :pillar, :final_label, :published_by, NOW())
                ON CONFLICT (site_id, unit_id) DO UPDATE SET
                    size_category = EXCLUDED.size_category,
                    size_range = EXCLUDED.size_range,
                    unit_type_code = EXCLUDED.unit_type_code,
                    climate_code = EXCLUDED.climate_code,
                    shape = EXCLUDED.shape,
                    pillar = EXCLUDED.pillar,
                    final_label = EXCLUDED.final_label,
                    published_by = EXCLUDED.published_by,
                    published_at = NOW()
            """), {
                'site_id': site_id,
                'unit_id': unit_id,
                'size_category': _trunc('size_category'),
                'size_range': _trunc('size_range'),
                'unit_type_code': _trunc('unit_type_code'),
                'climate_code': _trunc('climate_code'),
                'shape': _trunc('shape'),
                'pillar': _trunc('pillar'),
                'final_label': final_label[:30],
                'published_by': username,
            })
            published += 1

        session.commit()
        current_app.logger.info(f"Published {published} category labels by {username}")
        return jsonify({'success': True, 'published': published})

    except Exception as e:
        session.rollback()
        current_app.logger.error(f"Error publishing category labels: {e}")
        return jsonify({'error': 'Failed to publish labels'}), 500
    finally:
        session.close()


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
@cached(ttl_seconds=30)
def api_statistics_summary():
    """
    Overall API consumption summary.
    Query params:
        period: 1d, 7d, 30d, 90d (default 7d)
    """
    from web.models.api_statistic import ApiStatistic

    period = request.args.get('period', '7d')
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    session = get_session()
    try:
        base = session.query(ApiStatistic).filter(ApiStatistic.called_at >= since)

        total_calls = base.count()

        avg_response = session.query(
            func.avg(ApiStatistic.response_time_ms)
        ).filter(ApiStatistic.called_at >= since).scalar() or 0

        error_count = base.filter(ApiStatistic.status_code >= 400).count()

        # Calls per time bucket in SGT (hourly for 24h, daily otherwise)
        trunc_unit = 'hour' if period == '1d' else 'day'
        sgt_time = func.timezone('Asia/Singapore', func.timezone('UTC', ApiStatistic.called_at))
        volume = session.query(
            func.date_trunc(trunc_unit, sgt_time).label('bucket'),
            func.count(ApiStatistic.id).label('count')
        ).filter(
            ApiStatistic.called_at >= since
        ).group_by('bucket').order_by('bucket').all()

        time_unit, timeline = _build_volume_timeline(period, volume)

        return jsonify({
            'period': period,
            'since': since.isoformat(),
            'total_calls': total_calls,
            'avg_response_time_ms': round(float(avg_response), 2),
            'error_count': error_count,
            'error_rate': round(error_count / total_calls * 100, 2) if total_calls > 0 else 0,
            'time_unit': time_unit,
            'calls_per_day': timeline,
        })
    finally:
        session.close()


@api_bp.route('/statistics/endpoints')
@require_auth
@require_api_scope('statistics:read')
@rate_limit_api(max_requests=30, window_seconds=60)
@cached(ttl_seconds=30)
def api_statistics_endpoints():
    """
    Per-endpoint breakdown of API consumption.
    Query params:
        period: 1d, 7d, 30d, 90d (default 7d)
        sort: calls, avg_time, errors (default calls)
    """
    from web.models.api_statistic import ApiStatistic

    period = request.args.get('period', '7d')
    sort_by = request.args.get('sort', 'calls')
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    session = get_session()
    try:
        stats = session.query(
            ApiStatistic.endpoint,
            ApiStatistic.method,
            func.count(ApiStatistic.id).label('total_calls'),
            func.avg(ApiStatistic.response_time_ms).label('avg_response_ms'),
            func.max(ApiStatistic.response_time_ms).label('max_response_ms'),
            func.sum(case((ApiStatistic.status_code >= 400, 1), else_=0)).label('error_count'),
        ).filter(
            ApiStatistic.called_at >= since
        ).group_by(
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
@cached(ttl_seconds=60)
def api_statistics_top_consumers():
    """
    Top API consumers by client IP.
    Query params:
        period: 1d, 7d, 30d (default 7d)
        limit: number of results (default 20)
    """
    from web.models.api_statistic import ApiStatistic

    period = request.args.get('period', '7d')
    try:
        limit = min(int(request.args.get('limit', 20)), 100)
    except (ValueError, TypeError):
        limit = 20
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    session = get_session()
    try:
        stats = session.query(
            ApiStatistic.client_ip,
            func.count(ApiStatistic.id).label('total_calls'),
            func.count(func.distinct(ApiStatistic.endpoint)).label('unique_endpoints'),
            func.avg(ApiStatistic.response_time_ms).label('avg_ms'),
        ).filter(
            ApiStatistic.called_at >= since
        ).group_by(
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
@cached(ttl_seconds=60)
def api_statistics_slow_endpoints():
    """
    Endpoints ranked by response time (identifies performance bottlenecks).
    Query params:
        period: 1d, 7d, 30d (default 7d)
        min_calls: minimum call count to include (default 5)
    """
    from web.models.api_statistic import ApiStatistic

    period = request.args.get('period', '7d')
    try:
        min_calls = int(request.args.get('min_calls', 5))
    except (ValueError, TypeError):
        min_calls = 5
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    session = get_session()
    try:
        stats = session.query(
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
        ).group_by(
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
@cached(ttl_seconds=30)
def api_ext_statistics_summary():
    """
    Outbound API call summary.
    Query params: period (1d, 7d, 30d, 90d)
    """
    from web.models.external_api_statistic import ExternalApiStatistic

    period = request.args.get('period', '7d')
    days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
    since = datetime.utcnow() - timedelta(days=days)

    session = get_session()
    try:
        base = session.query(ExternalApiStatistic).filter(
            ExternalApiStatistic.called_at >= since
        )
        total = base.count()
        avg_ms = session.query(
            func.avg(ExternalApiStatistic.response_time_ms)
        ).filter(ExternalApiStatistic.called_at >= since).scalar() or 0
        error_count = base.filter(ExternalApiStatistic.success == False).count()

        by_service = session.query(
            ExternalApiStatistic.service_name,
            func.count(ExternalApiStatistic.id).label('count'),
            func.avg(ExternalApiStatistic.response_time_ms).label('avg_ms'),
            func.sum(case((ExternalApiStatistic.success == False, 1), else_=0)).label('errors'),
        ).filter(
            ExternalApiStatistic.called_at >= since
        ).group_by(ExternalApiStatistic.service_name).all()

        trunc_unit = 'hour' if period == '1d' else 'day'
        sgt_time = func.timezone('Asia/Singapore', func.timezone('UTC', ExternalApiStatistic.called_at))
        volume = session.query(
            func.date_trunc(trunc_unit, sgt_time).label('bucket'),
            func.count(ExternalApiStatistic.id).label('count'),
        ).filter(
            ExternalApiStatistic.called_at >= since
        ).group_by('bucket').order_by('bucket').all()

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
                    'total_calls': s.count,
                    'avg_response_ms': round(float(s.avg_ms or 0), 2),
                    'error_count': int(s.errors or 0),
                }
                for s in by_service
            ],
            'calls_per_day': timeline,
        })
    finally:
        session.close()


@api_bp.route('/statistics/external/services')
@require_auth
@require_api_scope('statistics:read')
@rate_limit_api(max_requests=30, window_seconds=60)
@cached(ttl_seconds=30)
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
@cached(ttl_seconds=30)
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
@cached(ttl_seconds=30)
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
@cached(ttl_seconds=30)
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
    BOOL_FIELDS = ['hidden_rate', 'available_for_chatbot', 'is_active']
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
        if request.args.get('include_concessions', '').lower() == 'true' and plan.linked_concessions:
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
# Read plans from ccws_discount (PBI DB) and update via CallCenterWs SOAP.
#
# GET  /api/ccws-discount-plans/<site_id>       — list plans for a site
# POST /api/ccws-discount-plans/update-simple   — enable/disable plans (simple)
# POST /api/ccws-discount-plans/update          — full plan update
# =============================================================================

@api_bp.route('/ccws-discount-plans/<int:site_id>')
@require_auth
@require_api_scope('scheduler:read')
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
@require_api_scope('scheduler:write')
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
@require_api_scope('scheduler:write')
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
@_require_sl_session_access
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
@_require_sl_session_access
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
@_require_sl_session_access
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
@_require_sl_session_access
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


# --- Padlocks CRUD ---

@api_bp.route('/smart-lock/padlocks')
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_sl_padlocks_list():
    """List padlocks, optionally filtered by site_id."""
    site_id = request.args.get('site_id')
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
@_require_sl_session_access
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
@_require_sl_session_access
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
@_require_sl_session_access
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
@_require_sl_session_access
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

        # Igloo data last refresh
        igloo_refresh = None
        if igloo_devs:
            igloo_refresh = max(
                (ig.updated_at for ig in igloo_devs if ig.updated_at), default=None
            )
            if igloo_refresh:
                igloo_refresh = igloo_refresh.isoformat()

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
@_require_sl_session_access
@require_api_scope('smart_lock:read')
def api_sl_config_list():
    """List smart lock site configurations."""
    from web.models.smart_lock import SmartLockSiteConfig
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
@_require_sl_session_access
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

    if not unit_id or not location_code:
        return jsonify({'error': 'unit_id and location_code required'}), 400

    session = current_app.get_middleware_session()
    try:
        record = session.query(GateAccessData).filter_by(
            location_code=location_code, unit_id=unit_id
        ).first()

        if not record:
            return jsonify({'error': 'No gate access data for this unit'}), 404

        # Site-scoping: verify caller-supplied site_id matches the record
        if site_id and record.site_id != site_id:
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
      synced          — should_have_pin=True + ESA-tagged PIN equals gate code
      push_pending    — should_have_pin=True + no/stale ESA PIN (cron will push)
      revoke_pending  — should_have_pin=False + ESA PIN still on device (cron will revoke)
      clean           — should_have_pin=False + no ESA PIN (nothing to do)
      no_gate_code    — rented + rentable + not locked/overlocked + no valid gate code

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

    def _esa_tag_audit(sid, uid):
        return f"ESA-{sid}-{uid}"

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

        # Load bRentable from ccws_units for the requested sites
        rentable_map = {}
        if site_ids:
            placeholders = ','.join(f':s{i}' for i in range(len(site_ids)))
            params = {f's{i}': sid for i, sid in enumerate(site_ids)}
            rows = session.execute(
                text(
                    f'SELECT "SiteID", "UnitID", "bRentable" '
                    f'FROM ccws_units WHERE "SiteID" IN ({placeholders}) '
                    f'AND deleted_at IS NULL'
                ),
                params,
            ).fetchall()
            for sid, uid, rentable in rows:
                rentable_map[(sid, uid)] = bool(rentable)

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

            is_rented = bool(gate and gate.is_rented)
            is_gate_locked = bool(gate and gate.is_gate_locked)
            is_overlocked = bool(gate and gate.is_overlocked)
            b_rentable = rentable_map.get((a.site_id, a.unit_id), False)
            has_valid_pin = bool(plain_pin and _PIN_RE_AUDIT.match(plain_pin))
            policy = site_configs.get(a.site_id, _AuditPolicyDefaults())
            tag = _esa_tag_audit(a.site_id, a.unit_id)

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
                    (e for e in access_list if e.get('name') == tag),
                    None,
                )
                esa_pin_value = esa_entry.get('pin') if esa_entry else None

                reason = None
                if should_have_pin_base and not has_valid_pin:
                    status = 'no_gate_code'
                elif should_have_pin_base and has_valid_pin:
                    if esa_entry and esa_pin_value == plain_pin:
                        status = 'synced'
                    else:
                        status = 'push_pending'
                else:
                    # should_have_pin = False
                    if esa_entry:
                        status = 'revoke_pending'
                        reason = _audit_revoke_reason(
                            is_rented, b_rentable, is_gate_locked, is_overlocked, policy
                        )
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

        _sl_audit(session, 'pin_audit', 'igloo',
                  detail=f'PIN audit for {len(results)} pair(s) across {len(site_ids)} site(s)')
        session.commit()

        return jsonify({'results': results, 'count': len(results)})
    except Exception:
        session.rollback()
        current_app.logger.exception("PIN audit error")
        return jsonify({'error': 'Failed to run PIN audit'}), 500
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

        # 3) Push to Igloo as custom PIN
        unit_name = gate.unit_name or f'Unit {unit_id}'
        client = IglooClient()
        result = client.create_custom_pin(device_id, pin, unit_name)

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
    """Create a custom/duration PIN on a device."""
    data = request.get_json()
    if not data or 'pin' not in data:
        return jsonify({'error': 'pin is required'}), 400

    pin = str(data['pin']).strip()
    name = (data.get('name') or '').strip()[:100] or 'Custom PIN'
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
        result = client.create_custom_pin(device_id, pin, name, start_dt=start_dt, end_dt=end_dt)
        _sl_audit(session, 'igloo_pin_created', 'igloo', device_id,
                  detail=f'Custom PIN created on {device_id}: {name}')
        session.commit()
        return jsonify({'success': True, 'result': result}), 201
    except Exception as e:
        session.rollback()
        current_app.logger.exception("Igloo create custom PIN error")
        return jsonify({'error': 'Failed to create PIN'}), 500
    finally:
        session.close()


@api_bp.route('/igloo/devices/<device_id>/pin/permanent', methods=['POST'])
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_igloo_create_permanent_pin(device_id):
    """Create a permanent algorithmic PIN."""
    session = get_session()
    try:
        client = _get_igloo_client()
        result = client.create_permanent_pin(device_id)
        _sl_audit(session, 'igloo_pin_created', 'igloo', device_id,
                  detail=f'Permanent PIN created on {device_id}')
        session.commit()
        return jsonify({'success': True, 'result': result}), 201
    except Exception as e:
        session.rollback()
        current_app.logger.exception("Igloo create permanent PIN error")
        return jsonify({'error': 'Failed to create permanent PIN'}), 500
    finally:
        session.close()


@api_bp.route('/igloo/devices/<device_id>/pin/daily', methods=['POST'])
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_igloo_create_daily_pin(device_id):
    """Create a daily rotating PIN."""
    session = get_session()
    try:
        client = _get_igloo_client()
        result = client.create_daily_pin(device_id)
        _sl_audit(session, 'igloo_pin_created', 'igloo', device_id,
                  detail=f'Daily PIN created on {device_id}')
        session.commit()
        return jsonify({'success': True, 'result': result}), 201
    except Exception as e:
        session.rollback()
        current_app.logger.exception("Igloo create daily PIN error")
        return jsonify({'error': 'Failed to create daily PIN'}), 500
    finally:
        session.close()


@api_bp.route('/igloo/devices/<device_id>/pin/hourly', methods=['POST'])
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_igloo_create_hourly_pin(device_id):
    """Create an hourly rotating PIN."""
    session = get_session()
    try:
        client = _get_igloo_client()
        result = client.create_hourly_pin(device_id)
        _sl_audit(session, 'igloo_pin_created', 'igloo', device_id,
                  detail=f'Hourly PIN created on {device_id}')
        session.commit()
        return jsonify({'success': True, 'result': result}), 201
    except Exception as e:
        session.rollback()
        current_app.logger.exception("Igloo create hourly PIN error")
        return jsonify({'error': 'Failed to create hourly PIN'}), 500
    finally:
        session.close()


@api_bp.route('/igloo/devices/<device_id>/pin/otp', methods=['POST'])
@require_auth
@_require_sl_session_access
@require_api_scope('smart_lock:write')
@rate_limit_api(max_requests=30, window_seconds=60)
def api_igloo_create_otp_pin(device_id):
    """Create a one-time PIN."""
    session = get_session()
    try:
        client = _get_igloo_client()
        result = client.create_otp_pin(device_id)
        _sl_audit(session, 'igloo_pin_created', 'igloo', device_id,
                  detail=f'OTP PIN created on {device_id}')
        session.commit()
        return jsonify({'success': True, 'result': result}), 201
    except Exception as e:
        session.rollback()
        current_app.logger.exception("Igloo create OTP PIN error")
        return jsonify({'error': 'Failed to create OTP PIN'}), 500
    finally:
        session.close()


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
    """Revoke a PIN or eKey."""
    session = get_session()
    try:
        client = _get_igloo_client()
        result = client.revoke_access(device_id, access_id)
        _sl_audit(session, 'igloo_access_revoked', 'igloo', device_id,
                  detail=f'Revoked access {access_id} on {device_id}')
        session.commit()
        return jsonify({'success': True, 'result': result})
    except Exception as e:
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
