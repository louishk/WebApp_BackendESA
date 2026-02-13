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

from flask import Blueprint, jsonify, request, current_app
from sqlalchemy import desc, func, case, text

from web.auth.jwt_auth import require_auth
from web.utils.rate_limit import rate_limit_api

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
os.makedirs(_CACHE_DIR, exist_ok=True)
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

            # Write to file cache
            try:
                response_data = response.get_json()
                if response_data is not None:
                    with open(path, 'w') as f:
                        json.dump(response_data, f)
            except (OSError, TypeError):
                pass

            return response
        wrapper.__name__ = func.__name__
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
        _pbi_engine = create_engine(pbi_url)
        _pbi_session_factory = sessionmaker(bind=_pbi_engine)
    return _pbi_session_factory()


# =============================================================================
# Status & Health
# =============================================================================

@api_bp.route('/status')
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
def health():
    """Health check endpoint."""
    return jsonify({
        'status': 'healthy',
        'timestamp': now_sgt().isoformat()
    })


# =============================================================================
# Jobs
# =============================================================================

@api_bp.route('/jobs')
@cached(ttl_seconds=30)
def api_list_jobs():
    """List all scheduled jobs."""
    from scheduler.config import SchedulerConfig
    from scheduler.utils import cron_to_human

    config = current_app.scheduler_config or SchedulerConfig.from_yaml()

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
def api_get_job(pipeline):
    """Get job details."""
    from scheduler.config import SchedulerConfig
    from scheduler.utils import cron_to_human

    config = current_app.scheduler_config or SchedulerConfig.from_yaml()

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
def api_update_job(pipeline):
    """Update pipeline schedule/settings."""
    from scheduler.config import SchedulerConfig
    from scheduler.utils import cron_to_human

    config = SchedulerConfig.from_yaml()

    if pipeline not in config.pipelines:
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
            return jsonify({'error': f'Invalid cron expression: {e}'}), 400

    success = config.update_pipeline_schedule(
        pipeline_name=pipeline,
        cron=cron,
        enabled=enabled,
        priority=priority
    )

    if not success:
        return jsonify({'error': 'Failed to update pipeline'}), 500

    p = config.pipelines[pipeline]
    cron_expr = p.schedule_config.get('cron', 'N/A')

    return jsonify({
        'success': True,
        'pipeline_name': pipeline,
        'schedule': cron_expr,
        'schedule_human': cron_to_human(cron_expr),
        'enabled': p.enabled,
        'priority': p.priority,
    })


@api_bp.route('/jobs/<pipeline>/enable', methods=['POST'])
@require_auth
def api_enable_job(pipeline):
    """Enable a pipeline."""
    from scheduler.config import SchedulerConfig

    config = SchedulerConfig.from_yaml()
    if pipeline not in config.pipelines:
        return jsonify({'error': 'Pipeline not found'}), 404

    success = config.update_pipeline_schedule(pipeline_name=pipeline, enabled=True)
    return jsonify({'success': success, 'enabled': True})


@api_bp.route('/jobs/<pipeline>/disable', methods=['POST'])
@require_auth
def api_disable_job(pipeline):
    """Disable a pipeline."""
    from scheduler.config import SchedulerConfig

    config = SchedulerConfig.from_yaml()
    if pipeline not in config.pipelines:
        return jsonify({'error': 'Pipeline not found'}), 404

    success = config.update_pipeline_schedule(pipeline_name=pipeline, enabled=False)
    return jsonify({'success': success, 'enabled': False})


@api_bp.route('/schedules/presets')
def api_schedule_presets():
    """Get available schedule presets."""
    from scheduler.utils import SCHEDULE_PRESETS
    return jsonify({'presets': SCHEDULE_PRESETS})


@api_bp.route('/jobs/upcoming')
@cached(ttl_seconds=15)
def api_upcoming_jobs():
    """Get upcoming scheduled executions."""
    from scheduler.config import SchedulerConfig
    from scheduler.utils import cron_to_human
    import pytz

    config = current_app.scheduler_config or SchedulerConfig.from_yaml()

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


@api_bp.route('/data-freshness')
@cached(ttl_seconds=60)
def api_data_freshness():
    """Get latest data dates for all pipelines."""
    from scheduler.config import SchedulerConfig, get_pbi_engine

    config = current_app.scheduler_config or SchedulerConfig.from_yaml()
    freshness = {}

    try:
        pbi_engine = get_pbi_engine(config)
    except Exception as e:
        return jsonify({'error': f'Could not connect to PBI database: {str(e)[:100]}'})

    # Build queries for all pipelines
    queries_to_run = []
    for name, pipeline in config.pipelines.items():
        table = pipeline.data_freshness.table
        column = pipeline.data_freshness.date_column

        if not table:
            freshness[name] = {'latest_date': None, 'error': 'No table configured'}
        elif not _is_valid_sql_identifier(table) or not _is_valid_sql_identifier(column):
            freshness[name] = {'latest_date': None, 'error': 'Invalid table or column name'}
        else:
            queries_to_run.append((name, table, column))

    # Execute all queries in a single connection
    if queries_to_run:
        try:
            with pbi_engine.connect() as conn:
                for name, table, column in queries_to_run:
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
                        freshness[name] = {'latest_date': None, 'error': str(e)[:100]}
        except Exception as e:
            # Connection failed - mark all as error
            for name, _, _ in queries_to_run:
                freshness[name] = {'latest_date': None, 'error': str(e)[:100]}

    return jsonify(freshness)


@api_bp.route('/jobs/<pipeline>/run-async', methods=['POST'])
@require_auth
@rate_limit_api(max_requests=10, window_seconds=60)
def api_run_job_async(pipeline):
    """Trigger job execution asynchronously."""
    from scheduler.config import SchedulerConfig
    from scheduler.executor import PipelineExecutor
    from scheduler.models import JobHistory
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    config = current_app.scheduler_config or SchedulerConfig.from_yaml()

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
@rate_limit_api(max_requests=10, window_seconds=60)
def api_run_job(pipeline):
    """Trigger job execution (synchronous)."""
    from scheduler.config import SchedulerConfig
    from scheduler.executor import PipelineExecutor

    config = current_app.scheduler_config or SchedulerConfig.from_yaml()

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
@cached(ttl_seconds=15)
def api_list_history():
    """List execution history with pagination."""
    from scheduler.models import JobHistory

    pipeline = request.args.get('pipeline')
    status = request.args.get('status')
    limit = int(request.args.get('limit', 50))
    offset = int(request.args.get('offset', 0))
    since = request.args.get('since')

    session = get_session()
    try:
        query = session.query(JobHistory)

        if pipeline:
            query = query.filter(JobHistory.pipeline_name == pipeline)
        if status:
            query = query.filter(JobHistory.status == status)
        if since:
            since_date = datetime.fromisoformat(since)
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
        data['error_traceback'] = record.error_traceback
        return jsonify(data)
    finally:
        session.close()


@api_bp.route('/history/stats')
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
            'error_message': job.error_message,
            'error_traceback': job.error_traceback,
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
def api_resources():
    """Get current resource usage."""
    from scheduler.resource_manager import get_resource_manager

    rm = get_resource_manager()
    return jsonify(rm.get_all_usage_dict())


# =============================================================================
# Config
# =============================================================================

@api_bp.route('/config')
def api_config():
    """Get scheduler configuration."""
    from scheduler.config import SchedulerConfig

    config = current_app.scheduler_config or SchedulerConfig.from_yaml()

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
            }
        }

        return jsonify(result)
    finally:
        session.close()


@api_bp.route('/services/scheduler/start', methods=['POST'])
@require_auth
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


@api_bp.route('/services/<service>/restart', methods=['POST'])
@require_auth
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
def api_list_pipelines():
    """List all pipeline configurations."""
    from scheduler.config import SchedulerConfig

    config = SchedulerConfig.from_yaml()

    pipelines = []
    for name, p in config.pipelines.items():
        pipelines.append({
            'name': name,
            'display_name': p.display_name,
            'description': getattr(p, 'description', ''),
            'module_path': p.module_path,
            'enabled': p.enabled,
            'schedule': p.schedule_config,
            'priority': p.priority,
            'depends_on': p.depends_on,
            'conflicts_with': p.conflicts_with,
            'resource_group': p.resource_group,
            'max_db_connections': p.max_db_connections,
            'timeout_seconds': p.timeout_seconds,
            'retry': {
                'max_attempts': p.retry.max_attempts,
                'delay_seconds': p.retry.delay_seconds,
                'backoff_multiplier': p.retry.backoff_multiplier,
            },
            'data_freshness': {
                'table': p.data_freshness.table,
                'date_column': p.data_freshness.date_column,
            },
            'default_args': p.default_args,
        })

    return jsonify({'pipelines': pipelines})


@api_bp.route('/pipelines/<name>')
def api_get_pipeline(name):
    """Get a specific pipeline configuration."""
    from scheduler.config import SchedulerConfig

    config = SchedulerConfig.from_yaml()

    if name not in config.pipelines:
        return jsonify({'error': 'Pipeline not found'}), 404

    p = config.pipelines[name]
    return jsonify({
        'name': name,
        'display_name': p.display_name,
        'description': getattr(p, 'description', ''),
        'module_path': p.module_path,
        'enabled': p.enabled,
        'schedule': p.schedule_config,
        'priority': p.priority,
        'depends_on': p.depends_on,
        'conflicts_with': p.conflicts_with,
        'resource_group': p.resource_group,
        'max_db_connections': p.max_db_connections,
        'timeout_seconds': p.timeout_seconds,
        'retry': {
            'max_attempts': p.retry.max_attempts,
            'delay_seconds': p.retry.delay_seconds,
            'backoff_multiplier': p.retry.backoff_multiplier,
        },
        'data_freshness': {
            'table': p.data_freshness.table,
            'date_column': p.data_freshness.date_column,
        },
        'default_args': p.default_args,
    })


@api_bp.route('/pipelines', methods=['POST'])
@require_auth
@rate_limit_api(max_requests=20, window_seconds=60)
def api_create_pipeline():
    """Create a new pipeline."""
    from scheduler.config import SchedulerConfig
    import yaml

    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    name = data.get('name')
    if not name:
        return jsonify({'error': 'Pipeline name is required'}), 400

    if not name.replace('_', '').replace('-', '').isalnum():
        return jsonify({'error': 'Pipeline name must be alphanumeric with underscores/hyphens'}), 400

    config = SchedulerConfig.from_yaml()
    if name in config.pipelines:
        return jsonify({'error': f'Pipeline {name} already exists'}), 400

    module_path = data.get('module_path', f'datalayer.{name}')
    is_valid, msg = _validate_module_path(module_path)
    if not is_valid:
        return jsonify({'error': msg}), 400

    pipeline_config = {
        'display_name': data.get('display_name', name.replace('_', ' ').title()),
        'description': data.get('description', ''),
        'module_path': module_path,
        'enabled': data.get('enabled', False),
        'schedule': {
            'type': 'cron',
            'cron': data.get('cron', '0 6 * * *')
        },
        'priority': data.get('priority', 5),
        'depends_on': data.get('depends_on', []),
        'conflicts_with': data.get('conflicts_with', []),
        'resource_group': data.get('resource_group', 'http_api'),
        'max_db_connections': data.get('max_db_connections', 2),
        'timeout_seconds': data.get('timeout_seconds', 3600),
        'retry': {
            'max_attempts': data.get('max_retries', 3),
            'delay_seconds': data.get('retry_delay', 300),
            'backoff_multiplier': data.get('backoff_multiplier', 2),
        },
        'data_freshness': {
            'table': data.get('freshness_table', ''),
            'date_column': data.get('freshness_column', ''),
        },
        'default_args': data.get('default_args', {'mode': 'auto'}),
    }

    config_path = Path(__file__).parent.parent.parent / 'config' / 'pipelines.yaml'
    with open(config_path) as f:
        yaml_data = yaml.safe_load(f)

    yaml_data['pipelines'][name] = pipeline_config

    with open(config_path, 'w') as f:
        yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False)

    return jsonify({
        'success': True,
        'message': f'Pipeline {name} created',
        'pipeline': pipeline_config
    })


@api_bp.route('/pipelines/<name>', methods=['PUT'])
@require_auth
@rate_limit_api(max_requests=20, window_seconds=60)
def api_update_pipeline(name):
    """Update a pipeline configuration."""
    from scheduler.config import SchedulerConfig
    import yaml

    config = SchedulerConfig.from_yaml()
    if name not in config.pipelines:
        return jsonify({'error': 'Pipeline not found'}), 404

    data = request.get_json()
    if not data:
        return jsonify({'error': 'No data provided'}), 400

    config_path = Path(__file__).parent.parent.parent / 'config' / 'pipelines.yaml'
    with open(config_path) as f:
        yaml_data = yaml.safe_load(f)

    current = yaml_data['pipelines'][name]

    if 'display_name' in data:
        current['display_name'] = data['display_name']
    if 'description' in data:
        current['description'] = data['description']
    if 'module_path' in data:
        is_valid, msg = _validate_module_path(data['module_path'])
        if not is_valid:
            return jsonify({'error': msg}), 400
        current['module_path'] = data['module_path']
    if 'enabled' in data:
        current['enabled'] = data['enabled']
    if 'cron' in data:
        current['schedule'] = {'type': 'cron', 'cron': data['cron']}
    if 'priority' in data:
        current['priority'] = data['priority']
    if 'depends_on' in data:
        current['depends_on'] = data['depends_on']
    if 'conflicts_with' in data:
        current['conflicts_with'] = data['conflicts_with']
    if 'resource_group' in data:
        current['resource_group'] = data['resource_group']
    if 'max_db_connections' in data:
        current['max_db_connections'] = data['max_db_connections']
    if 'timeout_seconds' in data:
        current['timeout_seconds'] = data['timeout_seconds']
    if 'max_retries' in data:
        current.setdefault('retry', {})['max_attempts'] = data['max_retries']
    if 'freshness_table' in data:
        current.setdefault('data_freshness', {})['table'] = data['freshness_table']
    if 'freshness_column' in data:
        current.setdefault('data_freshness', {})['date_column'] = data['freshness_column']

    with open(config_path, 'w') as f:
        yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False)

    return jsonify({
        'success': True,
        'message': f'Pipeline {name} updated',
    })


@api_bp.route('/pipelines/<name>', methods=['DELETE'])
@require_auth
@rate_limit_api(max_requests=10, window_seconds=60)
def api_delete_pipeline(name):
    """Delete a pipeline."""
    from scheduler.config import SchedulerConfig
    import yaml

    config = SchedulerConfig.from_yaml()
    if name not in config.pipelines:
        return jsonify({'error': 'Pipeline not found'}), 404

    config_path = Path(__file__).parent.parent.parent / 'config' / 'pipelines.yaml'
    with open(config_path) as f:
        yaml_data = yaml.safe_load(f)

    del yaml_data['pipelines'][name]

    with open(config_path, 'w') as f:
        yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False)

    return jsonify({
        'success': True,
        'message': f'Pipeline {name} deleted',
    })


@api_bp.route('/modules')
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
            'details': str(e)
        }), 502

    except Exception as e:
        current_app.logger.error(f"Unexpected error during billing day update: {e}")
        return jsonify({
            'success': False,
            'error': 'Unexpected error',
            'details': str(e)
        }), 500

    finally:
        soap_client.close()


# =============================================================================
# Sites List
# =============================================================================

@api_bp.route('/sites')
@require_auth
@cached(ttl_seconds=300)
def api_list_sites():
    """List all sites for dropdown selection."""
    from common.models import SiteInfo

    session = get_pbi_session()
    try:
        sites = session.query(SiteInfo).order_by(SiteInfo.Name).all()
        return jsonify({
            'sites': [
                {
                    'site_id': s.SiteID,
                    'site_code': s.SiteCode,
                    'name': s.Name
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
def api_inventory_units():
    """Get raw unit data from units_info for selected sites."""
    site_ids_param = request.args.get('site_ids', '')
    if not site_ids_param:
        return jsonify({'error': 'site_ids parameter is required'}), 400

    try:
        site_ids = [int(s.strip()) for s in site_ids_param.split(',') if s.strip()]
    except ValueError:
        return jsonify({'error': 'site_ids must be comma-separated integers'}), 400

    if not site_ids:
        return jsonify({'error': 'At least one site_id is required'}), 400

    session = get_pbi_session()
    try:
        placeholders = ', '.join([f':sid{i}' for i in range(len(site_ids))])
        params = {f'sid{i}': sid for i, sid in enumerate(site_ids)}

        query = text(f"""
            SELECT "SiteID", "UnitID", "sLocationCode", "sUnitName", "sTypeName",
                   "dcWidth", "dcLength", "bClimate", "bInside", "bPower", "bAlarm",
                   "sUnitNote", "sUnitDesc", "bRented", "bRentable",
                   "dcStdRate", "dcWebRate", "dcPushRate", "dcBoardRate",
                   "iFloor", "UnitTypeID"
            FROM units_info
            WHERE "SiteID" IN ({placeholders})
            ORDER BY "SiteID", "sUnitName"
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

    finally:
        session.close()


@api_bp.route('/inventory/distinct-types')
@require_auth
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
                site_ids = [int(s.strip()) for s in site_ids_param.split(',') if s.strip()]
            except ValueError:
                return jsonify({'error': 'site_ids must be comma-separated integers'}), 400

            placeholders = ', '.join([f':sid{i}' for i in range(len(site_ids))])
            params = {f'sid{i}': sid for i, sid in enumerate(site_ids)}

            query = text(f"""
                SELECT DISTINCT "sTypeName"
                FROM units_info
                WHERE "SiteID" IN ({placeholders})
                  AND "sTypeName" IS NOT NULL
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
    username = data.get('username', 'unknown')

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
def api_inventory_get_overrides():
    """Get saved per-unit overrides for selected sites."""
    from web.models.inventory import InventoryUnitOverride

    site_ids_param = request.args.get('site_ids', '')
    if not site_ids_param:
        return jsonify({'error': 'site_ids parameter is required'}), 400

    try:
        site_ids = [int(s.strip()) for s in site_ids_param.split(',') if s.strip()]
    except ValueError:
        return jsonify({'error': 'site_ids must be comma-separated integers'}), 400

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
def api_inventory_upsert_overrides():
    """Bulk upsert per-unit overrides."""
    from web.models.inventory import InventoryUnitOverride

    data = request.get_json()
    if not data or 'overrides' not in data:
        return jsonify({'error': 'overrides array is required'}), 400

    overrides_data = data['overrides']
    username = data.get('username', 'unknown')

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


# =============================================================================
# API Statistics - Consumption Monitoring
# =============================================================================

@api_bp.route('/statistics/summary')
@require_auth
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

        # Calls per day
        daily = session.query(
            func.date_trunc('day', ApiStatistic.called_at).label('day'),
            func.count(ApiStatistic.id).label('count')
        ).filter(
            ApiStatistic.called_at >= since
        ).group_by('day').order_by('day').all()

        return jsonify({
            'period': period,
            'since': since.isoformat(),
            'total_calls': total_calls,
            'avg_response_time_ms': round(float(avg_response), 2),
            'error_count': error_count,
            'error_rate': round(error_count / total_calls * 100, 2) if total_calls > 0 else 0,
            'calls_per_day': [
                {'date': d.day.isoformat() if d.day else None, 'count': d.count}
                for d in daily
            ],
        })
    finally:
        session.close()


@api_bp.route('/statistics/endpoints')
@require_auth
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

        daily = session.query(
            func.date_trunc('day', ExternalApiStatistic.called_at).label('day'),
            func.count(ExternalApiStatistic.id).label('count'),
        ).filter(
            ExternalApiStatistic.called_at >= since
        ).group_by('day').order_by('day').all()

        return jsonify({
            'period': period,
            'since': since.isoformat(),
            'total_calls': total,
            'avg_response_time_ms': round(float(avg_ms), 2),
            'error_count': error_count,
            'error_rate': round(error_count / total * 100, 2) if total > 0 else 0,
            'by_service': [
                {
                    'service_name': s.service_name,
                    'total_calls': s.count,
                    'avg_response_ms': round(float(s.avg_ms or 0), 2),
                    'error_count': int(s.errors or 0),
                }
                for s in by_service
            ],
            'calls_per_day': [
                {'date': d.day.isoformat() if d.day else None, 'count': d.count}
                for d in daily
            ],
        })
    finally:
        session.close()


@api_bp.route('/statistics/external/services')
@require_auth
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
