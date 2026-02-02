"""
REST API routes for scheduler.
Refactored from app.py to use blueprint pattern.
"""

from datetime import datetime, timedelta
import pytz
from pathlib import Path
from uuid import uuid4, UUID
import threading

from flask import Blueprint, jsonify, request, current_app
from sqlalchemy import desc, func, case, text

from web.auth.jwt_auth import require_auth

api_bp = Blueprint('api', __name__, url_prefix='/api')

# Singapore timezone for all timestamps
SGT = pytz.timezone('Asia/Singapore')


def now_sgt():
    """Get current time in Singapore timezone."""
    return datetime.now(SGT)


def get_session():
    """Get database session from app context."""
    return current_app.get_db_session()


# =============================================================================
# Status & Health
# =============================================================================

@api_bp.route('/status')
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


@api_bp.route('/data-freshness')
def api_data_freshness():
    """Get latest data dates for all pipelines."""
    from scheduler.config import SchedulerConfig, get_pbi_engine

    config = current_app.scheduler_config or SchedulerConfig.from_yaml()
    freshness = {}

    try:
        pbi_engine = get_pbi_engine(config)
    except Exception as e:
        return jsonify({'error': f'Could not connect to PBI database: {str(e)[:100]}'})

    for name, pipeline in config.pipelines.items():
        table = pipeline.data_freshness.table
        column = pipeline.data_freshness.date_column

        if not table:
            freshness[name] = {'latest_date': None, 'error': 'No table configured'}
            continue

        try:
            with pbi_engine.connect() as conn:
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

    return jsonify(freshness)


@api_bp.route('/jobs/<pipeline>/run-async', methods=['POST'])
@require_auth
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
            scheduled_at=now_sgt(),
            started_at=now_sgt(),
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

        job_history.completed_at = now_sgt()
        job_history.duration_seconds = result.duration_seconds
        job_history.records_processed = result.records_processed
        job_history.status = 'completed' if result.success else 'failed'
        if not result.success:
            job_history.error_message = result.error_message
            job_history.error_traceback = result.stderr[:5000] if result.stderr else None
        session.commit()
        session.close()

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
            job.completed_at = now_sgt()
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
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@api_bp.route('/services/scheduler/stop', methods=['POST'])
@require_auth
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
            return jsonify({
                'success': False,
                'error': f'Permission denied to stop process {pid}'
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

    pipeline_config = {
        'display_name': data.get('display_name', name.replace('_', ' ').title()),
        'description': data.get('description', ''),
        'module_path': data.get('module_path', f'datalayer.{name}'),
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
