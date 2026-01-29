"""
Flask Web Application for Scheduler Dashboard.
Provides REST API and simple HTML dashboard.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, send_from_directory, g
from flask_cors import CORS

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import JWT auth helpers
from web.auth import require_auth, optional_auth, init_auth


def create_app(config=None, db_url=None):
    """
    Create Flask application.

    Args:
        config: SchedulerConfig instance
        db_url: Database URL

    Returns:
        Flask application
    """
    app = Flask(__name__)
    CORS(app, supports_credentials=True)

    # Initialize JWT authentication
    init_auth(app)

    # Prevent caching of API responses
    @app.after_request
    def add_no_cache_headers(response):
        if request.path.startswith('/api/'):
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'
        return response

    # Store config
    app.scheduler_config = config
    app.db_url = db_url

    # Lazy imports to avoid circular dependencies
    _db_engine = None
    _session_factory = None

    def get_session():
        nonlocal _db_engine, _session_factory
        if _db_engine is None:
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker
            _db_engine = create_engine(app.db_url)
            _session_factory = sessionmaker(bind=_db_engine)
        return _session_factory()

    # =========================================================================
    # Dashboard Routes
    # =========================================================================

    @app.route('/')
    def dashboard():
        """Main dashboard page."""
        return render_template('dashboard.html')

    @app.route('/jobs')
    def jobs_page():
        """Jobs management page."""
        return render_template('jobs.html')

    @app.route('/history')
    def history_page():
        """Execution history page."""
        return render_template('history.html')

    @app.route('/settings')
    def settings_page():
        """Settings and administration page."""
        return render_template('settings.html')

    @app.route('/static/logo.jpeg')
    def serve_logo():
        """Serve the logo from media folder."""
        media_path = Path(__file__).parent.parent / 'media'
        return send_from_directory(media_path, 'ESA Logo.jpeg')

    # =========================================================================
    # REST API - Status
    # =========================================================================

    # Track web UI start time
    app.web_started_at = datetime.now()

    @app.route('/api/status')
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

            # Count running jobs from history
            running_count = session.query(JobHistory).filter_by(status='running').count()

            if state and state.status == 'running':
                # Full scheduler is running
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
                # Web UI only mode
                web_uptime = (datetime.now() - app.web_started_at).total_seconds()
                return jsonify({
                    'status': 'web_ui_only',
                    'mode': 'standalone',
                    'started_at': app.web_started_at.isoformat(),
                    'uptime_seconds': web_uptime,
                    'version': __version__,
                    'resources': resources,
                    'running_jobs': running_count,
                })

        finally:
            session.close()

    @app.route('/health')
    def health():
        """Health check endpoint."""
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat()
        })

    # =========================================================================
    # REST API - Jobs
    # =========================================================================

    @app.route('/api/jobs')
    def api_list_jobs():
        """List all scheduled jobs."""
        from scheduler.config import SchedulerConfig
        from scheduler.utils import cron_to_human

        if app.scheduler_config:
            config = app.scheduler_config
        else:
            config = SchedulerConfig.from_yaml()

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

        # Sort by priority
        jobs.sort(key=lambda x: x['priority'])

        return jsonify({'jobs': jobs})

    @app.route('/api/jobs/<pipeline>')
    def api_get_job(pipeline):
        """Get job details."""
        from scheduler.config import SchedulerConfig
        from scheduler.utils import cron_to_human

        if app.scheduler_config:
            config = app.scheduler_config
        else:
            config = SchedulerConfig.from_yaml()

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

    @app.route('/api/jobs/<pipeline>', methods=['PUT'])
    @require_auth
    def api_update_job(pipeline):
        """Update pipeline schedule/settings. Requires authentication."""
        from scheduler.config import SchedulerConfig
        from scheduler.utils import cron_to_human

        config = SchedulerConfig.from_yaml()

        if pipeline not in config.pipelines:
            return jsonify({'error': 'Pipeline not found'}), 404

        data = request.get_json() or {}

        # Extract update fields
        cron = data.get('cron')
        enabled = data.get('enabled')
        priority = data.get('priority')

        # Validate priority if provided
        if priority is not None:
            try:
                priority = int(priority)
                if not 1 <= priority <= 10:
                    return jsonify({'error': 'Priority must be between 1 and 10'}), 400
            except (ValueError, TypeError):
                return jsonify({'error': 'Priority must be an integer'}), 400

        # Validate cron if provided
        if cron:
            try:
                from croniter import croniter
                croniter(cron)
            except Exception as e:
                return jsonify({'error': f'Invalid cron expression: {e}'}), 400

        # Update config
        success = config.update_pipeline_schedule(
            pipeline_name=pipeline,
            cron=cron,
            enabled=enabled,
            priority=priority
        )

        if not success:
            return jsonify({'error': 'Failed to update pipeline'}), 500

        # Return updated pipeline info
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

    @app.route('/api/jobs/<pipeline>/enable', methods=['POST'])
    @require_auth
    def api_enable_job(pipeline):
        """Enable a pipeline. Requires authentication."""
        from scheduler.config import SchedulerConfig

        config = SchedulerConfig.from_yaml()
        if pipeline not in config.pipelines:
            return jsonify({'error': 'Pipeline not found'}), 404

        success = config.update_pipeline_schedule(pipeline_name=pipeline, enabled=True)
        return jsonify({'success': success, 'enabled': True})

    @app.route('/api/jobs/<pipeline>/disable', methods=['POST'])
    @require_auth
    def api_disable_job(pipeline):
        """Disable a pipeline. Requires authentication."""
        from scheduler.config import SchedulerConfig

        config = SchedulerConfig.from_yaml()
        if pipeline not in config.pipelines:
            return jsonify({'error': 'Pipeline not found'}), 404

        success = config.update_pipeline_schedule(pipeline_name=pipeline, enabled=False)
        return jsonify({'success': success, 'enabled': False})

    @app.route('/api/schedules/presets')
    def api_schedule_presets():
        """Get available schedule presets."""
        from scheduler.utils import SCHEDULE_PRESETS
        return jsonify({'presets': SCHEDULE_PRESETS})

    @app.route('/api/jobs/upcoming')
    def api_upcoming_jobs():
        """Get upcoming scheduled executions with next run times."""
        from scheduler.config import SchedulerConfig
        from scheduler.utils import cron_to_human
        import pytz

        if app.scheduler_config:
            config = app.scheduler_config
        else:
            config = SchedulerConfig.from_yaml()

        # Singapore timezone
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

                # Calculate seconds until next run
                seconds_until = (next_run - now).total_seconds()

                upcoming.append({
                    'pipeline_name': name,
                    'display_name': pipeline.display_name,
                    'schedule': cron_expr,
                    'schedule_human': cron_to_human(cron_expr),
                    'next_run': next_run.isoformat(),
                    'seconds_until': int(seconds_until),
                })
            except Exception as e:
                # Skip pipelines with invalid cron
                continue

        # Sort by next run time
        upcoming.sort(key=lambda x: x['seconds_until'])

        return jsonify({'upcoming': upcoming})

    @app.route('/api/data-freshness')
    def api_data_freshness():
        """Get latest data dates for all pipelines from PBI database."""
        from scheduler.config import SchedulerConfig, get_pbi_engine
        from sqlalchemy import text

        if app.scheduler_config:
            config = app.scheduler_config
        else:
            config = SchedulerConfig.from_yaml()

        freshness = {}

        # Use PBI database engine for data tables (not scheduler database)
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
                # Query max date from PBI database table
                with pbi_engine.connect() as conn:
                    query = text(f'SELECT MAX("{column}") as max_date FROM "{table}"')
                    result = conn.execute(query).fetchone()

                    if result and result[0]:
                        max_date = result[0]
                        # Format date/datetime
                        if hasattr(max_date, 'isoformat'):
                            freshness[name] = {'latest_date': max_date.isoformat()}
                        else:
                            freshness[name] = {'latest_date': str(max_date)}
                    else:
                        freshness[name] = {'latest_date': None}
            except Exception as e:
                freshness[name] = {'latest_date': None, 'error': str(e)[:100]}

        return jsonify(freshness)

    @app.route('/api/jobs/<pipeline>/run-async', methods=['POST'])
    @require_auth
    def api_run_job_async(pipeline):
        """Trigger job execution asynchronously with streaming support. Requires authentication."""
        from scheduler.config import SchedulerConfig
        from scheduler.executor import PipelineExecutor
        from scheduler.models import JobHistory
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from uuid import uuid4
        import threading

        if app.scheduler_config:
            config = app.scheduler_config
        else:
            config = SchedulerConfig.from_yaml()

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

        # Run in background thread with history tracking
        def run_pipeline():
            from scheduler.resource_manager import get_resource_manager

            # Create history record
            engine = create_engine(app.db_url)
            Session = sessionmaker(bind=engine)
            session = Session()

            job_history = JobHistory(
                job_id=f"{pipeline}_{execution_id}",
                pipeline_name=pipeline,
                execution_id=execution_id,
                status='running',
                priority=p.priority,
                scheduled_at=datetime.now(),
                started_at=datetime.now(),
                mode=mode,
                parameters=final_args,
                triggered_by='web'
            )
            session.add(job_history)
            session.commit()

            # Get resource manager and track resource usage
            rm = get_resource_manager()
            resource_group = p.resource_group  # soap_api, http_api, etc.
            db_slots = p.max_db_connections

            try:
                # Acquire resources before execution
                with rm.acquire(resource_group, count=1, timeout=300, job_id=str(execution_id)):
                    with rm.acquire('db_pool', count=db_slots, timeout=300, job_id=str(execution_id)):
                        # Execute pipeline
                        executor = PipelineExecutor()
                        result = executor.execute_streaming(
                            module_path=p.module_path,
                            args=final_args,
                            execution_id=execution_id,
                            timeout_seconds=p.timeout_seconds
                        )
            except TimeoutError as e:
                # Resource acquisition timeout
                from scheduler.executor import ExecutionResult
                result = ExecutionResult(
                    success=False,
                    exit_code=-1,
                    stdout='',
                    stderr=str(e),
                    duration_seconds=0,
                    error_message=f"Resource acquisition timeout: {e}"
                )

            # Update history with result
            job_history.completed_at = datetime.now()
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

    @app.route('/api/executions/<execution_id>/output')
    def api_get_execution_output(execution_id):
        """Get current output for a running execution."""
        from scheduler.executor import get_execution_output
        output, status = get_execution_output(execution_id)
        return jsonify({
            'execution_id': execution_id,
            'output': output,
            'status': status,
        })

    @app.route('/api/executions/<execution_id>/stream')
    def api_stream_execution(execution_id):
        """Server-Sent Events stream of execution output."""
        from scheduler.executor import get_execution_output
        import time

        def generate():
            last_index = 0
            wait_count = 0
            max_wait = 60  # Wait up to 30 seconds for execution to start

            while True:
                output, status = get_execution_output(execution_id)
                current_status = status.get('status', 'unknown')

                # Send new lines
                if len(output) > last_index:
                    for line in output[last_index:]:
                        yield f"data: {line}\n\n"
                    last_index = len(output)
                    wait_count = 0  # Reset wait when we get output

                # Check if done (completed/failed are terminal states)
                if current_status in ('completed', 'failed', 'error', 'timeout'):
                    yield f"event: done\ndata: {current_status}\n\n"
                    break

                # Handle unknown status (execution not registered yet)
                if current_status == 'unknown':
                    wait_count += 1
                    if wait_count > max_wait:
                        yield f"data: [ERROR] Timed out waiting for execution\n\n"
                        yield f"event: done\ndata: timeout\n\n"
                        break

                time.sleep(0.5)

        return app.response_class(
            generate(),
            mimetype='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'X-Accel-Buffering': 'no',
            }
        )

    @app.route('/api/jobs/<pipeline>/run', methods=['POST'])
    @require_auth
    def api_run_job(pipeline):
        """Trigger job execution. Requires authentication."""
        from scheduler.config import SchedulerConfig
        from scheduler.executor import PipelineExecutor
        from uuid import uuid4

        if app.scheduler_config:
            config = app.scheduler_config
        else:
            config = SchedulerConfig.from_yaml()

        if pipeline not in config.pipelines:
            return jsonify({'error': 'Pipeline not found'}), 404

        # Get args from request
        data = request.get_json() or {}
        mode = data.get('mode', 'auto')
        args = data.get('args', {})
        args['mode'] = mode

        # Execute
        p = config.pipelines[pipeline]
        execution_id = uuid4()

        # Merge with defaults
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

    # =========================================================================
    # REST API - History
    # =========================================================================

    @app.route('/api/history')
    def api_list_history():
        """List execution history with pagination."""
        from sqlalchemy import desc
        from scheduler.models import JobHistory

        # Parse query params
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

    @app.route('/api/history/<execution_id>')
    def api_get_execution(execution_id):
        """Get execution details."""
        from scheduler.models import JobHistory
        from uuid import UUID

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

    @app.route('/api/history/stats')
    def api_history_stats():
        """Get execution statistics."""
        from sqlalchemy import func, case
        from scheduler.models import JobHistory

        # Parse period
        period = request.args.get('period', '7d')
        days = {'1d': 1, '7d': 7, '30d': 30, '90d': 90}.get(period, 7)
        since_date = datetime.now() - timedelta(days=days)

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

    @app.route('/api/history/<int:history_id>')
    def api_get_history_detail(history_id):
        """Get detailed execution record including error traceback."""
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

    @app.route('/api/history/cleanup-stale', methods=['POST'])
    @require_auth
    def api_cleanup_stale():
        """Mark stale running jobs as failed (interrupted). Requires authentication."""
        from scheduler.models import JobHistory

        session = get_session()
        try:
            stale = session.query(JobHistory).filter_by(status='running').all()
            fixed = []
            for job in stale:
                job.status = 'failed'
                job.error_message = 'Interrupted - server restarted'
                job.completed_at = datetime.now()
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

    # =========================================================================
    # REST API - Resources
    # =========================================================================

    @app.route('/api/resources')
    def api_resources():
        """Get current resource usage."""
        from scheduler.resource_manager import get_resource_manager

        rm = get_resource_manager()
        return jsonify(rm.get_all_usage_dict())

    # =========================================================================
    # REST API - Config
    # =========================================================================

    @app.route('/api/config')
    def api_config():
        """Get scheduler configuration."""
        from scheduler.config import SchedulerConfig

        if app.scheduler_config:
            config = app.scheduler_config
        else:
            config = SchedulerConfig.from_yaml()

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

    # =========================================================================
    # REST API - Service Management
    # =========================================================================

    @app.route('/api/services/status')
    def api_services_status():
        """Get status of scheduler services."""
        import subprocess

        services = {
            'web_ui': 'backend-scheduler-web',
            'scheduler': 'backend-scheduler'
        }

        result = {}
        for key, service in services.items():
            try:
                output = subprocess.run(
                    ['sudo', '/bin/systemctl', 'status', service],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                # Parse status
                is_active = 'Active: active' in output.stdout
                result[key] = {
                    'service': service,
                    'active': is_active,
                    'status': 'running' if is_active else 'stopped',
                }
            except Exception as e:
                result[key] = {
                    'service': service,
                    'active': False,
                    'status': 'unknown',
                    'error': str(e)
                }

        return jsonify(result)

    @app.route('/api/services/<service>/restart', methods=['POST'])
    @require_auth
    def api_restart_service(service):
        """Restart a scheduler service. Requires authentication."""
        import subprocess

        service_map = {
            'web_ui': 'backend-scheduler-web',
            'scheduler': 'backend-scheduler'
        }

        if service not in service_map:
            return jsonify({'error': f'Unknown service: {service}'}), 400

        service_name = service_map[service]

        try:
            result = subprocess.run(
                ['sudo', '/bin/systemctl', 'restart', service_name],
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0:
                return jsonify({
                    'success': True,
                    'service': service_name,
                    'message': f'{service_name} restarted successfully'
                })
            else:
                return jsonify({
                    'success': False,
                    'service': service_name,
                    'error': result.stderr or 'Restart failed'
                }), 500

        except subprocess.TimeoutExpired:
            return jsonify({
                'success': False,
                'error': 'Restart command timed out'
            }), 500
        except Exception as e:
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

    # =========================================================================
    # REST API - Pipeline Management
    # =========================================================================

    @app.route('/api/pipelines')
    def api_list_pipelines():
        """List all pipeline configurations."""
        from scheduler.config import SchedulerConfig

        config = SchedulerConfig.from_yaml()

        pipelines = []
        for name, p in config.pipelines.items():
            pipelines.append({
                'name': name,
                'display_name': p.display_name,
                'description': p.description,
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

    @app.route('/api/pipelines/<name>')
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
            'description': p.description,
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

    @app.route('/api/pipelines', methods=['POST'])
    @require_auth
    def api_create_pipeline():
        """Create a new pipeline. Requires authentication."""
        from scheduler.config import SchedulerConfig
        import yaml

        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400

        name = data.get('name')
        if not name:
            return jsonify({'error': 'Pipeline name is required'}), 400

        # Validate name format
        if not name.replace('_', '').replace('-', '').isalnum():
            return jsonify({'error': 'Pipeline name must be alphanumeric with underscores/hyphens'}), 400

        config = SchedulerConfig.from_yaml()
        if name in config.pipelines:
            return jsonify({'error': f'Pipeline {name} already exists'}), 400

        # Build pipeline config
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

        # Load and update pipelines.yaml
        config_path = Path(__file__).parent.parent / 'config' / 'pipelines.yaml'
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

    @app.route('/api/pipelines/<name>', methods=['PUT'])
    @require_auth
    def api_update_pipeline(name):
        """Update a pipeline configuration. Requires authentication."""
        from scheduler.config import SchedulerConfig
        import yaml

        config = SchedulerConfig.from_yaml()
        if name not in config.pipelines:
            return jsonify({'error': 'Pipeline not found'}), 404

        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400

        # Load current pipelines.yaml
        config_path = Path(__file__).parent.parent / 'config' / 'pipelines.yaml'
        with open(config_path) as f:
            yaml_data = yaml.safe_load(f)

        current = yaml_data['pipelines'][name]

        # Update fields that were provided
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

        # Save updated config
        with open(config_path, 'w') as f:
            yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False)

        return jsonify({
            'success': True,
            'message': f'Pipeline {name} updated',
        })

    @app.route('/api/pipelines/<name>', methods=['DELETE'])
    @require_auth
    def api_delete_pipeline(name):
        """Delete a pipeline. Requires authentication."""
        from scheduler.config import SchedulerConfig
        import yaml

        config = SchedulerConfig.from_yaml()
        if name not in config.pipelines:
            return jsonify({'error': 'Pipeline not found'}), 404

        # Load and update pipelines.yaml
        config_path = Path(__file__).parent.parent / 'config' / 'pipelines.yaml'
        with open(config_path) as f:
            yaml_data = yaml.safe_load(f)

        del yaml_data['pipelines'][name]

        with open(config_path, 'w') as f:
            yaml.dump(yaml_data, f, default_flow_style=False, sort_keys=False)

        return jsonify({
            'success': True,
            'message': f'Pipeline {name} deleted',
        })

    @app.route('/api/modules')
    def api_list_modules():
        """List available Python modules in datalayer."""
        datalayer_path = Path(__file__).parent.parent / 'datalayer'
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

    return app


def run_app(host='0.0.0.0', port=5000, debug=False, db_url=None):
    """Run the Flask application."""
    # Load environment variables first
    from dotenv import load_dotenv
    load_dotenv()

    from scheduler.config import SchedulerConfig

    config = SchedulerConfig.from_yaml()

    if not db_url:
        import os
        # Support both naming conventions for flexibility
        host_db = os.getenv('DB_HOST') or os.getenv('POSTGRESQL_HOST')
        port_db = os.getenv('DB_PORT') or os.getenv('POSTGRESQL_PORT', '5432')
        database = os.getenv('DB_NAME') or os.getenv('POSTGRESQL_DATABASE')
        username = os.getenv('DB_USER') or os.getenv('DB_USERNAME') or os.getenv('POSTGRESQL_USERNAME')
        password = os.getenv('DB_PASSWORD') or os.getenv('POSTGRESQL_PASSWORD')
        sslmode = os.getenv('DB_SSLMODE', 'require')

        db_url = f"postgresql://{username}:{password}@{host_db}:{port_db}/{database}?sslmode={sslmode}"

    app = create_app(config, db_url)

    # Get host/port from environment if not specified
    host = os.getenv('FLASK_HOST', host)
    port = int(os.getenv('FLASK_PORT', port))
    debug = os.getenv('FLASK_DEBUG', str(debug)).lower() == 'true'

    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    run_app(debug=True)
