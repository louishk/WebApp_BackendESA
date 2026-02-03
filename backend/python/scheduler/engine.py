"""
APScheduler Engine - Core scheduler setup and management.
Handles job scheduling, persistence, and lifecycle management.
"""

import logging
import threading
import signal
import socket
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Callable, List
from uuid import uuid4

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.events import (
    EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED,
    EVENT_JOB_SUBMITTED, EVENT_SCHEDULER_STARTED, EVENT_SCHEDULER_SHUTDOWN
)

from scheduler.config import SchedulerConfig, PipelineDefinition
from scheduler.models import JobHistory, SchedulerState, create_scheduler_tables
from scheduler.resource_manager import ResourceManager, get_resource_manager
from scheduler.conflict_resolver import ConflictResolver, JobContext, JobStatus
from scheduler.executor import PipelineExecutor, ExecutionResult

logger = logging.getLogger(__name__)


class SchedulerEngine:
    """
    Main scheduler engine that orchestrates job execution.

    Responsibilities:
    - Initialize and manage APScheduler
    - Register pipeline jobs from configuration
    - Handle job execution with conflict resolution
    - Track execution history in database
    - Manage graceful shutdown
    """

    def __init__(
        self,
        config: SchedulerConfig,
        db_url: str,
        alert_manager: Optional['AlertManager'] = None
    ):
        """
        Initialize scheduler engine.

        Args:
            config: Scheduler configuration
            db_url: PostgreSQL connection URL
            alert_manager: Optional alert manager for notifications
        """
        self.config = config
        self.db_url = db_url
        self.alert_manager = alert_manager

        # Initialize components
        self.resource_manager = get_resource_manager({
            'db_pool': config.resources.db_pool,
            'soap_api': config.resources.soap_api,
            'http_api': config.resources.http_api,
        })
        self.conflict_resolver = ConflictResolver({
            'db_connections': config.resources.db_pool
        })
        self.executor = PipelineExecutor(config.daemon.working_directory)

        # APScheduler instance
        self._scheduler: Optional[BackgroundScheduler] = None
        self._db_engine = None
        self._session_factory = None

        # State tracking
        self._running = False
        self._shutdown_event = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None

        # Pending manual jobs queue
        self._pending_jobs: List[JobContext] = []
        self._pending_lock = threading.Lock()

        # Config file monitoring
        self._config_hash: Optional[str] = None
        self._config_monitor_thread: Optional[threading.Thread] = None

    def initialize(self):
        """Initialize database and APScheduler."""
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        logger.info("Initializing scheduler engine...")

        # Create database engine
        self._db_engine = create_engine(self.db_url)

        # Create scheduler tables
        create_scheduler_tables(self._db_engine)

        # Create session factory
        self._session_factory = sessionmaker(bind=self._db_engine)

        # Configure APScheduler
        # Use MemoryJobStore to avoid pickle issues with bound methods
        # Jobs are re-registered from config on each startup anyway
        jobstores = {
            'default': MemoryJobStore()
        }

        executors = {
            'default': ThreadPoolExecutor(max_workers=self.config.executor_max_workers)
        }

        job_defaults = {
            'coalesce': self.config.coalesce,
            'max_instances': self.config.max_instances,
            'misfire_grace_time': self.config.misfire_grace_time,
        }

        self._scheduler = BackgroundScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone=self.config.timezone
        )

        # Add event listeners
        self._scheduler.add_listener(self._on_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED)
        self._scheduler.add_listener(self._on_scheduler_event, EVENT_SCHEDULER_STARTED | EVENT_SCHEDULER_SHUTDOWN)

        logger.info("Scheduler engine initialized")

    def start(self):
        """Start the scheduler."""
        from scheduler import __version__

        if self._running:
            logger.warning("Scheduler is already running")
            return

        logger.info(f"Starting PBI Scheduler v{__version__}...")

        # Initialize if needed
        if not self._scheduler:
            self.initialize()

        # Register all enabled pipelines
        self._register_pipelines()

        # Update scheduler state in database
        self._update_state('running')

        # Start APScheduler
        self._scheduler.start()
        self._running = True

        # Start heartbeat thread
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

        # Start config monitor thread
        self._config_monitor_thread = threading.Thread(
            target=self._config_monitor_loop,
            daemon=True,
            name='ConfigMonitor'
        )
        self._config_monitor_thread.start()
        logger.info("Config file monitor started (checking every 10s)")

        logger.info("Scheduler started successfully")

    def stop(self, wait: bool = True):
        """
        Stop the scheduler.

        Args:
            wait: Whether to wait for running jobs to complete
        """
        if not self._running:
            logger.warning("Scheduler is not running")
            return

        logger.info("Stopping scheduler...")
        self._shutdown_event.set()

        # Update state
        self._update_state('stopping')

        # Stop APScheduler
        if self._scheduler:
            self._scheduler.shutdown(wait=wait)

        # Cancel any running executions
        self.executor.cancel_all()

        # Clear conflict resolver
        self.conflict_resolver.clear()

        self._running = False
        self._update_state('stopped')

        logger.info("Scheduler stopped")

    def pause(self):
        """Pause the scheduler (keeps jobs scheduled but doesn't run them)."""
        if self._scheduler:
            self._scheduler.pause()
            self._update_state('paused')
            logger.info("Scheduler paused")

    def resume(self):
        """Resume a paused scheduler."""
        if self._scheduler:
            self._scheduler.resume()
            self._update_state('running')
            logger.info("Scheduler resumed")

    def _register_pipelines(self):
        """Register all enabled pipelines with APScheduler."""
        for name, pipeline in self.config.pipelines.items():
            if not pipeline.enabled:
                logger.debug(f"Skipping disabled pipeline: {name}")
                continue

            try:
                trigger = self._create_trigger(pipeline)
                if trigger:
                    self._scheduler.add_job(
                        func=self._execute_pipeline,
                        trigger=trigger,
                        id=f"pipeline_{name}",
                        name=pipeline.display_name,
                        kwargs={'pipeline_name': name},
                        replace_existing=True
                    )
                    logger.info(f"Registered pipeline: {name} (priority={pipeline.priority}, {pipeline.schedule_config})")

            except Exception as e:
                logger.error(f"Failed to register pipeline {name}: {e}")

    def reload_pipelines(self):
        """Reload pipeline config from disk and re-register jobs."""
        from scheduler.config import SchedulerConfig

        logger.info("Reloading pipeline configuration...")

        # Load fresh config from YAML
        new_config = SchedulerConfig.from_yaml()

        # Remove all existing pipeline jobs from APScheduler
        for job in self._scheduler.get_jobs():
            if job.id.startswith('pipeline_'):
                self._scheduler.remove_job(job.id)
                logger.debug(f"Removed job: {job.id}")

        # Update config and re-register all pipelines
        self.config = new_config
        self._register_pipelines()

        logger.info(f"Reloaded {len(new_config.get_enabled_pipelines())} pipelines")

    def _config_monitor_loop(self):
        """Monitor pipelines.yaml for changes and auto-reload."""
        import hashlib
        from scheduler.config import BASE_DIR

        config_path = BASE_DIR / 'config' / 'pipelines.yaml'

        # Get initial hash
        try:
            self._config_hash = hashlib.md5(config_path.read_bytes()).hexdigest()
        except Exception:
            self._config_hash = None

        while not self._shutdown_event.wait(timeout=10):  # Check every 10 seconds
            try:
                current_hash = hashlib.md5(config_path.read_bytes()).hexdigest()

                if current_hash != self._config_hash:
                    logger.info("Config file changed, reloading pipelines...")
                    self.reload_pipelines()
                    self._config_hash = current_hash
            except Exception as e:
                logger.error(f"Config monitor error: {e}")

    def _create_trigger(self, pipeline: PipelineDefinition):
        """Create APScheduler trigger from pipeline config."""
        schedule = pipeline.schedule_config

        if pipeline.schedule_type == 'cron':
            cron_expr = schedule.get('cron', '0 6 * * *')  # Default 6 AM daily
            parts = cron_expr.split()

            if len(parts) >= 5:
                return CronTrigger(
                    minute=parts[0],
                    hour=parts[1],
                    day=parts[2],
                    month=parts[3],
                    day_of_week=parts[4],
                    timezone=self.config.timezone
                )

        elif pipeline.schedule_type == 'interval':
            return IntervalTrigger(
                seconds=schedule.get('seconds', 0),
                minutes=schedule.get('minutes', 0),
                hours=schedule.get('hours', 1),
            )

        elif pipeline.schedule_type == 'date':
            run_date = schedule.get('run_date')
            if run_date:
                return DateTrigger(run_date=run_date)

        logger.warning(f"Unknown schedule type for {pipeline.pipeline_name}: {pipeline.schedule_type}")
        return None

    def _execute_pipeline(self, pipeline_name: str):
        """
        Execute a pipeline job.
        This is the main entry point called by APScheduler.
        """
        pipeline = self.config.get_pipeline(pipeline_name)
        if not pipeline:
            logger.error(f"Pipeline not found: {pipeline_name}")
            return

        execution_id = uuid4()
        logger.info(f"[{execution_id}] Starting pipeline: {pipeline_name}")

        # Create job context
        job_context = JobContext(
            pipeline_name=pipeline_name,
            execution_id=execution_id,
            priority=pipeline.priority,
            scheduled_at=datetime.now(timezone.utc),
            config=pipeline,
            status=JobStatus.PENDING
        )

        # Check conflicts
        check = self.conflict_resolver.can_start(pipeline_name, pipeline)
        if not check.can_start:
            logger.warning(f"[{execution_id}] Pipeline blocked: {check.reason}")
            # Queue for later
            with self._pending_lock:
                self._pending_jobs.append(job_context)
            return

        # Create history record - returns execution_id string
        history_exec_id = self._create_history_record(job_context, 'scheduler')

        try:
            # Register as running
            self.conflict_resolver.register_start(job_context)

            # Acquire resources and execute
            with self.resource_manager.acquire(
                pipeline.resource_group,
                count=1,
                timeout=300,
                job_id=str(execution_id)
            ):
                with self.resource_manager.acquire(
                    'db_pool',
                    count=pipeline.max_db_connections,
                    timeout=300,
                    job_id=str(execution_id)
                ):
                    # Execute pipeline
                    result = self.executor.execute(
                        module_path=pipeline.module_path,
                        args=pipeline.default_args,
                        execution_id=execution_id,
                        timeout_seconds=pipeline.timeout_seconds
                    )

            # Update history
            self._update_history_result(history_exec_id, result)

            # Handle result
            if result.success:
                self.conflict_resolver.register_complete(pipeline_name, JobStatus.COMPLETED)
                logger.info(f"[{execution_id}] Pipeline completed: {result.records_processed} records")
            else:
                self.conflict_resolver.register_complete(pipeline_name, JobStatus.FAILED)
                self._handle_failure(result, job_context)

        except TimeoutError as e:
            logger.error(f"[{execution_id}] Resource timeout: {e}")
            self._update_history_error(history_exec_id, str(e))
            self.conflict_resolver.register_complete(pipeline_name, JobStatus.FAILED)

        except Exception as e:
            logger.exception(f"[{execution_id}] Pipeline execution error: {e}")
            self._update_history_error(history_exec_id, str(e))
            self.conflict_resolver.register_complete(pipeline_name, JobStatus.FAILED)

        finally:
            # Try to run any pending jobs
            self._process_pending_jobs()

    def run_pipeline_now(
        self,
        pipeline_name: str,
        args: Optional[Dict[str, Any]] = None,
        triggered_by: str = 'cli'
    ) -> str:
        """
        Trigger immediate execution of a pipeline.

        Args:
            pipeline_name: Pipeline to run
            args: Optional override arguments
            triggered_by: Who triggered this run

        Returns:
            Execution ID as string
        """
        pipeline = self.config.get_pipeline(pipeline_name)
        if not pipeline:
            raise ValueError(f"Pipeline not found: {pipeline_name}")

        execution_id = uuid4()

        # Merge args with defaults
        final_args = dict(pipeline.default_args)
        if args:
            final_args.update(args)

        # Create modified pipeline config with custom args
        from dataclasses import replace
        modified_pipeline = replace(pipeline, default_args=final_args)

        # Create job context
        job_context = JobContext(
            pipeline_name=pipeline_name,
            execution_id=execution_id,
            priority=pipeline.priority,
            scheduled_at=datetime.now(timezone.utc),
            config=modified_pipeline,
            status=JobStatus.QUEUED
        )

        # Check if can run immediately
        check = self.conflict_resolver.can_start(pipeline_name, pipeline)
        if check.can_start:
            # Run in thread pool
            self._scheduler.add_job(
                func=self._execute_pipeline,
                trigger='date',
                run_date=datetime.now(),
                id=f"manual_{pipeline_name}_{execution_id}",
                kwargs={'pipeline_name': pipeline_name},
                replace_existing=False
            )
        else:
            # Queue for later
            logger.info(f"[{execution_id}] Queuing pipeline: {check.reason}")
            with self._pending_lock:
                self._pending_jobs.append(job_context)

        return str(execution_id)

    def _process_pending_jobs(self):
        """Try to run any queued pending jobs."""
        with self._pending_lock:
            if not self._pending_jobs:
                return

            # Find next runnable job
            next_job = self.conflict_resolver.get_next_runnable(self._pending_jobs)

            if next_job:
                self._pending_jobs.remove(next_job)
                # Schedule for immediate execution
                self._scheduler.add_job(
                    func=self._execute_pipeline,
                    trigger='date',
                    run_date=datetime.now(),
                    id=f"queued_{next_job.pipeline_name}_{next_job.execution_id}",
                    kwargs={'pipeline_name': next_job.pipeline_name},
                    replace_existing=False
                )

    def _create_history_record(self, job_context: JobContext, triggered_by: str) -> str:
        """Create a new job history record. Returns execution_id."""
        session = self._session_factory()
        try:
            history = JobHistory(
                job_id=f"pipeline_{job_context.pipeline_name}",
                pipeline_name=job_context.pipeline_name,
                execution_id=job_context.execution_id,
                status='running',
                priority=job_context.priority,
                scheduled_at=job_context.scheduled_at,
                started_at=datetime.now(timezone.utc),
                mode=job_context.config.default_args.get('mode', 'auto'),
                parameters=job_context.config.default_args,
                attempt_number=job_context.attempt_number,
                max_retries=job_context.config.retry.max_attempts,
                triggered_by=triggered_by,
                host_name=socket.gethostname()
            )
            session.add(history)
            session.commit()
            # Return execution_id instead of detached object
            return str(job_context.execution_id)
        finally:
            session.close()

    def _update_history_result(self, execution_id: str, result: ExecutionResult):
        """Update history record with execution result."""
        session = self._session_factory()
        try:
            record = session.query(JobHistory).filter_by(
                execution_id=execution_id
            ).first()

            if record:
                record.status = 'completed' if result.success else 'failed'
                record.completed_at = datetime.now(timezone.utc)
                record.duration_seconds = result.duration_seconds
                record.records_processed = result.records_processed
                if result.error_message:
                    record.error_message = result.error_message
                    record.error_traceback = result.stderr[:10000] if result.stderr else None
                session.commit()
        finally:
            session.close()

    def _update_history_error(self, execution_id: str, error: str):
        """Update history record with error."""
        session = self._session_factory()
        try:
            record = session.query(JobHistory).filter_by(
                execution_id=execution_id
            ).first()

            if record:
                record.status = 'failed'
                record.completed_at = datetime.now(timezone.utc)
                record.error_message = error
                session.commit()
        finally:
            session.close()

    def _handle_failure(
        self,
        result: ExecutionResult,
        job_context: JobContext
    ):
        """Handle pipeline failure - retry logic and alerts."""
        # Send alert
        if self.alert_manager:
            self.alert_manager.send_failure_alert(
                pipeline_name=job_context.pipeline_name,
                execution_id=job_context.execution_id,
                error_message=result.error_message,
                attempt=job_context.attempt_number,
                max_retries=job_context.config.retry.max_attempts
            )

        # Check if should retry
        if job_context.attempt_number < job_context.config.retry.max_attempts:
            retry_delay = job_context.config.retry.delay_seconds * (
                job_context.config.retry.backoff_multiplier ** (job_context.attempt_number - 1)
            )
            logger.info(
                f"[{job_context.execution_id}] Scheduling retry {job_context.attempt_number + 1} "
                f"in {retry_delay}s"
            )

            # Schedule retry
            from datetime import timedelta
            retry_time = datetime.now() + timedelta(seconds=retry_delay)

            # Create new context with incremented attempt
            job_context.attempt_number += 1
            job_context.scheduled_at = retry_time

            self._scheduler.add_job(
                func=self._execute_pipeline,
                trigger='date',
                run_date=retry_time,
                id=f"retry_{job_context.pipeline_name}_{job_context.execution_id}",
                kwargs={'pipeline_name': job_context.pipeline_name},
                replace_existing=False
            )

    def _update_state(self, status: str):
        """Update scheduler state in database."""
        from scheduler import __version__

        if not self._session_factory:
            return

        session = self._session_factory()
        try:
            state = session.query(SchedulerState).filter_by(id=1).first()

            if not state:
                state = SchedulerState(
                    id=1,
                    status=status,
                    started_at=datetime.now() if status == 'running' else None,
                    host_name=socket.gethostname(),
                    pid=os.getpid(),
                    version=__version__
                )
                session.add(state)
            else:
                state.status = status
                if status == 'running':
                    state.started_at = datetime.now()
                state.host_name = socket.gethostname()
                state.pid = os.getpid()
                state.version = __version__
                state.last_heartbeat = datetime.now()

            session.commit()
        except Exception as e:
            logger.error(f"Failed to update scheduler state: {e}")
            session.rollback()
        finally:
            session.close()

    def _heartbeat_loop(self):
        """Background thread for heartbeat updates."""
        while not self._shutdown_event.wait(timeout=self.config.heartbeat_interval_seconds):
            self._update_heartbeat()

    def _update_heartbeat(self):
        """Update heartbeat timestamp."""
        if not self._session_factory:
            return

        session = self._session_factory()
        try:
            state = session.query(SchedulerState).filter_by(id=1).first()
            if state:
                state.last_heartbeat = datetime.now()
                session.commit()
        except Exception as e:
            logger.error(f"Failed to update heartbeat: {e}")
            session.rollback()
        finally:
            session.close()

    def _on_job_event(self, event):
        """Handle APScheduler job events."""
        if event.code == EVENT_JOB_ERROR:
            logger.error(f"Job {event.job_id} error: {event.exception}")
        elif event.code == EVENT_JOB_MISSED:
            logger.warning(f"Job {event.job_id} missed its scheduled run time")
        elif event.code == EVENT_JOB_EXECUTED:
            logger.debug(f"Job {event.job_id} executed successfully")

    def _on_scheduler_event(self, event):
        """Handle APScheduler lifecycle events."""
        if event.code == EVENT_SCHEDULER_STARTED:
            logger.info("APScheduler started")
        elif event.code == EVENT_SCHEDULER_SHUTDOWN:
            logger.info("APScheduler shutdown")

    def get_jobs(self) -> List[Dict[str, Any]]:
        """Get list of scheduled jobs."""
        if not self._scheduler:
            return []

        jobs = []
        for job in self._scheduler.get_jobs():
            jobs.append({
                'id': job.id,
                'name': job.name,
                'next_run': job.next_run_time.isoformat() if job.next_run_time else None,
                'trigger': str(job.trigger),
            })

        return jobs

    def get_status(self) -> Dict[str, Any]:
        """Get scheduler status."""
        return {
            'running': self._running,
            'jobs_scheduled': len(self._scheduler.get_jobs()) if self._scheduler else 0,
            'jobs_running': self.conflict_resolver.get_running_count(),
            'pending_jobs': len(self._pending_jobs),
            'resources': self.resource_manager.get_all_usage_dict(),
        }

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._running
