"""
Orchestrator daemon — runs as standalone systemd service `backend-orchestrator`.

Reads enabled pipelines from `mw_sync_pipelines` and registers them with
APScheduler using schedule_type/schedule_config:

  schedule_type='cron',     schedule_config={"cron": "0 5 * * 0"}
  schedule_type='interval', schedule_config={"interval_seconds": 3600}
  schedule_type='on_demand' → skipped (only runs via API)

On each fire, delegates to sync_service.executor.run(triggered_by='schedule').
The executor handles dedup + records a row in mw_sync_runs.

Also writes a heartbeat + status row to mw_sync_service_state every 30s.
"""

import logging
import os
import signal
import socket
import sys
from datetime import datetime, timezone

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from sync_service.config import session_scope
from sync_service.models import SyncPipeline, SyncServiceState
from sync_service.executor import get_executor
from sync_service import __version__ as _version

logger = logging.getLogger(__name__)


class OrchestratorDaemon:
    """Cron/interval scheduler for sync_service pipelines."""

    def __init__(self, tz: str = 'Asia/Singapore'):
        self.tz = tz
        self.scheduler = BlockingScheduler(timezone=tz)
        self.executor = get_executor()
        self._hostname = socket.gethostname()
        self._pid = os.getpid()

    # --------------------------------------------------------------
    # Job registration
    # --------------------------------------------------------------

    def load_jobs(self) -> int:
        """Register all enabled pipelines as APScheduler jobs. Returns count registered."""
        registered = 0
        with session_scope() as session:
            pipelines = (
                session.query(SyncPipeline)
                .filter(SyncPipeline.enabled.is_(True))
                .all()
            )
            for p in pipelines:
                if self._register(p):
                    registered += 1
        logger.info(f"Loaded {registered} scheduled pipelines")
        return registered

    def _register(self, p: SyncPipeline) -> bool:
        """Register one pipeline. Returns True if scheduled, False if skipped."""
        stype = (p.schedule_type or 'on_demand').lower()
        cfg = p.schedule_config or {}
        trigger = None

        if stype == 'cron':
            cron_expr = cfg.get('cron') or cfg.get('expression')
            if not cron_expr:
                logger.info(f"{p.pipeline_name}: cron schedule missing expression — skipped")
                return False
            try:
                trigger = CronTrigger.from_crontab(cron_expr, timezone=self.tz)
            except Exception as e:
                logger.warning(f"{p.pipeline_name}: invalid cron '{cron_expr}': {e}")
                return False
        elif stype == 'interval':
            secs = int(cfg.get('interval_seconds') or 0)
            if secs <= 0:
                logger.info(f"{p.pipeline_name}: interval missing or invalid — skipped")
                return False
            trigger = IntervalTrigger(seconds=secs)
        elif stype in ('on_demand', 'manual', ''):
            logger.debug(f"{p.pipeline_name}: on_demand — no schedule")
            return False
        else:
            logger.warning(f"{p.pipeline_name}: unknown schedule_type '{stype}' — skipped")
            return False

        self.scheduler.add_job(
            self._fire,
            trigger=trigger,
            id=f'pipeline_{p.pipeline_name}',
            name=p.display_name or p.pipeline_name,
            args=[p.pipeline_name],
            replace_existing=True,
            misfire_grace_time=300,
            coalesce=True,
            max_instances=1,
        )
        logger.info(f"Registered {p.pipeline_name} ({stype}={cfg})")
        return True

    def _fire(self, pipeline_name: str):
        """Execute a single pipeline run via the shared executor."""
        logger.info(f"Firing scheduled run: {pipeline_name}")
        try:
            # Honor per-pipeline timeout_seconds (mw_sync_pipelines); fall back to 30 min.
            with session_scope() as s:
                row = (
                    s.query(SyncPipeline.timeout_seconds)
                    .filter(SyncPipeline.pipeline_name == pipeline_name)
                    .first()
                )
            timeout = float(row[0]) if row and row[0] else 1800.0
            result = self.executor.run(
                pipeline_name=pipeline_name,
                triggered_by='schedule',
                triggered_by_detail=f'daemon@{self._hostname}',
                timeout=timeout,
            )
            logger.info(
                f"{pipeline_name} → status={result.status} records={result.records} "
                f"duration_ms={result.duration_ms} err={result.error}"
            )
        except Exception:
            logger.exception(f"Unhandled error while firing {pipeline_name}")

    # --------------------------------------------------------------
    # Health
    # --------------------------------------------------------------

    def _heartbeat(self):
        """Upsert mw_sync_service_state singleton with liveness info."""
        try:
            now = datetime.now(timezone.utc)
            with session_scope() as session:
                state = session.query(SyncServiceState).filter_by(id=1).first()
                if state is None:
                    state = SyncServiceState(
                        id=1,
                        status='running',
                        started_at=now,
                        host_name=self._hostname,
                        pid=self._pid,
                        last_heartbeat=now,
                        version=_version,
                    )
                    session.add(state)
                else:
                    state.status = 'running'
                    state.host_name = self._hostname
                    state.pid = self._pid
                    state.last_heartbeat = now
                    state.version = _version
        except Exception:
            logger.exception("Heartbeat write failed")

    def _mark_stopped(self):
        try:
            with session_scope() as session:
                state = session.query(SyncServiceState).filter_by(id=1).first()
                if state:
                    state.status = 'stopped'
                    state.last_heartbeat = datetime.now(timezone.utc)
        except Exception:
            logger.exception("Stop mark failed")

    # --------------------------------------------------------------
    # Lifecycle
    # --------------------------------------------------------------

    def run(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        )
        logger.info(f"Orchestrator daemon starting pid={self._pid} host={self._hostname}")

        # Mark running ASAP
        self._heartbeat()

        # Register pipeline jobs
        self.load_jobs()

        # Heartbeat every 30s
        self.scheduler.add_job(
            self._heartbeat,
            trigger=IntervalTrigger(seconds=30),
            id='_heartbeat',
            name='_heartbeat',
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )

        # Reload pipeline jobs every 5 min (picks up enable/disable/schedule changes)
        self.scheduler.add_job(
            self._reload,
            trigger=IntervalTrigger(minutes=5),
            id='_reload_jobs',
            name='_reload_jobs',
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )

        # Graceful shutdown
        def _stop(*_):
            logger.info("Received shutdown signal — stopping scheduler")
            try:
                self.scheduler.shutdown(wait=False)
            finally:
                self._mark_stopped()

        signal.signal(signal.SIGTERM, _stop)
        signal.signal(signal.SIGINT, _stop)

        try:
            self.scheduler.start()
        finally:
            self._mark_stopped()

    def _reload(self):
        """Rebuild job list from DB — picks up enable/disable/schedule changes."""
        current_ids = {
            j.id for j in self.scheduler.get_jobs()
            if j.id.startswith('pipeline_')
        }
        desired_ids = set()
        with session_scope() as session:
            pipelines = (
                session.query(SyncPipeline)
                .filter(SyncPipeline.enabled.is_(True))
                .all()
            )
            for p in pipelines:
                job_id = f'pipeline_{p.pipeline_name}'
                if self._register(p):
                    desired_ids.add(job_id)

        for stale in current_ids - desired_ids:
            try:
                self.scheduler.remove_job(stale)
                logger.info(f"Removed stale job {stale}")
            except Exception:
                pass


def main():
    OrchestratorDaemon().run()


if __name__ == '__main__':
    main()
