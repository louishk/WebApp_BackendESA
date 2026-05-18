"""
OnDemandExecutor — the core of sync_service.

Responsibilities:
  - Run pipelines on demand with caller-supplied scope
  - Deduplicate concurrent requests for the same (pipeline, scope)
  - Respect per-pipeline concurrency + global resource pool limits
  - Record every run in sync_runs table
  - Handle timeouts gracefully (return RunResult rather than hang)
"""

import hashlib
import json
import logging
import socket
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FuturesTimeout
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from uuid import uuid4

from sync_service.config import session_scope
from sync_service.freshness import check_freshness, is_stale
from sync_service.models import SyncPipeline, SyncRun
from sync_service.pipelines.base import BasePipeline, RunResult
from sync_service.registry import get_pipeline, instantiate_pipeline
from sync_service.resource_pool import ResourcePool, get_pool

logger = logging.getLogger(__name__)


def _hash_scope(scope: Dict[str, Any]) -> str:
    """Stable, short hash of a scope dict for dedup keying."""
    if not scope:
        return '__empty__'
    canonical = json.dumps(scope, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()[:32]


class OnDemandExecutor:
    """Thread-pool executor with in-flight deduplication.

    Single instance per process. Initialized by main.py or the Flask app factory
    (for API-driven runs).
    """

    def __init__(
        self,
        max_workers: int = 10,
        pool: Optional[ResourcePool] = None,
    ):
        self._pool = pool or get_pool()
        self._thread_pool = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix='sync-exec',
        )
        self._in_flight: Dict[Tuple[str, str], Future] = {}
        self._in_flight_lock = threading.Lock()
        self._registered_pipelines: set = set()
        self._shutdown = False

    def _ensure_registered(self, pipeline_row: SyncPipeline):
        """Register per-pipeline semaphore on first use."""
        if pipeline_row.pipeline_name not in self._registered_pipelines:
            self._pool.register_pipeline(
                pipeline_row.pipeline_name,
                pipeline_row.max_concurrency,
            )
            self._registered_pipelines.add(pipeline_row.pipeline_name)

    def ensure_fresh(
        self,
        pipeline_name: str,
        scope: Optional[Dict[str, Any]] = None,
        max_age_seconds: Optional[int] = None,
        timeout: float = 60.0,
        triggered_by: str = 'api',
        triggered_by_detail: Optional[str] = None,
    ) -> RunResult:
        """Ensure target data is fresh. Run the pipeline only if stale.

        This is the primary entry point for middleware callers.

        Args:
            pipeline_name: Pipeline to refresh
            scope: Scope dict (site_codes, date range, etc)
            max_age_seconds: Override the pipeline's default TTL
            timeout: Max seconds for the whole call (including wait + execution)
            triggered_by: 'api' | 'middleware' | 'schedule' | 'cli'
            triggered_by_detail: Optional caller identity

        Returns:
            RunResult with status='fresh' (skipped), 'refreshed' (ran), or
            'failed' (error).
        """
        pipeline_row = get_pipeline(pipeline_name)
        if pipeline_row is None:
            return RunResult(status='failed', scope=scope or {}, error='Pipeline not found')
        if not pipeline_row.enabled:
            return RunResult(status='failed', scope=scope or {}, error='Pipeline disabled')

        # Quick freshness check BEFORE acquiring any slots
        effective_scope = dict(pipeline_row.default_args or {})
        if scope:
            effective_scope.update(scope)

        try:
            age = check_freshness(pipeline_row, effective_scope)
        except Exception as e:
            logger.warning(f"ensure_fresh: freshness check failed for {pipeline_name}: {e}")
            age = None

        ttl = max_age_seconds if max_age_seconds is not None else pipeline_row.freshness_ttl_seconds

        if age is not None and age < ttl:
            # Already fresh — record a skip run and return
            self._record_run(
                pipeline_name=pipeline_name,
                scope=effective_scope,
                triggered_by=triggered_by,
                triggered_by_detail=triggered_by_detail,
                result=RunResult(
                    status='fresh', records=0, scope=effective_scope,
                    metadata={'freshness_age_seconds': age, 'ttl_used': ttl},
                ),
                was_fresh=True,
                was_deduplicated=False,
                freshness_age_seconds=int(age),
            )
            return RunResult(
                status='fresh', records=0, scope=effective_scope,
                metadata={'freshness_age_seconds': age, 'ttl_used': ttl},
            )

        # Need to run — go through the dedup + execution path
        return self._run_with_dedup(
            pipeline_row=pipeline_row,
            scope=effective_scope,
            max_age_seconds=max_age_seconds,
            timeout=timeout,
            triggered_by=triggered_by,
            triggered_by_detail=triggered_by_detail,
            freshness_age_seconds=int(age) if age is not None else None,
            skip_freshness_check=True,  # already checked above
        )

    def run(
        self,
        pipeline_name: str,
        scope: Optional[Dict[str, Any]] = None,
        timeout: float = 300.0,
        triggered_by: str = 'api',
        triggered_by_detail: Optional[str] = None,
    ) -> RunResult:
        """Force-run a pipeline regardless of freshness."""
        pipeline_row = get_pipeline(pipeline_name)
        if pipeline_row is None:
            return RunResult(status='failed', scope=scope or {}, error='Pipeline not found')
        if not pipeline_row.enabled:
            return RunResult(status='failed', scope=scope or {}, error='Pipeline disabled')

        effective_scope = dict(pipeline_row.default_args or {})
        if scope:
            effective_scope.update(scope)

        return self._run_with_dedup(
            pipeline_row=pipeline_row,
            scope=effective_scope,
            max_age_seconds=None,
            timeout=timeout,
            triggered_by=triggered_by,
            triggered_by_detail=triggered_by_detail,
            freshness_age_seconds=None,
            skip_freshness_check=True,
        )

    def _run_with_dedup(
        self,
        pipeline_row: SyncPipeline,
        scope: Dict[str, Any],
        max_age_seconds: Optional[int],
        timeout: float,
        triggered_by: str,
        triggered_by_detail: Optional[str],
        freshness_age_seconds: Optional[int],
        skip_freshness_check: bool,
    ) -> RunResult:
        """Run with in-flight dedup. Multiple concurrent calls for the same key
        attach to the same Future and share the result."""
        self._ensure_registered(pipeline_row)

        scope_hash = _hash_scope(scope)
        key = (pipeline_row.pipeline_name, scope_hash)

        was_deduplicated = False
        with self._in_flight_lock:
            future = self._in_flight.get(key)
            if future is not None and not future.done():
                was_deduplicated = True
                logger.info(
                    f"Deduping {pipeline_row.pipeline_name} scope_hash={scope_hash} — "
                    f"attaching to in-flight run"
                )
            else:
                future = self._thread_pool.submit(
                    self._execute_pipeline,
                    pipeline_row=pipeline_row,
                    scope=scope,
                    max_age_seconds=max_age_seconds,
                    skip_freshness_check=skip_freshness_check,
                )
                self._in_flight[key] = future

        # Insert a 'running' row up front so the dashboard / status filter can see
        # the run in flight. Dedup'd callers do NOT get their own running row — the
        # original submitter owns the row; dedup'd callers record their own
        # was_deduplicated=True row at completion.
        started_at = datetime.now(timezone.utc)
        execution_id = None
        if not was_deduplicated:
            execution_id = self._record_run_start(
                pipeline_name=pipeline_row.pipeline_name,
                scope=scope,
                scope_hash=scope_hash,
                triggered_by=triggered_by,
                triggered_by_detail=triggered_by_detail,
                started_at=started_at,
            )

        try:
            result = future.result(timeout=timeout)
        except FuturesTimeout:
            result = RunResult(
                status='failed',
                scope=scope,
                error=f'Execution exceeded timeout of {timeout}s',
            )
        except Exception as e:
            result = RunResult(status='failed', scope=scope, error=str(e)[:500])
        finally:
            with self._in_flight_lock:
                # Only clean up if we're the original submitter AND the future is done
                if not was_deduplicated and self._in_flight.get(key) is future and future.done():
                    del self._in_flight[key]

        # Record the run — every caller records their own perspective
        if execution_id is not None:
            self._record_run_finish(
                execution_id=execution_id,
                result=result,
                was_fresh=(result.status == 'fresh'),
                freshness_age_seconds=freshness_age_seconds,
                started_at=started_at,
            )
        else:
            # Dedup'd caller (needs its own dedup-flagged row), OR
            # _record_run_start failed (fallback: still record the outcome).
            self._record_run(
                pipeline_name=pipeline_row.pipeline_name,
                scope=scope,
                triggered_by=triggered_by,
                triggered_by_detail=triggered_by_detail,
                result=result,
                was_fresh=(result.status == 'fresh'),
                was_deduplicated=was_deduplicated,
                freshness_age_seconds=freshness_age_seconds,
            )
        return result

    def _execute_pipeline(
        self,
        pipeline_row: SyncPipeline,
        scope: Dict[str, Any],
        max_age_seconds: Optional[int],
        skip_freshness_check: bool,
    ) -> RunResult:
        """Build pipeline instance, acquire resources, execute."""
        try:
            pipeline: BasePipeline = instantiate_pipeline(pipeline_row)
        except Exception as e:
            logger.exception(f"Failed to instantiate {pipeline_row.pipeline_name}")
            return RunResult(status='failed', scope=scope, error=f'Instantiation failed: {e}')

        try:
            with self._pool.acquire(
                pipeline_name=pipeline_row.pipeline_name,
                resource_group=pipeline_row.resource_group or 'soap_api',
                timeout=min(60.0, pipeline_row.timeout_seconds),
            ):
                return pipeline.run(
                    scope=scope,
                    max_age_seconds=max_age_seconds,
                    skip_freshness_check=skip_freshness_check,
                )
        except TimeoutError as e:
            logger.warning(f"{pipeline_row.pipeline_name} resource wait timeout: {e}")
            return RunResult(status='failed', scope=scope, error=str(e))
        except Exception as e:
            logger.exception(f"{pipeline_row.pipeline_name} executor error")
            return RunResult(status='failed', scope=scope, error=str(e)[:500])

    def _record_run_start(
        self,
        pipeline_name: str,
        scope: Dict[str, Any],
        scope_hash: str,
        triggered_by: str,
        triggered_by_detail: Optional[str],
        started_at: datetime,
    ):
        """Insert a 'running' row at execution start. Returns execution_id (UUID)
        on success or None on DB failure — caller falls back to one-shot record."""
        execution_id = uuid4()
        try:
            with session_scope() as session:
                session.add(SyncRun(
                    execution_id=execution_id,
                    pipeline_name=pipeline_name,
                    scope=scope or {},
                    scope_hash=scope_hash,
                    triggered_by=triggered_by,
                    triggered_by_detail=(triggered_by_detail or '')[:200] or None,
                    status='running',
                    started_at=started_at,
                    completed_at=None,
                    was_fresh=False,
                    was_deduplicated=False,
                    host_name=socket.gethostname(),
                ))
            return execution_id
        except Exception:
            logger.exception(f"Failed to record run start for {pipeline_name}")
            return None

    def _record_run_finish(
        self,
        execution_id,
        result: RunResult,
        was_fresh: bool,
        freshness_age_seconds: Optional[int],
        started_at: datetime,
    ):
        """Update the 'running' row with the final outcome."""
        try:
            now = datetime.now(timezone.utc)
            status_map = {
                'fresh': 'completed',
                'refreshed': 'completed',
                'skipped': 'completed',
                'failed': 'failed',
            }
            # Fall back to wall-clock duration when the pipeline didn't set one
            # (e.g. timeout or pre-execution failure).
            duration_ms = result.duration_ms
            if duration_ms is None and started_at is not None:
                duration_ms = int((now - started_at).total_seconds() * 1000)
            with session_scope() as session:
                session.query(SyncRun).filter(
                    SyncRun.execution_id == execution_id
                ).update({
                    'status': status_map.get(result.status, 'failed'),
                    'completed_at': now,
                    'duration_ms': duration_ms,
                    'records_processed': result.records,
                    'result': result.to_dict() if hasattr(result, 'to_dict') else None,
                    'error_message': result.error,
                    'freshness_age_seconds': freshness_age_seconds,
                    'was_fresh': was_fresh,
                })
        except Exception:
            logger.exception(f"Failed to record run finish for execution_id={execution_id}")

    def _record_run(
        self,
        pipeline_name: str,
        scope: Dict[str, Any],
        triggered_by: str,
        triggered_by_detail: Optional[str],
        result: RunResult,
        was_fresh: bool,
        was_deduplicated: bool,
        freshness_age_seconds: Optional[int],
    ):
        """Persist a run record. Failures here are logged but not raised."""
        try:
            now = datetime.now(timezone.utc)
            status_map = {
                'fresh': 'completed',
                'refreshed': 'completed',
                'skipped': 'completed',
                'failed': 'failed',
            }
            with session_scope() as session:
                run = SyncRun(
                    execution_id=uuid4(),
                    pipeline_name=pipeline_name,
                    scope=scope or {},
                    scope_hash=_hash_scope(scope),
                    triggered_by=triggered_by,
                    triggered_by_detail=(triggered_by_detail or '')[:200] or None,
                    status=status_map.get(result.status, 'failed'),
                    started_at=now,
                    completed_at=now,
                    duration_ms=result.duration_ms,
                    records_processed=result.records,
                    result=result.to_dict(),
                    error_message=result.error,
                    freshness_age_seconds=freshness_age_seconds,
                    was_fresh=was_fresh,
                    was_deduplicated=was_deduplicated,
                    host_name=socket.gethostname(),
                )
                session.add(run)
        except Exception:
            logger.exception(f"Failed to record sync run for {pipeline_name}")

    def stats(self) -> Dict[str, Any]:
        """Current executor state for observability."""
        with self._in_flight_lock:
            in_flight_count = len(self._in_flight)
            in_flight_keys = [f'{p}:{h[:8]}' for (p, h) in self._in_flight.keys()]
        return {
            'in_flight': in_flight_count,
            'in_flight_keys': in_flight_keys,
            'registered_pipelines': sorted(self._registered_pipelines),
            'resource_pool': self._pool.stats(),
        }

    def shutdown(self, wait: bool = True):
        """Shut down the thread pool."""
        self._shutdown = True
        self._thread_pool.shutdown(wait=wait)


# Module-level singleton
_executor: Optional[OnDemandExecutor] = None
_executor_lock = threading.Lock()


def get_executor() -> OnDemandExecutor:
    """Get the process-wide OnDemandExecutor. Lazy-initialized."""
    global _executor
    if _executor is None:
        with _executor_lock:
            if _executor is None:
                _executor = OnDemandExecutor()
    return _executor


def init_executor(max_workers: int = 10, pool: Optional[ResourcePool] = None) -> OnDemandExecutor:
    """Re-initialize the executor (e.g., for daemon startup)."""
    global _executor
    with _executor_lock:
        if _executor is not None:
            _executor.shutdown(wait=False)
        _executor = OnDemandExecutor(max_workers=max_workers, pool=pool)
        return _executor
