"""
Shared resource pool — semaphore-based slots for SOAP, HTTP, DB connections.

Independent of scheduler/resource_manager.py. Simpler implementation focused on
sync_service needs: bounded parallelism + per-pipeline concurrency caps.
"""

import logging
import threading
from contextlib import contextmanager
from typing import Dict, Iterator, Optional

logger = logging.getLogger(__name__)


class ResourcePool:
    """Thread-safe resource pool using bounded semaphores.

    Two levels of limits:
      1. Global resource groups (soap_api, http_api, db_pool) — shared across
         all pipelines competing for the same upstream system
      2. Per-pipeline concurrency — max parallel runs of the same pipeline
         (different scopes)
    """

    def __init__(self, global_limits: Optional[Dict[str, int]] = None):
        default_limits = {
            'soap_api': 5,
            'http_api': 10,
            'db_pool': 5,
        }
        if global_limits:
            default_limits.update(global_limits)

        self._globals: Dict[str, threading.BoundedSemaphore] = {
            name: threading.BoundedSemaphore(limit)
            for name, limit in default_limits.items()
        }
        self._global_limits = default_limits
        self._global_usage: Dict[str, int] = {name: 0 for name in default_limits}

        self._pipeline_locks: Dict[str, threading.BoundedSemaphore] = {}
        self._pipeline_limits: Dict[str, int] = {}
        self._pipeline_usage: Dict[str, int] = {}
        self._registry_lock = threading.Lock()
        self._usage_lock = threading.Lock()

    def register_pipeline(self, pipeline_name: str, max_concurrency: int):
        """Register per-pipeline concurrency limit. Idempotent."""
        with self._registry_lock:
            if pipeline_name not in self._pipeline_locks:
                self._pipeline_locks[pipeline_name] = threading.BoundedSemaphore(max_concurrency)
                self._pipeline_limits[pipeline_name] = max_concurrency
                self._pipeline_usage[pipeline_name] = 0
                logger.debug(
                    f"ResourcePool: registered {pipeline_name} max_concurrency={max_concurrency}"
                )

    @contextmanager
    def acquire(
        self,
        pipeline_name: str,
        resource_group: str,
        timeout: float = 30.0,
    ) -> Iterator[None]:
        """Acquire per-pipeline slot + global resource slot.

        Args:
            pipeline_name: Pipeline requesting the slot
            resource_group: Global pool to acquire from (soap_api, http_api, db_pool)
            timeout: Max seconds to wait for both slots (combined)

        Raises:
            TimeoutError if either semaphore can't be acquired in time
        """
        pipeline_sem = self._pipeline_locks.get(pipeline_name)
        if pipeline_sem is None:
            raise ValueError(f"Pipeline {pipeline_name} not registered in resource pool")

        global_sem = self._globals.get(resource_group)
        if global_sem is None:
            raise ValueError(f"Unknown resource group: {resource_group}")

        # Acquire pipeline slot first — cheaper to fail fast
        if not pipeline_sem.acquire(timeout=timeout):
            raise TimeoutError(
                f"Could not acquire pipeline slot for {pipeline_name} within {timeout}s"
            )
        with self._usage_lock:
            self._pipeline_usage[pipeline_name] += 1

        try:
            if not global_sem.acquire(timeout=timeout):
                raise TimeoutError(
                    f"Could not acquire {resource_group} slot within {timeout}s"
                )
            with self._usage_lock:
                self._global_usage[resource_group] += 1
            try:
                yield
            finally:
                with self._usage_lock:
                    self._global_usage[resource_group] -= 1
                global_sem.release()
        finally:
            with self._usage_lock:
                self._pipeline_usage[pipeline_name] -= 1
            pipeline_sem.release()

    def stats(self) -> Dict[str, Dict[str, int]]:
        """Return current slot usage for observability.

        Counters are incremented on acquire and decremented on release, so
        `*_in_use` reflects live holders of each semaphore.
        """
        with self._usage_lock:
            return {
                'global_limits': dict(self._global_limits),
                'global_in_use': dict(self._global_usage),
                'pipeline_limits': dict(self._pipeline_limits),
                'pipeline_in_use': dict(self._pipeline_usage),
            }


# Module-level singleton (initialized by main.py / test setup)
_pool: Optional[ResourcePool] = None


def get_pool() -> ResourcePool:
    """Get the process-wide ResourcePool. Initialized on first call."""
    global _pool
    if _pool is None:
        _pool = ResourcePool()
    return _pool


def init_pool(global_limits: Optional[Dict[str, int]] = None) -> ResourcePool:
    """Initialize (or re-initialize) the process-wide ResourcePool."""
    global _pool
    _pool = ResourcePool(global_limits)
    return _pool
