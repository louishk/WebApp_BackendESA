"""
Resource Manager for preventing resource exhaustion.
Uses semaphores to control concurrent access to shared resources.
"""

import threading
import logging
from contextlib import contextmanager
from typing import Dict, Optional, Generator
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class ResourceUsage:
    """Current usage status of a resource."""
    name: str
    in_use: int
    limit: int
    available: int
    waiting: int = 0


class ResourceManager:
    """
    Manages shared resources to prevent exhaustion and deadlocks.

    Resource Groups:
    - db_pool: PostgreSQL connections (default max 10 of 15 total)
    - soap_api: SOAP API concurrent calls (default max 5)
    - http_api: HTTP API concurrent calls (default max 10)

    Thread-safe with support for:
    - Acquiring multiple slots at once
    - Timeouts to prevent indefinite blocking
    - Usage statistics for monitoring
    """

    DEFAULT_LIMITS = {
        'db_pool': 10,      # 15 total - 5 reserved for scheduler
        'soap_api': 5,      # Limit concurrent SOAP calls
        'http_api': 10,     # HTTP pool connections limit
    }

    def __init__(self, limits: Optional[Dict[str, int]] = None):
        """
        Initialize resource manager with specified limits.

        Args:
            limits: Dictionary of resource_name -> max_concurrent
        """
        self._limits = limits or self.DEFAULT_LIMITS.copy()
        self._semaphores: Dict[str, threading.BoundedSemaphore] = {}
        self._usage: Dict[str, int] = {}
        self._waiting: Dict[str, int] = {}
        self._lock = threading.RLock()

        # Initialize semaphores for each resource
        for resource, limit in self._limits.items():
            self._semaphores[resource] = threading.BoundedSemaphore(limit)
            self._usage[resource] = 0
            self._waiting[resource] = 0

        logger.info(f"ResourceManager initialized with limits: {self._limits}")

    @contextmanager
    def acquire(
        self,
        resource: str,
        count: int = 1,
        timeout: float = 300.0,
        job_id: Optional[str] = None
    ) -> Generator[None, None, None]:
        """
        Acquire resource slots with timeout.

        Args:
            resource: Resource name (db_pool, soap_api, http_api)
            count: Number of slots to acquire
            timeout: Timeout in seconds (default 5 minutes)
            job_id: Optional job ID for logging

        Yields:
            None - use as context manager

        Raises:
            ValueError: If resource is unknown
            TimeoutError: If cannot acquire within timeout
        """
        if resource not in self._semaphores:
            raise ValueError(f"Unknown resource: {resource}. Available: {list(self._semaphores.keys())}")

        if count > self._limits[resource]:
            raise ValueError(
                f"Requested {count} slots for {resource}, but limit is {self._limits[resource]}"
            )

        acquired = 0
        start_time = datetime.now()

        try:
            # Track waiting count
            with self._lock:
                self._waiting[resource] += 1

            # Acquire requested number of slots
            for i in range(count):
                # Calculate remaining timeout
                elapsed = (datetime.now() - start_time).total_seconds()
                remaining_timeout = max(0.1, timeout - elapsed)

                if not self._semaphores[resource].acquire(timeout=remaining_timeout):
                    raise TimeoutError(
                        f"Could not acquire {resource} slot {i + 1}/{count} within {timeout}s. "
                        f"Current usage: {self._usage[resource]}/{self._limits[resource]}"
                    )
                acquired += 1

            # Update usage counter
            with self._lock:
                self._usage[resource] += count
                self._waiting[resource] -= 1

            logger.debug(
                f"[{job_id or 'unknown'}] Acquired {count} {resource} slots "
                f"(total in use: {self._usage[resource]}/{self._limits[resource]})"
            )

            yield

        finally:
            # Release acquired slots
            for _ in range(acquired):
                self._semaphores[resource].release()

            with self._lock:
                self._usage[resource] -= acquired
                if acquired < count:
                    # Didn't acquire all - was waiting
                    self._waiting[resource] = max(0, self._waiting[resource] - 1)

            if acquired > 0:
                logger.debug(
                    f"[{job_id or 'unknown'}] Released {acquired} {resource} slots "
                    f"(total in use: {self._usage[resource]}/{self._limits[resource]})"
                )

    def try_acquire(
        self,
        resource: str,
        count: int = 1
    ) -> bool:
        """
        Try to acquire resource slots without blocking.

        Args:
            resource: Resource name
            count: Number of slots to acquire

        Returns:
            True if acquired, False otherwise (does NOT release automatically)
        """
        if resource not in self._semaphores:
            return False

        with self._lock:
            available = self._limits[resource] - self._usage[resource]
            return available >= count

    def get_usage(self, resource: Optional[str] = None) -> Dict[str, ResourceUsage]:
        """
        Get current resource usage statistics.

        Args:
            resource: Optional specific resource to query

        Returns:
            Dictionary of resource_name -> ResourceUsage
        """
        with self._lock:
            if resource:
                if resource not in self._limits:
                    return {}
                return {
                    resource: ResourceUsage(
                        name=resource,
                        in_use=self._usage[resource],
                        limit=self._limits[resource],
                        available=self._limits[resource] - self._usage[resource],
                        waiting=self._waiting[resource]
                    )
                }

            return {
                name: ResourceUsage(
                    name=name,
                    in_use=self._usage[name],
                    limit=self._limits[name],
                    available=self._limits[name] - self._usage[name],
                    waiting=self._waiting[name]
                )
                for name in self._limits
            }

    def get_all_usage_dict(self) -> Dict[str, Dict[str, int]]:
        """
        Get usage as nested dict for API responses.

        Returns:
            Dictionary suitable for JSON serialization
        """
        usage = self.get_usage()
        return {
            name: {
                'in_use': u.in_use,
                'limit': u.limit,
                'available': u.available,
                'waiting': u.waiting
            }
            for name, u in usage.items()
        }

    def is_resource_available(self, resource: str, count: int = 1) -> bool:
        """
        Check if resource slots are available without acquiring.

        Args:
            resource: Resource name
            count: Number of slots needed

        Returns:
            True if slots are available
        """
        with self._lock:
            if resource not in self._limits:
                return False
            available = self._limits[resource] - self._usage[resource]
            return available >= count

    def get_limit(self, resource: str) -> int:
        """Get the limit for a resource."""
        return self._limits.get(resource, 0)

    def set_limit(self, resource: str, limit: int):
        """
        Dynamically update resource limit.
        Note: Only affects future acquisitions, not currently held resources.
        """
        with self._lock:
            old_limit = self._limits.get(resource, 0)
            self._limits[resource] = limit

            if resource not in self._semaphores:
                self._semaphores[resource] = threading.BoundedSemaphore(limit)
                self._usage[resource] = 0
                self._waiting[resource] = 0

            logger.info(f"Resource {resource} limit changed: {old_limit} -> {limit}")


# Global resource manager instance (singleton pattern)
_resource_manager: Optional[ResourceManager] = None
_manager_lock = threading.Lock()


def get_resource_manager(limits: Optional[Dict[str, int]] = None) -> ResourceManager:
    """
    Get the global resource manager instance.

    Args:
        limits: Optional limits to use when creating (only used on first call)

    Returns:
        ResourceManager singleton instance
    """
    global _resource_manager

    with _manager_lock:
        if _resource_manager is None:
            _resource_manager = ResourceManager(limits)
        return _resource_manager


def reset_resource_manager():
    """Reset the global resource manager (useful for testing)."""
    global _resource_manager
    with _manager_lock:
        _resource_manager = None
