"""
Conflict Resolver for managing job dependencies and preventing concurrent conflicts.
Implements priority-based queue ordering and dependency checking.
"""

import threading
import logging
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum
from uuid import UUID

from scheduler.config import PipelineDefinition

logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """Job execution status."""
    PENDING = 'pending'
    QUEUED = 'queued'
    RUNNING = 'running'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELLED = 'cancelled'
    RETRYING = 'retrying'


@dataclass
class JobContext:
    """
    Context for a job in the queue.
    Contains all information needed for scheduling decisions.
    """
    pipeline_name: str
    execution_id: UUID
    priority: int
    scheduled_at: datetime
    config: PipelineDefinition
    status: JobStatus = JobStatus.PENDING
    attempt_number: int = 1
    queued_at: Optional[datetime] = None
    started_at: Optional[datetime] = None

    def __lt__(self, other: 'JobContext') -> bool:
        """Compare for priority queue ordering (lower priority number = higher priority)."""
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.scheduled_at < other.scheduled_at

    def __repr__(self):
        return f"<JobContext({self.pipeline_name}, priority={self.priority}, status={self.status.value})>"


@dataclass
class ConflictCheckResult:
    """Result of a conflict check."""
    can_start: bool
    reason: Optional[str] = None
    blocking_jobs: List[str] = field(default_factory=list)


class ConflictResolver:
    """
    Resolves scheduling conflicts between pipelines.

    Key Principles:
    1. Higher priority jobs are scheduled first (lower number = higher priority)
    2. Conflicting pipelines never run concurrently
    3. Dependencies must complete before dependents start
    4. Resource limits are strictly enforced

    Thread-safe implementation for concurrent access.
    """

    def __init__(self, resource_limits: Optional[Dict[str, int]] = None):
        """
        Initialize conflict resolver.

        Args:
            resource_limits: Dictionary of resource_name -> max_slots
        """
        self._lock = threading.RLock()
        self._running_jobs: Dict[str, JobContext] = {}  # pipeline_name -> JobContext
        self._resource_limits = resource_limits or {'db_connections': 15}
        self._recently_completed: Dict[str, datetime] = {}  # Track recent completions

    def can_start(
        self,
        pipeline_name: str,
        config: PipelineDefinition,
    ) -> ConflictCheckResult:
        """
        Check if a pipeline can start execution now.

        Checks:
        1. No dependencies are currently running
        2. No conflicting pipelines are running
        3. Resource limits are not exceeded

        Args:
            pipeline_name: Name of pipeline to check
            config: Pipeline configuration

        Returns:
            ConflictCheckResult with can_start flag and reason if blocked
        """
        with self._lock:
            blocking = []

            # Check 1: Dependencies must not be running
            # (they should complete first)
            if config.depends_on:
                for dep in config.depends_on:
                    if dep in self._running_jobs:
                        blocking.append(dep)
                        return ConflictCheckResult(
                            can_start=False,
                            reason=f"Waiting for dependency '{dep}' to complete",
                            blocking_jobs=[dep]
                        )

            # Check 2: No conflicts should be running
            if config.conflicts_with:
                for conflict in config.conflicts_with:
                    if conflict in self._running_jobs:
                        blocking.append(conflict)

                if blocking:
                    return ConflictCheckResult(
                        can_start=False,
                        reason=f"Conflicting pipeline(s) running: {', '.join(blocking)}",
                        blocking_jobs=blocking
                    )

            # Check 3: Resource availability
            current_db_usage = sum(
                j.config.max_db_connections
                for j in self._running_jobs.values()
            )
            max_db = self._resource_limits.get('db_connections', 15)

            if current_db_usage + config.max_db_connections > max_db:
                return ConflictCheckResult(
                    can_start=False,
                    reason=f"Insufficient DB connections: using {current_db_usage}/{max_db}, need {config.max_db_connections}",
                    blocking_jobs=list(self._running_jobs.keys())
                )

            return ConflictCheckResult(can_start=True)

    def register_start(self, job_context: JobContext):
        """
        Register a job as running.

        Args:
            job_context: Job context to register
        """
        with self._lock:
            self._running_jobs[job_context.pipeline_name] = job_context
            job_context.status = JobStatus.RUNNING
            job_context.started_at = datetime.now()

            logger.info(
                f"Job registered as running: {job_context.pipeline_name} "
                f"(execution_id={job_context.execution_id})"
            )

    def register_complete(
        self,
        pipeline_name: str,
        status: JobStatus = JobStatus.COMPLETED
    ):
        """
        Register a job as complete.

        Args:
            pipeline_name: Name of completed pipeline
            status: Final status (COMPLETED, FAILED, CANCELLED)
        """
        with self._lock:
            job = self._running_jobs.pop(pipeline_name, None)
            if job:
                job.status = status
                self._recently_completed[pipeline_name] = datetime.now()

                logger.info(
                    f"Job completed: {pipeline_name} with status {status.value}"
                )

    def get_running_jobs(self) -> Dict[str, JobContext]:
        """Get all currently running jobs."""
        with self._lock:
            return self._running_jobs.copy()

    def get_running_count(self) -> int:
        """Get count of running jobs."""
        with self._lock:
            return len(self._running_jobs)

    def is_pipeline_running(self, pipeline_name: str) -> bool:
        """Check if a specific pipeline is currently running."""
        with self._lock:
            return pipeline_name in self._running_jobs

    def get_next_runnable(
        self,
        pending_jobs: List[JobContext]
    ) -> Optional[JobContext]:
        """
        Get the highest priority job that can run now.

        Jobs are sorted by:
        1. Priority (lower number = higher priority)
        2. Scheduled time (earlier = first)

        Args:
            pending_jobs: List of pending jobs to consider

        Returns:
            Highest priority runnable job, or None if all blocked
        """
        # Sort by priority, then by scheduled time
        sorted_jobs = sorted(pending_jobs)

        for job in sorted_jobs:
            result = self.can_start(job.pipeline_name, job.config)
            if result.can_start:
                return job

        return None

    def get_queue_status(
        self,
        pending_jobs: List[JobContext]
    ) -> Dict[str, dict]:
        """
        Get detailed status for all pending jobs.

        Args:
            pending_jobs: List of pending jobs

        Returns:
            Dictionary of pipeline_name -> status info
        """
        status = {}

        for job in pending_jobs:
            result = self.can_start(job.pipeline_name, job.config)
            status[job.pipeline_name] = {
                'priority': job.priority,
                'scheduled_at': job.scheduled_at.isoformat(),
                'can_start': result.can_start,
                'blocked_by': result.blocking_jobs if not result.can_start else [],
                'reason': result.reason,
            }

        return status

    def get_dependency_order(
        self,
        pipelines: List[PipelineDefinition]
    ) -> List[str]:
        """
        Get topologically sorted order respecting dependencies.

        Args:
            pipelines: List of pipeline definitions

        Returns:
            List of pipeline names in dependency order
        """
        # Build dependency graph
        graph: Dict[str, Set[str]] = {}
        for p in pipelines:
            graph[p.pipeline_name] = set(p.depends_on or [])

        # Topological sort (Kahn's algorithm)
        in_degree = {name: len(deps) for name, deps in graph.items()}
        queue = [name for name, degree in in_degree.items() if degree == 0]
        result = []

        while queue:
            # Sort by priority within zero in-degree nodes
            queue.sort(key=lambda n: next(
                (p.priority for p in pipelines if p.pipeline_name == n), 99
            ))
            node = queue.pop(0)
            result.append(node)

            for name, deps in graph.items():
                if node in deps:
                    in_degree[name] -= 1
                    if in_degree[name] == 0:
                        queue.append(name)

        return result

    def would_cause_deadlock(
        self,
        pipeline_name: str,
        config: PipelineDefinition
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if starting this pipeline could cause a deadlock.

        A deadlock can occur when:
        - Pipeline A conflicts with B
        - Pipeline B conflicts with A
        - Both are waiting for each other

        Args:
            pipeline_name: Pipeline to check
            config: Pipeline configuration

        Returns:
            (would_deadlock, explanation)
        """
        with self._lock:
            # Check for circular conflict dependencies
            conflicts = set(config.conflicts_with or [])

            for running_name, running_job in self._running_jobs.items():
                running_conflicts = set(running_job.config.conflicts_with or [])

                # If both conflict with each other, that's expected (mutual exclusion)
                # Deadlock would be if they're waiting for each other's resources

                if pipeline_name in running_conflicts and running_name in conflicts:
                    # This is normal mutual exclusion, not deadlock
                    pass

            # Check dependency cycles
            visited = set()
            stack = [pipeline_name]

            while stack:
                current = stack.pop()
                if current in visited:
                    continue
                visited.add(current)

                # Get dependencies
                current_config = config if current == pipeline_name else None
                if current_config:
                    deps = current_config.depends_on or []
                    for dep in deps:
                        if dep == pipeline_name:
                            return True, f"Circular dependency detected: {pipeline_name} -> ... -> {dep}"
                        stack.append(dep)

            return False, None

    def clear(self):
        """Clear all running jobs (use for shutdown/reset)."""
        with self._lock:
            self._running_jobs.clear()
            self._recently_completed.clear()
            logger.info("ConflictResolver cleared all state")
