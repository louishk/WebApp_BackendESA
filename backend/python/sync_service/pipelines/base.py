"""
BasePipeline — abstract contract that every sync_service pipeline implements.

Every pipeline script subclasses BasePipeline and implements `run(scope, max_age_seconds)`.
The executor calls this uniformly for both scheduled and on-demand invocations.
"""

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional

from sync_service.models import SyncPipeline

logger = logging.getLogger(__name__)


@dataclass
class RunResult:
    """Uniform result returned by every pipeline.run() call."""
    status: str                              # 'fresh' | 'refreshed' | 'skipped' | 'failed'
    records: int = 0
    scope: Dict[str, Any] = field(default_factory=dict)
    duration_ms: int = 0
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)  # pipeline-specific info

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def success(self) -> bool:
        return self.status in ('fresh', 'refreshed', 'skipped')


class BasePipeline(ABC):
    """Contract for every sync_service pipeline.

    Subclasses implement `_execute()`. The base class handles:
      - Freshness check (skip if data is fresh enough)
      - Timing
      - Error wrapping into a RunResult

    Subclasses SHOULD NOT override `run()` — override `_execute()` instead.
    """

    def __init__(self, config_row: SyncPipeline):
        self.config = config_row
        self.name = config_row.pipeline_name
        self.log = logging.getLogger(f"sync_service.pipelines.{self.name}")

    @abstractmethod
    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        """Subclass implementation. Perform the actual fetch + write.

        Args:
            scope: Merged scope dict (default_args + request scope). Always a dict,
                   never None. Subclass inspects keys it cares about (site_codes,
                   customer_ids, date_from, etc).

        Returns:
            RunResult with status='refreshed' on success
        """
        raise NotImplementedError

    def run(
        self,
        scope: Optional[Dict[str, Any]] = None,
        max_age_seconds: Optional[int] = None,
        skip_freshness_check: bool = False,
    ) -> RunResult:
        """Public entry point — called by executor.

        Args:
            scope: Request scope (e.g., {"site_codes": ["L017"]}). None → full run.
            max_age_seconds: Override the pipeline's default TTL. If data is
                             fresher than this, return immediately without running.
            skip_freshness_check: Force execution regardless of freshness (e.g., for
                                  scheduled full runs or explicit /run endpoint).
        """
        # Merge default_args with request scope (request wins on conflict)
        effective_scope: Dict[str, Any] = dict(self.config.default_args or {})
        if scope:
            effective_scope.update(scope)

        start = time.monotonic()

        # Freshness gate
        if not skip_freshness_check:
            from sync_service.freshness import check_freshness
            try:
                age = check_freshness(self.config, effective_scope)
            except Exception as e:
                self.log.warning(f"Freshness check failed, proceeding with run: {e}")
                age = None

            ttl = (
                max_age_seconds
                if max_age_seconds is not None
                else self.config.freshness_ttl_seconds
            )

            if age is not None and age < ttl:
                duration_ms = int((time.monotonic() - start) * 1000)
                self.log.info(
                    f"{self.name} fresh (age={age:.0f}s < ttl={ttl}s) scope={effective_scope}"
                )
                return RunResult(
                    status='fresh',
                    records=0,
                    scope=effective_scope,
                    duration_ms=duration_ms,
                    metadata={'freshness_age_seconds': age, 'ttl_used': ttl},
                )

        # Execute
        try:
            self.log.info(f"{self.name} executing scope={effective_scope}")
            result = self._execute(effective_scope)
            result.duration_ms = int((time.monotonic() - start) * 1000)
            if result.scope is None or result.scope == {}:
                result.scope = effective_scope
            return result
        except Exception as e:
            duration_ms = int((time.monotonic() - start) * 1000)
            self.log.exception(f"{self.name} execution failed: {e}")
            return RunResult(
                status='failed',
                records=0,
                scope=effective_scope,
                duration_ms=duration_ms,
                error=str(e)[:500],
            )
