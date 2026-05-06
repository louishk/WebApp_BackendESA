"""
Neutral pipeline registry — shared by scheduler engine and sync orchestrator.

This module is the single interface for reading pipeline config from the
`scheduler_pipeline_config` table. Neither engine imports from the other;
both import from here.

Ownership is determined by the `managed_by` column:
  - 'scheduler'    → APScheduler engine executes on cron
  - 'orchestrator' → Sync orchestrator executes watermark/phase pipelines
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class PipelineRecord:
    """Neutral representation of a pipeline_config row.

    Mirrors the DB schema without any engine-specific helpers.
    Scheduler and sync orchestrator each adapt this to their internal types.
    """
    pipeline_name: str
    display_name: str
    description: Optional[str]
    module_path: str
    schedule_type: str
    schedule_config: Dict[str, Any]
    enabled: bool
    priority: int
    depends_on: List[str] = field(default_factory=list)
    conflicts_with: List[str] = field(default_factory=list)
    resource_group: str = 'soap_api'
    max_db_connections: int = 3
    estimated_duration_seconds: int = 600
    max_retries: int = 3
    retry_delay_seconds: int = 300
    retry_backoff_multiplier: float = 2.0
    timeout_seconds: int = 3600
    default_args: Dict[str, Any] = field(default_factory=dict)
    data_freshness_config: Dict[str, Any] = field(default_factory=dict)
    sync_config: Dict[str, Any] = field(default_factory=dict)
    pipeline_specific_args: Dict[str, Any] = field(default_factory=dict)
    managed_by: str = 'scheduler'

    @classmethod
    def from_row(cls, row) -> 'PipelineRecord':
        """Build from a SQLAlchemy PipelineConfig ORM row."""
        return cls(
            pipeline_name=row.pipeline_name,
            display_name=row.display_name,
            description=row.description,
            module_path=row.module_path,
            schedule_type=row.schedule_type,
            schedule_config=row.schedule_config or {},
            enabled=row.enabled,
            priority=row.priority or 5,
            depends_on=list(row.depends_on or []),
            conflicts_with=list(row.conflicts_with or []),
            resource_group=row.resource_group or 'soap_api',
            max_db_connections=row.max_db_connections or 3,
            estimated_duration_seconds=row.estimated_duration_seconds or 600,
            max_retries=row.max_retries or 3,
            retry_delay_seconds=row.retry_delay_seconds or 300,
            retry_backoff_multiplier=float(row.retry_backoff_multiplier or 2.0),
            timeout_seconds=row.timeout_seconds or 3600,
            default_args=row.default_args or {},
            data_freshness_config=row.data_freshness_config or {},
            sync_config=row.sync_config or {},
            pipeline_specific_args=row.pipeline_specific_args or {},
            managed_by=row.managed_by or 'scheduler',
        )


# Ownership discriminator values
MANAGED_BY_SCHEDULER = 'scheduler'
MANAGED_BY_ORCHESTRATOR = 'orchestrator'
VALID_OWNERS = (MANAGED_BY_SCHEDULER, MANAGED_BY_ORCHESTRATOR)


def load_pipelines(session, managed_by: Optional[str] = None) -> List[PipelineRecord]:
    """Load pipeline records, optionally filtered by owner.

    Args:
        session: SQLAlchemy session bound to esa_backend DB
        managed_by: 'scheduler', 'orchestrator', or None for all

    Returns:
        List of PipelineRecord, ordered by priority
    """
    from scheduler.models import PipelineConfig

    q = session.query(PipelineConfig)
    if managed_by is not None:
        if managed_by not in VALID_OWNERS:
            raise ValueError(f"managed_by must be one of {VALID_OWNERS}")
        q = q.filter(PipelineConfig.managed_by == managed_by)
    q = q.order_by(PipelineConfig.priority)
    return [PipelineRecord.from_row(r) for r in q.all()]


def load_scheduler_pipelines(session) -> List[PipelineRecord]:
    """Return pipelines owned by the APScheduler engine."""
    return load_pipelines(session, managed_by=MANAGED_BY_SCHEDULER)


def load_sync_pipelines(session) -> List[PipelineRecord]:
    """Return pipelines owned by the sync orchestrator."""
    return load_pipelines(session, managed_by=MANAGED_BY_ORCHESTRATOR)


def load_ownership_map(session) -> Dict[str, str]:
    """Return {pipeline_name: managed_by} for all pipelines.

    Used by cross-cutting dashboard endpoints to show who handles what.
    """
    from scheduler.models import PipelineConfig
    rows = session.query(
        PipelineConfig.pipeline_name,
        PipelineConfig.managed_by,
    ).all()
    return {name: (owner or 'scheduler') for name, owner in rows}


def transfer_pipeline(session, pipeline_name: str, new_owner: str) -> bool:
    """Transfer a pipeline between engines.

    Args:
        session: SQLAlchemy session
        pipeline_name: Pipeline to transfer
        new_owner: 'scheduler' or 'orchestrator'

    Returns:
        True if transferred, False if pipeline not found

    Raises:
        ValueError if new_owner is invalid
    """
    if new_owner not in VALID_OWNERS:
        raise ValueError(f"new_owner must be one of {VALID_OWNERS}")

    from scheduler.models import PipelineConfig
    row = session.query(PipelineConfig).filter_by(pipeline_name=pipeline_name).first()
    if not row:
        return False
    row.managed_by = new_owner
    return True
