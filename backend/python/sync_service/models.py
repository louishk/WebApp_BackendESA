"""
SQLAlchemy models for sync_service.

Own declarative_base — no shared metadata with scheduler/ or sync/.
"""

from datetime import datetime
from typing import Optional
from sqlalchemy import (
    Column, Integer, BigInteger, String, Boolean, DateTime, Text,
    ForeignKey, Index, CheckConstraint
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class SyncPipeline(Base):
    """Pipeline registry entry — one row per sync_service pipeline."""
    __tablename__ = 'mw_sync_pipelines'

    pipeline_name = Column(String(100), primary_key=True)
    display_name = Column(String(200), nullable=False)
    description = Column(Text)
    pipeline_class = Column(String(300), nullable=False)
    enabled = Column(Boolean, nullable=False, default=True)

    schedule_type = Column(String(20), default='on_demand')
    schedule_config = Column(JSONB, default=dict)

    freshness_table = Column(String(100))
    freshness_column = Column(String(100))
    freshness_scope_column = Column(String(100))
    freshness_ttl_seconds = Column(Integer, nullable=False, default=300)
    freshness_database = Column(String(20), nullable=False, default='pbi')

    max_concurrency = Column(Integer, nullable=False, default=5)
    resource_group = Column(String(50), default='soap_api')
    max_db_connections = Column(Integer, nullable=False, default=2)
    timeout_seconds = Column(Integer, nullable=False, default=600)
    max_retries = Column(Integer, nullable=False, default=3)
    retry_delay_seconds = Column(Integer, nullable=False, default=60)

    default_args = Column(JSONB, default=dict)

    # Manual frequency-bucket override (high|med|low). NULL = auto-derive from cron.
    # See sync_service.cadence.derive_frequency_category.
    frequency_category = Column(String(10))

    # Multi-destination write targets. NULL = derive from freshness_* fields.
    # Each entry: {"database": "middleware|pbi|backend", "table": "...", "column": "..."}.
    # Observability-only — pipeline code still writes wherever it was hardcoded.
    destinations = Column(JSONB)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True), nullable=False,
        server_default=func.now(), onupdate=func.now()
    )

    @property
    def resolved_frequency_category(self) -> Optional[str]:
        if self.frequency_category:
            return self.frequency_category
        from sync_service.cadence import derive_frequency_category
        return derive_frequency_category(self.schedule_config)

    @property
    def resolved_destinations(self) -> list:
        """Return destinations list, falling back to a 1-item list synthesised
        from the legacy freshness_* fields if `destinations` is NULL/empty."""
        if self.destinations:
            return self.destinations
        if self.freshness_table:
            return [{
                'database': self.freshness_database or 'middleware',
                'table': self.freshness_table,
                'column': self.freshness_column or 'updated_at',
            }]
        return []

    def to_dict(self) -> dict:
        return {
            'pipeline_name': self.pipeline_name,
            'display_name': self.display_name,
            'description': self.description,
            'pipeline_class': self.pipeline_class,
            'enabled': self.enabled,
            'schedule_type': self.schedule_type,
            'schedule_config': self.schedule_config or {},
            'freshness': {
                'table': self.freshness_table,
                'column': self.freshness_column,
                'scope_column': self.freshness_scope_column,
                'ttl_seconds': self.freshness_ttl_seconds,
                'database': self.freshness_database,
            },
            'execution': {
                'max_concurrency': self.max_concurrency,
                'resource_group': self.resource_group,
                'max_db_connections': self.max_db_connections,
                'timeout_seconds': self.timeout_seconds,
                'max_retries': self.max_retries,
                'retry_delay_seconds': self.retry_delay_seconds,
            },
            'default_args': self.default_args or {},
            'frequency_category': self.frequency_category,
            'resolved_frequency_category': self.resolved_frequency_category,
            'destinations': self.destinations,
            'resolved_destinations': self.resolved_destinations,
        }


class SyncRun(Base):
    """Execution history — one row per run attempt."""
    __tablename__ = 'mw_sync_runs'

    id = Column(BigInteger, primary_key=True)
    execution_id = Column(UUID(as_uuid=True), nullable=False, unique=True)
    pipeline_name = Column(
        String(100),
        ForeignKey('mw_sync_pipelines.pipeline_name', ondelete='CASCADE'),
        nullable=False,
    )

    scope = Column(JSONB, nullable=False, default=dict)
    scope_hash = Column(String(64), nullable=False)

    triggered_by = Column(String(50), nullable=False)
    triggered_by_detail = Column(String(200))

    status = Column(String(20), nullable=False, default='queued')
    queued_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    duration_ms = Column(Integer)

    records_processed = Column(Integer, default=0)
    result = Column(JSONB)
    error_message = Column(Text)

    freshness_age_seconds = Column(Integer)
    was_fresh = Column(Boolean)
    was_deduplicated = Column(Boolean, nullable=False, default=False)

    attempt_number = Column(Integer, nullable=False, default=1)
    host_name = Column(String(100))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    __table_args__ = (
        Index('idx_sync_runs_pipeline', 'pipeline_name', 'started_at'),
        Index('idx_sync_runs_scope_hash', 'pipeline_name', 'scope_hash', 'status'),
    )

    def to_dict(self) -> dict:
        return {
            'execution_id': str(self.execution_id),
            'pipeline_name': self.pipeline_name,
            'scope': self.scope or {},
            'triggered_by': self.triggered_by,
            'status': self.status,
            'queued_at': self.queued_at.isoformat() if self.queued_at else None,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'duration_ms': self.duration_ms,
            'records_processed': self.records_processed,
            'result': self.result,
            'error_message': self.error_message,
            'freshness_age_seconds': self.freshness_age_seconds,
            'was_fresh': self.was_fresh,
            'was_deduplicated': self.was_deduplicated,
            'attempt_number': self.attempt_number,
        }


class SyncStateEntry(Base):
    """Per-scope cursor/watermark for incremental sync."""
    __tablename__ = 'mw_sync_state'

    pipeline_name = Column(
        String(100),
        ForeignKey('mw_sync_pipelines.pipeline_name', ondelete='CASCADE'),
        primary_key=True,
    )
    scope_key = Column(String(300), primary_key=True, default='__all__')
    phase = Column(String(50), primary_key=True, default='main')

    cursor_value = Column(Text)
    cursor_type = Column(String(20), default='timestamp')
    last_sync_at = Column(DateTime(timezone=True))
    last_success_at = Column(DateTime(timezone=True))
    records_in_scope = Column(Integer)

    updated_at = Column(
        DateTime(timezone=True), nullable=False,
        server_default=func.now(), onupdate=func.now()
    )


class SyncServiceState(Base):
    """Singleton daemon state for health checks."""
    __tablename__ = 'mw_sync_service_state'

    id = Column(Integer, primary_key=True, default=1)
    status = Column(String(20), nullable=False, default='stopped')
    started_at = Column(DateTime(timezone=True))
    host_name = Column(String(100))
    pid = Column(Integer)
    last_heartbeat = Column(DateTime(timezone=True))
    version = Column(String(20))

    __table_args__ = (
        CheckConstraint('id = 1', name='chk_sync_service_singleton'),
    )
