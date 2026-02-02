"""
SQLAlchemy models for scheduler job tracking.
Follows the patterns established in common/models.py.
"""

from datetime import datetime
from uuid import uuid4
from sqlalchemy import (
    Column, Integer, String, Boolean, DateTime, Numeric, Text,
    Index, CheckConstraint
)
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class TimestampMixin:
    """Mixin for created_at and updated_at timestamps."""
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now()
    )
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now()
    )


class JobHistory(Base, TimestampMixin):
    """
    Track job execution history.
    Records every execution attempt with status, timing, and results.
    """
    __tablename__ = 'scheduler_job_history'

    id = Column(Integer, primary_key=True)
    job_id = Column(String(100), nullable=False, index=True)
    pipeline_name = Column(String(50), nullable=False, index=True)
    execution_id = Column(
        UUID(as_uuid=True),
        default=uuid4,
        unique=True,
        nullable=False,
        index=True
    )
    status = Column(
        String(20),
        nullable=False,
        default='pending',
        index=True
    )
    priority = Column(Integer, nullable=False, default=5)

    # Timing
    scheduled_at = Column(DateTime(timezone=True), nullable=False)
    started_at = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    duration_seconds = Column(Numeric(10, 2))

    # Execution details
    mode = Column(String(20))
    parameters = Column(JSONB)
    records_processed = Column(Integer)

    # Retry handling
    attempt_number = Column(Integer, nullable=False, default=1)
    max_retries = Column(Integer, nullable=False, default=3)
    retry_delay_seconds = Column(Integer, default=300)
    next_retry_at = Column(DateTime(timezone=True))

    # Error tracking
    error_message = Column(Text)
    error_traceback = Column(Text)

    # Alerts
    alert_sent = Column(Boolean, default=False)
    alert_sent_at = Column(DateTime(timezone=True))

    # Metadata
    triggered_by = Column(String(50), default='scheduler')  # scheduler, cli, api, manual
    host_name = Column(String(100))

    # Indexes for common queries
    __table_args__ = (
        Index('idx_job_history_scheduled_desc', scheduled_at.desc()),
        Index('idx_job_history_pipeline_status', pipeline_name, status),
        CheckConstraint(
            "status IN ('pending', 'queued', 'running', 'completed', 'failed', 'cancelled', 'retrying')",
            name='chk_job_status'
        ),
    )

    def __repr__(self):
        return (f"<JobHistory(id={self.id}, pipeline={self.pipeline_name}, "
                f"status={self.status}, execution_id={self.execution_id})>")

    @property
    def is_terminal(self) -> bool:
        """Check if job is in a terminal state."""
        return self.status in ('completed', 'failed', 'cancelled')

    @property
    def can_retry(self) -> bool:
        """Check if job can be retried."""
        return self.status == 'failed' and self.attempt_number < self.max_retries

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses."""
        return {
            'id': self.id,
            'job_id': self.job_id,
            'pipeline_name': self.pipeline_name,
            'execution_id': str(self.execution_id),
            'status': self.status,
            'priority': self.priority,
            'scheduled_at': self.scheduled_at.isoformat() if self.scheduled_at else None,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'duration_seconds': float(self.duration_seconds) if self.duration_seconds else None,
            'mode': self.mode,
            'parameters': self.parameters,
            'records_processed': self.records_processed,
            'attempt_number': self.attempt_number,
            'max_retries': self.max_retries,
            'error_message': self.error_message,
            'triggered_by': self.triggered_by,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }


class PipelineConfig(Base, TimestampMixin):
    """
    Pipeline configuration stored in database.
    Allows runtime configuration changes without YAML edits.
    """
    __tablename__ = 'scheduler_pipeline_config'

    pipeline_name = Column(String(50), primary_key=True)
    display_name = Column(String(100), nullable=False)
    description = Column(Text)
    module_path = Column(String(200), nullable=False)

    # Scheduling
    schedule_type = Column(String(20), nullable=False)  # cron, interval, date
    schedule_config = Column(JSONB, nullable=False)
    enabled = Column(Boolean, nullable=False, default=True)

    # Priority and dependencies
    priority = Column(Integer, nullable=False, default=5)
    depends_on = Column(ARRAY(String(50)))
    conflicts_with = Column(ARRAY(String(50)))

    # Resource requirements
    resource_group = Column(String(50), default='default')
    max_db_connections = Column(Integer, default=3)
    estimated_duration_seconds = Column(Integer)

    # Retry configuration
    max_retries = Column(Integer, nullable=False, default=3)
    retry_delay_seconds = Column(Integer, nullable=False, default=300)

    # Timeouts
    timeout_seconds = Column(Integer, default=3600)

    def __repr__(self):
        return f"<PipelineConfig(name={self.pipeline_name}, enabled={self.enabled})>"

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses."""
        return {
            'pipeline_name': self.pipeline_name,
            'display_name': self.display_name,
            'description': self.description,
            'module_path': self.module_path,
            'schedule_type': self.schedule_type,
            'schedule_config': self.schedule_config,
            'enabled': self.enabled,
            'priority': self.priority,
            'depends_on': self.depends_on or [],
            'conflicts_with': self.conflicts_with or [],
            'resource_group': self.resource_group,
            'max_db_connections': self.max_db_connections,
            'max_retries': self.max_retries,
            'timeout_seconds': self.timeout_seconds,
        }


class ResourceLock(Base):
    """
    Resource locks for preventing concurrent access.
    Used for distributed locking when multiple scheduler instances run.
    """
    __tablename__ = 'scheduler_resource_locks'

    resource_name = Column(String(100), primary_key=True)
    locked_by_job_id = Column(String(100))
    locked_by_execution_id = Column(UUID(as_uuid=True))
    locked_at = Column(DateTime(timezone=True))
    lock_expires_at = Column(DateTime(timezone=True))
    max_concurrent = Column(Integer, default=1)
    current_count = Column(Integer, default=0)

    def __repr__(self):
        return f"<ResourceLock(resource={self.resource_name}, count={self.current_count}/{self.max_concurrent})>"

    @property
    def is_expired(self) -> bool:
        """Check if lock has expired."""
        if not self.lock_expires_at:
            return False
        return datetime.now(self.lock_expires_at.tzinfo) > self.lock_expires_at

    @property
    def is_available(self) -> bool:
        """Check if resource is available for locking."""
        return self.current_count < self.max_concurrent or self.is_expired


class SchedulerState(Base):
    """
    Singleton table for scheduler daemon state.
    Used for health checks and preventing multiple instances.
    """
    __tablename__ = 'scheduler_state'

    id = Column(
        Integer,
        primary_key=True,
        default=1
    )
    status = Column(String(20), nullable=False, default='stopped')  # running, stopped, paused
    started_at = Column(DateTime(timezone=True))
    host_name = Column(String(100))
    pid = Column(Integer)
    last_heartbeat = Column(DateTime(timezone=True))
    version = Column(String(20))
    config_hash = Column(String(64))  # For detecting config changes

    __table_args__ = (
        CheckConstraint('id = 1', name='chk_singleton'),
        CheckConstraint(
            "status IN ('running', 'stopped', 'paused', 'starting', 'stopping')",
            name='chk_scheduler_status'
        ),
    )

    def __repr__(self):
        return f"<SchedulerState(status={self.status}, pid={self.pid})>"

    def to_dict(self) -> dict:
        """Convert to dictionary for API responses."""
        return {
            'status': self.status,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'host_name': self.host_name,
            'pid': self.pid,
            'last_heartbeat': self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            'version': self.version,
        }


def create_scheduler_tables(engine):
    """Create all scheduler tables if they don't exist."""
    Base.metadata.create_all(engine)


def drop_scheduler_tables(engine):
    """Drop all scheduler tables (use with caution!)."""
    Base.metadata.drop_all(engine)
