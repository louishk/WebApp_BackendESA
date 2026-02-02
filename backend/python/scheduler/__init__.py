"""
PBI Datalayer Pipeline Scheduler

A robust job scheduler for data pipelines with:
- Priority-based queue management
- Conflict resolution to prevent deadlocks
- Resource semaphores for connection pooling
- Retry with exponential backoff
- Slack/email alerts
- CLI and web UI interfaces
"""

from pathlib import Path

# Read version from VERSION file
_version_file = Path(__file__).parent / 'VERSION'
if _version_file.exists():
    __version__ = _version_file.read_text().strip()
else:
    __version__ = '1.0.0'

def get_version():
    """Return the current scheduler version."""
    return __version__

from scheduler.config import SchedulerConfig
from scheduler.models import JobHistory, PipelineConfig, ResourceLock, SchedulerState
from scheduler.resource_manager import ResourceManager
from scheduler.conflict_resolver import ConflictResolver, JobContext
from scheduler.executor import PipelineExecutor
from scheduler.alert_manager import AlertManager, AlertContext

__all__ = [
    '__version__',
    'get_version',
    'SchedulerConfig',
    'JobHistory',
    'PipelineConfig',
    'ResourceLock',
    'SchedulerState',
    'ResourceManager',
    'ConflictResolver',
    'JobContext',
    'PipelineExecutor',
    'AlertManager',
    'AlertContext',
]
