"""
Pipeline registry — load/resolve SyncPipeline rows and instantiate classes.
"""

import importlib
import logging
from typing import List, Optional

from sync_service.config import session_scope
from sync_service.models import SyncPipeline
from sync_service.pipelines.base import BasePipeline

logger = logging.getLogger(__name__)


def list_pipelines(enabled_only: bool = False) -> List[SyncPipeline]:
    """Return all sync_service pipeline registry rows."""
    with session_scope() as session:
        q = session.query(SyncPipeline)
        if enabled_only:
            q = q.filter(SyncPipeline.enabled.is_(True))
        rows = q.order_by(SyncPipeline.pipeline_name).all()
        # Detach from session so callers can use them after scope exits
        session.expunge_all()
        return rows


def get_pipeline(pipeline_name: str) -> Optional[SyncPipeline]:
    """Return a single pipeline row by name (detached from session)."""
    with session_scope() as session:
        row = session.query(SyncPipeline).filter_by(pipeline_name=pipeline_name).first()
        if row:
            session.expunge(row)
        return row


def resolve_pipeline_class(dotted_path: str) -> type:
    """Import and return a BasePipeline subclass by its fully-qualified dotted path.

    Example:
        resolve_pipeline_class('sync_service.pipelines.reservations.ReservationsPipeline')
    """
    module_path, _, class_name = dotted_path.rpartition('.')
    if not module_path:
        raise ValueError(f"Invalid pipeline_class (not fully qualified): {dotted_path}")

    module = importlib.import_module(module_path)
    cls = getattr(module, class_name, None)
    if cls is None:
        raise ImportError(f"Class {class_name} not found in {module_path}")

    if not issubclass(cls, BasePipeline):
        raise TypeError(f"{dotted_path} is not a BasePipeline subclass")

    return cls


def instantiate_pipeline(row: SyncPipeline) -> BasePipeline:
    """Build a pipeline instance from a registry row."""
    cls = resolve_pipeline_class(row.pipeline_class)
    return cls(row)
