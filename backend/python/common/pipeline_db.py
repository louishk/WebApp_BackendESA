"""
Reusable database context for pipeline scripts.

Centralizes the engine + session + upsert boilerplate that is repeated
across 19+ datalayer pipeline scripts. Opt-in — existing scripts continue
to work without changes.

Usage:
    from common.pipeline_db import PipelineDB

    with PipelineDB(config) as db:
        db.ensure_table(MyModel)
        count = db.push(MyModel, records, constraint_columns=['id'])
"""

import logging
from typing import Type, List, Dict, Any, Optional, Callable

from .config import DataLayerConfig
from .engine import create_engine_from_config
from .session import SessionManager
from .operations import UpsertOperations

logger = logging.getLogger(__name__)


class PipelineDB:
    """
    Reusable DB context for pipeline scripts.

    Holds a single engine for the lifetime of the pipeline run,
    avoiding repeated engine/pool creation on every push call.

    Supports context manager for auto-dispose:
        with PipelineDB(config) as db:
            db.push(Model, records, ['id'])
    """

    def __init__(self, config: DataLayerConfig, db_key: str = 'postgresql'):
        self.db_config = config.databases.get(db_key)
        if not self.db_config:
            raise ValueError(f"Database configuration '{db_key}' not found")

        self.engine = create_engine_from_config(self.db_config)
        self._session_manager = SessionManager(self.engine)

    def ensure_table(self, model_class: Type, base=None):
        """
        Create table if it doesn't exist.

        Args:
            model_class: SQLAlchemy model class
            base: Declarative base (defaults to model's metadata)
        """
        if base is not None:
            base.metadata.create_all(self.engine, tables=[model_class.__table__])
        else:
            model_class.__table__.create(self.engine, checkfirst=True)

    def push(
        self,
        model_class: Type,
        records: List[Dict[str, Any]],
        constraint_columns: List[str],
        chunk_size: int = 500,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> int:
        """
        Upsert records with chunking. Returns count processed.

        Args:
            model_class: SQLAlchemy model class
            records: List of record dicts
            constraint_columns: Columns that determine uniqueness
            chunk_size: Records per upsert chunk
            progress_callback: Optional callable(processed_so_far, total)

        Returns:
            Number of records processed
        """
        if not records:
            return 0

        total = len(records)
        total_processed = 0

        with self._session_manager.session_scope() as session:
            upsert_ops = UpsertOperations(session, self.db_config.db_type)

            for i in range(0, total, chunk_size):
                chunk = records[i:i + chunk_size]

                upsert_ops.upsert_batch(
                    model=model_class,
                    records=chunk,
                    constraint_columns=constraint_columns,
                    chunk_size=chunk_size,
                )

                total_processed += len(chunk)

                if progress_callback:
                    progress_callback(total_processed, total)

        return total_processed

    def session_scope(self):
        """
        Expose session_scope for scripts needing custom operations
        (e.g., delete-before-upsert, complex queries).
        """
        return self._session_manager.session_scope()

    def dispose(self):
        """Clean up engine connections."""
        self.engine.dispose()
        logger.debug("PipelineDB engine disposed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()
        return False
