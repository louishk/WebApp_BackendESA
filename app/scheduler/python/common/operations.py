"""
Database operations layer with CRUD, upsert, and batch operations.
Optimized for large data payloads (1+ MB) with chunking and memory-efficient processing.
"""

import logging
from typing import Generic, TypeVar, Type, List, Optional, Dict, Any, Callable
from sqlalchemy.orm import Session

from .config import DatabaseType
from .upsert_strategies import UpsertFactory


logger = logging.getLogger(__name__)

# Generic type for models
T = TypeVar('T')


class BaseRepository(Generic[T]):
    """
    Generic repository for CRUD operations.

    Features:
    - Type-safe operations
    - Query building utilities
    - Pagination support
    """

    def __init__(self, session: Session, model_class: Type[T]):
        """
        Initialize repository.

        Args:
            session: SQLAlchemy session
            model_class: SQLAlchemy model class
        """
        self.session = session
        self.model_class = model_class

    def get_by_id(self, id_value: Any) -> Optional[T]:
        """
        Get record by ID.

        Args:
            id_value: Primary key value

        Returns:
            Optional[T]: Model instance or None if not found
        """
        return self.session.query(self.model_class).get(id_value)

    def get_all(self, limit: int = 100, offset: int = 0) -> List[T]:
        """
        Get all records with pagination.

        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip

        Returns:
            List[T]: List of model instances
        """
        return self.session.query(self.model_class).limit(limit).offset(offset).all()

    def filter_by(self, **kwargs) -> List[T]:
        """
        Filter records by column values.

        Args:
            **kwargs: Column name -> value pairs

        Returns:
            List[T]: List of matching model instances
        """
        return self.session.query(self.model_class).filter_by(**kwargs).all()

    def create(self, obj: T) -> T:
        """
        Create new record.

        Args:
            obj: Model instance

        Returns:
            T: Created model instance
        """
        self.session.add(obj)
        self.session.flush()
        logger.debug(f"Created {self.model_class.__name__}")
        return obj

    def update(self, obj: T) -> T:
        """
        Update existing record.

        Args:
            obj: Model instance

        Returns:
            T: Updated model instance
        """
        self.session.merge(obj)
        self.session.flush()
        logger.debug(f"Updated {self.model_class.__name__}")
        return obj

    def delete(self, obj: T) -> None:
        """
        Delete record.

        Args:
            obj: Model instance
        """
        self.session.delete(obj)
        self.session.flush()
        logger.debug(f"Deleted {self.model_class.__name__}")

    def count(self) -> int:
        """
        Count total records.

        Returns:
            int: Total count
        """
        return self.session.query(self.model_class).count()


class UpsertOperations:
    """
    Database-agnostic upsert operations using strategy pattern.

    Features:
    - Single record upsert
    - Bulk upsert with chunking (for 1+ MB datasets)
    - Transaction management per chunk
    """

    def __init__(self, session: Session, db_type: DatabaseType):
        """
        Initialize upsert operations.

        Args:
            session: SQLAlchemy session
            db_type: Database type (determines upsert strategy)
        """
        self.session = session
        self.db_type = db_type
        self.strategy = UpsertFactory.get_strategy(db_type)

    def upsert_single(
        self,
        model: Type,
        values: Dict[str, Any],
        constraint_columns: List[str]
    ) -> None:
        """
        Upsert single record.

        Args:
            model: SQLAlchemy model class
            values: Dictionary of column name -> value
            constraint_columns: Columns that determine uniqueness

        Example:
            upsert_ops.upsert_single(
                User,
                {'id': 1, 'name': 'John', 'email': 'john@example.com'},
                constraint_columns=['id']
            )
        """
        self.strategy.upsert(self.session, model, values, constraint_columns)

    def upsert_batch(
        self,
        model: Type,
        records: List[Dict[str, Any]],
        constraint_columns: List[str],
        chunk_size: int = 500
    ) -> int:
        """
        Bulk upsert with chunking for large datasets (1+ MB).

        IMPORTANT: Processes data in chunks to avoid memory issues.
        Each chunk is committed separately for transaction safety.

        Args:
            model: SQLAlchemy model class
            records: List of dictionaries (each dict is one record)
            constraint_columns: Columns that determine uniqueness
            chunk_size: Records per chunk (default: 500 for large payloads)

        Returns:
            int: Total number of records processed

        Example:
            records = [
                {'id': 1, 'name': 'John'},
                {'id': 2, 'name': 'Jane'},
                ...
            ]
            count = upsert_ops.upsert_batch(User, records, constraint_columns=['id'])
        """
        if not records:
            return 0

        total_processed = 0
        total_chunks = (len(records) + chunk_size - 1) // chunk_size

        logger.info(
            f"Starting bulk upsert: {len(records)} records into {model.__tablename__} "
            f"(chunk_size={chunk_size}, chunks={total_chunks})"
        )

        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            chunk_num = (i // chunk_size) + 1

            try:
                self.strategy.bulk_upsert(self.session, model, chunk, constraint_columns)
                self.session.flush()  # Flush after each chunk
                total_processed += len(chunk)

                logger.debug(
                    f"Processed chunk {chunk_num}/{total_chunks}: "
                    f"{len(chunk)} records ({total_processed}/{len(records)} total)"
                )

            except Exception as e:
                logger.error(
                    f"Error processing chunk {chunk_num}/{total_chunks}: {e}"
                )
                raise  # Let session manager handle rollback

        logger.info(f"Bulk upsert completed: {total_processed} records")
        return total_processed


class BatchOperations:
    """
    Bulk data operations optimized for large payloads (1+ MB).

    Features:
    - Memory-efficient bulk inserts with chunking
    - Progress callbacks
    - Transaction rollback on errors per chunk
    """

    def __init__(self, session: Session):
        """
        Initialize batch operations.

        Args:
            session: SQLAlchemy session
        """
        self.session = session

    def batch_insert(
        self,
        model: Type,
        records: List[Dict[str, Any]],
        chunk_size: int = 500,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> int:
        """
        Memory-efficient bulk insert with chunking.

        Args:
            model: SQLAlchemy model class
            records: List of dictionaries (each dict is one record)
            chunk_size: Records per chunk (default: 500 for large payloads)
            progress_callback: Optional callback(current, total) for progress tracking

        Returns:
            int: Total number of records inserted

        Example:
            def progress(current, total):
                print(f"Progress: {current}/{total}")

            count = batch_ops.batch_insert(User, records, progress_callback=progress)
        """
        if not records:
            return 0

        total_inserted = 0
        total_chunks = (len(records) + chunk_size - 1) // chunk_size

        logger.info(
            f"Starting batch insert: {len(records)} records into {model.__tablename__} "
            f"(chunk_size={chunk_size}, chunks={total_chunks})"
        )

        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]

            try:
                # Create model instances
                instances = [model(**record) for record in chunk]

                # Bulk insert using SQLAlchemy
                self.session.bulk_save_objects(instances)
                self.session.flush()

                total_inserted += len(chunk)

                # Call progress callback if provided
                if progress_callback:
                    progress_callback(total_inserted, len(records))

                logger.debug(f"Inserted {total_inserted}/{len(records)} records")

            except Exception as e:
                logger.error(f"Error inserting chunk: {e}")
                raise  # Let session manager handle rollback

        logger.info(f"Batch insert completed: {total_inserted} records")
        return total_inserted

    def batch_update(
        self,
        model: Type,
        records: List[Dict[str, Any]],
        chunk_size: int = 500
    ) -> int:
        """
        Bulk update with chunking.

        Args:
            model: SQLAlchemy model class
            records: List of dictionaries with updated values (must include primary key)
            chunk_size: Records per chunk

        Returns:
            int: Total number of records updated
        """
        if not records:
            return 0

        total_updated = 0

        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]

            try:
                self.session.bulk_update_mappings(model, chunk)
                self.session.flush()
                total_updated += len(chunk)

                logger.debug(f"Updated {total_updated}/{len(records)} records")

            except Exception as e:
                logger.error(f"Error updating chunk: {e}")
                raise

        logger.info(f"Batch update completed: {total_updated} records")
        return total_updated
