"""
Database-specific upsert strategies using the Strategy pattern.
Handles differences in upsert syntax across PostgreSQL, MariaDB, and Azure SQL.
"""

import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Type
from sqlalchemy.orm import Session
from sqlalchemy.dialects import postgresql, mysql
from sqlalchemy import insert

from .config import DatabaseType


logger = logging.getLogger(__name__)


class UpsertStrategy(ABC):
    """Abstract base class for database-specific upsert strategies"""

    @abstractmethod
    def upsert(
        self,
        session: Session,
        model: Type,
        values: Dict[str, Any],
        constraint_columns: List[str]
    ) -> None:
        """
        Upsert single record.

        Args:
            session: SQLAlchemy session
            model: SQLAlchemy model class
            values: Dictionary of column name -> value
            constraint_columns: Columns that determine uniqueness (for conflict resolution)
        """
        pass

    @abstractmethod
    def bulk_upsert(
        self,
        session: Session,
        model: Type,
        values_list: List[Dict[str, Any]],
        constraint_columns: List[str]
    ) -> None:
        """
        Bulk upsert multiple records.

        Args:
            session: SQLAlchemy session
            model: SQLAlchemy model class
            values_list: List of dictionaries (each dict is one record)
            constraint_columns: Columns that determine uniqueness
        """
        pass


class PostgreSQLUpsertStrategy(UpsertStrategy):
    """
    PostgreSQL upsert using ON CONFLICT ... DO UPDATE.

    Syntax:
        INSERT INTO table (col1, col2) VALUES (:val1, :val2)
        ON CONFLICT (col1) DO UPDATE SET col2 = EXCLUDED.col2
    """

    # Columns to exclude from UPDATE SET (auto-managed by database/ORM)
    EXCLUDED_UPDATE_COLUMNS = {'created_at', 'updated_at'}

    def upsert(
        self,
        session: Session,
        model: Type,
        values: Dict[str, Any],
        constraint_columns: List[str]
    ) -> None:
        stmt = postgresql.insert(model).values(**values)
        excluded_cols = set(constraint_columns) | self.EXCLUDED_UPDATE_COLUMNS
        stmt = stmt.on_conflict_do_update(
            index_elements=constraint_columns,
            set_={k: v for k, v in values.items() if k not in excluded_cols}
        )
        session.execute(stmt)
        logger.debug(f"PostgreSQL upsert: {model.__tablename__}")

    def bulk_upsert(
        self,
        session: Session,
        model: Type,
        values_list: List[Dict[str, Any]],
        constraint_columns: List[str]
    ) -> None:
        if not values_list:
            return

        # Filter out auto-managed timestamp columns from values
        # These should be handled by database defaults, not passed in
        filtered_values = [
            {k: v for k, v in record.items() if k not in self.EXCLUDED_UPDATE_COLUMNS}
            for record in values_list
        ]

        # Use PostgreSQL insert with on_conflict
        stmt = postgresql.insert(model).values(filtered_values)

        # Build update dictionary using EXCLUDED (PostgreSQL special table)
        # Exclude constraint columns and auto-managed timestamp columns
        excluded_cols = set(constraint_columns) | self.EXCLUDED_UPDATE_COLUMNS
        update_dict = {
            k: stmt.excluded[k]
            for k in filtered_values[0].keys()
            if k not in excluded_cols
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=constraint_columns,
            set_=update_dict
        )

        session.execute(stmt)
        logger.debug(f"PostgreSQL bulk upsert: {len(values_list)} records into {model.__tablename__}")


class MariaDBUpsertStrategy(UpsertStrategy):
    """
    MariaDB/MySQL upsert using ON DUPLICATE KEY UPDATE.

    Syntax:
        INSERT INTO table (col1, col2) VALUES (:val1, :val2)
        ON DUPLICATE KEY UPDATE col2 = VALUES(col2)
    """

    def upsert(
        self,
        session: Session,
        model: Type,
        values: Dict[str, Any],
        constraint_columns: List[str]
    ) -> None:
        stmt = insert(model).values(**values)

        # Build update dictionary
        update_dict = {k: v for k, v in values.items() if k not in constraint_columns}

        stmt = stmt.on_duplicate_key_update(**update_dict)
        session.execute(stmt)
        logger.debug(f"MariaDB upsert: {model.__tablename__}")

    def bulk_upsert(
        self,
        session: Session,
        model: Type,
        values_list: List[Dict[str, Any]],
        constraint_columns: List[str]
    ) -> None:
        if not values_list:
            return

        stmt = insert(model).values(values_list)

        # Build update dictionary using inserted values
        update_dict = {
            k: stmt.inserted[k]
            for k in values_list[0].keys()
            if k not in constraint_columns
        }

        stmt = stmt.on_duplicate_key_update(**update_dict)
        session.execute(stmt)
        logger.debug(f"MariaDB bulk upsert: {len(values_list)} records into {model.__tablename__}")


class AzureSQLUpsertStrategy(UpsertStrategy):
    """
    Azure SQL Server upsert using MERGE statement.

    Note: SQLAlchemy doesn't have built-in support for SQL Server MERGE,
    so we use a simpler approach with INSERT or UPDATE logic.
    """

    def upsert(
        self,
        session: Session,
        model: Type,
        values: Dict[str, Any],
        constraint_columns: List[str]
    ) -> None:
        # Build WHERE clause for checking existence
        where_clause = {k: values[k] for k in constraint_columns}

        # Try to find existing record
        existing = session.query(model).filter_by(**where_clause).first()

        if existing:
            # Update existing record
            for key, value in values.items():
                if key not in constraint_columns:
                    setattr(existing, key, value)
            logger.debug(f"Azure SQL update: {model.__tablename__}")
        else:
            # Insert new record
            new_record = model(**values)
            session.add(new_record)
            logger.debug(f"Azure SQL insert: {model.__tablename__}")

    def bulk_upsert(
        self,
        session: Session,
        model: Type,
        values_list: List[Dict[str, Any]],
        constraint_columns: List[str]
    ) -> None:
        if not values_list:
            return

        # For Azure SQL, we use a simple approach: individual upserts
        # In production, consider using bulk_insert_mappings with additional logic
        for values in values_list:
            self.upsert(session, model, values, constraint_columns)

        logger.debug(f"Azure SQL bulk upsert: {len(values_list)} records into {model.__tablename__}")


class UpsertFactory:
    """Factory for creating database-specific upsert strategies"""

    _strategies = {
        DatabaseType.POSTGRESQL: PostgreSQLUpsertStrategy(),
        DatabaseType.MARIADB: MariaDBUpsertStrategy(),
        DatabaseType.AZURE_SQL: AzureSQLUpsertStrategy(),
    }

    @classmethod
    def get_strategy(cls, db_type: DatabaseType) -> UpsertStrategy:
        """
        Get upsert strategy for database type.

        Args:
            db_type: Database type

        Returns:
            UpsertStrategy: Database-specific upsert strategy

        Raises:
            ValueError: If database type is unsupported
        """
        strategy = cls._strategies.get(db_type)

        if strategy is None:
            raise ValueError(
                f"Unsupported database type for upsert: {db_type}. "
                f"Supported types: {', '.join([t.value for t in DatabaseType])}"
            )

        return strategy


def delete_current_month_records(
    session: Session,
    model: Type,
    year: int,
    month: int
) -> int:
    """
    Delete all records for the specified month before re-inserting fresh data.

    This ensures that daily extracts for the current month replace
    (not accumulate) previous records. Historical months should NOT
    use this function - they should continue using upsert.

    Args:
        session: SQLAlchemy session
        model: SQLAlchemy model class (RentRoll, Discount, etc.)
        year: Target year
        month: Target month (1-12)

    Returns:
        Number of deleted records
    """
    from .date_utils import get_first_day_of_month, get_last_day_of_month

    first_day = get_first_day_of_month(year, month)
    last_day = get_last_day_of_month(year, month)

    # Delete all records where extract_date falls within the month
    deleted = session.query(model).filter(
        model.extract_date >= first_day,
        model.extract_date <= last_day
    ).delete(synchronize_session=False)

    logger.info(f"Deleted {deleted} records from {model.__tablename__} for {year}-{month:02d}")

    return deleted
