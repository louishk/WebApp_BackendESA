"""
Session management for database operations with context managers.
Provides transaction safety with automatic commit/rollback.
"""

import logging
from contextlib import contextmanager
from typing import Generator
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.engine import Engine


logger = logging.getLogger(__name__)


class SessionManager:
    """
    Manages database sessions with automatic transaction handling.

    Features:
    - Context manager for session lifecycle
    - Automatic commit on success
    - Automatic rollback on error
    - Proper resource cleanup
    """

    def __init__(self, engine: Engine):
        """
        Initialize session manager.

        Args:
            engine: SQLAlchemy engine
        """
        self.engine = engine
        self.Session = sessionmaker(bind=engine, expire_on_commit=False)
        logger.debug("Session manager initialized")

    @contextmanager
    def session_scope(self) -> Generator[Session, None, None]:
        """
        Provide a transactional scope for database operations.

        Yields:
            Session: SQLAlchemy session

        Example:
            with session_manager.session_scope() as session:
                # Use session for queries and operations
                user = session.query(User).filter_by(id=1).first()
                # Auto-commit on success, auto-rollback on exception

        Note:
            - Commits automatically on successful completion
            - Rolls back automatically on exception
            - Closes session in all cases
        """
        session = self.Session()
        try:
            yield session
            session.commit()
            logger.debug("Session committed successfully")

        except Exception as e:
            session.rollback()
            logger.error(f"Session rolled back due to error: {e}")
            raise

        finally:
            session.close()
            logger.debug("Session closed")

    def get_session(self) -> Session:
        """
        Get a new session (manual transaction management required).

        Returns:
            Session: SQLAlchemy session

        Warning:
            Caller is responsible for commit/rollback/close.
            Prefer using session_scope() context manager instead.
        """
        return self.Session()
