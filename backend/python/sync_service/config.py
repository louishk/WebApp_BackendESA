"""
sync_service configuration and DB session management.

Relies only on common/config_loader.py for DB credentials.
Does NOT import from scheduler/, sync/, or web/routes/.
"""

import logging
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session

logger = logging.getLogger(__name__)

# Engine cache — one engine per database, created lazily
_engines: dict = {}
_session_factories: dict = {}


def get_engine(database: str = 'middleware') -> Engine:
    """Get or create a SQLAlchemy engine for the given database.

    Args:
        database: 'middleware' (esa_middleware — sync_service state, default),
                  'backend' (esa_backend), or 'pbi' (esa_pbi analytics)
    """
    global _engines
    if database not in _engines:
        from common.config_loader import get_database_url
        url = get_database_url(database)
        _engines[database] = create_engine(
            url,
            pool_size=5,
            max_overflow=5,
            pool_pre_ping=True,
            pool_recycle=1800,
        )
        logger.info(f"sync_service engine created for db={database}")
    return _engines[database]


def get_session_factory(database: str = 'middleware') -> sessionmaker:
    """Get or create a sessionmaker for the given database."""
    global _session_factories
    if database not in _session_factories:
        _session_factories[database] = sessionmaker(bind=get_engine(database))
    return _session_factories[database]


@contextmanager
def session_scope(database: str = 'middleware') -> Iterator[Session]:
    """Provide a transactional scope around a series of operations.

    Usage:
        with session_scope() as session:
            session.query(...)
    """
    SessionLocal = get_session_factory(database)
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def dispose_all():
    """Dispose all cached engines. Call during daemon shutdown."""
    global _engines, _session_factories
    for db, engine in _engines.items():
        try:
            engine.dispose()
            logger.info(f"sync_service engine disposed for db={db}")
        except Exception as e:
            logger.warning(f"Error disposing engine for {db}: {e}")
    _engines = {}
    _session_factories = {}
