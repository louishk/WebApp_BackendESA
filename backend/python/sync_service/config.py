"""sync_service DB session management.

Thin delegator over common.db so the orchestrator shares the same engine
pool and pool config as the rest of the backend.
"""

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from common.db import (
    dispose_all,
    get_engine as _get_engine,
    get_session as _get_session,
    session_scope as _session_scope,
)


def get_engine(database: str = 'middleware') -> Engine:
    """Cached engine for the given database. See common.db.get_engine."""
    return _get_engine(database)


def get_session_factory(database: str = 'middleware') -> sessionmaker:
    """Return a sessionmaker bound to the cached engine.

    Kept for backwards compatibility with sync_service callers; prefer
    `session_scope()` for new code.
    """
    return sessionmaker(bind=get_engine(database))


@contextmanager
def session_scope(database: str = 'middleware') -> Iterator[Session]:
    """Transactional context — commit/rollback/close handled."""
    with _session_scope(database) as session:
        yield session


__all__ = ['get_engine', 'get_session_factory', 'session_scope', 'dispose_all']
