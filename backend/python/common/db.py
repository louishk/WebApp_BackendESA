"""Canonical database connection module for the ESA backend.

Every subsystem (Flask web app, sync_service orchestrator, MCP server,
datalayer scripts) should obtain SQLAlchemy engines and sessions through
this module. One engine per (database, process), shared by all threads
in that process.

DEPLOYMENT CONSTRAINT — one instance of this module per OS process. The
engine cache is process-global, so co-mounting two security boundaries
(e.g., the MCP server and the Flask app) inside the same Gunicorn worker
would let them share connection pool slots and is NOT supported. The
current deployment runs them as separate systemd units (esa-backend,
backend-mcp, backend-orchestrator, backend-scheduler), which keeps each
in its own process and is the intended topology.

Usage:
    from common.db import get_session, session_scope, dispose_all

    # one-shot session — caller manages close
    s = get_session('pbi')
    try:
        rows = s.execute(text("SELECT 1")).fetchall()
    finally:
        s.close()

    # contextmanager — auto commit/rollback/close
    with session_scope('middleware') as s:
        s.execute(...)

    # on daemon shutdown
    dispose_all()

Pool config is centralized here. Override per-db if needed via
POOL_CONFIG below — kept tight so worst-case concurrent connections
across all subsystems stays under Azure PG's 90 usable slots.
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from common.config_loader import get_database_url

logger = logging.getLogger(__name__)

# Per-database pool tuning. pool_pre_ping is mandatory — Azure PG drops
# idle connections aggressively and stale-conn errors otherwise surface
# in callers. pool_recycle=300 keeps idle conns young enough that Azure
# never beats us to closing them.
POOL_CONFIG: dict[str, dict] = {
    'backend':    {'pool_size': 5, 'max_overflow': 10},
    'middleware': {'pool_size': 5, 'max_overflow': 10},
    'pbi':        {'pool_size': 5, 'max_overflow': 10},
}
_POOL_DEFAULTS = {
    'pool_pre_ping': True,
    'pool_recycle': 300,
}

_engines: dict[str, Engine] = {}
_session_factories: dict[str, sessionmaker] = {}
_lock = threading.Lock()


def _build_engine(db: str) -> Engine:
    url = get_database_url(db)
    cfg = {**_POOL_DEFAULTS, **POOL_CONFIG.get(db, {})}
    engine = create_engine(url, **cfg)
    logger.info(
        "common.db engine created db=%s pool_size=%s max_overflow=%s recycle=%ss",
        db, cfg['pool_size'], cfg['max_overflow'], cfg['pool_recycle'],
    )
    return engine


def get_engine(db: str = 'backend') -> Engine:
    """Return the cached engine for `db`. Thread-safe lazy init.

    db: 'backend' (esa_backend), 'middleware' (esa_middleware), 'pbi' (esa_pbi).
    """
    if db in _engines:
        return _engines[db]
    with _lock:
        if db not in _engines:
            engine = _build_engine(db)
            _engines[db] = engine
            _session_factories[db] = sessionmaker(bind=engine)
    return _engines[db]


def get_session(db: str = 'backend') -> Session:
    """Return a new Session bound to the cached engine for `db`.

    Caller is responsible for `session.close()` — prefer `session_scope()`
    when a contextmanager fits.
    """
    if db not in _session_factories:
        get_engine(db)
    return _session_factories[db]()


@contextmanager
def session_scope(db: str = 'backend') -> Iterator[Session]:
    """Transactional context: commits on success, rolls back on exception, always closes.

    with session_scope('pbi') as s:
        s.execute(...)
    """
    session = get_session(db)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def dispose_all() -> None:
    """Dispose every cached engine. Call during daemon shutdown."""
    with _lock:
        for db, engine in list(_engines.items()):
            try:
                engine.dispose()
                logger.info("common.db engine disposed db=%s", db)
            except Exception as e:
                logger.warning("common.db dispose failed db=%s err=%s", db, e)
        _engines.clear()
        _session_factories.clear()
