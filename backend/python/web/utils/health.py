"""
Shared health check probes for /health and /api/health endpoints.
"""

import logging
import time
from datetime import datetime, timezone

from flask import current_app
from sqlalchemy import text

logger = logging.getLogger(__name__)


def run_health_checks():
    """
    Run dependency probes and return (response_dict, http_status).

    Rules:
      - backend_db down  → status "unhealthy", HTTP 503
      - pbi_db down      → status "degraded",  HTTP 200
      - redis unavailable → no effect on overall status
    """
    checks = {}

    # ------------------------------------------------------------------
    # Backend DB probe
    # ------------------------------------------------------------------
    backend_ok = False
    try:
        session = current_app.get_db_session()
        try:
            t0 = time.monotonic()
            session.execute(text('SELECT 1'))
            latency_ms = round((time.monotonic() - t0) * 1000, 1)
            checks['backend_db'] = {'status': 'up', 'latency_ms': latency_ms}
            backend_ok = True
        finally:
            session.close()
    except Exception as e:
        logger.error("Health check: backend_db probe failed: %s", e)
        checks['backend_db'] = {'status': 'down'}

    # ------------------------------------------------------------------
    # PBI DB probe
    # ------------------------------------------------------------------
    pbi_ok = False
    try:
        from common.config_loader import get_database_url
        from sqlalchemy import create_engine

        # Lightweight probe engine with a short connect timeout
        pbi_url = get_database_url('pbi')
        probe_engine = create_engine(
            pbi_url,
            connect_args={'connect_timeout': 2},
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=False,
        )
        try:
            t0 = time.monotonic()
            with probe_engine.connect() as conn:
                conn.execute(text('SELECT 1'))
            latency_ms = round((time.monotonic() - t0) * 1000, 1)
            checks['pbi_db'] = {'status': 'up', 'latency_ms': latency_ms}
            pbi_ok = True
        finally:
            probe_engine.dispose()
    except Exception as e:
        logger.error("Health check: pbi_db probe failed: %s", e)
        checks['pbi_db'] = {'status': 'down'}

    # ------------------------------------------------------------------
    # Redis probe (optional — not currently deployed)
    # ------------------------------------------------------------------
    try:
        from common.config_loader import get_config
        redis_cfg = get_config().database.redis
        redis_enabled = getattr(redis_cfg, 'enabled', False) if redis_cfg else False
        redis_url = getattr(redis_cfg, 'url', None) if redis_cfg else None

        if redis_enabled and redis_url:
            import redis as _redis
            t0 = time.monotonic()
            r = _redis.from_url(redis_url, socket_connect_timeout=2, socket_timeout=2)
            r.ping()
            latency_ms = round((time.monotonic() - t0) * 1000, 1)
            checks['redis'] = {'status': 'up', 'latency_ms': latency_ms}
        else:
            checks['redis'] = {'status': 'unavailable'}
    except ImportError:
        checks['redis'] = {'status': 'unavailable'}
    except Exception as e:
        logger.warning("Health check: redis probe failed: %s", e)
        checks['redis'] = {'status': 'down'}

    # ------------------------------------------------------------------
    # Determine overall status
    # ------------------------------------------------------------------
    if not backend_ok:
        overall = 'unhealthy'
        http_status = 503
    elif not pbi_ok:
        overall = 'degraded'
        http_status = 200
    else:
        overall = 'healthy'
        http_status = 200

    body = {
        'status': overall,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'checks': checks,
    }
    return body, http_status
