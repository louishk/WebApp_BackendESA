"""
Outbound API call statistics tracker.

Works in two modes:
1. Flask mode: Uses app_context and app.get_db_session()
2. Standalone mode: Creates its own SQLAlchemy engine from config_loader

Provides:
- track_outbound_api() decorator for wrapping client methods
- record_outbound_call() function for manual recording
- init_outbound_stats() for Flask context
- init_outbound_stats_standalone() for scheduler/CLI usage
"""

import re
import time
import atexit
import threading
import logging
import functools
from queue import Queue, Empty
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

_outbound_queue = Queue(maxsize=10000)
_writer_thread = None
_writer_stop = threading.Event()
_flask_app = None
_standalone_engine = None


def _create_stat_object(item):
    """Create an ExternalApiStatistic ORM object from a dict."""
    from web.models.external_api_statistic import ExternalApiStatistic
    return ExternalApiStatistic(
        service_name=item['service_name'],
        endpoint=item['endpoint'],
        method=item['method'],
        status_code=item.get('status_code'),
        response_time_ms=item['response_time_ms'],
        request_size=item.get('request_size'),
        response_size=item.get('response_size'),
        success=item.get('success', True),
        error_message=item.get('error_message'),
        caller=item.get('caller'),
        called_at=item['called_at'],
    )


def _flush_batch(batch):
    """Flush a batch of stat records to DB, adapting to context."""
    global _flask_app, _standalone_engine

    if _flask_app is not None:
        try:
            with _flask_app.app_context():
                session = _flask_app.get_db_session()
                try:
                    for item in batch:
                        session.add(_create_stat_object(item))
                    session.commit()
                except Exception as e:
                    logger.error(f"Failed to write outbound stats batch (flask): {e}")
                    session.rollback()
                finally:
                    session.close()
        except Exception as e:
            logger.error(f"Outbound stats flask context error: {e}")
    elif _standalone_engine is not None:
        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=_standalone_engine)
        session = Session()
        try:
            for item in batch:
                session.add(_create_stat_object(item))
            session.commit()
        except Exception as e:
            logger.error(f"Failed to write outbound stats batch (standalone): {e}")
            session.rollback()
        finally:
            session.close()
    else:
        logger.warning(f"Outbound stats: no DB context, dropping {len(batch)} records")


def _outbound_writer():
    """Background thread that flushes outbound stats from queue to DB."""
    batch = []
    flush_interval = 5

    while not _writer_stop.is_set():
        try:
            while len(batch) < 100:
                item = _outbound_queue.get(timeout=flush_interval)
                batch.append(item)
        except Empty:
            pass

        if batch:
            _flush_batch(batch)
            batch.clear()

    # Final flush on shutdown - drain queue and flush everything
    try:
        while not _outbound_queue.empty():
            batch.append(_outbound_queue.get_nowait())
    except Empty:
        pass
    if batch:
        _flush_batch(batch)


def _flush_on_exit():
    """Flush remaining records before process exit (atexit handler).

    Signals the writer thread to stop and waits for it to complete
    its final flush of any batched items.
    """
    global _writer_thread
    if _writer_thread is None:
        return

    # Signal writer to stop and wait for final flush
    _writer_stop.set()
    _writer_thread.join(timeout=10)

    # Drain anything still in the queue (safety net)
    remaining = []
    try:
        while not _outbound_queue.empty():
            remaining.append(_outbound_queue.get_nowait())
    except Exception:
        pass

    if remaining:
        _flush_batch(remaining)


def _start_writer():
    """Start the background writer thread if not already running."""
    global _writer_thread
    if _writer_thread is not None and _writer_thread.is_alive():
        return

    _writer_stop.clear()
    _writer_thread = threading.Thread(
        target=_outbound_writer, daemon=True, name='outbound-stats-writer'
    )
    _writer_thread.start()
    atexit.register(_flush_on_exit)
    logger.info("Outbound API statistics writer started")


def init_outbound_stats(app):
    """Initialize outbound stats tracking in Flask context."""
    global _flask_app
    _flask_app = app

    try:
        from web.models.external_api_statistic import ExternalApiStatistic
        from sqlalchemy import create_engine
        engine = create_engine(app.db_url)
        ExternalApiStatistic.__table__.create(engine, checkfirst=True)
        engine.dispose()
        logger.info("External API statistics table verified")
    except Exception as e:
        logger.warning(f"Could not verify external_api_statistics table: {e}")

    _start_writer()


def init_outbound_stats_standalone():
    """Initialize outbound stats tracking outside Flask (scheduler, CLI)."""
    global _standalone_engine

    try:
        from common.config_loader import get_database_url
        from sqlalchemy import create_engine
        db_url = get_database_url('backend')
        _standalone_engine = create_engine(db_url)

        from web.models.external_api_statistic import ExternalApiStatistic
        ExternalApiStatistic.__table__.create(_standalone_engine, checkfirst=True)
        logger.info("External API statistics table verified (standalone)")
    except Exception as e:
        logger.warning(f"Could not init standalone outbound stats: {e}")

    _start_writer()


def _sanitize_error(msg):
    """Strip credentials/tokens from error messages before storage."""
    if not msg:
        return None
    s = str(msg)
    s = re.sub(r':::[A-Za-z0-9]{8,}', ':::***', s)
    s = re.sub(r'Bearer [A-Za-z0-9\-._~+/]+=*', 'Bearer ***', s)
    s = re.sub(r'(postgresql|mysql|mariadb)://[^@]+@', r'\1://***@', s)
    return s[:500]


def record_outbound_call(
    service_name: str,
    endpoint: str,
    method: str,
    status_code: Optional[int],
    response_time_ms: float,
    request_size: Optional[int] = None,
    response_size: Optional[int] = None,
    success: bool = True,
    error_message: Optional[str] = None,
    caller: Optional[str] = None,
):
    """
    Record an outbound API call. Use for ad-hoc calls not covered by the decorator.

    Example:
        start = time.monotonic()
        resp = requests.post(url, json=payload)
        record_outbound_call(
            service_name="slack", endpoint=url, method="POST",
            status_code=resp.status_code,
            response_time_ms=(time.monotonic() - start) * 1000,
        )
    """
    # Auto-init if writer not started
    if _writer_thread is None or not _writer_thread.is_alive():
        if _flask_app is None and _standalone_engine is None:
            try:
                init_outbound_stats_standalone()
            except Exception as e:
                logger.warning(f"Outbound stats auto-init failed: {e}")

    try:
        _outbound_queue.put_nowait({
            'service_name': service_name,
            'endpoint': (endpoint or 'unknown')[:500],
            'method': (method or 'UNKNOWN').upper(),
            'status_code': status_code,
            'response_time_ms': round(response_time_ms, 2),
            'request_size': request_size,
            'response_size': response_size,
            'success': success,
            'error_message': _sanitize_error(error_message),
            'caller': (str(caller)[:100]) if caller else None,
            'called_at': datetime.utcnow(),
        })
    except Exception:
        pass  # Never let stats tracking break real functionality


def track_outbound_api(service_name: str, endpoint_extractor=None):
    """
    Decorator for tracking outbound API calls on client methods.

    Args:
        service_name: e.g. "soap", "sugarcrm", "http"
        endpoint_extractor: Optional callable(args, kwargs) -> str
            to extract the endpoint/operation name from method arguments.

    Usage:
        class MyClient:
            @track_outbound_api(service_name="my_service")
            def request(self, method, url, **kwargs):
                return self.session.request(method, url, **kwargs)
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Extract endpoint
            if endpoint_extractor:
                endpoint = endpoint_extractor(args, kwargs)
            else:
                endpoint = _default_endpoint_extractor(func, args, kwargs)

            method = _extract_method(func, args, kwargs)

            start = time.monotonic()
            status_code = None
            request_size = None
            response_size = None
            success = True
            error_msg = None

            try:
                result = func(*args, **kwargs)

                # Extract status and sizes from requests.Response
                if hasattr(result, 'status_code'):
                    status_code = result.status_code
                if hasattr(result, 'request') and hasattr(result.request, 'body'):
                    body = result.request.body
                    request_size = len(body) if body else 0
                if hasattr(result, 'content'):
                    response_size = len(result.content)

                # Handle tuple returns like SugarCRM (data, error)
                if isinstance(result, tuple) and len(result) == 2:
                    _, err = result
                    if err is not None:
                        success = False
                        error_msg = _sanitize_error(err)

                return result

            except Exception as e:
                success = False
                error_msg = _sanitize_error(e)
                raise

            finally:
                elapsed_ms = (time.monotonic() - start) * 1000
                record_outbound_call(
                    service_name=service_name,
                    endpoint=endpoint or 'unknown',
                    method=method or 'UNKNOWN',
                    status_code=status_code,
                    response_time_ms=elapsed_ms,
                    request_size=request_size,
                    response_size=response_size,
                    success=success,
                    error_message=error_msg,
                )

        return wrapper
    return decorator


def _default_endpoint_extractor(func, args, kwargs):
    """Best-effort extraction of endpoint from method arguments."""
    for key in ('url', 'endpoint', 'operation', 'soap_action'):
        if key in kwargs:
            return str(kwargs[key])[:500]
    # Skip self (args[0]), try args[1]
    if len(args) > 1 and isinstance(args[1], str):
        return args[1][:500]
    return func.__name__


def _extract_method(func, args, kwargs):
    """Best-effort extraction of HTTP method from arguments."""
    if 'method' in kwargs:
        return str(kwargs['method']).upper()
    # SOAPClient always uses POST
    if 'call' in func.__name__.lower():
        return 'POST'
    # HTTPClient.request(self, method, url, ...)
    if func.__name__ == 'request' and len(args) > 1 and isinstance(args[1], str):
        return args[1].upper()
    return 'POST'
