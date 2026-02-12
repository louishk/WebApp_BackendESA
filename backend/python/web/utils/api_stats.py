"""
API statistics middleware for tracking endpoint consumption.

Records API calls asynchronously using a background writer thread
to avoid adding latency to request handling.
"""

import time
import threading
import logging
from queue import Queue, Empty
from datetime import datetime

from flask import request, g

logger = logging.getLogger(__name__)

# Background queue for async DB writes
_stats_queue = Queue(maxsize=10000)
_writer_thread = None
_writer_stop = threading.Event()


def _get_client_ip():
    """Get client IP, handling proxies."""
    if request.headers.get('X-Forwarded-For'):
        return request.headers.get('X-Forwarded-For').split(',')[0].strip()
    return request.remote_addr or 'unknown'


def _stats_writer(app):
    """Background thread that flushes stats from queue to DB in batches."""
    batch = []
    flush_interval = 5  # seconds

    while not _writer_stop.is_set():
        # Drain queue into batch
        try:
            while len(batch) < 100:
                item = _stats_queue.get(timeout=flush_interval)
                batch.append(item)
        except Empty:
            pass

        if not batch:
            continue

        # Flush batch to DB
        try:
            with app.app_context():
                from web.models.api_statistic import ApiStatistic
                session = app.get_db_session()
                try:
                    for item in batch:
                        stat = ApiStatistic(
                            endpoint=item['endpoint'],
                            method=item['method'],
                            status_code=item['status_code'],
                            response_time_ms=item['response_time_ms'],
                            client_ip=item['client_ip'],
                            user_agent=item['user_agent'],
                            request_size=item['request_size'],
                            response_size=item['response_size'],
                            called_at=item['called_at'],
                        )
                        session.add(stat)
                    session.commit()
                except Exception as e:
                    logger.error(f"Failed to write API stats batch: {e}")
                    session.rollback()
                finally:
                    session.close()
        except Exception as e:
            logger.error(f"API stats writer error: {e}")

        batch.clear()

    # Final flush on shutdown
    if batch:
        try:
            with app.app_context():
                session = app.get_db_session()
                try:
                    from web.models.api_statistic import ApiStatistic
                    for item in batch:
                        stat = ApiStatistic(
                            endpoint=item['endpoint'],
                            method=item['method'],
                            status_code=item['status_code'],
                            response_time_ms=item['response_time_ms'],
                            client_ip=item['client_ip'],
                            user_agent=item['user_agent'],
                            request_size=item['request_size'],
                            response_size=item['response_size'],
                            called_at=item['called_at'],
                        )
                        session.add(stat)
                    session.commit()
                except Exception:
                    session.rollback()
                finally:
                    session.close()
        except Exception:
            pass


def init_api_stats(app):
    """
    Initialize API statistics tracking on a Flask app.

    Creates the api_statistics table if it doesn't exist, then registers
    before_request/after_request hooks that measure response time
    and enqueue stats for background DB writing. Only tracks /api/ routes.
    """
    global _writer_thread

    # Ensure table exists
    try:
        from web.models.api_statistic import ApiStatistic
        from sqlalchemy import create_engine
        engine = create_engine(app.db_url)
        ApiStatistic.__table__.create(engine, checkfirst=True)
        engine.dispose()
        logger.info("API statistics table verified")
    except Exception as e:
        logger.warning(f"Could not verify api_statistics table: {e}")

    # Start background writer thread
    _writer_stop.clear()
    _writer_thread = threading.Thread(
        target=_stats_writer, args=(app,), daemon=True, name='api-stats-writer'
    )
    _writer_thread.start()
    logger.info("API statistics writer started")

    @app.before_request
    def _stats_before():
        if request.path.startswith('/api/'):
            g._stats_start_time = time.monotonic()

    @app.after_request
    def _stats_after(response):
        start = getattr(g, '_stats_start_time', None)
        if start is None:
            return response

        elapsed_ms = (time.monotonic() - start) * 1000

        # Skip the statistics endpoints themselves to avoid self-referential noise
        if request.path.startswith('/api/statistics'):
            return response

        try:
            stat_record = {
                'endpoint': request.path,
                'method': request.method,
                'status_code': response.status_code,
                'response_time_ms': round(elapsed_ms, 2),
                'client_ip': _get_client_ip(),
                'user_agent': (request.user_agent.string or '')[:255],
                'request_size': request.content_length or 0,
                'response_size': response.content_length or 0,
                'called_at': datetime.utcnow(),
            }
            _stats_queue.put_nowait(stat_record)
        except Exception:
            # Never let stats tracking break a real request
            pass

        return response
