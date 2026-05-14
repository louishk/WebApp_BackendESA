"""
sync_service — Standalone ETL orchestrator (cron + on-demand).

Provides:
  - Pipeline execution with scope (site, entity, date range)
  - Per-scope freshness checks (query target tables directly)
  - In-flight deduplication + per-pipeline concurrency limits
  - Parallel execution with shared resource pool (SOAP/HTTP/DB slots)
  - APScheduler-driven cron triggers from mw_sync_pipelines
  - REST API for middleware/chatbot callers

Only depends on common/ — no imports from web/routes/.
"""

__version__ = '0.1.0'
