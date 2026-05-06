"""
sync_service — Standalone, on-demand ETL orchestrator.

Independent from scheduler/ and sync/. Provides:
  - On-demand pipeline execution with scope (site, entity, date range)
  - Per-scope freshness checks (query target tables directly)
  - In-flight deduplication + per-pipeline concurrency limits
  - Parallel execution with shared resource pool (SOAP/HTTP/DB slots)
  - Scheduled execution via its own internal scheduler
  - REST API for middleware/chatbot callers

Only depends on common/ (config_loader, db_secrets_vault, soap_client) —
truly shared infrastructure. No imports from scheduler/, sync/, datalayer/,
or web/routes/.
"""

__version__ = '0.1.0'
