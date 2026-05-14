# ESA Backend — Project Rules

Self-storage management platform for Extra Space Asia. Flask web app + sync orchestrator data pipelines, deployed on Azure VM.

## Tech Stack
- **Backend**: Python 3, Flask, SQLAlchemy ORM, Gunicorn (4 workers)
- **Frontend**: Jinja2 templates + vanilla JavaScript (no React/Vue/Angular)
- **Databases**: PostgreSQL — three databases (Azure Flex Server, **90 usable connection slots total** across all clients):
  - `esa_backend` — app data (users, roles, pages, API keys, discount plans, inventory mappings)
  - `esa_pbi` — analytics/reporting data (rent rolls, site info, units, ECRI, ledger charges)
  - `esa_middleware` — orchestrator runtime + ccws_* mirrors + mw_* domain tables
- **External APIs**: SOAP (StorageMaker/SMD CallCenterWs), SugarCRM REST, EmbedSocial, Azure AI Foundry, Google BigQuery
- **Auth**: Flask-Login (session) for web UI, JWT (HS256) for API endpoints, Microsoft OAuth SSO
- **Config**: YAML files in `backend/python/config/` resolved through `common/config_loader.py`, secrets from DB vault (`app_secrets` table in esa_backend)
- **Pipelines**: Orchestrator daemon (`backend/python/sync_service/`) — APScheduler in-process, pipelines defined in `mw_sync_pipelines` (esa_middleware DB). All pipeline logic lives in `sync_service/pipelines/*.py`. (The legacy `backend-scheduler` daemon was fully decommissioned 2026-05-14; ~27k LOC removed across PR A-G; see `pre-scheduler-decom-final` tag for revert reference.)

## Project Structure
```
backend/python/
  common/           # Shared: models, config_loader, db, soap_client, sugarcrm_client, db_secrets_vault, secrets_vault, http_client, cache_manager
  sync_service/     # Orchestrator daemon + all pipeline implementations
    pipelines/      # One file per pipeline (rentroll, discount, mimo, fx_rate, sugarcrm_leads, ccws_*, igloo, etc.)
  web/
    routes/         # Flask blueprints: api.py, auth.py, admin.py, admin_siteinfo.py, main.py, tools.py, discount_plans.py, ecri.py, api_keys.py, reservations.py, statistics.py, orchestrator_ui.py
    auth/           # jwt_auth.py, decorators.py, oauth.py, session_auth.py
    utils/          # audit.py, rate_limit.py, validators.py, translation.py, api_stats.py
    models/         # SQLAlchemy models: user.py, role.py, page.py, api_key.py, api_statistic.py, discount_plan.py, discount_plan_config.py, inventory.py
    templates/      # Jinja2: base.html, tools/, admin/, ecri/, orchestrator/, statistics/, pages/
  config/           # YAML: scheduler.yaml (pipeline overrides only — see below), alerts.yaml, llm.yaml, app.yaml, database.yaml, apis.yaml, mcp.yaml
  migrations/       # SQL + Python migration files (069+ since scheduler decom)
scripts/            # deploy_to_vm.py
sql/                # Raw SQL scripts
pages/              # CMS-style page content
mcp_esa/            # Independent MCP server (Streamable HTTP transport)
```

## Code Conventions

### Route Patterns
- Each route file creates a Blueprint: `bp = Blueprint('name', __name__, url_prefix='/prefix')`
- Web routes use `@login_required` + permission decorators from `web/auth/decorators.py`
- API routes use `@require_auth` (JWT) + `@require_api_scope('scope_name')` from `web/auth/jwt_auth.py`
- Rate limiting: `@rate_limit_api(max_per_minute=N)` or `@rate_limit_login()`
- DB session: `current_app.get_db_session()` / `get_middleware_session()` / `get_pbi_session()` — all delegate to `common.db` (see DB section below)
- API responses: `jsonify({"status": "success", "data": ...})` or `jsonify({"error": "message"})` with appropriate HTTP status

### Security Rules (enforced in past pentest reviews)
- NEVER leak `str(e)` in error responses — use generic messages, log the real error
- NEVER use f-strings or `.format()` for SQL — always use parameterized queries or ORM
- NEVER commit `.env`, vault files, or secrets
- Always validate/sanitize user input at route boundaries
- Use `secrets.token_urlsafe()` for token generation, never `random`
- Audit sensitive operations via `audit_log(AuditEvent.X, ...)`
- Use constant-time comparison for credential checks

### Frontend (Templates)
- Jinja2 templates extend `base.html`
- Tool pages (`templates/tools/`) are self-contained HTML with inline `<script>` — they fetch from `/api/` endpoints
- No build system or bundler — vanilla JS with fetch API
- Permission-gated: each tool has a decorator (e.g., `@billing_tools_access_required`)

### Models
- Base: `common/models.py` — shared domain models (RentRoll, SiteInfo, etc.) using `declarative_base()`
- App models: `web/models/` — user, role, page, api_key, discount_plan, inventory
- Mixins: `TimestampMixin` (created_at/updated_at), `SoftDeleteMixin`, `BaseModel.to_dict()`

### SOAP API
- Client: `common/soap_client.py` — auto-injects auth (sCorpCode, sCorpUserName:::APIKEY, sCorpPassword)
- Retry with exponential backoff, XML-to-dict parsing, namespace stripping
- Track outbound calls via `common/outbound_stats.py`

## Database Access Pattern

**Canonical module: `backend/python/common/db.py`** — one engine per (db, process), threadsafe lazy init, shared by all subsystems (Flask, orchestrator, MCP, datalayer scripts). Pool defaults: `pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=300`.

```python
# Flask routes (Web UI / API)
db = current_app.get_db_session()       # esa_backend
db = current_app.get_middleware_session()  # esa_middleware
db = current_app.get_pbi_session()      # esa_pbi
try:
    result = db.query(Model).filter_by(...).first()
finally:
    db.close()

# Anywhere else (daemons, scripts, common/, sync_service/, mcp_esa/)
from common.db import get_session, get_engine, session_scope

session = get_session('pbi')            # caller closes
with session_scope('middleware') as s:  # auto commit/rollback/close
    s.execute(...)
engine = get_engine('backend')          # for raw conn.execute / metadata.create_all
```

**Why this matters.** Azure Postgres has only 90 usable slots across all clients (4 gunicorn workers + orchestrator + MCP + scheduler + datalayer subprocesses). Fragmented per-blueprint pools previously caused connection exhaustion. The shared module keeps worst-case footprint well under the ceiling and gives every caller the same `pool_pre_ping`/`recycle` so Azure idle-close doesn't surprise us.

**Regression guard.** `backend/python/tests/test_db_module_canonical.py` fails CI if anyone reintroduces `create_engine(get_database_url(...))` outside `common/db.py`. Allowlist: `migrations/`, `scripts/` (one-shot CLI probes), tests.

## Deploy
- Script: `scripts/deploy_to_vm.py` (paramiko-based, 6-step rsync pipeline)
- VM: `20.6.132.108`, user `esa_bk_admin`, SSH key `~/.ssh/id_ed25519_vm`
- Services (3 systemd units): `esa-backend` (gunicorn, 4 workers), `backend-orchestrator` (sync_service daemon), `backend-mcp` (MCP HTTP server)
- No git on VM — uses rsync

## What NOT to Do
- Don't add React, Vue, TypeScript, or a build system — the frontend is intentionally vanilla
- Don't restructure the blueprint pattern or move routes around without asking
- Don't add new pip dependencies without discussing — the VM has no auto-install
- Don't use `print()` for logging — use `logging.getLogger(__name__)`
- **Don't call `create_engine(get_database_url(...))` directly** — use `common.db.get_engine()` / `get_session()` / `session_scope()`. Per-call engines fragment the pool and we already hit Azure's 90-slot ceiling once because of this. A regression test enforces it.
- Don't reintroduce a `datalayer/` directory or `scheduler/` subpackage — the orchestrator owns every pipeline now. New pipelines: add a class in `sync_service/pipelines/<name>.py` and a `migrations/mw_seed_<name>.py` row in `mw_sync_pipelines`.
- Don't add `pipelines.yaml` — schedule config lives in `mw_sync_pipelines` DB rows; per-pipeline runtime overrides (location_codes, sql_chunk_size, etc.) live in `config/scheduler.yaml` under the top-level `pipelines:` key.
