# ESA Backend — Project Rules

Self-storage management platform for Extra Space Asia. Flask web app + APScheduler data pipelines, deployed on Azure VM.

## Tech Stack
- **Backend**: Python 3, Flask, SQLAlchemy ORM, Gunicorn (4 workers)
- **Frontend**: Jinja2 templates + vanilla JavaScript (no React/Vue/Angular)
- **Databases**: PostgreSQL — two databases:
  - `esa_backend` — app data (users, roles, pages, API keys, discount plans, inventory mappings)
  - `esa_pbi` — analytics/reporting data (rent rolls, site info, units, ECRI, ledger charges)
- **External APIs**: SOAP (StorageMaker/SMD CallCenterWs), SugarCRM REST, EmbedSocial, Azure AI Foundry, Google BigQuery
- **Auth**: Flask-Login (session) for web UI, JWT (HS256) for API endpoints, Microsoft OAuth SSO
- **Config**: YAML files in `backend/python/config/` resolved through `common/config_loader.py`, secrets from DB vault (`app_secrets` table in esa_backend)
- **Scheduler**: APScheduler with PostgreSQL job store, pipelines defined in `config/pipelines.yaml`

## Project Structure
```
backend/python/
  common/           # Shared: models, config_loader, soap_client, sugarcrm_client, db_secrets_vault, secrets_vault, http_client, cache_manager
  web/
    routes/         # Flask blueprints: api.py, auth.py, admin.py, admin_siteinfo.py, main.py, tools.py, discount_plans.py, ecri.py, api_keys.py, reservations.py, scheduler.py, statistics.py
    auth/           # jwt_auth.py, decorators.py, oauth.py, session_auth.py
    utils/          # audit.py, rate_limit.py, validators.py, translation.py, api_stats.py
    models/         # SQLAlchemy models: user.py, role.py, page.py, api_key.py, api_statistic.py, discount_plan.py, discount_plan_config.py, inventory.py
    templates/      # Jinja2: base.html, tools/, admin/, ecri/, scheduler/, statistics/, pages/
  datalayer/        # ETL pipeline modules (one per pipeline)
  config/           # YAML: scheduler.yaml, pipelines.yaml, alerts.yaml, llm.yaml
  migrations/       # SQL migration files
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
- DB session: `current_app.get_db_session()` for esa_backend; `get_pbi_session()` (lazy engine) for esa_pbi
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
```python
# esa_backend (app DB)
db = current_app.get_db_session()
try:
    result = db.query(Model).filter_by(...).first()
finally:
    db.close()

# esa_pbi (analytics DB) — lazy engine pattern
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from common.config_loader import get_database_url
engine = create_engine(get_database_url('pbi'))
Session = sessionmaker(bind=engine)
session = Session()
```

## Deploy
- Script: `scripts/deploy_to_vm.py` (paramiko-based, 6-step rsync pipeline)
- VM: `20.6.132.108`, user `esa_bk_admin`, SSH key `~/.ssh/id_ed25519_vm`
- Services: `esa-backend` (gunicorn), `backend-scheduler` (APScheduler daemon)
- No git on VM — uses rsync

## What NOT to Do
- Don't add React, Vue, TypeScript, or a build system — the frontend is intentionally vanilla
- Don't restructure the blueprint pattern or move routes around without asking
- Don't add new pip dependencies without discussing — the VM has no auto-install
- Don't use `print()` for logging — use `logging.getLogger(__name__)`
