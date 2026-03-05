# ESA Backend - Important Paths and Credentials Reference

## Database Connections
- Database config name: `pbi` (not `pbi_data`)
- Database session helper: `get_pbi_session()` from `/backend/python/web/routes/api.py`
- Vault master key env var: `VAULT_MASTER_KEY`

## Important Site IDs
- LSETUP (Test/Setup Site): Site ID = 27525, Site Code = "LSETUP"
  - Located in: `/backend/python/web/templates/tools/billing_date_changer.html` (lines 317-323)
  - Reference: `/backend/python/datalayer/tenant_ledger_charges_historical.py:87`

## Billing Date Changer
- Template: `/backend/python/web/templates/tools/billing_date_changer.html`
- API endpoint: `/api/billing-day/<site_id>` (GET)
- Update endpoint: `/api/billing-day/update` (POST)
- Route handler: `/backend/python/web/routes/api.py` (lines 1394-1631)

## Common API Patterns
```python
# Get PBI database session
from web.routes.api import get_pbi_session
session = get_pbi_session()

# Query sites
from common.models import SiteInfo
sites = session.query(SiteInfo).all()
```

## Deployment
- Deploy command: `python3 scripts/deploy_to_vm.py`
- Target VM: 20.6.132.108
- Services: esa-backend, backend-scheduler

## File Locations
- Models: `/backend/python/common/models.py`
- API routes: `/backend/python/web/routes/api.py`
- Config loader: `/backend/python/common/config_loader.py`
- Tool templates: `/backend/python/web/templates/tools/`