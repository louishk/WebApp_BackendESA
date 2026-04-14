"""
Manual smoke test for SugarCRM MCP service.

NOT run in CI — this hits the live tenant. Run manually against a dev/staging
tenant first, then prod with an account whose changes can be rolled back.

Usage:
    python3 mcp_esa/tests/smoke_sugarcrm.py read
    python3 mcp_esa/tests/smoke_sugarcrm.py write
    python3 mcp_esa/tests/smoke_sugarcrm.py admin
"""
import sys
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(name)s %(message)s')

from mcp_esa.config.settings import get_settings
from mcp_esa.services.sugarcrm_service import SugarCRMService, SugarCRMConfig


def _svc():
    s = get_settings()
    cfg = SugarCRMConfig(
        url=s.sugarcrm_url,
        username=s.sugarcrm_username,
        password=s.sugarcrm_password,
        client_id=s.sugarcrm_client_id,
        client_secret=s.sugarcrm_client_secret,
        platform=s.sugarcrm_platform,
        timeout=s.sugarcrm_timeout,
    )
    return SugarCRMService(cfg)


def read_smoke():
    svc = _svc()
    print("--- list_modules ---")
    mods = svc.list_modules()
    print(f"modules returned: {len(mods.get('modules', {}))}")
    print("--- list_records Accounts limit=3 ---")
    accts = svc.list_records("Accounts", limit=3, fields=["name"])
    print(json.dumps(accts, indent=2, default=str)[:500])
    print("--- list_fields Leads ---")
    f = svc.list_fields("Leads")
    print(f"Leads field count: {len(f.get('fields', {}))}")


def write_smoke():
    svc = _svc()
    print("--- create_record Leads ---")
    new = svc.create_record("Leads", {
        "first_name": "MCP", "last_name": "SmokeTest",
        "status": "New", "description": "mcp_esa smoke test — safe to delete",
    })
    lead_id = new.get("id")
    print(f"created lead id: {lead_id}")
    print("--- update_record ---")
    svc.update_record("Leads", lead_id, {"description": "updated by smoke test"})
    print("--- delete_record ---")
    svc.delete_record("Leads", lead_id)
    print("cleaned up")


def admin_smoke():
    svc = _svc()
    print("--- list_dropdowns ---")
    dd = svc.list_dropdowns()
    print(f"dropdown type: {type(dd).__name__}")
    # Intentionally does NOT create/delete fields — leave that for manual QA.


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "read"
    {"read": read_smoke, "write": write_smoke, "admin": admin_smoke}[mode]()
