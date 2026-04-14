"""
SugarCRM Tools Module

MCP tools for SugarCRM REST v11 operations: record CRUD, relationships,
and Studio/admin (custom fields, dropdowns, relationships, layouts).

Tools are grouped by tier (enforcement via api_keys.mcp_tools allowlist,
not code):
  sugarcrm_read  — 14 tools (SC_get_*, SC_list_*, SC_search*)
  sugarcrm_write — 8 tools  (SC_create_*, SC_update_*, SC_delete_* on records, SC_link_*/SC_unlink_*)
  sugarcrm_admin — 9 tools  (SC_*_field, SC_update_dropdown, SC_*_relationship, SC_update_layout, SC_studio_deploy)

Destructive tools (SC_delete_*, SC_studio_deploy) require an explicit
confirm=True argument to run.
"""
import json
import logging
from typing import Optional, Dict, List, TYPE_CHECKING

from mcp.server import Server

from mcp_esa.services.sugarcrm_service import (
    SugarCRMService, SugarCRMConfig, SugarCRMAPIError,
)

if TYPE_CHECKING:
    from mcp_esa.server.mcp_server import MCPServerApp

logger = logging.getLogger(__name__)

_service: Optional[SugarCRMService] = None


def _get_service(app) -> SugarCRMService:
    global _service
    if _service is None:
        s = app.settings
        cfg = SugarCRMConfig(
            url=s.sugarcrm_url,
            username=s.sugarcrm_username,
            password=s.sugarcrm_password,
            client_id=s.sugarcrm_client_id,
            client_secret=s.sugarcrm_client_secret,
            platform=s.sugarcrm_platform,
            timeout=s.sugarcrm_timeout,
        )
        _service = SugarCRMService(cfg)
    return _service


def register_sugarcrm_tools(server: Server, app: 'MCPServerApp') -> None:
    """Register all SugarCRM tools with the MCP server."""

    if not hasattr(server, '_tool_handlers'):
        server._tool_handlers = {}

    logger.info("Registering SugarCRM tools")

    # =========================================================================
    # READ TIER (14 tools)
    # =========================================================================

    async def SC_get_record(auth_context: Optional[Dict] = None,
                            module: str = None, record_id: str = None,
                            fields: Optional[List[str]] = None) -> str:
        """Get a single SugarCRM record by module and id."""
        try:
            if not module or not record_id:
                return "module and record_id are required"
            svc = _get_service(app)
            result = svc.get_record(module, record_id, fields)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_get_record: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_get_record", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_list_records(auth_context: Optional[Dict] = None,
                              module: str = None, filter: Optional[Dict] = None,
                              fields: Optional[List[str]] = None, limit: int = 20,
                              offset: int = 0, order_by: Optional[str] = None) -> str:
        """List records in a SugarCRM module with optional filtering."""
        try:
            if not module:
                return "module is required"
            svc = _get_service(app)
            result = svc.list_records(module, filter=filter, fields=fields,
                                      limit=limit, offset=offset, order_by=order_by)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_list_records: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_list_records", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_search(auth_context: Optional[Dict] = None,
                        module: str = None, q: str = None,
                        fields: Optional[List[str]] = None, limit: int = 20) -> str:
        """Full-text search within a SugarCRM module."""
        try:
            if not module or not q:
                return "module and q are required"
            svc = _get_service(app)
            result = svc.search(module, q, fields=fields, limit=limit)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_search: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_search", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_get_related(auth_context: Optional[Dict] = None,
                             module: str = None, record_id: str = None,
                             link_name: str = None, limit: int = 20,
                             offset: int = 0, fields: Optional[List[str]] = None) -> str:
        """Get related records for a given record via a named relationship link."""
        try:
            if not module or not record_id or not link_name:
                return "module, record_id, and link_name are required"
            svc = _get_service(app)
            result = svc.get_related(module, record_id, link_name,
                                     limit=limit, offset=offset, fields=fields)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_get_related: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_get_related", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_list_modules(auth_context: Optional[Dict] = None) -> str:
        """List all modules available in the SugarCRM instance."""
        try:
            svc = _get_service(app)
            result = svc.list_modules()
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_list_modules: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_list_modules", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_list_fields(auth_context: Optional[Dict] = None,
                             module: str = None) -> str:
        """List all fields defined on a SugarCRM module."""
        try:
            if not module:
                return "module is required"
            svc = _get_service(app)
            result = svc.list_fields(module)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_list_fields: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_list_fields", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_get_field(auth_context: Optional[Dict] = None,
                           module: str = None, field_name: str = None) -> str:
        """Get metadata for a single field on a SugarCRM module."""
        try:
            if not module or not field_name:
                return "module and field_name are required"
            svc = _get_service(app)
            result = svc.get_field(module, field_name)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_get_field: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_get_field", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_list_dropdowns(auth_context: Optional[Dict] = None) -> str:
        """List all dropdown (enum) definitions in SugarCRM Studio."""
        try:
            svc = _get_service(app)
            result = svc.list_dropdowns()
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_list_dropdowns: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_list_dropdowns", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_get_dropdown(auth_context: Optional[Dict] = None,
                              name: str = None) -> str:
        """Get the values for a named dropdown list."""
        try:
            if not name:
                return "name is required"
            svc = _get_service(app)
            result = svc.get_dropdown(name)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_get_dropdown: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_get_dropdown", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_get_layout(auth_context: Optional[Dict] = None,
                            module: str = None, view: str = None) -> str:
        """Get the layout definition for a module view (e.g. DetailView, EditView)."""
        try:
            if not module or not view:
                return "module and view are required"
            svc = _get_service(app)
            result = svc.get_layout(module, view)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_get_layout: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_get_layout", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_get_lead(auth_context: Optional[Dict] = None,
                          record_id: str = None,
                          fields: Optional[List[str]] = None) -> str:
        """Shortcut: get a single Lead record by id."""
        try:
            if not record_id:
                return "record_id is required"
            svc = _get_service(app)
            result = svc.get_record("Leads", record_id, fields)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_get_lead: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_get_lead", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_get_contact(auth_context: Optional[Dict] = None,
                             record_id: str = None,
                             fields: Optional[List[str]] = None) -> str:
        """Shortcut: get a single Contact record by id."""
        try:
            if not record_id:
                return "record_id is required"
            svc = _get_service(app)
            result = svc.get_record("Contacts", record_id, fields)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_get_contact: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_get_contact", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_get_account(auth_context: Optional[Dict] = None,
                             record_id: str = None,
                             fields: Optional[List[str]] = None) -> str:
        """Shortcut: get a single Account record by id."""
        try:
            if not record_id:
                return "record_id is required"
            svc = _get_service(app)
            result = svc.get_record("Accounts", record_id, fields)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_get_account: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_get_account", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_search_by_email(auth_context: Optional[Dict] = None,
                                 email: str = None, limit: int = 20) -> str:
        """Search Contacts and Leads by email address, returning combined results."""
        try:
            if not email:
                return "email is required"
            svc = _get_service(app)
            contacts = svc.search("Contacts", email, limit=limit)
            leads = svc.search("Leads", email, limit=limit)
            combined = {"contacts": contacts, "leads": leads}
            return json.dumps(combined, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_search_by_email: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_search_by_email", exc_info=True)
            return "Operation failed. Check server logs for details."

    # =========================================================================
    # WRITE TIER (8 tools)
    # =========================================================================

    async def SC_create_record(auth_context: Optional[Dict] = None,
                               module: str = None, data: Optional[Dict] = None) -> str:
        """Create a new record in the specified SugarCRM module."""
        try:
            if not module or not data:
                return "module and data are required"
            svc = _get_service(app)
            result = svc.create_record(module, data)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_create_record: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_create_record", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_update_record(auth_context: Optional[Dict] = None,
                               module: str = None, record_id: str = None,
                               data: Optional[Dict] = None) -> str:
        """Update fields on an existing SugarCRM record."""
        try:
            if not module or not record_id or not data:
                return "module, record_id, and data are required"
            svc = _get_service(app)
            result = svc.update_record(module, record_id, data)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_update_record: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_update_record", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_delete_record(auth_context: Optional[Dict] = None,
                               module: str = None, record_id: str = None,
                               confirm: bool = False) -> str:
        """Delete a SugarCRM record. Requires confirm=True."""
        try:
            if not confirm:
                return "Refused: destructive operation requires confirm=True"
            if not module or not record_id:
                return "module and record_id are required"
            svc = _get_service(app)
            svc.delete_record(module, record_id)
            return f"Deleted {module}/{record_id}"
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_delete_record: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_delete_record", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_link_records(auth_context: Optional[Dict] = None,
                              module: str = None, record_id: str = None,
                              link_name: str = None, related_id: str = None) -> str:
        """Link two records via a named relationship."""
        try:
            if not module or not record_id or not link_name or not related_id:
                return "module, record_id, link_name, and related_id are required"
            svc = _get_service(app)
            result = svc.link_records(module, record_id, link_name, related_id)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_link_records: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_link_records", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_unlink_records(auth_context: Optional[Dict] = None,
                                module: str = None, record_id: str = None,
                                link_name: str = None, related_id: str = None) -> str:
        """Remove a relationship link between two records."""
        try:
            if not module or not record_id or not link_name or not related_id:
                return "module, record_id, link_name, and related_id are required"
            svc = _get_service(app)
            result = svc.unlink_records(module, record_id, link_name, related_id)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_unlink_records: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_unlink_records", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_create_lead(auth_context: Optional[Dict] = None,
                             data: Optional[Dict] = None) -> str:
        """Shortcut: create a new Lead record."""
        try:
            if not data:
                return "data is required"
            svc = _get_service(app)
            result = svc.create_record("Leads", data)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_create_lead: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_create_lead", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_convert_lead(auth_context: Optional[Dict] = None,
                              lead_id: str = None,
                              convert_data: Optional[Dict] = None) -> str:
        """Convert a Lead to a Contact/Account/Opportunity using SugarCRM's convert endpoint."""
        try:
            if not lead_id or not convert_data:
                return "lead_id and convert_data are required"
            svc = _get_service(app)
            result = svc._request("POST", f"/Leads/{lead_id}/convert", json_body=convert_data)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_convert_lead: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_convert_lead", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_log_call(auth_context: Optional[Dict] = None,
                          data: Optional[Dict] = None) -> str:
        """Log a call activity record in SugarCRM."""
        try:
            if not data:
                return "data is required"
            svc = _get_service(app)
            result = svc.create_record("Calls", data)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_log_call: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_log_call", exc_info=True)
            return "Operation failed. Check server logs for details."

    # =========================================================================
    # ADMIN TIER (9 tools)
    # =========================================================================

    async def SC_create_field(auth_context: Optional[Dict] = None,
                              module: str = None, spec: Optional[Dict] = None) -> str:
        """Create a custom field on a SugarCRM module via Studio."""
        try:
            if not module or not spec:
                return "module and spec are required"
            svc = _get_service(app)
            result = svc.create_field(module, spec)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_create_field: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_create_field", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_update_field(auth_context: Optional[Dict] = None,
                              module: str = None, field_name: str = None,
                              spec: Optional[Dict] = None) -> str:
        """Update an existing custom field definition via Studio."""
        try:
            if not module or not field_name or not spec:
                return "module, field_name, and spec are required"
            svc = _get_service(app)
            result = svc.update_field(module, field_name, spec)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_update_field: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_update_field", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_delete_field(auth_context: Optional[Dict] = None,
                              module: str = None, field_name: str = None,
                              confirm: bool = False) -> str:
        """Delete a custom field from a SugarCRM module. Requires confirm=True."""
        try:
            if not confirm:
                return "Refused: destructive operation requires confirm=True"
            if not module or not field_name:
                return "module and field_name are required"
            svc = _get_service(app)
            result = svc.delete_field(module, field_name)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_delete_field: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_delete_field", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_update_dropdown(auth_context: Optional[Dict] = None,
                                 name: str = None,
                                 values: Optional[List] = None) -> str:
        """Update the values of a dropdown list in SugarCRM Studio."""
        try:
            if not name or values is None:
                return "name and values are required"
            svc = _get_service(app)
            result = svc.update_dropdown(name, values)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_update_dropdown: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_update_dropdown", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_create_relationship(auth_context: Optional[Dict] = None,
                                     spec: Optional[Dict] = None) -> str:
        """Create a new module-to-module relationship via Studio."""
        try:
            if not spec:
                return "spec is required"
            svc = _get_service(app)
            result = svc.create_relationship(spec)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_create_relationship: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_create_relationship", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_delete_relationship(auth_context: Optional[Dict] = None,
                                     rel_name: str = None,
                                     confirm: bool = False) -> str:
        """Delete a Studio-managed relationship. Requires confirm=True."""
        try:
            if not confirm:
                return "Refused: destructive operation requires confirm=True"
            if not rel_name:
                return "rel_name is required"
            svc = _get_service(app)
            result = svc.delete_relationship(rel_name)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_delete_relationship: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_delete_relationship", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_update_layout(auth_context: Optional[Dict] = None,
                               module: str = None, view: str = None,
                               spec: Optional[Dict] = None) -> str:
        """Update a view layout for a module via Studio."""
        try:
            if not module or not view or not spec:
                return "module, view, and spec are required"
            svc = _get_service(app)
            result = svc.update_layout(module, view, spec)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_update_layout: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_update_layout", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_studio_deploy(auth_context: Optional[Dict] = None,
                               confirm: bool = False) -> str:
        """Deploy all pending Studio changes to the SugarCRM instance. Requires confirm=True."""
        try:
            if not confirm:
                return "Refused: destructive operation requires confirm=True"
            svc = _get_service(app)
            result = svc.studio_deploy()
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_studio_deploy: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_studio_deploy", exc_info=True)
            return "Operation failed. Check server logs for details."

    async def SC_list_fields_admin(auth_context: Optional[Dict] = None,
                                   module: str = None) -> str:
        """Admin-tier alias for SC_list_fields; allows Studio bundle grant without read tier."""
        try:
            if not module:
                return "module is required"
            svc = _get_service(app)
            result = svc.list_fields(module)
            return json.dumps(result, indent=2, default=str)
        except SugarCRMAPIError as e:
            logger.warning("SugarCRM error in SC_list_fields_admin: %s (code=%s)", e, e.code)
            return f"SugarCRM error: {e.code or 'unknown'}"
        except Exception:
            logger.error("Unexpected error in SC_list_fields_admin", exc_info=True)
            return "Operation failed. Check server logs for details."

    # =========================================================================
    # INPUT SCHEMAS
    # =========================================================================

    _no_params = {"type": "object", "properties": {}, "required": []}

    _module_only = {
        "type": "object",
        "properties": {
            "module": {"type": "string", "description": "SugarCRM module name (e.g. Accounts, Leads, Contacts)"},
        },
        "required": ["module"],
    }

    _module_id = {
        "type": "object",
        "properties": {
            "module": {"type": "string", "description": "SugarCRM module name"},
            "record_id": {"type": "string", "description": "SugarCRM record id"},
            "fields": {"type": "array", "items": {"type": "string"}, "description": "Optional list of fields to return"},
        },
        "required": ["module", "record_id"],
    }

    _record_id_fields = {
        "type": "object",
        "properties": {
            "record_id": {"type": "string", "description": "SugarCRM record id"},
            "fields": {"type": "array", "items": {"type": "string"}, "description": "Optional list of fields to return"},
        },
        "required": ["record_id"],
    }

    _module_id_link = {
        "type": "object",
        "properties": {
            "module": {"type": "string", "description": "SugarCRM module name"},
            "record_id": {"type": "string", "description": "SugarCRM record id"},
            "link_name": {"type": "string", "description": "Relationship link name"},
            "related_id": {"type": "string", "description": "Related record id"},
        },
        "required": ["module", "record_id", "link_name", "related_id"],
    }

    _module_field_name = {
        "type": "object",
        "properties": {
            "module": {"type": "string", "description": "SugarCRM module name"},
            "field_name": {"type": "string", "description": "Field name"},
        },
        "required": ["module", "field_name"],
    }

    # Read tier schemas
    SC_get_record._input_schema = _module_id

    SC_list_records._input_schema = {
        "type": "object",
        "properties": {
            "module": {"type": "string", "description": "SugarCRM module name"},
            "filter": {"type": "object", "description": "Filter criteria as a JSON object"},
            "fields": {"type": "array", "items": {"type": "string"}, "description": "Fields to return"},
            "limit": {"type": "integer", "description": "Max records to return", "default": 20},
            "offset": {"type": "integer", "description": "Pagination offset", "default": 0},
            "order_by": {"type": "string", "description": "Field to order by"},
        },
        "required": ["module"],
    }

    SC_search._input_schema = {
        "type": "object",
        "properties": {
            "module": {"type": "string", "description": "SugarCRM module name"},
            "q": {"type": "string", "description": "Search query string"},
            "fields": {"type": "array", "items": {"type": "string"}, "description": "Fields to return"},
            "limit": {"type": "integer", "description": "Max records to return", "default": 20},
        },
        "required": ["module", "q"],
    }

    SC_get_related._input_schema = {
        "type": "object",
        "properties": {
            "module": {"type": "string", "description": "SugarCRM module name"},
            "record_id": {"type": "string", "description": "SugarCRM record id"},
            "link_name": {"type": "string", "description": "Relationship link name"},
            "limit": {"type": "integer", "description": "Max records to return", "default": 20},
            "offset": {"type": "integer", "description": "Pagination offset", "default": 0},
            "fields": {"type": "array", "items": {"type": "string"}, "description": "Fields to return"},
        },
        "required": ["module", "record_id", "link_name"],
    }

    SC_list_modules._input_schema = _no_params
    SC_list_fields._input_schema = _module_only
    SC_get_field._input_schema = _module_field_name
    SC_list_dropdowns._input_schema = _no_params

    SC_get_dropdown._input_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "Dropdown list name"},
        },
        "required": ["name"],
    }

    SC_get_layout._input_schema = {
        "type": "object",
        "properties": {
            "module": {"type": "string", "description": "SugarCRM module name"},
            "view": {"type": "string", "description": "View name (e.g. DetailView, EditView, ListView)"},
        },
        "required": ["module", "view"],
    }

    SC_get_lead._input_schema = _record_id_fields
    SC_get_contact._input_schema = _record_id_fields
    SC_get_account._input_schema = _record_id_fields

    SC_search_by_email._input_schema = {
        "type": "object",
        "properties": {
            "email": {"type": "string", "description": "Email address to search for"},
            "limit": {"type": "integer", "description": "Max records per module", "default": 20},
        },
        "required": ["email"],
    }

    # Write tier schemas
    SC_create_record._input_schema = {
        "type": "object",
        "properties": {
            "module": {"type": "string", "description": "SugarCRM module name"},
            "data": {"type": "object", "description": "Record field values as a JSON object"},
        },
        "required": ["module", "data"],
    }

    SC_update_record._input_schema = {
        "type": "object",
        "properties": {
            "module": {"type": "string", "description": "SugarCRM module name"},
            "record_id": {"type": "string", "description": "SugarCRM record id"},
            "data": {"type": "object", "description": "Fields to update as a JSON object"},
        },
        "required": ["module", "record_id", "data"],
    }

    SC_delete_record._input_schema = {
        "type": "object",
        "properties": {
            "module": {"type": "string", "description": "SugarCRM module name"},
            "record_id": {"type": "string", "description": "SugarCRM record id"},
            "confirm": {"type": "boolean", "description": "Must be true to execute deletion", "default": False},
        },
        "required": ["module", "record_id"],
    }

    SC_link_records._input_schema = _module_id_link
    SC_unlink_records._input_schema = _module_id_link

    SC_create_lead._input_schema = {
        "type": "object",
        "properties": {
            "data": {"type": "object", "description": "Lead field values as a JSON object"},
        },
        "required": ["data"],
    }

    SC_convert_lead._input_schema = {
        "type": "object",
        "properties": {
            "lead_id": {"type": "string", "description": "Lead record id to convert"},
            "convert_data": {"type": "object", "description": "Conversion payload (modules to create on convert)"},
        },
        "required": ["lead_id", "convert_data"],
    }

    SC_log_call._input_schema = {
        "type": "object",
        "properties": {
            "data": {"type": "object", "description": "Call record field values as a JSON object"},
        },
        "required": ["data"],
    }

    # Admin tier schemas
    SC_create_field._input_schema = {
        "type": "object",
        "properties": {
            "module": {"type": "string", "description": "SugarCRM module name"},
            "spec": {"type": "object", "description": "Field specification (name, type, label, etc.)"},
        },
        "required": ["module", "spec"],
    }

    SC_update_field._input_schema = {
        "type": "object",
        "properties": {
            "module": {"type": "string", "description": "SugarCRM module name"},
            "field_name": {"type": "string", "description": "Field name to update"},
            "spec": {"type": "object", "description": "Updated field specification"},
        },
        "required": ["module", "field_name", "spec"],
    }

    SC_delete_field._input_schema = {
        "type": "object",
        "properties": {
            "module": {"type": "string", "description": "SugarCRM module name"},
            "field_name": {"type": "string", "description": "Field name to delete"},
            "confirm": {"type": "boolean", "description": "Must be true to execute deletion", "default": False},
        },
        "required": ["module", "field_name"],
    }

    SC_update_dropdown._input_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "Dropdown list name"},
            "values": {"type": "array", "description": "New list of dropdown values"},
        },
        "required": ["name", "values"],
    }

    SC_create_relationship._input_schema = {
        "type": "object",
        "properties": {
            "spec": {"type": "object", "description": "Relationship specification (lhs_module, rhs_module, relationship_type, etc.)"},
        },
        "required": ["spec"],
    }

    SC_delete_relationship._input_schema = {
        "type": "object",
        "properties": {
            "rel_name": {"type": "string", "description": "Relationship name to delete"},
            "confirm": {"type": "boolean", "description": "Must be true to execute deletion", "default": False},
        },
        "required": ["rel_name"],
    }

    SC_update_layout._input_schema = {
        "type": "object",
        "properties": {
            "module": {"type": "string", "description": "SugarCRM module name"},
            "view": {"type": "string", "description": "View name (e.g. DetailView, EditView)"},
            "spec": {"type": "object", "description": "Layout specification"},
        },
        "required": ["module", "view", "spec"],
    }

    SC_studio_deploy._input_schema = {
        "type": "object",
        "properties": {
            "confirm": {"type": "boolean", "description": "Must be true to execute deployment", "default": False},
        },
        "required": [],
    }

    SC_list_fields_admin._input_schema = _module_only

    # =========================================================================
    # REGISTER ALL TOOLS
    # =========================================================================

    tools = {
        # Read tier
        "SC_get_record": SC_get_record,
        "SC_list_records": SC_list_records,
        "SC_search": SC_search,
        "SC_get_related": SC_get_related,
        "SC_list_modules": SC_list_modules,
        "SC_list_fields": SC_list_fields,
        "SC_get_field": SC_get_field,
        "SC_list_dropdowns": SC_list_dropdowns,
        "SC_get_dropdown": SC_get_dropdown,
        "SC_get_layout": SC_get_layout,
        "SC_get_lead": SC_get_lead,
        "SC_get_contact": SC_get_contact,
        "SC_get_account": SC_get_account,
        "SC_search_by_email": SC_search_by_email,
        # Write tier
        "SC_create_record": SC_create_record,
        "SC_update_record": SC_update_record,
        "SC_delete_record": SC_delete_record,
        "SC_link_records": SC_link_records,
        "SC_unlink_records": SC_unlink_records,
        "SC_create_lead": SC_create_lead,
        "SC_convert_lead": SC_convert_lead,
        "SC_log_call": SC_log_call,
        # Admin tier
        "SC_create_field": SC_create_field,
        "SC_update_field": SC_update_field,
        "SC_delete_field": SC_delete_field,
        "SC_update_dropdown": SC_update_dropdown,
        "SC_create_relationship": SC_create_relationship,
        "SC_delete_relationship": SC_delete_relationship,
        "SC_update_layout": SC_update_layout,
        "SC_studio_deploy": SC_studio_deploy,
        "SC_list_fields_admin": SC_list_fields_admin,
    }

    for name, handler in tools.items():
        server._tool_handlers[name] = handler

    logger.info(f"Registered {len(tools)} SugarCRM tools")
