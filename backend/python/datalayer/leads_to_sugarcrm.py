"""
Test script for SugarCRM Lead upsert with custom fields.

Tests the create/update CRUD methods on the Leads module,
including custom fields (fields ending in '_c').

Usage:
    python leads_to_sugarcrm.py
"""

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.sugarcrm_client import SugarCRMClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def print_record(record: dict, label: str = "Record") -> None:
    """Pretty-print a SugarCRM record."""
    print(f"\n  {label}:")
    for k, v in sorted(record.items()):
        if k.startswith('_') and k != '_action':
            continue
        print(f"    {k}: {v}")


def test_list_custom_fields(client: SugarCRMClient) -> None:
    """List all custom fields on the Leads module."""
    print("\n--- Listing Leads custom fields ---")
    field_defs, error = client.get_module_fields('Leads')
    if error:
        print(f"  FAIL: {error}")
        return

    custom = {
        name: info for name, info in field_defs.items()
        if isinstance(info, dict) and name.endswith('_c')
    }
    print(f"  Found {len(custom)} custom fields:")
    for name, info in sorted(custom.items()):
        print(f"    {name:<40} type={info.get('type', '?'):<15} label={info.get('label', '')}")

    if not custom:
        print("  (none found)")


def test_create_lead(client: SugarCRMClient, custom_fields: dict) -> str | None:
    """Create a test lead and return its ID."""
    print("\n--- Creating a test lead ---")
    fields = {
        'first_name': 'Test',
        'last_name': 'LeadCustomFields',
        'email': [{'email_address': 'test.customfields@example.com', 'primary_address': True}],
        'phone_mobile': '555-0199',
        'status': 'New',
        'lead_source': 'Web Site',
        **custom_fields,
    }
    print(f"  Payload: {json.dumps(fields, indent=4)}")

    result, error = client.create_record('Leads', fields)
    if error:
        print(f"  FAIL: {error}")
        return None

    record_id = result.get('id')
    print(f"  OK: created id={record_id}")
    print_record(result, "Created lead")
    return record_id


def test_update_lead(client: SugarCRMClient, record_id: str, custom_fields: dict) -> None:
    """Update the test lead with new custom field values."""
    print(f"\n--- Updating lead {record_id} ---")
    fields = {
        'status': 'Assigned',
        **custom_fields,
    }
    print(f"  Payload: {json.dumps(fields, indent=4)}")

    result, error = client.update_record('Leads', record_id, fields)
    if error:
        print(f"  FAIL: {error}")
        return

    print(f"  OK: updated id={result.get('id')}")
    print_record(result, "Updated lead")


def test_upsert_lead(client: SugarCRMClient, record_id: str, custom_fields: dict) -> None:
    """Upsert by ID (should update the existing lead)."""
    print(f"\n--- Upsert by ID {record_id} (expect update) ---")
    fields = {
        'id': record_id,
        'description': 'Upserted via test script',
        **custom_fields,
    }

    result, error = client.upsert_record('Leads', fields, lookup_field='id')
    if error:
        print(f"  FAIL: {error}")
        return

    print(f"  OK: action={result.get('_action')}, id={result.get('id')}")
    print_record(result, "Upserted lead")


def test_get_lead(client: SugarCRMClient, record_id: str) -> None:
    """Read back the lead and display its fields."""
    print(f"\n--- Reading back lead {record_id} ---")
    result, error = client.get_record('Leads', record_id)
    if error:
        print(f"  FAIL: {error}")
        return

    print_record(result, "Fetched lead")

    # Highlight custom fields
    custom = {k: v for k, v in result.items() if k.endswith('_c')}
    if custom:
        print("\n  Custom field values:")
        for k, v in sorted(custom.items()):
            print(f"    {k}: {v}")


def test_cleanup(client: SugarCRMClient, record_id: str) -> None:
    """Delete the test lead."""
    print(f"\n--- Cleaning up: deleting lead {record_id} ---")
    result, error = client.delete_record('Leads', record_id)
    if error:
        print(f"  FAIL: {error}")
        return
    print("  OK: deleted")


def main():
    print("=" * 60)
    print("SugarCRM Lead Upsert Test - Custom Fields")
    print("=" * 60)

    # Authenticate
    client = SugarCRMClient.from_env()
    if not client.authenticate():
        print("FATAL: authentication failed")
        sys.exit(1)
    print("Authenticated OK")

    try:
        # 1) Discover custom fields
        test_list_custom_fields(client)

        # 2) Build a sample set of custom field values to test with.
        #    Adjust these to match the custom fields that actually exist
        #    on your Leads module (discovered in step 1 above).
        #    Example — if you have 'score_c' (int) and 'source_detail_c' (varchar):
        sample_custom_create = {
            # 'score_c': '85',
            # 'source_detail_c': 'Landing page A',
        }
        sample_custom_update = {
            # 'score_c': '95',
            # 'source_detail_c': 'Landing page B (updated)',
        }
        sample_custom_upsert = {
            # 'score_c': '99',
        }

        # 3) Create
        record_id = test_create_lead(client, sample_custom_create)
        if not record_id:
            print("\nCreate failed — skipping remaining tests")
            return

        # 4) Update with new custom values
        test_update_lead(client, record_id, sample_custom_update)

        # 5) Upsert by ID (should update, not create a duplicate)
        test_upsert_lead(client, record_id, sample_custom_upsert)

        # 6) Read back and verify
        test_get_lead(client, record_id)

        # 7) Cleanup
        test_cleanup(client, record_id)

    finally:
        client.logout()

    print("\nAll tests completed.")


if __name__ == "__main__":
    main()
