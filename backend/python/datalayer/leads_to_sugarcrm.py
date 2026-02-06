"""
Leads to SugarCRM Pipeline - Create/Update Leads with Custom Fields

Creates or updates Lead records in SugarCRM via the REST v11 API,
including support for custom fields (fields ending in '_c').

Features:
- Create new leads with standard + custom fields
- Update existing leads by ID or email lookup
- Batch operations from CSV/JSON input files
- Dry-run mode for validation without API writes
- Field validation against SugarCRM metadata
- Deduplication via email lookup before creation

Usage:
    # Create a single lead (interactive/inline)
    python leads_to_sugarcrm.py --mode create \
        --fields '{"last_name":"Doe","email":"john@example.com","status":"New","custom_score_c":"85"}'

    # Update existing lead by ID
    python leads_to_sugarcrm.py --mode update --record-id abc-123-uuid \
        --fields '{"status":"Converted","custom_score_c":"95"}'

    # Upsert (create or update) by email lookup
    python leads_to_sugarcrm.py --mode upsert --lookup-field email \
        --fields '{"last_name":"Doe","email":"john@example.com","status":"New"}'

    # Batch create/update from CSV
    python leads_to_sugarcrm.py --mode batch --input leads.csv

    # Batch from JSON
    python leads_to_sugarcrm.py --mode batch --input leads.json

    # Dry run (validate only, no API calls)
    python leads_to_sugarcrm.py --mode create --fields '{"last_name":"Doe"}' --dry-run

    # List all custom fields on the Leads module
    python leads_to_sugarcrm.py --mode list-fields --custom-only

Configuration:
    Uses same apis.yaml credentials as sugarcrm_to_sql.py
"""

import argparse
import csv
import json
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.sugarcrm_client import SugarCRMClient

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# Field Helpers
# =============================================================================

# Standard Lead fields for reference (non-exhaustive, metadata is authoritative)
STANDARD_LEAD_FIELDS = {
    'salutation', 'first_name', 'last_name', 'full_name', 'title',
    'department', 'description', 'status', 'status_description',
    'lead_source', 'lead_source_description',
    'phone_work', 'phone_mobile', 'phone_home', 'phone_fax', 'phone_other',
    'email', 'email1', 'email2', 'webtolead_email1', 'webtolead_email2',
    'primary_address_street', 'primary_address_city', 'primary_address_state',
    'primary_address_postalcode', 'primary_address_country',
    'alt_address_street', 'alt_address_city', 'alt_address_state',
    'alt_address_postalcode', 'alt_address_country',
    'account_name', 'account_description', 'website',
    'do_not_call', 'converted', 'campaign_id',
    'assigned_user_id', 'team_id',
}

# Fields that are read-only and should not be sent in create/update
READ_ONLY_FIELDS = {
    'id', 'date_entered', 'date_modified', 'modified_user_id',
    'created_by', 'deleted', 'full_name',
}


def is_custom_field(field_name: str) -> bool:
    """Check if a field is a custom field (SugarCRM convention: suffix '_c')."""
    return field_name.endswith('_c')


def format_email_field(email: str) -> List[Dict[str, Any]]:
    """
    Format an email string into SugarCRM's email field structure.

    SugarCRM expects email fields as a list of dicts for the 'email' field:
        [{"email_address": "x@y.com", "primary_address": true}]

    For simple email fields like email1, webtolead_email1, a plain string works.
    """
    return [{'email_address': email, 'primary_address': True}]


def prepare_lead_fields(
    fields: Dict[str, Any],
    field_metadata: Optional[Dict[str, Dict]] = None
) -> Tuple[Dict[str, Any], List[str]]:
    """
    Prepare and validate lead fields for API submission.

    - Strips read-only fields
    - Formats email field if needed
    - Validates against metadata if provided

    Args:
        fields: Raw field dict from user input
        field_metadata: Optional SugarCRM metadata for the Leads module

    Returns:
        Tuple of (prepared_fields, warning_messages)
    """
    prepared = {}
    warnings = []

    valid_field_names = set()
    if field_metadata:
        valid_field_names = set(field_metadata.keys())

    for field_name, value in fields.items():
        # Skip read-only fields
        if field_name in READ_ONLY_FIELDS:
            warnings.append(f"Skipped read-only field: '{field_name}'")
            continue

        # Validate against metadata if available
        if valid_field_names and field_name not in valid_field_names:
            warnings.append(f"Unknown field '{field_name}' - not in Leads metadata (may still work if recently added)")

        # Format the 'email' field as list-of-dicts if it's a plain string
        if field_name == 'email' and isinstance(value, str) and '@' in value:
            prepared[field_name] = format_email_field(value)
        else:
            prepared[field_name] = value

    # Require last_name (SugarCRM mandatory field for Leads)
    if 'last_name' not in prepared:
        warnings.append("WARNING: 'last_name' is required by SugarCRM for Lead creation")

    return prepared, warnings


# =============================================================================
# Operations
# =============================================================================

def list_lead_fields(client: SugarCRMClient, custom_only: bool = False) -> None:
    """
    List all fields available on the Leads module.

    Args:
        client: Authenticated SugarCRM client
        custom_only: If True, only show custom fields (ending in '_c')
    """
    field_defs, error = client.get_module_fields('Leads')
    if error:
        print(f"ERROR: Could not fetch Leads metadata: {error}")
        return

    print(f"\n{'='*80}")
    print(f"SugarCRM Leads Module Fields {'(Custom Only)' if custom_only else '(All)'}")
    print(f"{'='*80}")
    print(f"{'Field Name':<40} {'Type':<15} {'Label'}")
    print(f"{'-'*40} {'-'*15} {'-'*30}")

    count = 0
    for name, info in sorted(field_defs.items()):
        if not isinstance(info, dict):
            continue

        field_type = info.get('type', 'unknown')
        label = info.get('label', info.get('vname', ''))

        if custom_only and not is_custom_field(name):
            continue

        # Skip link/collection types in display
        if field_type in ('link', 'collection', 'team_list'):
            continue

        print(f"  {name:<40} {field_type:<15} {label}")
        count += 1

    print(f"\nTotal: {count} fields")
    print(f"{'='*80}")


def create_lead(
    client: SugarCRMClient,
    fields: Dict[str, Any],
    dry_run: bool = False,
    validate: bool = True
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Create a new Lead in SugarCRM.

    Args:
        client: Authenticated SugarCRM client
        fields: Field values for the new lead (including custom fields)
        dry_run: If True, validate only without creating
        validate: Validate fields against metadata

    Returns:
        Tuple of (created_record, error_message)
    """
    # Validate fields
    field_metadata = None
    if validate:
        field_metadata, error = client.get_module_fields('Leads')
        if error:
            logger.warning(f"Could not fetch metadata for validation: {error}")

    prepared, warnings = prepare_lead_fields(fields, field_metadata)
    for w in warnings:
        print(f"  {w}")

    if dry_run:
        print("\n  [DRY RUN] Would create Lead with fields:")
        for k, v in prepared.items():
            print(f"    {k}: {v}")
        return {'_dry_run': True, **prepared}, None

    # Create the record
    result, error = client.create_record('Leads', prepared)
    if error:
        return None, f"Failed to create Lead: {error}"

    print(f"  Created Lead: id={result.get('id')}, name={result.get('full_name', 'N/A')}")
    return result, None


def update_lead(
    client: SugarCRMClient,
    record_id: str,
    fields: Dict[str, Any],
    dry_run: bool = False,
    validate: bool = True
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Update an existing Lead in SugarCRM.

    Args:
        client: Authenticated SugarCRM client
        record_id: UUID of the Lead to update
        fields: Field values to update (only these fields change)
        dry_run: If True, validate only without updating
        validate: Validate fields against metadata

    Returns:
        Tuple of (updated_record, error_message)
    """
    field_metadata = None
    if validate:
        field_metadata, error = client.get_module_fields('Leads')
        if error:
            logger.warning(f"Could not fetch metadata for validation: {error}")

    prepared, warnings = prepare_lead_fields(fields, field_metadata)
    for w in warnings:
        print(f"  {w}")

    if dry_run:
        print(f"\n  [DRY RUN] Would update Lead {record_id} with fields:")
        for k, v in prepared.items():
            print(f"    {k}: {v}")
        return {'_dry_run': True, 'id': record_id, **prepared}, None

    result, error = client.update_record('Leads', record_id, prepared)
    if error:
        return None, f"Failed to update Lead {record_id}: {error}"

    print(f"  Updated Lead: id={result.get('id')}, name={result.get('full_name', 'N/A')}")
    return result, None


def upsert_lead(
    client: SugarCRMClient,
    fields: Dict[str, Any],
    lookup_field: str = 'email',
    dry_run: bool = False,
    validate: bool = True
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Create or update a Lead. Searches by lookup_field first; updates if found,
    creates if not.

    Args:
        client: Authenticated SugarCRM client
        fields: Field values for the lead
        lookup_field: Field to search by for existing records (default: 'email')
        dry_run: If True, validate and show what would happen
        validate: Validate fields against metadata

    Returns:
        Tuple of (record, error_message)
    """
    field_metadata = None
    if validate:
        field_metadata, error = client.get_module_fields('Leads')
        if error:
            logger.warning(f"Could not fetch metadata for validation: {error}")

    prepared, warnings = prepare_lead_fields(fields, field_metadata)
    for w in warnings:
        print(f"  {w}")

    lookup_value = fields.get(lookup_field)
    if not lookup_value:
        return None, f"Lookup field '{lookup_field}' not found in provided fields"

    if dry_run:
        print(f"\n  [DRY RUN] Would upsert Lead (lookup: {lookup_field}={lookup_value}):")
        for k, v in prepared.items():
            print(f"    {k}: {v}")
        return {'_dry_run': True, **prepared}, None

    # Use the client's upsert method
    result, error = client.upsert_record(
        module='Leads',
        fields=prepared,
        lookup_field=lookup_field,
        lookup_value=lookup_value
    )
    if error:
        return None, f"Failed to upsert Lead: {error}"

    action = result.get('_action', 'unknown')
    print(f"  {action.capitalize()} Lead: id={result.get('id')}, name={result.get('full_name', 'N/A')}")
    return result, None


# =============================================================================
# Batch Operations
# =============================================================================

def load_records_from_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Load records from a CSV or JSON file.

    CSV: First row is headers (field names). Custom fields use their _c names.
    JSON: Array of objects, or object with 'records' key containing array.

    Args:
        file_path: Path to CSV or JSON file

    Returns:
        List of record dicts
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    if path.suffix.lower() == '.csv':
        records = []
        with open(path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Strip whitespace from keys and values, skip empty values
                clean = {}
                for k, v in row.items():
                    k = k.strip()
                    v = v.strip() if v else ''
                    if v:  # Only include non-empty values
                        clean[k] = v
                if clean:
                    records.append(clean)
        return records

    elif path.suffix.lower() == '.json':
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and 'records' in data:
            return data['records']
        else:
            raise ValueError("JSON must be an array or an object with a 'records' key")

    else:
        raise ValueError(f"Unsupported file type: {path.suffix} (use .csv or .json)")


def run_batch(
    client: SugarCRMClient,
    file_path: str,
    mode: str = 'upsert',
    lookup_field: str = 'email',
    dry_run: bool = False,
    validate: bool = True
) -> Dict[str, int]:
    """
    Process a batch of lead records from a file.

    Args:
        client: Authenticated SugarCRM client
        file_path: Path to CSV or JSON input file
        mode: 'create', 'update', or 'upsert' for each record
        lookup_field: Field for upsert matching
        dry_run: Validate only
        validate: Validate against metadata

    Returns:
        Dict with counts: {'created': N, 'updated': N, 'failed': N, 'skipped': N}
    """
    records = load_records_from_file(file_path)
    print(f"\n  Loaded {len(records)} records from {file_path}")

    # Fetch metadata once for the whole batch
    field_metadata = None
    if validate:
        field_metadata, error = client.get_module_fields('Leads')
        if error:
            logger.warning(f"Could not fetch metadata for validation: {error}")

    stats = {'created': 0, 'updated': 0, 'failed': 0, 'skipped': 0}

    for i, record in enumerate(records, 1):
        print(f"\n  [{i}/{len(records)}] Processing: {record.get('last_name', 'N/A')} "
              f"({record.get('email', record.get('email1', 'no email'))})")

        try:
            prepared, warnings = prepare_lead_fields(record, field_metadata)
            for w in warnings:
                print(f"    {w}")

            if dry_run:
                custom_fields = {k: v for k, v in prepared.items() if is_custom_field(k)}
                print(f"    [DRY RUN] Would {mode}: {len(prepared)} fields "
                      f"({len(custom_fields)} custom)")
                stats['skipped'] += 1
                continue

            if mode == 'create':
                result, error = client.create_record('Leads', prepared)
                if error:
                    print(f"    FAILED: {error}")
                    stats['failed'] += 1
                else:
                    print(f"    Created: id={result.get('id')}")
                    stats['created'] += 1

            elif mode == 'update':
                record_id = record.get('id') or record.get('sugar_id')
                if not record_id:
                    print(f"    SKIPPED: No 'id' field for update mode")
                    stats['skipped'] += 1
                    continue
                result, error = client.update_record('Leads', record_id, prepared)
                if error:
                    print(f"    FAILED: {error}")
                    stats['failed'] += 1
                else:
                    print(f"    Updated: id={result.get('id')}")
                    stats['updated'] += 1

            elif mode == 'upsert':
                lookup_value = record.get(lookup_field)
                if not lookup_value:
                    print(f"    SKIPPED: No '{lookup_field}' value for upsert")
                    stats['skipped'] += 1
                    continue
                result, error = client.upsert_record(
                    module='Leads',
                    fields=prepared,
                    lookup_field=lookup_field,
                    lookup_value=lookup_value
                )
                if error:
                    print(f"    FAILED: {error}")
                    stats['failed'] += 1
                else:
                    action = result.get('_action', 'unknown')
                    print(f"    {action.capitalize()}: id={result.get('id')}")
                    stats[action] = stats.get(action, 0) + 1

        except Exception as e:
            print(f"    ERROR: {e}")
            stats['failed'] += 1

    return stats


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Create/Update SugarCRM Leads with Custom Fields',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all custom fields on Leads module
  python leads_to_sugarcrm.py --mode list-fields --custom-only

  # Create a lead with custom fields
  python leads_to_sugarcrm.py --mode create \\
      --fields '{"last_name":"Doe","email":"john@example.com","custom_score_c":"85"}'

  # Update a lead by ID
  python leads_to_sugarcrm.py --mode update --record-id abc-123 \\
      --fields '{"status":"Converted"}'

  # Upsert by email (create if not found, update if found)
  python leads_to_sugarcrm.py --mode upsert --lookup-field email \\
      --fields '{"last_name":"Doe","email":"john@example.com","status":"New"}'

  # Batch upsert from CSV
  python leads_to_sugarcrm.py --mode batch --input leads.csv --batch-mode upsert

  # Dry run (validate without writing)
  python leads_to_sugarcrm.py --mode create --fields '{"last_name":"Test"}' --dry-run
        """
    )

    parser.add_argument(
        '--mode',
        choices=['create', 'update', 'upsert', 'batch', 'list-fields'],
        required=True,
        help='Operation mode'
    )

    parser.add_argument(
        '--fields',
        type=str,
        help='JSON string of field values (for create/update/upsert modes)'
    )

    parser.add_argument(
        '--record-id',
        type=str,
        help='SugarCRM record UUID (required for update mode)'
    )

    parser.add_argument(
        '--lookup-field',
        type=str,
        default='email',
        help='Field to match on for upsert (default: email)'
    )

    parser.add_argument(
        '--input',
        type=str,
        help='Input file path for batch mode (CSV or JSON)'
    )

    parser.add_argument(
        '--batch-mode',
        choices=['create', 'update', 'upsert'],
        default='upsert',
        help='Operation for each record in batch mode (default: upsert)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate and display what would happen without making API calls'
    )

    parser.add_argument(
        '--custom-only',
        action='store_true',
        help='For list-fields mode: only show custom fields'
    )

    parser.add_argument(
        '--no-validate',
        action='store_true',
        help='Skip field validation against SugarCRM metadata'
    )

    return parser.parse_args()


def main():
    """Main function."""
    args = parse_args()

    # Print header
    print("=" * 70)
    print("SugarCRM Leads - Create/Update Pipeline")
    print("=" * 70)
    print(f"Mode: {args.mode.upper()}")
    if args.dry_run:
        print("DRY RUN: No changes will be made")
    print("=" * 70)

    # Create and authenticate client
    client = SugarCRMClient.from_env()
    if not client.authenticate():
        print("ERROR: Failed to authenticate with SugarCRM")
        sys.exit(1)

    try:
        if args.mode == 'list-fields':
            list_lead_fields(client, custom_only=args.custom_only)

        elif args.mode == 'create':
            if not args.fields:
                print("ERROR: --fields is required for create mode")
                sys.exit(1)
            fields = json.loads(args.fields)
            result, error = create_lead(
                client, fields,
                dry_run=args.dry_run,
                validate=not args.no_validate
            )
            if error:
                print(f"\nERROR: {error}")
                sys.exit(1)

        elif args.mode == 'update':
            if not args.fields:
                print("ERROR: --fields is required for update mode")
                sys.exit(1)
            if not args.record_id:
                print("ERROR: --record-id is required for update mode")
                sys.exit(1)
            fields = json.loads(args.fields)
            result, error = update_lead(
                client, args.record_id, fields,
                dry_run=args.dry_run,
                validate=not args.no_validate
            )
            if error:
                print(f"\nERROR: {error}")
                sys.exit(1)

        elif args.mode == 'upsert':
            if not args.fields:
                print("ERROR: --fields is required for upsert mode")
                sys.exit(1)
            fields = json.loads(args.fields)
            result, error = upsert_lead(
                client, fields,
                lookup_field=args.lookup_field,
                dry_run=args.dry_run,
                validate=not args.no_validate
            )
            if error:
                print(f"\nERROR: {error}")
                sys.exit(1)

        elif args.mode == 'batch':
            if not args.input:
                print("ERROR: --input is required for batch mode")
                sys.exit(1)
            stats = run_batch(
                client,
                file_path=args.input,
                mode=args.batch_mode,
                lookup_field=args.lookup_field,
                dry_run=args.dry_run,
                validate=not args.no_validate
            )

            # Print summary
            print(f"\n{'='*70}")
            print("Batch Summary")
            print(f"{'='*70}")
            for key, count in stats.items():
                print(f"  {key.capitalize()}: {count}")
            print(f"  Total processed: {sum(stats.values())}")
            print(f"{'='*70}")

    finally:
        client.logout()

    print("\nDone.")


if __name__ == "__main__":
    main()
