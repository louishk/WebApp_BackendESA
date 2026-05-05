"""
Zoom Contacts Sync Pipeline

Pushes SugarCRM contacts and leads to Zoom Phone External Contacts.
Maintains a bidirectional mapping in zoom_contact_sync for tracking state.

Modes:
- backfill: Process all CRM records with phone numbers
- auto: Process only records modified since last sync

Usage:
    python -m datalayer.zoom_contacts_sync --mode backfill
    python -m datalayer.zoom_contacts_sync --mode auto
"""

import argparse
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from common.config_loader import get_database_url
from common.models import Base, ZoomContactSync, ZoomSyncState
from common.sugarcrm_client import SugarCRMClient
from common.zoom_client import ZoomClient, ZoomAPIError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

BATCH_COMMIT_SIZE = 50

# Country code inference by SugarCRM site/region patterns
# Fallback: if no country hint, default to +65 (Singapore — ESA HQ)
COUNTRY_CODES = {
    'SG': '65',
    'KR': '82',
    'MY': '60',
    'HK': '852',
    'JP': '81',
    'TW': '886',
}
DEFAULT_COUNTRY_CODE = '65'

# Phone field names in sugarcrm_contacts / sugarcrm_leads tables
PHONE_FIELDS = ['phone_mobile', 'phone_work', 'phone_home', 'phone_other', 'phone_fax']
PHONE_LABELS = {
    'phone_mobile': 'Mobile',
    'phone_work': 'Work',
    'phone_home': 'Home',
    'phone_other': 'Other',
    'phone_fax': 'Fax',
}


# =============================================================================
# Phone Normalization
# =============================================================================

def normalize_phone(raw: str, country_hint: Optional[str] = None) -> Optional[str]:
    """Normalize a phone number to E.164 format.

    Strips non-digit characters, infers country code if missing.
    Returns None for numbers that are too short to be valid.
    """
    if not raw:
        return None

    # Strip everything except digits and leading +
    has_plus = raw.strip().startswith('+')
    digits = re.sub(r'\D', '', raw)

    if not digits or len(digits) < 4:
        return None

    # Already has international prefix
    if has_plus and len(digits) >= 7:
        return f'+{digits}'

    # Detect existing country code (common patterns)
    for code_val in sorted(COUNTRY_CODES.values(), key=len, reverse=True):
        if digits.startswith(code_val) and len(digits) >= len(code_val) + 4:
            return f'+{digits}'

    # Infer country code
    cc = COUNTRY_CODES.get(country_hint, DEFAULT_COUNTRY_CODE)

    # Strip leading 0 (local trunk prefix)
    if digits.startswith('0'):
        digits = digits[1:]

    if len(digits) < 4:
        return None

    return f'+{cc}{digits}'


def extract_phone_numbers(
    record: Dict[str, Any],
    country_hint: Optional[str] = None,
) -> List[str]:
    """Extract and normalize phone numbers from a CRM record.

    Returns list of E.164 strings (Zoom requires flat string array, not objects).
    """
    phones: List[str] = []
    seen: Set[str] = set()

    for field in PHONE_FIELDS:
        raw = record.get(field)
        if not raw:
            continue
        normalized = normalize_phone(str(raw), country_hint)
        if normalized and normalized not in seen:
            seen.add(normalized)
            phones.append(normalized)

    return phones


def infer_country_hint(record: Dict[str, Any]) -> Optional[str]:
    """Infer country code hint from CRM record fields."""
    # Try primary_address_country, billing_address_country, etc.
    for field in ['primary_address_country', 'billing_address_country', 'country']:
        val = record.get(field, '')
        if not val:
            continue
        val_upper = str(val).upper().strip()
        # Direct match
        if val_upper in COUNTRY_CODES:
            return val_upper
        # Common names
        country_name_map = {
            'SINGAPORE': 'SG', 'KOREA': 'KR', 'SOUTH KOREA': 'KR',
            'MALAYSIA': 'MY', 'HONG KONG': 'HK', 'JAPAN': 'JP', 'TAIWAN': 'TW',
        }
        if val_upper in country_name_map:
            return country_name_map[val_upper]
    return None


# =============================================================================
# CRM Data Fetching
# =============================================================================

def fetch_crm_records(
    pbi_engine,
    module: str,
    since: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Fetch records from sugarcrm_contacts or sugarcrm_leads with phone numbers.

    Args:
        pbi_engine: SQLAlchemy engine for esa_pbi.
        module: 'Contacts' or 'Leads'.
        since: If set, only fetch records modified after this datetime.

    Returns:
        List of record dicts.
    """
    table = 'sugarcrm_contacts' if module == 'Contacts' else 'sugarcrm_leads'

    # Build WHERE clauses for records that have at least one phone
    phone_filter = ' OR '.join(
        f'({f} IS NOT NULL AND {f} != \'\')' for f in PHONE_FIELDS
    )

    query = f"""
        SELECT *
        FROM {table}
        WHERE ({phone_filter})
    """
    params = {}

    if since:
        query += " AND date_modified > :since"
        params['since'] = since

    with pbi_engine.connect() as conn:
        result = conn.execute(text(query), params)
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]


# =============================================================================
# Zoom Payload Builder
# =============================================================================

def build_zoom_payload(
    record: Dict[str, Any],
    module: str,
    phones: List[str],
) -> Dict[str, Any]:
    """Build a Zoom External Contact payload from a CRM record."""
    first = record.get('first_name', '') or ''
    last = record.get('last_name', '') or ''
    name = f"{first} {last}".strip() or 'Unknown'

    email = record.get('email1', '') or record.get('email', '') or ''

    description = f"SugarCRM {module}: {record.get('sugar_id', 'N/A')}"

    payload = {
        'name': name[:200],
        'phone_numbers': phones,
        'description': description[:500],
        'auto_call_recorded': True,  # Auto-record calls to/from CRM contacts
    }
    if email:
        payload['email'] = email

    return payload


# =============================================================================
# Sync Logic
# =============================================================================

def sync_contacts_for_module(
    zoom_client: ZoomClient,
    backend_session,
    pbi_engine,
    module: str,
    since: Optional[datetime] = None,
    limit: Optional[int] = None,
    sugar_client: Optional[SugarCRMClient] = None,
) -> Tuple[int, int, int, int]:
    """Sync CRM records from one module to Zoom External Contacts.

    Returns:
        Tuple of (created, updated, skipped, errors).
    """
    crm_records = fetch_crm_records(pbi_engine, module, since)
    if limit:
        crm_records = crm_records[:limit]
        logger.info("Limited to first %d %s records (test mode)", limit, module)
    logger.info("Fetched %d %s records with phones", len(crm_records), module)

    created = 0
    updated = 0
    skipped = 0
    errors = 0
    processed = 0

    for record in tqdm(crm_records, desc=f"  Syncing {module}", unit="rec"):
        sugar_id = record.get('sugar_id')
        if not sugar_id:
            skipped += 1
            continue

        country_hint = infer_country_hint(record)
        phones = extract_phone_numbers(record, country_hint)
        if not phones:
            skipped += 1
            continue

        payload = build_zoom_payload(record, module, phones)

        # Check existing mapping
        existing = backend_session.query(ZoomContactSync).filter_by(
            sugar_id=sugar_id, sugar_module=module,
        ).first()

        try:
            if existing and existing.zoom_contact_id:
                # Check if anything changed
                old_phones = existing.phone_numbers or []
                old_name = existing.name_pushed or ''
                new_phones_set = set(phones)
                # Handle both new (flat str) and legacy ({number, label}) formats
                old_phones_set = set()
                if isinstance(old_phones, list):
                    for p in old_phones:
                        if isinstance(p, str):
                            old_phones_set.add(p)
                        elif isinstance(p, dict) and 'number' in p:
                            old_phones_set.add(p['number'])

                if new_phones_set == old_phones_set and payload['name'] == old_name:
                    skipped += 1
                    continue

                # Update existing contact
                zoom_client.update_external_contact(existing.zoom_contact_id, payload)
                existing.phone_numbers = phones
                existing.name_pushed = payload['name']
                existing.sync_status = 'synced'
                existing.error_message = None
                existing.last_synced_at = datetime.now(timezone.utc)
                updated += 1
            else:
                # Create new contact
                result = zoom_client.create_external_contact(payload)
                zoom_contact_id = result.get('external_contact_id') or result.get('id', '')

                if existing:
                    existing.zoom_contact_id = zoom_contact_id
                    existing.phone_numbers = phones
                    existing.name_pushed = payload['name']
                    existing.sync_status = 'synced'
                    existing.error_message = None
                    existing.last_synced_at = datetime.now(timezone.utc)
                else:
                    backend_session.add(ZoomContactSync(
                        sugar_id=sugar_id,
                        sugar_module=module,
                        zoom_contact_id=zoom_contact_id,
                        phone_numbers=phones,
                        name_pushed=payload['name'],
                        sync_status='synced',
                        last_synced_at=datetime.now(timezone.utc),
                    ))
                created += 1

                # Writeback the Zoom external_contact_id to SugarCRM custom field
                if sugar_client and zoom_contact_id:
                    try:
                        _, sugar_err = sugar_client.update_record(
                            module, sugar_id,
                            {'es_zoom_contact_id_c': zoom_contact_id},
                        )
                        if sugar_err:
                            logger.warning(
                                "SugarCRM writeback failed for %s/%s: %s",
                                module, sugar_id, sugar_err,
                            )
                    except Exception:
                        logger.exception(
                            "SugarCRM writeback exception for %s/%s",
                            module, sugar_id,
                        )

        except ZoomAPIError:
            logger.warning("Failed to sync %s %s to Zoom", module, sugar_id)
            if existing:
                existing.sync_status = 'error'
                existing.error_message = 'Zoom API error'
            else:
                backend_session.add(ZoomContactSync(
                    sugar_id=sugar_id,
                    sugar_module=module,
                    sync_status='error',
                    error_message='Zoom API error',
                ))
            errors += 1

        processed += 1
        if processed % BATCH_COMMIT_SIZE == 0:
            backend_session.commit()

    # Final commit for remaining records
    backend_session.commit()

    return created, updated, skipped, errors


def handle_deletions(
    zoom_client: ZoomClient,
    backend_session,
    pbi_engine,
) -> int:
    """Delete Zoom contacts whose CRM records no longer exist or have no phones.

    Returns number of deletions.
    """
    # Get all synced mappings
    synced = backend_session.query(ZoomContactSync).filter(
        ZoomContactSync.sync_status == 'synced',
        ZoomContactSync.zoom_contact_id.isnot(None),
    ).all()

    if not synced:
        return 0

    # Build set of valid CRM IDs (with phones) from both tables
    valid_ids: Set[Tuple[str, str]] = set()
    for module in ['Contacts', 'Leads']:
        records = fetch_crm_records(pbi_engine, module)
        for rec in records:
            sid = rec.get('sugar_id')
            if sid:
                valid_ids.add((sid, module))

    deleted = 0
    for mapping in tqdm(synced, desc="  Checking deletions", unit="rec"):
        if (mapping.sugar_id, mapping.sugar_module) not in valid_ids:
            try:
                zoom_client.delete_external_contact(mapping.zoom_contact_id)
                mapping.sync_status = 'deleted'
                mapping.zoom_contact_id = None
                deleted += 1
            except ZoomAPIError:
                logger.warning("Failed to delete Zoom contact %s", mapping.zoom_contact_id)
                mapping.sync_status = 'error'
                mapping.error_message = 'Delete failed'

            if deleted % BATCH_COMMIT_SIZE == 0:
                backend_session.commit()

    backend_session.commit()
    return deleted


# =============================================================================
# Sync State Management
# =============================================================================

def get_last_sync(backend_session, sync_name: str) -> Optional[datetime]:
    """Read last successful sync time from zoom_sync_state."""
    state = backend_session.query(ZoomSyncState).filter_by(sync_name=sync_name).first()
    if state and state.last_success_at:
        return state.last_success_at
    return None


def update_sync_state(backend_session, sync_name: str, records_processed: int):
    """Update sync state after a successful run."""
    now = datetime.now(timezone.utc)
    state = backend_session.query(ZoomSyncState).filter_by(sync_name=sync_name).first()
    if state:
        state.last_sync_at = now
        state.last_success_at = now
        state.records_processed = records_processed
        state.updated_at = now
    else:
        backend_session.add(ZoomSyncState(
            sync_name=sync_name,
            last_sync_at=now,
            last_success_at=now,
            records_processed=records_processed,
        ))
    backend_session.commit()


# =============================================================================
# Pipeline Runner
# =============================================================================

def run_pipeline(mode: str, limit: Optional[int] = None) -> Dict[str, int]:
    """Run the Zoom contacts sync pipeline.

    Args:
        mode: 'backfill' (all records) or 'auto' (incremental since last sync).
        limit: Optional cap on records per module (for testing).

    Returns:
        Summary dict with created/updated/skipped/errors/deleted counts.
    """
    # Mappings + CRM data both live in esa_pbi
    pbi_url = get_database_url('pbi')
    pbi_engine = create_engine(pbi_url)

    # Ensure tables exist (in esa_pbi)
    Base.metadata.create_all(pbi_engine, tables=[
        ZoomContactSync.__table__,
        ZoomSyncState.__table__,
    ])

    Session = sessionmaker(bind=pbi_engine)
    session = Session()

    try:
        # Determine time window
        since = None
        if mode == 'auto':
            since = get_last_sync(session, 'contacts_push')
            if since:
                logger.info("Auto mode: fetching records modified since %s", since.isoformat())
            else:
                logger.info("Auto mode: no previous sync found, processing all records")

        # Initialize Zoom client
        zoom_client = ZoomClient()

        # Initialize SugarCRM client (for writeback of zoom_contact_id to es_zoom_contact_id_c)
        sugar_client = SugarCRMClient.from_env()
        if not sugar_client.authenticate():
            logger.warning("SugarCRM authentication failed — writeback disabled")
            sugar_client = None

        # Sync Contacts
        print("[STAGE:SYNC] Syncing SugarCRM Contacts to Zoom")
        c_created, c_updated, c_skipped, c_errors = sync_contacts_for_module(
            zoom_client, session, pbi_engine, 'Contacts', since, limit=limit,
            sugar_client=sugar_client,
        )
        print(f"  Contacts: {c_created} created, {c_updated} updated, "
              f"{c_skipped} skipped, {c_errors} errors")

        # Sync Leads
        print("[STAGE:SYNC] Syncing SugarCRM Leads to Zoom")
        l_created, l_updated, l_skipped, l_errors = sync_contacts_for_module(
            zoom_client, session, pbi_engine, 'Leads', since, limit=limit,
            sugar_client=sugar_client,
        )
        print(f"  Leads: {l_created} created, {l_updated} updated, "
              f"{l_skipped} skipped, {l_errors} errors")

        # Handle deletions (only on full backfill — skip in limited test runs)
        deleted = 0
        if mode == 'backfill' and not limit:
            print("[STAGE:CLEANUP] Checking for deleted CRM records")
            deleted = handle_deletions(zoom_client, session, pbi_engine)
            print(f"  Deleted: {deleted} stale Zoom contacts")

        total_processed = (c_created + c_updated + l_created + l_updated + deleted)
        update_sync_state(session, 'contacts_push', total_processed)

        return {
            'created': c_created + l_created,
            'updated': c_updated + l_updated,
            'skipped': c_skipped + l_skipped,
            'errors': c_errors + l_errors,
            'deleted': deleted,
        }

    finally:
        try:
            if 'sugar_client' in locals() and sugar_client:
                sugar_client.logout()
        except Exception:
            pass
        session.close()
        pbi_engine.dispose()


# =============================================================================
# CLI
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description='Zoom Contacts Sync Pipeline — push SugarCRM contacts/leads to Zoom Phone',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m datalayer.zoom_contacts_sync --mode backfill
  python -m datalayer.zoom_contacts_sync --mode auto
        """,
    )
    parser.add_argument(
        '--mode',
        choices=['backfill', 'auto'],
        required=True,
        help='Extraction mode: backfill (all records), auto (incremental)',
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Cap records per module (for safe testing)',
    )
    return parser.parse_args()


def main():
    args = parse_args()

    print("=" * 70)
    print("Zoom Contacts Sync Pipeline")
    print("=" * 70)
    print(f"Mode: {args.mode.upper()}")
    if args.limit:
        print(f"Limit: {args.limit} per module (TEST MODE)")
    print(f"Target: Zoom Phone External Contacts")
    print("=" * 70)
    print("[STAGE:INIT] Zoom Contacts Sync")

    results = run_pipeline(args.mode, limit=args.limit)

    total = sum(results.values())
    print(f"[STAGE:COMPLETE] {total} records processed")
    print("\n" + "=" * 70)
    print("Pipeline completed!")
    print(f"  Created: {results['created']}")
    print(f"  Updated: {results['updated']}")
    print(f"  Skipped: {results['skipped']}")
    print(f"  Errors:  {results['errors']}")
    print(f"  Deleted: {results['deleted']}")
    print("=" * 70)


if __name__ == "__main__":
    main()
