"""
One-time SugarCRM Lead status migration.

Maps legacy status values to new convention:
  Assigned   → Contacted
  In Process → Qualified
  Converted  → Signed Up
  Dead       → Lost
  Recycled   → Nurture

New status stays as-is. Any other unrecognised status is skipped.

Usage:
  python datalayer/leads_status_migration.py              # dry-run (default)
  python datalayer/leads_status_migration.py --execute    # actual update
"""

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.sugarcrm_client import SugarCRMClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

STATUS_MAP = {
    'Assigned': 'Contacted',
    'In Process': 'Qualified',
    'Converted': 'Signed Up',
    'Dead': 'Lost',
    'Recycled': 'Nurture',
}

# Fields we need — id and status only
FIELDS = ['id', 'status']

# Brief pause between updates to avoid hammering the API
UPDATE_DELAY_SECONDS = 0.05
MAX_RETRIES = 3


def migrate_status(client: SugarCRMClient, old_status: str, new_status: str, execute: bool) -> dict:
    """
    Process all leads with old_status and update them to new_status.

    Returns a summary dict with keys: old_status, new_status, found, updated, errors.
    """
    summary = {
        'old_status': old_status,
        'new_status': new_status,
        'found': 0,
        'updated': 0,
        'errors': 0,
    }

    filter_expr = [{'status': {'$equals': old_status}}]

    logger.info("Processing status '%s' → '%s' (execute=%s)", old_status, new_status, execute)

    for batch in client.fetch_all_records(
        module='Leads',
        filter_expr=filter_expr,
        fields=FIELDS,
        order_by='date_modified:ASC',
    ):
        for record in batch:
            summary['found'] += 1
            lead_id = record.get('id')

            if not execute:
                continue

            # Attempt update with retries
            for attempt in range(1, MAX_RETRIES + 1):
                result, error = client.update_record('Leads', lead_id, {'status': new_status})
                if error:
                    if attempt < MAX_RETRIES:
                        wait = 2 ** attempt
                        logger.warning(
                            "Update failed for lead %s (attempt %d/%d): %s — retrying in %ds",
                            lead_id, attempt, MAX_RETRIES, error, wait,
                        )
                        time.sleep(wait)
                    else:
                        logger.error(
                            "Update permanently failed for lead %s after %d attempts: %s",
                            lead_id, MAX_RETRIES, error,
                        )
                        summary['errors'] += 1
                else:
                    summary['updated'] += 1
                    logger.debug("Updated lead %s: '%s' → '%s'", lead_id, old_status, new_status)
                    time.sleep(UPDATE_DELAY_SECONDS)
                    break

    if not execute:
        logger.info(
            "  [DRY RUN] Would update %d lead(s) from '%s' to '%s'",
            summary['found'], old_status, new_status,
        )
    else:
        logger.info(
            "  Done: found=%d updated=%d errors=%d",
            summary['found'], summary['updated'], summary['errors'],
        )

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Migrate legacy SugarCRM Lead statuses to new convention.",
    )
    parser.add_argument(
        '--execute',
        action='store_true',
        default=False,
        help="Actually perform updates (default is dry-run, which only counts affected leads).",
    )
    args = parser.parse_args()

    execute = args.execute

    if execute:
        logger.info("=== LEADS STATUS MIGRATION — EXECUTE MODE ===")
    else:
        logger.info("=== LEADS STATUS MIGRATION — DRY RUN (pass --execute to apply changes) ===")

    try:
        client = SugarCRMClient.from_env()
    except Exception:
        logger.exception("Failed to initialise SugarCRM client")
        sys.exit(1)

    if not client.authenticate():
        logger.error("SugarCRM authentication failed — aborting")
        sys.exit(1)

    all_summaries = []
    total_found = 0
    total_updated = 0
    total_errors = 0

    try:
        for old_status, new_status in STATUS_MAP.items():
            summary = migrate_status(client, old_status, new_status, execute)
            all_summaries.append(summary)
            total_found += summary['found']
            total_updated += summary['updated']
            total_errors += summary['errors']
    except Exception:
        logger.exception("Unexpected error during migration")
        sys.exit(1)
    finally:
        client.logout()

    logger.info("=== MIGRATION COMPLETE ===")
    logger.info("  Total leads found:   %d", total_found)
    if execute:
        logger.info("  Total leads updated: %d", total_updated)
        logger.info("  Total errors:        %d", total_errors)
    else:
        logger.info("  (dry-run — no records were modified)")

    if execute and total_errors > 0:
        logger.warning("%d lead(s) failed to update — check logs above for details", total_errors)
        sys.exit(2)


if __name__ == '__main__':
    main()
