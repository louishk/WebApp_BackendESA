"""
One-time SugarCRM Lead source/type → lead_source + lead_source_details_c mapping.

Rule (safe/unambiguous only — disagreements are parked for a later pass):

  | source_of_call_c | type_of_customer_c | action                                            |
  |------------------|--------------------|---------------------------------------------------|
  | set              | set, maps to same  | update both fields (lead_source_details_c = CSV)  |
  | set              | set, maps other LS | PARK (disagree, logged)                           |
  | set              | empty              | update both from CSV                              |
  | empty            | set                | lead_source from type, details = 'Direct'         |
  | empty            | empty              | skip                                              |

The legacy source_of_call_c / type_of_customer_c fields are never modified.
Idempotent: leads where both target fields already match are skipped.

Usage:
  python datalayer/leads_source_of_call_mapping.py                       # dry-run
  python datalayer/leads_source_of_call_mapping.py --execute             # actual update
  python datalayer/leads_source_of_call_mapping.py --csv PATH            # custom CSV
  python datalayer/leads_source_of_call_mapping.py --only "Google"       # one source value (source-pass only)
  python datalayer/leads_source_of_call_mapping.py --max-records 20000   # cap total leads scanned
"""

import argparse
import csv
import logging
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from common.sugarcrm_client import SugarCRMClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

DEFAULT_CSV = Path(__file__).resolve().parents[3] / 'temp' / 'source_of_call_mapping_1.csv'

FIELDS = ['id', 'source_of_call_c', 'type_of_customer_c', 'lead_source', 'lead_source_details_c']

BULK_SIZE = 20
BULK_DELAY_SECONDS = 0.2
MAX_RETRIES = 3

# Type-of-customer → lead_source
TYPE_TO_LEAD_SOURCE = {
    'Web': 'Website',
    'Phone Call': 'Phone_call',
    'Walk in': 'Walk_In',
    'Walk-in': 'Walk_In',
    'Appt Visit': 'Walk_In',
    'Outdoor': 'Walk_In',
    'WhatsApp': 'Chatbot',
    'Whatsapp': 'Chatbot',
    'LiveChat': 'Chatbot',
    'Chatbot_Drop_Off': 'Chatbot',
    'Kakao': 'Chatbot',
    'Kakaotalk': 'Chatbot',
    'Email': 'Email',
    'ExpressApp': 'Mobile_app',
    'Online_Reservation': 'Booking_engine',
    'Yes_Reservation': 'Booking_engine',
}
FALLBACK_DETAILS = 'Direct'


def load_mapping(csv_path: Path) -> dict[str, tuple[str, str]]:
    """Load the CSV: source_of_call_c -> (lead_source, lead_source_details_c)."""
    out: dict[str, tuple[str, str]] = {}
    with csv_path.open('r', encoding='utf-8-sig', newline='') as f:
        for raw in csv.DictReader(f):
            source = (raw.get('source_of_call_c') or '').strip()
            lead_source = (raw.get('lead_source') or '').strip()
            details = (raw.get('lead_source_details_c') or '').strip()
            if source and lead_source and details:
                out[source] = (lead_source, details)
    return out


def _flush_bulk(client: SugarCRMClient, pending: list[tuple[str, dict]], summary: dict) -> None:
    if not pending:
        return
    body = {
        'requests': [
            {'url': f'/v11/Leads/{lead_id}', 'method': 'PUT', 'data': payload}
            for lead_id, payload in pending
        ]
    }

    last_error = None
    result = None
    for attempt in range(1, MAX_RETRIES + 1):
        result, error = client.post('bulk', data=body)
        if not error:
            last_error = None
            break
        last_error = error
        if attempt < MAX_RETRIES:
            wait = 2 ** attempt
            logger.warning("Bulk PUT failed (attempt %d/%d, %d records): %s — retrying in %ds",
                           attempt, MAX_RETRIES, len(pending), error, wait)
            time.sleep(wait)

    if last_error or not isinstance(result, list):
        logger.error("Bulk PUT permanently failed for %d records: %s",
                     len(pending), last_error or 'unexpected response shape')
        summary['errors'] += len(pending)
        pending.clear()
        return

    for (lead_id, _), resp in zip(pending, result):
        status = resp.get('status') if isinstance(resp, dict) else None
        if status in (200, 201):
            summary['updated'] += 1
        else:
            contents = (resp or {}).get('contents') if isinstance(resp, dict) else resp
            logger.error("Bulk sub-request failed for lead %s: status=%s contents=%s",
                         lead_id, status, str(contents)[:300])
            summary['errors'] += 1

    pending.clear()
    time.sleep(BULK_DELAY_SECONDS)


def _new_summary() -> dict:
    return {
        'found': 0,
        'already_correct': 0,
        'updated': 0,
        'errors': 0,
        'parked_disagree': 0,        # source+type disagree
        'parked_unmapped_type': 0,   # type present but no rule
        'parked_unmapped_source': 0, # source present but not in CSV (should be 0)
        'skipped_empty': 0,          # both fields empty
    }


def decide(record: dict, source_map: dict[str, tuple[str, str]]) -> tuple[str, str, str] | None:
    """Return (lead_source, details, reason) for a record, or None to skip with a reason tag.

    The caller interprets a None return by reading the reason from a separate call.
    To keep it simple we return a tuple (lead_source, details, 'ok'|'park_*'|'skip_*').
    lead_source/details are empty strings when no update should happen.
    """
    src = (record.get('source_of_call_c') or '').strip()
    typ = (record.get('type_of_customer_c') or '').strip()

    has_src = bool(src)
    has_typ = bool(typ)

    source_ls = None
    source_details = None
    if has_src:
        if src not in source_map:
            return '', '', 'parked_unmapped_source'
        source_ls, source_details = source_map[src]

    type_ls = None
    if has_typ:
        type_ls = TYPE_TO_LEAD_SOURCE.get(typ)
        if type_ls is None:
            # unmapped type — if we have source, fall through and use source; else park.
            if not has_src:
                return '', '', 'parked_unmapped_type'

    # Case matrix
    if has_src and has_typ and type_ls is not None:
        if source_ls == type_ls:
            return source_ls, source_details, 'ok'
        return '', '', 'parked_disagree'
    if has_src and has_typ and type_ls is None:
        # type is present but unmapped, source is the only signal
        return source_ls, source_details, 'ok'
    if has_src and not has_typ:
        return source_ls, source_details, 'ok'
    if not has_src and has_typ and type_ls is not None:
        return type_ls, FALLBACK_DETAILS, 'ok'
    return '', '', 'skipped_empty'


def process_all(
    client: SugarCRMClient,
    source_map: dict[str, tuple[str, str]],
    execute: bool,
    max_records: int | None,
    only_source: str | None,
) -> tuple[dict, dict]:
    """Single pass over Leads with any source or any type set. Returns (summary, disagree_counts)."""
    summary = _new_summary()
    disagree_counts: dict[tuple[str, str], int] = defaultdict(int)

    # Filter: at least one of source_of_call_c or type_of_customer_c is set.
    # Using $not_null OR $not_equals '' pair handles both NULL and empty strings.
    if only_source:
        filter_expr = [{'source_of_call_c': {'$equals': only_source}}]
        logger.info("Single-source mode: source_of_call_c=%r", only_source)
    else:
        filter_expr = [
            {'$or': [
                {'$and': [
                    {'source_of_call_c': {'$not_null': ''}},
                    {'source_of_call_c': {'$not_equals': ''}},
                ]},
                {'$and': [
                    {'type_of_customer_c': {'$not_null': ''}},
                    {'type_of_customer_c': {'$not_equals': ''}},
                ]},
            ]}
        ]

    pending: list[tuple[str, dict]] = []
    scanned = 0
    stop = False

    for batch in client.fetch_all_records(
        module='Leads',
        filter_expr=filter_expr,
        fields=FIELDS,
        order_by='date_modified:ASC',
    ):
        for record in batch:
            if max_records is not None and scanned >= max_records:
                stop = True
                break

            scanned += 1
            summary['found'] += 1
            lead_id = record.get('id')

            target_ls, target_details, reason = decide(record, source_map)

            if reason == 'ok':
                if (record.get('lead_source') == target_ls
                        and record.get('lead_source_details_c') == target_details):
                    summary['already_correct'] += 1
                    continue

                if not execute:
                    continue

                pending.append((lead_id, {
                    'lead_source': target_ls,
                    'lead_source_details_c': target_details,
                }))
                if len(pending) >= BULK_SIZE:
                    _flush_bulk(client, pending, summary)
                continue

            # Non-"ok" reasons -> bucket counters
            summary[reason] = summary.get(reason, 0) + 1
            if reason == 'parked_disagree':
                src = (record.get('source_of_call_c') or '').strip()
                typ = (record.get('type_of_customer_c') or '').strip()
                disagree_counts[(src, typ)] += 1

            # Progress heartbeat every 5000
        if scanned and scanned % 5000 == 0:
            logger.info("  ...scanned=%d  found=%d  updated=%d  parked_disagree=%d  parked_unmapped_type=%d",
                        scanned, summary['found'], summary['updated'],
                        summary['parked_disagree'], summary['parked_unmapped_type'])
        if stop:
            break

    if execute:
        _flush_bulk(client, pending, summary)

    return summary, disagree_counts


def main():
    parser = argparse.ArgumentParser(
        description="Map source_of_call_c + type_of_customer_c → lead_source + lead_source_details_c on SugarCRM Leads.",
    )
    parser.add_argument('--execute', action='store_true', default=False,
                        help="Actually perform updates (default is dry-run).")
    parser.add_argument('--csv', type=Path, default=DEFAULT_CSV,
                        help=f"Path to mapping CSV (default: {DEFAULT_CSV}).")
    parser.add_argument('--only', type=str, default=None,
                        help="Process only this source_of_call_c value (exact match).")
    parser.add_argument('--max-records', type=int, default=None,
                        help="Cap total leads scanned (testing).")
    parser.add_argument('--park-report', type=Path,
                        default=Path(__file__).resolve().parents[3] / 'temp' / 'logs' / 'parked_disagree.csv',
                        help="Where to write the (source, type, count) CSV of parked disagreements.")
    args = parser.parse_args()

    if not args.csv.exists():
        logger.error("Mapping CSV not found: %s", args.csv)
        sys.exit(1)

    source_map = load_mapping(args.csv)
    logger.info("Loaded %d source_of_call_c rows from %s", len(source_map), args.csv)
    logger.info("Type-rules: %d type_of_customer_c values mapped; fallback details=%r",
                len(TYPE_TO_LEAD_SOURCE), FALLBACK_DETAILS)

    if args.execute:
        logger.info("=== MAPPING RUN — EXECUTE MODE ===")
    else:
        logger.info("=== MAPPING RUN — DRY RUN (pass --execute to apply changes) ===")
    if args.max_records is not None:
        logger.info("Global cap: stop after scanning %d leads.", args.max_records)

    try:
        client = SugarCRMClient.from_env()
    except Exception:
        logger.exception("Failed to initialise SugarCRM client")
        sys.exit(1)

    if not client.authenticate():
        logger.error("SugarCRM authentication failed — aborting")
        sys.exit(1)

    try:
        summary, disagree_counts = process_all(
            client=client,
            source_map=source_map,
            execute=args.execute,
            max_records=args.max_records,
            only_source=args.only,
        )
    except Exception:
        logger.exception("Unexpected error during mapping run")
        sys.exit(1)
    finally:
        client.logout()

    logger.info("=== MAPPING RUN COMPLETE ===")
    logger.info("  Total leads found:              %d", summary['found'])
    logger.info("  Already had correct values:     %d", summary['already_correct'])
    logger.info("  Parked — source/type disagree:  %d", summary['parked_disagree'])
    logger.info("  Parked — unmapped type:         %d", summary['parked_unmapped_type'])
    logger.info("  Parked — unmapped source:       %d", summary['parked_unmapped_source'])
    logger.info("  Skipped — empty both:           %d", summary['skipped_empty'])
    if args.execute:
        logger.info("  Updated:                        %d", summary['updated'])
        logger.info("  Errors:                         %d", summary['errors'])
    else:
        will_update = summary['found'] - summary['already_correct'] - summary['parked_disagree'] \
                      - summary['parked_unmapped_type'] - summary['parked_unmapped_source'] \
                      - summary['skipped_empty']
        logger.info("  Would update (dry-run):         %d", will_update)

    if disagree_counts:
        args.park_report.parent.mkdir(parents=True, exist_ok=True)
        with args.park_report.open('w', encoding='utf-8', newline='') as f:
            w = csv.writer(f)
            w.writerow(['source_of_call_c', 'type_of_customer_c', 'count',
                        'source_lead_source', 'type_lead_source'])
            for (src, typ), n in sorted(disagree_counts.items(), key=lambda x: -x[1]):
                src_ls = source_map.get(src, ('?', ''))[0]
                typ_ls = TYPE_TO_LEAD_SOURCE.get(typ, '?')
                w.writerow([src, typ, n, src_ls, typ_ls])
        logger.info("Wrote parked-disagreement report → %s (%d combinations)",
                    args.park_report, len(disagree_counts))

    if args.execute and summary['errors'] > 0:
        logger.warning("%d lead(s) failed to update", summary['errors'])
        sys.exit(2)


if __name__ == '__main__':
    main()
