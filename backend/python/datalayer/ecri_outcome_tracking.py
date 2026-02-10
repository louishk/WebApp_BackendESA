"""
ECRI Outcome Tracking Pipeline.

Runs weekly (or as configured) to check if any ECRI'd ledgers have moved out
within the attribution window. Records outcomes in ecri_outcomes table.

Usage:
    python -m datalayer.ecri_outcome_tracking --mode auto
"""

import sys
from pathlib import Path
from datetime import datetime, date, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from common.config_loader import get_database_url


def run(mode='auto', **kwargs):
    """
    Track outcomes for executed ECRI batches.

    For each executed batch within the attribution window:
    - Check cc_ledgers for move-outs (dMovedOut IS NOT NULL)
    - Check cc_ledgers for scheduled move-outs (dSchedOut IS NOT NULL)
    - Record as 'stayed' if still active after attribution window expires
    """
    pbi_url = get_database_url('pbi')
    engine = create_engine(pbi_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    today = date.today()
    records_processed = 0

    try:
        print(f"[ECRI Outcome Tracking] Starting at {datetime.now().isoformat()}")
        print(f"[ECRI Outcome Tracking] Mode: {mode}")

        # Get executed batches within their attribution windows
        batches_sql = text("""
            SELECT batch_id, executed_at, attribution_window_days
            FROM ecri_batches
            WHERE status = 'executed'
              AND executed_at IS NOT NULL
        """)
        batches = session.execute(batches_sql).fetchall()
        print(f"[ECRI Outcome Tracking] Found {len(batches)} executed batches")

        for batch_row in batches:
            batch_id = batch_row[0]
            executed_at = batch_row[1]
            attribution_days = batch_row[2] or 90

            executed_date = executed_at.date() if hasattr(executed_at, 'date') else executed_at
            window_end = executed_date + timedelta(days=attribution_days)

            print(f"\n[Batch {batch_id}] Executed: {executed_date}, Window ends: {window_end}")

            # Get batch ledgers that don't have outcomes yet
            pending_sql = text("""
                SELECT bl.site_id, bl.ledger_id, bl.notice_date
                FROM ecri_batch_ledgers bl
                LEFT JOIN ecri_outcomes o
                    ON bl.batch_id = o.batch_id
                    AND bl.site_id = o.site_id
                    AND bl.ledger_id = o.ledger_id
                WHERE bl.batch_id = :batch_id
                  AND bl.api_status = 'success'
                  AND o.id IS NULL
            """)
            pending = session.execute(pending_sql, {'batch_id': batch_id}).fetchall()
            print(f"[Batch {batch_id}] {len(pending)} ledgers pending outcome check")

            for led_row in pending:
                site_id = led_row[0]
                ledger_id = led_row[1]
                notice_date = led_row[2]

                # Check current ledger status in cc_ledgers
                status_sql = text("""
                    SELECT "dMovedOut", "dSchedOut"
                    FROM cc_ledgers
                    WHERE "SiteID" = :site_id AND "LedgerID" = :ledger_id
                    LIMIT 1
                """)
                status = session.execute(status_sql, {
                    'site_id': site_id,
                    'ledger_id': ledger_id
                }).fetchone()

                if not status:
                    continue

                moved_out = status[0]
                sched_out = status[1]

                outcome_type = None
                outcome_date = None
                days_after = None

                if moved_out is not None:
                    # Tenant has moved out
                    outcome_type = 'moved_out'
                    outcome_date = moved_out.date() if hasattr(moved_out, 'date') else moved_out
                    if notice_date:
                        days_after = (outcome_date - notice_date).days
                elif sched_out is not None and sched_out.date() <= window_end:
                    # Tenant has scheduled move-out within window
                    outcome_type = 'scheduled_out'
                    outcome_date = sched_out.date() if hasattr(sched_out, 'date') else sched_out
                    if notice_date:
                        days_after = (outcome_date - notice_date).days
                elif today > window_end:
                    # Attribution window has expired and tenant is still there
                    outcome_type = 'stayed'
                    outcome_date = window_end
                    if notice_date:
                        days_after = (window_end - notice_date).days

                if outcome_type:
                    # Calculate months at new rent
                    months_at_new = None
                    if notice_date and outcome_date:
                        months_at_new = max(0,
                            (outcome_date.year - notice_date.year) * 12 +
                            (outcome_date.month - notice_date.month)
                        )

                    insert_sql = text("""
                        INSERT INTO ecri_outcomes
                            (batch_id, site_id, ledger_id, outcome_date,
                             outcome_type, days_after_notice, months_at_new_rent)
                        VALUES
                            (:batch_id, :site_id, :ledger_id, :outcome_date,
                             :outcome_type, :days_after, :months_at_new)
                        ON CONFLICT (batch_id, site_id, ledger_id, outcome_type) DO NOTHING
                    """)
                    session.execute(insert_sql, {
                        'batch_id': batch_id,
                        'site_id': site_id,
                        'ledger_id': ledger_id,
                        'outcome_date': outcome_date,
                        'outcome_type': outcome_type,
                        'days_after': days_after,
                        'months_at_new': months_at_new,
                    })
                    records_processed += 1
                    print(f"  Ledger {site_id}/{ledger_id}: {outcome_type}")

            session.commit()

        print(f"\n[ECRI Outcome Tracking] Complete. {records_processed} outcomes recorded.")
        return records_processed

    except Exception as e:
        session.rollback()
        print(f"[ECRI Outcome Tracking] ERROR: {e}")
        raise
    finally:
        session.close()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='ECRI Outcome Tracking')
    parser.add_argument('--mode', default='auto', help='Run mode')
    args = parser.parse_args()
    run(mode=args.mode)
