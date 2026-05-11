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
        print("[STAGE:INIT] ECRIOutcomeTracking")
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
        print("[STAGE:FETCH] Checking executed batches for outcomes")
        print(f"[ECRI Outcome Tracking] Found {len(batches)} executed batches")

        for batch_row in batches:
            batch_id = batch_row[0]
            executed_at = batch_row[1]
            attribution_days = batch_row[2] or 90

            executed_date = executed_at.date() if hasattr(executed_at, 'date') else executed_at
            window_end = executed_date + timedelta(days=attribution_days)

            print(f"\n[Batch {batch_id}] Executed: {executed_date}, Window ends: {window_end}")

            # Get pending ledgers joined with current SMD status in a single query.
            # LEFT JOIN to the view so ledgers absent from active pipeline come back
            # as NULL (= tenant moved out).
            pending_sql = text("""
                SELECT bl.site_id, bl.ledger_id, bl.notice_date,
                       v."dSchedOut" AS sched_out,
                       (v."SiteID" IS NULL) AS is_absent
                FROM ecri_batch_ledgers bl
                LEFT JOIN ecri_outcomes o
                    ON bl.batch_id = o.batch_id
                    AND bl.site_id = o.site_id
                    AND bl.ledger_id = o.ledger_id
                LEFT JOIN vw_ecri_eligible_ledgers v
                    ON v."SiteID" = bl.site_id
                    AND v."LedgerID" = bl.ledger_id
                WHERE bl.batch_id = :batch_id
                  AND bl.api_status = 'success'
                  AND o.id IS NULL
            """)
            pending = session.execute(pending_sql, {'batch_id': batch_id}).fetchall()
            print(f"[Batch {batch_id}] {len(pending)} ledgers pending outcome check")

            rows_to_insert = []
            for led_row in pending:
                site_id = led_row[0]
                ledger_id = led_row[1]
                notice_date = led_row[2]
                sched_out = led_row[3]
                is_absent = led_row[4]

                outcome_type = None
                outcome_date = None
                days_after = None

                if is_absent:
                    # Absent from live pipeline → tenant has moved out.
                    # Best-effort outcome_date = today; attribution math still works.
                    outcome_type = 'moved_out'
                    outcome_date = today
                    if notice_date:
                        days_after = (outcome_date - notice_date).days
                elif sched_out is not None and sched_out.date() <= window_end:
                    outcome_type = 'scheduled_out'
                    outcome_date = sched_out.date() if hasattr(sched_out, 'date') else sched_out
                    if notice_date:
                        days_after = (outcome_date - notice_date).days
                elif today > window_end:
                    # Attribution window expired, tenant still there
                    outcome_type = 'stayed'
                    outcome_date = window_end
                    if notice_date:
                        days_after = (window_end - notice_date).days

                if outcome_type:
                    months_at_new = None
                    if notice_date and outcome_date:
                        months_at_new = max(0,
                            (outcome_date.year - notice_date.year) * 12 +
                            (outcome_date.month - notice_date.month)
                        )
                    rows_to_insert.append({
                        'batch_id': batch_id,
                        'site_id': site_id,
                        'ledger_id': ledger_id,
                        'outcome_date': outcome_date,
                        'outcome_type': outcome_type,
                        'days_after': days_after,
                        'months_at_new': months_at_new,
                    })

            if rows_to_insert:
                insert_sql = text("""
                    INSERT INTO ecri_outcomes
                        (batch_id, site_id, ledger_id, outcome_date,
                         outcome_type, days_after_notice, months_at_new_rent)
                    VALUES
                        (:batch_id, :site_id, :ledger_id, :outcome_date,
                         :outcome_type, :days_after, :months_at_new)
                    ON CONFLICT (batch_id, site_id, ledger_id, outcome_type) DO NOTHING
                """)
                session.execute(insert_sql, rows_to_insert)
                records_processed += len(rows_to_insert)
                print(f"[Batch {batch_id}] Inserted {len(rows_to_insert)} outcomes")

            session.commit()

        print(f"[STAGE:COMPLETE] {records_processed} records")
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
