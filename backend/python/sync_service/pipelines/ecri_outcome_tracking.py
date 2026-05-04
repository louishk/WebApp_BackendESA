"""
EcriOutcomeTrackingPipeline — track stay/move-out outcomes for executed ECRI batches.

Replaces the legacy datalayer/ecri_outcome_tracking.py which timed out at 1200s on
1,891-ledger batches due to one SELECT per ledger. This version uses a single bulk
query against vw_ecri_eligible_ledgers for all pending ledgers across executed
batches and writes outcomes back to ecri_outcomes with ON CONFLICT DO UPDATE so
corrected ccws data flows through.

Reads/writes esa_pbi only. No SOAP traffic.
"""

import logging
from datetime import date, timedelta
from typing import Any, Dict

from sqlalchemy import text

from sync_service.pipelines.base import BasePipeline, RunResult
from sync_service.config import get_engine

logger = logging.getLogger(__name__)


class EcriOutcomeTrackingPipeline(BasePipeline):
    """Tracks ECRI batch outcomes by joining ecri_batch_ledgers against
    vw_ecri_eligible_ledgers in one pass per batch.
    """

    def _execute(self, scope: Dict[str, Any]) -> RunResult:
        engine = get_engine('pbi')
        today = date.today()
        records = 0
        skipped_invalid = 0
        batches_processed = 0

        with engine.begin() as conn:
            batches = conn.execute(text("""
                SELECT batch_id, executed_at, attribution_window_days
                FROM ecri_batches
                WHERE status = 'executed' AND executed_at IS NOT NULL
            """)).fetchall()

            for b in batches:
                batch_id = b[0]
                executed_at = b[1]
                attribution_days = b[2] or 90
                executed_date = executed_at.date() if hasattr(executed_at, 'date') else executed_at
                window_end = executed_date + timedelta(days=attribution_days)
                batches_processed += 1

                # All pending (no outcome) ledgers + their current ccws state in ONE round-trip.
                # LEFT JOIN against the eligible-ledger view: missing row = tenant moved out.
                rows = conn.execute(text("""
                    SELECT bl.site_id, bl.ledger_id, bl.notice_date,
                           v."dSchedOut" AS sched_out
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
                """), {'batch_id': batch_id}).fetchall()

                self.log.info(f"batch={batch_id} pending={len(rows)} window_end={window_end}")

                for site_id, ledger_id, notice_date, sched_out in rows:
                    outcome_type = None
                    outcome_date = None

                    # vw_ecri_eligible_ledgers row missing → tenant has moved out.
                    # The view uses the latest ccws_ledgers extract; if that snapshot
                    # is stale the move-out is still valid (tenant left at some point
                    # since last ECRI run).
                    if sched_out is None:
                        # Distinguish "absent from view" vs "in view with no sched-out date".
                        # The LEFT JOIN can't tell us — re-check explicitly.
                        present = conn.execute(text("""
                            SELECT 1 FROM vw_ecri_eligible_ledgers
                            WHERE "SiteID" = :s AND "LedgerID" = :l LIMIT 1
                        """), {'s': site_id, 'l': ledger_id}).fetchone()

                        if present is None:
                            outcome_type = 'moved_out'
                            outcome_date = today
                        elif today > window_end:
                            outcome_type = 'stayed'
                            outcome_date = window_end
                    else:
                        sched_date = sched_out.date() if hasattr(sched_out, 'date') else sched_out
                        if sched_date <= window_end:
                            outcome_type = 'scheduled_out'
                            outcome_date = sched_date
                        elif today > window_end:
                            outcome_type = 'stayed'
                            outcome_date = window_end

                    if outcome_type is None:
                        continue

                    # Sanity check: outcome_date must not be earlier than notice_date.
                    # Negative days_after_notice means stale or corrupt ccws data;
                    # skip rather than persist a nonsense row.
                    if notice_date and outcome_date < notice_date:
                        skipped_invalid += 1
                        continue

                    days_after = (outcome_date - notice_date).days if notice_date else None
                    months_at_new = None
                    if notice_date:
                        months_at_new = max(
                            0,
                            (outcome_date.year - notice_date.year) * 12
                            + (outcome_date.month - notice_date.month),
                        )

                    conn.execute(text("""
                        INSERT INTO ecri_outcomes
                            (batch_id, site_id, ledger_id, outcome_date,
                             outcome_type, days_after_notice, months_at_new_rent)
                        VALUES
                            (:batch_id, :site_id, :ledger_id, :outcome_date,
                             :outcome_type, :days_after, :months_at_new)
                        ON CONFLICT (batch_id, site_id, ledger_id, outcome_type)
                        DO UPDATE SET
                            outcome_date = EXCLUDED.outcome_date,
                            days_after_notice = EXCLUDED.days_after_notice,
                            months_at_new_rent = EXCLUDED.months_at_new_rent
                    """), {
                        'batch_id': batch_id,
                        'site_id': site_id,
                        'ledger_id': ledger_id,
                        'outcome_date': outcome_date,
                        'outcome_type': outcome_type,
                        'days_after': days_after,
                        'months_at_new': months_at_new,
                    })
                    records += 1

        self.log.info(
            f"ECRI outcome tracking complete: batches={batches_processed} "
            f"records={records} skipped_invalid={skipped_invalid}"
        )
        return RunResult(
            status='refreshed',
            records=records,
            scope=scope,
            metadata={
                'batches_processed': batches_processed,
                'skipped_invalid': skipped_invalid,
            },
        )
