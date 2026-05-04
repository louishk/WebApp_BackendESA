"""
ecri_year_impact — calendar-year revenue impact for ECRI batches.

Two views:
  1. Portfolio: aggregate across all executed batches.
  2. Per-batch: scoped to one batch.

Per-tenant per-month contribution uses day-proration:
  days_active_in_month / days_in_month * increase_amt_sgd

- Active portion starts at MAX(effective_date, month_start).
- Active portion ends at MIN(outcome_date - 1 day, month_end) or
  MIN(today, month_end) for forecast months (assumes current active status persists).
- Outcomes: ecri_outcomes with outcome_type in ('moved_out','scheduled_out').
- Status of month:
    'actual'   — month_end <= today
    'forecast' — month_start > today (not yet elapsed)
    'partial'  — month contains today (mix of actual days + forecast days)

Pure module: takes SQLAlchemy session and returns dicts. No Flask imports.
"""

from datetime import date, timedelta
from calendar import monthrange
from decimal import Decimal

from sqlalchemy import text


def _month_bounds(year, month):
    last = monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last), last


def compute_year_impact(session, year, batch_id=None, today=None):
    """Return month-by-month year impact in SGD.

    Args:
        session: SQLAlchemy Session against esa_pbi.
        year: int, calendar year (e.g., 2026).
        batch_id: UUID string or None for portfolio-wide.
        today: date override for testing; defaults to date.today().

    Returns a dict:
        {
          "year": int,
          "batch_id": str or None,
          "months": [
             {"month": "2026-04", "planned_sgd": float, "actual_sgd": float,
              "churn_loss_sgd": float, "status": "actual"|"partial"|"forecast"},
             ...
          ],
          "ytd_actual_sgd": float,
          "eoy_projection_sgd": float,
          "batches_covered": int,
          "ledgers_covered": int,
        }
    """
    today = today or date.today()

    # Load all successfully pushed ledgers with their effective_date, FX, outcomes.
    # For a single batch, scope down; otherwise pull all executed batches.
    params = {'year_start': date(year, 1, 1), 'year_end': date(year, 12, 31)}
    batch_filter = ''
    if batch_id:
        batch_filter = 'AND bl.batch_id = :batch_id'
        params['batch_id'] = batch_id

    rows = session.execute(text(f"""
        SELECT
            bl.id,
            bl.batch_id,
            bl.site_id,
            bl.ledger_id,
            bl.currency,
            bl.increase_amt,
            bl.old_rent,
            bl.effective_date,
            (SELECT MIN(outcome_date) FROM ecri_outcomes o
              WHERE o.batch_id = bl.batch_id AND o.site_id = bl.site_id
                AND o.ledger_id = bl.ledger_id
                AND o.outcome_type IN ('moved_out', 'scheduled_out')
            ) AS outcome_date
          FROM ecri_batch_ledgers bl
          JOIN ecri_batches b ON b.batch_id = bl.batch_id
         WHERE b.status = 'executed'
           AND bl.api_status = 'success'
           AND bl.effective_date IS NOT NULL
           AND bl.effective_date <= :year_end
           {batch_filter}
    """), params).fetchall()

    # FX rates — latest snapshot. SGD = 1.0, others = native/rate.
    fx_rows = session.execute(text(
        "SELECT target_currency, rate FROM fx_rates "
        "WHERE rate_date = (SELECT MAX(rate_date) FROM fx_rates)"
    )).fetchall()
    fx = {r[0]: float(r[1]) for r in fx_rows}
    fx['SGD'] = 1.0

    def to_sgd(amt, cur):
        rate = fx.get(cur or 'SGD', 1.0)
        return float(amt) / rate if amt is not None else 0.0

    months = []
    batches_covered = set()
    ledgers_covered = 0
    ytd_actual = 0.0
    eoy_projection = 0.0

    for m in range(1, 13):
        m_start, m_end, days_in_month = _month_bounds(year, m)
        if m_end < today:
            status = 'actual'
        elif m_start > today:
            status = 'forecast'
        else:
            status = 'partial'

        planned_sgd = 0.0
        actual_sgd = 0.0
        churn_loss_sgd = 0.0

        for r in rows:
            eff = r.effective_date
            if not eff or eff > m_end:
                # Tenant's increase hadn't taken effect by end of this month
                continue

            inc_sgd = to_sgd(r.increase_amt, r.currency)
            old_rent_sgd = to_sgd(r.old_rent, r.currency)
            active_start = max(eff, m_start)

            # Planned (uplift) = day-prorated increase as if never churned
            if m_end >= active_start:
                planned_days = (m_end - active_start).days + 1
                planned_sgd += inc_sgd * planned_days / days_in_month

            # Effective end for the tenant in this month, accounting for churn only
            effective_end = m_end
            if r.outcome_date and r.outcome_date <= m_end:
                effective_end = r.outcome_date - timedelta(days=1)

            # Churn loss = full base rent (old_rent) day-prorated for days lost
            # to an outcome. Carries forward through EOY to offset the upside.
            if r.outcome_date and r.outcome_date <= m_end:
                lost_start = max(r.outcome_date, active_start)
                if m_end >= lost_start:
                    lost_days = (m_end - lost_start).days + 1
                    churn_loss_sgd += old_rent_sgd * lost_days / days_in_month

            # Actual = day-prorated increase, capped at today for partial months
            actual_end = effective_end
            if status == 'partial':
                actual_end = min(actual_end, today)
            elif status == 'forecast':
                actual_end = None
            if actual_end and actual_end >= active_start:
                actual_days = (actual_end - active_start).days + 1
                actual_sgd += inc_sgd * actual_days / days_in_month

            batches_covered.add(str(r.batch_id))

        ledgers_covered = len(rows)

        months.append({
            'month': f"{year}-{m:02d}",
            'planned_sgd': round(planned_sgd, 2),
            'actual_sgd': round(actual_sgd, 2) if status != 'forecast' else None,
            # Churn loss carries forward: known outcomes reduce future months too,
            # offsetting the EOY win on a per-month basis.
            'churn_loss_sgd': round(churn_loss_sgd, 2),
            'forecast_sgd': round(planned_sgd - churn_loss_sgd, 2),
            'status': status,
        })

        if status == 'actual':
            ytd_actual += actual_sgd
        elif status == 'partial':
            ytd_actual += actual_sgd
        # EOY = planned net of already-realized churn loss carried forward
        eoy_projection += (planned_sgd - churn_loss_sgd)

    return {
        'year': year,
        'batch_id': batch_id,
        'months': months,
        'ytd_actual_sgd': round(ytd_actual, 2),
        'eoy_projection_sgd': round(eoy_projection, 2),
        'batches_covered': len(batches_covered),
        'ledgers_covered': ledgers_covered,
    }
