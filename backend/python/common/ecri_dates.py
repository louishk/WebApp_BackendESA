"""
ecri_dates — billing-cycle-aware effective date computation for ECRI.

Pure module: no Flask, no ORM, no DB imports. Unit-testable with date objects.

Three constraints interact:
  1. Notice deadline — notice letter must be sent BEFORE the Billing Generation
                       Date (BGD = LAD - 14) so the scheduled rate change is
                       already in SMD when the bill is produced.
  2. PaidThruDate    — current rate is locked until dPaidThru; the target LAD
                       must be after dPaidThru for the increase to apply.
  3. 14-day notice   — tenant must receive at least 14 days between the notice
                       letter and the effective date. This is always satisfied
                       because effective = LAD and notice = BGD - 1 = LAD - 15,
                       giving 15 days of notice.

The effective date is always the LAD itself (the tenant's billing anniversary).
SMD's billing engine picks up the scheduled rate change when generating the bill
14 days before, so the increase lands cleanly at the start of the next cycle.
"""

import calendar
import math
from datetime import date, timedelta
from decimal import Decimal, ROUND_CEILING


# Currency-specific rounding unit for ceil-to-next-whole.
# Ceil to next dollar/ringgit/1000 Won — clean invoices, never under-captures.
_CEIL_UNIT = {
    'SGD': Decimal('1'),      # ceil to next dollar
    'MYR': Decimal('1'),      # ceil to next ringgit
    'KRW': Decimal('1000'),   # ceil to next 1000 Won
    'HKD': Decimal('1'),      # ceil to next dollar
}


def round_new_rent(amount: Decimal, currency: str = 'SGD') -> Decimal:
    """Ceil the new rent to the next whole unit for the currency.

    SGD/MYR/HKD → next whole dollar/ringgit/dollar.
    KRW → next 1,000 Won.
    Falls back to ceil-to-dollar for unknown currencies.
    """
    unit = _CEIL_UNIT.get(currency, Decimal('1'))
    if unit == 0:
        return amount
    # Divide, ceil, multiply back
    return (amount / unit).to_integral_value(rounding=ROUND_CEILING) * unit


def next_lease_anniversary(anniv: date, today: date) -> date:
    """Return the next calendar date >= today whose day-of-month matches
    anniv.day, with month-end clamping when the target month is shorter.

    Only anniv.day is used; anniv.month and anniv.year are ignored.
    """
    anniv_day = anniv.day

    def clamped_date(year: int, month: int) -> date:
        last = calendar.monthrange(year, month)[1]
        return date(year, month, min(anniv_day, last))

    candidate = clamped_date(today.year, today.month)
    if candidate >= today:
        return candidate

    if today.month == 12:
        return clamped_date(today.year + 1, 1)
    return clamped_date(today.year, today.month + 1)


def _add_months(d: date, months: int) -> date:
    """Add `months` to a date, clamping to the last day of the target month."""
    month = d.month - 1 + months
    year = d.year + month // 12
    month = month % 12 + 1
    last = calendar.monthrange(year, month)[1]
    return date(year, month, min(d.day, last))


def compute_effective_date(
    anniv: date | None,
    paid_thru: date | None,
    today: date,
    notice_days: int = 14,
    bgd_offset_days: int = 14,
) -> tuple[date, date, str]:
    """Compute the billing-cycle-aware effective date for an ECRI notice.

    The effective date is always a Lease Anniversary Date (LAD). The notice
    date is BGD - 1 (one day before billing generation) to ensure the
    scheduled rate change is in SMD before the bill runs.

    Green:  notice_date >= today AND next_LAD > dPaidThru
            → can catch this billing cycle
    Amber:  same as Green but dPaidThru > today (rate currently locked,
            but unlocks before the LAD)
    Red:    notice_date < today OR next_LAD <= dPaidThru
            → missed this cycle, push to next month's LAD
    Unknown: anniv is None (no billing anchor available)

    Args:
        anniv:           Lease Anniversary Date (only .day matters); None if unknown.
        paid_thru:       Paid-through date; None means treat as unlocked (today).
        today:           Reference date (usually date.today()).
        notice_days:     Unused (kept for interface compat); notice is always BGD - 1.
        bgd_offset_days: Days before LAD that SMD generates the next bill (default 14).

    Returns:
        (effective_date, notice_date, bucket)
    """
    bgd_delta = timedelta(days=bgd_offset_days)
    pt = paid_thru if paid_thru is not None else today

    if anniv is None:
        effective_date = max(today + timedelta(days=notice_days), pt + timedelta(days=1))
        notice_date = effective_date - timedelta(days=notice_days)
        return effective_date, notice_date, 'unknown'

    next_lad = next_lease_anniversary(anniv, today)
    next_bgd = next_lad - bgd_delta
    notice_date = next_bgd - timedelta(days=1)

    can_catch = notice_date >= today and next_lad > pt

    if can_catch:
        effective_date = next_lad
        if paid_thru is not None and paid_thru > today:
            bucket = 'amber'
        else:
            bucket = 'green'
        return effective_date, notice_date, bucket

    # Missed this cycle — push to next month's LAD
    target_lad = _add_months(next_lad, 1)
    target_bgd = target_lad - bgd_delta
    target_notice = target_bgd - timedelta(days=1)

    # If still can't catch (e.g. paid_thru extends past target_lad), keep pushing
    while target_notice < today or target_lad <= pt:
        target_lad = _add_months(target_lad, 1)
        target_bgd = target_lad - bgd_delta
        target_notice = target_bgd - timedelta(days=1)

    return target_lad, target_notice, 'red'


def compute_advance_effective_date(
    anniv: date | None,
    projected_paid_thru: date | None,
    today: date,
    notice_days: int = 14,
    bgd_offset_days: int = 14,
    prepay_buffer_days: int = 7,
) -> tuple[date, date, str]:
    """Compute an effective date for an advance-scheduling (Pre-Load) batch.

    Unlike the standard flow, here we want the rate change to land AFTER the
    tenant's *projected* paid_thru — i.e. beyond the window a prepayer or
    recent-move-in would extend their rent-lock through. We inflate paid_thru
    by ``prepay_buffer_days`` and delegate to the existing algorithm's
    "keep pushing" loop so the returned LAD is guaranteed > projected_paid_thru.

    Args:
        anniv:               Lease anniversary date (only .day matters).
        projected_paid_thru: Paid-through date projected forward (e.g. from
                             discount expiration + one cycle, or current dPaidThru
                             for heavy prepayers). None treated as today.
        today:               Reference date (usually date.today()).
        notice_days:         Unused; kept for interface parity.
        bgd_offset_days:     Days before LAD that SMD generates the next bill.
        prepay_buffer_days:  Safety margin added to projected_paid_thru so the
                             scheduled date sits cleanly past any last-minute
                             prepayment.

    Returns:
        (effective_date, notice_date, bucket) — same shape as
        :func:`compute_effective_date`. The bucket will almost always be
        ``'red'`` or ``'unknown'`` on advance batches by construction.
    """
    pt_inflated = (
        projected_paid_thru + timedelta(days=prepay_buffer_days)
        if projected_paid_thru is not None
        else today
    )
    return compute_effective_date(
        anniv=anniv,
        paid_thru=pt_inflated,
        today=today,
        notice_days=notice_days,
        bgd_offset_days=bgd_offset_days,
    )
