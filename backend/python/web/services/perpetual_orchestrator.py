"""
Perpetual + prepay orchestrator — pure decision logic.

Given a freshly-completed move-in, returns the list of follow-up SOAP
calls (PaymentSimpleCash, ScheduleTenantRateChange_v2) that should be
enqueued in mw_lease_followup_jobs. The /api/reservations/move-in
handler runs this AFTER the SOAP MoveIn returns successfully.

Decision tree
─────────────
Three flags drive the decision:

  plan.discount_perpetual         (TRUE → discount applies forever via Tenant's Rate)
  plan.prepayment_months          (N    → customer paid for N months upfront)
  concession.b_prepay             (TRUE → SiteLink natively bundles N months at MoveIn)

Resulting jobs:

  Standard plan (no perpetual, no native prepay)
      ScheduleTenantRateChange  at move_in + 12 mo  (or admin default)

  SiteLink-native prepay (concession.b_prepay=TRUE, perpetual=FALSE)
      ScheduleTenantRateChange  at move_in + iPrePaidMonths + 1
      No PaymentSimpleCash — SiteLink already bundled the prepay in MoveIn.

  Perpetual without prepay (perpetual=TRUE, prepayment_months=NULL)
      ScheduleTenantRateChange  at move_in + 12 mo  (or admin default)
      No PaymentSimpleCash.

  Perpetual + custom prepay (perpetual=TRUE, prepayment_months=N)
      PaymentSimpleCash         amount = (payment_amount - soap_cost)
      ScheduleTenantRateChange  at move_in + N

In every case the rate change is at:
    effective_rate × (1 + ecri_pct/100)

Two master switches gate output:
    ecri_auto_schedule_enabled       — when OFF, no rate-change jobs emitted
    perpetual_auto_payment_enabled   — when OFF, no PaymentSimpleCash emitted

Returns a list of dicts ready for lease_followup_queue.enqueue().
This module has NO side effects — no DB writes, no SOAP calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)


@dataclass
class OrchestrationContext:
    """Inputs to the orchestrator decision."""
    site_code: str
    ledger_id: int
    tenant_id: int
    unit_id: int
    move_in_date: date
    payment_amount: Decimal           # what bot passed to /move-in
    soap_movein_cost: Decimal         # what SOAP MoveIn actually consumed
    effective_rate: Decimal           # post-discount monthly rent (excl. tax)
    # Plan-level flags (resolved from the booked plan or candidate row)
    discount_perpetual: bool = False
    prepayment_months: Optional[int] = None
    post_prepay_ecri_pct: Optional[Decimal] = None
    # Native concession prepay flags (from ccws_discount)
    concession_b_prepay: bool = False
    concession_prepaid_months: int = 0
    # Globals (resolved from mw_recommender_settings)
    ecri_default_pct: Decimal = Decimal('5.0')
    ecri_default_offset_months: int = 12
    ecri_min_offset_months: int = 6
    ecri_auto_schedule_enabled: bool = False
    perpetual_auto_payment_enabled: bool = False
    # Provenance — for the queue audit
    related_request_id: Optional[str] = None
    related_session_id: Optional[str] = None
    related_customer_id: Optional[str] = None


def determine_followups(ctx: OrchestrationContext) -> List[Dict[str, Any]]:
    """Compute jobs to enqueue. Returns empty list when both master switches
    are off OR when the plan needs no follow-up.

    Each returned dict is a row to insert into mw_lease_followup_jobs:
        action_type    : 'prepayment' | 'schedule_rate_change'
        payload        : params for the SOAP call (JSON-serialisable)
        site_code/tenant/unit/ledger : copied from context
    """
    jobs: List[Dict[str, Any]] = []

    is_native_prepay = bool(ctx.concession_b_prepay) and ctx.concession_prepaid_months > 0
    is_orchestrated_prepay = (
        ctx.discount_perpetual
        and (ctx.prepayment_months or 0) > 0
        and not is_native_prepay
    )

    # ── PaymentSimpleCash — only for orchestrated (custom) prepay ──
    if is_orchestrated_prepay:
        prepay_amount = ctx.payment_amount - ctx.soap_movein_cost
        if prepay_amount <= Decimal('0.50'):
            logger.info(
                "Orchestrated-prepay flagged but no surplus payment "
                "(payment=%s, soap_cost=%s). Skipping PaymentSimpleCash.",
                ctx.payment_amount, ctx.soap_movein_cost,
            )
        elif not ctx.perpetual_auto_payment_enabled:
            logger.info(
                "perpetual_auto_payment_enabled=OFF — would have pushed $%s "
                "via PaymentSimpleCash for ledger %s. Skipping.",
                prepay_amount, ctx.ledger_id,
            )
        else:
            jobs.append(_make_prepayment_job(ctx, prepay_amount))

    # ── ScheduleTenantRateChange — fires for ALL successful move-ins ──
    if not ctx.ecri_auto_schedule_enabled:
        logger.info(
            "ecri_auto_schedule_enabled=OFF — would have scheduled rate change "
            "for ledger %s. Skipping.", ctx.ledger_id,
        )
    else:
        offset_months = _resolve_offset_months(ctx, is_native_prepay)
        ecri_pct = _resolve_ecri_pct(ctx)
        new_rate = (ctx.effective_rate * (Decimal('1') + ecri_pct / Decimal('100'))).quantize(Decimal('0.01'))
        scheduled_for = ctx.move_in_date + relativedelta(months=offset_months)
        jobs.append(_make_schedule_rate_change_job(ctx, new_rate, scheduled_for))

    return jobs


def _resolve_offset_months(ctx: OrchestrationContext, is_native_prepay: bool) -> int:
    """Pick the right offset months for the rate-change schedule."""
    if is_native_prepay:
        # SiteLink prepaid N months natively → revisit the rate the period after
        offset = (ctx.concession_prepaid_months or 0) + 1
    elif ctx.prepayment_months:
        # Our orchestrated prepay window
        offset = ctx.prepayment_months
    else:
        offset = ctx.ecri_default_offset_months
    # Floor — never schedule earlier than the admin's minimum
    return max(int(offset), int(ctx.ecri_min_offset_months))


def _resolve_ecri_pct(ctx: OrchestrationContext) -> Decimal:
    """Plan-level override → admin default."""
    if ctx.post_prepay_ecri_pct is not None:
        return Decimal(ctx.post_prepay_ecri_pct)
    return Decimal(ctx.ecri_default_pct)


def _make_prepayment_job(ctx: OrchestrationContext, amount: Decimal) -> Dict[str, Any]:
    return {
        'action_type': 'prepayment',
        'site_code':   ctx.site_code,
        'ledger_id':   ctx.ledger_id,
        'tenant_id':   ctx.tenant_id,
        'unit_id':     ctx.unit_id,
        'payload': {
            'sLocationCode':    ctx.site_code,
            'iTenantID':        str(ctx.tenant_id),
            'iUnitID':          str(ctx.unit_id),
            'dcPaymentAmount':  f"{amount:.2f}",
        },
        'related_request_id':  ctx.related_request_id,
        'related_session_id':  ctx.related_session_id,
        'related_customer_id': ctx.related_customer_id,
    }


def _make_schedule_rate_change_job(
    ctx: OrchestrationContext, new_rate: Decimal, scheduled_for: date,
) -> Dict[str, Any]:
    # Format dScheduledChange as ISO datetime — SiteLink expects this.
    dt_iso = datetime(scheduled_for.year, scheduled_for.month, scheduled_for.day).isoformat()
    return {
        'action_type': 'schedule_rate_change',
        'site_code':   ctx.site_code,
        'ledger_id':   ctx.ledger_id,
        'tenant_id':   ctx.tenant_id,
        'unit_id':     ctx.unit_id,
        'payload': {
            'sLocationCode':       ctx.site_code,
            'LedgerID':            str(ctx.ledger_id),
            'dcNewRate':           f"{new_rate:.4f}",
            'dScheduledChange':    dt_iso,
            'iRatesTaxInclusive':  '0',
        },
        'related_request_id':  ctx.related_request_id,
        'related_session_id':  ctx.related_session_id,
        'related_customer_id': ctx.related_customer_id,
    }
