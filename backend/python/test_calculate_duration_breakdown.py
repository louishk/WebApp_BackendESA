"""
Tests for calculate_duration_breakdown().

Design decisions documented here:

LSETUP day-25 (late move-in):
    calculate_movein_cost() bundles a full "Second Monthly Rent Fee" into the
    move-in charge when day > day_start_prorate_plus_next (default 17). The
    duration breakdown reflects this faithfully:
    - month 1: partial rent (proration_factor < 1), deposit, admin fee, and
               the second-month rent+insurance are all included in month 1's
               `total` (it mirrors the actual move-in payment).
    - month 2: full rent, no deposit/admin, billing_date = 1st of next calendar
               month. Its `total` is the recurring monthly charge GOING FORWARD,
               not an additional move-in charge. The second-month rent already
               paid at move-in means the tenant actually has a credit for
               month 2 in SiteLink — but the breakdown shows the economic cost
               per period, not the cash-flow sequence. Callers who need to
               communicate "month 2 already paid" should check
               breakdown[0].rent_proration_factor < 1.0 on a non-anniversary
               plan (the late-movein signal).
    - months 3..N: recurring full months.

    This "split per month" approach (rather than bundling month 2 into month 1's
    total) is chosen because:
    1. It keeps all N months present in breakdown so duration_months == len(breakdown).
    2. total_contract = sum(breakdown) correctly represents the total economic cost
       over the tenure — month 2's cost is real regardless of when it was paid.
    3. Downstream consumers (chatbot, quote UI) can always reconstruct the
       cash-flow view from the data.
"""
import sys
import os
from datetime import date
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common.movein_cost_calculator import (
    calculate_duration_breakdown,
    calculate_movein_cost,
    ChargeTypeTax,
    estimate_total,
    DurationQuote,
    MonthlyBreakdown,
)

# ---------------------------------------------------------------------------
# Shared fixtures (tax configs typical for a SG site)
# ---------------------------------------------------------------------------

RENT_TAX = ChargeTypeTax("Rent", tax1_rate=Decimal("9"), tax2_rate=Decimal("0"))
ADMIN_TAX = ChargeTypeTax("AdminFee", tax1_rate=Decimal("9"), tax2_rate=Decimal("0"))
DEP_TAX = ChargeTypeTax("SecDep", tax1_rate=Decimal("0"), tax2_rate=Decimal("0"))
INS_TAX = ChargeTypeTax("Insurance", tax1_rate=Decimal("8"), tax2_rate=Decimal("0"))

STD_RATE = Decimal("100.00")
DEPOSIT = Decimal("100.00")
ADMIN_FEE = Decimal("30.00")


def _quote(move_in_date, duration_months=6, pc_discount=0, fixed_discount=0,
           insurance_premium=0, anniversary_billing=False,
           concession_in_month=1, max_amount_off=None,
           day_start_prorate_plus_next=17,
           std_rate=None, security_deposit=None, admin_fee=None,
           unit_id=0, plan_id=0, concession_id=0):
    return calculate_duration_breakdown(
        std_rate=std_rate if std_rate is not None else STD_RATE,
        security_deposit=security_deposit if security_deposit is not None else DEPOSIT,
        admin_fee=admin_fee if admin_fee is not None else ADMIN_FEE,
        move_in_date=move_in_date,
        rent_tax=RENT_TAX,
        admin_tax=ADMIN_TAX,
        deposit_tax=DEP_TAX,
        insurance_tax=INS_TAX,
        pc_discount=pc_discount,
        fixed_discount=fixed_discount,
        insurance_premium=insurance_premium,
        anniversary_billing=anniversary_billing,
        day_start_prorate_plus_next=day_start_prorate_plus_next,
        duration_months=duration_months,
        concession_in_month=concession_in_month,
        max_amount_off=max_amount_off,
        unit_id=unit_id,
        plan_id=plan_id,
        concession_id=concession_id,
    )


# ---------------------------------------------------------------------------
# Test 1: Anniversary plan, 6 months, no discount
# ---------------------------------------------------------------------------

def test_anniversary_no_discount_6_months():
    """6 identical recurring months (after month 1 admin+deposit). Sum matches."""
    move_in = date(2026, 5, 15)
    q = _quote(move_in, duration_months=6, anniversary_billing=True)

    assert isinstance(q, DurationQuote)
    assert len(q.breakdown) == 6
    assert q.duration_months == 6

    # Month 1 has deposit + admin_fee; months 2-6 do not.
    m1 = q.breakdown[0]
    assert m1.deposit == DEPOSIT
    assert m1.admin_fee == ADMIN_FEE
    assert m1.rent_proration_factor == Decimal("1")  # anniversary = full month

    for m in q.breakdown[1:]:
        assert m.deposit == Decimal("0"), f"month {m.month_index} should have zero deposit"
        assert m.admin_fee == Decimal("0"), f"month {m.month_index} should have zero admin_fee"
        assert m.rent_proration_factor == Decimal("1")

    # Months 2-6 should all have identical totals (same rent, no discount, no ins)
    recurring_totals = [m.total for m in q.breakdown[1:]]
    assert len(set(recurring_totals)) == 1, "months 2-6 totals should be identical"

    # Verify total_contract == sum of individual totals
    assert q.total_contract == sum(m.total for m in q.breakdown)
    assert q.first_month_total == q.breakdown[0].total

    # Sanity: total = 6 × recurring_monthly + deposit + admin (with taxes)
    # Recurring monthly = 100 + 9% tax = 109.00
    expected_recurring_monthly = Decimal("100.00") + Decimal("9.00")  # 109.00
    assert recurring_totals[0] == expected_recurring_monthly

    expected_total = (
        6 * expected_recurring_monthly
        + DEPOSIT  # deposit (0% tax)
        + ADMIN_FEE + Decimal("2.70")  # admin 30 + 9% = 32.70
    )
    assert abs(q.total_contract - expected_total) < Decimal("0.02")

    assert q.confidence == "high"
    assert q.confidence_reason is None


# ---------------------------------------------------------------------------
# Test 2: LSETUP day 5 (early move-in, no second month)
# ---------------------------------------------------------------------------

def test_lsetup_day5_6months_no_discount():
    """Day 5 < threshold 17 — month 1 prorated, months 2-6 full. No bundling."""
    move_in = date(2026, 5, 5)  # day 5, May has 31 days
    q = _quote(move_in, duration_months=6, anniversary_billing=False)

    assert len(q.breakdown) == 6

    m1 = q.breakdown[0]
    # Proration factor: (31 - 5 + 1) / 31 = 27/31 ≈ 0.87
    expected_factor = Decimal("27") / Decimal("31")
    expected_factor_rounded = expected_factor.quantize(Decimal("0.01"))
    assert abs(m1.rent_proration_factor - expected_factor_rounded) < Decimal("0.005")

    assert m1.rent_proration_factor < Decimal("1")
    assert m1.deposit == DEPOSIT
    assert m1.admin_fee == ADMIN_FEE

    # Months 2-6 should be full rent
    for m in q.breakdown[1:]:
        assert m.rent == Decimal("100.00")
        assert m.rent_proration_factor == Decimal("1")
        assert m.deposit == Decimal("0")
        assert m.admin_fee == Decimal("0")

    # Cross-check against calculate_movein_cost for month 1
    m1_charges, _ = calculate_movein_cost(
        std_rate=STD_RATE, security_deposit=DEPOSIT, admin_fee=ADMIN_FEE,
        move_in_date=move_in, rent_tax=RENT_TAX, admin_tax=ADMIN_TAX,
        deposit_tax=DEP_TAX, insurance_tax=INS_TAX,
    )
    assert m1.total == estimate_total(m1_charges)

    assert q.total_contract == sum(m.total for m in q.breakdown)
    assert q.confidence == "high"


# ---------------------------------------------------------------------------
# Test 3: LSETUP day 25 (late move-in, second month bundled)
# ---------------------------------------------------------------------------

def test_lsetup_day25_6months_no_discount():
    """
    Day 25 > threshold 17 — month 1 partial + month 2 full are bundled into
    the move-in charge. Design decision: the breakdown emits month 2 as a
    separate entry with full rent values. Month 1's `total` includes the
    second-month amount (it mirrors calculate_movein_cost output). Months 3-6
    are the recurring future payments.

    See module docstring for the rationale.
    """
    move_in = date(2026, 5, 25)  # day 25, May has 31 days; 25 > 17
    q = _quote(move_in, duration_months=6, anniversary_billing=False)

    assert len(q.breakdown) == 6

    m1 = q.breakdown[0]
    m2 = q.breakdown[1]

    # Month 1: partial proration factor (7/31 remaining days from day 25 inclusive)
    # days remaining = 31 - 25 + 1 = 7
    assert m1.rent_proration_factor < Decimal("1")
    assert m1.deposit == DEPOSIT
    assert m1.admin_fee == ADMIN_FEE

    # Month 1 total must equal what calculate_movein_cost returns (includes 2nd month)
    m1_charges, _ = calculate_movein_cost(
        std_rate=STD_RATE, security_deposit=DEPOSIT, admin_fee=ADMIN_FEE,
        move_in_date=move_in, rent_tax=RENT_TAX, admin_tax=ADMIN_TAX,
        deposit_tax=DEP_TAX, insurance_tax=INS_TAX,
    )
    assert m1.total == estimate_total(m1_charges), (
        "Month 1 total must equal calculate_movein_cost total (bundled 2nd month)"
    )

    # Month 2: full rent, no deposit/admin, billing_date = June 1
    assert m2.rent == Decimal("100.00")
    assert m2.rent_proration_factor == Decimal("1")
    assert m2.deposit == Decimal("0")
    assert m2.admin_fee == Decimal("0")
    assert m2.billing_date == date(2026, 6, 1)

    # Months 3-6: recurring full months
    for m in q.breakdown[2:]:
        assert m.rent == Decimal("100.00")
        assert m.rent_proration_factor == Decimal("1")
        assert m.deposit == Decimal("0")
        assert m.admin_fee == Decimal("0")

    assert q.total_contract == sum(m.total for m in q.breakdown)
    assert q.confidence == "high"


# ---------------------------------------------------------------------------
# Test 4: Concession iInMonth=3, pc_discount=10%, anniversary billing
# ---------------------------------------------------------------------------

def test_concession_in_month_3_anniversary():
    """Discount applies months 1, 2, 3. Months 4-6 are full price."""
    move_in = date(2026, 5, 1)
    q = _quote(
        move_in, duration_months=6, anniversary_billing=True,
        pc_discount=10, concession_in_month=3,
    )

    assert len(q.breakdown) == 6

    # Months 1-3: should have a non-zero discount
    for m in q.breakdown[:3]:
        assert m.discount > Decimal("0"), (
            f"month {m.month_index} should have discount applied"
        )

    # Months 4-6: no discount
    for m in q.breakdown[3:]:
        assert m.discount == Decimal("0"), (
            f"month {m.month_index} should have no discount (beyond iInMonth=3)"
        )

    # Verify discount amount for months 2-3 (full rent, 10%)
    expected_disc = Decimal("10.00")  # 10% of 100.00
    for m in q.breakdown[1:3]:  # months 2, 3
        assert m.discount == expected_disc

    # Rent tax on discounted months uses gross-minus-discount-tax method
    # tax(100) - tax(10) = 9.00 - 0.90 = 8.10
    expected_rent_tax_discounted = Decimal("8.10")
    for m in q.breakdown[1:3]:
        assert m.rent_tax == expected_rent_tax_discounted

    # Full price months 4-6: tax = 100 * 9% = 9.00
    for m in q.breakdown[3:]:
        assert m.rent_tax == Decimal("9.00")

    assert q.total_contract == sum(m.total for m in q.breakdown)
    assert q.confidence == "high"


# ---------------------------------------------------------------------------
# Test 5: Free-month plan (pc_discount=100) — low confidence
# ---------------------------------------------------------------------------

def test_free_month_plan_low_confidence():
    """100% discount → confidence='low_unsupported_concession', breakdown still computed."""
    move_in = date(2026, 5, 1)
    q = _quote(move_in, duration_months=3, anniversary_billing=True, pc_discount=100)

    assert q.confidence == "low_unsupported_concession"
    assert q.confidence_reason == "free_month_plan"

    # Breakdown is still computed (best-effort)
    assert len(q.breakdown) == 3
    assert q.total_contract == sum(m.total for m in q.breakdown)

    # Month 1 discount should equal the full rent (100% of 100.00 = 100.00)
    # BUT capped at the prorated/full rent amount
    m1 = q.breakdown[0]
    assert m1.discount == m1.rent  # 100% discount = full rent amount


# ---------------------------------------------------------------------------
# Test 6: 1-month duration edge case
# ---------------------------------------------------------------------------

def test_single_month_duration():
    """1-month tenure: single breakdown entry; total_contract == first_month_total."""
    move_in = date(2026, 5, 15)
    q = _quote(move_in, duration_months=1, anniversary_billing=True)

    assert len(q.breakdown) == 1
    assert q.first_month_total == q.total_contract
    assert q.monthly_average == q.total_contract
    assert q.breakdown[0].month_index == 1


# ---------------------------------------------------------------------------
# Test 7: 12-month wine plan with $3 insurance + 8% insurance_tax
# ---------------------------------------------------------------------------

def test_12_month_with_insurance():
    """$3 insurance + 8% insurance_tax appears on every month."""
    move_in = date(2026, 5, 1)
    insurance_premium = Decimal("3.00")
    q = _quote(
        move_in, duration_months=12, anniversary_billing=True,
        insurance_premium=insurance_premium,
    )

    assert len(q.breakdown) == 12

    # Every month should have insurance
    for m in q.breakdown:
        assert m.insurance == insurance_premium, (
            f"month {m.month_index} missing insurance"
        )
        # 8% insurance tax on $3 = $0.24
        expected_ins_tax = Decimal("0.24")
        assert m.insurance_tax == expected_ins_tax, (
            f"month {m.month_index} insurance_tax expected {expected_ins_tax}, "
            f"got {m.insurance_tax}"
        )

    # Verify total_contract includes all insurance charges
    assert q.total_contract == sum(m.total for m in q.breakdown)

    # Each recurring month (2-12) total = rent + rent_tax + insurance + insurance_tax
    # = 100 + 9 + 3 + 0.24 = 112.24
    expected_recurring = Decimal("112.24")
    for m in q.breakdown[1:]:
        assert m.total == expected_recurring, (
            f"month {m.month_index} total expected {expected_recurring}, got {m.total}"
        )

    assert q.confidence == "high"


# ---------------------------------------------------------------------------
# Test 8: max_amount_off caps the discount
# ---------------------------------------------------------------------------

def test_max_amount_off_caps_discount():
    """pc_discount=50% on $100 = $50, but max_amount_off=$20 caps it at $20."""
    move_in = date(2026, 5, 1)
    q = _quote(
        move_in, duration_months=3, anniversary_billing=True,
        pc_discount=50, concession_in_month=3,
        max_amount_off=Decimal("20"),
    )

    # Month 1 discount is determined by calculate_movein_cost (no max_amount_off there)
    # Months 2-3: discount should be capped at 20
    for m in q.breakdown[1:3]:
        assert m.discount == Decimal("20.00"), (
            f"month {m.month_index} discount should be capped at 20, got {m.discount}"
        )


# ---------------------------------------------------------------------------
# Test 9: Billing dates are correct (1st-of-month billing)
# ---------------------------------------------------------------------------

def test_billing_dates_1st_of_month():
    """Non-anniversary: billing dates are 1st of each calendar month."""
    move_in = date(2026, 5, 15)
    q = _quote(move_in, duration_months=4, anniversary_billing=False)

    assert q.breakdown[0].billing_date == date(2026, 5, 15)  # move-in date
    assert q.breakdown[1].billing_date == date(2026, 6, 1)
    assert q.breakdown[2].billing_date == date(2026, 7, 1)
    assert q.breakdown[3].billing_date == date(2026, 8, 1)


# ---------------------------------------------------------------------------
# Test 10: Billing dates correct for anniversary billing
# ---------------------------------------------------------------------------

def test_billing_dates_anniversary():
    """Anniversary billing: billing dates follow the monthly anniversary of move-in."""
    move_in = date(2026, 5, 15)
    q = _quote(move_in, duration_months=4, anniversary_billing=True)

    assert q.breakdown[0].billing_date == date(2026, 5, 15)
    assert q.breakdown[1].billing_date == date(2026, 6, 15)
    assert q.breakdown[2].billing_date == date(2026, 7, 15)
    assert q.breakdown[3].billing_date == date(2026, 8, 15)


# ---------------------------------------------------------------------------
# Test 11: month_index values are sequential 1..N
# ---------------------------------------------------------------------------

def test_month_indices_sequential():
    move_in = date(2026, 5, 1)
    for n in [1, 3, 6, 12]:
        q = _quote(move_in, duration_months=n, anniversary_billing=True)
        assert [m.month_index for m in q.breakdown] == list(range(1, n + 1))


# ---------------------------------------------------------------------------
# Test 12: passthrough fields on DurationQuote
# ---------------------------------------------------------------------------

def test_passthrough_ids():
    move_in = date(2026, 5, 1)
    q = _quote(move_in, duration_months=2, anniversary_billing=True,
               unit_id=42, plan_id=7, concession_id=99)
    assert q.unit_id == 42
    assert q.plan_id == 7
    assert q.concession_id == 99
    assert q.move_in_date == move_in


# ---------------------------------------------------------------------------
# Test 13: fixed_discount on recurring months (within concession_in_month)
# ---------------------------------------------------------------------------

def test_fixed_discount_recurring():
    """$15 fixed discount on months 1-2, months 3+ full price."""
    move_in = date(2026, 5, 1)
    q = _quote(
        move_in, duration_months=4, anniversary_billing=True,
        fixed_discount=15, concession_in_month=2,
    )

    # Month 2: fixed discount $15
    assert q.breakdown[1].discount == Decimal("15.00")

    # Month 3, 4: no discount
    for m in q.breakdown[2:]:
        assert m.discount == Decimal("0")


# ---------------------------------------------------------------------------
# Test 14: LSETUP day 25, 1-month tenure (edge: late-movein with N=1)
# ---------------------------------------------------------------------------

def test_lsetup_day25_1_month():
    """Single month with late move-in: breakdown has 1 entry, total = full movein cost."""
    move_in = date(2026, 5, 25)
    q = _quote(move_in, duration_months=1, anniversary_billing=False)

    assert len(q.breakdown) == 1
    m1_charges, _ = calculate_movein_cost(
        std_rate=STD_RATE, security_deposit=DEPOSIT, admin_fee=ADMIN_FEE,
        move_in_date=move_in, rent_tax=RENT_TAX, admin_tax=ADMIN_TAX,
        deposit_tax=DEP_TAX, insurance_tax=INS_TAX,
    )
    assert q.first_month_total == estimate_total(m1_charges)
    assert q.total_contract == q.first_month_total
