"""
Internal MoveInCost calculator.

Replicates SiteLink's MoveInCostRetrieveWithDiscount calculation for
display-only cost estimates in the booking engine.

NOT for binding amounts — call SOAP MoveInCostRetrieveWithDiscount_v4
to get the exact dcPaymentAmount before move-in (amount match is strict
to the cent and a $0.01 mismatch causes Ret_Code=-11 rejection).

Supports two billing modes:
- 1st-of-month (bAnnivDateLeasing=false): prorate partial first month;
  late move-ins (day >= iDayStrtProratePlusNext) trigger a second full month
- Anniversary (bAnnivDateLeasing=true): full month from move-in date,
  no proration, no second month

Validated against LSETUP on 2026-04-16 with 60 scenarios. Core engine
matches SOAP to the cent for rent proration, tax, discounts, admin fee,
and security deposit. Insurance and second-month logic require correct
ChargeDescriptionsRetrieve data.
"""

from calendar import monthrange
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, ROUND_HALF_UP, ROUND_DOWN
from typing import List, Optional, Tuple


@dataclass
class ChargeTypeTax:
    """Tax rates for a specific charge type (from ccws_charge_descriptions).

    Rates are stored as percentages (9.0 means 9%), matching the SOAP
    response and DB column format.
    """
    category: str
    tax1_rate: Decimal = Decimal("0")
    tax2_rate: Decimal = Decimal("0")
    default_price: Decimal = Decimal("0")


@dataclass
class CostLine:
    """One line in the cost breakdown."""
    description: str
    charge_amount: Decimal
    discount: Decimal
    tax1: Decimal
    tax2: Decimal
    total: Decimal


def _round2(value: Decimal) -> Decimal:
    """Standard 2dp rounding (used for proration). HALF_UP."""
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _round2_tax(value: Decimal) -> Decimal:
    """Tax-specific 2dp rounding. SiteLink rounds tax HALF_UP.

    Empirically validated on LSETUP 2026-04-29 across 10 move-in days
    (probe_soap_tax_rounding.py vs MoveInCostRetrieveWithDiscount_Reservation_v4):
      day  rent_chg  rent_tax_soap  HALF_UP  | ins_chg  ins_tax_soap  HALF_UP
        5     65.32          5.88     5.88   |    2.61          0.21     0.21
       14     43.55          3.92     3.92   |    1.74          0.14     0.14
       17     36.29          3.27     3.27   |    1.45          0.12     0.12
       23     21.77          1.96     1.96   |    0.87          0.07     0.07
       30      4.84          0.44     0.44   |    0.19          0.02     0.02
    All 5 boundary cases (per line) match HALF_UP. None match ROUND_DOWN.
    Same rule for rent (9%) and insurance (8%).

    The earlier "truncation" docstring (2026-04-17 sample) was wrong — the
    cases observed didn't actually cross a rounding boundary, or were of
    discount-tax-derivation lines, not direct rate × charge.
    """
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _prorate(monthly_amount, move_in_day: int, days_in_month: int) -> Decimal:
    """Prorate inclusive of move-in day."""
    remaining = days_in_month - move_in_day + 1
    return _round2(
        Decimal(str(monthly_amount)) * Decimal(remaining) / Decimal(days_in_month)
    )


def _tax(amount: Decimal, rate_pct) -> Decimal:
    """Calculate tax from a percentage rate (e.g. 9.0 = 9%). HALF_DOWN per SiteLink."""
    return _round2_tax(
        Decimal(str(amount)) * Decimal(str(rate_pct)) / Decimal("100")
    )


def calculate_movein_cost(
    std_rate,
    security_deposit,
    admin_fee,
    move_in_date,
    rent_tax: ChargeTypeTax,
    admin_tax: Optional[ChargeTypeTax] = None,
    deposit_tax: Optional[ChargeTypeTax] = None,
    insurance_tax: Optional[ChargeTypeTax] = None,
    pc_discount=0,
    fixed_discount=0,
    insurance_premium=0,
    anniversary_billing: bool = False,
    day_start_prorate_plus_next: int = 17,
) -> Tuple[List[CostLine], Optional[str]]:
    """
    Calculate estimated move-in cost.

    Args:
        std_rate: Monthly rent rate (numeric)
        security_deposit: Flat deposit amount (numeric)
        admin_fee: Flat admin fee (from ccws_charge_descriptions AdminFee.dcPrice)
        move_in_date: datetime.date or datetime.datetime
        rent_tax: ChargeTypeTax for Rent category (9% on SG sites)
        admin_tax: ChargeTypeTax for AdminFee (defaults to rent_tax)
        deposit_tax: ChargeTypeTax for SecDep (defaults to 0%)
        insurance_tax: ChargeTypeTax for Insurance (defaults to rent_tax;
            on LSETUP this is 8%, distinct from the 9% GST on rent)
        pc_discount: Percentage discount on first-month rent (0-100).
            For SiteLink "Recurring Discount" plans only. Plans like
            "Free Month" or "X Months Free" are prepaid promotions with
            different multi-month math — the calculator does NOT model
            those; fall back to SOAP for accurate totals.
        fixed_discount: Fixed-dollar discount amount applied to first-month
            rent (capped at the prorated amount).
        insurance_premium: Monthly insurance premium (0 = no insurance)
        anniversary_billing: True for anniversary mode (no proration)
        day_start_prorate_plus_next: 1st-of-month threshold for second
            month charge — late move-ins on day > this value get billed
            for the partial first month + a full second month

    Returns:
        Tuple of (List[CostLine], Optional[str]) where the second element
        is a reason code string when confidence is low, None when high.

        Reason codes:
            "free_month_plan"            — 100% pc_discount (free-month concession,
                                           multi-month math not modelled)
            "late_move_in_with_discount" — late move-in day with any discount applied
                                           (discount interaction with 2nd month unclear)
            "prepaid_multi_month"        — fixed_discount >= std_rate (full-month
                                           prepay pattern, calculator understimates)
            "unknown_discount_structure" — discount present but doesn't match any
                                           known pattern above

    Limitations (returns wrong total — fall back to SOAP when reason is not None):
        - "Free Month"/"X Months Free" / prepaid concessions
        - 100% discount plans (require multi-month prepay)
        - "Recurring Discount" applied to second-month line: SiteLink
          only applies it to the first month in the move-in cost call,
          so the 2nd month rent here is full-price (no discount).
    """
    if admin_tax is None:
        admin_tax = rent_tax
    if deposit_tax is None:
        deposit_tax = ChargeTypeTax("SecDep")
    if insurance_tax is None:
        insurance_tax = rent_tax

    # --- Confidence detection ---
    _pc = Decimal(str(pc_discount)) if pc_discount else Decimal("0")
    _fixed = Decimal(str(fixed_discount)) if fixed_discount else Decimal("0")
    _rate = Decimal(str(std_rate))
    _has_discount = _pc > 0 or _fixed > 0
    _low_confidence_reason: Optional[str] = None

    if _pc >= Decimal("100"):
        _low_confidence_reason = "free_month_plan"
    elif _fixed >= _rate:
        _low_confidence_reason = "prepaid_multi_month"

    day = move_in_date.day
    _, days_in_month = monthrange(move_in_date.year, move_in_date.month)

    charges: List[CostLine] = []

    # --- First Monthly Rent ---
    if anniversary_billing:
        rent_base = _round2(Decimal(str(std_rate)))
    else:
        rent_base = _prorate(std_rate, day, days_in_month)

    discount_amt = Decimal("0")
    if pc_discount and Decimal(str(pc_discount)) > 0:
        discount_amt = _round2(
            rent_base * Decimal(str(pc_discount)) / Decimal("100")
        )
    elif fixed_discount and Decimal(str(fixed_discount)) > 0:
        # Fixed discount is capped at the prorated rent amount
        discount_amt = min(_round2(Decimal(str(fixed_discount))), rent_base)

    # SiteLink tax calculation when a discount is present:
    #   tax = tax(full_base) - tax(discount)
    # This is mathematically the same as tax on (base-discount) but with
    # different rounding behavior at the boundary. SiteLink reports the
    # full ChargeAmount (not net) and a separate dcDiscount field — the
    # TaxAmount is the net after subtracting discount tax.
    rent_full_tax1 = _tax(rent_base, rent_tax.tax1_rate)
    rent_full_tax2 = _tax(rent_base, rent_tax.tax2_rate)
    if discount_amt > 0:
        rent_disc_tax1 = _tax(discount_amt, rent_tax.tax1_rate)
        rent_disc_tax2 = _tax(discount_amt, rent_tax.tax2_rate)
    else:
        rent_disc_tax1 = Decimal("0")
        rent_disc_tax2 = Decimal("0")
    rent_t1 = rent_full_tax1 - rent_disc_tax1
    rent_t2 = rent_full_tax2 - rent_disc_tax2

    rent_after_disc = rent_base - discount_amt
    charges.append(CostLine(
        description="First Monthly Rent Fee",
        charge_amount=rent_base,
        discount=discount_amt,
        tax1=rent_t1,
        tax2=rent_t2,
        total=rent_after_disc + rent_t1 + rent_t2,
    ))

    # --- Admin Fee ---
    admin = _round2(Decimal(str(admin_fee)))
    admin_t1 = _tax(admin, admin_tax.tax1_rate)
    admin_t2 = _tax(admin, admin_tax.tax2_rate)
    charges.append(CostLine(
        description="Administrative Fee",
        charge_amount=admin,
        discount=Decimal("0"),
        tax1=admin_t1,
        tax2=admin_t2,
        total=admin + admin_t1 + admin_t2,
    ))

    # --- Security Deposit ---
    dep = _round2(Decimal(str(security_deposit)))
    dep_t1 = _tax(dep, deposit_tax.tax1_rate)
    dep_t2 = _tax(dep, deposit_tax.tax2_rate)
    charges.append(CostLine(
        description="Security Deposit",
        charge_amount=dep,
        discount=Decimal("0"),
        tax1=dep_t1,
        tax2=dep_t2,
        total=dep + dep_t1 + dep_t2,
    ))

    # --- Second Month (1st-of-month billing only, late move-in) ---
    # Triggers when move-in day is strictly greater than the threshold.
    # LSETUP test: day 17 + threshold 17 → no 2nd month;
    #              day 28 + threshold 17 → 2nd month charged.
    late_movein = (
        not anniversary_billing
        and day > day_start_prorate_plus_next
    )

    # Detect late-move-in + discount interaction (unreliable case)
    if _low_confidence_reason is None and late_movein and _has_discount:
        _low_confidence_reason = "late_move_in_with_discount"
    elif _low_confidence_reason is None and _has_discount:
        # Discount present but doesn't match any specifically modelled pattern —
        # mark as unknown only if the discount is unusually large (>50% of rate),
        # which may indicate a promotional structure we can't model.
        if _pc > Decimal("50") or _fixed > _rate * Decimal("0.5"):
            _low_confidence_reason = "unknown_discount_structure"

    if late_movein:
        # SiteLink doesn't apply move-in discounts to the 2nd month line —
        # only to the first month. Confirmed by LSETUP test 2026-04-17.
        full_rent = _round2(Decimal(str(std_rate)))
        rent2_t1 = _tax(full_rent, rent_tax.tax1_rate)
        rent2_t2 = _tax(full_rent, rent_tax.tax2_rate)
        charges.append(CostLine(
            description="Second Monthly Rent Fee",
            charge_amount=full_rent,
            discount=Decimal("0"),
            tax1=rent2_t1,
            tax2=rent2_t2,
            total=full_rent + rent2_t1 + rent2_t2,
        ))

    # --- Insurance ---
    if insurance_premium and Decimal(str(insurance_premium)) > 0:
        if anniversary_billing:
            ins_base = _round2(Decimal(str(insurance_premium)))
        else:
            ins_base = _prorate(insurance_premium, day, days_in_month)

        ins_t1 = _tax(ins_base, insurance_tax.tax1_rate)
        charges.append(CostLine(
            description="First Month Insurance",
            charge_amount=ins_base,
            discount=Decimal("0"),
            tax1=ins_t1,
            tax2=Decimal("0"),
            total=ins_base + ins_t1,
        ))

        if late_movein:
            ins2_base = _round2(Decimal(str(insurance_premium)))
            ins2_t1 = _tax(ins2_base, insurance_tax.tax1_rate)
            charges.append(CostLine(
                description="Second Month Insurance",
                charge_amount=ins2_base,
                discount=Decimal("0"),
                tax1=ins2_t1,
                tax2=Decimal("0"),
                total=ins2_base + ins2_t1,
            ))

    return charges, _low_confidence_reason


def estimate_total(charges: List[CostLine]) -> Decimal:
    """Sum all charge line totals."""
    return sum((c.total for c in charges), Decimal("0"))


@dataclass
class MonthlyBreakdown:
    """Cost breakdown for a single month in a duration quote."""
    month_index: int               # 1..N
    billing_date: date             # 1st of the billing month (or move-in date for month 1)
    rent: Decimal
    rent_proration_factor: Decimal  # 1.0 = full month, <1.0 = prorated
    discount: Decimal               # negative or zero (stored as positive, subtracted from rent)
    insurance: Decimal
    deposit: Decimal                # 0 except month 1
    admin_fee: Decimal              # 0 except month 1
    rent_tax: Decimal
    insurance_tax: Decimal
    total: Decimal                  # net charge for the month


@dataclass
class DurationQuote:
    """Month-by-month cost quote for a given tenure."""
    unit_id: int
    plan_id: int
    concession_id: int
    move_in_date: date
    duration_months: int
    breakdown: List[MonthlyBreakdown]
    first_month_total: Decimal      # = breakdown[0].total
    monthly_average: Decimal        # = total_contract / duration_months
    total_contract: Decimal         # sum of breakdown[*].total
    confidence: str                 # 'high' | 'low_unsupported_concession'
    confidence_reason: Optional[str]


def _next_month_date(d: date) -> date:
    """Return the 1st of the month following d."""
    if d.month == 12:
        return date(d.year + 1, 1, 1)
    return date(d.year, d.month + 1, 1)


def _add_months(d: date, n: int) -> date:
    """Add n months to date d (day-1 result — always returns 1st of target month)."""
    month = d.month - 1 + n
    year = d.year + month // 12
    month = month % 12 + 1
    return date(year, month, 1)


def calculate_duration_breakdown(
    std_rate,
    security_deposit,
    admin_fee,
    move_in_date,
    rent_tax: ChargeTypeTax,
    admin_tax: Optional[ChargeTypeTax] = None,
    deposit_tax: Optional[ChargeTypeTax] = None,
    insurance_tax: Optional[ChargeTypeTax] = None,
    pc_discount=0,
    fixed_discount=0,
    insurance_premium=0,
    anniversary_billing: bool = False,
    day_start_prorate_plus_next: int = 17,
    *,
    duration_months: int,
    concession_in_month: int = 1,
    concession_prepay_months: int = 0,
    max_amount_off: Optional[Decimal] = None,
    unit_id: int = 0,
    plan_id: int = 0,
    concession_id: int = 0,
    discount_perpetual: bool = False,
) -> DurationQuote:
    """
    Compute a month-by-month cost breakdown for the requested tenure.

    Month 1 delegates entirely to calculate_movein_cost() — all proration,
    deposit, admin fee, discount, and tax logic is identical and SOAP-validated.

    Months 2..N use simpler full-month math:
    - Full rent + insurance + tax each month.
    - Discount applied when month_index <= concession_in_month (iInMonth window).
    - No deposit or admin fee on recurring months.

    LSETUP late-day case (1st-of-month billing, day > threshold):
    When calculate_movein_cost triggers a "Second Monthly Rent Fee", that second
    month is bundled into the move-in payment. The breakdown reflects this by
    making month 2 a full-month entry with rent_proration_factor=1.0 but
    billing_date = 1st of month 2. Months 3..N are the recurring schedule.
    The `total` on month 2 is the real cost — it was charged at move-in, not
    as a separate future billing. Callers can detect the bundled case by
    checking DurationQuote.breakdown[0].rent_proration_factor < 1.0 on a
    non-anniversary plan when duration_months > 1.

    Anniversary billing: month 1 = full rent on move-in date; month 2 = full
    rent on the monthly anniversary; etc. No late-day bundling.

    Discount math (months 2..N):
    - pc_discount: applied as percentage of full rent, subject to max_amount_off.
    - fixed_discount: applied as flat amount, capped at full rent.
    - Mirrors month 1's logic but without proration.

    Confidence: 'high' when no low-confidence reason; 'low_unsupported_concession'
    when calculate_movein_cost returns a reason code (free-month, prepay, etc.).
    """
    if admin_tax is None:
        admin_tax = rent_tax
    if deposit_tax is None:
        deposit_tax = ChargeTypeTax("SecDep")
    if insurance_tax is None:
        insurance_tax = rent_tax

    _rate = Decimal(str(std_rate))
    _ins = Decimal(str(insurance_premium)) if insurance_premium else Decimal("0")
    _pc = Decimal(str(pc_discount)) if pc_discount else Decimal("0")
    _fixed = Decimal(str(fixed_discount)) if fixed_discount else Decimal("0")
    _max_off = Decimal(str(max_amount_off)) if max_amount_off is not None else None

    # --- Month 1: delegate to existing validated calculator ---
    m1_charges, low_reason = calculate_movein_cost(
        std_rate=std_rate,
        security_deposit=security_deposit,
        admin_fee=admin_fee,
        move_in_date=move_in_date,
        rent_tax=rent_tax,
        admin_tax=admin_tax,
        deposit_tax=deposit_tax,
        insurance_tax=insurance_tax,
        pc_discount=pc_discount,
        fixed_discount=fixed_discount,
        insurance_premium=insurance_premium,
        anniversary_billing=anniversary_billing,
        day_start_prorate_plus_next=day_start_prorate_plus_next,
    )

    # Determine whether a second month was bundled into the move-in charge.
    day = move_in_date.day
    late_movein = not anniversary_billing and day > day_start_prorate_plus_next

    # Parse month 1 lines from the CostLine list.
    # Lines are: First Monthly Rent Fee, Administrative Fee, Security Deposit,
    # [Second Monthly Rent Fee], [First Month Insurance], [Second Month Insurance]
    m1_rent_line = next(c for c in m1_charges if "First Monthly Rent" in c.description)
    m1_admin_line = next(c for c in m1_charges if "Administrative" in c.description)
    m1_dep_line = next(c for c in m1_charges if "Security Deposit" in c.description)
    m1_ins_line = next(
        (c for c in m1_charges if c.description == "First Month Insurance"), None
    )

    # Proration factor for month 1 (1.0 for anniversary billing or move-in on day 1)
    if anniversary_billing:
        m1_factor = Decimal("1")
    else:
        _, days_in_month = monthrange(move_in_date.year, move_in_date.month)
        remaining = days_in_month - day + 1
        m1_factor = _round2(Decimal(remaining) / Decimal(days_in_month))

    m1_rent_tax = m1_rent_line.tax1 + m1_rent_line.tax2
    m1_ins_tax = m1_ins_line.tax1 if m1_ins_line else Decimal("0")
    m1_ins_amt = m1_ins_line.charge_amount if m1_ins_line else Decimal("0")

    m1_total = sum(c.total for c in m1_charges)

    # For month 1, the "rent" field holds the gross prorated rent (before discount).
    # The discount is what calculate_movein_cost computed.
    m1_breakdown = MonthlyBreakdown(
        month_index=1,
        billing_date=move_in_date if isinstance(move_in_date, date) else move_in_date.date(),
        rent=m1_rent_line.charge_amount,
        rent_proration_factor=m1_factor,
        discount=m1_rent_line.discount,
        insurance=m1_ins_amt,
        deposit=m1_dep_line.charge_amount,
        admin_fee=m1_admin_line.charge_amount,
        rent_tax=m1_rent_tax,
        insurance_tax=m1_ins_tax,
        total=m1_total,
    )

    breakdown: List[MonthlyBreakdown] = [m1_breakdown]

    # --- Months 2..N ---
    # When late_movein: month 2 is already bundled into the move-in payment.
    # We still emit a MonthlyBreakdown for it so the caller has the complete
    # picture, but its total was charged at move-in.
    # The recurring schedule (future billing dates) starts at month 3 for
    # late-movein cases, or month 2 otherwise.

    # Billing date anchor: 1st of the calendar month following move_in_date.
    # For anniversary billing the anchor is the monthly anniversary of move_in_date.
    if anniversary_billing:
        # Month 2 billing date = move_in_date + 1 month (same day, or last day of month)
        def _anniv_date(n: int) -> date:
            """Return the billing date for month n (1-indexed). Month 1 = move_in_date."""
            target_month = move_in_date.month - 1 + n
            target_year = move_in_date.year + (target_month - 1) // 12
            target_month = (target_month - 1) % 12 + 1
            _, days_in_target = monthrange(target_year, target_month)
            target_day = min(move_in_date.day, days_in_target)
            return date(target_year, target_month, target_day)
    else:
        def _anniv_date(n: int) -> date:
            """Return the 1st of the nth billing month (n >= 2 means +n-1 calendar months)."""
            # Month 1 = move_in_date month; month 2 = next calendar month 1st; etc.
            return _add_months(date(move_in_date.year, move_in_date.month, 1), n - 1)

    for month_idx in range(2, duration_months + 1):
        billing_dt = _anniv_date(month_idx)

        # Full rent (no proration on months 2+)
        full_rent = _round2(_rate)

        # Discount: apply only within concession_in_month window OR for the
        # entire lease when the plan was flagged discount_perpetual (operator
        # applies Tenant's Rate at move-in to bake the discount in).
        if discount_perpetual or month_idx <= concession_in_month:
            if _pc > Decimal("0"):
                disc = _round2(full_rent * _pc / Decimal("100"))
                # SiteLink stores dcMaxAmountOff=0 to mean "no cap" — only
                # apply the cap when it's a real positive ceiling.
                if _max_off is not None and _max_off > Decimal("0"):
                    disc = min(disc, _max_off)
            elif _fixed > Decimal("0"):
                disc = min(_round2(_fixed), full_rent)
            else:
                disc = Decimal("0")
        else:
            disc = Decimal("0")

        # Tax on rent (gross - discount tax method, mirrors month 1)
        r_full_t1 = _tax(full_rent, rent_tax.tax1_rate)
        r_full_t2 = _tax(full_rent, rent_tax.tax2_rate)
        if disc > Decimal("0"):
            r_disc_t1 = _tax(disc, rent_tax.tax1_rate)
            r_disc_t2 = _tax(disc, rent_tax.tax2_rate)
        else:
            r_disc_t1 = r_disc_t2 = Decimal("0")
        r_tax = (r_full_t1 - r_disc_t1) + (r_full_t2 - r_disc_t2)

        # Insurance (full premium, no proration on months 2+)
        if _ins > Decimal("0"):
            ins_amt = _round2(_ins)
            ins_tax_amt = _tax(ins_amt, insurance_tax.tax1_rate)
        else:
            ins_amt = Decimal("0")
            ins_tax_amt = Decimal("0")

        rent_after_disc = full_rent - disc
        month_total = rent_after_disc + r_tax + ins_amt + ins_tax_amt

        breakdown.append(MonthlyBreakdown(
            month_index=month_idx,
            billing_date=billing_dt,
            rent=full_rent,
            rent_proration_factor=Decimal("1"),
            discount=disc,
            insurance=ins_amt,
            deposit=Decimal("0"),
            admin_fee=Decimal("0"),
            rent_tax=r_tax,
            insurance_tax=ins_tax_amt,
            total=month_total,
        ))

    total_contract = sum(m.total for m in breakdown)
    first_month_total = breakdown[0].total
    monthly_average = _round2(total_contract / Decimal(str(duration_months)))

    confidence = "high" if low_reason is None else "low_unsupported_concession"

    return DurationQuote(
        unit_id=unit_id,
        plan_id=plan_id,
        concession_id=concession_id,
        move_in_date=move_in_date if isinstance(move_in_date, date) else move_in_date.date(),
        duration_months=duration_months,
        breakdown=breakdown,
        first_month_total=first_month_total,
        monthly_average=monthly_average,
        total_contract=total_contract,
        confidence=confidence,
        confidence_reason=low_reason,
    )


def load_site_billing_config(site_code: str):
    """
    Load proration / billing-mode config for a site from ccws_site_billing_config.

    Returns a dict with keys:
        anniversary_billing: bool
        day_start_prorating: int
        day_start_prorate_plus_next: int

    Falls back to defaults (1st-of-month, threshold day 17) if the site
    has no row yet — caller should run ccws_site_billing_config_to_sql.py
    to populate.
    """
    from sqlalchemy import create_engine, text
    from common.config_loader import get_database_url

    engine = create_engine(get_database_url('middleware'))
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT b_anniv_date_leasing,
                   i_day_strt_prorating,
                   i_day_strt_prorate_plus_next
            FROM ccws_site_billing_config
            WHERE "SiteCode" = :site
        """), {"site": site_code}).first()

    if not row:
        return {
            "anniversary_billing": False,
            "day_start_prorating": 1,
            "day_start_prorate_plus_next": 17,
        }
    return {
        "anniversary_billing": bool(row[0]),
        "day_start_prorating": int(row[1]),
        "day_start_prorate_plus_next": int(row[2]),
    }
