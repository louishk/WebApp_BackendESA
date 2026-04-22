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
    """Tax-specific 2dp rounding. SiteLink truncates (ROUND_DOWN) tax.

    Empirically determined on LSETUP 2026-04-17:
    - 35.50 * 0.09 = 3.195 → SOAP returns 3.19 (truncation, not HALF_UP)
    - 17.75 * 0.09 = 1.5975 → SOAP returns 1.59 (truncation, not HALF_DOWN)
    Combined with the discount tax method (full_tax - disc_tax), this
    matches SOAP exactly across all tested scenarios.
    """
    return value.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


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
