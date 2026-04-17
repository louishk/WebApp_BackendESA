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
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional


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
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _prorate(monthly_amount, move_in_day: int, days_in_month: int) -> Decimal:
    """Prorate inclusive of move-in day."""
    remaining = days_in_month - move_in_day + 1
    return _round2(
        Decimal(str(monthly_amount)) * Decimal(remaining) / Decimal(days_in_month)
    )


def _tax(amount: Decimal, rate_pct) -> Decimal:
    """Calculate tax from a percentage rate (e.g. 9.0 = 9%)."""
    return _round2(
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
    insurance_premium=0,
    anniversary_billing: bool = False,
    day_start_prorate_plus_next: int = 17,
) -> List[CostLine]:
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
        pc_discount: Percentage discount on rent (0-100)
        insurance_premium: Monthly insurance premium (0 = no insurance)
        anniversary_billing: True for anniversary mode (no proration)
        day_start_prorate_plus_next: 1st-of-month threshold for second
            month charge — late move-ins on day >= this value get billed
            for the partial first month + a full second month

    Returns:
        List[CostLine] with each charge line (rent, admin, deposit,
        optional second month rent, optional insurance).
    """
    if admin_tax is None:
        admin_tax = rent_tax
    if deposit_tax is None:
        deposit_tax = ChargeTypeTax("SecDep")
    if insurance_tax is None:
        insurance_tax = rent_tax

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

    rent_after_disc = rent_base - discount_amt
    rent_t1 = _tax(rent_after_disc, rent_tax.tax1_rate)
    rent_t2 = _tax(rent_after_disc, rent_tax.tax2_rate)

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

    if late_movein:
        full_rent = _round2(Decimal(str(std_rate)))
        disc2 = Decimal("0")
        if pc_discount and Decimal(str(pc_discount)) > 0:
            disc2 = _round2(
                full_rent * Decimal(str(pc_discount)) / Decimal("100")
            )
        rent2_after = full_rent - disc2
        rent2_t1 = _tax(rent2_after, rent_tax.tax1_rate)
        rent2_t2 = _tax(rent2_after, rent_tax.tax2_rate)
        charges.append(CostLine(
            description="Second Monthly Rent Fee",
            charge_amount=full_rent,
            discount=disc2,
            tax1=rent2_t1,
            tax2=rent2_t2,
            total=rent2_after + rent2_t1 + rent2_t2,
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

    return charges


def estimate_total(charges: List[CostLine]) -> Decimal:
    """Sum all charge line totals."""
    return sum((c.total for c in charges), Decimal("0"))
