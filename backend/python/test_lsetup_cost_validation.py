"""
MoveInCostRetrieve Internal Replication Validation
====================================================
Builds up cost calculations internally, then calls SOAP to compare.
Tests multiple scenarios: different units, dates, discounts, insurance.

Goal: determine how many scenarios our internal formula matches SOAP exactly.
"""
import sys, os, json
from calendar import monthrange
from decimal import Decimal, ROUND_HALF_UP
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta
from common.config import DataLayerConfig
from common.soap_client import SOAPClient
from sqlalchemy import create_engine, text
from common.config_loader import get_database_url

CC_NS = "http://tempuri.org/CallCenterWs/CallCenterWs"
SITE = "LSETUP"

# ============================================================================
# Internal calculation engine
# ============================================================================

def calc_proration(monthly_rate, move_in_day, days_in_month):
    """Prorate: rate * remaining_days / days_in_month (inclusive of move-in day)."""
    remaining = days_in_month - move_in_day + 1
    prorated = Decimal(str(monthly_rate)) * Decimal(remaining) / Decimal(days_in_month)
    return prorated.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def calc_tax(amount, tax_rate):
    """Simple tax: round(amount * rate, 2)."""
    tax = Decimal(str(amount)) * Decimal(str(tax_rate))
    return tax.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def calc_discount(base_amount, pc_discount):
    """Percentage discount on rent."""
    disc = Decimal(str(base_amount)) * Decimal(str(pc_discount)) / Decimal("100")
    return disc.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def calculate_movein_cost(
    std_rate, admin_fee, security_deposit,
    tax1_rate, tax2_rate,
    move_in_date,
    pc_discount=0, fixed_discount=0,
    insurance_premium=0, insurance_tax_rate=None,
):
    """
    Calculate move-in cost internally.
    Returns list of charge line dicts matching SOAP response shape.
    """
    day = move_in_date.day
    _, days_in_month = monthrange(move_in_date.year, move_in_date.month)

    charges = []

    # 1. Prorated rent
    prorated_rent = calc_proration(std_rate, day, days_in_month)

    # Apply discount
    discount_amount = Decimal("0")
    if pc_discount > 0:
        discount_amount = calc_discount(prorated_rent, pc_discount)
    elif fixed_discount > 0:
        # Fixed discount — cap at prorated rent
        discount_amount = min(Decimal(str(fixed_discount)), prorated_rent)

    rent_after_disc = prorated_rent - discount_amount
    rent_tax1 = calc_tax(rent_after_disc, tax1_rate)
    rent_tax2 = calc_tax(rent_after_disc, tax2_rate)
    rent_total = rent_after_disc + rent_tax1 + rent_tax2

    charges.append({
        "description": "First Monthly Rent Fee",
        "amount": float(prorated_rent),
        "discount": float(discount_amount),
        "tax1": float(rent_tax1),
        "tax2": float(rent_tax2),
        "total": float(rent_total),
    })

    # 2. Admin fee (flat, taxed)
    admin = Decimal(str(admin_fee))
    admin_tax1 = calc_tax(admin, tax1_rate)
    admin_tax2 = calc_tax(admin, tax2_rate)
    admin_total = admin + admin_tax1 + admin_tax2

    charges.append({
        "description": "Administrative Fee",
        "amount": float(admin),
        "discount": 0,
        "tax1": float(admin_tax1),
        "tax2": float(admin_tax2),
        "total": float(admin_total),
    })

    # 3. Security deposit (flat, no tax)
    dep = Decimal(str(security_deposit))
    charges.append({
        "description": "Security Deposit",
        "amount": float(dep),
        "discount": 0,
        "tax1": 0,
        "tax2": 0,
        "total": float(dep),
    })

    # 4. Insurance (prorated, taxed — possibly at different rate)
    if insurance_premium > 0:
        ins_prorated = calc_proration(insurance_premium, day, days_in_month)
        ins_tax_rate = insurance_tax_rate if insurance_tax_rate is not None else tax1_rate
        ins_tax1 = calc_tax(ins_prorated, ins_tax_rate)
        ins_total = ins_prorated + ins_tax1

        charges.append({
            "description": "First Month Insurance",
            "amount": float(ins_prorated),
            "discount": 0,
            "tax1": float(ins_tax1),
            "tax2": 0,
            "total": float(ins_total),
        })

    return charges


# ============================================================================
# SOAP client
# ============================================================================

def get_client():
    config = DataLayerConfig.from_env()
    cc_url = config.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
    return SOAPClient(
        base_url=cc_url, corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user, api_key=config.soap.api_key,
        corp_password=config.soap.corp_password, timeout=60, retries=1,
    )


def soap_call(client, op, params, result_tag="RT"):
    return client.call(operation=op, parameters=params,
                       soap_action=f"{CC_NS}/{op}", namespace=CC_NS, result_tag=result_tag)


# ============================================================================
# Test scenarios
# ============================================================================

def run_scenario(client, scenario, unit_info, waiting_id):
    """Run one scenario: calculate internally, call SOAP, compare."""
    name = scenario["name"]
    move_in = scenario["move_in_date"]
    concession_id = scenario.get("concession_id", 0)
    insurance_id = scenario.get("insurance_id", 0)
    pc_discount = scenario.get("pc_discount", 0)
    fixed_discount = scenario.get("fixed_discount", 0)
    insurance_premium = scenario.get("insurance_premium", 0)
    insurance_tax_rate = scenario.get("insurance_tax_rate", None)

    move_in_str = move_in.strftime("%Y-%m-%dT00:00:00")

    # Internal calculation
    # Tax rate from SOAP/DB is in percentage (9.0 = 9%), convert to decimal (0.09)
    tax1_pct = float(unit_info["dcTax1Rate"])
    tax2_pct = float(unit_info.get("dcTax2Rate", 0))
    internal = calculate_movein_cost(
        std_rate=float(unit_info["dcStdRate"]),
        admin_fee=30,  # hardcoded for LSETUP — this is a known gap
        security_deposit=float(unit_info["dcStdSecDep"]),
        tax1_rate=tax1_pct / 100 if tax1_pct > 1 else tax1_pct,
        tax2_rate=tax2_pct / 100 if tax2_pct > 1 else tax2_pct,
        move_in_date=move_in,
        pc_discount=pc_discount,
        fixed_discount=fixed_discount,
        insurance_premium=insurance_premium,
        insurance_tax_rate=insurance_tax_rate,
    )
    internal_total = sum(c["total"] for c in internal)

    # SOAP call
    soap_result = soap_call(client, "MoveInCostRetrieveWithDiscount_Reservation_v4", {
        "sLocationCode": SITE,
        "iUnitID": str(unit_info["UnitID"]),
        "dMoveInDate": move_in_str,
        "InsuranceCoverageID": str(insurance_id),
        "ConcessionPlanID": str(concession_id),
        "WaitingID": str(waiting_id),
        "iPromoGlobalNum": "0",
        "ChannelType": "0",
        "bApplyInsuranceCredit": "false",
    }, result_tag="Table")

    soap_total = sum(float(r.get("dcTotal", 0)) for r in soap_result)

    # Compare line by line
    match = True
    print(f"\n  --- {name} ---")
    print(f"  Move-in: {move_in.strftime('%Y-%m-%d')}, Unit: {unit_info['sUnitName']}, "
          f"Rate: ${unit_info['dcStdRate']}")

    for i, (calc, soap) in enumerate(zip(internal, soap_result)):
        calc_t = calc["total"]
        soap_t = float(soap.get("dcTotal", 0))
        calc_d = calc["discount"]
        soap_d = float(soap.get("dcDiscount", 0))
        ok = abs(calc_t - soap_t) < 0.01
        disc_ok = abs(calc_d - soap_d) < 0.01
        if not ok or not disc_ok:
            match = False

        status = "OK" if ok else "MISMATCH"
        print(f"    {calc['description']:30s}  "
              f"Calc=${calc_t:>8.2f}  SOAP=${soap_t:>8.2f}  "
              f"Disc: Calc=${calc_d:.2f} SOAP=${soap_d:.2f}  "
              f"[{status}]")

    # Check for extra SOAP rows we didn't predict
    if len(soap_result) > len(internal):
        match = False
        for extra in soap_result[len(internal):]:
            print(f"    {'UNEXPECTED':30s}  "
                  f"SOAP=${float(extra.get('dcTotal', 0)):>8.2f}  "
                  f"{extra.get('ChargeDescription', '?')}  [MISSING FROM CALC]")

    total_match = abs(internal_total - soap_total) < 0.01
    if not total_match:
        match = False

    status = "PASS" if match else "FAIL"
    print(f"    {'TOTAL':30s}  "
          f"Calc=${internal_total:>8.2f}  SOAP=${soap_total:>8.2f}  [{status}]")

    return match


def main():
    client = get_client()
    engine = create_engine(get_database_url('pbi'))

    try:
        # Get multiple available units with different rates
        units = soap_call(client, "UnitsInformationAvailableUnitsOnly_v2",
                          {"sLocationCode": SITE}, result_tag="Table")

        # Pick units with different rates
        seen_rates = set()
        test_units = []
        for u in units:
            rate = u.get("dcStdRate", "0")
            if rate not in seen_rates and len(test_units) < 4:
                seen_rates.add(rate)
                test_units.append(u)

        print(f"Selected {len(test_units)} test units:")
        for u in test_units:
            print(f"  UnitID={u['UnitID']}, Name={u.get('sUnitName')}, "
                  f"Rate=${u['dcStdRate']}, Deposit=${u.get('dcStdSecDep', '?')}, "
                  f"Tax1={u.get('dcTax1Rate', '?')}")

        # Get discount info from DB
        with engine.connect() as conn:
            discounts = conn.execute(text("""
                SELECT "ConcessionID", "sPlanName", "dcPCDiscount", "dcFixedDiscount"
                FROM ccws_discount
                WHERE "SiteID" = 27525 AND "dDisabled" IS NULL
                ORDER BY "ConcessionID"
                LIMIT 10
            """)).fetchall()
            print(f"\nAvailable discounts:")
            for d in discounts:
                print(f"  CID={d[0]}, Name={d[1]}, PC={d[2]}%, Fixed=${d[3]}")

        # Create tenant + reservation for testing
        t = soap_call(client, "TenantNewDetailed_v3", {
            "sLocationCode": SITE, "sWebPassword": "", "sMrMrs": "",
            "sFName": "CostVal", "sMI": "", "sLName": f"Test{datetime.now().strftime('%H%M%S')}",
            "sCompany": "", "sAddr1": "1 Test St", "sAddr2": "", "sCity": "Singapore",
            "sRegion": "", "sPostalCode": "000000", "sCountry": "SG", "sPhone": "99999999",
            "sMrMrsAlt": "", "sFNameAlt": "", "sMIAlt": "", "sLNameAlt": "",
            "sAddr1Alt": "", "sAddr2Alt": "", "sCityAlt": "", "sRegionAlt": "",
            "sPostalCodeAlt": "", "sCountryAlt": "", "sPhoneAlt": "",
            "sMrMrsBus": "", "sFNameBus": "", "sMIBus": "", "sLNameBus": "",
            "sCompanyBus": "", "sAddr1Bus": "", "sAddr2Bus": "", "sCityBus": "",
            "sRegionBus": "", "sPostalCodeBus": "", "sCountryBus": "", "sPhoneBus": "",
            "sEmail": "test@example.com", "sEmailAlt": "", "sEmailBus": "",
            "sFax": "", "sFaxAlt": "", "sFaxBus": "", "sMobile": "",
            "dDOB": "1990-01-01T00:00:00", "sSSN": "", "sDriversLic": "",
            "bBusiness": "false", "sIDType": "", "sIDNum": "", "sIDIssuer": "",
            "bTestMode": "false",
        })
        tid = int(t[0]["TenantID"])

        # Build scenarios
        scenarios = []
        base_dates = [
            datetime(2026, 4, 17),   # mid-month April (30 days)
            datetime(2026, 5, 1),    # 1st of month May (31 days)
            datetime(2026, 5, 15),   # mid-month May
            datetime(2026, 5, 28),   # near end of month
            datetime(2026, 6, 1),    # 1st of June (30 days)
            datetime(2026, 2, 15),   # mid-Feb (28 days, non-leap)
        ]

        for ui, unit in enumerate(test_units[:2]):  # use 2 units
            for date in base_dates:
                date_str = date.strftime("%Y-%m-%dT00:00:00")
                expires = (date + timedelta(days=14)).strftime("%Y-%m-%dT00:00:00")
                followup = (date + timedelta(days=3)).strftime("%Y-%m-%dT00:00:00")

                # Create a reservation for this unit+date combo
                r = soap_call(client, "ReservationNewWithSource_v6", {
                    "sLocationCode": SITE, "sTenantID": str(tid),
                    "sUnitID": str(unit["UnitID"]),
                    "dNeeded": date_str, "sComment": "Cost validation",
                    "iSource": "0", "sSource": "CostVal",
                    "QTRentalTypeID": "0", "iInquiryType": "0",
                    "dcQuotedRate": "0", "dExpires": expires,
                    "dFollowUp": followup, "sTrackingCode": "",
                    "sCallerID": "", "ConcessionID": "0", "PromoGlobalNum": "0",
                })
                wid = int(r[0]["Ret_Code"])

                # Scenario: no discount, no insurance
                scenarios.append({
                    "name": f"Unit {unit['sUnitName']} (${unit['dcStdRate']}) @ {date.strftime('%b %d')} — baseline",
                    "move_in_date": date,
                    "unit": unit,
                    "waiting_id": wid,
                })

                # Scenario: 5% discount
                scenarios.append({
                    "name": f"Unit {unit['sUnitName']} @ {date.strftime('%b %d')} — 5% discount",
                    "move_in_date": date,
                    "unit": unit,
                    "waiting_id": wid,
                    "concession_id": 4661,
                    "pc_discount": 5,
                })

                # Scenario: 10% discount
                scenarios.append({
                    "name": f"Unit {unit['sUnitName']} @ {date.strftime('%b %d')} — 10% discount",
                    "move_in_date": date,
                    "unit": unit,
                    "waiting_id": wid,
                    "concession_id": 4662,
                    "pc_discount": 10,
                })

                # Scenario: with insurance ($3/mo, use same tax rate as unit)
                ins_tax = float(unit.get("dcTax1Rate", 0))
                ins_tax_dec = ins_tax / 100 if ins_tax > 1 else ins_tax
                scenarios.append({
                    "name": f"Unit {unit['sUnitName']} @ {date.strftime('%b %d')} — insurance $3/mo (tax={ins_tax}%)",
                    "move_in_date": date,
                    "unit": unit,
                    "waiting_id": wid,
                    "insurance_id": 9649,
                    "insurance_premium": 3,
                    "insurance_tax_rate": ins_tax_dec,
                })

                # Scenario: discount + insurance
                scenarios.append({
                    "name": f"Unit {unit['sUnitName']} @ {date.strftime('%b %d')} — 5% disc + insurance",
                    "move_in_date": date,
                    "unit": unit,
                    "waiting_id": wid,
                    "concession_id": 4661,
                    "pc_discount": 5,
                    "insurance_id": 9649,
                    "insurance_premium": 3,
                    "insurance_tax_rate": ins_tax_dec,
                })

                # Cancel reservation so unit is available for next test
                soap_call(client, "ReservationUpdate_v4", {
                    "sLocationCode": SITE,
                    "iWaitingID": str(wid),
                    "iStatus": "2",  # 2 = cancelled
                    "dExpires": expires,
                    "dFollowUp": followup,
                    "sNote": "Cost validation cleanup",
                })

        # Run all scenarios
        print(f"\n{'='*70}")
        print(f"  RUNNING {len(scenarios)} SCENARIOS")
        print(f"{'='*70}")

        passed = 0
        failed = 0
        failures = []

        for s in scenarios:
            unit = s.pop("unit")
            wid = s.pop("waiting_id")

            # Need a fresh reservation since we cancelled
            date_str = s["move_in_date"].strftime("%Y-%m-%dT00:00:00")
            expires = (s["move_in_date"] + timedelta(days=14)).strftime("%Y-%m-%dT00:00:00")
            followup = (s["move_in_date"] + timedelta(days=3)).strftime("%Y-%m-%dT00:00:00")
            r = soap_call(client, "ReservationNewWithSource_v6", {
                "sLocationCode": SITE, "sTenantID": str(tid),
                "sUnitID": str(unit["UnitID"]),
                "dNeeded": date_str, "sComment": "CV",
                "iSource": "0", "sSource": "CV",
                "QTRentalTypeID": "0", "iInquiryType": "0",
                "dcQuotedRate": "0", "dExpires": expires,
                "dFollowUp": followup, "sTrackingCode": "",
                "sCallerID": "", "ConcessionID": "0", "PromoGlobalNum": "0",
            })
            wid = int(r[0]["Ret_Code"])

            ok = run_scenario(client, s, unit, wid)
            if ok:
                passed += 1
            else:
                failed += 1
                failures.append(s["name"])

            # Cleanup
            soap_call(client, "ReservationUpdate_v4", {
                "sLocationCode": SITE, "iWaitingID": str(wid),
                "iStatus": "2", "dExpires": expires,
                "dFollowUp": followup, "sNote": "Cleanup",
            })

        # Summary
        print(f"\n{'='*70}")
        print(f"  VALIDATION SUMMARY")
        print(f"{'='*70}")
        print(f"  Total scenarios: {passed + failed}")
        print(f"  Passed: {passed}")
        print(f"  Failed: {failed}")
        print(f"  Match rate: {passed/(passed+failed)*100:.1f}%")
        if failures:
            print(f"\n  Failed scenarios:")
            for f in failures:
                print(f"    - {f}")

    finally:
        client.close()


if __name__ == "__main__":
    main()
