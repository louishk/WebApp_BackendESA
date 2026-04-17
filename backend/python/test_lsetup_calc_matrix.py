"""
LSETUP Calculator Validation Matrix
====================================

Test the internal MoveInCost calculator against SOAP across many configurations:
- No discount, % discounts (5/10/25/50%), fixed-amount discounts ($5/$10/$50/$100), 100% free
- With and without insurance (different premiums)
- Different move-in dates (early/mid/late month, different month lengths)
- Edge cases: deposit=0 (calculator-only), admin_fee=0 (calculator-only)

For each scenario:
  - Compute calculator output
  - Call SOAP MoveInCostRetrieveWithDiscount_Reservation_v4 for ground truth
  - Compare line-by-line and total
  - Report PASS/FAIL with the diff

Reuses one tenant + one reservation per (unit, date) combo to minimize SOAP load.
"""
import sys, os
from datetime import datetime, timedelta
from decimal import Decimal
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sqlalchemy import create_engine, text
from common.config import DataLayerConfig
from common.config_loader import get_database_url
from common.soap_client import SOAPClient
from common.movein_cost_calculator import (
    calculate_movein_cost, ChargeTypeTax, estimate_total
)

CC_NS = "http://tempuri.org/CallCenterWs/CallCenterWs"
SITE = "LSETUP"


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
                       soap_action=f"{CC_NS}/{op}", namespace=CC_NS,
                       result_tag=result_tag)


def make_tenant(client):
    r = soap_call(client, "TenantNewDetailed_v3", {
        "sLocationCode": SITE, "sWebPassword": "", "sMrMrs": "",
        "sFName": "Matrix", "sMI": "",
        "sLName": f"T{datetime.now().strftime('%H%M%S%f')}",
        "sCompany": "", "sAddr1": "1 Test St", "sAddr2": "",
        "sCity": "SG", "sRegion": "", "sPostalCode": "000000",
        "sCountry": "SG", "sPhone": "99999999",
        "sMrMrsAlt": "", "sFNameAlt": "", "sMIAlt": "", "sLNameAlt": "",
        "sAddr1Alt": "", "sAddr2Alt": "", "sCityAlt": "", "sRegionAlt": "",
        "sPostalCodeAlt": "", "sCountryAlt": "", "sPhoneAlt": "",
        "sMrMrsBus": "", "sFNameBus": "", "sMIBus": "", "sLNameBus": "",
        "sCompanyBus": "", "sAddr1Bus": "", "sAddr2Bus": "", "sCityBus": "",
        "sRegionBus": "", "sPostalCodeBus": "", "sCountryBus": "", "sPhoneBus": "",
        "sEmail": "m@test.com", "sEmailAlt": "", "sEmailBus": "",
        "sFax": "", "sFaxAlt": "", "sFaxBus": "", "sMobile": "",
        "dDOB": "1990-01-01T00:00:00", "sSSN": "", "sDriversLic": "",
        "bBusiness": "false", "sIDType": "", "sIDNum": "", "sIDIssuer": "",
        "bTestMode": "false",
    })
    return int(r[0]["TenantID"])


def make_reservation(client, tid, unit_id, move_in_iso):
    expires = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%dT00:00:00")
    followup = (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%dT00:00:00")
    r = soap_call(client, "ReservationNewWithSource_v6", {
        "sLocationCode": SITE, "sTenantID": str(tid), "sUnitID": str(unit_id),
        "dNeeded": move_in_iso, "sComment": "Matrix",
        "iSource": "0", "sSource": "Matrix",
        "QTRentalTypeID": "0", "iInquiryType": "0",
        "dcQuotedRate": "0", "dExpires": expires, "dFollowUp": followup,
        "sTrackingCode": "", "sCallerID": "", "ConcessionID": "0",
        "PromoGlobalNum": "0",
    })
    return int(r[0]["Ret_Code"])


def cancel_reservation(client, wid):
    expires = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%dT00:00:00")
    followup = (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%dT00:00:00")
    try:
        soap_call(client, "ReservationUpdate_v4", {
            "sLocationCode": SITE, "iWaitingID": str(wid),
            "iStatus": "2",
            "dExpires": expires, "dFollowUp": followup,
            "sNote": "Matrix cleanup",
        })
    except Exception:
        pass


def soap_movein_cost(client, unit_id, wid, move_in_iso, ins_id, cid):
    return soap_call(client, "MoveInCostRetrieveWithDiscount_Reservation_v4", {
        "sLocationCode": SITE, "iUnitID": str(unit_id),
        "dMoveInDate": move_in_iso,
        "InsuranceCoverageID": str(ins_id),
        "ConcessionPlanID": str(cid),
        "WaitingID": str(wid),
        "iPromoGlobalNum": "0", "ChannelType": "0",
        "bApplyInsuranceCredit": "false",
    }, result_tag="Table")


def cmp_money(a, b, tol=0.01):
    return abs(float(a) - float(b)) < tol


def main():
    client = get_client()
    engine = create_engine(get_database_url('pbi'))

    # Pull config from synced DB
    with engine.connect() as conn:
        cfg_rows = conn.execute(text("""
            SELECT "sChgCategory", "dcTax1Rate", "dcTax2Rate", "dcPrice"
            FROM ccws_charge_descriptions
            WHERE "SiteCode"='LSETUP'
              AND "sChgCategory" IN ('Rent','AdminFee','SecDep','Insurance')
              AND "dDisabled" IS NULL
        """)).fetchall()
    cfg = {r[0]: r for r in cfg_rows}
    rent_tax = ChargeTypeTax('Rent', cfg['Rent'][1], cfg['Rent'][2])
    admin_tax = ChargeTypeTax('AdminFee', cfg['AdminFee'][1], cfg['AdminFee'][2])
    dep_tax = ChargeTypeTax('SecDep', cfg['SecDep'][1], cfg['SecDep'][2])
    ins_tax = ChargeTypeTax('Insurance', cfg['Insurance'][1], cfg['Insurance'][2])
    admin_fee = float(cfg['AdminFee'][3])

    # Pick a known unit
    units = soap_call(client, "UnitsInformationAvailableUnitsOnly_v2",
                      {"sLocationCode": SITE}, result_tag="Table")
    unit = next(u for u in units if float(u.get('dcStdRate', 0)) >= 70)
    unit_id = unit['UnitID']
    std_rate = float(unit['dcStdRate'])
    deposit = float(unit.get('dcStdSecDep', std_rate))
    print(f"Test unit: {unit_id} ({unit.get('sUnitName')}) "
          f"rate=${std_rate} deposit=${deposit}")
    print(f"Charge config: Rent={cfg['Rent'][1]}% Admin={cfg['AdminFee'][1]}% "
          f"SecDep={cfg['SecDep'][1]}% Insurance={cfg['Insurance'][1]}%")
    print(f"Admin fee from config: ${admin_fee}\n")

    # Insurance plans
    with engine.connect() as conn:
        ins_rows = conn.execute(text("""
            SELECT "InsurCoverageID", "dcCoverage", "dcPremium"
            FROM ccws_insurance_coverage
            WHERE "SiteCode"='LSETUP'
            ORDER BY "dcCoverage"
        """)).fetchall()
    ins_3 = next(r for r in ins_rows if float(r[2]) == 3.0)
    ins_10 = next(r for r in ins_rows if float(r[2]) == 10.0)

    # Discount selections — only "Recurring Discount" (iAmtType=1, simple %)
    # to avoid prepaid/free-month plans which have multi-month math we don't model
    with engine.connect() as conn:
        disc_rows = conn.execute(text("""
            SELECT cd."ConcessionID", cd."sPlanName", cd."dcPCDiscount",
                   cd."dcFixedDiscount", cd."iAmtType"
            FROM ccws_discount cd JOIN siteinfo si ON cd."SiteID" = si."SiteID"
            WHERE si."SiteCode"='LSETUP' AND cd."dDisabled" IS NULL
        """)).fetchall()
    # Recurring Discount plans only for percentage tests
    recurring = [r for r in disc_rows
                 if 'Recurring Discount' in (r[1] or '') and float(r[2]) > 0]
    disc_by_pc = {float(r[2]): r for r in recurring}
    # Fixed coupons (iAmtType=0)
    disc_by_fixed = {float(r[3]): r for r in disc_rows
                     if float(r[3]) > 0 and r[4] == 0}

    def get_disc(pc=None, fixed=None):
        if pc is not None and pc in disc_by_pc:
            r = disc_by_pc[pc]
            return int(r[0]), float(r[2]), 0
        if fixed is not None and fixed in disc_by_fixed:
            r = disc_by_fixed[fixed]
            return int(r[0]), 0, float(r[3])
        return 0, 0, 0

    # Build scenario matrix
    dates = [
        (datetime(2026, 5, 1),  "May 01 (1st of 31-day month)"),
        (datetime(2026, 5, 15), "May 15 (mid 31-day month)"),
        (datetime(2026, 4, 17), "Apr 17 (mid 30-day month)"),
        (datetime(2026, 5, 28), "May 28 (late, triggers 2nd month)"),
        (datetime(2026, 2, 15), "Feb 15 (28-day month)"),
        (datetime(2026, 6, 1),  "Jun 01 (1st of 30-day month)"),
    ]

    # Each scenario: (label, concession_id, pc, fixed, ins_id, ins_premium)
    # Only "Recurring Discount" plans for percentage cases — prepaid/free-month
    # plans have multi-month math the calculator does not model
    base_scenarios = [
        ("baseline (no disc, no ins)",      *get_disc(),          0, 0),
        ("5% Recurring",                    *get_disc(pc=5),      0, 0),
        ("10% Recurring",                   *get_disc(pc=10),     0, 0),
        ("25% Recurring",                   *get_disc(pc=25),     0, 0),
        ("50% Recurring",                   *get_disc(pc=50),     0, 0),
        ("$5 fixed coupon",                 *get_disc(fixed=5),   0, 0),
        ("$10 fixed coupon",                *get_disc(fixed=10),  0, 0),
        ("$50 fixed coupon",                *get_disc(fixed=50),  0, 0),
        ("insurance $3/mo",                 *get_disc(),          int(ins_3[0]), float(ins_3[2])),
        ("insurance $10/mo",                *get_disc(),          int(ins_10[0]), float(ins_10[2])),
        ("5% Recurring + $3 ins",           *get_disc(pc=5),      int(ins_3[0]), float(ins_3[2])),
        ("50% Recurring + $10 ins",         *get_disc(pc=50),     int(ins_10[0]), float(ins_10[2])),
        ("$50 fixed + $3 ins",              *get_disc(fixed=50),  int(ins_3[0]), float(ins_3[2])),
    ]

    # Run matrix
    results = []
    tid = make_tenant(client)
    print(f"Tenant for matrix: {tid}\n")

    for date, date_label in dates:
        move_in_iso = date.strftime("%Y-%m-%dT00:00:00")
        wid = make_reservation(client, tid, unit_id, move_in_iso)
        try:
            for label, cid, pc, fixed, iid, premium in base_scenarios:
                # SOAP ground truth
                soap_charges = soap_movein_cost(client, unit_id, wid, move_in_iso, iid, cid)
                soap_total = sum(float(r.get('dcTotal', 0)) for r in soap_charges)

                calc_charges = calculate_movein_cost(
                    std_rate=std_rate, security_deposit=deposit, admin_fee=admin_fee,
                    move_in_date=date,
                    rent_tax=rent_tax, admin_tax=admin_tax,
                    deposit_tax=dep_tax, insurance_tax=ins_tax,
                    pc_discount=pc, fixed_discount=fixed,
                    insurance_premium=premium,
                )
                calc_total = float(estimate_total(calc_charges))

                ok = cmp_money(calc_total, soap_total)
                status = 'PASS' if ok else 'FAIL'
                results.append({
                    'date': date_label, 'scenario': label,
                    'soap_total': soap_total, 'calc_total': calc_total,
                    'status': status,
                    'diff': abs(calc_total - soap_total),
                })
                marker = '✓' if ok else '✗'
                print(f"  [{marker}] {date_label} | {label}: "
                      f"calc=${calc_total:.2f} soap=${soap_total:.2f} {status}")
        finally:
            cancel_reservation(client, wid)

    # Edge case: calculator-only sanity checks (no SOAP comparison meaningful)
    print(f"\n{'='*70}")
    print("  CALCULATOR EDGE CASES (no SOAP comparison)")
    print(f"{'='*70}")

    edge_date = datetime(2026, 4, 17)
    edge_cases = [
        ("baseline (calc-only ref)",
         dict(std_rate=75, security_deposit=75, admin_fee=30)),
        ("admin_fee=0 (admin waived)",
         dict(std_rate=75, security_deposit=75, admin_fee=0)),
        ("security_deposit=0 (no deposit)",
         dict(std_rate=75, security_deposit=0, admin_fee=30)),
        ("admin_fee=0 + deposit=0 (rent only)",
         dict(std_rate=75, security_deposit=0, admin_fee=0)),
        ("admin_fee=0 + deposit=0 + 100% disc",
         dict(std_rate=75, security_deposit=0, admin_fee=0, pc_discount=100)),
        ("anniversary billing (full month)",
         dict(std_rate=75, security_deposit=75, admin_fee=30, anniversary_billing=True)),
    ]
    for label, kw in edge_cases:
        ch = calculate_movein_cost(
            move_in_date=edge_date, rent_tax=rent_tax,
            admin_tax=admin_tax, deposit_tax=dep_tax,
            insurance_tax=ins_tax, **kw)
        total = float(estimate_total(ch))
        lines = ", ".join(f"{c.description.split()[0]}=${c.total}" for c in ch)
        print(f"  {label}: ${total:.2f}  [{lines}]")

    # Summary
    print(f"\n{'='*70}")
    print("  MATRIX SUMMARY")
    print(f"{'='*70}")
    passed = sum(1 for r in results if r['status'] == 'PASS')
    failed = sum(1 for r in results if r['status'] == 'FAIL')
    skipped = sum(1 for r in results if r['status'] == 'SKIP')
    total = len(results)
    print(f"  Total scenarios:  {total}")
    print(f"  Passed:           {passed}")
    print(f"  Failed:           {failed}")
    print(f"  Skipped (fixed):  {skipped}")
    if total - skipped > 0:
        print(f"  Pass rate (excl. skip): {passed/(total-skipped)*100:.1f}%")

    if failed:
        print(f"\n  Failures:")
        for r in results:
            if r['status'] == 'FAIL':
                print(f"    - {r['date']} | {r['scenario']}: "
                      f"calc=${r['calc_total']:.2f} soap=${r['soap_total']:.2f} "
                      f"diff=${r.get('diff', 0):.2f}")

    client.close()


if __name__ == "__main__":
    main()
