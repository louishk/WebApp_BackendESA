"""
Probe SOAP MoveInCostRetrieveWithDiscount_Reservation_v4 for several
LSETUP concessions whose prepay/iInMonth flags vary. Compare the SOAP
move-in payment (line items + total) against our calculator's first-month
total + duration_months breakdown.

Goal: see exactly how SiteLink charges a prepay-style concession (multi-
month rent bundled into the move-in payment, or single-month with discount,
or anniversary-billing multi-month reset). Validate our calculator's
recurring-month assumption against what SiteLink actually does.

Run from /var/www/backend/backend/python on the VM:
    python3 scripts/probe_lsetup_prepay_durations.py
"""
from __future__ import annotations

import os, sys
from datetime import datetime, date
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url
from common.soap_client import SOAPClient
from common.config import DataLayerConfig
from common.movein_cost_calculator import (
    calculate_duration_breakdown, ChargeTypeTax,
)

CC_NS = "http://tempuri.org/CallCenterWs/CallCenterWs"
SITE = "LSETUP"
MOVE_IN = "2026-05-15T00:00:00"

# Concessions to probe — drawn from ccws_discount where SiteCode=LSETUP.
# Each entry: (concession_id, sPlanName, expectation_string).
CONCESSIONS = [
    (8055,  "30% OFF (prepay flag, no prepaid_months)",   "first-month 30%; recurring full"),
    (4645,  "7th Month Free (prepay 6)",                  "month-1 + 5 prepaid bundled?"),
    (4639,  "13th Month Free (prepay 12)",                "month-1 + 11 prepaid bundled?"),
    (10775, "2 Months Free (100% pct, prepay 4)",         "month-1 free; 3 prepaid full?"),
    (11281, "1.5 Months Free (50% pct, prepay 3)",        "month-1 50% off; 2 prepaid full?"),
    (4676,  "5th Month Free (prepay 4)",                  "month-1 + 3 prepaid bundled?"),
]

DURATIONS = [6, 9, 12]


def get_soap_client() -> SOAPClient:
    cfg = DataLayerConfig.from_env()
    cc_url = cfg.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
    return SOAPClient(
        base_url=cc_url, corp_code=cfg.soap.corp_code,
        corp_user=cfg.soap.corp_user, api_key=cfg.soap.api_key,
        corp_password=cfg.soap.corp_password, timeout=60, retries=1,
    )


def soap_call(client, op, params, result_tag="Table"):
    return client.call(operation=op, parameters=params,
                       soap_action=f"{CC_NS}/{op}", namespace=CC_NS,
                       result_tag=result_tag)


def pick_unit(engine):
    """Pick the cheapest available LSETUP unit with a real std_rate."""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT "UnitID", "dcStdRate", "dcStdSecDep", "dcWidth", "dcLength",
                   "sTypeName"
            FROM ccws_available_units
            WHERE "sLocationCode" = :s AND "dcStdRate" > 0
            ORDER BY "dcStdRate" LIMIT 1
        """), {'s': SITE}).fetchone()
        if not row:
            raise SystemExit(f"No available units at {SITE}")
        return dict(row._mapping)


def pick_insurance(engine):
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT "InsurCoverageID", "dcPremium"
            FROM ccws_insurance_coverage
            WHERE "SiteCode" = :s ORDER BY "dcPremium" LIMIT 1
        """), {'s': SITE}).fetchone()
        return int(row[0]), float(row[1])


def billing_config(engine):
    """Pull tax + admin fee + deposit from middleware. Tax rates live on
    ccws_charge_descriptions itself (per-category)."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT cd."sChgCategory" AS cat,
                   cd."dcPrice"     AS price,
                   COALESCE(cd."dcTax1Rate", 0) AS t1,
                   COALESCE(cd."dcTax2Rate", 0) AS t2
            FROM ccws_charge_descriptions cd
            JOIN mw_siteinfo s ON s."SiteID" = cd."SiteID"
            WHERE s."SiteCode" = :s
              AND cd."sChgCategory" IN ('Rent','AdminFee','SecDep','Insurance')
              AND cd."dDisabled" IS NULL
        """), {'s': SITE}).fetchall()
    out = {}
    for cat, price, t1, t2 in rows:
        # SiteLink stores the rate as a fraction (0.09 = 9%); the
        # calculator's ChargeTypeTax also expects fractional values, so
        # we pass through directly without dividing by 100 again.
        out[cat] = {
            'price': float(price or 0),
            't1':    float(t1 or 0),
            't2':    float(t2 or 0),
        }
    return out


def main():
    client = get_soap_client()
    engine = create_engine(get_database_url('middleware'))

    unit = pick_unit(engine)
    ins_id, ins_premium = pick_insurance(engine)
    cfg = billing_config(engine)

    print(f"\nProbe target — site {SITE}")
    print(f"  unit_id      : {unit['UnitID']} ({unit['sTypeName']})")
    print(f"  std_rate     : ${unit['dcStdRate']}")
    print(f"  std_sec_dep  : ${unit['dcStdSecDep']}")
    print(f"  insurance    : id={ins_id} ${ins_premium}/mo")
    print(f"  move-in date : {MOVE_IN[:10]}")
    print()
    print("  charge config (from ccws_charge_descriptions):")
    for k, v in cfg.items():
        print(f"    {k:<10}: price=${v['price']:.2f}  tax1={v['t1']}%  tax2={v['t2']}%")

    # Tenant + reservation needed for variant=reservation; create one once.
    tenant = soap_call(client, "TenantNewDetailed_v3", {
        "sLocationCode": SITE, "sWebPassword": "", "sMrMrs": "",
        "sFName": "DurationProbe", "sMI": "",
        "sLName": f"T{datetime.now().strftime('%H%M%S')}",
        "sCompany": "", "sAddr1": "1 Test St", "sAddr2": "",
        "sCity": "Singapore", "sRegion": "", "sPostalCode": "000000",
        "sCountry": "SG", "sPhone": "99999999",
        "sMrMrsAlt": "", "sFNameAlt": "", "sMIAlt": "", "sLNameAlt": "",
        "sAddr1Alt": "", "sAddr2Alt": "", "sCityAlt": "", "sRegionAlt": "",
        "sPostalCodeAlt": "", "sCountryAlt": "", "sPhoneAlt": "",
        "sMrMrsBus": "", "sFNameBus": "", "sMIBus": "", "sLNameBus": "",
        "sCompanyBus": "", "sAddr1Bus": "", "sAddr2Bus": "", "sCityBus": "",
        "sRegionBus": "", "sPostalCodeBus": "", "sCountryBus": "", "sPhoneBus": "",
        "sEmail": "probe@test.com", "sEmailAlt": "", "sEmailBus": "",
        "sFax": "", "sFaxAlt": "", "sFaxBus": "", "sMobile": "",
        "dDOB": "1990-01-01T00:00:00", "sSSN": "", "sDriversLic": "",
        "bBusiness": "false", "sIDType": "", "sIDNum": "", "sIDIssuer": "",
        "bTestMode": "false",
    }, result_tag="RT")
    tid = int(tenant[0]["TenantID"])
    print(f"  tenant_id    : {tid} (created for probe)")

    res = soap_call(client, "ReservationNewWithSource_v6", {
        "sLocationCode": SITE, "sTenantID": str(tid), "sUnitID": str(unit['UnitID']),
        "dNeeded": MOVE_IN, "sComment": "Duration-probe (DELETE)",
        "iSource": "0", "sSource": "DurationProbe",
        "QTRentalTypeID": "0", "iInquiryType": "0",
        "dcQuotedRate": str(unit['dcStdRate']),
        "dExpires": "2026-06-15T00:00:00", "dFollowUp": "2026-06-04T00:00:00",
        "sTrackingCode": "", "sCallerID": "",
        "ConcessionID": "0", "PromoGlobalNum": "0",
    }, result_tag="RT")
    wid = int(res[0]["Ret_Code"])
    print(f"  waiting_id   : {wid} (will cancel at end)\n")

    # ---- Probe loop -------------------------------------------------------
    for cid, label, expect in CONCESSIONS:
        print("=" * 72)
        print(f"Concession {cid} — {label}")
        print(f"  expectation: {expect}")
        try:
            charges = soap_call(client,
                "MoveInCostRetrieveWithDiscount_Reservation_v4", {
                    "sLocationCode": SITE,
                    "iUnitID": str(unit['UnitID']),
                    "dMoveInDate": MOVE_IN,
                    "InsuranceCoverageID": str(ins_id),
                    "ConcessionPlanID": str(cid),
                    "WaitingID": str(wid),
                    "iPromoGlobalNum": "0",
                    "ChannelType": "0",
                    "bApplyInsuranceCredit": "false",
                }, result_tag="Table")
        except Exception as e:
            print(f"  SOAP error: {e}\n")
            continue

        # Print line items
        total = Decimal('0')
        print(f"  SOAP line items ({len(charges)} rows):")
        for c in charges:
            desc = c.get('ChargeDescription') or c.get('Description') or '?'
            amt = Decimal(str(c.get('ChargeAmount') or c.get('Amount') or 0))
            tax1 = Decimal(str(c.get('Tax1') or c.get('dcTax1') or 0))
            tax2 = Decimal(str(c.get('Tax2') or c.get('dcTax2') or 0))
            line_total = amt + tax1 + tax2
            total += line_total
            print(f"    {desc:<40}  amt=${amt:>9}  tax=${(tax1+tax2):>6}  total=${line_total:>9}")
        print(f"  SOAP grand total (move-in payment) = ${total:.2f}\n")

        # Same probe through our calculator for several lease durations
        from common.movein_cost_calculator import calculate_movein_cost
        rt = ChargeTypeTax('Rent',
            tax1_rate=Decimal(str(cfg.get('Rent',{}).get('t1', 0))) / 100,
            tax2_rate=Decimal(str(cfg.get('Rent',{}).get('t2', 0))) / 100)
        at = ChargeTypeTax('AdminFee',
            tax1_rate=Decimal(str(cfg.get('AdminFee',{}).get('t1', 0))) / 100,
            tax2_rate=Decimal(str(cfg.get('AdminFee',{}).get('t2', 0))) / 100)
        dt = ChargeTypeTax('SecDep')
        it = ChargeTypeTax('Insurance',
            tax1_rate=Decimal(str(cfg.get('Insurance',{}).get('t1', 0))) / 100,
            tax2_rate=Decimal(str(cfg.get('Insurance',{}).get('t2', 0))) / 100)

        # Pull the concession params from middleware to drive the calculator
        with engine.connect() as conn:
            crow = conn.execute(text("""
                SELECT "iAmtType","dcPCDiscount","dcFixedDiscount",
                       "dcMaxAmountOff","iInMonth","bPrepay","iPrePaidMonths"
                FROM ccws_discount WHERE "ConcessionID" = :c
                  AND "SiteID" IN (SELECT "SiteID" FROM mw_siteinfo WHERE "SiteCode"='LSETUP')
            """), {'c': cid}).fetchone()
        amt_type = crow[0]; pct = float(crow[1] or 0); fixed = float(crow[2] or 0)
        max_off = crow[3]; in_month = int(crow[4] or 1)
        prepay = bool(crow[5]); prepaid_months = int(crow[6] or 0)
        sec_dep = float(unit['dcStdSecDep'] or unit['dcStdRate'])
        admin_fee = cfg.get('AdminFee', {}).get('price', 0)

        for dur in DURATIONS:
            try:
                q = calculate_duration_breakdown(
                    std_rate=float(unit['dcStdRate']),
                    security_deposit=sec_dep,
                    admin_fee=admin_fee,
                    move_in_date=date(2026, 5, 15),
                    rent_tax=rt, admin_tax=at, deposit_tax=dt, insurance_tax=it,
                    pc_discount=pct, fixed_discount=fixed,
                    insurance_premium=ins_premium,
                    anniversary_billing=True,   # LSETUP is anniversary billing
                    duration_months=dur,
                    concession_in_month=in_month,
                    concession_prepay_months=prepaid_months,
                    max_amount_off=max_off,
                )
                m1 = q.breakdown[0]
                disc_total = sum((mb.discount or Decimal('0')) for mb in q.breakdown)
                print(f"  Calculator duration={dur:>2}mo: "
                      f"month1_total=${q.first_month_total:>8} "
                      f"contract_total=${q.total_contract:>9} "
                      f"discount_total=${disc_total:>7}  conf={q.confidence}")
            except Exception as e:
                print(f"  Calculator duration={dur:>2}mo failed: {e}")
        print()

    # Cleanup
    try:
        soap_call(client, "ReservationUpdate_v4", {
            "sLocationCode": SITE, "iWaitingID": str(wid), "iStatus": "2",
            "dExpires": "2026-06-15T00:00:00", "dFollowUp": "2026-06-04T00:00:00",
            "sNote": "probe cleanup",
        }, result_tag="RT")
        print(f"Reservation {wid} cancelled.")
    except Exception as e:
        print(f"Cleanup failed (manually cancel waiting_id={wid}): {e}")


if __name__ == "__main__":
    main()
