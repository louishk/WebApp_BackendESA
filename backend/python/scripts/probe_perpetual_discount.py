"""
Probe SOAP for LSETUP concession 4666 "30% Recurring Discount".

This concession has the same flag config as production SS-TAC-Move-R-30%:
  iAmtType=1, dcPCDiscount=30, iInMonth=1, bPrepay=false, iPrePaidMonths=1.

The staff/CRM screenshot showed that SiteLink applies the discount
perpetually via the "Apply Tenant's Rate" button — months 2+ are charged
at the discounted rate even though the discount line only shows on
month 1.

This probe asks two questions:
  1. What does MoveInCostRetrieveWithDiscount_Reservation_v4 return for
     this concession? Confirms "discount-line on month 1 only" at the
     reservation API level.
  2. Does SOAP expose any "future rent schedule" or "rate after move-in"
     operation that would tell us whether the post-move-in lease will
     have the discount baked in via Tenant's Rate?

Run on the VM:
    PYTHONPATH=/var/www/backend/backend/python python3 scripts/probe_perpetual_discount.py
"""
from __future__ import annotations
import os, sys
from datetime import datetime
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url
from common.soap_client import SOAPClient
from common.config import DataLayerConfig

CC_NS = "http://tempuri.org/CallCenterWs/CallCenterWs"
SITE = "LSETUP"
MOVE_IN = "2026-05-15T00:00:00"
CONCESSION_ID = 4666  # "30% Recurring Discount"


def get_client() -> SOAPClient:
    cfg = DataLayerConfig.from_env()
    cc_url = cfg.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
    return SOAPClient(
        base_url=cc_url, corp_code=cfg.soap.corp_code,
        corp_user=cfg.soap.corp_user, api_key=cfg.soap.api_key,
        corp_password=cfg.soap.corp_password, timeout=60, retries=1,
    )


def soap(client, op, params, result_tag="Table"):
    return client.call(operation=op, parameters=params,
                       soap_action=f"{CC_NS}/{op}", namespace=CC_NS,
                       result_tag=result_tag)


def main():
    engine = create_engine(get_database_url('middleware'))
    client = get_client()

    # ---- Pick a unit ----
    with engine.connect() as conn:
        u = conn.execute(text("""
            SELECT "UnitID","dcStdRate","dcStdSecDep","sTypeName"
            FROM ccws_available_units
            WHERE "sLocationCode"=:s AND "dcStdRate" > 0
            ORDER BY "dcStdRate" LIMIT 1
        """), {'s': SITE}).fetchone()
    unit_id = int(u[0]); std_rate = float(u[1])
    print(f"Probe target — site={SITE} unit={unit_id} std_rate=${std_rate}\n")

    with engine.connect() as conn:
        ins = conn.execute(text("""
            SELECT "InsurCoverageID","dcPremium" FROM ccws_insurance_coverage
            WHERE "SiteCode"=:s ORDER BY "dcPremium" LIMIT 1
        """), {'s': SITE}).fetchone()
    ins_id = int(ins[0])

    # ---- Tenant + reservation ----
    tenant = soap(client, "TenantNewDetailed_v3", {
        "sLocationCode": SITE, "sWebPassword": "", "sMrMrs": "",
        "sFName": "PerpetualProbe", "sMI": "",
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
    res = soap(client, "ReservationNewWithSource_v6", {
        "sLocationCode": SITE, "sTenantID": str(tid), "sUnitID": str(unit_id),
        "dNeeded": MOVE_IN, "sComment": "Perpetual-probe (DELETE)",
        "iSource": "0", "sSource": "PerpetualProbe",
        "QTRentalTypeID": "0", "iInquiryType": "0",
        "dcQuotedRate": str(std_rate),
        "dExpires": "2026-06-15T00:00:00", "dFollowUp": "2026-06-04T00:00:00",
        "sTrackingCode": "", "sCallerID": "",
        "ConcessionID": "0", "PromoGlobalNum": "0",
    }, result_tag="RT")
    wid = int(res[0]["Ret_Code"])
    print(f"Tenant {tid} · Reservation {wid}\n")

    # ============================================================
    # 1. MoveInCostRetrieve — what SOAP says the move-in payment is
    # ============================================================
    print("=" * 70)
    print(f"1) MoveInCostRetrieveWithDiscount_Reservation_v4 with concession {CONCESSION_ID}")
    print("=" * 70)
    charges = soap(client, "MoveInCostRetrieveWithDiscount_Reservation_v4", {
        "sLocationCode": SITE, "iUnitID": str(unit_id),
        "dMoveInDate": MOVE_IN, "InsuranceCoverageID": str(ins_id),
        "ConcessionPlanID": str(CONCESSION_ID), "WaitingID": str(wid),
        "iPromoGlobalNum": "0", "ChannelType": "0",
        "bApplyInsuranceCredit": "false",
    }, result_tag="Table")
    total = Decimal('0')
    for c in charges:
        amt = Decimal(str(c.get('ChargeAmount') or 0))
        t1 = Decimal(str(c.get('Tax1') or 0))
        t2 = Decimal(str(c.get('Tax2') or 0))
        line = amt + t1 + t2
        total += line
        print(f"  {(c.get('ChargeDescription') or '?'):<40}  amt=${amt:>9}  tax=${(t1+t2):>6}  total=${line:>9}")
    print(f"  TOTAL = ${total:.2f}\n")

    # ============================================================
    # 2. Try a couple of "schedule" operations to see what SOAP
    #    can tell us about post-move-in behaviour.
    # ============================================================
    print("=" * 70)
    print("2) Try TenantBillingScheduleRetrieve / RentTenantRateRetrieve (if available)")
    print("=" * 70)

    candidate_ops = [
        ("TenantBillingScheduleRetrieve",
         {"sLocationCode": SITE, "iWaitingID": str(wid)}),
        ("RentTenantRateRetrieve",
         {"sLocationCode": SITE, "iWaitingID": str(wid)}),
        ("LeaseFutureRentScheduleRetrieve",
         {"sLocationCode": SITE, "iWaitingID": str(wid)}),
        ("RentRateRetrieve_v3",
         {"sLocationCode": SITE, "iUnitID": str(unit_id),
          "dStartDate": MOVE_IN, "iMonths": "6",
          "ConcessionPlanID": str(CONCESSION_ID),
          "iPromoGlobalNum": "0"}),
        ("ConcessionGetForMoveIn",
         {"sLocationCode": SITE, "iUnitID": str(unit_id),
          "ConcessionPlanID": str(CONCESSION_ID),
          "dMoveInDate": MOVE_IN}),
    ]
    for op_name, params in candidate_ops:
        try:
            rows = soap(client, op_name, params, result_tag="Table")
            print(f"  [{op_name}] returned {len(rows) if rows else 0} rows")
            if rows:
                for r in rows[:8]:
                    print(f"     {r}")
        except Exception as e:
            print(f"  [{op_name}] ERROR: {str(e)[:140]}")
        print()

    # ============================================================
    # 3. Cleanup
    # ============================================================
    try:
        soap(client, "ReservationUpdate_v4", {
            "sLocationCode": SITE, "iWaitingID": str(wid), "iStatus": "2",
            "dExpires": "2026-06-15T00:00:00", "dFollowUp": "2026-06-04T00:00:00",
            "sNote": "probe cleanup",
        }, result_tag="RT")
        print(f"Reservation {wid} cancelled.")
    except Exception as e:
        print(f"Cleanup err (manually cancel waiting_id={wid}): {e}")


if __name__ == "__main__":
    main()
