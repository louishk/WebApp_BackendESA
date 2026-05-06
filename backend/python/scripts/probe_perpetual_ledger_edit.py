"""
Live probe — full reserve / move-in / inspect ledger / try edits / move-out
cycle on LSETUP, with concession 4666 "30% Recurring Discount".

Goal: understand what the ledger actually looks like after move-in, and
whether we can use ChargePriceUpdate or RecurringChargeAddToLedger_v1
to push the discount across all months programmatically.

Steps:
  1. Reserve a small LSETUP unit
  2. Get cost (validation step)
  3. Move-in with concession 4666 (real lease creation, real money cycle —
     uses pay_method=2 cash to skip card processing on the test site)
  4. ChargesAllByLedgerID — print every line
  5. Try ChargePriceUpdate on the next-period rent charge — reduce by 30%
  6. ChargesAllByLedgerID again — confirm what changed
  7. Try RecurringChargeAddToLedger_v1 with negative amount (best-effort)
  8. ChargesAllByLedgerID — confirm
  9. MoveOut cleanup

DO NOT run against production sites. LSETUP only.

Run on the VM:
    PYTHONPATH=/var/www/backend/backend/python python3 scripts/probe_perpetual_ledger_edit.py
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


def print_ledger(client, ledger_id: int, label: str):
    print(f"\n{'-'*70}")
    print(f"  Ledger snapshot — {label}")
    print(f"{'-'*70}")
    try:
        rows = soap(client, "ChargesAllByLedgerID", {"ledgerId": str(ledger_id)})
    except Exception as e:
        print(f"  ChargesAllByLedgerID failed: {e}")
        return
    if not rows:
        print("  (no charges)")
        return
    # First row — show all columns once so we know the schema
    if rows:
        print(f"  schema columns: {list(rows[0].keys())}")
        print()
    for r in rows[:25]:
        # Try to extract the most relevant fields
        cid = r.get('ChargeID') or r.get('iChargeID') or r.get('id') or ''
        desc = r.get('Description') or r.get('sDescription') or r.get('ChargeDescription') or '?'
        amt = r.get('Amount') or r.get('dcAmount') or r.get('ChargeAmount') or r.get('dcAmtPreTax') or '?'
        ds = r.get('StartDate') or r.get('dStart') or r.get('dStartDate') or ''
        de = r.get('EndDate') or r.get('dEnd') or r.get('dEndDate') or ''
        ds = str(ds)[:10]; de = str(de)[:10]
        print(f"  charge={cid:<7} {desc:<40} amt={amt!s:<10} {ds}..{de}")
    if len(rows) > 25:
        print(f"  ... ({len(rows)} total rows)")


def main():
    engine = create_engine(get_database_url('middleware'))
    client = get_client()

    # 1. Pick the cheapest LSETUP unit
    with engine.connect() as conn:
        u = conn.execute(text("""
            SELECT "UnitID","dcStdRate","dcStdSecDep","sTypeName"
            FROM ccws_available_units
            WHERE "sLocationCode"=:s AND "dcStdRate" > 0
            ORDER BY "dcStdRate" LIMIT 1
        """), {'s': SITE}).fetchone()
    unit_id = int(u[0]); std_rate = float(u[1])
    print(f"Unit:   {unit_id}  ({u[3]})  std_rate=${std_rate}  std_sec_dep=${u[2]}")

    with engine.connect() as conn:
        ins = conn.execute(text("""
            SELECT "InsurCoverageID","dcPremium" FROM ccws_insurance_coverage
            WHERE "SiteCode"=:s ORDER BY "dcPremium" LIMIT 1
        """), {'s': SITE}).fetchone()
    ins_id = int(ins[0]); ins_premium = float(ins[1])
    print(f"Insurance: id={ins_id}  ${ins_premium}/mo")
    print(f"Concession: {CONCESSION_ID} (30% Recurring Discount, iInMonth=1)\n")

    move_in_dt = datetime.now().replace(microsecond=0)
    move_in_iso = move_in_dt.strftime("%Y-%m-%dT00:00:00")
    end_iso = (move_in_dt.replace(year=move_in_dt.year + 1)).strftime("%Y-%m-%dT00:00:00")

    # 2. Tenant + reservation
    print("[1] Creating tenant + reservation…")
    tenant = soap(client, "TenantNewDetailed_v3", {
        "sLocationCode": SITE, "sWebPassword": "", "sMrMrs": "",
        "sFName": "PerpProbe", "sMI": "",
        "sLName": f"T{move_in_dt.strftime('%H%M%S')}",
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
        "dNeeded": move_in_iso, "sComment": "Perpetual ledger probe (DELETE)",
        "iSource": "0", "sSource": "PerpProbe",
        "QTRentalTypeID": "0", "iInquiryType": "0",
        "dcQuotedRate": str(std_rate),
        "dExpires": end_iso, "dFollowUp": move_in_iso,
        "sTrackingCode": "", "sCallerID": "",
        "ConcessionID": "0", "PromoGlobalNum": "0",
    }, result_tag="RT")
    wid = int(res[0]["Ret_Code"])
    print(f"   tenant={tid}  reservation={wid}\n")

    # 3. Get move-in cost
    print("[2] Fetching move-in cost…")
    cost = soap(client, "MoveInCostRetrieveWithDiscount_Reservation_v4", {
        "sLocationCode": SITE, "iUnitID": str(unit_id),
        "dMoveInDate": move_in_iso, "InsuranceCoverageID": str(ins_id),
        "ConcessionPlanID": str(CONCESSION_ID), "WaitingID": str(wid),
        "iPromoGlobalNum": "0", "ChannelType": "0",
        "bApplyInsuranceCredit": "false",
    })
    move_in_cost = Decimal('0')
    for c in cost:
        amt = Decimal(str(c.get('ChargeAmount') or 0))
        t1 = Decimal(str(c.get('Tax1') or 0))
        t2 = Decimal(str(c.get('Tax2') or 0))
        move_in_cost += amt + t1 + t2
    print(f"   move-in payment due = ${move_in_cost:.2f}\n")

    # 4. Move-In (cash bypass — pay_method=2, no card)
    print("[3] Moving in (pay_method=2 cash)…")
    try:
        movein = soap(client, "MoveInReservation_v6", {
            "sLocationCode": SITE,
            "WaitingID": str(wid), "TenantID": str(tid), "UnitID": str(unit_id),
            "dStartDate": move_in_iso, "dEndDate": end_iso,
            "dcPaymentAmount": f"{move_in_cost:.2f}",
            "iCreditCardType": "0", "sCreditCardNumber": "",
            "sCreditCardCVV": "", "dExpirationDate": "2030-01-01T00:00:00",
            "sBillingName": "", "sBillingAddress": "", "sBillingZipCode": "",
            "InsuranceCoverageID": str(ins_id),
            "ConcessionPlanID": str(CONCESSION_ID),
            "iPayMethod": "2",
            "sABARoutingNum": "", "sAccountNum": "", "iAccountType": "0",
            "iSource": "0",
            "bTestMode": "false",
            "bApplyInsuranceCredit": "false",
            "iPromoGlobalNum": "0",
        }, result_tag="RT")
    except Exception as e:
        print(f"   MoveIn error: {e}\n")
        # Cancel the reservation as best-effort cleanup
        try:
            soap(client, "ReservationUpdate_v4", {
                "sLocationCode": SITE, "iWaitingID": str(wid), "iStatus": "2",
                "dExpires": end_iso, "dFollowUp": move_in_iso,
                "sNote": "probe cleanup",
            }, result_tag="RT")
            print(f"   reservation {wid} cancelled.")
        except Exception as ex:
            print(f"   reservation cleanup failed: {ex}")
        raise SystemExit(1)

    if not movein:
        print("   MoveIn returned empty result — aborting")
        raise SystemExit(1)
    ret_code = movein[0].get("Ret_Code")
    ret_msg = movein[0].get("Ret_Msg", "")
    print(f"   MoveIn ret_code={ret_code} ret_msg={ret_msg!r}")

    # ret_code is the LedgerID
    try:
        ledger_id = int(ret_code)
    except Exception:
        print("   could not parse LedgerID from ret_code")
        raise SystemExit(1)
    print(f"   ledger_id={ledger_id}\n")

    # 5. Inspect the ledger
    print_ledger(client, ledger_id, "AFTER MOVE-IN (concession 4666 applied)")

    # 6. Try to find a future Rent charge to test ChargePriceUpdate on
    print("\n[4] Trying ChargePriceUpdate on a future Rent charge…")
    try:
        rows = soap(client, "ChargesAllByLedgerID", {"ledgerId": str(ledger_id)})
        rent_rows = []
        for r in rows:
            desc = r.get('Description') or r.get('sDescription') or r.get('ChargeDescription') or ''
            cat = r.get('sChgCategory') or r.get('Category') or ''
            if 'Rent' in str(desc) or 'Rent' in str(cat):
                rent_rows.append(r)
        print(f"   found {len(rent_rows)} rent rows")
        if rent_rows:
            target = rent_rows[-1]   # use the last one (most future)
            ch_id = target.get('ChargeID') or target.get('iChargeID') or target.get('id')
            cur_amt = Decimal(str(target.get('Amount') or target.get('dcAmount') or target.get('ChargeAmount') or 0))
            new_amt = (cur_amt * Decimal('0.7')).quantize(Decimal('0.01'))
            print(f"   targeting charge_id={ch_id}  current ${cur_amt}  new ${new_amt}")
            try:
                upd = soap(client, "ChargePriceUpdate", {
                    "ledgerId": str(ledger_id),
                    "chargeId": str(ch_id),
                    "amount": str(new_amt),
                }, result_tag="RT")
                print(f"   ChargePriceUpdate result: {upd}")
            except Exception as e:
                print(f"   ChargePriceUpdate error: {e}")
        else:
            print("   no rent rows visible — may need a different inspection op")
    except Exception as e:
        print(f"   ChargesAllByLedgerID failed pre-update: {e}")

    print_ledger(client, ledger_id, "AFTER ChargePriceUpdate attempt")

    # 7. Try RecurringChargeAddToLedger_v1 with a negative amount (Rent desc id)
    print("\n[5] Trying RecurringChargeAddToLedger_v1 with -30% Rent...")
    with engine.connect() as conn:
        rent_desc = conn.execute(text("""
            SELECT "ChargeDescID" FROM ccws_charge_descriptions
            WHERE "SiteCode"='LSETUP' AND "sChgCategory"='Rent' AND "dDisabled" IS NULL
            LIMIT 1
        """)).scalar()
    rent_desc_id = int(rent_desc) if rent_desc else None
    print(f"   rent ChargeDescID = {rent_desc_id}")
    if rent_desc_id:
        recur_amt = -(Decimal(str(std_rate)) * Decimal('0.30')).quantize(Decimal('0.01'))
        try:
            rc = soap(client, "RecurringChargeAddToLedger_v1", {
                "LedgerID": str(ledger_id),
                "ChargeDescID": str(rent_desc_id),
                "dcInitialChargeAmt": "0",
                "dcRecurringRateAmt": str(recur_amt),
                "iQty": "1",
            }, result_tag="RT")
            print(f"   RecurringChargeAddToLedger_v1 result: {rc}")
        except Exception as e:
            print(f"   RecurringChargeAddToLedger_v1 error: {e}")

    print_ledger(client, ledger_id, "AFTER RecurringChargeAddToLedger attempt")

    # 8. Cleanup — MoveOut
    print("\n[6] Cleanup MoveOut…")
    try:
        mo = soap(client, "MoveOut", {
            "sLocationCode": SITE, "sUsagePassword": "",
            "TenantID": str(tid), "UnitID": str(unit_id),
        }, result_tag="RT")
        print(f"   MoveOut result: {mo}")
    except Exception as e:
        print(f"   MoveOut error: {e}")
        print(f"   *** MANUALLY MOVE OUT — TenantID={tid} UnitID={unit_id} on {SITE} ***")


if __name__ == "__main__":
    main()
