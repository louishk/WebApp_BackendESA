"""
LSETUP Full Booking Engine Flow Experiment
============================================

End-to-end test exercising every new component:

1. Browse available units (would be /api/units/available)
2. Pull discount plans from DB (would be /api/reservations/discount-plans)
3. Pull insurance plans from DB (synced via new pipeline)
4. Pull charge config from DB (synced via new pipeline)
5. Calculate cost INTERNALLY using the calculator (no SOAP)
6. Call MoveInCostRetrieve SOAP for ground truth
7. Compare calculator vs SOAP — validate match
8. Create tenant
9. Reserve unit with discount
10. Add reservation fee — REAL MODE (bTestMode=false) — actually posts
11. Move-in with bTestMode=true (validate without committing lease)

This is the booking engine flow that the future frontend will execute.
"""

import sys
import os
import json
from datetime import datetime, timedelta, date
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
                       soap_action=f"{CC_NS}/{op}",
                       namespace=CC_NS, result_tag=result_tag)


def section(title):
    print(f"\n{'='*70}\n  {title}\n{'='*70}")


def main():
    client = get_client()
    engine = create_engine(get_database_url('pbi'))
    move_in_date = datetime(2026, 5, 17)
    move_in_iso = move_in_date.strftime("%Y-%m-%dT00:00:00")
    end_date = (move_in_date + timedelta(days=30)).strftime("%Y-%m-%dT00:00:00")
    expires = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%dT00:00:00")
    followup = (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%dT00:00:00")

    try:
        # =====================================================================
        # STEP 1: Browse available units
        # =====================================================================
        section("STEP 1: Browse available units (UnitsInformationAvailableUnitsOnly_v2)")
        units = soap_call(client, "UnitsInformationAvailableUnitsOnly_v2",
                          {"sLocationCode": SITE}, result_tag="Table")
        print(f"  {len(units)} units available")
        # Pick a unit with a unique rate for clarity
        unit = next((u for u in units if float(u.get('dcStdRate', 0)) > 50), units[0])
        unit_id = unit['UnitID']
        unit_name = unit.get('sUnitName', '?')
        std_rate = float(unit.get('dcStdRate', 0))
        deposit = float(unit.get('dcStdSecDep', std_rate))
        print(f"  Selected: UnitID={unit_id} Name={unit_name} Rate=${std_rate} Deposit=${deposit}")

        # =====================================================================
        # STEP 2: Fetch discount plans from DB (synced)
        # =====================================================================
        section("STEP 2: Discount plans from DB (ccws_discount, synced)")
        with engine.connect() as conn:
            plans = conn.execute(text("""
                SELECT cd."ConcessionID", cd."sPlanName", cd."dcPCDiscount", cd."dcFixedDiscount"
                FROM ccws_discount cd
                JOIN siteinfo si ON cd."SiteID" = si."SiteID"
                WHERE si."SiteCode" = :site AND cd."dDisabled" IS NULL
                ORDER BY cd."dcPCDiscount" DESC
                LIMIT 10
            """), {"site": SITE}).fetchall()
        print(f"  {len(plans)} active discount plans:")
        for p in plans[:5]:
            print(f"    CID={p[0]} {p[1]:30s} {p[2]}% off")
        # Pick a 5% discount
        chosen_discount = next((p for p in plans if float(p[2]) == 5.0), None)
        cid = int(chosen_discount[0]) if chosen_discount else 0
        pc_disc = float(chosen_discount[2]) if chosen_discount else 0
        print(f"  Selected: ConcessionID={cid} ({pc_disc}% discount)")

        # =====================================================================
        # STEP 3: Fetch insurance plans from DB (synced)
        # =====================================================================
        section("STEP 3: Insurance plans from DB (ccws_insurance_coverage, synced)")
        with engine.connect() as conn:
            ins_rows = conn.execute(text("""
                SELECT "InsurCoverageID", "dcCoverage", "dcPremium"
                FROM ccws_insurance_coverage
                WHERE "SiteCode" = :site
                ORDER BY "dcCoverage"
                LIMIT 5
            """), {"site": SITE}).fetchall()
        print(f"  {len(ins_rows)} insurance plans (showing first 5):")
        for r in ins_rows:
            print(f"    CoverageID={r[0]}: ${r[1]} coverage @ ${r[2]}/mo")
        chosen_ins = ins_rows[0] if ins_rows else None
        ins_id = int(chosen_ins[0]) if chosen_ins else 0
        ins_premium = float(chosen_ins[2]) if chosen_ins else 0
        print(f"  Selected: CoverageID={ins_id} (${ins_premium}/mo)")

        # =====================================================================
        # STEP 4: Fetch charge type config from DB (synced)
        # =====================================================================
        section("STEP 4: Charge type config from DB (ccws_charge_descriptions, synced)")
        with engine.connect() as conn:
            charges = conn.execute(text("""
                SELECT "sChgCategory", "dcTax1Rate", "dcTax2Rate", "dcPrice"
                FROM ccws_charge_descriptions
                WHERE "SiteCode" = :site
                  AND "sChgCategory" IN ('Rent','AdminFee','SecDep','Insurance')
                  AND "dDisabled" IS NULL
            """), {"site": SITE}).fetchall()
        cfg = {r[0]: r for r in charges}
        for cat, row in cfg.items():
            print(f"  {cat}: tax1={row[1]}% tax2={row[2]}% price=${row[3]}")
        admin_fee = float(cfg['AdminFee'][3])
        print(f"  Admin fee: ${admin_fee}")

        # =====================================================================
        # STEP 5: Calculate cost INTERNALLY (no SOAP)
        # =====================================================================
        section("STEP 5: Internal calculator (no SOAP round-trip)")
        rent_tax = ChargeTypeTax('Rent', cfg['Rent'][1], cfg['Rent'][2])
        admin_tax = ChargeTypeTax('AdminFee', cfg['AdminFee'][1], cfg['AdminFee'][2])
        dep_tax = ChargeTypeTax('SecDep', cfg['SecDep'][1], cfg['SecDep'][2])
        ins_tax = ChargeTypeTax('Insurance', cfg['Insurance'][1], cfg['Insurance'][2])

        internal_charges = calculate_movein_cost(
            std_rate=std_rate,
            security_deposit=deposit,
            admin_fee=admin_fee,
            move_in_date=move_in_date,
            rent_tax=rent_tax, admin_tax=admin_tax,
            deposit_tax=dep_tax, insurance_tax=ins_tax,
            pc_discount=pc_disc,
            insurance_premium=ins_premium,
        )
        internal_total = estimate_total(internal_charges)
        print(f"  Calculator output ({len(internal_charges)} lines):")
        for c in internal_charges:
            print(f"    {c.description:30s} amount=${c.charge_amount:>7} disc=${c.discount} tax1=${c.tax1} total=${c.total}")
        print(f"  INTERNAL TOTAL: ${internal_total}")

        # =====================================================================
        # STEP 6: Need a tenant + reservation BEFORE calling MoveInCostRetrieve
        # =====================================================================
        section("STEP 6: Create tenant (TenantNewDetailed_v3)")
        tenant_result = soap_call(client, "TenantNewDetailed_v3", {
            "sLocationCode": SITE, "sWebPassword": "", "sMrMrs": "",
            "sFName": "FullFlow",
            "sMI": "",
            "sLName": f"Test{datetime.now().strftime('%H%M%S')}",
            "sCompany": "", "sAddr1": "1 Test St", "sAddr2": "",
            "sCity": "Singapore", "sRegion": "", "sPostalCode": "000000",
            "sCountry": "SG", "sPhone": "99999999",
            "sMrMrsAlt": "", "sFNameAlt": "", "sMIAlt": "", "sLNameAlt": "",
            "sAddr1Alt": "", "sAddr2Alt": "", "sCityAlt": "", "sRegionAlt": "",
            "sPostalCodeAlt": "", "sCountryAlt": "", "sPhoneAlt": "",
            "sMrMrsBus": "", "sFNameBus": "", "sMIBus": "", "sLNameBus": "",
            "sCompanyBus": "", "sAddr1Bus": "", "sAddr2Bus": "", "sCityBus": "",
            "sRegionBus": "", "sPostalCodeBus": "", "sCountryBus": "", "sPhoneBus": "",
            "sEmail": "fullflow@test.com", "sEmailAlt": "", "sEmailBus": "",
            "sFax": "", "sFaxAlt": "", "sFaxBus": "", "sMobile": "",
            "dDOB": "1990-01-01T00:00:00", "sSSN": "", "sDriversLic": "",
            "bBusiness": "false", "sIDType": "", "sIDNum": "", "sIDIssuer": "",
            "bTestMode": "false",
        })
        tid = int(tenant_result[0]["TenantID"])
        print(f"  TenantID: {tid}")

        section("STEP 6b: Reserve unit (ReservationNewWithSource_v6, with discount)")
        res = soap_call(client, "ReservationNewWithSource_v6", {
            "sLocationCode": SITE, "sTenantID": str(tid), "sUnitID": str(unit_id),
            "dNeeded": move_in_iso, "sComment": "Full flow experiment",
            "iSource": "0", "sSource": "FullFlow",
            "QTRentalTypeID": "0", "iInquiryType": "0",
            "dcQuotedRate": str(std_rate),
            "dExpires": expires, "dFollowUp": followup,
            "sTrackingCode": "", "sCallerID": "",
            "ConcessionID": str(cid),
            "PromoGlobalNum": "0",
        })
        wid = int(res[0]["Ret_Code"])
        print(f"  WaitingID: {wid} (with ConcessionID={cid} attached)")

        # =====================================================================
        # STEP 7: SOAP MoveInCostRetrieve for ground truth
        # =====================================================================
        section("STEP 7: SOAP MoveInCostRetrieveWithDiscount_Reservation_v4 (ground truth)")
        soap_charges = soap_call(client,
            "MoveInCostRetrieveWithDiscount_Reservation_v4", {
                "sLocationCode": SITE, "iUnitID": str(unit_id),
                "dMoveInDate": move_in_iso,
                "InsuranceCoverageID": str(ins_id),
                "ConcessionPlanID": str(cid),
                "WaitingID": str(wid),
                "iPromoGlobalNum": "0", "ChannelType": "0",
                "bApplyInsuranceCredit": "false",
            }, result_tag="Table")
        soap_total = sum(float(r.get('dcTotal', 0)) for r in soap_charges)
        print(f"  SOAP output ({len(soap_charges)} lines):")
        for r in soap_charges:
            print(f"    {r.get('ChargeDescription', '?'):30s} "
                  f"amount={r.get('ChargeAmount'):>7} disc={r.get('dcDiscount'):>6} "
                  f"total={r.get('dcTotal'):>7}")
        print(f"  SOAP TOTAL: ${soap_total:.2f}")

        # =====================================================================
        # STEP 8: Validate calculator vs SOAP
        # =====================================================================
        section("STEP 8: Calculator vs SOAP comparison")
        diff = abs(float(internal_total) - soap_total)
        match = diff < 0.01
        print(f"  Internal: ${internal_total}")
        print(f"  SOAP:     ${soap_total:.2f}")
        print(f"  Diff:     ${diff:.2f}")
        print(f"  Match:    {'YES (within $0.01)' if match else 'NO — investigate'}")

        # =====================================================================
        # STEP 9: Add reservation fee — REAL MODE
        # =====================================================================
        section("STEP 9: ReservationFeeAddWithSource_v2 — REAL MODE (bTestMode=false)")
        fee_result = soap_call(client, "ReservationFeeAddWithSource_v2", {
            "sLocationCode": SITE,
            "iTenantID": str(tid),
            "iWaitingListID": str(wid),
            "iCreditCardType": "5",
            "sCreditCardNumber": "4111111111111111",
            "sCreditCardCVV": "123",
            "dExpirationDate": "2030-01-01T00:00:00",
            "sBillingName": "FullFlow Test",
            "sBillingAddress": "1 Test St",
            "sBillingZipCode": "000000",
            "bTestMode": "false",
            "iSource": "0",
        })
        fee_rc = fee_result[0].get('Ret_Code')
        fee_msg = fee_result[0].get('Ret_Msg', '')
        print(f"  Ret_Code={fee_rc} Ret_Msg={fee_msg}")
        if fee_rc and int(fee_rc) > 0:
            print(f"  >>> SUCCESS — reservation fee posted to SiteLink in REAL mode")
        else:
            print(f"  >>> FAIL — fee did not post")

        # =====================================================================
        # STEP 10: Move-in (bTestMode=true to avoid creating real lease)
        # =====================================================================
        section("STEP 10: MoveInReservation_v6 — bTestMode=true (validate, no real lease)")
        movein = soap_call(client, "MoveInReservation_v6", {
            "sLocationCode": SITE,
            "WaitingID": str(wid),
            "TenantID": str(tid),
            "UnitID": str(unit_id),
            "dStartDate": move_in_iso,
            "dEndDate": end_date,
            "dcPaymentAmount": f"{soap_total:.2f}",
            "iCreditCardType": "0",
            "sCreditCardNumber": "", "sCreditCardCVV": "",
            "dExpirationDate": "2030-01-01T00:00:00",
            "sBillingName": "", "sBillingAddress": "", "sBillingZipCode": "",
            "InsuranceCoverageID": str(ins_id),
            "ConcessionPlanID": str(cid),
            "iPayMethod": "2",  # cash bypass
            "sABARoutingNum": "", "sAccountNum": "", "iAccountType": "0",
            "iSource": "0",
            "bTestMode": "true",  # safe test
            "bApplyInsuranceCredit": "false",
            "iPromoGlobalNum": "0",
        })
        mi_rc = movein[0].get('Ret_Code')
        mi_msg = movein[0].get('Ret_Msg', '')
        lease_num = movein[0].get('iLeaseNum')
        print(f"  Ret_Code={mi_rc} (LedgerID if positive)")
        print(f"  iLeaseNum={lease_num}")
        print(f"  Ret_Msg={mi_msg}")
        if mi_rc and int(mi_rc) > 0:
            print(f"  >>> Move-in validated (bTestMode=true — no real lease created)")
        else:
            print(f"  >>> Move-in validation failed")

        # =====================================================================
        # SUMMARY
        # =====================================================================
        section("FULL FLOW SUMMARY")
        print(f"  Site:               {SITE}")
        print(f"  Unit:               {unit_id} ({unit_name}) @ ${std_rate}/mo")
        print(f"  Discount:           ConcessionID={cid} ({pc_disc}%)")
        print(f"  Insurance:          CoverageID={ins_id} (${ins_premium}/mo)")
        print(f"  Move-in date:       {move_in_date.strftime('%Y-%m-%d')}")
        print(f"  Calculator total:   ${internal_total}")
        print(f"  SOAP ground truth:  ${soap_total:.2f}")
        print(f"  Match:              {'YES' if match else 'NO'}")
        print(f"  Tenant created:     {tid}")
        print(f"  Reservation:        WaitingID={wid}")
        print(f"  Fee posted (real):  {'YES' if fee_rc and int(fee_rc) > 0 else 'NO'}")
        print(f"  Move-in validated:  {'YES' if mi_rc and int(mi_rc) > 0 else 'NO'}")

    finally:
        client.close()


if __name__ == "__main__":
    main()
