"""
LSETUP Round 3:
1. MoveInCostRetrieve with ConcessionID=4661 (5% Recurring Discount)
2. MoveInCostRetrieve with InsuranceCoverageID=9649 ($1000 coverage, $3 premium)
3. MoveInCostRetrieve with BOTH discount + insurance
4. DiscountPlansRetrieve debug — why does SOAP return 0?
"""
import sys, os, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta
from common.config import DataLayerConfig
from common.soap_client import SOAPClient

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
                       soap_action=f"{CC_NS}/{op}", namespace=CC_NS, result_tag=result_tag)

def pr(label, r):
    print(f"\n{'='*60}\n  {label}\n{'='*60}")
    if isinstance(r, list):
        for i, row in enumerate(r[:10]):
            print(f"  Row {i}: {json.dumps(row, indent=2, default=str)}")
    else:
        print(f"  {r}")

client = get_client()
start_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")

try:
    # Get unit
    units = soap_call(client, "UnitsInformationAvailableUnitsOnly_v2",
                      {"sLocationCode": SITE}, result_tag="Table")
    uid = units[0]["UnitID"]
    print(f"UnitID: {uid}")

    # Need a WaitingID — create tenant + reservation
    t = soap_call(client, "TenantNewDetailed_v3", {
        "sLocationCode": SITE, "sWebPassword": "", "sMrMrs": "",
        "sFName": "DiscTest", "sMI": "", "sLName": f"R3{datetime.now().strftime('%H%M%S')}",
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
    expires = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%dT00:00:00")
    followup = (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%dT00:00:00")
    r = soap_call(client, "ReservationNewWithSource_v6", {
        "sLocationCode": SITE, "sTenantID": str(tid), "sUnitID": str(uid),
        "dNeeded": start_date, "sComment": "Discount test", "iSource": "0",
        "sSource": "DiscTest", "QTRentalTypeID": "0", "iInquiryType": "0",
        "dcQuotedRate": "0", "dExpires": expires, "dFollowUp": followup,
        "sTrackingCode": "", "sCallerID": "", "ConcessionID": "0", "PromoGlobalNum": "0",
    })
    wid = int(r[0]["Ret_Code"])
    print(f"TenantID: {tid}, WaitingID: {wid}")

    # =========================================================================
    # TEST 1: No discount, no insurance (baseline)
    # =========================================================================
    print("\n[TEST 1] MoveInCost — NO discount, NO insurance...")
    cost1 = soap_call(client, "MoveInCostRetrieveWithDiscount_Reservation_v4", {
        "sLocationCode": SITE, "iUnitID": str(uid), "dMoveInDate": start_date,
        "InsuranceCoverageID": "0", "ConcessionPlanID": "0",
        "WaitingID": str(wid), "iPromoGlobalNum": "0",
        "ChannelType": "0", "bApplyInsuranceCredit": "false",
    }, result_tag="Table")
    total1 = sum(float(row.get("dcTotal", 0)) for row in cost1)
    for row in cost1:
        print(f"  {row.get('ChargeDescription', '?'):30s} Amount={row.get('ChargeAmount'):>8s} Tax={row.get('TaxAmount'):>8s} Total={row.get('dcTotal'):>8s} Discount={row.get('dcDiscount')}")
    print(f"  BASELINE TOTAL: ${total1:.2f}")

    # =========================================================================
    # TEST 2: With 5% Recurring Discount (CID=4661)
    # =========================================================================
    print("\n[TEST 2] MoveInCost — ConcessionPlanID=4661 (5% Recurring)...")
    cost2 = soap_call(client, "MoveInCostRetrieveWithDiscount_Reservation_v4", {
        "sLocationCode": SITE, "iUnitID": str(uid), "dMoveInDate": start_date,
        "InsuranceCoverageID": "0", "ConcessionPlanID": "4661",
        "WaitingID": str(wid), "iPromoGlobalNum": "0",
        "ChannelType": "0", "bApplyInsuranceCredit": "false",
    }, result_tag="Table")
    total2 = sum(float(row.get("dcTotal", 0)) for row in cost2)
    for row in cost2:
        print(f"  {row.get('ChargeDescription', '?'):30s} Amount={row.get('ChargeAmount'):>8s} Tax={row.get('TaxAmount'):>8s} Total={row.get('dcTotal'):>8s} Discount={row.get('dcDiscount')}")
    print(f"  WITH 5% DISCOUNT TOTAL: ${total2:.2f} (saved ${total1-total2:.2f})")

    # =========================================================================
    # TEST 3: With 10% Recurring Discount (CID=4662)
    # =========================================================================
    print("\n[TEST 3] MoveInCost — ConcessionPlanID=4662 (10% Recurring)...")
    cost3 = soap_call(client, "MoveInCostRetrieveWithDiscount_Reservation_v4", {
        "sLocationCode": SITE, "iUnitID": str(uid), "dMoveInDate": start_date,
        "InsuranceCoverageID": "0", "ConcessionPlanID": "4662",
        "WaitingID": str(wid), "iPromoGlobalNum": "0",
        "ChannelType": "0", "bApplyInsuranceCredit": "false",
    }, result_tag="Table")
    total3 = sum(float(row.get("dcTotal", 0)) for row in cost3)
    for row in cost3:
        print(f"  {row.get('ChargeDescription', '?'):30s} Amount={row.get('ChargeAmount'):>8s} Tax={row.get('TaxAmount'):>8s} Total={row.get('dcTotal'):>8s} Discount={row.get('dcDiscount')}")
    print(f"  WITH 10% DISCOUNT TOTAL: ${total3:.2f} (saved ${total1-total3:.2f})")

    # =========================================================================
    # TEST 4: With Insurance (CoverageID=9649, $1000/$3 premium)
    # =========================================================================
    print("\n[TEST 4] MoveInCost — InsuranceCoverageID=9649 ($1000 coverage, $3/mo)...")
    cost4 = soap_call(client, "MoveInCostRetrieveWithDiscount_Reservation_v4", {
        "sLocationCode": SITE, "iUnitID": str(uid), "dMoveInDate": start_date,
        "InsuranceCoverageID": "9649", "ConcessionPlanID": "0",
        "WaitingID": str(wid), "iPromoGlobalNum": "0",
        "ChannelType": "0", "bApplyInsuranceCredit": "false",
    }, result_tag="Table")
    total4 = sum(float(row.get("dcTotal", 0)) for row in cost4)
    for row in cost4:
        print(f"  {row.get('ChargeDescription', '?'):30s} Amount={row.get('ChargeAmount'):>8s} Tax={row.get('TaxAmount'):>8s} Total={row.get('dcTotal'):>8s} Discount={row.get('dcDiscount')}")
    print(f"  WITH INSURANCE TOTAL: ${total4:.2f} (insurance adds ${total4-total1:.2f})")

    # =========================================================================
    # TEST 5: Discount + Insurance combined
    # =========================================================================
    print("\n[TEST 5] MoveInCost — 5% Discount + Insurance...")
    cost5 = soap_call(client, "MoveInCostRetrieveWithDiscount_Reservation_v4", {
        "sLocationCode": SITE, "iUnitID": str(uid), "dMoveInDate": start_date,
        "InsuranceCoverageID": "9649", "ConcessionPlanID": "4661",
        "WaitingID": str(wid), "iPromoGlobalNum": "0",
        "ChannelType": "0", "bApplyInsuranceCredit": "false",
    }, result_tag="Table")
    total5 = sum(float(row.get("dcTotal", 0)) for row in cost5)
    for row in cost5:
        print(f"  {row.get('ChargeDescription', '?'):30s} Amount={row.get('ChargeAmount'):>8s} Tax={row.get('TaxAmount'):>8s} Total={row.get('dcTotal'):>8s} Discount={row.get('dcDiscount')}")
    print(f"  DISCOUNT + INSURANCE TOTAL: ${total5:.2f}")

    # =========================================================================
    # TEST 6: DiscountPlansRetrieve — why 0 from SOAP? Try IncludingDisabled
    # =========================================================================
    print("\n[TEST 6] DiscountPlansRetrieveIncludingDisabled — try RT tag...")
    d = soap_call(client, "DiscountPlansRetrieveIncludingDisabled",
                  {"sLocationCode": SITE}, result_tag="RT")
    pr("DiscountPlansRetrieveIncludingDisabled (RT)", d)

    # =========================================================================
    # SUMMARY
    # =========================================================================
    print(f"\n{'='*60}")
    print(f"  COST COMPARISON SUMMARY")
    print(f"{'='*60}")
    print(f"  Baseline (no discount, no insurance):     ${total1:.2f}")
    print(f"  With 5% discount:                         ${total2:.2f} (saved ${total1-total2:.2f})")
    print(f"  With 10% discount:                        ${total3:.2f} (saved ${total1-total3:.2f})")
    print(f"  With insurance ($3/mo):                   ${total4:.2f} (added ${total4-total1:.2f})")
    print(f"  With 5% discount + insurance:             ${total5:.2f}")

finally:
    client.close()
