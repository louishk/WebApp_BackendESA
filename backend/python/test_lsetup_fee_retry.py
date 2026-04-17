"""
LSETUP Fee Retry — test ReservationFeeAddWithSource_v2 with iCreditCardType=1 (Visa)
Uses the tenant and reservation from the previous test run.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta
from common.config import DataLayerConfig
from common.soap_client import SOAPClient
import json

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
        for i, row in enumerate(r[:5]):
            print(f"  Row {i}: {json.dumps(row, indent=2, default=str)}")
    else:
        print(f"  {r}")

client = get_client()
start_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
expires = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%dT00:00:00")
followup = (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%dT00:00:00")
end_date = (datetime.now() + timedelta(days=31)).strftime("%Y-%m-%dT00:00:00")

try:
    # Create fresh tenant + reservation for this test
    print("[1] Creating fresh tenant...")
    t = soap_call(client, "TenantNewDetailed_v3", {
        "sLocationCode": SITE, "sWebPassword": "", "sMrMrs": "",
        "sFName": "FeeTest", "sMI": "", "sLName": f"Run{datetime.now().strftime('%H%M%S')}",
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
    print(f"  TenantID: {tid}")

    # Find available unit
    units = soap_call(client, "UnitsInformationAvailableUnitsOnly_v2",
                      {"sLocationCode": SITE}, result_tag="Table")
    uid = units[0]["UnitID"]
    rate = units[0].get("dcStdRate", "0")
    print(f"  UnitID: {uid}, Rate: {rate}")

    # Reserve
    print("\n[2] Reserving...")
    r = soap_call(client, "ReservationNewWithSource_v6", {
        "sLocationCode": SITE, "sTenantID": str(tid), "sUnitID": str(uid),
        "dNeeded": start_date, "sComment": "Fee test", "iSource": "0",
        "sSource": "FeeTest", "QTRentalTypeID": "0", "iInquiryType": "0",
        "dcQuotedRate": str(rate), "dExpires": expires, "dFollowUp": followup,
        "sTrackingCode": "", "sCallerID": "", "ConcessionID": "0", "PromoGlobalNum": "0",
    })
    wid = int(r[0]["Ret_Code"])
    print(f"  WaitingID: {wid}")

    # Test fee add with different CC types
    for cc_type, cc_label in [(1, "Visa"), (2, "MasterCard"), (3, "Amex")]:
        print(f"\n[3] ReservationFeeAddWithSource_v2 iCreditCardType={cc_type} ({cc_label}) bTestMode=true...")
        try:
            result = soap_call(client, "ReservationFeeAddWithSource_v2", {
                "sLocationCode": SITE,
                "iTenantID": str(tid),
                "iWaitingListID": str(wid),
                "iCreditCardType": str(cc_type),
                "sCreditCardNumber": "4111111111111111",
                "sCreditCardCVV": "123",
                "dExpirationDate": "2030-01-01T00:00:00",
                "sBillingName": "FeeTest",
                "sBillingAddress": "1 Test St",
                "sBillingZipCode": "000000",
                "bTestMode": "true",
                "iSource": "0",
            })
            pr(f"Fee Add CC Type {cc_type} ({cc_label})", result)
            if result:
                row = result[0] if isinstance(result, list) else result
                rc = row.get("Ret_Code", "N/A")
                rm = row.get("Ret_Msg", "N/A")
                print(f"  Ret_Code={rc}, Ret_Msg={rm}")
                if rc and int(rc) > 0:
                    print(f"  >>> SUCCESS with CC type {cc_type}!")
                    break
        except Exception as e:
            print(f"  ERROR: {e}")

    # Also try v1
    print(f"\n[4] ReservationFeeAddWithSource (v1) iCreditCardType=1 bTestMode=true...")
    try:
        result_v1 = soap_call(client, "ReservationFeeAddWithSource", {
            "sLocationCode": SITE,
            "iTenantID": str(tid),
            "iWaitingListID": str(wid),
            "iCreditCardType": "1",
            "sCreditCardNumber": "4111111111111111",
            "sCreditCardCVV": "123",
            "dExpirationDate": "2030-01-01T00:00:00",
            "sBillingName": "FeeTest",
            "sBillingAddress": "1 Test St",
            "sBillingZipCode": "000000",
            "bTestMode": "true",
            "iSource": "0",
        })
        pr("Fee Add v1", result_v1)
        if result_v1:
            row = result_v1[0] if isinstance(result_v1, list) else result_v1
            print(f"  Ret_Code={row.get('Ret_Code', 'N/A')}, Ret_Msg={row.get('Ret_Msg', 'N/A')}")
    except Exception as e:
        print(f"  ERROR: {e}")

finally:
    client.close()
