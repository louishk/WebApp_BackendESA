"""
LSETUP Round 2:
1. Debug DiscountPlansRetrieve — try different result tags
2. Debug InsuranceCoverageRetrieve_V3 — try with/without unit_id, different tags
3. Retry ReservationFeeAddWithSource with $100 fee now configured
4. Test MoveInCostRetrieve with a ConcessionID
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
        if len(r) > 10:
            print(f"  ... and {len(r)-10} more rows")
    else:
        print(f"  {r}")

client = get_client()
start_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
expires = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%dT00:00:00")
followup = (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%dT00:00:00")
end_date = (datetime.now() + timedelta(days=31)).strftime("%Y-%m-%dT00:00:00")

try:
    # =========================================================================
    # TEST 1: DiscountPlansRetrieve — try different result tags
    # =========================================================================
    print("\n[TEST 1a] DiscountPlansRetrieve with result_tag='Table'...")
    try:
        d = soap_call(client, "DiscountPlansRetrieve", {"sLocationCode": SITE}, result_tag="Table")
        print(f"  Count: {len(d)}")
        if d: pr("Discounts (Table tag)", d)
    except Exception as e:
        print(f"  Error: {e}")

    print("\n[TEST 1b] DiscountPlansRetrieve with result_tag='RT'...")
    try:
        d2 = soap_call(client, "DiscountPlansRetrieve", {"sLocationCode": SITE}, result_tag="RT")
        print(f"  Count: {len(d2)}")
        if d2: pr("Discounts (RT tag)", d2)
    except Exception as e:
        print(f"  Error: {e}")

    # Try DiscountPlansRetrieveIncludingDisabled
    print("\n[TEST 1c] DiscountPlansRetrieveIncludingDisabled with result_tag='Table'...")
    try:
        d3 = soap_call(client, "DiscountPlansRetrieveIncludingDisabled",
                        {"sLocationCode": SITE}, result_tag="Table")
        print(f"  Count: {len(d3)}")
        if d3: pr("Discounts Including Disabled", d3)
    except Exception as e:
        print(f"  Error: {e}")

    # Try raw call to see what comes back
    print("\n[TEST 1d] DiscountPlansRetrieve raw XML check...")
    try:
        # Use call_raw if available, otherwise just try with different tags
        for tag in ["DiscountPlansRetrieveResult", "diffgram", "NewDataSet"]:
            try:
                d4 = soap_call(client, "DiscountPlansRetrieve", {"sLocationCode": SITE}, result_tag=tag)
                if d4:
                    print(f"  Tag '{tag}': {len(d4)} results")
                    pr(f"Discounts ({tag})", d4)
                    break
            except:
                print(f"  Tag '{tag}': no match")
    except Exception as e:
        print(f"  Error: {e}")

    # =========================================================================
    # TEST 2: InsuranceCoverageRetrieve_V3 — debug
    # =========================================================================
    # First get a unit ID
    units = soap_call(client, "UnitsInformationAvailableUnitsOnly_v2",
                      {"sLocationCode": SITE}, result_tag="Table")
    uid = units[0]["UnitID"] if units else "0"
    print(f"\n  Using UnitID: {uid}")

    print("\n[TEST 2a] InsuranceCoverageRetrieve_V3 with UnitID=0 (all plans)...")
    try:
        ins = soap_call(client, "InsuranceCoverageRetrieve_V3",
                        {"sLocationCode": SITE, "iUnitID": "0"}, result_tag="Table")
        print(f"  Count: {len(ins)}")
        if ins: pr("Insurance (UnitID=0)", ins)
    except Exception as e:
        print(f"  Error: {e}")

    print(f"\n[TEST 2b] InsuranceCoverageRetrieve_V3 with UnitID={uid}...")
    try:
        ins2 = soap_call(client, "InsuranceCoverageRetrieve_V3",
                         {"sLocationCode": SITE, "iUnitID": str(uid)}, result_tag="Table")
        print(f"  Count: {len(ins2)}")
        if ins2: pr(f"Insurance (UnitID={uid})", ins2)
    except Exception as e:
        print(f"  Error: {e}")

    # Try older versions
    print("\n[TEST 2c] InsuranceCoverageRetrieve (v1) ...")
    try:
        ins3 = soap_call(client, "InsuranceCoverageRetrieve",
                         {"sLocationCode": SITE}, result_tag="Table")
        print(f"  Count: {len(ins3)}")
        if ins3: pr("Insurance v1", ins3)
    except Exception as e:
        print(f"  Error: {e}")

    print("\n[TEST 2d] InsuranceCoverageRetrieve_V2...")
    try:
        ins4 = soap_call(client, "InsuranceCoverageRetrieve_V2",
                         {"sLocationCode": SITE}, result_tag="Table")
        print(f"  Count: {len(ins4)}")
        if ins4: pr("Insurance V2", ins4)
    except Exception as e:
        print(f"  Error: {e}")

    # =========================================================================
    # TEST 3: Retry ReservationFeeAddWithSource (fee now $100)
    # =========================================================================
    print("\n[TEST 3] ReservationFee retry with $100 fee configured...")

    # Check fee amount first
    fee = soap_call(client, "ReservationFeeRetrieve", {"sLocationCode": SITE}, result_tag="Table")
    pr("ReservationFeeRetrieve", fee)

    # Create fresh tenant + reservation
    t = soap_call(client, "TenantNewDetailed_v3", {
        "sLocationCode": SITE, "sWebPassword": "", "sMrMrs": "",
        "sFName": "FeeTest2", "sMI": "", "sLName": f"R2{datetime.now().strftime('%H%M%S')}",
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

    r = soap_call(client, "ReservationNewWithSource_v6", {
        "sLocationCode": SITE, "sTenantID": str(tid), "sUnitID": str(uid),
        "dNeeded": start_date, "sComment": "Fee retry", "iSource": "0",
        "sSource": "FeeTest2", "QTRentalTypeID": "0", "iInquiryType": "0",
        "dcQuotedRate": "0", "dExpires": expires, "dFollowUp": followup,
        "sTrackingCode": "", "sCallerID": "", "ConcessionID": "0", "PromoGlobalNum": "0",
    })
    wid = int(r[0]["Ret_Code"])
    print(f"  WaitingID: {wid}")

    # Try with all CC types including higher numbers
    for cc_type in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
        print(f"\n  Fee Add v2 iCreditCardType={cc_type} bTestMode=true...")
        try:
            result = soap_call(client, "ReservationFeeAddWithSource_v2", {
                "sLocationCode": SITE,
                "iTenantID": str(tid),
                "iWaitingListID": str(wid),
                "iCreditCardType": str(cc_type),
                "sCreditCardNumber": "4111111111111111",
                "sCreditCardCVV": "123",
                "dExpirationDate": "2030-01-01T00:00:00",
                "sBillingName": "FeeTest2",
                "sBillingAddress": "1 Test St",
                "sBillingZipCode": "000000",
                "bTestMode": "true",
                "iSource": "0",
            })
            row = result[0] if isinstance(result, list) else result
            rc = row.get("Ret_Code", "N/A")
            rm = row.get("Ret_Msg", "N/A")
            print(f"    Ret_Code={rc}, Ret_Msg={rm}")
            if rc != "N/A" and int(rc) > 0:
                print(f"    >>> SUCCESS with CC type {cc_type}!")
                pr(f"Fee Add SUCCESS (type {cc_type})", result)
                break
        except Exception as e:
            print(f"    Error: {e}")

    # =========================================================================
    # TEST 4: MoveInCostRetrieve WITH a discount (need ConcessionID first)
    # =========================================================================
    # We'll try with ConcessionID if discounts were found above
    print("\n[TEST 4] MoveInCostRetrieve with ConcessionID...")

    # Try to get discounts one more time with the pipeline data
    # Check ccws_discount table directly
    print("  Checking ccws_discount table for LSETUP ConcessionIDs...")
    try:
        from sqlalchemy import create_engine, text
        from common.config_loader import get_database_url
        engine = create_engine(get_database_url('pbi'))
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT "ConcessionID", "sConcessionName", "iDiscountType", "bEnabled"
                FROM ccws_discount
                WHERE "SiteCode" = 'LSETUP'
                ORDER BY "ConcessionID"
                LIMIT 10
            """)).fetchall()
            if rows:
                for row in rows:
                    print(f"    ConcessionID={row[0]}, Name={row[1]}, Type={row[2]}, Enabled={row[3]}")
                # Use first enabled one for cost test
                enabled = [r for r in rows if str(r[3]).lower() == 'true']
                if enabled:
                    test_cid = enabled[0][0]
                    print(f"\n  Testing MoveInCostRetrieve with ConcessionID={test_cid}...")
                    cost = soap_call(client, "MoveInCostRetrieveWithDiscount_Reservation_v4", {
                        "sLocationCode": SITE,
                        "iUnitID": str(uid),
                        "dMoveInDate": start_date,
                        "InsuranceCoverageID": "0",
                        "ConcessionPlanID": str(test_cid),
                        "WaitingID": str(wid),
                        "iPromoGlobalNum": "0",
                        "ChannelType": "0",
                        "bApplyInsuranceCredit": "false",
                    }, result_tag="Table")
                    pr(f"MoveInCost with ConcessionID={test_cid}", cost)
                    if cost:
                        total = sum(float(row.get("dcTotal", 0)) for row in cost)
                        disc = sum(float(row.get("dcDiscount", 0)) for row in cost)
                        print(f"\n  Total: ${total:.2f}, Discount applied: ${disc:.2f}")
            else:
                print("    No ccws_discount rows found for LSETUP")
    except Exception as e:
        print(f"    DB error: {e}")

finally:
    client.close()
