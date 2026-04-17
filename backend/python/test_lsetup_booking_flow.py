"""
LSETUP Booking Engine Flow Test
================================
Tests the full booking flow against LSETUP test site:
1. Find an available unit
2. Create a tenant
3. Reserve the unit (no cost retrieve first)
4. Try ReservationFeeAddWithSource with dummy CC
5. MoveInCostRetrieve to get exact amount
6. Move-in with our own price vs correct price (bTestMode=true)

All destructive operations use bTestMode=true unless explicitly overridden.
"""

import sys
import os
import json
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common.config import DataLayerConfig
from common.soap_client import SOAPClient

CC_NS = "http://tempuri.org/CallCenterWs/CallCenterWs"
SITE = "LSETUP"


def get_client():
    config = DataLayerConfig.from_env()
    cc_url = config.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
    return SOAPClient(
        base_url=cc_url,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=60,
        retries=1,
    )


def soap_call(client, operation, params, result_tag="RT"):
    """Call a SOAP operation and return parsed results."""
    return client.call(
        operation=operation,
        parameters=params,
        soap_action=f"{CC_NS}/{operation}",
        namespace=CC_NS,
        result_tag=result_tag,
    )


def print_result(label, results):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    if isinstance(results, list) and len(results) > 0:
        for i, r in enumerate(results[:5]):  # limit to 5 rows
            print(f"  Row {i}: {json.dumps(r, indent=2, default=str)}")
        if len(results) > 5:
            print(f"  ... and {len(results)-5} more rows")
    elif isinstance(results, dict):
        print(f"  {json.dumps(results, indent=2, default=str)}")
    else:
        print(f"  {results}")


def main():
    client = get_client()
    start_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
    end_date = (datetime.now() + timedelta(days=31)).strftime("%Y-%m-%dT00:00:00")

    try:
        # =====================================================================
        # STEP 1: Find an available unit on LSETUP
        # =====================================================================
        print("\n[STEP 1] Finding available units on LSETUP...")
        units = soap_call(client, "UnitsInformationAvailableUnitsOnly_v2", {
            "sLocationCode": SITE,
        }, result_tag="Table")
        print(f"  Found {len(units)} available units")
        if not units:
            print("  ERROR: No available units on LSETUP")
            return

        # Pick first available unit
        unit = units[0]
        unit_id = unit.get("UnitID") or unit.get("iUnitID")
        unit_name = unit.get("sUnitName", "?")
        std_rate = unit.get("dcStdRate", "0")
        print(f"  Selected: UnitID={unit_id}, Name={unit_name}, StdRate={std_rate}")

        # =====================================================================
        # STEP 2: Get discount plans for this site
        # =====================================================================
        print("\n[STEP 2] Fetching discount plans...")
        discounts = soap_call(client, "DiscountPlansRetrieve", {
            "sLocationCode": SITE,
        }, result_tag="Table")
        print(f"  Found {len(discounts)} discount plans")
        if discounts:
            for d in discounts[:3]:
                print(f"    ConcessionID={d.get('ConcessionID')}, "
                      f"Name={d.get('sConcessionName')}, "
                      f"Type={d.get('iDiscountType')}")

        # =====================================================================
        # STEP 3: Get insurance options
        # =====================================================================
        print("\n[STEP 3] Fetching insurance options...")
        insurance = soap_call(client, "InsuranceCoverageRetrieve_V3", {
            "sLocationCode": SITE,
            "iUnitID": str(unit_id),
        }, result_tag="Table")
        print(f"  Found {len(insurance)} insurance plans")
        if insurance:
            for ins in insurance[:3]:
                print(f"    CoverageID={ins.get('InsuranceCoverageID')}, "
                      f"Amount={ins.get('dcCoverageAmount')}, "
                      f"Premium={ins.get('dcPremium')}")

        # =====================================================================
        # STEP 4: Create a test tenant
        # =====================================================================
        print("\n[STEP 4] Creating test tenant (bTestMode=true)...")
        tenant_result = soap_call(client, "TenantNewDetailed_v3", {
            "sLocationCode": SITE,
            "sWebPassword": "",
            "sMrMrs": "",
            "sFName": "BookingTest",
            "sMI": "",
            "sLName": f"Flow{datetime.now().strftime('%H%M%S')}",
            "sCompany": "",
            "sAddr1": "1 Test Street",
            "sAddr2": "",
            "sCity": "Singapore",
            "sRegion": "",
            "sPostalCode": "000000",
            "sCountry": "SG",
            "sPhone": "99999999",
            "sMrMrsAlt": "", "sFNameAlt": "", "sMIAlt": "", "sLNameAlt": "",
            "sAddr1Alt": "", "sAddr2Alt": "", "sCityAlt": "", "sRegionAlt": "",
            "sPostalCodeAlt": "", "sCountryAlt": "", "sPhoneAlt": "",
            "sMrMrsBus": "", "sFNameBus": "", "sMIBus": "", "sLNameBus": "",
            "sCompanyBus": "", "sAddr1Bus": "", "sAddr2Bus": "", "sCityBus": "",
            "sRegionBus": "", "sPostalCodeBus": "", "sCountryBus": "", "sPhoneBus": "",
            "sEmail": "test@example.com",
            "sEmailAlt": "", "sEmailBus": "",
            "sFax": "", "sFaxAlt": "", "sFaxBus": "",
            "sMobile": "",
            "dDOB": "1990-01-01T00:00:00",
            "sSSN": "",
            "sDriversLic": "",
            "bBusiness": "false",
            "sIDType": "",
            "sIDNum": "",
            "sIDIssuer": "",
            "bTestMode": "true",
        })
        print_result("TenantNewDetailed_v3 (bTestMode=true)", tenant_result)
        tenant_id = None
        if tenant_result:
            row = tenant_result[0] if isinstance(tenant_result, list) else tenant_result
            tenant_id = row.get("TenantID")
            ret_code = row.get("Ret_Code", "")
            print(f"  TenantID={tenant_id}, Ret_Code={ret_code}")
            if tenant_id:
                print(f"  Tenant ID (test): {tenant_id}")

        # For subsequent steps, create a REAL tenant
        print("\n[STEP 4b] Creating REAL test tenant...")
        tenant_result_real = soap_call(client, "TenantNewDetailed_v3", {
            "sLocationCode": SITE,
            "sWebPassword": "",
            "sMrMrs": "",
            "sFName": "BookingTest",
            "sMI": "",
            "sLName": f"Real{datetime.now().strftime('%H%M%S')}",
            "sCompany": "",
            "sAddr1": "1 Test Street",
            "sAddr2": "",
            "sCity": "Singapore",
            "sRegion": "",
            "sPostalCode": "000000",
            "sCountry": "SG",
            "sPhone": "99999999",
            "sMrMrsAlt": "", "sFNameAlt": "", "sMIAlt": "", "sLNameAlt": "",
            "sAddr1Alt": "", "sAddr2Alt": "", "sCityAlt": "", "sRegionAlt": "",
            "sPostalCodeAlt": "", "sCountryAlt": "", "sPhoneAlt": "",
            "sMrMrsBus": "", "sFNameBus": "", "sMIBus": "", "sLNameBus": "",
            "sCompanyBus": "", "sAddr1Bus": "", "sAddr2Bus": "", "sCityBus": "",
            "sRegionBus": "", "sPostalCodeBus": "", "sCountryBus": "", "sPhoneBus": "",
            "sEmail": "test@example.com",
            "sEmailAlt": "", "sEmailBus": "",
            "sFax": "", "sFaxAlt": "", "sFaxBus": "",
            "sMobile": "",
            "dDOB": "1990-01-01T00:00:00",
            "sSSN": "",
            "sDriversLic": "",
            "bBusiness": "false",
            "sIDType": "",
            "sIDNum": "",
            "sIDIssuer": "",
            "bTestMode": "false",
        })
        print_result("TenantNewDetailed_v3 (REAL)", tenant_result_real)
        real_tenant_id = None
        if tenant_result_real:
            row = tenant_result_real[0] if isinstance(tenant_result_real, list) else tenant_result_real
            real_tenant_id = row.get("TenantID")
            if real_tenant_id:
                real_tenant_id = int(real_tenant_id)
                print(f"  REAL Tenant ID: {real_tenant_id}")

        if not real_tenant_id:
            print("  ERROR: Failed to create real tenant. Cannot continue.")
            return

        # =====================================================================
        # STEP 5: Reserve the unit (no cost retrieve needed)
        # =====================================================================
        print(f"\n[STEP 5] Reserving unit {unit_id} for tenant {real_tenant_id}...")
        expires = (datetime.now() + timedelta(days=14)).strftime("%Y-%m-%dT00:00:00")
        followup = (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%dT00:00:00")
        reservation_result = soap_call(client, "ReservationNewWithSource_v6", {
            "sLocationCode": SITE,
            "sTenantID": str(real_tenant_id),
            "sUnitID": str(unit_id),
            "dNeeded": start_date,
            "sComment": "Booking engine flow test",
            "iSource": "0",
            "sSource": "BookingEngineTest",
            "QTRentalTypeID": "0",
            "iInquiryType": "0",
            "dcQuotedRate": str(std_rate),
            "dExpires": expires,
            "dFollowUp": followup,
            "sTrackingCode": "",
            "sCallerID": "",
            "ConcessionID": "0",
            "PromoGlobalNum": "0",
        })
        print_result("ReservationNewWithSource_v6", reservation_result)
        waiting_id = None
        if reservation_result:
            row = reservation_result[0] if isinstance(reservation_result, list) else reservation_result
            ret_code = row.get("Ret_Code", "")
            ret_msg = row.get("Ret_Msg", "")
            print(f"  Ret_Code={ret_code}, Ret_Msg={ret_msg}")
            if ret_code and int(ret_code) > 0:
                waiting_id = int(ret_code)
                print(f"  WaitingID: {waiting_id}")

        if not waiting_id:
            print("  ERROR: Reservation failed. Cannot continue.")
            return

        # =====================================================================
        # STEP 6: Try ReservationFeeAddWithSource with dummy CC
        # =====================================================================
        print(f"\n[STEP 6] ReservationFeeAddWithSource (bTestMode=true, dummy CC)...")

        # First get the reservation fee amount for this site
        print("  Fetching ReservationFeeRetrieve...")
        fee_result = soap_call(client, "ReservationFeeRetrieve", {
            "sLocationCode": SITE,
        }, result_tag="Table")
        print_result("ReservationFeeRetrieve", fee_result)

        # Try adding fee with dummy CC
        fee_add_result = soap_call(client, "ReservationFeeAddWithSource_v2", {
            "sLocationCode": SITE,
            "iTenantID": str(real_tenant_id),
            "iWaitingListID": str(waiting_id),
            "iCreditCardType": "0",
            "sCreditCardNumber": "4111111111111111",
            "sCreditCardCVV": "123",
            "dExpirationDate": "2030-01-01T00:00:00",
            "sBillingName": "BookingTest",
            "sBillingAddress": "1 Test Street",
            "sBillingZipCode": "000000",
            "bTestMode": "true",
            "iSource": "0",
        })
        print_result("ReservationFeeAddWithSource_v2 (bTestMode=true)", fee_add_result)
        if fee_add_result:
            row = fee_add_result[0] if isinstance(fee_add_result, list) else fee_add_result
            print(f"  Ret_Code={row.get('Ret_Code', 'N/A')}, Ret_Msg={row.get('Ret_Msg', 'N/A')}")

        # =====================================================================
        # STEP 7: MoveInCostRetrieve to get the correct amount
        # =====================================================================
        print(f"\n[STEP 7] MoveInCostRetrieveWithDiscount_Reservation_v4...")
        cost_result = soap_call(client, "MoveInCostRetrieveWithDiscount_Reservation_v4", {
            "sLocationCode": SITE,
            "iUnitID": str(unit_id),
            "dMoveInDate": start_date,
            "InsuranceCoverageID": "0",
            "ConcessionPlanID": "0",
            "WaitingID": str(waiting_id),
            "iPromoGlobalNum": "0",
            "ChannelType": "0",
            "bApplyInsuranceCredit": "false",
        }, result_tag="Table")
        print_result("MoveInCostRetrieveWithDiscount_Reservation_v4", cost_result)

        correct_total = 0
        if cost_result:
            for row in cost_result:
                dc_total = float(row.get("dcTotal", 0))
                charge = row.get("sChargeDescription", "?")
                print(f"    {charge}: {dc_total}")
                correct_total += dc_total
            print(f"  CORRECT TOTAL: {correct_total:.2f}")

        # =====================================================================
        # STEP 8: Move-in with WRONG amount (bTestMode=true)
        # =====================================================================
        wrong_amount = "99.99"
        print(f"\n[STEP 8a] MoveInReservation_v6 with WRONG amount ${wrong_amount} (bTestMode=true)...")
        movein_wrong = soap_call(client, "MoveInReservation_v6", {
            "sLocationCode": SITE,
            "WaitingID": str(waiting_id),
            "TenantID": str(real_tenant_id),
            "UnitID": str(unit_id),
            "dStartDate": start_date,
            "dEndDate": end_date,
            "dcPaymentAmount": wrong_amount,
            "iCreditCardType": "0",
            "sCreditCardNumber": "",
            "sCreditCardCVV": "",
            "dExpirationDate": "2030-01-01T00:00:00",
            "sBillingName": "",
            "sBillingAddress": "",
            "sBillingZipCode": "",
            "InsuranceCoverageID": "0",
            "ConcessionPlanID": "0",
            "iPayMethod": "2",
            "sABARoutingNum": "",
            "sAccountNum": "",
            "iAccountType": "0",
            "iSource": "0",
            "bTestMode": "true",
            "bApplyInsuranceCredit": "false",
            "iPromoGlobalNum": "0",
        })
        print_result("MoveInReservation_v6 WRONG AMOUNT (bTestMode=true)", movein_wrong)
        if movein_wrong:
            row = movein_wrong[0] if isinstance(movein_wrong, list) else movein_wrong
            print(f"  Ret_Code={row.get('Ret_Code', 'N/A')}, Ret_Msg={row.get('Ret_Msg', 'N/A')}")

        # =====================================================================
        # STEP 8b: Move-in with CORRECT amount (bTestMode=true)
        # =====================================================================
        if correct_total > 0:
            print(f"\n[STEP 8b] MoveInReservation_v6 with CORRECT amount ${correct_total:.2f} (bTestMode=true)...")
            movein_correct = soap_call(client, "MoveInReservation_v6", {
                "sLocationCode": SITE,
                "WaitingID": str(waiting_id),
                "TenantID": str(real_tenant_id),
                "UnitID": str(unit_id),
                "dStartDate": start_date,
                "dEndDate": end_date,
                "dcPaymentAmount": f"{correct_total:.2f}",
                "iCreditCardType": "0",
                "sCreditCardNumber": "",
                "sCreditCardCVV": "",
                "dExpirationDate": "2030-01-01T00:00:00",
                "sBillingName": "",
                "sBillingAddress": "",
                "sBillingZipCode": "",
                "InsuranceCoverageID": "0",
                "ConcessionPlanID": "0",
                "iPayMethod": "2",
                "sABARoutingNum": "",
                "sAccountNum": "",
                "iAccountType": "0",
                "iSource": "0",
                "bTestMode": "true",
                "bApplyInsuranceCredit": "false",
                "iPromoGlobalNum": "0",
            })
            print_result("MoveInReservation_v6 CORRECT AMOUNT (bTestMode=true)", movein_correct)
            if movein_correct:
                row = movein_correct[0] if isinstance(movein_correct, list) else movein_correct
                print(f"  Ret_Code={row.get('Ret_Code', 'N/A')}, Ret_Msg={row.get('Ret_Msg', 'N/A')}")

        # =====================================================================
        # STEP 8c: Move-in with check (iPayMethod=3) + CORRECT amount
        # =====================================================================
        if correct_total > 0:
            print(f"\n[STEP 8c] MoveInReservation_v6 with CHECK (iPayMethod=3), amount ${correct_total:.2f} (bTestMode=true)...")
            movein_check = soap_call(client, "MoveInReservation_v6", {
                "sLocationCode": SITE,
                "WaitingID": str(waiting_id),
                "TenantID": str(real_tenant_id),
                "UnitID": str(unit_id),
                "dStartDate": start_date,
                "dEndDate": end_date,
                "dcPaymentAmount": f"{correct_total:.2f}",
                "iCreditCardType": "0",
                "sCreditCardNumber": "",
                "sCreditCardCVV": "",
                "dExpirationDate": "2030-01-01T00:00:00",
                "sBillingName": "",
                "sBillingAddress": "",
                "sBillingZipCode": "",
                "InsuranceCoverageID": "0",
                "ConcessionPlanID": "0",
                "iPayMethod": "3",
                "sABARoutingNum": "",
                "sAccountNum": "",
                "iAccountType": "0",
                "iSource": "0",
                "bTestMode": "true",
                "bApplyInsuranceCredit": "false",
                "iPromoGlobalNum": "0",
            })
            print_result("MoveInReservation_v6 CHECK (bTestMode=true)", movein_check)
            if movein_check:
                row = movein_check[0] if isinstance(movein_check, list) else movein_check
                print(f"  Ret_Code={row.get('Ret_Code', 'N/A')}, Ret_Msg={row.get('Ret_Msg', 'N/A')}")

        # =====================================================================
        # SUMMARY
        # =====================================================================
        print(f"\n{'='*60}")
        print("  TEST SUMMARY")
        print(f"{'='*60}")
        print(f"  Site: {SITE}")
        print(f"  Unit: {unit_id} ({unit_name}), StdRate={std_rate}")
        print(f"  Tenant: {real_tenant_id}")
        print(f"  WaitingID: {waiting_id}")
        print(f"  Correct total: ${correct_total:.2f}")
        print(f"  Start: {start_date}, End: {end_date}")

    finally:
        client.close()


if __name__ == "__main__":
    main()
