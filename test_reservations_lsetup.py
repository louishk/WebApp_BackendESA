#!/usr/bin/env python3
"""
Test all reservation SOAP endpoints against LSETUP.
Run from project root: python3 test_reservations_lsetup.py
"""

import sys
import os

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'backend', 'python'))

from common.config import DataLayerConfig
from common.soap_client import SOAPClient, SOAPFaultError

CC_NS = "http://tempuri.org/CallCenterWs/CallCenterWs"
SITE_CODE = "LSETUP"


def get_client():
    config = DataLayerConfig.from_env()
    cc_url = config.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
    return SOAPClient(
        base_url=cc_url,
        corp_code=config.soap.corp_code,
        corp_user=config.soap.corp_user,
        api_key=config.soap.api_key,
        corp_password=config.soap.corp_password,
        timeout=config.soap.timeout,
        retries=config.soap.retries,
    )


def soap_action(op):
    return f"{CC_NS}/{op}"


def separator(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def test_reservation_fee_retrieve(client):
    separator("1. ReservationFeeRetrieve")
    try:
        results = client.call(
            operation="ReservationFeeRetrieve",
            parameters={"sLocationCode": SITE_CODE},
            soap_action=soap_action("ReservationFeeRetrieve"),
            namespace=CC_NS,
            result_tag="Table",
        )
        print(f"  OK — {len(results) if results else 0} fee records")
        if results:
            for r in results:
                print(f"     {r}")
        return True
    except Exception as e:
        print(f"  FAIL — {e}")
        return False


def test_reservation_list(client):
    separator("2. ReservationList_v3 (all)")
    try:
        results = client.call(
            operation="ReservationList_v3",
            parameters={
                "sLocationCode": SITE_CODE,
                "iGlobalWaitingNum": "0",
                "WaitingID": "0",
            },
            soap_action=soap_action("ReservationList_v3"),
            namespace=CC_NS,
            result_tag="Table",
        )
        count = len(results) if results else 0
        print(f"  OK — {count} reservations")
        if results:
            for r in results[:5]:
                print(f"     WaitingID={r.get('WaitingID')} Unit={r.get('UnitID')} "
                      f"Status={r.get('iStatus')} Name={r.get('sFirstName', '')} {r.get('sLastName', '')}")
            if count > 5:
                print(f"     ... and {count - 5} more")
        return results
    except Exception as e:
        print(f"  FAIL — {e}")
        return None


def test_make_reservation(client):
    separator("3. MakeReservation (quick)")
    try:
        results = client.call(
            operation="MakeReservation",
            parameters={
                "sLocationCode": SITE_CODE,
                "iUnitID": "0",  # unit 0 = let system pick
                "sFirstName": "Test",
                "sLastName": "Reservation",
                "sPhone": "+6500000000",
                "sEmail": "test@esasia.com",
                "sNote": "Automated test from ESA Backend",
            },
            soap_action=soap_action("MakeReservation"),
            namespace=CC_NS,
            result_tag="RT",
        )
        print(f"  OK — Response: {results}")
        if results:
            print(f"     Ret_Code={results[0].get('Ret_Code')}")
            print(f"     Ret_Msg={results[0].get('Ret_Msg')}")
        return results
    except SOAPFaultError as e:
        print(f"  SOAP FAULT — {e}")
        return None
    except Exception as e:
        print(f"  FAIL — {e}")
        return None


def test_reservation_new_v6(client):
    separator("4. ReservationNewWithSource_v6")
    from datetime import datetime, timedelta
    today = datetime.utcnow().date()
    try:
        results = client.call(
            operation="ReservationNewWithSource_v6",
            parameters={
                "sLocationCode": SITE_CODE,
                "sTenantID": "0",
                "sUnitID": "0",
                "dNeeded": (today + timedelta(days=1)).isoformat(),
                "sComment": "Test v6 reservation from ESA Backend",
                "iSource": "0",
                "sSource": "ESA Backend Test",
                "QTRentalTypeID": "0",
                "iInquiryType": "0",
                "dcQuotedRate": "0",
                "dExpires": (today + timedelta(days=14)).isoformat(),
                "dFollowUp": (today + timedelta(days=3)).isoformat(),
                "sTrackingCode": "",
                "sCallerID": "",
                "ConcessionID": "0",
                "PromoGlobalNum": "0",
            },
            soap_action=soap_action("ReservationNewWithSource_v6"),
            namespace=CC_NS,
            result_tag="RT",
        )
        print(f"  OK — Response: {results}")
        if results:
            for k, v in results[0].items():
                print(f"     {k}={v}")
        return results
    except SOAPFaultError as e:
        print(f"  SOAP FAULT — {e}")
        return None
    except Exception as e:
        print(f"  FAIL — {e}")
        return None


def test_reservation_get(client, waiting_id):
    separator(f"5. ReservationList_v3 (single WaitingID={waiting_id})")
    try:
        results = client.call(
            operation="ReservationList_v3",
            parameters={
                "sLocationCode": SITE_CODE,
                "iGlobalWaitingNum": "0",
                "WaitingID": str(waiting_id),
            },
            soap_action=soap_action("ReservationList_v3"),
            namespace=CC_NS,
            result_tag="Table",
        )
        if results:
            print(f"  OK — Found reservation")
            for k, v in results[0].items():
                print(f"     {k}={v}")
        else:
            print(f"  OK — No reservation found for WaitingID={waiting_id}")
        return results
    except Exception as e:
        print(f"  FAIL — {e}")
        return None


def test_reservation_note_insert(client, waiting_id):
    separator(f"6. ReservationNoteInsert (WaitingID={waiting_id})")
    try:
        results = client.call(
            operation="ReservationNoteInsert",
            parameters={
                "sLocationCode": SITE_CODE,
                "WaitingID": str(waiting_id),
                "sNote": "Test note from ESA Backend automated test",
            },
            soap_action=soap_action("ReservationNoteInsert"),
            namespace=CC_NS,
            result_tag="RT",
        )
        print(f"  OK — Response: {results}")
        return results
    except SOAPFaultError as e:
        print(f"  SOAP FAULT — {e}")
        return None
    except Exception as e:
        print(f"  FAIL — {e}")
        return None


def test_reservation_notes_retrieve(client, waiting_id):
    separator(f"7. ReservationNotesRetrieve (WaitingID={waiting_id})")
    try:
        results = client.call(
            operation="ReservationNotesRetrieve",
            parameters={
                "sLocationCode": SITE_CODE,
                "WaitingID": str(waiting_id),
            },
            soap_action=soap_action("ReservationNotesRetrieve"),
            namespace=CC_NS,
            result_tag="Table",
        )
        count = len(results) if results else 0
        print(f"  OK — {count} notes")
        if results:
            for r in results:
                print(f"     {r}")
        return results
    except Exception as e:
        print(f"  FAIL — {e}")
        return None


def test_reservation_update(client, waiting_id, reservation_data):
    separator(f"8. ReservationUpdate_v4 (WaitingID={waiting_id})")
    try:
        results = client.call(
            operation="ReservationUpdate_v4",
            parameters={
                "sLocationCode": SITE_CODE,
                "WaitingID": str(waiting_id),
                "sTenantID": str(reservation_data.get('TenantID', '0')),
                "sUnitID": str(reservation_data.get('UnitID', '0')),
                "dNeeded": reservation_data.get('dNeeded', ''),
                "sComment": "Updated by ESA Backend test",
                "iStatus": str(reservation_data.get('iStatus', 0)),
                "bFollowup": "false",
                "dFollowup": "",
                "dFollowupLast": "",
                "iInquiryType": str(reservation_data.get('iInquiryType', 0)),
                "dcQuotedRate": str(reservation_data.get('dcQuotedRate', 0)),
                "dExpires": reservation_data.get('dExpires', ''),
                "QTRentalTypeID": str(reservation_data.get('QTRentalTypeID', 0)),
                "QTCancellationTypeID": "0",
                "sCancellationReason": "",
                "ConcessionID": str(reservation_data.get('ConcessionID', 0)),
            },
            soap_action=soap_action("ReservationUpdate_v4"),
            namespace=CC_NS,
            result_tag="RT",
        )
        print(f"  OK — Response: {results}")
        if results:
            for k, v in results[0].items():
                print(f"     {k}={v}")
        return results
    except SOAPFaultError as e:
        print(f"  SOAP FAULT — {e}")
        return None
    except Exception as e:
        print(f"  FAIL — {e}")
        return None


def test_send_confirmation(client, waiting_id):
    separator(f"9. SendReservationConfirmationEmail (WaitingID={waiting_id})")
    try:
        results = client.call(
            operation="SendReservationConfirmationEmail",
            parameters={
                "sLocationCode": SITE_CODE,
                "waitingId": str(waiting_id),
                "moveInLink": "",
            },
            soap_action=soap_action("SendReservationConfirmationEmail"),
            namespace=CC_NS,
            result_tag="RT",
        )
        print(f"  OK — Response: {results}")
        if results:
            for k, v in results[0].items():
                print(f"     {k}={v}")
        return results
    except SOAPFaultError as e:
        print(f"  SOAP FAULT — {e}")
        return None
    except Exception as e:
        print(f"  FAIL — {e}")
        return None


def test_cancel_reservation(client, waiting_id, reservation_data):
    separator(f"10. Cancel via ReservationUpdate_v4 (WaitingID={waiting_id})")
    try:
        results = client.call(
            operation="ReservationUpdate_v4",
            parameters={
                "sLocationCode": SITE_CODE,
                "WaitingID": str(waiting_id),
                "sTenantID": str(reservation_data.get('TenantID', '0')),
                "sUnitID": str(reservation_data.get('UnitID', '0')),
                "dNeeded": reservation_data.get('dNeeded', ''),
                "sComment": reservation_data.get('sComment', ''),
                "iStatus": "2",  # cancelled
                "bFollowup": "false",
                "dFollowup": "",
                "dFollowupLast": "",
                "iInquiryType": str(reservation_data.get('iInquiryType', 0)),
                "dcQuotedRate": str(reservation_data.get('dcQuotedRate', 0)),
                "dExpires": reservation_data.get('dExpires', ''),
                "QTRentalTypeID": str(reservation_data.get('QTRentalTypeID', 0)),
                "QTCancellationTypeID": "0",
                "sCancellationReason": "Automated test cleanup",
                "ConcessionID": str(reservation_data.get('ConcessionID', 0)),
            },
            soap_action=soap_action("ReservationUpdate_v4"),
            namespace=CC_NS,
            result_tag="RT",
        )
        print(f"  OK — Response: {results}")
        if results:
            for k, v in results[0].items():
                print(f"     {k}={v}")
        return results
    except SOAPFaultError as e:
        print(f"  SOAP FAULT — {e}")
        return None
    except Exception as e:
        print(f"  FAIL — {e}")
        return None


def main():
    print("Reservation SOAP Endpoint Tests — LSETUP")
    print("=" * 70)

    client = get_client()
    results_summary = {}

    # 1. Fee retrieve (read-only, safe)
    ok = test_reservation_fee_retrieve(client)
    results_summary['ReservationFeeRetrieve'] = 'PASS' if ok else 'FAIL'

    # 2. List existing reservations
    existing = test_reservation_list(client)
    results_summary['ReservationList_v3 (all)'] = 'PASS' if existing is not None else 'FAIL'

    # 3. MakeReservation (quick)
    quick_res = test_make_reservation(client)
    results_summary['MakeReservation'] = 'PASS' if quick_res else 'FAIL'

    # 4. ReservationNewWithSource_v6
    v6_res = test_reservation_new_v6(client)
    results_summary['ReservationNewWithSource_v6'] = 'PASS' if v6_res else 'FAIL'

    # Pick a waiting_id to test detail operations
    # Try from v6 first, then quick, then existing list
    waiting_id = None
    res_data = {}

    if v6_res and v6_res[0].get('WaitingID'):
        waiting_id = v6_res[0]['WaitingID']
    elif v6_res and v6_res[0].get('iWaitingID'):
        waiting_id = v6_res[0]['iWaitingID']

    # If we didn't get a waiting_id from create, try listing to find one
    if not waiting_id and existing:
        waiting_id = existing[0].get('WaitingID')
        res_data = existing[0]

    if waiting_id:
        print(f"\n  >>> Using WaitingID={waiting_id} for detail tests")

        # 5. Get single reservation
        single = test_reservation_get(client, waiting_id)
        results_summary['ReservationList_v3 (single)'] = 'PASS' if single else 'FAIL'
        if single:
            res_data = single[0]

        # 6. Insert note
        note_res = test_reservation_note_insert(client, waiting_id)
        results_summary['ReservationNoteInsert'] = 'PASS' if note_res else 'FAIL'

        # 7. Retrieve notes
        notes = test_reservation_notes_retrieve(client, waiting_id)
        results_summary['ReservationNotesRetrieve'] = 'PASS' if notes is not None else 'FAIL'

        # 8. Update reservation
        if res_data:
            update_res = test_reservation_update(client, waiting_id, res_data)
            results_summary['ReservationUpdate_v4'] = 'PASS' if update_res else 'FAIL'
        else:
            print("\n  SKIP — No reservation data for update test")
            results_summary['ReservationUpdate_v4'] = 'SKIP'

        # 9. Send confirmation email
        confirm_res = test_send_confirmation(client, waiting_id)
        results_summary['SendReservationConfirmationEmail'] = 'PASS' if confirm_res else 'FAIL'

        # 10. Cancel (cleanup)
        if res_data:
            cancel_res = test_cancel_reservation(client, waiting_id, res_data)
            results_summary['Cancel (ReservationUpdate_v4)'] = 'PASS' if cancel_res else 'FAIL'
        else:
            results_summary['Cancel (ReservationUpdate_v4)'] = 'SKIP'
    else:
        print("\n  >>> No WaitingID available — skipping detail tests")
        for op in ['ReservationList_v3 (single)', 'ReservationNoteInsert',
                    'ReservationNotesRetrieve', 'ReservationUpdate_v4',
                    'SendReservationConfirmationEmail', 'Cancel (ReservationUpdate_v4)']:
            results_summary[op] = 'SKIP'

    # Summary
    separator("RESULTS SUMMARY")
    passed = sum(1 for v in results_summary.values() if v == 'PASS')
    failed = sum(1 for v in results_summary.values() if v == 'FAIL')
    skipped = sum(1 for v in results_summary.values() if v == 'SKIP')

    for op, status in results_summary.items():
        icon = '✓' if status == 'PASS' else ('✗' if status == 'FAIL' else '–')
        print(f"  {icon} {op}: {status}")

    print(f"\n  {passed} passed, {failed} failed, {skipped} skipped")

    client.close()
    return 0 if failed == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
