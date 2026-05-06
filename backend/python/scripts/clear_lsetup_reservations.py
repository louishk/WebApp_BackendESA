"""
Cancel all leads + reservations on LSETUP (test site) via ReservationUpdate_v4.

Mirrors the working signature in web/routes/reservations.py::reservation_cancel:
- Uses param name `WaitingID` (NOT `iWaitingID`)
- Echoes back all existing fields (TenantID, UnitID, dates, etc.)
- Sets iStatus=2 + QTCancellationTypeID + sCancellationReason
- Verifies Ret_Code > 0 for success

Usage:
    PYTHONPATH=. python3 scripts/clear_lsetup_reservations.py            # dry-run
    PYTHONPATH=. python3 scripts/clear_lsetup_reservations.py --execute  # cancel
"""
from __future__ import annotations
import os, sys, time
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.soap_client import SOAPClient
from common.config import DataLayerConfig

CC_NS = "http://tempuri.org/CallCenterWs/CallCenterWs"
SITE = "LSETUP"
REASON = "bulk cleanup of test-site"


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


def list_reservations(client):
    return soap(client, "ReservationList_v3",
                {"sLocationCode": SITE, "iGlobalWaitingNum": "0", "WaitingID": "0"},
                result_tag="Table") or []


def _parse_date(val, fallback_days_ahead: int) -> str:
    """Parse an SMD date or fall back to now+N days. SMD rejects empty dates."""
    if val:
        try:
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %I:%M:%S %p", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(str(val).split('.')[0], fmt).strftime("%Y-%m-%dT%H:%M:%S")
                except ValueError:
                    continue
        except Exception:
            pass
    return (datetime.now() + timedelta(days=fallback_days_ahead)).strftime("%Y-%m-%dT%H:%M:%S")


def cancel_one(client, res: dict) -> tuple[bool, str]:
    wid = res.get('WaitingID')
    needed   = _parse_date(res.get('dNeeded'),   1)
    followup = _parse_date(res.get('dFollowup'), 3)
    # Force expiry to yesterday so SMD's auto-flush picks it up
    expires  = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        rt = soap(client, "ReservationUpdate_v4", {
            "sLocationCode": SITE,
            "WaitingID": str(wid),
            "sTenantID": str(res.get('TenantID') or '0'),
            "sUnitID":   str(res.get('UnitID')   or '0'),
            "dNeeded": needed,
            "sComment": res.get('sComment') or '',
            "iStatus": "2",
            "bFollowup": "false",
            "dFollowup": followup,
            "dFollowupLast": followup,
            "iInquiryType": str(res.get('iInquiryType') or 0),
            "dcQuotedRate": str(res.get('dcRate_Quoted') or 0),
            "dExpires": expires,
            "QTRentalTypeID": str(res.get('QTRentalTypeID') or 0),
            "QTCancellationTypeID": "0",
            "sCancellationReason": REASON,
            "ConcessionID": str(res.get('ConcessionID') or 0),
        }, result_tag="RT")
        ret_code = (rt[0].get('Ret_Code') if rt else None)
        ret_msg  = (rt[0].get('Ret_Msg')  if rt else None)
        try:
            ok = ret_code is not None and int(ret_code) > 0
        except (TypeError, ValueError):
            ok = False
        return ok, f"ret_code={ret_code} msg={ret_msg}"
    except Exception as e:
        return False, str(e)[:200]


def main():
    execute = "--execute" in sys.argv
    client = get_client()

    all_rows = list_reservations(client)
    rows = [r for r in all_rows if not r.get('dConverted_ToMoveIn')]
    print(f"LSETUP active total: {len(all_rows)}; targeting leads+reservations: {len(rows)}")

    if not execute:
        print("DRY RUN — pass --execute to actually cancel.")
        return

    print(f"\nCancelling {len(rows)} via ReservationUpdate_v4...")
    ok = 0; fail = 0
    for i, r in enumerate(rows, 1):
        success, msg = cancel_one(client, r)
        if success:
            ok += 1
        else:
            fail += 1
            print(f"  [{i}/{len(rows)}] WaitingID={r.get('WaitingID')} FAIL: {msg}")
        if i % 10 == 0:
            print(f"  progress {i}/{len(rows)} (ok={ok} fail={fail})")
        time.sleep(0.05)
    print(f"\nDone. ok={ok} fail={fail}")


if __name__ == "__main__":
    main()
