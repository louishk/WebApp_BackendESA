#!/usr/bin/env python3
"""Discover the integer pinType enum + verify ESA-{site}-{unit} labelling.

Programs real PINs on the test keypad. Gated by IGLOO_PROBE_CONFIRM=YES.
Cleans up every PIN it creates.

Plan:
  1. For each candidate (pinType_int, has_endDate, label) probe shape:
     - submit jobType=4 with that shape
     - poll
     - if success, list /devices/{id}/access, find the new entry
     - record what `pinType` the API normalized to (string)
     - delete the PIN by accessId

  2. Print a definitive int → string mapping table.
"""
from __future__ import annotations

import json
import logging
import os
import secrets
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

_BACKEND = Path(__file__).resolve().parent.parent / "backend" / "python"
sys.path.insert(0, str(_BACKEND))
try:
    from dotenv import load_dotenv
    load_dotenv(_BACKEND.parent.parent / ".env")
except Exception:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("pintypes")

import requests as rq
from common.igloo_client import IglooClient, API_BASE_URL  # noqa: E402

DEVICE_ID = "EK2X11000666"
BRIDGE_ID = "EB1X08776b94"
DEPT_ID = "69bbb99062fb6e2159903b50"


def hour_aligned(hours_from_now):
    now = datetime.now(timezone.utc)
    return (now.replace(minute=0, second=0, microsecond=0) +
            timedelta(hours=hours_from_now)).strftime("%Y-%m-%dT%H:%M:%S+00:00")


def submit(headers, body):
    url = f"{API_BASE_URL}/devices/{DEVICE_ID}/jobs/bridges/{BRIDGE_ID}"
    r = rq.post(url, headers=headers, json=body, timeout=30)
    log.info("POST jobType=%s pinType=%s -> HTTP %s body=%s",
             body.get("jobType"),
             (body.get("jobData") or {}).get("pinType"),
             r.status_code, r.text[:300])
    if r.status_code in (200, 201):
        return r.json().get("jobId"), None
    try:
        return None, (r.json() or {}).get("error") or r.text[:200]
    except Exception:
        return None, r.text[:200]


def poll(headers, job_id, timeout=60, interval=2):
    deadline = time.time() + timeout
    url = f"{API_BASE_URL}/bridge/jobs/{job_id}"
    last = None
    while time.time() < deadline:
        r = rq.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            return last
        last = r.json()
        if last.get("completed"):
            return last
        time.sleep(interval)
    return last


def list_access(headers):
    url = f"{API_BASE_URL}/devices/{DEVICE_ID}/access"
    r = rq.get(url, headers=headers, timeout=30)
    if r.status_code != 200:
        return []
    body = r.json()
    if isinstance(body, list):
        return body
    return body.get("payload") or body.get("data") or body.get("accesses") or []


def delete_pin(headers, access_id):
    body = {"jobType": 5, "departmentId": DEPT_ID, "jobData": {"accessId": access_id}}
    jid, _ = submit(headers, body)
    if not jid:
        return False
    res = poll(headers, jid, timeout=60)
    return bool(res and res.get("completed") and (res.get("jobResponse") or {}).get("jobStatus") == 0)


def fresh_pin():
    while True:
        p = "".join(str(secrets.randbelow(10)) for _ in range(6))
        if not p.startswith("0"):
            return p


def main():
    if os.environ.get("IGLOO_PROBE_CONFIRM") != "YES":
        print("Set IGLOO_PROBE_CONFIRM=YES to run.")
        return 2

    client = IglooClient()
    headers = client._auth_headers()

    # Each attempt: (label, jobData)
    SITE_ID = 999
    UNIT_ID = 12345
    base_label = f"ESA-{SITE_ID}-{UNIT_ID}"

    attempts = []
    # Try ints 1..7 with both "duration-like" (start+end) and "permanent-like" (start only) shapes
    for pt in (1, 2, 3, 4, 5, 6, 7):
        attempts.append({
            "tag": f"int{pt}-duration",
            "pinType": pt,
            "with_end": True,
        })
        attempts.append({
            "tag": f"int{pt}-permanent",
            "pinType": pt,
            "with_end": False,
        })
    # And try the string forms the direct API uses
    for s in ("permanent", "duration", "otp"):
        attempts.append({
            "tag": f"str-{s}",
            "pinType": s,
            "with_end": s == "duration",
        })

    results = []
    for a in attempts:
        pin = fresh_pin()
        access_name = f"{base_label}-{a['tag']}-{pin[-3:]}"
        jd = {
            "accessName": access_name,
            "pin": pin,
            "pinType": a["pinType"],
            "startDate": hour_aligned(1),
        }
        if a["with_end"]:
            jd["endDate"] = hour_aligned(2)

        body = {"jobType": 4, "departmentId": DEPT_ID, "jobData": jd}
        jid, err = submit(headers, body)
        result_row = {
            "tag": a["tag"],
            "pinType_sent": a["pinType"],
            "with_end": a["with_end"],
            "submit_ok": bool(jid),
            "submit_error": err,
        }
        if not jid:
            results.append(result_row)
            continue

        final = poll(headers, jid, timeout=60)
        ok = bool(final and final.get("completed") and
                  (final.get("jobResponse") or {}).get("jobStatus") == 0)
        result_row["job_completed"] = bool(final and final.get("completed"))
        result_row["job_status"] = (final or {}).get("jobResponse", {}).get("jobStatus")

        if ok:
            time.sleep(1)
            entries = list_access(headers)
            new_entry = next((e for e in entries
                              if e.get("name") == access_name or e.get("description") == access_name),
                             None)
            if new_entry:
                result_row["api_pinType_returned"] = new_entry.get("pinType")
                result_row["api_name"] = new_entry.get("name")
                result_row["api_isCustomPin"] = new_entry.get("isCustomPin")
                result_row["api_pin"] = new_entry.get("pin")
                # Cleanup
                aid = new_entry.get("id") or new_entry.get("accessId")
                deleted = delete_pin(headers, aid) if aid else False
                result_row["cleanup_deleted"] = deleted
        else:
            result_row["api_pinType_returned"] = None
        results.append(result_row)

    print("\n=== Pin-type probe results ===")
    print(json.dumps(results, indent=2, default=str))

    print("\n=== Int → string mapping (succeeded shapes only) ===")
    for r in results:
        if r.get("api_pinType_returned"):
            print(f"   pinType_sent={r['pinType_sent']!r:>18}  with_end={r['with_end']:1}  -> "
                  f"api_pinType={r['api_pinType_returned']!r}  label_kept={r.get('api_name')!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
