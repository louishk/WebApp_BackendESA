#!/usr/bin/env python3
"""Test create-pin (jobType=4) and delete-pin (jobType=5) via bridge.

This DOES program a real PIN on a real keypad — gated by IGLOO_PROBE_CONFIRM=YES.

Test plan:
  1. Confirm gate
  2. Create a 1-hour duration PIN, poll until completed
  3. Read access list to confirm PIN is present + capture accessId
  4. Delete the PIN (try jobData shapes in order: {pin} → {accessId} → both)
  5. Confirm PIN gone from access list

Run from project root:
  IGLOO_PROBE_CONFIRM=YES PYTHONPATH=backend/python python3 scripts/probe_igloo_bridge_pin.py
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
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(_BACKEND.parent.parent / ".env")
except Exception:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("pinprobe")

import requests as rq
from common.igloo_client import IglooClient, API_BASE_URL  # noqa: E402

# --- Hardcoded fixtures from earlier probe (keypad+bridge that worked) -------
DEVICE_ID = "EK2X11000666"          # keypad
BRIDGE_ID = "EB1X08776b94"          # bridge paired to that keypad
DEPT_ID = "69bbb99062fb6e2159903b50"  # department this device belongs to


def hour_aligned(hours_from_now: int) -> str:
    now = datetime.now(timezone.utc)
    aligned = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=hours_from_now)
    return aligned.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def submit(headers, body):
    url = f"{API_BASE_URL}/devices/{DEVICE_ID}/jobs/bridges/{BRIDGE_ID}"
    log.info("POST %s body=%s", url, json.dumps(body))
    r = rq.post(url, headers=headers, json=body, timeout=30)
    log.info("  -> HTTP %s body=%s", r.status_code, r.text[:600])
    if r.status_code in (200, 201):
        return r.json().get("jobId")
    return None


def poll(headers, job_id, timeout=120, interval=3):
    deadline = time.time() + timeout
    url = f"{API_BASE_URL}/bridge/jobs/{job_id}"
    last = None
    while time.time() < deadline:
        r = rq.get(url, headers=headers, timeout=30)
        if r.status_code != 200:
            log.warning("  poll HTTP %s: %s", r.status_code, r.text[:200])
            return last
        last = r.json()
        completed = last.get("completed")
        log.info("  poll job=%s completed=%s body=%s", job_id, completed, json.dumps(last)[:400])
        if completed:
            return last
        time.sleep(interval)
    return last


def list_access(client):
    url = f"{API_BASE_URL}/devices/{DEVICE_ID}/access"
    r = rq.get(url, headers=client._auth_headers(), timeout=30)  # type: ignore[attr-defined]
    if r.status_code != 200:
        log.warning("list_access HTTP %s: %s", r.status_code, r.text[:200])
        return []
    return r.json().get("payload", []) or r.json().get("data", []) or r.json().get("accesses", []) or r.json()


def main():
    if os.environ.get("IGLOO_PROBE_CONFIRM") != "YES":
        print("Refusing to run: set IGLOO_PROBE_CONFIRM=YES to actually program a PIN on the keypad.")
        return 2

    client = IglooClient()
    headers = client._auth_headers()  # type: ignore[attr-defined]

    pin = "".join([str(secrets.randbelow(10)) for _ in range(6)])
    while pin.startswith("0"):
        pin = "".join([str(secrets.randbelow(10)) for _ in range(6)])
    name = f"probe-{datetime.now(timezone.utc).strftime('%H%M%S')}"
    start = hour_aligned(1)
    end = hour_aligned(2)

    print(f"\n=== Step 1: snapshot existing access ===")
    before = list_access(client)
    print(f"  before: {len(before) if isinstance(before, list) else 'unknown'} entries")
    if isinstance(before, list):
        for a in before[:5]:
            print("   ", json.dumps(a)[:200])

    # ---- CREATE PIN (jobType=4) -----------------------------------------
    print(f"\n=== Step 2: create-pin (jobType=4) PIN={pin} name={name} ===")
    # The YAML example uses pinType=4 with startDate+endDate (duration).
    # We try a sequence of jobData shapes to find what the live API accepts.
    create_attempts = [
        {
            "label": "doc-shape (pinType=4 duration)",
            "jobData": {
                "accessName": name,
                "pin": pin,
                "pinType": 4,
                "startDate": start,
                "endDate": end,
            },
        },
        {
            "label": "string-pinType (duration)",
            "jobData": {
                "accessName": name,
                "pin": pin,
                "pinType": "duration",
                "startDate": start,
                "endDate": end,
            },
        },
        {
            "label": "customPin field",
            "jobData": {
                "accessName": name,
                "customPin": pin,
                "pinType": "duration",
                "startDate": start,
                "endDate": end,
            },
        },
    ]

    create_result = None
    for attempt in create_attempts:
        print(f"\n  -- attempt: {attempt['label']}")
        body = {"jobType": 4, "departmentId": DEPT_ID, "jobData": attempt["jobData"]}
        jid = submit(headers, body)
        if not jid:
            continue
        result = poll(headers, jid, timeout=120)
        if result and result.get("completed") and (result.get("jobResponse") or {}).get("jobStatus") == 0:
            create_result = result
            create_result["_attempt"] = attempt["label"]
            break
        if result and result.get("completed"):
            print(f"  job completed with non-zero status: {json.dumps(result)[:400]}")

    if not create_result:
        print("\n[FAIL] No create-pin attempt succeeded. Aborting before delete.")
        return 1

    print(f"\n[OK] create-pin succeeded via shape='{create_result['_attempt']}'")
    print(f"     final response: {json.dumps(create_result, indent=2)[:1000]}")

    # ---- VERIFY PIN IS THERE -------------------------------------------
    time.sleep(2)
    print(f"\n=== Step 3: list access after create ===")
    after_create = list_access(client)
    new_entry = None
    if isinstance(after_create, list):
        before_ids = {a.get("id") or a.get("accessId") for a in (before if isinstance(before, list) else [])}
        for a in after_create:
            aid = a.get("id") or a.get("accessId")
            if aid not in before_ids:
                new_entry = a
                break
    print(f"  after-create entries: {len(after_create) if isinstance(after_create, list) else 'unknown'}")
    print(f"  new entry: {json.dumps(new_entry, indent=2) if new_entry else '(not found via diff)'}")

    access_id = (new_entry or {}).get("id") or (new_entry or {}).get("accessId")

    # ---- DELETE PIN (jobType=5) -----------------------------------------
    print(f"\n=== Step 4: delete-pin (jobType=5) ===")
    delete_attempts = [
        {"label": "by pin", "jobData": {"pin": pin}},
        {"label": "by accessId (if known)", "jobData": {"accessId": access_id} if access_id else None},
        {"label": "by customPin", "jobData": {"customPin": pin}},
        {"label": "pin+accessId", "jobData": {"pin": pin, "accessId": access_id} if access_id else None},
    ]

    delete_result = None
    for attempt in delete_attempts:
        if attempt["jobData"] is None:
            continue
        print(f"\n  -- delete attempt: {attempt['label']}")
        body = {"jobType": 5, "departmentId": DEPT_ID, "jobData": attempt["jobData"]}
        jid = submit(headers, body)
        if not jid:
            continue
        result = poll(headers, jid, timeout=120)
        if result and result.get("completed") and (result.get("jobResponse") or {}).get("jobStatus") == 0:
            delete_result = result
            delete_result["_attempt"] = attempt["label"]
            break
        if result and result.get("completed"):
            print(f"  delete job completed with non-zero status: {json.dumps(result)[:400]}")

    if not delete_result:
        print("\n[WARN] All delete-pin attempts failed. Manual cleanup of PIN may be needed.")
        return 1

    print(f"\n[OK] delete-pin succeeded via shape='{delete_result['_attempt']}'")
    print(f"     final response: {json.dumps(delete_result, indent=2)[:1000]}")

    # ---- CONFIRM PIN GONE -----------------------------------------------
    time.sleep(2)
    print(f"\n=== Step 5: list access after delete ===")
    after_delete = list_access(client)
    still_present = False
    if isinstance(after_delete, list) and access_id:
        still_present = any((a.get("id") or a.get("accessId")) == access_id for a in after_delete)
    print(f"  PIN still present: {still_present}")

    print(f"\n=== Probe summary ===")
    print(json.dumps({
        "pin": pin,
        "name": name,
        "access_id": access_id,
        "create_shape": create_result.get("_attempt"),
        "delete_shape": delete_result.get("_attempt"),
        "create_jobType_returned": create_result.get("jobType"),
        "delete_jobType_returned": delete_result.get("jobType"),
        "still_present_after_delete": still_present,
    }, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
