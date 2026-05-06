#!/usr/bin/env python3
"""Probe live Igloo API to inspect what a bridge-enabled deployment looks like.

Read-only / safe operations only:
  1. List devices, group by type (Lock / Keypad / Bridge / Keybox)
  2. For each Bridge: dump linkedDevices and linkedAccessories
  3. For each Lock: show whether linkedDevices contains a Bridge ref
  4. Submit one safe bridge job per detected (lock, bridge) pair: `battery-level`
     and poll GetBridgeJobStatus until success/failed/timeout
  5. Submit one `device-status` job (also read-only)
  6. Print a small JSON summary

No PIN create/delete is performed.

Run from project root:
  PYTHONPATH=backend/python python3 scripts/probe_igloo_bridge.py
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

# Make backend/python importable
_BACKEND = Path(__file__).resolve().parent.parent / "backend" / "python"
sys.path.insert(0, str(_BACKEND))

# Load .env for vault bootstrap secrets
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(_BACKEND.parent.parent / ".env")
except Exception:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("probe")

from common.igloo_client import IglooClient, API_BASE_URL  # noqa: E402


def short(d, *keys):
    return {k: d.get(k) for k in keys if k in d}


def main():
    client = IglooClient()

    # 1. List devices
    log.info("Listing all devices…")
    devices = client.list_devices()
    log.info("Got %d devices", len(devices))

    by_type: dict[str, list] = {}
    for d in devices:
        by_type.setdefault(d.get("type") or "Unknown", []).append(d)
    print("\n=== Device counts by type ===")
    for t, lst in sorted(by_type.items()):
        print(f"  {t}: {len(lst)}")

    bridges = by_type.get("Bridge", [])
    locks = by_type.get("Lock", [])
    keypads = by_type.get("Keypad", [])

    if not bridges:
        print("\n[WARN] No Bridge devices returned. Aborting bridge probe.")
        return 0

    print(f"\n=== Bridges ({len(bridges)}) ===")
    for b in bridges[:10]:
        print(json.dumps({
            **short(b, "deviceId", "deviceName", "type", "batteryLevel",
                       "propertyName", "departmentName", "pairedAt", "lastSync"),
            "linkedDevices": b.get("linkedDevices"),
            "linkedAccessories": b.get("linkedAccessories"),
        }, indent=2, default=str))

    # Build device -> bridge map. In this deployment bridges link to Keypads, not Locks.
    # We accept any non-Bridge linked device as a candidate "PIN-bearing" device.
    device_to_bridge: dict[str, str] = {}
    bridge_to_devices: dict[str, list[dict]] = {}
    for b in bridges:
        ld = b.get("linkedDevices") or []
        linked = []
        for ent in ld if isinstance(ld, list) else []:
            if not isinstance(ent, dict):
                continue
            t = (ent.get("type") or "").lower()
            did = ent.get("deviceId") or ent.get("id")
            if did and t != "bridge":
                linked.append({"deviceId": did, "type": ent.get("type"), "name": ent.get("name")})
                device_to_bridge.setdefault(did, b["deviceId"])
        if linked:
            bridge_to_devices[b["deviceId"]] = linked

    print(f"\n=== bridge -> linked devices (from bridge.linkedDevices) ===")
    for bid, ls in bridge_to_devices.items():
        print(f"  bridge {bid}: {ls}")

    print(f"\n=== device -> bridge pairings discovered: {len(device_to_bridge)} ===")
    for did, bid in device_to_bridge.items():
        print(f"  device {did}  ->  bridge {bid}")

    if not device_to_bridge:
        print("\n[WARN] No device<->bridge pair found via linkedDevices. Aborting.")
        return 0

    test_device, test_bridge = next(iter(device_to_bridge.items()))
    print(f"\n=== Probing bridge-proxied job ===")
    print(f"   device={test_device}  bridge={test_bridge}")

    # Look up device's departmentId — print full device blob for inspection
    dev_obj = next((d for d in devices if d.get("deviceId") == test_device), {})
    bridge_obj = next((d for d in devices if d.get("deviceId") == test_bridge), {})
    print(f"\n   device blob: {json.dumps(dev_obj, indent=2, default=str)}")
    print(f"\n   bridge blob: {json.dumps(bridge_obj, indent=2, default=str)}")

    dept_id = dev_obj.get("departmentId") or bridge_obj.get("departmentId")
    print(f"   device.departmentId={dev_obj.get('departmentId')} bridge.departmentId={bridge_obj.get('departmentId')}")

    # List departments + try them all
    depts = client.list_departments()
    print(f"   departments: {[(d.get('id'), d.get('name')) for d in depts]}")
    if not dept_id and depts:
        dept_id = depts[0].get("id")

    headers = client._auth_headers()  # type: ignore[attr-defined]

    def submit(job_type, extra: dict | None = None) -> str | None:
        # Canonical schema (from IGW-API-V2.yaml): jobType is INT, payload uses jobData
        body: dict = {"jobType": job_type}
        if dept_id:
            body["departmentId"] = dept_id
        if extra:
            body.update(extra)
        url = f"{API_BASE_URL}/devices/{test_device}/jobs/bridges/{test_bridge}"
        log.info("POST %s body=%s", url, body)
        import requests as _rq
        # Use plain requests to bypass the HTTPClient retry+raise so we can read body on 4xx
        try:
            r = _rq.post(url, headers=headers, json=body, timeout=30)
        except Exception as exc:
            log.exception("submit %s transport error: %s", job_type, exc)
            return None
        log.info("RESP %s body=%s", r.status_code, r.text[:1000])
        if r.status_code >= 400:
            return None
        try:
            data = r.json()
            return data.get("jobId") or (data.get("payload") or {}).get("jobId")
        except Exception:
            return None

    def poll(job_id: str, timeout: float = 30.0, interval: float = 2.0) -> dict | None:
        deadline = time.time() + timeout
        url = f"{API_BASE_URL}/bridge/jobs/{job_id}"
        last = None
        while time.time() < deadline:
            try:
                import requests as _rq
                r = _rq.get(url, headers=headers, timeout=30)
                if r.status_code != 200:
                    log.info("poll %s HTTP %s body=%s", job_id, r.status_code, r.text[:300])
                    return last
                body = r.json()
                last = body
                completed = body.get("completed")
                # Doc shows top-level "completed" boolean and "jobResponse"
                log.info("poll %s -> completed=%s body=%s", job_id, completed, json.dumps(body)[:250])
                if completed is True:
                    return body
            except Exception as exc:
                log.warning("poll error: %s", exc)
            time.sleep(interval)
        return last

    summary = {
        "device_counts": {t: len(v) for t, v in by_type.items()},
        "bridges_seen": len(bridges),
        "locks_seen": len(locks),
        "keypads_seen": len(keypads),
        "device_bridge_pairs": len(device_to_bridge),
        "probe_device": test_device,
        "probe_bridge": test_bridge,
        "jobs": {},
    }

    # Try each department for this device — discover which department the device belongs to
    print("\n=== Trying battery-level (jobType=9) against each department ===")
    working_dept = None
    for d in depts:
        candidate = d.get("id")
        body = {"jobType": 9, "departmentId": candidate}
        url = f"{API_BASE_URL}/devices/{test_device}/jobs/bridges/{test_bridge}"
        import requests as _rq
        r = _rq.post(url, headers=headers, json=body, timeout=30)
        log.info("  dept=%s -> HTTP %s body=%s", candidate, r.status_code, r.text[:300])
        if r.status_code in (200, 201):
            working_dept = candidate
            break
    print(f"   working_dept={working_dept}")

    if not working_dept:
        # Try without departmentId at all (in case the doc lies)
        for body_variant in [
            {"jobType": 9},
            {"jobType": 9, "jobData": {}},
        ]:
            r = _rq.post(url, headers=headers, json=body_variant, timeout=30)
            log.info("  no-dept body=%s -> HTTP %s body=%s", body_variant, r.status_code, r.text[:300])
            if r.status_code in (200, 201):
                working_dept = ""
                break

    dept_id = working_dept or dept_id
    summary["working_dept"] = working_dept

    # Now run the safe-jobs probe with the working dept
    safe_jobs = [
        ("battery-level", 9, None),
        ("device-status", 10, None),
        ("activity-logs", 15, None),
    ]
    accepted = []
    for label, jt, extra in safe_jobs:
        jid = submit(jt, extra)
        if jid:
            accepted.append(label)
            result = poll(jid, timeout=30.0, interval=2.0)
            summary["jobs"][label] = {
                "submitted": True,
                "jobType": jt,
                "jobId": jid,
                "final": result,
            }
        else:
            summary["jobs"][label] = {"submitted": False, "jobType": jt}
    summary["accepted_job_types"] = accepted

    print("\n=== Probe summary ===")
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main() or 0)
