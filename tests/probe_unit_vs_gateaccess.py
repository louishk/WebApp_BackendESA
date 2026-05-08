"""
Probe SOAP UnitsInformation_v3 vs GateAccessData for site L031 and compare
bRented/rented status per unit. Goal: determine which endpoint is the
authoritative source-of-truth for occupancy after a customer move-out.

Run from VM (or anywhere with vault access + SOAP creds):
    cd backend/python
    python3 ../tests/probe_unit_vs_gateaccess.py L031

Optional: pass UNIT_IDS env to filter the printout (e.g. "167319,167320").
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend" / "python"))

from sync_service.pipelines._ccws_utils import (  # noqa: E402
    NAMESPACE, build_soap_client, to_bool, to_int,
)


def fetch_units(soap, site_code):
    return soap.call(
        operation="UnitsInformation_v3",
        parameters={"sLocationCode": site_code},
        soap_action=f"{NAMESPACE}/UnitsInformation_v3",
        namespace=NAMESPACE,
        result_tag="Table",
    ) or []


def fetch_gate(soap, site_code):
    return soap.call(
        operation="GateAccessData",
        parameters={
            "sLocationCode": site_code,
            "iMinutesSinceLastUpdate": "0",
        },
        soap_action=f"{NAMESPACE}/GateAccessData",
        namespace=NAMESPACE,
        result_tag="Table",
    ) or []


def main():
    site_code = sys.argv[1] if len(sys.argv) > 1 else "L031"
    filter_ids = {
        int(x) for x in os.environ.get("UNIT_IDS", "").split(",") if x.strip().isdigit()
    }

    print(f"\n=== Probing site {site_code} ===")
    soap = build_soap_client()
    try:
        units_raw = fetch_units(soap, site_code)
        gate_raw = fetch_gate(soap, site_code)
    finally:
        try:
            soap.close()
        except Exception:
            pass

    print(f"UnitsInformation_v3 returned {len(units_raw)} rows")
    print(f"GateAccessData       returned {len(gate_raw)} rows")

    units = {}
    for r in units_raw:
        uid = to_int(r.get("UnitID"))
        if uid is None:
            continue
        units[uid] = {
            "sUnitName": r.get("sUnitName"),
            "bRented": to_bool(r.get("bRented")),
            "bRentable": to_bool(r.get("bRentable")),
            "iDaysVacant": to_int(r.get("iDaysVacant")),
            "iDaysRented": to_int(r.get("iDaysRented")),
            "dMovedIn": r.get("dMovedIn"),
        }

    gate = {}
    for r in gate_raw:
        uid = to_int(r.get("UnitID"))
        if uid is None:
            continue
        gate[uid] = {
            "sUnitName": r.get("sUnitName"),
            "bRented": to_bool(r.get("bRented")),
            "bGateLocked": to_bool(r.get("bGateLocked")),
            "bOverlocked": to_bool(r.get("bOverlocked")),
            "sAccessCode": r.get("sAccessCode") or "",
        }

    union = sorted(set(units) | set(gate))
    if filter_ids:
        union = [u for u in union if u in filter_ids]

    print()
    hdr = (
        f"{'UnitID':>8}  {'Name':<10}  "
        f"{'Units.bRented':>14}  {'Gate.bRented':>13}  "
        f"{'GateLocked':>10}  {'Overlocked':>10}  {'HasCode':>7}  "
        f"{'DaysVacant':>10}  {'DaysRented':>10}  {'Match':>7}"
    )
    print(hdr)
    print("-" * len(hdr))

    only_in_units = []
    only_in_gate = []
    mismatched = []
    matched = 0

    for uid in union:
        u = units.get(uid)
        g = gate.get(uid)
        if u and not g:
            only_in_units.append(uid)
            print(
                f"{uid:>8}  {(u['sUnitName'] or ''):<10}  "
                f"{str(u['bRented']):>14}  {'<MISSING>':>13}  "
                f"{'-':>10}  {'-':>10}  {'-':>7}  "
                f"{str(u['iDaysVacant']):>10}  {str(u['iDaysRented']):>10}  {'GATE-':>7}"
            )
            continue
        if g and not u:
            only_in_gate.append(uid)
            print(
                f"{uid:>8}  {(g['sUnitName'] or ''):<10}  "
                f"{'<MISSING>':>14}  {str(g['bRented']):>13}  "
                f"{str(g['bGateLocked']):>10}  {str(g['bOverlocked']):>10}  "
                f"{('Y' if g['sAccessCode'] else 'N'):>7}  "
                f"{'-':>10}  {'-':>10}  {'UNITS-':>7}"
            )
            continue
        ur = bool(u['bRented'])
        gr = bool(g['bRented'])
        same = ur == gr
        if same:
            matched += 1
        else:
            mismatched.append(uid)
        print(
            f"{uid:>8}  {(u['sUnitName'] or ''):<10}  "
            f"{str(ur):>14}  {str(gr):>13}  "
            f"{str(g['bGateLocked']):>10}  {str(g['bOverlocked']):>10}  "
            f"{('Y' if g['sAccessCode'] else 'N'):>7}  "
            f"{str(u['iDaysVacant']):>10}  {str(u['iDaysRented']):>10}  "
            f"{('OK' if same else 'DIFF'):>7}"
        )

    print()
    print(f"Summary for {site_code}:")
    print(f"  Units only in UnitsInformation_v3 (no gate row): {len(only_in_units)}")
    print(f"  Units only in GateAccessData       (no unit row): {len(only_in_gate)}")
    print(f"  Both endpoints, bRented matches:                 {matched}")
    print(f"  Both endpoints, bRented DIVERGES:                {len(mismatched)}")
    if mismatched:
        # Break down the divergence direction
        units_says_vacant_gate_says_rented = [
            uid for uid in mismatched
            if not units[uid]['bRented'] and gate[uid]['bRented']
        ]
        units_says_rented_gate_says_vacant = [
            uid for uid in mismatched
            if units[uid]['bRented'] and not gate[uid]['bRented']
        ]
        print(f"    Units=vacant / Gate=rented (stale gate): "
              f"{len(units_says_vacant_gate_says_rented)}")
        print(f"    Units=rented / Gate=vacant (stale units): "
              f"{len(units_says_rented_gate_says_vacant)}")
        sample = mismatched[:20]
        print(f"    Sample mismatched UnitIDs: {sample}")

    if only_in_units:
        print(f"  Sample units missing from GateAccessData: {only_in_units[:20]}")


if __name__ == "__main__":
    main()
