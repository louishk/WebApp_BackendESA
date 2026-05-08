"""
Dump raw GateAccessData fields for a specific site/unit, side-by-side with
UnitsInformation_v3 fields. Confirms exactly what SOAP returns and whether
our ccws_gate_access pipeline is mapping bRented correctly.

Run on VM:
    sudo -n bash -c 'set -a && source /var/www/backend/backend/python/.env && set +a && \
        cd /var/www/backend/backend/python && \
        PYTHONPATH=/var/www/backend/backend/python venv/bin/python \
        /tmp/probe_gateaccess_raw.py L001 5'

Args: site_code, max_units_to_dump
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend" / "python"))

from sync_service.pipelines._ccws_utils import (  # noqa: E402
    NAMESPACE, build_soap_client, to_int, to_bool,
)


def main():
    site_code = sys.argv[1] if len(sys.argv) > 1 else "L001"
    max_dump = int(sys.argv[2]) if len(sys.argv) > 2 else 5

    soap = build_soap_client()
    try:
        units = soap.call(
            operation="UnitsInformation_v3",
            parameters={"sLocationCode": site_code},
            soap_action=f"{NAMESPACE}/UnitsInformation_v3",
            namespace=NAMESPACE, result_tag="Table",
        ) or []
        gate = soap.call(
            operation="GateAccessData",
            parameters={
                "sLocationCode": site_code,
                "iMinutesSinceLastUpdate": "0",
            },
            soap_action=f"{NAMESPACE}/GateAccessData",
            namespace=NAMESPACE, result_tag="Table",
        ) or []
    finally:
        try: soap.close()
        except Exception: pass

    print(f"\n=== {site_code}: UnitsInformation_v3 sample row keys ===")
    if units:
        print(sorted(units[0].keys()))
    print(f"\n=== {site_code}: GateAccessData sample row keys ===")
    if gate:
        print(sorted(gate[0].keys()))

    # Build maps
    units_by_uid = {to_int(r.get("UnitID")): r for r in units if r.get("UnitID")}
    gate_by_uid = {to_int(r.get("UnitID")): r for r in gate if r.get("UnitID")}

    # Find some interesting comparison cases:
    #   (a) units with bRented=true in both
    #   (b) units with bRented=false in both
    #   (c) any divergence
    rented_match = []
    vacant_match = []
    diverge = []
    for uid, g in gate_by_uid.items():
        u = units_by_uid.get(uid)
        if u is None:
            continue
        ub = bool(to_bool(u.get("bRented")))
        gb = bool(to_bool(g.get("bRented")))
        if ub and gb:
            rented_match.append(uid)
        elif (not ub) and (not gb):
            vacant_match.append(uid)
        else:
            diverge.append((uid, ub, gb))

    print(f"\nTotals: rented_match={len(rented_match)} "
          f"vacant_match={len(vacant_match)} diverge={len(diverge)}")
    if diverge:
        print(f"DIVERGE samples (uid, units.bRented, gate.bRented): {diverge[:10]}")

    print(f"\n=== Raw RENTED samples (max {max_dump}) ===")
    for uid in rented_match[:max_dump]:
        u = units_by_uid[uid]; g = gate_by_uid[uid]
        print(f"\nUnit {uid} ({u.get('sUnitName')}):")
        print(f"  UnitsInformation_v3.bRented   = {u.get('bRented')!r}")
        print(f"  UnitsInformation_v3.iDaysRented = {u.get('iDaysRented')!r}")
        print(f"  UnitsInformation_v3.iDaysVacant = {u.get('iDaysVacant')!r}")
        print(f"  GateAccessData.bRented        = {g.get('bRented')!r}")
        print(f"  GateAccessData.bGateLocked    = {g.get('bGateLocked')!r}")
        print(f"  GateAccessData.bOverlocked    = {g.get('bOverlocked')!r}")
        print(f"  GateAccessData.sAccessCode    = {(g.get('sAccessCode') or '')[:1]}{'*' * max(0, len(g.get('sAccessCode') or '')-1)}")
        print(f"  GateAccessData.iKeypadZ       = {g.get('iKeypadZ')!r}")
        print(f"  GateAccessData all keys -> values:")
        for k in sorted(g.keys()):
            v = g[k]
            if k == 'sAccessCode' and v:
                v = f"<{len(v)} chars>"
            print(f"     {k}: {v!r}")

    print(f"\n=== Raw VACANT samples (max {max_dump}) ===")
    for uid in vacant_match[:max_dump]:
        u = units_by_uid[uid]; g = gate_by_uid[uid]
        print(f"\nUnit {uid} ({u.get('sUnitName')}):")
        print(f"  UnitsInformation_v3.bRented   = {u.get('bRented')!r}")
        print(f"  UnitsInformation_v3.iDaysRented = {u.get('iDaysRented')!r}")
        print(f"  UnitsInformation_v3.iDaysVacant = {u.get('iDaysVacant')!r}")
        print(f"  GateAccessData.bRented        = {g.get('bRented')!r}")
        print(f"  GateAccessData.bGateLocked    = {g.get('bGateLocked')!r}")
        print(f"  GateAccessData.bOverlocked    = {g.get('bOverlocked')!r}")
        sac = g.get('sAccessCode') or ''
        print(f"  GateAccessData.sAccessCode    = {('<'+str(len(sac))+' chars>') if sac else 'EMPTY'}")
        print(f"  GateAccessData.iKeypadZ       = {g.get('iKeypadZ')!r}")


if __name__ == "__main__":
    main()
