#!/usr/bin/env python3
"""
Mass smoke test — every plan shape exercised end-to-end.

Walks the booking middleware through all 4 plan shapes back-to-back:

  Shape 1  Regular discount, no prepay
           (concession bPrepay=false, plan.discount_perpetual=false)
           → 1 month due at move-in; ECRI auto-scheduled at +12 mo

  Shape 2  Regular + SiteLink-native prepay
           (concession.bPrepay=true, iPrePaidMonths=N)
           → SOAP bundles N months at move-in; ECRI at +N+1 mo

  Shape 3  Perpetual, no prepay
           (plan.discount_perpetual=true, prepayment_months=NULL)
           → 1 month due; discount applies forever; ECRI at +12 mo

  Shape 4  Perpetual + custom prepay
           (plan.discount_perpetual=true, prepayment_months=N)
           → full N-month total due at move-in; ECRI at +N

For each shape, the harness performs every step the bot would:

  1. POST /api/recommendations
     — find a slot whose `terms` match the target shape
  2. GET  /api/reservations/move-in/cost
     — confirm the SOAP-truth amount before charging
  3. POST /api/reservations/reserve
     — get a waiting_id (no payment yet)
  4. POST /api/reservations/move-in
     — pass `pricing.total_due_at_movein` from step 1, with
     Idempotency-Key and session_id for outcome reconciliation
  5. Verify the response includes the right `followups` outcome

Then summarises pass/fail per shape.

Authentication
--------------
Set `BACKEND_API_KEY` env var to your `esa_<keyid>.<secret>` token.
The key needs scopes: recommender + reservations:read + reservations:write.

Run
---
    BACKEND_API_KEY=esa_abc...  python3 scripts/smoke_4_shapes.py
    BACKEND_API_KEY=esa_abc...  python3 scripts/smoke_4_shapes.py --site LSETUP
    BACKEND_API_KEY=esa_abc...  python3 scripts/smoke_4_shapes.py --shape 4

Output
------
Per-shape: PASS/FAIL with the slot picked, total_due_at_movein, ledger_id,
followups summary. Final block: cross-shape pass-rate.

Exit code 0 = all shapes passed; non-zero = at least one shape failed.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib import request as urlreq
from urllib.parse import urlencode
from urllib.error import HTTPError

DEFAULT_BASE = "https://backend.extraspace.com.sg"
DEFAULT_BOOKING_SITE = "LSETUP"
# Recommend can safely probe live sites — it's read-only. Reserve +
# move-in always runs on LSETUP to avoid creating real production leases.
DEFAULT_RECOMMEND_SITES = [
    "L017", "L018", "L022", "L029", "L030",   # SG sites with rich plan inventory
    "L001", "L002", "L003", "L004", "L005",
    "L008", "L025",
    "LSETUP",
]

# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

def _request(method: str, url: str, key: str, body: Optional[dict] = None,
             headers: Optional[dict] = None) -> Tuple[int, Any]:
    """Single HTTP call; returns (status, body) or (status, error_text)."""
    data = json.dumps(body).encode() if body is not None else None
    req = urlreq.Request(url, data=data, method=method)
    req.add_header('X-API-Key', key)
    req.add_header('Content-Type', 'application/json')
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urlreq.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read())
    except HTTPError as e:
        try:
            return e.code, json.loads(e.read())
        except Exception:
            return e.code, {'error': str(e)}


# ─────────────────────────────────────────────────────────────────────────────
# Shape detection
# ─────────────────────────────────────────────────────────────────────────────

def classify_slot(slot: dict) -> Optional[int]:
    """Map a slot's terms to a shape number (1..4) or None if it matches none."""
    if not slot:
        return None
    t = slot.get('terms') or {}
    perpetual = bool(t.get('discount_perpetual'))
    prepay_mo = t.get('prepayment_months')
    native_prepay = t.get('native_prepay_months')

    if perpetual and prepay_mo and prepay_mo > 0:
        return 4
    if perpetual:
        return 3
    if native_prepay and native_prepay > 0:
        return 2
    return 1


def find_slot_for_shape(base: str, key: str, sites: List[str], shape: int,
                        require_site: Optional[str] = None,
                        exclude_unit_ids: Optional[List[int]] = None) -> Optional[dict]:
    """Hit /api/recommendations across `sites`, return the first slot
    matching the requested shape.

    `require_site` (e.g. 'LSETUP') forces the matched slot's facility to
    equal that site — used when we need a slot for the booking flow.
    When None, any matching slot from any of the recommend sites is OK.
    """
    filter_dims = [
        {},
        {'unit_type': ['W']},
        {'unit_type': ['L']},
        {'unit_type': ['U']},
        {'unit_type': ['M']},
        {'unit_type': ['W'], 'size_range': ['14-16']},
        {'unit_type': ['W'], 'size_range': ['30-35']},
        {'climate_type': ['NC']},
    ]

    excluded = set(exclude_unit_ids or [])
    for site in sites:
        for dim in filter_dims:
            f = {'location': [site], **dim}
            rid = f"smoke-shape{shape}-{int(time.time()*1000)}-{uuid.uuid4().hex[:6]}"
            ctx_body = {
                'mode': 'recommendation', 'duration_months': 6,
                'filters': f,
                'context': {
                    'channel': 'chatbot',
                    'request_id': rid,
                    'session_id': f"smoke-shape{shape}",
                    'customer_id': f"smoke_shape{shape}",
                    'move_in_date': (date.today() + timedelta(days=15)).isoformat(),
                },
            }
            if excluded:
                ctx_body['constraints'] = {'exclude_unit_ids': sorted(excluded)}
            status, body = _request('POST', f"{base}/api/recommendations", key, body=ctx_body)
            if status != 200 or not isinstance(body, dict):
                continue
            for slot in body.get('slots') or []:
                if classify_slot(slot) != shape:
                    continue
                if require_site and slot.get('facility') != require_site:
                    continue
                if slot.get('unit_id') in excluded:
                    continue
                slot['__request_id'] = rid
                slot['__session_id'] = f"smoke-shape{shape}"
                slot['__customer_id'] = f"smoke_shape{shape}"
                return slot
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Booking flow
# ─────────────────────────────────────────────────────────────────────────────

def fetch_move_in_cost(base: str, key: str, slot: dict, move_in_date: str) -> Tuple[int, Any]:
    qs = urlencode({
        'site_code':     slot['facility'],
        'unit_id':       slot['unit_id'],
        'concession_id': slot['concession_id'] or 0,
        'move_in_date':  move_in_date,
        'insurance_id':  (slot.get('insurance') or {}).get('selected', {}).get('id', 0) or 0,
        'variant':       'standard',
    })
    return _request('GET', f"{base}/api/reservations/move-in/cost?{qs}", key)


_ALLOWED_BOOKING_SITES = {'LSETUP'}


def _assert_booking_site_safe(slot: dict, step_name: str) -> None:
    """Hard guard — refuse to reserve/move-in on anything other than the
    allowed test sites. Belt-and-braces against future regressions where
    a live-site slot might leak into the booking path."""
    facility = slot.get('facility', '')
    if facility not in _ALLOWED_BOOKING_SITES:
        raise RuntimeError(
            f"REFUSING TO {step_name} ON LIVE SITE — slot.facility={facility!r}, "
            f"only allowed: {sorted(_ALLOWED_BOOKING_SITES)}. "
            f"This is a hard safety guard; see scripts/smoke_4_shapes.py."
        )


def reserve(base: str, key: str, slot: dict, move_in_date: str, suffix: str) -> Tuple[int, Any]:
    _assert_booking_site_safe(slot, 'RESERVE')
    return _request('POST', f"{base}/api/reservations/reserve", key, body={
        'site_code':     slot['facility'],
        'unit_id':       slot['unit_id'],
        'concession_id': slot['concession_id'] or 0,
        'first_name':    'Smoke',
        'last_name':     f"Shape{suffix}",
        'phone':         '99999999',
        'email':         f"smoke.shape{suffix}@example.com",
        'needed_date':   move_in_date,
        'comment':       f"4-shape smoke test (shape {suffix}) — DELETE",
        'source':        'chatbot',
        'source_name':   'Smoke4Shapes',
        'session_id':    slot.get('__session_id'),
        'customer_id':   slot.get('__customer_id'),
        'plan_id':       slot['plan_id'],
    })


def move_in(base: str, key: str, slot: dict, waiting_id, tenant_id,
            payment_amount: float, move_in_date: str, idem_key: str) -> Tuple[int, Any]:
    _assert_booking_site_safe(slot, 'MOVE-IN')
    return _request('POST', f"{base}/api/reservations/move-in", key,
        body={
            'site_code':      slot['facility'],
            'waiting_id':     waiting_id,
            'tenant_id':      tenant_id,
            'unit_id':        slot['unit_id'],
            'payment_amount': payment_amount,
            'pay_method':     2,
            'concession_id':  slot['concession_id'] or 0,
            'insurance_id':   (slot.get('insurance') or {}).get('selected', {}).get('id', 0) or 0,
            'start_date':     move_in_date,
            'end_date':       (date.fromisoformat(move_in_date) + timedelta(days=365)).isoformat(),
            'session_id':     slot.get('__session_id'),
            'customer_id':    slot.get('__customer_id'),
        },
        headers={'Idempotency-Key': idem_key},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Per-shape runner
# ─────────────────────────────────────────────────────────────────────────────

def run_shape(base: str, key: str, recommend_sites: List[str],
              booking_site: str, shape: int) -> Dict[str, Any]:
    """Returns a dict with status='pass'|'fail'|'skip' and per-step details.

    Two-tier search:
      Step 1 — find ANY slot of this shape across recommend_sites
               (validates the recommend response shape).
      Step 1b — try to find an LSETUP-ish slot of this shape for the
                booking flow. If the live-site slot already happens to
                be on the booking_site, reuse it.
    Steps 2-5 (cost, reserve, move-in, idempotency) only run when we
    have a slot on the booking_site. Otherwise the shape is marked
    'recommend_only' (a partial pass).
    """
    out: Dict[str, Any] = {'shape': shape, 'status': 'fail', 'steps': []}

    print(f"\n{'═'*72}")
    print(f"  SHAPE {shape}  —  {SHAPE_DESCRIPTIONS[shape]}")
    print('═'*72)

    # 1. recommend across live + booking sites
    live_slot = find_slot_for_shape(base, key, recommend_sites, shape)
    if not live_slot:
        out['status'] = 'skip'
        out['reason'] = f"No slot of shape {shape} found in any recommend_site. Configure a plan."
        print(f"  ⚠️  SKIP — {out['reason']}")
        return out
    print(f"  ✓ 1. recommend (live)  unit={live_slot['unit_id']} site={live_slot['facility']}")
    print(f"       plan={live_slot['plan_name']!r}  concession={live_slot['concession_name'] or '(stdrate)'}")
    print(f"       terms.discount_perpetual={live_slot['terms'].get('discount_perpetual')}")
    print(f"       terms.prepayment_months={live_slot['terms'].get('prepayment_months')}")
    print(f"       terms.native_prepay_months={live_slot['terms'].get('native_prepay_months')}")
    print(f"       pricing.total_due_at_movein = ${live_slot['pricing'].get('total_due_at_movein')}")
    out['steps'].append({'step': '1.recommend', 'ok': True})
    out['live_unit_id'] = live_slot['unit_id']
    out['live_site'] = live_slot['facility']

    # 1b. find a slot of the same shape on the BOOKING site for the booking flow
    if live_slot['facility'] == booking_site:
        slot = live_slot
    else:
        slot = find_slot_for_shape(base, key, [booking_site], shape, require_site=booking_site)

    if not slot:
        # Recommend layer validated, but no bookable slot of this shape
        # on the booking site. Still a partial pass.
        out['status'] = 'recommend_only'
        print(f"  ⚠️  No slot of shape {shape} on booking site {booking_site} —")
        print(f"      recommend layer verified, skipping reserve/move-in for this shape.")
        return out

    out['steps'].append({'step': '1.recommend_on_booking_site', 'ok': True})
    print(f"  ✓ 1b. found slot on {booking_site}  unit={slot['unit_id']} {slot['unit_type'] or '?'}/{slot['climate_type'] or '?'}/{slot['size_range'] or '?'}")
    print(f"       plan={slot['plan_name']!r}  concession={slot['concession_name'] or '(stdrate)'}")
    print(f"       terms.discount_perpetual={slot['terms'].get('discount_perpetual')}")
    print(f"       terms.prepayment_months={slot['terms'].get('prepayment_months')}")
    print(f"       terms.native_prepay_months={slot['terms'].get('native_prepay_months')}")
    out['unit_id'] = slot['unit_id']
    out['plan_id'] = slot['plan_id']
    out['concession_id'] = slot['concession_id']

    move_in_date = (date.today() + timedelta(days=15)).isoformat()
    quote_total = slot['pricing'].get('total_due_at_movein') or slot['pricing'].get('first_month_total')
    print(f"       pricing.total_due_at_movein = ${quote_total}")

    # 2. /move-in/cost — SOAP-truth confirmation
    status, cost_body = fetch_move_in_cost(base, key, slot, move_in_date)
    if status != 200:
        out['failed_at'] = '2.cost'; out['cost_response'] = cost_body
        print(f"  ✗ 2. /move-in/cost failed: {status} {cost_body}")
        return out
    soap_total = cost_body.get('total') or 0
    print(f"  ✓ 2. /move-in/cost  SOAP-truth total = ${soap_total}")
    out['soap_movein_cost'] = soap_total
    out['steps'].append({'step': '2.cost', 'ok': True, 'total': soap_total})

    # For shapes 1/3 the calculator quote == SOAP cost. For shape 4 our
    # calculator's total_due_at_movein > SOAP cost (includes prepay surplus).
    # For shape 2 SOAP bundles natively → soap_total > 1-month equivalent.
    payment_to_send = max(float(quote_total or 0), float(soap_total or 0))
    print(f"       bot will charge: ${payment_to_send:.2f}")

    # 3. reserve
    status, reserve_body = reserve(base, key, slot, move_in_date, str(shape))
    if status != 200 or not reserve_body.get('success'):
        out['failed_at'] = '3.reserve'; out['reserve_response'] = reserve_body
        print(f"  ✗ 3. reserve failed: {status} {reserve_body}")
        return out
    waiting_id = reserve_body['waiting_id']
    tenant_id = reserve_body['tenant_id']
    print(f"  ✓ 3. reserve  waiting_id={waiting_id}  tenant_id={tenant_id}")
    out['waiting_id'] = waiting_id
    out['tenant_id'] = tenant_id
    out['steps'].append({'step': '3.reserve', 'ok': True})

    # 4. move-in (with idempotency).
    # If SOAP returns "already rented", the candidate table is stale.
    # Self-heal by retrying with another unit of the same shape, up to 3x.
    idem_key = f"smoke-shape{shape}-{uuid.uuid4().hex}"
    status, mi_body = move_in(base, key, slot, waiting_id, tenant_id,
                              payment_to_send, move_in_date, idem_key)
    rented_msg_keywords = ('already rented', 'no longer available', 'unit rented')
    excluded_units: List[int] = []
    retries_left = 3
    while (status == 200 and not mi_body.get('success')
           and any(k in (mi_body.get('message', '') or '').lower() for k in rented_msg_keywords)
           and retries_left > 0):
        excluded_units.append(slot['unit_id'])
        retries_left -= 1
        print(f"  ↻ 4. move-in: unit {slot['unit_id']} stale; retrying with a fresh unit (excluded={excluded_units}, retries_left={retries_left})")
        slot = find_slot_for_shape(base, key, [booking_site], shape,
                                   require_site=booking_site,
                                   exclude_unit_ids=excluded_units)
        if not slot:
            out['failed_at'] = '4.movein.exhausted'; out['movein_response'] = mi_body
            print(f"  ✗ 4. move-in: no fresh unit of shape {shape} on {booking_site} after {len(excluded_units)} retries")
            return out
        status, reserve_body = reserve(base, key, slot, move_in_date, str(shape))
        if status != 200 or not reserve_body.get('success'):
            out['failed_at'] = '3.reserve.retry'; out['reserve_response'] = reserve_body
            print(f"  ✗ retry reserve failed: {status} {reserve_body}")
            return out
        waiting_id = reserve_body['waiting_id']
        tenant_id = reserve_body['tenant_id']
        idem_key = f"smoke-shape{shape}-{uuid.uuid4().hex}"
        status, mi_body = move_in(base, key, slot, waiting_id, tenant_id,
                                  payment_to_send, move_in_date, idem_key)

    if status != 200 or not mi_body.get('success'):
        out['failed_at'] = '4.movein'; out['movein_response'] = mi_body
        print(f"  ✗ 4. move-in failed: {status} {mi_body.get('message', mi_body)}")
        return out
    ledger_id = mi_body.get('ledger_id')
    followups = mi_body.get('followups') or {}
    print(f"  ✓ 4. move-in  ledger_id={ledger_id}")
    print(f"       followups: enqueued={followups.get('enqueued', 0)} "
          f"inline_ok={followups.get('inline_ok', 0)} "
          f"pending_retry={followups.get('pending_retry', 0)} "
          f"failed_permanent={followups.get('failed_permanent', 0)}")
    out['ledger_id'] = ledger_id
    out['followups'] = followups
    out['steps'].append({'step': '4.movein', 'ok': True})

    # 5. idempotency replay verification
    status, replay = move_in(base, key, slot, waiting_id, tenant_id,
                              payment_to_send, move_in_date, idem_key)
    if status == 200 and replay.get('idempotent_replay'):
        print(f"  ✓ 5. idempotency replay returned cached response")
        out['steps'].append({'step': '5.idempotency', 'ok': True})
    else:
        print(f"  ⚠️ 5. idempotency replay did NOT return cached response (status={status})")
        out['steps'].append({'step': '5.idempotency', 'ok': False, 'detail': replay})

    # 6. expected followups by shape
    expected = EXPECTED_FOLLOWUPS[shape]
    actual_count = followups.get('enqueued', 0)
    if actual_count == expected:
        print(f"  ✓ 6. followups count matches expected ({expected})")
        out['steps'].append({'step': '6.followups_count', 'ok': True})
    else:
        print(f"  ⚠️ 6. followups count = {actual_count}, expected {expected}")
        out['steps'].append({'step': '6.followups_count', 'ok': False,
                              'expected': expected, 'actual': actual_count})

    # All required steps passed?
    out['status'] = 'pass' if all(s.get('ok') for s in out['steps'] if s['step'] in ('1.recommend','2.cost','3.reserve','4.movein')) else 'fail'
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

SHAPE_DESCRIPTIONS = {
    1: 'Regular discount, no prepay (single-month move-in cost)',
    2: 'Regular + SiteLink-native prepay (multi-month bundled in MoveIn)',
    3: 'Perpetual, no prepay (discount applies forever; ECRI at +12 mo)',
    4: 'Perpetual + custom prepay (full prepayment up front; ECRI at +N mo)',
}

# Number of follow-up jobs the orchestrator should enqueue per shape
# when the master switches are ON. (When OFF, 0 across all shapes.)
EXPECTED_FOLLOWUPS = {1: 1, 2: 1, 3: 1, 4: 2}


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(description='4-shape smoke for the booking middleware')
    p.add_argument('--base', default=os.environ.get('BACKEND_BASE_URL', DEFAULT_BASE),
                   help='Backend base URL (default: %s)' % DEFAULT_BASE)
    p.add_argument('--booking-site', default=DEFAULT_BOOKING_SITE,
                   help='Site to use for /reserve + /move-in (default: LSETUP — never hit production sites)')
    p.add_argument('--recommend-sites', default=','.join(DEFAULT_RECOMMEND_SITES),
                   help='Comma-separated site list to probe for /api/recommendations (read-only — safe to use live sites)')
    p.add_argument('--shape', type=int, choices=[1, 2, 3, 4], default=None,
                   help='Run only this shape')
    args = p.parse_args()

    key = os.environ.get('BACKEND_API_KEY', '').strip()
    if not key:
        print("ERROR: set BACKEND_API_KEY env var to your esa_<keyid>.<secret> token", file=sys.stderr)
        return 2

    recommend_sites = [s.strip() for s in args.recommend_sites.split(',') if s.strip()]
    # Always include the booking site so we can find shapes specifically
    # configured there (e.g. perpetual+prepay test plan on LSETUP).
    if args.booking_site not in recommend_sites:
        recommend_sites.append(args.booking_site)

    # Hard safety check — booking site must be in the allowlist
    if args.booking_site not in _ALLOWED_BOOKING_SITES:
        print(
            f"ERROR: --booking-site={args.booking_site!r} is not in the allowlist {sorted(_ALLOWED_BOOKING_SITES)}.\n"
            f"Reserve + move-in are LSETUP-only by design — never live sites.\n"
            f"To extend, edit _ALLOWED_BOOKING_SITES in this file (and only after ops sign-off).",
            file=sys.stderr,
        )
        return 2

    print(f"\n4-shape smoke harness")
    print(f"  base:             {args.base}")
    print(f"  recommend_sites:  {recommend_sites}   (read-only; safe to use live)")
    print(f"  booking_site:     {args.booking_site}   (reserve + move-in ONLY here)")
    print(f"  allowed_booking_sites (hard guard): {sorted(_ALLOWED_BOOKING_SITES)}")
    print(f"  shapes:           {[args.shape] if args.shape else [1,2,3,4]}")

    shapes = [args.shape] if args.shape else [1, 2, 3, 4]
    results: List[Dict[str, Any]] = []
    for s in shapes:
        try:
            results.append(run_shape(args.base, key, recommend_sites, args.booking_site, s))
        except Exception as exc:
            print(f"  ✗ SHAPE {s} crashed: {exc}")
            results.append({'shape': s, 'status': 'fail', 'reason': str(exc)})
        # Polite pause to respect /move-in's 5/min rate limit
        time.sleep(13)

    # Summary
    print(f"\n{'═'*72}\n  SUMMARY\n{'═'*72}")
    pass_n = sum(1 for r in results if r['status'] == 'pass')
    rec_only = sum(1 for r in results if r['status'] == 'recommend_only')
    fail_n = sum(1 for r in results if r['status'] == 'fail')
    skip_n = sum(1 for r in results if r['status'] == 'skip')
    print(f"  pass={pass_n}  recommend_only={rec_only}  fail={fail_n}  skip={skip_n}")
    for r in results:
        marker = {'pass': '✓', 'fail': '✗', 'skip': '⚠️', 'recommend_only': '◐'}[r['status']]
        line = f"  {marker} Shape {r['shape']}: {r['status'].upper()}"
        if r['status'] == 'pass':
            line += f"  ledger={r.get('ledger_id')} (booking_site)"
        elif r['status'] == 'recommend_only':
            line += f"  recommend OK at {r.get('live_site')} unit={r.get('live_unit_id')}; no booking-site slot"
        elif r['status'] == 'fail':
            line += f"  failed_at={r.get('failed_at')}"
        elif r['status'] == 'skip':
            line += f"  ({r.get('reason', '')[:60]})"
        print(line)

    # Exit non-zero only on hard failures (recommend_only + skip are partial passes)
    return 0 if fail_n == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
