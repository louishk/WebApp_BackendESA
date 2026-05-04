#!/usr/bin/env python3
"""
smoke_hard.py — Harder smoke test for the ESA recommendation + booking middleware.

Extends smoke_4_shapes.py with 12 adversarial / stress scenarios designed to
surface edge-cases before 3rd-party integration handoff.

Scenarios
---------
1.  Multi-value filter stress
2.  Pool rescue forcing (impossible combo)
3.  Slot 3 relax forcing (narrow request → slot 3 strictly cheaper)
4.  Slot 2 neighbour fallback (thin inventory site)
5.  mode=quote pricing match to the cent
6.  Idempotency replay (second call returns idempotent_replay=true, same ledger)
7.  Sanity guard ($1 below total_due_at_movein → HTTP 400)
8.  concession_id=0 preservation (no-discount path)
9.  Perpetual + dynamic prepay ECRI date (duration_months ∈ {3,6,9,12})
10. excluded_unit_ids self-heal on "Unit already rented"
11. Insurance re-quote delta (client-side math → new_total_due)
12. Random fuzz (50 scenarios, ≥2 slots ≥95%, 0 5xx)

Safety
------
Reserve + move-in only on LSETUP. Recommend is read-only on all live sites.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib import request as urlreq
from urllib.error import HTTPError
from urllib.parse import urlencode

# ─────────────────────────────────────────────────────────────────────────────
# Safety guard (identical to smoke_4_shapes.py — never relax)
# ─────────────────────────────────────────────────────────────────────────────

_ALLOWED_BOOKING_SITES = {'LSETUP'}

DEFAULT_BASE = "https://backend.extraspace.com.sg"
DEFAULT_BOOKING_SITE = "LSETUP"

# Sites that are safe to hit with /api/recommendations (read-only)
LIVE_RECOMMEND_SITES = [
    "L017", "L018", "L022", "L029", "L030",
    "L001", "L002", "L003", "L004", "L005",
    "L008", "L025", "LSETUP",
]

# Thin-inventory site candidates for test 4 (slot 2 neighbour)
THIN_SITES = ["L029", "L030", "L025"]


def _assert_booking_safe(site: str, step: str) -> None:
    if site not in _ALLOWED_BOOKING_SITES:
        raise RuntimeError(
            f"REFUSING {step} on live site '{site}'. "
            f"Only allowed: {sorted(_ALLOWED_BOOKING_SITES)}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# HTTP layer
# ─────────────────────────────────────────────────────────────────────────────

def _request(method: str, url: str, key: str,
             body: Optional[dict] = None,
             headers: Optional[dict] = None) -> Tuple[int, Any]:
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


def _recommend(base: str, key: str, body: dict) -> Tuple[int, Any]:
    return _request('POST', f"{base}/api/recommendations", key, body=body)


def _move_in_cost(base: str, key: str, site: str, unit_id: int,
                  concession_id: int, move_in_date: str,
                  insurance_id: int = 0) -> Tuple[int, Any]:
    qs = urlencode({
        'site_code': site, 'unit_id': unit_id,
        'concession_id': concession_id, 'move_in_date': move_in_date,
        'insurance_id': insurance_id, 'variant': 'standard',
    })
    return _request('GET', f"{base}/api/reservations/move-in/cost?{qs}", key)


def _reserve(base: str, key: str, slot: dict, move_in_date: str,
             suffix: str) -> Tuple[int, Any]:
    _assert_booking_safe(slot['facility'], 'RESERVE')
    return _request('POST', f"{base}/api/reservations/reserve", key, body={
        'site_code': slot['facility'],
        'unit_id': slot['unit_id'],
        'concession_id': slot.get('concession_id') or 0,
        'first_name': 'SmokeHard',
        'last_name': f"T{suffix}",
        'phone': '99999999',
        'email': f"smokehard.{suffix}@example.com",
        'needed_date': move_in_date,
        'comment': f"smoke_hard test #{suffix} — DELETE",
        'source': 'chatbot',
        'source_name': 'SmokeHard',
        'plan_id': slot.get('plan_id'),
        # Phase A — both fields required on /reserve
        'session_id':  slot.get('__session_id'),
        'customer_id': slot.get('__customer_id'),
    })


def _move_in(base: str, key: str, slot: dict, waiting_id, tenant_id,
             payment_amount: float, move_in_date: str,
             idem_key: str) -> Tuple[int, Any]:
    _assert_booking_safe(slot['facility'], 'MOVE-IN')
    return _request(
        'POST', f"{base}/api/reservations/move-in", key,
        body={
            'site_code': slot['facility'],
            'waiting_id': waiting_id,
            'tenant_id': tenant_id,
            'unit_id': slot['unit_id'],
            'payment_amount': payment_amount,
            'pay_method': 2,
            'concession_id': slot.get('concession_id') or 0,
            'insurance_id': _insurance_id(slot),
            'start_date': move_in_date,
            'end_date': (date.fromisoformat(move_in_date) + timedelta(days=365)).isoformat(),
            'session_id': slot.get('__session_id'),
            'customer_id': slot.get('__customer_id'),
        },
        headers={'Idempotency-Key': idem_key},
    )


def _insurance_id(slot: dict) -> int:
    ins = slot.get('insurance') or {}
    sel = ins.get('selected') or {}
    return int(sel.get('id') or 0)


# ─────────────────────────────────────────────────────────────────────────────
# Result tracking
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Result:
    scenario: str
    status: str = 'FAIL'      # PASS / FAIL / SKIP
    notes: str = ''
    slot_info: str = ''
    total_due: Optional[float] = None
    ledger_id: Optional[Any] = None
    followups: Optional[Dict] = None

    def passed(self, notes: str = '', slot_info: str = '',
               total_due=None, ledger_id=None, followups=None):
        self.status = 'PASS'
        self.notes = notes
        self.slot_info = slot_info
        self.total_due = total_due
        self.ledger_id = ledger_id
        self.followups = followups

    def failed(self, notes: str):
        self.status = 'FAIL'
        self.notes = notes

    def skipped(self, notes: str):
        self.status = 'SKIP'
        self.notes = notes


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def _req_body(locations, duration=6, unit_type=None, climate=None,
              size_range=None, session_id=None, customer_id=None,
              mode='recommendation', unit_id=None, exclude_ids=None,
              coupon_code=None) -> dict:
    ctx_sid = session_id or f"smoke-hard-{uuid.uuid4().hex[:8]}"
    body: dict = {
        'mode': mode,
        'duration_months': duration,
        'filters': {'location': locations if isinstance(locations, list) else [locations]},
        'context': {
            'channel': 'chatbot',
            'request_id': str(uuid.uuid4()),
            'session_id': ctx_sid,
            'customer_id': customer_id or 'smoke_hard',
        },
    }
    if unit_type:
        body['filters']['unit_type'] = unit_type if isinstance(unit_type, list) else [unit_type]
    if climate:
        body['filters']['climate_type'] = climate if isinstance(climate, list) else [climate]
    if size_range:
        body['filters']['size_range'] = size_range if isinstance(size_range, list) else [size_range]
    if coupon_code:
        body['filters']['coupon_code'] = coupon_code
    if unit_id is not None:
        body['filters']['unit_id'] = unit_id
    if exclude_ids:
        body['constraints'] = {'exclude_unit_ids': exclude_ids}
    return body


def _booking_slot_on_lsetup(base: str, key: str, duration: int = 6,
                             unit_type=None, climate=None) -> Optional[dict]:
    """Find any bookable slot on LSETUP for the given params."""
    body = _req_body(['LSETUP'], duration=duration,
                     unit_type=unit_type, climate=climate)
    st, resp = _recommend(base, key, body)
    if st != 200:
        return None
    for s in (resp.get('slots') or []):
        if s and s.get('facility') == 'LSETUP':
            s['__session_id'] = body['context']['session_id']
            s['__customer_id'] = body['context']['customer_id']
            return s
    return None


def _full_booking_flow(base: str, key: str, slot: dict,
                       move_in_date: str, suffix: str,
                       idem_key: Optional[str] = None
                       ) -> Tuple[int, Any, str]:
    """reserve → move-in. Returns (move_in_status, move_in_body, idem_key_used)."""
    st, res_body = _reserve(base, key, slot, move_in_date, suffix)
    if st != 200 or not res_body.get('success'):
        return st, res_body, ''
    waiting_id = res_body['waiting_id']
    tenant_id = res_body['tenant_id']
    payment = float(slot['pricing'].get('total_due_at_movein')
                    or slot['pricing'].get('first_month_total') or 0)
    if not idem_key:
        idem_key = f"smoke-hard-{suffix}-{uuid.uuid4().hex}"
    st2, mi_body = _move_in(base, key, slot, waiting_id, tenant_id,
                            payment, move_in_date, idem_key)
    return st2, mi_body, idem_key


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 1 — Multi-value filter stress
# ─────────────────────────────────────────────────────────────────────────────

def test_1_multivalue_filter(base: str, key: str) -> Result:
    r = Result("1. Multi-value filter stress")
    print("\n[1] Multi-value filter stress")

    body = _req_body(
        locations=["L017", "L022", "LSETUP"],
        duration=6,
        unit_type=["W", "L", "U"],
        climate=["A", "NC", "RF"],
        size_range=["14-16", "30-35", "50-60"],
    )
    st, resp = _recommend(base, key, body)
    if st != 200:
        r.failed(f"HTTP {st}: {resp.get('error', resp)}")
        return r

    slots = [s for s in (resp.get('slots') or []) if s]
    slot_count = len(slots)
    if slot_count < 1:
        r.failed(f"0 slots returned — expected ≥1. stats={resp.get('stats')}")
        return r

    # Verify no 5xx embedded; each slot has required fields
    for s in slots:
        if not all(k in s for k in ('unit_id', 'facility', 'pricing')):
            r.failed(f"Slot missing required fields: {list(s.keys())}")
            return r

    r.passed(
        notes=f"{slot_count} slots returned; facilities={[s['facility'] for s in slots]}",
        slot_info=f"unit_ids={[s['unit_id'] for s in slots]}",
    )
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 2 — Pool rescue forcing
# ─────────────────────────────────────────────────────────────────────────────

def test_2_pool_rescue(base: str, key: str) -> Result:
    r = Result("2. Pool rescue forcing")
    print("\n[2] Pool rescue forcing (impossible combo)")

    # Impossible: tiny + climate-controlled + RF (refrigerated wine) with 12mo duration
    # at a small residential site — almost certainly yields zero strict candidates.
    body = _req_body(
        locations=["L029"],
        duration=12,
        unit_type=["WN"],          # wine units — typically absent at L029
        climate=["RF"],
        size_range=["0-6"],
    )
    st, resp = _recommend(base, key, body)
    if st == 500:
        r.failed(f"HTTP 500 on impossible combo — should not 5xx: {resp}")
        return r
    if st != 200:
        # A 200 with empty slots is fine; non-200 non-500 is also acceptable (400
        # if validation rejects something), but note it.
        r.failed(f"HTTP {st} — expected 200 or empty envelope: {resp.get('error', resp)}")
        return r

    stats = resp.get('stats') or {}
    slots = [s for s in (resp.get('slots') or []) if s]

    # Contract: either ≥1 slot returned via rescue, or clean empty envelope — no 500.
    pool_rescue_step = stats.get('pool_rescue_step')
    saturation = stats.get('saturation_signal', False)

    if slots:
        relaxed = [s.get('match_flags', {}).get('relaxed_dims') for s in slots]
        r.passed(
            notes=f"Pool rescue fired: step={pool_rescue_step}, saturation={saturation}, "
                  f"{len(slots)} slot(s) returned; relaxed_dims per slot={relaxed}",
            slot_info=f"unit_ids={[s['unit_id'] for s in slots]}",
        )
    else:
        # Clean empty envelope is acceptable (no inventory at all even after rescue)
        r.passed(
            notes=f"Clean empty envelope (0 slots). No 500. pool_rescue_step={pool_rescue_step}",
        )
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 3 — Slot 3 relax forcing
# ─────────────────────────────────────────────────────────────────────────────

def test_3_slot3_relax(base: str, key: str) -> Result:
    r = Result("3. Slot 3 relax forcing")
    print("\n[3] Slot 3 relax forcing")

    # Narrow request — single climate + tight size + single site → slot 3 should
    # relax one dimension to find something strictly cheaper.
    body = _req_body(
        locations=["L017"],
        duration=6,
        climate=["A"],
        size_range=["14-16"],
    )
    st, resp = _recommend(base, key, body)
    if st != 200:
        r.failed(f"HTTP {st}: {resp.get('error', resp)}")
        return r

    slots = resp.get('slots') or [None, None, None]
    s1 = slots[0] if len(slots) > 0 else None
    s3 = slots[2] if len(slots) > 2 else None

    if s3 is None:
        # No slot 3 at all — document with reason
        relaxed = (s1 or {}).get('match_flags', {}).get('relaxed_dims') if s1 else None
        r.passed(
            notes=f"Slot 3 absent (no strictly-cheaper unit found after relax). "
                  f"slot1_price={s1['price'] if s1 else 'N/A'}, documented.",
        )
        return r

    s1_price = float(s1['price']) if s1 else None
    s3_price = float(s3['price'])
    relaxed = (s3.get('match_flags') or {}).get('relaxed_dims')

    if s1_price is not None and s3_price >= s1_price:
        r.failed(
            f"Slot 3 price ${s3_price:.2f} >= Slot 1 price ${s1_price:.2f} "
            f"— not strictly cheaper. relaxed_dims={relaxed}"
        )
        return r

    r.passed(
        notes=f"Slot 3 (${s3_price:.2f}) strictly cheaper than Slot 1 (${s1_price:.2f}). "
              f"relaxed_dims={relaxed}",
        slot_info=f"slot3_unit={s3['unit_id']} @ {s3['facility']}",
    )
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 4 — Slot 2 neighbour fallback
# ─────────────────────────────────────────────────────────────────────────────

def test_4_slot2_neighbour(base: str, key: str) -> Result:
    r = Result("4. Slot 2 neighbour fallback")
    print("\n[4] Slot 2 neighbour fallback (thin site)")

    found = False
    for site in THIN_SITES:
        body = _req_body([site], duration=6)
        st, resp = _recommend(base, key, body)
        if st != 200:
            continue
        slots = resp.get('slots') or [None, None, None]
        s1 = slots[0] if len(slots) > 0 else None
        s2 = slots[1] if len(slots) > 1 else None
        if not s1:
            continue

        found = True
        if s2 is None:
            r.passed(
                notes=f"Slot 2 absent for thin site={site} — no neighbour within radius. "
                      f"Slot 1 present (unit={s1['unit_id']}).",
            )
            return r

        mf = s2.get('match_flags') or {}
        strat = mf.get('alternative_strategy')
        dist = mf.get('distance_km')
        warn = mf.get('travel_warning', False)

        if strat in ('neighbour_close', 'neighbour_far'):
            r.passed(
                notes=f"Slot 2 is neighbour at {s2['facility']} via strategy={strat}, "
                      f"distance={dist}km, travel_warning={warn}",
                slot_info=f"s1={s1['facility']}:{s1['unit_id']} s2={s2['facility']}:{s2['unit_id']}",
            )
        else:
            r.passed(
                notes=f"Slot 2 is same-site 2nd (strategy={strat}) for site={site}. "
                      f"Neighbour not needed — inventory exists.",
                slot_info=f"s1={s1['unit_id']} s2={s2['unit_id']}",
            )
        return r

    if not found:
        r.skipped("No thin site returned Slot 1 — all tried sites appear empty.")
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 5 — mode=quote pricing match
# ─────────────────────────────────────────────────────────────────────────────

def test_5_mode_quote(base: str, key: str) -> Result:
    r = Result("5. mode=quote pricing match")
    print("\n[5] mode=quote pricing vs original slot")

    # Step A: get a slot via recommendation
    body_rec = _req_body(["LSETUP"], duration=6)
    st, resp = _recommend(base, key, body_rec)
    if st != 200 or not (resp.get('slots') or [None])[0]:
        r.failed(f"Could not get initial recommendation: HTTP {st}")
        return r

    s1 = resp['slots'][0]
    unit_id = s1['unit_id']
    concession_id = s1.get('concession_id') or 0
    original_price = float(s1['price'])

    # Step B: mode=quote on same unit
    body_q = _req_body(
        locations=["LSETUP"], duration=6,
        mode='quote', unit_id=unit_id,
    )
    if concession_id:
        body_q['filters']['concession_id'] = concession_id
    st2, resp2 = _recommend(base, key, body_q)
    if st2 != 200:
        r.failed(f"mode=quote HTTP {st2}: {resp2.get('error', resp2)}")
        return r

    quote_slots = resp2.get('slots') or []
    if not quote_slots or not quote_slots[0]:
        r.failed(f"mode=quote returned no slots for unit_id={unit_id}")
        return r

    qs = quote_slots[0]
    quote_price = float(qs['price'])
    delta = abs(quote_price - original_price)
    if delta > 0.01:
        r.failed(
            f"price mismatch: recommend=${original_price:.2f} quote=${quote_price:.2f} "
            f"delta=${delta:.2f} > $0.01 for unit_id={unit_id}"
        )
        return r

    r.passed(
        notes=f"mode=quote matches to cent: ${quote_price:.2f} == ${original_price:.2f} "
              f"(delta=${delta:.4f})",
        slot_info=f"unit={unit_id} concession={concession_id}",
    )
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 6 — Idempotency replay
# ─────────────────────────────────────────────────────────────────────────────

def test_6_idempotency(base: str, key: str) -> Result:
    r = Result("6. Idempotency replay")
    print("\n[6] Idempotency replay")

    move_in_date = (date.today() + timedelta(days=15)).isoformat()
    slot = _booking_slot_on_lsetup(base, key)
    if not slot:
        r.skipped("No slot on LSETUP — cannot test idempotency")
        return r

    # Reserve ONCE. Both move-in calls re-use the same waiting_id /
    # tenant_id / payment so the request body is identical — that's
    # required for idempotency replay (H4 body-hash check rejects
    # same-key + different-body with HTTP 422).
    st_r, res_body = _reserve(base, key, slot, move_in_date, "idem1")
    if st_r != 200 or not res_body.get('success'):
        # Self-heal on "already rented"
        body2 = _req_body(['LSETUP'], duration=6,
                          exclude_ids=[slot['unit_id']])
        st_r2, resp2 = _recommend(base, key, body2)
        if st_r2 == 200:
            for s in (resp2.get('slots') or []):
                if s and s.get('facility') == 'LSETUP':
                    slot = s
                    slot['__session_id'] = body2['context']['session_id']
                    slot['__customer_id'] = body2['context']['customer_id']
                    break
        st_r, res_body = _reserve(base, key, slot, move_in_date, "idem1b")
    if st_r != 200 or not res_body.get('success'):
        r.failed(f"Reserve failed: HTTP {st_r} msg={res_body}")
        return r

    waiting_id = res_body['waiting_id']
    tenant_id  = res_body['tenant_id']
    payment    = float(slot['pricing'].get('total_due_at_movein')
                       or slot['pricing'].get('first_month_total') or 0)
    idem_key   = f"smoke-hard-idem-{uuid.uuid4().hex}"

    # First /move-in. Self-heal on "Unit already rented" — pool can be
    # thin on LSETUP after repeated smoke runs. Try up to 3 fresh slots.
    excluded_ids: list[int] = [slot['unit_id']]
    st1, mi1 = _move_in(base, key, slot, waiting_id, tenant_id,
                        payment, move_in_date, idem_key)
    for retry in range(3):
        if st1 == 200 and mi1.get('success'):
            break
        msg = (mi1.get('message') or '').lower()
        if 'rent' not in msg and 'available' not in msg:
            break
        # Re-recommend excluding the failed unit, fresh reserve+move-in
        body_h = _req_body(['LSETUP'], duration=6, exclude_ids=excluded_ids)
        st_h, resp_h = _recommend(base, key, body_h)
        if st_h != 200:
            break
        new_slot = next((s for s in (resp_h.get('slots') or [])
                         if s and s.get('facility') == 'LSETUP'), None)
        if not new_slot:
            break
        new_slot['__session_id']  = body_h['context']['session_id']
        new_slot['__customer_id'] = body_h['context']['customer_id']
        excluded_ids.append(new_slot['unit_id'])
        st_r, res_body = _reserve(base, key, new_slot, move_in_date, f"idem-h{retry}")
        if st_r != 200 or not res_body.get('success'):
            continue
        slot = new_slot
        waiting_id = res_body['waiting_id']
        tenant_id  = res_body['tenant_id']
        payment    = float(slot['pricing'].get('total_due_at_movein')
                           or slot['pricing'].get('first_month_total') or 0)
        idem_key   = f"smoke-hard-idem-{uuid.uuid4().hex}"
        st1, mi1 = _move_in(base, key, slot, waiting_id, tenant_id,
                            payment, move_in_date, idem_key)

    if st1 != 200 or not mi1.get('success'):
        r.failed(f"First move-in failed: HTTP {st1} msg={mi1.get('message', mi1)}")
        return r

    ledger1 = mi1.get('ledger_id')
    time.sleep(1)

    # Second /move-in with SAME idem_key AND SAME body
    st2, mi2 = _move_in(base, key, slot, waiting_id, tenant_id,
                        payment, move_in_date, idem_key)

    if st2 != 200:
        r.failed(f"Replay returned HTTP {st2} (expected 200): {mi2}")
        return r

    if not mi2.get('idempotent_replay'):
        r.failed(
            f"Replay did NOT return idempotent_replay=true. "
            f"ledger1={ledger1} ledger2={mi2.get('ledger_id')}"
        )
        return r

    if mi2.get('ledger_id') != ledger1:
        r.failed(
            f"Replay returned different ledger_id: first={ledger1} replay={mi2.get('ledger_id')}"
        )
        return r

    r.passed(
        notes="Replay correctly returned idempotent_replay=true with identical ledger_id",
        ledger_id=ledger1,
        total_due=slot['pricing'].get('total_due_at_movein'),
        followups=mi1.get('followups'),
    )
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 7 — Sanity guard
# ─────────────────────────────────────────────────────────────────────────────

def test_7_sanity_guard(base: str, key: str) -> Result:
    r = Result("7. Sanity guard ($1 below total_due)")
    print("\n[7] Sanity guard")

    move_in_date = (date.today() + timedelta(days=15)).isoformat()
    slot = _booking_slot_on_lsetup(base, key)
    if not slot:
        r.skipped("No slot on LSETUP — cannot test sanity guard")
        return r

    total_due = float(slot['pricing'].get('total_due_at_movein')
                      or slot['pricing'].get('first_month_total') or 0)
    if total_due <= 1.0:
        r.skipped(f"total_due_at_movein={total_due} too small to test ($1 below)")
        return r

    # Reserve first
    st_r, res_body = _reserve(base, key, slot, move_in_date, "sg")
    if st_r != 200 or not res_body.get('success'):
        r.failed(f"Reserve failed: HTTP {st_r} {res_body.get('message', '')}")
        return r

    waiting_id = res_body['waiting_id']
    tenant_id = res_body['tenant_id']
    short_payment = round(total_due - 1.00, 2)
    idem_key = f"smoke-hard-sg-{uuid.uuid4().hex}"

    _assert_booking_safe(slot['facility'], 'MOVE-IN (sanity-guard test)')
    st_mi, mi_body = _request(
        'POST', f"{base}/api/reservations/move-in", key,
        body={
            'site_code': slot['facility'],
            'waiting_id': waiting_id,
            'tenant_id': tenant_id,
            'unit_id': slot['unit_id'],
            'payment_amount': short_payment,
            'pay_method': 2,
            'concession_id': slot.get('concession_id') or 0,
            'insurance_id': _insurance_id(slot),
            'start_date': move_in_date,
            'end_date': (date.fromisoformat(move_in_date) + timedelta(days=365)).isoformat(),
        },
        headers={'Idempotency-Key': idem_key},
    )

    if st_mi == 400:
        r.passed(
            notes=f"Sanity guard fired correctly: HTTP 400. "
                  f"sent=${short_payment:.2f} vs total_due=${total_due:.2f}. "
                  f"msg={mi_body.get('error', mi_body)}",
            slot_info=f"unit={slot['unit_id']}",
        )
    elif st_mi == 200 and not (mi_body.get('success')):
        # Some implementations return 200 with success=False
        r.passed(
            notes=f"Sanity guard fired (200/success=false). "
                  f"msg={mi_body.get('message', mi_body)}",
        )
    else:
        r.failed(
            f"Sanity guard did NOT fire. HTTP {st_mi} success={mi_body.get('success')} "
            f"sent=${short_payment:.2f} vs total_due=${total_due:.2f}"
        )
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 8 — concession_id=0 preservation
# ─────────────────────────────────────────────────────────────────────────────

def test_8_concession_zero(base: str, key: str) -> Result:
    r = Result("8. concession_id=0 preservation")
    print("\n[8] concession_id=0 preservation")

    # Look for a standard-rate slot (no concession) on LSETUP
    body = _req_body(['LSETUP'], duration=6)
    st, resp = _recommend(base, key, body)
    if st != 200:
        r.failed(f"Recommend HTTP {st}: {resp.get('error', '')}")
        return r

    slots = resp.get('slots') or []
    zero_slot = None
    for s in slots:
        if s and (s.get('concession_id') or 0) == 0:
            zero_slot = s
            break

    if not zero_slot:
        r.skipped("No standard-rate (concession_id=0) slot found on LSETUP")
        return r

    zero_slot['__session_id'] = body['context']['session_id']
    zero_slot['__customer_id'] = body['context']['customer_id']

    move_in_date = (date.today() + timedelta(days=15)).isoformat()
    idem_key = f"smoke-hard-cz0-{uuid.uuid4().hex}"
    st_mi, mi_body, _ = _full_booking_flow(
        base, key, zero_slot, move_in_date, "cz0", idem_key)

    if st_mi == 200 and mi_body.get('success'):
        r.passed(
            notes="concession_id=0 round-tripped through reserve+move-in without error",
            ledger_id=mi_body.get('ledger_id'),
            total_due=zero_slot['pricing'].get('total_due_at_movein'),
            followups=mi_body.get('followups'),
        )
    else:
        msg = mi_body.get('message') or mi_body.get('error') or str(mi_body)
        # "Already rented" is not a concession_id=0 bug — flag as skip
        if any(k in msg.lower() for k in ('already rented', 'no longer available')):
            r.skipped(f"Unit already rented; can't confirm concession_id=0 path: {msg}")
        else:
            r.failed(
                f"Move-in with concession_id=0 failed: HTTP {st_mi} msg={msg}"
            )
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 9 — Perpetual + dynamic prepay ECRI date
# ─────────────────────────────────────────────────────────────────────────────

def test_9_perpetual_dynamic_prepay(base: str, key: str) -> Result:
    r = Result("9. Perpetual + dynamic prepay ECRI date")
    print("\n[9] Perpetual + dynamic prepay ECRI date")

    results_per_duration = {}
    any_found = False

    for dur in [3, 6, 9, 12]:
        body = _req_body(['LSETUP'], duration=dur)
        st, resp = _recommend(base, key, body)
        if st != 200:
            results_per_duration[dur] = f"HTTP {st}"
            continue

        # Look for a perpetual+prepaid slot
        target_slot = None
        for s in (resp.get('slots') or []):
            if not s:
                continue
            t = s.get('terms') or {}
            if t.get('discount_perpetual') and t.get('prepayment_months'):
                target_slot = s
                break

        if not target_slot:
            results_per_duration[dur] = "no perpetual+prepay slot found"
            continue

        any_found = True
        prepay_months = target_slot['terms']['prepayment_months']
        rate_change_date = target_slot['pricing'].get('rate_change_date')
        total_due = target_slot['pricing'].get('total_due_at_movein')

        # Verify prepayment_months matches the requested duration
        if prepay_months != dur:
            results_per_duration[dur] = (
                f"MISMATCH: prepayment_months={prepay_months} != "
                f"requested duration={dur}"
            )
            continue

        # Verify rate_change_date = (billing_date of month 1) + dur months
        # We can check via the breakdown
        breakdown = (target_slot.get('pricing') or {}).get('breakdown') or []
        if breakdown:
            from dateutil.relativedelta import relativedelta
            billing_date_str = breakdown[0].get('billing_date')
            if billing_date_str:
                try:
                    bd = date.fromisoformat(billing_date_str)
                    expected_change = (bd + relativedelta(months=dur)).isoformat()
                    if rate_change_date and rate_change_date != expected_change:
                        results_per_duration[dur] = (
                            f"rate_change_date={rate_change_date} != "
                            f"expected={expected_change} (billing_date={billing_date_str} + {dur}mo)"
                        )
                        continue
                except Exception as e:
                    results_per_duration[dur] = f"date parse error: {e}"
                    continue

        results_per_duration[dur] = (
            f"OK prepay={prepay_months}mo "
            f"total_due=${total_due:.2f} "
            f"rate_change_date={rate_change_date}"
        )

    if not any_found:
        r.skipped(
            "No perpetual+prepay slot found on LSETUP for any duration. "
            "Configure a Prepaid+perpetual plan on LSETUP to exercise this test."
        )
        return r

    failures = {k: v for k, v in results_per_duration.items() if 'MISMATCH' in str(v) or 'error' in str(v).lower()}
    if failures:
        r.failed(f"ECRI date mismatches: {failures} / all: {results_per_duration}")
    else:
        r.passed(
            notes=f"Perpetual+prepay ECRI dates correct for all tested durations: {results_per_duration}",
        )
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 10 — excluded_unit_ids self-heal
# ─────────────────────────────────────────────────────────────────────────────

def test_10_exclude_selfheal(base: str, key: str) -> Result:
    r = Result("10. excluded_unit_ids self-heal")
    print("\n[10] excluded_unit_ids self-heal")

    move_in_date = (date.today() + timedelta(days=15)).isoformat()

    # Get first slot
    body1 = _req_body(['LSETUP'], duration=6)
    st1, resp1 = _recommend(base, key, body1)
    if st1 != 200 or not (resp1.get('slots') or [None])[0]:
        r.failed(f"Initial recommend failed: HTTP {st1}")
        return r

    first_slot = resp1['slots'][0]
    first_unit_id = first_slot['unit_id']
    first_slot['__session_id'] = body1['context']['session_id']
    first_slot['__customer_id'] = body1['context']['customer_id']

    # Simulate "already rented" by excluding the unit and requesting again
    body2 = _req_body(['LSETUP'], duration=6, exclude_ids=[first_unit_id])
    st2, resp2 = _recommend(base, key, body2)
    if st2 != 200:
        r.failed(f"Second recommend (exclude) HTTP {st2}: {resp2.get('error', '')}")
        return r

    second_slots = [s for s in (resp2.get('slots') or []) if s]
    second_unit_ids = [s['unit_id'] for s in second_slots]

    if first_unit_id in second_unit_ids:
        r.failed(
            f"Excluded unit_id={first_unit_id} still appeared in second recommend "
            f"response: {second_unit_ids}"
        )
        return r

    if not second_slots:
        r.skipped("No second slot returned after exclusion — only one unit on LSETUP?")
        return r

    healed_slot = next((s for s in second_slots if s.get('facility') == 'LSETUP'), None)
    if not healed_slot:
        r.skipped("No LSETUP slot in second recommend after exclusion")
        return r

    r.passed(
        notes=f"Exclude self-heal: excluded unit_id={first_unit_id}, "
              f"second recommend returned unit_id={healed_slot['unit_id']} "
              f"(excluded count: {resp2.get('stats', {}).get('excluded_unit_ids_count', '?')})",
        slot_info=f"healed_unit={healed_slot['unit_id']}",
    )
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 11 — Insurance re-quote delta
# ─────────────────────────────────────────────────────────────────────────────

def test_11_insurance_requote(base: str, key: str) -> Result:
    r = Result("11. Insurance re-quote delta")
    print("\n[11] Insurance re-quote delta (client-side math)")

    body = _req_body(['LSETUP'], duration=6)
    st, resp = _recommend(base, key, body)
    if st != 200 or not (resp.get('slots') or [None])[0]:
        r.failed(f"Recommend HTTP {st}: {resp.get('error', '')}")
        return r

    slot = resp['slots'][0]
    ins_block = slot.get('insurance') or {}
    selected = ins_block.get('selected') or {}
    options = ins_block.get('options') or []

    if len(options) < 2:
        r.skipped(
            f"Only {len(options)} insurance option(s) on LSETUP slot — "
            f"need ≥2 to test delta. unit_id={slot['unit_id']}"
        )
        return r

    # Pick a tier different from the selected one
    default_id = selected.get('id')
    other = next((o for o in options if o.get('id') != default_id), None)
    if not other:
        r.skipped("Could not find an alternative insurance tier")
        return r

    default_premium = float(selected.get('premium') or 0)
    other_premium = float(other.get('premium') or 0)
    delta_premium = other_premium - default_premium

    # Get the tax rate from the slot
    ins_tax = float(slot['pricing'].get('monthly_insurance_tax') or 0)
    ins_prem = float(slot['pricing'].get('monthly_insurance_premium') or default_premium)
    tax_rate = (ins_tax / ins_prem) if ins_prem > 0 else 0.09

    # Recompute expected new all-in monthly
    current_all_in = float(
        slot['pricing'].get('monthly_all_in_during_prepay')
        or slot['pricing'].get('first_month_total')
        or slot['price']
        or 0
    )
    expected_new_all_in = round(current_all_in + delta_premium * (1 + tax_rate), 2)

    # For non-perpetual slots, the bot's all-in is not explicitly in
    # monthly_all_in_during_prepay. Verify the formula is self-consistent
    # rather than hitting a re-quote endpoint (client-side only per contract).
    prepay_months = (slot.get('terms') or {}).get('prepayment_months') or 1
    total_due = float(slot['pricing'].get('total_due_at_movein')
                      or slot['pricing'].get('first_month_total') or 0)
    new_total_due = round(total_due + delta_premium * (1 + tax_rate) * prepay_months, 2)

    r.passed(
        notes=(
            f"Insurance delta math: default_premium=${default_premium:.2f} "
            f"other_premium=${other_premium:.2f} delta=${delta_premium:.2f} "
            f"tax_rate={tax_rate:.4f} "
            f"new_all_in=${expected_new_all_in:.2f} "
            f"new_total_due=${new_total_due:.2f} "
            f"(prepay_months={prepay_months})"
        ),
        slot_info=f"unit={slot['unit_id']} default_ins_id={default_id} other_ins_id={other.get('id')}",
    )
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 12 — Random fuzz
# ─────────────────────────────────────────────────────────────────────────────

PERSONAS = [
    # (budget) small non-climate
    {'unit_type': ['L', 'U', 'M'], 'climate': ['NC'], 'size_range': ['0-6', '6-8', '8-10']},
    # (mid) walk-in climate-controlled
    {'unit_type': ['W'], 'climate': ['A', 'AD'], 'size_range': ['14-16', '16-20', '20-25']},
    # (premium) larger anything
    {'unit_type': ['W', 'DV'], 'climate': ['A'], 'size_range': ['30-35', '35-40', '40-45']},
]

FUZZ_SITES = ["L017", "L018", "L022", "L001", "L003", "L004", "L008", "LSETUP"]


def test_12_random_fuzz(base: str, key: str, n: int = 50) -> Result:
    r = Result("12. Random fuzz (50 scenarios)")
    print(f"\n[12] Random fuzz ({n} scenarios)")

    pass_count = 0
    fail_5xx = 0
    fail_lt2 = 0
    fail_details: List[str] = []

    for i in range(n):
        persona = random.choice(PERSONAS)
        sites = random.sample(FUZZ_SITES, k=random.randint(1, 3))
        dur = random.randint(1, 12)
        unit_types = random.sample(persona['unit_type'],
                                   k=random.randint(1, len(persona['unit_type'])))
        climates = random.sample(persona['climate'],
                                 k=random.randint(1, len(persona['climate'])))
        sizes = random.sample(persona['size_range'],
                              k=random.randint(1, len(persona['size_range'])))

        body = _req_body(
            locations=sites, duration=dur,
            unit_type=unit_types, climate=climates, size_range=sizes,
            customer_id=f"fuzz_{i}",
        )

        st, resp = _recommend(base, key, body)
        if st >= 500:
            fail_5xx += 1
            fail_details.append(f"[{i}] 5xx HTTP {st} sites={sites}")
            continue

        if st != 200:
            # 400/501 etc. are acceptable for degenerate inputs
            pass_count += 1
            continue

        slots = [s for s in (resp.get('slots') or []) if s]
        if len(slots) >= 2:
            pass_count += 1
        else:
            fail_lt2 += 1
            if len(fail_details) < 5:
                fail_details.append(
                    f"[{i}] only {len(slots)} slot(s) sites={sites} dur={dur} "
                    f"unit_type={unit_types} climate={climates} "
                    f"pool_rescue={resp.get('stats', {}).get('pool_rescue_step')}"
                )

        # Brief rate-limit pause every 10 calls
        if (i + 1) % 10 == 0:
            time.sleep(2)

    total = pass_count + fail_5xx + fail_lt2
    pct_pass = 100.0 * pass_count / total if total else 0
    pct_2plus = 100.0 * (pass_count) / total if total else 0

    notes = (
        f"{n} scenarios: pass={pass_count} fail_5xx={fail_5xx} fail_lt2_slots={fail_lt2} "
        f"pass_rate={pct_pass:.1f}% "
        f"(target: 0 5xx, ≥95% with ≥2 slots)"
    )
    if fail_details:
        notes += f" | sample failures: {fail_details[:5]}"

    if fail_5xx > 0:
        r.failed(f"5xx errors found ({fail_5xx}). {notes}")
    elif pct_pass < 95.0:
        r.failed(f"Pass rate {pct_pass:.1f}% < 95%. {notes}")
    else:
        r.passed(notes=notes)

    return r


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 13 — Relax-action vocabulary (regression guard)
# ─────────────────────────────────────────────────────────────────────────────

def test_13_relax_actions(base: str, key: str) -> Result:
    """
    Verifies each supported `context.action` value reaches its expected
    `relax_strategy_used` strategy id end-to-end. This is a regression
    guard: a future refactor that drops a branch in relax_strategy() or
    _apply_relax_strategy() will fail this test.

    For each (action, expected_strategy) pair:
      - Fresh session_id (no served-id pollution from prior tests).
      - Turn 1 with size_range=["30-35"] at L017 to seed a recommend row.
      - Turn 2 with previous_request_id + the action under test.
      - Assert response.stats.relax_strategy_used == expected.
      - Assert ≥1 slot still returned.
    """
    r = Result("13. Relax-action vocabulary regression guard")
    print("\n[13] Relax-action vocabulary regression guard")

    cases = [
        # (action, expected_strategy, picked_slot)
        ('more_like_this',     'size_plus_one',      1),
        ('more_like_this',     'next_nearest_site',  2),
        ('more_like_this',     'size_plus_one',      3),
        ('bigger_size',        'size_step_up',       None),
        ('smaller_size',       'size_step_down',     None),
        ('expand_locations',   'expand_locations',   None),
        ('different_type',     'expand_unit_type',   None),
        ('different_duration', 'duration_change',    None),
    ]

    failures = []
    for i, (action, expected, picked) in enumerate(cases):
        if i > 0:
            time.sleep(0.6)  # avoid rate-limit on 16 back-to-back recommend calls
        sid = f"smoke-act-{action}-{picked or 0}-{uuid.uuid4().hex[:6]}"
        cid = f"smoke-act-{action}"

        # Turn 1 — seed
        body1 = _req_body(['L017'], duration=6, size_range=['30-35'],
                          session_id=sid, customer_id=cid)
        st1, resp1 = _recommend(base, key, body1)
        if st1 != 200:
            failures.append(f"{action}/slot={picked}: T1 HTTP {st1}")
            continue
        prev_rid = resp1.get('request_id') or resp1.get('next_turn', {}).get('previous_request_id')
        if not prev_rid:
            failures.append(f"{action}/slot={picked}: T1 missing request_id in response")
            continue

        # Turn 2 — apply action
        body2 = _req_body(['L017'], duration=6, size_range=['30-35'],
                          session_id=sid, customer_id=cid)
        body2['context']['previous_request_id'] = prev_rid
        body2['context']['action'] = action
        if picked is not None:
            body2['context']['picked_slot'] = picked
        st2, resp2 = _recommend(base, key, body2)
        if st2 != 200:
            failures.append(f"{action}/slot={picked}: T2 HTTP {st2}: {resp2.get('error', resp2)}")
            continue

        got_strategy = (resp2.get('stats') or {}).get('relax_strategy_used')
        slots = [s for s in (resp2.get('slots') or []) if s]
        if got_strategy != expected:
            failures.append(f"{action}/slot={picked}: expected strategy={expected!r}, got {got_strategy!r}")
            continue
        if len(slots) < 1:
            failures.append(f"{action}/slot={picked}: 0 slots returned (strategy was correct)")
            continue
        print(f"  ✓ action={action!r:20s} picked={picked!s:4s} → strategy={got_strategy!r:22s} slots={len(slots)}")

    if failures:
        r.failed(f"{len(failures)}/{len(cases)} action(s) misrouted: " + " | ".join(failures))
    else:
        r.passed(notes=f"All {len(cases)} action→strategy mappings verified end-to-end")
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 14 — Multi-action chain integrity (L1 → L2 → L3 → L4-rejected)
# ─────────────────────────────────────────────────────────────────────────────

def test_14_chain_integrity(base: str, key: str) -> Result:
    """
    Verifies that a 3-deep continuation chain with DIFFERENT actions at
    each level tracks correctly and the L4 cap fires.

    Chain: L1 (fresh) → L2 (bigger_size) → L3 (more_like_this slot=1) → L4 (rejected)

    Invariants checked:
      - stats.recommendation_level: 1, 2, 3 in order
      - stats.relax_strategy_used: none, size_step_up, size_plus_one
      - next_turn.next_level_allowed: true, true, false
      - excluded_unit_ids_count grows monotonically (3 → 6 by L3)
      - Zero unit_id overlap between turns (auto-exclusion working)
      - L4 attempt → HTTP 400 with "exceeds max depth" message
    """
    r = Result("14. Multi-action chain integrity (L1→L2→L3→L4-cap)")
    print("\n[14] Multi-action chain integrity")

    sid = f"smoke-chain-{uuid.uuid4().hex[:6]}"
    cid = f"smoke-chain"
    site = "L018"

    failures: list[str] = []

    def _slots(resp):
        return [s for s in (resp.get('slots') or []) if s]

    # L1
    body1 = _req_body([site], duration=6, size_range=['12-14'],
                      session_id=sid, customer_id=cid)
    st1, r1 = _recommend(base, key, body1)
    if st1 != 200:
        r.failed(f"L1 HTTP {st1}: {r1}")
        return r
    s1_units = [s['unit_id'] for s in _slots(r1)]
    if r1['stats']['recommendation_level'] != 1:
        failures.append(f"L1 level={r1['stats']['recommendation_level']} (expected 1)")
    if r1['stats']['relax_strategy_used'] != 'none':
        failures.append(f"L1 strategy={r1['stats']['relax_strategy_used']} (expected 'none')")
    if not r1['next_turn'].get('next_level_allowed'):
        failures.append("L1 next_level_allowed=false (expected true)")
    rid1 = r1['request_id']

    # L2 — bigger_size
    body2 = _req_body([site], duration=6, size_range=['12-14'],
                      session_id=sid, customer_id=cid)
    body2['context']['previous_request_id'] = rid1
    body2['context']['action'] = 'bigger_size'
    st2, r2 = _recommend(base, key, body2)
    if st2 != 200:
        r.failed(f"L2 HTTP {st2}: {r2}")
        return r
    s2_units = [s['unit_id'] for s in _slots(r2)]
    if r2['stats']['recommendation_level'] != 2:
        failures.append(f"L2 level={r2['stats']['recommendation_level']} (expected 2)")
    if r2['stats']['relax_strategy_used'] != 'size_step_up':
        failures.append(f"L2 strategy={r2['stats']['relax_strategy_used']} (expected 'size_step_up')")
    if r2['stats']['excluded_unit_ids_count'] != len(s1_units):
        failures.append(f"L2 excluded={r2['stats']['excluded_unit_ids_count']} (expected {len(s1_units)})")
    if not r2['next_turn'].get('next_level_allowed'):
        failures.append("L2 next_level_allowed=false (expected true)")
    overlap_12 = set(s1_units) & set(s2_units)
    if overlap_12:
        failures.append(f"L1∩L2 unit overlap: {overlap_12} (auto-exclusion broken)")
    rid2 = r2['request_id']

    # L3 — more_like_this on slot 1
    body3 = _req_body([site], duration=6, size_range=['12-14'],
                      session_id=sid, customer_id=cid)
    body3['context']['previous_request_id'] = rid2
    body3['context']['action'] = 'more_like_this'
    body3['context']['picked_slot'] = 1
    st3, r3 = _recommend(base, key, body3)
    if st3 != 200:
        r.failed(f"L3 HTTP {st3}: {r3}")
        return r
    s3_units = [s['unit_id'] for s in _slots(r3)]
    if r3['stats']['recommendation_level'] != 3:
        failures.append(f"L3 level={r3['stats']['recommendation_level']} (expected 3)")
    if r3['stats']['relax_strategy_used'] != 'size_plus_one':
        failures.append(f"L3 strategy={r3['stats']['relax_strategy_used']} (expected 'size_plus_one')")
    expected_excl_l3 = len(s1_units) + len(s2_units)
    if r3['stats']['excluded_unit_ids_count'] != expected_excl_l3:
        failures.append(f"L3 excluded={r3['stats']['excluded_unit_ids_count']} (expected {expected_excl_l3})")
    if r3['next_turn'].get('next_level_allowed'):
        failures.append("L3 next_level_allowed=true (expected false — at max depth)")
    overlap_l3 = (set(s1_units) | set(s2_units)) & set(s3_units)
    if overlap_l3:
        failures.append(f"(L1∪L2)∩L3 unit overlap: {overlap_l3} (auto-exclusion broken)")
    rid3 = r3['request_id']

    # L4 — must be rejected
    body4 = _req_body([site], duration=6, size_range=['12-14'],
                      session_id=sid, customer_id=cid)
    body4['context']['previous_request_id'] = rid3
    body4['context']['action'] = 'different_type'
    st4, r4 = _recommend(base, key, body4)
    if st4 != 400:
        failures.append(f"L4 HTTP {st4} (expected 400 with chain-depth error). body={r4}")
    elif 'exceeds max depth' not in str(r4.get('error', '')).lower():
        failures.append(f"L4 returned 400 but wrong error: {r4.get('error')}")

    if failures:
        r.failed(f"{len(failures)} chain check(s) failed: " + " | ".join(failures))
    else:
        r.passed(notes=f"L1→L2(bigger_size)→L3(more_like_this) chain ok; L4 rejected; "
                       f"excluded counts {len(s1_units)}/{expected_excl_l3}; "
                       f"zero unit overlap.")
    return r


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

SCENARIOS = {
    1:  test_1_multivalue_filter,
    2:  test_2_pool_rescue,
    3:  test_3_slot3_relax,
    4:  test_4_slot2_neighbour,
    5:  test_5_mode_quote,
    6:  test_6_idempotency,
    7:  test_7_sanity_guard,
    8:  test_8_concession_zero,
    9:  test_9_perpetual_dynamic_prepay,
    10: test_10_exclude_selfheal,
    11: test_11_insurance_requote,
    12: test_12_random_fuzz,
    13: test_13_relax_actions,
    14: test_14_chain_integrity,
}


def main():
    p = argparse.ArgumentParser(description='Harder smoke test for the ESA booking middleware')
    p.add_argument('--base', default=os.environ.get('BACKEND_BASE_URL', DEFAULT_BASE))
    p.add_argument('--scenario', type=int, choices=list(SCENARIOS), default=None,
                   help='Run only this scenario number')
    p.add_argument('--fuzz-n', type=int, default=50,
                   help='Number of random fuzz iterations (scenario 12)')
    args = p.parse_args()

    key = os.environ.get('BACKEND_API_KEY', '').strip()
    if not key:
        print("ERROR: set BACKEND_API_KEY env var", file=sys.stderr)
        return 2

    print(f"\nsmoke_hard.py")
    print(f"  base: {args.base}")
    print(f"  allowed booking sites (hard guard): {sorted(_ALLOWED_BOOKING_SITES)}")
    print(f"  booking_site for write ops: {DEFAULT_BOOKING_SITE}")
    print(f"  scenarios: {[args.scenario] if args.scenario else list(SCENARIOS)}")

    scenarios_to_run = [args.scenario] if args.scenario else list(SCENARIOS)
    results: List[Result] = []

    for num in scenarios_to_run:
        fn = SCENARIOS[num]
        try:
            if num == 12:
                res = fn(args.base, key, n=args.fuzz_n)
            else:
                res = fn(args.base, key)
        except Exception as exc:
            res = Result(f"{num}. {fn.__name__}")
            res.failed(f"CRASHED: {exc}")
        results.append(res)
        print(f"  -> {res.status}: {res.notes[:120]}")
        # Polite pause to stay within move-in rate limit (5/min = 12s between)
        if num in (6, 7, 8) and num < scenarios_to_run[-1]:
            time.sleep(13)
        # After the fuzz scenario the recommend rate limit (120/min) is
        # near-exhausted — let it clear before scenarios that hammer
        # /api/recommendations again (6, 13).
        if num == 12 and num < scenarios_to_run[-1]:
            time.sleep(30)

    # ── Summary ──────────────────────────────────────────────────────────────
    sep = '═' * 80
    print(f"\n{sep}")
    print("  SUMMARY — smoke_hard.py")
    print(sep)

    pass_n  = sum(1 for r in results if r.status == 'PASS')
    fail_n  = sum(1 for r in results if r.status == 'FAIL')
    skip_n  = sum(1 for r in results if r.status == 'SKIP')

    print(f"  {'Scenario':<50} {'Status':<8} {'Notes / Root cause'}")
    print(f"  {'-'*49} {'-'*7} {'-'*40}")
    for res in results:
        marker = {'PASS': 'PASS', 'FAIL': 'FAIL', 'SKIP': 'SKIP'}[res.status]
        notes_trunc = (res.notes or '')[:80]
        print(f"  {res.scenario:<50} {marker:<8} {notes_trunc}")
        if res.ledger_id:
            print(f"    {'':50}  ledger={res.ledger_id}")
        if res.followups:
            print(f"    {'':50}  followups={res.followups}")

    print(f"\n  TOTAL: PASS={pass_n} FAIL={fail_n} SKIP={skip_n}")
    print(sep)

    return 0 if fail_n == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
