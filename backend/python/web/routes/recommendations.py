"""
POST /api/recommendations — Unit recommendation HTTP endpoint.

Wires up the recommender service (web/services/recommender.py) to the Flask
request/response cycle. All ranking and DB logic lives in the service; this
module handles:
  - JSON parsing + ValidationError → 400
  - Mode/level dispatch (only 'recommendation'/'standard' in v1)
  - Hidden-rate filtering for public channels
  - Slot building + quoting
  - Response envelope construction
  - Duplicate request_id detection (409)
  - Log + commit + error recovery

Channel allowlist: ['web', 'chatbot', 'api', 'admin']
Public channels (hidden_rate filtered): web, chatbot
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, current_app, jsonify, request
from sqlalchemy.exc import IntegrityError

from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.rate_limit import rate_limit_api
from web.services import recommender
from web.services.recommender import (
    CandidateRow,
    RecommendationRequest,
    ValidationError,
)

logger = logging.getLogger(__name__)

recommendations_bp = Blueprint(
    'recommendations', __name__, url_prefix='/api/recommendations'
)

# Channels the bot/web is allowed to use
_ALLOWED_CHANNELS = {'web', 'chatbot', 'api', 'admin'}

# Channels where hidden-rate plans should be suppressed
_PUBLIC_CHANNELS = {'web', 'chatbot'}

# Slot labels (1-indexed)
_SLOT_LABELS = {1: 'Best Match', 2: 'Nearest Available', 3: 'Best Price'}


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------

def _dec_to_num(v: Any) -> Optional[float]:
    """Convert Decimal (or anything numeric) to a plain float for JSON."""
    if v is None:
        return None
    return float(v)


def _date_str(d: Any) -> Optional[str]:
    """ISO-format a date or datetime, or return None."""
    if d is None:
        return None
    if isinstance(d, (date, datetime)):
        return d.isoformat()
    return str(d)


def _serialise_breakdown(breakdown) -> List[Dict[str, Any]]:
    return [
        {
            'month_index': mb.month_index,
            'billing_date': _date_str(mb.billing_date),
            'rent': _dec_to_num(mb.rent),
            'rent_proration_factor': _dec_to_num(mb.rent_proration_factor),
            'discount': _dec_to_num(mb.discount),
            'insurance': _dec_to_num(mb.insurance),
            'deposit': _dec_to_num(mb.deposit),
            'admin_fee': _dec_to_num(mb.admin_fee),
            'rent_tax': _dec_to_num(mb.rent_tax),
            'insurance_tax': _dec_to_num(mb.insurance_tax),
            'total': _dec_to_num(mb.total),
        }
        for mb in breakdown
    ]


def _serialise_quote(quote) -> Dict[str, Any]:
    return {
        'first_month_total': _dec_to_num(quote.first_month_total),
        'monthly_average': _dec_to_num(quote.monthly_average),
        'total_contract': _dec_to_num(quote.total_contract),
        'breakdown': _serialise_breakdown(quote.breakdown),
    }


def _serialise_slot(
    slot_num: int,
    row: CandidateRow,
    quote,
    match_flags: Optional[Dict] = None,
) -> Dict[str, Any]:
    # Parse distribution_channel CSV → list (empty/null → null = "all channels")
    dc_raw = (row.distribution_channel or '').strip()
    dc_list: Optional[List[str]] = None
    if dc_raw:
        dc_list = [c.strip() for c in dc_raw.split(',') if c.strip()]
    return {
        'slot': slot_num,
        'label': _SLOT_LABELS.get(slot_num, f'Slot {slot_num}'),
        'match_flags': match_flags or {},
        'unit_id': row.unit_id,
        'facility': row.site_code,
        'unit_type': row.unit_type,
        'climate_type': row.climate_type,
        'size_range': row.size_range,
        'size_sqft_actual': None,   # dcWidth×dcLength not on candidate table in v1
        'price': _dec_to_num(quote.first_month_total),
        'plan_id': row.plan_id,
        'concession_id': row.concession_id,
        'smart_lock': row.smart_lock,
        # Authorised channels for this plan. null = open to all (the
        # recommender only emits plans the caller is authorised for, but
        # we expose the full list so 3rd parties can confirm scope).
        'authorised_channels': dc_list,
        'is_hidden_rate': bool(row.hidden_rate),
        'pricing': _serialise_quote(quote),
    }


# ---------------------------------------------------------------------------
# Route
# ---------------------------------------------------------------------------

@recommendations_bp.route('', methods=['POST'])
@require_auth
@require_api_scope('recommender')
@rate_limit_api(max_requests=120)
def recommend():
    """
    POST /api/recommendations

    Body (JSON):
      {
        "mode": "recommendation",
        "level": "standard",
        "duration_months": 6,
        "filters": {
          "location": ["L031"],
          "unit_type": ["W"],
          "climate_type": ["AD"],
          "size_range": ["30-35"]
        },
        "context": {
          "channel": "chatbot",
          "request_id": "<unique per turn>",
          "session_id": "<per conversation>",
          "customer_id": null,
          "previous_request_id": null,
          "picked_slot": null,
          "action": null
        },
        "constraints": {}
      }
    """
    raw = request.get_json(silent=True)
    if not raw:
        return jsonify({'error': 'Request body must be valid JSON', 'field': None}), 400

    # 1. Validate + normalise
    try:
        req = recommender.normalise_request(raw)
    except ValidationError as exc:
        field = getattr(exc, 'field', None)
        return jsonify({'error': str(exc), 'field': field}), 400

    # 2. Channel allowlist
    channel = req.context.get('channel', 'api')
    if channel not in _ALLOWED_CHANNELS:
        return jsonify({
            'error': 'Invalid channel',
            'field': 'context.channel',
            'allowed': sorted(_ALLOWED_CHANNELS),
            'received': channel,
        }), 400

    # 3. Mode dispatch
    if req.mode != 'recommendation':
        return jsonify({
            'error': 'mode not implemented',
            'supported': ['recommendation'],
            'mode': req.mode,
        }), 501

    # 4. Level dispatch
    if req.level != 'standard':
        return jsonify({
            'error': 'level not implemented',
            'supported': ['standard'],
            'level': req.level,
        }), 501

    # 5. Parse optional move_in_date from context
    move_in_date: Optional[date] = None
    raw_ctx = raw.get('context') or {}
    mid_raw = raw_ctx.get('move_in_date')
    if mid_raw:
        try:
            move_in_date = date.fromisoformat(str(mid_raw))
        except (ValueError, TypeError):
            return jsonify({
                'error': 'context.move_in_date must be an ISO date string (YYYY-MM-DD)',
                'field': 'context.move_in_date',
            }), 400

    request_id = req.context['request_id']
    db = current_app.get_middleware_session()
    try:
        # 6. Session resume (merges prior filters, excludes prior units)
        req = recommender.resume_session(req, db)

        relax_used = req.context.get('_relax_strategy', 'none')

        # 7. Fetch candidate pool
        pool = recommender.fetch_candidate_pool(req, db)
        total_matches_before_slotting = len(pool)

        # 8. Filter hidden-rate plans on public channels
        filter_applied: List[str] = []
        if channel in _PUBLIC_CHANNELS:
            before = len(pool)
            pool = [r for r in pool if not r.hidden_rate]
            if len(pool) < before:
                filter_applied.append('hidden_rate_suppressed')

        candidates_pool_size = len(pool)

        # 9. Build slots
        slot1_row = recommender.build_slot1(pool, req)
        slot2_row = recommender.build_slot2(pool, req, db)
        slot3_row = recommender.build_slot3(pool, req, slot1_row, db)

        # Ensure all slot unit_ids are distinct (guards against edge cases)
        slot_unit_ids: set[int] = set()
        if slot1_row is not None:
            slot_unit_ids.add(slot1_row.unit_id)
        if slot2_row is not None:
            if slot2_row.unit_id in slot_unit_ids:
                slot2_row = None
            else:
                slot_unit_ids.add(slot2_row.unit_id)
        if slot3_row is not None:
            if slot3_row.unit_id in slot_unit_ids:
                slot3_row = None

        # 10. Quote each non-None slot
        def _quote(row: Optional[CandidateRow]):
            if row is None:
                return None
            try:
                return recommender.quote_slot(row, req, db, move_in_date=move_in_date)
            except Exception as exc:
                logger.error(
                    "quote_slot failed unit_id=%s plan_id=%s request_id=%s: %s",
                    row.unit_id, row.plan_id, request_id, exc, exc_info=True,
                )
                return None

        slot1_quote = _quote(slot1_row)
        if slot1_row is not None and slot1_quote is None:
            slot1_row = None  # quote failed; treat as empty

        slot2_quote = _quote(slot2_row)
        if slot2_row is not None and slot2_quote is None:
            slot2_row = None

        slot3_quote = _quote(slot3_row)
        if slot3_row is not None and slot3_quote is None:
            slot3_row = None

        # 11. Build response envelope
        served_at = datetime.now(timezone.utc).isoformat()

        def _build_slot_obj(num: int, row, quote) -> Optional[Dict]:
            if row is None or quote is None:
                return None
            return _serialise_slot(num, row, quote)

        slots_payload = [
            _build_slot_obj(1, slot1_row, slot1_quote),
            _build_slot_obj(2, slot2_row, slot2_quote),
            _build_slot_obj(3, slot3_row, slot3_quote),
        ]

        # Build the slots_with_quotes list for log_served
        slots_with_quotes: List[Optional[Tuple[CandidateRow, Any]]] = []
        for row, quote in [
            (slot1_row, slot1_quote),
            (slot2_row, slot2_quote),
            (slot3_row, slot3_quote),
        ]:
            if row is not None and quote is not None:
                slots_with_quotes.append((row, quote))
            else:
                slots_with_quotes.append(None)

        envelope: Dict[str, Any] = {
            'mode': req.mode,
            'level': req.level,
            'request_id': request_id,
            'served_at': served_at,
            'ttl_seconds': 60,
            'stats': {
                'total_matches_before_slotting': total_matches_before_slotting,
                'candidates_pool_size': candidates_pool_size,
                'filter_applied': filter_applied,
            },
            'slots': slots_payload,
            'tracking_id': None,  # filled after log_served
        }

        # 12. Log to mw_recommendations_served
        try:
            tracking_id = recommender.log_served(
                req=req,
                slots_with_quotes=slots_with_quotes,
                pool_size=candidates_pool_size,
                total_matches=total_matches_before_slotting,
                relax_strategy_used=relax_used,
                response=envelope,
                db_session=db,
            )
            db.commit()
            envelope['tracking_id'] = tracking_id
        except IntegrityError:
            db.rollback()
            # Unique constraint on request_id — replay attack or bot bug
            logger.warning("Duplicate request_id rejected: %s", request_id)
            return jsonify({
                'error': 'Duplicate request_id — each turn must use a unique request_id',
                'request_id': request_id,
            }), 409

        return jsonify(envelope), 200

    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        logger.error(
            "Recommender failure request_id=%s: %s",
            request_id, exc, exc_info=True,
        )
        return jsonify({
            'error': 'internal recommender error',
            'request_id': request_id,
        }), 500
    finally:
        db.close()
