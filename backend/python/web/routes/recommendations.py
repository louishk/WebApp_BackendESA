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


def _build_headlines(quote) -> Dict[str, Any]:
    """Pull the headline numbers out of breakdown[0] + post-promo month
    so the bot can answer common customer questions ("what's the deposit?",
    "what's the monthly after the promo?") without iterating the breakdown.
    """
    if not quote or not quote.breakdown:
        return {}
    first = quote.breakdown[0]
    # First month with no discount applied — used as "monthly_after_promo".
    # If only month 1 has a discount (in_month=1), month 2 is the post-promo
    # rent. Pick the earliest non-discounted month; fall back to last.
    post_promo = None
    for mb in quote.breakdown[1:]:
        if (mb.discount or 0) == 0 and (mb.deposit or 0) == 0 and (mb.admin_fee or 0) == 0:
            post_promo = mb
            break
    if post_promo is None and len(quote.breakdown) > 1:
        post_promo = quote.breakdown[-1]
    return {
        'first_month_rent': _dec_to_num(first.rent),
        'first_month_discount': _dec_to_num(first.discount),
        'first_month_insurance': _dec_to_num(first.insurance),
        'first_month_tax': _dec_to_num(
            (first.rent_tax or 0) + (first.insurance_tax or 0)
        ),
        'deposit_amount': _dec_to_num(first.deposit),
        'admin_fee_amount': _dec_to_num(first.admin_fee),
        'monthly_after_promo': _dec_to_num(post_promo.total) if post_promo else None,
    }


def _serialise_slot(
    slot_num: int,
    row: CandidateRow,
    quote,
    db_session=None,
    match_flags: Optional[Dict] = None,
) -> Dict[str, Any]:
    # Parse distribution_channel CSV → list (empty/null → null = "all channels")
    dc_raw = (row.distribution_channel or '').strip()
    dc_list: Optional[List[str]] = None
    if dc_raw:
        dc_list = [c.strip() for c in dc_raw.split(',') if c.strip()]

    # Phase 3.6 — discount summary, headlines, terms, insurance.
    discount_summary = recommender.render_discount_summary(
        amt_type=row.amt_type,
        pct_discount=row.pct_discount,
        fixed_discount=row.fixed_discount,
        in_month=row.in_month,
        max_amount_off=row.max_amount_off,
        prepay=row.prepay,
        prepaid_months=row.prepaid_months,
    )

    insurance_block: Optional[Dict[str, Any]] = None
    if db_session is not None:
        try:
            options = recommender._load_insurance_options(row.site_id, db_session)
        except Exception:
            options = []
        # Selected = the option whose premium matches the quote's
        # first_month insurance line (the calculator picks the cheapest
        # available premium today).
        selected = None
        if options and quote and quote.breakdown:
            target = float(quote.breakdown[0].insurance or 0)
            for opt in options:
                if opt.get('premium') is not None and abs(float(opt['premium']) - target) < 0.005:
                    selected = opt
                    break
        if selected is None and options:
            selected = options[0]
        insurance_block = {
            'selected': selected,
            'options': options,
            'min_required': recommender._load_insurance_minimum(
                row.site_id, row.unit_type, db_session
            ),
        }

    terms_block = {
        'lock_in_months': row.lock_in_months,
        'lock_in_period': row.lock_in_period,   # raw string fallback
        'payment_terms': row.payment_terms,
        'min_duration_months': row.min_duration_months,
        'max_duration_months': row.max_duration_months,
        'promo_valid_until': (
            row.promo_valid_until.isoformat()
            if hasattr(row.promo_valid_until, 'isoformat')
            else row.promo_valid_until
        ),
    }

    return {
        'slot': slot_num,
        'label': _SLOT_LABELS.get(slot_num, f'Slot {slot_num}'),
        'match_flags': match_flags or {},
        'unit_id': row.unit_id,
        'facility': row.site_code,
        'unit_type': row.unit_type,
        'climate_type': row.climate_type,
        'size_range': row.size_range,
        'size_sqft': _dec_to_num(row.size_sqft),
        'size_sqft_actual': _dec_to_num(row.size_sqft),  # back-compat alias
        'price': _dec_to_num(quote.first_month_total),
        'plan_id': row.plan_id,
        'plan_name': row.plan_name or None,
        'concession_id': row.concession_id,
        'concession_name': row.concession_name,
        'discount_summary': discount_summary,
        'smart_lock': row.smart_lock,
        # Authorised channels for this plan. null = open to all (the
        # recommender only emits plans the caller is authorised for, but
        # we expose the full list so 3rd parties can confirm scope).
        'authorised_channels': dc_list,
        'is_hidden_rate': bool(row.hidden_rate),
        'headlines': _build_headlines(quote),
        'terms': terms_block,
        'insurance': insurance_block,
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

    # 3. Mode dispatch — `recommendation` (3-slot) and `quote` (single unit).
    if req.mode not in ('recommendation', 'quote'):
        return jsonify({
            'error': 'mode not implemented',
            'supported': ['recommendation', 'quote'],
            'mode': req.mode,
        }), 501

    # `quote` mode requires filters.unit_id and skips slot 2/3 entirely.
    if req.mode == 'quote' and not req.filters.get('unit_id'):
        return jsonify({
            'error': 'mode=quote requires filters.unit_id',
            'field': 'filters.unit_id',
        }), 400

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
        # In quote mode there is no slot 2 / slot 3 — the bot already named
        # the exact unit it wants priced. Slot 1 is the cheapest concession
        # available for that unit (or the explicit concession_id, when given).
        if req.mode == 'quote':
            slot1_row = pool[0] if pool else None
            slot2_row = None
            slot3_row = None
        else:
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
        # When the admin setting `drop_low_confidence_quotes` is on, we
        # discard the slot when the calculator flags the quote as low-
        # confidence (free-month / multi-month-prepay edge cases the
        # calculator doesn't model exactly).
        try:
            from web.services import recommender_settings
            drop_low_conf = bool(recommender_settings.get_setting('drop_low_confidence_quotes', db))
        except Exception:
            drop_low_conf = False

        def _quote(row: Optional[CandidateRow]):
            if row is None:
                return None
            try:
                q = recommender.quote_slot(row, req, db, move_in_date=move_in_date)
                if drop_low_conf and q is not None and getattr(q, 'confidence', 'high') != 'high':
                    logger.info(
                        "dropping slot — low-confidence quote unit_id=%s reason=%s",
                        row.unit_id, getattr(q, 'confidence_reason', None),
                    )
                    return None
                return q
            except Exception as exc:
                logger.error(
                    "quote_slot failed unit_id=%s plan_id=%s request_id=%s: %s",
                    row.unit_id, row.plan_id, request_id, exc, exc_info=True,
                )
                return None

        slot1_quote = _quote(slot1_row)
        if slot1_row is not None and slot1_quote is None:
            slot1_row = None  # quote failed or dropped; treat as empty

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
            mf: Dict[str, Any] = {}
            relaxed = getattr(row, '_slot3_relaxed_dims', None)
            if relaxed:
                mf['relaxed_dims'] = list(relaxed)
            return _serialise_slot(num, row, quote, db_session=db, match_flags=mf)

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

        # Phase 3.7 — surface continuity tokens so the bot can render a
        # follow-up turn without re-deriving them.
        excluded_unit_ids = req.constraints.get('exclude_unit_ids') or []
        size_relaxed_to = req.filters.get('size_range') if relax_used in (
            'size_plus_one', 'wider_size_band'
        ) else None

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
                'relax_strategy_used': relax_used,
                'excluded_unit_ids_count': len(excluded_unit_ids),
                'size_relaxed_to': size_relaxed_to,
            },
            'slots': slots_payload,
            'next_turn': {
                'previous_request_id': request_id,
                'session_id': req.context.get('session_id'),
                'supported_actions': [
                    'more_like_this', 'different_options',
                    'different_size', 'different_site',
                ],
            },
            'pricing_note': (
                'Calculator-quoted; re-fetch '
                'GET /api/reservations/move-in/cost at booking time '
                'for SOAP-truth before charging.'
            ),
            'reserve_template': {
                'endpoint': 'POST /api/reservations/reserve',
                'required': [
                    'site_code', 'unit_id', 'concession_id',
                    'first_name', 'last_name', 'phone', 'email',
                    'needed_date',
                ],
                'recommended': ['plan_id', 'session_id', 'customer_id'],
            },
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
