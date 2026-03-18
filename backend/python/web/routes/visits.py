"""
Visit session API routes — shortlist and visit workflow management.

Internal API for web UI staff (session auth).
Manages visit sessions, shortlist items, and visit outcomes.
"""

import logging
import re
from datetime import datetime, timezone

from flask import Blueprint, jsonify, request, current_app
from flask_login import current_user, login_required
from sqlalchemy.exc import IntegrityError

from web.auth.decorators import inventory_tools_access_required
from web.models.visit_session import VisitSession, VisitShortlistItem
from web.utils.rate_limit import rate_limit_api
from web.utils.audit import audit_log, AuditEvent

_UUID_PATTERN = re.compile(r'^[a-f0-9\-]{36}$', re.IGNORECASE)

logger = logging.getLogger(__name__)

visits_bp = Blueprint('visits', __name__, url_prefix='/api/visits')

VALID_FLOW_TYPES = VisitSession.VALID_FLOW_TYPES
VALID_OUTCOMES = VisitSession.VALID_OUTCOMES


# =============================================================================
# Session Routes
# =============================================================================

@visits_bp.route('', methods=['POST'])
@login_required
@inventory_tools_access_required
@rate_limit_api(max_requests=10, window_seconds=60)
def create_session():
    """Create a new visit session. One active session per user enforced."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_code = (data.get('site_code') or '').strip().upper()
    flow_type = (data.get('flow_type') or 'walk_in').strip()
    lead_id = (data.get('lead_id') or '').strip() or None

    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    if len(site_code) > 10:
        return jsonify({'error': 'site_code too long'}), 400

    if lead_id and not _UUID_PATTERN.match(lead_id):
        return jsonify({'error': 'Invalid lead_id format'}), 400

    if flow_type not in VALID_FLOW_TYPES:
        return jsonify({'error': f'flow_type must be one of: {", ".join(VALID_FLOW_TYPES)}'}), 400

    db = current_app.get_db_session()
    try:
        # Cap at 10 concurrent active sessions to prevent abuse
        active_count = db.query(VisitSession).filter_by(
            staff_user_id=current_user.id,
            status='active',
        ).count()

        if active_count >= 10:
            return jsonify({'error': 'Too many active sessions (max 10)'}), 409

        session = VisitSession(
            site_code=site_code,
            staff_user_id=current_user.id,
            flow_type=flow_type,
            lead_id=lead_id,
        )
        db.add(session)
        db.commit()

        audit_log(
            AuditEvent.VISIT_SESSION_CREATED,
            f"Created visit session: id={session.id}, site={site_code}, type={flow_type}",
        )

        return jsonify({'status': 'success', 'data': session.to_dict()}), 201

    except Exception:
        db.rollback()
        logger.exception("Failed to create visit session")
        return jsonify({'error': 'Failed to create visit session'}), 500
    finally:
        db.close()


@visits_bp.route('/active', methods=['GET'])
@login_required
@inventory_tools_access_required
@rate_limit_api(max_requests=30, window_seconds=60)
def get_active_sessions():
    """Get all of the current user's active sessions."""
    db = current_app.get_db_session()
    try:
        sessions = db.query(VisitSession).filter_by(
            staff_user_id=current_user.id,
            status='active',
        ).order_by(VisitSession.created_at.desc()).all()

        return jsonify({
            'status': 'success',
            'data': [s.to_dict() for s in sessions],
        })

    except Exception:
        logger.exception("Failed to get active visit session")
        return jsonify({'error': 'Failed to retrieve session'}), 500
    finally:
        db.close()


@visits_bp.route('/<int:session_id>', methods=['GET'])
@login_required
@inventory_tools_access_required
@rate_limit_api(max_requests=30, window_seconds=60)
def get_session(session_id):
    """Get a session with its shortlist items."""
    db = current_app.get_db_session()
    try:
        session = db.query(VisitSession).filter_by(
            id=session_id,
            staff_user_id=current_user.id,
        ).first()

        if not session:
            return jsonify({'error': 'Session not found'}), 404

        return jsonify({'status': 'success', 'data': session.to_dict()})

    except Exception:
        logger.exception("Failed to get visit session")
        return jsonify({'error': 'Failed to retrieve session'}), 500
    finally:
        db.close()


@visits_bp.route('/<int:session_id>', methods=['PATCH'])
@login_required
@inventory_tools_access_required
@rate_limit_api(max_requests=10, window_seconds=60)
def update_session(session_id):
    """Update session (link lead_id, change status)."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    db = current_app.get_db_session()
    try:
        session = db.query(VisitSession).filter_by(
            id=session_id,
            staff_user_id=current_user.id,
        ).first()

        if not session:
            return jsonify({'error': 'Session not found'}), 404

        # Update allowed fields
        if 'lead_id' in data:
            val = (data['lead_id'] or '').strip() or None
            if val and not _UUID_PATTERN.match(val):
                return jsonify({'error': 'Invalid lead_id format'}), 400
            session.lead_id = val

        if 'status' in data:
            new_status = data['status']
            if new_status not in VisitSession.VALID_STATUSES:
                return jsonify({'error': f'Invalid status: {new_status}'}), 400
            session.status = new_status
            if new_status == 'completed':
                session.completed_at = datetime.now(timezone.utc)

        session.updated_at = datetime.now(timezone.utc)
        db.commit()

        audit_log(
            AuditEvent.VISIT_SESSION_UPDATED,
            f"Updated visit session: id={session_id}, fields={list(data.keys())}",
        )

        return jsonify({'status': 'success', 'data': session.to_dict()})

    except Exception:
        db.rollback()
        logger.exception("Failed to update visit session")
        return jsonify({'error': 'Failed to update session'}), 500
    finally:
        db.close()


# =============================================================================
# Shortlist Routes
# =============================================================================

@visits_bp.route('/<int:session_id>/shortlist', methods=['POST'])
@login_required
@inventory_tools_access_required
@rate_limit_api(max_requests=30, window_seconds=60)
def add_shortlist_item(session_id):
    """Add a unit to the session shortlist."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    db = current_app.get_db_session()
    try:
        session = db.query(VisitSession).filter_by(
            id=session_id,
            staff_user_id=current_user.id,
            status='active',
        ).first()

        if not session:
            return jsonify({'error': 'Active session not found'}), 404

        site_id = data.get('site_id')
        unit_id = data.get('unit_id')

        if not site_id or not unit_id:
            return jsonify({'error': 'site_id and unit_id are required'}), 400

        # Get current max sort_order
        from sqlalchemy import func
        max_order = db.query(func.coalesce(func.max(VisitShortlistItem.sort_order), 0)).filter_by(
            session_id=session_id
        ).scalar()

        item = VisitShortlistItem(
            session_id=session_id,
            site_id=int(site_id),
            unit_id=int(unit_id),
            unit_name=(data.get('unit_name') or '').strip()[:50] or None,
            category_label=(data.get('category_label') or '').strip()[:100] or None,
            area=data.get('area'),
            floor=data.get('floor'),
            climate_code=(data.get('climate_code') or '').strip()[:5] or None,
            std_rate=data.get('std_rate'),
            indicative_rate=data.get('indicative_rate'),
            discount_plan_id=data.get('discount_plan_id'),
            concession_id=data.get('concession_id', 0),
            notes=(data.get('notes') or '').strip() or None,
            sort_order=max_order + 1,
        )
        db.add(item)
        db.commit()

        return jsonify({'status': 'success', 'data': item.to_dict()}), 201

    except IntegrityError:
        db.rollback()
        return jsonify({'error': 'Unit already in shortlist'}), 409
    except Exception:
        db.rollback()
        logger.exception("Failed to add shortlist item")
        return jsonify({'error': 'Failed to add unit to shortlist'}), 500
    finally:
        db.close()


@visits_bp.route('/<int:session_id>/shortlist/<int:item_id>', methods=['DELETE'])
@login_required
@inventory_tools_access_required
@rate_limit_api(max_requests=30, window_seconds=60)
def remove_shortlist_item(session_id, item_id):
    """Remove a unit from the shortlist."""
    db = current_app.get_db_session()
    try:
        # Verify ownership through session
        item = (
            db.query(VisitShortlistItem)
            .join(VisitSession)
            .filter(
                VisitShortlistItem.id == item_id,
                VisitShortlistItem.session_id == session_id,
                VisitSession.staff_user_id == current_user.id,
            )
            .first()
        )

        if not item:
            return jsonify({'error': 'Item not found'}), 404

        db.delete(item)
        db.commit()

        return jsonify({'status': 'success'})

    except Exception:
        db.rollback()
        logger.exception("Failed to remove shortlist item")
        return jsonify({'error': 'Failed to remove unit from shortlist'}), 500
    finally:
        db.close()


@visits_bp.route('/<int:session_id>/shortlist/<int:item_id>', methods=['PATCH'])
@login_required
@inventory_tools_access_required
@rate_limit_api(max_requests=10, window_seconds=60)
def update_shortlist_item(session_id, item_id):
    """Update shortlist item (notes, discount plan, sort order)."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    db = current_app.get_db_session()
    try:
        item = (
            db.query(VisitShortlistItem)
            .join(VisitSession)
            .filter(
                VisitShortlistItem.id == item_id,
                VisitShortlistItem.session_id == session_id,
                VisitSession.staff_user_id == current_user.id,
            )
            .first()
        )

        if not item:
            return jsonify({'error': 'Item not found'}), 404

        if 'notes' in data:
            item.notes = (data['notes'] or '').strip() or None
        if 'discount_plan_id' in data:
            item.discount_plan_id = data['discount_plan_id']
        if 'concession_id' in data:
            item.concession_id = data['concession_id']
        if 'indicative_rate' in data:
            item.indicative_rate = data['indicative_rate']
        if 'sort_order' in data:
            item.sort_order = int(data['sort_order'])

        db.commit()

        return jsonify({'status': 'success', 'data': item.to_dict()})

    except Exception:
        db.rollback()
        logger.exception("Failed to update shortlist item")
        return jsonify({'error': 'Failed to update shortlist item'}), 500
    finally:
        db.close()


@visits_bp.route('/recommend-offers', methods=['GET'])
@login_required
@inventory_tools_access_required
@rate_limit_api(max_requests=30, window_seconds=60)
def recommend_offers():
    """Recommend best discount plans for a given unit."""
    site_code = request.args.get('site_code', '').strip().upper()
    area = request.args.get('area', type=float)
    std_rate = request.args.get('std_rate', type=float)
    tenancy_months = request.args.get('tenancy_months', type=int)

    if not site_code:
        return jsonify({'error': 'site_code is required'}), 400

    if area is not None and (area <= 0 or area > 100000):
        return jsonify({'error': 'area out of valid range'}), 400
    if std_rate is not None and (std_rate <= 0 or std_rate > 1000000):
        return jsonify({'error': 'std_rate out of valid range'}), 400
    if tenancy_months is not None and (tenancy_months < 1 or tenancy_months > 120):
        return jsonify({'error': 'tenancy_months out of valid range'}), 400

    db = current_app.get_db_session()
    try:
        from web.services.offer_engine import recommend_offers as _recommend
        offers = _recommend(db, site_code, area, tenancy_months, std_rate)
        return jsonify({'status': 'success', 'data': offers})
    except Exception:
        logger.exception("Failed to get offer recommendations")
        return jsonify({'error': 'Failed to get recommendations'}), 500
    finally:
        db.close()


@visits_bp.route('/<int:session_id>/outcome', methods=['POST'])
@login_required
@inventory_tools_access_required
@rate_limit_api(max_requests=10, window_seconds=60)
def set_outcome(session_id):
    """Set visit outcome and complete the session (Phase 3 logic placeholder)."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    outcome = (data.get('outcome') or '').strip()
    if outcome not in VALID_OUTCOMES:
        return jsonify({'error': f'outcome must be one of: {", ".join(VALID_OUTCOMES)}'}), 400

    lost_reason = (data.get('lost_reason') or '').strip()
    if outcome == 'lost' and not lost_reason:
        return jsonify({'error': 'lost_reason is required when outcome is lost'}), 400
    if lost_reason and len(lost_reason) > 100:
        return jsonify({'error': 'lost_reason exceeds maximum length'}), 400

    db = current_app.get_db_session()
    try:
        session = db.query(VisitSession).filter_by(
            id=session_id,
            staff_user_id=current_user.id,
            status='active',
        ).first()

        if not session:
            return jsonify({'error': 'Active session not found'}), 404

        session.outcome = outcome
        session.outcome_notes = (data.get('outcome_notes') or '').strip()[:1000] or None
        session.lost_reason = lost_reason or None
        session.status = 'completed'
        session.completed_at = datetime.now(timezone.utc)
        session.updated_at = datetime.now(timezone.utc)

        db.commit()

        audit_log(
            AuditEvent.VISIT_SESSION_COMPLETED,
            f"Visit session completed: id={session_id}, outcome={outcome}",
        )

        return jsonify({'status': 'success', 'data': session.to_dict()})

    except Exception:
        db.rollback()
        logger.exception("Failed to set visit outcome")
        return jsonify({'error': 'Failed to set visit outcome'}), 500
    finally:
        db.close()
