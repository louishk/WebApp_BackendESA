"""
Reservation Fees — per-site reservation fee management (Revenue tool).

Two blueprints in this file:
- `reservation_fees_bp`  (session/UI, mounted at /tools/reservation-fees)
- `reservation_fees_api_bp`  (external JWT/API key, mounted at /api/reservation-fees)
"""

import logging
from decimal import Decimal, InvalidOperation

from flask import Blueprint, render_template, request, jsonify, current_app
from flask_login import login_required, current_user

from web.auth.decorators import revenue_tools_access_required
from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.audit import audit_log, AuditEvent
from web.models.reservation_fee import ReservationFee

logger = logging.getLogger(__name__)

reservation_fees_bp = Blueprint('reservation_fees', __name__, url_prefix='/reservation-fees')
reservation_fees_api_bp = Blueprint('reservation_fees_api', __name__, url_prefix='/api/reservation-fees')


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_pbi_session():
    from flask import current_app
    return current_app.get_pbi_session()


def _lookup_site(site_id):
    """Return (site_id, site_code, name) from PBI siteinfo, or None."""
    from common.models import SiteInfo
    s = _get_pbi_session()
    try:
        row = (s.query(SiteInfo.SiteID, SiteInfo.SiteCode, SiteInfo.Name)
               .filter(SiteInfo.SiteID == site_id)
               .first())
        return row
    finally:
        s.close()


def _list_sites():
    """Return list of {site_id, site_code, name} for dropdown."""
    from common.models import SiteInfo
    s = _get_pbi_session()
    try:
        rows = (s.query(SiteInfo.SiteID, SiteInfo.SiteCode, SiteInfo.Name)
                .filter(SiteInfo.SiteCode.isnot(None))
                .order_by(SiteInfo.SiteCode)
                .all())
        return [{'site_id': r[0], 'site_code': r[1], 'name': r[2]} for r in rows]
    finally:
        s.close()


def _parse_fee(value):
    """Parse fee to Decimal; raise ValueError on bad input."""
    if value is None or value == '':
        raise ValueError('reservation_fee is required')
    try:
        d = Decimal(str(value))
    except (InvalidOperation, TypeError):
        raise ValueError('reservation_fee must be a number')
    if d < 0:
        raise ValueError('reservation_fee must be >= 0')
    return d


def _query_fees(site_id=None):
    db = current_app.get_middleware_session()
    try:
        q = db.query(ReservationFee)
        if site_id is not None:
            q = q.filter(ReservationFee.site_id == site_id)
        return [f.to_dict() for f in q.order_by(ReservationFee.site_code).all()]
    finally:
        db.close()


def _create_fee(site_id, fee, actor):
    site = _lookup_site(site_id)
    if not site:
        return None, ('Site not found', 404)
    db = current_app.get_middleware_session()
    try:
        existing = db.query(ReservationFee).filter_by(site_id=site_id).first()
        if existing:
            return None, ('Reservation fee already exists for this site', 409)
        rf = ReservationFee(
            site_id=site[0],
            site_code=site[1],
            reservation_fee=fee,
            created_by=actor,
            updated_by=actor,
        )
        db.add(rf)
        db.commit()
        db.refresh(rf)
        result = rf.to_dict()
    finally:
        db.close()
    audit_log(AuditEvent.CONFIG_UPDATED,
              f"reservation_fee created site={site[1]} fee={fee}", user=actor)
    return result, None


def _update_fee(fee_id, fee, actor):
    db = current_app.get_middleware_session()
    try:
        rf = db.query(ReservationFee).filter_by(id=fee_id).first()
        if not rf:
            return None, ('Reservation fee not found', 404)
        rf.reservation_fee = fee
        rf.updated_by = actor
        db.commit()
        db.refresh(rf)
        result = rf.to_dict()
        site_code = rf.site_code
    finally:
        db.close()
    audit_log(AuditEvent.CONFIG_UPDATED,
              f"reservation_fee updated id={fee_id} site={site_code} fee={fee}", user=actor)
    return result, None


def _delete_fee(fee_id, actor):
    db = current_app.get_middleware_session()
    try:
        rf = db.query(ReservationFee).filter_by(id=fee_id).first()
        if not rf:
            return False, ('Reservation fee not found', 404)
        site_code = rf.site_code
        db.delete(rf)
        db.commit()
    finally:
        db.close()
    audit_log(AuditEvent.CONFIG_UPDATED,
              f"reservation_fee deleted id={fee_id} site={site_code}", user=actor)
    return True, None


def _actor_username():
    try:
        if current_user and current_user.is_authenticated:
            return current_user.username
    except Exception:
        pass
    from flask import g
    user = getattr(g, 'current_user', None) or {}
    return user.get('sub') or 'api'


# ---------------------------------------------------------------------------
# Session UI blueprint
# ---------------------------------------------------------------------------

@reservation_fees_bp.route('/')
@login_required
@revenue_tools_access_required
def page():
    return render_template('tools/reservation_fee.html')


@reservation_fees_bp.route('/sites')
@login_required
@revenue_tools_access_required
def page_sites():
    try:
        return jsonify({'sites': _list_sites()})
    except Exception:
        logger.exception("Failed to list sites for reservation fee tool")
        return jsonify({'error': 'Failed to load sites'}), 500


@reservation_fees_bp.route('/list')
@login_required
@revenue_tools_access_required
def page_list():
    try:
        site_id = request.args.get('site_id', type=int)
        return jsonify({'fees': _query_fees(site_id)})
    except Exception:
        logger.exception("Failed to list reservation fees")
        return jsonify({'error': 'Failed to load reservation fees'}), 500


@reservation_fees_bp.route('/create', methods=['POST'])
@login_required
@revenue_tools_access_required
def page_create():
    data = request.get_json(silent=True) or {}
    try:
        site_id = int(data.get('site_id'))
    except (TypeError, ValueError):
        return jsonify({'error': 'site_id must be an integer'}), 400
    try:
        fee = _parse_fee(data.get('reservation_fee'))
    except ValueError as ve:
        return jsonify({'error': str(ve)}), 400
    try:
        result, err = _create_fee(site_id, fee, _actor_username())
        if err:
            msg, code = err
            return jsonify({'error': msg}), code
        return jsonify({'success': True, 'fee': result}), 201
    except Exception:
        logger.exception("Failed to create reservation fee")
        return jsonify({'error': 'Failed to create reservation fee'}), 500


@reservation_fees_bp.route('/<int:fee_id>', methods=['PUT'])
@login_required
@revenue_tools_access_required
def page_update(fee_id):
    data = request.get_json(silent=True) or {}
    try:
        fee = _parse_fee(data.get('reservation_fee'))
    except ValueError as ve:
        return jsonify({'error': str(ve)}), 400
    try:
        result, err = _update_fee(fee_id, fee, _actor_username())
        if err:
            msg, code = err
            return jsonify({'error': msg}), code
        return jsonify({'success': True, 'fee': result})
    except Exception:
        logger.exception("Failed to update reservation fee")
        return jsonify({'error': 'Failed to update reservation fee'}), 500


@reservation_fees_bp.route('/<int:fee_id>', methods=['DELETE'])
@login_required
@revenue_tools_access_required
def page_delete(fee_id):
    try:
        ok, err = _delete_fee(fee_id, _actor_username())
        if err:
            msg, code = err
            return jsonify({'error': msg}), code
        return jsonify({'success': True})
    except Exception:
        logger.exception("Failed to delete reservation fee")
        return jsonify({'error': 'Failed to delete reservation fee'}), 500


# ---------------------------------------------------------------------------
# External JWT / API-key blueprint
# ---------------------------------------------------------------------------

@reservation_fees_api_bp.route('', methods=['GET'])
@reservation_fees_api_bp.route('/', methods=['GET'])
@require_auth
@require_api_scope('reservation_fees:read')
def api_list():
    try:
        site_id = request.args.get('site_id', type=int)
        return jsonify({'success': True, 'fees': _query_fees(site_id)})
    except Exception:
        logger.exception("API failed to list reservation fees")
        return jsonify({'error': 'Failed to load reservation fees'}), 500


@reservation_fees_api_bp.route('', methods=['POST'])
@reservation_fees_api_bp.route('/', methods=['POST'])
@require_auth
@require_api_scope('reservation_fees:write')
def api_create():
    data = request.get_json(silent=True) or {}
    try:
        site_id = int(data.get('site_id'))
    except (TypeError, ValueError):
        return jsonify({'error': 'site_id must be an integer'}), 400
    try:
        fee = _parse_fee(data.get('reservation_fee'))
    except ValueError as ve:
        return jsonify({'error': str(ve)}), 400
    try:
        result, err = _create_fee(site_id, fee, _actor_username())
        if err:
            msg, code = err
            return jsonify({'error': msg}), code
        return jsonify({'success': True, 'fee': result}), 201
    except Exception:
        logger.exception("API failed to create reservation fee")
        return jsonify({'error': 'Failed to create reservation fee'}), 500


@reservation_fees_api_bp.route('/<int:fee_id>', methods=['PUT'])
@require_auth
@require_api_scope('reservation_fees:write')
def api_update(fee_id):
    data = request.get_json(silent=True) or {}
    try:
        fee = _parse_fee(data.get('reservation_fee'))
    except ValueError as ve:
        return jsonify({'error': str(ve)}), 400
    try:
        result, err = _update_fee(fee_id, fee, _actor_username())
        if err:
            msg, code = err
            return jsonify({'error': msg}), code
        return jsonify({'success': True, 'fee': result})
    except Exception:
        logger.exception("API failed to update reservation fee")
        return jsonify({'error': 'Failed to update reservation fee'}), 500


@reservation_fees_api_bp.route('/<int:fee_id>', methods=['DELETE'])
@require_auth
@require_api_scope('reservation_fees:write')
def api_delete(fee_id):
    try:
        ok, err = _delete_fee(fee_id, _actor_username())
        if err:
            msg, code = err
            return jsonify({'error': msg}), code
        return jsonify({'success': True})
    except Exception:
        logger.exception("API failed to delete reservation fee")
        return jsonify({'error': 'Failed to delete reservation fee'}), 500
