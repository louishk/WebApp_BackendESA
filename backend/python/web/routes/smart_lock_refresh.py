"""
Manual per-site smart-lock refresh chain.

POST /api/smart-lock/refresh   — enqueue chain for given site_ids
GET  /api/smart-lock/refresh/<chain_id> — poll aggregated status
"""

import logging
import threading
import uuid
from datetime import datetime, timedelta

from flask import Blueprint, jsonify, request, current_app
from flask_login import current_user, login_required
from sqlalchemy import text

from web.auth.decorators import smart_lock_access_required
from web.models.inventory import SmartLockRefreshCooldown

logger = logging.getLogger(__name__)

smart_lock_refresh_bp = Blueprint('smart_lock_refresh', __name__)

CHAIN_PIPELINES = ['igloo', 'ccws_units', 'ccws_gate_access', 'igloo_pin_sync']


def _check_cooldowns(db, site_ids):
    """Return list of {site_id, available_at} for sites still on cooldown."""
    cutoff = datetime.utcnow() - timedelta(minutes=SmartLockRefreshCooldown.COOLDOWN_MINUTES)
    rows = (
        db.query(SmartLockRefreshCooldown)
        .filter(SmartLockRefreshCooldown.site_id.in_(site_ids))
        .filter(SmartLockRefreshCooldown.last_refresh_at > cutoff)
        .all()
    )
    return [
        {
            'site_id': r.site_id,
            'available_at': (
                r.last_refresh_at + timedelta(minutes=SmartLockRefreshCooldown.COOLDOWN_MINUTES)
            ).isoformat(),
        }
        for r in rows
    ]


def _upsert_cooldowns(db, site_ids, user_id, chain_id):
    """Upsert cooldown rows for all target sites."""
    now = datetime.utcnow()
    for sid in site_ids:
        existing = db.query(SmartLockRefreshCooldown).filter_by(site_id=sid).first()
        if existing:
            existing.last_refresh_at = now
            existing.last_refresh_by = user_id
            existing.last_chain_id = chain_id
            existing.updated_at = now
        else:
            db.add(SmartLockRefreshCooldown(
                site_id=sid,
                last_refresh_at=now,
                last_refresh_by=user_id,
                last_chain_id=chain_id,
                updated_at=now,
            ))
    db.commit()


def _run_chain(site_ids, chain_id, user_id):
    """Fire-and-forget: run each pipeline sequentially; failures don't abort."""
    from sync_service.executor import get_executor
    ex = get_executor()
    for pipeline in CHAIN_PIPELINES:
        scope = {'site_ids': site_ids, 'chain_id': chain_id}
        try:
            ex.run(
                pipeline_name=pipeline,
                scope=scope,
                triggered_by='manual',
                triggered_by_detail=f'user:{user_id}',
                timeout=600,
            )
        except Exception:
            logger.exception('chain %s pipeline %s failed', chain_id, pipeline)


@smart_lock_refresh_bp.route('/api/smart-lock/refresh', methods=['POST'])
@login_required
@smart_lock_access_required
def refresh():
    body = request.get_json(silent=True) or {}
    site_ids = body.get('site_ids') or []
    if not site_ids:
        return jsonify({'error': 'site_ids required'}), 400

    try:
        site_ids = [int(s) for s in site_ids]
    except (TypeError, ValueError):
        return jsonify({'error': 'site_ids must be integers'}), 400

    for sid in site_ids:
        if not current_user.can_see_site(sid):
            return jsonify({'error': 'forbidden'}), 403

    db = current_app.get_db_session()
    try:
        blocked = _check_cooldowns(db, site_ids)
        if blocked:
            return jsonify({'error': 'cooldown_active', 'blocked': blocked}), 409

        chain_id = str(uuid.uuid4())
        user_id = current_user.id
        _upsert_cooldowns(db, site_ids, user_id, chain_id)
    finally:
        db.close()

    threading.Thread(
        target=_run_chain,
        args=(site_ids, chain_id, user_id),
        daemon=True,
        name=f'smartlock-refresh-{chain_id[:8]}',
    ).start()

    return jsonify({
        'chain_id': chain_id,
        'pipelines': CHAIN_PIPELINES,
    })


@smart_lock_refresh_bp.route('/api/smart-lock/refresh/<string:chain_id>', methods=['GET'])
@login_required
@smart_lock_access_required
def refresh_status(chain_id):
    mw_session = current_app.get_middleware_session()
    try:
        rows = mw_session.execute(
            text("""
                SELECT pipeline_name, status, started_at, completed_at
                FROM mw_sync_runs
                WHERE scope->>'chain_id' = :chain_id
                ORDER BY id ASC
            """),
            {'chain_id': chain_id},
        ).fetchall()
    except Exception:
        logger.exception('Failed to query mw_sync_runs for chain %s', chain_id)
        return jsonify({'error': 'Failed to retrieve status'}), 500
    finally:
        mw_session.close()

    # Build per-pipeline status from DB rows
    pipeline_data = {p: {'name': p, 'status': 'pending', 'started_at': None, 'finished_at': None}
                     for p in CHAIN_PIPELINES}
    for row in rows:
        name, status, started_at, completed_at = row
        if name in pipeline_data:
            pipeline_data[name] = {
                'name': name,
                'status': status,
                'started_at': started_at.isoformat() if started_at else None,
                'finished_at': completed_at.isoformat() if completed_at else None,
            }

    pipelines = [pipeline_data[p] for p in CHAIN_PIPELINES]

    statuses = {p['status'] for p in pipelines}
    if statuses == {'pending'}:
        overall = 'pending'
    elif 'running' in statuses or ('completed' in statuses and 'pending' in statuses):
        overall = 'running'
    elif statuses <= {'completed'}:
        overall = 'completed'
    elif 'completed' in statuses and 'failed' in statuses:
        overall = 'partial_failure'
    elif 'failed' in statuses and 'pending' not in statuses and 'running' not in statuses:
        overall = 'failed'
    else:
        overall = 'running'

    return jsonify({
        'chain_id': chain_id,
        'status': overall,
        'pipelines': pipelines,
    })
