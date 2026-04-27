"""
Recommendation Engine admin routes.

The recommender itself (ranking logic) lives in a future phase. This module
owns the **configuration** of the candidate pool that feeds it:

- Unit Availability: globally exclude entire unit_type codes (e.g. MB, BZ,
  PR — mailboxes, biz-plus, parking) from the `mw_unit_discount_candidates`
  pipeline so they never show up as recommendation candidates.

Data path:
    admin toggles here
      → mw_recommender_excluded_unit_types
      → read on every UnitDiscountCandidatesPipeline run
      → candidates with parsed unit_type ∈ exclusions are dropped.
"""
from functools import wraps
from datetime import datetime

from flask import (
    Blueprint, current_app, flash, jsonify, redirect, render_template,
    request, url_for,
)
from flask_login import current_user, login_required
from sqlalchemy import text

from web.utils.audit import audit_log, AuditEvent


recommendation_engine_bp = Blueprint(
    'recommendation_engine', __name__, url_prefix='/recommendation-engine',
)


def _get_session():
    """Middleware session — all recommender config lives in esa_middleware."""
    return current_app.get_middleware_session()


def _require_config_permission(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user.is_authenticated:
            return redirect(url_for('auth.login'))
        if not current_user.can_manage_configs():
            flash('Config management access required.', 'error')
            return redirect(url_for('main.dashboard'))
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Landing
# ---------------------------------------------------------------------------

@recommendation_engine_bp.route('/')
@login_required
def index():
    """Landing page — lists the configuration tools in this module."""
    session = _get_session()
    try:
        excluded_count = session.execute(text(
            "SELECT COUNT(*) FROM mw_recommender_excluded_unit_types"
        )).scalar() or 0
        unit_type_count = session.execute(text(
            "SELECT COUNT(*) FROM mw_dim_unit_type"
        )).scalar() or 0
        candidate_count = session.execute(text(
            "SELECT COUNT(*) FROM mw_unit_discount_candidates"
        )).scalar() or 0
    finally:
        session.close()
    return render_template(
        'admin/recommendation_engine/index.html',
        excluded_count=excluded_count,
        unit_type_count=unit_type_count,
        candidate_count=candidate_count,
    )


# ---------------------------------------------------------------------------
# Unit availability — manage global unit_type exclusions
# ---------------------------------------------------------------------------

@recommendation_engine_bp.route('/unit-availability', methods=['GET'])
@login_required
def unit_availability():
    """Show every dim_unit_type with an include/exclude toggle."""
    session = _get_session()
    try:
        types = session.execute(text("""
            SELECT code, description, type_group, sort_order
            FROM mw_dim_unit_type
            ORDER BY sort_order
        """)).mappings().all()
        excluded_rows = session.execute(text("""
            SELECT unit_type, reason, updated_at, updated_by
            FROM mw_recommender_excluded_unit_types
        """)).mappings().all()
    finally:
        session.close()

    excluded_map = {r['unit_type']: dict(r) for r in excluded_rows}
    grouped: dict = {}
    for t in types:
        grouped.setdefault(t['type_group'], []).append({
            'code': t['code'],
            'description': t['description'],
            'excluded': t['code'] in excluded_map,
            'reason': (excluded_map.get(t['code']) or {}).get('reason') or '',
            'updated_at': (excluded_map.get(t['code']) or {}).get('updated_at'),
            'updated_by': (excluded_map.get(t['code']) or {}).get('updated_by'),
        })

    return render_template(
        'admin/recommendation_engine/unit_availability.html',
        grouped=grouped,
        total_types=len(types),
        excluded_count=len(excluded_map),
    )


@recommendation_engine_bp.route('/unit-availability/save', methods=['POST'])
@login_required
@_require_config_permission
def unit_availability_save():
    """Bulk-replace the exclusion set from the submitted checkboxes.

    Form fields:
      - excluded_unit_type (repeated) : every checked unit_type code
      - reason_<code>                 : per-code optional reason text
    """
    excluded = set(request.form.getlist('excluded_unit_type'))
    # Never accept a code we don't have in the dim table — guards against
    # POST tampering.
    session = _get_session()
    try:
        valid = {
            r[0] for r in session.execute(text(
                "SELECT code FROM mw_dim_unit_type"
            )).fetchall()
        }
        clean = sorted(excluded & valid)
        rejected = excluded - valid
        if rejected:
            current_app.logger.warning(
                f"unit_availability_save rejected unknown codes: {sorted(rejected)}"
            )

        now = datetime.utcnow()
        actor = getattr(current_user, 'username', None) or 'unknown'

        # Replace-set semantics: DELETE WHERE NOT IN(:clean) + UPSERT each.
        if clean:
            session.execute(text("""
                DELETE FROM mw_recommender_excluded_unit_types
                WHERE unit_type <> ALL(:keep)
            """), {'keep': clean})
        else:
            session.execute(text(
                "DELETE FROM mw_recommender_excluded_unit_types"
            ))

        for code in clean:
            reason = (request.form.get(f'reason_{code}', '') or '').strip() or None
            session.execute(text("""
                INSERT INTO mw_recommender_excluded_unit_types (
                    unit_type, reason, created_at, updated_at,
                    created_by, updated_by
                ) VALUES (:code, :reason, :now, :now, :actor, :actor)
                ON CONFLICT (unit_type) DO UPDATE SET
                    reason = EXCLUDED.reason,
                    updated_at = EXCLUDED.updated_at,
                    updated_by = EXCLUDED.updated_by
            """), {'code': code, 'reason': reason, 'now': now, 'actor': actor})

        session.commit()
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"unit_availability_save failed: {e}")
        flash('Could not save unit availability. Check server logs.', 'error')
        return redirect(url_for('recommendation_engine.unit_availability'))
    finally:
        session.close()

    try:
        audit_log(
            AuditEvent.CONFIG_UPDATED,
            f"Recommender unit-type exclusions set to {clean}",
        )
    except Exception:
        pass

    flash(
        f'Saved. {len(clean)} unit type(s) excluded from the recommender pool. '
        'The candidates pipeline will apply on its next run.',
        'success',
    )
    return redirect(url_for('recommendation_engine.unit_availability'))


@recommendation_engine_bp.route('/api/exclusions', methods=['GET'])
@login_required
def api_exclusions():
    """Read-only JSON export of the current exclusion set."""
    session = _get_session()
    try:
        rows = session.execute(text("""
            SELECT unit_type, reason, updated_at, updated_by
            FROM mw_recommender_excluded_unit_types
            ORDER BY unit_type
        """)).mappings().all()
        return jsonify([
            {
                'unit_type': r['unit_type'],
                'reason': r['reason'],
                'updated_at': r['updated_at'].isoformat() if r['updated_at'] else None,
                'updated_by': r['updated_by'],
            } for r in rows
        ])
    finally:
        session.close()
