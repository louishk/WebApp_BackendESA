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
    from web.services import recommender_settings

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
        # Live tunables for the form
        settings_specs = recommender_settings.list_specs()
        settings_values = recommender_settings.get_all_settings(session)
    finally:
        session.close()
    return render_template(
        'admin/recommendation_engine/index.html',
        excluded_count=excluded_count,
        unit_type_count=unit_type_count,
        candidate_count=candidate_count,
        settings_specs=settings_specs,
        settings_values=settings_values,
    )


# ---------------------------------------------------------------------------
# Simulator — admin-side recommender tester (no JWT / curl needed)
# ---------------------------------------------------------------------------

@recommendation_engine_bp.route('/simulator', methods=['GET'])
@login_required
@_require_config_permission
def simulator():
    """Fill a form, see the same envelope the chatbot would receive."""
    session = _get_session()
    try:
        site_rows = session.execute(text("""
            SELECT "SiteCode", "Country", "Name"
            FROM mw_siteinfo
            WHERE "SiteCode" IS NOT NULL
            ORDER BY "Country", "SiteCode"
        """)).fetchall()
        sites = [{'code': r[0], 'country': r[1] or '', 'name': r[2] or ''} for r in site_rows]

        ut_rows = session.execute(text(
            "SELECT code, description FROM mw_dim_unit_type ORDER BY sort_order"
        )).fetchall()
        unit_types = [{'code': r[0], 'description': r[1] or ''} for r in ut_rows]

        ct_rows = session.execute(text(
            "SELECT code, description FROM mw_dim_climate_type ORDER BY sort_order"
        )).fetchall()
        climate_types = [{'code': r[0], 'description': r[1] or ''} for r in ct_rows]

        sr_rows = session.execute(text(
            "SELECT range_code, description FROM mw_dim_size_range ORDER BY sort_order"
        )).fetchall()
        size_ranges = [{'code': r[0], 'description': r[1] or ''} for r in sr_rows]
    finally:
        session.close()
    return render_template(
        'admin/recommendation_engine/simulator.html',
        sites=sites, unit_types=unit_types,
        climate_types=climate_types, size_ranges=size_ranges,
        channels=['chatbot', 'web', 'api', 'admin'],
    )


@recommendation_engine_bp.route('/simulator/run', methods=['POST'])
@login_required
@_require_config_permission
def simulator_run():
    """Run the recommender pipeline and return the envelope.

    No JWT, no rate limit, no log_served — purely an admin debugging tool.
    Same code path as /api/recommendations but writes nothing to
    mw_recommendations_served (would pollute conversion analytics).
    """
    from decimal import Decimal
    import uuid as _uuid
    from datetime import date, datetime, timezone
    from web.services import recommender

    def _dec(v):
        if v is None:
            return None
        if isinstance(v, Decimal):
            return float(v)
        return v

    def _quote_to_json(q):
        if q is None:
            return None
        return {
            'first_month_total': _dec(q.first_month_total),
            'monthly_average': _dec(q.monthly_average),
            'total_contract': _dec(q.total_contract),
            'duration_months': q.duration_months,
            'confidence': q.confidence,
            'confidence_reason': q.confidence_reason,
            'breakdown': [{
                'month_index': b.month_index,
                'billing_date': b.billing_date.isoformat() if b.billing_date else None,
                'rent': _dec(b.rent),
                'rent_proration_factor': _dec(b.rent_proration_factor),
                'discount': _dec(b.discount),
                'insurance': _dec(b.insurance),
                'deposit': _dec(b.deposit),
                'admin_fee': _dec(b.admin_fee),
                'rent_tax': _dec(b.rent_tax),
                'insurance_tax': _dec(b.insurance_tax),
                'total': _dec(b.total),
            } for b in q.breakdown],
        }

    def _row_to_json(row, slot_num, label, quote):
        if row is None:
            return None
        dc_raw = (row.distribution_channel or '').strip()
        dc_list = [c.strip() for c in dc_raw.split(',') if c.strip()] if dc_raw else None
        return {
            'slot': slot_num,
            'label': label,
            'unit_id': row.unit_id,
            'facility': row.site_code,
            'unit_type': row.unit_type,
            'climate_type': row.climate_type,
            'size_range': row.size_range,
            'plan_id': row.plan_id,
            'plan_name': row.plan_name,
            'concession_id': row.concession_id,
            'std_rate': _dec(row.std_rate),
            'effective_rate': _dec(row.effective_rate),
            'is_hidden_rate': bool(row.hidden_rate),
            'authorised_channels': dc_list,
            'smart_lock': row.smart_lock,
            'pricing': _quote_to_json(quote),
        }

    raw = request.get_json(silent=True) or {}
    # Auto-fill required identifiers
    ctx = raw.setdefault('context', {})
    ctx.setdefault('request_id', f'sim-{_uuid.uuid4()}')
    ctx.setdefault('session_id', f'sim-{_uuid.uuid4()}')
    ctx.setdefault('customer_id', 'simulator')

    try:
        req = recommender.normalise_request(raw)
    except recommender.ValidationError as exc:
        return jsonify({'error': str(exc), 'where': 'normalise'}), 400

    db = _get_session()
    try:
        try:
            pool = recommender.fetch_candidate_pool(req, db)
        except Exception as exc:
            current_app.logger.error("simulator pool fetch failed: %s", exc, exc_info=True)
            return jsonify({'error': 'pool fetch failed', 'detail': str(exc)}), 500

        # When the pool is empty, run a diagnostic — drop each filter one at
        # a time and report how many rows we'd get. Tells admin which filter
        # is the bottleneck instead of staring at three empty slot cards.
        diagnostic = None
        if not pool:
            diagnostic = _filter_drop_diagnostic(req, db, recommender)

        slot1 = recommender.build_slot1(pool, req)
        slot2 = recommender.build_slot2(pool, req, db)
        slot3 = recommender.build_slot3(pool, req, slot1, db)

        # Distinct unit_ids
        seen = set()
        for row in (slot1, slot2, slot3):
            if row and row.unit_id in seen:
                # nullify duplicates
                if row is slot2: slot2 = None
                elif row is slot3: slot3 = None
            elif row:
                seen.add(row.unit_id)

        def _quote(row):
            if row is None:
                return None
            try:
                return recommender.quote_slot(row, req, db)
            except Exception as exc:
                current_app.logger.warning("simulator quote failed unit=%s: %s",
                                            row.unit_id, exc)
                return None

        q1 = _quote(slot1)
        q2 = _quote(slot2)
        q3 = _quote(slot3)

        return jsonify({
            'mode': req.mode,
            'level': req.level,
            'request_id': req.context['request_id'],
            'served_at': datetime.now(timezone.utc).isoformat(),
            'stats': {
                'candidates_pool_size': len(pool),
                'distinct_sites_in_pool': len({r.site_code for r in pool}),
                'distinct_plans_in_pool': len({r.plan_id for r in pool}),
                'filters_applied': {k: v for k, v in req.filters.items() if v},
            },
            'diagnostic': diagnostic,
            'slots': [
                _row_to_json(slot1, 1, 'Best Match', q1),
                _row_to_json(slot2, 2, 'Nearest Available', q2),
                _row_to_json(slot3, 3, 'Best Price', q3),
            ],
        })
    finally:
        try: db.close()
        except Exception: pass


def _filter_drop_diagnostic(req, db, recommender_module) -> dict:
    """When the simulator pool is empty, try dropping each filter one at a time
    and report the resulting pool size. The smallest filter that 'unlocks'
    matches is the bottleneck.

    Also returns a flat sample of what's actually available at the requested
    location(s) so admin can see the closest size buckets.
    """
    out: dict = {
        'message': 'Pool is empty with current filters.',
        'try_dropping': [],
        'available_at_location': [],
    }
    # Snapshot the filter-relaxed permutations
    base_filters = dict(req.filters)
    drop_keys = [k for k in ('unit_type', 'climate_type', 'size_range', 'coupon_code') if base_filters.get(k)]
    for key in drop_keys:
        relaxed = dict(base_filters)
        relaxed.pop(key, None)
        # Make a shallow-copy of req with relaxed filters
        try:
            req.filters = relaxed
            relaxed_pool = recommender_module.fetch_candidate_pool(req, db)
            out['try_dropping'].append({
                'remove': key,
                'matches': len(relaxed_pool),
            })
        finally:
            req.filters = base_filters

    # Show what's actually at the requested location (top 12 by combination)
    locations = base_filters.get('location') or []
    if locations:
        try:
            rows = db.execute(text("""
                SELECT site_code, unit_type, climate_type, size_range, COUNT(*) AS n
                FROM mw_unit_discount_candidates
                WHERE site_code = ANY(:locs)
                GROUP BY site_code, unit_type, climate_type, size_range
                ORDER BY n DESC
                LIMIT 12
            """), {'locs': locations}).fetchall()
            out['available_at_location'] = [
                {
                    'site': r[0], 'unit_type': r[1], 'climate_type': r[2],
                    'size_range': r[3], 'count': r[4],
                } for r in rows
            ]
        except Exception:
            pass

    return out


# ---------------------------------------------------------------------------
# Settings save
# ---------------------------------------------------------------------------

@recommendation_engine_bp.route('/settings/save', methods=['POST'])
@login_required
@_require_config_permission
def save_settings():
    """Persist edits from the settings form on the landing page."""
    from web.services import recommender_settings

    # Build update dict from form fields. Each setting has a known key.
    updates = {}
    for spec in recommender_settings.list_specs():
        if spec.type_ == 'bool':
            # Checkboxes only post when checked → infer FALSE from absence.
            updates[spec.key] = '1' if request.form.get(spec.key) else '0'
        else:
            raw = request.form.get(spec.key)
            if raw is not None:
                updates[spec.key] = raw

    session = _get_session()
    try:
        changed = recommender_settings.update_settings(
            updates, updated_by=getattr(current_user, 'username', 'admin'),
            db_session=session,
        )
    finally:
        session.close()

    if changed:
        audit_log(
            AuditEvent.CONFIG_UPDATED,
            f"Updated {changed} recommender setting(s)",
        )
        flash(f'Saved {changed} setting change(s).', 'success')
    else:
        flash('No changes to save.', 'info')
    return redirect(url_for('recommendation_engine.index'))


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
