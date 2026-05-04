"""
ECRI (Existing Customer Rate Increase) routes.

Provides dashboard, eligibility review, batch management, and analytics
for automating self-storage rent increases.
"""

import random
from datetime import datetime, date, timedelta
from uuid import uuid4

from common.ecri_dates import (
    compute_effective_date,
    compute_advance_effective_date,
    next_lease_anniversary,
    round_new_rent,
)

from flask import Blueprint, render_template, request, jsonify, current_app
from flask_login import login_required, current_user
from sqlalchemy import func, text, desc
from sqlalchemy.orm import sessionmaker

from web.auth.decorators import (
    ecri_access_required, ecri_manage_required,
    ecri_exclusion_required, ecri_objection_required,
    ecri_objection_approve_required, ecri_finalize_required,
    ecri_execute_required, ecri_reasons_manage_required,
)

ecri_bp = Blueprint('ecri', __name__, url_prefix='/ecri')


# =============================================================================
# PBI Database Session (ECRI data lives in esa_pbi)
# =============================================================================

_pbi_engine = None
_pbi_session_factory = None


def get_pbi_session():
    """Get PBI database session for ECRI queries."""
    global _pbi_engine, _pbi_session_factory
    if _pbi_engine is None:
        from common.config_loader import get_database_url
        from sqlalchemy import create_engine
        pbi_url = get_database_url('pbi')
        _pbi_engine = create_engine(
            pbi_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=300,
        )
        _pbi_session_factory = sessionmaker(bind=_pbi_engine)
    return _pbi_session_factory()


# =============================================================================
# Page Routes (HTML)
# =============================================================================

@ecri_bp.route('/')
@login_required
@ecri_access_required
def dashboard():
    """ECRI dashboard — active batches, recent performance, data freshness."""
    return render_template('ecri/dashboard.html')


@ecri_bp.route('/eligibility')
@login_required
@ecri_access_required
def eligibility():
    """Eligible tenants list with filters and exclusion breakdown."""
    return render_template('ecri/eligibility.html')


@ecri_bp.route('/batches')
@login_required
@ecri_access_required
def batches_list():
    """Full list of ECRI batches with filters and stage-aware actions."""
    return render_template('ecri/batches.html')


@ecri_bp.route('/batch/create')
@login_required
@ecri_manage_required
def batch_create():
    """Configure new ECRI batch."""
    return render_template('ecri/batch_create.html')


@ecri_bp.route('/batch/<batch_id>/review')
@login_required
@ecri_manage_required
def batch_review(batch_id):
    """Review batch summary before execution."""
    return render_template('ecri/batch_review.html', batch_id=batch_id)


@ecri_bp.route('/objections')
@login_required
@ecri_access_required
def objections_dashboard():
    """Cross-batch objections dashboard."""
    return render_template('ecri/objections.html')


@ecri_bp.route('/batch/<batch_id>/analytics')
@login_required
@ecri_access_required
def batch_analytics(batch_id):
    """Outcomes, control group comparison, revenue impact."""
    return render_template('ecri/analytics.html', batch_id=batch_id)


def get_app_db_session():
    """Get esa_backend (app) DB session — for reasons tables, users, roles."""
    return current_app.get_db_session()


# =============================================================================
# API: Data Freshness
# =============================================================================

@ecri_bp.route('/api/data-freshness')
@login_required
@ecri_access_required
def api_data_freshness():
    """Check freshness of key tables used by ECRI."""
    session = get_pbi_session()
    try:
        tables = {
            'ccws_ledgers': 'extract_date',
            'rentroll_enriched': 'extract_date',
            'rentroll': 'extract_date',
            'fx_rates': 'rate_date',
        }
        freshness = {}
        for table, col in tables.items():
            try:
                result = session.execute(
                    text(f'SELECT MAX("{col}") FROM "{table}"')
                ).scalar()
                freshness[table] = {
                    'latest_date': result.isoformat() if result else None,
                    'stale': (date.today() - result).days > 7 if result else True
                }
            except Exception as e:
                current_app.logger.error(f"ECRI freshness query error for {table}: {e}")
                freshness[table] = {'latest_date': None, 'stale': True, 'error': 'Query failed'}

        return jsonify({'freshness': freshness})
    finally:
        session.close()


# =============================================================================
# API: Eligibility
# =============================================================================

@ecri_bp.route('/api/eligible-tenants')
@login_required
@ecri_access_required
def api_eligible_tenants():
    """
    Returns eligible tenant list with benchmarks.

    Query params:
    - site_ids: comma-separated site IDs (required)
    - min_tenure_months: override default 12
    - sched_out_exclusion_days: override default 30
    """
    site_ids_param = request.args.get('site_ids', '')
    if not site_ids_param:
        return jsonify({'error': 'site_ids parameter required'}), 400

    try:
        site_ids = [int(s.strip()) for s in site_ids_param.split(',')]
    except ValueError:
        return jsonify({'error': 'site_ids must be comma-separated integers'}), 400

    min_tenure = int(request.args.get('min_tenure_months', 12))
    sched_out_days = int(request.args.get('sched_out_exclusion_days', 30))
    discount_ref_pct = float(request.args.get('discount_reference_pct', 40))

    session = get_pbi_session()
    try:
        today = date.today()
        tenure_cutoff = today - timedelta(days=min_tenure * 30)
        sched_out_cutoff = today + timedelta(days=sched_out_days)

        # Build the eligibility SQL (Steps 2-4 from the spec).
        # vw_ecri_eligible_ledgers = ccws_ledgers (active tenants, live pipeline)
        # joined to latest rentroll_enriched. See migration 030.
        eligibility_sql = text("""
            SELECT
                l."SiteID",
                l."LedgerID",
                l."UnitID",
                l."TenantName" AS tenant_name,
                l."dMovedIn",
                l."dMovedOut",
                l."dSchedOut",
                l."dRentLastChanged",
                l."dSchedRentStrt",
                l."dcRent" AS current_rent,
                l."dcSchedRent",
                l."bExcludeFromRevenueMgmt",
                l."TenantID",
                l."sUnit" AS unit_name,
                l."sTypeName" AS unit_type,
                l."dcStdRate" AS std_rate
            FROM vw_ecri_eligible_ledgers l
            WHERE l."SiteID" = ANY(:site_ids)
              -- Step 2: Active only (ccws_ledgers is active-only by design;
              --         dMovedIn check kept as a safety net)
              AND l."dMovedIn" IS NOT NULL
              -- Step 3: Not scheduled out within exclusion window
              AND (l."dSchedOut" IS NULL OR l."dSchedOut" > :sched_out_cutoff)
              -- Step 4: No pending increase
              AND (l."dSchedRentStrt" IS NULL OR l."dSchedRentStrt" < CURRENT_DATE)
              -- Step 4: Last increase 12+ months ago
              AND (COALESCE(l."dRentLastChanged", l."dMovedIn") <= :tenure_cutoff)
            ORDER BY l."SiteID", l."LedgerID"
        """)

        result = session.execute(eligibility_sql, {
            'site_ids': site_ids,
            'sched_out_cutoff': sched_out_cutoff,
            'tenure_cutoff': tenure_cutoff,
        })

        rows = result.fetchall()
        columns = result.keys()

        # Build eligible tenants list with benchmarking
        eligible = []
        # Group by site+unit_type for in-place median calculation
        site_type_rents = {}

        for row in rows:
            r = dict(zip(columns, row))
            current_rent = float(r['current_rent']) if r['current_rent'] else 0
            site_id = r['SiteID']
            unit_type = r['unit_type'] or 'Unknown'

            key = (site_id, unit_type)
            if key not in site_type_rents:
                site_type_rents[key] = []
            site_type_rents[key].append(current_rent)

        # Calculate medians
        def median(values):
            if not values:
                return None
            s = sorted(values)
            n = len(s)
            if n % 2 == 0:
                return (s[n // 2 - 1] + s[n // 2]) / 2
            return s[n // 2]

        site_type_medians = {k: median(v) for k, v in site_type_rents.items()}

        # Build final list
        for row in rows:
            r = dict(zip(columns, row))
            current_rent = float(r['current_rent']) if r['current_rent'] else 0
            std_rate = float(r['std_rate']) if r['std_rate'] else None
            site_id = r['SiteID']
            unit_type = r['unit_type'] or 'Unknown'
            moved_in = r['dMovedIn']

            # In-place benchmark
            in_place_median = site_type_medians.get((site_id, unit_type))
            variance_vs_site = None
            if in_place_median and current_rent and in_place_median > 0:
                variance_vs_site = round((current_rent - in_place_median) / in_place_median * 100, 1)

            # Market rate benchmark
            market_rate = None
            variance_vs_market = None
            if std_rate and std_rate > 0:
                market_rate = round(float(std_rate) * (1 - discount_ref_pct / 100), 2)
                if current_rent and market_rate > 0:
                    variance_vs_market = round((current_rent - market_rate) / market_rate * 100, 1)

            # Tenure calculation
            tenure_months = None
            if moved_in:
                tenure_months = (today.year - moved_in.year) * 12 + (today.month - moved_in.month)

            eligible.append({
                'site_id': site_id,
                'ledger_id': r['LedgerID'],
                'tenant_id': r['TenantID'],
                'unit_id': r['UnitID'],
                'unit_name': r['unit_name'],
                'unit_type': unit_type,
                'tenant_name': r['tenant_name'],
                'current_rent': current_rent,
                'std_rate': float(std_rate) if std_rate else None,
                'moved_in_date': moved_in.isoformat() if moved_in else None,
                'last_increase_date': r['dRentLastChanged'].isoformat() if r['dRentLastChanged'] else None,
                'tenure_months': tenure_months,
                'in_place_median_site': round(in_place_median, 2) if in_place_median else None,
                'market_rate': market_rate,
                'variance_vs_site': variance_vs_site,
                'variance_vs_market': variance_vs_market,
            })

        # Exclusion summary (run a separate query for counts).
        # Source = vw_ecri_eligible_ledgers which is ccws_ledgers (active-only),
        # so the "inactive" counter will always be 0.
        exclusion_sql = text("""
            SELECT
                COUNT(*) FILTER (WHERE "dMovedIn" IS NULL) AS excluded_inactive,
                COUNT(*) FILTER (WHERE "dSchedOut" IS NOT NULL AND "dSchedOut" <= :sched_out_cutoff
                    AND "dMovedIn" IS NOT NULL) AS excluded_sched_out,
                COUNT(*) FILTER (WHERE "dSchedRentStrt" IS NOT NULL AND "dSchedRentStrt" >= CURRENT_DATE
                    AND "dMovedIn" IS NOT NULL) AS excluded_pending_increase,
                COUNT(*) FILTER (WHERE COALESCE("dRentLastChanged", "dMovedIn") > :tenure_cutoff
                    AND "dMovedIn" IS NOT NULL
                    AND ("dSchedRentStrt" IS NULL OR "dSchedRentStrt" < CURRENT_DATE)) AS excluded_recent_increase,
                0 AS excluded_rev_mgmt,
                COUNT(*) AS total_ledgers
            FROM vw_ecri_eligible_ledgers
            WHERE "SiteID" = ANY(:site_ids)
        """)

        excl_result = session.execute(exclusion_sql, {
            'site_ids': site_ids,
            'sched_out_cutoff': sched_out_cutoff,
            'tenure_cutoff': tenure_cutoff,
        }).fetchone()

        exclusion_summary = {
            'total_ledgers': excl_result[5] if excl_result else 0,
            'excluded_inactive': excl_result[0] if excl_result else 0,
            'excluded_sched_out': excl_result[1] if excl_result else 0,
            'excluded_pending_increase': excl_result[2] if excl_result else 0,
            'excluded_recent_increase': excl_result[3] if excl_result else 0,
            'excluded_rev_mgmt': excl_result[4] if excl_result else 0,
            'eligible': len(eligible),
        }

        return jsonify({
            'eligible': eligible,
            'exclusion_summary': exclusion_summary,
            'parameters': {
                'site_ids': site_ids,
                'min_tenure_months': min_tenure,
                'sched_out_exclusion_days': sched_out_days,
                'discount_reference_pct': discount_ref_pct,
            }
        })

    except Exception as e:
        current_app.logger.error(f"ECRI eligibility error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


# =============================================================================
# API: Batches
# =============================================================================

@ecri_bp.route('/api/batches')
@login_required
@ecri_access_required
def api_list_batches():
    """List all ECRI batches."""
    from common.models import ECRIBatch

    session = get_pbi_session()
    try:
        batches = session.query(ECRIBatch).order_by(desc(ECRIBatch.created_at)).all()
        return jsonify({
            'batches': [b.to_dict() for b in batches]
        })
    finally:
        session.close()


@ecri_bp.route('/api/batch', methods=['POST'])
@login_required
@ecri_manage_required
def api_create_batch():
    """Create a new ECRI batch from eligible tenant list."""
    from common.models import ECRIBatch, ECRIBatchLedger

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_ids = data.get('site_ids', [])
    target_pct = data.get('target_increase_pct')
    control_enabled = data.get('control_group_enabled', False)
    group_config = data.get('group_config')
    ledgers = data.get('ledgers', [])
    name = data.get('name', '')

    if not site_ids:
        return jsonify({'error': 'site_ids required'}), 400
    if not ledgers:
        return jsonify({'error': 'ledgers list required'}), 400
    if target_pct is None and not control_enabled:
        return jsonify({'error': 'target_increase_pct required when control groups disabled'}), 400

    # Configuration params
    min_tenure = data.get('min_tenure_months', 12)
    notice_days = data.get('notice_period_days', 14)
    discount_ref = data.get('discount_reference_pct', 40.0)
    attribution_days = data.get('attribution_window_days', 90)

    session = get_pbi_session()
    try:
        batch_id = uuid4()
        today = date.today()

        # Pre-fetch dPaidThru and dAnniv for all (site_id, ledger_id) pairs
        # from the view so we can compute billing-cycle-aware effective dates.
        site_ledger_pairs = [(int(l['site_id']), int(l['ledger_id'])) for l in ledgers]
        anniv_map = {}  # (site_id, ledger_id) -> (paid_thru: date|None, anniv: date|None)
        if site_ledger_pairs:
            all_site_ids = list({p[0] for p in site_ledger_pairs})
            rows = session.execute(text(
                'SELECT "SiteID", "LedgerID", "dPaidThru", "dAnniv" '
                'FROM vw_ecri_eligible_ledgers '
                'WHERE "SiteID" = ANY(:site_ids)'
            ), {'site_ids': all_site_ids}).fetchall()
            wanted = set(site_ledger_pairs)
            for r in rows:
                key = (int(r[0]), int(r[1]))
                if key in wanted:
                    pt = r[2].date() if r[2] is not None else None
                    an = r[3].date() if r[3] is not None else None
                    anniv_map[key] = (pt, an)

        batch = ECRIBatch(
            batch_id=batch_id,
            name=name or f"ECRI Batch {today.isoformat()}",
            site_ids=site_ids,
            target_increase_pct=target_pct,
            control_group_enabled=control_enabled,
            group_config=group_config,
            total_ledgers=len(ledgers),
            status='draft',
            created_by=current_user.username if current_user.is_authenticated else None,
            min_tenure_months=min_tenure,
            notice_period_days=notice_days,
            discount_reference_pct=discount_ref,
            attribution_window_days=attribution_days,
            notes=data.get('notes', ''),
        )
        session.add(batch)

        # Determine group assignments if control groups enabled
        group_percentages = [target_pct] if not control_enabled else (group_config or {}).get('percentages', [0, target_pct])
        num_groups = len(group_percentages) if control_enabled else 1

        # Shuffle for random assignment
        indices = list(range(len(ledgers)))
        if control_enabled:
            random.shuffle(indices)

        for i, idx in enumerate(indices):
            led = ledgers[idx]
            group_idx = i % num_groups if control_enabled else 0
            pct = group_percentages[group_idx] if control_enabled else target_pct
            old_rent = float(led['current_rent'])
            currency = led.get('currency', 'SGD') or 'SGD'
            raw_new = old_rent * (1 + pct / 100)
            from decimal import Decimal
            new_rent = float(round_new_rent(Decimal(str(raw_new)), currency))
            increase_amt = round(new_rent - old_rent, 2)

            paid_thru, anniv = anniv_map.get((int(led['site_id']), int(led['ledger_id'])), (None, None))
            effective_date, notice_date, bucket = compute_effective_date(
                anniv, paid_thru, today, notice_days
            )

            batch_ledger = ECRIBatchLedger(
                batch_id=batch_id,
                site_id=led['site_id'],
                ledger_id=led['ledger_id'],
                tenant_id=led['tenant_id'],
                unit_id=led.get('unit_id'),
                unit_name=led.get('unit_name'),
                tenant_name=led.get('tenant_name'),
                control_group=group_idx,
                old_rent=old_rent,
                new_rent=new_rent,
                increase_pct=pct,
                increase_amt=increase_amt,
                planned_new_rent=new_rent,
                planned_increase_pct=pct,
                planned_increase_amt=increase_amt,
                notice_date=notice_date,
                effective_date=effective_date,
                paid_thru_date=paid_thru,
                next_lad=next_lease_anniversary(anniv, today) if anniv else None,
                bucket=bucket,
                in_place_median_site=led.get('in_place_median_site'),
                in_place_median_country=led.get('in_place_median_country'),
                market_rate=led.get('market_rate'),
                std_rate=led.get('std_rate'),
                variance_vs_site=led.get('variance_vs_site'),
                variance_vs_market=led.get('variance_vs_market'),
                moved_in_date=led.get('moved_in_date'),
                last_increase_date=led.get('last_increase_date'),
                tenure_months=led.get('tenure_months'),
                api_status='pending',
            )
            session.add(batch_ledger)

        session.commit()

        return jsonify({
            'success': True,
            'batch_id': str(batch_id),
            'total_ledgers': len(ledgers),
            'status': 'draft',
        })

    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI batch creation error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


# =============================================================================
# API: Advance-Scheduling ("Pre-Load Batch")
#
# Targets tenants the standard monthly flow would miss — recent move-ins whose
# move-in discount is about to expire (triggering a big bump), and heavy
# prepayers whose dPaidThru pushes their next LAD past the normal ECRI window.
# Effective dates are computed to land AFTER the projected paid_thru so
# SiteLink does not snap the change forward to the next free billing cycle.
# =============================================================================

@ecri_bp.route('/api/advance-eligible')
@login_required
@ecri_access_required
def api_advance_eligible():
    """Return advance-scheduling eligible tenants grouped by segment.

    Query params:
      - site_ids (required): comma-separated site IDs
      - discount_reference_pct (optional): default 40
    """
    site_ids_param = request.args.get('site_ids', '')
    if not site_ids_param:
        return jsonify({'error': 'site_ids parameter required'}), 400

    try:
        site_ids = [int(s.strip()) for s in site_ids_param.split(',')]
    except ValueError:
        return jsonify({'error': 'site_ids must be comma-separated integers'}), 400

    discount_ref_pct = float(request.args.get('discount_reference_pct', 40))

    session = get_pbi_session()
    try:
        today = date.today()

        rows = session.execute(text("""
            SELECT
                "SiteID", "LedgerID", "TenantID", "UnitID",
                "TenantName", unit_name, unit_type,
                "dMovedIn", paid_thru, "dAnniv",
                current_rent, std_rate,
                "dRentLastChanged",
                segment, discount_expires, projected_paid_thru
            FROM vw_ecri_advance_eligible_ledgers
            WHERE "SiteID" = ANY(:site_ids)
            ORDER BY segment, "SiteID", "LedgerID"
        """), {'site_ids': site_ids}).fetchall()

        # Median site rent by unit_type (for variance_vs_site benchmark)
        site_type_rents: dict[tuple[int, str], list[float]] = {}
        for r in rows:
            cr = float(r.current_rent) if r.current_rent else 0
            if cr:
                site_type_rents.setdefault(
                    (r.SiteID, r.unit_type or 'Unknown'), []
                ).append(cr)

        def median(values):
            if not values:
                return None
            s = sorted(values)
            n = len(s)
            if n % 2 == 0:
                return (s[n // 2 - 1] + s[n // 2]) / 2
            return s[n // 2]

        medians = {k: median(v) for k, v in site_type_rents.items()}

        segments: dict[str, list[dict]] = {
            'recent_movein': [],
            'heavy_prepayer': [],
        }

        for r in rows:
            cr = float(r.current_rent) if r.current_rent else 0
            std = float(r.std_rate) if r.std_rate else None
            unit_type = r.unit_type or 'Unknown'
            moved_in = r.dMovedIn

            ipm = medians.get((r.SiteID, unit_type))
            var_site = None
            if ipm and cr and ipm > 0:
                var_site = round((cr - ipm) / ipm * 100, 1)

            market_rate = None
            var_market = None
            if std and std > 0:
                market_rate = round(std * (1 - discount_ref_pct / 100), 2)
                if cr and market_rate > 0:
                    var_market = round((cr - market_rate) / market_rate * 100, 1)

            tenure_months = None
            if moved_in:
                tenure_months = (today.year - moved_in.year) * 12 + (today.month - moved_in.month)

            entry = {
                'site_id': r.SiteID,
                'ledger_id': r.LedgerID,
                'tenant_id': r.TenantID,
                'unit_id': r.UnitID,
                'unit_name': r.unit_name,
                'unit_type': unit_type,
                'tenant_name': r.TenantName,
                'current_rent': cr,
                'std_rate': std,
                'moved_in_date': moved_in.isoformat() if moved_in else None,
                'last_increase_date': r.dRentLastChanged.isoformat() if r.dRentLastChanged else None,
                'tenure_months': tenure_months,
                'in_place_median_site': round(ipm, 2) if ipm else None,
                'market_rate': market_rate,
                'variance_vs_site': var_site,
                'variance_vs_market': var_market,
                'paid_thru': r.paid_thru.isoformat() if r.paid_thru else None,
                'projected_paid_thru': r.projected_paid_thru.isoformat() if r.projected_paid_thru else None,
                'discount_expires': r.discount_expires.isoformat() if r.discount_expires else None,
                'segment': r.segment,
            }
            segments[r.segment].append(entry)

        return jsonify({
            'segments': segments,
            'counts': {k: len(v) for k, v in segments.items()},
            'parameters': {
                'site_ids': site_ids,
                'discount_reference_pct': discount_ref_pct,
            },
        })

    except Exception as e:
        current_app.logger.error(f"ECRI advance-eligibility error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


@ecri_bp.route('/api/advance-batch', methods=['POST'])
@login_required
@ecri_manage_required
def api_create_advance_batch():
    """Create a Pre-Load Batch (batch_type='advance').

    Same payload shape as POST /ecri/api/batch, with one extra per-ledger
    field required: ``projected_paid_thru`` (ISO date). Optional per-ledger
    fields: ``segment``, ``discount_expires``. Control groups are not used
    on advance batches.
    """
    from common.models import ECRIBatch, ECRIBatchLedger
    from decimal import Decimal

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body required'}), 400

    site_ids = data.get('site_ids', [])
    target_pct = data.get('target_increase_pct')
    ledgers = data.get('ledgers', [])
    name = data.get('name', '')

    if not site_ids:
        return jsonify({'error': 'site_ids required'}), 400
    if not ledgers:
        return jsonify({'error': 'ledgers list required'}), 400
    if target_pct is None:
        return jsonify({'error': 'target_increase_pct required'}), 400

    notice_days = data.get('notice_period_days', 14)
    discount_ref = data.get('discount_reference_pct', 40.0)
    attribution_days = data.get('attribution_window_days', 90)
    prepay_buffer = int(data.get('prepay_buffer_days', 7))

    # Dedupe: a ledger in both segments should appear once. Prefer recent_movein
    # because it's the narrower, more actionable signal.
    seg_priority = {'recent_movein': 0, 'heavy_prepayer': 1}
    dedup: dict[tuple[int, int], dict] = {}
    for led in ledgers:
        key = (int(led['site_id']), int(led['ledger_id']))
        existing = dedup.get(key)
        new_seg_rank = seg_priority.get(led.get('segment'), 99)
        old_seg_rank = seg_priority.get(existing['segment'], 99) if existing else 99
        if existing is None or new_seg_rank < old_seg_rank:
            dedup[key] = led
    ledgers = list(dedup.values())

    session = get_pbi_session()
    try:
        batch_id = uuid4()
        today = date.today()

        # Fetch anniv for each ledger (projected_paid_thru already on each entry)
        site_ledger_pairs = [(int(l['site_id']), int(l['ledger_id'])) for l in ledgers]
        anniv_map: dict[tuple[int, int], date | None] = {}
        if site_ledger_pairs:
            all_site_ids = list({p[0] for p in site_ledger_pairs})
            rows = session.execute(text(
                'SELECT "SiteID", "LedgerID", "dAnniv" '
                'FROM vw_ecri_eligible_ledgers '
                'WHERE "SiteID" = ANY(:site_ids)'
            ), {'site_ids': all_site_ids}).fetchall()
            wanted = set(site_ledger_pairs)
            for r in rows:
                key = (int(r[0]), int(r[1]))
                if key in wanted:
                    anniv_map[key] = r[2].date() if r[2] is not None else None

        batch = ECRIBatch(
            batch_id=batch_id,
            name=name or f"ECRI Pre-Load {today.isoformat()}",
            batch_type='advance',
            site_ids=site_ids,
            target_increase_pct=target_pct,
            control_group_enabled=False,
            group_config=None,
            total_ledgers=len(ledgers),
            status='draft',
            created_by=current_user.username if current_user.is_authenticated else None,
            min_tenure_months=0,  # advance batches intentionally include <12mo tenure
            notice_period_days=notice_days,
            discount_reference_pct=discount_ref,
            attribution_window_days=attribution_days,
            notes=data.get('notes', ''),
        )
        session.add(batch)

        for led in ledgers:
            old_rent = float(led['current_rent'])
            currency = led.get('currency', 'SGD') or 'SGD'
            raw_new = old_rent * (1 + target_pct / 100)
            new_rent = float(round_new_rent(Decimal(str(raw_new)), currency))
            increase_amt = round(new_rent - old_rent, 2)

            ppt_raw = led.get('projected_paid_thru')
            projected_pt: date | None = None
            if ppt_raw:
                try:
                    projected_pt = date.fromisoformat(ppt_raw[:10])
                except ValueError:
                    projected_pt = None

            disc_exp_raw = led.get('discount_expires')
            disc_exp: date | None = None
            if disc_exp_raw:
                try:
                    disc_exp = date.fromisoformat(disc_exp_raw[:10])
                except ValueError:
                    disc_exp = None

            moved_in_raw = led.get('moved_in_date')
            moved_in_date: date | None = None
            if moved_in_raw:
                try:
                    moved_in_date = date.fromisoformat(moved_in_raw[:10])
                except ValueError:
                    moved_in_date = None

            anniv = anniv_map.get((int(led['site_id']), int(led['ledger_id'])))
            effective_date, notice_date, bucket = compute_advance_effective_date(
                anniv, projected_pt, today,
                notice_days=notice_days,
                prepay_buffer_days=prepay_buffer,
            )

            batch_ledger = ECRIBatchLedger(
                batch_id=batch_id,
                site_id=led['site_id'],
                ledger_id=led['ledger_id'],
                tenant_id=led['tenant_id'],
                unit_id=led.get('unit_id'),
                unit_name=led.get('unit_name'),
                tenant_name=led.get('tenant_name'),
                control_group=0,
                old_rent=old_rent,
                new_rent=new_rent,
                increase_pct=target_pct,
                increase_amt=increase_amt,
                planned_new_rent=new_rent,
                planned_increase_pct=target_pct,
                planned_increase_amt=increase_amt,
                currency=currency,
                notice_date=notice_date,
                effective_date=effective_date,
                paid_thru_date=projected_pt,
                next_lad=next_lease_anniversary(anniv, today) if anniv else None,
                bucket=bucket,
                segment=led.get('segment'),
                projected_paid_thru=projected_pt,
                discount_expires=disc_exp,
                in_place_median_site=led.get('in_place_median_site'),
                market_rate=led.get('market_rate'),
                std_rate=led.get('std_rate'),
                variance_vs_site=led.get('variance_vs_site'),
                variance_vs_market=led.get('variance_vs_market'),
                moved_in_date=moved_in_date,
                tenure_months=led.get('tenure_months'),
                api_status='pending',
            )
            session.add(batch_ledger)

        session.commit()

        return jsonify({
            'success': True,
            'batch_id': str(batch_id),
            'total_ledgers': len(ledgers),
            'status': 'draft',
            'batch_type': 'advance',
        })

    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI advance-batch creation error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


@ecri_bp.route('/api/batch/<batch_id>')
@login_required
@ecri_access_required
def api_get_batch(batch_id):
    """Get batch details with ledger list. All monetary values are
    returned both in native currency and normalized to SGD using the
    latest fx_rates row (1 SGD = rate target_currency)."""
    from common.models import ECRIBatch, ECRIBatchLedger

    session = get_pbi_session()
    try:
        batch = session.query(ECRIBatch).filter_by(batch_id=batch_id).first()
        if not batch:
            return jsonify({'error': 'Batch not found'}), 404

        ledgers = session.query(ECRIBatchLedger).filter_by(batch_id=batch_id).order_by(
            ECRIBatchLedger.site_id, ECRIBatchLedger.ledger_id
        ).all()

        # Auto-promote to 'executed' when all ledgers have been processed.
        # Catches partial-push batches that never hit the full-batch flip,
        # and manually patched rows that bypassed the worker.
        if batch.status == 'draft' and ledgers:
            pending_count = sum(1 for l in ledgers if l.api_status == 'pending')
            if pending_count == 0:
                batch.status = 'executed'
                batch.executed_at = datetime.utcnow()
                session.commit()

        # Load latest FX rates (base SGD). rate = "1 SGD = X target_currency".
        # To convert native → SGD: native / rate. SGD itself is not in the
        # table; we hardcode rate=1.
        fx_rows = session.execute(text(
            "SELECT target_currency, rate FROM fx_rates "
            "WHERE rate_date = (SELECT MAX(rate_date) FROM fx_rates)"
        )).fetchall()
        fx_map = {row[0]: float(row[1]) for row in fx_rows}
        fx_map['SGD'] = 1.0
        fx_date = session.execute(text("SELECT MAX(rate_date) FROM fx_rates")).scalar()

        def to_sgd(amount, currency):
            if amount is None:
                return None
            cur = currency or 'SGD'
            rate = fx_map.get(cur)
            if not rate:
                return None
            return round(float(amount) / rate, 2)

        batch_dict = batch.to_dict()

        ledger_dicts = []
        total_increase_sgd = 0.0
        by_group = {}
        for l in ledgers:
            ld = l.to_dict()
            ld['old_rent_sgd'] = to_sgd(l.old_rent, l.currency)
            ld['new_rent_sgd'] = to_sgd(l.new_rent, l.currency)
            ld['increase_amt_sgd'] = to_sgd(l.increase_amt, l.currency)
            ledger_dicts.append(ld)

            inc_sgd = ld['increase_amt_sgd'] or 0.0
            total_increase_sgd += inc_sgd

            g = l.control_group
            if g not in by_group:
                by_group[g] = {'count': 0, 'total_increase_sgd': 0.0, 'pcts': []}
            by_group[g]['count'] += 1
            by_group[g]['total_increase_sgd'] += inc_sgd
            by_group[g]['pcts'].append(float(l.increase_pct))

        for g in by_group:
            pcts = by_group[g]['pcts']
            by_group[g]['avg_pct'] = round(sum(pcts) / len(pcts), 1) if pcts else 0
            by_group[g]['total_increase_sgd'] = round(by_group[g]['total_increase_sgd'], 2)
            # Legacy alias for templates still reading `total_increase`
            by_group[g]['total_increase'] = by_group[g]['total_increase_sgd']
            del by_group[g]['pcts']

        batch_dict['ledgers'] = ledger_dicts
        batch_dict['fx'] = {
            'base': 'SGD',
            'rate_date': fx_date.isoformat() if fx_date else None,
            'rates': fx_map,
        }
        batch_dict['summary'] = {
            'total_monthly_increase': round(total_increase_sgd, 2),
            'total_monthly_increase_sgd': round(total_increase_sgd, 2),
            'total_annual_increase': round(total_increase_sgd * 12, 2),
            'total_annual_increase_sgd': round(total_increase_sgd * 12, 2),
            'groups': by_group,
            'api_status_counts': {
                'pending': sum(1 for l in ledgers if l.api_status == 'pending'),
                'success': sum(1 for l in ledgers if l.api_status == 'success'),
                'failed': sum(1 for l in ledgers if l.api_status == 'failed'),
                'skipped': sum(1 for l in ledgers if l.api_status == 'skipped'),
            },
            'exclusion_counts': {
                'none': sum(1 for l in ledgers if (l.exclusion_status or 'none') == 'none'),
                'requested': sum(1 for l in ledgers if l.exclusion_status == 'requested'),
                'approved': sum(1 for l in ledgers if l.exclusion_status == 'approved'),
                'rejected': sum(1 for l in ledgers if l.exclusion_status == 'rejected'),
            },
            'bucket_counts': {
                'green': sum(1 for l in ledgers if l.bucket == 'green'),
                'amber': sum(1 for l in ledgers if l.bucket == 'amber'),
                'red': sum(1 for l in ledgers if l.bucket == 'red'),
                'unknown': sum(1 for l in ledgers if l.bucket == 'unknown' or l.bucket is None),
            },
        }

        return jsonify(batch_dict)
    finally:
        session.close()


@ecri_bp.route('/api/batch/<batch_id>/cancel', methods=['POST'])
@login_required
@ecri_manage_required
def api_cancel_batch(batch_id):
    """Cancel a draft batch."""
    from common.models import ECRIBatch

    session = get_pbi_session()
    try:
        batch = session.query(ECRIBatch).filter_by(batch_id=batch_id).first()
        if not batch:
            return jsonify({'error': 'Batch not found'}), 404
        if batch.status not in ('draft', 'site_review', 'rev_approved', 'review'):
            return jsonify({'error': f'Cannot cancel batch in {batch.status} status'}), 400

        batch.status = 'cancelled'
        batch.cancelled_at = datetime.utcnow()
        session.commit()

        return jsonify({'success': True, 'status': 'cancelled'})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI batch cancel error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


# =============================================================================
# API: Execute Batch (Push to SiteLink) — async background processing
# =============================================================================

import threading
import logging

_ecri_logger = logging.getLogger('ecri.execute')


def _execute_batch_worker(batch_id, ledger_ids_filter, app_config):
    """Background worker: push ECRI ledgers to SMD one at a time, committing
    per-row so progress is visible via the progress endpoint."""
    from common.models import ECRIBatch, ECRIBatchLedger, SiteInfo
    from common.config import DataLayerConfig
    from common.soap_client import SOAPClient, SOAPFaultError

    session = get_pbi_session()
    soap_client = None
    try:
        batch = session.query(ECRIBatch).filter_by(batch_id=batch_id).first()
        if not batch:
            _ecri_logger.error(f"Batch {batch_id} not found in worker")
            return

        q = session.query(ECRIBatchLedger).filter(
            ECRIBatchLedger.batch_id == batch_id,
            ECRIBatchLedger.api_status == 'pending',
            ECRIBatchLedger.exclusion_status != 'approved',
        )
        if ledger_ids_filter:
            q = q.filter(ECRIBatchLedger.id.in_(ledger_ids_filter))
        ledgers = q.all()

        # Mark approved-exclusion rows as skipped immediately
        excluded_q = session.query(ECRIBatchLedger).filter(
            ECRIBatchLedger.batch_id == batch_id,
            ECRIBatchLedger.api_status == 'pending',
            ECRIBatchLedger.exclusion_status == 'approved',
        )
        if ledger_ids_filter:
            excluded_q = excluded_q.filter(ECRIBatchLedger.id.in_(ledger_ids_filter))
        for excl_led in excluded_q.all():
            excl_led.api_status = 'skipped'
            excl_led.api_response = {
                'reason': 'Excluded in site review',
                'exclusion_reason_code': excl_led.exclusion_reason_code,
                'exclusion_notes': excl_led.exclusion_notes,
            }
            excl_led.api_executed_at = datetime.utcnow()
        session.commit()

        if not ledgers:
            batch.status = 'draft'
            session.commit()
            return

        config = DataLayerConfig.from_env()
        if not config.soap:
            _ecri_logger.error("SOAP configuration not available")
            batch.status = 'draft'
            session.commit()
            return

        cc_url = config.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
        soap_client = SOAPClient(
            base_url=cc_url,
            corp_code=config.soap.corp_code,
            corp_user=config.soap.corp_user,
            api_key=config.soap.api_key,
            corp_password=config.soap.corp_password,
            timeout=config.soap.timeout,
            retries=config.soap.retries
        )

        site_codes = {}
        sites = session.query(SiteInfo).filter(SiteInfo.SiteID.in_(batch.site_ids)).all()
        for s in sites:
            site_codes[s.SiteID] = s.SiteCode

        for led in ledgers:
            try:
                site_code = site_codes.get(led.site_id)
                if not site_code:
                    led.api_status = 'skipped'
                    led.api_response = {'error': f'No site code for SiteID {led.site_id}'}
                    session.commit()
                    continue

                if batch.control_group_enabled and led.increase_pct == 0:
                    led.api_status = 'skipped'
                    led.api_response = {'reason': 'Control group - no increase'}
                    session.commit()
                    continue

                if not led.effective_date:
                    led.api_status = 'skipped'
                    led.api_response = {'reason': 'effective_date is empty'}
                    session.commit()
                    continue

                payload = {
                    "sLocationCode": site_code,
                    "LedgerID": str(led.ledger_id),
                    "dcNewRate": f"{float(led.new_rent):.2f}",
                    "dScheduledChange": led.effective_date.strftime('%Y-%m-%dT00:00:00'),
                }

                try:
                    api_result = soap_client.call(
                        operation="ScheduleTenantRateChange",
                        parameters=payload,
                        soap_action="http://tempuri.org/CallCenterWs/CallCenterWs/ScheduleTenantRateChange",
                        namespace="http://tempuri.org/CallCenterWs/CallCenterWs",
                        result_tag="RT",
                    )
                    ret_code = api_result[0].get('Ret_Code') if api_result else None
                    ret_msg = api_result[0].get('Ret_Msg') if api_result else None

                    if ret_code is not None and str(ret_code) == '-1':
                        led.api_status = 'failed'
                        led.api_response = {'error': 'SMD rejected', 'ret_code': ret_code, 'ret_msg': ret_msg, 'payload': payload}
                    else:
                        led.api_status = 'success'
                        led.api_response = {'ret_code': ret_code, 'ret_msg': ret_msg, 'payload': payload}
                    led.api_executed_at = datetime.utcnow()

                except SOAPFaultError as e:
                    _ecri_logger.error(f"SOAP fault ledger {led.ledger_id} site {led.site_id}: {e}")
                    led.api_status = 'failed'
                    led.api_response = {'error': 'SOAP API error', 'payload': payload}
                    led.api_executed_at = datetime.utcnow()

                except Exception as e:
                    _ecri_logger.error(f"Error ledger {led.ledger_id} site {led.site_id}: {e}")
                    led.api_status = 'failed'
                    led.api_response = {'error': 'Internal error', 'payload': payload}
                    led.api_executed_at = datetime.utcnow()

                # Commit after every row so progress is visible
                session.commit()

            except Exception as e:
                _ecri_logger.error(f"Outer error for ledger {led.ledger_id}: {e}")
                session.rollback()

        # Check if all ledgers in the batch are done (pending = not yet processed)
        remaining = session.query(ECRIBatchLedger).filter(
            ECRIBatchLedger.batch_id == batch_id,
            ECRIBatchLedger.api_status == 'pending',
        ).count()
        if remaining == 0:
            batch.status = 'executed'
            batch.executed_at = datetime.utcnow()
        else:
            batch.status = 'draft'
        session.commit()

    except Exception as e:
        _ecri_logger.error(f"Worker fatal error for batch {batch_id}: {e}")
        try:
            session.rollback()
            batch = session.query(ECRIBatch).filter_by(batch_id=batch_id).first()
            if batch and batch.status == 'executing':
                batch.status = 'draft'
                session.commit()
        except Exception:
            pass
    finally:
        if soap_client:
            soap_client.close()
        session.close()


@ecri_bp.route('/api/batch/<batch_id>/execute', methods=['POST'])
@login_required
@ecri_manage_required
def api_execute_batch(batch_id):
    """
    Execute ECRI batch — push rent increases to SiteLink API in background.

    Returns 202 immediately. Poll GET /ecri/api/batch/<id>/progress for status.

    Body (all optional):
        ledger_ids (list)   — only process these ecri_batch_ledgers.id rows.
        confirm_live (bool) — required when any selected site_id is NOT in
                              the LSETUP allowlist.
    """
    from common.models import ECRIBatch, ECRIBatchLedger

    body = request.get_json(silent=True) or {}
    ledger_ids = body.get('ledger_ids') or None
    confirm_live = bool(body.get('confirm_live', False))

    if ledger_ids is not None:
        try:
            ledger_ids = [int(x) for x in ledger_ids]
        except (TypeError, ValueError):
            return jsonify({'error': 'ledger_ids must be a list of integers'}), 400
        if not ledger_ids:
            return jsonify({'error': 'ledger_ids is empty'}), 400

    session = get_pbi_session()
    try:
        batch = session.query(ECRIBatch).filter_by(batch_id=batch_id).first()
        if not batch:
            return jsonify({'error': 'Batch not found'}), 404
        if batch.status == 'executed':
            return jsonify({'error': 'Batch already executed'}), 400
        if batch.status == 'executing':
            return jsonify({'error': 'Batch is already being executed — check progress'}), 409
        if batch.status == 'cancelled':
            return jsonify({'error': 'Batch is cancelled'}), 400
        if batch.status not in ('draft', 'rev_approved'):
            return jsonify({'error': f'Batch must be in rev_approved status to execute (current: {batch.status})'}), 400

        q = session.query(ECRIBatchLedger).filter_by(
            batch_id=batch_id, api_status='pending'
        )
        if ledger_ids is not None:
            q = q.filter(ECRIBatchLedger.id.in_(ledger_ids))
        ledger_count = q.count()

        if ledger_count == 0:
            return jsonify({'error': 'No pending ledgers to execute'}), 400

        # Stale-batch guard
        stale_count = q.filter(ECRIBatchLedger.effective_date < date.today()).count()
        if stale_count > 0:
            return jsonify({
                'error': f'{stale_count} ledger(s) have effective_date in the past — batch is stale',
            }), 409

        # LSETUP guard
        try:
            from common.config_loader import get_config
            allowlist = set(get_config().ecri.get('lsetup_allowlist_site_ids', []) or [])
        except Exception:
            allowlist = set()

        if ledger_ids:
            site_ids_in_selection = {r[0] for r in session.query(ECRIBatchLedger.site_id).filter(
                ECRIBatchLedger.batch_id == batch_id,
                ECRIBatchLedger.id.in_(ledger_ids)
            ).distinct().all()}
        else:
            site_ids_in_selection = set(batch.site_ids or [])

        non_lsetup = sorted(site_ids_in_selection - allowlist)
        if non_lsetup and not confirm_live:
            return jsonify({
                'error': 'confirm_live=true required for non-LSETUP sites',
                'non_lsetup_site_ids': non_lsetup,
            }), 409

        # Mark batch as executing and start background thread
        batch.status = 'executing'
        session.commit()

        thread = threading.Thread(
            target=_execute_batch_worker,
            args=(str(batch_id), ledger_ids, None),
            daemon=True,
        )
        thread.start()

        return jsonify({
            'success': True,
            'batch_id': str(batch_id),
            'status': 'executing',
            'pending_count': ledger_count,
            'message': f'Execution started for {ledger_count} ledger(s). Poll /ecri/api/batch/{batch_id}/progress for status.',
        }), 202

    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI batch execution error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


@ecri_bp.route('/api/batch/<batch_id>/progress')
@login_required
@ecri_access_required
def api_batch_progress(batch_id):
    """Poll endpoint for background execution progress."""
    from common.models import ECRIBatch, ECRIBatchLedger

    session = get_pbi_session()
    try:
        batch = session.query(ECRIBatch).filter_by(batch_id=batch_id).first()
        if not batch:
            return jsonify({'error': 'Batch not found'}), 404

        counts = {}
        rows = session.execute(text(
            "SELECT api_status, COUNT(*) FROM ecri_batch_ledgers "
            "WHERE batch_id = :bid GROUP BY api_status"
        ), {'bid': str(batch_id)}).fetchall()
        for row in rows:
            counts[row[0]] = row[1]

        total = sum(counts.values())
        pending = counts.get('pending', 0)
        done = total - pending

        return jsonify({
            'batch_id': str(batch_id),
            'batch_status': batch.status,
            'total': total,
            'pending': pending,
            'processed': done,
            'success': counts.get('success', 0),
            'failed': counts.get('failed', 0),
            'skipped': counts.get('skipped', 0),
            'pct': round(done / total * 100, 1) if total > 0 else 0,
            'finished': batch.status != 'executing',
        })
    finally:
        session.close()


# =============================================================================
# API: Outcomes & Analytics
# =============================================================================

@ecri_bp.route('/api/objections/search')
@login_required
@ecri_access_required
def api_objections_search():
    """Tenant-first search across all executed batches.

    Query params:
      q: search string (matches tenant_name ILIKE, unit_name ILIKE, ledger_id,
         tenant_id exact). Minimum 2 chars.

    Applies site scope from current_user.allowed_site_ids when non-empty.
    Returns only rows from batches with status='executed' and api_status='success'.
    """
    q = (request.args.get('q') or '').strip()
    if len(q) < 2:
        return jsonify({'results': [], 'count': 0})

    session = get_pbi_session()
    try:
        sql = text("""
            SELECT bl.id AS batch_ledger_id, bl.batch_id, bl.site_id, bl.ledger_id,
                   bl.tenant_id, bl.tenant_name, bl.unit_name,
                   bl.old_rent, bl.new_rent, bl.increase_pct, bl.currency,
                   bl.effective_date, bl.api_executed_at,
                   b.name AS batch_name,
                   (SELECT COUNT(*) FROM ecri_objections o
                     WHERE o.batch_ledger_id = bl.id
                       AND o.status IN ('pending_approval','approved','applied')
                   ) AS active_objection_count,
                   (SELECT status FROM ecri_objections o
                     WHERE o.batch_ledger_id = bl.id
                     ORDER BY o.raised_at DESC LIMIT 1) AS last_objection_status
              FROM ecri_batch_ledgers bl
              JOIN ecri_batches b ON b.batch_id = bl.batch_id
             WHERE b.status = 'executed'
               AND bl.api_status = 'success'
               AND (
                 bl.tenant_name ILIKE :like_q
                 OR bl.unit_name ILIKE :like_q
                 OR CAST(bl.ledger_id AS TEXT) = :exact_q
                 OR CAST(bl.tenant_id AS TEXT) = :exact_q
               )
             ORDER BY bl.api_executed_at DESC NULLS LAST
             LIMIT 100
        """)
        rows = session.execute(sql, {'like_q': f'%{q}%', 'exact_q': q}).fetchall()

        allowed = set(getattr(current_user, 'allowed_site_ids', None) or [])
        results = []
        for r in rows:
            if allowed and r.site_id not in allowed:
                continue
            results.append({
                'batch_ledger_id': r.batch_ledger_id,
                'batch_id': str(r.batch_id),
                'batch_name': r.batch_name,
                'site_id': r.site_id,
                'ledger_id': r.ledger_id,
                'tenant_id': r.tenant_id,
                'tenant_name': r.tenant_name,
                'unit_name': r.unit_name,
                'old_rent': float(r.old_rent) if r.old_rent is not None else None,
                'new_rent': float(r.new_rent) if r.new_rent is not None else None,
                'increase_pct': float(r.increase_pct) if r.increase_pct is not None else None,
                'currency': r.currency,
                'effective_date': r.effective_date.isoformat() if r.effective_date else None,
                'pushed_at': r.api_executed_at.isoformat() if r.api_executed_at else None,
                'active_objection_count': int(r.active_objection_count or 0),
                'last_objection_status': r.last_objection_status,
            })
        return jsonify({'results': results, 'count': len(results)})
    finally:
        session.close()


@ecri_bp.route('/api/year-impact')
@login_required
@ecri_access_required
def api_year_impact():
    """Portfolio-level calendar-year revenue impact across all executed batches."""
    from common.ecri_year_impact import compute_year_impact
    year = int(request.args.get('year', date.today().year))
    session = get_pbi_session()
    try:
        return jsonify(compute_year_impact(session, year))
    finally:
        session.close()


@ecri_bp.route('/api/batch/<batch_id>/year-impact')
@login_required
@ecri_access_required
def api_batch_year_impact(batch_id):
    """Calendar-year revenue impact scoped to one batch."""
    from common.ecri_year_impact import compute_year_impact
    year = int(request.args.get('year', date.today().year))
    session = get_pbi_session()
    try:
        return jsonify(compute_year_impact(session, year, batch_id=batch_id))
    finally:
        session.close()


@ecri_bp.route('/api/batch/<batch_id>/outcomes')
@login_required
@ecri_access_required
def api_batch_outcomes(batch_id):
    """Get outcome tracking data for a batch."""
    from common.models import ECRIBatch, ECRIBatchLedger, ECRIOutcome

    session = get_pbi_session()
    try:
        batch = session.query(ECRIBatch).filter_by(batch_id=batch_id).first()
        if not batch:
            return jsonify({'error': 'Batch not found'}), 404

        ledgers = session.query(ECRIBatchLedger).filter_by(batch_id=batch_id).all()
        outcomes = session.query(ECRIOutcome).filter_by(batch_id=batch_id).all()

        # Build outcome map
        outcome_map = {}
        for o in outcomes:
            outcome_map[o.ledger_id] = o.to_dict()

        # Group analysis
        groups = {}
        for led in ledgers:
            g = led.control_group
            if g not in groups:
                groups[g] = {
                    'group': g,
                    'count': 0,
                    'increase_pct': float(led.increase_pct),
                    'stayed': 0,
                    'moved_out': 0,
                    'scheduled_out': 0,
                    'pending': 0,
                    'monthly_gain_stayed': 0,
                    'monthly_loss_churn': 0,
                    'monthly_loss_scheduled': 0,
                }

            groups[g]['count'] += 1
            outcome = outcome_map.get(led.ledger_id)

            if outcome:
                otype = outcome['outcome_type']
                if otype == 'stayed':
                    groups[g]['stayed'] += 1
                    groups[g]['monthly_gain_stayed'] += float(led.increase_amt)
                elif otype == 'moved_out':
                    groups[g]['moved_out'] += 1
                    groups[g]['monthly_loss_churn'] += float(led.new_rent)
                elif otype == 'scheduled_out':
                    groups[g]['scheduled_out'] += 1
                    groups[g]['monthly_loss_scheduled'] += float(led.new_rent)
            else:
                groups[g]['pending'] += 1

        # Calculate rates
        for g in groups:
            total = groups[g]['count']
            resolved = total - groups[g]['pending']
            if resolved > 0:
                groups[g]['churn_rate'] = round(groups[g]['moved_out'] / resolved * 100, 1)
                groups[g]['stay_rate'] = round(groups[g]['stayed'] / resolved * 100, 1)
            else:
                groups[g]['churn_rate'] = None
                groups[g]['stay_rate'] = None

            groups[g]['monthly_gain_stayed'] = round(groups[g]['monthly_gain_stayed'], 2)
            groups[g]['monthly_loss_churn'] = round(groups[g]['monthly_loss_churn'], 2)
            groups[g]['monthly_loss_scheduled'] = round(groups[g]['monthly_loss_scheduled'], 2)
            groups[g]['net_monthly_impact'] = round(
                groups[g]['monthly_gain_stayed']
                - groups[g]['monthly_loss_churn']
                - groups[g]['monthly_loss_scheduled'],
                2,
            )

        return jsonify({
            'batch_id': batch_id,
            'batch_status': batch.status,
            'attribution_window_days': batch.attribution_window_days,
            'executed_at': batch.executed_at.isoformat() if batch.executed_at else None,
            'groups': list(groups.values()),
            'outcomes': [o.to_dict() for o in outcomes],
        })

    finally:
        session.close()


@ecri_bp.route('/api/analytics/summary')
@login_required
@ecri_access_required
def api_analytics_summary():
    """Get overall ECRI performance summary across all batches."""
    from common.models import ECRIBatch, ECRIBatchLedger, ECRIOutcome

    session = get_pbi_session()
    try:
        # Get executed batches
        batches = session.query(ECRIBatch).filter_by(status='executed').order_by(
            desc(ECRIBatch.executed_at)
        ).all()

        summary = {
            'total_batches': len(batches),
            'total_ledgers_processed': 0,
            'total_monthly_gain': 0,
            'total_monthly_loss': 0,
            'overall_churn_rate': None,
            'batches': [],
        }

        total_resolved = 0
        total_churned = 0

        for batch in batches:
            ledgers = session.query(ECRIBatchLedger).filter_by(batch_id=batch.batch_id).all()
            outcomes = session.query(ECRIOutcome).filter_by(batch_id=batch.batch_id).all()

            outcome_map = {o.ledger_id: o for o in outcomes}
            batch_gain = 0
            batch_loss = 0
            batch_stayed = 0
            batch_churned = 0

            for led in ledgers:
                o = outcome_map.get(led.ledger_id)
                if o:
                    total_resolved += 1
                    if o.outcome_type == 'stayed':
                        batch_stayed += 1
                        batch_gain += float(led.increase_amt)
                    elif o.outcome_type == 'moved_out':
                        batch_churned += 1
                        total_churned += 1
                        batch_loss += float(led.new_rent)

            summary['total_ledgers_processed'] += len(ledgers)
            summary['total_monthly_gain'] += batch_gain
            summary['total_monthly_loss'] += batch_loss

            summary['batches'].append({
                'batch_id': str(batch.batch_id),
                'name': batch.name,
                'executed_at': batch.executed_at.isoformat() if batch.executed_at else None,
                'total_ledgers': len(ledgers),
                'stayed': batch_stayed,
                'churned': batch_churned,
                'monthly_gain': round(batch_gain, 2),
                'monthly_loss': round(batch_loss, 2),
                'churn_rate': round(batch_churned / (batch_stayed + batch_churned) * 100, 1)
                    if (batch_stayed + batch_churned) > 0 else None,
            })

        if total_resolved > 0:
            summary['overall_churn_rate'] = round(total_churned / total_resolved * 100, 1)

        summary['total_monthly_gain'] = round(summary['total_monthly_gain'], 2)
        summary['total_monthly_loss'] = round(summary['total_monthly_loss'], 2)
        summary['net_monthly_impact'] = round(
            summary['total_monthly_gain'] - summary['total_monthly_loss'], 2
        )

        return jsonify(summary)
    finally:
        session.close()


# =============================================================================
# API: Batch Workflow — Submit for Review, Finalize, Reopen
# =============================================================================

@ecri_bp.route('/api/batch/<batch_id>/submit-review', methods=['POST'])
@login_required
@ecri_manage_required
def api_submit_batch_for_review(batch_id):
    """draft → site_review. Stamps deadline = MIN(notice_date) - 3 days."""
    from common.models import ECRIBatch, ECRIBatchLedger
    from web.utils.audit import audit_log, AuditEvent

    session = get_pbi_session()
    try:
        batch = session.query(ECRIBatch).filter_by(batch_id=batch_id).first()
        if not batch:
            return jsonify({'error': 'Batch not found'}), 404
        if batch.status != 'draft':
            return jsonify({'error': f'Batch must be in draft status (current: {batch.status})'}), 400

        min_notice = session.execute(
            text("SELECT MIN(notice_date) FROM ecri_batch_ledgers WHERE batch_id = :bid"),
            {'bid': str(batch_id)}
        ).scalar()

        deadline = (min_notice - timedelta(days=3)) if min_notice else None
        batch.status = 'site_review'
        batch.site_review_deadline = deadline
        batch.submitted_for_review_at = datetime.utcnow()
        batch.submitted_for_review_by = current_user.username
        session.commit()

        audit_log(AuditEvent.ECRI_BATCH_SUBMITTED_FOR_REVIEW,
                  f"Batch {batch_id} submitted for site review, deadline={deadline}")
        return jsonify({'success': True, 'status': 'site_review', 'site_review_deadline': deadline.isoformat() if deadline else None})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI submit-review error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


@ecri_bp.route('/api/batch/<batch_id>/finalize', methods=['POST'])
@login_required
@ecri_finalize_required
def api_finalize_batch(batch_id):
    """site_review → rev_approved. All exclusion_status must be decided (not 'requested')."""
    from common.models import ECRIBatch, ECRIBatchLedger
    from web.utils.audit import audit_log, AuditEvent

    session = get_pbi_session()
    try:
        batch = session.query(ECRIBatch).filter_by(batch_id=batch_id).first()
        if not batch:
            return jsonify({'error': 'Batch not found'}), 404
        if batch.status != 'rev_review':
            return jsonify({'error': f'Batch must be in rev_review status (current: {batch.status})'}), 400

        requested_count = session.query(ECRIBatchLedger).filter_by(
            batch_id=batch_id, exclusion_status='requested'
        ).count()
        if requested_count > 0:
            return jsonify({
                'error': f'{requested_count} exclusion request(s) are still pending — decide all before finalizing',
                'pending_exclusion_count': requested_count,
            }), 409

        batch.status = 'rev_approved'
        session.commit()

        today = date.today()
        force = batch.site_review_deadline and today > batch.site_review_deadline
        event = AuditEvent.ECRI_BATCH_FORCE_FINALIZED if force else AuditEvent.ECRI_BATCH_FINALIZED
        audit_log(event, f"Batch {batch_id} finalized (force={force})")
        return jsonify({'success': True, 'status': 'rev_approved', 'force_finalized': force})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI finalize error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


@ecri_bp.route('/api/batch/<batch_id>/close-site-review', methods=['POST'])
@login_required
@ecri_finalize_required
def api_close_site_review(batch_id):
    """site_review → rev_review. Ops have finished flagging; Revenue now
    approves/rejects each request and finalises."""
    from common.models import ECRIBatch
    from web.utils.audit import audit_log, AuditEvent

    session = get_pbi_session()
    try:
        batch = session.query(ECRIBatch).filter_by(batch_id=batch_id).first()
        if not batch:
            return jsonify({'error': 'Batch not found'}), 404
        if batch.status != 'site_review':
            return jsonify({'error': f'Batch must be in site_review status (current: {batch.status})'}), 400

        batch.status = 'rev_review'
        session.commit()

        audit_log(AuditEvent.ECRI_BATCH_SUBMITTED_FOR_REVIEW,
                  f"Batch {batch_id} site review closed, moved to rev_review")
        return jsonify({'success': True, 'status': 'rev_review'})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI close-site-review error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


@ecri_bp.route('/api/batch/<batch_id>/reopen-review', methods=['POST'])
@login_required
@ecri_finalize_required
def api_reopen_review(batch_id):
    """Reopen to an earlier stage.
    - rev_approved → rev_review   (re-decide exclusions without reopening to ops)
    - rev_review   → site_review  (let ops flag more exclusions)
    """
    from common.models import ECRIBatch
    from web.utils.audit import audit_log, AuditEvent

    session = get_pbi_session()
    try:
        batch = session.query(ECRIBatch).filter_by(batch_id=batch_id).first()
        if not batch:
            return jsonify({'error': 'Batch not found'}), 404
        if batch.status == 'rev_approved':
            batch.status = 'rev_review'
            new_status = 'rev_review'
        elif batch.status == 'rev_review':
            batch.status = 'site_review'
            new_status = 'site_review'
        else:
            return jsonify({'error': f'Cannot reopen from {batch.status}'}), 400
        session.commit()

        audit_log(AuditEvent.ECRI_BATCH_REOPENED, f"Batch {batch_id} reopened to {new_status}")
        return jsonify({'success': True, 'status': new_status})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI reopen-review error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


# =============================================================================
# API: Exclusion requests (Stage 2–3)
# =============================================================================

@ecri_bp.route('/api/batch/<batch_id>/exclusion', methods=['POST'])
@login_required
@ecri_exclusion_required
def api_request_exclusion(batch_id):
    """Ops requests exclusion for a ledger row (batch must be in site_review)."""
    from common.models import ECRIBatch, ECRIBatchLedger
    from web.utils.audit import audit_log, AuditEvent

    data = request.get_json() or {}
    row_id = data.get('ledger_row_id')
    reason_code = data.get('reason_code', '').strip()
    notes = data.get('notes', '').strip()

    if not row_id:
        return jsonify({'error': 'ledger_row_id required'}), 400
    if not reason_code:
        return jsonify({'error': 'reason_code required'}), 400

    session = get_pbi_session()
    try:
        batch = session.query(ECRIBatch).filter_by(batch_id=batch_id).first()
        if not batch:
            return jsonify({'error': 'Batch not found'}), 404
        if batch.status != 'site_review':
            return jsonify({'error': 'Batch is not in site_review status'}), 400

        led = session.query(ECRIBatchLedger).filter_by(id=row_id, batch_id=batch_id).first()
        if not led:
            return jsonify({'error': 'Ledger row not found in this batch'}), 404

        # Site-scoping check
        if not current_user.can_see_site(led.site_id):
            return jsonify({'error': 'Access denied — site not in your allowed list'}), 403

        if led.exclusion_status != 'none':
            return jsonify({'error': f'Row already has exclusion_status={led.exclusion_status}'}), 409

        led.exclusion_status = 'requested'
        led.exclusion_reason_code = reason_code
        led.exclusion_notes = notes or None
        led.exclusion_requested_by = current_user.id
        led.exclusion_requested_at = datetime.utcnow()
        session.commit()

        audit_log(AuditEvent.ECRI_EXCLUSION_REQUESTED,
                  f"Ledger row {row_id} (site={led.site_id}, ledger={led.ledger_id}) exclusion requested, reason={reason_code}")
        return jsonify({'success': True, 'exclusion_status': 'requested'})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI request exclusion error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


@ecri_bp.route('/api/exclusion/<int:row_id>', methods=['DELETE'])
@login_required
@ecri_access_required
def api_withdraw_exclusion(row_id):
    """Owner withdraws their exclusion request (only if still 'requested')."""
    from common.models import ECRIBatch, ECRIBatchLedger
    from web.utils.audit import audit_log, AuditEvent

    session = get_pbi_session()
    try:
        led = session.query(ECRIBatchLedger).filter_by(id=row_id).first()
        if not led:
            return jsonify({'error': 'Ledger row not found'}), 404
        if led.exclusion_status != 'requested':
            return jsonify({'error': f'Cannot withdraw — exclusion_status is {led.exclusion_status}'}), 400
        if led.exclusion_requested_by != current_user.id:
            return jsonify({'error': 'You can only withdraw your own exclusion requests'}), 403

        batch = session.query(ECRIBatch).filter_by(batch_id=led.batch_id).first()
        if batch and batch.status != 'site_review':
            return jsonify({'error': 'Batch is no longer in site_review'}), 400

        led.exclusion_status = 'none'
        led.exclusion_reason_code = None
        led.exclusion_notes = None
        led.exclusion_requested_by = None
        led.exclusion_requested_at = None
        session.commit()

        audit_log(AuditEvent.ECRI_EXCLUSION_WITHDRAWN, f"Ledger row {row_id} exclusion withdrawn")
        return jsonify({'success': True, 'exclusion_status': 'none'})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI withdraw exclusion error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


@ecri_bp.route('/api/exclusion/<int:row_id>/approve', methods=['POST'])
@login_required
@ecri_finalize_required
def api_approve_exclusion(row_id):
    """Revenue approves an exclusion request."""
    from common.models import ECRIBatch, ECRIBatchLedger
    from web.utils.audit import audit_log, AuditEvent

    data = request.get_json() or {}
    decision_notes = data.get('notes', '').strip()

    session = get_pbi_session()
    try:
        led = session.query(ECRIBatchLedger).filter_by(id=row_id).first()
        if not led:
            return jsonify({'error': 'Ledger row not found'}), 404
        if led.exclusion_status != 'requested':
            return jsonify({'error': f'Cannot approve — exclusion_status is {led.exclusion_status}'}), 400

        led.exclusion_status = 'approved'
        led.exclusion_decided_by = current_user.id
        led.exclusion_decided_at = datetime.utcnow()
        led.exclusion_decision_notes = decision_notes or None
        session.commit()

        audit_log(AuditEvent.ECRI_EXCLUSION_APPROVED,
                  f"Ledger row {row_id} (site={led.site_id}, ledger={led.ledger_id}) exclusion approved")
        return jsonify({'success': True, 'exclusion_status': 'approved'})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI approve exclusion error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


@ecri_bp.route('/api/exclusion/<int:row_id>/reject', methods=['POST'])
@login_required
@ecri_finalize_required
def api_reject_exclusion(row_id):
    """Revenue rejects an exclusion request — row will still be pushed."""
    from common.models import ECRIBatch, ECRIBatchLedger
    from web.utils.audit import audit_log, AuditEvent

    data = request.get_json() or {}
    decision_notes = data.get('notes', '').strip()

    session = get_pbi_session()
    try:
        led = session.query(ECRIBatchLedger).filter_by(id=row_id).first()
        if not led:
            return jsonify({'error': 'Ledger row not found'}), 404
        if led.exclusion_status != 'requested':
            return jsonify({'error': f'Cannot reject — exclusion_status is {led.exclusion_status}'}), 400

        led.exclusion_status = 'rejected'
        led.exclusion_decided_by = current_user.id
        led.exclusion_decided_at = datetime.utcnow()
        led.exclusion_decision_notes = decision_notes or None
        session.commit()

        audit_log(AuditEvent.ECRI_EXCLUSION_REJECTED,
                  f"Ledger row {row_id} (site={led.site_id}, ledger={led.ledger_id}) exclusion rejected")
        return jsonify({'success': True, 'exclusion_status': 'rejected'})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI reject exclusion error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


@ecri_bp.route('/api/batch/<batch_id>/exclusions')
@login_required
@ecri_access_required
def api_batch_exclusions(batch_id):
    """List all exclusion requests for a batch, optionally filtered by site."""
    from common.models import ECRIBatchLedger

    session = get_pbi_session()
    try:
        q = session.query(ECRIBatchLedger).filter(
            ECRIBatchLedger.batch_id == batch_id,
            ECRIBatchLedger.exclusion_status != 'none',
        )
        # Site scoping for ops
        if current_user.allowed_site_ids:
            q = q.filter(ECRIBatchLedger.site_id.in_(current_user.allowed_site_ids))

        rows = q.order_by(ECRIBatchLedger.site_id, ECRIBatchLedger.ledger_id).all()
        return jsonify({'exclusions': [r.to_dict() for r in rows]})
    finally:
        session.close()


# =============================================================================
# API: Objections (Stage 5 — post-push)
# =============================================================================

@ecri_bp.route('/api/objection', methods=['POST'])
@login_required
@ecri_objection_required
def api_create_objection():
    """Create an objection for a pushed ledger row."""
    from common.models import ECRIBatchLedger, ECRIObjection
    from web.utils.audit import audit_log, AuditEvent

    data = request.get_json() or {}
    batch_ledger_id = data.get('batch_ledger_id')
    new_increase_pct = data.get('new_increase_pct')
    new_new_rent = data.get('new_new_rent')
    reason_code = data.get('reason_code', '').strip()
    reason_notes = data.get('reason_notes', '').strip()

    if not batch_ledger_id:
        return jsonify({'error': 'batch_ledger_id required'}), 400
    if new_increase_pct is None and new_new_rent is None:
        return jsonify({'error': 'new_increase_pct or new_new_rent required'}), 400
    if not reason_code:
        return jsonify({'error': 'reason_code required'}), 400

    try:
        new_increase_pct = float(new_increase_pct) if new_increase_pct is not None else None
        new_new_rent = float(new_new_rent) if new_new_rent is not None else None
    except (TypeError, ValueError):
        return jsonify({'error': 'Invalid numeric values'}), 400

    session = get_pbi_session()
    try:
        led = session.query(ECRIBatchLedger).filter_by(id=batch_ledger_id).first()
        if not led:
            return jsonify({'error': 'Ledger row not found'}), 404
        if led.api_status != 'success':
            return jsonify({'error': 'Objections can only be raised on successfully pushed rows'}), 400

        # Site-scoping
        if not current_user.can_see_site(led.site_id):
            return jsonify({'error': 'Access denied — site not in your allowed list'}), 403

        # One active objection per ledger
        existing = session.query(ECRIObjection).filter(
            ECRIObjection.batch_ledger_id == batch_ledger_id,
            ECRIObjection.status.in_(('pending_approval', 'approved', 'applied')),
        ).first()
        if existing:
            return jsonify({'error': f'An active objection already exists (id={existing.id}, status={existing.status})'}), 409

        orig_pct = float(led.increase_pct)
        orig_rent = float(led.new_rent)

        # Resolve new_increase_pct / new_new_rent from each other if only one given
        if new_increase_pct is None:
            old_rent = float(led.old_rent)
            new_increase_pct = round((new_new_rent - old_rent) / old_rent * 100, 2) if old_rent else 0.0
        if new_new_rent is None:
            new_new_rent = round(float(led.old_rent) * (1 + new_increase_pct / 100), 4)

        reduction_pct = orig_pct - new_increase_pct
        reduction_abs = orig_rent - new_new_rent

        user_max_pct, user_max_abs = current_user.ecri_limits()

        # Increase-direction always requires approval
        if reduction_pct < 0 or reduction_abs < 0:
            requires_approval = True
        elif reduction_pct <= user_max_pct and reduction_abs <= user_max_abs:
            requires_approval = False
        else:
            requires_approval = True

        status = 'approved' if not requires_approval else 'pending_approval'

        obj = ECRIObjection(
            batch_ledger_id=batch_ledger_id,
            batch_id=led.batch_id,
            site_id=led.site_id,
            ledger_id=led.ledger_id,
            original_increase_pct=orig_pct,
            original_new_rent=orig_rent,
            currency=led.currency or 'SGD',
            new_increase_pct=new_increase_pct,
            new_new_rent=new_new_rent,
            reason_code=reason_code,
            reason_notes=reason_notes or None,
            status=status,
            requires_approval=requires_approval,
            raised_by_user_id=current_user.id,
            raised_by_username=current_user.username,
        )
        if not requires_approval:
            obj.approver_user_id = current_user.id
            obj.approver_username = current_user.username
            obj.approved_at = datetime.utcnow()
            obj.approval_notes = 'Auto-approved (within user limits)'

        session.add(obj)
        session.commit()

        audit_log(AuditEvent.ECRI_OBJECTION_CREATED,
                  f"Objection {obj.id} created for ledger row {batch_ledger_id} (auto_approved={not requires_approval})")
        return jsonify({'success': True, 'objection': obj.to_dict()}), 201
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI create objection error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


@ecri_bp.route('/api/objections')
@login_required
@ecri_access_required
def api_list_objections():
    """List objections, site-scoped for ops, with optional filters."""
    from common.models import ECRIObjection

    status_filter = request.args.get('status')
    site_filter = request.args.get('site_id')
    batch_filter = request.args.get('batch_id')

    session = get_pbi_session()
    try:
        q = session.query(ECRIObjection)

        if current_user.allowed_site_ids:
            q = q.filter(ECRIObjection.site_id.in_(current_user.allowed_site_ids))
        if site_filter:
            try:
                q = q.filter(ECRIObjection.site_id == int(site_filter))
            except ValueError:
                pass
        if status_filter:
            q = q.filter(ECRIObjection.status == status_filter)
        if batch_filter:
            q = q.filter(ECRIObjection.batch_id == batch_filter)

        objections = q.order_by(ECRIObjection.raised_at.desc()).limit(500).all()
        return jsonify({'objections': [o.to_dict() for o in objections]})
    finally:
        session.close()


@ecri_bp.route('/api/objection/<int:obj_id>/approve', methods=['POST'])
@login_required
@ecri_objection_approve_required
def api_approve_objection(obj_id):
    """Approver approves a pending objection (checks approver's own limits)."""
    from common.models import ECRIObjection
    from web.utils.audit import audit_log, AuditEvent

    data = request.get_json() or {}
    approval_notes = data.get('notes', '').strip()

    session = get_pbi_session()
    try:
        obj = session.query(ECRIObjection).filter_by(id=obj_id).first()
        if not obj:
            return jsonify({'error': 'Objection not found'}), 404
        if obj.status != 'pending_approval':
            return jsonify({'error': f'Cannot approve — status is {obj.status}'}), 400

        # Approver gate: check approver's own limits cover the requested reduction
        approver_max_pct, approver_max_abs = current_user.ecri_limits()
        reduction_pct = float(obj.original_increase_pct) - float(obj.new_increase_pct or obj.original_increase_pct)
        reduction_abs = float(obj.original_new_rent) - float(obj.new_new_rent or obj.original_new_rent)

        if approver_max_pct < reduction_pct or approver_max_abs < reduction_abs:
            return jsonify({
                'error': 'Your authority is below the requested reduction. Escalate to a higher-rank approver.',
                'required_pct': reduction_pct,
                'required_abs': reduction_abs,
                'your_max_pct': approver_max_pct,
                'your_max_abs': approver_max_abs,
            }), 403

        obj.status = 'approved'
        obj.approver_user_id = current_user.id
        obj.approver_username = current_user.username
        obj.approved_at = datetime.utcnow()
        obj.approval_notes = approval_notes or None
        session.commit()

        audit_log(AuditEvent.ECRI_OBJECTION_APPROVED, f"Objection {obj_id} approved")
        return jsonify({'success': True, 'status': 'approved'})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI approve objection error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


@ecri_bp.route('/api/objection/<int:obj_id>/reject', methods=['POST'])
@login_required
@ecri_objection_approve_required
def api_reject_objection(obj_id):
    """Approver rejects a pending objection (terminal)."""
    from common.models import ECRIObjection
    from web.utils.audit import audit_log, AuditEvent

    data = request.get_json() or {}
    approval_notes = data.get('notes', '').strip()

    session = get_pbi_session()
    try:
        obj = session.query(ECRIObjection).filter_by(id=obj_id).first()
        if not obj:
            return jsonify({'error': 'Objection not found'}), 404
        if obj.status != 'pending_approval':
            return jsonify({'error': f'Cannot reject — status is {obj.status}'}), 400

        obj.status = 'rejected'
        obj.approver_user_id = current_user.id
        obj.approver_username = current_user.username
        obj.approved_at = datetime.utcnow()
        obj.approval_notes = approval_notes or None
        session.commit()

        audit_log(AuditEvent.ECRI_OBJECTION_REJECTED, f"Objection {obj_id} rejected")
        return jsonify({'success': True, 'status': 'rejected'})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI reject objection error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


@ecri_bp.route('/api/objection/<int:obj_id>/apply', methods=['POST'])
@login_required
@ecri_objection_required
def api_apply_objection(obj_id):
    """Push the approved objection rate to SMD via ScheduleTenantRateChange."""
    from common.models import ECRIObjection, ECRIBatchLedger, SiteInfo
    from common.config import DataLayerConfig
    from common.soap_client import SOAPClient, SOAPFaultError
    from web.utils.audit import audit_log, AuditEvent

    session = get_pbi_session()
    try:
        obj = session.query(ECRIObjection).filter_by(id=obj_id).first()
        if not obj:
            return jsonify({'error': 'Objection not found'}), 404
        if obj.status not in ('approved', 'applied'):
            return jsonify({'error': f'Objection must be approved before applying (status: {obj.status})'}), 400
        if obj.raised_by_user_id != current_user.id and not current_user.can_approve_ecri_objection():
            return jsonify({'error': 'Not authorised to apply this objection'}), 403

        # Site scoping
        if not current_user.can_see_site(obj.site_id):
            return jsonify({'error': 'Access denied'}), 403

        led = session.query(ECRIBatchLedger).filter_by(id=obj.batch_ledger_id).first()
        if not led:
            return jsonify({'error': 'Original ledger row not found'}), 404

        config = DataLayerConfig.from_env()
        if not config.soap:
            return jsonify({'error': 'SOAP not configured'}), 500

        site = session.query(SiteInfo).filter_by(SiteID=obj.site_id).first()
        if not site or not site.SiteCode:
            return jsonify({'error': f'No site code for SiteID {obj.site_id}'}), 500

        cc_url = config.soap.base_url.replace('ReportingWs.asmx', 'CallCenterWs.asmx')
        soap_client = SOAPClient(
            base_url=cc_url,
            corp_code=config.soap.corp_code,
            corp_user=config.soap.corp_user,
            api_key=config.soap.api_key,
            corp_password=config.soap.corp_password,
            timeout=config.soap.timeout,
            retries=config.soap.retries,
        )

        new_rent_val = float(obj.new_new_rent)
        payload = {
            "sLocationCode": site.SiteCode,
            "LedgerID": str(obj.ledger_id),
            "dcNewRate": f"{new_rent_val:.2f}",
            "dScheduledChange": led.effective_date.strftime('%Y-%m-%dT00:00:00') if led.effective_date else datetime.utcnow().strftime('%Y-%m-%dT00:00:00'),
        }

        try:
            api_result = soap_client.call(
                operation="ScheduleTenantRateChange",
                parameters=payload,
                soap_action="http://tempuri.org/CallCenterWs/CallCenterWs/ScheduleTenantRateChange",
                namespace="http://tempuri.org/CallCenterWs/CallCenterWs",
                result_tag="RT",
            )
            ret_code = api_result[0].get('Ret_Code') if api_result else None
            ret_msg = api_result[0].get('Ret_Msg') if api_result else None

            if ret_code is not None and str(ret_code) == '-1':
                return jsonify({'error': 'SMD rejected the rate change', 'ret_code': ret_code, 'ret_msg': ret_msg}), 502
        except SOAPFaultError as e:
            current_app.logger.error(f"ECRI apply objection SOAP error: {e}")
            return jsonify({'error': 'SOAP API error'}), 502
        except Exception as e:
            current_app.logger.error(f"ECRI apply objection error: {e}")
            return jsonify({'error': 'Failed to push to SMD'}), 500
        finally:
            soap_client.close()

        # Update objection
        obj.status = 'applied'
        obj.applied_at = datetime.utcnow()
        obj.applied_ret_code = str(ret_code) if ret_code is not None else None
        obj.applied_ret_msg = str(ret_msg) if ret_msg is not None else None

        # Update the original ledger row
        led.new_rent = obj.new_new_rent
        led.increase_pct = obj.new_increase_pct
        led.increase_amt = float(obj.new_new_rent) - float(led.old_rent)

        session.commit()

        audit_log(AuditEvent.ECRI_OBJECTION_APPLIED,
                  f"Objection {obj_id} applied — ledger {obj.ledger_id} site {obj.site_id} new_rent={new_rent_val}")
        return jsonify({'success': True, 'status': 'applied', 'ret_code': str(ret_code), 'ret_msg': str(ret_msg)})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI apply objection outer error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


@ecri_bp.route('/api/objection/<int:obj_id>/cancel', methods=['POST'])
@login_required
@ecri_access_required
def api_cancel_objection(obj_id):
    """Owner cancels a pending/approved objection (not if already applied)."""
    from common.models import ECRIObjection
    from web.utils.audit import audit_log, AuditEvent

    session = get_pbi_session()
    try:
        obj = session.query(ECRIObjection).filter_by(id=obj_id).first()
        if not obj:
            return jsonify({'error': 'Objection not found'}), 404
        if obj.raised_by_user_id != current_user.id:
            return jsonify({'error': 'Only the owner can cancel an objection'}), 403
        if obj.status not in ('pending_approval', 'approved'):
            return jsonify({'error': f'Cannot cancel objection in {obj.status} status'}), 400

        obj.status = 'cancelled'
        session.commit()

        audit_log(AuditEvent.ECRI_OBJECTION_CANCELLED, f"Objection {obj_id} cancelled by owner")
        return jsonify({'success': True, 'status': 'cancelled'})
    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI cancel objection error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        session.close()


# =============================================================================
# Admin: Reasons CRUD
# =============================================================================

@ecri_bp.route('/admin/api/ecri-reasons/exclusion', methods=['GET'])
@login_required
@ecri_access_required
def api_list_exclusion_reasons():
    from web.models.ecri_reasons import ECRIExclusionReason
    db = get_app_db_session()
    try:
        reasons = db.query(ECRIExclusionReason).order_by(ECRIExclusionReason.sort_order, ECRIExclusionReason.id).all()
        return jsonify({'reasons': [r.to_dict() for r in reasons]})
    finally:
        db.close()


@ecri_bp.route('/admin/api/ecri-reasons/exclusion', methods=['POST'])
@login_required
@ecri_reasons_manage_required
def api_create_exclusion_reason():
    from web.models.ecri_reasons import ECRIExclusionReason
    from web.utils.audit import audit_log, AuditEvent
    data = request.get_json() or {}
    code = data.get('code', '').strip()
    label = data.get('label', '').strip()
    if not code or not label:
        return jsonify({'error': 'code and label required'}), 400
    db = get_app_db_session()
    try:
        r = ECRIExclusionReason(code=code, label=label, sort_order=data.get('sort_order', 100))
        db.add(r)
        db.commit()
        audit_log(AuditEvent.ECRI_REASON_ADDED, f"Exclusion reason added: {code}")
        return jsonify({'success': True, 'reason': r.to_dict()}), 201
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"ECRI exclusion reason create error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        db.close()


@ecri_bp.route('/admin/api/ecri-reasons/exclusion/<int:reason_id>', methods=['PATCH'])
@login_required
@ecri_reasons_manage_required
def api_update_exclusion_reason(reason_id):
    from web.models.ecri_reasons import ECRIExclusionReason
    from web.utils.audit import audit_log, AuditEvent
    data = request.get_json() or {}
    db = get_app_db_session()
    try:
        r = db.query(ECRIExclusionReason).filter_by(id=reason_id).first()
        if not r:
            return jsonify({'error': 'Reason not found'}), 404
        if 'label' in data:
            r.label = data['label']
        if 'sort_order' in data:
            r.sort_order = data['sort_order']
        if 'active' in data:
            r.active = bool(data['active'])
        db.commit()
        audit_log(AuditEvent.ECRI_REASON_UPDATED, f"Exclusion reason {r.code} updated")
        return jsonify({'success': True, 'reason': r.to_dict()})
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"ECRI exclusion reason update error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        db.close()


@ecri_bp.route('/admin/api/ecri-reasons/exclusion/<int:reason_id>', methods=['DELETE'])
@login_required
@ecri_reasons_manage_required
def api_deactivate_exclusion_reason(reason_id):
    from web.models.ecri_reasons import ECRIExclusionReason
    from web.utils.audit import audit_log, AuditEvent
    db = get_app_db_session()
    try:
        r = db.query(ECRIExclusionReason).filter_by(id=reason_id).first()
        if not r:
            return jsonify({'error': 'Reason not found'}), 404
        r.active = False
        db.commit()
        audit_log(AuditEvent.ECRI_REASON_DEACTIVATED, f"Exclusion reason {r.code} deactivated")
        return jsonify({'success': True})
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"ECRI exclusion reason deactivate error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        db.close()


@ecri_bp.route('/admin/api/ecri-reasons/objection', methods=['GET'])
@login_required
@ecri_access_required
def api_list_objection_reasons():
    from web.models.ecri_reasons import ECRIObjectionReason
    db = get_app_db_session()
    try:
        reasons = db.query(ECRIObjectionReason).order_by(ECRIObjectionReason.sort_order, ECRIObjectionReason.id).all()
        return jsonify({'reasons': [r.to_dict() for r in reasons]})
    finally:
        db.close()


@ecri_bp.route('/admin/api/ecri-reasons/objection', methods=['POST'])
@login_required
@ecri_reasons_manage_required
def api_create_objection_reason():
    from web.models.ecri_reasons import ECRIObjectionReason
    from web.utils.audit import audit_log, AuditEvent
    data = request.get_json() or {}
    code = data.get('code', '').strip()
    label = data.get('label', '').strip()
    if not code or not label:
        return jsonify({'error': 'code and label required'}), 400
    db = get_app_db_session()
    try:
        r = ECRIObjectionReason(code=code, label=label, sort_order=data.get('sort_order', 100))
        db.add(r)
        db.commit()
        audit_log(AuditEvent.ECRI_REASON_ADDED, f"Objection reason added: {code}")
        return jsonify({'success': True, 'reason': r.to_dict()}), 201
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"ECRI objection reason create error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        db.close()


@ecri_bp.route('/admin/api/ecri-reasons/objection/<int:reason_id>', methods=['PATCH'])
@login_required
@ecri_reasons_manage_required
def api_update_objection_reason(reason_id):
    from web.models.ecri_reasons import ECRIObjectionReason
    from web.utils.audit import audit_log, AuditEvent
    data = request.get_json() or {}
    db = get_app_db_session()
    try:
        r = db.query(ECRIObjectionReason).filter_by(id=reason_id).first()
        if not r:
            return jsonify({'error': 'Reason not found'}), 404
        if 'label' in data:
            r.label = data['label']
        if 'sort_order' in data:
            r.sort_order = data['sort_order']
        if 'active' in data:
            r.active = bool(data['active'])
        db.commit()
        audit_log(AuditEvent.ECRI_REASON_UPDATED, f"Objection reason {r.code} updated")
        return jsonify({'success': True, 'reason': r.to_dict()})
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"ECRI objection reason update error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        db.close()


@ecri_bp.route('/admin/api/ecri-reasons/objection/<int:reason_id>', methods=['DELETE'])
@login_required
@ecri_reasons_manage_required
def api_deactivate_objection_reason(reason_id):
    from web.models.ecri_reasons import ECRIObjectionReason
    from web.utils.audit import audit_log, AuditEvent
    db = get_app_db_session()
    try:
        r = db.query(ECRIObjectionReason).filter_by(id=reason_id).first()
        if not r:
            return jsonify({'error': 'Reason not found'}), 404
        r.active = False
        db.commit()
        audit_log(AuditEvent.ECRI_REASON_DEACTIVATED, f"Objection reason {r.code} deactivated")
        return jsonify({'success': True})
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"ECRI objection reason deactivate error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        db.close()


# =============================================================================
# Admin: User ECRI settings (limits + allowed sites)
# =============================================================================

@ecri_bp.route('/admin/api/users/<int:user_id>/ecri-settings', methods=['PUT'])
@login_required
def api_update_user_ecri_settings(user_id):
    """Update a user's ECRI limits and allowed_site_ids. Requires can_manage_users."""
    from web.models.user import User
    from web.utils.audit import audit_log, AuditEvent
    if not current_user.can_manage_users():
        from flask import abort
        abort(403)
    data = request.get_json() or {}
    db = get_app_db_session()
    try:
        user = db.query(User).filter_by(id=user_id).first()
        if not user:
            return jsonify({'error': 'User not found'}), 404
        if 'ecri_max_pct_reduction' in data:
            user.ecri_max_pct_reduction = float(data['ecri_max_pct_reduction'])
        if 'ecri_max_abs_reduction' in data:
            user.ecri_max_abs_reduction = float(data['ecri_max_abs_reduction'])
        if 'allowed_site_ids' in data:
            site_ids = data['allowed_site_ids']
            user.allowed_site_ids = [int(x) for x in site_ids] if site_ids else None
            audit_log(AuditEvent.ECRI_USER_SITES_CHANGED,
                      f"User {user_id} allowed_site_ids set to {user.allowed_site_ids}")
        audit_log(AuditEvent.ECRI_USER_LIMITS_CHANGED,
                  f"User {user_id} ECRI limits updated: max_pct={user.ecri_max_pct_reduction}, max_abs={user.ecri_max_abs_reduction}")
        db.commit()
        return jsonify({'success': True, 'user': user.to_dict()})
    except Exception as e:
        db.rollback()
        current_app.logger.error(f"ECRI user settings update error: {e}")
        return jsonify({'error': 'An internal error occurred'}), 500
    finally:
        db.close()


# =============================================================================
# Admin: Reasons page route
# =============================================================================

@ecri_bp.route('/admin/ecri-reasons')
@login_required
@ecri_reasons_manage_required
def admin_ecri_reasons():
    """Admin page for managing exclusion and objection reason codes."""
    return render_template('admin/ecri_reasons.html')


@ecri_bp.route('/admin/ecri-user-limits')
@login_required
def admin_ecri_user_limits():
    """Legacy URL — redirects to the page under User Management."""
    from flask import redirect, url_for
    return redirect(url_for('admin.ecri_user_limits'), code=301)
