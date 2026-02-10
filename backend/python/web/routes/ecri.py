"""
ECRI (Existing Customer Rate Increase) routes.

Provides dashboard, eligibility review, batch management, and analytics
for automating self-storage rent increases.
"""

import random
from datetime import datetime, date, timedelta
from uuid import uuid4

from flask import Blueprint, render_template, request, jsonify, current_app
from flask_login import login_required, current_user
from sqlalchemy import func, text, desc
from sqlalchemy.orm import sessionmaker

from web.auth.decorators import ecri_access_required, ecri_manage_required

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
        _pbi_engine = create_engine(pbi_url)
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


@ecri_bp.route('/batch/<batch_id>/analytics')
@login_required
@ecri_access_required
def batch_analytics(batch_id):
    """Outcomes, control group comparison, revenue impact."""
    return render_template('ecri/analytics.html', batch_id=batch_id)


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
            'cc_ledgers': 'extract_date',
            'cc_charges': 'extract_date',
            'cc_tenants': 'extract_date',
            'rentroll': 'extract_date',
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
                freshness[table] = {'latest_date': None, 'stale': True, 'error': str(e)[:100]}

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

        # Build the eligibility SQL (Steps 2-4 from the spec)
        eligibility_sql = text("""
            SELECT
                l."SiteID",
                l."LedgerID",
                l."unitID" AS "UnitID",
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
                -- Unit info from rentroll (latest extract)
                rr."sUnit" AS unit_name,
                rr."sTypeName" AS unit_type,
                rr."dcStdRate" AS std_rate
            FROM cc_ledgers l
            LEFT JOIN LATERAL (
                SELECT rr2."sUnit", rr2."sTypeName", rr2."dcStdRate"
                FROM rentroll rr2
                WHERE rr2."SiteID" = l."SiteID"
                  AND rr2."UnitID" = l."unitID"
                ORDER BY rr2.extract_date DESC
                LIMIT 1
            ) rr ON true
            WHERE l."SiteID" = ANY(:site_ids)
              -- Step 2: Active only
              AND l."dMovedIn" IS NOT NULL
              AND l."dMovedOut" IS NULL
              -- Step 3: Not scheduled out within exclusion window
              AND (l."dSchedOut" IS NULL OR l."dSchedOut" > :sched_out_cutoff)
              -- Step 4: No pending increase
              AND (l."dSchedRentStrt" IS NULL OR l."dSchedRentStrt" < CURRENT_DATE)
              -- Step 4: Last increase 12+ months ago
              AND (COALESCE(l."dRentLastChanged", l."dMovedIn") <= :tenure_cutoff)
              -- Not excluded from revenue management
              AND (l."bExcludeFromRevenueMgmt" IS NULL OR l."bExcludeFromRevenueMgmt" = FALSE)
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

        # Exclusion summary (run a separate query for counts)
        exclusion_sql = text("""
            SELECT
                COUNT(*) FILTER (WHERE "dMovedIn" IS NULL OR "dMovedOut" IS NOT NULL) AS excluded_inactive,
                COUNT(*) FILTER (WHERE "dSchedOut" IS NOT NULL AND "dSchedOut" <= :sched_out_cutoff
                    AND "dMovedIn" IS NOT NULL AND "dMovedOut" IS NULL) AS excluded_sched_out,
                COUNT(*) FILTER (WHERE ("dSchedRentStrt" IS NOT NULL AND "dSchedRentStrt" >= CURRENT_DATE)
                    AND "dMovedIn" IS NOT NULL AND "dMovedOut" IS NULL) AS excluded_pending_increase,
                COUNT(*) FILTER (WHERE COALESCE("dRentLastChanged", "dMovedIn") > :tenure_cutoff
                    AND "dMovedIn" IS NOT NULL AND "dMovedOut" IS NULL
                    AND ("dSchedRentStrt" IS NULL OR "dSchedRentStrt" < CURRENT_DATE)) AS excluded_recent_increase,
                COUNT(*) FILTER (WHERE "bExcludeFromRevenueMgmt" = TRUE
                    AND "dMovedIn" IS NOT NULL AND "dMovedOut" IS NULL) AS excluded_rev_mgmt,
                COUNT(*) AS total_ledgers
            FROM cc_ledgers
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
        return jsonify({'error': str(e)}), 500
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
        effective = today + timedelta(days=notice_days)

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
            increase_amt = round(old_rent * pct / 100, 2)
            new_rent = round(old_rent + increase_amt, 2)

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
                notice_date=today,
                effective_date=effective,
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
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


@ecri_bp.route('/api/batch/<batch_id>')
@login_required
@ecri_access_required
def api_get_batch(batch_id):
    """Get batch details with ledger list."""
    from common.models import ECRIBatch, ECRIBatchLedger

    session = get_pbi_session()
    try:
        batch = session.query(ECRIBatch).filter_by(batch_id=batch_id).first()
        if not batch:
            return jsonify({'error': 'Batch not found'}), 404

        ledgers = session.query(ECRIBatchLedger).filter_by(batch_id=batch_id).order_by(
            ECRIBatchLedger.site_id, ECRIBatchLedger.ledger_id
        ).all()

        batch_dict = batch.to_dict()
        batch_dict['ledgers'] = [l.to_dict() for l in ledgers]

        # Summary stats
        total_increase = sum(float(l.increase_amt) for l in ledgers)
        by_group = {}
        for l in ledgers:
            g = l.control_group
            if g not in by_group:
                by_group[g] = {'count': 0, 'total_increase': 0, 'avg_pct': 0, 'pcts': []}
            by_group[g]['count'] += 1
            by_group[g]['total_increase'] += float(l.increase_amt)
            by_group[g]['pcts'].append(float(l.increase_pct))

        for g in by_group:
            by_group[g]['avg_pct'] = round(sum(by_group[g]['pcts']) / len(by_group[g]['pcts']), 1)
            by_group[g]['total_increase'] = round(by_group[g]['total_increase'], 2)
            del by_group[g]['pcts']

        batch_dict['summary'] = {
            'total_monthly_increase': round(total_increase, 2),
            'total_annual_increase': round(total_increase * 12, 2),
            'groups': by_group,
            'api_status_counts': {
                'pending': sum(1 for l in ledgers if l.api_status == 'pending'),
                'success': sum(1 for l in ledgers if l.api_status == 'success'),
                'failed': sum(1 for l in ledgers if l.api_status == 'failed'),
                'skipped': sum(1 for l in ledgers if l.api_status == 'skipped'),
            }
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
        if batch.status not in ('draft', 'review'):
            return jsonify({'error': f'Cannot cancel batch in {batch.status} status'}), 400

        batch.status = 'cancelled'
        batch.cancelled_at = datetime.utcnow()
        session.commit()

        return jsonify({'success': True, 'status': 'cancelled'})
    except Exception as e:
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


# =============================================================================
# API: Execute Batch (Push to SiteLink)
# =============================================================================

@ecri_bp.route('/api/batch/<batch_id>/execute', methods=['POST'])
@login_required
@ecri_manage_required
def api_execute_batch(batch_id):
    """
    Execute ECRI batch — push rent increases to SiteLink API.

    Processes one ledger at a time:
    1. Verify tenant still active via local data
    2. Call ScheduleRentIncrease API
    3. Log result per ledger
    """
    from common.models import ECRIBatch, ECRIBatchLedger

    session = get_pbi_session()
    try:
        batch = session.query(ECRIBatch).filter_by(batch_id=batch_id).first()
        if not batch:
            return jsonify({'error': 'Batch not found'}), 404
        if batch.status == 'executed':
            return jsonify({'error': 'Batch already executed'}), 400
        if batch.status == 'cancelled':
            return jsonify({'error': 'Batch is cancelled'}), 400

        ledgers = session.query(ECRIBatchLedger).filter_by(
            batch_id=batch_id, api_status='pending'
        ).all()

        if not ledgers:
            return jsonify({'error': 'No pending ledgers to execute'}), 400

        # Get SOAP config
        from common.config import DataLayerConfig
        from common.soap_client import SOAPClient, SOAPFaultError
        config = DataLayerConfig.from_env()

        if not config.soap:
            return jsonify({'error': 'SOAP configuration not available'}), 500

        # Build CallCenterWs URL
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

        # Get site code mapping
        from common.models import SiteInfo
        site_codes = {}
        sites = session.query(SiteInfo).filter(SiteInfo.SiteID.in_(batch.site_ids)).all()
        for s in sites:
            site_codes[s.SiteID] = s.SiteCode

        results = {'success': 0, 'failed': 0, 'skipped': 0, 'errors': []}

        try:
            for led in ledgers:
                site_code = site_codes.get(led.site_id)
                if not site_code:
                    led.api_status = 'skipped'
                    led.api_response = {'error': f'No site code for SiteID {led.site_id}'}
                    results['skipped'] += 1
                    continue

                # Skip control group 0 (0% increase)
                if batch.control_group_enabled and led.increase_pct == 0:
                    led.api_status = 'skipped'
                    led.api_response = {'reason': 'Control group - no increase'}
                    results['skipped'] += 1
                    continue

                try:
                    api_result = soap_client.call(
                        operation="ScheduleRentIncrease",
                        parameters={
                            "sLocationCode": site_code,
                            "iLedgerID": led.ledger_id,
                            "dcSchedRent": str(led.new_rent),
                            "dSchedRentStrt": led.effective_date.isoformat() if led.effective_date else '',
                        },
                        soap_action="http://tempuri.org/CallCenterWs/CallCenterWs/ScheduleRentIncrease",
                        namespace="http://tempuri.org/CallCenterWs/CallCenterWs",
                        result_tag="Table",
                    )
                    led.api_status = 'success'
                    led.api_response = {'result': api_result}
                    led.api_executed_at = datetime.utcnow()
                    results['success'] += 1

                except SOAPFaultError as e:
                    led.api_status = 'failed'
                    led.api_response = {'error': str(e)}
                    led.api_executed_at = datetime.utcnow()
                    results['failed'] += 1
                    results['errors'].append({
                        'ledger_id': led.ledger_id,
                        'site_id': led.site_id,
                        'error': str(e)
                    })

                except Exception as e:
                    led.api_status = 'failed'
                    led.api_response = {'error': str(e)}
                    led.api_executed_at = datetime.utcnow()
                    results['failed'] += 1
                    results['errors'].append({
                        'ledger_id': led.ledger_id,
                        'site_id': led.site_id,
                        'error': str(e)
                    })

            # Update batch status
            batch.status = 'executed'
            batch.executed_at = datetime.utcnow()
            session.commit()

        finally:
            soap_client.close()

        return jsonify({
            'success': True,
            'batch_id': batch_id,
            'results': results,
        })

    except Exception as e:
        session.rollback()
        current_app.logger.error(f"ECRI batch execution error: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()


# =============================================================================
# API: Outcomes & Analytics
# =============================================================================

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
            groups[g]['net_monthly_impact'] = round(
                groups[g]['monthly_gain_stayed'] - groups[g]['monthly_loss_churn'], 2
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
