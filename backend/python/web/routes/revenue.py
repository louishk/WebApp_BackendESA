"""
Revenue Management routes.

Rate benchmarking, demand analysis, customer segmentation, and occupancy trends.
All data sourced from esa_pbi enriched views (rentroll_enriched, units_info_enriched,
mimo_enriched, vw_budget_monthly).
"""

import logging
from datetime import date

from flask import Blueprint, render_template, request, jsonify, current_app
from flask_login import login_required
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from web.auth.decorators import revenue_tools_access_required

logger = logging.getLogger(__name__)

revenue_bp = Blueprint('revenue', __name__, url_prefix='/revenue')


# =============================================================================
# PBI Database Session (revenue data lives in esa_pbi)
# =============================================================================

_pbi_engine = None
_pbi_session_factory = None


def get_pbi_session():
    """Get PBI database session for revenue queries."""
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

@revenue_bp.route('/')
@login_required
@revenue_tools_access_required
def dashboard():
    """Revenue management dashboard — rate benchmarking and demand analysis."""
    return render_template('tools/revenue_management.html')


# =============================================================================
# API: Rate Benchmarking
# =============================================================================

@revenue_bp.route('/api/rate-benchmarking')
@login_required
@revenue_tools_access_required
def api_rate_benchmarking():
    """
    Per-category rate analysis using enriched views.

    Returns occupied vs vacant metrics grouped by type/climate/size,
    plus budget targets and discount reference.
    """
    site_id = request.args.get('site_id', type=int)
    if not site_id:
        return jsonify({'error': 'site_id is required'}), 400

    session = get_pbi_session()
    try:
        # Get site info for site_code (needed for budget lookup)
        site_row = session.execute(
            text('SELECT "SiteID", "SiteCode", "Name", "Country" FROM siteinfo WHERE "SiteID" = :sid'),
            {'sid': site_id}
        ).fetchone()
        if not site_row:
            return jsonify({'error': 'Site not found'}), 404

        site_code = site_row[1]

        # --- Rate benchmarking by category ---
        categories = session.execute(text("""
            WITH latest AS (
                SELECT MAX(extract_date) AS dt
                FROM rentroll_enriched WHERE "SiteID" = :site_id
            )
            SELECT
                ui.label_type_code,
                ui.label_climate_code,
                ui.label_size_range,
                ui.label_size_category,
                ui.label_shape,
                ui.label_pillar,
                -- Occupied
                COUNT(*) FILTER (WHERE r."bRented" = true) AS occ_count,
                SUM(r.dcarea_fixed) FILTER (WHERE r."bRented" = true) AS occ_total_area,
                SUM(r.revenue_effective) FILTER (WHERE r."bRented" = true) AS occ_total_rent,
                AVG(r.revenue_effective / NULLIF(r.dcarea_fixed, 0))
                    FILTER (WHERE r."bRented" = true) AS occ_avg_rate_sqft,
                AVG(r."dcStdRate" / NULLIF(r.dcarea_fixed, 0))
                    FILTER (WHERE r."bRented" = true) AS occ_avg_std_sqft,
                COUNT(*) FILTER (WHERE r."bRented" = true AND r.disc_dcdiscount > 0) AS occ_discounted_count,
                AVG(r.disc_dcdiscount / NULLIF(r.disc_dcprice, 0) * 100)
                    FILTER (WHERE r."bRented" = true AND r.disc_dcdiscount > 0) AS occ_avg_discount_pct,
                ARRAY_AGG(DISTINCT r.disc_sconcessionplan)
                    FILTER (WHERE r."bRented" = true AND r.disc_sconcessionplan IS NOT NULL) AS occ_concession_plans,
                -- Vacant
                COUNT(*) FILTER (WHERE r."bRented" = false) AS vac_count,
                SUM(r.dcarea_fixed) FILTER (WHERE r."bRented" = false) AS vac_total_area,
                AVG(r."dcStdRate" / NULLIF(r.dcarea_fixed, 0))
                    FILTER (WHERE r."bRented" = false) AS vac_std_rate_sqft,
                AVG(r."dcWebRate" / NULLIF(r.dcarea_fixed, 0))
                    FILTER (WHERE r."bRented" = false) AS vac_web_rate_sqft,
                AVG(r."dcPushRate" / NULLIF(r.dcarea_fixed, 0))
                    FILTER (WHERE r."bRented" = false) AS vac_push_rate_sqft,
                AVG(r."iDaysVacant") FILTER (WHERE r."bRented" = false) AS vac_avg_days_vacant,
                SUM(r."dcStdRate") FILTER (WHERE r."bRented" = false) AS vac_total_std_rate
            FROM rentroll_enriched r
            JOIN latest l ON r.extract_date = l.dt
            LEFT JOIN units_info_enriched ui
                ON ui."SiteID" = r."SiteID" AND ui."UnitID" = r."UnitID"
            WHERE r."SiteID" = :site_id AND r."bRentable" = true
            GROUP BY ui.label_type_code, ui.label_climate_code, ui.label_size_range,
                     ui.label_size_category, ui.label_shape, ui.label_pillar
            ORDER BY ui.label_type_code, ui.label_climate_code, ui.label_size_range
        """), {'site_id': site_id}).fetchall()

        # --- Discount reference (lowest active plan %) ---
        disc_ref = session.execute(text("""
            SELECT MIN("dcPCDiscount") AS min_pct_discount
            FROM ccws_discount
            WHERE "SiteID" = :site_id
              AND ("bNeverExpires" = true OR "dPlanEnd" >= CURRENT_DATE)
              AND "dcPCDiscount" > 0
        """), {'site_id': site_id}).fetchone()

        min_discount_pct = float(disc_ref[0]) if disc_ref and disc_ref[0] else None

        # --- Budget targets ---
        budget_rows = session.execute(text("""
            SELECT date, avr_rental_rate, occupancy_pct, rental_revenue,
                   occupied_nla, total_available_nla
            FROM vw_budget_monthly
            WHERE site_code = :site_code AND date IN (
                DATE_TRUNC('month', CURRENT_DATE)::date,
                (DATE_TRUNC('year', CURRENT_DATE) + INTERVAL '11 months')::date
            )
            ORDER BY date
        """), {'site_code': site_code}).fetchall()

        budget = {}
        for row in budget_rows:
            bdate = row[0]
            entry = {
                'avr_rental_rate': float(row[1]) if row[1] else None,
                'occupancy_pct': float(row[2]) if row[2] else None,
                'rental_revenue': float(row[3]) if row[3] else None,
                'occupied_nla': float(row[4]) if row[4] else None,
                'total_available_nla': float(row[5]) if row[5] else None,
            }
            if bdate.month == date.today().month:
                budget['current_month'] = entry
            elif bdate.month == 12:
                budget['year_end'] = entry

        # --- Build response ---
        cat_list = []
        for row in categories:
            cat_list.append({
                'type_code': row[0],
                'climate_code': row[1],
                'size_range': row[2],
                'size_category': row[3],
                'shape': row[4],
                'pillar': row[5],
                'occupied': {
                    'count': row[6] or 0,
                    'total_area': float(row[7]) if row[7] else 0,
                    'total_rent': float(row[8]) if row[8] else 0,
                    'avg_rate_sqft': float(row[9]) if row[9] else None,
                    'avg_std_sqft': float(row[10]) if row[10] else None,
                    'discounted_count': row[11] or 0,
                    'avg_discount_pct': float(row[12]) if row[12] else None,
                    'concession_plans': row[13] or [],
                },
                'vacant': {
                    'count': row[14] or 0,
                    'total_area': float(row[15]) if row[15] else 0,
                    'std_rate_sqft': float(row[16]) if row[16] else None,
                    'web_rate_sqft': float(row[17]) if row[17] else None,
                    'push_rate_sqft': float(row[18]) if row[18] else None,
                    'avg_days_vacant': float(row[19]) if row[19] else None,
                    'total_std_rate': float(row[20]) if row[20] else 0,
                },
            })

        return jsonify({
            'status': 'success',
            'data': {
                'site': {
                    'site_id': site_row[0],
                    'site_code': site_row[1],
                    'name': site_row[2],
                    'country': site_row[3],
                },
                'budget': budget,
                'discount_reference': {
                    'min_pct_discount': min_discount_pct,
                },
                'categories': cat_list,
            }
        })

    except Exception as e:
        logger.error("Rate benchmarking error for site %s: %s", site_id, e)
        return jsonify({'error': 'Failed to load rate benchmarking data'}), 500
    finally:
        session.close()


# =============================================================================
# API: Customer Segments
# =============================================================================

@revenue_bp.route('/api/customer-segments')
@login_required
@revenue_tools_access_required
def api_customer_segments():
    """
    Tenant segmentation by length-of-stay range and discount status.

    Uses rentroll_enriched which already has los_range and discount fields.
    """
    site_id = request.args.get('site_id', type=int)
    if not site_id:
        return jsonify({'error': 'site_id is required'}), 400

    session = get_pbi_session()
    try:
        rows = session.execute(text("""
            WITH latest AS (
                SELECT MAX(extract_date) AS dt
                FROM rentroll_enriched WHERE "SiteID" = :site_id
            )
            SELECT
                r.los_range,
                COUNT(*) AS tenant_count,
                SUM(r.dcarea_fixed) AS total_area,
                AVG(r.revenue_effective / NULLIF(r.dcarea_fixed, 0)) AS avg_rate_sqft,
                AVG(r."dcStdRate" / NULLIF(r.dcarea_fixed, 0)) AS avg_std_sqft,
                COUNT(*) FILTER (WHERE r.disc_dcdiscount > 0) AS discounted_count,
                AVG(r.disc_dcdiscount / NULLIF(r.disc_dcprice, 0) * 100)
                    FILTER (WHERE r.disc_dcdiscount > 0) AS avg_discount_pct,
                SUM(r.revenue_effective) AS total_rent
            FROM rentroll_enriched r
            JOIN latest l ON r.extract_date = l.dt
            WHERE r."SiteID" = :site_id
              AND r."bRented" = true
              AND r."bRentable" = true
              AND r."LedgerID" > 0
            GROUP BY r.los_range
            ORDER BY MIN(r.days_rented)
        """), {'site_id': site_id}).fetchall()

        segments = []
        for row in rows:
            segments.append({
                'los_range': row[0],
                'tenant_count': row[1] or 0,
                'total_area': float(row[2]) if row[2] else 0,
                'avg_rate_sqft': float(row[3]) if row[3] else None,
                'avg_std_sqft': float(row[4]) if row[4] else None,
                'discounted_count': row[5] or 0,
                'avg_discount_pct': float(row[6]) if row[6] else None,
                'total_rent': float(row[7]) if row[7] else 0,
            })

        return jsonify({'status': 'success', 'data': {'segments': segments}})

    except Exception as e:
        logger.error("Customer segments error for site %s: %s", site_id, e)
        return jsonify({'error': 'Failed to load customer segments'}), 500
    finally:
        session.close()


# =============================================================================
# API: Demand Analysis
# =============================================================================

@revenue_bp.route('/api/demand-analysis')
@login_required
@revenue_tools_access_required
def api_demand_analysis():
    """
    Book movement analysis: category-level mimo data, occupancy snapshots,
    vacancy distribution, and lead volume.
    """
    site_id = request.args.get('site_id', type=int)
    if not site_id:
        return jsonify({'error': 'site_id is required'}), 400

    session = get_pbi_session()
    try:
        # --- A. Category-level movement from mimo_enriched ---
        movement = session.execute(text("""
            SELECT
                ui.label_type_code,
                ui.label_climate_code,
                ui.label_size_range,
                -- 7-day
                COUNT(*) FILTER (WHERE m."MoveIn" = 1 AND m."MoveDate" >= NOW() - INTERVAL '7 days') AS mi_7d,
                COUNT(*) FILTER (WHERE m."MoveOut" = 1 AND m."MoveDate" >= NOW() - INTERVAL '7 days') AS mo_7d,
                -- 14-day
                COUNT(*) FILTER (WHERE m."MoveIn" = 1 AND m."MoveDate" >= NOW() - INTERVAL '14 days') AS mi_14d,
                COUNT(*) FILTER (WHERE m."MoveOut" = 1 AND m."MoveDate" >= NOW() - INTERVAL '14 days') AS mo_14d,
                -- 30-day
                COUNT(*) FILTER (WHERE m."MoveIn" = 1 AND m."MoveDate" >= NOW() - INTERVAL '30 days') AS mi_30d,
                COUNT(*) FILTER (WHERE m."MoveOut" = 1 AND m."MoveDate" >= NOW() - INTERVAL '30 days') AS mo_30d,
                -- 60-day
                COUNT(*) FILTER (WHERE m."MoveIn" = 1 AND m."MoveDate" >= NOW() - INTERVAL '60 days') AS mi_60d,
                COUNT(*) FILTER (WHERE m."MoveOut" = 1 AND m."MoveDate" >= NOW() - INTERVAL '60 days') AS mo_60d,
                -- 90-day
                COUNT(*) FILTER (WHERE m."MoveIn" = 1 AND m."MoveDate" >= NOW() - INTERVAL '90 days') AS mi_90d,
                COUNT(*) FILTER (WHERE m."MoveOut" = 1 AND m."MoveDate" >= NOW() - INTERVAL '90 days') AS mo_90d,
                -- Move-in pricing (30d)
                AVG(m."MovedInRentalRate" / NULLIF(m."MovedInArea_fixed", 0))
                    FILTER (WHERE m."MoveIn" = 1 AND m."MoveDate" >= NOW() - INTERVAL '30 days') AS avg_mi_rate_sqft_30d,
                AVG(m."MovedInDaysVacant")
                    FILTER (WHERE m."MoveIn" = 1 AND m."MoveDate" >= NOW() - INTERVAL '30 days') AS avg_days_vacant_before_mi_30d,
                AVG(m."dcDiscount")
                    FILTER (WHERE m."MoveIn" = 1 AND m."MoveDate" >= NOW() - INTERVAL '30 days') AS avg_mi_discount_30d
            FROM mimo_enriched m
            JOIN units_info_enriched ui ON ui.mimo_id = m.mimo_id
            WHERE m."SiteID" = :site_id
            GROUP BY ui.label_type_code, ui.label_climate_code, ui.label_size_range
            ORDER BY ui.label_type_code, ui.label_climate_code, ui.label_size_range
        """), {'site_id': site_id}).fetchall()

        movement_data = []
        for row in movement:
            movement_data.append({
                'type_code': row[0],
                'climate_code': row[1],
                'size_range': row[2],
                'periods': {
                    '7d': {'mi': row[3] or 0, 'mo': row[4] or 0, 'net': (row[3] or 0) - (row[4] or 0)},
                    '14d': {'mi': row[5] or 0, 'mo': row[6] or 0, 'net': (row[5] or 0) - (row[6] or 0)},
                    '30d': {'mi': row[7] or 0, 'mo': row[8] or 0, 'net': (row[7] or 0) - (row[8] or 0)},
                    '60d': {'mi': row[9] or 0, 'mo': row[10] or 0, 'net': (row[9] or 0) - (row[10] or 0)},
                    '90d': {'mi': row[11] or 0, 'mo': row[12] or 0, 'net': (row[11] or 0) - (row[12] or 0)},
                },
                'move_in_pricing': {
                    'avg_rate_sqft_30d': float(row[13]) if row[13] else None,
                    'avg_days_vacant_30d': float(row[14]) if row[14] else None,
                    'avg_discount_30d': float(row[15]) if row[15] else None,
                },
            })

        # --- B. Site-level occupancy snapshots ---
        occ_snapshots = session.execute(text("""
            SELECT
                os.extract_date,
                os."iTotalUnits", os."iOccupiedUnits", os."iVacantUnits",
                os."dcUnitOccupancy", os."dcAreaOccupancy", os."dcEconomicOccupancy",
                os."dcPotentialRevenue", os."dcActualRevenue"
            FROM mgmt_occupancy_statistics os
            WHERE os."SiteID" = :site_id
              AND os.extract_date IN (
                  (SELECT MAX(extract_date) FROM mgmt_occupancy_statistics WHERE "SiteID" = :site_id),
                  (SELECT MAX(extract_date) FROM mgmt_occupancy_statistics WHERE "SiteID" = :site_id AND extract_date <= CURRENT_DATE - 7),
                  (SELECT MAX(extract_date) FROM mgmt_occupancy_statistics WHERE "SiteID" = :site_id AND extract_date <= CURRENT_DATE - 14),
                  (SELECT MAX(extract_date) FROM mgmt_occupancy_statistics WHERE "SiteID" = :site_id AND extract_date <= CURRENT_DATE - 30),
                  (SELECT MAX(extract_date) FROM mgmt_occupancy_statistics WHERE "SiteID" = :site_id AND extract_date <= CURRENT_DATE - 60),
                  (SELECT MAX(extract_date) FROM mgmt_occupancy_statistics WHERE "SiteID" = :site_id AND extract_date <= CURRENT_DATE - 90)
              )
            ORDER BY os.extract_date DESC
        """), {'site_id': site_id}).fetchall()

        snapshots = []
        for row in occ_snapshots:
            snapshots.append({
                'date': row[0].isoformat() if row[0] else None,
                'total_units': row[1],
                'occupied_units': row[2],
                'vacant_units': row[3],
                'unit_occupancy': float(row[4]) if row[4] else None,
                'area_occupancy': float(row[5]) if row[5] else None,
                'economic_occupancy': float(row[6]) if row[6] else None,
                'potential_revenue': float(row[7]) if row[7] else None,
                'actual_revenue': float(row[8]) if row[8] else None,
            })

        # --- C. Vacancy distribution by category ---
        vacancy_dist = session.execute(text("""
            WITH latest AS (
                SELECT MAX(extract_date) AS dt
                FROM rentroll_enriched WHERE "SiteID" = :site_id
            )
            SELECT
                ui.label_type_code,
                ui.label_climate_code,
                ui.label_size_range,
                CASE
                    WHEN r."iDaysVacant" <= 7 THEN '0-7d'
                    WHEN r."iDaysVacant" <= 14 THEN '8-14d'
                    WHEN r."iDaysVacant" <= 30 THEN '15-30d'
                    WHEN r."iDaysVacant" <= 60 THEN '31-60d'
                    WHEN r."iDaysVacant" <= 90 THEN '61-90d'
                    ELSE '90d+'
                END AS vacancy_bucket,
                COUNT(*) AS unit_count,
                SUM(r.dcarea_fixed) AS total_area,
                AVG(r."dcStdRate" / NULLIF(r.dcarea_fixed, 0)) AS avg_std_rate_sqft
            FROM rentroll_enriched r
            JOIN latest l ON r.extract_date = l.dt
            LEFT JOIN units_info_enriched ui
                ON ui."SiteID" = r."SiteID" AND ui."UnitID" = r."UnitID"
            WHERE r."SiteID" = :site_id
              AND r."bRentable" = true
              AND r."bRented" = false
            GROUP BY ui.label_type_code, ui.label_climate_code, ui.label_size_range,
                     vacancy_bucket
            ORDER BY ui.label_type_code, ui.label_climate_code, ui.label_size_range,
                     MIN(r."iDaysVacant")
        """), {'site_id': site_id}).fetchall()

        vacancy_data = []
        for row in vacancy_dist:
            vacancy_data.append({
                'type_code': row[0],
                'climate_code': row[1],
                'size_range': row[2],
                'bucket': row[3],
                'count': row[4] or 0,
                'total_area': float(row[5]) if row[5] else 0,
                'avg_std_rate_sqft': float(row[6]) if row[6] else None,
            })

        # --- D. Lead volume (site-level) ---
        site_row2 = session.execute(
            text('SELECT "SiteCode" FROM siteinfo WHERE "SiteID" = :sid'),
            {'sid': site_id}
        ).fetchone()
        lead_counts = {}
        if site_row2:
            lead_rows = session.execute(text("""
                SELECT
                    COUNT(*) FILTER (WHERE date_entered >= NOW() - INTERVAL '7 days') AS leads_7d,
                    COUNT(*) FILTER (WHERE date_entered >= NOW() - INTERVAL '14 days') AS leads_14d,
                    COUNT(*) FILTER (WHERE date_entered >= NOW() - INTERVAL '30 days') AS leads_30d,
                    COUNT(*) FILTER (WHERE date_entered >= NOW() - INTERVAL '60 days') AS leads_60d,
                    COUNT(*) FILTER (WHERE date_entered >= NOW() - INTERVAL '90 days') AS leads_90d
                FROM vw_customer_master
                WHERE site_code = :site_code
                  AND is_valid_for_analysis = true
            """), {'site_code': site_row2[0]}).fetchone()
            if lead_rows:
                lead_counts = {
                    '7d': lead_rows[0] or 0,
                    '14d': lead_rows[1] or 0,
                    '30d': lead_rows[2] or 0,
                    '60d': lead_rows[3] or 0,
                    '90d': lead_rows[4] or 0,
                }

        return jsonify({
            'status': 'success',
            'data': {
                'movement': movement_data,
                'occupancy_snapshots': snapshots,
                'vacancy_distribution': vacancy_data,
                'lead_volume': lead_counts,
            }
        })

    except Exception as e:
        logger.error("Demand analysis error for site %s: %s", site_id, e)
        return jsonify({'error': 'Failed to load demand analysis data'}), 500
    finally:
        session.close()


# =============================================================================
# API: Occupancy Trend (time series for charts)
# =============================================================================

@revenue_bp.route('/api/occupancy-trend')
@login_required
@revenue_tools_access_required
def api_occupancy_trend():
    """Daily occupancy time series for Chart.js line chart."""
    site_id = request.args.get('site_id', type=int)
    days = request.args.get('days', 90, type=int)
    if not site_id:
        return jsonify({'error': 'site_id is required'}), 400

    # Clamp to valid range
    days = max(1, min(days, 365))

    session = get_pbi_session()
    try:
        rows = session.execute(text("""
            SELECT extract_date,
                   "dcUnitOccupancy", "dcAreaOccupancy", "dcEconomicOccupancy",
                   "dcActualRevenue", "dcPotentialRevenue"
            FROM mgmt_occupancy_statistics
            WHERE "SiteID" = :site_id
              AND extract_date >= CURRENT_DATE - :days
            ORDER BY extract_date
        """), {'site_id': site_id, 'days': days}).fetchall()

        trend = []
        for row in rows:
            trend.append({
                'date': row[0].isoformat() if row[0] else None,
                'unit_occupancy': float(row[1]) if row[1] else None,
                'area_occupancy': float(row[2]) if row[2] else None,
                'economic_occupancy': float(row[3]) if row[3] else None,
                'actual_revenue': float(row[4]) if row[4] else None,
                'potential_revenue': float(row[5]) if row[5] else None,
            })

        return jsonify({'status': 'success', 'data': {'trend': trend}})

    except Exception as e:
        logger.error("Occupancy trend error for site %s: %s", site_id, e)
        return jsonify({'error': 'Failed to load occupancy trend'}), 500
    finally:
        session.close()
