"""
Revenue Management Service — Data Access Layer
Queries esa_pbi enriched views for revenue analytics MCP tools.
"""

import asyncio
import logging
import re
import time
from datetime import date as _date
from typing import Optional, List, Dict, Any

import asyncpg

from mcp_esa.config.database_presets import get_database_presets

logger = logging.getLogger(__name__)

_pbi_pool: Optional[asyncpg.Pool] = None
_pbi_lock = asyncio.Lock()

_SITE_CODE_RE = re.compile(r'^[A-Z]\d{3}$')


def _to_float(val) -> Optional[float]:
    if val is None:
        return None
    return float(val)


def _to_iso(val) -> Optional[str]:
    if val is None:
        return None
    return val.isoformat() if hasattr(val, 'isoformat') else str(val)


def validate_site_code(site_code: Optional[str]) -> Optional[str]:
    if site_code is None:
        return None
    if not _SITE_CODE_RE.match(site_code):
        raise ValueError(f"Invalid site_code format: must match [A-Z]NNN")
    return site_code


def validate_date(val: Optional[str], param_name: str = 'date') -> Optional[str]:
    if not val:
        return None
    try:
        _date.fromisoformat(val)
    except ValueError:
        raise ValueError(f"Invalid {param_name}: must be YYYY-MM-DD")
    return val


async def _get_pool() -> asyncpg.Pool:
    global _pbi_pool
    if _pbi_pool is not None:
        return _pbi_pool
    async with _pbi_lock:
        if _pbi_pool is not None:
            return _pbi_pool
        presets = get_database_presets()
        config = presets.get_preset('esa_pbi')
        if not config:
            raise Exception("esa_pbi database preset not configured")
        _pbi_pool = await asyncpg.create_pool(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
            ssl='require' if config.ssl else 'prefer',
            min_size=2,
            max_size=8,
        )
        logger.info("Revenue service asyncpg pool created (esa_pbi)")
        return _pbi_pool


async def _query(sql: str, params: list = None) -> List[dict]:
    pool = await _get_pool()
    params = params or []
    start = time.time()
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
    elapsed = round((time.time() - start) * 1000, 1)
    logger.debug(f"Revenue query: {len(rows)} rows in {elapsed}ms")
    return [dict(r) for r in rows]


async def get_portfolio_snapshot(extract_date: str = None) -> dict:
    extract_date = validate_date(extract_date, 'extract_date')
    rows = await _query("""
        WITH latest AS (
            SELECT COALESCE($1::date, MAX(extract_date)) AS dt
            FROM rentroll_enriched
        )
        SELECT
            l.dt AS extract_date,
            COUNT(*) FILTER (WHERE r."bRentable" = true) AS total_units,
            COUNT(*) FILTER (WHERE r."bRented" = true AND r."bRentable" = true) AS occupied,
            COUNT(*) FILTER (WHERE r."bRented" = false AND r."bRentable" = true) AS vacant,
            SUM(r.dcarea_fixed) FILTER (WHERE r."bRentable" = true) AS total_area,
            SUM(r.dcarea_fixed) FILTER (WHERE r."bRented" = true AND r."bRentable" = true) AS rented_area,
            SUM(r.revenue_effective) FILTER (WHERE r."bRented" = true) AS total_revenue,
            AVG(r.revenue_effective / NULLIF(r.dcarea_fixed, 0))
                FILTER (WHERE r."bRented" = true AND r.dcarea_fixed > 0) AS avg_inplace_sqft,
            AVG(r."dcStdRate" / NULLIF(r.dcarea_fixed, 0))
                FILTER (WHERE r."bRentable" = true AND r.dcarea_fixed > 0) AS avg_std_sqft,
            COUNT(*) FILTER (WHERE r."bRented" = true AND r."InsuranceRevenue" > 0) AS insured_units,
            COUNT(*) FILTER (WHERE r."bRented" = true AND r.disc_dcdiscount > 0) AS discounted_units
        FROM rentroll_enriched r
        JOIN latest l ON r.extract_date = l.dt
        GROUP BY l.dt
    """, [extract_date])

    if not rows:
        return {}

    r = rows[0]
    total = r['total_units'] or 0
    occupied = r['occupied'] or 0
    total_area = _to_float(r['total_area']) or 0
    rented_area = _to_float(r['rented_area']) or 0
    total_revenue = _to_float(r['total_revenue']) or 0

    return {
        'extract_date': _to_iso(r['extract_date']),
        'total_units': total,
        'occupied': occupied,
        'vacant': r['vacant'] or 0,
        'occ_pct_unit': round(occupied / total * 100, 2) if total else None,
        'occ_pct_area': round(rented_area / total_area * 100, 2) if total_area else None,
        'total_revenue': round(total_revenue, 2),
        'revpas': round(total_revenue / total_area, 2) if total_area else None,
        'avg_inplace_sqft': _to_float(r['avg_inplace_sqft']),
        'avg_std_sqft': _to_float(r['avg_std_sqft']),
        'insurance_penetration': round((r['insured_units'] or 0) / occupied * 100, 2) if occupied else None,
        'discount_penetration': round((r['discounted_units'] or 0) / occupied * 100, 2) if occupied else None,
    }


async def get_site_performance(extract_date: str = None, site_code: str = None) -> List[dict]:
    extract_date = validate_date(extract_date, 'extract_date')
    site_code = validate_site_code(site_code)
    rows = await _query("""
        WITH latest AS (
            SELECT COALESCE($1::date, MAX(extract_date)) AS dt
            FROM rentroll_enriched
        )
        SELECT
            s."SiteCode" AS site_code,
            s."Name" AS name,
            s."Country" AS country,
            COUNT(*) FILTER (WHERE r."bRentable" = true) AS total_units,
            COUNT(*) FILTER (WHERE r."bRented" = true AND r."bRentable" = true) AS occupied,
            COUNT(*) FILTER (WHERE r."bRented" = false AND r."bRentable" = true) AS vacant,
            SUM(r.dcarea_fixed) FILTER (WHERE r."bRentable" = true) AS total_area,
            SUM(r.dcarea_fixed) FILTER (WHERE r."bRented" = true AND r."bRentable" = true) AS rented_area,
            SUM(r.revenue_effective) FILTER (WHERE r."bRented" = true) AS revenue,
            AVG(r.revenue_effective / NULLIF(r.dcarea_fixed, 0))
                FILTER (WHERE r."bRented" = true AND r.dcarea_fixed > 0) AS avg_inplace_sqft
        FROM rentroll_enriched r
        JOIN latest l ON r.extract_date = l.dt
        JOIN siteinfo s ON s."SiteID" = r."SiteID"
        WHERE ($2::text IS NULL OR s."SiteCode" = $2)
        GROUP BY s."SiteCode", s."Name", s."Country"
        ORDER BY s."SiteCode"
    """, [extract_date, site_code])

    result = []
    for r in rows:
        total = r['total_units'] or 0
        occupied = r['occupied'] or 0
        total_area = _to_float(r['total_area']) or 0
        rented_area = _to_float(r['rented_area']) or 0
        revenue = _to_float(r['revenue']) or 0
        result.append({
            'site_code': r['site_code'],
            'name': r['name'],
            'country': r['country'],
            'total_units': total,
            'occupied': occupied,
            'vacant': r['vacant'] or 0,
            'occ_pct_unit': round(occupied / total * 100, 2) if total else None,
            'occ_pct_area': round(rented_area / total_area * 100, 2) if total_area else None,
            'revenue': round(revenue, 2),
            'revpas': round(revenue / total_area, 2) if total_area else None,
            'avg_inplace_sqft': _to_float(r['avg_inplace_sqft']),
        })
    return result


async def get_budget_variance(month: str = None, site_code: str = None) -> List[dict]:
    month = validate_date(month, 'month')
    site_code = validate_site_code(site_code)
    rows = await _query("""
        WITH target_month AS (
            SELECT COALESCE($1::date, DATE_TRUNC('month', CURRENT_DATE)::date) AS m
        ),
        actuals AS (
            SELECT
                s."SiteCode" AS site_code,
                SUM(r.revenue_effective) FILTER (WHERE r."bRented" = true) AS actual_revenue,
                COUNT(*) FILTER (WHERE r."bRentable" = true) AS total_units,
                COUNT(*) FILTER (WHERE r."bRented" = true AND r."bRentable" = true) AS occupied
            FROM rentroll_enriched r
            JOIN siteinfo s ON s."SiteID" = r."SiteID"
            WHERE r.extract_date = (SELECT MAX(extract_date) FROM rentroll_enriched)
            GROUP BY s."SiteCode"
        )
        SELECT
            a.site_code,
            a.actual_revenue,
            b.rental_revenue AS budget_revenue,
            a.occupied,
            a.total_units,
            b.occupancy_pct AS budget_occ
        FROM actuals a
        LEFT JOIN vw_budget_monthly b
            ON b.site_code = a.site_code
            AND b.date = (SELECT m FROM target_month)
        WHERE b.rental_revenue IS NOT NULL
          AND ($2::text IS NULL OR b.site_code = $2)
        ORDER BY a.site_code
    """, [month, site_code])

    result = []
    for r in rows:
        actual = _to_float(r['actual_revenue']) or 0
        budget = _to_float(r['budget_revenue']) or 0
        total = r['total_units'] or 0
        occupied = r['occupied'] or 0
        actual_occ = round(occupied / total * 100, 2) if total else None
        result.append({
            'site_code': r['site_code'],
            'actual_revenue': round(actual, 2),
            'budget_revenue': round(budget, 2),
            'variance_pct': round((actual - budget) / budget * 100, 2) if budget else None,
            'achievement_pct': round(actual / budget * 100, 2) if budget else None,
            'actual_occ': actual_occ,
            'budget_occ': _to_float(r['budget_occ']),
        })
    return result


async def get_occupancy_trends(days: int = 90, site_code: str = None) -> List[dict]:
    days = max(1, min(int(days), 365))
    site_code = validate_site_code(site_code)


    if site_code:
        rows = await _query("""
            SELECT
                r.extract_date,
                ROUND(COUNT(*) FILTER (WHERE r."bRented" = true AND r."bRentable" = true)::numeric /
                    NULLIF(COUNT(*) FILTER (WHERE r."bRentable" = true), 0) * 100, 2) AS unit_occ,
                ROUND(SUM(r.dcarea_fixed) FILTER (WHERE r."bRented" = true AND r."bRentable" = true)::numeric /
                    NULLIF(SUM(r.dcarea_fixed) FILTER (WHERE r."bRentable" = true), 0) * 100, 2) AS area_occ,
                ROUND(SUM(r.revenue_effective) FILTER (WHERE r."bRented" = true)::numeric /
                    NULLIF(SUM(r."dcStdRate") FILTER (WHERE r."bRentable" = true), 0) * 100, 2) AS economic_occ,
                SUM(r.revenue_effective) FILTER (WHERE r."bRented" = true) AS actual_revenue,
                SUM(r."dcStdRate") FILTER (WHERE r."bRentable" = true) AS potential_revenue
            FROM rentroll_enriched r
            JOIN siteinfo s ON s."SiteID" = r."SiteID"
            WHERE s."SiteCode" = $1
              AND r.extract_date >= CURRENT_DATE - $2 * INTERVAL '1 day'
            GROUP BY r.extract_date
            ORDER BY r.extract_date
        """, [site_code, days])
    else:
        rows = await _query("""
            SELECT
                r.extract_date,
                ROUND(COUNT(*) FILTER (WHERE r."bRented" = true AND r."bRentable" = true)::numeric /
                    NULLIF(COUNT(*) FILTER (WHERE r."bRentable" = true), 0) * 100, 2) AS unit_occ,
                ROUND(SUM(r.dcarea_fixed) FILTER (WHERE r."bRented" = true AND r."bRentable" = true)::numeric /
                    NULLIF(SUM(r.dcarea_fixed) FILTER (WHERE r."bRentable" = true), 0) * 100, 2) AS area_occ,
                ROUND(SUM(r.revenue_effective) FILTER (WHERE r."bRented" = true)::numeric /
                    NULLIF(SUM(r."dcStdRate") FILTER (WHERE r."bRentable" = true), 0) * 100, 2) AS economic_occ,
                SUM(r.revenue_effective) FILTER (WHERE r."bRented" = true) AS actual_revenue,
                SUM(r."dcStdRate") FILTER (WHERE r."bRentable" = true) AS potential_revenue
            FROM rentroll_enriched r
            WHERE r.extract_date >= CURRENT_DATE - $1 * INTERVAL '1 day'
            GROUP BY r.extract_date
            ORDER BY r.extract_date
        """, [days])

    return [
        {
            'date': _to_iso(r['extract_date']),
            'unit_occ': _to_float(r['unit_occ']),
            'area_occ': _to_float(r['area_occ']),
            'economic_occ': _to_float(r['economic_occ']),
            'actual_revenue': _to_float(r['actual_revenue']),
            'potential_revenue': _to_float(r['potential_revenue']),
        }
        for r in rows
    ]


async def get_movement_analysis(days: int = 30, site_code: str = None) -> dict:
    days = max(1, min(int(days), 365))
    site_code = validate_site_code(site_code)


    if site_code:
        rows = await _query("""
            SELECT
                COUNT(*) FILTER (WHERE m."MoveIn" = 1) AS total_mi,
                COUNT(*) FILTER (WHERE m."MoveOut" = 1) AS total_mo,
                AVG(m."MovedInRentalRate" / NULLIF(m."MovedInArea_fixed", 0))
                    FILTER (WHERE m."MoveIn" = 1 AND m."MovedInArea_fixed" > 0) AS avg_mi_rate_sqft,
                AVG(m."MovedInDaysVacant")
                    FILTER (WHERE m."MoveIn" = 1) AS avg_days_vacant_before_mi,
                AVG(m."MovedOutDaysRented")
                    FILTER (WHERE m."MoveOut" = 1 AND m."MovedOutDaysRented" > 0) AS avg_los_at_mo
            FROM mimo_enriched m
            JOIN siteinfo s ON s."SiteID" = m."SiteID"
            WHERE s."SiteCode" = $1
              AND m."MoveDate" >= CURRENT_DATE - $2 * INTERVAL '1 day'
        """, [site_code, days])
    else:
        rows = await _query("""
            SELECT
                COUNT(*) FILTER (WHERE m."MoveIn" = 1) AS total_mi,
                COUNT(*) FILTER (WHERE m."MoveOut" = 1) AS total_mo,
                AVG(m."MovedInRentalRate" / NULLIF(m."MovedInArea_fixed", 0))
                    FILTER (WHERE m."MoveIn" = 1 AND m."MovedInArea_fixed" > 0) AS avg_mi_rate_sqft,
                AVG(m."MovedInDaysVacant")
                    FILTER (WHERE m."MoveIn" = 1) AS avg_days_vacant_before_mi,
                AVG(m."MovedOutDaysRented")
                    FILTER (WHERE m."MoveOut" = 1 AND m."MovedOutDaysRented" > 0) AS avg_los_at_mo
            FROM mimo_enriched m
            WHERE m."MoveDate" >= CURRENT_DATE - $1 * INTERVAL '1 day'
        """, [days])

    if not rows:
        return {}

    r = rows[0]
    mi = r['total_mi'] or 0
    mo = r['total_mo'] or 0
    return {
        'total_move_ins': mi,
        'total_move_outs': mo,
        'net_absorption': mi - mo,
        'avg_mi_rate_sqft': _to_float(r['avg_mi_rate_sqft']),
        'avg_days_vacant_before_mi': _to_float(r['avg_days_vacant_before_mi']),
        'avg_los_at_moveout': _to_float(r['avg_los_at_mo']),
    }


async def get_rate_analysis(extract_date: str = None, site_code: str = None) -> List[dict]:
    extract_date = validate_date(extract_date, 'extract_date')
    site_code = validate_site_code(site_code)
    rows = await _query("""
        WITH latest AS (
            SELECT COALESCE($1::date, MAX(extract_date)) AS dt FROM rentroll_enriched
        )
        SELECT
            ui.label_type_code,
            ui.label_climate_code,
            ui.label_size_range,
            COUNT(*) FILTER (WHERE r."bRented" = true) AS occ_count,
            COUNT(*) FILTER (WHERE r."bRented" = false) AS vac_count,
            AVG(r.revenue_effective / NULLIF(r.dcarea_fixed, 0))
                FILTER (WHERE r."bRented" = true AND r.dcarea_fixed > 0) AS avg_inplace_sqft,
            AVG(r."dcStdRate" / NULLIF(r.dcarea_fixed, 0))
                FILTER (WHERE r.dcarea_fixed > 0) AS avg_std_sqft,
            AVG(r.disc_dcdiscount / NULLIF(r.disc_dcprice, 0) * 100)
                FILTER (WHERE r."bRented" = true AND r.disc_dcdiscount > 0) AS avg_discount_pct,
            AVG(r."iDaysVacant") FILTER (WHERE r."bRented" = false) AS avg_days_vacant
        FROM rentroll_enriched r
        JOIN latest l ON r.extract_date = l.dt
        LEFT JOIN units_info_enriched ui
            ON ui."SiteID" = r."SiteID" AND ui."UnitID" = r."UnitID"
        JOIN siteinfo s ON s."SiteID" = r."SiteID"
        WHERE r."bRentable" = true
          AND ($2::text IS NULL OR s."SiteCode" = $2)
        GROUP BY ui.label_type_code, ui.label_climate_code, ui.label_size_range
        ORDER BY ui.label_type_code, ui.label_climate_code, ui.label_size_range
    """, [extract_date, site_code])

    return [
        {
            'label_type_code': r['label_type_code'],
            'label_climate_code': r['label_climate_code'],
            'label_size_range': r['label_size_range'],
            'occ_count': r['occ_count'] or 0,
            'vac_count': r['vac_count'] or 0,
            'avg_inplace_sqft': _to_float(r['avg_inplace_sqft']),
            'avg_std_sqft': _to_float(r['avg_std_sqft']),
            'avg_discount_pct': _to_float(r['avg_discount_pct']),
            'avg_days_vacant': _to_float(r['avg_days_vacant']),
        }
        for r in rows
    ]


async def get_customer_segments(extract_date: str = None, site_code: str = None) -> List[dict]:
    extract_date = validate_date(extract_date, 'extract_date')
    site_code = validate_site_code(site_code)
    rows = await _query("""
        WITH latest AS (
            SELECT COALESCE($1::date, MAX(extract_date)) AS dt FROM rentroll_enriched
        )
        SELECT
            r.los_range,
            COUNT(*) AS tenant_count,
            SUM(r.dcarea_fixed) AS total_area,
            AVG(r.revenue_effective / NULLIF(r.dcarea_fixed, 0)) FILTER (WHERE r.dcarea_fixed > 0) AS avg_rate_sqft,
            COUNT(*) FILTER (WHERE r.disc_dcdiscount > 0) AS discounted_count,
            AVG(r.disc_dcdiscount / NULLIF(r.disc_dcprice, 0) * 100)
                FILTER (WHERE r.disc_dcdiscount > 0) AS avg_discount_pct,
            SUM(r.revenue_effective) AS total_rent
        FROM rentroll_enriched r
        JOIN latest l ON r.extract_date = l.dt
        JOIN siteinfo s ON s."SiteID" = r."SiteID"
        WHERE r."bRented" = true AND r."bRentable" = true AND r."LedgerID" > 0
          AND ($2::text IS NULL OR s."SiteCode" = $2)
        GROUP BY r.los_range
        ORDER BY MIN(r.days_rented)
    """, [extract_date, site_code])

    return [
        {
            'los_range': r['los_range'],
            'tenant_count': r['tenant_count'] or 0,
            'total_area': _to_float(r['total_area']),
            'avg_rate_sqft': _to_float(r['avg_rate_sqft']),
            'discounted_count': r['discounted_count'] or 0,
            'avg_discount_pct': _to_float(r['avg_discount_pct']),
            'total_rent': _to_float(r['total_rent']),
        }
        for r in rows
    ]


async def get_anomalies(extract_date: str = None, site_code: str = None) -> dict:


    # 1. Occupancy drops WoW > 2pp
    occ_drops = await _query("""
        WITH occ_by_site AS (
            SELECT
                r."SiteID",
                r.extract_date,
                ROUND(COUNT(*) FILTER (WHERE r."bRented" = true AND r."bRentable" = true)::numeric /
                    NULLIF(COUNT(*) FILTER (WHERE r."bRentable" = true), 0) * 100, 2) AS occ
            FROM rentroll_enriched r
            WHERE r.extract_date IN (
                (SELECT MAX(extract_date) FROM rentroll_enriched),
                (SELECT MAX(extract_date) FROM rentroll_enriched WHERE extract_date <= CURRENT_DATE - 7)
            )
            GROUP BY r."SiteID", r.extract_date
        ),
        current_occ AS (
            SELECT o."SiteID", s."SiteCode", s."Name", o.occ
            FROM occ_by_site o
            JOIN siteinfo s ON s."SiteID" = o."SiteID"
            WHERE o.extract_date = (SELECT MAX(extract_date) FROM rentroll_enriched)
        ),
        prev_occ AS (
            SELECT o."SiteID", o.occ
            FROM occ_by_site o
            WHERE o.extract_date = (SELECT MAX(extract_date) FROM rentroll_enriched WHERE extract_date <= CURRENT_DATE - 7)
        )
        SELECT c."SiteCode" AS site_code, c."Name" AS name,
               c.occ AS current_occ_pct, p.occ AS prev_occ_pct,
               ROUND((c.occ - p.occ)::numeric, 2) AS change_pct
        FROM current_occ c
        JOIN prev_occ p ON p."SiteID" = c."SiteID"
        WHERE (c.occ - p.occ) < -2
        ORDER BY (c.occ - p.occ)
    """)

    # 2. Revenue below budget > 10%
    revenue_concerns = await _query("""
        WITH actuals AS (
            SELECT s."SiteCode" AS site_code,
                   SUM(r.revenue_effective) FILTER (WHERE r."bRented" = true) AS actual_revenue
            FROM rentroll_enriched r
            JOIN siteinfo s ON s."SiteID" = r."SiteID"
            WHERE r.extract_date = (SELECT MAX(extract_date) FROM rentroll_enriched)
            GROUP BY s."SiteCode"
        )
        SELECT a.site_code,
               a.actual_revenue,
               b.rental_revenue AS budget_revenue,
               ROUND(((a.actual_revenue - b.rental_revenue) / NULLIF(b.rental_revenue, 0) * 100)::numeric, 2) AS variance_pct
        FROM actuals a
        JOIN vw_budget_monthly b
            ON b.site_code = a.site_code
            AND b.date = DATE_TRUNC('month', CURRENT_DATE)::date
        WHERE b.rental_revenue > 0
          AND (a.actual_revenue - b.rental_revenue) / b.rental_revenue < -0.10
        ORDER BY (a.actual_revenue - b.rental_revenue) / b.rental_revenue
    """)

    # 3. Long-vacant concentration (>30% of vacant units 60d+)
    vacancy_spikes = await _query("""
        WITH latest AS (
            SELECT MAX(extract_date) AS dt FROM rentroll_enriched
        )
        SELECT s."SiteCode" AS site_code, s."Name" AS name,
               COUNT(*) FILTER (WHERE r."bRented" = false) AS total_vacant,
               COUNT(*) FILTER (WHERE r."bRented" = false AND r."iDaysVacant" >= 60) AS long_vacant,
               ROUND(
                   COUNT(*) FILTER (WHERE r."bRented" = false AND r."iDaysVacant" >= 60)::numeric /
                   NULLIF(COUNT(*) FILTER (WHERE r."bRented" = false), 0) * 100, 2
               ) AS long_vacant_pct
        FROM rentroll_enriched r
        JOIN latest l ON r.extract_date = l.dt
        JOIN siteinfo s ON s."SiteID" = r."SiteID"
        WHERE r."bRentable" = true
        GROUP BY s."SiteCode", s."Name"
        HAVING COUNT(*) FILTER (WHERE r."bRented" = false) > 0
           AND COUNT(*) FILTER (WHERE r."bRented" = false AND r."iDaysVacant" >= 60)::numeric /
               NULLIF(COUNT(*) FILTER (WHERE r."bRented" = false), 0) > 0.30
        ORDER BY long_vacant_pct DESC
    """)

    # 4. High discount penetration (>40%)
    discount_alerts = await _query("""
        WITH latest AS (
            SELECT MAX(extract_date) AS dt FROM rentroll_enriched
        )
        SELECT s."SiteCode" AS site_code, s."Name" AS name,
               ROUND(
                   COUNT(*) FILTER (WHERE r."bRented" = true AND r.disc_dcdiscount > 0)::numeric /
                   NULLIF(COUNT(*) FILTER (WHERE r."bRented" = true), 0) * 100, 2
               ) AS discount_penetration_pct,
               ROUND(AVG(r.disc_dcdiscount / NULLIF(r.disc_dcprice, 0) * 100)
                   FILTER (WHERE r."bRented" = true AND r.disc_dcdiscount > 0)::numeric, 2
               ) AS avg_discount_pct
        FROM rentroll_enriched r
        JOIN latest l ON r.extract_date = l.dt
        JOIN siteinfo s ON s."SiteID" = r."SiteID"
        WHERE r."bRentable" = true
        GROUP BY s."SiteCode", s."Name"
        HAVING COUNT(*) FILTER (WHERE r."bRented" = true AND r.disc_dcdiscount > 0)::numeric /
               NULLIF(COUNT(*) FILTER (WHERE r."bRented" = true), 0) > 0.40
        ORDER BY discount_penetration_pct DESC
    """)

    return {
        'occ_drops': [
            {'site_code': r['site_code'], 'name': r['name'],
             'current_occ_pct': _to_float(r['current_occ_pct']),
             'prev_occ_pct': _to_float(r['prev_occ_pct']),
             'change_pct': _to_float(r['change_pct'])}
            for r in occ_drops
        ],
        'revenue_concerns': [
            {'site_code': r['site_code'],
             'actual_revenue': _to_float(r['actual_revenue']),
             'budget_revenue': _to_float(r['budget_revenue']),
             'variance_pct': _to_float(r['variance_pct'])}
            for r in revenue_concerns
        ],
        'vacancy_spikes': [
            {'site_code': r['site_code'], 'name': r['name'],
             'total_vacant': r['total_vacant'],
             'long_vacant': r['long_vacant'],
             'long_vacant_pct': _to_float(r['long_vacant_pct'])}
            for r in vacancy_spikes
        ],
        'discount_alerts': [
            {'site_code': r['site_code'], 'name': r['name'],
             'discount_penetration_pct': _to_float(r['discount_penetration_pct']),
             'avg_discount_pct': _to_float(r['avg_discount_pct'])}
            for r in discount_alerts
        ],
    }
