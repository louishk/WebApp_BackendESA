#!/usr/bin/env python3
"""
Pricing Recalibration — Generate CSV of current vs target pricing per unit.

Takes site-level budget ARR/sqft, distributes across unit categories using
a -log yield curve and climate multipliers, outputs one row per unit.

Usage:
    cd backend/python
    python -m scripts.pricing_recalibration
    python -m scripts.pricing_recalibration --dry-run   # print stats only, no CSV
"""

import argparse
import csv
import logging
import math
import sys
from collections import defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ── Configurable Parameters ──────────────────────────────────────────────────

# Current market discounts (for "book rate" = std x (1 - disc%))
CURRENT_DISCOUNT = {
    'Singapore': 0.35,
    'Malaysia': 0.35,
    'South Korea': 0.50,
}

# Future uniform discount (new_std x (1 - this) = target ARR)
FUTURE_DISCOUNT = 0.30

# Climate multipliers (NC baseline = 1.0, ascending: NC < D < A < AD < RF)
CLIMATE_MATRIX = {
    'NC': 1.00,
    'D': 1.15,
    'A': 1.20,
    'AD': 1.25,
    'RF': 1.30,
}

# Currency rounding: decimal places per currency
CURRENCY_DECIMALS = {
    'SGD': 2,
    'MYR': 2,
    'KRW': 0,
    'HKD': 2,
}

# sqft to sqm conversion factor
SQFT_TO_SQM = 1 / 10.7639

# ── End Configurable Parameters ──────────────────────────────────────────────

# Project root (anchored to this file's known location in backend/python/scripts/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent

QUERY = text("""
    SELECT
        ui."SiteID"             AS site_id,
        si."SiteCode"           AS site_code,
        si."Name"               AS site_name,
        si."Country"            AS country,
        b.currency              AS currency,
        ui."UnitID"             AS unit_id,
        ui."sUnitName"          AS s_unit,
        ui.dcarea_fixed         AS area_sqft,
        ui."iFloor"             AS floor,
        ui."sTypeName"          AS s_type_name,
        ui.category_label       AS final_label,
        ui.label_size_category  AS size_category,
        ui.label_size_range     AS size_range,
        ui.label_type_code      AS unit_type_code,
        ui.label_climate_code   AS climate_code,
        ui.label_shape          AS shape,
        ui.label_pillar         AS pillar,
        ui."bRented"            AS is_rented_flag,
        ui."dcStdRate"          AS current_std_rate,
        ui."dcWebRate"          AS web_rate,
        ui."dcPushRate"         AS push_rate,
        ui."iDaysVacant"        AS days_vacant,
        rr."LedgerID"          AS ledger_id,
        rr."sTenant"           AS tenant_name,
        rr."dLeaseDate"        AS moved_in_date,
        rr.days_rented,
        rr.los_range,
        rr.revenue_effective   AS actual_rent,
        b.avr_rental_rate      AS budget_arr_sqft,
        COUNT(*) OVER (PARTITION BY ui."SiteID", ui."UnitID") AS tenant_count
    FROM units_info_enriched ui
    INNER JOIN siteinfo si
        ON si."SiteID" = ui."SiteID"
    LEFT JOIN vw_budget_monthly b
        ON b.site_code = si."SiteCode"
        AND b.date = date_trunc('month', CURRENT_DATE)
    LEFT JOIN rentroll_enriched rr
        ON rr."SiteID" = ui."SiteID"
        AND rr."UnitID" = ui."UnitID"
        AND rr.extract_date = (SELECT MAX(extract_date) FROM rentroll_enriched)
        AND rr."bRented" = true
    WHERE ui.dcarea_fixed > 0
      AND ui.deleted_at IS NULL
      AND ui.label_type_code NOT IN ('P', 'ST', 'SC')
      AND ui."sTypeName" NOT ILIKE '%%mail%%box%%'
      AND ui."sTypeName" NOT ILIKE '%%parking%%'
      AND ui."sTypeName" NOT ILIKE '%%car%%park%%'
      AND ui."sTypeName" NOT ILIKE '%%bizplus%%'
      AND b.avr_rental_rate IS NOT NULL
    ORDER BY ui."SiteID", ui."UnitID"
""")


def get_engine():
    """Create SQLAlchemy engine for esa_pbi database."""
    url = get_database_url('pbi')
    return create_engine(url)


def fetch_data(engine):
    """Fetch all occupied storage units with labels, site info, and budget."""
    logger.info('Fetching unit data...')
    with engine.connect() as conn:
        result = conn.execute(QUERY)
        rows = [dict(r._mapping) for r in result]

    # Convert Decimal values to float for arithmetic compatibility
    for r in rows:
        for k, v in r.items():
            if isinstance(v, Decimal):
                r[k] = float(v)

    if not rows:
        return rows

    # Log summary
    sites = set(r['site_id'] for r in rows)
    logger.info(f'Fetched {len(rows)} units across {len(sites)} sites')

    # Warn about unknown climate codes
    known_climates = set(CLIMATE_MATRIX.keys())
    found_climates = set(r['climate_code'] for r in rows if r['climate_code'])
    unknown = found_climates - known_climates
    if unknown:
        logger.warning(f'Unknown climate codes (will use multiplier 1.0): {unknown}')

    return rows


def fit_yield_curves(rows):
    """
    Fit a -log yield curve per site from current pricing.

    Returns dict: site_id -> (a, b) coefficients where rate = a - b * ln(area).
    """
    # Group by site_id + size_range to build buckets
    # Use deduplicated NLA (divide by tenant_count) to avoid multi-tenant overcounting
    site_buckets = defaultdict(lambda: defaultdict(lambda: {'nla': 0.0, 'weighted_rate': 0.0, 'count': 0}))

    for r in rows:
        # Only fit curve from occupied units with a std rate
        if not r['current_std_rate'] or r['current_std_rate'] <= 0:
            continue
        if not r.get('is_rented_flag'):
            continue

        disc = CURRENT_DISCOUNT.get(r['country'], 0.35)
        book_sqft = (r['current_std_rate'] / r['area_sqft']) * (1 - disc)

        bucket = site_buckets[r['site_id']][r['size_range']]
        # Deduplicate NLA for multi-tenant units
        nla_share = r['area_sqft'] / r['tenant_count']
        bucket['nla'] += nla_share
        bucket['weighted_rate'] += book_sqft * nla_share
        bucket['count'] += 1

    curves = {}
    for site_id, buckets in site_buckets.items():
        # Build regression data: x = ln(avg_area), y = avg_book_rate, w = total_nla
        points = []
        for sr, bk in buckets.items():
            if bk['nla'] > 0 and bk['count'] > 0:
                avg_area = bk['nla'] / bk['count']
                avg_rate = bk['weighted_rate'] / bk['nla']
                points.append((math.log(avg_area), avg_rate, bk['nla']))

        if len(points) < 2:
            logger.warning(f'Site {site_id}: only {len(points)} size buckets, using flat rate')
            if points:
                curves[site_id] = (points[0][1], 0.0)
            continue

        # Weighted least squares: y = a + slope * x  (slope expected negative)
        sw = sum(w for _, _, w in points)
        sx = sum(w * x for x, _, w in points)
        sy = sum(w * y for _, y, w in points)
        sxx = sum(w * x * x for x, _, w in points)
        sxy = sum(w * x * y for x, y, w in points)

        denom = sxx - (sx * sx / sw)
        if abs(denom) < 1e-10:
            curves[site_id] = (sy / sw, 0.0)
            continue

        slope = (sxy - sx * sy / sw) / denom
        intercept = (sy - slope * sx) / sw

        # rate = intercept + slope * ln(area) where slope < 0
        # store as (a, b) where rate = a - b * ln(area), b = -slope
        curves[site_id] = (intercept, -slope)

    return curves


def compute_recalibrated(rows, curves):
    """
    For each unit, compute the recalibrated target rate:
    1. Yield curve fitted rate for its area
    2. Climate multiplier
    3. Rescale so NLA-weighted site average = budget ARR/sqft
    """
    # Step 1: Compute raw fitted rate for every unit (before rescaling)
    for r in rows:
        a, b = curves.get(r['site_id'], (0, 0))
        fitted_base = a - b * math.log(r['area_sqft'])
        fitted_base = max(fitted_base, 0.01)  # floor at near-zero

        climate = r['climate_code'] or 'NC'
        r['_fitted_rate'] = fitted_base * CLIMATE_MATRIX.get(climate, 1.0)

    # Step 2: Rescale per site so NLA-weighted avg = budget ARR/sqft
    # Deduplicate NLA for multi-tenant units; count each unit once
    site_totals = defaultdict(lambda: {'weighted_fitted': 0.0, 'nla': 0.0, 'budget': 0.0})
    seen_units = set()
    for r in rows:
        unit_key = (r['site_id'], r['unit_id'])
        if unit_key in seen_units:
            continue
        seen_units.add(unit_key)

        st = site_totals[r['site_id']]
        st['weighted_fitted'] += r['_fitted_rate'] * r['area_sqft']
        st['nla'] += r['area_sqft']
        st['budget'] = float(r['budget_arr_sqft'])

    site_scale = {}
    for site_id, st in site_totals.items():
        if st['nla'] > 0 and st['weighted_fitted'] > 0:
            current_wavg = st['weighted_fitted'] / st['nla']
            site_scale[site_id] = st['budget'] / current_wavg
        else:
            site_scale[site_id] = 1.0

    # Step 3: Apply scale factor and derive final columns
    for r in rows:
        scale = site_scale.get(r['site_id'], 1.0)
        r['new_target_arr_sqft'] = r['_fitted_rate'] * scale
        r['new_std_sqft'] = r['new_target_arr_sqft'] / (1 - FUTURE_DISCOUNT)
        r['new_std_rate'] = r['new_std_sqft'] * r['area_sqft']

    # Validation: log per-site summary
    for site_id, st in sorted(site_totals.items()):
        site_rows = [r for r in rows if r['site_id'] == site_id]
        sc = site_rows[0]['site_code']
        country = site_rows[0]['country']
        currency = site_rows[0]['currency'] or '???'

        # Deduplicated wavg for validation
        seen_v = set()
        wavg_num = 0.0
        for r in site_rows:
            uk = (r['site_id'], r['unit_id'])
            if uk not in seen_v:
                seen_v.add(uk)
                wavg_num += r['new_target_arr_sqft'] * r['area_sqft']
        wavg_new = wavg_num / st['nla']
        residual = abs(wavg_new - st['budget']) / st['budget'] * 100
        rented = sum(1 for r in site_rows if r.get('ledger_id'))
        vacant = len(site_rows) - rented

        logger.info(
            f'  {sc} ({country}, {currency}): '
            f'rented={rented}, vacant={vacant}, NLA={st["nla"]:.0f}, '
            f'budget={st["budget"]:.2f}, new_wavg={wavg_new:.2f}, '
            f'residual={residual:.4f}%, scale={site_scale[site_id]:.4f}'
        )

    return rows


def write_csv(rows, output_path):
    """Write the recalibration CSV with all derived columns."""
    columns = [
        'site_id', 'site_code', 'site_name', 'country', 'currency',
        'unit_id', 's_unit',
        'area_sqft', 'area_sqm', 'floor', 's_type_name', 'days_vacant',
        'final_label', 'size_category', 'size_range', 'unit_type_code',
        'climate_code', 'shape', 'pillar',
        'is_rented',
        'ledger_id', 'tenant_name', 'moved_in_date', 'tenure_months', 'los_range',
        'is_multi_tenant', 'tenant_count', 'tenant_share_pct',
        'current_std_rate', 'current_std_sqft', 'current_std_sqm',
        'current_disc_pct',
        'current_book_rate', 'current_book_sqft', 'current_book_sqm',
        'actual_rent', 'actual_rent_sqft', 'actual_rent_sqm',
        'actual_vs_budget_pct',
        'budget_arr_sqft', 'budget_arr_sqm',
        'new_target_arr_sqft', 'new_target_arr_sqm',
        'new_std_rate', 'new_std_sqft', 'new_std_sqm',
        'change_vs_current_std_pct',
    ]

    for r in rows:
        currency = r['currency'] or 'SGD'
        dp = CURRENCY_DECIMALS.get(currency, 2)
        disc = CURRENT_DISCOUNT.get(r['country'], 0.35)
        area = r['area_sqft']
        tc = r['tenant_count']

        # Rented status
        r['is_rented'] = bool(r.get('is_rented_flag'))

        # Tenure
        r['tenure_months'] = int(r['days_rented'] / 30.44) if r['days_rented'] else 0

        # Multi-tenancy
        r['is_multi_tenant'] = tc > 1
        r['tenant_share_pct'] = round(100.0 / tc, 1) if tc else 100.0

        # Compute derived values from RAW (unrounded) intermediates
        raw_std_sqft = r['current_std_rate'] / area if area else 0
        raw_book_rate = r['current_std_rate'] * (1 - disc)
        raw_book_sqft = raw_book_rate / area if area else 0
        raw_actual = float(r['actual_rent'] or 0)
        raw_actual_sqft = raw_actual / area if area else 0
        raw_budget = float(r['budget_arr_sqft'] or 0)

        # Change % computed from raw values (not rounded)
        if raw_std_sqft > 0:
            r['change_vs_current_std_pct'] = round(
                (r['new_std_sqft'] - raw_std_sqft) / raw_std_sqft * 100, 1
            )
        else:
            r['change_vs_current_std_pct'] = 0

        # Sqm columns (KR uses sqm as primary, SG/MY get sqft primary with sqm ref)
        area_sqm = area * SQFT_TO_SQM
        r['area_sqm'] = round(area_sqm, 2)

        # Per-sqm rates (= per-sqft rate / SQFT_TO_SQM = per-sqft rate * 10.7639)
        sqm_factor = 1 / SQFT_TO_SQM  # ~10.7639
        r['current_std_sqm'] = round(raw_std_sqft * sqm_factor, dp)
        r['current_book_sqm'] = round(raw_book_sqft * sqm_factor, dp)
        r['actual_rent_sqm'] = round(raw_actual_sqft * sqm_factor, dp)
        r['budget_arr_sqm'] = round(raw_budget * sqm_factor, dp)
        r['new_target_arr_sqm'] = round(r['new_target_arr_sqft'] * sqm_factor, dp)
        r['new_std_sqm'] = round(r['new_std_sqft'] * sqm_factor, dp)

        # Now round sqft values for output
        r['current_std_sqft'] = round(raw_std_sqft, dp)
        r['current_disc_pct'] = disc
        r['current_book_rate'] = round(raw_book_rate, dp)
        r['current_book_sqft'] = round(raw_book_sqft, dp)
        r['actual_rent_sqft'] = round(raw_actual_sqft, dp)
        r['actual_vs_budget_pct'] = round(raw_actual_sqft / raw_budget * 100, 1) if raw_budget else 0
        r['new_target_arr_sqft'] = round(r['new_target_arr_sqft'], dp)
        r['new_std_rate'] = round(r['new_std_rate'], dp)
        r['new_std_sqft'] = round(r['new_std_sqft'], dp)
        r['current_std_rate'] = round(r['current_std_rate'], dp)
        r['actual_rent'] = round(raw_actual, dp)
        r['budget_arr_sqft'] = round(raw_budget, dp)
        r['area_sqft'] = round(area, 1)

    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f'CSV written: {output_path} ({len(rows)} rows)')


def main():
    parser = argparse.ArgumentParser(description='Pricing recalibration CSV generator')
    parser.add_argument('--dry-run', action='store_true', help='Print stats only, no CSV')
    parser.add_argument('--output', type=str, default=None, help='Custom output path')
    args = parser.parse_args()

    engine = get_engine()
    logger.info('Connected to esa_pbi')

    # Fetch data
    rows = fetch_data(engine)
    if not rows:
        logger.error('No data returned — check budget and label coverage')
        sys.exit(1)

    # Fit yield curves
    logger.info('Fitting yield curves...')
    curves = fit_yield_curves(rows)
    for site_id, (a, b) in sorted(curves.items()):
        sc = next(r['site_code'] for r in rows if r['site_id'] == site_id)
        logger.info(f'  {sc}: rate = {a:.4f} - {b:.4f} * ln(area)')

    # Compute recalibrated rates
    logger.info('Computing recalibrated rates...')
    rows = compute_recalibrated(rows, curves)

    if args.dry_run:
        logger.info('Dry run — no CSV written')
        return

    # Write CSV
    if args.output:
        output_path = Path(args.output)
    else:
        output_dir = PROJECT_ROOT / 'output'
        output_dir.mkdir(exist_ok=True)
        output_path = output_dir / f'pricing_recalibration_{date.today().isoformat()}.csv'

    write_csv(rows, output_path)


if __name__ == '__main__':
    main()
