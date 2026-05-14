"""
Pricing Anomalies API — surfaces unit category combinations where in-place
$/sqft deviates meaningfully from peer categories in the same country.

Peer group: same (size_category, size_range, type_code, climate_code, country),
varying shape/pillar. Deviation flagged at ±8% with occupancy guard.
"""

import logging

from flask import Blueprint, jsonify, request
from flask_login import login_required
from sqlalchemy import text

from web.auth.decorators import revenue_tools_access_required
from web.auth.jwt_auth import require_auth, require_api_scope
from web.utils.rate_limit import rate_limit_api

logger = logging.getLogger(__name__)

pricing_anomalies_bp = Blueprint(
    'pricing_anomalies', __name__, url_prefix='/api/pricing-anomalies'
)

# ---------------------------------------------------------------------------
# PBI session (lazy, shared with revenue.py pattern)
# ---------------------------------------------------------------------------

def _get_pbi_session():
    from flask import current_app
    return current_app.get_pbi_session()


# ---------------------------------------------------------------------------
# type_group → label_type_code mapping
# Walk-In: W and variants; Locker: L and variants; Wine: WN variants; Others: rest
# ---------------------------------------------------------------------------

_TYPE_GROUP_CODES = {
    'Walk-In': {'W', 'E', 'S'},
    'Locker':  {'U', 'M', 'L', 'SU', 'SM', 'SL', 'EU', 'EM', 'EL'},
    'Wine':    {'WN', 'WNU', 'WNM', 'WNL', 'SWN', 'SWNU', 'SWNM', 'SWNL'},
    'Others':  {'DV', 'RB', 'MB', 'BZ', 'SC', 'SB', 'PR'},
}

# All known codes; used to build the "Others" IN list dynamically
_ALL_KNOWN_CODES = set().union(*_TYPE_GROUP_CODES.values())


def _type_codes_for_groups(groups: list[str]) -> list[str] | None:
    """Return list of label_type_codes for the requested type_group values,
    or None if no type_group filter was requested."""
    codes: set[str] = set()
    want_others = 'Others' in groups
    for g in groups:
        if g in _TYPE_GROUP_CODES and g != 'Others':
            codes |= _TYPE_GROUP_CODES[g]
    if want_others:
        # "Others" adds a NULL-label sentinel; the named codes already in `codes`
        # from sibling group selections must NOT be subtracted.
        codes |= {'__others__'}
    return list(codes) if codes else None


# ---------------------------------------------------------------------------
# Main endpoint
# ---------------------------------------------------------------------------

@pricing_anomalies_bp.route('', methods=['GET'], strict_slashes=False)
@pricing_anomalies_bp.route('/', methods=['GET'], strict_slashes=False)
@require_auth
@require_api_scope('revenue')
@rate_limit_api(max_requests=30, window_seconds=60)
def get_pricing_anomalies():
    # --- parse query params ---
    country_filter = request.args.get('country', '').strip() or None
    site_filter = [s.strip() for s in request.args.get('site', '').split(',') if s.strip()]
    type_group_filter = [g.strip() for g in request.args.get('type_group', '').split(',') if g.strip()]
    size_cat_filter = [s.strip() for s in request.args.get('size_category', '').split(',') if s.strip()]
    aggregate = request.args.get('aggregate', '').strip().lower()  # 'country' or ''
    peer_mode = request.args.get('peer_mode', 'country').strip().lower()  # 'country' or 'site'
    if peer_mode not in ('country', 'site'):
        peer_mode = 'country'

    type_codes = _type_codes_for_groups(type_group_filter) if type_group_filter else None
    want_others_null = type_codes is not None and '__others__' in type_codes
    if want_others_null:
        type_codes = [c for c in type_codes if c != '__others__']

    session = _get_pbi_session()
    try:
        rows = _run_query(
            session,
            country_filter=country_filter,
            site_filter=site_filter or None,
            type_codes=type_codes,
            want_others_null=want_others_null,
            size_cat_filter=size_cat_filter or None,
            aggregate=aggregate,
            peer_mode=peer_mode,
        )
        return jsonify({"status": "success", "data": {"rows": rows}})
    except Exception as exc:
        logger.error("pricing_anomalies query failed: %s", exc, exc_info=True)
        return jsonify({"error": "Failed to retrieve pricing anomaly data"}), 500
    finally:
        session.close()


# ---------------------------------------------------------------------------
# Query builder
# ---------------------------------------------------------------------------

def _run_query(session, *, country_filter, site_filter, type_codes,
               want_others_null, size_cat_filter, aggregate, peer_mode='country'):

    params: dict = {}

    # --- WHERE clauses accumulated as strings ---
    where_clauses = [
        "u.deleted_at IS NULL",
        "u.\"bRentable\" = TRUE",
        # exclude Hong Kong by default (only override if country explicitly requested)
    ]

    if country_filter:
        where_clauses.append("s.\"Country\" = :country")
        params['country'] = country_filter
    else:
        where_clauses.append("s.\"Country\" != 'Hong Kong'")

    if site_filter:
        # Parameterize each site code individually
        placeholders = ', '.join(f':site_{i}' for i in range(len(site_filter)))
        where_clauses.append(f"u.\"sLocationCode\" IN ({placeholders})")
        for i, sc in enumerate(site_filter):
            params[f'site_{i}'] = sc

    if type_codes is not None or want_others_null:
        if type_codes and want_others_null:
            placeholders = ', '.join(f':tc_{i}' for i in range(len(type_codes)))
            where_clauses.append(
                f"(u.\"label_type_code\" IN ({placeholders}) OR u.\"label_type_code\" IS NULL)"
            )
            for i, tc in enumerate(type_codes):
                params[f'tc_{i}'] = tc
        elif type_codes:
            placeholders = ', '.join(f':tc_{i}' for i in range(len(type_codes)))
            where_clauses.append(f"u.\"label_type_code\" IN ({placeholders})")
            for i, tc in enumerate(type_codes):
                params[f'tc_{i}'] = tc
        else:
            # only "Others" (NULL codes)
            where_clauses.append("u.\"label_type_code\" IS NULL")

    if size_cat_filter:
        placeholders = ', '.join(f':sc_{i}' for i in range(len(size_cat_filter)))
        where_clauses.append(f"u.\"label_size_category\" IN ({placeholders})")
        for i, sc in enumerate(size_cat_filter):
            params[f'sc_{i}'] = sc

    where_sql = ' AND '.join(where_clauses)

    # --- grouping ---
    if aggregate == 'country':
        group_cols = 's."Country"'
        select_site = "NULL::varchar AS site_code"
        select_country = 's."Country" AS country'
    else:
        group_cols = 's."Country", u."sLocationCode"'
        select_site = 'u."sLocationCode" AS site_code'
        select_country = 's."Country" AS country'

    # Peer-group join keys.  Country-mode peers across sites; site-mode peers within site.
    # When aggregating by country, force country peer mode (site_code is NULL in that view).
    use_site_peer = (peer_mode == 'site' and aggregate != 'country')
    peer_join_extra = 'AND pr.site_code = b.site_code' if use_site_peer else ''
    peer_group_cols = (
        '"site_code", size_category, size_range, unit_type_code, climate_code'
        if use_site_peer
        else 'country, size_category, size_range, unit_type_code, climate_code'
    )
    peer_join_keys = (
        '''pr.site_code = b.site_code
           AND pr.size_category = b.size_category
           AND pr.size_range = b.size_range
           AND pr.unit_type_code = b.unit_type_code
           AND pr.climate_code = b.climate_code'''
        if use_site_peer
        else '''pr.country = b.country
           AND pr.size_category = b.size_category
           AND pr.size_range = b.size_range
           AND pr.unit_type_code = b.unit_type_code
           AND pr.climate_code = b.climate_code'''
    )

    sql = text(f"""
WITH latest_rr AS (
    SELECT "UnitID", "dcRent", "dcStdRate"
    FROM rentroll_enriched
    WHERE extract_date = (SELECT MAX(extract_date) FROM rentroll_enriched)
      AND "bRented" = TRUE
),
base AS (
    SELECT
        {select_country},
        {select_site},
        u."category_label",
        u."label_size_category"  AS size_category,
        u."label_size_range"     AS size_range,
        u."label_type_code"      AS unit_type_code,
        u."label_climate_code"   AS climate_code,
        u."label_shape"          AS shape,
        u."label_pillar"         AS pillar,
        COUNT(*)                 AS unit_count,
        SUM(u."dcarea_fixed")    AS rentable_sqft,
        SUM(CASE WHEN u."bRented" THEN u."dcarea_fixed" ELSE 0 END) AS occ_sqft,
        -- in-place rent (post-discount) per occupied unit
        SUM(CASE WHEN u."bRented" THEN rr."dcRent" ELSE NULL END)   AS total_rent,
        -- standard rate (pre-discount) per occupied unit — for discount % calc
        SUM(CASE WHEN u."bRented" THEN rr."dcStdRate" ELSE NULL END) AS total_std,
        SUM(CASE WHEN u."bRented" AND rr."UnitID" IS NOT NULL
                 THEN u."dcarea_fixed" ELSE 0 END)                   AS rented_sqft_with_rate
    FROM units_info_enriched u
    JOIN siteinfo s ON u."SiteID" = s."SiteID"
    LEFT JOIN latest_rr rr ON rr."UnitID" = u."UnitID"
    WHERE {where_sql}
    GROUP BY {group_cols}, u."category_label", u."label_size_category",
             u."label_size_range", u."label_type_code", u."label_climate_code",
             u."label_shape", u."label_pillar"
),
peer_rates AS (
    -- Weighted avg $/sqft per peer group.
    -- Peer mode:
    --   country (default) → same (country, size, type, climate) across sites + shape/pillar
    --   site              → same (site, size, type, climate)  across shape/pillar only
    SELECT
        {peer_group_cols},
        SUM(total_rent)            AS peer_total_rent,
        SUM(rented_sqft_with_rate) AS peer_rented_sqft
    FROM base
    WHERE rented_sqft_with_rate > 0
    GROUP BY {peer_group_cols}
)
SELECT
    b.country,
    b.site_code,
    b.category_label,
    b.size_category,
    b.size_range,
    b.unit_type_code,
    b.climate_code,
    b.shape,
    b.pillar,
    b.unit_count,
    ROUND(b.rentable_sqft::numeric, 2)   AS rentable_sqft,
    CASE WHEN b.rentable_sqft > 0
         THEN ROUND((b.occ_sqft / b.rentable_sqft * 100)::numeric, 1)
         ELSE 0 END                       AS occ_pct_area,
    CASE WHEN b.rented_sqft_with_rate > 0
         THEN ROUND((b.total_rent / b.rented_sqft_with_rate)::numeric, 4)
         ELSE NULL END                    AS in_place_sqft,
    -- REVPAS = total post-discount rent / TOTAL rentable sqft (rented + vacant)
    CASE WHEN b.rentable_sqft > 0
         THEN ROUND((b.total_rent / b.rentable_sqft)::numeric, 4)
         ELSE NULL END                    AS revpas_sqft,
    -- Standard rate per sqft (pre-discount) — same denominator as in_place_sqft
    CASE WHEN b.rented_sqft_with_rate > 0
         THEN ROUND((b.total_std / b.rented_sqft_with_rate)::numeric, 4)
         ELSE NULL END                    AS std_rate_sqft,
    -- Effective discount % = 1 - in_place / std
    CASE WHEN b.total_std > 0
         THEN ROUND(((1 - b.total_rent / b.total_std) * 100)::numeric, 2)
         ELSE NULL END                    AS discount_pct,
    CASE WHEN pr.peer_rented_sqft > 0
         THEN ROUND((pr.peer_total_rent / pr.peer_rented_sqft)::numeric, 4)
         ELSE NULL END                    AS peer_avg_sqft
FROM base b
LEFT JOIN peer_rates pr
       ON  {peer_join_keys}
ORDER BY b.country, b.site_code NULLS LAST, b.category_label
    """)

    result = session.execute(sql, params).fetchall()
    keys = ['country', 'site_code', 'category_label', 'size_category', 'size_range',
            'unit_type_code', 'climate_code', 'shape', 'pillar', 'unit_count',
            'rentable_sqft', 'occ_pct_area', 'in_place_sqft', 'revpas_sqft',
            'std_rate_sqft', 'discount_pct', 'peer_avg_sqft']

    rows = []
    for row in result:
        d = dict(zip(keys, row))
        # Convert Decimal → float for JSON serialisation
        for k in ('rentable_sqft', 'occ_pct_area', 'in_place_sqft', 'revpas_sqft',
                  'std_rate_sqft', 'discount_pct', 'peer_avg_sqft'):
            if d[k] is not None:
                d[k] = float(d[k])

        ipr = d['in_place_sqft']
        par = d['peer_avg_sqft']
        occ = d['occ_pct_area']

        if ipr is not None and par is not None and par > 0:
            dev = (ipr - par) / par * 100
            d['deviation_pct'] = round(dev, 2)
        else:
            dev = None
            d['deviation_pct'] = None

        # Flag logic
        if occ >= 98:
            d['flag'] = 'full'
        elif dev is not None and dev <= -8 and occ >= 85:
            d['flag'] = 'undervalued'
        elif dev is not None and dev >= 8 and occ <= 75:
            d['flag'] = 'overvalued'
        else:
            d['flag'] = 'ok'

        rows.append(d)

    return rows
