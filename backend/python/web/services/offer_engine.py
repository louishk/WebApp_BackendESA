"""
Best Offer Engine — recommends discount plans for a given unit/site.

Pure Python module with no Flask dependency. Accepts a SQLAlchemy session
and returns scored, ranked discount plan recommendations.
"""

import logging
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)


def recommend_offers(
    db_session,
    site_code: str,
    area_sqft: Optional[float] = None,
    tenancy_months: Optional[int] = None,
    std_rate: Optional[float] = None,
    top_n: int = 3,
) -> list:
    """
    Recommend the best discount plans for a given site and unit.

    Args:
        db_session: SQLAlchemy session bound to esa_backend.
        site_code: Site code to match against (e.g. 'L001').
        area_sqft: Unit area in sqft (reserved for future area-based filtering).
        tenancy_months: Intended tenancy in months (reserved for future filtering).
        std_rate: Standard rate for the unit; used to compute indicative_rate.
        top_n: Maximum number of plans to return (default 3).

    Returns:
        List of dicts, each representing a scored plan, sorted by score descending.
    """
    from web.models.discount_plan import DiscountPlan

    today = date.today()

    try:
        plans = db_session.query(DiscountPlan).filter_by(is_active=True).all()
    except Exception:
        logger.exception("Failed to query DiscountPlan table")
        raise

    results = []

    for plan in plans:
        # --- Filter: applicable_sites ---
        applicable = plan.applicable_sites or {}
        if not applicable.get(site_code):
            continue

        # --- Filter: date validity ---
        if plan.period_start and plan.period_start > today:
            continue
        if plan.period_end and plan.period_end < today:
            continue
        if plan.booking_period_start and plan.booking_period_start > today:
            continue
        if plan.booking_period_end and plan.booking_period_end < today:
            continue

        # --- Score ---
        discount_numeric = float(plan.discount_numeric) if plan.discount_numeric is not None else 0.0
        score = discount_numeric

        if not plan.hidden_rate:
            score += 5
        if plan.plan_type == 'Evergreen':
            score += 2

        # --- Indicative rate ---
        indicative_rate = None
        if std_rate is not None and plan.discount_numeric is not None:
            if plan.discount_type == 'percentage':
                indicative_rate = round(std_rate * (1 - discount_numeric / 100), 2)
            elif plan.discount_type == 'fixed_amount':
                indicative_rate = round(std_rate - discount_numeric, 2)

        # --- Concession check ---
        concessions = plan.linked_concessions or []
        has_concession = any(
            isinstance(c, dict) and c.get('site_id') is not None
            for c in concessions
        )
        # We don't have a site_code→site_id mapping here, so we surface all
        # concession entries and let the caller decide. has_concession_for_site
        # is set True if any concession entry is present (best-effort).
        has_concession_for_site = has_concession

        results.append({
            'plan_id': plan.id,
            'plan_name': plan.plan_name,
            'plan_type': plan.plan_type,
            'discount_type': plan.discount_type,
            'discount_value': plan.discount_value,
            'discount_numeric': discount_numeric if plan.discount_numeric is not None else None,
            'indicative_rate': indicative_rate,
            'has_concession_for_site': has_concession_for_site,
            'linked_concessions': concessions,
            '_score': score,
        })

    results.sort(key=lambda x: x['_score'], reverse=True)

    # Strip internal score field before returning
    top = results[:top_n]
    for r in top:
        del r['_score']

    logger.debug(
        "recommend_offers: site=%s area=%s std_rate=%s → %d candidates, returning %d",
        site_code, area_sqft, std_rate, len(results), len(top),
    )

    return top
