"""
Size-range bucket neighbour helpers.

Reads mw_dim_size_range once per process (cached) and exposes:
  - size_range_neighbours(center, radius_pct=20) -> list[str]
  - size_range_neighbours_step(center, n_steps=1) -> list[str]
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Cache: range_code -> (sort_order, midpoint_or_None)
# Populated lazily on first call. Key ordering is not significant;
# results are always sorted by sort_order.
_DIM_CACHE: dict[str, tuple[int, Optional[float]]] = {}
_CACHE_LOADED = False


def _load_cache() -> None:
    """Populate _DIM_CACHE from mw_dim_size_range. Called once per process."""
    global _DIM_CACHE, _CACHE_LOADED
    if _CACHE_LOADED:
        return
    try:
        from sync_service.config import get_engine
        from sqlalchemy import text

        engine = get_engine('middleware')
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT range_code, sort_order FROM mw_dim_size_range ORDER BY sort_order")
            ).fetchall()

        cache: dict[str, tuple[int, Optional[float]]] = {}
        for range_code, sort_order in rows:
            midpoint = _parse_midpoint(range_code)
            cache[range_code] = (sort_order, midpoint)

        _DIM_CACHE = cache
        _CACHE_LOADED = True
        logger.debug("size_range_window: loaded %d buckets from mw_dim_size_range", len(cache))
    except Exception as e:
        logger.warning("size_range_window: failed to load mw_dim_size_range, functions will use [center] fallback: %s", e)
        _CACHE_LOADED = True  # don't retry on every call


def _parse_midpoint(range_code: str) -> Optional[float]:
    """
    Parse a range_code like '30-35' into its midpoint (32.5).
    Open-ended codes like '250+' return None — midpoint math is skipped for them.
    Malformed codes also return None.
    """
    code = range_code.strip()
    if code.endswith('+'):
        return None
    if '-' not in code:
        return None
    try:
        parts = code.split('-', 1)
        low = float(parts[0])
        high = float(parts[1])
        return (low + high) / 2.0
    except (ValueError, IndexError):
        return None


def clear_cache() -> None:
    """Reset the in-process cache. Used by tests to inject mock data."""
    global _DIM_CACHE, _CACHE_LOADED
    _DIM_CACHE = {}
    _CACHE_LOADED = False


def size_range_neighbours(center: str, radius_pct: int = 20) -> list[str]:
    """
    Return all size-range buckets whose midpoint falls within ±radius_pct%
    of the center bucket's midpoint, sorted by sort_order.

    The center bucket is always included in the result (even if it has no
    midpoint, it will still appear as the sole entry in the fallback list).

    If center is not found in mw_dim_size_range, returns [center].
    Open-ended buckets (e.g. '250+') have no midpoint so they are excluded
    from percentage-based results; use size_range_neighbours_step for them.
    """
    _load_cache()

    if center not in _DIM_CACHE:
        return [center]

    center_sort, center_mid = _DIM_CACHE[center]

    if center_mid is None:
        # Open-ended bucket — no percentage math possible; return just the center.
        return [center]

    lo = center_mid * (1.0 - radius_pct / 100.0)
    hi = center_mid * (1.0 + radius_pct / 100.0)

    result = []
    for code, (sort_order, midpoint) in _DIM_CACHE.items():
        if midpoint is None:
            continue
        if lo <= midpoint <= hi:
            result.append((sort_order, code))

    result.sort(key=lambda x: x[0])
    return [code for _, code in result]


def size_range_neighbours_step(center: str, n_steps: int = 1) -> list[str]:
    """
    Return all size-range buckets whose sort_order is within ±n_steps of
    the center bucket's sort_order, sorted by sort_order.

    Used by the relax engine to expand the size window by N buckets.
    If center is not found in mw_dim_size_range, returns [center].
    """
    _load_cache()

    if center not in _DIM_CACHE:
        return [center]

    center_sort, _ = _DIM_CACHE[center]
    lo = center_sort - n_steps
    hi = center_sort + n_steps

    result = []
    for code, (sort_order, _midpoint) in _DIM_CACHE.items():
        if lo <= sort_order <= hi:
            result.append((sort_order, code))

    result.sort(key=lambda x: x[0])
    return [code for _, code in result]
