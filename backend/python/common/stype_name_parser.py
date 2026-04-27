"""
sTypeName parser — decomposes a SiteLink unit type name into the six dim
columns defined by ESA SOP COM01 (Jan 2026).

Canonical format (separator '/', max 30 chars):
    Size / Range / Type / Climate / Shape / Pillar [/ CaseCount]

CaseCount is an integer and is required only when Type is a wine code
(WN, WNU, WNM, WNL, SWN, SWNU, SWNM, SWNL).

Special short-code-only labelling: for Type codes MB, BZ, SC, SB, PR the
SOP labels the whole inventory entry as just the unit type code (no
size/range/climate/shape/pillar).

Anything that does not match either form — e.g. legacy free-form names
like "Walk-In" or "AC Walk-In" — yields parse_ok=False and the raw string
so downstream code can decide whether to fall back to mapping tables.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

# Hardcoded from sql/dim_inventory_labels.sql. The SOP dim tables are
# small and stable; if they change (new unit type, new climate code),
# update these sets and add a regression test.
SIZE_CATEGORY_CODES = frozenset({'S', 'M', 'L', 'XL'})

SIZE_RANGE_CODES = frozenset({
    '0-6', '6-8', '8-10', '10-12', '12-14', '14-16', '16-18', '18-20',
    '20-22', '22-24', '24-26', '26-28', '28-30', '30-35', '35-40', '40-45',
    '45-50', '50-60', '60-70', '70-80', '80-90', '90-110', '110-130',
    '130-150', '150-175', '175-200', '200-225', '225-250', '250+',
})

UNIT_TYPE_CODES = frozenset({
    'W', 'E', 'S',
    'U', 'M', 'L',
    'SU', 'SM', 'SL',
    'EU', 'EM', 'EL',
    'WN', 'WNU', 'WNM', 'WNL',
    'SWN', 'SWNU', 'SWNM', 'SWNL',
    'DV', 'RB', 'MB', 'BZ', 'SC', 'SB', 'PR',
})

CLIMATE_TYPE_CODES = frozenset({'NC', 'A', 'D', 'AD', 'RF'})
UNIT_SHAPE_CODES = frozenset({'SS', 'WR', 'NR', 'OS'})
PILLAR_CODES = frozenset({'P', 'NP'})

# Unit types that label as a bare code with no other components.
SHORT_CODE_ONLY_TYPES = frozenset({'MB', 'BZ', 'SC', 'SB', 'PR'})

# Unit types whose full label carries a 7th component — the case count.
WINE_TYPES = frozenset({'WN', 'WNU', 'WNM', 'WNL', 'SWN', 'SWNU', 'SWNM', 'SWNL'})

SEPARATOR = '/'


@dataclass(frozen=True)
class StypeNameParts:
    raw: str
    size_category: Optional[str] = None
    size_range: Optional[str] = None
    unit_type: Optional[str] = None
    climate_type: Optional[str] = None
    unit_shape: Optional[str] = None
    pillar: Optional[str] = None
    case_count: Optional[int] = None
    parse_ok: bool = False
    invalid_tokens: tuple = field(default_factory=tuple)


def _clean_tokens(s: str) -> list[str]:
    return [t.strip() for t in s.split(SEPARATOR)]


def parse_stype_name(raw: Optional[str]) -> StypeNameParts:
    """Parse a SiteLink sTypeName string into SOP COM01 components.

    Returns a StypeNameParts with parse_ok=True only when the input
    matches either:
      (a) the short-code-only form: a single token in SHORT_CODE_ONLY_TYPES, or
      (b) the full form: 6 valid tokens (7 for wine types, 7th = integer).

    Legacy or malformed strings yield parse_ok=False with whatever tokens
    were recognisable preserved.
    """
    if raw is None:
        return StypeNameParts(raw='')
    s = raw.strip()
    if not s:
        return StypeNameParts(raw=raw)

    # Short-code-only case: no separator, token is one of MB/BZ/SC/SB/PR.
    if SEPARATOR not in s:
        if s in SHORT_CODE_ONLY_TYPES:
            return StypeNameParts(raw=raw, unit_type=s, parse_ok=True)
        return StypeNameParts(raw=raw)

    tokens = _clean_tokens(s)
    # Expect 6 tokens (non-wine) or 7 tokens (wine).
    if len(tokens) not in (6, 7):
        return StypeNameParts(raw=raw)

    size_cat, size_range, unit_type, climate, shape, pillar, *rest = tokens
    case_count_raw = rest[0] if rest else None

    invalid: list[str] = []

    def _check(code: Optional[str], allowed: frozenset, label: str) -> Optional[str]:
        if code is None or code == '':
            invalid.append(label)
            return None
        if code not in allowed:
            invalid.append(f'{label}={code!r}')
            return None
        return code

    size_cat_ok = _check(size_cat, SIZE_CATEGORY_CODES, 'size_category')
    size_range_ok = _check(size_range, SIZE_RANGE_CODES, 'size_range')
    unit_type_ok = _check(unit_type, UNIT_TYPE_CODES, 'unit_type')
    climate_ok = _check(climate, CLIMATE_TYPE_CODES, 'climate_type')
    shape_ok = _check(shape, UNIT_SHAPE_CODES, 'unit_shape')
    pillar_ok = _check(pillar, PILLAR_CODES, 'pillar')

    case_count: Optional[int] = None
    if unit_type_ok in WINE_TYPES:
        if case_count_raw is None or case_count_raw == '':
            invalid.append('case_count=missing')
        else:
            try:
                case_count = int(case_count_raw)
            except (TypeError, ValueError):
                invalid.append(f'case_count={case_count_raw!r}')
    elif case_count_raw is not None:
        # 7th token present on a non-wine type — malformed per SOP.
        invalid.append(f'unexpected_7th={case_count_raw!r}')

    parse_ok = not invalid
    return StypeNameParts(
        raw=raw,
        size_category=size_cat_ok,
        size_range=size_range_ok,
        unit_type=unit_type_ok,
        climate_type=climate_ok,
        unit_shape=shape_ok,
        pillar=pillar_ok,
        case_count=case_count,
        parse_ok=parse_ok,
        invalid_tokens=tuple(invalid),
    )
