"""
Seed mw_site_distance with placeholder rows for every same-country
(from_site, to_site) pair (from_site != to_site).

  distance_km  = 999.99   (placeholder — fill via admin UI)
  same_country = TRUE
  notes        = 'placeholder — fill via admin UI'

Cross-country pairs are NOT seeded (not useful for nearest-site logic).
Re-running is safe: ON CONFLICT (from_site_code, to_site_code) DO NOTHING.

Run from backend/python:
    python3 migrations/mw_seed_site_distance_skeleton.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def _load_sites_by_country(conn) -> dict:
    """Return {country: [site_code, ...]} from mw_siteinfo."""
    rows = conn.execute(text(
        'SELECT "SiteCode", "Country" FROM mw_siteinfo '
        'WHERE "SiteCode" IS NOT NULL AND "Country" IS NOT NULL '
        'ORDER BY "Country", "SiteCode"'
    )).fetchall()

    by_country: dict = {}
    for site_code, country in rows:
        if not site_code or not country:
            continue
        by_country.setdefault(country, []).append(site_code)
    return by_country


def main():
    engine = create_engine(get_database_url('middleware'))

    with engine.begin() as conn:
        print('[1] Loading site codes from mw_siteinfo...')
        by_country = _load_sites_by_country(conn)
        total_sites = sum(len(v) for v in by_country.values())
        print(f'    {total_sites} sites across {len(by_country)} countries: '
              + ', '.join(f'{c} ({len(s)})' for c, s in sorted(by_country.items())))

        print('[2] Building same-country pair list...')
        pairs = []
        for country, codes in by_country.items():
            for from_code in codes:
                for to_code in codes:
                    if from_code != to_code:
                        pairs.append({
                            'from_site_code': from_code,
                            'to_site_code': to_code,
                            'distance_km': '999.99',
                            'same_country': True,
                            'notes': 'placeholder — fill via admin UI',
                        })
        print(f'    {len(pairs)} pairs to seed')

        if not pairs:
            print('    nothing to seed — check mw_siteinfo has rows with Country set')
            return

        print('[3] Inserting into mw_site_distance (ON CONFLICT DO NOTHING)...')
        result = conn.execute(text("""
            INSERT INTO mw_site_distance
                (from_site_code, to_site_code, distance_km, same_country, notes)
            VALUES
                (:from_site_code, :to_site_code, :distance_km, :same_country, :notes)
            ON CONFLICT (from_site_code, to_site_code) DO NOTHING
        """), pairs)
        inserted = result.rowcount if result.rowcount is not None else 0
        skipped = len(pairs) - inserted
        print(f'    Seeded {inserted} same-country site-pair rows ({skipped} already existed).')

    print('done.')


if __name__ == '__main__':
    main()
