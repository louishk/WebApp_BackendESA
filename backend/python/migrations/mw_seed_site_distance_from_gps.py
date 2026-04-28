"""
Compute real same-country distances for mw_site_distance from
mw_siteinfo's Latitude/Longitude using the Haversine formula.

Replaces the placeholder 999.99 km values seeded by
mw_seed_site_distance_skeleton.py.

Idempotent — re-running with no GPS changes is a no-op (UPDATE only
changes rows where the computed value differs).

Re-run after adding a new site or correcting GPS coords. The recommender's
Slot 2 reads this table directly; admins can still override individual
distances via the /admin/site-distance UI for special cases (e.g. real
driving time vs. straight-line).

Run from backend/python:
    python3 migrations/mw_seed_site_distance_from_gps.py
"""
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in kilometres."""
    R = 6371.0088  # mean Earth radius (km)
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        print('[1] Loading site GPS coordinates from mw_siteinfo...')
        rows = conn.execute(text("""
            SELECT "SiteCode", "Country", "Latitude", "Longitude"
            FROM mw_siteinfo
            WHERE "Latitude" IS NOT NULL AND "Longitude" IS NOT NULL
        """)).fetchall()
        sites = [(r[0], r[1], float(r[2]), float(r[3])) for r in rows]
        print(f'    {len(sites)} sites with GPS')

        # Group by country for the same-country pairs we care about.
        by_country: dict[str, list] = {}
        for code, country, lat, lon in sites:
            by_country.setdefault(country, []).append((code, lat, lon))

        print('[2] Computing Haversine distances for same-country pairs...')
        updated = 0
        skipped = 0
        for country, country_sites in by_country.items():
            if len(country_sites) < 2:
                continue
            for i, (a_code, a_lat, a_lon) in enumerate(country_sites):
                for j, (b_code, b_lat, b_lon) in enumerate(country_sites):
                    if i == j:
                        continue
                    km = round(haversine_km(a_lat, a_lon, b_lat, b_lon), 2)
                    # Update only if value differs from current (idempotent).
                    result = conn.execute(text("""
                        UPDATE mw_site_distance
                        SET distance_km = :km,
                            notes = COALESCE(NULLIF(notes, 'placeholder — fill via admin UI'), notes),
                            updated_at = now(),
                            updated_by = 'haversine_seeder'
                        WHERE from_site_code = :a
                          AND to_site_code   = :b
                          AND ABS(distance_km - :km) > 0.01
                    """), {'km': km, 'a': a_code, 'b': b_code})
                    if result.rowcount:
                        updated += 1
                    else:
                        skipped += 1

        print(f'    Updated {updated} pairs; {skipped} already current.')

        # Diagnostics: any seeded pairs left as 999.99 (= GPS missing for at least one side).
        leftovers = conn.execute(text("""
            SELECT from_site_code, to_site_code
            FROM mw_site_distance
            WHERE distance_km > 998
            ORDER BY from_site_code, to_site_code
        """)).fetchall()
        if leftovers:
            print(f'[!] {len(leftovers)} pair(s) still at placeholder — '
                  f'GPS missing on one side:')
            for a, b in leftovers[:20]:
                print(f'      {a} → {b}')


if __name__ == '__main__':
    main()
