"""
Drop legacy DiscountPlan columns: sitelink_discount_name + custom_fields.

`sitelink_discount_name` was a loose text label that pointed at SiteLink
concession plans by name. It's been superseded by `linked_concessions`
(JSONB array of {site_id, concession_id, sPlanName} tuples). Per the
explicit "no backfill" directive, plans that never populated
linked_concessions simply produce no candidate rows after this drop —
the dashboard's audit badge already flags them as "no linked concessions".

`custom_fields` was a UI-driven extra-fields bag that never shipped any
production usage.

Pre-flight: prints a diagnostic of any plan that has sitelink_discount_name
set but linked_concessions empty so admins can see which plans are about
to silently lose their candidate rows. The drop proceeds regardless.

Run from backend/python:
    python3 migrations/mw_drop_legacy_discount_plan_columns.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import create_engine, text
from common.config_loader import get_database_url


def main():
    engine = create_engine(get_database_url('middleware'))
    with engine.begin() as conn:
        # 1. Diagnostic — orphan plans (legacy name set, no concessions linked).
        orphans = conn.execute(text("""
            SELECT id, plan_name, sitelink_discount_name
            FROM mw_discount_plans
            WHERE sitelink_discount_name IS NOT NULL
              AND sitelink_discount_name <> ''
              AND (linked_concessions IS NULL
                   OR linked_concessions = '[]'::jsonb)
            ORDER BY id
        """)).fetchall()
        if orphans:
            print(f'[!] {len(orphans)} plan(s) had sitelink_discount_name '
                  'but no linked_concessions — they will produce zero '
                  'candidates until linked through the UI:')
            for r in orphans:
                print(f'      #{r[0]}  {r[1]!r}  (sitelink_discount_name={r[2]!r})')
        else:
            print('[*] No orphan plans — every legacy name is also linked.')

        # 2. Diagnostic — non-empty custom_fields (should be zero).
        cf_used = conn.execute(text("""
            SELECT id, plan_name
            FROM mw_discount_plans
            WHERE custom_fields IS NOT NULL
              AND custom_fields <> '{}'::jsonb
            ORDER BY id
        """)).fetchall()
        if cf_used:
            print(f'[!] {len(cf_used)} plan(s) have non-empty custom_fields '
                  '— this data will be lost:')
            for r in cf_used:
                print(f'      #{r[0]}  {r[1]!r}')
        else:
            print('[*] No plans use custom_fields — safe to drop.')

        # 3. Drop both columns.
        print('[1] Dropping sitelink_discount_name + custom_fields...')
        conn.execute(text("""
            ALTER TABLE mw_discount_plans
                DROP COLUMN IF EXISTS sitelink_discount_name,
                DROP COLUMN IF EXISTS custom_fields
        """))
        print('    done')


if __name__ == '__main__':
    main()
