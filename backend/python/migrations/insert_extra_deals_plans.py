"""
Migration: Insert 'Extra Deals. Extra Perks.' discount plans (5 variants).

Run from project root:
    cd backend/python && python migrations/insert_extra_deals_plans.py

Requires: .env with DB_PASSWORD, PBI_DB_PASSWORD
"""

import sys
import os
import json
from datetime import date

# Add parent to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '..', '.env'))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def get_db_url(db_name):
    """Build DB URL from env vars."""
    if db_name == 'backend':
        pw = os.environ['DB_PASSWORD']
        return f"postgresql://esa_pbi_admin:{pw}@esapbi.postgres.database.azure.com:5432/backend?sslmode=require"
    elif db_name == 'pbi':
        pw = os.environ.get('PBI_DB_PASSWORD', os.environ['DB_PASSWORD'])
        return f"postgresql://esa_pbi_admin:{pw}@esapbi.postgres.database.azure.com:5432/esa_pbi?sslmode=require"


def get_sites_by_country(pbi_session):
    """Query site codes grouped by country from siteinfo table."""
    rows = pbi_session.execute(
        text('SELECT "SiteCode", "Country" FROM siteinfo WHERE "SiteCode" IS NOT NULL ORDER BY "SiteCode"')
    ).fetchall()
    by_country = {}
    for code, country in rows:
        by_country.setdefault(country, []).append(code)
    return by_country


def build_applicable_sites(site_codes):
    """Build JSONB dict {code: true} for applicable_sites."""
    return {code: True for code in site_codes}


def build_plans(sites_by_country):
    """Build the 5 discount plan records."""

    # Map country names to site codes
    sg_sites = sites_by_country.get('Singapore', [])
    my_sites = sites_by_country.get('Malaysia', [])
    kr_sites = sites_by_country.get('South Korea', [])

    # Common fields
    common = dict(
        plan_type='Evergreen',
        objective='Offer a permanent discount year round with flexible payment term.',
        period_range='From Now to Dec 31, 2026',
        promo_period_end=date(2026, 12, 31),
        switch_to_us='Not Eligible',
        referral_program='Not Eligible',
        lock_in_period='Fixed terms - Min 3M up to 12M',
        payment_terms='Flexible (Monthly)',
        deposit='1M Deposit',
        distribution_channel='All Channels',
        hidden_rate=False,
        available_for_chatbot=False,
        is_active=True,
        sort_order=0,
        created_by='migration',
    )

    # Shared T&Cs (items that are identical across all 5)
    tcs_shared_prefix = [
        "1. This offer is applicable to new customers only.",
        "2. This offer cannot be used in conjunction with any other offers or promotions.",
        "3. The offer applies to selected units at designated facilities and is subject to availability at the time of booking.",
        "4. Move-in date must occur within 14 days of booking confirmation.",
    ]
    tcs_shared_suffix = [
        "6. No prepayment is required. This offer is based on fixed lease terms of 3, 6, or 12 months as selected at the time of booking.",
        "7. Mandatory security deposit of one-month standard rent (refundable) is payable on or before move-in.",
        "8. The promotional discount applies from the 1st month through to the last month of the selected fixed term. The prorated move-in month counts as the 1st month.",
        "9. The minimum storage period is 3 months, and maximum is 12 months.",
        "10. A minimum 14 days' written notice is required prior to the end of the fixed lease term. Upon promotion expiry, the lease will automatically continue on a flexible month-to-month basis at the prevailing standard rate.",
        "11. Promotional terms and conditions are subject to change, with reasonable notice where practicable.",
        "12. In case of dispute, Extra Space Asia has the final say in all matters.",
        "13. Other terms and conditions apply.",
    ]

    def make_tcs(admin_fee_clause):
        return tcs_shared_prefix + [admin_fee_clause] + tcs_shared_suffix

    # --- Plan definitions ---

    plans = []

    # 1. SG Self Storage - 10% Off
    plans.append({
        **common,
        'plan_name': 'Extra Deals Extra Perks - SG Self Storage',
        'applicable_sites': build_applicable_sites(sg_sites),
        'discount_value': '10% Off on all Units',
        'discount_type': 'percentage',
        'discount_numeric': 10.0,
        'offers': [
            {"label": "Option 1: e-Move rebate", "tiers": {"3M": "$100", "6M": "$200", "12M": "$500"}},
            {"label": "Option 2: Supermarket/Grab Voucher", "tiers": {"3M": "$50", "6M": "$100", "12M": "$250"}},
            {"label": "Option 3: BIZplus Credits", "tiers": {"3M": "$100", "6M": "$200", "12M": "$500"}},
            {"label": "Option 4: Wine voucher", "tiers": {"3M": "$50", "6M": "$100", "12M": "$250"}},
        ],
        'terms_conditions': make_tcs(
            "5. A one-time administration fee of $32.70 (inclusive of GST) is payable on or before the move-in date."
        ),
    })

    # 2. SG Wine Storage - 20% Off
    plans.append({
        **common,
        'plan_name': 'Extra Deals Extra Perks - SG Wine Storage',
        'applicable_sites': build_applicable_sites(sg_sites),
        'discount_value': '20% Off on all Units',
        'discount_type': 'percentage',
        'discount_numeric': 20.0,
        'offers': [
            {"label": "Option 1: e-Move rebate", "tiers": {"3M": "$100", "6M": "$200", "12M": "$500"}},
            {"label": "Option 2: Supermarket/Grab Voucher", "tiers": {"3M": "$50", "6M": "$100", "12M": "$250"}},
            {"label": "Option 3: BIZplus Credits", "tiers": {"3M": "$100", "6M": "$200", "12M": "$500"}},
            {"label": "Option 4: Wine voucher", "tiers": {"3M": "$50", "6M": "$100", "12M": "$250"}},
        ],
        'terms_conditions': make_tcs(
            "5. A one-time administration fee of $32.70 (inclusive of GST) is payable on or before the move-in date."
        ),
        'notes': 'Wine storage units only',
    })

    # 3. MY Self Storage - 10% Off
    plans.append({
        **common,
        'plan_name': 'Extra Deals Extra Perks - MY Self Storage',
        'applicable_sites': build_applicable_sites(my_sites),
        'discount_value': '10% Off on all Units',
        'discount_type': 'percentage',
        'discount_numeric': 10.0,
        'offers': [
            {"label": "Option 1: Moving rebate", "tiers": {"3M": "RM50", "6M": "RM100", "12M": "RM300"}},
            {"label": "Option 2: Supermarket/Grab Voucher", "tiers": {"3M": "RM50", "6M": "RM100", "12M": "RM250"}},
        ],
        'terms_conditions': make_tcs(
            "5. A one-time administration fee of RM30 is payable on or before the move-in date."
        ),
    })

    # 4. MY Wine Storage - 10% Off
    plans.append({
        **common,
        'plan_name': 'Extra Deals Extra Perks - MY Wine Storage',
        'applicable_sites': build_applicable_sites(my_sites),
        'discount_value': '10% Off on all Units',
        'discount_type': 'percentage',
        'discount_numeric': 10.0,
        'offers': [
            {"label": "Option 1: Moving rebate", "tiers": {"3M": "RM50", "6M": "RM100", "12M": "RM300"}},
            {"label": "Option 2: Supermarket/Grab Voucher", "tiers": {"3M": "RM50", "6M": "RM100", "12M": "RM250"}},
        ],
        'terms_conditions': make_tcs(
            "5. A one-time administration fee of RM30 is payable on or before the move-in date."
        ),
        'notes': 'Wine storage units only',
    })

    # 5. KR Self Storage - 20% Off
    plans.append({
        **common,
        'plan_name': 'Extra Deals Extra Perks - KR Self Storage',
        'applicable_sites': build_applicable_sites(kr_sites),
        'discount_value': '20% Off on all Units',
        'discount_type': 'percentage',
        'discount_numeric': 20.0,
        'offers': [
            {"label": "Option 1: Moving rebate", "tiers": {"3M": "₩100,000", "6M": "₩200,000", "12M": "₩500,000"}},
            {"label": "Option 2: Supermarket Voucher", "tiers": {"3M": "₩50,000", "6M": "₩100,000", "12M": "₩250,000"}},
        ],
        'terms_conditions': make_tcs(
            "5. A one-time administration fee of ₩15,000 (inclusive of tax) is payable on or before the move-in date."
        ),
    })

    return plans


def main():
    # Connect to both DBs
    pbi_engine = create_engine(get_db_url('pbi'))
    backend_engine = create_engine(get_db_url('backend'))

    PbiSession = sessionmaker(bind=pbi_engine)
    BackendSession = sessionmaker(bind=backend_engine)

    # 1. Get site codes by country
    pbi_session = PbiSession()
    try:
        sites_by_country = get_sites_by_country(pbi_session)
        print("Sites by country:")
        for country, codes in sorted(sites_by_country.items()):
            print(f"  {country}: {', '.join(codes)}")
    finally:
        pbi_session.close()

    # 2. Build plans
    plans = build_plans(sites_by_country)

    # 3. Insert into discount_plans
    backend_session = BackendSession()
    try:
        for plan_data in plans:
            # Check if already exists
            existing = backend_session.execute(
                text("SELECT id FROM discount_plans WHERE plan_name = :name"),
                {'name': plan_data['plan_name']}
            ).fetchone()

            if existing:
                print(f"  SKIP (exists): {plan_data['plan_name']} (id={existing[0]})")
                continue

            # Build INSERT
            cols = [
                'plan_type', 'plan_name', 'objective', 'period_range',
                'promo_period_end', 'applicable_sites',
                'discount_value', 'discount_type', 'discount_numeric',
                'offers', 'terms_conditions',
                'switch_to_us', 'referral_program', 'lock_in_period',
                'payment_terms', 'deposit', 'distribution_channel',
                'hidden_rate', 'available_for_chatbot',
                'is_active', 'sort_order', 'created_by',
            ]
            if plan_data.get('notes'):
                cols.append('notes')

            params = {}
            for col in cols:
                val = plan_data.get(col)
                if isinstance(val, (dict, list)):
                    params[col] = json.dumps(val)
                else:
                    params[col] = val

            col_list = ', '.join(cols)

            # Use CAST() instead of :: for JSONB to avoid SQLAlchemy param conflicts
            jsonb_cols = {'applicable_sites', 'offers', 'terms_conditions'}
            val_parts = []
            for c in cols:
                if c in jsonb_cols:
                    val_parts.append(f'CAST(:{c} AS jsonb)')
                else:
                    val_parts.append(f':{c}')
            val_list = ', '.join(val_parts)

            sql = f"INSERT INTO discount_plans ({col_list}) VALUES ({val_list})"
            backend_session.execute(text(sql), params)
            print(f"  INSERT: {plan_data['plan_name']}")

        backend_session.commit()
        print("\nDone! All plans inserted successfully.")

    except Exception as e:
        backend_session.rollback()
        print(f"\nERROR: {e}")
        raise
    finally:
        backend_session.close()


if __name__ == '__main__':
    main()
