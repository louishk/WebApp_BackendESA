#!/usr/bin/env python3
"""
Seed script for discount_plans table.
Populates the initial discount plans from the Excel-based tracking sheet.

Usage:
    python migrations/011_seed_discount_plans.py

This script:
1. Creates the discount_plans table if not exists (runs the SQL migration)
2. Seeds the plans from the original Excel data
3. Skips plans that already exist (by plan_name)
"""

import os
import sys
from pathlib import Path

# Add parent directory to path
backend_path = Path(__file__).parent.parent
sys.path.insert(0, str(backend_path))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


# Common T&Cs shared by most plans
COMMON_TCS_EN = [
    "This offer is not applicable to existing customers of RedBox Storage, and cannot be used in conjunction with any other promotional offers.",
    "This offer is subject to self-storage availability at the time of booking.",
    "Move-in date must occur within 14 days maximum after the booking confirmation date.",
    "Mandatory security deposit of One-month standard rent (Refundable), $75 administrative cost (one-off), and storage insurance fee (From $40/monthly) to be collected on/before the move-in day.",
    "",
    "Termination Notice - 1-month.",
    "All matters and disputes will be subject to the final decision of RedBox Storage Limited.",
]

# Standard Rate has a slightly different T&C (no "not applicable to existing customers")
STANDARD_TCS_EN = [
    "",
    "This offer is subject to self-storage availability at the time of booking.",
    "Move-in date must occur within 14 days maximum after the booking confirmation date.",
    "Mandatory security deposit of One-month standard rent (Refundable), $75 administrative cost (one-off), and storage insurance fee (From $40/monthly) to be collected on/before the move-in day.",
    "",
    "Termination Notice - 1-month.",
    "All matters and disputes will be subject to the final decision of RedBox Storage Limited.",
]


SEED_PLANS = [
    {
        "plan_type": "Evergreen",
        "plan_name": "Standard Rate",
        "notes": None,
        "objective": "Standard Rate - Best Rate in a non-promotional condition.",
        "period_range": "Permanent",
        "move_in_range": "Maximum 14 days after the booking date",
        "applicable_sites": {"L001": True, "L003": False, "L004": True, "L005": True, "L006": True, "L007": False, "L008": False, "L009": False, "L010": False},
        "discount_value": "No Discount",
        "discount_type": "none",
        "discount_numeric": None,
        "discount_segmentation": "No Discount",
        "clawback_condition": None,
        "deposit": "1 Month (Refundable)",
        "payment_terms": "Monthly",
        "termination_notice": "1 Month",
        "extra_offer": None,
        "terms_conditions": STANDARD_TCS_EN,
        "is_active": True,
        "sort_order": 1,
    },
    {
        "plan_type": "Evergreen",
        "plan_name": "Prepaid - 6M",
        "notes": "To stop and switch to Free Months offer",
        "objective": "Offer a discount to Prepaid customers.",
        "period_range": "Permanent",
        "move_in_range": "Maximum 14 days after the booking date",
        "applicable_sites": {"L001": False, "L003": False, "L004": False, "L005": False, "L006": False, "L007": False, "L008": False, "L009": False, "L010": False},
        "discount_value": "5%",
        "discount_type": "percentage",
        "discount_numeric": 5.0,
        "discount_segmentation": ">=5% < 10%",
        "clawback_condition": None,
        "deposit": "1 Month (Refundable)",
        "payment_terms": "Prepaid (6M)",
        "termination_notice": "1 Month",
        "extra_offer": None,
        "terms_conditions": COMMON_TCS_EN,
        "is_active": True,
        "sort_order": 2,
    },
    {
        "plan_type": "Evergreen",
        "plan_name": "Prepaid - 12M",
        "notes": "To stop and switch to Free Months offer",
        "objective": "Offer a discount to Prepaid customers.",
        "period_range": "Permanent",
        "move_in_range": "Maximum 14 days after the booking date",
        "applicable_sites": {"L001": False, "L003": False, "L004": False, "L005": False, "L006": False, "L007": False, "L008": False, "L009": False, "L010": False},
        "discount_value": "8%",
        "discount_type": "percentage",
        "discount_numeric": 8.0,
        "discount_segmentation": ">=5% < 10%",
        "clawback_condition": None,
        "deposit": "1 Month (Refundable)",
        "payment_terms": "Prepaid (12M)",
        "termination_notice": "1 Month",
        "extra_offer": None,
        "terms_conditions": COMMON_TCS_EN,
        "is_active": True,
        "sort_order": 3,
    },
    {
        "plan_type": "Evergreen",
        "plan_name": "Permanent Offer 6M",
        "notes": "Replace 6M Prepaid",
        "objective": "Permanent Offer 6M",
        "period_range": "Permanent",
        "move_in_range": "Maximum 14 days after the booking date",
        "applicable_sites": {"L001": False, "L003": False, "L004": False, "L005": False, "L006": False, "L007": False, "L008": False, "L009": False, "L010": False},
        "discount_value": "First 2 Weeks Free",
        "discount_type": "free_period",
        "discount_numeric": None,
        "discount_segmentation": ">=5% < 10%",
        "clawback_condition": "If Tenure less than 6M, 2 weeks will be charged",
        "deposit": "1 Month (Refundable)",
        "payment_terms": "Monthly",
        "termination_notice": "1 Month",
        "extra_offer": None,
        "terms_conditions": COMMON_TCS_EN,
        "is_active": True,
        "sort_order": 4,
    },
    {
        "plan_type": "Evergreen",
        "plan_name": "Permanent Offer 12M",
        "notes": "Replace 12M Prepaid",
        "objective": "Permanent Offer 12M",
        "period_range": "Permanent",
        "move_in_range": "Maximum 14 days after the booking date",
        "applicable_sites": {"L001": False, "L003": False, "L004": False, "L005": False, "L006": False, "L007": False, "L008": False, "L009": False, "L010": False},
        "discount_value": "1st Month Free",
        "discount_type": "free_period",
        "discount_numeric": None,
        "discount_segmentation": ">=5% < 10%",
        "clawback_condition": "If Tenure less than 12M, 1 month will be charged",
        "deposit": "1 Month (Refundable)",
        "payment_terms": "Monthly",
        "termination_notice": "1 Month",
        "extra_offer": None,
        "terms_conditions": COMMON_TCS_EN,
        "is_active": True,
        "sort_order": 5,
    },
    {
        "plan_type": "Evergreen",
        "plan_name": "Referral - Referee",
        "notes": None,
        "objective": "Offer an Incentive to customer referring us.",
        "period_range": "Permanent",
        "move_in_range": "Maximum 14 days after the booking date",
        "applicable_sites": {"L001": False, "L003": False, "L004": False, "L005": False, "L006": False, "L007": False, "L008": False, "L009": False, "L010": False},
        "discount_value": "20% - 1st Month for Referee",
        "discount_type": "percentage",
        "discount_numeric": 20.0,
        "discount_segmentation": ">=0% < 5%",
        "clawback_condition": "20% of first month storage only",
        "deposit": "1 Month (Refundable)",
        "payment_terms": None,
        "termination_notice": "1 Month",
        "extra_offer": None,
        "terms_conditions": COMMON_TCS_EN,
        "is_active": True,
        "sort_order": 6,
    },
    {
        "plan_type": "Evergreen",
        "plan_name": "Referral - Referer",
        "notes": None,
        "objective": "Offer an Incentive to customer referring us.",
        "period_range": "Permanent",
        "move_in_range": "Maximum 14 days after the booking date",
        "applicable_sites": {"L001": False, "L003": False, "L004": False, "L005": False, "L006": False, "L007": False, "L008": False, "L009": False, "L010": False},
        "discount_value": "300HKD for Referer",
        "discount_type": "fixed_amount",
        "discount_numeric": 300.0,
        "discount_segmentation": ">=0% < 5%",
        "clawback_condition": "20% of first month storage only",
        "deposit": "1 Month (Refundable)",
        "payment_terms": None,
        "termination_notice": "1 Month",
        "extra_offer": None,
        "terms_conditions": COMMON_TCS_EN,
        "is_active": True,
        "sort_order": 7,
    },
    {
        "plan_type": "Evergreen",
        "plan_name": "Friends and Family",
        "notes": None,
        "objective": "Offer an Incentive to customer being referred by a Redbox Staff.",
        "period_range": "Permanent",
        "move_in_range": "Maximum 14 days after the booking date",
        "applicable_sites": {"L001": False, "L003": False, "L004": False, "L005": False, "L006": False, "L007": False, "L008": False, "L009": False, "L010": False},
        "discount_value": "20%",
        "discount_type": "percentage",
        "discount_numeric": 20.0,
        "discount_segmentation": ">=15% < 30%",
        "clawback_condition": "This offer is only applicable to Infard or RedBox Storage Office Staff. A contract with at least 6 months prepayment will be required to sign with RedBox Storage to enjoy the discount.",
        "deposit": "1 Month (Refundable)",
        "payment_terms": "Prepaid (6M)",
        "termination_notice": "1 Month",
        "extra_offer": None,
        "terms_conditions": COMMON_TCS_EN,
        "is_active": True,
        "sort_order": 8,
    },
    {
        "plan_type": "Evergreen",
        "plan_name": "Friends and Family - New",
        "notes": "Replace Staff Referral",
        "objective": "Offer an Incentive to customer being referred by a Redbox Staff.",
        "period_range": "Permanent",
        "move_in_range": "Maximum 14 days after the booking date",
        "applicable_sites": {"L001": False, "L003": False, "L004": False, "L005": False, "L006": False, "L007": False, "L008": False, "L009": False, "L010": False},
        "discount_value": "1 Month Free every 6M",
        "discount_type": "free_period",
        "discount_numeric": None,
        "discount_segmentation": ">=15% < 30%",
        "clawback_condition": "Max 2 referral per Staff per Year. Autopay Mandatory. Approval on Request by RM.",
        "deposit": "1 Month (Refundable)",
        "payment_terms": "Monthly",
        "termination_notice": "1 Month",
        "extra_offer": "-20% Off Merchandise",
        "terms_conditions": COMMON_TCS_EN,
        "is_active": True,
        "sort_order": 9,
    },
    {
        "plan_type": "Evergreen",
        "plan_name": "Staff Rate",
        "notes": None,
        "objective": "Staff Benefits exclusive to Redbox Staff.",
        "period_range": "Permanent",
        "move_in_range": "Maximum 14 days after the booking date",
        "applicable_sites": {"L001": False, "L003": False, "L004": False, "L005": False, "L006": False, "L007": False, "L008": False, "L009": False, "L010": False},
        "discount_value": "50%",
        "discount_type": "percentage",
        "discount_numeric": 50.0,
        "discount_segmentation": ">=30%",
        "clawback_condition": None,
        "deposit": "1 Month (Refundable)",
        "payment_terms": None,
        "termination_notice": "1 Month",
        "extra_offer": None,
        "terms_conditions": COMMON_TCS_EN,
        "is_active": True,
        "sort_order": 10,
    },
    # Example tactical plan from the second image
    {
        "plan_type": "Tactical",
        "plan_name": "Direct Mailing Offer - TKO Residents",
        "notes": None,
        "objective": "Tactical offer to attract new customers from TKO area",
        "period_range": "From 11 August till 30 September 2025",
        "period_start": "2025-08-11",
        "period_end": "2025-09-30",
        "move_in_range": "Within 14 days of booking confirmation",
        "applicable_sites": {"L001": False, "L003": False, "L004": True, "L005": True, "L006": True, "L007": True, "L008": True, "L009": True, "L010": True},
        "discount_value": "Flexi 45% off / LT12M 50% off",
        "discount_type": "percentage",
        "discount_numeric": None,
        "discount_segmentation": None,
        "clawback_condition": None,
        "offers": [
            {"tier": "Flexi", "discount": "45% off"},
            {"tier": "Long Term 12M", "discount": "50% off", "note": "Long Term Plan 12M only and with free transportation"},
        ],
        "deposit": "No Deposit or With Deposit, subject to billing arrangements",
        "payment_terms": "Flexible (Monthly)",
        "termination_notice": "1 Month",
        "extra_offer": None,
        "hidden_rate": True,
        "available_for_chatbot": True,
        "chatbot_notes": "But on special condition",
        "sales_extra_discount": "Not Eligible",
        "switch_to_us": "Not Eligible",
        "referral_program": "Not Eligible",
        "distribution_channel": "Direct Mailing",
        "rate_rules_sites": "L004 Yau Tong, L005 Tuen Mun, L006 Tsuen Wan, L007 Tsuen Wan Smart, L008 City One, L009 Chai Wan CIC, L010 Sai Wan",
        "promotion_codes": ["Direct Mail Flexi 45% off", "Direct Mail LT12M 50% off"],
        "terms_conditions": [
            "This offer applies to new RedBox Storage customers only.",
            "It cannot be used in conjunction with any other promotions.",
            "The offer is subject to self-storage availability at the time of booking, on a first-come, first-served basis.",
            "Move-in must occur within 14 days of booking confirmation.",
            "Deposit and other fees Policy: No Deposit Plans are subject to a mandatory registration to payment by Credit Card Autopay or Direct Debit/FPS through RedBox portal. $100 administrative cost (one-off) and charges of SAFE Keeping Plan to be collected on/before the move-in day. With Deposit: Mandatory security deposit of One-month standard rent (Refundable), $100 administrative cost (one-off) and charges of SAFE Keeping Plan to be collected on/before the move-in day.",
            "The discounted rental period varies according to the discount offers at the time. The prorated month counts as the 1st month. Afterward, the standard rate applies.",
            "Termination Policy: Termination Notice - 1-month advance notice",
            "All matters and disputes will be subject to the final decision of RedBox Storage Limited.",
        ],
        "terms_conditions_cn": [
            "1. \u512a\u60e0\u9069\u7528\u65bc\u7d05\u76d2\u8ff7\u4f60\u5009\u5e97\u65b0\u5ba2\u6236\u3002",
            "2. \u4e0d\u53ef\u8207\u5176\u4ed6\u63a8\u5ee3\u6d3b\u52d5\u540c\u6642\u4f7f\u7528\u3002",
            "3. \u512a\u60e0\u9069\u7528\u65bc\u6307\u5b9a\u8ff7\u4f60\u5009\uff0c\u4e26\u8996\u4e4e\u6578\u91cf\u4f9b\u61c9\u60c5\u6cc1\u800c\u5b9a\uff0c\u5148\u5230\u5148\u5f97\uff0c\u984d\u6eff\u5373\u6b62\u3002",
            "4. \u78ba\u5b9a\u9810\u7d04\u5f8c\uff0c\u9808\u572814\u5929\u5167\u5165\u5009\u3002",
            "5. \u6309\u91d1\u53ca\u5176\u4ed6\u6536\u8cbb\u9805\u76ee",
            "6. \u512a\u60e0\u79df\u91d1\u79df\u671f\u6839\u64da\u7576\u6642\u7684\u6298\u6263\u512a\u60e0\u800c\u5b9a\u3002",
            "7. \u9000\u5009\u901a\u77e5\u70ba\u4e00\u500b\u6708",
            "8. \u6240\u6709\u4e8b\u5b9c\u548c\u722d\u8b70\u5c07\u7531\u7d05\u76d2\u8ff7\u4f60\u5009\u505a\u51fa\u6700\u7d42\u6c7a\u5b9a\u3002",
        ],
        "is_active": True,
        "sort_order": 20,
    },
]


def get_database_url():
    """Get database URL from the application's config loader."""
    try:
        from common.config_loader import get_database_url as get_app_db_url
        return get_app_db_url('backend')
    except Exception as e:
        print(f"Warning: Could not load config, using environment variable: {e}")
        return os.environ.get('DATABASE_URL', 'sqlite:///app.db')


def run_migration_sql(engine):
    """Create the discount_plans table if it doesn't exist."""
    sql_file = Path(__file__).parent / '011_discount_plans.sql'
    if sql_file.exists():
        with open(sql_file) as f:
            sql = f.read()
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
        print("  Table 'discount_plans' ready.")
    else:
        print(f"  Warning: SQL file not found at {sql_file}")


def seed_plans(engine):
    """Insert seed data, skipping existing plans."""
    import json as json_module

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        inserted = 0
        skipped = 0

        for plan_data in SEED_PLANS:
            name = plan_data['plan_name']

            # Check if already exists
            exists = session.execute(
                text("SELECT id FROM discount_plans WHERE plan_name = :name"),
                {'name': name}
            ).fetchone()

            if exists:
                print(f"    Skipped (exists): {name}")
                skipped += 1
                continue

            # Build column/value lists
            cols = []
            vals = {}
            for key, value in plan_data.items():
                if key in ('period_start', 'period_end') and value and isinstance(value, str):
                    from datetime import date as d
                    value = d.fromisoformat(value)
                if isinstance(value, (dict, list)):
                    value = json_module.dumps(value)
                cols.append(key)
                vals[key] = value

            col_str = ', '.join(cols)
            param_str = ', '.join(f':{c}' for c in cols)

            session.execute(
                text(f"INSERT INTO discount_plans ({col_str}) VALUES ({param_str})"),
                vals
            )
            print(f"    Inserted: {name}")
            inserted += 1

        session.commit()
        print(f"\n  Seed complete: {inserted} inserted, {skipped} skipped.")

    except Exception as e:
        session.rollback()
        print(f"  Error seeding plans: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        session.close()


def main():
    """Run the discount plans migration and seed."""
    print("=" * 60)
    print("Discount Plans - Migration & Seed")
    print("=" * 60)

    database_url = get_database_url()

    # Mask password in output
    display_url = database_url
    if '@' in display_url:
        parts = display_url.split('@')
        before_at = parts[0]
        if ':' in before_at:
            proto_user = before_at.rsplit(':', 1)[0]
            display_url = f"{proto_user}:****@{parts[1]}"
    print(f"Database: {display_url}")

    engine = create_engine(database_url)

    print("\nStep 1: Creating table...")
    run_migration_sql(engine)

    print("\nStep 2: Seeding plans...")
    seed_plans(engine)

    # Verify
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM discount_plans")).scalar()
        print(f"\nVerification: {count} discount plans in database.")

    print("\n" + "=" * 60)
    print("Done!")
    print("=" * 60)


if __name__ == '__main__':
    main()
