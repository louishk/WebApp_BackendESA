"""
SugarCRM Leads Data Quality Analysis

Analyzes email/phone field fill rates and duplicate patterns to support
customer deduplication and journey tracking.

Usage:
    python leads_data_quality.py [--output csv|json|console]

Requires:
    - PostgreSQL connection configured in .env
    - sugarcrm_leads table populated
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import text

# Add common module to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from common.config import DatabaseConfig, DatabaseType
from common.engine import create_engine_from_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# SQL QUERIES
# =============================================================================

QUERY_FILL_RATES = """
SELECT
    field_name,
    filled_count,
    total_count,
    fill_rate_pct
FROM (
    SELECT 'TOTAL RECORDS' as field_name,
           COUNT(*) as filled_count,
           COUNT(*) as total_count,
           100.0 as fill_rate_pct,
           0 as sort_order
    FROM sugarcrm_leads WHERE deleted = false

    UNION ALL

    SELECT 'email1' as field_name,
           COUNT(*) FILTER (WHERE email1 IS NOT NULL AND TRIM(email1) != ''),
           COUNT(*),
           ROUND(100.0 * COUNT(*) FILTER (WHERE email1 IS NOT NULL AND TRIM(email1) != '') / NULLIF(COUNT(*), 0), 2),
           1
    FROM sugarcrm_leads WHERE deleted = false

    UNION ALL

    SELECT 'email' as field_name,
           COUNT(*) FILTER (WHERE email IS NOT NULL AND TRIM(email) != ''),
           COUNT(*),
           ROUND(100.0 * COUNT(*) FILTER (WHERE email IS NOT NULL AND TRIM(email) != '') / NULLIF(COUNT(*), 0), 2),
           2
    FROM sugarcrm_leads WHERE deleted = false

    UNION ALL

    SELECT 'email2' as field_name,
           COUNT(*) FILTER (WHERE email2 IS NOT NULL AND TRIM(email2) != ''),
           COUNT(*),
           ROUND(100.0 * COUNT(*) FILTER (WHERE email2 IS NOT NULL AND TRIM(email2) != '') / NULLIF(COUNT(*), 0), 2),
           3
    FROM sugarcrm_leads WHERE deleted = false

    UNION ALL

    SELECT 'webtolead_email1' as field_name,
           COUNT(*) FILTER (WHERE webtolead_email1 IS NOT NULL AND TRIM(webtolead_email1) != ''),
           COUNT(*),
           ROUND(100.0 * COUNT(*) FILTER (WHERE webtolead_email1 IS NOT NULL AND TRIM(webtolead_email1) != '') / NULLIF(COUNT(*), 0), 2),
           4
    FROM sugarcrm_leads WHERE deleted = false

    UNION ALL

    SELECT 'webtolead_email2' as field_name,
           COUNT(*) FILTER (WHERE webtolead_email2 IS NOT NULL AND TRIM(webtolead_email2) != ''),
           COUNT(*),
           ROUND(100.0 * COUNT(*) FILTER (WHERE webtolead_email2 IS NOT NULL AND TRIM(webtolead_email2) != '') / NULLIF(COUNT(*), 0), 2),
           5
    FROM sugarcrm_leads WHERE deleted = false

    UNION ALL

    SELECT 'phone_mobile' as field_name,
           COUNT(*) FILTER (WHERE phone_mobile IS NOT NULL AND TRIM(phone_mobile) != ''),
           COUNT(*),
           ROUND(100.0 * COUNT(*) FILTER (WHERE phone_mobile IS NOT NULL AND TRIM(phone_mobile) != '') / NULLIF(COUNT(*), 0), 2),
           6
    FROM sugarcrm_leads WHERE deleted = false

    UNION ALL

    SELECT 'phone_work' as field_name,
           COUNT(*) FILTER (WHERE phone_work IS NOT NULL AND TRIM(phone_work) != ''),
           COUNT(*),
           ROUND(100.0 * COUNT(*) FILTER (WHERE phone_work IS NOT NULL AND TRIM(phone_work) != '') / NULLIF(COUNT(*), 0), 2),
           7
    FROM sugarcrm_leads WHERE deleted = false
) sub
ORDER BY sort_order
"""

QUERY_CONTACT_COVERAGE = """
SELECT
    contact_category,
    lead_count,
    ROUND(100.0 * lead_count / SUM(lead_count) OVER(), 2) as percentage
FROM (
    SELECT
        CASE
            WHEN has_any_email AND has_mobile THEN '1. Both Email & Mobile'
            WHEN has_any_email AND NOT has_mobile THEN '2. Email Only'
            WHEN NOT has_any_email AND has_mobile THEN '3. Mobile Only'
            ELSE '4. Neither (Unidentifiable)'
        END as contact_category,
        COUNT(*) as lead_count
    FROM (
        SELECT
            sugar_id,
            (COALESCE(
                NULLIF(TRIM(email1), ''),
                NULLIF(TRIM(email), ''),
                NULLIF(TRIM(email2), ''),
                NULLIF(TRIM(webtolead_email1), ''),
                NULLIF(TRIM(webtolead_email2), '')
            ) IS NOT NULL) as has_any_email,
            (phone_mobile IS NOT NULL AND TRIM(phone_mobile) != ''
             AND LENGTH(REGEXP_REPLACE(phone_mobile, '[^0-9]', '', 'g')) >= 8) as has_mobile
        FROM sugarcrm_leads
        WHERE deleted = false
    ) sub
    GROUP BY 1
) final
ORDER BY contact_category
"""

QUERY_EMAIL_DUPLICATES = """
WITH email_normalized AS (
    SELECT
        sugar_id,
        LOWER(TRIM(COALESCE(
            NULLIF(TRIM(email1), ''),
            NULLIF(TRIM(email), ''),
            NULLIF(TRIM(email2), ''),
            NULLIF(TRIM(webtolead_email1), ''),
            NULLIF(TRIM(webtolead_email2), '')
        ))) as primary_email
    FROM sugarcrm_leads
    WHERE deleted = false
),
email_counts AS (
    SELECT
        primary_email,
        COUNT(*) as lead_count
    FROM email_normalized
    WHERE primary_email IS NOT NULL
    GROUP BY primary_email
)
SELECT
    duplicate_category,
    unique_email_count as unique_customers,
    total_leads,
    ROUND(100.0 * unique_email_count / SUM(unique_email_count) OVER(), 2) as pct_of_customers,
    ROUND(100.0 * total_leads / SUM(total_leads) OVER(), 2) as pct_of_leads
FROM (
    SELECT
        CASE
            WHEN lead_count = 1 THEN '1. Single lead (unique)'
            WHEN lead_count = 2 THEN '2. Two leads'
            WHEN lead_count BETWEEN 3 AND 5 THEN '3. Three to five leads'
            WHEN lead_count BETWEEN 6 AND 10 THEN '4. Six to ten leads'
            ELSE '5. More than 10 leads'
        END as duplicate_category,
        COUNT(*) as unique_email_count,
        SUM(lead_count) as total_leads,
        MIN(lead_count) as min_for_sort
    FROM email_counts
    GROUP BY 1
) sub
ORDER BY min_for_sort
"""

QUERY_TOP_DUPLICATES = """
WITH email_normalized AS (
    SELECT
        sugar_id,
        full_name,
        date_entered,
        status,
        lead_source,
        LOWER(TRIM(COALESCE(
            NULLIF(TRIM(email1), ''),
            NULLIF(TRIM(email), ''),
            NULLIF(TRIM(email2), '')
        ))) as primary_email
    FROM sugarcrm_leads
    WHERE deleted = false
)
SELECT
    primary_email,
    COUNT(*) as lead_count,
    MIN(date_entered)::date as first_lead,
    MAX(date_entered)::date as last_lead
FROM email_normalized
WHERE primary_email IS NOT NULL
GROUP BY primary_email
HAVING COUNT(*) > 5
ORDER BY lead_count DESC
LIMIT 20
"""

QUERY_SUMMARY = """
WITH stats AS (
    SELECT
        COUNT(*) as total_leads,
        COUNT(DISTINCT LOWER(TRIM(COALESCE(
            NULLIF(TRIM(email1), ''),
            NULLIF(TRIM(email), ''),
            NULLIF(TRIM(email2), ''),
            NULLIF(TRIM(webtolead_email1), ''),
            'MOBILE:' || REGEXP_REPLACE(phone_mobile, '[^0-9+]', '', 'g')
        )))) as unique_customers
    FROM sugarcrm_leads
    WHERE deleted = false
)
SELECT
    total_leads,
    unique_customers,
    total_leads - unique_customers as duplicate_leads,
    ROUND(100.0 * (total_leads - unique_customers) / NULLIF(total_leads, 0), 2) as duplicate_percentage,
    ROUND(total_leads::decimal / NULLIF(unique_customers, 0), 2) as avg_leads_per_customer
FROM stats
"""


def get_db_config() -> DatabaseConfig:
    """Load PostgreSQL configuration from environment."""
    return DatabaseConfig(
        db_type=DatabaseType.POSTGRESQL,
        host=os.getenv('POSTGRESQL_HOST', 'localhost'),
        port=int(os.getenv('POSTGRESQL_PORT', '5432')),
        database=os.getenv('POSTGRESQL_DATABASE', 'mydb'),
        username=os.getenv('POSTGRESQL_USERNAME', 'user'),
        password=os.getenv('POSTGRESQL_PASSWORD', 'password')
    )


def run_analysis(output_format: str = 'console', output_dir: Optional[str] = None):
    """
    Run the data quality analysis and output results.

    Args:
        output_format: 'console', 'csv', or 'json'
        output_dir: Directory for output files (defaults to script directory)
    """
    # Load environment
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(env_path)

    logger.info("Starting SugarCRM Leads Data Quality Analysis")

    # Create database connection
    db_config = get_db_config()
    engine = create_engine_from_config(db_config)

    results = {}

    with engine.connect() as conn:
        # 1. Field Fill Rates
        logger.info("Analyzing field fill rates...")
        df_fill_rates = pd.read_sql(text(QUERY_FILL_RATES), conn)
        results['fill_rates'] = df_fill_rates

        # 2. Contact Coverage
        logger.info("Analyzing contact coverage (email vs mobile)...")
        df_coverage = pd.read_sql(text(QUERY_CONTACT_COVERAGE), conn)
        results['contact_coverage'] = df_coverage

        # 3. Email Duplicates
        logger.info("Analyzing duplicate patterns...")
        df_duplicates = pd.read_sql(text(QUERY_EMAIL_DUPLICATES), conn)
        results['email_duplicates'] = df_duplicates

        # 4. Top Duplicated Emails
        logger.info("Finding top duplicated emails...")
        df_top_dupes = pd.read_sql(text(QUERY_TOP_DUPLICATES), conn)
        results['top_duplicates'] = df_top_dupes

        # 5. Summary Statistics
        logger.info("Calculating summary statistics...")
        df_summary = pd.read_sql(text(QUERY_SUMMARY), conn)
        results['summary'] = df_summary

    # Output results
    if output_format == 'console':
        print_results(results)
    elif output_format == 'csv':
        save_csv(results, output_dir)
    elif output_format == 'json':
        save_json(results, output_dir)

    logger.info("Analysis complete!")
    return results


def print_results(results: dict):
    """Print results to console with formatting."""
    print("\n" + "=" * 70)
    print("SUGARCRM LEADS DATA QUALITY ANALYSIS")
    print("=" * 70)

    # Summary
    print("\n### SUMMARY ###")
    summary = results['summary'].iloc[0]
    print(f"  Total Leads:           {summary['total_leads']:,}")
    print(f"  Unique Customers:      {summary['unique_customers']:,}")
    print(f"  Duplicate Leads:       {summary['duplicate_leads']:,}")
    print(f"  Duplicate Percentage:  {summary['duplicate_percentage']}%")
    print(f"  Avg Leads/Customer:    {summary['avg_leads_per_customer']}")

    # Fill Rates
    print("\n### FIELD FILL RATES ###")
    print(results['fill_rates'].to_string(index=False))

    # Contact Coverage
    print("\n### CONTACT COVERAGE ###")
    print(results['contact_coverage'].to_string(index=False))

    # Duplicate Distribution
    print("\n### DUPLICATE DISTRIBUTION ###")
    print(results['email_duplicates'].to_string(index=False))

    # Top Duplicates
    print("\n### TOP DUPLICATED EMAILS (>5 leads) ###")
    if len(results['top_duplicates']) > 0:
        print(results['top_duplicates'].to_string(index=False))
    else:
        print("  No emails with more than 5 leads found.")

    print("\n" + "=" * 70)


def save_csv(results: dict, output_dir: Optional[str] = None):
    """Save results to CSV files."""
    if output_dir is None:
        output_dir = Path(__file__).parent / 'output'
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    for name, df in results.items():
        filepath = output_dir / f"leads_quality_{name}_{timestamp}.csv"
        df.to_csv(filepath, index=False)
        logger.info(f"Saved: {filepath}")


def save_json(results: dict, output_dir: Optional[str] = None):
    """Save results to JSON file."""
    if output_dir is None:
        output_dir = Path(__file__).parent / 'output'
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filepath = output_dir / f"leads_quality_analysis_{timestamp}.json"

    # Convert DataFrames to dict
    output = {}
    for name, df in results.items():
        output[name] = df.to_dict(orient='records')

    with open(filepath, 'w') as f:
        json.dump(output, f, indent=2, default=str)

    logger.info(f"Saved: {filepath}")


def main():
    parser = argparse.ArgumentParser(
        description='SugarCRM Leads Data Quality Analysis'
    )
    parser.add_argument(
        '--output', '-o',
        choices=['console', 'csv', 'json'],
        default='console',
        help='Output format (default: console)'
    )
    parser.add_argument(
        '--output-dir', '-d',
        help='Output directory for csv/json files'
    )

    args = parser.parse_args()
    run_analysis(args.output, args.output_dir)


if __name__ == '__main__':
    main()
