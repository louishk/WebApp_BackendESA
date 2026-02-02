"""
Create Customer Views in PostgreSQL

Creates the following views for leads deduplication and customer journey analysis:
- vw_customer_master: Base view with customer_id and enrichment columns
- vw_customer_summary: Aggregated customer metrics
- vw_customer_journey: Timeline view with journey metrics

Usage:
    python create_customer_views.py
"""

import os
import sys
import logging
from pathlib import Path

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


def create_views():
    """Create all customer views in PostgreSQL."""
    # Load environment
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / '.env'
    load_dotenv(env_path)

    logger.info("Starting view creation process")

    # Create database connection
    db_config = get_db_config()
    engine = create_engine_from_config(db_config)

    # Read SQL file
    sql_file = Path(__file__).parent.parent / 'sql' / '02_customer_views.sql'

    with open(sql_file, 'r') as f:
        sql_content = f.read()

    # Split into individual statements (by CREATE OR REPLACE VIEW and COMMENT ON VIEW)
    # We'll execute each view creation separately for better error handling

    with engine.connect() as conn:
        # View 1: vw_customer_master
        logger.info("Creating view: vw_customer_master...")
        try:
            # Extract and execute vw_customer_master
            view1_sql = extract_view_sql(sql_content, 'vw_customer_master')
            conn.execute(text(view1_sql))
            conn.commit()
            logger.info("✓ vw_customer_master created successfully")
        except Exception as e:
            logger.error(f"✗ Failed to create vw_customer_master: {e}")
            raise

        # View 2: vw_customer_summary
        logger.info("Creating view: vw_customer_summary...")
        try:
            view2_sql = extract_view_sql(sql_content, 'vw_customer_summary')
            conn.execute(text(view2_sql))
            conn.commit()
            logger.info("✓ vw_customer_summary created successfully")
        except Exception as e:
            logger.error(f"✗ Failed to create vw_customer_summary: {e}")
            raise

        # View 3: vw_customer_journey
        logger.info("Creating view: vw_customer_journey...")
        try:
            view3_sql = extract_view_sql(sql_content, 'vw_customer_journey')
            conn.execute(text(view3_sql))
            conn.commit()
            logger.info("✓ vw_customer_journey created successfully")
        except Exception as e:
            logger.error(f"✗ Failed to create vw_customer_journey: {e}")
            raise

        # Verify views
        logger.info("\nVerifying views...")

        # Check vw_customer_master
        result = conn.execute(text("""
            SELECT
                COUNT(*) as total_leads,
                COUNT(DISTINCT customer_id) as unique_customers,
                COUNT(*) FILTER (WHERE is_identifiable = true) as identifiable_leads
            FROM vw_customer_master
        """))
        row = result.fetchone()
        logger.info(f"  vw_customer_master: {row[0]:,} leads, {row[1]:,} unique customers, {row[2]:,} identifiable")

        # Check vw_customer_summary
        result = conn.execute(text("""
            SELECT
                COUNT(*) as total_customers,
                COUNT(*) FILTER (WHERE is_repeat_customer = true) as repeat_customers,
                COUNT(*) FILTER (WHERE ever_converted = true) as converted_customers
            FROM vw_customer_summary
        """))
        row = result.fetchone()
        logger.info(f"  vw_customer_summary: {row[0]:,} customers, {row[1]:,} repeat, {row[2]:,} converted")

        # Check vw_customer_journey
        result = conn.execute(text("""
            SELECT
                COUNT(*) as total_records,
                COUNT(*) FILTER (WHERE is_first_lead = true) as first_touches,
                COUNT(*) FILTER (WHERE journey_stage = 'Re-engagement') as re_engagements
            FROM vw_customer_journey
        """))
        row = result.fetchone()
        logger.info(f"  vw_customer_journey: {row[0]:,} records, {row[1]:,} first touches, {row[2]:,} re-engagements")

    logger.info("\n✓ All views created and verified successfully!")


def extract_view_sql(full_sql: str, view_name: str) -> str:
    """Extract a single view's CREATE statement from the full SQL file."""
    import re

    # Pattern to match CREATE OR REPLACE VIEW ... until the next CREATE or COMMENT or end
    dashes = "-" * 40
    pattern = rf"(CREATE OR REPLACE VIEW {view_name} AS.*?)((?=CREATE OR REPLACE VIEW)|(?=-- {dashes})|\Z)"

    match = re.search(pattern, full_sql, re.DOTALL | re.IGNORECASE)

    if match:
        view_sql = match.group(1).strip()
        # Remove trailing comments
        view_sql = re.sub(r"COMMENT ON VIEW.*?;", "", view_sql, flags=re.DOTALL)
        # Ensure it ends with semicolon
        if not view_sql.rstrip().endswith(';'):
            view_sql = view_sql.rstrip() + ';'
        return view_sql
    else:
        raise ValueError(f"Could not find view {view_name} in SQL file")


if __name__ == '__main__':
    create_views()
