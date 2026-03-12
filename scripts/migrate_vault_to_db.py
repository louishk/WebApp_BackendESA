#!/usr/bin/env python3
"""
Migrate secrets from file-based .vault/ to app_secrets DB table.

Usage:
    python scripts/migrate_vault_to_db.py [--dry-run] [--environment all]

Requires:
    - VAULT_MASTER_KEY env var (or .vault/.key file)
    - DB_PASSWORD env var (or in .env)
"""

import sys
import os
import argparse
import logging

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'backend', 'python'))

# Load .env for bootstrap secrets
from dotenv import load_dotenv
env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
if os.path.exists(env_path):
    load_dotenv(env_path)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# Keys that must stay as env vars (bootstrap secrets)
BOOTSTRAP_KEYS = {'DB_PASSWORD', 'PBI_DB_PASSWORD', 'VAULT_MASTER_KEY'}


def main():
    parser = argparse.ArgumentParser(description='Migrate file vault to DB secrets table')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be migrated without writing')
    parser.add_argument('--environment', default='all', help='Environment tag for migrated secrets (default: all)')
    args = parser.parse_args()

    # Load existing file vault
    from common.secrets_vault import LocalSecretsVault, _find_vault_dir
    vault_dir = _find_vault_dir()
    file_vault = LocalSecretsVault(vault_dir=vault_dir)
    keys = file_vault.list_keys()

    if not keys:
        logger.info("No secrets found in file vault. Nothing to migrate.")
        return

    logger.info(f"Found {len(keys)} secrets in file vault")

    # Separate bootstrap vs migratable
    to_migrate = [k for k in keys if k not in BOOTSTRAP_KEYS]
    skipped = [k for k in keys if k in BOOTSTRAP_KEYS]

    if skipped:
        logger.info(f"Skipping bootstrap secrets (keep as env vars): {', '.join(skipped)}")

    if args.dry_run:
        logger.info("=== DRY RUN ===")
        for key in to_migrate:
            logger.info(f"  Would migrate: {key} (environment={args.environment})")
        logger.info(f"Total: {len(to_migrate)} secrets to migrate")
        return

    # Build DB URL directly (bypasses config_loader to avoid circular vault dependency)
    from common.db_secrets_vault import _build_backend_db_url, DatabaseSecretsVault

    db_url = _build_backend_db_url()

    # Ensure table exists
    from sqlalchemy import create_engine, text
    engine = create_engine(db_url)
    migration_sql = os.path.join(
        os.path.dirname(__file__), '..', 'backend', 'python', 'migrations', '025_app_secrets.sql'
    )
    if os.path.exists(migration_sql):
        with open(migration_sql, 'r') as f:
            sql = f.read()
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
        logger.info("Ensured app_secrets table exists")

    # Initialize DB vault
    db_vault = DatabaseSecretsVault(db_url=db_url, environment=args.environment)

    migrated = 0
    errors = 0
    for key in to_migrate:
        try:
            value = file_vault.get(key)
            if value is not None:
                db_vault.set(
                    key=key,
                    value=value,
                    environment=args.environment,
                    updated_by='migrate_vault_to_db'
                )
                logger.info(f"  Migrated: {key}")
                migrated += 1
            else:
                logger.warning(f"  Skipped (null value): {key}")
        except Exception as e:
            logger.error(f"  Failed to migrate {key}: {type(e).__name__}")
            errors += 1

    logger.info(f"Migration complete: {migrated} migrated, {errors} errors, {len(skipped)} bootstrap skipped")

    if errors:
        sys.exit(1)


if __name__ == '__main__':
    main()
