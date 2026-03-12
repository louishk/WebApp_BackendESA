"""
Secrets Vault Interface for WebApp Backend

Public API:
    get_vault()      — singleton vault instance (DatabaseSecretsVault)
    vault_config()   — get config from vault with env fallback
    secure_config()  — alias for vault_config()

All secrets are stored in the app_secrets table (esa_backend DB).
Bootstrap secrets (DB_PASSWORD, VAULT_MASTER_KEY) must be set as env vars.
"""

import os
import logging
from typing import Any

logger = logging.getLogger(__name__)

# Singleton instance
_vault_instance = None


def get_vault():
    """
    Get or create vault singleton instance.
    Uses DB-backed vault (app_secrets table in esa_backend).
    """
    global _vault_instance

    if _vault_instance is None:
        from common.db_secrets_vault import DatabaseSecretsVault
        _vault_instance = DatabaseSecretsVault()
        logger.info("Using database-backed secrets vault")

    return _vault_instance


def vault_config(
    key: str,
    default: Any = None,
    cast: type = None
) -> Any:
    """
    Get configuration value from vault with fallback to environment.
    Drop-in replacement for python-decouple's config().

    Args:
        key: Configuration key
        default: Default value if not found
        cast: Type to cast the value to (int, bool, float, etc.)

    Returns:
        Configuration value
    """
    value = None

    # Try vault first
    try:
        vault = get_vault()
        value = vault.get(key)
    except (ValueError, Exception):
        pass  # Vault not available

    # Fallback to environment
    if value is None:
        value = os.environ.get(key)

    # Use default if still not found
    if value is None:
        value = default

    # Apply cast if specified and value exists
    if value is not None and cast is not None:
        if cast == bool:
            if isinstance(value, str):
                value = value.lower() in ('true', '1', 'yes', 'on')
            else:
                value = bool(value)
        else:
            value = cast(value)

    return value


def secure_config(key: str, default: Any = None, cast: type = None) -> Any:
    """Alias for vault_config for clearer intent"""
    return vault_config(key, default, cast)
