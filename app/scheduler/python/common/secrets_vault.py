"""
Local Secrets Vault Implementation for WebApp Backend
Provides encrypted storage for sensitive configuration data

Supports both PHP (main backend) and Python (scheduler) applications.
"""

import os
import json
import base64
import logging
from pathlib import Path
from typing import Any, Dict, Optional, List, Union
from datetime import datetime
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
import secrets as secrets_module

logger = logging.getLogger(__name__)


class LocalSecretsVault:
    """
    Local encrypted secrets storage for WebApp Backend and Scheduler
    """

    # Unique salt for WebApp Backend
    VAULT_SALT = b'webapp-backend-vault-2024'

    # Sensitive keys that should be stored in vault
    SENSITIVE_KEYS = [
        # ===========================================
        # Main Backend (.env)
        # ===========================================
        # SSH Credentials
        'VM_SSH_PASSWORD',

        # Database Passwords
        'DB_PASSWORD',

        # Microsoft OAuth
        'MS_OAUTH_CLIENT_SECRET',

        # JWT Configuration
        'JWT_SECRET',

        # ===========================================
        # Scheduler App (.env)
        # ===========================================
        # Scheduler Database
        'SCHEDULER_DB_PASSWORD',

        # PBI Database
        'PBI_DB_PASSWORD',

        # SOAP API
        'SOAP_API_KEY',
        'SOAP_CORP_PASSWORD',

        # SugarCRM
        'SUGARCRM_PASSWORD',
        'SUGARCRM_CLIENT_SECRET',

        # Alerts
        'SLACK_WEBHOOK_URL',
        'SMTP_PASSWORD',

        # Backend Auth
        'BACKEND_AUTH_SECRET',

        # ===========================================
        # Generic patterns (auto-detected)
        # ===========================================
        # Any key ending with _PASSWORD, _SECRET, _API_KEY, _TOKEN
    ]

    def __init__(self, vault_dir: str = ".vault", master_key_env: str = "VAULT_MASTER_KEY"):
        """
        Initialize the local secrets vault

        Args:
            vault_dir: Directory to store encrypted secrets
            master_key_env: Environment variable name for master key
        """
        self.vault_dir = Path(vault_dir)
        self.master_key_env = master_key_env
        self.secrets_file = self.vault_dir / "secrets.enc"
        self.metadata_file = self.vault_dir / "metadata.json"
        self.rotation_log = self.vault_dir / "rotation.log"

        self._fernet = None
        self._cache = {}
        self._cache_ttl = 300  # 5 minutes cache
        self._cache_timestamps = {}

        self._ensure_vault_structure()
        self._initialize_encryption()

    def _ensure_vault_structure(self):
        """Create vault directory structure if it doesn't exist"""
        self.vault_dir.mkdir(exist_ok=True)

        # Set restrictive permissions (owner only)
        try:
            os.chmod(self.vault_dir, 0o700)
        except (OSError, PermissionError):
            logger.warning("Could not set restrictive permissions on vault directory")

        # Create default metadata if not exists
        if not self.metadata_file.exists():
            metadata = {
                "created_at": datetime.utcnow().isoformat(),
                "last_rotation": None,
                "version": "1.0.0",
                "salt_identifier": "webapp-backend",
                "rotation_policy": {
                    "enabled": True,
                    "interval_days": 90,
                    "last_check": datetime.utcnow().isoformat()
                }
            }
            self._save_metadata(metadata)

    def _save_metadata(self, metadata: Dict[str, Any]):
        """Save vault metadata"""
        with open(self.metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        try:
            os.chmod(self.metadata_file, 0o600)
        except (OSError, PermissionError):
            pass

    def _load_metadata(self) -> Dict[str, Any]:
        """Load vault metadata"""
        if self.metadata_file.exists():
            with open(self.metadata_file, 'r') as f:
                return json.load(f)
        return {}

    def _initialize_encryption(self):
        """Initialize encryption with master key"""
        master_key = os.environ.get(self.master_key_env)

        if not master_key:
            # Try to load from key file
            key_file = self.vault_dir / ".key"
            if key_file.exists():
                with open(key_file, 'rb') as f:
                    master_key = f.read().decode('utf-8').strip()
                # Set in environment for current session
                os.environ[self.master_key_env] = master_key
            else:
                raise ValueError(
                    f"Master key not found. Set {self.master_key_env} environment variable "
                    f"or run scripts/setup_vault.py to initialize"
                )

        # Derive encryption key from master key
        self._fernet = self._create_fernet(master_key)

    def _create_fernet(self, master_key: str) -> Fernet:
        """Create Fernet instance from master key"""
        # Use PBKDF2 to derive key from master key
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=self.VAULT_SALT,
            iterations=100000,
            backend=default_backend()
        )
        key = base64.urlsafe_b64encode(kdf.derive(master_key.encode()))
        return Fernet(key)

    def get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        """
        Get a secret value

        Args:
            key: Secret key
            default: Default value if key not found

        Returns:
            Decrypted secret value or default
        """
        # Check cache first
        if key in self._cache:
            timestamp = self._cache_timestamps.get(key, 0)
            if datetime.utcnow().timestamp() - timestamp < self._cache_ttl:
                return self._cache[key]

        # Load from encrypted storage
        secrets = self._load_secrets()
        value = secrets.get(key, default)

        # Update cache
        if value is not None:
            self._cache[key] = value
            self._cache_timestamps[key] = datetime.utcnow().timestamp()

        return value

    def set(self, key: str, value: Any) -> None:
        """
        Set a secret value

        Args:
            key: Secret key
            value: Secret value to encrypt and store
        """
        secrets = self._load_secrets()
        secrets[key] = value
        self._save_secrets(secrets)

        # Update cache
        self._cache[key] = value
        self._cache_timestamps[key] = datetime.utcnow().timestamp()

        # Log the change (without the value)
        self._log_operation(f"SET: {key}")

    def delete(self, key: str) -> bool:
        """
        Delete a secret

        Args:
            key: Secret key to delete

        Returns:
            True if deleted, False if not found
        """
        secrets = self._load_secrets()
        if key in secrets:
            del secrets[key]
            self._save_secrets(secrets)

            # Remove from cache
            self._cache.pop(key, None)
            self._cache_timestamps.pop(key, None)

            self._log_operation(f"DELETE: {key}")
            return True
        return False

    def list_keys(self) -> List[str]:
        """List all secret keys (not values)"""
        secrets = self._load_secrets()
        return list(secrets.keys())

    def _load_secrets(self) -> Dict[str, Any]:
        """Load and decrypt secrets from file"""
        if not self.secrets_file.exists():
            return {}

        try:
            with open(self.secrets_file, 'rb') as f:
                encrypted_data = f.read()

            if not encrypted_data:
                return {}

            decrypted_data = self._fernet.decrypt(encrypted_data)
            return json.loads(decrypted_data.decode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to load secrets: {e}")
            return {}

    def _save_secrets(self, secrets: Dict[str, Any]):
        """Encrypt and save secrets to file"""
        try:
            json_data = json.dumps(secrets)
            encrypted_data = self._fernet.encrypt(json_data.encode('utf-8'))

            with open(self.secrets_file, 'wb') as f:
                f.write(encrypted_data)

            # Set restrictive permissions
            try:
                os.chmod(self.secrets_file, 0o600)
            except (OSError, PermissionError):
                pass
        except Exception as e:
            logger.error(f"Failed to save secrets: {e}")
            raise

    def _is_sensitive_key(self, key: str) -> bool:
        """Check if a key should be considered sensitive"""
        return (
            key in self.SENSITIVE_KEYS or
            key.endswith('_PASSWORD') or
            key.endswith('_SECRET') or
            key.endswith('_API_KEY') or
            key.endswith('_TOKEN') or
            'WEBHOOK' in key
        )

    def migrate_from_env(self, env_file: str = ".env") -> Dict[str, str]:
        """
        Migrate secrets from .env file to vault

        Args:
            env_file: Path to .env file

        Returns:
            Dictionary of migrated keys
        """
        migrated = {}
        env_path = Path(env_file)

        if not env_path.exists():
            logger.warning(f"Environment file {env_file} not found")
            return migrated

        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()

                    # Check if this is a sensitive key
                    if self._is_sensitive_key(key) and value:
                        self.set(key, value)
                        migrated[key] = '***MIGRATED***'
                        logger.info(f"Migrated {key} to vault")

        self._log_operation(f"MIGRATION: Migrated {len(migrated)} secrets from {env_file}")
        return migrated

    def rotate_master_key(self) -> str:
        """
        Rotate the master encryption key

        Returns:
            New master key
        """
        # Load current secrets
        secrets = self._load_secrets()

        # Generate new master key
        new_master_key = secrets_module.token_urlsafe(32)

        # Create new Fernet with new key
        new_fernet = self._create_fernet(new_master_key)

        # Re-encrypt all secrets with new key
        json_data = json.dumps(secrets)
        encrypted_data = new_fernet.encrypt(json_data.encode('utf-8'))

        # Save with new encryption
        with open(self.secrets_file, 'wb') as f:
            f.write(encrypted_data)

        # Update metadata
        metadata = self._load_metadata()
        metadata['last_rotation'] = datetime.utcnow().isoformat()
        self._save_metadata(metadata)

        # Update current Fernet
        self._fernet = new_fernet

        self._log_operation("ROTATION: Master key rotated successfully")

        return new_master_key

    def check_rotation_needed(self) -> bool:
        """Check if key rotation is needed based on policy"""
        metadata = self._load_metadata()
        policy = metadata.get('rotation_policy', {})

        if not policy.get('enabled', False):
            return False

        last_rotation = metadata.get('last_rotation')
        if not last_rotation:
            return True

        last_rotation_date = datetime.fromisoformat(last_rotation)
        interval_days = policy.get('interval_days', 90)

        return (datetime.utcnow() - last_rotation_date).days >= interval_days

    def _log_operation(self, message: str):
        """Log vault operations for audit"""
        timestamp = datetime.utcnow().isoformat()
        log_entry = f"[{timestamp}] {message}\n"

        with open(self.rotation_log, 'a') as f:
            f.write(log_entry)

    def export_for_php(self) -> Dict[str, Any]:
        """
        Export vault info for PHP consumption
        Returns metadata needed by PHP to decrypt secrets
        """
        return {
            "vault_dir": str(self.vault_dir.absolute()),
            "secrets_file": str(self.secrets_file.absolute()),
            "salt": base64.b64encode(self.VAULT_SALT).decode('utf-8'),
            "iterations": 100000
        }


# Singleton instance
_vault_instance = None


def get_vault(vault_dir: str = None) -> LocalSecretsVault:
    """
    Get or create vault singleton instance

    Args:
        vault_dir: Path to vault directory. If None, searches for .vault
                   in current directory or parent directories.
    """
    global _vault_instance

    if _vault_instance is None:
        # Auto-discover vault directory if not specified
        if vault_dir is None:
            vault_dir = _find_vault_dir()

        try:
            _vault_instance = LocalSecretsVault(vault_dir=vault_dir)
        except ValueError as e:
            logger.warning(f"Vault not available: {e}")
            raise

    return _vault_instance


def _find_vault_dir() -> str:
    """Find .vault directory by searching up from current directory"""
    current = Path.cwd()

    # Check current and parent directories
    for _ in range(5):  # Limit search depth
        vault_path = current / ".vault"
        if vault_path.exists():
            return str(vault_path)
        if current.parent == current:
            break
        current = current.parent

    # Default to .vault in current directory
    return ".vault"


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
