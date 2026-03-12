"""
Database-backed Secrets Vault for WebApp Backend

Stores encrypted secrets in the app_secrets table (esa_backend DB).
Drop-in replacement for LocalSecretsVault with the same interface.

Bootstrap secrets that MUST remain as env vars / .env:
  - DB_PASSWORD (needed to connect to the DB that holds the secrets)
  - VAULT_MASTER_KEY (needed to decrypt the values)

Works in ALL contexts: Flask web app, scheduler daemon, datalayer scripts,
deploy scripts — no Flask app context required.
"""

import os
import logging
import base64
import secrets as secrets_module
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
from sqlalchemy import Column, Integer, String, DateTime, Text, create_engine
from sqlalchemy.engine import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

VALID_ENVIRONMENTS = {'all', 'production', 'development'}

# Standalone model — no dependency on web.models.base
_Base = declarative_base()


class AppSecretRow(_Base):
    """Canonical model for app_secrets table. Used by all vault operations."""
    __tablename__ = 'app_secrets'

    id = Column(Integer, primary_key=True)
    key = Column(String(100), unique=True, nullable=False, index=True)
    value_encrypted = Column(Text, nullable=False)
    environment = Column(String(20), nullable=False, default='all', index=True)
    description = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_by = Column(String(100), nullable=True)


def _build_backend_db_url() -> URL:
    """
    Build esa_backend DB URL from database.yaml + env var DB_PASSWORD.
    Bypasses config_loader entirely to avoid circular dependency.
    Returns sqlalchemy.engine.URL (password masked in repr/str).
    """
    yaml_path = None
    search_dirs = [
        Path(__file__).parent.parent.parent.parent / 'backend' / 'config',
        Path(__file__).parent.parent / 'config',
        Path(__file__).parent.parent.parent / 'config',
    ]
    for d in search_dirs:
        candidate = d / 'database.yaml'
        if candidate.exists():
            yaml_path = candidate
            break

    if yaml_path is None:
        raise FileNotFoundError("Cannot find database.yaml for bootstrap DB connection")

    with open(yaml_path, 'r') as f:
        db_config = yaml.safe_load(f)

    backend = db_config.get('backend', {})
    password = os.environ.get('DB_PASSWORD')
    if not password:
        raise ValueError(
            "DB_PASSWORD not found in environment. "
            "This is a bootstrap secret that must be set as an env var or in .env"
        )

    return URL.create(
        drivername='postgresql',
        username=backend.get('username', 'postgres'),
        password=password,
        host=backend.get('host', 'localhost'),
        port=backend.get('port', 5432),
        database=backend.get('name', 'backend'),
        query={'sslmode': backend.get('sslmode', 'require')},
    )


class DatabaseSecretsVault:
    """
    Encrypted secrets stored in PostgreSQL app_secrets table.
    Same interface as LocalSecretsVault for drop-in replacement.
    """

    _SALT_META_KEY = '_VAULT_SALT'

    def __init__(self, db_url=None, master_key: str = None, environment: str = None):
        """
        Args:
            db_url: SQLAlchemy URL for esa_backend. If None, auto-built from database.yaml + env.
            master_key: Encryption master key. Falls back to VAULT_MASTER_KEY env var.
            environment: 'production', 'development', or None (defaults to APP_ENVIRONMENT env).
        """
        self._db_url = db_url
        self._environment = environment or os.environ.get('APP_ENVIRONMENT', 'all')
        self._engine = None
        self._session_factory = None
        self._engine_lock = threading.Lock()
        self._fernet = None

        self._cache = {}
        self._cache_ttl = 30
        self._cache_timestamps = {}

        # Resolve master key
        master_key = master_key or os.environ.get('VAULT_MASTER_KEY')
        if not master_key:
            key_file = self._find_key_file()
            if key_file and key_file.exists():
                with open(key_file, 'rb') as f:
                    master_key = f.read().decode('utf-8').strip()
            else:
                raise ValueError(
                    "Master key not found. Set VAULT_MASTER_KEY environment variable."
                )
        self._master_key = master_key

        # Init Fernet with per-instance salt from DB (or legacy fallback)
        salt = self._load_or_create_salt()
        self._fernet = self._create_fernet(master_key, salt)

    @staticmethod
    def _find_key_file() -> Optional[Path]:
        """Search for .vault_master_key file (legacy fallback)."""
        return None

    def _create_fernet(self, master_key: str, salt: bytes) -> Fernet:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        key = base64.urlsafe_b64encode(kdf.derive(master_key.encode()))
        return Fernet(key)

    def _load_or_create_salt(self) -> bytes:
        """Load per-instance salt from DB, or create one on first run."""
        session = self._get_session()
        try:
            row = session.query(AppSecretRow).filter(
                AppSecretRow.key == self._SALT_META_KEY
            ).first()

            if row:
                return base64.b64decode(row.value_encrypted)

            # First run: generate random salt and store it
            salt = secrets_module.token_bytes(16)
            meta = AppSecretRow(
                key=self._SALT_META_KEY,
                value_encrypted=base64.b64encode(salt).decode('utf-8'),
                environment='all',
                description='PBKDF2 salt for vault encryption (do not delete)',
                updated_by='system',
            )
            session.add(meta)
            session.commit()
            logger.info("Generated new vault salt")
            return salt
        except Exception as e:
            try:
                session.rollback()
            except Exception:
                pass
            # Race condition: another process inserted the salt first
            from sqlalchemy.exc import IntegrityError
            if isinstance(e, IntegrityError):
                row = session.query(AppSecretRow).filter(
                    AppSecretRow.key == self._SALT_META_KEY
                ).first()
                if row:
                    return base64.b64decode(row.value_encrypted)
            # DB not ready — fail hard, do not silently degrade to weak static salt
            logger.critical(f"Cannot load vault salt from DB: {type(e).__name__}. "
                            "Ensure app_secrets table exists and DB is reachable.")
            raise RuntimeError(
                "DB vault salt unavailable. Run migration 025_app_secrets.sql first."
            ) from e
        finally:
            session.close()

    def _get_session(self):
        """Lazy-init DB engine with thread-safe double-checked locking."""
        if self._engine is None:
            with self._engine_lock:
                if self._engine is None:
                    if self._db_url is None:
                        self._db_url = _build_backend_db_url()
                    self._engine = create_engine(self._db_url, pool_pre_ping=True)
                    self._session_factory = sessionmaker(bind=self._engine)
        return self._session_factory()

    def get(self, key: str, default: Optional[Any] = None) -> Optional[Any]:
        if key in self._cache:
            ts = self._cache_timestamps.get(key, 0)
            if datetime.utcnow().timestamp() - ts < self._cache_ttl:
                return self._cache[key]

        session = self._get_session()
        try:
            rows = session.query(AppSecretRow).filter(
                AppSecretRow.key == key,
                AppSecretRow.environment.in_([self._environment, 'all'])
            ).all()

            if not rows:
                return default

            # Prefer environment-specific over 'all'
            row = next((r for r in rows if r.environment == self._environment), None)
            if row is None:
                row = next((r for r in rows if r.environment == 'all'), None)
            if row is None:
                return default

            value = self._fernet.decrypt(row.value_encrypted.encode('utf-8')).decode('utf-8')

            self._cache[key] = value
            self._cache_timestamps[key] = datetime.utcnow().timestamp()
            return value
        except Exception as e:
            logger.error(f"Failed to get secret '{key}': {type(e).__name__}")
            return default
        finally:
            session.close()

    def set(self, key: str, value: Any, environment: str = None,
            description: str = None, updated_by: str = None) -> None:
        encrypted = self._fernet.encrypt(str(value).encode('utf-8')).decode('utf-8')
        env = environment or self._environment
        if env not in VALID_ENVIRONMENTS:
            raise ValueError(f"Invalid environment '{env}'. Must be one of: {VALID_ENVIRONMENTS}")

        session = self._get_session()
        try:
            row = session.query(AppSecretRow).filter(
                AppSecretRow.key == key,
                AppSecretRow.environment == env
            ).first()

            if row:
                row.value_encrypted = encrypted
                row.updated_at = datetime.utcnow()
                if description is not None:
                    row.description = description
                if updated_by:
                    row.updated_by = updated_by
            else:
                row = AppSecretRow(
                    key=key,
                    value_encrypted=encrypted,
                    environment=env,
                    description=description,
                    updated_by=updated_by,
                )
                session.add(row)

            session.commit()

            self._cache[key] = str(value)
            self._cache_timestamps[key] = datetime.utcnow().timestamp()

            logger.info(f"Secret '{key}' set for environment '{env}'")
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to set secret '{key}': {type(e).__name__}")
            raise
        finally:
            session.close()

    def delete(self, key: str) -> bool:
        if key == self._SALT_META_KEY:
            logger.warning("Attempted to delete vault salt meta-key — blocked")
            return False
        session = self._get_session()
        try:
            rows = session.query(AppSecretRow).filter(AppSecretRow.key == key).all()
            if not rows:
                return False
            for row in rows:
                session.delete(row)
            session.commit()

            self._cache.pop(key, None)
            self._cache_timestamps.pop(key, None)

            logger.info(f"Secret '{key}' deleted")
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to delete secret '{key}': {type(e).__name__}")
            return False
        finally:
            session.close()

    def list_keys(self) -> List[str]:
        session = self._get_session()
        try:
            rows = session.query(AppSecretRow.key).filter(
                AppSecretRow.environment.in_([self._environment, 'all']),
                AppSecretRow.key != self._SALT_META_KEY,
            ).distinct().all()
            return [r[0] for r in rows]
        except Exception as e:
            logger.error(f"Failed to list secrets: {type(e).__name__}")
            return []
        finally:
            session.close()

    def list_all(self) -> List[Dict[str, Any]]:
        """List all secrets with metadata (no values). For admin UI."""
        session = self._get_session()
        try:
            rows = session.query(AppSecretRow).filter(
                AppSecretRow.key != self._SALT_META_KEY,
            ).order_by(AppSecretRow.key).all()
            return [{
                'id': r.id,
                'key': r.key,
                'environment': r.environment,
                'description': r.description,
                'has_value': bool(r.value_encrypted),
                'created_at': r.created_at.isoformat() if r.created_at else None,
                'updated_at': r.updated_at.isoformat() if r.updated_at else None,
                'updated_by': r.updated_by,
            } for r in rows]
        except Exception as e:
            logger.error(f"Failed to list all secrets: {type(e).__name__}")
            return []
        finally:
            session.close()

    def has_key(self, key: str) -> bool:
        """Check if a secret exists without decrypting it."""
        session = self._get_session()
        try:
            return session.query(AppSecretRow.id).filter(
                AppSecretRow.key == key,
                AppSecretRow.environment.in_([self._environment, 'all'])
            ).first() is not None
        except Exception:
            return False
        finally:
            session.close()

    def clear_cache(self):
        self._cache.clear()
        self._cache_timestamps.clear()
