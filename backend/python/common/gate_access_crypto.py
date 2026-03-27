"""
Gate access code encryption/decryption.

Uses VAULT_MASTER_KEY + a dedicated PBKDF2 salt (_GATE_ACCESS_SALT in app_secrets)
to derive a Fernet key. Access codes are encrypted before DB storage and decrypted
on demand when a user clicks "reveal" in the UI.
"""

import base64
import logging
import os
import secrets as secrets_module
import threading

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

_SALT_KEY = '_GATE_ACCESS_SALT'
_ITERATIONS = 100_000

# Module-level singleton
_instance = None
_lock = threading.Lock()


class GateAccessCrypto:
    """Encrypt/decrypt gate access codes using VAULT_MASTER_KEY + dedicated salt."""

    def __init__(self, master_key: str, db_url):
        self._engine = create_engine(db_url, pool_pre_ping=True)
        self._session_factory = sessionmaker(bind=self._engine)
        salt = self._load_or_create_salt()
        self._fernet = self._derive_fernet(master_key, salt)

    def _derive_fernet(self, master_key: str, salt: bytes) -> Fernet:
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=_ITERATIONS,
            backend=default_backend(),
        )
        key = base64.urlsafe_b64encode(kdf.derive(master_key.encode()))
        return Fernet(key)

    def _load_or_create_salt(self) -> bytes:
        from common.db_secrets_vault import AppSecretRow

        session = self._session_factory()
        try:
            row = session.query(AppSecretRow).filter(
                AppSecretRow.key == _SALT_KEY
            ).first()

            if row:
                return base64.b64decode(row.value_encrypted)

            # First run — generate 16-byte random salt, store raw (not Fernet-encrypted)
            salt = secrets_module.token_bytes(16)
            meta = AppSecretRow(
                key=_SALT_KEY,
                value_encrypted=base64.b64encode(salt).decode('utf-8'),
                environment='all',
                description='PBKDF2 salt for gate access code encryption (do not delete)',
                updated_by='system',
            )
            session.add(meta)
            session.commit()
            logger.info("Generated new gate access encryption salt")
            return salt
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def encrypt(self, plaintext: str) -> str:
        """Encrypt an access code. Returns base64 Fernet ciphertext."""
        if not plaintext:
            return ''
        return self._fernet.encrypt(plaintext.encode('utf-8')).decode('utf-8')

    def decrypt(self, ciphertext: str) -> str:
        """Decrypt an access code. Returns plaintext."""
        if not ciphertext:
            return ''
        return self._fernet.decrypt(ciphertext.encode('utf-8')).decode('utf-8')


def get_gate_crypto() -> GateAccessCrypto:
    """Get or create the module-level singleton."""
    global _instance
    if _instance is None:
        with _lock:
            if _instance is None:
                from common.config_loader import get_database_url
                master_key = os.environ.get('VAULT_MASTER_KEY')
                if not master_key:
                    raise RuntimeError("VAULT_MASTER_KEY not set")
                db_url = get_database_url('backend')
                _instance = GateAccessCrypto(master_key, db_url)
    return _instance
