"""API Key model for external API access with per-endpoint scopes."""

import hashlib
import secrets
from datetime import datetime

from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import JSONB, ARRAY
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from web.models.base import Base


# Available API scopes — add new ones here as endpoints are added
API_SCOPES = {
    'discount_plans:read': 'Read discount plans',
    'discount_plans:write': 'Create / update discount plans',
    'scheduler:read': 'Read scheduler jobs and history',
    'scheduler:write': 'Trigger and manage scheduler jobs',
    'ecri:read': 'Read ECRI batches and eligibility',
    'statistics:read': 'Read API usage statistics',
    'inventory:read': 'Read unit inventory data',
}


def generate_api_key():
    """
    Generate a new API key pair.

    Returns:
        tuple: (key_id, raw_secret, full_key)
            - key_id: 8-char public prefix for identification (stored in DB)
            - raw_secret: 40-char random secret (shown once to user)
            - full_key: "esa_{key_id}.{secret}" — what the user sends in headers
    """
    key_id = secrets.token_hex(4)           # 8 chars
    raw_secret = secrets.token_urlsafe(30)  # ~40 chars
    full_key = f"esa_{key_id}.{raw_secret}"
    return key_id, raw_secret, full_key


def hash_api_secret(raw_secret):
    """Hash the secret portion of an API key for safe storage."""
    return hashlib.sha256(raw_secret.encode('utf-8')).hexdigest()


class ApiKey(Base):
    """
    API Key for authenticated external access.

    Each key belongs to a user and has a set of scopes controlling which
    API endpoints it can access. The raw secret is shown once at creation
    and only the SHA-256 hash is stored.
    """
    __tablename__ = 'api_keys'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    name = Column(String(255), nullable=False, comment="User-given label for this key")
    key_id = Column(String(16), unique=True, nullable=False, comment="Public prefix for identification")
    key_hash = Column(String(64), nullable=False, comment="SHA-256 hash of the secret")

    # Scopes: list of scope strings, e.g. ["discount_plans:read", "scheduler:read"]
    scopes = Column(JSONB, nullable=False, default=list, comment="Allowed API scopes")

    is_active = Column(Boolean, nullable=False, default=True)
    last_used_at = Column(DateTime(timezone=True), comment="Last time this key was used")
    expires_at = Column(DateTime(timezone=True), comment="Optional expiry date")

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    # Relationship
    user = relationship('User', backref='api_keys')

    def verify_secret(self, raw_secret):
        """Check a raw secret against the stored hash."""
        return hash_api_secret(raw_secret) == self.key_hash

    def has_scope(self, scope):
        """Check if this key has the given scope."""
        if not self.scopes:
            return False
        return scope in self.scopes

    def is_valid(self):
        """Check if the key is active and not expired."""
        if not self.is_active:
            return False
        if self.expires_at and datetime.utcnow() > self.expires_at.replace(tzinfo=None):
            return False
        return True

    def to_dict(self):
        """Convert to dictionary (never includes the hash)."""
        return {
            'id': self.id,
            'user_id': self.user_id,
            'name': self.name,
            'key_prefix': f"esa_{self.key_id}.",
            'scopes': self.scopes or [],
            'is_active': self.is_active,
            'last_used_at': self.last_used_at.isoformat() if self.last_used_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }

    def __repr__(self):
        return f"<ApiKey {self.key_id} user={self.user_id} name={self.name}>"
