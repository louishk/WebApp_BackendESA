"""API Key model for external API access with per-endpoint scopes, rate limits, and quotas."""

import hashlib
import hmac
import secrets
from datetime import datetime, date

from sqlalchemy import Column, Integer, String, Boolean, DateTime, Date, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
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

# Defaults
DEFAULT_RATE_LIMIT = 60       # requests per minute
DEFAULT_DAILY_QUOTA = 10000   # requests per day


def generate_api_key():
    """
    Generate a new API key pair.

    Returns:
        tuple: (key_id, raw_secret, full_key)
            - key_id: 8-char public prefix (stored in DB)
            - raw_secret: ~40-char random secret (shown once)
            - full_key: "esa_{key_id}.{secret}" — sent in X-API-Key header
    """
    key_id = secrets.token_hex(4)
    raw_secret = secrets.token_urlsafe(30)
    full_key = f"esa_{key_id}.{raw_secret}"
    return key_id, raw_secret, full_key


def hash_api_secret(raw_secret):
    """Hash the secret portion of an API key for safe storage."""
    return hashlib.sha256(raw_secret.encode('utf-8')).hexdigest()


class ApiKey(Base):
    """
    API Key for authenticated external access.

    One key per user. Scopes, rate limits, and quotas are managed by admins
    under User Management. The raw secret is shown once at creation;
    only the SHA-256 hash is stored.
    """
    __tablename__ = 'api_keys'

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False, unique=True,
                     comment="One key per user")
    name = Column(String(255), nullable=False, default='Default', comment="Label for this key")
    key_id = Column(String(16), unique=True, nullable=False, comment="Public prefix for identification")
    key_hash = Column(String(64), nullable=False, comment="SHA-256 hash of the secret")

    # Scopes: managed by admins via User Management
    scopes = Column(JSONB, nullable=False, default=list, comment="Allowed API scopes")

    # Rate limiting: requests per minute
    rate_limit = Column(Integer, nullable=False, default=DEFAULT_RATE_LIMIT,
                        comment="Max requests per minute (0 = unlimited)")

    # Daily quota
    daily_quota = Column(Integer, nullable=False, default=DEFAULT_DAILY_QUOTA,
                         comment="Max requests per day (0 = unlimited)")
    daily_usage = Column(Integer, nullable=False, default=0,
                         comment="Request count for current day")
    quota_reset_date = Column(Date, comment="Date when daily_usage was last reset")

    is_active = Column(Boolean, nullable=False, default=True)
    last_used_at = Column(DateTime(timezone=True), comment="Last time this key was used")
    expires_at = Column(DateTime(timezone=True), comment="Optional expiry date")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    # Relationship — one-to-one
    user = relationship('User', backref='api_key', uselist=False)

    def verify_secret(self, raw_secret):
        """Check a raw secret against the stored hash (timing-safe)."""
        return hmac.compare_digest(hash_api_secret(raw_secret), self.key_hash)

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

    def check_and_increment_quota(self):
        """
        Check daily quota and increment usage.
        Auto-resets the counter when the date rolls over.

        Returns:
            tuple: (allowed: bool, remaining: int)
        """
        today = date.today()

        # Reset counter if new day
        if self.quota_reset_date != today:
            self.daily_usage = 0
            self.quota_reset_date = today

        # 0 = unlimited
        if self.daily_quota == 0:
            self.daily_usage += 1
            return True, -1  # -1 means unlimited

        if self.daily_usage >= self.daily_quota:
            return False, 0

        self.daily_usage += 1
        remaining = self.daily_quota - self.daily_usage
        return True, remaining

    def to_dict(self):
        """Convert to dictionary (never includes the hash)."""
        return {
            'id': self.id,
            'user_id': self.user_id,
            'name': self.name,
            'key_prefix': f"esa_{self.key_id}.",
            'scopes': self.scopes or [],
            'rate_limit': self.rate_limit,
            'daily_quota': self.daily_quota,
            'daily_usage': self.daily_usage,
            'is_active': self.is_active,
            'last_used_at': self.last_used_at.isoformat() if self.last_used_at else None,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }

    def __repr__(self):
        return f"<ApiKey {self.key_id} user={self.user_id}>"
