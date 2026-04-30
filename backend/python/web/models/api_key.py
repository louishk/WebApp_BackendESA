"""API Key model for external API access with per-endpoint scopes, rate limits, and quotas."""

import bcrypt
import secrets
from datetime import datetime, date, timezone

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
    'sync:read': 'Read sync orchestrator pipelines and state',
    'sync:write': 'Manage sync orchestrator pipelines, trigger runs, reset state',
    'ecri:read': 'Read ECRI batches and eligibility',
    'statistics:read': 'Read API usage statistics',
    'inventory:read': 'Read unit inventory data',
    'inventory:write': 'Update inventory mappings and overrides',
    'reservations:read': 'Read reservations, notes, and fees',
    'reservations:write': 'Create/update/cancel reservations and tenants',
    'reservations:track': 'Push external reservation tracking records and lifecycle events',
    'smart_lock:read': 'Read smart lock assignments, keypads, and padlocks',
    'smart_lock:write': 'Create/update smart lock assignments and devices',
    'reservation_fees:read': 'Read per-site reservation fees',
    'reservation_fees:write': 'Create/update/delete per-site reservation fees',
    'billing:read': 'Read billing data (tax rates, charges, ledgers, payments)',
    'billing:write': 'Write billing operations (add charges, apply payments, refunds)',
    'recommender': 'Legacy alias of recommender:read (kept for backward compat)',
    'recommender:read': 'Recommendation engine read — POST /api/recommendations + GET /api/reservations/move-in/cost',
    'recommender:write': 'Recommendation engine write — POST /api/reservations/reserve + POST /api/reservations/move-in (idempotent + perpetual/prepay orchestration)',
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
    """Hash the secret portion of an API key using bcrypt."""
    return bcrypt.hashpw(raw_secret.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')


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
    key_hash = Column(String(255), nullable=False, comment="bcrypt hash of the secret")

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

    # MCP access control
    mcp_enabled = Column(Boolean, nullable=False, default=False,
                         comment="Whether this key can access the MCP server")
    mcp_tools = Column(JSONB, nullable=False, default=list,
                       comment="Allowed MCP tool names (empty list = all tools)")
    mcp_db_presets = Column(JSONB, nullable=False, default=list,
                            comment="Allowed DB preset names (empty list = all presets)")
    mcp_db_table_rules = Column(JSONB, nullable=False, default=dict,
                                 comment="Per-preset table allow-lists (empty = all tables)")

    is_active = Column(Boolean, nullable=False, default=True)
    last_used_at = Column(DateTime(timezone=True), comment="Last time this key was used")
    expires_at = Column(DateTime(timezone=True), comment="Optional expiry date")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    # Relationship — one-to-one
    user = relationship('User', backref='api_key', uselist=False)

    def verify_secret(self, raw_secret):
        """Check a raw secret against the stored bcrypt hash."""
        return bcrypt.checkpw(raw_secret.encode('utf-8'), self.key_hash.encode('utf-8'))

    def has_scope(self, scope):
        """Check if this key has the given scope."""
        if not self.scopes:
            return False
        return scope in self.scopes

    def is_valid(self):
        """Check if the key is active and not expired."""
        if not self.is_active:
            return False
        if self.expires_at and datetime.now(timezone.utc) > self.expires_at.replace(tzinfo=timezone.utc):
            return False
        return True

    def check_and_increment_quota(self):
        """Deprecated: quota enforcement is now done atomically in jwt_auth._authenticate_api_key."""
        raise NotImplementedError("Use the atomic SQL UPDATE in jwt_auth._authenticate_api_key instead.")

    def has_mcp_tool_access(self, tool_name):
        """Check if this key can access a specific MCP tool. Empty list = all tools allowed."""
        if not self.mcp_enabled:
            return False
        if not self.mcp_tools:
            return True  # Empty list means all tools
        return tool_name in self.mcp_tools

    def get_allowed_tables(self, preset_name):
        """Get allowed tables for a preset. Returns None if no restrictions."""
        if not self.mcp_db_table_rules:
            return None
        tables = self.mcp_db_table_rules.get(preset_name)
        if not tables:
            return None  # Empty list or missing key = no restrictions
        return tables

    def to_dict(self):
        """Convert to dictionary (never includes the hash)."""
        return {
            'id': self.id,
            'user_id': self.user_id,
            'name': self.name,
            'key_prefix': f"esa_{self.key_id}.",
            'scopes': self.scopes or [],
            'mcp_enabled': self.mcp_enabled,
            'mcp_tools': self.mcp_tools or [],
            'mcp_db_presets': self.mcp_db_presets or [],
            'mcp_db_table_rules': self.mcp_db_table_rules or {},
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
