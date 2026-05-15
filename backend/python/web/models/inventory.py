"""Inventory-adjacent models (smart-lock refresh cooldown lives here)."""

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime

from web.models.base import Base


class SmartLockRefreshCooldown(Base):
    """Per-site cooldown tracking for manual smart-lock refresh chains."""
    __tablename__ = 'smart_lock_refresh_cooldowns'

    COOLDOWN_MINUTES = 15

    site_id = Column(Integer, primary_key=True)
    last_refresh_at = Column(DateTime, nullable=False)
    last_refresh_by = Column(Integer, nullable=True)
    last_chain_id = Column(String(64), nullable=True)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)
