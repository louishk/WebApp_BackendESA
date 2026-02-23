"""App link rules for deep linking / smart app redirects."""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Index

from web.models.base import Base


class AppLinkRule(Base):
    """
    Maps URL domain patterns to native app URI schemes.
    Used for smart redirects that open the correct app on mobile devices.
    """
    __tablename__ = 'app_link_rules'

    id = Column(Integer, primary_key=True)
    domain_pattern = Column(String(255), nullable=False, unique=True)
    name = Column(String(100), nullable=False)
    ios_scheme = Column(String(500), nullable=True)
    ios_app_store_url = Column(String(500), nullable=True)
    android_scheme = Column(String(500), nullable=True)
    android_play_store_url = Column(String(500), nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    priority = Column(Integer, nullable=False, default=0)
    created_by = Column(String(255))
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('ix_app_link_rules_domain', 'domain_pattern'),
        Index('ix_app_link_rules_active', 'is_active'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'domain_pattern': self.domain_pattern,
            'name': self.name,
            'ios_scheme': self.ios_scheme,
            'ios_app_store_url': self.ios_app_store_url,
            'android_scheme': self.android_scheme,
            'android_play_store_url': self.android_play_store_url,
            'is_active': self.is_active,
            'priority': self.priority,
            'created_by': self.created_by,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self):
        return f"<AppLinkRule {self.name} ({self.domain_pattern})>"
