"""URL shortener models for link management and click tracking."""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship

from web.models.base import Base


class ShortLink(Base):
    """
    Shortened URL record.
    Stores the mapping from short code to original URL with metadata.
    """
    __tablename__ = 'short_links'

    id = Column(Integer, primary_key=True)
    short_code = Column(String(20), unique=True, nullable=False, index=True)
    original_url = Column(Text, nullable=False)
    title = Column(String(255))
    tags = Column(String(500))
    is_active = Column(Boolean, nullable=False, default=True)
    expires_at = Column(DateTime, nullable=True)
    password_hash = Column(String(255), nullable=True)
    max_clicks = Column(Integer, nullable=True)
    total_clicks = Column(Integer, nullable=False, default=0)
    unique_clicks = Column(Integer, nullable=False, default=0)
    deep_link_enabled = Column(Boolean, nullable=False, default=False)
    created_by = Column(String(255))
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    clicks = relationship('LinkClick', backref='link', lazy='dynamic', cascade='all, delete-orphan')

    __table_args__ = (
        Index('ix_short_links_created_by', 'created_by'),
        Index('ix_short_links_created_at', 'created_at'),
        Index('ix_short_links_is_active', 'is_active'),
    )

    def is_expired(self):
        """Check if the link has expired."""
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at

    def is_click_capped(self):
        """Check if the link has reached its click cap."""
        if self.max_clicks is None:
            return False
        return self.total_clicks >= self.max_clicks

    def is_accessible(self):
        """Check if the link can be accessed (active, not expired, not capped)."""
        return self.is_active and not self.is_expired() and not self.is_click_capped()

    def to_dict(self, include_url=True):
        result = {
            'id': self.id,
            'short_code': self.short_code,
            'title': self.title,
            'tags': self.tags.split(',') if self.tags else [],
            'is_active': self.is_active,
            'expires_at': self.expires_at.isoformat() if self.expires_at else None,
            'has_password': self.password_hash is not None,
            'max_clicks': self.max_clicks,
            'total_clicks': self.total_clicks,
            'unique_clicks': self.unique_clicks,
            'deep_link_enabled': self.deep_link_enabled,
            'created_by': self.created_by,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }
        if include_url:
            result['original_url'] = self.original_url
        return result

    def __repr__(self):
        return f"<ShortLink {self.short_code} -> {self.original_url[:50]}>"


class LinkClick(Base):
    """
    Individual click event on a shortened link.
    Captures visitor metadata for analytics.
    """
    __tablename__ = 'link_clicks'

    id = Column(Integer, primary_key=True)
    link_id = Column(Integer, ForeignKey('short_links.id', ondelete='CASCADE'), nullable=False)
    clicked_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    ip_address = Column(String(45))
    user_agent = Column(Text)
    referer = Column(Text)
    country = Column(String(100))
    city = Column(String(100))
    device_type = Column(String(20))
    browser = Column(String(50))
    os = Column(String(50))

    __table_args__ = (
        Index('ix_link_clicks_link_id', 'link_id'),
        Index('ix_link_clicks_clicked_at', 'clicked_at'),
        Index('ix_link_clicks_link_clicked', 'link_id', 'clicked_at'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'link_id': self.link_id,
            'clicked_at': self.clicked_at.isoformat() if self.clicked_at else None,
            'ip_address': self.ip_address,
            'referer': self.referer,
            'country': self.country,
            'city': self.city,
            'device_type': self.device_type,
            'browser': self.browser,
            'os': self.os,
        }

    def __repr__(self):
        return f"<LinkClick link_id={self.link_id} at={self.clicked_at}>"
