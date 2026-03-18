"""Visit session and shortlist models for walk-in/guided visit workflow."""

from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Numeric, Text, DateTime, ForeignKey, UniqueConstraint,
)
from sqlalchemy.orm import relationship

from web.models.base import Base


class VisitSession(Base):
    __tablename__ = 'visit_sessions'

    id = Column(Integer, primary_key=True)
    lead_id = Column(String(36), nullable=True)
    site_code = Column(String(10), nullable=False)
    staff_user_id = Column(Integer, nullable=False)
    flow_type = Column(String(20), nullable=False, default='walk_in')
    status = Column(String(30), nullable=False, default='active')
    outcome = Column(String(30), nullable=True)
    outcome_notes = Column(Text, nullable=True)
    lost_reason = Column(String(100), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime(timezone=True), nullable=True)

    shortlist_items = relationship(
        'VisitShortlistItem',
        back_populates='session',
        cascade='all, delete-orphan',
        order_by='VisitShortlistItem.sort_order',
    )

    VALID_STATUSES = {'active', 'completed', 'cancelled'}
    VALID_OUTCOMES = {'reserved', 'converted', 'visit_completed', 'lost'}
    VALID_FLOW_TYPES = {'walk_in', 'guided'}

    def to_dict(self):
        return {
            'id': self.id,
            'lead_id': self.lead_id,
            'site_code': self.site_code,
            'staff_user_id': self.staff_user_id,
            'flow_type': self.flow_type,
            'status': self.status,
            'outcome': self.outcome,
            'outcome_notes': self.outcome_notes,
            'lost_reason': self.lost_reason,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'shortlist_items': [item.to_dict() for item in self.shortlist_items],
        }


class VisitShortlistItem(Base):
    __tablename__ = 'visit_shortlist_items'

    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey('visit_sessions.id', ondelete='CASCADE'), nullable=False)
    site_id = Column(Integer, nullable=False)
    unit_id = Column(Integer, nullable=False)
    unit_name = Column(String(50), nullable=True)
    category_label = Column(String(100), nullable=True)
    area = Column(Numeric(10, 2), nullable=True)
    floor = Column(Integer, nullable=True)
    climate_code = Column(String(5), nullable=True)
    std_rate = Column(Numeric(10, 2), nullable=True)
    indicative_rate = Column(Numeric(10, 2), nullable=True)
    discount_plan_id = Column(Integer, nullable=True)
    concession_id = Column(Integer, default=0)
    notes = Column(Text, nullable=True)
    sort_order = Column(Integer, default=0)
    added_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    session = relationship('VisitSession', back_populates='shortlist_items')

    __table_args__ = (
        UniqueConstraint('session_id', 'site_id', 'unit_id', name='uq_shortlist_session_unit'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'session_id': self.session_id,
            'site_id': self.site_id,
            'unit_id': self.unit_id,
            'unit_name': self.unit_name,
            'category_label': self.category_label,
            'area': float(self.area) if self.area else None,
            'floor': self.floor,
            'climate_code': self.climate_code,
            'std_rate': float(self.std_rate) if self.std_rate else None,
            'indicative_rate': float(self.indicative_rate) if self.indicative_rate else None,
            'discount_plan_id': self.discount_plan_id,
            'concession_id': self.concession_id,
            'notes': self.notes,
            'sort_order': self.sort_order,
            'added_at': self.added_at.isoformat() if self.added_at else None,
        }
