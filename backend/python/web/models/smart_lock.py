"""Smart Lock models for keypad/padlock management."""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, DateTime, ForeignKey, UniqueConstraint,
    Boolean, Text,
)
from sqlalchemy.orm import relationship

from web.models.base import Base


class SmartLockKeypad(Base):
    """A 3rd-party keypad identifier assigned to a site."""
    __tablename__ = 'smart_lock_keypads'

    id = Column(Integer, primary_key=True)
    keypad_id = Column(String(50), unique=True, nullable=False)
    site_id = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False, default='not_assigned')
    notes = Column(String(255))
    created_by = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'keypad_id': self.keypad_id,
            'site_id': self.site_id,
            'status': self.status,
            'notes': self.notes,
            'created_by': self.created_by,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self):
        return f"<SmartLockKeypad {self.keypad_id} site={self.site_id}>"


class SmartLockPadlock(Base):
    """A 3rd-party padlock identifier assigned to a site."""
    __tablename__ = 'smart_lock_padlocks'

    id = Column(Integer, primary_key=True)
    padlock_id = Column(String(50), unique=True, nullable=False)
    site_id = Column(Integer, nullable=False)
    status = Column(String(20), nullable=False, default='not_assigned')
    notes = Column(String(255))
    created_by = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'padlock_id': self.padlock_id,
            'site_id': self.site_id,
            'status': self.status,
            'notes': self.notes,
            'created_by': self.created_by,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self):
        return f"<SmartLockPadlock {self.padlock_id} site={self.site_id}>"


class SmartLockUnitAssignment(Base):
    """Links a keypad and/or padlock to a specific unit."""
    __tablename__ = 'smart_lock_unit_assignments'

    id = Column(Integer, primary_key=True)
    site_id = Column(Integer, nullable=False)
    unit_id = Column(Integer, nullable=False)
    keypad_pk = Column(Integer, ForeignKey('smart_lock_keypads.id', ondelete='SET NULL'))
    padlock_pk = Column(Integer, ForeignKey('smart_lock_padlocks.id', ondelete='SET NULL'))
    assigned_by = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    keypad = relationship('SmartLockKeypad', foreign_keys=[keypad_pk], lazy='joined')
    padlock = relationship('SmartLockPadlock', foreign_keys=[padlock_pk], lazy='joined')

    __table_args__ = (
        UniqueConstraint('site_id', 'unit_id', name='uq_sl_assignment_site_unit'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'site_id': self.site_id,
            'unit_id': self.unit_id,
            'keypad_pk': self.keypad_pk,
            'padlock_pk': self.padlock_pk,
            'keypad_id': self.keypad.keypad_id if self.keypad else None,
            'padlock_id': self.padlock.padlock_id if self.padlock else None,
            'assigned_by': self.assigned_by,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self):
        return f"<SmartLockUnitAssignment site={self.site_id} unit={self.unit_id}>"


class SmartLockAuditLog(Base):
    """Append-only audit log for all smart lock operations."""
    __tablename__ = 'smart_lock_audit_log'

    id = Column(Integer, primary_key=True)
    action = Column(String(50), nullable=False)
    entity_type = Column(String(20), nullable=False)
    entity_id = Column(String(50))
    site_id = Column(Integer)
    unit_id = Column(Integer)
    detail = Column(String(500))
    username = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'action': self.action,
            'entity_type': self.entity_type,
            'entity_id': self.entity_id,
            'site_id': self.site_id,
            'unit_id': self.unit_id,
            'detail': self.detail,
            'username': self.username,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }

    def __repr__(self):
        return f"<SmartLockAuditLog {self.action} by {self.username}>"


class GateAccessData(Base):
    """Gate access data from SMD GateAccessData SOAP endpoint.
    Access codes are Fernet-encrypted at rest."""
    __tablename__ = 'gate_access_data'

    id = Column(Integer, primary_key=True)
    location_code = Column(String(10), nullable=False)
    site_id = Column(Integer, nullable=False)
    unit_id = Column(Integer, nullable=False)
    unit_name = Column(String(50), nullable=False)
    is_rented = Column(Boolean, nullable=False, default=False)
    access_code_enc = Column(Text)
    access_code2_enc = Column(Text)
    is_gate_locked = Column(Boolean, nullable=False, default=False)
    is_overlocked = Column(Boolean, nullable=False, default=False)
    keypad_zone = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('location_code', 'unit_id', name='uq_gate_access_loc_unit'),
    )

    def to_dict(self):
        return {
            'location_code': self.location_code,
            'site_id': self.site_id,
            'unit_id': self.unit_id,
            'unit_name': self.unit_name,
            'is_rented': self.is_rented,
            'is_gate_locked': self.is_gate_locked,
            'is_overlocked': self.is_overlocked,
            'keypad_zone': self.keypad_zone,
            'has_access_code': bool(self.access_code_enc),
            'has_access_code2': bool(self.access_code2_enc),
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }
