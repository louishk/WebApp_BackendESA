"""ECRI reason-code tables — admin-editable via /admin/ecri-reasons."""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime

from web.models.base import Base


class ECRIExclusionReason(Base):
    """Admin-editable list of reasons ops can use when requesting exclusion."""
    __tablename__ = 'ecri_exclusion_reasons'

    id = Column(Integer, primary_key=True)
    code = Column(String(40), unique=True, nullable=False)
    label = Column(String(200), nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    sort_order = Column(Integer, nullable=False, default=100)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'code': self.code,
            'label': self.label,
            'active': self.active,
            'sort_order': self.sort_order,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }

    def __repr__(self):
        return f"<ECRIExclusionReason {self.code}>"


class ECRIObjectionReason(Base):
    """Admin-editable list of reasons ops can use when raising an objection."""
    __tablename__ = 'ecri_objection_reasons'

    id = Column(Integer, primary_key=True)
    code = Column(String(40), unique=True, nullable=False)
    label = Column(String(200), nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    sort_order = Column(Integer, nullable=False, default=100)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'code': self.code,
            'label': self.label,
            'active': self.active,
            'sort_order': self.sort_order,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }

    def __repr__(self):
        return f"<ECRIObjectionReason {self.code}>"
