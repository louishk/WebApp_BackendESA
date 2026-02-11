"""Inventory checker models for naming convention standardization."""

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Boolean, UniqueConstraint

from web.models.base import Base


class InventoryTypeMapping(Base):
    """
    Maps existing sTypeName values from SiteLink units_info
    to standardized SOP unit type codes.
    """
    __tablename__ = 'inventory_type_mappings'

    id = Column(Integer, primary_key=True)
    source_type_name = Column(String(100), unique=True, nullable=False)
    mapped_type_code = Column(String(10), nullable=False)
    mapped_climate_code = Column(String(5))
    created_by = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'source_type_name': self.source_type_name,
            'mapped_type_code': self.mapped_type_code,
            'mapped_climate_code': self.mapped_climate_code,
            'created_by': self.created_by,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self):
        return f"<InventoryTypeMapping {self.source_type_name} -> {self.mapped_type_code}>"


class InventoryUnitOverride(Base):
    """
    Per-unit overrides for auto-calculated naming convention fields.
    NULL values mean use the auto-calculated value.
    """
    __tablename__ = 'inventory_unit_overrides'

    id = Column(Integer, primary_key=True)
    site_id = Column(Integer, nullable=False)
    unit_id = Column(Integer, nullable=False)
    unit_type_code = Column(String(10))
    size_category = Column(String(5))
    size_range = Column(String(10))
    shape = Column(String(5))
    pillar = Column(String(5))
    climate_code = Column(String(5))
    reviewed = Column(Boolean, default=False)
    updated_by = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('site_id', 'unit_id', name='uq_inventory_override_site_unit'),
    )

    def to_dict(self):
        return {
            'id': self.id,
            'site_id': self.site_id,
            'unit_id': self.unit_id,
            'unit_type_code': self.unit_type_code,
            'size_category': self.size_category,
            'size_range': self.size_range,
            'shape': self.shape,
            'pillar': self.pillar,
            'climate_code': self.climate_code,
            'reviewed': self.reviewed or False,
            'updated_by': self.updated_by,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self):
        return f"<InventoryUnitOverride site={self.site_id} unit={self.unit_id}>"
