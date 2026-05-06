"""Reservation Fee model — per-site reservation fee in local currency."""

from sqlalchemy import Column, Integer, String, DateTime, Numeric
from sqlalchemy.sql import func

from web.models.base import Base


class ReservationFee(Base):
    """One row per site. Currency is implied by the site's country."""
    __tablename__ = 'mw_reservation_fees'

    id = Column(Integer, primary_key=True, autoincrement=True)
    site_id = Column(Integer, nullable=False, unique=True)
    site_code = Column(String(10), nullable=False, index=True)
    reservation_fee = Column(Numeric(12, 2), nullable=False)

    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())
    created_by = Column(String(255))
    updated_by = Column(String(255))

    def to_dict(self):
        return {
            'id': self.id,
            'site_id': self.site_id,
            'site_code': self.site_code,
            'reservation_fee': float(self.reservation_fee) if self.reservation_fee is not None else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'created_by': self.created_by,
            'updated_by': self.updated_by,
        }

    def __repr__(self):
        return f"<ReservationFee site={self.site_code} fee={self.reservation_fee}>"
