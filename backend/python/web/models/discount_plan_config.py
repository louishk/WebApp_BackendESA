"""DiscountPlanConfig model for translatable dropdown options."""

from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func

from web.models.base import Base


class DiscountPlanConfig(Base):
    """
    Configurable dropdown options for discount plan fields.

    Each row defines one option for a specific field (e.g. deposit, payment_terms).
    The option_value stores the English text; translations holds JSONB with
    language-code keys (ko, zh_cn, zh_tw, ms, ja).
    """
    __tablename__ = 'discount_plan_config'

    FIELD_NAMES = [
        'deposit', 'payment_terms', 'termination_notice',
        'switch_to_us', 'referral_program', 'distribution_channel',
        'move_in_range', 'lock_in_period',
    ]

    id = Column(Integer, primary_key=True, autoincrement=True)
    field_name = Column(String(50), nullable=False, index=True)
    option_value = Column(String(255), nullable=False)
    translations = Column(JSONB, default=dict)
    sort_order = Column(Integer, default=0)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    def to_dict(self):
        return {
            'id': self.id,
            'field_name': self.field_name,
            'option_value': self.option_value,
            'translations': self.translations or {},
            'sort_order': self.sort_order,
            'is_active': self.is_active,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    def get_translated(self, lang_code):
        """Return translated value for lang_code, falling back to English option_value."""
        if lang_code == 'en':
            return self.option_value
        translations = self.translations or {}
        return translations.get(lang_code, self.option_value)

    def __repr__(self):
        return f"<DiscountPlanConfig {self.field_name}={self.option_value}>"
