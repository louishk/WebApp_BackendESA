"""Database models for Flask app."""
from web.models.base import Base
from web.models.role import Role
from web.models.user import User
from web.models.page import Page
from web.models.inventory import InventoryTypeMapping, InventoryUnitOverride
from web.models.api_statistic import ApiStatistic
from web.models.external_api_statistic import ExternalApiStatistic
from web.models.discount_plan import DiscountPlan
from web.models.discount_plan_config import DiscountPlanConfig
from web.models.api_key import ApiKey
from web.models.smart_lock import (
    SmartLockKeypad, SmartLockPadlock, SmartLockUnitAssignment, SmartLockAuditLog,
)
