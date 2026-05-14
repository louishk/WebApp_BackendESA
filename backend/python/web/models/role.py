"""Role model for Flask app."""

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Boolean

from web.models.base import Base


class Role(Base):
    """
    Role model for role-based access control.

    Defines permissions that can be assigned to users.
    """
    __tablename__ = 'roles'

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(String(255), default='')

    # System permissions
    can_access_sync = Column(Boolean, default=False)
    can_access_billing_tools = Column(Boolean, default=False)
    can_manage_users = Column(Boolean, default=False)
    can_manage_pages = Column(Boolean, default=False)
    can_manage_roles = Column(Boolean, default=False)
    can_manage_configs = Column(Boolean, default=False)
    can_access_inventory_tools = Column(Boolean, default=False)
    can_access_discount_tools = Column(Boolean, default=False)
    can_access_ecri = Column(Boolean, default=False)
    can_manage_ecri = Column(Boolean, default=False)
    can_access_statistics = Column(Boolean, default=False)
    can_access_smart_lock = Column(Boolean, default=False)
    # Admin-tier smart lock: manage bridges/keypads/padlocks + site config.
    # can_access_smart_lock is the basic gate (assignments + refresh).
    can_admin_smart_lock = Column(Boolean, default=False)
    can_access_revenue_tools = Column(Boolean, default=False)
    can_access_pricing_anomalies_tools = Column(Boolean, default=False)
    # ECRI workflow permissions (migration 042)
    can_request_ecri_exclusion = Column(Boolean, default=False)
    can_create_ecri_objection = Column(Boolean, default=False)
    can_approve_ecri_objection = Column(Boolean, default=False)
    can_finalize_ecri_batch = Column(Boolean, default=False)
    can_execute_ecri_batch = Column(Boolean, default=False)
    can_manage_ecri_reasons = Column(Boolean, default=False)

    is_system = Column(Boolean, default=False)  # Prevent deletion of core roles
    created_at = Column(DateTime, default=datetime.utcnow)

    # Default roles configuration
    DEFAULT_ROLES = [
        {
            'name': 'admin',
            'description': 'Full system access',
            'can_access_sync': True,
            'can_access_billing_tools': True,
            'can_access_inventory_tools': True,
            'can_access_discount_tools': True,
            'can_manage_users': True,
            'can_manage_pages': True,
            'can_manage_roles': True,
            'can_manage_configs': True,
            'can_access_ecri': True,
            'can_manage_ecri': True,
            'can_access_statistics': True,
            'can_access_smart_lock': True,
            'can_admin_smart_lock': True,
            'can_access_revenue_tools': True,
            'can_access_pricing_anomalies_tools': True,
            'is_system': True
        },
        {
            'name': 'sync_admin',
            'description': 'Scheduler management',
            'can_access_sync': True,
            'can_access_billing_tools': True,
            'can_manage_users': False,
            'can_manage_pages': False,
            'can_manage_roles': False,
            'can_manage_configs': False,
            'is_system': True
        },
        {
            'name': 'editor',
            'description': 'Page management',
            'can_access_sync': False,
            'can_access_billing_tools': False,
            'can_manage_users': False,
            'can_manage_pages': True,
            'can_manage_roles': False,
            'can_manage_configs': False,
            'is_system': True
        },
        {
            'name': 'viewer',
            'description': 'Read-only access',
            'can_access_sync': False,
            'can_access_billing_tools': False,
            'can_manage_users': False,
            'can_manage_pages': False,
            'can_manage_roles': False,
            'can_manage_configs': False,
            'is_system': True
        }
    ]

    def get_permissions_list(self):
        """Get list of permission names this role has."""
        permissions = []
        if self.can_access_sync:
            permissions.append('Scheduler')
        if self.can_access_billing_tools:
            permissions.append('Billing Tools')
        if self.can_access_inventory_tools:
            permissions.append('Inventory Tools')
        if self.can_access_discount_tools:
            permissions.append('Discount Tools')
        if self.can_manage_users:
            permissions.append('Users')
        if self.can_manage_pages:
            permissions.append('Pages')
        if self.can_manage_roles:
            permissions.append('Roles')
        if self.can_manage_configs:
            permissions.append('Configs')
        if self.can_access_ecri:
            permissions.append('ECRI View')
        if self.can_manage_ecri:
            permissions.append('ECRI Manage')
        if self.can_access_statistics:
            permissions.append('Statistics')
        if self.can_access_smart_lock:
            permissions.append('Smart Lock')
        if self.can_admin_smart_lock:
            permissions.append('Smart Lock Admin')
        if self.can_access_revenue_tools:
            permissions.append('Revenue Tools')
        if self.can_access_pricing_anomalies_tools:
            permissions.append('Pricing Anomalies')
        if self.can_request_ecri_exclusion:
            permissions.append('ECRI Exclusion Request')
        if self.can_create_ecri_objection:
            permissions.append('ECRI Objection Create')
        if self.can_approve_ecri_objection:
            permissions.append('ECRI Objection Approve')
        if self.can_finalize_ecri_batch:
            permissions.append('ECRI Finalize')
        if self.can_execute_ecri_batch:
            permissions.append('ECRI Execute')
        if self.can_manage_ecri_reasons:
            permissions.append('ECRI Reasons Admin')
        return permissions

    def to_dict(self):
        """Convert role to dictionary."""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'can_access_sync': self.can_access_sync,
            'can_access_billing_tools': self.can_access_billing_tools,
            'can_access_inventory_tools': self.can_access_inventory_tools,
            'can_access_discount_tools': self.can_access_discount_tools,
            'can_manage_users': self.can_manage_users,
            'can_manage_pages': self.can_manage_pages,
            'can_manage_roles': self.can_manage_roles,
            'can_manage_configs': self.can_manage_configs,
            'can_access_ecri': self.can_access_ecri,
            'can_manage_ecri': self.can_manage_ecri,
            'can_access_statistics': self.can_access_statistics,
            'can_access_smart_lock': self.can_access_smart_lock,
            'can_admin_smart_lock': self.can_admin_smart_lock,
            'can_access_revenue_tools': self.can_access_revenue_tools,
            'can_access_pricing_anomalies_tools': self.can_access_pricing_anomalies_tools,
            'can_request_ecri_exclusion': self.can_request_ecri_exclusion,
            'can_create_ecri_objection': self.can_create_ecri_objection,
            'can_approve_ecri_objection': self.can_approve_ecri_objection,
            'can_finalize_ecri_batch': self.can_finalize_ecri_batch,
            'can_execute_ecri_batch': self.can_execute_ecri_batch,
            'can_manage_ecri_reasons': self.can_manage_ecri_reasons,
            'is_system': self.is_system,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }

    def __repr__(self):
        return f"<Role {self.name}>"
