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
    can_access_scheduler = Column(Boolean, default=False)
    can_access_billing_tools = Column(Boolean, default=False)
    can_manage_users = Column(Boolean, default=False)
    can_manage_pages = Column(Boolean, default=False)
    can_manage_roles = Column(Boolean, default=False)
    can_manage_configs = Column(Boolean, default=False)
    can_access_ecri = Column(Boolean, default=False)
    can_manage_ecri = Column(Boolean, default=False)

    is_system = Column(Boolean, default=False)  # Prevent deletion of core roles
    created_at = Column(DateTime, default=datetime.utcnow)

    # Default roles configuration
    DEFAULT_ROLES = [
        {
            'name': 'admin',
            'description': 'Full system access',
            'can_access_scheduler': True,
            'can_access_billing_tools': True,
            'can_manage_users': True,
            'can_manage_pages': True,
            'can_manage_roles': True,
            'can_manage_configs': True,
            'can_access_ecri': True,
            'can_manage_ecri': True,
            'is_system': True
        },
        {
            'name': 'scheduler_admin',
            'description': 'Scheduler management',
            'can_access_scheduler': True,
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
            'can_access_scheduler': False,
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
            'can_access_scheduler': False,
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
        if self.can_access_scheduler:
            permissions.append('Scheduler')
        if self.can_access_billing_tools:
            permissions.append('Billing Tools')
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
        return permissions

    def to_dict(self):
        """Convert role to dictionary."""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description,
            'can_access_scheduler': self.can_access_scheduler,
            'can_access_billing_tools': self.can_access_billing_tools,
            'can_manage_users': self.can_manage_users,
            'can_manage_pages': self.can_manage_pages,
            'can_manage_roles': self.can_manage_roles,
            'can_manage_configs': self.can_manage_configs,
            'can_access_ecri': self.can_access_ecri,
            'can_manage_ecri': self.can_manage_ecri,
            'is_system': self.is_system,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }

    def __repr__(self):
        return f"<Role {self.name}>"
