"""User model for Flask app."""

from datetime import datetime
from flask_login import UserMixin
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, Table
from sqlalchemy.orm import relationship

from web.models.base import Base

# Many-to-many join table for users <-> roles
user_roles = Table(
    'user_roles',
    Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id', ondelete='CASCADE'), primary_key=True),
    Column('role_id', Integer, ForeignKey('roles.id', ondelete='CASCADE'), primary_key=True),
)


class User(Base, UserMixin):
    """
    User model for authentication and authorization.

    Supports both local username/password and Microsoft OAuth authentication.
    Users can have multiple roles; permissions are the union (OR) of all assigned roles.
    """
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String(255), unique=True, nullable=False)
    email = Column(String(255), unique=True, nullable=True)
    password = Column(String(255), nullable=True)  # NULL for OAuth-only users
    role_id = Column(Integer, ForeignKey('roles.id'), nullable=True)  # Legacy, kept for compat
    auth_provider = Column(String(20), default='local')  # 'local' or 'microsoft'
    department = Column(String(255), nullable=True)
    job_title = Column(String(255), nullable=True)
    office_location = Column(String(255), nullable=True)
    employee_id = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Many-to-many relationship to Role
    roles = relationship('Role', secondary=user_roles, backref='role_users')

    @property
    def role(self):
        """Backward-compatible property returning the first role (or None)."""
        return self.roles[0] if self.roles else None

    def can_access_scheduler(self):
        """Check if user can access scheduler functionality."""
        return any(r.can_access_scheduler for r in self.roles)

    def can_access_billing_tools(self):
        """Check if user can access billing tools (Billing Date Changer, etc.)."""
        return any(r.can_access_billing_tools for r in self.roles)

    def can_access_inventory_tools(self):
        """Check if user can access inventory tools (Inventory Checker, etc.)."""
        return any(r.can_access_inventory_tools for r in self.roles)

    def can_manage_users(self):
        """Check if user can manage other users."""
        return any(r.can_manage_users for r in self.roles)

    def can_manage_pages(self):
        """Check if user can manage pages."""
        return any(r.can_manage_pages for r in self.roles)

    def can_manage_roles(self):
        """Check if user can manage roles."""
        return any(r.can_manage_roles for r in self.roles)

    def can_manage_configs(self):
        """Check if user can manage configurations."""
        return any(r.can_manage_configs for r in self.roles)

    def can_access_ecri(self):
        """Check if user can view ECRI dashboards and eligibility."""
        return any(r.can_access_ecri for r in self.roles)

    def can_manage_ecri(self):
        """Check if user can create batches and execute ECRI pushes."""
        return any(r.can_manage_ecri for r in self.roles)

    def has_role(self, role_names):
        """Check if user has one of the specified role names."""
        if isinstance(role_names, str):
            role_names = [role_names]
        return any(r.name in role_names for r in self.roles)

    def has_any_role_id(self, role_ids):
        """Check if user has any of the given role IDs. Used by page access control."""
        return any(str(r.id) in role_ids for r in self.roles)

    def to_dict(self):
        """Convert user to dictionary (excluding password)."""
        return {
            'id': self.id,
            'username': self.username,
            'email': self.email,
            'role_id': self.roles[0].id if self.roles else None,
            'role_name': self.roles[0].name if self.roles else None,
            'role_names': [r.name for r in self.roles],
            'auth_provider': self.auth_provider,
            'department': self.department,
            'job_title': self.job_title,
            'office_location': self.office_location,
            'employee_id': self.employee_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self):
        role_names = ', '.join(r.name for r in self.roles) if self.roles else 'no-role'
        return f"<User {self.username} ({role_names})>"
