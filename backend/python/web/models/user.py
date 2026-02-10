"""User model for Flask app."""

from datetime import datetime
from flask_login import UserMixin
from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relationship

from web.models.base import Base


class User(Base, UserMixin):
    """
    User model for authentication and authorization.

    Supports both local username/password and Microsoft OAuth authentication.
    """
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String(255), unique=True, nullable=False)
    email = Column(String(255), unique=True, nullable=True)
    password = Column(String(255), nullable=True)  # NULL for OAuth-only users
    role_id = Column(Integer, ForeignKey('roles.id'), nullable=False)
    auth_provider = Column(String(20), default='local')  # 'local' or 'microsoft'
    department = Column(String(255), nullable=True)
    job_title = Column(String(255), nullable=True)
    office_location = Column(String(255), nullable=True)
    employee_id = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationship to Role
    role = relationship('Role', backref='users')

    def can_access_scheduler(self):
        """Check if user can access scheduler functionality."""
        return self.role.can_access_scheduler if self.role else False

    def can_access_billing_tools(self):
        """Check if user can access billing tools (Billing Date Changer, etc.)."""
        return self.role.can_access_billing_tools if self.role else False

    def can_manage_users(self):
        """Check if user can manage other users."""
        return self.role.can_manage_users if self.role else False

    def can_manage_pages(self):
        """Check if user can manage pages."""
        return self.role.can_manage_pages if self.role else False

    def can_manage_roles(self):
        """Check if user can manage roles."""
        return self.role.can_manage_roles if self.role else False

    def can_manage_configs(self):
        """Check if user can manage configurations."""
        return self.role.can_manage_configs if self.role else False

    def can_access_ecri(self):
        """Check if user can view ECRI dashboards and eligibility."""
        return self.role.can_access_ecri if self.role else False

    def can_manage_ecri(self):
        """Check if user can create batches and execute ECRI pushes."""
        return self.role.can_manage_ecri if self.role else False

    def has_role(self, role_names):
        """Check if user has one of the specified role names."""
        if isinstance(role_names, str):
            role_names = [role_names]
        return self.role.name in role_names if self.role else False

    def to_dict(self):
        """Convert user to dictionary (excluding password)."""
        return {
            'id': self.id,
            'username': self.username,
            'email': self.email,
            'role_id': self.role_id,
            'role_name': self.role.name if self.role else None,
            'auth_provider': self.auth_provider,
            'department': self.department,
            'job_title': self.job_title,
            'office_location': self.office_location,
            'employee_id': self.employee_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self):
        role_name = self.role.name if self.role else 'no-role'
        return f"<User {self.username} ({role_name})>"
