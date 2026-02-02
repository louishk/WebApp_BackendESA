"""User model for Flask app."""

from datetime import datetime
from flask_login import UserMixin
from sqlalchemy import Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


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
    role = Column(String(20), nullable=False, default='viewer')
    auth_provider = Column(String(20), default='local')  # 'local' or 'microsoft'
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Valid roles
    ROLES = ['admin', 'scheduler_admin', 'editor', 'viewer']

    def has_role(self, roles):
        """Check if user has one of the specified roles."""
        if isinstance(roles, str):
            roles = [roles]
        return self.role in roles

    def can_access_scheduler(self):
        """Check if user can access scheduler functionality."""
        return self.role in ['admin', 'scheduler_admin']

    def can_manage_users(self):
        """Check if user can manage other users (admin only)."""
        return self.role == 'admin'

    def can_manage_pages(self):
        """Check if user can manage pages (admin and editor)."""
        return self.role in ['admin', 'editor']

    def to_dict(self):
        """Convert user to dictionary (excluding password)."""
        return {
            'id': self.id,
            'username': self.username,
            'email': self.email,
            'role': self.role,
            'auth_provider': self.auth_provider,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self):
        return f"<User {self.username} ({self.role})>"
