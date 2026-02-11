"""Page model for Flask app."""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean

from web.models.base import Base


class Page(Base):
    """
    Page model for content management.

    Stores dynamic pages that can be served by the application.
    """
    __tablename__ = 'pages'

    id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    slug = Column(String(255), unique=True, nullable=False)
    content = Column(Text, default='')
    extension = Column(String(10), default='html')

    # Access control
    is_public = Column(Boolean, default=False)  # No login required
    view_roles = Column(String(255), default='')  # Comma-separated role IDs
    view_users = Column(Text, default='')  # Comma-separated user IDs
    edit_roles = Column(String(255), default='')  # Comma-separated role IDs
    edit_users = Column(Text, default='')  # Comma-separated user IDs

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Allowed file extensions
    ALLOWED_EXTENSIONS = ['html', 'js', 'css', 'txt']

    def _parse_id_list(self, value):
        """Parse comma-separated ID string into list of strings."""
        if not value:
            return []
        return [x.strip() for x in value.split(',') if x.strip()]

    def get_view_roles_list(self):
        """Get list of role IDs that can view this page."""
        return self._parse_id_list(self.view_roles)

    def get_view_users_list(self):
        """Get list of user IDs that can view this page."""
        return self._parse_id_list(self.view_users)

    def get_edit_roles_list(self):
        """Get list of role IDs that can edit this page."""
        return self._parse_id_list(self.edit_roles)

    def get_edit_users_list(self):
        """Get list of user IDs that can edit this page."""
        return self._parse_id_list(self.edit_users)

    def can_view(self, user):
        """
        Check if user can view this page.

        Logic:
        - Public pages: anyone can view
        - Users with page management permission can view all pages
        - No restrictions (empty view_roles and view_users): any authenticated user
        - Otherwise: user must have matching role ID or be in user list
        """
        if self.is_public:
            return True
        if not user or not user.is_authenticated:
            return False
        # Users who can manage pages can view all pages
        if user.can_manage_pages():
            return True
        # No restrictions means any authenticated user can view
        if not self.view_roles and not self.view_users:
            return True
        # Check role-based access
        role_ids = self.get_view_roles_list()
        if role_ids and user.has_any_role_id(role_ids):
            return True
        # Check user-specific access
        user_ids = self.get_view_users_list()
        if user_ids and str(user.id) in user_ids:
            return True
        return False

    def can_edit(self, user):
        """
        Check if user can edit this page.

        Logic:
        - No restrictions (empty edit_roles and edit_users): any authenticated user
        - Otherwise: user must have matching role ID or be in user list
        """
        if not user or not user.is_authenticated:
            return False
        # Users with page management permission can always edit
        if hasattr(user, 'can_manage_pages') and user.can_manage_pages():
            return True
        # No restrictions set: require page management permission (secure default)
        if not self.edit_roles and not self.edit_users:
            return False
        # Check role-based access
        role_ids = self.get_edit_roles_list()
        if role_ids and user.has_any_role_id(role_ids):
            return True
        # Check user-specific access
        user_ids = self.get_edit_users_list()
        if user_ids and str(user.id) in user_ids:
            return True
        return False

    def to_dict(self):
        """Convert page to dictionary."""
        return {
            'id': self.id,
            'title': self.title,
            'slug': self.slug,
            'content': self.content,
            'extension': self.extension,
            'is_public': self.is_public,
            'view_roles': self.view_roles,
            'view_users': self.view_users,
            'edit_roles': self.edit_roles,
            'edit_users': self.edit_users,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self):
        return f"<Page {self.slug}>"
