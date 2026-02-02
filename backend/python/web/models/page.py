"""Page model for Flask app."""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


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
    is_secure = Column(Boolean, default=False)  # Requires login to view
    edit_restricted = Column(Boolean, default=False)  # Admin-only edit
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Allowed file extensions
    ALLOWED_EXTENSIONS = ['php', 'html', 'js', 'css', 'txt']

    def to_dict(self):
        """Convert page to dictionary."""
        return {
            'id': self.id,
            'title': self.title,
            'slug': self.slug,
            'content': self.content,
            'extension': self.extension,
            'is_secure': self.is_secure,
            'edit_restricted': self.edit_restricted,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }

    def __repr__(self):
        return f"<Page {self.slug}>"
