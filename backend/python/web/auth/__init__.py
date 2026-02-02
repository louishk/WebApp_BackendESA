"""Authentication module for Flask app."""
# JWT auth for API routes
from .jwt_auth import require_auth, optional_auth, init_auth

# OAuth for Microsoft login
from .oauth import init_oauth, oauth

# Session auth for web UI
from .session_auth import login_manager

# Decorators for role-based access
from .decorators import login_required, require_roles, admin_required, scheduler_access_required, editor_required
