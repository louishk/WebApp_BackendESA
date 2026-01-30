# Migrate PHP Backend to Pure Flask/Python

## Goal
Eliminate PHP and Apache entirely. Run everything from a single Flask application with Nginx + Gunicorn.

## Features to Keep
- Microsoft OAuth login
- Local username/password login
- User management (admin only)
- Page management (admin + editor)
- Scheduler (existing Flask functionality)
- RBAC: admin, scheduler_admin, editor, viewer

## Features to Drop
- SEO tools
- PHP proxy layer

---

## Architecture

**Current:** Browser → Nginx/Apache → PHP → Flask (proxy)
**Target:** Browser → Nginx → Gunicorn → Flask (direct)

---

## New Dependencies

Add to `app/scheduler/python/requirements.txt`:
```
Authlib>=1.3.0              # Microsoft OAuth
Flask-Login>=0.6.3          # Session-based auth for web UI
Flask-WTF>=1.2.1            # CSRF protection
bcrypt>=4.1.2               # Password hashing
gunicorn>=21.2.0            # Production WSGI server
```

---

## File Structure Changes

```
app/scheduler/python/web/
├── app.py                    # Update: application factory
├── config.py                 # Keep
│
├── auth/                     # NEW
│   ├── __init__.py
│   ├── jwt_auth.py           # Rename from auth.py (existing JWT)
│   ├── oauth.py              # Microsoft OAuth with Authlib
│   └── decorators.py         # @login_required, @require_role
│
├── models/                   # NEW
│   ├── __init__.py
│   ├── user.py               # User model (from PHP schema)
│   └── page.py               # Page model (from PHP schema)
│
├── routes/                   # NEW
│   ├── __init__.py
│   ├── main.py               # / /dashboard
│   ├── auth.py               # /auth/login /auth/microsoft
│   ├── admin.py              # /admin/users /admin/pages
│   ├── scheduler.py          # Refactor existing scheduler routes
│   └── api.py                # Refactor existing API routes
│
├── templates/
│   ├── base.html             # Update with auth navbar
│   ├── login.html            # NEW
│   ├── dashboard.html        # Update
│   ├── scheduler/            # Move existing scheduler templates
│   │   ├── dashboard.html
│   │   ├── jobs.html
│   │   ├── history.html
│   │   └── settings.html
│   └── admin/                # NEW
│       ├── users/
│       │   ├── list.html
│       │   └── edit.html
│       └── pages/
│           ├── list.html
│           └── edit.html
│
├── static/                   # NEW
│   ├── css/main.css
│   └── img/logo.jpeg
│
└── wsgi.py                   # NEW: Gunicorn entry point
```

---

## Implementation Steps

### Step 1: Create Directory Structure
Create new folders: `auth/`, `models/`, `routes/`, `static/`, `templates/admin/`

### Step 2: Add Dependencies
Update requirements.txt and install

### Step 3: Create User & Page Models
- Port from existing `users` and `pages` PostgreSQL tables
- Use SQLAlchemy declarative base

### Step 4: Implement Authentication
- **Local login**: bcrypt password verification, Flask-Login sessions
- **Microsoft OAuth**: Authlib with Azure AD
- Keep existing JWT for API routes

### Step 5: Create Route Blueprints
- `main_bp`: `/`, `/dashboard`
- `auth_bp`: `/auth/login`, `/auth/microsoft`, `/auth/logout`
- `admin_bp`: `/admin/users/*`, `/admin/pages/*`
- `scheduler_bp`: `/scheduler/*` (refactor from app.py)
- `api_bp`: `/api/*` (refactor from app.py)

### Step 6: Update Templates
- Update `base.html` with auth navbar
- Create `login.html`
- Create admin templates
- Move scheduler templates to `templates/scheduler/`

### Step 7: Create Application Factory
- Refactor `app.py` to use `create_app()` pattern
- Initialize Flask-Login, Authlib, CSRF
- Register all blueprints

### Step 8: Production Setup
- Create `wsgi.py` for Gunicorn
- Update systemd service
- Configure Nginx (remove Apache/PHP)

---

## Key Code: Application Factory

```python
# web/app.py
from flask import Flask
from flask_login import LoginManager
from flask_cors import CORS
from authlib.integrations.flask_client import OAuth

login_manager = LoginManager()
oauth = OAuth()

def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY')
    app.config['SQLALCHEMY_DATABASE_URI'] = build_db_url()

    # Initialize extensions
    login_manager.init_app(app)
    CORS(app, supports_credentials=True)

    # Microsoft OAuth
    oauth.init_app(app)
    oauth.register(
        name='microsoft',
        server_metadata_url=f"https://login.microsoftonline.com/{tenant}/v2.0/.well-known/openid-configuration",
        client_kwargs={'scope': 'openid profile email User.Read'}
    )

    # Register blueprints
    from .routes.main import main_bp
    from .routes.auth import auth_bp
    from .routes.admin import admin_bp
    from .routes.scheduler import scheduler_bp
    from .routes.api import api_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(scheduler_bp)
    app.register_blueprint(api_bp)

    return app
```

---

## Key Code: OAuth Routes

```python
# web/routes/auth.py
@auth_bp.route('/auth/microsoft')
def microsoft_login():
    redirect_uri = url_for('auth.microsoft_callback', _external=True)
    return oauth.microsoft.authorize_redirect(redirect_uri)

@auth_bp.route('/auth/microsoft/callback')
def microsoft_callback():
    token = oauth.microsoft.authorize_access_token()
    user_info = oauth.microsoft.get('https://graph.microsoft.com/v1.0/me').json()

    email = user_info.get('mail') or user_info.get('userPrincipalName')

    # Find or create user (default role: viewer)
    user = User.query.filter_by(email=email).first()
    if not user:
        user = User(username=email.split('@')[0], email=email, role='viewer')
        db.session.add(user)
        db.session.commit()

    login_user(user)
    return redirect(url_for('main.dashboard'))
```

---

## Key Code: User Model

```python
# web/models/user.py
from flask_login import UserMixin
from sqlalchemy import Column, Integer, String, DateTime, Boolean

class User(Base, UserMixin):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(255), unique=True, nullable=False)
    password = Column(String(255), nullable=True)  # NULL for OAuth-only users
    role = Column(String(20), nullable=False, default='viewer')
    auth_provider = Column(String(20), default='local')  # 'local' or 'microsoft'
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    ROLES = ['admin', 'scheduler_admin', 'editor', 'viewer']

    def has_role(self, roles):
        if isinstance(roles, str):
            roles = [roles]
        return self.role in roles

    def can_access_scheduler(self):
        return self.role in ['admin', 'scheduler_admin']

    def can_manage_users(self):
        return self.role == 'admin'

    def can_manage_pages(self):
        return self.role in ['admin', 'editor']
```

---

## Key Code: Page Model

```python
# web/models/page.py
class Page(Base):
    __tablename__ = 'pages'

    id = Column(Integer, primary_key=True)
    title = Column(String(255), nullable=False)
    slug = Column(String(255), unique=True, nullable=False)
    content = Column(Text, default='')
    extension = Column(String(10), default='html')
    is_secure = Column(Boolean, default=False)       # Requires login to view
    edit_restricted = Column(Boolean, default=False)  # Admin-only edit
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    ALLOWED_EXTENSIONS = ['php', 'html', 'js', 'css', 'txt']
```

---

## Key Code: Role Decorator

```python
# web/auth/decorators.py
from functools import wraps
from flask import abort
from flask_login import current_user

def require_role(roles):
    """Decorator to require specific roles."""
    if isinstance(roles, str):
        roles = [roles]

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                abort(401)
            if current_user.role not in roles:
                abort(403)
            return f(*args, **kwargs)
        return decorated_function
    return decorator
```

---

## Production Deployment

### Nginx Config (replace Apache)
```nginx
server {
    listen 443 ssl http2;
    server_name backend.extraspace.com.sg;

    ssl_certificate /etc/ssl/certs/your-cert.pem;
    ssl_certificate_key /etc/ssl/private/your-key.pem;

    # Static files - served directly by Nginx
    location /static/ {
        alias /var/www/backend/app/scheduler/python/web/static/;
        expires 30d;
        add_header Cache-Control "public, immutable";
    }

    # All other requests to Gunicorn
    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket support (if needed)
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

### Systemd Service
```ini
[Unit]
Description=ESA Backend Flask Application
After=network.target

[Service]
User=www-data
Group=www-data
WorkingDirectory=/var/www/backend/app/scheduler/python
Environment="PATH=/var/www/backend/app/scheduler/python/venv/bin"
ExecStart=/var/www/backend/app/scheduler/python/venv/bin/gunicorn \
    --workers 4 \
    --bind 127.0.0.1:5000 \
    --access-logfile /var/log/esa-backend/access.log \
    --error-logfile /var/log/esa-backend/error.log \
    wsgi:app
Restart=always

[Install]
WantedBy=multi-user.target
```

### WSGI Entry Point
```python
# wsgi.py
from web.app import create_app

app = create_app()

if __name__ == '__main__':
    app.run()
```

---

## Environment Variables

Existing variables still used:
- `MS_OAUTH_CLIENT_ID` - Microsoft OAuth client ID
- `MS_OAUTH_CLIENT_SECRET` - Microsoft OAuth client secret
- `MS_OAUTH_TENANT` - Azure AD tenant ID
- `MS_OAUTH_REDIRECT_URI` - OAuth callback URL (update to `/auth/microsoft/callback`)
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USERNAME`, `DB_PASSWORD` - Database
- `JWT_SECRET`, `JWT_EXPIRY` - API authentication

New variable:
- `FLASK_SECRET_KEY` - Session encryption key (generate with `python -c "import secrets; print(secrets.token_hex(32))"`)

---

## URL Changes

| Old (PHP)                      | New (Flask)                    |
|--------------------------------|--------------------------------|
| `/login.php`                   | `/auth/login`                  |
| `/login_microsoft.php`         | `/auth/microsoft`              |
| `/oauth_callback.php`          | `/auth/microsoft/callback`     |
| `/logout.php`                  | `/auth/logout`                 |
| `/dashboard.php`               | `/dashboard`                   |
| `/admin/list_users.php`        | `/admin/users`                 |
| `/admin/edit_user.php?id=1`    | `/admin/users/1/edit`          |
| `/admin/list_pages.php`        | `/admin/pages`                 |
| `/admin/edit_page.php?id=1`    | `/admin/pages/1/edit`          |
| `/app/scheduler/`              | `/scheduler/`                  |
| `/app/scheduler/api/proxy.php` | `/api/` (direct, no proxy)     |

---

## Verification Checklist

1. **Local login**: `POST /auth/login` with username/password → redirects to dashboard
2. **OAuth flow**: Click "Sign in with Microsoft" → Microsoft login → callback → dashboard
3. **User CRUD**: `/admin/users` - list, create, edit, delete (admin only)
4. **Page CRUD**: `/admin/pages` - list, create, edit, delete (admin + editor)
5. **Scheduler access**: `/scheduler/` works for admin + scheduler_admin roles
6. **API with JWT**: `curl -H "Authorization: Bearer $JWT" /api/jobs` returns job list
7. **Role restrictions**: viewer cannot access `/admin/*` or `/scheduler/`
8. **Secure pages**: pages with `is_secure=True` require login

---

## Rollback Plan

PHP files remain in place during migration. If issues arise:

1. **Quick rollback**: Update Nginx config to route traffic back to Apache/PHP
2. **Database safe**: No breaking schema changes - both PHP and Flask can read same tables
3. **Gradual migration**: Can run PHP and Flask side-by-side during testing

---

## Timeline Estimate

| Phase | Tasks | Duration |
|-------|-------|----------|
| 1 | Directory structure, dependencies | 1 day |
| 2 | Auth (local + OAuth) | 2 days |
| 3 | User management | 1 day |
| 4 | Page management | 1 day |
| 5 | Refactor scheduler routes | 1 day |
| 6 | Templates and testing | 2 days |
| 7 | Production deployment | 1 day |
| **Total** | | **~9 days** |
