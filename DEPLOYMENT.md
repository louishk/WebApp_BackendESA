# WebApp Backend - Deployment Guide

## Overview

This backend has been restructured to:
- Use PostgreSQL instead of MySQL
- Include the PBI Scheduler as the first app under `app/`
- Preserve the working O365 (Microsoft OAuth) authentication
- Add JWT-based authentication for the scheduler API

## Directory Structure

```
WebApp_Backend/
├── .env                          # Main environment config
├── .htaccess                     # Apache URL rewriting
├── config.php                    # PostgreSQL PDO + JWT helpers
├── index.php                     # Login page + page routing
├── login.php                     # Local authentication
├── login_microsoft.php           # OAuth initiation
├── oauth_callback.php            # OAuth callback + JWT generation
├── dashboard.php                 # User dashboard
├── logout.php
├── deploy.sh                     # Server deployment script
│
├── admin/
│   ├── admin.php                 # Admin dashboard
│   ├── list_users.php
│   ├── edit_user.php
│   ├── delete_user.php
│   ├── register.php
│   ├── reset_password.php
│   ├── list_pages.php
│   ├── edit_page.php
│   ├── delete_page.php
│   └── delete_folder.php
│
├── app/
│   └── scheduler/                # PBI Scheduler App
│       ├── .htaccess             # Security rules
│       ├── index.php             # Entry point (requires auth)
│       ├── api/
│       │   ├── auth.php          # JWT validation helpers
│       │   └── proxy.php         # PHP→Python API proxy
│       └── python/               # Python scheduler
│           ├── .env              # Scheduler config
│           ├── requirements.txt
│           ├── run_scheduler.py
│           ├── start_scheduler.sh
│           ├── config/           # YAML configs
│           ├── web/              # Flask web UI
│           └── systemd/          # Service files
│
├── pages/
│   └── seo/
│       └── richsnippet.php       # SEO tool
│
├── sql/
│   ├── setup_postgresql.sql      # Database schema
│   └── migrate_mysql_to_postgresql.php
│
└── vendor/                       # Composer dependencies
```

## Prerequisites

### Server Requirements
- Ubuntu 22.04+ or similar Linux
- PHP 8.0+ with extensions: pgsql, curl, json, mbstring
- Python 3.10+
- Apache 2.4+ with mod_rewrite
- PostgreSQL client tools

### Azure Resources
- PostgreSQL: `esapbi.postgres.database.azure.com`
- Database: `backend` (for app data) + `esa_pbi` (for data layer)
- Microsoft Entra ID app registration: `412b9f8e-57c8-463b-9ad4-e01b90a9b20e`

## Deployment Steps

### 1. Database Setup

Connect to PostgreSQL and create the database:

```bash
psql "host=esapbi.postgres.database.azure.com port=5432 dbname=postgres user=esa_pbi_admin sslmode=require"
```

```sql
CREATE DATABASE backend;
\c backend
```

Then run the schema:

```bash
psql "host=esapbi.postgres.database.azure.com port=5432 dbname=backend user=esa_pbi_admin sslmode=require" -f sql/setup_postgresql.sql
```

### 2. Data Migration (Optional)

If migrating from the old MySQL database:

```bash
php sql/migrate_mysql_to_postgresql.php
```

### 3. Server Deployment

```bash
# Copy files to server
scp -r . user@57.158.27.35:/var/www/html/

# SSH to server and run deployment
ssh user@57.158.27.35
cd /var/www/html
sudo ./deploy.sh
```

Or manually:

```bash
# Install PHP PostgreSQL extension
sudo apt-get install php-pgsql
sudo systemctl restart apache2

# Set up Python environment
cd /var/www/html/app/scheduler/python
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate

# Install systemd service
sudo cp systemd/pbi-scheduler-web.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable pbi-scheduler-web
sudo systemctl start pbi-scheduler-web
```

### 4. Configuration

Update `/var/www/html/.env` with your credentials:

```env
# PostgreSQL
DB_HOST=esapbi.postgres.database.azure.com
DB_PORT=5432
DB_NAME=backend
DB_USERNAME=esa_pbi_admin
DB_PASSWORD=your_password
DB_SSLMODE=require

# Microsoft OAuth
MS_OAUTH_CLIENT_ID=412b9f8e-57c8-463b-9ad4-e01b90a9b20e
MS_OAUTH_CLIENT_SECRET=your_secret
MS_OAUTH_REDIRECT_URI=https://backend.redboxstorage.hk/oauth_callback.php
MS_OAUTH_TENANT=9798bd2a-c0ae-49f1-b879-5344bf51f42a

# JWT
JWT_SECRET=generate_random_32_char_string
JWT_EXPIRY=3600

# Scheduler
SCHEDULER_API_URL=http://localhost:5000
```

## User Roles

| Role | Access |
|------|--------|
| `admin` | Full access to everything |
| `scheduler_admin` | Scheduler + admin dashboard |
| `editor` | Page management |
| `viewer` | Read-only access |

## Authentication Flow

### Local Login
1. User submits username/password to `login.php`
2. Password verified against PostgreSQL `users` table
3. Session created with user data
4. JWT token generated for scheduler access (admin/scheduler_admin)

### Microsoft OAuth
1. User clicks "Sign in with Microsoft" → `login_microsoft.php`
2. Redirected to Microsoft login
3. Returns to `oauth_callback.php` with auth code
4. User created/updated in PostgreSQL
5. Session created with JWT token

## Scheduler Integration

### PHP → Python Communication
- PHP proxy at `/app/scheduler/api/proxy.php`
- Forwards authenticated requests to Python Flask API
- JWT token passed in Authorization header

### Running the Scheduler

```bash
# Start web UI only
cd /var/www/html/app/scheduler/python
./start_scheduler.sh web

# Start daemon
./start_scheduler.sh daemon

# Use CLI
./start_scheduler.sh cli jobs list
```

### Service Management

```bash
# Check status
sudo systemctl status pbi-scheduler-web

# View logs
sudo journalctl -u pbi-scheduler-web -f

# Restart
sudo systemctl restart pbi-scheduler-web
```

## Troubleshooting

### Database Connection Failed
- Check PostgreSQL firewall allows your server IP
- Verify SSL mode is `require` for Azure PostgreSQL
- Test connection: `psql "host=... dbname=backend user=... sslmode=require"`

### OAuth Error
- Verify redirect URI matches Azure AD app registration
- Check client secret hasn't expired
- Ensure user consent granted for app permissions

### Scheduler Not Responding
- Check service: `sudo systemctl status pbi-scheduler-web`
- Check logs: `sudo journalctl -u pbi-scheduler-web -f`
- Verify Python venv is correctly set up

### 403 Forbidden
- Check user role in database
- Verify session is active
- Check JWT token hasn't expired

## Security Notes

- `.env` files are blocked by Apache
- SQL files are not accessible via web
- Python files in scheduler are protected
- JWT tokens expire after 1 hour (configurable)
- Passwords are bcrypt hashed
