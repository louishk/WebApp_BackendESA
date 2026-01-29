# Backend Migration Progress Tracker

## Project: WebApp_BackendESA
**Started:** 2026-01-28
**VM:** 57.158.27.35 (backend.extraspace.com.sg)

---

## Current Status: READY FOR DEPLOYMENT - All PHP Updates Complete

---

## Completed Tasks

### 1. Cleanup - Delete Unused Files [DONE]
- [x] Deleted `app/rps/` directory
- [x] Deleted `pages/chatwoot/`
- [x] Deleted `pages/datalayer/`
- [x] Deleted `pages/dbupload/`
- [x] Deleted `pages/fss/`
- [x] Deleted `pages/rapidstor/`
- [x] Deleted `pages/waba/`
- [x] Deleted `pages/info.php`
- [x] Deleted `admin/manage_python.php`
- [x] Deleted `admin/terminal.php`
- [x] Deleted `admin/oauth_callback.php`
- [x] Deleted `python/` directory
- [x] Deleted `test.php`

### 2. VM - PHP Installation [DONE]
- [x] Installed PHP 8.3-fpm
- [x] Installed php-pgsql (PostgreSQL)
- [x] Installed php-curl
- [x] Installed php-mbstring
- [x] Installed php-xml
- [x] Installed php-zip
- [x] Installed Composer globally

### 3. Create New Files (Local) [DONE]
- [x] Created `app/scheduler/` directory structure
- [x] Created `app/scheduler/index.php`
- [x] Created `app/scheduler/api/proxy.php`
- [x] Created `app/scheduler/api/auth.php`
- [x] Created `app/scheduler/.htaccess`
- [x] Python scheduler NOT COPIED - uses existing at /opt/pbi-scheduler on VM
- [x] Created `sql/setup_postgresql.sql`
- [x] Created `healthcheck.php`
- [x] Created `.gitignore`
- [ ] OPTIONAL: Create `sql/migrate_mysql_to_postgresql.php`
- [ ] OPTIONAL: Create `deploy.sh`

### 4. Update Existing Files (Local) [DONE]
- [x] Updated `.env` for PostgreSQL
- [x] Updated `config.php` for PostgreSQL + JWT
- [x] Updated `composer.json` (add firebase/php-jwt)
- [x] Updated `oauth_callback.php` (JWT generation for scheduler)
- [x] Updated `login.php` (JWT for local login, fetch email)
- [x] Updated `dashboard.php` (scheduler links, removed old links)
- [x] Updated `admin/admin.php` (scheduler dashboard link)
- [x] Updated `admin/register.php` (scheduler_admin role + PostgreSQL)
- [x] Updated `admin/edit_user.php` (scheduler_admin role)
- [x] Updated `pages/seo/richsnippet.php` (PostgreSQL syntax)

---

## Pending Tasks

### 5. VM Deployment
- [ ] Push to GitHub
- [ ] Pull on VM `/var/www/backend`
- [ ] Run `composer update`
- [ ] Configure nginx for PHP + scheduler proxy
- [ ] Set file permissions
- [ ] Run PostgreSQL setup script
- [ ] Test deployment

### 6. Database Setup
- [ ] Create `backend` database tables
- [ ] Migrate data from MySQL (if needed)
- [ ] Create default admin user

---

## Current File Structure

```
WebApp_BackendESA/
├── .env                    # UPDATED - PostgreSQL + MS OAuth
├── .gitignore              # NEW
├── .htaccess
├── composer.json           # UPDATED - added JWT
├── config.php              # UPDATED - PostgreSQL + JWT
├── dashboard.php           # UPDATED - scheduler links
├── healthcheck.php         # NEW
├── index.php
├── login.php               # UPDATED - JWT generation
├── login_microsoft.php
├── oauth_callback.php      # UPDATED - JWT generation
├── logout.php
├── PROGRESS.md             # This file
├── REFERENCE_FILES.md
├── SETUP_STEPS.md
│
├── admin/
│   ├── admin.php           # UPDATED - scheduler link
│   ├── register.php        # UPDATED - scheduler_admin role
│   ├── edit_user.php       # UPDATED - scheduler_admin role
│   └── [other files]
│
├── app/
│   └── scheduler/          # NEW
│       ├── .htaccess
│       ├── index.php
│       └── api/
│           ├── auth.php
│           └── proxy.php
│
├── pages/
│   └── seo/
│       └── richsnippet.php # UPDATED - PostgreSQL
│
├── sql/                    # NEW
│   └── setup_postgresql.sql
│
└── vendor/
```

---

## Quick Reference

### SSH
```bash
sshpass -p 'K9wKmtRfj3zJqRU' ssh esa_pbi_admin@57.158.27.35
```

### PostgreSQL
```bash
PGPASSWORD='K9wKmtRfj3zJqRU' psql -h esapbi.postgres.database.azure.com -U esa_pbi_admin -d backend
```

---

## Notes
- Scheduler already running at `/opt/pbi-scheduler/Scripts/` on VM (port 5000)
- PHP backend will proxy API calls to scheduler via `app/scheduler/api/proxy.php`
- nginx needs update to serve PHP AND proxy scheduler
- Domain: backend.extraspace.com.sg (SSL via Let's Encrypt)

---

## Change Log

### 2026-01-28 - PHP Updates Complete
All 6 PHP files updated for PostgreSQL migration and scheduler integration:

1. **oauth_callback.php** - Added JWT token generation after Microsoft OAuth login
2. **login.php** - Added JWT token generation + email fetch for local login
3. **admin/register.php** - Added `scheduler_admin` role + PostgreSQL `CURRENT_TIMESTAMP`
4. **admin/edit_user.php** - Added `scheduler_admin` role option
5. **admin/admin.php** - Added Scheduler dashboard link card
6. **pages/seo/richsnippet.php** - Converted MySQL to PostgreSQL (uses global $pdo)

**Ready for:** Git commit and VM deployment
