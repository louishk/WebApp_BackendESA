#!/usr/bin/env python3
"""
Migration script for Role-Based Access Control System.

Run this script to apply the RBAC migration to the database.
Usage: python run_migration.py

This script:
1. Creates the roles table
2. Seeds default roles (admin, scheduler_admin, editor, viewer)
3. Adds role_id to users table and migrates existing role data
4. Adds new access control columns to pages table
5. Migrates existing page security settings
"""

import os
import sys
from pathlib import Path

# Add parent directory to path for imports
backend_path = Path(__file__).parent.parent
sys.path.insert(0, str(backend_path))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def get_database_url():
    """Get database URL from the application's config loader."""
    try:
        from common.config_loader import get_database_url as get_app_db_url
        return get_app_db_url('backend')
    except Exception as e:
        print(f"Warning: Could not load config, using environment variable: {e}")
        return os.environ.get('DATABASE_URL', 'sqlite:///app.db')


def run_migration():
    """Execute the RBAC migration."""
    database_url = get_database_url()

    # Mask password in output
    display_url = database_url
    if '@' in display_url:
        parts = display_url.split('@')
        before_at = parts[0]
        if ':' in before_at:
            # Mask password
            proto_user = before_at.rsplit(':', 1)[0]
            display_url = f"{proto_user}:****@{parts[1]}"

    print(f"Connecting to database: {display_url}")

    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()

    # Detect database type
    is_postgresql = 'postgresql' in database_url.lower()
    is_sqlite = 'sqlite' in database_url.lower()

    try:
        # Step 1: Create roles table
        print("Step 1: Creating roles table...")
        if is_postgresql:
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS roles (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(50) UNIQUE NOT NULL,
                    description VARCHAR(255) DEFAULT '',
                    can_access_scheduler BOOLEAN DEFAULT FALSE,
                    can_manage_users BOOLEAN DEFAULT FALSE,
                    can_manage_pages BOOLEAN DEFAULT FALSE,
                    can_manage_roles BOOLEAN DEFAULT FALSE,
                    can_manage_configs BOOLEAN DEFAULT FALSE,
                    is_system BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
        else:
            session.execute(text("""
                CREATE TABLE IF NOT EXISTS roles (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name VARCHAR(50) UNIQUE NOT NULL,
                    description VARCHAR(255) DEFAULT '',
                    can_access_scheduler BOOLEAN DEFAULT 0,
                    can_manage_users BOOLEAN DEFAULT 0,
                    can_manage_pages BOOLEAN DEFAULT 0,
                    can_manage_roles BOOLEAN DEFAULT 0,
                    can_manage_configs BOOLEAN DEFAULT 0,
                    is_system BOOLEAN DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """))
        session.commit()
        print("  Roles table created.")

        # Step 2: Seed default roles
        print("Step 2: Seeding default roles...")
        default_roles = [
            ('admin', 'Full system access', True, True, True, True, True, True),
            ('scheduler_admin', 'Scheduler management', True, False, False, False, False, True),
            ('editor', 'Page management', False, False, True, False, False, True),
            ('viewer', 'Read-only access', False, False, False, False, False, True),
        ]

        for role in default_roles:
            # Check if role exists
            exists = session.execute(
                text("SELECT id FROM roles WHERE name = :name"),
                {'name': role[0]}
            ).fetchone()

            if not exists:
                session.execute(text("""
                    INSERT INTO roles
                    (name, description, can_access_scheduler, can_manage_users, can_manage_pages, can_manage_roles, can_manage_configs, is_system)
                    VALUES (:name, :desc, :sched, :users, :pages, :roles, :configs, :system)
                """), {
                    'name': role[0], 'desc': role[1], 'sched': role[2], 'users': role[3],
                    'pages': role[4], 'roles': role[5], 'configs': role[6], 'system': role[7]
                })
                print(f"    Added role: {role[0]}")
            else:
                print(f"    Role exists: {role[0]}")

        session.commit()
        print("  Default roles seeded.")

        # Step 3: Check and add role_id column to users
        print("Step 3: Adding role_id to users table...")
        try:
            if is_postgresql:
                session.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS role_id INTEGER REFERENCES roles(id)"))
            else:
                session.execute(text("ALTER TABLE users ADD COLUMN role_id INTEGER REFERENCES roles(id)"))
            session.commit()
            print("  role_id column added.")
        except Exception as e:
            if 'duplicate column' in str(e).lower() or 'already exists' in str(e).lower():
                print("  role_id column already exists.")
            else:
                print(f"  Note: {e}")
            session.rollback()

        # Step 4: Migrate existing user roles
        print("Step 4: Migrating user roles...")
        role_mappings = ['admin', 'scheduler_admin', 'editor', 'viewer']

        # Check if 'role' column exists in users table
        try:
            session.execute(text("SELECT role FROM users LIMIT 1"))
            has_role_column = True
        except Exception:
            has_role_column = False
            session.rollback()

        if has_role_column:
            for role_name in role_mappings:
                result = session.execute(text("""
                    UPDATE users
                    SET role_id = (SELECT id FROM roles WHERE name = :role_name)
                    WHERE role = :role_name AND role_id IS NULL
                """), {'role_name': role_name})
                if result.rowcount > 0:
                    print(f"    Migrated {result.rowcount} users with role '{role_name}'")

            # Set default for any remaining users
            result = session.execute(text("""
                UPDATE users
                SET role_id = (SELECT id FROM roles WHERE name = 'viewer')
                WHERE role_id IS NULL
            """))
            if result.rowcount > 0:
                print(f"    Set default role for {result.rowcount} users")

            session.commit()
            print("  User roles migrated.")
        else:
            print("  No 'role' column found in users table, skipping migration.")

        # Step 5: Add page access control columns
        print("Step 5: Adding page access control columns...")
        page_columns = [
            ('is_public', 'BOOLEAN DEFAULT FALSE' if is_postgresql else 'BOOLEAN DEFAULT 0'),
            ('view_roles', "VARCHAR(255) DEFAULT ''"),
            ('view_users', "TEXT DEFAULT ''"),
            ('edit_roles', "VARCHAR(255) DEFAULT ''"),
            ('edit_users', "TEXT DEFAULT ''"),
        ]
        for col_name, col_def in page_columns:
            try:
                if is_postgresql:
                    session.execute(text(f"ALTER TABLE pages ADD COLUMN IF NOT EXISTS {col_name} {col_def}"))
                else:
                    session.execute(text(f"ALTER TABLE pages ADD COLUMN {col_name} {col_def}"))
                session.commit()
                print(f"    {col_name} column added.")
            except Exception as e:
                if 'duplicate column' in str(e).lower() or 'already exists' in str(e).lower():
                    print(f"    {col_name} column already exists.")
                else:
                    print(f"    Note for {col_name}: {e}")
                session.rollback()

        # Step 6: Migrate existing page data
        print("Step 6: Migrating page data...")

        # Check if is_secure column exists before migrating
        try:
            session.execute(text("SELECT is_secure FROM pages LIMIT 1"))
            # If we get here, is_secure exists, so migrate the data
            result = session.execute(text("UPDATE pages SET is_public = TRUE WHERE is_secure = FALSE OR is_secure IS NULL"))
            print(f"    Set {result.rowcount} pages as public (was not secure)")
            result = session.execute(text("UPDATE pages SET is_public = FALSE WHERE is_secure = TRUE"))
            print(f"    Set {result.rowcount} pages as not public (was secure)")
            session.commit()
        except Exception as e:
            print(f"    is_secure column not found, skipping is_public migration: {e}")
            session.rollback()

        # Check if edit_restricted column exists
        try:
            session.execute(text("SELECT edit_restricted FROM pages LIMIT 1"))
            result = session.execute(text("""
                UPDATE pages
                SET edit_roles = (SELECT CAST(id AS VARCHAR) FROM roles WHERE name = 'admin')
                WHERE edit_restricted = TRUE AND (edit_roles IS NULL OR edit_roles = '')
            """))
            print(f"    Migrated {result.rowcount} pages with edit_restricted to use admin role")
            session.commit()
        except Exception as e:
            print(f"    edit_restricted column not found, skipping edit_roles migration: {e}")
            session.rollback()

        # Verification
        print("\nVerification:")
        roles = session.execute(text("SELECT id, name, is_system FROM roles ORDER BY id")).fetchall()
        print(f"  Roles: {len(roles)} found")
        for r in roles:
            print(f"    - {r[0]}: {r[1]} (system: {r[2]})")

        users_with_role = session.execute(text("SELECT COUNT(*) FROM users WHERE role_id IS NOT NULL")).fetchone()
        total_users = session.execute(text("SELECT COUNT(*) FROM users")).fetchone()
        print(f"  Users with role_id: {users_with_role[0]}/{total_users[0]}")

        pages_count = session.execute(text("SELECT COUNT(*) FROM pages")).fetchone()
        print(f"  Pages: {pages_count[0]} total")

        print("\nMigration completed successfully!")

    except Exception as e:
        session.rollback()
        print(f"Migration failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        session.close()


if __name__ == '__main__':
    run_migration()
