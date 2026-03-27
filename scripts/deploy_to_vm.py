#!/usr/bin/env python3
"""
Deploy to VM Script

Deploys ESA Backend to Azure VM with full environment setup via rsync.
Supports selective service restart — only restarts services affected by changes.

Usage:
    python scripts/deploy_to_vm.py                  # Selective deploy (rsync)
    python scripts/deploy_to_vm.py --force           # Full deploy (restart all)
    python scripts/deploy_to_vm.py --status         # Check service status
    python scripts/deploy_to_vm.py --cmd "ls"       # Run custom command
    python scripts/deploy_to_vm.py --dry-run        # Show what would happen

Deployment Steps:
    1. Update code via rsync
    2. Check/create Python venv
    3. Check/install requirements (if requirements.txt changed)
    4. Check .env (DB_PASSWORD, VAULT_MASTER_KEY)
    5. Determine restart scope (which services need restarting)
    6. Selectively restart affected services

Configuration (from .env):
    - VM_SSH_HOST: VM IP address
    - VM_SSH_PORT: SSH port (default: 22)
    - VM_SSH_ROOT_USERNAME: SSH username
    - VM_SSH_PASSWORD: SSH password (from DB vault or env)
"""

import argparse
import json
import sys
import subprocess
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'backend' / 'python'))

try:
    import paramiko
except ImportError:
    print("ERROR: paramiko is required. Install with: pip install paramiko")
    sys.exit(1)

from decouple import config as env_config
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(project_root / '.env')

# VM paths
# Note: Repo cloned to /var/www/backend, Python code is in backend/python subdirectory
VM_BACKEND_PATH = "/var/www/backend"
VM_PYTHON_PATH = f"{VM_BACKEND_PATH}/backend/python"
VM_VENV_PATH = f"{VM_PYTHON_PATH}/venv"
VM_ENV_PATH = f"{VM_PYTHON_PATH}/.env"
VM_DEPLOY_MANIFEST = f"{VM_BACKEND_PATH}/.deploy-manifest"

# Rsync exclude patterns (files that should NOT be synced)
RSYNC_EXCLUDES = [
    '.git',
    '.idea',
    '__pycache__',
    '*.pyc',
    '.env',           # VM has its own .env
    'venv',           # VM has its own venv
    'node_modules',
    '.DS_Store',
    '*.log',
    'logs/',          # Preserve VM log dir ownership (www-data)
    '.vault/',        # Preserve VM vault secrets
]

# Service restart rules: path prefixes → set of services that need restart
# Order matters — first match wins for each changed file
SERVICE_RULES = [
    # Template/static-only changes need no restart (Jinja2 auto-reloads)
    {
        'prefixes': [
            'backend/python/web/templates/',
            'backend/python/web/static/',
            'pages/',
        ],
        'services': set(),  # No restart needed
        'label': 'templates/static',
    },
    # Systemd unit files → full stop/start of the specific service
    {
        'prefixes': ['backend/python/systemd/esa-backend.service'],
        'services': {'esa-backend'},
        'label': 'systemd:esa-backend',
        'full_restart': True,
    },
    {
        'prefixes': ['backend/python/systemd/backend-scheduler.service'],
        'services': {'backend-scheduler'},
        'label': 'systemd:backend-scheduler',
        'full_restart': True,
    },
    {
        'prefixes': ['backend/python/systemd/backend-mcp.service'],
        'services': {'backend-mcp'},
        'label': 'systemd:backend-mcp',
        'full_restart': True,
    },
    # Requirements → all services (pip install needed first)
    {
        'prefixes': [
            'backend/python/requirements.txt',
            'mcp_esa/requirements.txt',
        ],
        'services': {'esa-backend', 'backend-scheduler', 'backend-mcp'},
        'label': 'requirements',
    },
    # MCP server
    {
        'prefixes': [
            'mcp_esa/',
            'backend/python/config/mcp.yaml',
        ],
        'services': {'backend-mcp'},
        'label': 'mcp',
    },
    # Scheduler / datalayer
    {
        'prefixes': [
            'backend/python/scheduler/',
            'backend/python/datalayer/',
            'backend/python/config/pipelines.yaml',
            'backend/python/config/scheduler.yaml',
        ],
        'services': {'backend-scheduler'},
        'label': 'scheduler/datalayer',
    },
    # Web app routes, auth, utils, models
    {
        'prefixes': [
            'backend/python/web/',
            'backend/python/config/app.yaml',
            'backend/python/config/database.yaml',
            'backend/python/wsgi.py',
        ],
        'services': {'esa-backend'},
        'label': 'web',
    },
    # Common modules affect both web and scheduler
    {
        'prefixes': [
            'backend/python/common/',
        ],
        'services': {'esa-backend', 'backend-scheduler'},
        'label': 'common',
    },
]


def get_ssh_credentials():
    """Get SSH credentials — uses SSH key auth with fallback to password."""
    host = env_config('VM_SSH_HOST', default='20.6.132.108')
    port = env_config('VM_SSH_PORT', default=22, cast=int)
    username = env_config('VM_SSH_ROOT_USERNAME', default='esa_bk_admin')

    # Try SSH key first (preferred)
    ssh_key_path = Path.home() / '.ssh' / 'id_ed25519_vm'
    if ssh_key_path.exists():
        return {
            'host': host,
            'port': port,
            'username': username,
            'password': None,
            'key_filename': str(ssh_key_path),
        }

    # Fallback to password from vault
    password = None
    try:
        from common.secrets_vault import vault_config
        password = vault_config('VM_SSH_PASSWORD', default=None)
    except Exception:
        pass

    if not password:
        raise ValueError("No SSH key (~/.ssh/id_ed25519_vm) and no VM_SSH_PASSWORD in vault")

    return {
        'host': host,
        'port': port,
        'username': username,
        'password': password,
        'key_filename': None,
    }


def run_ssh_command(credentials: dict, command: str, verbose: bool = True, timeout: int = 300) -> tuple:
    """
    Run a command on the VM via SSH.

    Returns:
        Tuple of (stdout, stderr, exit_code)
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        connect_kwargs = {
            'hostname': credentials['host'],
            'port': credentials['port'],
            'username': credentials['username'],
            'timeout': 30,
        }
        if credentials.get('key_filename'):
            connect_kwargs['key_filename'] = credentials['key_filename']
        if credentials.get('password'):
            connect_kwargs['password'] = credentials['password']
        client.connect(**connect_kwargs)

        if verbose:
            # Truncate long commands for display
            display_cmd = command if len(command) < 100 else command[:100] + "..."
            print(f"  $ {display_cmd}")

        stdin, stdout, stderr = client.exec_command(command, timeout=timeout)

        stdout_text = stdout.read().decode('utf-8')
        stderr_text = stderr.read().decode('utf-8')
        exit_code = stdout.channel.recv_exit_status()

        if verbose and stdout_text:
            # Indent output
            for line in stdout_text.strip().split('\n'):
                print(f"    {line}")

        if verbose and stderr_text and exit_code != 0:
            for line in stderr_text.strip().split('\n'):
                print(f"    [ERR] {line}")

        return stdout_text, stderr_text, exit_code

    finally:
        client.close()


def get_local_git_sha() -> str:
    """Get current git commit SHA from local repo."""
    try:
        result = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            capture_output=True, text=True,
            cwd=str(project_root),
        )
        return result.stdout.strip() if result.returncode == 0 else ''
    except Exception:
        return ''


def get_deployed_sha(credentials: dict) -> str:
    """Read the previously deployed git SHA from the VM manifest."""
    stdout, _, exit_code = run_ssh_command(
        credentials,
        f"cat {VM_DEPLOY_MANIFEST} 2>/dev/null || echo ''",
        verbose=False,
    )
    return stdout.strip()


def write_deploy_manifest(credentials: dict, sha: str) -> None:
    """Write the current git SHA to the VM deploy manifest."""
    run_ssh_command(
        credentials,
        f"echo '{sha}' | sudo tee {VM_DEPLOY_MANIFEST} > /dev/null",
        verbose=False,
    )


def get_changed_files(old_sha: str, new_sha: str) -> list:
    """Get list of files changed between two git SHAs."""
    if not old_sha or not new_sha:
        return []
    try:
        result = subprocess.run(
            ['git', 'diff', '--name-only', old_sha, new_sha],
            capture_output=True, text=True,
            cwd=str(project_root),
        )
        if result.returncode != 0:
            return []
        return [f for f in result.stdout.strip().split('\n') if f]
    except Exception:
        return []


def determine_restart_scope(changed_files: list) -> dict:
    """
    Determine which services need restarting based on changed files.

    Returns:
        dict with keys:
            'services': set of service names to restart
            'full_restart_services': set of services needing full stop/start
            'reasons': dict mapping service → list of reasons
            'skip_pip': bool — True if requirements.txt didn't change
    """
    services = set()
    full_restart_services = set()
    reasons = {}
    needs_pip = False
    unmatched_files = []

    for filepath in changed_files:
        matched = False
        for rule in SERVICE_RULES:
            for prefix in rule['prefixes']:
                if filepath.startswith(prefix) or filepath == prefix:
                    matched = True
                    for svc in rule['services']:
                        services.add(svc)
                        reasons.setdefault(svc, []).append(f"{filepath} ({rule['label']})")
                    if rule.get('full_restart'):
                        full_restart_services.update(rule['services'])
                    if rule['label'] == 'requirements':
                        needs_pip = True
                    break
            if matched:
                break
        if not matched:
            unmatched_files.append(filepath)

    # Unmatched files (e.g., scripts/, sql/, docs/) → no restart needed
    # But if we're unsure, we don't blindly restart everything

    return {
        'services': services,
        'full_restart_services': full_restart_services,
        'reasons': reasons,
        'skip_pip': not needs_pip,
        'unmatched_files': unmatched_files,
    }


def step_update_code_rsync(credentials: dict, dry_run: bool = False, verbose: bool = True) -> bool:
    """Step 1: Update code via rsync."""
    print("\n[1/6] Syncing code via rsync...")

    excludes = ' '.join(f"--exclude='{e}'" for e in RSYNC_EXCLUDES)
    dry_run_flag = '-n' if dry_run else ''
    temp_dir = "/tmp/webapp-deploy"

    # Build SSH options for rsync
    ssh_opts = f"-o StrictHostKeyChecking=no -p {credentials['port']}"
    if credentials.get('key_filename'):
        ssh_opts += f" -i {credentials['key_filename']}"

    rsync_cmd = (
        f"rsync -avz --delete {dry_run_flag} {excludes} "
        f"-e 'ssh {ssh_opts}' "
        f"{project_root}/ "
        f"{credentials['username']}@{credentials['host']}:{temp_dir}/"
    )

    # If using password auth (no SSH key), prepend sshpass
    if not credentials.get('key_filename') and credentials.get('password'):
        rsync_cmd = f"sshpass -p '{credentials['password']}' " + rsync_cmd

    try:
        result = subprocess.run(rsync_cmd, shell=True, capture_output=True, text=True)

        if verbose:
            lines = result.stdout.strip().split('\n') if result.stdout else []
            if len(lines) > 20:
                print(f"    Synced {len(lines)} files")
            elif lines:
                for line in lines[:10]:
                    print(f"    {line}")

        if result.returncode != 0:
            print(f"    ERROR: rsync failed: {result.stderr}")
            return False

        if dry_run:
            return True

        # Move from temp to /var/www/backend, preserving VM-specific files
        move_cmd = f"""
            sudo rsync -a --delete \
                --exclude='.env' \
                --exclude='backend/python/.env' \
                --exclude='backend/python/venv' \
                {temp_dir}/ {VM_BACKEND_PATH}/

            sudo chown -R www-data:www-data {VM_BACKEND_PATH}

            # Ensure logs directory exists with correct ownership
            sudo mkdir -p {VM_PYTHON_PATH}/logs
            sudo chown -R www-data:www-data {VM_PYTHON_PATH}/logs
            sudo chmod 755 {VM_PYTHON_PATH}/logs

            rm -rf {temp_dir}
            echo "Code deployed to {VM_BACKEND_PATH}"
        """

        stdout, stderr, exit_code = run_ssh_command(credentials, move_cmd, verbose)
        return exit_code == 0

    except FileNotFoundError:
        print("    ERROR: rsync or sshpass not found")
        return False


def step_check_venv(credentials: dict, verbose: bool = True) -> bool:
    """Step 2: Check/create Python virtual environment."""
    print("\n[2/6] Checking Python virtual environment...")

    venv_cmd = f"""
        if [ -d "{VM_VENV_PATH}" ] && [ -f "{VM_VENV_PATH}/bin/python" ]; then
            echo "venv exists"
            {VM_VENV_PATH}/bin/python --version
        else
            echo "Creating venv..."
            sudo -u www-data python3.12 -m venv {VM_VENV_PATH} || \
            sudo -u www-data python3.11 -m venv {VM_VENV_PATH} || \
            sudo -u www-data python3 -m venv {VM_VENV_PATH}
            echo "venv created"
            {VM_VENV_PATH}/bin/python --version
        fi
    """

    stdout, stderr, exit_code = run_ssh_command(credentials, venv_cmd, verbose)
    return exit_code == 0


def step_check_requirements(credentials: dict, verbose: bool = True, skip: bool = False) -> bool:
    """Step 3: Check/install Python requirements."""
    if skip:
        print("\n[3/6] Skipping requirements (no changes detected)")
        return True

    print("\n[3/6] Installing Python requirements...")

    req_cmd = f"""
        cd {VM_PYTHON_PATH}

        # Upgrade pip first
        sudo -u www-data {VM_VENV_PATH}/bin/pip install --upgrade pip -q

        # Check if requirements need update by comparing installed vs required
        echo "Installing/updating requirements..."
        sudo -u www-data {VM_VENV_PATH}/bin/pip install -r requirements.txt -q

        # Install MCP server requirements
        if [ -f "{VM_BACKEND_PATH}/mcp_esa/requirements.txt" ]; then
            sudo -u www-data {VM_VENV_PATH}/bin/pip install -r {VM_BACKEND_PATH}/mcp_esa/requirements.txt -q
        fi

        # Verify key packages
        {VM_VENV_PATH}/bin/python -c "import flask; import gunicorn; import authlib; print('Key packages OK')"
    """

    stdout, stderr, exit_code = run_ssh_command(credentials, req_cmd, verbose, timeout=300)
    return exit_code == 0


def step_check_env(credentials: dict, verbose: bool = True) -> bool:
    """Step 4: Check .env file exists and has required bootstrap vars."""
    print("\n[4/6] Checking .env configuration...")

    env_cmd = f"""
        if [ -f "{VM_PYTHON_PATH}/.env" ]; then
            echo ".env exists"

            # Check for required bootstrap variables
            MISSING=""
            for VAR in DB_PASSWORD VAULT_MASTER_KEY; do
                if grep -q "^$VAR=" {VM_PYTHON_PATH}/.env; then
                    echo "$VAR: SET"
                else
                    echo "ERROR: $VAR not found in .env"
                    MISSING="$MISSING $VAR"
                fi
            done

            if [ -n "$MISSING" ]; then
                exit 1
            fi
        else
            echo "ERROR: .env file not found at {VM_PYTHON_PATH}/.env"
            exit 1
        fi
    """

    stdout, stderr, exit_code = run_ssh_command(credentials, env_cmd, verbose)
    return exit_code == 0


def step_determine_restart_scope(credentials: dict, force: bool = False, verbose: bool = True) -> dict:
    """Step 5: Determine which services need restarting based on changed files."""
    print("\n[5/6] Determining restart scope...")

    if force:
        print("    --force: restarting all services")
        return {
            'services': {'esa-backend', 'backend-scheduler', 'backend-mcp'},
            'full_restart_services': {'esa-backend', 'backend-scheduler', 'backend-mcp'},
            'reasons': {
                'esa-backend': ['--force flag'],
                'backend-scheduler': ['--force flag'],
                'backend-mcp': ['--force flag'],
            },
            'skip_pip': False,
            'unmatched_files': [],
        }

    current_sha = get_local_git_sha()
    deployed_sha = get_deployed_sha(credentials)

    if not current_sha:
        print("    WARNING: Cannot determine local git SHA — restarting all services")
        return {
            'services': {'esa-backend', 'backend-scheduler', 'backend-mcp'},
            'full_restart_services': set(),
            'reasons': {'esa-backend': ['no git SHA'], 'backend-scheduler': ['no git SHA'], 'backend-mcp': ['no git SHA']},
            'skip_pip': False,
            'unmatched_files': [],
        }

    if not deployed_sha:
        print("    No deploy manifest found (first deploy?) — restarting all services")
        return {
            'services': {'esa-backend', 'backend-scheduler', 'backend-mcp'},
            'full_restart_services': set(),
            'reasons': {'esa-backend': ['first deploy'], 'backend-scheduler': ['first deploy'], 'backend-mcp': ['first deploy']},
            'skip_pip': False,
            'unmatched_files': [],
        }

    if current_sha == deployed_sha:
        print("    No changes since last deploy (same SHA)")
        return {
            'services': set(),
            'full_restart_services': set(),
            'reasons': {},
            'skip_pip': True,
            'unmatched_files': [],
        }

    print(f"    Deployed: {deployed_sha[:10]}")
    print(f"    Current:  {current_sha[:10]}")

    changed_files = get_changed_files(deployed_sha, current_sha)
    if not changed_files:
        print("    WARNING: git diff returned no files — restarting all services")
        return {
            'services': {'esa-backend', 'backend-scheduler', 'backend-mcp'},
            'full_restart_services': set(),
            'reasons': {'esa-backend': ['git diff empty'], 'backend-scheduler': ['git diff empty'], 'backend-mcp': ['git diff empty']},
            'skip_pip': False,
            'unmatched_files': [],
        }

    print(f"    {len(changed_files)} file(s) changed")

    scope = determine_restart_scope(changed_files)

    if not scope['services']:
        print("    No service restart needed (template/static/docs changes only)")
    else:
        for svc in sorted(scope['services']):
            restart_type = "full restart" if svc in scope['full_restart_services'] else "graceful reload" if svc == 'esa-backend' else "restart"
            file_reasons = scope['reasons'].get(svc, [])
            # Show up to 3 reasons
            shown = file_reasons[:3]
            extra = len(file_reasons) - 3
            reason_str = ', '.join(shown)
            if extra > 0:
                reason_str += f" +{extra} more"
            print(f"    {svc}: {restart_type} — {reason_str}")

    if scope['unmatched_files']:
        if verbose:
            print(f"    ({len(scope['unmatched_files'])} file(s) don't affect any service: {', '.join(scope['unmatched_files'][:5])})")

    return scope


def step_restart_services(credentials: dict, scope: dict, verbose: bool = True) -> bool:
    """Step 6: Restart only the services that need it."""
    services_to_restart = scope['services']
    full_restart = scope['full_restart_services']

    if not services_to_restart:
        print("\n[6/6] No services to restart")
        return True

    print(f"\n[6/6] Restarting services: {', '.join(sorted(services_to_restart))}...")

    # Build the restart command
    parts = []

    # Always copy service files and daemon-reload if any service is restarting
    parts.append(f"""
        # Copy service files if they exist
        if [ -f "{VM_PYTHON_PATH}/systemd/esa-backend.service" ]; then
            sudo cp {VM_PYTHON_PATH}/systemd/esa-backend.service /etc/systemd/system/
        fi
        if [ -f "{VM_PYTHON_PATH}/systemd/backend-scheduler.service" ]; then
            sudo cp {VM_PYTHON_PATH}/systemd/backend-scheduler.service /etc/systemd/system/
        fi
        if [ -f "{VM_PYTHON_PATH}/systemd/backend-mcp.service" ]; then
            sudo cp {VM_PYTHON_PATH}/systemd/backend-mcp.service /etc/systemd/system/
        fi
        sudo systemctl daemon-reload
    """)

    # Sync nginx config if web app is restarting
    if 'esa-backend' in services_to_restart:
        parts.append(f"""
            # Sync nginx config
            NGINX_SRC="{VM_PYTHON_PATH}/config/nginx-esa-backend.conf"
            NGINX_DEST="/etc/nginx/sites-available/esa-backend"
            if [ -f "$NGINX_SRC" ]; then
                sudo cp "$NGINX_SRC" "$NGINX_DEST"
                if sudo nginx -t 2>&1; then
                    sudo systemctl reload nginx
                    echo "Nginx config updated and reloaded"
                else
                    echo "WARNING: Nginx config test failed — kept previous config"
                fi
            fi
        """)

    # Ensure log directories exist
    parts.append(f"""
        sudo mkdir -p /var/log/esa-backend
        sudo chown www-data:www-data /var/log/esa-backend
        sudo mkdir -p {VM_PYTHON_PATH}/logs
        sudo chown www-data:www-data {VM_PYTHON_PATH}/logs
    """)

    # Restart each service appropriately
    if 'esa-backend' in services_to_restart:
        if 'esa-backend' in full_restart:
            # Systemd unit changed — full stop/start
            parts.append("""
                echo "esa-backend: full restart (unit file changed)..."
                sudo systemctl stop esa-backend 2>/dev/null || true
                sleep 1
                sudo systemctl enable esa-backend
                sudo systemctl start esa-backend
            """)
        else:
            # Graceful reload via HUP — zero downtime
            parts.append("""
                echo "esa-backend: graceful reload (HUP)..."
                if sudo systemctl is-active --quiet esa-backend; then
                    sudo systemctl reload esa-backend
                    RELOAD_EXIT=$?
                    if [ $RELOAD_EXIT -ne 0 ]; then
                        echo "WARNING: reload failed (exit $RELOAD_EXIT), falling back to restart"
                        sudo systemctl restart esa-backend
                    fi
                else
                    echo "esa-backend was not running, starting..."
                    sudo systemctl enable esa-backend
                    sudo systemctl start esa-backend
                fi
            """)

    if 'backend-scheduler' in services_to_restart:
        if 'backend-scheduler' in full_restart:
            parts.append("""
                echo "backend-scheduler: full restart (unit file changed)..."
                sudo systemctl stop backend-scheduler 2>/dev/null || true
                sleep 1
                sudo systemctl enable backend-scheduler
                sudo systemctl start backend-scheduler
            """)
        else:
            parts.append("""
                echo "backend-scheduler: restarting..."
                sudo systemctl restart backend-scheduler
            """)

    if 'backend-mcp' in services_to_restart:
        if 'backend-mcp' in full_restart:
            parts.append("""
                echo "backend-mcp: full restart (unit file changed)..."
                sudo systemctl stop backend-mcp 2>/dev/null || true
                sleep 1
                sudo systemctl enable backend-mcp
                sudo systemctl start backend-mcp
            """)
        else:
            parts.append("""
                echo "backend-mcp: restarting..."
                sudo systemctl restart backend-mcp
            """)

    # Wait and verify
    parts.append("sleep 3")

    for svc in sorted(services_to_restart):
        parts.append(f"""
            if sudo systemctl is-active --quiet {svc}; then
                echo "{svc}: RUNNING"
                sudo systemctl status {svc} --no-pager | head -5
            else
                echo "{svc}: FAILED"
                sudo journalctl -u {svc} --no-pager -n 20
                exit 1
            fi
        """)

    # Also verify services we didn't restart are still running
    all_services = {'esa-backend', 'backend-scheduler', 'backend-mcp'}
    untouched = all_services - services_to_restart
    for svc in sorted(untouched):
        parts.append(f"""
            if sudo systemctl is-active --quiet {svc}; then
                echo "{svc}: RUNNING (untouched)"
            else
                echo "{svc}: NOT RUNNING (was not restarted)"
            fi
        """)

    restart_cmd = '\n'.join(parts)
    stdout, stderr, exit_code = run_ssh_command(credentials, restart_cmd, verbose)
    return exit_code == 0


def deploy(dry_run: bool = False, force: bool = False, verbose: bool = True) -> bool:
    """
    Full deployment to VM via rsync with selective restart.

    Args:
        dry_run: Show what would happen without doing it
        force: Restart all services regardless of changes
        verbose: Print output
    """
    credentials = get_ssh_credentials()

    mode_label = "force" if force else "selective"
    print(f"\nDeploying to {credentials['host']} (mode: {mode_label})")
    print("=" * 50)

    if dry_run:
        print("DRY RUN - no changes will be made\n")

    current_sha = get_local_git_sha()

    # Step 1: Update code (no pre-stop — rsync while services run)
    success = step_update_code_rsync(credentials, dry_run, verbose)

    if not success:
        print("ERROR: Code update failed!")
        return False

    if dry_run:
        print("\nDry run complete")
        return True

    # Step 2: Check venv
    if not step_check_venv(credentials, verbose):
        print("ERROR: venv setup failed!")
        return False

    # Step 5 (early): Determine restart scope to decide if pip install is needed
    scope = step_determine_restart_scope(credentials, force, verbose)

    # Step 3: Check requirements (skip if no requirements changes and not force)
    if not step_check_requirements(credentials, verbose, skip=scope['skip_pip']):
        print("ERROR: Requirements installation failed!")
        return False

    # Step 4: Check .env
    if not step_check_env(credentials, verbose):
        print("WARNING: .env check failed - service may not start correctly")

    # Step 6: Selectively restart affected services
    if not step_restart_services(credentials, scope, verbose):
        print("ERROR: Failed to restart services!")
        return False

    # Write deploy manifest on success
    if current_sha:
        write_deploy_manifest(credentials, current_sha)
        print(f"\n    Deploy manifest updated: {current_sha[:10]}")

    print("\n" + "=" * 50)
    print("Deployment complete!")
    return True


def check_status(verbose: bool = True) -> bool:
    """Check service status on VM."""
    credentials = get_ssh_credentials()

    print(f"\nChecking status on {credentials['host']}...")
    print("=" * 50)

    status_cmd = f"""
        echo "=== Services ==="
        sudo systemctl status esa-backend --no-pager 2>/dev/null || echo "esa-backend: not running"
        echo ""
        sudo systemctl status backend-scheduler --no-pager 2>/dev/null || echo "backend-scheduler: not running"
        echo ""
        sudo systemctl status backend-mcp --no-pager 2>/dev/null || echo "backend-mcp: not running"
        echo ""

        echo "=== Deploy Manifest ==="
        if [ -f "{VM_DEPLOY_MANIFEST}" ]; then
            echo "Last deployed SHA: $(cat {VM_DEPLOY_MANIFEST})"
        else
            echo "No deploy manifest found"
        fi

        echo ""
        echo "=== Python Environment ==="
        if [ -f "{VM_VENV_PATH}/bin/python" ]; then
            {VM_VENV_PATH}/bin/python --version
        else
            echo "venv not found"
        fi

        echo ""
        echo "=== Disk Usage ==="
        df -h {VM_BACKEND_PATH} 2>/dev/null || df -h /var/www

        echo ""
        echo "=== Recent Logs ==="
        sudo journalctl -u esa-backend --no-pager -n 10 2>/dev/null || echo "No logs available"
    """

    stdout, stderr, exit_code = run_ssh_command(credentials, status_cmd, verbose)
    return exit_code == 0


def run_custom_command(command: str, verbose: bool = True) -> bool:
    """Run a custom command on the VM."""
    credentials = get_ssh_credentials()

    print(f"\nRunning command on {credentials['host']}...")
    print("=" * 50)

    stdout, stderr, exit_code = run_ssh_command(credentials, command, verbose)
    return exit_code == 0


def main():
    parser = argparse.ArgumentParser(
        description='Deploy ESA Backend to Azure VM',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/deploy_to_vm.py                  # Selective deploy via rsync
  python scripts/deploy_to_vm.py --force           # Full deploy (restart all)
  python scripts/deploy_to_vm.py --status         # Check service status
  python scripts/deploy_to_vm.py --cmd "ls -la"   # Run custom command
  python scripts/deploy_to_vm.py --dry-run        # Preview deployment
        """
    )

    parser.add_argument(
        '--status',
        action='store_true',
        help='Check service status only (no deploy)'
    )

    parser.add_argument(
        '--cmd',
        type=str,
        help='Run a custom command on the VM'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would happen without making changes'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force restart all services (ignore change detection)'
    )

    parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='Quiet mode (less output)'
    )

    args = parser.parse_args()
    verbose = not args.quiet

    print("=" * 50)
    print("ESA Backend - VM Deployment")
    print("=" * 50)

    try:
        if args.status:
            success = check_status(verbose)
        elif args.cmd:
            success = run_custom_command(args.cmd, verbose)
        else:
            success = deploy(dry_run=args.dry_run, force=args.force, verbose=verbose)

        if not success:
            sys.exit(1)

    except ValueError as e:
        print(f"\nERROR: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nAborted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
