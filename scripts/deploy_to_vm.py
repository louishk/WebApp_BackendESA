#!/usr/bin/env python3
"""
Deploy to VM Script

Deploys ESA Backend to Azure VM with full environment setup via rsync.

Usage:
    python scripts/deploy_to_vm.py                  # Full deploy (rsync)
    python scripts/deploy_to_vm.py --status         # Check service status
    python scripts/deploy_to_vm.py --cmd "ls"       # Run custom command
    python scripts/deploy_to_vm.py --dry-run        # Show what would happen

Deployment Steps:
    1. Stop all services (esa-backend, scheduler)
    2. Update code via rsync
    3. Check/create Python venv
    4. Check/install requirements
    5. Check .env (DB_PASSWORD, VAULT_MASTER_KEY)
    6. Start services

Configuration (from .env):
    - VM_SSH_HOST: VM IP address
    - VM_SSH_PORT: SSH port (default: 22)
    - VM_SSH_ROOT_USERNAME: SSH username
    - VM_SSH_PASSWORD: SSH password (from DB vault or env)
"""

import argparse
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


def step_stop_services(credentials: dict, verbose: bool = True) -> bool:
    """Step 1: Stop all running services and kill stray processes."""
    print("\n[1/6] Stopping services...")

    stop_cmd = """
        sudo systemctl stop esa-backend 2>/dev/null || true
        sudo systemctl stop backend-scheduler 2>/dev/null || true
        sudo systemctl stop backend-mcp 2>/dev/null || true

        # Kill any stray Python/gunicorn processes from backend
        sudo pkill -9 -f '/var/www/backend.*python' 2>/dev/null || true
        sudo pkill -9 -f 'gunicorn.*wsgi' 2>/dev/null || true
        sudo pkill -9 -f 'flask.*run' 2>/dev/null || true
        sudo pkill -9 -f 'mcp_esa.main' 2>/dev/null || true

        sleep 2

        # Verify nothing is running on port 5000
        if sudo lsof -i :5000 2>/dev/null; then
            echo "WARNING: Port 5000 still in use"
            sudo fuser -k 5000/tcp 2>/dev/null || true
        fi

        # Verify nothing is running on port 8002
        if sudo lsof -i :8002 2>/dev/null; then
            echo "WARNING: Port 8002 still in use"
            sudo fuser -k 8002/tcp 2>/dev/null || true
        fi

        echo "Services stopped"
    """

    stdout, stderr, exit_code = run_ssh_command(credentials, stop_cmd, verbose)
    return True  # Always continue even if some services weren't running


def step_update_code_rsync(credentials: dict, dry_run: bool = False, verbose: bool = True) -> bool:
    """Step 2: Update code via rsync."""
    print("\n[2/6] Syncing code via rsync...")

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
            rm -rf {temp_dir}
            echo "Code deployed to {VM_BACKEND_PATH}"
        """

        stdout, stderr, exit_code = run_ssh_command(credentials, move_cmd, verbose)
        return exit_code == 0

    except FileNotFoundError:
        print("    ERROR: rsync or sshpass not found")
        return False



def step_check_venv(credentials: dict, verbose: bool = True) -> bool:
    """Step 3: Check/create Python virtual environment."""
    print("\n[3/6] Checking Python virtual environment...")

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


def step_check_requirements(credentials: dict, verbose: bool = True) -> bool:
    """Step 4: Check/install Python requirements."""
    print("\n[4/6] Checking Python requirements...")

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
    """Step 5: Check .env file exists and has required bootstrap vars."""
    print("\n[5/6] Checking .env configuration...")

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


def step_start_services(credentials: dict, verbose: bool = True) -> bool:
    """Step 6: Start services."""
    print("\n[6/6] Starting services...")

    start_cmd = f"""
        # Reload systemd in case service files changed
        sudo systemctl daemon-reload

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

        # Sync nginx config if it exists
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

        # Create log directories if needed
        sudo mkdir -p /var/log/esa-backend
        sudo chown www-data:www-data /var/log/esa-backend
        sudo mkdir -p /var/www/backend/backend/python/logs
        sudo chown www-data:www-data /var/www/backend/backend/python/logs

        # Enable and start esa-backend (web UI)
        sudo systemctl enable esa-backend
        sudo systemctl start esa-backend

        # Enable and start backend-scheduler (scheduler daemon)
        sudo systemctl enable backend-scheduler
        sudo systemctl start backend-scheduler

        # Enable and start backend-mcp (MCP server)
        sudo systemctl enable backend-mcp
        sudo systemctl start backend-mcp

        sleep 3

        # Check esa-backend status
        if sudo systemctl is-active --quiet esa-backend; then
            echo "esa-backend: RUNNING"
            sudo systemctl status esa-backend --no-pager | head -5
        else
            echo "esa-backend: FAILED"
            sudo journalctl -u esa-backend --no-pager -n 20
            exit 1
        fi

        # Check backend-scheduler status
        if sudo systemctl is-active --quiet backend-scheduler; then
            echo "backend-scheduler: RUNNING"
            sudo systemctl status backend-scheduler --no-pager | head -5
        else
            echo "backend-scheduler: FAILED"
            sudo journalctl -u backend-scheduler --no-pager -n 20
            exit 1
        fi

        # Check backend-mcp status
        if sudo systemctl is-active --quiet backend-mcp; then
            echo "backend-mcp: RUNNING"
            sudo systemctl status backend-mcp --no-pager | head -5
        else
            echo "backend-mcp: FAILED"
            sudo journalctl -u backend-mcp --no-pager -n 20
            exit 1
        fi
    """

    stdout, stderr, exit_code = run_ssh_command(credentials, start_cmd, verbose)
    return exit_code == 0


def deploy(dry_run: bool = False, verbose: bool = True) -> bool:
    """
    Full deployment to VM via rsync.

    Args:
        dry_run: Show what would happen without doing it
        verbose: Print output
    """
    credentials = get_ssh_credentials()

    print(f"\nDeploying to {credentials['host']} (mode: rsync)")
    print("=" * 50)

    if dry_run:
        print("DRY RUN - no changes will be made\n")

    # Step 1: Stop services
    if not dry_run:
        if not step_stop_services(credentials, verbose):
            print("WARNING: Could not stop all services")

    # Step 2: Update code
    success = step_update_code_rsync(credentials, dry_run, verbose)

    if not success:
        print("ERROR: Code update failed!")
        return False

    if dry_run:
        print("\nDry run complete")
        return True

    # Step 3: Check venv
    if not step_check_venv(credentials, verbose):
        print("ERROR: venv setup failed!")
        return False

    # Step 4: Check requirements
    if not step_check_requirements(credentials, verbose):
        print("ERROR: Requirements installation failed!")
        return False

    # Step 5: Check .env
    if not step_check_env(credentials, verbose):
        print("WARNING: .env check failed - service may not start correctly")

    # Step 6: Start services
    if not step_start_services(credentials, verbose):
        print("ERROR: Failed to start services!")
        return False

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
        sudo systemctl status backend-mcp --no-pager 2>/dev/null || echo "backend-mcp: not running"
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
  python scripts/deploy_to_vm.py                  # Full deploy via rsync
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
            success = deploy(dry_run=args.dry_run, verbose=verbose)

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
