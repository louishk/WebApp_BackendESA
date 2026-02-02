#!/usr/bin/env python3
"""
Deploy to VM Script

Deploys ESA Backend to Azure VM with full environment setup.

Usage:
    python scripts/deploy_to_vm.py                  # Full deploy (rsync)
    python scripts/deploy_to_vm.py --pull           # Deploy via git pull
    python scripts/deploy_to_vm.py --hard-reset     # Deploy via git reset --hard + pull
    python scripts/deploy_to_vm.py --status         # Check service status
    python scripts/deploy_to_vm.py --cmd "ls"       # Run custom command
    python scripts/deploy_to_vm.py --dry-run        # Show what would happen

Deployment Steps:
    1. Stop all services (esa-backend, scheduler)
    2. Kill any stray Python processes
    3. Update code (rsync / git pull / git reset --hard)
    4. Check/create Python venv
    5. Check/install requirements
    6. Verify vault system and credentials
    7. Start services

Configuration (from .env and vault):
    - VM_SSH_HOST: VM IP address
    - VM_SSH_PORT: SSH port (default: 22)
    - VM_SSH_ROOT_USERNAME: SSH username
    - VM_SSH_PASSWORD: SSH password (from vault)
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
VM_VAULT_PATH = f"{VM_PYTHON_PATH}/.vault"
VM_ENV_PATH = f"{VM_PYTHON_PATH}/.env"

# Rsync exclude patterns (files that should NOT be synced)
RSYNC_EXCLUDES = [
    '.git',
    '.idea',
    '__pycache__',
    '*.pyc',
    '.env',           # VM has its own .env
    '.vault',         # VM has its own vault
    'venv',           # VM has its own venv
    'node_modules',
    '.DS_Store',
    '*.log',
    'secrets.enc',
]

# Required vault secrets for the application
REQUIRED_VAULT_SECRETS = [
    'SCHEDULER_DB_PASSWORD',
    'MS_OAUTH_CLIENT_SECRET',
]


def get_ssh_credentials():
    """Get SSH credentials from .env and vault."""
    from common.secrets_vault import secure_config

    host = env_config('VM_SSH_HOST', default=None)
    port = env_config('VM_SSH_PORT', default=22, cast=int)
    username = env_config('VM_SSH_ROOT_USERNAME', default=None)
    password = secure_config('VM_SSH_PASSWORD', default=None)

    if not all([host, username, password]):
        missing = []
        if not host:
            missing.append('VM_SSH_HOST')
        if not username:
            missing.append('VM_SSH_ROOT_USERNAME')
        if not password:
            missing.append('VM_SSH_PASSWORD (in vault)')
        raise ValueError(f"Missing SSH credentials: {', '.join(missing)}")

    return {
        'host': host,
        'port': port,
        'username': username,
        'password': password
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
        client.connect(
            hostname=credentials['host'],
            port=credentials['port'],
            username=credentials['username'],
            password=credentials['password'],
            timeout=30
        )

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
    print("\n[1/7] Stopping services...")

    stop_cmd = """
        sudo systemctl stop esa-backend 2>/dev/null || true
        sudo systemctl stop backend-scheduler 2>/dev/null || true
        sudo systemctl stop backend-scheduler-web 2>/dev/null || true

        # Kill any stray Python/gunicorn processes from backend
        sudo pkill -9 -f '/var/www/backend.*python' 2>/dev/null || true
        sudo pkill -9 -f 'gunicorn.*wsgi' 2>/dev/null || true
        sudo pkill -9 -f 'flask.*run' 2>/dev/null || true

        sleep 2

        # Verify nothing is running on port 5000
        if sudo lsof -i :5000 2>/dev/null; then
            echo "WARNING: Port 5000 still in use"
            sudo fuser -k 5000/tcp 2>/dev/null || true
        fi

        echo "Services stopped"
    """

    stdout, stderr, exit_code = run_ssh_command(credentials, stop_cmd, verbose)
    return True  # Always continue even if some services weren't running


def step_update_code_rsync(credentials: dict, dry_run: bool = False, verbose: bool = True) -> bool:
    """Step 2a: Update code via rsync."""
    print("\n[2/7] Syncing code via rsync...")

    excludes = ' '.join(f"--exclude='{e}'" for e in RSYNC_EXCLUDES)
    dry_run_flag = '-n' if dry_run else ''
    temp_dir = "/tmp/webapp-deploy"

    rsync_cmd = (
        f"sshpass -p '{credentials['password']}' "
        f"rsync -avz --delete {dry_run_flag} {excludes} "
        f"-e 'ssh -o StrictHostKeyChecking=no -p {credentials['port']}' "
        f"{project_root}/ "
        f"{credentials['username']}@{credentials['host']}:{temp_dir}/"
    )

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
                --exclude='.vault' \
                --exclude='backend/python/.env' \
                --exclude='backend/python/venv' \
                --exclude='backend/python/.vault' \
                {temp_dir}/ {VM_BACKEND_PATH}/

            sudo chown -R www-data:www-data {VM_BACKEND_PATH}
            rm -rf {temp_dir}
            echo "Code deployed to {VM_BACKEND_PATH}"
        """

        stdout, stderr, exit_code = run_ssh_command(credentials, move_cmd, verbose)
        return exit_code == 0

    except FileNotFoundError:
        print("    ERROR: sshpass not found. Install with: sudo apt install sshpass")
        return False


def step_update_code_git(credentials: dict, hard_reset: bool = False, verbose: bool = True) -> bool:
    """Step 2b: Update code via git pull (or hard reset)."""
    action = "hard reset + pull" if hard_reset else "git pull"
    print(f"\n[2/7] Updating code via {action}...")

    if hard_reset:
        git_cmd = f"""
            cd {VM_BACKEND_PATH}
            sudo -u www-data git fetch origin
            sudo -u www-data git reset --hard origin/master
            sudo -u www-data git clean -fd
            echo "Git reset complete"
            sudo -u www-data git log --oneline -1
        """
    else:
        git_cmd = f"""
            cd {VM_BACKEND_PATH}
            sudo -u www-data git pull origin master
            echo "Git pull complete"
            sudo -u www-data git log --oneline -1
        """

    stdout, stderr, exit_code = run_ssh_command(credentials, git_cmd, verbose)
    return exit_code == 0


def step_check_venv(credentials: dict, verbose: bool = True) -> bool:
    """Step 3: Check/create Python virtual environment."""
    print("\n[3/7] Checking Python virtual environment...")

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
    print("\n[4/7] Checking Python requirements...")

    req_cmd = f"""
        cd {VM_PYTHON_PATH}

        # Upgrade pip first
        sudo -u www-data {VM_VENV_PATH}/bin/pip install --upgrade pip -q

        # Check if requirements need update by comparing installed vs required
        echo "Installing/updating requirements..."
        sudo -u www-data {VM_VENV_PATH}/bin/pip install -r requirements.txt -q

        # Verify key packages
        {VM_VENV_PATH}/bin/python -c "import flask; import gunicorn; import authlib; print('Key packages OK')"
    """

    stdout, stderr, exit_code = run_ssh_command(credentials, req_cmd, verbose, timeout=300)
    return exit_code == 0


def step_check_env(credentials: dict, verbose: bool = True) -> bool:
    """Step 5: Check .env file exists and has required vars."""
    print("\n[5/7] Checking .env configuration...")

    env_cmd = f"""
        if [ -f "{VM_PYTHON_PATH}/.env" ]; then
            echo ".env exists"

            # Check for required variable (only VAULT_MASTER_KEY needed - other config is in YAML)
            if grep -q "^VAULT_MASTER_KEY=" {VM_PYTHON_PATH}/.env; then
                echo "VAULT_MASTER_KEY: SET"
            else
                echo "ERROR: VAULT_MASTER_KEY not found in .env"
                exit 1
            fi
        else
            echo "ERROR: .env file not found at {VM_PYTHON_PATH}/.env"
            exit 1
        fi
    """

    stdout, stderr, exit_code = run_ssh_command(credentials, env_cmd, verbose)
    return exit_code == 0


def step_check_vault(credentials: dict, verbose: bool = True) -> bool:
    """Step 6: Check vault system and required secrets."""
    print("\n[6/7] Checking vault system...")

    # Build the check for required secrets
    secrets_check = " && ".join([
        f'python -c "from common.secrets_vault import secure_config; '
        f"v = secure_config('{s}'); "
        f"print('{s}:', 'SET' if v else 'MISSING')\""
        for s in REQUIRED_VAULT_SECRETS
    ])

    vault_cmd = f"""
        cd {VM_PYTHON_PATH}

        if [ -d "{VM_VAULT_PATH}" ]; then
            echo "Vault directory exists"
            ls -la {VM_VAULT_PATH}/
        else
            echo "ERROR: Vault directory not found at {VM_VAULT_PATH}"
            echo "Run vault setup on VM first"
            exit 1
        fi

        # Check if we can load secrets
        export PYTHONPATH="{VM_PYTHON_PATH}"
        source {VM_VENV_PATH}/bin/activate

        {secrets_check}
    """

    stdout, stderr, exit_code = run_ssh_command(credentials, vault_cmd, verbose)

    # Check if any secrets are missing
    if 'MISSING' in stdout:
        print("    WARNING: Some vault secrets are missing!")
        return False

    return exit_code == 0


def step_start_services(credentials: dict, verbose: bool = True) -> bool:
    """Step 7: Start services."""
    print("\n[7/7] Starting services...")

    start_cmd = f"""
        # Reload systemd in case service files changed
        sudo systemctl daemon-reload

        # Copy service file if not exists
        if [ -f "{VM_PYTHON_PATH}/systemd/esa-backend.service" ]; then
            sudo cp {VM_PYTHON_PATH}/systemd/esa-backend.service /etc/systemd/system/
            sudo systemctl daemon-reload
        fi

        # Create log directory if needed
        sudo mkdir -p /var/log/esa-backend
        sudo chown www-data:www-data /var/log/esa-backend

        # Enable and start esa-backend
        sudo systemctl enable esa-backend
        sudo systemctl start esa-backend

        sleep 3

        # Check status
        if sudo systemctl is-active --quiet esa-backend; then
            echo "esa-backend: RUNNING"
            sudo systemctl status esa-backend --no-pager | head -5
        else
            echo "esa-backend: FAILED"
            sudo journalctl -u esa-backend --no-pager -n 20
            exit 1
        fi
    """

    stdout, stderr, exit_code = run_ssh_command(credentials, start_cmd, verbose)
    return exit_code == 0


def deploy(mode: str = 'rsync', dry_run: bool = False, verbose: bool = True) -> bool:
    """
    Full deployment to VM.

    Args:
        mode: 'rsync', 'pull', or 'hard-reset'
        dry_run: Show what would happen without doing it
        verbose: Print output
    """
    credentials = get_ssh_credentials()

    print(f"\nDeploying to {credentials['host']} (mode: {mode})")
    print("=" * 50)

    if dry_run:
        print("DRY RUN - no changes will be made\n")

    # Step 1: Stop services
    if not dry_run:
        if not step_stop_services(credentials, verbose):
            print("WARNING: Could not stop all services")

    # Step 2: Update code
    if mode == 'rsync':
        success = step_update_code_rsync(credentials, dry_run, verbose)
    elif mode == 'pull':
        if dry_run:
            print("\n[2/7] Would run: git pull")
            success = True
        else:
            success = step_update_code_git(credentials, hard_reset=False, verbose=verbose)
    elif mode == 'hard-reset':
        if dry_run:
            print("\n[2/7] Would run: git reset --hard + git pull")
            success = True
        else:
            success = step_update_code_git(credentials, hard_reset=True, verbose=verbose)
    else:
        print(f"ERROR: Unknown mode: {mode}")
        return False

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

    # Step 6: Check vault
    if not step_check_vault(credentials, verbose):
        print("WARNING: Vault check failed - some features may not work")

    # Step 7: Start services
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
  python scripts/deploy_to_vm.py --pull           # Deploy via git pull
  python scripts/deploy_to_vm.py --hard-reset     # Wipe and deploy via git reset --hard
  python scripts/deploy_to_vm.py --status         # Check service status
  python scripts/deploy_to_vm.py --cmd "ls -la"   # Run custom command
  python scripts/deploy_to_vm.py --dry-run        # Preview deployment
        """
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        '--pull',
        action='store_true',
        help='Update code via git pull (instead of rsync)'
    )
    mode_group.add_argument(
        '--hard-reset',
        action='store_true',
        help='Wipe local changes and update via git reset --hard + pull'
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
            # Determine mode
            if args.hard_reset:
                mode = 'hard-reset'
            elif args.pull:
                mode = 'pull'
            else:
                mode = 'rsync'

            success = deploy(mode=mode, dry_run=args.dry_run, verbose=verbose)

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
