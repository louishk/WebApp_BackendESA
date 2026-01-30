#!/usr/bin/env python3
"""
Deploy to VM Script

Syncs code to Azure VM using rsync over SSH.

Usage:
    python scripts/deploy_to_vm.py              # Sync code to VM
    python scripts/deploy_to_vm.py --restart    # Sync and restart scheduler
    python scripts/deploy_to_vm.py --status     # Check scheduler status
    python scripts/deploy_to_vm.py --cmd "ls"   # Run custom command
    python scripts/deploy_to_vm.py --dry-run    # Show what would be synced

Configuration (from .env and vault):
    - VM_SSH_HOST: VM IP address
    - VM_SSH_PORT: SSH port (default: 22)
    - VM_SSH_ROOT_USERNAME: SSH username
    - VM_SSH_PASSWORD: SSH password (from vault)
"""

import argparse
import sys
import os
import subprocess
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'app' / 'scheduler' / 'python'))

try:
    import paramiko
except ImportError:
    print("ERROR: paramiko is required. Install with: pip install paramiko")
    sys.exit(1)

from decouple import config as env_config
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(project_root / '.env')

# Rsync exclude patterns
RSYNC_EXCLUDES = [
    '.git',
    '.idea',
    '__pycache__',
    '*.pyc',
    '.env',
    '.vault',
    'venv',
    'node_modules',
    '.DS_Store',
    '*.log',
    'secrets.enc',
]


def get_ssh_credentials():
    """Get SSH credentials from .env and vault."""
    # Import vault after loading .env (needs VAULT_MASTER_KEY)
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


def run_ssh_command(credentials: dict, command: str, verbose: bool = True) -> tuple:
    """
    Run a command on the VM via SSH.

    Args:
        credentials: SSH credentials dict
        command: Command to execute
        verbose: Print output in real-time

    Returns:
        Tuple of (stdout, stderr, exit_code)
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        if verbose:
            print(f"Connecting to {credentials['host']}:{credentials['port']}...")

        client.connect(
            hostname=credentials['host'],
            port=credentials['port'],
            username=credentials['username'],
            password=credentials['password'],
            timeout=30
        )

        if verbose:
            print(f"Running: {command}")
            print("-" * 50)

        stdin, stdout, stderr = client.exec_command(command, timeout=120)

        # Read output
        stdout_text = stdout.read().decode('utf-8')
        stderr_text = stderr.read().decode('utf-8')
        exit_code = stdout.channel.recv_exit_status()

        if verbose:
            if stdout_text:
                print(stdout_text)
            if stderr_text:
                print(f"STDERR: {stderr_text}", file=sys.stderr)
            print("-" * 50)
            print(f"Exit code: {exit_code}")

        return stdout_text, stderr_text, exit_code

    finally:
        client.close()


def run_rsync(credentials: dict, dry_run: bool = False, verbose: bool = True) -> bool:
    """
    Sync code to VM using rsync.

    Uses a two-step approach to handle permissions:
    1. Rsync to temp directory (user has write access)
    2. Sudo rsync from temp to /var/www/html

    Args:
        credentials: SSH credentials dict
        dry_run: Show what would be synced without doing it
        verbose: Print output

    Returns:
        True if successful
    """
    excludes = ' '.join(f"--exclude='{e}'" for e in RSYNC_EXCLUDES)
    dry_run_flag = '-n' if dry_run else ''
    temp_dir = "/tmp/webapp-deploy"

    # Step 1: Rsync to temp directory
    rsync_cmd = (
        f"sshpass -p '{credentials['password']}' "
        f"rsync -avz --delete {dry_run_flag} {excludes} "
        f"-e 'ssh -o StrictHostKeyChecking=no -p {credentials['port']}' "
        f"{project_root}/ "
        f"{credentials['username']}@{credentials['host']}:{temp_dir}/"
    )

    if verbose:
        if dry_run:
            print("DRY RUN - showing what would be synced:")
        print(f"Syncing {project_root} to {credentials['host']}:{temp_dir}/")
        print("-" * 50)

    try:
        result = subprocess.run(
            rsync_cmd,
            shell=True,
            capture_output=True,
            text=True
        )

        if verbose:
            if result.stdout:
                lines = result.stdout.strip().split('\n')
                # Show summary for large outputs
                if len(lines) > 30:
                    print('\n'.join(lines[:15]))
                    print(f"... ({len(lines) - 30} more files)")
                    print('\n'.join(lines[-15:]))
                else:
                    print(result.stdout)
            if result.stderr:
                stderr = '\n'.join(
                    line for line in result.stderr.split('\n')
                    if 'password' not in line.lower()
                )
                if stderr.strip():
                    print(f"STDERR: {stderr}")
            print("-" * 50)

        if result.returncode != 0:
            return False

        # Step 2: Move from temp to /var/www/html using sudo (skip for dry run)
        # IMPORTANT: Exclude .env, .vault, and venv from deletion to preserve VM-specific files
        if not dry_run:
            if verbose:
                print("Moving files to /var/www/html with sudo...")

            # Exclude patterns that should be preserved on the VM
            vm_preserve = [
                '.env',
                '.vault',
                'app/scheduler/python/.env',
                'app/scheduler/python/venv',
            ]
            vm_excludes = ' '.join(f"--exclude='{e}'" for e in vm_preserve)

            move_cmd = (
                f"sudo rsync -a --delete {vm_excludes} {temp_dir}/ /var/www/html/ && "
                f"sudo chown -R www-data:www-data /var/www/html && "
                f"rm -rf {temp_dir} && "
                # Remove any stray .vault in scheduler directory so it uses the main vault
                f"sudo rm -rf /var/www/html/app/scheduler/python/.vault"
            )
            stdout, stderr, exit_code = run_ssh_command(
                credentials, move_cmd, verbose=False
            )

            if exit_code != 0:
                print(f"ERROR moving files: {stderr}")
                return False

            if verbose:
                print("Files deployed successfully!")

        return True

    except FileNotFoundError:
        print("ERROR: sshpass not found. Install with: sudo apt install sshpass")
        return False


def deploy(restart_scheduler: bool = False, dry_run: bool = False, verbose: bool = True):
    """
    Deploy latest code to VM.

    Args:
        restart_scheduler: Also restart the scheduler service
        dry_run: Show what would be synced without doing it
        verbose: Print output
    """
    credentials = get_ssh_credentials()

    # Sync code via rsync
    print("\n[1/2] Syncing code to VM...")
    success = run_rsync(credentials, dry_run=dry_run, verbose=verbose)

    if not success:
        print("ERROR: Rsync failed!")
        return False

    if dry_run:
        print("Dry run complete - no changes made")
        return True

    print("Code sync successful!")

    # Restart scheduler if requested
    if restart_scheduler:
        print("\n[2/2] Restarting scheduler...")
        stdout, stderr, exit_code = run_ssh_command(
            credentials,
            "sudo systemctl restart backend-scheduler && sudo systemctl restart backend-scheduler-web && "
            "sleep 2 && sudo systemctl status backend-scheduler --no-pager",
            verbose
        )

        if exit_code != 0:
            print("WARNING: Scheduler restart may have issues")
        else:
            print("Scheduler restarted successfully!")
    else:
        print("\n[2/2] Skipping scheduler restart (use --restart to restart)")

    return True


def check_status(verbose: bool = True):
    """Check scheduler status on VM."""
    credentials = get_ssh_credentials()

    print("\nChecking scheduler status...")
    stdout, stderr, exit_code = run_ssh_command(
        credentials,
        "sudo systemctl status backend-scheduler backend-scheduler-web --no-pager",
        verbose
    )

    return exit_code == 0


def run_custom_command(command: str, verbose: bool = True):
    """Run a custom command on the VM."""
    credentials = get_ssh_credentials()

    stdout, stderr, exit_code = run_ssh_command(
        credentials,
        command,
        verbose
    )

    return exit_code == 0


def main():
    parser = argparse.ArgumentParser(
        description='Deploy to Azure VM',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/deploy_to_vm.py              # Sync code to VM
  python scripts/deploy_to_vm.py --restart    # Sync and restart scheduler
  python scripts/deploy_to_vm.py --status     # Check scheduler status
  python scripts/deploy_to_vm.py --cmd "ls"   # Run custom command
  python scripts/deploy_to_vm.py --dry-run    # Show what would be synced
        """
    )

    parser.add_argument(
        '--restart',
        action='store_true',
        help='Restart scheduler after deploying'
    )

    parser.add_argument(
        '--status',
        action='store_true',
        help='Check scheduler status only (no deploy)'
    )

    parser.add_argument(
        '--cmd',
        type=str,
        help='Run a custom command on the VM'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be synced without making changes'
    )

    parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='Quiet mode (less output)'
    )

    args = parser.parse_args()
    verbose = not args.quiet

    print("=" * 50)
    print("VM Deployment Script")
    print("=" * 50)

    try:
        if args.status:
            success = check_status(verbose)
        elif args.cmd:
            success = run_custom_command(args.cmd, verbose)
        else:
            success = deploy(restart_scheduler=args.restart, dry_run=args.dry_run, verbose=verbose)

        print("\n" + "=" * 50)
        if success:
            print("Done!")
        else:
            print("Completed with warnings")
            sys.exit(1)
        print("=" * 50)

    except ValueError as e:
        print(f"\nERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
