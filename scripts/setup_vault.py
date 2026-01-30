#!/usr/bin/env python3
"""
WebApp Backend Vault Setup Script

Initialize the secrets vault and migrate credentials from .env file.

Usage:
    # Generate new master key and initialize vault
    python scripts/setup_vault.py

    # Use existing master key
    VAULT_MASTER_KEY="your-key-here" python scripts/setup_vault.py

    # Migrate from specific .env file
    python scripts/setup_vault.py --env-file /path/to/.env
"""

import os
import sys
import argparse
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'scripts'))

import secrets


def generate_master_key() -> str:
    """Generate a secure master key"""
    return secrets.token_urlsafe(32)


def setup_vault(
    vault_dir: str = ".vault",
    env_file: str = ".env",
    master_key: str = None
) -> dict:
    """
    Initialize vault and migrate secrets from .env

    Args:
        vault_dir: Directory for vault storage
        env_file: Path to .env file to migrate from
        master_key: Optional master key (generates new if not provided)

    Returns:
        Setup status dict
    """
    result = {
        "success": False,
        "vault_dir": None,
        "master_key": None,
        "migrated_keys": [],
        "errors": []
    }

    try:
        # Determine master key
        if not master_key:
            master_key = os.environ.get('VAULT_MASTER_KEY')

        if not master_key:
            # Generate new master key
            master_key = generate_master_key()
            print("\n" + "=" * 60)
            print("IMPORTANT: New Master Key Generated")
            print("=" * 60)
            print(f"\nVAULT_MASTER_KEY={master_key}\n")
            print("Save this key securely! Options:")
            print("  1. Add to environment: export VAULT_MASTER_KEY='...'")
            print("  2. The key is saved to .vault/.key file automatically")
            print("  3. For PHP, add to .env: VAULT_MASTER_KEY=...")
            print("=" * 60 + "\n")
            result["master_key"] = master_key
        else:
            print("Using existing master key from environment")
            result["master_key"] = "***existing***"

        # Set in environment for vault initialization
        os.environ['VAULT_MASTER_KEY'] = master_key

        # Create vault directory
        vault_path = Path(vault_dir)
        vault_path.mkdir(exist_ok=True)

        # Save master key to .key file
        key_file = vault_path / ".key"
        if not key_file.exists():
            with open(key_file, 'w') as f:
                f.write(master_key)
            os.chmod(key_file, 0o600)
            print(f"Master key saved to {key_file}")

        result["vault_dir"] = str(vault_path.absolute())

        # Initialize vault
        from secrets_vault import LocalSecretsVault
        vault = LocalSecretsVault(vault_dir=vault_dir)

        print(f"\nVault initialized at: {vault_path.absolute()}")

        # Migrate from .env if it exists
        env_path = Path(env_file)
        if env_path.exists():
            print(f"\nMigrating secrets from: {env_path.absolute()}")
            migrated = vault.migrate_from_env(env_file)

            if migrated:
                print(f"\nMigrated {len(migrated)} secrets:")
                for key in sorted(migrated.keys()):
                    print(f"  - {key}")
                result["migrated_keys"] = list(migrated.keys())
            else:
                print("No secrets found to migrate")
        else:
            print(f"\n.env file not found at {env_path}, skipping migration")

        # Show vault status
        print("\n" + "-" * 40)
        print("Vault Status:")
        print("-" * 40)
        keys = vault.list_keys()
        print(f"Total secrets stored: {len(keys)}")

        if keys:
            print("\nStored keys:")
            for key in sorted(keys):
                print(f"  - {key}")

        # Export PHP info
        php_info = vault.export_for_php()
        print("\n" + "-" * 40)
        print("PHP Configuration Info:")
        print("-" * 40)
        print(f"Vault directory: {php_info['vault_dir']}")
        print(f"Salt (base64): {php_info['salt']}")
        print(f"PBKDF2 iterations: {php_info['iterations']}")

        result["success"] = True
        print("\nVault setup complete!")

    except Exception as e:
        result["errors"].append(str(e))
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()

    return result


def main():
    parser = argparse.ArgumentParser(description="Setup WebApp Backend Secrets Vault")
    parser.add_argument(
        "--vault-dir",
        default=".vault",
        help="Directory for vault storage (default: .vault)"
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to .env file to migrate from (default: .env)"
    )
    parser.add_argument(
        "--key",
        help="Master key to use (or set VAULT_MASTER_KEY env var)"
    )

    args = parser.parse_args()

    # Change to project root
    os.chdir(PROJECT_ROOT)

    print("=" * 60)
    print("WebApp Backend - Vault Setup")
    print("=" * 60)

    result = setup_vault(
        vault_dir=args.vault_dir,
        env_file=args.env_file,
        master_key=args.key
    )

    if not result["success"]:
        print("\nSetup failed with errors:")
        for error in result["errors"]:
            print(f"  - {error}")
        sys.exit(1)

    # Print next steps
    print("\n" + "=" * 60)
    print("Next Steps:")
    print("=" * 60)
    print("1. Add VAULT_MASTER_KEY to your .env file:")
    print(f"   VAULT_MASTER_KEY={result['master_key'] if result['master_key'] != '***existing***' else '<your-key>'}")
    print("")
    print("2. The sensitive values in .env can now be removed")
    print("   (they will be loaded from vault instead)")
    print("")
    print("3. Add .vault/ to .gitignore (except metadata.json)")
    print("=" * 60)


if __name__ == "__main__":
    main()
