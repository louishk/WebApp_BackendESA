#!/usr/bin/env python3
"""
Main Entry Point for Modular SQL Sync Client

This script provides a command-line interface for the modular SQL synchronization
system that syncs data between on-premises SQL Server and Azure SQL Database.
"""

import sys
import os
import argparse
from typing import List

# Add current directory to Python path to ensure local imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from sync_client import PooledSQLSyncClient
    from data_control import DataControlManager
    from dotenv import load_dotenv
except ImportError as e:
    print(f"Import error: {e}")
    print("Please ensure all required modules are available:")
    print("  - sync_client.py")
    print("  - sync_db_connection.py")
    print("  - data_handler.py")
    print("  - sync_manager.py")
    print("  - data_control.py")
    print("  - python-dotenv (pip install python-dotenv)")
    sys.exit(1)

# Load environment variables
load_dotenv()


def validate_environment():
    """Validate required environment variables"""
    required_vars = [
        'SQL_SERVERNAME', 'DATABASE_NAME',
        'AZURE_SERVER', 'AZURE_DATABASE_SLDB',
        'AZURE_USERNAME', 'AZURE_PASSWORD'
    ]

    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        print(f"ERROR: Missing required environment variables: {', '.join(missing_vars)}")
        print("Please check your .env file.")
        return False

    return True


def display_configuration():
    """Display current configuration with correct mode detection"""
    print("Modular SQL Sync Configuration:")

    # Import here to avoid circular imports
    from sync_db_connection import create_connection_manager

    # Create temporary connection manager to detect actual mode
    try:
        temp_manager = create_connection_manager()
        info = temp_manager.get_connection_info()
        connection_mode = info.get('connection_mode', 'Unknown')

        print(f"  Connection Mode: {connection_mode}")

        if connection_mode == 'SSH Tunnel':
            # Show SSH mode configuration
            print(f"  SSH Host: {info.get('vm_ssh_host')}")
            print(f"  SSH Username: {info.get('vm_ssh_username')}")
            print(f"  SSH Port: {info.get('vm_ssh_port')}")
            print(f"  VM Database: {info.get('vm_database')}")
            print(f"  Local Tunnel Port: {info.get('vm_local_tunnel_port')}")
            print(f"  Tunnel Status: {info.get('tunnel_status')}")
        else:
            # Show local mode configuration
            print(f"  On-Premises Server: {info.get('onprem_server')}")
            print(f"  On-Premises Database: {info.get('onprem_database')}")
            print(f"  Auth Mode: {info.get('auth_mode')}")

        # Always show Azure configuration
        print(f"  Azure Server: {info.get('azure_server')}")
        print(f"  Azure Database: {info.get('azure_database')}")
        print(f"  Azure Port: {info.get('azure_port')}")

        # Clean up temp manager
        temp_manager.close_connections()

    except Exception as e:
        # Fallback to old display if connection manager fails
        print(f"  Connection Mode: Error detecting mode ({e})")
        print(f"  On-Premises Server: {os.getenv('SQL_SERVERNAME', 'Not set')}")
        print(f"  On-Premises Database: {os.getenv('DATABASE_NAME', 'Not set')}")
        print(f"  Azure Server: {os.getenv('AZURE_SERVER', 'Not set')}")
        print(f"  Azure Database: {os.getenv('AZURE_DATABASE_SLDB', 'Not set')}")
        print(f"  Azure Port: {os.getenv('AZURE_PORT', '1433')}")
        print(f"  Auth Mode: {os.getenv('AUTHMODE', 'Windows')}")

    # Show common settings
    print(f"  Sync Workers: {os.getenv('SYNC_WORKERS', '4')}")
    print(f"  Fast Sync: {os.getenv('USE_FAST_SYNC', 'true')}")
    print()


def run_data_control(sync_client: PooledSQLSyncClient, tables: List[str],
                     show_details: bool, show_timestamps: bool):
    """Run data control comparison between on-premises and Azure"""
    import time

    print("🔍 Starting data control comparison...")
    start_time = time.time()

    # Get engines
    onprem_engine = sync_client.connection_manager.get_onprem_engine()
    azure_engine = sync_client.connection_manager.get_azure_engine()

    # Create data control manager
    data_control = DataControlManager()

    # Run comparison
    comparisons = data_control.compare_all_tables(onprem_engine, azure_engine, tables)

    # Calculate execution time
    execution_time = time.time() - start_time

    # Print report with timing
    data_control.print_comparison_report(comparisons, show_details, show_timestamps, execution_time)

    # Return summary for exit code determination
    summary = data_control.generate_summary_report(comparisons)
    return summary['error_count'] == 0 and summary['warning_count'] == 0


def show_site_info(sync_client: PooledSQLSyncClient, tables: List[str]):
    """Show SiteID information for specified tables"""
    print("SiteID Information:")
    print("-" * 50)

    site_info = sync_client.show_site_info(tables)

    for table_name, info in site_info.items():
        status = info.get("status")

        if status == "not_found":
            print(f"❌ {table_name}: Table not found")
        elif status == "no_siteid":
            print(f"➖ {table_name}: No SiteID column")
        elif status == "no_data":
            print(f"⚠️  {table_name}: Has SiteID column but no data")
        elif status == "error":
            print(f"❌ {table_name}: Error - {info.get('error', 'Unknown error')}")
        elif status == "ok":
            row_count = info.get("row_count", 0)
            unique_sites = info.get("unique_sites", 0)
            min_site_id = info.get("min_site_id")
            max_site_id = info.get("max_site_id")

            if unique_sites == 1:
                print(f"✅ {table_name}: {row_count:,} rows, SiteID: {min_site_id}")
            else:
                print(
                    f"⚠️  {table_name}: {row_count:,} rows, Multiple SiteIDs: {min_site_id} to {max_site_id} ({unique_sites} unique)")


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Modular SQL-to-SQL Sync Client",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --test-connections              # Test database connections
  python main.py --dry-run                       # Analyze tables without syncing
  python main.py --tables Payments Customers     # Sync specific tables
  python main.py --show-site-info                # Show SiteID information
  python main.py --data-control                  # Compare row counts between databases
  python main.py --data-control --tables Payments # Compare specific tables
  python main.py --data-control-details          # Detailed comparison report
  python main.py                                 # Run full sync

Environment Variables:
  SQL_SERVERNAME         - On-premises SQL Server name
  DATABASE_NAME          - On-premises database name
  AZURE_SERVER          - Azure SQL Server name
  AZURE_DATABASE_SLDB   - Azure database name
  AZURE_USERNAME        - Azure username
  AZURE_PASSWORD        - Azure password
  SYNC_WORKERS          - Number of parallel workers (default: 4)
  USE_FAST_SYNC         - Enable fast sync (default: true)
  LOG_LEVEL             - Logging level (default: INFO)
  CONSOLE_LOG_LEVEL     - Console logging level (default: WARNING)
        """
    )

    parser.add_argument("--dry-run", action="store_true",
                        help="Run in dry-run mode (analyze only)")
    parser.add_argument("--tables", nargs="+",
                        help="Sync only specific tables")
    parser.add_argument("--test-connections", action="store_true",
                        help="Test database connections only")
    parser.add_argument("--show-site-info", action="store_true",
                        help="Show SiteID information for tables")
    parser.add_argument("--site-info-tables", nargs="+",
                        help="Tables to check for SiteID info (default: common tables)")
    parser.add_argument("--data-control", action="store_true",
                        help="Compare row counts between on-premises and Azure")
    parser.add_argument("--data-control-details", action="store_true",
                        help="Show detailed table-by-table comparison")
    parser.add_argument("--data-control-timestamps", action="store_true",
                        help="Include timestamp comparison in data control report")
    parser.add_argument("--config", action="store_true",
                        help="Show configuration and exit")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress configuration display")

    args = parser.parse_args()

    # Show configuration unless quiet mode
    if not args.quiet and not args.config:
        display_configuration()

    # Show config and exit if requested
    if args.config:
        display_configuration()
        sys.exit(0)

    # Validate environment
    if not validate_environment():
        sys.exit(1)

    # Initialize sync client
    try:
        sync_client = PooledSQLSyncClient()
    except Exception as e:
        print(f"Configuration error: {e}")
        sys.exit(1)

    try:
        if args.test_connections:
            # Test connections only
            if sync_client.test_connections():
                print("[OK] All connections successful!")
                sys.exit(0)
            else:
                print("[ERROR] Connection test failed!")
                sys.exit(1)

        elif args.show_site_info:
            # Show SiteID information
            tables_to_check = args.site_info_tables or ['Payments', 'Waitings', 'Customers', 'Sites']
            show_site_info(sync_client, tables_to_check)
            sys.exit(0)

        elif args.data_control or args.data_control_details or args.data_control_timestamps:
            # Run data control comparison
            show_details = args.data_control_details or args.data_control or True  # Always show some details
            show_timestamps = args.data_control_timestamps

            success = run_data_control(sync_client, args.tables, show_details, show_timestamps)

            if success:
                print("\n✅ Data control check passed!")
                sys.exit(0)
            else:
                print("\n⚠️  Data control check found discrepancies!")
                sys.exit(1)

        else:
            # Run the sync
            print("Starting sync process...")
            results = sync_client.sync_all_tables(
                dry_run=args.dry_run,
                table_filter=args.tables
            )

            # Final status
            successful = sum(1 for r in results if r.get('status') == 'success')
            failed = sum(1 for r in results if r.get('status') == 'error')

            if failed == 0:
                print(f"\n🎉 Sync completed successfully! ({successful} tables)")
                sys.exit(0)
            else:
                print(f"\n⚠️  Sync completed with {failed} failures out of {len(results)} tables")
                sys.exit(1)

    except KeyboardInterrupt:
        print("\nSync interrupted by user")
        sys.exit(130)  # Standard exit code for SIGINT

    except Exception as e:
        print(f"Sync failed: {str(e)}")
        sys.exit(1)

    finally:
        # Clean up connections
        try:
            sync_client.cleanup_connections()
        except:
            pass


if __name__ == "__main__":
    main()