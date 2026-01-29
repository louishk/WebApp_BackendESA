"""
Database Connection and Pooling Module

Handles SQLAlchemy engine creation with connection pooling for both on-premises
and Azure SQL Server instances. Provides connection testing and validation.
Enhanced with optional SSH tunnel support for remote VM connections.
"""

import logging
import os
import time
import urllib.parse
from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

# Optional SSH tunnel support
try:
    from sshtunnel import SSHTunnelForwarder

    SSH_AVAILABLE = True
except ImportError:
    SSH_AVAILABLE = False
    SSHTunnelForwarder = None

# Load environment variables
load_dotenv()


class PooledConnectionManager:
    """Manages SQLAlchemy connection pools for on-premises and Azure databases

    Enhanced with optional SSH tunnel support:
    - Local mode: Direct connection (existing functionality)
    - SSH mode: Tunnel through remote VM (new functionality)
    """

    def __init__(self):
        """Initialize connection manager and create engines"""
        self.onprem_engine = None
        self.azure_engine = None
        self.ssh_tunnel = None
        self.use_ssh = False
        self.logger = logging.getLogger('PooledConnectionManager')

        # Check if SSH tunnel should be used (optional)
        self._check_ssh_config()

        # Validate environment variables first
        self._validate_environment()

        # Setup SSH tunnel if needed (optional)
        if self.use_ssh:
            self._setup_ssh_tunnel()

        # Create engines
        self._create_engines()

    def _check_ssh_config(self):
        """Check if SSH tunnel configuration is present and valid"""
        ssh_host = os.getenv('VM_SSH_HOST')

        if ssh_host:
            # SSH mode requested
            if not SSH_AVAILABLE:
                raise ImportError(
                    "SSH tunnel requested but 'sshtunnel' package not installed. Run: pip install sshtunnel")

            ssh_username = os.getenv('VM_SSH_USERNAME')
            ssh_password = os.getenv('VM_SSH_PASSWORD')

            if not (ssh_username and ssh_password):
                raise ValueError("SSH tunnel requires VM_SSH_HOST, VM_SSH_USERNAME, and VM_SSH_PASSWORD")

            self.use_ssh = True
            self.logger.info("SSH tunnel mode enabled")
        else:
            # Local mode (existing behavior)
            self.use_ssh = False
            self.logger.info("Local connection mode enabled")

    def _setup_ssh_tunnel(self):
        """Create SSH tunnel to remote VM SQL Server"""
        try:
            ssh_host = os.getenv('VM_SSH_HOST')
            ssh_port = int(os.getenv('VM_SSH_PORT', 22))
            ssh_username = os.getenv('VM_SSH_USERNAME')
            ssh_password = os.getenv('VM_SSH_PASSWORD')

            vm_sql_port = int(os.getenv('VM_SQL_PORT', 1433))
            local_tunnel_port = int(os.getenv('VM_LOCAL_TUNNEL_PORT', 9999))

            # Create SSH tunnel
            self.ssh_tunnel = SSHTunnelForwarder(
                (ssh_host, ssh_port),
                ssh_username=ssh_username,
                ssh_password=ssh_password,
                remote_bind_address=('127.0.0.1', vm_sql_port),
                local_bind_address=('127.0.0.1', local_tunnel_port),
                set_keepalive=30
            )

            # Start the tunnel
            self.ssh_tunnel.start()
            self.logger.info(f"SSH tunnel established: localhost:{local_tunnel_port} -> {ssh_host}:{vm_sql_port}")

            # Give tunnel a moment to establish
            time.sleep(2)

        except Exception as e:
            raise Exception(f"Failed to create SSH tunnel: {str(e)}")

    def _validate_environment(self):
        """Validate required environment variables"""
        # Azure variables (always required)
        required_vars = [
            'AZURE_SERVER', 'AZURE_DATABASE_SLDB',
            'AZURE_USERNAME', 'AZURE_PASSWORD'
        ]

        # On-premises variables (depend on mode)
        if self.use_ssh:
            # SSH/VM mode requirements
            required_vars.extend([
                'VM_SSH_HOST', 'VM_SSH_USERNAME', 'VM_SSH_PASSWORD',
                'VM_DATABASE_NAME'
            ])
        else:
            # Local mode requirements (existing)
            required_vars.extend([
                'SQL_SERVERNAME', 'DATABASE_NAME'
            ])

        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            mode = "SSH/VM" if self.use_ssh else "Local"
            raise ValueError(f"Missing required environment variables for {mode} mode: {', '.join(missing_vars)}")

    def _create_engines(self):
        """Create SQLAlchemy engines for both databases"""
        try:
            # On-premises engine
            self.onprem_engine = self._create_onprem_engine()
            self.logger.debug("On-premises engine created successfully")

            # Azure engine
            self.azure_engine = self._create_azure_engine()
            self.logger.debug("Azure engine created successfully")

        except Exception as e:
            raise Exception(f"Failed to create database engines: {str(e)}")

    def _create_onprem_engine(self):
        """Create on-premises SQL Server engine"""
        if self.use_ssh:
            # SSH tunnel mode - connect through tunnel
            server = '127.0.0.1'
            port = int(os.getenv('VM_LOCAL_TUNNEL_PORT', 9999))
            database = os.getenv('VM_DATABASE_NAME')
            driver = os.getenv('VM_SQL_DRIVER', 'ODBC Driver 17 for SQL Server')

            return self._create_sql_server_engine(
                driver=driver,
                server=server,
                database=database,
                port=port,
                pool_size=5,  # Smaller pool for SSH tunnel
                max_overflow=10,
                pool_timeout=60,
                pool_recycle=3600,
                db_type='ssh_tunnel'
            )
        else:
            # Local mode (existing functionality)
            server = os.getenv('SQL_SERVERNAME')
            database = os.getenv('DATABASE_NAME')
            auth_mode = os.getenv('AUTHMODE', 'Windows')
            username = os.getenv('USERNAME') if auth_mode.lower() != 'windows' else None
            password = os.getenv('PASSWORD') if auth_mode.lower() != 'windows' else None

            return self._create_sql_server_engine(
                driver='SQL Server',
                server=server,
                database=database,
                username=username,
                password=password,
                pool_size=10,
                max_overflow=20,
                pool_timeout=60,
                pool_recycle=3600,
                db_type='onprem'
            )

    def _create_azure_engine(self):
        """Create Azure SQL Database engine"""
        server = os.getenv('AZURE_SERVER')
        database = os.getenv('AZURE_DATABASE_SLDB')
        username = os.getenv('AZURE_USERNAME')
        password = os.getenv('AZURE_PASSWORD')
        driver = os.getenv('AZURE_DRIVER', 'ODBC Driver 18 for SQL Server')
        port = int(os.getenv('AZURE_PORT', 1433))

        return self._create_sql_server_engine(
            driver=driver,
            server=server,
            database=database,
            username=username,
            password=password,
            port=port,
            pool_size=10,
            max_overflow=20,
            pool_timeout=60,
            pool_recycle=3600,
            db_type='azure'
        )

    def _create_sql_server_engine(self, driver: str, server: str, database: str,
                                  username: Optional[str] = None, password: Optional[str] = None,
                                  port: int = 1433, pool_size: int = 5, max_overflow: int = 10,
                                  pool_timeout: int = 30, pool_recycle: int = 1800,
                                  pool_pre_ping: bool = True, retries: int = 3,
                                  retry_delay: int = 5, db_type: str = 'azure'):
        """
        Create SQL Server engine with connection pooling and retry logic

        Args:
            driver: ODBC driver name
            server: Server name/address
            database: Database name
            username: Username (None for Windows auth)
            password: Password (None for Windows auth)
            port: Port number (default 1433)
            pool_size: Number of connections to maintain
            max_overflow: Additional connections allowed
            pool_timeout: Timeout for getting connection from pool
            pool_recycle: Recycle connections after this many seconds
            pool_pre_ping: Test connections before use
            retries: Number of retry attempts
            retry_delay: Delay between retries
            db_type: 'azure', 'onprem', or 'ssh_tunnel' for connection string formatting

        Returns:
            SQLAlchemy Engine instance
        """
        attempt = 0

        # Build connection URL
        if db_type == 'azure':
            connection_url = (
                f"mssql+pyodbc://{urllib.parse.quote_plus(username)}:{urllib.parse.quote_plus(password)}"
                f"@{server}:{port}/{database}?driver={urllib.parse.quote_plus(driver)}"
                f"&Encrypt=yes&TrustServerCertificate=yes&Connection Timeout={pool_timeout}"
            )
        elif db_type == 'ssh_tunnel':
            # SSH tunnel connection - use Windows auth through tunnel
            connection_url = (
                f"mssql+pyodbc://@{server}:{port}/{database}?driver={urllib.parse.quote_plus(driver)}"
                f"&Trusted_Connection=yes&TrustServerCertificate=yes&Connection Timeout={pool_timeout}"
                f"&Encrypt=no"
            )
        elif db_type == 'onprem':
            # Local on-premises connection (existing logic)
            if username and password:
                # SQL Server authentication
                connection_url = (
                    f"mssql+pyodbc://{urllib.parse.quote_plus(username)}:{urllib.parse.quote_plus(password)}"
                    f"@{server}/{database}?driver={urllib.parse.quote_plus(driver)}"
                    f"&TrustServerCertificate=yes&Connection Timeout={pool_timeout}"
                )
            else:
                # Windows authentication
                connection_url = (
                    f"mssql+pyodbc://@{server}/{database}?driver={urllib.parse.quote_plus(driver)}"
                    f"&Trusted_Connection=yes&TrustServerCertificate=yes&Connection Timeout={pool_timeout}"
                )
        else:
            raise ValueError("Unsupported database type. Please use 'azure', 'onprem', or 'ssh_tunnel'.")

        # Retry logic for engine creation
        while attempt < retries:
            try:
                engine = create_engine(
                    connection_url,
                    pool_size=pool_size,
                    max_overflow=max_overflow,
                    pool_timeout=pool_timeout,
                    pool_recycle=pool_recycle,
                    pool_pre_ping=pool_pre_ping
                )

                self.logger.debug(f"SQLAlchemy engine created successfully for {db_type}")
                return engine

            except OperationalError as oe:
                attempt += 1
                self.logger.error(f"Connection attempt {attempt} failed for {db_type}: {oe}")

                if attempt >= retries:
                    self.logger.critical(f"Max retries reached for {db_type}. Could not create SQLAlchemy engine.")
                    raise

                time.sleep(retry_delay)

    def test_connections(self) -> bool:
        """
        Test both database connections

        Returns:
            bool: True if both connections successful, False otherwise
        """
        self.logger.info("Testing database connections...")

        # Test on-premises
        connection_type = "SSH tunnel" if self.use_ssh else "local"
        try:
            with self.onprem_engine.connect() as conn:
                result = conn.execute(text("SELECT @@VERSION, DB_NAME(), @@SERVERNAME"))
                row = result.fetchone()
                database = row[1]
                server = row[2] if len(row) > 2 else "Unknown"
                version_info = row[0][:100] + "..." if len(row[0]) > 100 else row[0]

                print(f"✓ On-premises ({connection_type}): {server}/{database}")
                self.logger.debug(f"On-premises connected via {connection_type}: {server}/{database} - {version_info}")

        except Exception as e:
            print(f"✗ On-premises ({connection_type}) connection failed")
            self.logger.error(f"On-premises ({connection_type}) connection failed: {str(e)}")
            return False

        # Test Azure
        try:
            with self.azure_engine.connect() as conn:
                result = conn.execute(text("SELECT @@VERSION, DB_NAME()"))
                row = result.fetchone()
                database = row[1]
                version_info = row[0][:100] + "..." if len(row[0]) > 100 else row[0]

                print(f"✓ Azure: {database}")
                self.logger.debug(f"Azure connected: {database} - {version_info}")

        except Exception as e:
            print(f"✗ Azure connection failed")
            self.logger.error(f"Azure connection failed: {str(e)}")
            return False

        return True

    def get_onprem_engine(self):
        """Get on-premises database engine"""
        if not self.onprem_engine:
            raise RuntimeError("On-premises engine not initialized")
        return self.onprem_engine

    def get_azure_engine(self):
        """Get Azure database engine"""
        if not self.azure_engine:
            raise RuntimeError("Azure engine not initialized")
        return self.azure_engine

    def close_connections(self):
        """Close all database connections and SSH tunnel if active"""
        try:
            if self.onprem_engine:
                self.onprem_engine.dispose()
                self.logger.debug("On-premises engine disposed")

            if self.azure_engine:
                self.azure_engine.dispose()
                self.logger.debug("Azure engine disposed")

            # Close SSH tunnel if it exists
            if self.ssh_tunnel:
                self.ssh_tunnel.stop()
                self.logger.debug("SSH tunnel closed")

        except Exception as e:
            self.logger.error(f"Error closing connections: {str(e)}")

    def get_connection_info(self) -> dict:
        """
        Get connection information for display

        Returns:
            dict: Connection configuration details
        """
        info = {
            'connection_mode': 'SSH Tunnel' if self.use_ssh else 'Local',
            'azure_server': os.getenv('AZURE_SERVER'),
            'azure_database': os.getenv('AZURE_DATABASE_SLDB'),
            'azure_port': os.getenv('AZURE_PORT', '1433'),
        }

        if self.use_ssh:
            # SSH mode info
            info.update({
                'vm_ssh_host': os.getenv('VM_SSH_HOST'),
                'vm_ssh_port': os.getenv('VM_SSH_PORT', '22'),
                'vm_ssh_username': os.getenv('VM_SSH_USERNAME'),
                'vm_database': os.getenv('VM_DATABASE_NAME'),
                'vm_local_tunnel_port': os.getenv('VM_LOCAL_TUNNEL_PORT', '9999'),
                'tunnel_status': 'Active' if self.ssh_tunnel and self.ssh_tunnel.is_active else 'Inactive'
            })
        else:
            # Local mode info (existing)
            info.update({
                'onprem_server': os.getenv('SQL_SERVERNAME'),
                'onprem_database': os.getenv('DATABASE_NAME'),
                'auth_mode': os.getenv('AUTHMODE', 'Windows')
            })

        return info


def create_connection_manager() -> PooledConnectionManager:
    """
    Factory function to create a PooledConnectionManager instance

    Returns:
        PooledConnectionManager: Configured connection manager
    """
    return PooledConnectionManager()


# For backward compatibility and testing
if __name__ == "__main__":
    # Test the connection manager
    try:
        manager = create_connection_manager()

        print("Connection Manager initialized successfully!")
        print("\nConnection Info:")
        info = manager.get_connection_info()
        for key, value in info.items():
            if 'password' not in key.lower():  # Don't display passwords
                print(f"  {key}: {value}")

        print("\nTesting connections...")
        if manager.test_connections():
            print("\n✅ All connections successful!")
        else:
            print("\n❌ Connection test failed!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        if 'manager' in locals():
            manager.close_connections()