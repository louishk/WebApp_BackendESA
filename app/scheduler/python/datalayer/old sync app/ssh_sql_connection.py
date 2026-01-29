"""
Database Connection and Pooling Module with SSH Tunnel Support

Handles SQLAlchemy engine creation with SSH tunneling for remote SQL Server access.
This approach avoids modifying SQL Server authentication settings.

Save this as: ssh_sql_connection.py
"""

import logging
import os
import time
import urllib.parse
from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
import threading

# Load environment variables
load_dotenv()


class SSHPooledConnectionManager:
    """Manages SQLAlchemy connection pools with SSH tunneling for remote databases"""

    def __init__(self):
        """Initialize connection manager with SSH tunnel"""
        self.onprem_engine = None
        self.azure_engine = None
        self.ssh_tunnel = None
        self.logger = logging.getLogger('SSHPooledConnectionManager')

        # Validate environment variables first
        self._validate_environment()

        # Create SSH tunnel and engines
        self._setup_ssh_tunnel()
        self._create_engines()

    def _validate_environment(self):
        """Validate required environment variables"""
        required_vars = [
            'SSH_HOST', 'SSH_USERNAME', 'SSH_PASSWORD', 'DATABASE_NAME',
            'AZURE_SERVER', 'AZURE_DATABASE_SLDB',
            'AZURE_USERNAME', 'AZURE_PASSWORD'
        ]

        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    def _setup_ssh_tunnel(self):
        """Create SSH tunnel to SQL Server"""
        try:
            ssh_host = os.getenv('SSH_HOST')
            ssh_port = int(os.getenv('SSH_PORT', 22))
            ssh_username = os.getenv('SSH_USERNAME')
            ssh_password = os.getenv('SSH_PASSWORD')
            ssh_key_file = os.getenv('SSH_KEY_FILE')  # Optional: use key instead of password

            sql_port = int(os.getenv('SQL_PORT', 1433))
            local_port = int(os.getenv('LOCAL_TUNNEL_PORT', 9999))

            # Create SSH tunnel
            if ssh_key_file and os.path.exists(ssh_key_file):
                # Use SSH key authentication
                self.ssh_tunnel = SSHTunnelForwarder(
                    (ssh_host, ssh_port),
                    ssh_username=ssh_username,
                    ssh_pkey=ssh_key_file,
                    remote_bind_address=('localhost', sql_port),
                    local_bind_address=('localhost', local_port),
                    set_keepalive=30
                )
            else:
                # Use password authentication
                self.ssh_tunnel = SSHTunnelForwarder(
                    (ssh_host, ssh_port),
                    ssh_username=ssh_username,
                    ssh_password=ssh_password,
                    remote_bind_address=('localhost', sql_port),
                    local_bind_address=('localhost', local_port),
                    set_keepalive=30
                )

            # Start the tunnel
            self.ssh_tunnel.start()
            self.logger.info(f"SSH tunnel established: localhost:{local_port} -> {ssh_host}:{sql_port}")

            # Give tunnel a moment to establish
            time.sleep(2)

        except Exception as e:
            raise Exception(f"Failed to create SSH tunnel: {str(e)}")

    def _create_engines(self):
        """Create SQLAlchemy engines for both databases"""
        try:
            # On-premises engine (through SSH tunnel)
            self.onprem_engine = self._create_onprem_engine()
            self.logger.debug("On-premises engine created successfully")

            # Azure engine (direct connection)
            self.azure_engine = self._create_azure_engine()
            self.logger.debug("Azure engine created successfully")

        except Exception as e:
            raise Exception(f"Failed to create database engines: {str(e)}")

    def _create_onprem_engine(self):
        """Create on-premises SQL Server engine through SSH tunnel"""
        # Connect through SSH tunnel using localhost
        server = 'localhost'
        port = int(os.getenv('LOCAL_TUNNEL_PORT', 9999))
        database = os.getenv('DATABASE_NAME')
        driver = os.getenv('SQL_DRIVER', 'ODBC Driver 17 for SQL Server')

        return self._create_sql_server_engine(
            driver=driver,
            server=server,
            database=database,
            port=port,
            pool_size=5,  # Smaller pool for SSH tunnel
            max_overflow=10,
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
        Create SQL Server engine with connection pooling

        Args:
            driver: ODBC driver name
            server: Server name/address
            database: Database name
            username: Username (None for Windows auth)
            password: Password (None for Windows auth)
            port: Port number
            pool_size: Number of connections to maintain
            max_overflow: Additional connections allowed
            pool_timeout: Timeout for getting connection from pool
            pool_recycle: Recycle connections after this many seconds
            pool_pre_ping: Test connections before use
            retries: Number of retry attempts
            retry_delay: Delay between retries
            db_type: 'azure' or 'onprem' for connection string formatting

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
        elif db_type == 'onprem':
            # For SSH tunnel, use Windows authentication (current user)
            # This works because we're connecting to localhost through the tunnel
            connection_url = (
                f"mssql+pyodbc://@{server}:{port}/{database}?driver={urllib.parse.quote_plus(driver)}"
                f"&Trusted_Connection=yes&TrustServerCertificate=yes&Connection Timeout={pool_timeout}"
                f"&Encrypt=no"
            )
        else:
            raise ValueError("Unsupported database type. Please use 'azure' or 'onprem'.")

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

        # Test on-premises (through SSH tunnel)
        try:
            with self.onprem_engine.connect() as conn:
                result = conn.execute(text("SELECT @@VERSION, DB_NAME(), @@SERVERNAME"))
                row = result.fetchone()
                database = row[1]
                server = row[2]
                version_info = row[0][:100] + "..." if len(row[0]) > 100 else row[0]

                print(f"✓ On-premises (SSH): {server}/{database}")
                self.logger.debug(f"On-premises connected via SSH: {server}/{database} - {version_info}")

        except Exception as e:
            print(f"✗ On-premises connection failed: {str(e)}")
            self.logger.error(f"On-premises connection failed: {str(e)}")
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
            print(f"✗ Azure connection failed: {str(e)}")
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
        """Close all database connections and SSH tunnel"""
        try:
            if self.onprem_engine:
                self.onprem_engine.dispose()
                self.logger.debug("On-premises engine disposed")

            if self.azure_engine:
                self.azure_engine.dispose()
                self.logger.debug("Azure engine disposed")

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
        return {
            'ssh_host': os.getenv('SSH_HOST'),
            'ssh_port': os.getenv('SSH_PORT', '22'),
            'ssh_username': os.getenv('SSH_USERNAME'),
            'local_tunnel_port': os.getenv('LOCAL_TUNNEL_PORT', '9999'),
            'onprem_database': os.getenv('DATABASE_NAME'),
            'azure_server': os.getenv('AZURE_SERVER'),
            'azure_database': os.getenv('AZURE_DATABASE_SLDB'),
            'azure_port': os.getenv('AZURE_PORT', '1433'),
            'tunnel_status': 'Active' if self.ssh_tunnel and self.ssh_tunnel.is_active else 'Inactive'
        }


def create_ssh_connection_manager() -> SSHPooledConnectionManager:
    """
    Factory function to create an SSHPooledConnectionManager instance

    Returns:
        SSHPooledConnectionManager: Configured connection manager with SSH tunnel
    """
    return SSHPooledConnectionManager()


# For testing
if __name__ == "__main__":
    # Test the SSH connection manager
    try:
        manager = create_ssh_connection_manager()

        print("SSH Connection Manager initialized successfully!")
        print("\nConnection Info:")
        info = manager.get_connection_info()
        for key, value in info.items():
            if 'password' not in key.lower():
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