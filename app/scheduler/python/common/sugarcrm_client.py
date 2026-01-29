"""
SugarCRM REST API Client for data pipelines.

Production-ready client with OAuth2 authentication, pagination,
and batch fetching optimized for large data volumes (400K+ records).

Example Usage:
    from common.sugarcrm_client import SugarCRMClient

    client = SugarCRMClient.from_env()
    if client.authenticate():
        # Fetch all Leads in batches
        for batch in client.fetch_all_records('Leads'):
            process_batch(batch)

        client.logout()
"""

import logging
from typing import Optional, Dict, Any, List, Generator, Tuple
from datetime import datetime, timedelta
from decouple import config as env_config

import requests

logger = logging.getLogger(__name__)


class SugarCRMClient:
    """
    SugarCRM REST API client with OAuth2 authentication.

    Features:
    - OAuth2 password grant authentication
    - Automatic token refresh
    - Paginated batch fetching (for 400K+ records)
    - Metadata retrieval for dynamic schema generation
    - Connection pooling via requests.Session
    """

    DEFAULT_API_VERSION = "v11"
    DEFAULT_BATCH_SIZE = 200  # SugarCRM API optimal batch size
    DEFAULT_TIMEOUT = 120  # Increased for large payloads
    MAX_RETRIES = 3  # Retry count for failed requests

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        client_id: str = "sugar",
        client_secret: str = "",
        platform: str = "mobile",
        api_version: str = DEFAULT_API_VERSION,
        timeout: int = DEFAULT_TIMEOUT
    ):
        """
        Initialize SugarCRM client.

        Args:
            base_url: SugarCRM instance URL (e.g., https://extraspace.sugarondemand.com)
            username: Username for authentication
            password: Password for authentication
            client_id: OAuth2 client ID (default: "sugar")
            client_secret: OAuth2 client secret (default: "")
            platform: Platform identifier (default: "mobile")
            api_version: REST API version (default: "v11")
            timeout: Request timeout in seconds (default: 60)
        """
        self.base_url = f"{base_url.rstrip('/')}/rest/{api_version}"
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret
        self.platform = platform
        self.timeout = timeout

        # Session management
        self.session = requests.Session()
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_expiry: Optional[datetime] = None

        logger.debug(f"SugarCRM client initialized: {self.base_url}")

    @classmethod
    def from_env(cls) -> 'SugarCRMClient':
        """
        Create client from environment variables.

        Required env vars:
        - SUGARCRM_BASE_URL or SUGARCRM_URL
        - SUGARCRM_USERNAME
        - SUGARCRM_PASSWORD

        Optional env vars:
        - SUGARCRM_CLIENT_ID (default: "sugar")
        - SUGARCRM_CLIENT_SECRET (default: "")
        - SUGARCRM_PLATFORM (default: "mobile")
        - SUGARCRM_API_VERSION (default: "v11")
        - SUGARCRM_TIMEOUT (default: 60)
        """
        base_url = env_config('SUGARCRM_BASE_URL', default=None)
        if not base_url:
            base_url = env_config('SUGARCRM_URL')

        return cls(
            base_url=base_url,
            username=env_config('SUGARCRM_USERNAME'),
            password=env_config('SUGARCRM_PASSWORD'),
            client_id=env_config('SUGARCRM_CLIENT_ID', default='sugar'),
            client_secret=env_config('SUGARCRM_CLIENT_SECRET', default=''),
            platform=env_config('SUGARCRM_PLATFORM', default='mobile'),
            api_version=env_config('SUGARCRM_API_VERSION', default=cls.DEFAULT_API_VERSION),
            timeout=env_config('SUGARCRM_TIMEOUT', default=cls.DEFAULT_TIMEOUT, cast=int),
        )

    # =========================================================================
    # Authentication
    # =========================================================================

    def authenticate(self) -> bool:
        """
        Authenticate with SugarCRM using OAuth2 password grant.

        Returns:
            bool: True if successful, False otherwise
        """
        auth_url = f"{self.base_url}/oauth2/token"

        payload = {
            "grant_type": "password",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "username": self.username,
            "password": self.password,
            "platform": self.platform
        }

        try:
            logger.info(f"Authenticating to SugarCRM: {self.base_url}")
            response = self.session.post(auth_url, json=payload, timeout=self.timeout)

            if response.status_code == 200:
                data = response.json()
                self.access_token = data.get('access_token')
                self.refresh_token = data.get('refresh_token')

                # Calculate token expiry (usually 1 hour)
                expires_in = data.get('expires_in', 3600)
                self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 60)

                self.session.headers.update({
                    'OAuth-Token': self.access_token,
                    'Content-Type': 'application/json'
                })

                logger.info("SugarCRM authentication successful")
                return True
            else:
                logger.error(f"Authentication failed: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return False

    def refresh_authentication(self) -> bool:
        """Refresh the access token using the refresh token."""
        if not self.refresh_token:
            return self.authenticate()

        auth_url = f"{self.base_url}/oauth2/token"

        payload = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "refresh_token": self.refresh_token,
            "platform": self.platform
        }

        try:
            response = self.session.post(auth_url, json=payload, timeout=self.timeout)

            if response.status_code == 200:
                data = response.json()
                self.access_token = data.get('access_token')
                self.refresh_token = data.get('refresh_token')

                expires_in = data.get('expires_in', 3600)
                self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 60)

                self.session.headers.update({'OAuth-Token': self.access_token})
                logger.debug("Token refreshed successfully")
                return True
            else:
                logger.warning("Token refresh failed, re-authenticating")
                return self.authenticate()

        except Exception as e:
            logger.warning(f"Token refresh error: {e}, re-authenticating")
            return self.authenticate()

    def _ensure_authenticated(self) -> bool:
        """Ensure we have a valid access token."""
        if not self.access_token:
            return self.authenticate()

        # Check if token is about to expire
        if self.token_expiry and datetime.now() >= self.token_expiry:
            return self.refresh_authentication()

        return True

    def logout(self) -> bool:
        """Logout and invalidate the current token."""
        if not self.access_token:
            return True

        logout_url = f"{self.base_url}/oauth2/logout"

        try:
            response = self.session.post(logout_url, timeout=self.timeout)
            self.access_token = None
            self.refresh_token = None
            self.token_expiry = None
            logger.info("SugarCRM logout successful")
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Logout error: {e}")
            return False

    # =========================================================================
    # Core Request Methods
    # =========================================================================

    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        retry_on_401: bool = True
    ) -> Tuple[Optional[Any], Optional[str]]:
        """
        Make an API request with automatic token handling.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint path (without base URL)
            data: Request body data
            params: Query parameters
            retry_on_401: Retry once on 401 Unauthorized

        Returns:
            Tuple of (response_data, error_message)
        """
        if not self._ensure_authenticated():
            return None, "Authentication failed"

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            response = self.session.request(
                method=method.upper(),
                url=url,
                json=data if method.upper() in ['POST', 'PUT'] else None,
                params=params,
                timeout=self.timeout
            )

            if response.status_code in [200, 201]:
                return response.json(), None
            elif response.status_code == 401 and retry_on_401:
                logger.debug("Got 401, refreshing token and retrying")
                if self.refresh_authentication():
                    return self._request(method, endpoint, data, params, retry_on_401=False)
                return None, "Authentication failed after retry"
            else:
                error_msg = f"API Error {response.status_code}: {response.text[:500]}"
                logger.error(error_msg)
                return None, error_msg

        except requests.exceptions.Timeout:
            return None, f"Request timeout after {self.timeout}s"
        except requests.exceptions.ConnectionError as e:
            return None, f"Connection error: {e}"
        except Exception as e:
            return None, f"Request error: {e}"

    def get(self, endpoint: str, params: Optional[Dict] = None) -> Tuple[Optional[Any], Optional[str]]:
        """Make a GET request."""
        return self._request('GET', endpoint, params=params)

    def post(self, endpoint: str, data: Optional[Dict] = None, params: Optional[Dict] = None) -> Tuple[Optional[Any], Optional[str]]:
        """Make a POST request."""
        if params:
            # POST with both body and query params
            if not self._ensure_authenticated():
                return None, "Authentication failed"
            url = f"{self.base_url}/{endpoint.lstrip('/')}"
            try:
                response = self.session.post(url, json=data, params=params, timeout=self.timeout)
                if response.status_code in [200, 201]:
                    return response.json(), None
                else:
                    return None, f"API Error {response.status_code}: {response.text[:500]}"
            except Exception as e:
                return None, str(e)
        return self._request('POST', endpoint, data=data)

    # =========================================================================
    # Module Data Operations
    # =========================================================================

    def filter_records(
        self,
        module: str,
        filter_expr: Optional[List[Dict]] = None,
        fields: Optional[List[str]] = None,
        max_num: int = 200,
        offset: int = 0,
        order_by: Optional[str] = None,
        deleted: bool = False
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Filter records using POST /:module/filter endpoint.

        Args:
            module: Module name (e.g., 'Accounts', 'Contacts', 'Leads')
            filter_expr: Filter expression list
            fields: List of fields to return (None = all fields)
            max_num: Maximum number of records (default: 200)
            offset: Records to skip for pagination
            order_by: Sort order (e.g., 'date_modified:DESC')
            deleted: Include deleted records

        Returns:
            Tuple of (response_data, error_message)
            response_data contains: {'records': [...], 'next_offset': int}
        """
        endpoint = f"{module}/filter"
        params = {
            'max_num': max_num,
            'offset': offset
        }
        if fields:
            params['fields'] = ','.join(fields)
        if order_by:
            params['order_by'] = order_by

        data = {'deleted': deleted}
        if filter_expr:
            data['filter'] = filter_expr

        return self.post(endpoint, data=data, params=params)

    def count_records(
        self,
        module: str,
        filter_expr: Optional[List[Dict]] = None
    ) -> Tuple[Optional[int], Optional[str]]:
        """
        Get count of records matching filter.

        Args:
            module: Module name
            filter_expr: Optional filter expression

        Returns:
            Tuple of (count, error_message)
        """
        endpoint = f"{module}/filter/count"
        data = {}
        if filter_expr:
            data['filter'] = filter_expr

        result, error = self.post(endpoint, data=data)
        if error:
            return None, error

        return result.get('record_count', 0), None

    def fetch_all_records(
        self,
        module: str,
        filter_expr: Optional[List[Dict]] = None,
        fields: Optional[List[str]] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
        order_by: str = 'date_modified:ASC'
    ) -> Generator[List[Dict], None, None]:
        """
        Generator that fetches all records using pagination.

        Memory-efficient: yields batches instead of loading all into memory.

        Args:
            module: Module name
            filter_expr: Optional filter expression
            fields: Fields to return (None = all)
            batch_size: Records per API call (default: 200)
            order_by: Sort order for consistent pagination

        Yields:
            List of records (batch_size at a time)

        Example:
            for batch in client.fetch_all_records('Leads'):
                for record in batch:
                    process(record)
        """
        offset = 0
        total_fetched = 0

        while True:
            result, error = self.filter_records(
                module=module,
                filter_expr=filter_expr,
                fields=fields,
                max_num=batch_size,
                offset=offset,
                order_by=order_by
            )

            if error:
                logger.error(f"Error fetching {module} at offset {offset}: {error}")
                break

            records = result.get('records', [])
            if not records:
                break

            total_fetched += len(records)
            logger.debug(f"Fetched {len(records)} {module} records (total: {total_fetched})")

            yield records

            # Check if there are more records
            next_offset = result.get('next_offset', -1)
            if next_offset == -1 or next_offset <= offset:
                break

            offset = next_offset

        logger.info(f"Finished fetching {module}: {total_fetched} records total")

    # =========================================================================
    # Metadata Operations
    # =========================================================================

    def get_metadata(
        self,
        modules: Optional[List[str]] = None,
        type_filter: Optional[str] = None
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Get metadata for modules.

        Args:
            modules: List of module names to get metadata for
            type_filter: Type filter (e.g., 'modules')

        Returns:
            Tuple of (metadata_dict, error_message)
        """
        params = {}
        if modules:
            params['module_filter'] = ','.join(modules)
        if type_filter:
            params['type_filter'] = type_filter

        return self.get('metadata', params=params)

    def get_module_fields(self, module: str) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Get field definitions for a specific module.

        Args:
            module: Module name

        Returns:
            Tuple of (fields_dict, error_message)
            fields_dict format: {'field_name': {'name': str, 'type': str, ...}, ...}
        """
        result, error = self.get_metadata(modules=[module])
        if error:
            return None, error

        modules_data = result.get('modules', {})
        module_meta = modules_data.get(module, {})
        fields = module_meta.get('fields', {})

        return fields, None

    def get_available_modules(self) -> Tuple[Optional[List[str]], Optional[str]]:
        """
        Get list of available module names.

        Returns:
            Tuple of (module_names_list, error_message)
        """
        result, error = self.get_metadata(type_filter='modules')
        if error:
            return None, error

        modules = result.get('modules', {})
        return list(modules.keys()), None

    # =========================================================================
    # Utility Methods
    # =========================================================================

    def test_connection(self) -> bool:
        """Test connection to SugarCRM."""
        if not self.authenticate():
            return False

        # Quick metadata check
        result, error = self.get_metadata(type_filter='modules')
        if error:
            logger.error(f"Connection test failed: {error}")
            return False

        module_count = len(result.get('modules', {}))
        logger.info(f"Connection test successful: {module_count} modules available")
        return True

    def __repr__(self) -> str:
        """String representation."""
        return f"SugarCRMClient(base_url={self.base_url}, authenticated={bool(self.access_token)})"
