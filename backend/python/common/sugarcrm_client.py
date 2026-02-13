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

import requests

from common.outbound_stats import track_outbound_api

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
        Create client from unified config system (apis.yaml + vault).

        Config loaded from apis.yaml:
        - sugarcrm.base_url: SugarCRM instance URL
        - sugarcrm.username: Username
        - sugarcrm.password_vault: Vault key for password (auto-resolved)
        - sugarcrm.client_id: OAuth client ID (default: "sugar")
        - sugarcrm.client_secret_vault: Vault key for client secret (auto-resolved)
        - sugarcrm.platform: Platform identifier (default: "mobile")
        - sugarcrm.api_version: API version (default: "v11")
        - sugarcrm.timeout: Request timeout (default: 60)
        """
        from common.config_loader import get_config

        app_config = get_config()
        sugar_cfg = app_config.apis.sugarcrm

        if not sugar_cfg or not sugar_cfg.base_url:
            raise ValueError("SugarCRM configuration not found. Check apis.yaml.")

        return cls(
            base_url=sugar_cfg.base_url,
            username=sugar_cfg.username,
            password=sugar_cfg.password_vault,  # Auto-resolved from vault
            client_id=getattr(sugar_cfg, 'client_id', 'sugar'),
            client_secret=sugar_cfg.client_secret_vault or '',  # Auto-resolved from vault
            platform=getattr(sugar_cfg, 'platform', 'mobile'),
            api_version=getattr(sugar_cfg, 'api_version', cls.DEFAULT_API_VERSION),
            timeout=getattr(sugar_cfg, 'timeout', cls.DEFAULT_TIMEOUT),
        )

    # =========================================================================
    # Authentication
    # =========================================================================

    @track_outbound_api(service_name="sugarcrm", endpoint_extractor=lambda args, kwargs: "oauth2/token")
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

    @track_outbound_api(
        service_name="sugarcrm",
        endpoint_extractor=lambda args, kwargs: kwargs.get('endpoint', args[2] if len(args) > 2 else 'unknown')
    )
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

    def put(self, endpoint: str, data: Optional[Dict] = None) -> Tuple[Optional[Any], Optional[str]]:
        """Make a PUT request."""
        return self._request('PUT', endpoint, data=data)

    def delete(self, endpoint: str) -> Tuple[Optional[Any], Optional[str]]:
        """Make a DELETE request."""
        return self._request('DELETE', endpoint)

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
    # Record CRUD Operations
    # =========================================================================

    def get_record(
        self,
        module: str,
        record_id: str,
        fields: Optional[List[str]] = None
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Get a single record by ID.

        Args:
            module: Module name (e.g., 'Leads')
            record_id: SugarCRM record UUID
            fields: List of fields to return (None = all fields)

        Returns:
            Tuple of (record_dict, error_message)
        """
        endpoint = f"{module}/{record_id}"
        params = {}
        if fields:
            params['fields'] = ','.join(fields)
        return self.get(endpoint, params=params if params else None)

    def create_record(
        self,
        module: str,
        fields: Dict[str, Any]
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Create a new record in a SugarCRM module.

        Custom fields (ending in '_c') are included as regular fields.

        Args:
            module: Module name (e.g., 'Leads')
            fields: Dict of field_name -> value (including custom fields like 'my_field_c')

        Returns:
            Tuple of (created_record, error_message)
            created_record contains the full record with its new 'id'

        Example:
            record, error = client.create_record('Leads', {
                'first_name': 'John',
                'last_name': 'Doe',
                'email': [{'email_address': 'john@example.com', 'primary_address': True}],
                'phone_mobile': '555-0100',
                'status': 'New',
                'lead_source': 'Web Site',
                'custom_score_c': '85',
            })
        """
        return self.post(module, data=fields)

    def update_record(
        self,
        module: str,
        record_id: str,
        fields: Dict[str, Any]
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Update an existing record in a SugarCRM module.

        Only the fields provided will be updated; other fields remain unchanged.
        Custom fields (ending in '_c') are included as regular fields.

        Args:
            module: Module name (e.g., 'Leads')
            record_id: SugarCRM record UUID to update
            fields: Dict of field_name -> value to update

        Returns:
            Tuple of (updated_record, error_message)

        Example:
            record, error = client.update_record('Leads', 'abc-123-uuid', {
                'status': 'Converted',
                'custom_score_c': '95',
            })
        """
        endpoint = f"{module}/{record_id}"
        return self.put(endpoint, data=fields)

    def upsert_record(
        self,
        module: str,
        fields: Dict[str, Any],
        lookup_field: str = 'id',
        lookup_value: Optional[str] = None
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Create or update a record. If a record matching the lookup is found, update it;
        otherwise create a new one.

        Args:
            module: Module name (e.g., 'Leads')
            fields: Dict of field_name -> value
            lookup_field: Field to match on for finding existing records (default: 'id')
            lookup_value: Value to look up. If None and lookup_field is 'id',
                          uses fields.get('id'). For other fields, must be provided.

        Returns:
            Tuple of (record, error_message)
            The record dict includes '_action': 'created' or '_action': 'updated'
        """
        # Determine lookup value
        if lookup_value is None:
            if lookup_field == 'id':
                lookup_value = fields.get('id')
            else:
                lookup_value = fields.get(lookup_field)

        # Try to find existing record
        if lookup_value:
            if lookup_field == 'id':
                existing, error = self.get_record(module, lookup_value)
                if existing and not error:
                    # Record exists - update it
                    update_fields = {k: v for k, v in fields.items() if k != 'id'}
                    result, error = self.update_record(module, lookup_value, update_fields)
                    if result:
                        result['_action'] = 'updated'
                    return result, error
            else:
                # Search by custom field
                filter_expr = [{lookup_field: {'$equals': lookup_value}}]
                search_result, error = self.filter_records(
                    module=module,
                    filter_expr=filter_expr,
                    max_num=1
                )
                if search_result and search_result.get('records'):
                    existing_id = search_result['records'][0]['id']
                    update_fields = {k: v for k, v in fields.items() if k != 'id'}
                    result, error = self.update_record(module, existing_id, update_fields)
                    if result:
                        result['_action'] = 'updated'
                    return result, error

        # No existing record found - create new
        create_fields = {k: v for k, v in fields.items() if k != 'id'}
        result, error = self.create_record(module, create_fields)
        if result:
            result['_action'] = 'created'
        return result, error

    def delete_record(
        self,
        module: str,
        record_id: str
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """
        Delete a record from a SugarCRM module.

        Args:
            module: Module name (e.g., 'Leads')
            record_id: SugarCRM record UUID to delete

        Returns:
            Tuple of (response, error_message)
        """
        endpoint = f"{module}/{record_id}"
        return self.delete(endpoint)

    def find_leads_by_email(
        self,
        email: str,
        fields: Optional[List[str]] = None
    ) -> Tuple[Optional[List[Dict]], Optional[str]]:
        """
        Find leads by email address.

        Useful for deduplication before creating new leads.

        Args:
            email: Email address to search for
            fields: Fields to return (None = all)

        Returns:
            Tuple of (list_of_matching_records, error_message)
        """
        filter_expr = [{
            '$or': [
                {'email': {'$equals': email}},
                {'webtolead_email1': {'$equals': email}},
            ]
        }]
        result, error = self.filter_records(
            module='Leads',
            filter_expr=filter_expr,
            fields=fields,
            max_num=10
        )
        if error:
            return None, error
        return result.get('records', []), None

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
