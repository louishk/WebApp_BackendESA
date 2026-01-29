"""
HTTP client for API data retrieval with connection pooling and retry logic.
Optimized for large payloads (1+ MB).
"""

import logging
from typing import Dict, List, Optional, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logger = logging.getLogger(__name__)


class HTTPClient:
    """
    HTTP client with connection pooling and automatic retry logic.

    Features:
    - Connection pooling for better performance
    - Automatic retry with exponential backoff
    - Configurable timeouts (increased for large payloads)
    - Support for all HTTP methods (GET, POST, PUT, DELETE, PATCH)
    """

    def __init__(
        self,
        pool_connections: int = 10,
        pool_maxsize: int = 20,
        total_retries: int = 3,
        backoff_factor: float = 1.0,
        status_forcelist: Optional[List[int]] = None,
        allowed_methods: Optional[List[str]] = None,
        default_timeout: int = 30  # Increased for large payloads (1+ MB)
    ):
        """
        Initialize HTTP client with connection pooling and retry logic.

        Args:
            pool_connections: Number of connection pools to cache
            pool_maxsize: Maximum number of connections per pool
            total_retries: Total number of retry attempts
            backoff_factor: Backoff factor for retries (delay = backoff_factor * (2 ** retry_count))
            status_forcelist: HTTP status codes to retry on
            allowed_methods: HTTP methods to retry (default: all standard methods)
            default_timeout: Default timeout in seconds (30s for large payloads)
        """
        self.default_timeout = default_timeout
        self.session = self._create_session(
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
            total_retries=total_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist or [429, 500, 502, 503, 504],
            allowed_methods=allowed_methods or ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]
        )

    def _create_session(
        self,
        pool_connections: int,
        pool_maxsize: int,
        total_retries: int,
        backoff_factor: float,
        status_forcelist: List[int],
        allowed_methods: List[str]
    ) -> requests.Session:
        """
        Create requests session with connection pooling and retry configuration.

        Args:
            pool_connections: Number of connection pools
            pool_maxsize: Maximum connections per pool
            total_retries: Number of retry attempts
            backoff_factor: Backoff factor for retries
            status_forcelist: Status codes to retry on
            allowed_methods: HTTP methods to retry

        Returns:
            requests.Session: Configured session with retry logic
        """
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=total_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            allowed_methods=allowed_methods,
            raise_on_status=False  # Don't raise exception, let caller handle
        )

        # Create adapter with retry strategy and connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
            pool_block=True  # Block when pool is full instead of creating new connections
        )

        # Mount adapter for both HTTP and HTTPS
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        logger.info(
            f"HTTP client initialized: pool_connections={pool_connections}, "
            f"pool_maxsize={pool_maxsize}, retries={total_retries}, timeout={self.default_timeout}s"
        )

        return session

    def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        json: Optional[Any] = None,
        timeout: Optional[int] = None,
        **kwargs
    ) -> requests.Response:
        """
        Make HTTP request with retry logic.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, PATCH, etc.)
            url: Request URL
            headers: Optional request headers
            params: Optional URL parameters
            data: Optional request body (form data)
            json: Optional JSON request body
            timeout: Optional timeout in seconds (uses default if not provided)
            **kwargs: Additional arguments passed to requests

        Returns:
            requests.Response: HTTP response

        Raises:
            requests.exceptions.RequestException: On request failure
        """
        timeout = timeout or self.default_timeout
        headers = headers or {}

        try:
            response = self.session.request(
                method=method.upper(),
                url=url,
                headers=headers,
                params=params,
                data=data,
                json=json,
                timeout=timeout,
                **kwargs
            )

            logger.debug(
                f"{method.upper()} {url} -> {response.status_code} "
                f"(size: {len(response.content)} bytes)"
            )

            # Raise for 4xx/5xx status codes
            response.raise_for_status()

            return response

        except requests.exceptions.Timeout as e:
            logger.error(f"Request timeout after {timeout}s: {method.upper()} {url}")
            raise

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {method.upper()} {url} - {e}")
            raise

    def get(self, url: str, **kwargs) -> requests.Response:
        """
        Make GET request.

        Args:
            url: Request URL
            **kwargs: Additional arguments passed to request()

        Returns:
            requests.Response: HTTP response
        """
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> requests.Response:
        """
        Make POST request.

        Args:
            url: Request URL
            **kwargs: Additional arguments passed to request()

        Returns:
            requests.Response: HTTP response
        """
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs) -> requests.Response:
        """
        Make PUT request.

        Args:
            url: Request URL
            **kwargs: Additional arguments passed to request()

        Returns:
            requests.Response: HTTP response
        """
        return self.request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs) -> requests.Response:
        """
        Make DELETE request.

        Args:
            url: Request URL
            **kwargs: Additional arguments passed to request()

        Returns:
            requests.Response: HTTP response
        """
        return self.request("DELETE", url, **kwargs)

    def patch(self, url: str, **kwargs) -> requests.Response:
        """
        Make PATCH request.

        Args:
            url: Request URL
            **kwargs: Additional arguments passed to request()

        Returns:
            requests.Response: HTTP response
        """
        return self.request("PATCH", url, **kwargs)

    def close(self):
        """Close the HTTP session and release connections"""
        if self.session:
            self.session.close()
            logger.info("HTTP client session closed")
