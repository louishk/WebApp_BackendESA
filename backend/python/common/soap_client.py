"""
SOAP Client Module

Base SOAP client for making SOAP API calls with common authentication handling.
Supports retry logic, XML parsing, and namespace stripping.

Key Features:
- Automatic authentication injection (sCorpCode, sCorpUserName with :::APIKEY format, sCorpPassword)
- Retry logic with exponential backoff
- SOAP fault detection and error handling
- XML response parsing to Python dict/list
- Namespace stripping

Example Usage:
    from Scripts.common import SOAPClient

    soap_client = SOAPClient(
        base_url="https://api.example.com/Service.asmx",
        corp_code="C234",
        api_key="CODIGO3E57HV9VJER9WY",  # Will be formatted as :::CODIGO3E57HV9VJER9WY
        corp_password="password"
    )

    results = soap_client.call(
        operation="GetData",
        parameters={"sLocationCode": "L001"},  # Auth auto-injected
        soap_action="http://tempuri.org/GetData",
        namespace="http://tempuri.org/"
    )
"""

import requests
import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Optional
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common.outbound_stats import track_outbound_api


class SOAPFaultError(Exception):
    """Exception raised when a SOAP fault is encountered."""
    pass


class SOAPClient:
    """
    Base SOAP client with common authentication handling.

    Automatically injects common auth fields (sCorpCode, sCorpUserName, sCorpPassword)
    into all SOAP requests.
    """

    def __init__(
        self,
        base_url: str,
        corp_code: str,
        corp_user: str,
        api_key: str,
        corp_password: str,
        timeout: int = 60,
        retries: int = 3
    ):
        """
        Initialize SOAP client with common authentication credentials.

        Args:
            base_url: SOAP service base URL (e.g., https://api.example.com/Service.asmx)
            corp_code: Corporation code (e.g., "C234")
            corp_user: Corporate username (e.g., "louis")
            api_key: API key WITHOUT ::: prefix (e.g., "CODIGO3E57HV9VJER9WY")
            corp_password: Corporation password
            timeout: Request timeout in seconds (default: 60 for large payloads)
            retries: Number of retry attempts (default: 3)
        """
        self.base_url = base_url
        self.corp_code = corp_code
        self.corp_username = f"{corp_user}:::{api_key}"  # Format: louis:::CODIGO3E57HV9VJER9WY
        self.corp_password = corp_password
        self.timeout = timeout
        self.retries = retries

        # Create HTTP session with connection pooling
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        Create HTTP session with connection pooling and retry logic.

        Returns:
            Configured requests.Session
        """
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
            backoff_factor=1  # Exponential backoff: 1s, 2s, 4s
        )

        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    @track_outbound_api(
        service_name="soap",
        endpoint_extractor=lambda args, kwargs: kwargs.get('operation', args[1] if len(args) > 1 else 'unknown')
    )
    def call(
        self,
        operation: str,
        parameters: Dict[str, Any],
        soap_action: str,
        namespace: str,
        result_tag: Optional[str] = None,
        strip_namespaces: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Make SOAP call with automatic authentication injection.

        Common auth fields are automatically injected:
        - sCorpCode
        - sCorpUserName (formatted as corp_user:::api_key, e.g., louis:::CODIGO3E57HV9VJER9WY)
        - sCorpPassword

        Args:
            operation: SOAP operation name (e.g., "RentRoll")
            parameters: Report-specific parameters (auth will be auto-injected)
            soap_action: SOAP action header value
            namespace: XML namespace for the operation
            result_tag: Tag name to extract from response (e.g., "RentRoll")
            strip_namespaces: Whether to remove XML namespaces (default: True)

        Returns:
            List of dictionaries containing parsed XML data

        Raises:
            SOAPFaultError: If SOAP fault is encountered
            requests.exceptions.RequestException: If HTTP request fails
        """
        # Merge common auth with report-specific parameters
        full_params = {
            "sCorpCode": self.corp_code,
            "sCorpUserName": self.corp_username,
            "sCorpPassword": self.corp_password,
            **parameters  # Report-specific params
        }

        # Build SOAP envelope
        envelope = self._build_soap_envelope(operation, full_params, namespace)

        # Set SOAP headers
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": soap_action
        }

        # Make SOAP request
        response = self.session.post(
            self.base_url,
            data=envelope.encode('utf-8'),
            headers=headers,
            timeout=self.timeout
        )

        # Check HTTP status
        response.raise_for_status()

        # Parse XML response
        return self._parse_soap_response(
            response.content,
            result_tag=result_tag,
            strip_namespaces=strip_namespaces
        )

    def _build_soap_envelope(
        self,
        operation: str,
        parameters: Dict[str, Any],
        namespace: str
    ) -> str:
        """
        Build SOAP 1.1 envelope with all parameters.

        Args:
            operation: SOAP operation name
            parameters: All parameters (including auth)
            namespace: XML namespace for the operation

        Returns:
            SOAP XML envelope as string
        """
        # Build parameter XML from ALL parameters
        param_xml = ""
        for key, value in parameters.items():
            # Escape XML special characters
            value_escaped = str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            param_xml += f"      <{key}>{value_escaped}</{key}>\n"

        envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <{operation} xmlns="{namespace}">
{param_xml}    </{operation}>
  </soap:Body>
</soap:Envelope>"""

        return envelope

    def _parse_soap_response(
        self,
        response_xml: bytes,
        result_tag: Optional[str] = None,
        strip_namespaces: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Parse SOAP XML response to Python list of dicts.

        Args:
            response_xml: SOAP response XML bytes
            result_tag: Tag name to extract (e.g., "RentRoll")
            strip_namespaces: Whether to remove XML namespaces

        Returns:
            List of dictionaries containing parsed data

        Raises:
            SOAPFaultError: If SOAP fault is present
        """
        root = ET.fromstring(response_xml)

        # Check for SOAP fault
        self._check_soap_fault(root)

        # Strip namespaces if requested
        if strip_namespaces:
            self._strip_namespaces(root)

        # Find all elements with result_tag
        if result_tag:
            elements = root.findall(f".//{result_tag}")
        else:
            # Find all elements in Body
            elements = root.findall(".//Body/*")

        # Convert to list of dicts
        # Handle both child elements and attributes (some APIs return data as attributes)
        results = []
        for elem in elements:
            row_data = {}

            # First, check for child elements
            has_children = False
            for child in elem:
                # Skip schema elements and diffgram metadata
                if 'schema' in child.tag.lower() or 'diffgram' in child.tag.lower():
                    continue
                has_children = True
                row_data[child.tag] = child.text

            # If no child elements, try to get data from attributes
            # (Some APIs like ChargesAllByLedgerID return data as XML attributes)
            if not has_children and elem.attrib:
                for key, value in elem.attrib.items():
                    # Skip namespace/diffgram attributes
                    if ':' in key or key.startswith('{'):
                        continue
                    row_data[key] = value

            # Only add if we got some data
            if row_data:
                results.append(row_data)

        return results

    @track_outbound_api(service_name="soap", endpoint_extractor=lambda args, kwargs: "ManagementSummary")
    def call_management_summary(
        self,
        parameters: Dict[str, Any],
        soap_action: str,
        namespace: str,
        table_names: List[str]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Specialized call for ManagementSummary endpoint that returns multiple tables.

        Makes a single API call and extracts data for all specified tables.

        Args:
            parameters: Report-specific parameters (auth will be auto-injected)
            soap_action: SOAP action header value
            namespace: XML namespace for the operation
            table_names: List of table names to extract (e.g., ['Deposits', 'Receipts', ...])

        Returns:
            Dictionary mapping table names to list of records
        """
        # Merge common auth with report-specific parameters
        full_params = {
            "sCorpCode": self.corp_code,
            "sCorpUserName": self.corp_username,
            "sCorpPassword": self.corp_password,
            **parameters
        }

        # Build SOAP envelope
        envelope = self._build_soap_envelope('ManagementSummary', full_params, namespace)

        # Set SOAP headers
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": soap_action
        }

        # Make SOAP request
        response = self.session.post(
            self.base_url,
            data=envelope.encode('utf-8'),
            headers=headers,
            timeout=self.timeout
        )

        response.raise_for_status()

        # Parse XML response
        root = ET.fromstring(response.content)
        self._check_soap_fault(root)
        self._strip_namespaces(root)

        # Extract each table
        result = {}
        for table_name in table_names:
            elements = root.findall(f".//{table_name}")
            table_records = []

            for elem in elements:
                row_data = {}
                for child in elem:
                    if 'schema' not in child.tag.lower() and 'diffgram' not in child.tag.lower():
                        row_data[child.tag] = child.text
                if row_data:
                    table_records.append(row_data)

            result[table_name] = table_records

        return result

    def _strip_namespaces(self, root: ET.Element) -> None:
        """
        Remove XML namespaces from all elements in-place.

        Args:
            root: XML root element
        """
        for elem in root.iter():
            if "}" in elem.tag:
                elem.tag = elem.tag.split("}", 1)[1]

    def _check_soap_fault(self, root: ET.Element) -> None:
        """
        Check for SOAP fault and raise exception if found.

        Args:
            root: XML root element

        Raises:
            SOAPFaultError: If SOAP fault is present
        """
        fault = root.find(".//{http://schemas.xmlsoap.org/soap/envelope/}Fault")

        if fault is not None:
            # Try to find faultcode and faultstring with and without namespace
            faultcode = fault.findtext("{http://schemas.xmlsoap.org/soap/envelope/}faultcode")
            if not faultcode:
                faultcode = fault.findtext("faultcode", "Unknown")

            faultstring = fault.findtext("{http://schemas.xmlsoap.org/soap/envelope/}faultstring")
            if not faultstring:
                faultstring = fault.findtext("faultstring", "Unknown error")

            raise SOAPFaultError(f"SOAP Fault [{faultcode}]: {faultstring}")

    def close(self):
        """Close the HTTP session."""
        if self.session:
            self.session.close()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
