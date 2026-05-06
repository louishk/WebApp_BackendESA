"""
SiteLink pricing client — high-level wrapper around ``common/soap_client.py``
for the four unit-rate-update operation families.

All rate-update endpoints live on the CallCenterWs SOAP service and are gated
by a per-site ``sUsagePassword`` (separate from ``sCorpPassword``).

API selection:
  - ``corp_code == 'CCTST'``  →  vault key ``SITELINK_API_KEY_TEST`` (sandbox)
  - all other corp codes       →  vault key ``SOAP_API_KEY``         (production)

Usage-password vault keys (one per operation family):
  - ``SITELINK_USAGE_PW_STANDARD_RATE``  (gates v1, v2, v3 standard-rate calls)
  - ``SITELINK_USAGE_PW_WEB_RATE``
  - ``SITELINK_USAGE_PW_PUSH_RATE``

These keys must be present in the DB vault (``app_secrets`` table).  The client
raises ``RuntimeError`` at call time if a required key is missing — no write
operation is attempted.

Ret_Code mapping (from RATE_UPDATE_TEST_REPORT.md):
  ┌──────────────────┬──────────────────────────┐
  │  Ret_Code        │  error_kind              │
  ├──────────────────┼──────────────────────────┤
  │  1               │  ok                      │
  │  -1, msg=None    │  bad_usage_password      │
  │  -75             │  facility_disabled       │
  │  -95             │  corp_not_licensed       │
  │  -98             │  auth                    │
  │  anything else   │  unknown                 │
  └──────────────────┴──────────────────────────┘

Live-test instructions
----------------------
Set the environment variable ``RUN_LIVE_SITELINK_TESTS=1`` and run:

    cd backend/python
    PYTHONPATH=. RUN_LIVE_SITELINK_TESTS=1 pytest tests/test_sitelink_pricing_client.py -v -k live

The live suite targets C234/LSETUP unit 106073:
  1. Reads the current standard rate as baseline.
  2. Bumps it by $1 and verifies Ret_Code=1.
  3. Restores the original rate and verifies Ret_Code=1.
  4. Asserts that update_push_rate returns error_kind='corp_not_licensed'.

Requires vault access (DB credentials in ``.env``) and network connectivity
to api.smdservers.net.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Literal, Optional

from common.secrets_vault import vault_config

logger = logging.getLogger(__name__)

# CallCenterWs endpoint details
_CC_BASE_URL = "https://api.smdservers.net/CCWs_3.5/CallCenterWs.asmx"
_CC_NS = "http://tempuri.org/CallCenterWs/CallCenterWs"
_TEST_CORP_CODES = frozenset({'CCTST'})


# ---------------------------------------------------------------------------
# Public dataclasses
# ---------------------------------------------------------------------------

@dataclass
class RateUpdateResult:
    """Normalised result from any unit rate-update call."""
    success: bool
    ret_code: int
    ret_msg: Optional[str]
    unit_ids: list[int]
    error_kind: Literal[
        'ok', 'bad_usage_password', 'corp_not_licensed',
        'facility_disabled', 'auth', 'unknown'
    ]


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class SiteLinkPricingClient:
    """High-level client for SiteLink unit-rate-update SOAP operations.

    Instantiation is cheap — secrets are loaded lazily on first use of each
    operation family.  One client instance per (corp_code, location_code) pair
    is the recommended pattern.
    """

    def __init__(self, corp_code: str, location_code: str) -> None:
        self._corp_code = corp_code
        self._location_code = location_code
        self._api_key: Optional[str] = None
        self._corp_password: Optional[str] = None
        self._usage_pw_std: Optional[str] = None
        self._usage_pw_web: Optional[str] = None
        self._usage_pw_push: Optional[str] = None
        self._soap: Optional[object] = None  # SOAPClient instance, lazy

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def update_standard_rate(
        self,
        unit_ids: list[int],
        std_rate: Decimal,
        tax_inclusive: bool = False,
        version: Literal['v1', 'v2'] = 'v2',
    ) -> RateUpdateResult:
        """Update the standard rate for vacant units.

        Mirrors ``dcStdRate`` into ``dcPushRate`` automatically on the SiteLink
        side — a direct push-rate call is usually unnecessary.

        Args:
            unit_ids:      List of SiteLink unit IDs to update.
            std_rate:      New standard rate (Decimal for precision).
            tax_inclusive: Whether the rate is tax-inclusive (default False).
            version:       'v1' or 'v2' (default 'v2', adds tax-inclusive flag).
        """
        operation = 'UnitStandardRateUpdate' if version == 'v1' else 'UnitStandardRateUpdate_v2'
        params: dict = {
            'sUsagePassword': self._get_usage_pw_std(),
            'sUnitIDsCommaDelimited': _ids_to_str(unit_ids),
            'dcStdRate': str(std_rate),
        }
        if version == 'v2':
            params['iRatesTaxInclusive'] = '1' if tax_inclusive else '0'
        return self._call(operation, params, unit_ids)

    def update_monthly_weekly(
        self,
        unit_ids: list[int],
        monthly: Decimal,
        weekly: Decimal,
        tax_inclusive: bool = False,
    ) -> RateUpdateResult:
        """Update both monthly and weekly standard rates (UnitStandardRateUpdate_v3).

        This is the preferred operation when setting pricing-tool-recommended
        rates, as it sets both monthly and weekly in a single round-trip.
        Also auto-mirrors ``dcStdMonthlyRate`` into ``dcPushRate``.

        Args:
            unit_ids:      List of SiteLink unit IDs to update.
            monthly:       New standard monthly rate.
            weekly:        New standard weekly rate.
            tax_inclusive: Whether the rates are tax-inclusive (default False).
        """
        params = {
            'sUsagePassword': self._get_usage_pw_std(),
            'sUnitIDsCommaDelimited': _ids_to_str(unit_ids),
            'dcStdMonthlyRate': str(monthly),
            'dcStdWeeklyRate': str(weekly),
            'iRatesTaxInclusive': '1' if tax_inclusive else '0',
        }
        return self._call('UnitStandardRateUpdate_v3', params, unit_ids)

    def update_web_rate(
        self,
        unit_ids: list[int],
        web_rate: Decimal,
        tax_inclusive: bool = False,
    ) -> RateUpdateResult:
        """Update the web/online rate for units.

        Args:
            unit_ids:      List of SiteLink unit IDs to update.
            web_rate:      New web rate.
            tax_inclusive: Whether the rate is tax-inclusive (default False).
        """
        params = {
            'sUsagePassword': self._get_usage_pw_web(),
            'sUnitIDsCommaDelimited': _ids_to_str(unit_ids),
            'dcWebRate': str(web_rate),
            'iRatesTaxInclusive': '1' if tax_inclusive else '0',
        }
        return self._call('UnitWebRateUpdate', params, unit_ids)

    def update_push_rate(
        self,
        unit_ids: list[int],
        push_rate: Decimal,
        tax_inclusive: bool = False,
        version: Literal['v1', 'v2'] = 'v2',
    ) -> RateUpdateResult:
        """Update the push rate for units.

        Note: C234 is not licensed for push-rate writes (Ret_Code=-95).
        Standard-rate updates auto-mirror push rates, so this method is rarely
        needed in practice.

        Args:
            unit_ids:      List of SiteLink unit IDs to update.
            push_rate:     New push rate.
            tax_inclusive: Whether the rate is tax-inclusive (default False).
            version:       'v1' or 'v2' (default 'v2').
        """
        operation = 'UnitPushRateUpdate' if version == 'v1' else 'UnitPushRateUpdate_v2'
        params = {
            'sUsagePassword': self._get_usage_pw_push(),
            'sUnitIDsCommaDelimited': _ids_to_str(unit_ids),
            'dcPushRate': str(push_rate),
            'iRatesTaxInclusive': '1' if tax_inclusive else '0',
        }
        return self._call(operation, params, unit_ids)

    # ------------------------------------------------------------------
    # Internal: SOAP dispatch
    # ------------------------------------------------------------------

    def _call(
        self,
        operation: str,
        params: dict,
        unit_ids: list[int],
    ) -> RateUpdateResult:
        """Build the SOAP call, normalise the response, return RateUpdateResult."""
        client = self._get_soap_client()
        soap_action = f"{_CC_NS}/{operation}"
        try:
            raw = client.call(
                operation=operation,
                parameters=params,
                soap_action=soap_action,
                namespace=_CC_NS,
                result_tag='RT',
            )
        except Exception as exc:
            logger.error(
                "sitelink_pricing_client: SOAP call %s failed: %s",
                operation, type(exc).__name__,
            )
            return RateUpdateResult(
                success=False,
                ret_code=0,
                ret_msg=None,
                unit_ids=unit_ids,
                error_kind='unknown',
            )

        return _parse_result(raw, unit_ids)

    # ------------------------------------------------------------------
    # Internal: lazy secret loading
    # ------------------------------------------------------------------

    def _get_api_key(self) -> str:
        if self._api_key is None:
            vault_key = 'SITELINK_API_KEY_TEST' if self._corp_code in _TEST_CORP_CODES else 'SOAP_API_KEY'
            value = vault_config(vault_key)
            if not value:
                raise RuntimeError(
                    f"sitelink_pricing_client: vault key '{vault_key}' is missing or empty. "
                    "Add it via /admin/secrets before calling rate-update operations."
                )
            self._api_key = value
        return self._api_key

    def _get_corp_password(self) -> str:
        if self._corp_password is None:
            value = vault_config('SOAP_CORP_PASSWORD')
            if not value:
                raise RuntimeError(
                    "sitelink_pricing_client: vault key 'SOAP_CORP_PASSWORD' is missing or empty."
                )
            self._corp_password = value
        return self._corp_password

    def _get_usage_pw_std(self) -> str:
        if self._usage_pw_std is None:
            self._usage_pw_std = _require_vault('SITELINK_USAGE_PW_STANDARD_RATE')
        return self._usage_pw_std

    def _get_usage_pw_web(self) -> str:
        if self._usage_pw_web is None:
            self._usage_pw_web = _require_vault('SITELINK_USAGE_PW_WEB_RATE')
        return self._usage_pw_web

    def _get_usage_pw_push(self) -> str:
        if self._usage_pw_push is None:
            self._usage_pw_push = _require_vault('SITELINK_USAGE_PW_PUSH_RATE')
        return self._usage_pw_push

    def _get_soap_client(self):
        """Lazy-init the underlying SOAPClient instance."""
        if self._soap is None:
            from common.soap_client import SOAPClient
            from common.config_loader import get_config
            cfg = get_config()
            corp_user = (cfg.apis.soap.corp_user if cfg.apis and cfg.apis.soap else None) or 'data'
            self._soap = SOAPClient(
                base_url=_CC_BASE_URL,
                corp_code=self._corp_code,
                corp_user=corp_user,
                api_key=self._get_api_key(),
                corp_password=self._get_corp_password(),
            )
        return self._soap


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ids_to_str(unit_ids: list[int]) -> str:
    return ','.join(str(i) for i in unit_ids)


def _parse_result(raw: list[dict], unit_ids: list[int]) -> RateUpdateResult:
    """Normalise the SOAPClient list-of-dicts response into a RateUpdateResult."""
    # SOAPClient returns a list; the rate-update response carries one RT node.
    if not raw:
        logger.warning("sitelink_pricing_client: empty SOAP response for units %s", unit_ids)
        return RateUpdateResult(
            success=False, ret_code=0, ret_msg=None,
            unit_ids=unit_ids, error_kind='unknown',
        )

    row = raw[0]
    # Some parsing paths hand back the RT children directly, others wrap them.
    ret_code_raw = row.get('Ret_Code') or row.get('ret_code')
    ret_msg_raw  = row.get('Ret_Msg')  or row.get('ret_msg')

    try:
        ret_code = int(ret_code_raw)
    except (TypeError, ValueError):
        logger.warning(
            "sitelink_pricing_client: unexpected Ret_Code value %r", ret_code_raw
        )
        return RateUpdateResult(
            success=False, ret_code=0, ret_msg=str(ret_msg_raw),
            unit_ids=unit_ids, error_kind='unknown',
        )

    ret_msg: Optional[str] = str(ret_msg_raw) if ret_msg_raw is not None else None
    error_kind = _classify(ret_code, ret_msg)
    success = error_kind == 'ok'

    if not success:
        logger.warning(
            "sitelink_pricing_client: rate update failed for units %s — "
            "Ret_Code=%d, Ret_Msg=%r, error_kind=%s",
            unit_ids, ret_code, ret_msg, error_kind,
        )

    return RateUpdateResult(
        success=success,
        ret_code=ret_code,
        ret_msg=ret_msg,
        unit_ids=unit_ids,
        error_kind=error_kind,
    )


def _classify(
    ret_code: int,
    ret_msg: Optional[str],
) -> Literal['ok', 'bad_usage_password', 'corp_not_licensed', 'facility_disabled', 'auth', 'unknown']:
    """Map a (Ret_Code, Ret_Msg) pair to a normalised error_kind string."""
    if ret_code == 1:
        return 'ok'
    if ret_code == -1 and ret_msg is None:
        return 'bad_usage_password'
    if ret_code == -75:
        return 'facility_disabled'
    if ret_code == -95:
        return 'corp_not_licensed'
    if ret_code == -98:
        return 'auth'
    return 'unknown'


def _require_vault(key: str) -> str:
    """Read a vault key; raise RuntimeError with a clear message if absent."""
    value = vault_config(key)
    if not value:
        raise RuntimeError(
            f"sitelink_pricing_client: vault key '{key}' is missing or empty. "
            "Add it via /admin/secrets (Phase 1 setup) before calling this operation."
        )
    return value
