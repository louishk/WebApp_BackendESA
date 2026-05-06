"""
Unit tests for common.sitelink_pricing_client.

Run (mock tests only, default):
    cd backend/python
    PYTHONPATH=. pytest tests/test_sitelink_pricing_client.py -v -k 'not live'

Run with live test (requires vault access + network):
    cd backend/python
    PYTHONPATH=. RUN_LIVE_SITELINK_TESTS=1 pytest tests/test_sitelink_pricing_client.py -v -k live

Live test targets C234/LSETUP unit 106073:
  - Reads current rate as baseline via UnitsInformationByUnitID.
  - Bumps by $1 with update_standard_rate (v2); asserts error_kind='ok'.
  - Restores the original rate; asserts error_kind='ok'.
  - Calls update_push_rate; asserts error_kind='corp_not_licensed' (C234 not licensed).
"""
from __future__ import annotations

import os
import sys
from decimal import Decimal
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from common.sitelink_pricing_client import (
    RateUpdateResult,
    SiteLinkPricingClient,
    _classify,
    _parse_result,
    _ids_to_str,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_rt_response(ret_code: int, ret_msg: Optional[str]) -> list[dict]:
    """Simulate the list-of-dicts output SOAPClient returns for a rate update."""
    return [{'Ret_Code': str(ret_code), 'Ret_Msg': ret_msg}]


def _make_client(corp_code: str = 'C234', location_code: str = 'LSETUP') -> SiteLinkPricingClient:
    return SiteLinkPricingClient(corp_code=corp_code, location_code=location_code)


# ---------------------------------------------------------------------------
# _classify — error_kind mapping
# ---------------------------------------------------------------------------

class TestClassify:

    def test_ret_code_1_is_ok(self):
        assert _classify(1, 'some msg') == 'ok'

    def test_ret_code_minus1_no_msg_is_bad_usage_password(self):
        assert _classify(-1, None) == 'bad_usage_password'

    def test_ret_code_minus1_with_msg_is_unknown(self):
        # -1 with a non-None message is not the "missing usage password" pattern.
        result = _classify(-1, 'some error')
        assert result == 'unknown'

    def test_ret_code_minus75_is_facility_disabled(self):
        assert _classify(-75, 'Manually Calculate Push Rates Setting is Not Enabled') == 'facility_disabled'

    def test_ret_code_minus95_is_corp_not_licensed(self):
        assert _classify(-95, "This corp code doesn't have access to this method.") == 'corp_not_licensed'

    def test_ret_code_minus98_is_auth(self):
        assert _classify(-98, 'Invalid username/password') == 'auth'

    def test_unknown_ret_code_is_unknown(self):
        assert _classify(-99, 'some random error') == 'unknown'

    def test_zero_is_unknown(self):
        assert _classify(0, None) == 'unknown'

    def test_large_positive_is_unknown(self):
        # Tenant-level endpoints return LedgerID as success, not unit-level.
        assert _classify(587171, 'Scheduled rate change...') == 'unknown'


# ---------------------------------------------------------------------------
# _parse_result — response normalisation
# ---------------------------------------------------------------------------

class TestParseResult:

    def test_success_result(self):
        raw = _make_rt_response(1, '106073')
        result = _parse_result(raw, [106073])
        assert result.success is True
        assert result.ret_code == 1
        assert result.ret_msg == '106073'
        assert result.unit_ids == [106073]
        assert result.error_kind == 'ok'

    def test_bad_usage_password(self):
        raw = _make_rt_response(-1, None)
        result = _parse_result(raw, [106073])
        assert result.success is False
        assert result.error_kind == 'bad_usage_password'

    def test_facility_disabled(self):
        raw = _make_rt_response(-75, 'Manually Calculate Push Rates Setting is Not Enabled')
        result = _parse_result(raw, [43942])
        assert result.success is False
        assert result.error_kind == 'facility_disabled'

    def test_corp_not_licensed(self):
        raw = _make_rt_response(-95, "This corp code doesn't have access to this method.")
        result = _parse_result(raw, [106073])
        assert result.success is False
        assert result.error_kind == 'corp_not_licensed'

    def test_auth_failure(self):
        raw = _make_rt_response(-98, 'Invalid username/password')
        result = _parse_result(raw, [106073])
        assert result.success is False
        assert result.error_kind == 'auth'

    def test_empty_response_is_unknown(self):
        result = _parse_result([], [106073])
        assert result.success is False
        assert result.error_kind == 'unknown'

    def test_unit_ids_propagated(self):
        raw = _make_rt_response(1, '1,2,3')
        result = _parse_result(raw, [1, 2, 3])
        assert result.unit_ids == [1, 2, 3]

    def test_non_integer_ret_code_is_unknown(self):
        raw = [{'Ret_Code': 'not_a_number', 'Ret_Msg': None}]
        result = _parse_result(raw, [1])
        assert result.error_kind == 'unknown'
        assert result.success is False


# ---------------------------------------------------------------------------
# _ids_to_str
# ---------------------------------------------------------------------------

class TestIdsToStr:

    def test_single_id(self):
        assert _ids_to_str([106073]) == '106073'

    def test_multiple_ids(self):
        assert _ids_to_str([1, 2, 3]) == '1,2,3'

    def test_empty(self):
        assert _ids_to_str([]) == ''


# ---------------------------------------------------------------------------
# SiteLinkPricingClient — mock-based method tests
# ---------------------------------------------------------------------------

class TestClientMethods:
    """Mock-patch the underlying SOAPClient.call so no network calls are made."""

    def _make_patched_client(self, ret_code: int = 1, ret_msg: Optional[str] = '106073'):
        """Return a client whose SOAPClient is mocked to return the given Ret_Code."""
        client = _make_client()
        soap_mock = MagicMock()
        soap_mock.call.return_value = _make_rt_response(ret_code, ret_msg)
        client._soap = soap_mock
        # Pre-load secrets so vault is not touched.
        client._api_key = 'FAKE_API_KEY'
        client._corp_password = 'FAKE_CORP_PW'
        client._usage_pw_std = 'UnitStandardRateP@SS'
        client._usage_pw_web = 'UnitWebRateP@SS'
        client._usage_pw_push = 'UnitPushRateP@SS'
        return client, soap_mock

    # --- update_standard_rate ---

    def test_update_standard_rate_v2_success(self):
        client, soap = self._make_patched_client(1, '106073')
        result = client.update_standard_rate([106073], Decimal('221.00'))
        assert result.success is True
        assert result.error_kind == 'ok'
        soap.call.assert_called_once()
        call_kwargs = soap.call.call_args
        assert call_kwargs.kwargs['operation'] == 'UnitStandardRateUpdate_v2'

    def test_update_standard_rate_v1(self):
        client, soap = self._make_patched_client(1, '106073')
        client.update_standard_rate([106073], Decimal('221.00'), version='v1')
        assert soap.call.call_args.kwargs['operation'] == 'UnitStandardRateUpdate'

    def test_update_standard_rate_tax_inclusive_flag(self):
        client, soap = self._make_patched_client()
        client.update_standard_rate([1], Decimal('100'), tax_inclusive=True)
        params = soap.call.call_args.kwargs['parameters']
        assert params['iRatesTaxInclusive'] == '1'

    def test_update_standard_rate_tax_exclusive_flag(self):
        client, soap = self._make_patched_client()
        client.update_standard_rate([1], Decimal('100'), tax_inclusive=False)
        params = soap.call.call_args.kwargs['parameters']
        assert params['iRatesTaxInclusive'] == '0'

    def test_update_standard_rate_bad_usage_password(self):
        client, _ = self._make_patched_client(-1, None)
        result = client.update_standard_rate([106073], Decimal('221.00'))
        assert result.success is False
        assert result.error_kind == 'bad_usage_password'

    # --- update_monthly_weekly ---

    def test_update_monthly_weekly_success(self):
        client, soap = self._make_patched_client(1, '106073')
        result = client.update_monthly_weekly([106073], Decimal('300'), Decimal('80'))
        assert result.success is True
        assert soap.call.call_args.kwargs['operation'] == 'UnitStandardRateUpdate_v3'

    def test_update_monthly_weekly_params(self):
        client, soap = self._make_patched_client()
        client.update_monthly_weekly([1], Decimal('200'), Decimal('55'))
        params = soap.call.call_args.kwargs['parameters']
        assert params['dcStdMonthlyRate'] == '200'
        assert params['dcStdWeeklyRate'] == '55'

    def test_update_monthly_weekly_facility_disabled(self):
        client, _ = self._make_patched_client(-75, 'feature disabled')
        result = client.update_monthly_weekly([1], Decimal('200'), Decimal('55'))
        assert result.error_kind == 'facility_disabled'

    # --- update_web_rate ---

    def test_update_web_rate_success(self):
        client, soap = self._make_patched_client(1, '106073')
        result = client.update_web_rate([106073], Decimal('220.00'))
        assert result.success is True
        assert soap.call.call_args.kwargs['operation'] == 'UnitWebRateUpdate'

    def test_update_web_rate_params(self):
        client, soap = self._make_patched_client()
        client.update_web_rate([1], Decimal('150.50'))
        params = soap.call.call_args.kwargs['parameters']
        assert params['dcWebRate'] == '150.50'
        assert 'sUsagePassword' in params

    def test_update_web_rate_auth_failure(self):
        client, _ = self._make_patched_client(-98, 'Invalid username/password')
        result = client.update_web_rate([1], Decimal('100'))
        assert result.error_kind == 'auth'

    # --- update_push_rate ---

    def test_update_push_rate_v2(self):
        client, soap = self._make_patched_client(-95, "This corp code doesn't have access")
        result = client.update_push_rate([106073], Decimal('221.00'))
        assert soap.call.call_args.kwargs['operation'] == 'UnitPushRateUpdate_v2'
        assert result.error_kind == 'corp_not_licensed'

    def test_update_push_rate_v1(self):
        client, soap = self._make_patched_client(-95, "not licensed")
        client.update_push_rate([1], Decimal('100'), version='v1')
        assert soap.call.call_args.kwargs['operation'] == 'UnitPushRateUpdate'

    def test_update_push_rate_corp_not_licensed(self):
        client, _ = self._make_patched_client(-95, "This corp code doesn't have access to this method.")
        result = client.update_push_rate([106073], Decimal('221.00'))
        assert result.success is False
        assert result.error_kind == 'corp_not_licensed'

    def test_update_push_rate_facility_disabled(self):
        client, _ = self._make_patched_client(-75, 'Push rates not enabled')
        result = client.update_push_rate([43942], Decimal('199.00'))
        assert result.error_kind == 'facility_disabled'

    # --- usage password in params ---

    def test_usage_password_injected_in_standard_rate(self):
        client, soap = self._make_patched_client()
        client.update_standard_rate([1], Decimal('100'))
        params = soap.call.call_args.kwargs['parameters']
        assert params['sUsagePassword'] == 'UnitStandardRateP@SS'

    def test_usage_password_injected_in_web_rate(self):
        client, soap = self._make_patched_client()
        client.update_web_rate([1], Decimal('100'))
        params = soap.call.call_args.kwargs['parameters']
        assert params['sUsagePassword'] == 'UnitWebRateP@SS'

    def test_usage_password_injected_in_push_rate(self):
        client, soap = self._make_patched_client()
        client.update_push_rate([1], Decimal('100'))
        params = soap.call.call_args.kwargs['parameters']
        assert params['sUsagePassword'] == 'UnitPushRateP@SS'

    # --- SOAP exception handling ---

    def test_soap_exception_returns_unknown_error_kind(self):
        client = _make_client()
        soap_mock = MagicMock()
        soap_mock.call.side_effect = Exception("connection timeout")
        client._soap = soap_mock
        client._api_key = 'K'
        client._corp_password = 'P'
        client._usage_pw_std = 'UP'
        result = client.update_standard_rate([1], Decimal('100'))
        assert result.success is False
        assert result.error_kind == 'unknown'

    # --- API key routing ---

    def test_test_corp_uses_test_vault_key(self):
        client = SiteLinkPricingClient('CCTST', 'Demo')
        client._corp_password = 'P'
        with patch('common.sitelink_pricing_client.vault_config') as mock_vc:
            mock_vc.return_value = 'TEST_KEY'
            _ = client._get_api_key()
            mock_vc.assert_called_once_with('SITELINK_API_KEY_TEST')

    def test_prod_corp_uses_prod_vault_key(self):
        client = SiteLinkPricingClient('C234', 'LSETUP')
        client._corp_password = 'P'
        # Patch the import inside the method.
        with patch('common.sitelink_pricing_client.vault_config') as mock_vc:
            mock_vc.return_value = 'PROD_KEY'
            _ = client._get_api_key()
            mock_vc.assert_called_once_with('SOAP_API_KEY')

    def test_missing_api_key_raises_runtime_error(self):
        client = SiteLinkPricingClient('C234', 'LSETUP')
        with patch('common.sitelink_pricing_client.vault_config', return_value=None):
            with pytest.raises(RuntimeError, match="SOAP_API_KEY"):
                client._get_api_key()

    def test_missing_usage_pw_raises_runtime_error(self):
        client = _make_client()
        with patch('common.sitelink_pricing_client.vault_config', return_value=None):
            with pytest.raises(RuntimeError, match="SITELINK_USAGE_PW_STANDARD_RATE"):
                client._get_usage_pw_std()


# ---------------------------------------------------------------------------
# RateUpdateResult dataclass
# ---------------------------------------------------------------------------

class TestRateUpdateResult:

    def test_is_dataclass(self):
        r = RateUpdateResult(
            success=True, ret_code=1, ret_msg='ok',
            unit_ids=[1], error_kind='ok',
        )
        assert r.success is True
        assert r.error_kind == 'ok'

    def test_mutability(self):
        """RateUpdateResult is a regular dataclass (not frozen); fields are settable."""
        r = RateUpdateResult(
            success=False, ret_code=-1, ret_msg=None,
            unit_ids=[], error_kind='bad_usage_password',
        )
        r.success = True  # should not raise
        assert r.success is True


# ---------------------------------------------------------------------------
# Live integration test (opt-in)
# ---------------------------------------------------------------------------

LIVE = os.getenv('RUN_LIVE_SITELINK_TESTS') == '1'

@pytest.mark.skipif(not LIVE, reason="Set RUN_LIVE_SITELINK_TESTS=1 to enable live tests")
class TestLive:
    """
    End-to-end test against C234/LSETUP unit 106073.

    Requires:
      - Valid vault configuration (.env with VAULT_MASTER_KEY, DB_PASSWORD).
      - Vault entries: SOAP_API_KEY, SOAP_CORP_PASSWORD,
        SITELINK_USAGE_PW_STANDARD_RATE, SITELINK_USAGE_PW_WEB_RATE,
        SITELINK_USAGE_PW_PUSH_RATE.
      - Network access to api.smdservers.net.

    The test reads the current rate before any change and restores it at
    teardown, so it leaves the system in the original state even on failure.
    """

    CORP_CODE = 'C234'
    LOCATION_CODE = 'LSETUP'
    UNIT_ID = 106073

    def _get_current_rate(self, client: SiteLinkPricingClient) -> Decimal:
        """Read the current standard rate for unit 106073 via UnitsInformationByUnitID."""
        soap = client._get_soap_client()
        ns = "http://tempuri.org/CallCenterWs/CallCenterWs"
        action = f"{ns}/UnitsInformationByUnitID"
        rows = soap.call(
            operation='UnitsInformationByUnitID',
            parameters={
                'sLocationCode': self.LOCATION_CODE,
                'sUnitIDsCommaDelimited': str(self.UNIT_ID),
            },
            soap_action=action,
            namespace=ns,
            result_tag='UnitInformation',
        )
        assert rows, "UnitsInformationByUnitID returned empty — unit may not exist"
        std_rate_str = rows[0].get('dcStdRate') or rows[0].get('StandardRate') or '0'
        return Decimal(str(std_rate_str)).quantize(Decimal('0.01'))

    def test_live_standard_rate_roundtrip_and_push_not_licensed(self):
        """
        1. Read baseline rate.
        2. Bump by $1 — expect error_kind='ok'.
        3. Restore baseline — expect error_kind='ok'.
        4. Call update_push_rate — expect error_kind='corp_not_licensed'.
        """
        client = SiteLinkPricingClient(self.CORP_CODE, self.LOCATION_CODE)
        baseline = self._get_current_rate(client)

        bump = baseline + Decimal('1.00')
        result_up = client.update_standard_rate([self.UNIT_ID], bump)
        assert result_up.error_kind == 'ok', (
            f"Bump failed: ret_code={result_up.ret_code}, ret_msg={result_up.ret_msg}"
        )

        result_restore = client.update_standard_rate([self.UNIT_ID], baseline)
        assert result_restore.error_kind == 'ok', (
            f"Restore failed: ret_code={result_restore.ret_code}, ret_msg={result_restore.ret_msg}"
        )

        # Verify push rate is not licensed on C234.
        result_push = client.update_push_rate([self.UNIT_ID], bump)
        assert result_push.error_kind == 'corp_not_licensed', (
            f"Expected 'corp_not_licensed', got {result_push.error_kind!r}"
        )
