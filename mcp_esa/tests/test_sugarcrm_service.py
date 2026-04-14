"""Unit tests for SugarCRMService. Uses mocked httpx to avoid real API calls."""
import pytest
from unittest.mock import MagicMock, patch

from mcp_esa.services.sugarcrm_service import (
    SugarCRMService, SugarCRMConfig, SugarCRMAPIError
)


def _make_config():
    return SugarCRMConfig(
        url="https://sugar.example.com",
        username="u",
        password="p",
        client_id="sugar",
        client_secret="",
        platform="mcp_esa",
        timeout=5,
    )


def _mock_response(json_body, status=200):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = json_body
    r.text = str(json_body)
    return r


def test_ensure_token_calls_oauth_and_caches():
    svc = SugarCRMService(_make_config())
    with patch.object(svc._client, 'post') as post:
        post.return_value = _mock_response({
            "access_token": "AT", "expires_in": 3600, "refresh_token": "RT"
        })
        svc._ensure_token()
        svc._ensure_token()  # cached, should not call again
    assert post.call_count == 1
    assert svc._access_token == "AT"
