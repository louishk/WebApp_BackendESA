"""
Tests for common/azure_claude_client.py

Run (mock only, no real API calls):
    pytest backend/python/tests/test_azure_claude_client.py -k 'not live'

Run live integration test (requires vault secrets + Azure endpoint):
    RUN_LIVE_AZURE_CLAUDE_TESTS=1 pytest backend/python/tests/test_azure_claude_client.py -k live
"""

from __future__ import annotations

import json
import os
import sys
import types
import unittest
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Minimal stubs so the module loads without the real anthropic SDK installed.
# These are injected into sys.modules before any import of azure_claude_client.
# ---------------------------------------------------------------------------

def _make_anthropic_stub():
    """Build a minimal sys.modules['anthropic'] stub."""
    stub = types.ModuleType("anthropic")

    @dataclass
    class _Usage:
        input_tokens: int = 100
        output_tokens: int = 50
        cache_creation_input_tokens: int = 0
        cache_read_input_tokens: int = 0

    @dataclass
    class _TextBlock:
        type: str = "text"
        text: str = ""

    @dataclass
    class _Message:
        content: list = field(default_factory=list)
        usage: _Usage = field(default_factory=_Usage)
        model: str = "claude-sonnet-4-6"
        stop_reason: str = "end_turn"

    class _Messages:
        def create(self, **kwargs):
            raise NotImplementedError("stub — override in each test")

    class _Anthropic:
        def __init__(self, **kwargs):
            self.messages = _Messages()

    stub.Anthropic = _Anthropic
    stub._TextBlock = _TextBlock
    stub._Message = _Message
    stub._Usage = _Usage

    # Common exception classes used in the module
    class _APIError(Exception):
        pass
    class _APITimeoutError(_APIError):
        pass
    class _AuthenticationError(_APIError):
        pass

    stub.APIError = _APIError
    stub.APITimeoutError = _APITimeoutError
    stub.AuthenticationError = _AuthenticationError

    return stub


_ANTHROPIC_STUB = _make_anthropic_stub()


# ---------------------------------------------------------------------------
# Helpers shared across tests
# ---------------------------------------------------------------------------

SAMPLE_PAYLOAD = {
    "site_id": "L001",
    "s_type_name": "M/30-35/W/NC/SS/NP",
    "total_units": 12,
    "vacant_units": 4,
    "occ_pct_now": 0.667,
    "delta_occ_30d": -0.05,
    "net_movein_30d": -2,
    "baseline_recommended_monthly": 250.0,
    "active_discounts": [],
    "memory": [],
}

_VALID_PUSH_DOWN_JSON = json.dumps({
    "ai_action": "push_down_strong",
    "ai_adjustment_pct": -5.0,
    "ai_self_confidence": 0.72,
    "ai_reasoning": "Occupancy down 5pp and net move-in negative. Prior nudge_down yielded positive T+30.",
})

_VALID_HOLD_JSON = json.dumps({
    "ai_action": "hold",
    "ai_adjustment_pct": 0.0,
    "ai_self_confidence": 0.30,
    "ai_reasoning": "Conflicting signals, cold-start humility applied.",
})

_VALID_NUDGE_UP_JSON = json.dumps({
    "ai_action": "nudge_up",
    "ai_adjustment_pct": 2.0,
    "ai_self_confidence": 0.55,
    "ai_reasoning": "Mild positive demand with supportive move-in trend.",
})


def _build_mock_response(text: str, cache_read: int = 0):
    """Build an anthropic-stub Message with the given assistant text."""
    stub = _ANTHROPIC_STUB
    block = stub._TextBlock(type="text", text=text)
    usage = stub._Usage(cache_read_input_tokens=cache_read)
    return stub._Message(content=[block], usage=usage)


# ---------------------------------------------------------------------------
# Fixtures: patch vault_config and anthropic import for unit tests
# ---------------------------------------------------------------------------

_VAULT_VALUES = {
    "AZURE_CLAUDE_ENDPOINT":           "https://fake-endpoint.cognitiveservices.azure.com",
    "AZURE_CLAUDE_KEY":                "fake-key",
    "AZURE_CLAUDE_DEPLOYMENT_PRIMARY": "claude-sonnet-4-6",
    "AZURE_CLAUDE_DEPLOYMENT_FAST":    "claude-haiku-4-5",
}


def _vault_side_effect(key: str) -> str:
    if key not in _VAULT_VALUES:
        raise KeyError(f"Vault key missing: {key}")
    return _VAULT_VALUES[key]


class _BaseTest(unittest.TestCase):
    """Sets up vault + anthropic SDK mocks for every test."""

    def setUp(self):
        # Ensure the stub is in sys.modules before importing the client
        sys.modules["anthropic"] = _ANTHROPIC_STUB  # type: ignore[assignment]

        # Reset the module cache so _init_client runs fresh each test
        import importlib
        if "common.azure_claude_client" in sys.modules:
            del sys.modules["common.azure_claude_client"]

        self.vault_patch = patch(
            "common.secrets_vault.vault_config",
            side_effect=_vault_side_effect,
        )
        self.vault_mock = self.vault_patch.start()

        # Import after patching
        from common.azure_claude_client import AzureClaudeClient, _load_system_prompt
        _load_system_prompt.cache_clear()

        self.AzureClaudeClient = AzureClaudeClient
        self._load_system_prompt = _load_system_prompt

    def tearDown(self):
        self.vault_patch.stop()

    def _make_client_with_response(self, response_text: str, cache_read: int = 0):
        """Return a client whose messages.create returns the given text."""
        mock_response = _build_mock_response(response_text, cache_read=cache_read)
        client = self.AzureClaudeClient.__new__(self.AzureClaudeClient)
        client._client = MagicMock()
        client._client.messages.create.return_value = mock_response
        return client


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------

class TestSuccessfulCall(_BaseTest):

    def test_parsed_action_and_cache_hit_false(self):
        client = self._make_client_with_response(_VALID_PUSH_DOWN_JSON, cache_read=0)
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            result = client.review_pricing(SAMPLE_PAYLOAD)

        self.assertTrue(result.parse_succeeded)
        self.assertEqual(result.ai_action, "push_down_strong")
        self.assertAlmostEqual(result.ai_adjustment_pct, -5.0)
        self.assertAlmostEqual(result.ai_self_confidence, 0.72)
        self.assertEqual(result.model_used, "claude-sonnet-4-6")
        self.assertFalse(result.cache_hit)

    def test_cache_hit_true_when_cache_read_tokens_nonzero(self):
        client = self._make_client_with_response(_VALID_HOLD_JSON, cache_read=1500)
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            result = client.review_pricing(SAMPLE_PAYLOAD)

        self.assertTrue(result.parse_succeeded)
        self.assertTrue(result.cache_hit)

    def test_nudge_up_parsed_correctly(self):
        client = self._make_client_with_response(_VALID_NUDGE_UP_JSON)
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            result = client.review_pricing(SAMPLE_PAYLOAD)

        self.assertTrue(result.parse_succeeded)
        self.assertEqual(result.ai_action, "nudge_up")
        self.assertAlmostEqual(result.ai_adjustment_pct, 2.0)

    def test_raw_response_text_stored(self):
        client = self._make_client_with_response(_VALID_HOLD_JSON)
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            result = client.review_pricing(SAMPLE_PAYLOAD)

        self.assertIn("hold", result.raw_response_text)


class TestMalformedJson(_BaseTest):

    def test_no_json_returns_parse_failed(self):
        client = self._make_client_with_response("Sorry, I cannot help with that.")
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            result = client.review_pricing(SAMPLE_PAYLOAD)

        self.assertFalse(result.parse_succeeded)
        self.assertEqual(result.ai_action, "hold")
        self.assertAlmostEqual(result.ai_adjustment_pct, 0.0)

    def test_truncated_json_returns_parse_failed(self):
        client = self._make_client_with_response('{"ai_action": "nudge_up"')
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            result = client.review_pricing(SAMPLE_PAYLOAD)

        self.assertFalse(result.parse_succeeded)

    def test_invalid_action_enum_returns_parse_failed(self):
        bad = json.dumps({
            "ai_action": "rocket_up",
            "ai_adjustment_pct": 5.0,
            "ai_self_confidence": 0.8,
            "ai_reasoning": "Bad action value.",
        })
        client = self._make_client_with_response(bad)
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            result = client.review_pricing(SAMPLE_PAYLOAD)

        self.assertFalse(result.parse_succeeded)
        self.assertEqual(result.ai_action, "hold")

    def test_missing_required_field_returns_parse_failed(self):
        bad = json.dumps({
            "ai_action": "nudge_up",
            # ai_adjustment_pct missing
            "ai_self_confidence": 0.5,
            "ai_reasoning": "Missing field.",
        })
        client = self._make_client_with_response(bad)
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            result = client.review_pricing(SAMPLE_PAYLOAD)

        self.assertFalse(result.parse_succeeded)

    def test_confidence_out_of_range_returns_parse_failed(self):
        bad = json.dumps({
            "ai_action": "hold",
            "ai_adjustment_pct": 0.0,
            "ai_self_confidence": 1.5,  # > 1.0
            "ai_reasoning": "Over-confident.",
        })
        client = self._make_client_with_response(bad)
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            result = client.review_pricing(SAMPLE_PAYLOAD)

        self.assertFalse(result.parse_succeeded)

    def test_json_embedded_in_prose_is_still_extracted(self):
        """The client should tolerate prose before/after the JSON block."""
        prose = f"Here is my analysis:\n{_VALID_NUDGE_UP_JSON}\nThank you."
        client = self._make_client_with_response(prose)
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            result = client.review_pricing(SAMPLE_PAYLOAD)

        self.assertTrue(result.parse_succeeded)
        self.assertEqual(result.ai_action, "nudge_up")


class TestAdjustmentBandClamping(_BaseTest):

    def test_nudge_up_with_out_of_band_pct_clamped_and_parse_failed(self):
        # nudge_up band is [1.0, 3.0]; 9.0 is outside
        out_of_band = json.dumps({
            "ai_action": "nudge_up",
            "ai_adjustment_pct": 9.0,
            "ai_self_confidence": 0.6,
            "ai_reasoning": "Over-adjustment test.",
        })
        client = self._make_client_with_response(out_of_band)
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            result = client.review_pricing(SAMPLE_PAYLOAD)

        # Clamped to band maximum
        self.assertAlmostEqual(result.ai_adjustment_pct, 3.0)
        # parse_succeeded is False because value was outside band
        self.assertFalse(result.parse_succeeded)
        # Action itself is still nudge_up (only the pct was wrong)
        self.assertEqual(result.ai_action, "nudge_up")

    def test_push_down_strong_below_band_clamped(self):
        # push_down_strong band is [-8.0, -4.0]; -10.0 is below
        out_of_band = json.dumps({
            "ai_action": "push_down_strong",
            "ai_adjustment_pct": -10.0,
            "ai_self_confidence": 0.8,
            "ai_reasoning": "Extreme push test.",
        })
        client = self._make_client_with_response(out_of_band)
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            result = client.review_pricing(SAMPLE_PAYLOAD)

        self.assertAlmostEqual(result.ai_adjustment_pct, -8.0)
        self.assertFalse(result.parse_succeeded)

    def test_hold_with_nonzero_pct_clamped_to_zero(self):
        # hold must be exactly 0.0
        out_of_band = json.dumps({
            "ai_action": "hold",
            "ai_adjustment_pct": 2.5,
            "ai_self_confidence": 0.5,
            "ai_reasoning": "Hold but wrong pct.",
        })
        client = self._make_client_with_response(out_of_band)
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            result = client.review_pricing(SAMPLE_PAYLOAD)

        self.assertAlmostEqual(result.ai_adjustment_pct, 0.0)
        self.assertFalse(result.parse_succeeded)


class TestSdkExceptions(_BaseTest):

    def test_timeout_returns_graceful_fallback(self):
        client = self.AzureClaudeClient.__new__(self.AzureClaudeClient)
        client._client = MagicMock()
        client._client.messages.create.side_effect = Exception("Request timed out")
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            result = client.review_pricing(SAMPLE_PAYLOAD)

        self.assertFalse(result.parse_succeeded)
        self.assertEqual(result.ai_action, "hold")
        self.assertAlmostEqual(result.ai_adjustment_pct, 0.0)
        # Must not raise

    def test_auth_error_returns_graceful_fallback(self):
        client = self.AzureClaudeClient.__new__(self.AzureClaudeClient)
        client._client = MagicMock()
        client._client.messages.create.side_effect = Exception("401 Unauthorized")
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            result = client.review_pricing(SAMPLE_PAYLOAD)

        self.assertFalse(result.parse_succeeded)
        self.assertEqual(result.ai_action, "hold")

    def test_network_error_does_not_bubble(self):
        client = self.AzureClaudeClient.__new__(self.AzureClaudeClient)
        client._client = MagicMock()
        client._client.messages.create.side_effect = ConnectionError("DNS failure")
        with patch.object(client, "_resolve_deployment", return_value="claude-sonnet-4-6"):
            # This must NOT raise
            result = client.review_pricing(SAMPLE_PAYLOAD)

        self.assertIsInstance(result.parse_succeeded, bool)
        self.assertFalse(result.parse_succeeded)


class TestTierResolution(_BaseTest):

    def test_fast_tier_uses_haiku_deployment(self):
        """tier='fast' must pick AZURE_CLAUDE_DEPLOYMENT_FAST from vault."""
        mock_response = _build_mock_response(_VALID_HOLD_JSON)
        client = self.AzureClaudeClient.__new__(self.AzureClaudeClient)
        client._client = MagicMock()
        client._client.messages.create.return_value = mock_response

        result = client.review_pricing(SAMPLE_PAYLOAD, tier="fast")

        self.assertEqual(result.model_used, "claude-haiku-4-5")

    def test_primary_tier_uses_sonnet_deployment(self):
        """tier='primary' must pick AZURE_CLAUDE_DEPLOYMENT_PRIMARY from vault."""
        mock_response = _build_mock_response(_VALID_HOLD_JSON)
        client = self.AzureClaudeClient.__new__(self.AzureClaudeClient)
        client._client = MagicMock()
        client._client.messages.create.return_value = mock_response

        result = client.review_pricing(SAMPLE_PAYLOAD, tier="primary")

        self.assertEqual(result.model_used, "claude-sonnet-4-6")


# ---------------------------------------------------------------------------
# Live integration test — skipped by default
# ---------------------------------------------------------------------------

@unittest.skipUnless(
    os.getenv("RUN_LIVE_AZURE_CLAUDE_TESTS") == "1",
    "Set RUN_LIVE_AZURE_CLAUDE_TESTS=1 to run live Azure endpoint test",
)
class TestLiveIntegration(unittest.TestCase):
    """
    Hits the real Azure Claude endpoint.

    Requires vault secrets:
        AZURE_CLAUDE_ENDPOINT, AZURE_CLAUDE_KEY,
        AZURE_CLAUDE_DEPLOYMENT_PRIMARY, AZURE_CLAUDE_DEPLOYMENT_FAST

    Run with:
        RUN_LIVE_AZURE_CLAUDE_TESTS=1 pytest backend/python/tests/test_azure_claude_client.py -k live -s
    """

    def test_live_review_pricing_returns_valid_result(self):
        # Remove stub so the real SDK is used
        sys.modules.pop("anthropic", None)

        from common.azure_claude_client import AzureClaudeClient, _load_system_prompt
        _load_system_prompt.cache_clear()

        client = AzureClaudeClient()
        # Minimal payload — enough for the model to respond coherently
        payload = {
            "site_id": "L001",
            "s_type_name": "S/8-10/M/A/SS/NP",
            "total_units": 30,
            "vacant_units": 3,
            "occ_pct_now": 0.90,
            "delta_occ_30d": 0.05,
            "net_movein_30d": 2,
            "baseline_recommended_monthly": 180.0,
            "active_discounts": [],
            "memory": [],
        }

        result = client.review_pricing(payload, tier="primary")

        self.assertIsNotNone(result)
        self.assertTrue(
            result.parse_succeeded,
            f"Live parse failed. Raw: {result.raw_response_text!r}",
        )
        self.assertIn(
            result.ai_action,
            {"hold", "nudge_up", "push_up_strong", "nudge_down", "push_down_strong"},
        )
        self.assertGreaterEqual(result.ai_self_confidence, 0.0)
        self.assertLessEqual(result.ai_self_confidence, 1.0)
        self.assertIsInstance(result.ai_reasoning, str)
        self.assertGreater(len(result.ai_reasoning), 0)


if __name__ == "__main__":
    unittest.main()
