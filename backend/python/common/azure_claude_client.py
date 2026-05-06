"""
Azure Cognitive Services — Claude client for the Pricing Tool AI review layer.

Calls Claude (Sonnet 4.6 / Haiku 4.5) hosted on Azure Cognitive Services using
the Anthropic Python SDK with prompt caching and strict-JSON output parsing.

Usage example:
    from common.azure_claude_client import AzureClaudeClient

    client = AzureClaudeClient()
    result = client.review_pricing({"site_id": "L001", "s_type_name": "M/30-35/W/NC/SS/NP", ...})
    if result.parse_succeeded:
        print(result.ai_action, result.ai_adjustment_pct)

Live integration test (requires vault secrets + real endpoint):
    RUN_LIVE_AZURE_CLAUDE_TESTS=1 pytest backend/python/tests/test_azure_claude_client.py -k live

# TODO(deps): pip install anthropic>=0.40.0
#   The `anthropic` SDK is not yet in requirements.txt.
#   Add before deploying, or discuss with the team first (project rule: no new
#   pip deps without discussion).
"""

from __future__ import annotations

import functools
import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from common.secrets_vault import vault_config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Valid enum values and per-action adjustment bands (from the system prompt)
# ---------------------------------------------------------------------------

_VALID_ACTIONS = frozenset(
    {"hold", "nudge_up", "push_up_strong", "nudge_down", "push_down_strong"}
)

# (inclusive_min, inclusive_max) for ai_adjustment_pct per action
_ACTION_BANDS: dict[str, tuple[float, float]] = {
    "hold":            (0.0,  0.0),
    "nudge_up":        (1.0,  3.0),
    "push_up_strong":  (4.0,  8.0),
    "nudge_down":      (-3.0, -1.0),
    "push_down_strong":(-8.0, -4.0),
}

_SYSTEM_PROMPT_PATH = (
    Path(__file__).parent / "prompts" / "pricing_ai_review.md"
)


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class PricingReviewResult:
    ai_action: Literal["hold", "nudge_up", "push_up_strong", "nudge_down", "push_down_strong"]
    ai_adjustment_pct: float
    ai_self_confidence: float
    ai_reasoning: str
    model_used: str
    cache_hit: bool
    raw_response_text: str
    parse_succeeded: bool


def _fallback_result(
    model_used: str = "",
    raw_response_text: str = "",
    ai_reasoning: str = "Parse failed",
    cache_hit: bool = False,
) -> PricingReviewResult:
    return PricingReviewResult(
        ai_action="hold",
        ai_adjustment_pct=0.0,
        ai_self_confidence=0.0,
        ai_reasoning=ai_reasoning,
        model_used=model_used,
        cache_hit=cache_hit,
        raw_response_text=raw_response_text,
        parse_succeeded=False,
    )


# ---------------------------------------------------------------------------
# System prompt — loaded once per process
# ---------------------------------------------------------------------------

@functools.cache
def _load_system_prompt() -> str:
    """Read pricing_ai_review.md once and cache for the process lifetime."""
    return _SYSTEM_PROMPT_PATH.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# JSON extraction + validation
# ---------------------------------------------------------------------------

def _extract_json(text: str) -> dict | None:
    """Extract the first {...} JSON object from assistant text."""
    match = re.search(r"\{[^{}]*\}", text, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group())
    except json.JSONDecodeError:
        return None


def _validate_and_clamp(data: dict) -> tuple[dict, bool]:
    """
    Validate the parsed JSON against the schema from the system prompt.
    Returns (normalised_dict, parse_succeeded).

    If parse_succeeded is False the caller should treat the result as a
    soft failure (fallback to baseline) but still log the raw response.
    Adjustment-band violations are clamped; type/enum violations return hold.
    """
    # --- required fields ---
    for field in ("ai_action", "ai_adjustment_pct", "ai_self_confidence", "ai_reasoning"):
        if field not in data:
            return data, False

    action = data.get("ai_action")
    if action not in _VALID_ACTIONS:
        return data, False

    try:
        adj = float(data["ai_adjustment_pct"])
        conf = float(data["ai_self_confidence"])
    except (TypeError, ValueError):
        return data, False

    if not (0.0 <= conf <= 1.0):
        return data, False

    reasoning = str(data.get("ai_reasoning", ""))

    # --- band clamping ---
    lo, hi = _ACTION_BANDS[action]
    clamped = adj
    parse_ok = True
    if adj < lo:
        clamped = lo
        parse_ok = False
    elif adj > hi:
        clamped = hi
        parse_ok = False

    normalised = {
        "ai_action": action,
        "ai_adjustment_pct": round(clamped, 4),
        "ai_self_confidence": round(conf, 4),
        "ai_reasoning": reasoning,
    }
    return normalised, parse_ok


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class AzureClaudeClient:
    """
    Claude client routed through Azure Cognitive Services.

    Azure routing path used:
        POST {endpoint}/anthropic/v1/messages
    i.e. base_url = f"{endpoint.rstrip('/')}/anthropic/v1"

    The Anthropic SDK accepts a custom base_url and injects the provided
    auth_token as the Authorization header (Bearer scheme).  Azure Cognitive
    Services for Claude expects the API key in an 'api-key' header — this is
    handled by the SDK's default_headers override below.

    Alternatives considered:
      - /openai/deployments/{name}/messages  — OpenAI-compatible shim, not used
        here because we need cache_control which is an Anthropic-native field.
      - /anthropic/v1/messages (no /v1 prefix) — some Azure docs show this form;
        the SDK appends /messages automatically so we provide the base without it.

    Lazy import: `import anthropic` is deferred to first use so this module
    loads cleanly even when the SDK is not installed.
    """

    def __init__(self) -> None:
        # Vault keys loaded lazily inside review_pricing so vault rotations
        # are picked up without restarting the process.
        self._client = None  # anthropic.Anthropic, initialised on first call

        if os.getenv("AZURE_CLAUDE_PROBE_ON_INIT") == "1":
            self._init_client()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _init_client(self) -> None:
        """Lazy SDK initialisation — deferred so import is optional."""
        try:
            import anthropic  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "The 'anthropic' package is required for AzureClaudeClient. "
                "Install it: pip install anthropic>=0.40.0"
            ) from exc

        endpoint = vault_config("AZURE_CLAUDE_ENDPOINT").rstrip("/")
        api_key = vault_config("AZURE_CLAUDE_KEY")

        # Azure Cognitive Services uses 'api-key' header, not 'Authorization'.
        # We pass auth_token so the SDK doesn't complain about missing ANTHROPIC_API_KEY,
        # then override with the correct Azure header.
        self._client = anthropic.Anthropic(
            base_url=f"{endpoint}/anthropic/v1",
            auth_token=api_key,
            default_headers={"api-key": api_key},
        )

    def _get_client(self):
        if self._client is None:
            self._init_client()
        return self._client

    def _resolve_deployment(self, tier: Literal["primary", "fast"]) -> str:
        key = (
            "AZURE_CLAUDE_DEPLOYMENT_PRIMARY"
            if tier == "primary"
            else "AZURE_CLAUDE_DEPLOYMENT_FAST"
        )
        return vault_config(key)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def review_pricing(
        self,
        user_payload: dict,
        tier: Literal["primary", "fast"] = "primary",
        max_tokens: int = 1024,
        timeout_s: float = 30.0,
    ) -> PricingReviewResult:
        """
        Call Claude to review a pricing recommendation.

        Args:
            user_payload: Per-type input snapshot.  Serialised as JSON and
                          sent verbatim as the user message.
            tier:         'primary' → AZURE_CLAUDE_DEPLOYMENT_PRIMARY (Sonnet 4.6).
                          'fast'    → AZURE_CLAUDE_DEPLOYMENT_FAST (Haiku 4.5).
            max_tokens:   Output token budget (default 1024 — sufficient for the
                          four-field JSON response plus any prose).
            timeout_s:    Per-request wall-clock timeout in seconds.

        Returns:
            PricingReviewResult — never raises.  On any failure parse_succeeded
            is False and ai_action defaults to 'hold'.
        """
        deployment = ""
        raw_text = ""

        try:
            import anthropic  # noqa: PLC0415 — deferred import

            deployment = self._resolve_deployment(tier)
            client = self._get_client()
            system_prompt = _load_system_prompt()
            user_message = json.dumps(user_payload, ensure_ascii=False, indent=2)

            response = client.messages.create(
                model=deployment,
                max_tokens=max_tokens,
                system=[
                    {
                        "type": "text",
                        "text": system_prompt,
                        # Prompt caching: system prompt is large and stable across
                        # the nightly batch (~90% cost saving after first request).
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                messages=[{"role": "user", "content": user_message}],
                timeout=timeout_s,
            )

            # Extract text from the first content block
            raw_text = ""
            for block in response.content:
                if hasattr(block, "text"):
                    raw_text = block.text
                    break

            # Cache hit detection
            cache_hit = False
            if hasattr(response, "usage") and response.usage is not None:
                cache_read = getattr(response.usage, "cache_read_input_tokens", 0) or 0
                cache_hit = cache_read > 0

            # Parse + validate
            parsed = _extract_json(raw_text)
            if parsed is None:
                logger.warning(
                    "azure_claude_client: no JSON object found in response "
                    "(model=%s, tier=%s)",
                    deployment, tier,
                )
                return _fallback_result(
                    model_used=deployment,
                    raw_response_text=raw_text,
                    ai_reasoning="No JSON found in response",
                    cache_hit=cache_hit,
                )

            normalised, parse_ok = _validate_and_clamp(parsed)

            if not parse_ok:
                logger.warning(
                    "azure_claude_client: JSON parse issue "
                    "(model=%s, raw_action=%s, raw_adj=%s) — clamped or defaulted",
                    deployment,
                    parsed.get("ai_action"),
                    parsed.get("ai_adjustment_pct"),
                )

            action = normalised.get("ai_action", "hold")
            if action not in _VALID_ACTIONS:
                return _fallback_result(
                    model_used=deployment,
                    raw_response_text=raw_text,
                    ai_reasoning="Invalid ai_action value",
                    cache_hit=cache_hit,
                )

            return PricingReviewResult(
                ai_action=action,  # type: ignore[arg-type]
                ai_adjustment_pct=float(normalised.get("ai_adjustment_pct", 0.0)),
                ai_self_confidence=float(normalised.get("ai_self_confidence", 0.0)),
                ai_reasoning=str(normalised.get("ai_reasoning", "")),
                model_used=deployment,
                cache_hit=cache_hit,
                raw_response_text=raw_text,
                parse_succeeded=parse_ok,
            )

        except ImportError:
            logger.exception("azure_claude_client: anthropic SDK not installed")
            return _fallback_result(
                model_used=deployment,
                raw_response_text=raw_text,
                ai_reasoning="anthropic SDK not installed",
            )

        except Exception:
            logger.exception(
                "azure_claude_client: API call failed (model=%s, tier=%s)",
                deployment, tier,
            )
            return _fallback_result(
                model_used=deployment,
                raw_response_text=raw_text,
                ai_reasoning="API call failed",
            )
