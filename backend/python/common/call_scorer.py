"""
Call Scorer

Uses Azure AI Foundry (Grok) to score a Zoom call transcript across the
dimensions defined in `call_scoring_config` (esa_pbi). Returns a dict whose
keys match each enabled dimension's `key` plus metadata (quality_overall,
score_confidence, score_summary).

The prompt + dimensions + JSON schema are all built dynamically from the
config so editing the rubric in the admin UI is enough — no code change needed.

Usage:
    from common.call_scorer import score_call

    result = score_call(
        transcript="[Agent (James Oh)]: Hello...\\n[Customer]: Yes...",
        direction='outbound',
        agent_name='James Oh',
        customer_name='Mr Lee',
        duration_sec=171,
    )
    # {
    #   'quality_politeness': 8,
    #   'quality_tone': 9,
    #   ...
    #   'call_category': 'Sales',
    #   'sentiment': 'neutral',
    #   'quality_overall': 8,
    #   'score_confidence': 9,
    #   'score_status': 'done',
    #   'score_model': 'grok-3-mini@cfgv1'
    # }
"""

import json
import logging
import re
from typing import Any, Dict, Optional

from common.scoring_config import get_active_config

logger = logging.getLogger(__name__)


def _get_grok_client():
    """Get OpenAI-compat client configured for Azure AI Foundry."""
    from openai import OpenAI
    from common.config_loader import get_config

    foundry = get_config().llm.azure_foundry
    base_url = getattr(foundry, 'base_url', None)
    api_key = getattr(foundry, 'api_key_vault', None)
    if not base_url or not api_key:
        raise ValueError(
            "Azure AI Foundry not configured. "
            "Check config/llm.yaml and AZURE_FOUNDRY_API_KEY in vault."
        )
    return OpenAI(base_url=base_url, api_key=api_key)


def _build_dimension_spec(dim: Dict[str, Any]) -> str:
    """Render one dimension as a human-readable line for the LLM prompt."""
    dtype = dim.get('type', 'text')
    parts = [f"  - {dim['key']} ({dim.get('label','')}, type={dtype})"]
    if dtype == 'int':
        parts.append(f"min={dim.get('min', 1)}, max={dim.get('max', 10)}")
    elif dtype == 'enum':
        vbp = dim.get('values_by_parent')
        if vbp and dim.get('parent_key'):
            parent = dim['parent_key']
            lines = [f"values (hierarchical, pick based on {parent}):"]
            for pval, subs in vbp.items():
                lines.append(f"        if {parent}=\"{pval}\": {subs}")
            parts.append("\n      ".join(lines))
        else:
            vals = dim.get('values', [])
            parts.append(f"values={vals} (must pick exactly one of these)")
    elif dtype == 'text':
        ml = dim.get('max_length')
        if ml:
            parts.append(f"max_length={ml}")
    elif dtype == 'bool':
        parts.append("true|false")
    applies = dim.get('applies_to', 'all')
    if applies != 'all':
        parts.append(f"only-when={applies}")
    rubric = dim.get('rubric', '')
    return ', '.join(parts) + (f"\n      rubric: {rubric}" if rubric else '')


def _build_user_prompt(
    transcript: str,
    direction: str,
    agent_name: Optional[str],
    customer_name: Optional[str],
    duration_sec: int,
    dimensions: list,
) -> str:
    """Build the user message: call context + transcript + score schema."""
    enabled = [d for d in dimensions if d.get('enabled', True)]
    spec_lines = '\n'.join(_build_dimension_spec(d) for d in enabled)
    keys_list = [d['key'] for d in enabled]
    schema_keys = keys_list + ['quality_overall', 'score_confidence', 'score_summary']
    schema_hint = (
        '{\n  "' + '": null,\n  "'.join(schema_keys) + '": null\n}'
    )

    return (
        f"Call context:\n"
        f"  direction: {direction}\n"
        f"  agent: {agent_name or 'unknown'}\n"
        f"  customer: {customer_name or 'unknown'}\n"
        f"  duration: {duration_sec or 0} seconds\n\n"
        f"Score these dimensions:\n{spec_lines}\n\n"
        f"Also include:\n"
        f"  - quality_overall (int 1-10): composite of all enabled quality_* scores\n"
        f"  - score_confidence (int 1-10): your self-reported confidence in this scoring\n"
        f"  - score_summary (text, max 200 chars): one-sentence summary of the call quality\n\n"
        f"Return a JSON object with these exact keys:\n{schema_hint}\n\n"
        f"=== TRANSCRIPT ===\n{transcript}"
    )


def score_call(
    transcript: str,
    *,
    direction: str = 'outbound',
    agent_name: Optional[str] = None,
    customer_name: Optional[str] = None,
    duration_sec: int = 0,
    config_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Score one call transcript using the active rubric.

    Args:
        transcript: Conversation-formatted transcript ([Agent (Name)] / [Customer])
        direction: 'inbound' or 'outbound'
        agent_name: Internal staff name (for context only)
        customer_name: External party name (for context only)
        duration_sec: Call duration in seconds (for context)
        config_override: Optional config dict to use instead of the active one
            (used by the admin UI's "Test" button to preview unsaved drafts)

    Returns:
        Dict with all scored dimensions + metadata. Always includes:
            score_status: 'done' | 'error'
            score_model: e.g. 'grok-3-mini@cfgv3'
        On error:
            score_error: str (sanitized)
    """
    if not transcript or not transcript.strip():
        return {
            'score_status': 'error',
            'score_error': 'Empty transcript',
        }

    cfg = config_override or get_active_config()
    model = cfg.get('model', 'grok-3-mini')
    temperature = float(cfg.get('temperature', 0.2))
    max_tokens = int(cfg.get('max_tokens', 1500))
    system_prompt = cfg.get('system_prompt', '')
    dimensions = cfg.get('dimensions', [])
    version = cfg.get('_version', 0)
    score_model_tag = f"{model}@cfgv{version}"

    # Optionally enrich the system prompt with context hints
    hints = cfg.get('context_hints', {})
    if hints:
        company = hints.get('company', '')
        topics = hints.get('common_topics', [])
        if company or topics:
            system_prompt = (
                system_prompt
                + f"\n\nContext: company={company}; common topics={', '.join(topics)}."
            )

    user_prompt = _build_user_prompt(
        transcript, direction, agent_name, customer_name, duration_sec, dimensions,
    )

    try:
        client = _get_grok_client()
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        raw = response.choices[0].message.content.strip()
    except Exception as e:
        logger.exception("Grok scoring call failed")
        return {
            'score_status': 'error',
            'score_error': f"LLM call failed: {type(e).__name__}",
            'score_model': score_model_tag,
        }

    # Strip markdown fences if Grok added any despite response_format
    if raw.startswith('```'):
        raw = re.sub(r'^```(?:json)?\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("Grok returned non-JSON output: %r", raw[:300])
        return {
            'score_status': 'error',
            'score_error': 'LLM returned non-JSON output',
            'score_model': score_model_tag,
            'raw_response': raw[:1000],
        }

    if not isinstance(parsed, dict):
        return {
            'score_status': 'error',
            'score_error': 'LLM returned non-object JSON',
            'score_model': score_model_tag,
        }

    # Coerce known numeric fields (Grok sometimes returns strings)
    for d in dimensions:
        key = d.get('key')
        if key not in parsed:
            continue
        v = parsed[key]
        if v is None:
            continue
        dtype = d.get('type')
        try:
            if dtype == 'int':
                parsed[key] = int(v)
            elif dtype == 'bool':
                parsed[key] = bool(v) if isinstance(v, bool) else str(v).lower() in ('true', '1', 'yes')
            elif dtype == 'text':
                ml = d.get('max_length')
                if ml and isinstance(v, str):
                    parsed[key] = v[:ml]
        except (TypeError, ValueError):
            logger.warning("Failed to coerce %s=%r to %s, leaving as-is", key, v, dtype)

    parsed['score_status'] = 'done'
    parsed['score_model'] = score_model_tag

    return parsed
