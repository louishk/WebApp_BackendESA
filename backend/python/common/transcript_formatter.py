"""
Transcript Formatter

Uses the existing Azure AI Foundry (Grok) LLM to format raw call transcripts
into a readable conversation with speaker labels ([Agent] / [Customer]).

Whisper does not do speaker diarization, so the raw output is a single text
block. The LLM infers turn boundaries from context and labels them based on
the call direction (who initiated the call).

Usage:
    from common.transcript_formatter import format_as_conversation

    formatted = format_as_conversation(
        text="Hello. Hi, is that Mr Lee?...",
        direction="outbound",
        agent_name="James Oh",
    )
"""

import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)


def _get_client():
    """Get OpenAI client configured for Azure AI Foundry."""
    from openai import OpenAI
    from common.config_loader import get_config

    config = get_config()
    foundry = config.llm.azure_foundry

    base_url = getattr(foundry, 'base_url', None)
    model = getattr(foundry, 'model', 'grok-3-mini')
    api_key = getattr(foundry, 'api_key_vault', None)

    if not base_url or not api_key:
        raise ValueError(
            "Azure AI Foundry not configured. "
            "Check config/llm.yaml and AZURE_FOUNDRY_API_KEY in vault."
        )

    return OpenAI(base_url=base_url, api_key=api_key), model


def format_as_conversation(
    text: str,
    direction: str = 'outbound',
    agent_name: Optional[str] = None,
    customer_name: Optional[str] = None,
    language: Optional[str] = None,
) -> str:
    """Format a raw transcript into a [Agent] / [Customer] conversation.

    Args:
        text: Raw transcript text (single block, no speaker labels).
        direction: 'inbound' or 'outbound' — determines who spoke first.
                   outbound: agent initiates → first line is [Agent]
                   inbound:  customer initiates → first line is [Customer]
        agent_name: Optional real name of the agent (used in label).
        customer_name: Optional real name of the customer (used in label).
        language: Language hint for the LLM (e.g. "English", "Malay").

    Returns:
        Formatted transcript string. If the LLM call fails, returns the
        original text unchanged (fail-soft).
    """
    if not text or not text.strip():
        return text

    agent_label = f"Agent ({agent_name})" if agent_name else "Agent"
    customer_label = f"Customer ({customer_name})" if customer_name else "Customer"

    if direction == 'outbound':
        first_speaker = agent_label
        first_reason = "this is an outbound call, so the agent initiated"
    else:
        first_speaker = customer_label
        first_reason = "this is an inbound call, so the customer initiated"

    lang_note = f" The conversation is in {language}." if language else ""

    system_prompt = (
        "You are a transcript formatter for phone call recordings at a "
        "self-storage company (Extra Space Asia). You receive a raw transcript "
        "as a single block of text with no speaker labels. Your job is to split "
        "it into a readable conversation.\n\n"
        f"Rules:\n"
        f"1. The first speaker is [{first_speaker}] ({first_reason}).\n"
        f"2. Alternate speakers: [{agent_label}] and [{customer_label}].\n"
        f"3. Preserve the exact original wording — do NOT paraphrase or summarize.\n"
        f"4. Each turn on its own line, prefixed with the speaker label in square brackets.\n"
        f"5. Use context (questions, acknowledgments, topic shifts) to decide turn boundaries.\n"
        f"6. Return ONLY the formatted conversation — no preamble, no headers.\n"
        f"{lang_note}"
    )

    try:
        client, model = _get_client()
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text},
            ],
            temperature=0.2,
            max_tokens=4000,
        )
        formatted = response.choices[0].message.content.strip()
        if not formatted:
            logger.warning("LLM returned empty output, falling back to raw text")
            return text
        # Quality gate: response must contain at least one speaker label
        if '[Agent' not in formatted and '[Customer' not in formatted:
            logger.warning(
                "LLM returned no speaker labels (first 200 chars: %r), falling back",
                formatted[:200],
            )
            return text
        return formatted
    except Exception:
        logger.exception("Transcript formatting failed, returning raw text")
        return text
