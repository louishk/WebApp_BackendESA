"""
LLM Manager for MCP Server
Provides chat completion via Azure AI Foundry (OpenAI-compatible API).
"""

import asyncio
import logging

logger = logging.getLogger(__name__)

_client = None
_model = None


def _init_client():
    """Lazy-initialize the OpenAI client from config/vault."""
    global _client, _model
    if _client is not None:
        return _client, _model

    try:
        from openai import OpenAI
        from common.config_loader import get_config

        config = get_config()
        foundry = config.llm.azure_foundry

        base_url = getattr(foundry, 'base_url', None)
        model = getattr(foundry, 'model', 'grok-3-mini')
        api_key = getattr(foundry, 'api_key_vault', None)

        if not base_url or not api_key:
            logger.warning("Azure AI Foundry not configured — AI tools will return raw data only")
            return None, None

        _client = OpenAI(base_url=base_url, api_key=api_key)
        _model = model
        logger.info(f"LLM client initialized: {base_url} / {model}")
        return _client, _model

    except ImportError:
        logger.warning("openai package not installed — AI tools will return raw data only")
        return None, None
    except Exception as e:
        logger.error(f"Failed to initialize LLM client: {e}")
        return None, None


class ChatProvider:
    """Simple chat completion wrapper matching the interface expected by GA tools."""

    def __init__(self, client, model):
        self._client = client
        self._model = model

    async def chat_completion(self, messages, temperature=0.7, max_tokens=4000):
        """Run a chat completion and return {'content': response_text}.
        Matches the interface expected by GA AI tools."""
        response = await asyncio.to_thread(
            self._client.chat.completions.create,
            model=self._model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        return {'content': response.choices[0].message.content}


class LLMManager:
    """Manager matching the interface: get_chat_provider() -> ChatProvider."""

    def get_chat_provider(self):
        client, model = _init_client()
        if not client:
            raise ValueError("LLM not configured")
        return ChatProvider(client, model)


_manager = None


def get_llm_manager():
    """Get the global LLM manager instance. Returns None if not available."""
    global _manager
    if _manager is None:
        client, _ = _init_client()
        if client is None:
            return None
        _manager = LLMManager()
    return _manager
