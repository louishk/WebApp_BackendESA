"""
Translation service using Azure AI Foundry (Grok 3 Mini) for discount plan T&Cs.
Translates terms between any supported language pair.
"""

import re
import logging

logger = logging.getLogger(__name__)

ALL_LANGUAGES = {
    'en': 'English',
    'ko': 'Korean',
    'zh_cn': 'Chinese Simplified',
    'zh_tw': 'Chinese Traditional',
    'ms': 'Malay',
    'ja': 'Japanese',
}

# Backward-compat alias
TARGET_LANGUAGES = {k: v for k, v in ALL_LANGUAGES.items() if k != 'en'}


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
        raise ValueError("Azure AI Foundry not configured. Check config/llm.yaml and vault for AZURE_FOUNDRY_API_KEY.")

    return OpenAI(base_url=base_url, api_key=api_key), model


def translate_terms(terms_list: list[str], target_lang: str, source_lang: str = 'en') -> list[str]:
    """
    Translate a list of T&C clauses from source_lang to target_lang.

    Args:
        terms_list: List of T&C strings in the source language
        target_lang: Target language code (en, ko, zh_cn, zh_tw, ms, ja)
        source_lang: Source language code (default 'en')

    Returns:
        List of translated strings in the same order
    """
    source_name = ALL_LANGUAGES.get(source_lang)
    target_name = ALL_LANGUAGES.get(target_lang)
    if not source_name or not target_name:
        raise ValueError(f"Unknown language code: {source_lang!r} or {target_lang!r}")
    client, model = _get_client()

    numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(terms_list))

    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    f"You are a professional translator for a self-storage company (RedBox Storage). "
                    f"Translate the following numbered terms and conditions from {source_name} to {target_name}. "
                    f"Keep the numbering. Return ONLY the translated numbered list, nothing else. "
                    f"Maintain the same business tone and legal accuracy."
                ),
            },
            {
                "role": "user",
                "content": numbered,
            },
        ],
        temperature=0.3,
        max_tokens=4000,
    )

    raw = response.choices[0].message.content.strip()

    # Parse numbered lines back into a list
    lines = []
    for line in raw.split('\n'):
        line = line.strip()
        if not line:
            continue
        # Strip leading number + dot/bracket/parenthesis
        cleaned = re.sub(r'^\d+[\.\)\]]\s*', '', line)
        if cleaned:
            lines.append(cleaned)

    # If parsing didn't yield the right count, pad or truncate
    if len(lines) != len(terms_list):
        logger.warning(
            f"Translation parse mismatch for {source_lang}->{target_lang}: "
            f"expected {len(terms_list)} items, got {len(lines)}. Adjusting."
        )
        while len(lines) < len(terms_list):
            lines.append('')
        lines = lines[:len(terms_list)]

    return lines


def translate_terms_all_languages(terms_list: list[str], source_lang: str = 'en', target_langs: list[str] | None = None) -> dict:
    """
    Translate T&Cs to multiple target languages.

    Args:
        terms_list: List of T&C strings in the source language
        source_lang: Source language code (default 'en')
        target_langs: List of target language codes, or None for all except source

    Returns:
        Dict keyed by language code: {'ko': [...], 'zh_cn': [...], ...}
    """
    if target_langs is None:
        target_langs = [lc for lc in ALL_LANGUAGES if lc != source_lang]

    translations = {}
    for lang_code in target_langs:
        if lang_code == source_lang:
            continue
        try:
            translations[lang_code] = translate_terms(terms_list, lang_code, source_lang)
        except Exception as e:
            logger.error(f"Translation to {lang_code} failed: {e}")
            translations[lang_code] = [f"[Translation error: {str(e)[:100]}]"]
    return translations
