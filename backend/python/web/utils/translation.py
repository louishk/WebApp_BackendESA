"""
Translation service using Azure AI Foundry (Grok 3 Mini) for discount plan T&Cs.
Translates English terms to Korean, Chinese Simplified, Chinese Traditional, Malay, Japanese.
"""

import re
import logging

logger = logging.getLogger(__name__)

TARGET_LANGUAGES = {
    'ko': 'Korean',
    'zh_cn': 'Chinese Simplified',
    'zh_tw': 'Chinese Traditional',
    'ms': 'Malay',
    'ja': 'Japanese',
}


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


def translate_terms(terms_list: list[str], target_lang: str) -> list[str]:
    """
    Translate a list of T&C clauses to the target language.
    Batches all clauses in one prompt for consistency.

    Args:
        terms_list: List of English T&C strings
        target_lang: Language code (ko, zh_cn, zh_tw, ms, ja)

    Returns:
        List of translated strings in the same order
    """
    lang_name = TARGET_LANGUAGES.get(target_lang, target_lang)
    client, model = _get_client()

    numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(terms_list))

    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    f"You are a professional translator for a self-storage company (RedBox Storage). "
                    f"Translate the following numbered terms and conditions from English to {lang_name}. "
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
            f"Translation parse mismatch for {target_lang}: "
            f"expected {len(terms_list)} items, got {len(lines)}. Adjusting."
        )
        while len(lines) < len(terms_list):
            lines.append('')
        lines = lines[:len(terms_list)]

    return lines


def translate_terms_all_languages(terms_list: list[str]) -> dict:
    """
    Translate T&Cs to all target languages.

    Returns:
        Dict keyed by language code: {'ko': [...], 'zh_cn': [...], ...}
    """
    translations = {}
    for lang_code in TARGET_LANGUAGES:
        try:
            translations[lang_code] = translate_terms(terms_list, lang_code)
        except Exception as e:
            logger.error(f"Translation to {lang_code} failed: {e}")
            translations[lang_code] = [f"[Translation error: {str(e)[:100]}]"]
    return translations
