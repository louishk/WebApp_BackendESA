"""
Azure OpenAI Whisper Speech-to-Text Client

Wraps the Azure OpenAI Whisper deployment for two operations:
  - transcribe(): returns original-language text + detected language
  - translate(): returns English translation of any source language

Both endpoints accept the same audio file types (mp3, mp4, mpeg, mpga, m4a,
wav, webm) up to 25 MB. For longer audio, the caller must chunk the file.

Config (from llm.yaml):
    azure_whisper:
      base_url:    "https://...cognitiveservices.azure.com"
      deployment:  "whisper"
      api_version: "2024-06-01"
      api_key_vault: "AZURE_WHISPER_API_KEY"
      timeout: 300

Usage:
    from common.speech_client import WhisperClient
    client = WhisperClient()
    result = client.transcribe(audio_bytes, filename='call.mp3')
    # {'text': '...', 'language': 'en', 'duration': 12.06}

    en_result = client.translate(audio_bytes, filename='call.mp3')
    # {'text': 'English translation', 'duration': 12.06}
"""

import logging
import threading
import time
from collections import deque
from typing import Any, Dict, Optional

from common.http_client import HTTPClient
from common.outbound_stats import track_outbound_api
from common.secrets_vault import vault_config

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 300
MAX_FILE_BYTES = 25 * 1024 * 1024  # 25 MB Whisper hard limit
RATE_LIMIT_WINDOW_SEC = 60          # Azure Whisper deployment limit window
RATE_LIMIT_MAX_REQUESTS = 3          # 3 requests / minute


class WhisperAPIError(Exception):
    """Raised when an Azure Whisper API call fails."""
    pass


def _load_config() -> Dict[str, Any]:
    """Load Azure Whisper config from llm.yaml."""
    try:
        from common.config_loader import get_config
        cfg = get_config().llm.azure_whisper
        return {
            'base_url': getattr(cfg, 'base_url', '').rstrip('/'),
            'deployment': getattr(cfg, 'deployment', 'whisper'),
            'api_version': getattr(cfg, 'api_version', '2024-06-01'),
            'timeout': int(getattr(cfg, 'timeout', DEFAULT_TIMEOUT)),
        }
    except Exception:
        logger.exception("Failed to load azure_whisper config")
        return {
            'base_url': '',
            'deployment': 'whisper',
            'api_version': '2024-06-01',
            'timeout': DEFAULT_TIMEOUT,
        }


class _RateLimiter:
    """Sliding-window rate limiter (thread-safe).

    Blocks until a request slot is available within the window. Used to honour
    the Azure Whisper deployment quota of N requests per W seconds.
    """

    def __init__(self, max_requests: int, window_sec: int):
        self.max_requests = max_requests
        self.window_sec = window_sec
        self._timestamps: deque = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                # Drop timestamps outside the window
                cutoff = now - self.window_sec
                while self._timestamps and self._timestamps[0] < cutoff:
                    self._timestamps.popleft()
                if len(self._timestamps) < self.max_requests:
                    self._timestamps.append(now)
                    return
                # Need to wait until the oldest request ages out
                wait = self._timestamps[0] + self.window_sec - now + 0.1
            if wait > 0:
                logger.info("Whisper rate limit hit — sleeping %.1fs", wait)
                time.sleep(wait)


class WhisperClient:
    """Azure OpenAI Whisper REST client."""

    # Class-level rate limiter shared by all instances in the process
    _rate_limiter = _RateLimiter(RATE_LIMIT_MAX_REQUESTS, RATE_LIMIT_WINDOW_SEC)

    def __init__(self, http_client: Optional[HTTPClient] = None):
        cfg = _load_config()
        self._base_url = cfg['base_url']
        self._deployment = cfg['deployment']
        self._api_version = cfg['api_version']
        self._timeout = cfg['timeout']

        self._api_key = vault_config('AZURE_WHISPER_API_KEY')
        if not self._base_url or not self._api_key:
            raise WhisperAPIError(
                "Azure Whisper not configured. Check llm.yaml and "
                "AZURE_WHISPER_API_KEY in the vault."
            )

        self._http = http_client or HTTPClient(default_timeout=self._timeout)

    def _endpoint(self, op: str) -> str:
        """Build the full URL for transcriptions or translations."""
        return (f"{self._base_url}/openai/deployments/{self._deployment}/"
                f"audio/{op}?api-version={self._api_version}")

    @track_outbound_api(
        service_name="azure_whisper",
        endpoint_extractor=lambda args, kwargs: kwargs.get('op', 'unknown'),
    )
    def _post_audio(
        self,
        op: str,
        audio_bytes: bytes,
        filename: str,
        language: Optional[str] = None,
        prompt: Optional[str] = None,
    ):
        """POST a multipart audio file to the Whisper endpoint.

        Args:
            op: 'transcriptions' or 'translations'
            audio_bytes: raw audio file content
            filename: original filename (extension drives format detection)
            language: ISO 639-1 hint (transcriptions only — improves accuracy)
            prompt: optional context prompt (Whisper system instructions)
        """
        if not audio_bytes:
            raise WhisperAPIError("Empty audio payload")
        if len(audio_bytes) > MAX_FILE_BYTES:
            raise WhisperAPIError(
                f"Audio file exceeds Whisper 25 MB limit ({len(audio_bytes)} bytes)"
            )

        url = self._endpoint(op)
        files = {'file': (filename, audio_bytes, 'audio/mpeg')}
        data = {'response_format': 'verbose_json'}
        if language and op == 'transcriptions':
            data['language'] = language
        if prompt:
            data['prompt'] = prompt

        # Honour deployment rate limit (3 req/min)
        self._rate_limiter.acquire()

        try:
            resp = self._http.request(
                'POST',
                url,
                headers={'api-key': self._api_key},
                files=files,
                data=data,
                timeout=self._timeout,
            )
            return resp
        except Exception:
            logger.exception("Azure Whisper %s call failed", op)
            raise WhisperAPIError(f"Whisper {op} request failed")

    def transcribe(
        self,
        audio_bytes: bytes,
        filename: str = 'audio.mp3',
        language: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Transcribe audio in its original language.

        Args:
            audio_bytes: raw audio file content
            filename: original filename (used for content-type detection)
            language: optional ISO 639-1 hint to improve accuracy

        Returns:
            Dict with keys: text, language, duration, segments
        """
        resp = self._post_audio('transcriptions', audio_bytes, filename, language=language)
        data = resp.json()
        return {
            'text': data.get('text', ''),
            'language': data.get('language'),
            'duration': data.get('duration'),
            'segments': data.get('segments', []),
        }

    def translate(
        self,
        audio_bytes: bytes,
        filename: str = 'audio.mp3',
    ) -> Dict[str, Any]:
        """Translate audio to English regardless of source language.

        Returns:
            Dict with keys: text (English), duration, segments
        """
        resp = self._post_audio('translations', audio_bytes, filename)
        data = resp.json()
        return {
            'text': data.get('text', ''),
            'duration': data.get('duration'),
            'segments': data.get('segments', []),
        }

    def transcribe_and_translate(
        self,
        audio_bytes: bytes,
        filename: str = 'audio.mp3',
    ) -> Dict[str, Any]:
        """Run both operations: get original-language text + English translation.

        If the detected language is already English, the translation step is
        skipped and `text_en` mirrors `text_original`.

        Returns:
            Dict with keys: text_original, text_en, language, duration
        """
        original = self.transcribe(audio_bytes, filename)
        lang = (original.get('language') or '').lower()
        if lang in ('english', 'en') or not original.get('text'):
            return {
                'text_original': original.get('text', ''),
                'text_en': original.get('text', ''),
                'language': lang or 'en',
                'duration': original.get('duration'),
            }
        translation = self.translate(audio_bytes, filename)
        return {
            'text_original': original.get('text', ''),
            'text_en': translation.get('text', ''),
            'language': lang,
            'duration': original.get('duration'),
        }
