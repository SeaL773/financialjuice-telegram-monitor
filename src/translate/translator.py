import json
from typing import cast

import httpx

from src.core.config import (
    TRANSLATE_API_KEY,
    TRANSLATE_BASE_URL,
    TRANSLATE_EXTRA_BODY,
    TRANSLATE_HEADERS,
    TRANSLATE_MAX_TOKENS,
    TRANSLATE_MODEL,
    TRANSLATE_SYSTEM_PROMPT,
    TRANSLATE_TEMPERATURE,
    TRANSLATE_TIMEOUT,
)
from src.core.security_limits import MAX_TRANSLATION_HTTP_RESPONSE_BYTES
from src.utils.LoggerManager import logger


def _request_headers() -> dict[str, str]:
    headers: dict[str, str] = {}
    seen: set[str] = set()
    for name, value in TRANSLATE_HEADERS.items():
        normalized = name.casefold()
        if normalized in seen or normalized == "content-type":
            continue
        if normalized == "authorization" and TRANSLATE_API_KEY:
            continue
        headers[name] = value
        seen.add(normalized)

    headers["Content-Type"] = "application/json"
    if TRANSLATE_API_KEY:
        # Canonical credentials replace every case variant of custom Authorization.
        headers["Authorization"] = f"Bearer {TRANSLATE_API_KEY}"
    return headers


def _request_body(text: str) -> dict[str, object]:
    body = dict(TRANSLATE_EXTRA_BODY)
    # Canonical translation fields are reserved and override extra-body values.
    body.update(
        {
            "model": TRANSLATE_MODEL,
            "messages": [
                {"role": "system", "content": TRANSLATE_SYSTEM_PROMPT},
                {"role": "user", "content": text.strip()},
            ],
            "temperature": TRANSLATE_TEMPERATURE,
            "max_tokens": TRANSLATE_MAX_TOKENS,
        }
    )
    return body


def _content_length_too_large(response: httpx.Response) -> bool:
    raw_length: str | None = response.headers.get("Content-Length")
    if raw_length is None:
        return False
    try:
        return int(raw_length) > MAX_TRANSLATION_HTTP_RESPONSE_BYTES
    except (TypeError, ValueError):
        return False


def _parse_content(content: bytes) -> str | None:
    try:
        raw_data: object = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
        logger.warning("Translation API returned malformed JSON")
        return None

    if not isinstance(raw_data, dict):
        logger.warning("Translation API returned an invalid response object")
        return None
    data = cast(dict[str, object], raw_data)
    choices = data.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        logger.warning("Translation API response has no valid choices")
        return None
    choice = cast(dict[str, object], choices[0])
    message = choice.get("message")
    if not isinstance(message, dict):
        logger.warning("Translation API response has no valid message")
        return None
    message_data = cast(dict[str, object], message)
    output = message_data.get("content")
    if not isinstance(output, str) or not output.strip():
        logger.warning("Translation API returned empty content")
        return None
    return output.strip()


async def _read_bounded_response(response: httpx.Response) -> bytes | None:
    if _content_length_too_large(response):
        logger.warning("Translation API response rejected: Content-Length exceeds limit")
        return None

    chunks: list[bytes] = []
    total = 0
    async for chunk in response.aiter_bytes():
        total += len(chunk)
        if total > MAX_TRANSLATION_HTTP_RESPONSE_BYTES:
            logger.warning("Translation API response rejected: body exceeds limit")
            return None
        chunks.append(chunk)
    return b"".join(chunks)


async def translate(text: str) -> str | None:
    if not text or not text.strip():
        return None

    endpoint = TRANSLATE_BASE_URL.rstrip("/") + "/chat/completions"
    try:
        async with httpx.AsyncClient(timeout=TRANSLATE_TIMEOUT) as client:
            async with client.stream(
                "POST",
                endpoint,
                headers=_request_headers(),
                json=_request_body(text),
            ) as response:
                if response.status_code < 200 or response.status_code >= 300:
                    logger.warning(f"Translation API returned HTTP {response.status_code}")
                    return None
                content = await _read_bounded_response(response)
                if content is None:
                    return None
                return _parse_content(content)
    except httpx.TimeoutException:
        logger.warning(f"Translation API timeout after {TRANSLATE_TIMEOUT}s")
        return None
    except httpx.RequestError as exc:
        logger.warning(f"Translation API network error: {type(exc).__name__}")
        return None
    except Exception as exc:
        logger.error(f"Translation API request failed: {type(exc).__name__}")
        return None
