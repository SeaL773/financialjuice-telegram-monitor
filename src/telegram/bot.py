"""Telegram Bot API for sending news alerts."""

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, List, Optional

import httpx

from src.core.config import TG_BOT_TOKEN, TG_CHAT_ID, TG_THREAD_ID
from src.telegram.rendering import (
    MAX_RENDERED_HTML_LENGTH,
    MAX_TELEGRAM_CHUNKS,
    TELEGRAM_TEXT_LIMIT,
    TelegramRenderLimitError,
)
from src.utils.LoggerManager import logger

_TG_BASE = "https://api.telegram.org/bot"
_GROUP_SEND_ATTEMPTS = 3
_GROUP_RETRY_DELAY = 0.1


@dataclass(frozen=True)
class TelegramGroupResult:
    message_ids: List[int]
    next_chunk_index: int
    complete: bool


async def tg_send(text: str, important: bool = False) -> Optional[int]:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return None

    payload: dict[str, Any] = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if TG_THREAD_ID:
        payload["message_thread_id"] = TG_THREAD_ID

    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{_TG_BASE}{TG_BOT_TOKEN}/sendMessage",
                           json=payload, timeout=10)
            d = r.json()
            if r.status_code != 200 or not d.get("ok"):
                logger.warning(f"TG send error: {d.get('error_code')} {d.get('description', '')}")
                return None
            return d.get("result", {}).get("message_id")
    except Exception as e:
        logger.error(f"TG send failed: {e}")
        return None


async def tg_edit_message(message_id: int, text: str) -> bool:
    if not TG_BOT_TOKEN or not TG_CHAT_ID or not message_id:
        return False

    payload = {
        "chat_id": TG_CHAT_ID,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(f"{_TG_BASE}{TG_BOT_TOKEN}/editMessageText",
                           json=payload, timeout=10)
            d = r.json()
            description = str(d.get("description", ""))
            if r.status_code != 200 or not d.get("ok"):
                if "message is not modified" in description.lower():
                    return True
                logger.warning(
                    f"TG edit error msg_id={message_id}: "
                    f"{d.get('error_code')} {description}"
                )
                return False
            return True
    except Exception as e:
        logger.error(f"TG edit failed msg_id={message_id}: {e}")
        return False


async def tg_send_group(texts: List[str], important: bool = False) -> List[int]:
    """Backward-compatible complete-group helper."""
    result = await tg_send_group_resumable(texts, important=important)
    return result.message_ids if result.complete else []


async def tg_send_group_resumable(
    texts: List[str],
    important: bool = False,
    start_index: int = 0,
    existing_message_ids: Optional[List[int]] = None,
    on_progress: Optional[Callable[[List[int], int], Awaitable[bool]]] = None,
) -> TelegramGroupResult:
    """Send from start_index and expose durable prefix progress on failure."""
    if len(texts) > MAX_TELEGRAM_CHUNKS:
        raise TelegramRenderLimitError(
            f"Telegram chunk count {len(texts)} exceeds {MAX_TELEGRAM_CHUNKS}"
        )
    if any(not isinstance(text, str) for text in texts):
        raise ValueError("Telegram group chunks must be strings")
    oversized = next((len(text) for text in texts if len(text) > TELEGRAM_TEXT_LIMIT), None)
    if oversized is not None:
        raise TelegramRenderLimitError(
            f"Telegram chunk length {oversized} exceeds {TELEGRAM_TEXT_LIMIT}"
        )
    total_length = sum(len(text) for text in texts)
    if total_length > MAX_RENDERED_HTML_LENGTH:
        raise TelegramRenderLimitError(
            f"Telegram group length {total_length} exceeds {MAX_RENDERED_HTML_LENGTH}"
        )
    message_ids = list(existing_message_ids or [])
    if any(not isinstance(message_id, int) or isinstance(message_id, bool) or message_id <= 0
           for message_id in message_ids):
        raise ValueError("Telegram group message IDs must be positive integers")
    if start_index != len(message_ids) or start_index < 0 or start_index > len(texts):
        raise ValueError("Telegram group resume state is inconsistent")
    for index in range(start_index, len(texts)):
        text = texts[index]
        message_id: Optional[int] = None
        for attempt in range(_GROUP_SEND_ATTEMPTS):
            message_id = await tg_send(text, important=important)
            if message_id is not None:
                break
            if attempt + 1 < _GROUP_SEND_ATTEMPTS:
                await asyncio.sleep(_GROUP_RETRY_DELAY)
        if message_id is None:
            logger.warning(
                f"TG group incomplete at chunk {index + 1}/{len(texts)}; "
                f"sent_prefix={len(message_ids)}"
            )
            return TelegramGroupResult(message_ids, index, False)
        candidate_message_ids = [*message_ids, message_id]
        if on_progress is not None:
            acknowledged = await on_progress(candidate_message_ids, index + 1)
            if not acknowledged:
                logger.warning(
                    f"TG group progress not durably acknowledged at chunk "
                    f"{index + 1}/{len(texts)}; stopping"
                )
                return TelegramGroupResult(message_ids, index, False)
        message_ids = candidate_message_ids
    return TelegramGroupResult(message_ids, len(texts), True)
