import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx

from src.core.security_limits import (
    MAX_ITEMS_PER_PROCESS_BATCH,
    MAX_STARTUP_EMBEDDED_JSON_BYTES,
    MAX_STARTUP_RESPONSE_BYTES,
    MAX_WS_ITEM_SERIALIZED_BYTES,
)
FJ_HOME_URL = "https://www.financialjuice.com/home"
FJ_STARTUP_URL = "https://live.financialjuice.com/FJService.asmx/Startup"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


class StartupError(Exception):
    """Base class for failures contacting or decoding the Startup endpoint."""


class StartupAuthError(StartupError):
    pass


class StartupTransportError(StartupError):
    pass


class StartupProtocolError(StartupError, ValueError):
    pass


class StartupIngressError(StartupProtocolError):
    pass


def _parse_startup_content(content: bytes) -> List[Dict[str, Any]]:
    if len(content) > MAX_STARTUP_RESPONSE_BYTES:
        raise StartupIngressError(
            f"Startup response bytes={len(content)} limit={MAX_STARTUP_RESPONSE_BYTES}"
        )
    if not content.strip():
        return []
    root = ET.fromstring(content)
    embedded = root.text or ""
    embedded_bytes = len(embedded.encode("utf-8"))
    if embedded_bytes > MAX_STARTUP_EMBEDDED_JSON_BYTES:
        raise StartupIngressError(
            f"Startup embedded JSON bytes={embedded_bytes} "
            f"limit={MAX_STARTUP_EMBEDDED_JSON_BYTES}"
        )
    data = json.loads(embedded)
    news = data.get("News", []) if isinstance(data, dict) else []
    if not isinstance(news, list):
        raise StartupIngressError("Startup News is not a list")
    if len(news) > MAX_ITEMS_PER_PROCESS_BATCH:
        raise StartupIngressError(
            f"Startup News count={len(news)} limit={MAX_ITEMS_PER_PROCESS_BATCH}"
        )
    items: List[Dict[str, Any]] = []
    for record in news:
        if not isinstance(record, dict):
            raise StartupIngressError("Startup News contains a non-object item")
        record_bytes = len(
            json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        )
        if record_bytes > MAX_WS_ITEM_SERIALIZED_BYTES:
            raise StartupIngressError(
                f"Startup News item bytes={record_bytes} limit={MAX_WS_ITEM_SERIALIZED_BYTES}"
            )
        items.append(record)
    return items


async def fetch_startup(
    client: httpx.AsyncClient,
    cookies: Dict[str, str],
    info: str,
) -> List[Dict[str, Any]]:
    if not info:
        raise StartupAuthError("Startup info token is empty")

    local_now = datetime.now(timezone.utc).astimezone()
    utc_offset = local_now.utcoffset()
    tz_offset = int(utc_offset.total_seconds()) // 3600 if utc_offset is not None else 0

    params = {
        "info": json.dumps(info),
        "TimeOffset": str(tz_offset),
        "tabID": "0",
        "oldID": "0",
        "TickerID": "0",
        "FeedCompanyID": "0",
        "strSearch": "",
        "extraNID": "0",
    }

    try:
        async with client.stream(
            "GET",
            FJ_STARTUP_URL,
            params=params,
            cookies=cookies,
            headers={"Referer": "https://www.financialjuice.com/"},
        ) as r:
            if r.status_code in (401, 403) or 300 <= r.status_code < 400:
                raise StartupAuthError(f"Startup authentication failed: {r.status_code}")
            if r.status_code != 200:
                raise StartupTransportError(f"Startup HTTP status: {r.status_code}")
            content_length = r.headers.get("content-length")
            if content_length is not None:
                try:
                    declared_length = int(content_length)
                except ValueError:
                    raise StartupIngressError("Invalid Startup Content-Length")
                if declared_length > MAX_STARTUP_RESPONSE_BYTES:
                    raise StartupIngressError(
                        f"Startup Content-Length={declared_length} "
                        f"limit={MAX_STARTUP_RESPONSE_BYTES}"
                    )
            chunks: list[bytes] = []
            received = 0
            async for chunk in r.aiter_bytes():
                received += len(chunk)
                if received > MAX_STARTUP_RESPONSE_BYTES:
                    raise StartupIngressError(
                        f"Startup response bytes>{MAX_STARTUP_RESPONSE_BYTES}"
                    )
                chunks.append(chunk)
            content = b"".join(chunks)
        return _parse_startup_content(content)
    except StartupError:
        raise
    except httpx.HTTPError as exc:
        raise StartupTransportError(f"Startup request failed: {exc}") from exc
    except (ET.ParseError, json.JSONDecodeError, UnicodeError, TypeError) as exc:
        raise StartupProtocolError(f"Invalid Startup response: {exc}") from exc
