import asyncio
import json
import importlib
from collections.abc import AsyncIterator, Awaitable, Callable
from types import ModuleType
from typing import Any, Optional, Protocol

from src.core.security_limits import (
    MAX_ITEMS_PER_PROCESS_BATCH,
    MAX_WS_ITEM_SERIALIZED_BYTES,
    MAX_WS_NESTED_PAYLOAD_BYTES,
    MAX_WS_TEXT_FRAME_BYTES,
)
from src.utils.LoggerManager import logger

NEWS_CHANNEL_PREFIXES = ("feed:", "feedmain:")

_ws_common: ModuleType = importlib.import_module("src.api.ws_common")
USER_AGENT: str = getattr(_ws_common, "USER_AGENT")
WSAuthError: type[Exception] = getattr(_ws_common, "WSAuthError")
WSConnectionError: type[Exception] = getattr(_ws_common, "WSConnectionError")


class _WSMessage(Protocol):
    type: object
    data: str


class _WebSocket(Protocol):
    closed: bool

    def __aiter__(self) -> AsyncIterator[_WSMessage]: ...

    async def close(self) -> object: ...

    async def send_json(self, data: object) -> object: ...

    async def send_str(self, data: str) -> object: ...

    async def receive(self) -> _WSMessage: ...

    def exception(self) -> BaseException | None: ...


class _Session(Protocol):
    def ws_connect(self, url: str, **kwargs: object) -> Awaitable[_WebSocket]: ...


def _ws_message_types() -> tuple[object, object, object, object, object]:
    import importlib

    aiohttp = importlib.import_module("aiohttp")
    ws_msg_type = getattr(aiohttp, "WSMsgType")
    return (
        getattr(ws_msg_type, "TEXT"),
        getattr(ws_msg_type, "ERROR"),
        getattr(ws_msg_type, "CLOSE"),
        getattr(ws_msg_type, "CLOSED"),
        getattr(ws_msg_type, "CLOSING"),
    )


class CentrifugoClient:
    def __init__(
        self,
        ws_url: str,
        token: str,
        session: _Session,
        receive_timeout: float = 180.0,
        on_activity: Optional[Callable[[], None]] = None,
    ):
        self.ws_url: str = ws_url
        self.token: str = token
        self._session: _Session = session
        self._ws: Optional[_WebSocket] = None
        self._receive_timeout = receive_timeout
        self._on_activity = on_activity

    async def close(self) -> None:
        if self._ws is not None and not self._ws.closed:
            _ = await self._ws.close()
        self._ws = None

    async def connect(self) -> None:
        ws_text, ws_error, _, _, _ = _ws_message_types()
        self._ws = await self._session.ws_connect(
            self.ws_url,
            headers={
                "User-Agent": USER_AGENT,
                "Origin": "https://www.financialjuice.com",
            },
        )

        ws = self._ws
        await ws.send_json(
            {
                "id": 1,
                "connect": {
                    "token": self.token,
                    "name": "js",
                },
            }
        )

        try:
            msg = await asyncio.wait_for(ws.receive(), timeout=self._receive_timeout)
        except asyncio.TimeoutError as exc:
            raise WSConnectionError(
                f"No Centrifugo connect frame received for {self._receive_timeout:g}s"
            ) from exc
        if msg.type == ws_text:
            data = self._parse_json(msg.data)
            error = data.get("error")
            if error:
                code = error.get("code")
                message = error.get("message", "unknown error")
                if code in {101, 109, 110, 111, 3500, 3501}:
                    raise WSAuthError(f"Centrifugo connect rejected: {message}")
                raise WSConnectionError(f"Centrifugo connect failed: {message}")

            connect = data.get("connect")
            if data.get("id") != 1 or not isinstance(connect, dict):
                raise WSConnectionError(f"Unexpected Centrifugo connect response: {msg.data[:200]}")

            version = connect.get("version", "unknown")
            channels = ", ".join(sorted((connect.get("subs") or {}).keys()))
            logger.info(f"🔌 Centrifugo connected (version={version}, channels={channels})")
            return

        if msg.type == ws_error:
            raise WSConnectionError(f"WS error frame: {ws.exception()}")
        raise WSConnectionError(f"Unexpected WS frame during connect: {msg.type}")

    async def listen(self) -> AsyncIterator[list[dict[str, Any]]]:
        if self._ws is None:
            raise RuntimeError("listen() before connect()")

        ws = self._ws
        ws_text, ws_error, ws_close, ws_closed, ws_closing = _ws_message_types()
        while not ws.closed:
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=self._receive_timeout)
            except asyncio.TimeoutError as exc:
                raise WSConnectionError(
                    f"No Centrifugo frame received for {self._receive_timeout:g}s"
                ) from exc
            if msg.type == ws_text:
                if self._on_activity is not None:
                    self._on_activity()
                if msg.data == "{}":
                    await ws.send_str("{}")
                    continue
                items = self._parse_frame(msg.data)
                if items:
                    yield items
            elif msg.type == ws_error:
                raise WSConnectionError(f"WS error frame: {ws.exception()}")
            elif msg.type in (ws_close, ws_closed, ws_closing):
                raise WSConnectionError("WS closed by server")

    @staticmethod
    def _parse_json(text: str) -> dict[str, Any]:
        frame_bytes = len(text.encode("utf-8"))
        if frame_bytes > MAX_WS_TEXT_FRAME_BYTES:
            raise WSConnectionError(
                f"Centrifugo frame bytes={frame_bytes} limit={MAX_WS_TEXT_FRAME_BYTES}"
            )
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise WSConnectionError(f"Invalid Centrifugo frame: {text[:200]}") from exc
        if not isinstance(data, dict):
            raise WSConnectionError(f"Unexpected Centrifugo payload type: {type(data).__name__}")
        return data

    @classmethod
    def _parse_frame(cls, text: str) -> list[dict[str, Any]]:
        frame_bytes = len(text.encode("utf-8"))
        if frame_bytes > MAX_WS_TEXT_FRAME_BYTES:
            raise WSConnectionError(
                f"Centrifugo frame bytes={frame_bytes} limit={MAX_WS_TEXT_FRAME_BYTES}"
            )

        out: list[dict[str, Any]] = []
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                envelope = json.loads(line)
            except json.JSONDecodeError:
                logger.warning(f"Skip unparsable Centrifugo object: {line[:200]}")
                continue
            if not isinstance(envelope, dict):
                logger.warning(f"Skip Centrifugo payload type: {type(envelope).__name__}")
                continue
            out.extend(cls._parse_envelope(envelope))
        return out

    @classmethod
    def _parse_envelope(cls, envelope: dict[str, Any]) -> list[dict[str, Any]]:
        error = envelope.get("error")
        if error:
            code = error.get("code")
            message = error.get("message", "unknown error")
            if code in {101, 109, 110, 111, 3500, 3501}:
                raise WSAuthError(f"Centrifugo server error: {message}")
            raise WSConnectionError(f"Centrifugo server error: {message}")

        push = envelope.get("push")
        if not isinstance(push, dict):
            return []

        channel = push.get("channel") or ""
        if not channel.startswith(NEWS_CHANNEL_PREFIXES):
            return []

        pub = push.get("pub")
        if not isinstance(pub, dict):
            return []

        data = pub.get("data")
        if not isinstance(data, dict):
            return []

        # Centrifugo wraps news in {ev, msg, t} — msg is a JSON string of news items
        msg_raw = data.get("msg")
        if msg_raw is None:
            return []
        if isinstance(msg_raw, str):
            message_bytes = len(msg_raw.encode("utf-8"))
            if message_bytes > MAX_WS_NESTED_PAYLOAD_BYTES:
                logger.warning(
                    f"Reject oversized Centrifugo msg bytes={message_bytes} "
                    f"limit={MAX_WS_NESTED_PAYLOAD_BYTES}"
                )
                return []
            try:
                msg_raw = json.loads(msg_raw)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse Centrifugo msg: {msg_raw[:200]}")
                return []

        out: list[dict[str, Any]] = []
        items = msg_raw if isinstance(msg_raw, list) else [msg_raw]
        if len(items) > MAX_ITEMS_PER_PROCESS_BATCH:
            logger.warning(
                f"Reject Centrifugo item count={len(items)} "
                f"limit={MAX_ITEMS_PER_PROCESS_BATCH}"
            )
            return []
        for item in items:
            if isinstance(item, dict):
                item_bytes = len(
                    json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                )
                if item_bytes > MAX_WS_ITEM_SERIALIZED_BYTES:
                    logger.warning(
                        f"Reject oversized Centrifugo item bytes={item_bytes} "
                        f"limit={MAX_WS_ITEM_SERIALIZED_BYTES}"
                    )
                    return []
                normalized = dict(item)
                normalized["__ws_channel__"] = channel
                out.append(normalized)
        return out
