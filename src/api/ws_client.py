import json
import importlib
import asyncio
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from types import ModuleType
from typing import Any, Optional, Protocol
from urllib.parse import urlencode, urlparse, parse_qs

from src.utils.LoggerManager import logger


class _Response(Protocol):
    status: int

    async def json(self) -> object: ...

    async def text(self) -> str: ...


class _ResponseContext(Protocol):
    async def __aenter__(self) -> _Response: ...

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> object: ...


class _WSMessage(Protocol):
    type: object
    data: str


class _WebSocket(Protocol):
    closed: bool

    def __aiter__(self) -> AsyncIterator[_WSMessage]: ...

    async def close(self) -> object: ...

    async def receive(self) -> _WSMessage: ...

    def exception(self) -> BaseException | None: ...


class _Session(Protocol):
    def get(self, url: str, **kwargs: object) -> _ResponseContext: ...

    def ws_connect(self, url: str, **kwargs: object) -> Awaitable[_WebSocket]: ...

    async def close(self) -> object: ...


_aiohttp: ModuleType = importlib.import_module("aiohttp")
_ws_common: ModuleType = importlib.import_module("src.api.ws_common")
_ws_parser: ModuleType = importlib.import_module("src.api.ws_parser")
USER_AGENT: str = getattr(_ws_common, "USER_AGENT")
WSAuthError: type[Exception] = getattr(_ws_common, "WSAuthError")
WSConnectionError: type[Exception] = getattr(_ws_common, "WSConnectionError")
_parse_signalr_frame: Callable[[str], list[dict[str, Any]]] = getattr(
    _ws_parser, "parse_signalr_frame"
)
_ws_frame_server_error: type[ValueError] = getattr(_ws_parser, "WSFrameServerError")
_client_session_factory: Callable[[], _Session] = getattr(_aiohttp, "ClientSession")
_ws_msg_type: object = getattr(_aiohttp, "WSMsgType")
_WS_TEXT: object = getattr(_ws_msg_type, "TEXT")
_WS_ERROR: object = getattr(_ws_msg_type, "ERROR")
_WS_CLOSE: object = getattr(_ws_msg_type, "CLOSE")
_WS_CLOSED: object = getattr(_ws_msg_type, "CLOSED")
_WS_CLOSING: object = getattr(_ws_msg_type, "CLOSING")

CLIENT_PROTOCOL = "2.1"
HUB_NAME = "newshub"
CONNECTION_DATA = json.dumps([{"name": HUB_NAME}])
NEGOTIATE_URL = "https://www.financialjuice.com/signalr/negotiate"
def _build_azure_url(
    redirect_url: str,
    op: str,
    extra: Optional[Mapping[str, str]] = None,
) -> str:
    parsed = urlparse(redirect_url)
    base_qs = parse_qs(parsed.query)
    flat_qs = {k: v[0] for k, v in base_qs.items()}
    flat_qs["clientProtocol"] = CLIENT_PROTOCOL
    flat_qs["connectionData"] = CONNECTION_DATA
    if extra:
        flat_qs.update(extra)
    base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}/{op}"
    return f"{base}?{urlencode(flat_qs)}"


class FJSignalRClient:
    def __init__(
        self,
        cookies: dict[str, str],
        feedtoken: str,
        session: Optional[_Session] = None,
        receive_timeout: float = 180.0,
        on_activity: Optional[Callable[[], None]] = None,
    ):
        self.cookies = cookies
        self.feedtoken = feedtoken
        self._owns_session = session is None
        self._session = session
        self._ws: Optional[_WebSocket] = None
        self._access_token: Optional[str] = None
        self._connection_token: Optional[str] = None
        self._redirect_url: Optional[str] = None
        self._tab_id = str(uuid.uuid4())
        self._receive_timeout = receive_timeout
        self._on_activity = on_activity

    async def __aenter__(self) -> "FJSignalRClient":
        if self._session is None:
            self._session = _client_session_factory()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
            self._ws = None
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    def _require_session(self) -> _Session:
        if self._session is None:
            raise WSConnectionError("HTTP session is unavailable")
        return self._session

    def _require_redirect_url(self) -> str:
        if self._redirect_url is None:
            raise WSConnectionError("RedirectUrl is unavailable")
        return self._redirect_url

    def _require_access_token(self) -> str:
        if self._access_token is None:
            raise WSConnectionError("AccessToken is unavailable")
        return self._access_token

    def _require_connection_token(self) -> str:
        if self._connection_token is None:
            raise WSConnectionError("ConnectionToken is unavailable")
        return self._connection_token

    @staticmethod
    def _required_string(data: object, key: str, context: str) -> str:
        if not isinstance(data, dict):
            raise WSConnectionError(f"Malformed {context} response: expected object")
        value = data.get(key)
        if not isinstance(value, str) or not value:
            raise WSConnectionError(f"Malformed {context} response: missing {key}")
        return value

    async def _negotiate_fj(self) -> None:
        params = {
            "clientProtocol": CLIENT_PROTOCOL,
            "ftoken": self.feedtoken,
            "tabID": self._tab_id,
            "connectionData": CONNECTION_DATA,
            "_": str(int(time.time() * 1000)),
        }
        session = self._require_session()
        async with session.get(
            NEGOTIATE_URL,
            params=params,
            cookies=self.cookies,
            headers={
                "User-Agent": USER_AGENT,
                "Referer": "https://www.financialjuice.com/home",
                "Origin": "https://www.financialjuice.com",
            },
            timeout=20,
        ) as r:
            if r.status == 401 or r.status == 403:
                raise WSAuthError(f"FJ negotiate auth failed: {r.status}")
            if r.status != 200:
                raise WSConnectionError(f"FJ negotiate failed: {r.status}")
            data = await r.json()
        self._redirect_url = self._required_string(data, "RedirectUrl", "FJ negotiate")
        self._access_token = self._required_string(data, "AccessToken", "FJ negotiate")
        if isinstance(data, dict) and data.get("TryWebSockets", True) is False:
            raise WSConnectionError("Server reports TryWebSockets=false")

    async def _negotiate_azure(self) -> None:
        url = _build_azure_url(self._require_redirect_url(), "negotiate")
        session = self._require_session()
        access_token = self._require_access_token()
        async with session.get(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Authorization": f"Bearer {access_token}",
                "Referer": "https://www.financialjuice.com/home",
                "Origin": "https://www.financialjuice.com",
            },
            timeout=20,
        ) as r:
            if r.status == 401 or r.status == 403:
                raise WSAuthError(f"Azure negotiate auth failed: {r.status}")
            if r.status != 200:
                body = await r.text()
                raise WSConnectionError(f"Azure negotiate failed: {r.status} {body[:200]}")
            data = await r.json()
        self._connection_token = self._required_string(
            data, "ConnectionToken", "Azure negotiate"
        )

    async def _connect_ws(self) -> None:
        redirect_url = self._require_redirect_url()
        connection_token = self._require_connection_token()
        access_token = self._require_access_token()
        ws_url = _build_azure_url(
            redirect_url,
            "connect",
            {
                "transport": "webSockets",
                "connectionToken": connection_token,
                "tid": "1",
            },
        ).replace("https://", "wss://")

        cookie_header = "; ".join(f"{k}={v}" for k, v in self.cookies.items())
        session = self._require_session()
        self._ws = await session.ws_connect(
            ws_url,
            headers={
                "User-Agent": USER_AGENT,
                "Authorization": f"Bearer {access_token}",
                "Cookie": cookie_header,
                "Origin": "https://www.financialjuice.com",
            },
            heartbeat=30,
            timeout=30,
        )

    async def _start(self) -> None:
        redirect_url = self._require_redirect_url()
        connection_token = self._require_connection_token()
        access_token = self._require_access_token()
        url = _build_azure_url(
            redirect_url,
            "start",
            {
                "transport": "webSockets",
                "connectionToken": connection_token,
            },
        )
        session = self._require_session()
        async with session.get(
            url,
            headers={
                "User-Agent": USER_AGENT,
                "Authorization": f"Bearer {access_token}",
                "Referer": "https://www.financialjuice.com/home",
                "Origin": "https://www.financialjuice.com",
            },
            timeout=20,
        ) as r:
            if r.status != 200:
                raise WSConnectionError(f"start failed: {r.status}")

    async def connect(self) -> None:
        if self._session is None:
            self._session = _client_session_factory()
        await self._negotiate_fj()
        await self._negotiate_azure()
        await self._connect_ws()
        await self._start()
        logger.info(f"🔌 SignalR connected (tabID={self._tab_id[:8]}…)")

    async def listen(self) -> AsyncIterator[list[dict[str, Any]]]:
        if self._ws is None:
            raise RuntimeError("listen() before connect()")
        ws = self._ws
        while not ws.closed:
            try:
                msg = await asyncio.wait_for(
                    ws.receive(), timeout=self._receive_timeout
                )
            except asyncio.TimeoutError as exc:
                raise WSConnectionError(
                    f"No SignalR frame received for {self._receive_timeout:g}s"
                ) from exc
            if msg.type == _WS_TEXT:
                items = self._parse_frame(msg.data)
                if self._on_activity is not None:
                    self._on_activity()
                if items:
                    yield items
            elif msg.type == _WS_ERROR:
                raise WSConnectionError(f"WS error frame: {ws.exception()}")
            elif msg.type in (_WS_CLOSE, _WS_CLOSED, _WS_CLOSING):
                raise WSConnectionError("WS closed by server")

    @staticmethod
    def _parse_frame(text: str) -> list[dict[str, Any]]:
        try:
            return _parse_signalr_frame(text)
        except _ws_frame_server_error as exc:
            raise WSAuthError(f"Server error frame: {exc}") from exc
