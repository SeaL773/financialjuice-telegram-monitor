import html
import json
import importlib
import unittest
from unittest.mock import AsyncMock, Mock, patch

from src.api.startup_client import (
    StartupAuthError,
    StartupProtocolError,
    StartupTransportError,
    _parse_startup_content,
    fetch_startup,
)
from src.core.security_limits import (
    MAX_ITEMS_PER_PROCESS_BATCH,
    MAX_STARTUP_EMBEDDED_JSON_BYTES,
    MAX_STARTUP_RESPONSE_BYTES,
    MAX_WS_INVOCATIONS,
    MAX_WS_NESTED_PAYLOAD_BYTES,
    MAX_WS_TEXT_FRAME_BYTES,
)

ws_parser = importlib.import_module("src.api.ws_parser")
parse_signalr_frame = ws_parser.parse_signalr_frame


def streaming_client(
    *,
    status_code: int = 200,
    headers: dict[str, str] | None = None,
    chunks: list[bytes] | None = None,
):
    response = Mock(status_code=status_code, headers=headers or {})

    async def aiter_bytes():
        for chunk in chunks or []:
            yield chunk

    response.aiter_bytes = aiter_bytes
    context = Mock()
    context.__aenter__ = AsyncMock(return_value=response)
    context.__aexit__ = AsyncMock(return_value=None)
    client = Mock(stream=Mock(return_value=context))
    return client, response


class WebSocketIngressTestCase(unittest.TestCase):
    def test_oversized_frame_rejected_before_json_load(self):
        text = "x" * (MAX_WS_TEXT_FRAME_BYTES + 1)
        with patch("src.api.ws_parser.json.loads", side_effect=AssertionError("must not parse")):
            self.assertEqual([], parse_signalr_frame(text))

    def test_too_many_invocations_and_items_rejected(self):
        envelope = {"M": [{}] * (MAX_WS_INVOCATIONS + 1)}
        self.assertEqual([], parse_signalr_frame(json.dumps(envelope)))
        payload = [{}] * (MAX_ITEMS_PER_PROCESS_BATCH + 1)
        envelope = {"M": [{"M": "sendUpdates", "A": [payload]}]}
        self.assertEqual([], parse_signalr_frame(json.dumps(envelope)))

    def test_oversized_nested_json_rejected_before_nested_parse(self):
        payload = "x" * (MAX_WS_NESTED_PAYLOAD_BYTES + 1)
        envelope = {"M": [{"M": "sendUpdates", "A": [payload]}]}
        original_loads = json.loads
        calls = 0

        def counted_loads(value):
            nonlocal calls
            calls += 1
            return original_loads(value)

        with patch("src.api.ws_parser.json.loads", side_effect=counted_loads):
            self.assertEqual([], parse_signalr_frame(json.dumps(envelope)))
        self.assertEqual(1, calls)

    def test_normal_methods_parse_unchanged(self):
        envelope = {
            "M": [
                {"M": "sendUpdates", "A": [json.dumps([{"NewsID": 1}])]},
                {"M": "sendHeadlineUpdated", "A": [{"NewsID": 2}]},
            ]
        }
        parsed = parse_signalr_frame(json.dumps(envelope))
        self.assertEqual([1, 2], [record["NewsID"] for record in parsed])
        self.assertEqual(
            ["sendUpdates", "sendHeadlineUpdated"],
            [record["__ws_method__"] for record in parsed],
        )


class StartupIngressTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_content_length_over_limit_rejected_before_content_access(self):
        client, response = streaming_client(
            headers={"content-length": str(MAX_STARTUP_RESPONSE_BYTES + 1)}
        )
        response.aiter_bytes = Mock(side_effect=AssertionError("must not iterate body"))
        with self.assertRaises(StartupProtocolError):
            await fetch_startup(client, {}, "info")

    async def test_startup_failures_are_classified(self):
        client = Mock()
        for status, error_type in (
            (401, StartupAuthError),
            (403, StartupAuthError),
            (500, StartupTransportError),
        ):
            with self.subTest(status=status):
                client, _ = streaming_client(status_code=status)
                with self.assertRaises(error_type):
                    await fetch_startup(client, {}, "info")

        context = Mock()
        context.__aenter__ = AsyncMock(
            side_effect=__import__("httpx").ConnectError("offline")
        )
        context.__aexit__ = AsyncMock(return_value=None)
        client = Mock(stream=Mock(return_value=context))
        with self.assertRaises(StartupTransportError):
            await fetch_startup(client, {}, "info")

        client, _ = streaming_client(chunks=[b"not xml"])
        with self.assertRaises(StartupProtocolError):
            await fetch_startup(client, {}, "info")

    async def test_auth_status_precedes_content_length_validation(self):
        client, response = streaming_client(
            status_code=401,
            headers={"content-length": str(MAX_STARTUP_RESPONSE_BYTES + 1)},
        )
        response.aiter_bytes = Mock(side_effect=AssertionError("must not iterate body"))

        with self.assertRaises(StartupAuthError):
            await fetch_startup(client, {}, "info")

    async def test_successful_empty_startup_response_is_not_an_error(self):
        client, _ = streaming_client()
        self.assertEqual([], await fetch_startup(client, {}, "info"))

    async def test_redirect_is_auth_error(self):
        client, _ = streaming_client(status_code=302, headers={"location": "/login"})
        with self.assertRaises(StartupAuthError):
            await fetch_startup(client, {}, "info")

    async def test_chunked_overflow_stops_iteration_at_limit(self):
        yielded = 0

        async def chunks():
            nonlocal yielded
            for chunk in (b"x" * MAX_STARTUP_RESPONSE_BYTES, b"y", b"unread"):
                yielded += 1
                yield chunk

        response = Mock(status_code=200, headers={})
        response.aiter_bytes = chunks
        context = Mock()
        context.__aenter__ = AsyncMock(return_value=response)
        context.__aexit__ = AsyncMock(return_value=None)
        client = Mock(stream=Mock(return_value=context))

        with self.assertRaises(StartupProtocolError):
            await fetch_startup(client, {}, "info")
        self.assertEqual(2, yielded)

    def test_content_and_embedded_json_limits(self):
        with self.assertRaises(ValueError):
            _parse_startup_content(b"x" * (MAX_STARTUP_RESPONSE_BYTES + 1))
        embedded = "x" * (MAX_STARTUP_EMBEDDED_JSON_BYTES + 1)
        with self.assertRaises(ValueError):
            _parse_startup_content(f"<string>{embedded}</string>".encode())

    def test_news_count_limit_and_normal_response(self):
        too_many = html.escape(json.dumps({"News": [{}] * (MAX_ITEMS_PER_PROCESS_BATCH + 1)}))
        with self.assertRaises(ValueError):
            _parse_startup_content(f"<string>{too_many}</string>".encode())
        normal_json = html.escape(json.dumps({"News": [{"NewsID": 1}]}))
        parsed = _parse_startup_content(f"<string>{normal_json}</string>".encode())
        self.assertEqual([{"NewsID": 1}], parsed)


if __name__ == "__main__":
    unittest.main()
