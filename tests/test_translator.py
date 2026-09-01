import importlib
import json
import os
import unittest
from types import TracebackType
from typing import Self, cast
from unittest.mock import patch

import httpx

from src.core.security_limits import MAX_TRANSLATION_HTTP_RESPONSE_BYTES
from src.translate import translator


class FakeStreamingResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        chunks: list[bytes] | None = None,
        headers: dict[str, str] | None = None,
    ):
        self.status_code = status_code
        self.headers = httpx.Headers(headers)
        self.chunks = chunks or []
        self.iterated_chunks = 0
        self.closed = False

    async def aiter_bytes(self):
        for chunk in self.chunks:
            self.iterated_chunks += 1
            yield chunk


class FakeStreamContext:
    def __init__(self, response: FakeStreamingResponse, error: Exception | None):
        self.response = response
        self.error = error

    async def __aenter__(self) -> FakeStreamingResponse:
        if self.error is not None:
            raise self.error
        return self.response

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        self.response.closed = True
        return False


class FakeAsyncClient:
    response: FakeStreamingResponse | None = None
    error: Exception | None = None
    timeout: float | None = None
    request: dict[str, object] | None = None

    def __init__(self, timeout: float):
        type(self).timeout = timeout

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        return False

    def stream(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str],
        json: dict[str, object],
    ) -> FakeStreamContext:
        type(self).request = {
            "method": method,
            "url": url,
            "headers": headers,
            "json": json,
        }
        if type(self).response is None:
            raise AssertionError("FakeAsyncClient.response was not configured")
        response = type(self).response
        if response is None:
            raise AssertionError("FakeAsyncClient.response was not configured")
        return FakeStreamContext(response, type(self).error)


def api_response(
    data: object = None,
    *,
    status_code: int = 200,
    content: bytes | None = None,
    chunks: list[bytes] | None = None,
    headers: dict[str, str] | None = None,
) -> FakeStreamingResponse:
    if chunks is None:
        payload = content if content is not None else json.dumps(data).encode("utf-8")
        chunks = [payload]
    return FakeStreamingResponse(
        status_code=status_code,
        chunks=chunks,
        headers=headers,
    )


class TranslatorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        FakeAsyncClient.response = api_response(
            {"choices": [{"message": {"content": "  测试译文  "}}]}
        )
        FakeAsyncClient.error = None
        FakeAsyncClient.timeout = None
        FakeAsyncClient.request = None
        client_patch = patch(
            "src.translate.translator.httpx.AsyncClient", FakeAsyncClient
        )
        _ = client_patch.start()
        self.addCleanup(client_patch.stop)

    def _captured_request(self) -> dict[str, object]:
        request = FakeAsyncClient.request
        self.assertIsNotNone(request)
        return request or {}

    def _captured_headers(self) -> dict[str, str]:
        return cast(dict[str, str], self._captured_request()["headers"])

    def _captured_body(self) -> dict[str, object]:
        return cast(dict[str, object], self._captured_request()["json"])

    async def test_success_builds_endpoint_messages_and_tuning(self):
        with patch.multiple(
            translator,
            TRANSLATE_BASE_URL="https://provider.test/v1/",
            TRANSLATE_API_KEY="secret",
            TRANSLATE_MODEL="model-a",
            TRANSLATE_TIMEOUT=12.5,
            TRANSLATE_MAX_TOKENS=321,
            TRANSLATE_TEMPERATURE=0.2,
            TRANSLATE_HEADERS={},
            TRANSLATE_EXTRA_BODY={},
            TRANSLATE_SYSTEM_PROMPT="system prompt",
        ):
            result = await translator.translate("  headline  ")

        self.assertEqual("测试译文", result)
        self.assertEqual(12.5, FakeAsyncClient.timeout)
        request = self._captured_request()
        self.assertEqual(
            "https://provider.test/v1/chat/completions", request["url"]
        )
        self.assertEqual("POST", request["method"])
        headers = self._captured_headers()
        body = self._captured_body()
        self.assertEqual("Bearer secret", headers["Authorization"])
        self.assertEqual(
            [
                {"role": "system", "content": "system prompt"},
                {"role": "user", "content": "headline"},
            ],
            body["messages"],
        )
        self.assertEqual("model-a", body["model"])
        self.assertEqual(321, body["max_tokens"])
        self.assertEqual(0.2, body["temperature"])

    async def test_empty_key_omits_authorization_for_local_server(self):
        with patch.multiple(
            translator,
            TRANSLATE_API_KEY="",
            TRANSLATE_HEADERS={"X-Local": "yes"},
        ):
            result = await translator.translate("headline")

        self.assertEqual("测试译文", result)
        headers = self._captured_headers()
        self.assertNotIn("Authorization", headers)
        self.assertEqual("yes", headers["X-Local"])

    async def test_custom_headers_are_case_insensitive_and_canonical_values_win(self):
        with patch.multiple(
            translator,
            TRANSLATE_API_KEY="canonical-key",
            TRANSLATE_HEADERS={
                "authorization": "Bearer lowercase-key",
                "AuThOrIzAtIoN": "Bearer mixed-key",
                "content-type": "text/plain",
                "CONTENT-TYPE": "application/xml",
                "HTTP-Referer": "https://example.test",
            },
        ):
            _ = await translator.translate("headline")

        headers = self._captured_headers()
        self.assertEqual("Bearer canonical-key", headers["Authorization"])
        self.assertEqual("application/json", headers["Content-Type"])
        self.assertEqual("https://example.test", headers["HTTP-Referer"])
        self.assertEqual(
            1, sum(name.casefold() == "authorization" for name in headers)
        )
        self.assertEqual(
            1, sum(name.casefold() == "content-type" for name in headers)
        )

    async def test_custom_authorization_allowed_when_key_empty(self):
        with patch.multiple(
            translator,
            TRANSLATE_API_KEY="",
            TRANSLATE_HEADERS={
                "authorization": "Token first",
                "AuThOrIzAtIoN": "Token ignored",
            },
        ):
            _ = await translator.translate("headline")

        headers = self._captured_headers()
        self.assertEqual("Token first", headers["authorization"])
        self.assertEqual(
            1, sum(name.casefold() == "authorization" for name in headers)
        )

    async def test_case_insensitive_duplicate_custom_headers_keep_first(self):
        with patch.multiple(
            translator,
            TRANSLATE_API_KEY="",
            TRANSLATE_HEADERS={
                "X-Custom": "first",
                "x-custom": "second",
                "X-Other": "preserved",
            },
        ):
            _ = await translator.translate("headline")

        headers = self._captured_headers()
        self.assertEqual("first", headers["X-Custom"])
        self.assertNotIn("x-custom", headers)
        self.assertEqual("preserved", headers["X-Other"])

    async def test_extra_body_is_merged_but_canonical_fields_win(self):
        with patch.multiple(
            translator,
            TRANSLATE_MODEL="canonical-model",
            TRANSLATE_EXTRA_BODY={
                "reasoning_effort": "low",
                "model": "wrong-model",
                "max_tokens": 999,
            },
            TRANSLATE_MAX_TOKENS=123,
        ):
            _ = await translator.translate("headline")

        body = self._captured_body()
        self.assertEqual("low", body["reasoning_effort"])
        self.assertEqual("canonical-model", body["model"])
        self.assertEqual(123, body["max_tokens"])

    async def test_timeout_and_network_error_return_none(self):
        FakeAsyncClient.error = httpx.ReadTimeout("slow")
        self.assertIsNone(await translator.translate("headline"))

        FakeAsyncClient.error = httpx.ConnectError("offline")
        self.assertIsNone(await translator.translate("headline"))

    async def test_non_success_response_returns_none(self):
        FakeAsyncClient.response = api_response(
            {"error": {"message": "private provider detail"}}, status_code=429
        )
        self.assertIsNone(await translator.translate("headline"))
        self.assertTrue(FakeAsyncClient.response.closed)
        self.assertEqual(0, FakeAsyncClient.response.iterated_chunks)

    async def test_content_length_over_limit_returns_none(self):
        response = api_response(
            {"choices": [{"message": {"content": "ignored"}}]},
            headers={"Content-Length": str(MAX_TRANSLATION_HTTP_RESPONSE_BYTES + 1)},
        )
        FakeAsyncClient.response = response
        self.assertIsNone(await translator.translate("headline"))
        self.assertEqual(0, response.iterated_chunks)
        self.assertTrue(response.closed)

    async def test_streaming_body_over_limit_stops_before_remaining_chunks(self):
        response = api_response(
            chunks=[
                b"x" * MAX_TRANSLATION_HTTP_RESPONSE_BYTES,
                b"overflow",
                b"must-not-be-read",
            ]
        )
        FakeAsyncClient.response = response
        self.assertIsNone(await translator.translate("headline"))
        self.assertEqual(2, response.iterated_chunks)
        self.assertTrue(response.closed)

    async def test_malformed_or_empty_response_returns_none(self):
        invalid_responses = [
            api_response(content=b"not-json"),
            api_response({}),
            api_response({"choices": []}),
            api_response({"choices": [{}]}),
            api_response({"choices": [{"message": {}}]}),
            api_response({"choices": [{"message": {"content": "  "}}]}),
        ]
        for response in invalid_responses:
            with self.subTest(first_chunk=response.chunks[0][:30]):
                FakeAsyncClient.response = response
                self.assertIsNone(await translator.translate("headline"))
                self.assertTrue(response.closed)

    async def test_blank_input_does_not_make_request(self):
        self.assertIsNone(await translator.translate("  "))
        self.assertIsNone(FakeAsyncClient.request)


class TranslationConfigTests(unittest.TestCase):
    def test_public_defaults_disable_translation_and_tolerate_thread_id(self):
        with patch.dict(
            os.environ,
            {
                "FJ_TRANSLATE_ENABLED": "false",
                "TG_THREAD_ID": "your_thread_id",
            },
            clear=False,
        ):
            config = importlib.reload(importlib.import_module("src.core.config"))
        self.assertFalse(config.TRANSLATE_ENABLED)
        self.assertIsNone(config.TG_THREAD_ID)

    CONFIG_NAMES: set[str] = {
        "FJ_TRANSLATE_API_KEY",
        "FJ_TRANSLATE_API_KEY_FILE",
        "FJ_TRANSLATE_BASE_URL",
        "FJ_TRANSLATE_MODEL",
        "FJ_TRANSLATE_TIMEOUT",
        "FJ_TRANSLATE_MAX_TOKENS",
        "FJ_TRANSLATE_TEMPERATURE",
        "FJ_TRANSLATE_HEADERS_JSON",
        "FJ_TRANSLATE_EXTRA_BODY_JSON",
        "KIMI_API_KEY",
        "KIMI_BASE_URL",
        "KIMI_MODEL",
    }

    def _reload_config(self, values: dict[str, str]):
        from src.core import config

        clean_env = {key: value for key, value in os.environ.items() if key not in self.CONFIG_NAMES}
        clean_env.update(values)
        with patch.dict(os.environ, clean_env, clear=True):
            return importlib.reload(config)

    def tearDown(self):
        from src.core import config

        _ = importlib.reload(config)

    def test_kimi_aliases_are_fallbacks(self):
        config = self._reload_config(
            {
                "KIMI_API_KEY": "legacy-key",
                "KIMI_BASE_URL": "https://legacy.test/v1",
                "KIMI_MODEL": "legacy-model",
            }
        )
        self.assertEqual("legacy-key", config.TRANSLATE_API_KEY)
        self.assertEqual("https://legacy.test/v1", config.TRANSLATE_BASE_URL)
        self.assertEqual("legacy-model", config.TRANSLATE_MODEL)

    def test_canonical_values_override_kimi_aliases(self):
        config = self._reload_config(
            {
                "FJ_TRANSLATE_API_KEY": "canonical-key",
                "FJ_TRANSLATE_BASE_URL": "https://canonical.test/v1",
                "FJ_TRANSLATE_MODEL": "canonical-model",
                "KIMI_API_KEY": "legacy-key",
                "KIMI_BASE_URL": "https://legacy.test/v1",
                "KIMI_MODEL": "legacy-model",
            }
        )
        self.assertEqual("canonical-key", config.TRANSLATE_API_KEY)
        self.assertEqual("https://canonical.test/v1", config.TRANSLATE_BASE_URL)
        self.assertEqual("canonical-model", config.TRANSLATE_MODEL)

    def test_json_and_numeric_config_validation(self):
        config = self._reload_config(
            {
                "FJ_TRANSLATE_TIMEOUT": "not-a-number",
                "FJ_TRANSLATE_MAX_TOKENS": "0",
                "FJ_TRANSLATE_TEMPERATURE": "nan",
                "FJ_TRANSLATE_HEADERS_JSON": json.dumps(
                    {"X-Valid": "yes", "X-Invalid": 1}
                ),
                "FJ_TRANSLATE_EXTRA_BODY_JSON": "[]",
            }
        )
        self.assertEqual(60.0, config.TRANSLATE_TIMEOUT)
        self.assertEqual(256, config.TRANSLATE_MAX_TOKENS)
        self.assertEqual(0.6, config.TRANSLATE_TEMPERATURE)
        self.assertEqual({}, config.TRANSLATE_HEADERS)
        self.assertEqual({"thinking": {"type": "disabled"}}, config.TRANSLATE_EXTRA_BODY)

        config = self._reload_config(
            {
                "FJ_TRANSLATE_TIMEOUT": "10.5",
                "FJ_TRANSLATE_MAX_TOKENS": "512",
                "FJ_TRANSLATE_TEMPERATURE": "0.1",
                "FJ_TRANSLATE_HEADERS_JSON": '{"X-Test":"value"}',
                "FJ_TRANSLATE_EXTRA_BODY_JSON": '{"top_p":0.9}',
            }
        )
        self.assertEqual(10.5, config.TRANSLATE_TIMEOUT)
        self.assertEqual(512, config.TRANSLATE_MAX_TOKENS)
        self.assertEqual(0.6, config.TRANSLATE_TEMPERATURE)
        self.assertEqual({"X-Test": "value"}, config.TRANSLATE_HEADERS)
        self.assertEqual(
            {"top_p": 0.9, "thinking": {"type": "disabled"}},
            config.TRANSLATE_EXTRA_BODY,
        )


if __name__ == "__main__":
    unittest.main()
