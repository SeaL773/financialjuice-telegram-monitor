import asyncio
import json
import os
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from src.core.news_processor import NewsProcessor
from src.telegram.bot import (
    TelegramRichResult,
    build_rich_html,
    tg_send_rich_message,
)
from src.translate.queue_worker import TranslationJob, TranslationQueueWorker


def news(description="Description", breaking=True, level="active"):
    return {
        "NewsID": 1,
        "Title": "Title",
        "Description": description,
        "PostedLong": "10:00",
        "EURL": "https://example.test",
        "Breaking": breaking,
        "Level": level,
    }


class CapturingWorker:
    def __init__(self, result=True):
        self.jobs = []
        self.result = result

    def submit(self, job):
        self.jobs.append(job)
        return self.result


class RichTests(unittest.IsolatedAsyncioTestCase):
    tempdir = None
    state_path = ""

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.state_path = os.path.join(self.tempdir.name, "state.json")

    async def test_translation_recovery_and_submit_retry(self):
        worker = CapturingWorker(result=False)
        with patch("src.core.news_processor.TRANSLATE_ENABLED", True), patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])
        ):
            processor = NewsProcessor(worker, self.state_path)
            await processor.process([news()])
            self.assertEqual("pending", processor._state["1"]["translation_status"])
            self.assertEqual(1, len(worker.jobs))
            worker.result = True
            await processor.process([news()])
            await processor.process([news()])
        self.assertEqual(2, len(worker.jobs))

    async def test_translation_apply_marks_applied_and_stale_is_rejected(self):
        worker = CapturingWorker()
        with patch("src.core.news_processor.TRANSLATE_ENABLED", True), patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])
        ), patch("src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=True)):
            processor = NewsProcessor(worker, self.state_path)
            await processor.process([news()])
            self.assertFalse(await processor.apply_translated_revision("1", 2, 101, "stale"))
            self.assertTrue(await processor.apply_translated_revision("1", 1, 101, "译文"))
        self.assertEqual("applied", processor._state["1"]["translation_status"])

    async def test_rich_enabled_success_persists_mode_and_payload(self):
        worker = CapturingWorker()
        with patch("src.core.news_processor.TRANSLATE_ENABLED", False), patch(
            "src.core.news_processor.TELEGRAM_RICH_MESSAGES_ENABLED", True
        ), patch("src.core.news_processor.tg_send_rich_message", new=AsyncMock(return_value=TelegramRichResult(222))), patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock()
        ) as classic:
            processor = NewsProcessor(worker, self.state_path)
            await processor.process([news()])
        classic.assert_not_awaited()
        self.assertEqual("rich", processor._state["1"]["telegram_mode"])
        self.assertEqual(222, processor._state["1"]["telegram_message_id"])
        self.assertEqual(1, processor._state["1"]["notification_revision"])

    async def test_rich_transient_error_does_not_classic_duplicate(self):
        with patch("src.core.news_processor.TELEGRAM_RICH_MESSAGES_ENABLED", True), patch(
            "src.core.news_processor.tg_send_rich_message", new=AsyncMock(return_value=TelegramRichResult(None))
        ), patch("src.core.news_processor.tg_send_group", new=AsyncMock()) as classic:
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([news()])
        classic.assert_not_awaited()
        self.assertEqual(0, processor._state["1"]["notification_revision"])

    async def test_rich_transient_error_retries_identical_payload_and_persists_restart(self):
        with patch("src.core.news_processor.TELEGRAM_RICH_MESSAGES_ENABLED", True), patch(
            "src.core.news_processor.tg_send_rich_message",
            new=AsyncMock(side_effect=[TelegramRichResult(None), TelegramRichResult(223)]),
        ) as rich, patch("src.core.news_processor.tg_send_group", new=AsyncMock()) as classic:
            processor = NewsProcessor(state_path=self.state_path)
            payload = news()
            await processor.process([payload])
            self.assertIsNotNone(processor._state["1"]["pending_rich"])
            restarted = NewsProcessor(state_path=self.state_path)
            await restarted.process([payload])

        self.assertEqual(2, rich.await_count)
        classic.assert_not_awaited()
        self.assertEqual(223, restarted._state["1"]["telegram_message_id"])
        self.assertIsNone(restarted._state["1"]["pending_rich"])
        self.assertEqual(1, restarted._state["1"]["notification_revision"])

    async def test_rich_long_content_uses_classic(self):
        with patch("src.core.news_processor.TELEGRAM_RICH_MESSAGES_ENABLED", True), patch(
            "src.core.news_processor.tg_send_rich_message", new=AsyncMock(return_value=TelegramRichResult(None, True))
        ), patch("src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])) as classic:
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([news("x" * 130000)])
        classic.assert_awaited_once()
        self.assertEqual("classic", processor._state["1"]["telegram_mode"])


class RichTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_payload_is_nested_object_and_normalized(self):
        response = httpx.Response(200, json={"ok": True, "result": {"message_id": 7}})
        captured = {}

        class Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def post(self, _url, **kwargs):
                captured.update(kwargs)
                return response

        with patch("src.telegram.bot.TG_BOT_TOKEN", "token"), patch(
            "src.telegram.bot.TG_CHAT_ID", "chat"
        ), patch("src.telegram.bot.httpx.AsyncClient", Client):
            result = await tg_send_rich_message(
                "<b>Title</b>",
                "<ul><li>one</li><li><script>x</script>two</li></ul><svg>bad</svg>",
                "10:00",
            )
        self.assertEqual(7, result.message_id)
        rich_message = captured["json"]["rich_message"]
        self.assertIsInstance(rich_message, dict)
        rich_html = rich_message["html"]
        self.assertNotIn("<ul>", rich_html)
        self.assertNotIn("<script>", rich_html)
        self.assertNotIn("TradingView", rich_html)
        self.assertIn("• one", rich_html)
        self.assertIn("Source time: 10:00", rich_html)

    async def test_clear_http_errors_fallback_but_transient_errors_retry(self):
        class Client:
            status = 400

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def post(self, *_args, **_kwargs):
                return httpx.Response(self.status, json={"ok": False})

        with patch("src.telegram.bot.TG_BOT_TOKEN", "token"), patch(
            "src.telegram.bot.TG_CHAT_ID", "chat"
        ), patch("src.telegram.bot.httpx.AsyncClient", Client):
            self.assertTrue((await tg_send_rich_message("t", "d", "s")).fallback_classic)
            Client.status = 429
            self.assertFalse((await tg_send_rich_message("t", "d", "s")).fallback_classic)

    async def test_malformed_or_incomplete_success_response_stays_retryable(self):
        class Response:
            status_code = 200

            def __init__(self, data=None, error=None):
                self.data = data
                self.error = error

            def json(self):
                if self.error is not None:
                    raise self.error
                return self.data

        class Client:
            response = Response(error=ValueError("malformed"))

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_args):
                return False

            async def post(self, *_args, **_kwargs):
                return self.response

        with patch("src.telegram.bot.TG_BOT_TOKEN", "token"), patch(
            "src.telegram.bot.TG_CHAT_ID", "chat"
        ), patch("src.telegram.bot.httpx.AsyncClient", Client):
            malformed = await tg_send_rich_message("t", "d", "s")
            self.assertIsNone(malformed.message_id)
            self.assertFalse(malformed.fallback_classic)

            for result in ({"ok": True}, {"ok": True, "result": {}}, {"ok": True, "result": {"message_id": "7"}}):
                Client.response = Response(data=result)
                incomplete = await tg_send_rich_message("t", "d", "s")
                self.assertIsNone(incomplete.message_id)
                self.assertFalse(incomplete.fallback_classic)


class RichBuilderTests(unittest.TestCase):
    def test_normalizer_limit_becomes_explicit_error(self):
        with self.assertRaises(ValueError):
            build_rich_html("title", "<ul>" * 1100, "time")


class TranslationWorkerTests(unittest.IsolatedAsyncioTestCase):
    def _job(self, apply_translation, finish_attempt):
        return TranslationJob(
            news_id="1", revision=1, message_id=101, original_text="original",
            source_time="10:00", prefix="UPDATE\n", apply_translation=apply_translation,
            finish_attempt=finish_attempt,
        )

    async def test_translate_exception_finishes_once_and_worker_remains_usable(self):
        finished = []
        worker = TranslationQueueWorker()
        with patch("src.translate.queue_worker.translate", new=AsyncMock(side_effect=[RuntimeError("x"), "translated"])):
            first = self._job(AsyncMock(side_effect=RuntimeError("apply")), lambda *args: finished.append(args))
            worker.submit(first)
            await worker.start()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            second = self._job(AsyncMock(return_value=False), lambda *args: finished.append(args))
            worker.submit(second)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            await worker.stop()
        self.assertEqual(2, len(finished))
        self.assertFalse(finished[0][-1])
        self.assertFalse(finished[1][-1])

    async def test_apply_exception_finishes_once(self):
        finished = []
        worker = TranslationQueueWorker()
        with patch("src.translate.queue_worker.translate", new=AsyncMock(return_value="translated")):
            worker.submit(self._job(AsyncMock(side_effect=RuntimeError("apply")), lambda *args: finished.append(args)))
            await worker.start()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            await worker.stop()
        self.assertEqual(1, len(finished))
        self.assertFalse(finished[0][-1])


if __name__ == "__main__":
    unittest.main()
