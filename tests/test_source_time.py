import html
import json
import os
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch

from src.api.ws_parser import parse_signalr_frame
from src.core.news_processor import NewsProcessor, _normalized_content
from src.telegram.bot import TelegramRichResult


def item(**overrides):
    payload = {
        "NewsID": 1,
        "Title": "JOLTS headline",
        "Description": "Details",
        "PostedLong": "01 September 2026",
        "PostedShort": "11:03",
        "EURL": "https://example.test/news",
        "Breaking": True,
        "Level": "active",
    }
    payload.update(overrides)
    return payload


class SourceTimeSelectionTests(unittest.TestCase):
    def test_posted_utc_time_is_converted_to_et(self):
        self.assertEqual(
            "10:00 02 September 2026",
            _normalized_content(
                item(PostedLong="02 September 2026", PostedShort="14:00")
            )["source_time"],
        )

    def test_posted_utc_winter_time_uses_est(self):
        self.assertEqual(
            "09:00 02 January 2026",
            _normalized_content(
                item(PostedLong="02 January 2026", PostedShort="14:00")
            )["source_time"],
        )

    def test_posted_utc_seconds_are_converted_to_et(self):
        self.assertEqual(
            "10:00:27 02 September 2026",
            _normalized_content(
                item(PostedLong="02 September 2026", PostedShort="14:00:27")
            )["source_time"],
        )
    def test_iso_with_milliseconds_z_and_offset_preserves_seconds(self):
        self.assertEqual(
            "07:03:27 01 September 2026",
            _normalized_content(item(DatePublished="2026-09-01T11:03:27.456Z"))["source_time"],
        )
        self.assertEqual(
            "07:03:27 01 September 2026",
            _normalized_content(item(DatePublished="2026-09-01T07:03:27.456-04:00"))["source_time"],
        )

    def test_epoch_seconds_and_milliseconds_are_converted_to_et(self):
        self.assertEqual(
            "07:03:27 01 September 2026",
            _normalized_content(item(timestamp=1788260607))["source_time"],
        )
        self.assertEqual(
            "07:03:27 01 September 2026",
            _normalized_content(item(timestamp=1788260607456))["source_time"],
        )

    def test_date_only_posted_long_combines_with_precise_or_minute_posted_short(self):
        self.assertEqual(
            "07:03 01 September 2026",
            _normalized_content(item())["source_time"],
        )
        self.assertEqual(
            "07:03:27 01 September 2026",
            _normalized_content(item(PostedShort="11:03:27"))["source_time"],
        )

    def test_seconds_win_over_field_order_and_minute_precision(self):
        self.assertEqual(
            "07:03:27 01 September 2026",
            _normalized_content(
                item(PostedLong="11:03 01 September 2026", PostedShort="11:03:27")
            )["source_time"],
        )

    def test_naive_iso_is_not_treated_as_utc(self):
        self.assertEqual(
            "07:03 01 September 2026",
            _normalized_content(
                item(DatePublished="2026-09-01T11:03:27", PostedShort="11:03")
            )["source_time"],
        )

    def test_missing_or_invalid_date_published_falls_back_without_inventing_seconds(self):
        self.assertEqual(
            "07:03 01 September 2026",
            _normalized_content(item(DatePublished="not-a-time"))["source_time"],
        )
        self.assertEqual(
            "07:03 01 September 2026",
            _normalized_content(item(DatePublished="2026-09-01"))["source_time"],
        )

    def test_no_time_fields_preserves_previous(self):
        sparse = {"NewsID": 1, "Title": "Updated"}
        self.assertEqual(
            "09:08:07 31 August 2026",
            _normalized_content(sparse, {"source_time": "09:08:07 31 August 2026"})["source_time"],
        )

    def test_signalr_parser_passes_timestamp_fields_through_unchanged(self):
        source = item(DatePublished="2026-09-01T11:03:27.456Z")
        envelope = {"M": [{"M": "sendUpdates", "A": [json.dumps([source])]}]}
        parsed = parse_signalr_frame(json.dumps(envelope))
        self.assertEqual(source["DatePublished"], parsed[0]["DatePublished"])
        self.assertEqual(source["PostedShort"], parsed[0]["PostedShort"])


class SourceTimePersistenceAndRenderingTests(unittest.IsolatedAsyncioTestCase):
    async def test_precise_time_is_stored_archived_and_rendered_in_classic_output(self):
        sent = AsyncMock(return_value=[101])
        archive = Mock(return_value=[{}])
        with tempfile.TemporaryDirectory() as tempdir, patch(
            "src.core.news_processor.tg_send_group", new=sent
        ), patch("src.core.news_processor.save_news_items_batch", new=archive), patch(
            "src.core.news_processor.save_breaking_item", return_value=True
        ):
            processor = NewsProcessor(state_path=os.path.join(tempdir, "state.json"))
            await processor.process([item(DatePublished="2026-09-01T11:03:27.456Z")])

        expected = "07:03:27 01 September 2026"
        self.assertEqual(expected, processor._state["1"]["source_time"])
        archive_call = archive.call_args
        self.assertIsNotNone(archive_call)
        if archive_call is None:
            return
        self.assertEqual(expected, archive_call.args[0][0]["time"])
        send_call = sent.await_args
        self.assertIsNotNone(send_call)
        if send_call is None:
            return
        rendered = html.unescape(send_call.args[0][0])
        self.assertIn(f"Source time: {expected}", rendered)

    async def test_precise_time_is_forwarded_to_rich_rendering(self):
        rich = AsyncMock(return_value=TelegramRichResult(222))
        with tempfile.TemporaryDirectory() as tempdir, patch(
            "src.core.news_processor.TELEGRAM_RICH_MESSAGES_ENABLED", True
        ), patch("src.core.news_processor.tg_send_rich_message", new=rich), patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock()
        ), patch("src.core.news_processor.save_news_items_batch", return_value=[{}]), patch(
            "src.core.news_processor.save_breaking_item", return_value=True
        ):
            processor = NewsProcessor(state_path=os.path.join(tempdir, "state.json"))
            await processor.process([item(DatePublished="2026-09-01T11:03:27.456Z")])

        rich_call = rich.await_args
        self.assertIsNotNone(rich_call)
        if rich_call is None:
            return
        self.assertEqual("07:03:27 01 September 2026", rich_call.args[2])


class SourceTimeCorrectionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.state_path = os.path.join(self.tempdir.name, "state.json")
        for target in (
            "src.core.news_processor.save_news_items_batch",
            "src.core.news_processor.save_breaking_item",
        ):
            patcher = patch(target, return_value=[{}])
            patcher.start()
            self.addCleanup(patcher.stop)

    async def test_corrected_time_edits_english_footer_once(self):
        send = AsyncMock(return_value=[101])
        edit = AsyncMock(return_value=True)
        with patch("src.core.news_processor.tg_send_group", new=send), patch(
            "src.core.news_processor.tg_edit_message", new=edit
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(DatePublished="2026-09-01T11:03:27.456Z")])
            await processor.process([item(DatePublished="2026-09-01T09:55:04.850Z")])
            await processor.process([item(DatePublished="2026-09-01T09:55:04.850Z")])

        self.assertEqual(1, send.await_count)
        edit.assert_awaited_once()
        rendered = html.unescape(edit.await_args.args[1])
        self.assertIn("Source time: 05:55:04 01 September 2026", rendered)
        self.assertIn("JOLTS headline", rendered)
        self.assertIn("🚨 BREAKING", rendered)
        self.assertEqual(1, processor._state["1"]["revision"])
        self.assertEqual(
            "05:55:04 01 September 2026", processor._state["1"]["telegram_source_time"]
        )

    async def test_corrected_time_keeps_published_translation(self):
        with patch("src.core.news_processor.TRANSLATE_ENABLED", True), patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])
        ), patch("src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=True)):
            processor = NewsProcessor(Mock(submit=Mock(return_value=True)), self.state_path)
            await processor.process([item(DatePublished="2026-09-01T11:03:27.456Z")])
            self.assertTrue(
                await processor.apply_translated_revision("1", 1, 101, "JOLTS 中文译文")
            )

        edit = AsyncMock(return_value=True)
        with patch("src.core.news_processor.TRANSLATE_ENABLED", True), patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock()
        ) as send, patch("src.core.news_processor.tg_edit_message", new=edit):
            await processor.process([item(DatePublished="2026-09-01T09:55:04.850Z")])

        send.assert_not_awaited()
        rendered = html.unescape(edit.await_args.args[1])
        self.assertIn("JOLTS 中文译文", rendered)
        self.assertIn("Source time: 05:55:04 01 September 2026", rendered)
        self.assertNotIn("07:03:27", rendered)

    async def test_translation_uses_current_source_time(self):
        edit = AsyncMock(return_value=True)
        with patch("src.core.news_processor.TRANSLATE_ENABLED", True), patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])
        ), patch("src.core.news_processor.tg_edit_message", new=edit):
            processor = NewsProcessor(Mock(submit=Mock(return_value=True)), self.state_path)
            await processor.process([item(DatePublished="2026-09-01T11:03:27.456Z")])
            self.assertTrue(await processor.apply_translated_revision("1", 1, 101, "译文"))

        rendered = html.unescape(edit.await_args.args[1])
        self.assertIn("Source time: 07:03:27 01 September 2026", rendered)
        self.assertEqual("译文", processor._state["1"]["translation_text"])

    async def test_corrected_time_edits_rich_message_in_place(self):
        rich_edit = AsyncMock(return_value=True)
        with patch("src.core.news_processor.TELEGRAM_RICH_MESSAGES_ENABLED", True), patch(
            "src.core.news_processor.tg_send_rich_message",
            new=AsyncMock(return_value=TelegramRichResult(222)),
        ), patch("src.core.news_processor.tg_send_group", new=AsyncMock()), patch(
            "src.core.news_processor.tg_edit_rich_headline", new=rich_edit
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(DatePublished="2026-09-01T11:03:27.456Z")])
            await processor.process([item(DatePublished="2026-09-01T09:55:04.850Z")])

        rich_edit.assert_awaited_once()
        self.assertEqual(222, rich_edit.await_args.args[0])
        self.assertEqual("05:55:04 01 September 2026", rich_edit.await_args.args[3])

    async def test_multi_message_representation_is_not_refreshed(self):
        edit = AsyncMock(return_value=True)
        with patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101, 102])
        ), patch("src.core.news_processor.tg_edit_message", new=edit):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(DatePublished="2026-09-01T11:03:27.456Z")])
            await processor.process([item(DatePublished="2026-09-01T09:55:04.850Z")])
            await processor.process([item(DatePublished="2026-09-01T09:55:04.850Z")])

        edit.assert_not_awaited()
        self.assertEqual(
            "05:55:04 01 September 2026", processor._state["1"]["telegram_source_time"]
        )

    async def test_failed_edit_is_retried_on_next_sighting(self):
        edit = AsyncMock(side_effect=[False, True])
        with patch("src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])), patch(
            "src.core.news_processor.tg_edit_message", new=edit
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(DatePublished="2026-09-01T11:03:27.456Z")])
            await processor.process([item(DatePublished="2026-09-01T09:55:04.850Z")])
            self.assertEqual(
                "07:03:27 01 September 2026", processor._state["1"]["telegram_source_time"]
            )
            await processor.process([item(DatePublished="2026-09-01T09:55:04.850Z")])

        self.assertEqual(2, edit.await_count)
        self.assertEqual(
            "05:55:04 01 September 2026", processor._state["1"]["telegram_source_time"]
        )


if __name__ == "__main__":
    unittest.main()
