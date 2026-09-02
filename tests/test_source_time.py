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


if __name__ == "__main__":
    unittest.main()
