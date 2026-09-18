import asyncio
import html
import json
import os
import tempfile
import unittest
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

from src.archive.storage import save_breaking_item, save_news_items_batch
from src.core.news_processor import (
    MAX_DESCRIPTION_LENGTH,
    MAX_ITEMS_PER_PROCESS_BATCH,
    MAX_LEVEL_LENGTH,
    MAX_PENDING_ARCHIVE_RECORDS,
    MAX_PROCESSOR_STATE_BYTES,
    MAX_SOURCE_METHOD_LENGTH,
    MAX_TRANSLATION_INPUT_LENGTH,
    NewsProcessor,
)
from src.telegram.bot import tg_send_group, tg_send_group_resumable
from src.telegram.rendering import (
    MAX_RENDERED_HTML_LENGTH,
    MAX_TELEGRAM_CHUNKS,
    TELEGRAM_TEXT_LIMIT,
    TelegramRenderLimitError,
    escape_html,
    render_message_chunks,
)
from src.translate.queue_worker import TranslationJob, TranslationQueueWorker


def item(
    news_id: int | str = 1,
    title: str = "Title",
    description: str = "Description",
    source_time: str = "10:00 01 January 2026",
    breaking: bool = True,
    level: str = "active",
) -> dict[str, Any]:
    return {
        "NewsID": news_id,
        "Title": title,
        "Description": description,
        "PostedLong": source_time,
        "EURL": "https://example.test/news",
        "Breaking": breaking,
        "Level": level,
        "__ws_method__": "sendUpdates",
    }


class ProcessorTestCase(unittest.IsolatedAsyncioTestCase):
    tempdir: Any = None
    state_path = ""
    archive_send: Any = None
    archive_breaking: Any = None

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.state_path = os.path.join(self.tempdir.name, "state.json")
        self.archive_send = patch(
            "src.core.news_processor.save_news_items_batch", return_value=[{}]
        )
        self.archive_breaking = patch(
            "src.core.news_processor.save_breaking_item", return_value=True
        )
        self.archive_send.start()
        self.archive_breaking.start()
        self.addCleanup(self.archive_send.stop)
        self.addCleanup(self.archive_breaking.stop)

    async def test_new_breaking_send_contains_full_content_and_source_time(self):
        sent = []

        async def fake_send_group(texts, important=False, **_kwargs):
            sent.extend(texts)
            return [101]

        with patch("src.core.news_processor.tg_send_group", side_effect=fake_send_group):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(title="A < B", description="Full & expanded")])

        self.assertEqual(101, processor._state["1"]["telegram_message_id"])
        self.assertEqual(1, processor._state["1"]["notification_revision"])
        self.assertIn("A &lt; B", sent[0])
        self.assertIn("Full &amp; expanded", sent[0])
        self.assertIn("05:00 01 January 2026", sent[0])

    async def test_same_id_description_expansion_edits_existing(self):
        with patch("src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])), patch(
            "src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=True)
        ) as edit:
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="Short")])
            await processor.process([item(description="Short plus complete expansion")])

        edit.assert_awaited_once()
        edit_call = edit.await_args
        self.assertIsNotNone(edit_call)
        if edit_call is not None:
            self.assertIn("complete expansion", edit_call.args[1])
        self.assertEqual(2, processor._state["1"]["revision"])
        self.assertEqual(2, processor._state["1"]["notification_revision"])

    async def test_identical_payload_no_action_and_int_string_id_dedup(self):
        send = AsyncMock(return_value=[101])
        edit = AsyncMock(return_value=True)
        with patch("src.core.news_processor.tg_send_group", new=send), patch(
            "src.core.news_processor.tg_edit_message", new=edit
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(news_id=7)])
            await processor.process([item(news_id="7")])

        self.assertEqual(1, send.await_count)
        edit.assert_not_awaited()
        self.assertEqual(1, processor._state["7"]["revision"])

    async def test_breaking_upgrade_sends(self):
        send = AsyncMock(return_value=[202])
        with patch("src.core.news_processor.tg_send_group", new=send):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(breaking=False, level="news-general")])
            await processor.process([item(breaking=True, level="active")])

        send.assert_awaited_once()
        send_call = send.await_args
        self.assertIsNotNone(send_call)
        if send_call is not None:
            self.assertIn("UPGRADED", send_call.args[0][0])
        self.assertEqual("upgrade", processor._state["1"]["notification_kind"])

    async def test_restart_recovery_avoids_duplicate_send(self):
        send = AsyncMock(return_value=[303])
        with patch("src.core.news_processor.tg_send_group", new=send):
            first = NewsProcessor(state_path=self.state_path)
            await first.process([item()])
            second = NewsProcessor(state_path=self.state_path)
            await second.process([item()])

        self.assertEqual(1, send.await_count)
        self.assertEqual(303, second._state["1"]["telegram_message_id"])

    async def test_corrupt_state_falls_back_to_empty(self):
        with open(self.state_path, "w", encoding="utf-8") as state_file:
            state_file.write("{broken")
        send = AsyncMock(return_value=[404])
        with patch("src.core.news_processor.tg_send_group", new=send):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item()])
        self.assertIn("1", processor._state)

    async def test_edit_failure_sends_update_replacement_and_tracks_first_id(self):
        send = AsyncMock(side_effect=[[101], [501, 502]])
        with patch("src.core.news_processor.tg_send_group", new=send), patch(
            "src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=False)
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="old")])
            await processor.process([item(description="new")])

        self.assertEqual(501, processor._state["1"]["telegram_message_id"])
        send_call = send.await_args
        self.assertIsNotNone(send_call)
        if send_call is not None:
            self.assertIn("UPDATE", send_call.args[0][0])

    async def test_long_revision_preserved_across_numbered_chunks(self):
        long_description = "line<&>\n" * 1200
        sent_groups = []

        async def fake_send_group(texts, important=False, **_kwargs):
            sent_groups.append(texts)
            base_id = 100 * len(sent_groups)
            return [base_id + index for index in range(1, len(texts) + 1)]

        with patch("src.core.news_processor.tg_send_group", side_effect=fake_send_group), patch(
            "src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=True)
        ) as edit:
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="short")])
            await processor.process([item(description=long_description)])

        edit.assert_not_awaited()
        update_chunks = sent_groups[-1]
        self.assertGreater(len(update_chunks), 1)
        self.assertTrue(all("UPDATE (" in chunk for chunk in update_chunks))
        decoded = html.unescape("".join(chunk.split("</b>\n", 1)[1] for chunk in update_chunks))
        self.assertIn(long_description, decoded)
        self.assertTrue(processor._state["1"]["telegram_is_group"])

    async def test_persisted_pending_revision_retries_after_restart(self):
        send = AsyncMock(return_value=[101])
        with patch("src.core.news_processor.tg_send_group", new=send), patch(
            "src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=False)
        ):
            first = NewsProcessor(state_path=self.state_path)
            await first.process([item(description="one")])
            send.return_value = []
            await first.process([item(description="two")])
            self.assertEqual(1, first._state["1"]["notification_revision"])

            send.return_value = [202]
            second = NewsProcessor(state_path=self.state_path)
            await second.process([item(description="two")])

        self.assertEqual(2, second._state["1"]["notification_revision"])
        self.assertEqual(202, second._state["1"]["telegram_message_id"])

    async def test_initial_send_failure_retries_identical_payload(self):
        send = AsyncMock(side_effect=[[], [303]])
        with patch("src.core.news_processor.tg_send_group", new=send):
            processor = NewsProcessor(state_path=self.state_path)
            payload = item()
            await processor.process([payload])
            self.assertEqual(0, processor._state["1"]["notification_revision"])
            await processor.process([payload])

        self.assertEqual(2, send.await_count)
        self.assertEqual(1, processor._state["1"]["notification_revision"])
        self.assertEqual(303, processor._state["1"]["telegram_message_id"])

    async def test_partial_group_failure_does_not_mark_revision_delivered(self):
        long_description = "long line\n" * 1000
        send = AsyncMock(side_effect=[[101], []])
        with patch("src.core.news_processor.tg_send_group", new=send), patch(
            "src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=True)
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="short")])
            await processor.process([item(description=long_description)])

        self.assertEqual(2, processor._state["1"]["revision"])
        self.assertEqual(1, processor._state["1"]["notification_revision"])
        self.assertEqual(101, processor._state["1"]["telegram_message_id"])

    async def test_short_revision_after_multi_chunk_sends_new_group_without_edit(self):
        send = AsyncMock(side_effect=[[101], [201, 202, 203], [301]])
        edit = AsyncMock(return_value=True)
        with patch("src.core.news_processor.tg_send_group", new=send), patch(
            "src.core.news_processor.tg_edit_message", new=edit
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="short")])
            await processor.process([item(description="long\n" * 1200)])
            self.assertTrue(processor._state["1"]["telegram_is_group"])
            await processor.process([item(description="short again")])

        edit.assert_not_awaited()
        self.assertEqual(301, processor._state["1"]["telegram_message_id"])
        self.assertFalse(processor._state["1"]["telegram_is_group"])

    async def test_translation_edit_is_serialized_before_newer_english_revision(self):
        send = AsyncMock(return_value=[101])
        edit_started = asyncio.Event()
        release_edit = asyncio.Event()
        edited_texts = []

        async def controlled_edit(_message_id, text):
            edited_texts.append(text)
            if "旧译文" in text:
                edit_started.set()
                await release_edit.wait()
            return True

        with patch("src.core.news_processor.tg_send_group", new=send), patch(
            "src.core.news_processor.tg_edit_message", side_effect=controlled_edit
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="revision one")])
            translation = asyncio.create_task(
                processor.apply_translated_revision(
                    "1", 1, 101, "revision one\n旧译文\nSource time: source"
                )
            )
            await edit_started.wait()
            newer = asyncio.create_task(
                processor.process([item(description="revision two")])
            )
            await asyncio.sleep(0)
            self.assertFalse(newer.done())
            release_edit.set()
            self.assertTrue(await translation)
            await newer

        self.assertEqual(2, len(edited_texts))
        self.assertIn("旧译文", edited_texts[0])
        self.assertIn("revision two", edited_texts[1])
        self.assertEqual(2, processor._state["1"]["notification_revision"])

    async def test_over_limit_description_rejected_without_side_effects(self):
        send = AsyncMock(return_value=[101])
        archive = AsyncMock()
        breaking_archive = AsyncMock()
        with patch("src.core.news_processor.tg_send_group", new=send), patch(
            "src.core.news_processor.save_news_items_batch", new=archive
        ), patch(
            "src.core.news_processor.save_breaking_item", new=breaking_archive
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([
                item(news_id="oversized", description="x" * (MAX_DESCRIPTION_LENGTH + 1))
            ])

        self.assertNotIn("oversized", processor._state)
        send.assert_not_awaited()
        archive.assert_not_awaited()
        breaking_archive.assert_not_awaited()

    async def test_over_limit_translation_skipped_with_complete_english_preserved(self):
        class CapturingWorker:
            def __init__(self):
                self.jobs = []

            def submit(self, job):
                self.jobs.append(job)
                return True

        worker = CapturingWorker()
        sent_groups = []

        async def complete_send(texts, important=False, **_kwargs):
            sent_groups.append(texts)
            return list(range(1, len(texts) + 1))

        description = "e" * (MAX_TRANSLATION_INPUT_LENGTH + 1)
        with patch("src.core.news_processor.tg_send_group", side_effect=complete_send):
            processor = NewsProcessor(translation_worker=worker, state_path=self.state_path)
            await processor.process([item(description=description)])

        self.assertEqual([], worker.jobs)
        recovered = html.unescape(
            "".join(
                chunk.split("</b>\n", 1)[1] if "</b>\n" in chunk else chunk
                for chunk in sent_groups[0]
            )
        )
        self.assertIn(description, recovered)
        self.assertEqual(1, processor._state["1"]["notification_revision"])

    async def test_normal_accepted_large_content_is_fully_preserved(self):
        description = ("earnings line <&>\n" * 8_000)[:200_000]
        sent_groups = []

        async def complete_send(texts, important=False, **_kwargs):
            sent_groups.append(texts)
            return list(range(1, len(texts) + 1))

        with patch("src.core.news_processor.tg_send_group", side_effect=complete_send):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description=description)])

        self.assertGreater(len(sent_groups[0]), 1)
        recovered = html.unescape(
            "".join(chunk.split("</b>\n", 1)[1] for chunk in sent_groups[0])
        )
        self.assertIn(description, recovered)

    async def test_downgraded_alert_description_revision_still_edits(self):
        send = AsyncMock(return_value=[101])
        edit = AsyncMock(return_value=True)
        with patch("src.core.news_processor.tg_send_group", new=send), patch(
            "src.core.news_processor.tg_edit_message", new=edit
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="breaking")])
            await processor.process([
                item(description="breaking", breaking=False, level="news-general")
            ])
            await processor.process([
                item(description="expanded after downgrade", breaking=False, level="news-general")
            ])

        edit.assert_awaited_once()
        edit_call = edit.await_args
        self.assertIsNotNone(edit_call)
        if edit_call is not None:
            self.assertIn("expanded after downgrade", edit_call.args[1])
        self.assertEqual(2, processor._state["1"]["notification_revision"])

    async def test_sparse_update_preserves_description_time_and_eurl(self):
        send = AsyncMock(return_value=[101])
        edit = AsyncMock(return_value=True)
        with patch("src.core.news_processor.tg_send_group", new=send), patch(
            "src.core.news_processor.tg_edit_message", new=edit
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(title="Old", description="Keep me")])
            await processor.process([{
                "NewsID": 1,
                "Title": "New",
                "Breaking": True,
                "Level": "active",
                "__ws_method__": "sendHeadlineUpdated",
            }])

        state = processor._state["1"]
        self.assertEqual("Keep me", state["description"])
        self.assertEqual("05:00 01 January 2026", state["source_time"])
        self.assertEqual("https://example.test/news", state["eurl"])
        self.assertEqual(2, state["revision"])
        edit_call = edit.await_args
        self.assertIsNotNone(edit_call)
        if edit_call is not None:
            self.assertIn("Keep me", edit_call.args[1])

    async def test_explicit_empty_description_clears_and_revises(self):
        edit = AsyncMock(return_value=True)
        with patch("src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])), patch("src.core.news_processor.tg_edit_message", new=edit):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="Remove me")])
            await processor.process([{
                "NewsID": 1,
                "Description": "",
                "Breaking": True,
                "Level": "active",
            }])

        self.assertEqual("", processor._state["1"]["description"])
        self.assertEqual(2, processor._state["1"]["revision"])
        edit_call = edit.await_args
        self.assertIsNotNone(edit_call)
        if edit_call is not None:
            self.assertNotIn("Remove me", edit_call.args[1])

    async def test_metadata_only_ws_poll_difference_refreshes_footer_without_revising(self):
        send = AsyncMock(return_value=[101])
        edit = AsyncMock(return_value=True)
        with patch("src.core.news_processor.tg_send_group", new=send), patch(
            "src.core.news_processor.tg_edit_message", new=edit
        ), patch(
            "src.core.news_processor.save_news_items_batch", return_value=[{}]
        ) as archive_save:
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(source_time="10:00 01 January 2026")], source="WS")
            archive_save.reset_mock()
            await processor.process([{
                **item(source_time="10:00:27 01 January 2026"),
                "EURL": "https://example.test/poll",
                "__ws_method__": "",
            }], source="POLL")

        self.assertEqual(1, processor._state["1"]["revision"])
        self.assertEqual("05:00:27 01 January 2026", processor._state["1"]["source_time"])
        self.assertEqual("https://example.test/poll", processor._state["1"]["eurl"])
        self.assertEqual("POLL", processor._state["1"]["source_method"])
        self.assertEqual(1, send.await_count)
        edit.assert_awaited_once()
        self.assertIn("05:00:27 01 January 2026", edit.await_args.args[1])
        archive_save.assert_not_called()

    async def test_simultaneous_content_expansion_and_upgrade_has_coherent_markers(self):
        breaking_records = []

        def capture_breaking(record):
            breaking_records.append(record)
            return True

        with patch("src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[202])), patch("src.core.news_processor.save_breaking_item", side_effect=capture_breaking):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([
                item(description="short", breaking=False, level="news-general")
            ])
            await processor.process([
                item(description="expanded", breaking=True, level="active")
            ])

        self.assertEqual(1, len(breaking_records))
        self.assertTrue(breaking_records[0]["upgraded"])
        self.assertTrue(breaking_records[0]["content_revision"])

    async def test_raw_archive_oserror_retries_on_identical_payload(self):
        raw = Mock(side_effect=[OSError("disk"), [{}]])
        with patch("src.core.news_processor.save_news_items_batch", new=raw), patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])
        ):
            processor = NewsProcessor(state_path=self.state_path)
            payload = item(breaking=False, level="news-general")
            await processor.process([payload])
            self.assertEqual(0, processor._state["1"]["raw_archive_revision"])
            await processor.process([payload])
        self.assertEqual(2, raw.call_count)
        self.assertEqual(1, processor._state["1"]["raw_archive_revision"])

    async def test_breaking_archive_failure_retries_independently(self):
        breaking = Mock(side_effect=[OSError("disk"), True])
        send = AsyncMock(return_value=[101])
        with patch("src.core.news_processor.save_breaking_item", new=breaking), patch(
            "src.core.news_processor.tg_send_group", new=send
        ):
            processor = NewsProcessor(state_path=self.state_path)
            payload = item()
            await processor.process([payload])
            self.assertEqual(1, processor._state["1"]["notification_revision"])
            self.assertEqual(0, processor._state["1"]["breaking_archive_revision"])
            await processor.process([payload])
        self.assertEqual(2, breaking.call_count)
        self.assertEqual(1, processor._state["1"]["breaking_archive_revision"])

    async def test_pending_archive_retries_after_restart_and_false_is_idempotent_success(self):
        with patch(
            "src.core.news_processor.save_news_items_batch", side_effect=OSError("disk")
        ):
            first = NewsProcessor(state_path=self.state_path)
            payload = item(breaking=False, level="news-general")
            await first.process([payload])
        with patch("src.core.news_processor.save_news_items_batch", return_value=[]):
            second = NewsProcessor(state_path=self.state_path)
            await second.process([payload])
        self.assertEqual(1, second._state["1"]["raw_archive_revision"])
        self.assertEqual([], second._state["1"]["raw_archive_pending"])

    async def test_initial_group_partial_resume_does_not_resend_first_chunk(self):
        long_description = "initial group\n" * 1000
        calls = []

        async def partial(texts, important=False, start_index=0, existing_message_ids=None, on_progress=None):
            calls.append((start_index, list(existing_message_ids or [])))
            if len(calls) == 1:
                if on_progress is not None:
                    await on_progress([11], 1)
                from src.telegram.bot import TelegramGroupResult
                return TelegramGroupResult([11], 1, False)
            from src.telegram.bot import TelegramGroupResult
            ids = [11] + list(range(12, 12 + len(texts) - 1))
            return TelegramGroupResult(ids, len(texts), True)

        with patch("src.core.news_processor.tg_send_group", side_effect=partial):
            processor = NewsProcessor(state_path=self.state_path)
            payload = item(description=long_description)
            await processor.process([payload])
            await processor.process([payload])
        self.assertEqual((0, []), calls[0])
        self.assertEqual((1, [11]), calls[1])
        self.assertEqual(1, processor._state["1"]["notification_revision"])

    async def test_partial_group_resumes_after_restart(self):
        long_description = "restart group\n" * 1000

        async def first_partial(texts, important=False, start_index=0, existing_message_ids=None, on_progress=None):
            if on_progress is not None:
                await on_progress([21], 1)
            from src.telegram.bot import TelegramGroupResult
            return TelegramGroupResult([21], 1, False)

        with patch("src.core.news_processor.tg_send_group", side_effect=first_partial):
            first = NewsProcessor(state_path=self.state_path)
            payload = item(description=long_description)
            await first.process([payload])

        observed = []

        async def resume(texts, important=False, start_index=0, existing_message_ids=None, on_progress=None):
            observed.append((start_index, list(existing_message_ids or [])))
            from src.telegram.bot import TelegramGroupResult
            ids = [21] + list(range(22, 22 + len(texts) - 1))
            return TelegramGroupResult(ids, len(texts), True)

        with patch("src.core.news_processor.tg_send_group", side_effect=resume):
            second = NewsProcessor(state_path=self.state_path)
            await second.process([payload])
        self.assertEqual([(1, [21])], observed)
        self.assertEqual(1, second._state["1"]["notification_revision"])

    async def test_update_replacement_partial_resume_skips_sent_prefix(self):
        long_description = "updated group\n" * 1000
        calls = []

        async def sender(texts, important=False, start_index=0, existing_message_ids=None, on_progress=None):
            calls.append((start_index, list(existing_message_ids or [])))
            from src.telegram.bot import TelegramGroupResult
            if len(calls) == 1:
                return TelegramGroupResult([101], len(texts), True)
            if len(calls) == 2:
                if on_progress is not None:
                    await on_progress([31], 1)
                return TelegramGroupResult([31], 1, False)
            ids = [31] + list(range(32, 32 + len(texts) - 1))
            return TelegramGroupResult(ids, len(texts), True)

        with patch("src.core.news_processor.tg_send_group", side_effect=sender), patch(
            "src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=True)
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="short")])
            payload = item(description=long_description)
            await processor.process([payload])
            await processor.process([payload])

        self.assertEqual((1, [31]), calls[2])
        self.assertEqual(2, processor._state["1"]["notification_revision"])

    async def test_translation_group_resume_and_newer_revision_abandons_stale_progress(self):
        calls = []

        async def sender(texts, important=False, start_index=0, existing_message_ids=None, on_progress=None):
            calls.append((start_index, list(existing_message_ids or [])))
            from src.telegram.bot import TelegramGroupResult
            if len(calls) == 1:
                return TelegramGroupResult([101], len(texts), True)
            if len(calls) == 2:
                if on_progress is not None:
                    await on_progress([41], 1)
                return TelegramGroupResult([41], 1, False)
            return TelegramGroupResult([201], len(texts), True)

        with patch("src.core.news_processor.tg_send_group", side_effect=sender), patch(
            "src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=False)
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="revision one")])
            translated = "translated\n" * 1000
            self.assertFalse(
                await processor.apply_translated_revision("1", 1, 101, translated)
            )
            pending = processor._state["1"]["pending_group"]
            self.assertEqual("translation", pending["kind"])
            self.assertEqual(1, pending["next_chunk_index"])
            await processor.process([item(description="revision two")])

        self.assertIsNone(processor._state["1"]["pending_group"])
        self.assertEqual(2, processor._state["1"]["notification_revision"])
        self.assertEqual((0, []), calls[2])

    async def test_translation_group_identical_retry_resumes_next_chunk(self):
        calls = []

        async def sender(texts, important=False, start_index=0, existing_message_ids=None, on_progress=None):
            calls.append((start_index, list(existing_message_ids or [])))
            from src.telegram.bot import TelegramGroupResult
            if len(calls) == 1:
                return TelegramGroupResult([101], len(texts), True)
            if len(calls) == 2:
                if on_progress is not None:
                    await on_progress([51], 1)
                return TelegramGroupResult([51], 1, False)
            ids = [51] + list(range(52, 52 + len(texts) - 1))
            return TelegramGroupResult(ids, len(texts), True)

        with patch("src.core.news_processor.tg_send_group", side_effect=sender), patch(
            "src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=False)
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="revision one")])
            translated = "translated\n" * 1000
            self.assertFalse(
                await processor.apply_translated_revision("1", 1, 101, translated)
            )
            self.assertTrue(
                await processor.apply_translated_revision("1", 1, 101, translated)
            )

        self.assertEqual((1, [51]), calls[2])
        self.assertIsNone(processor._state["1"]["pending_group"])

    async def test_translation_group_resumes_after_restart_on_identical_payload(self):
        async def partial(texts, important=False, start_index=0, existing_message_ids=None, on_progress=None):
            from src.telegram.bot import TelegramGroupResult
            if start_index == 0 and on_progress is not None:
                await on_progress([61], 1)
            return TelegramGroupResult([61], 1, False)

        with patch("src.core.news_processor.tg_send_group", side_effect=partial), patch(
            "src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=False)
        ):
            first = NewsProcessor(state_path=self.state_path)
            payload = item(description="revision one")
            await first.process([payload])
            self.assertFalse(
                await first.apply_translated_revision(
                    "1", 1, 101, "translated\n" * 1000
                )
            )

        observed = []

        async def resume(texts, important=False, start_index=0, existing_message_ids=None, on_progress=None):
            observed.append((start_index, list(existing_message_ids or [])))
            from src.telegram.bot import TelegramGroupResult
            ids = [61] + list(range(62, 62 + len(texts) - 1))
            return TelegramGroupResult(ids, len(texts), True)

        with patch("src.core.news_processor.tg_send_group", side_effect=resume):
            second = NewsProcessor(state_path=self.state_path)
            await second.process([payload])

        self.assertEqual([(1, [61])], observed)
        self.assertIsNone(second._state["1"]["pending_group"])

    async def test_progress_persist_failure_stops_group_and_restores_durable_memory(self):
        description = "durable progress\n" * 1000
        processor = NewsProcessor(state_path=self.state_path)
        original_persist = processor._persist
        failed = False

        def fail_first_progress():
            nonlocal failed
            pending = processor._state.get("1", {}).get("pending_group")
            if (
                not failed
                and isinstance(pending, dict)
                and pending.get("next_chunk_index") == 1
            ):
                failed = True
                raise OSError("state unavailable")
            original_persist()

        send_chunk = AsyncMock(side_effect=[11, 12, 13, 14, 15, 16, 17, 18])
        with patch.object(processor, "_persist", side_effect=fail_first_progress), patch(
            "src.telegram.bot.tg_send", new=send_chunk
        ), patch("src.telegram.bot.asyncio.sleep", new=AsyncMock()):
            await processor.process([item(description=description)])

        self.assertEqual(1, send_chunk.await_count)
        pending = processor._state["1"]["pending_group"]
        self.assertEqual([], pending["message_ids"])
        self.assertEqual(0, pending["next_chunk_index"])
        with open(self.state_path, "r", encoding="utf-8") as state_file:
            durable = json.load(state_file)["entries"]["1"]["pending_group"]
        self.assertEqual([], durable["message_ids"])
        self.assertEqual(0, durable["next_chunk_index"])

    async def test_same_process_retry_uses_durable_index_after_progress_failure(self):
        description = "same process retry\n" * 1000
        processor = NewsProcessor(state_path=self.state_path)
        original_persist = processor._persist
        failed = False

        def fail_first_progress():
            nonlocal failed
            pending = processor._state.get("1", {}).get("pending_group")
            if not failed and isinstance(pending, dict) and pending.get("next_chunk_index") == 1:
                failed = True
                raise OSError("state unavailable")
            original_persist()

        sent_texts = []

        async def send_chunk(text, important=False):
            sent_texts.append(text)
            return 100 + len(sent_texts)

        with patch.object(processor, "_persist", side_effect=fail_first_progress), patch(
            "src.telegram.bot.tg_send", side_effect=send_chunk
        ):
            payload = item(description=description)
            await processor.process([payload])
            await processor.process([payload])

        self.assertGreater(len(sent_texts), 2)
        self.assertEqual(sent_texts[0], sent_texts[1])
        self.assertEqual(1, processor._state["1"]["notification_revision"])
        self.assertIsNone(processor._state["1"]["pending_group"])

    async def test_restart_retries_orphan_chunk_from_durable_index(self):
        description = "restart orphan\n" * 1000
        first = NewsProcessor(state_path=self.state_path)
        original_persist = first._persist
        failed = False
        first_sent = []

        def fail_first_progress():
            nonlocal failed
            pending = first._state.get("1", {}).get("pending_group")
            if not failed and isinstance(pending, dict) and pending.get("next_chunk_index") == 1:
                failed = True
                raise OSError("state unavailable")
            original_persist()

        async def first_send(text, important=False):
            first_sent.append(text)
            return 11

        payload = item(description=description)
        with patch.object(first, "_persist", side_effect=fail_first_progress), patch(
            "src.telegram.bot.tg_send", side_effect=first_send
        ):
            await first.process([payload])

        restarted_sent = []

        async def restarted_send(text, important=False):
            restarted_sent.append(text)
            return 20 + len(restarted_sent)

        with patch("src.telegram.bot.tg_send", side_effect=restarted_send):
            restarted = NewsProcessor(state_path=self.state_path)
            await restarted.process([payload])

        self.assertEqual(first_sent[0], restarted_sent[0])
        self.assertEqual(1, restarted._state["1"]["notification_revision"])
        self.assertIsNone(restarted._state["1"]["pending_group"])

    async def test_translation_progress_persist_failure_is_contained_and_guarded(self):
        with patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="revision one")])

        original_persist = processor._persist
        failed = False

        def fail_translation_progress():
            nonlocal failed
            pending = processor._state["1"].get("pending_group")
            if (
                not failed
                and isinstance(pending, dict)
                and pending.get("kind") == "translation"
                and pending.get("next_chunk_index") == 1
            ):
                failed = True
                raise ValueError("state budget")
            original_persist()

        send_chunk = AsyncMock(return_value=71)
        with patch.object(processor, "_persist", side_effect=fail_translation_progress), patch(
            "src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=False)
        ), patch("src.telegram.bot.tg_send", new=send_chunk):
            applied = await processor.apply_translated_revision(
                "1", 1, 101, "translated\n" * 1000
            )

        self.assertFalse(applied)
        self.assertEqual(1, send_chunk.await_count)
        pending = processor._state["1"]["pending_group"]
        self.assertEqual([], pending["message_ids"])
        self.assertEqual(0, pending["next_chunk_index"])
        with patch(
            "src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=True)
        ):
            await processor.process([item(description="revision two")])
        self.assertEqual(2, processor._state["1"]["revision"])
        self.assertNotEqual("translation", (
            processor._state["1"].get("pending_group") or {}
        ).get("kind"))

    async def test_group_finalization_persist_failure_retains_durable_complete_pending(self):
        description = "finalize failure\n" * 1000
        processor = NewsProcessor(state_path=self.state_path)
        original_persist = processor._persist
        failed = False

        def fail_finalization():
            nonlocal failed
            state = processor._state.get("1")
            if (
                not failed
                and state is not None
                and state.get("pending_group") is None
                and state.get("telegram_message_id") is not None
            ):
                failed = True
                raise OSError("finalization unavailable")
            original_persist()

        send = AsyncMock(side_effect=range(101, 200))
        with patch.object(processor, "_persist", side_effect=fail_finalization), patch(
            "src.telegram.bot.tg_send", new=send
        ):
            await processor.process([item(description=description)])

        pending = processor._state["1"]["pending_group"]
        self.assertEqual(len(pending["chunks"]), pending["next_chunk_index"])
        self.assertEqual(len(pending["chunks"]), len(pending["message_ids"]))
        self.assertEqual(0, processor._state["1"]["notification_revision"])
        with open(self.state_path, "r", encoding="utf-8") as state_file:
            durable = json.load(state_file)["entries"]["1"]
        self.assertEqual(pending, durable["pending_group"])
        self.assertEqual(0, durable["notification_revision"])

    async def test_same_process_finalization_retry_sends_zero_chunks(self):
        description = "same finalize retry\n" * 1000
        processor = NewsProcessor(state_path=self.state_path)
        original_persist = processor._persist
        failed = False

        def fail_finalization():
            nonlocal failed
            state = processor._state.get("1")
            if (
                not failed
                and state is not None
                and state.get("pending_group") is None
                and state.get("telegram_message_id") is not None
            ):
                failed = True
                raise OSError("finalization unavailable")
            original_persist()

        payload = item(description=description)
        first_send = AsyncMock(side_effect=range(201, 300))
        with patch.object(processor, "_persist", side_effect=fail_finalization), patch(
            "src.telegram.bot.tg_send", new=first_send
        ):
            await processor.process([payload])

        retry_send = AsyncMock(return_value=999)
        with patch("src.telegram.bot.tg_send", new=retry_send):
            await processor.process([payload])

        retry_send.assert_not_awaited()
        self.assertIsNone(processor._state["1"]["pending_group"])
        self.assertEqual(1, processor._state["1"]["notification_revision"])

    async def test_restart_finalization_retry_sends_zero_chunks(self):
        description = "restart finalize retry\n" * 1000
        first = NewsProcessor(state_path=self.state_path)
        original_persist = first._persist
        failed = False

        def fail_finalization():
            nonlocal failed
            state = first._state.get("1")
            if (
                not failed
                and state is not None
                and state.get("pending_group") is None
                and state.get("telegram_message_id") is not None
            ):
                failed = True
                raise OSError("finalization unavailable")
            original_persist()

        payload = item(description=description)
        with patch.object(first, "_persist", side_effect=fail_finalization), patch(
            "src.telegram.bot.tg_send", new=AsyncMock(side_effect=range(301, 400))
        ):
            await first.process([payload])

        retry_send = AsyncMock(return_value=999)
        with patch("src.telegram.bot.tg_send", new=retry_send):
            restarted = NewsProcessor(state_path=self.state_path)
            await restarted.process([payload])

        retry_send.assert_not_awaited()
        self.assertIsNone(restarted._state["1"]["pending_group"])
        self.assertEqual(1, restarted._state["1"]["notification_revision"])

    async def test_translation_finalization_failure_retries_without_resend(self):
        with patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="revision one")])

        original_persist = processor._persist
        failed = False

        def fail_translation_finalization():
            nonlocal failed
            state = processor._state["1"]
            if (
                not failed
                and state.get("pending_group") is None
                and state.get("notification_kind") == "update"
            ):
                failed = True
                raise ValueError("finalization budget")
            original_persist()

        translated = "translated finalization\n" * 1000
        with patch.object(
            processor, "_persist", side_effect=fail_translation_finalization
        ), patch(
            "src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=False)
        ), patch(
            "src.telegram.bot.tg_send", new=AsyncMock(side_effect=range(401, 500))
        ):
            self.assertFalse(
                await processor.apply_translated_revision("1", 1, 101, translated)
            )

        pending = processor._state["1"]["pending_group"]
        self.assertEqual("translation", pending["kind"])
        self.assertEqual(len(pending["chunks"]), pending["next_chunk_index"])
        self.assertEqual("breaking", processor._state["1"]["notification_kind"])

        retry_send = AsyncMock(return_value=999)
        with patch("src.telegram.bot.tg_send", new=retry_send):
            restarted = NewsProcessor(state_path=self.state_path)
            await restarted.process([item(description="revision one")])

        retry_send.assert_not_awaited()
        self.assertIsNone(restarted._state["1"]["pending_group"])
        self.assertEqual("update", restarted._state["1"]["notification_kind"])
        self.assertEqual(1, restarted._state["1"]["telegram_revision"])

    async def test_archive_backpressure_rejects_revision_without_mutation_or_telegram(self):
        edit = AsyncMock(return_value=True)
        with patch("src.core.news_processor.save_news_items_batch", side_effect=OSError("disk")), patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])
        ), patch("src.core.news_processor.tg_edit_message", new=edit), patch(
            "src.core.news_processor.MAX_PENDING_ARCHIVE_RECORDS", 1
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="one")])
            state_before = dict(processor._state["1"])
            await processor.process([item(description="two")])

        self.assertEqual(1, processor._state["1"]["revision"])
        self.assertEqual("one", processor._state["1"]["description"])
        self.assertEqual(state_before["notification_revision"], processor._state["1"]["notification_revision"])
        edit.assert_not_awaited()

    async def test_new_breaking_atomic_commit_survives_crash_before_archive_flush(self):
        processor = NewsProcessor(state_path=self.state_path)
        payload = item(news_id=81, description="atomic new")
        with patch.object(
            processor, "_flush_archives", side_effect=RuntimeError("simulated crash")
        ), patch("src.core.news_processor.tg_send_group", new=AsyncMock()) as send:
            with self.assertRaises(RuntimeError):
                await processor.process([payload])
            send.assert_not_awaited()

        state = processor._state["81"]
        self.assertEqual(1, state["revision"])
        self.assertEqual(1, len(state["raw_archive_pending"]))
        self.assertEqual(1, len(state["breaking_archive_pending"]))

        raw = Mock(return_value=[])
        breaking = Mock(return_value=False)
        with patch("src.core.news_processor.save_news_items_batch", new=raw), patch(
            "src.core.news_processor.save_breaking_item", new=breaking
        ), patch("src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[181])):
            restarted = NewsProcessor(state_path=self.state_path)
            await restarted.process([payload])

        raw.assert_called_once()
        breaking.assert_called_once()
        self.assertEqual([], restarted._state["81"]["raw_archive_pending"])
        self.assertEqual([], restarted._state["81"]["breaking_archive_pending"])

    async def test_content_revision_atomic_commit_survives_crash_before_archive_flush(self):
        with patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="old")])

        original_flush = processor._flush_archives

        def crash_on_new_pending(nid, state):
            if state.get("raw_archive_pending"):
                raise RuntimeError("simulated crash")
            original_flush(nid, state)

        revised = item(description="expanded")
        with patch.object(processor, "_flush_archives", side_effect=crash_on_new_pending), patch(
            "src.core.news_processor.tg_edit_message", new=AsyncMock()
        ) as edit:
            with self.assertRaises(RuntimeError):
                await processor.process([revised])
            edit.assert_not_awaited()

        self.assertEqual(2, processor._state["1"]["revision"])
        self.assertEqual(1, len(processor._state["1"]["raw_archive_pending"]))
        self.assertEqual(1, len(processor._state["1"]["breaking_archive_pending"]))

        raw = Mock(return_value=[])
        breaking = Mock(return_value=False)
        with patch("src.core.news_processor.save_news_items_batch", new=raw), patch(
            "src.core.news_processor.save_breaking_item", new=breaking
        ), patch("src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=True)):
            restarted = NewsProcessor(state_path=self.state_path)
            await restarted.process([revised])

        raw.assert_called_once()
        breaking.assert_called_once()
        self.assertEqual([], restarted._state["1"]["raw_archive_pending"])
        self.assertEqual([], restarted._state["1"]["breaking_archive_pending"])

    async def test_editorial_persist_failure_rolls_back_and_skips_sinks(self):
        with patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="stable")])

        before = json.loads(json.dumps(processor._state["1"]))
        edit = AsyncMock(return_value=True)
        raw = Mock(return_value=[{}])
        breaking = Mock(return_value=True)
        with patch.object(processor, "_persist", side_effect=OSError("state disk")), patch(
            "src.core.news_processor.tg_edit_message", new=edit
        ), patch("src.core.news_processor.save_news_items_batch", new=raw), patch(
            "src.core.news_processor.save_breaking_item", new=breaking
        ):
            await processor.process([item(description="must rollback")])

        self.assertEqual(before, processor._state["1"])
        edit.assert_not_awaited()
        raw.assert_not_called()
        breaking.assert_not_called()

    async def test_oversized_batch_rejected_without_any_mutation(self):
        send = AsyncMock(return_value=[101])
        archive = Mock(return_value=[{}])
        with patch("src.core.news_processor.tg_send_group", new=send), patch(
            "src.core.news_processor.save_news_items_batch", new=archive
        ), patch("src.core.news_processor.MAX_ITEMS_PER_PROCESS_BATCH", 2):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(news_id=1), item(news_id=2), item(news_id=3)])
        self.assertEqual({}, processor._state)
        send.assert_not_awaited()
        archive.assert_not_called()

    async def test_boundary_batch_processes_all_items(self):
        with patch("src.core.news_processor.MAX_ITEMS_PER_PROCESS_BATCH", 2):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([
                item(news_id=1, breaking=False, level="news-general"),
                item(news_id=2, breaking=False, level="news-general"),
            ])
        self.assertEqual({"1", "2"}, set(processor._state))

    async def test_state_capacity_rejects_new_id_when_all_entries_pending(self):
        with patch("src.core.news_processor.MAX_STATE_ENTRIES", 1), patch(
            "src.core.news_processor.save_news_items_batch", side_effect=OSError("disk")
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(news_id=1, breaking=False, level="news-general")])
            await processor.process([item(news_id=2, breaking=False, level="news-general")])
        self.assertEqual({"1"}, set(processor._state))

    async def test_global_pending_count_rejects_multi_id_revision(self):
        with patch("src.core.news_processor.MAX_GLOBAL_PENDING_ARCHIVE_RECORDS", 1), patch(
            "src.core.news_processor.save_news_items_batch", side_effect=OSError("disk")
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(news_id=1, breaking=False, level="news-general")])
            await processor.process([item(news_id=2, breaking=False, level="news-general")])
        self.assertNotIn("2", processor._state)

    async def test_global_pending_bytes_rejects_multi_id_revision(self):
        with patch("src.core.news_processor.MAX_GLOBAL_PENDING_ARCHIVE_BYTES", 1_000), patch(
            "src.core.news_processor.save_news_items_batch", side_effect=OSError("disk")
        ):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([
                item(news_id=1, description="x" * 300, breaking=False, level="news-general")
            ])
            await processor.process([
                item(news_id=2, description="y" * 300, breaking=False, level="news-general")
            ])
        self.assertNotIn("2", processor._state)

    async def test_runtime_state_budget_rejects_revision_before_mutation(self):
        edit = AsyncMock(return_value=True)
        with patch("src.core.news_processor.MAX_RUNTIME_STATE_BYTES", 3_000), patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])
        ), patch("src.core.news_processor.tg_edit_message", new=edit):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([item(description="small")])
            await processor.process([item(description="x" * 5_000)])
        self.assertEqual(1, processor._state["1"]["revision"])
        self.assertEqual("small", processor._state["1"]["description"])
        edit.assert_not_awaited()

    async def test_runtime_state_budget_rejects_second_news_id(self):
        with patch("src.core.news_processor.MAX_RUNTIME_STATE_BYTES", 3_500):
            processor = NewsProcessor(state_path=self.state_path)
            await processor.process([
                item(news_id=1, description="x" * 500, breaking=False, level="news-general")
            ])
            await processor.process([
                item(news_id=2, description="y" * 2_000, breaking=False, level="news-general")
            ])
        self.assertIn("1", processor._state)
        self.assertNotIn("2", processor._state)

    async def test_overlong_live_metadata_rejected(self):
        processor = NewsProcessor(state_path=self.state_path)
        await processor.process([item(level="x" * (MAX_LEVEL_LENGTH + 1))])
        await processor.process([{
            **item(news_id=2),
            "__ws_method__": "x" * (MAX_SOURCE_METHOD_LENGTH + 1),
        }])
        self.assertEqual({}, processor._state)


class BaselineMigrationTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_current_day_archive_prevents_duplicate_breaking_on_first_deployment(self):
        with tempfile.TemporaryDirectory() as tempdir:
            state_path = os.path.join(tempdir, "news_processor_state.json")
            day_dir = os.path.join(tempdir, "2026-01-01")
            os.makedirs(day_dir)
            record = {
                "news_id": "9",
                "title": "Archived",
                "description": "Existing",
                "time": "source",
                "link": "https://example.test/9",
                "revision": 1,
            }
            with open(os.path.join(day_dir, "raw.json"), "w", encoding="utf-8") as raw_file:
                json.dump([record], raw_file)
            with open(os.path.join(day_dir, "breaking.json"), "w", encoding="utf-8") as breaking_file:
                json.dump([record], breaking_file)
            send = AsyncMock(return_value=[101])
            with patch("src.core.news_processor.load_daily_archive_records", return_value=([record], [record])), patch("src.core.news_processor.tg_send_group", new=send), patch("src.core.news_processor.save_news_items_batch", return_value=[]), patch(
                "src.core.news_processor.save_breaking_item", return_value=False
            ):
                processor = NewsProcessor(state_path=state_path)
                await processor.process([item(
                    news_id=9,
                    title="Archived",
                    description="Existing",
                    source_time="source",
                )])

            send.assert_not_awaited()
            self.assertTrue(processor._state["9"]["alert_recorded"])
            self.assertIsNone(processor._state["9"]["telegram_message_id"])
            self.assertTrue(os.path.exists(state_path))

    async def test_legacy_breaking_expansion_sends_update_replacement(self):
        record = {
            "news_id": "9",
            "title": "Archived",
            "description": "Existing",
            "time": "source",
            "link": "https://example.test/9",
            "revision": 1,
        }
        with tempfile.TemporaryDirectory() as tempdir:
            state_path = os.path.join(tempdir, "news_processor_state.json")
            send = AsyncMock(return_value=[909])
            with patch("src.core.news_processor.load_daily_archive_records", return_value=([record], [record])), patch("src.core.news_processor.tg_send_group", new=send), patch("src.core.news_processor.save_news_items_batch", return_value=[{}]), patch(
                "src.core.news_processor.save_breaking_item", return_value=True
            ):
                processor = NewsProcessor(state_path=state_path)
                await processor.process([item(
                    news_id=9,
                    title="Archived",
                    description="Existing plus expansion",
                    source_time="source",
                )])

            send.assert_awaited_once()
            send_call = send.await_args
            self.assertIsNotNone(send_call)
            if send_call is not None:
                self.assertIn("UPDATE", send_call.args[0][0])
            self.assertEqual(909, processor._state["9"]["telegram_message_id"])
            self.assertEqual(2, processor._state["9"]["notification_revision"])


class RenderingTestCase(unittest.TestCase):
    def test_html_escaping(self):
        self.assertEqual("&lt;x&gt; &amp; y", escape_html("<x> & y"))

    def test_telegram_boundaries(self):
        self.assertEqual(1, len(render_message_chunks("a" * 4095)))
        self.assertEqual(1, len(render_message_chunks("a" * 4096)))
        chunks = render_message_chunks("a" * 4097)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(chunk) <= TELEGRAM_TEXT_LIMIT for chunk in chunks))
        recovered = "".join(chunk.split("</b>\n", 1)[1] for chunk in chunks)
        self.assertEqual("a" * 4097, recovered)

    def test_chunk_limit_raises_without_partial_render(self):
        with self.assertRaises(TelegramRenderLimitError):
            render_message_chunks("a" * (MAX_RENDERED_HTML_LENGTH + 1))


class TelegramGroupTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_partial_group_returns_failure_after_finite_retries(self):
        send = AsyncMock(side_effect=[11, None, None, None])
        with patch("src.telegram.bot.tg_send", new=send), patch(
            "src.telegram.bot.asyncio.sleep", new=AsyncMock()
        ):
            result = await tg_send_group(["one", "two"])
        self.assertEqual([], result)
        self.assertEqual(4, send.await_count)

    async def test_resumable_helper_rejects_invalid_input_before_send(self):
        send = AsyncMock(return_value=1)
        invalid_cases = [
            (["x"] * (MAX_TELEGRAM_CHUNKS + 1), 0, []),
            (["x" * (TELEGRAM_TEXT_LIMIT + 1)], 0, []),
            (["one", "two"], 1, []),
            (["one"], 0, [1]),
        ]
        with patch("src.telegram.bot.tg_send", new=send):
            for chunks, start_index, ids in invalid_cases:
                with self.assertRaises((ValueError, TelegramRenderLimitError)):
                    await tg_send_group_resumable(
                        chunks, start_index=start_index, existing_message_ids=ids
                    )
        send.assert_not_awaited()

    async def test_resumable_helper_stops_when_progress_not_durably_acknowledged(self):
        send = AsyncMock(side_effect=[11, 12])
        progress = AsyncMock(return_value=False)
        with patch("src.telegram.bot.tg_send", new=send):
            result = await tg_send_group_resumable(
                ["one", "two"], on_progress=progress
            )
        self.assertFalse(result.complete)
        self.assertEqual([], result.message_ids)
        self.assertEqual(0, result.next_chunk_index)
        self.assertEqual(1, send.await_count)
        progress.assert_awaited_once_with([11], 1)

    async def test_resumable_helper_complete_index_sends_zero(self):
        send = AsyncMock(return_value=99)
        progress = AsyncMock(return_value=True)
        with patch("src.telegram.bot.tg_send", new=send):
            result = await tg_send_group_resumable(
                ["one", "two"],
                start_index=2,
                existing_message_ids=[11, 12],
                on_progress=progress,
            )
        self.assertTrue(result.complete)
        self.assertEqual([11, 12], result.message_ids)
        self.assertEqual(2, result.next_chunk_index)
        send.assert_not_awaited()
        progress.assert_not_awaited()


class PersistedStateSecurityTestCase(unittest.IsolatedAsyncioTestCase):
    def _valid_state(self) -> dict[str, Any]:
        content = {"title": "Title", "description": "Description"}
        fingerprint = __import__("hashlib").sha256(
            json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return {
            "level": "active",
            "breaking": True,
            "fingerprint": fingerprint,
            "revision": 1,
            "title": "Title",
            "description": "Description",
            "source_time": "source",
            "eurl": "https://example.test",
            "telegram_message_id": 101,
            "prefix": "🚨 BREAKING\n",
            "notification_kind": "breaking",
            "source_method": "WS",
            "notification_revision": 1,
            "telegram_revision": 1,
            "telegram_is_group": False,
            "telegram_message_ids": [101],
            "alert_recorded": True,
            "raw_archive_revision": 1,
            "breaking_archive_revision": 1,
            "raw_archive_pending": [],
            "breaking_archive_pending": [],
            "pending_group": None,
            "last_seen_seq": 1,
        }

    def test_oversized_state_rejected_before_json_load(self):
        with tempfile.TemporaryDirectory() as tempdir:
            path = os.path.join(tempdir, "state.json")
            with open(path, "wb") as state_file:
                state_file.truncate(MAX_PROCESSOR_STATE_BYTES + 1)
            with patch("src.archive.storage.json.load", side_effect=AssertionError("must not load")):
                processor = NewsProcessor(state_path=path)
        self.assertEqual({}, processor._state)

    def test_loaded_overlong_description_entry_rejected(self):
        state = self._valid_state()
        state["description"] = "x" * (MAX_DESCRIPTION_LENGTH + 1)
        with tempfile.TemporaryDirectory() as tempdir:
            path = os.path.join(tempdir, "state.json")
            with open(path, "w", encoding="utf-8") as state_file:
                json.dump({"version": 4, "entries": {"1": state}}, state_file)
            processor = NewsProcessor(state_path=path)
        self.assertNotIn("1", processor._state)

    def test_loaded_pending_archive_over_count_discards_state_entry(self):
        state = self._valid_state()
        record = {
            "news_id": "1", "title": "Title", "description": "Description",
            "time": "source", "link": "", "is_important": True,
            "level": "active", "breaking": True, "fingerprint": state["fingerprint"],
            "revision": 1, "source_method": "WS",
        }
        state["raw_archive_pending"] = [record] * (MAX_PENDING_ARCHIVE_RECORDS + 1)
        with tempfile.TemporaryDirectory() as tempdir:
            path = os.path.join(tempdir, "state.json")
            with open(path, "w", encoding="utf-8") as state_file:
                json.dump({"version": 4, "entries": {"1": state}}, state_file)
            processor = NewsProcessor(state_path=path)
        self.assertNotIn("1", processor._state)

    def test_loaded_pending_archive_over_bytes_discards_state_entry(self):
        state = self._valid_state()
        record = {
            "news_id": "1", "title": "Title", "description": "Description",
            "time": "source", "link": "", "is_important": True,
            "level": "active", "breaking": True, "fingerprint": state["fingerprint"],
            "revision": 1, "source_method": "WS",
        }
        state["raw_archive_pending"] = [record]
        with tempfile.TemporaryDirectory() as tempdir:
            path = os.path.join(tempdir, "state.json")
            with open(path, "w", encoding="utf-8") as state_file:
                json.dump({"version": 4, "entries": {"1": state}}, state_file)
            with patch("src.core.news_processor.MAX_PENDING_ARCHIVE_BYTES", 10):
                processor = NewsProcessor(state_path=path)
        self.assertNotIn("1", processor._state)

    async def test_malformed_pending_groups_discarded_without_send(self):
        base = self._valid_state()
        valid_chunks = ["one", "two"]
        valid_digest = __import__("hashlib").sha256("\0".join(valid_chunks).encode()).hexdigest()
        malformed = [
            {"revision": 1, "kind": "translation", "digest": valid_digest,
             "chunks": ["x"] * (MAX_TELEGRAM_CHUNKS + 1), "message_ids": [], "next_chunk_index": 0},
            {"revision": 1, "kind": "translation", "digest": valid_digest,
             "chunks": ["x" * (TELEGRAM_TEXT_LIMIT + 1)], "message_ids": [], "next_chunk_index": 0},
            {"revision": 1, "kind": "translation", "digest": valid_digest,
             "chunks": valid_chunks, "message_ids": [11], "next_chunk_index": 0},
            {"revision": 1, "kind": "translation", "digest": "0" * 64,
             "chunks": valid_chunks, "message_ids": [], "next_chunk_index": 0},
        ]
        for pending_group in malformed:
            with self.subTest(pending_group=pending_group):
                state = dict(base)
                state["pending_group"] = pending_group
                with tempfile.TemporaryDirectory() as tempdir:
                    path = os.path.join(tempdir, "state.json")
                    with open(path, "w", encoding="utf-8") as state_file:
                        json.dump({"version": 4, "entries": {"1": state}}, state_file)
                    send = AsyncMock(return_value=[999])
                    with patch("src.core.news_processor.tg_send_group", new=send):
                        processor = NewsProcessor(state_path=path)
                        await processor.process([item()])
                    send.assert_not_awaited()
                    self.assertIsNone(processor._state["1"]["pending_group"])

    def test_loaded_invalid_metadata_and_message_ids_rejected(self):
        variants = []
        for field, value in (
            ("level", "x" * (MAX_LEVEL_LENGTH + 1)),
            ("source_method", "x" * (MAX_SOURCE_METHOD_LENGTH + 1)),
            ("prefix", "BAD"),
            ("notification_kind", "BAD"),
            ("telegram_message_id", -1),
            ("telegram_message_id", True),
            ("telegram_message_ids", [-1]),
            ("telegram_message_ids", [True]),
            ("telegram_message_ids", list(range(1, MAX_TELEGRAM_CHUNKS + 2))),
            ("telegram_message_ids", [999]),
        ):
            variants.append((field, value))
        for field, value in variants:
            with self.subTest(field=field, value_type=type(value).__name__):
                state = self._valid_state()
                state[field] = value
                with tempfile.TemporaryDirectory() as tempdir:
                    path = os.path.join(tempdir, "state.json")
                    with open(path, "w", encoding="utf-8") as state_file:
                        json.dump({"version": 4, "entries": {"1": state}}, state_file)
                    processor = NewsProcessor(state_path=path)
                self.assertNotIn("1", processor._state)


class ArchiveRotationTestCase(unittest.TestCase):
    def test_raw_archive_rotates_by_count_and_keeps_newest_complete_records(self):
        with tempfile.TemporaryDirectory() as tempdir, patch(
            "src.archive.storage.DATA_DIR", tempdir
        ), patch("src.archive.storage.RAW_MAX_RECORDS", 3), patch(
            "src.archive.storage.RAW_MAX_SERIALIZED_BYTES", 100_000
        ):
            records = [
                {"news_id": str(index), "title": f"title-{index}", "revision": 1}
                for index in range(5)
            ]
            save_news_items_batch(records, date_str="2026-01-01")
            path = os.path.join(tempdir, "2026-01-01", "raw.json")
            with open(path, "r", encoding="utf-8") as archive_file:
                archived = json.load(archive_file)
        self.assertEqual(["2", "3", "4"], [record["news_id"] for record in archived])

    def test_breaking_archive_rotates_by_size_and_keeps_newest_complete_records(self):
        with tempfile.TemporaryDirectory() as tempdir, patch(
            "src.archive.storage.DATA_DIR", tempdir
        ), patch("src.archive.storage.BREAKING_MAX_RECORDS", 10), patch(
            "src.archive.storage.BREAKING_MAX_SERIALIZED_BYTES", 700
        ):
            for index in range(5):
                save_breaking_item(
                    {
                        "news_id": str(index),
                        "title": f"title-{index}",
                        "description": "x" * 200,
                        "fingerprint": str(index),
                        "revision": 1,
                    },
                    date_str="2026-01-01",
                )
            path = os.path.join(tempdir, "2026-01-01", "breaking.json")
            with open(path, "r", encoding="utf-8") as archive_file:
                archived = json.load(archive_file)
        self.assertLess(len(archived), 5)
        self.assertEqual("4", archived[-1]["news_id"])
        self.assertTrue(all(record["description"] == "x" * 200 for record in archived))


class TranslationWorkerTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_stale_translation_blocked(self):
        worker = TranslationQueueWorker()
        apply_translation = AsyncMock(return_value=False)
        job = TranslationJob(
            news_id="1",
            revision=1,
            message_id=10,
            original_text="old",
            apply_translation=apply_translation,
        )
        with patch("src.translate.queue_worker.translate", new=AsyncMock(return_value="旧")):
            await worker._handle(job)
        apply_translation.assert_awaited_once()

    async def test_translation_edit_failure_uses_replacement_and_reports_id(self):
        worker = TranslationQueueWorker()
        apply_translation = AsyncMock(return_value=True)
        job = TranslationJob(
            news_id="1",
            revision=3,
            message_id=10,
            original_text="latest",
            apply_translation=apply_translation,
        )
        with patch("src.translate.queue_worker.translate", new=AsyncMock(return_value="最新")):
            await worker._handle(job)
        apply_translation.assert_awaited_once()

    async def test_translation_failure_preserves_latest_english(self):
        worker = TranslationQueueWorker()
        apply_translation = AsyncMock(return_value=True)
        job = TranslationJob(
            news_id="1",
            revision=4,
            message_id=10,
            original_text="latest English",
            apply_translation=apply_translation,
        )
        with patch("src.translate.queue_worker.translate", new=AsyncMock(return_value=None)):
            await worker._handle(job)
        apply_translation.assert_not_awaited()


class StateAuditTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_revision_state_and_archive_fields_are_serializable(self):
        with tempfile.TemporaryDirectory() as tempdir:
            state_path = os.path.join(tempdir, "state.json")
            records = []

            def archive(items):
                records.extend(items)
                return items

            with patch("src.core.news_processor.save_news_items_batch", side_effect=archive), patch(
                "src.core.news_processor.save_breaking_item", return_value=True
            ), patch("src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[1])), patch(
                "src.core.news_processor.tg_edit_message", new=AsyncMock(return_value=True)
            ):
                processor = NewsProcessor(state_path=state_path)
                await processor.process([item(description="one")])
                await processor.process([item(description="two")])

            with open(state_path, "r", encoding="utf-8") as state_file:
                persisted = json.load(state_file)
            self.assertEqual(2, persisted["entries"]["1"]["revision"])
            self.assertEqual(2, persisted["entries"]["1"]["notification_revision"])
            self.assertEqual([1, 2], [record["revision"] for record in records])
            self.assertTrue(all(record["fingerprint"] for record in records))
            self.assertTrue(all("description" in record for record in records))
            self.assertTrue(all(record["source_method"] == "sendUpdates" for record in records))


if __name__ == "__main__":
    unittest.main()
