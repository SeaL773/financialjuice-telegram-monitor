import html
import os
import tempfile
import unittest
from typing import Any
from unittest.mock import AsyncMock, patch

from src.core.news_processor import NewsProcessor
from src.telegram import rendering
from src.telegram.rendering import (
    MAX_NORMALIZED_LIST_DEPTH,
    TelegramRenderLimitError,
    normalize_upstream_html,
    render_message_chunks,
)
from src.translate.queue_worker import TranslationJob


class RenderingTestCase(unittest.TestCase):
    def test_financialjuice_sample_is_readable_and_script_is_removed(self):
        source = (
            "<ul><li>Stocks rise &#39;strongly&#39;</li>"
            "<li>TradingView<script>window.alert('bad')</script> content</li></ul>"
        )
        self.assertEqual(
            "• Stocks rise 'strongly'\n• TradingView content",
            normalize_upstream_html(source),
        )

    def test_entities_are_decoded_before_telegram_escaping(self):
        normalized = normalize_upstream_html("<p>A &#39;quote&#39; &amp; a &lt;tag&gt;</p>")
        self.assertEqual("A 'quote' & a <tag>", normalized)
        self.assertEqual(["A 'quote' &amp; a &lt;tag&gt;"], render_message_chunks(normalized))

    def test_unordered_ordered_and_nested_lists(self):
        source = "<ul><li>One<ul><li>Nested</li></ul></li></ul><ol><li>Two</li></ol>"
        self.assertEqual(
            "• One\n  • Nested\n1. Two",
            normalize_upstream_html(source),
        )

    def test_malformed_and_unknown_markup_degrades_to_text(self):
        self.assertEqual(
            "Known visible\nAfter",
            normalize_upstream_html("<div>Known <custom data-x='x'>visible</custom><br>After"),
        )
        self.assertEqual(
            "",
            normalize_upstream_html("<script>before <b>also dropped"),
        )

    def test_plain_text_is_preserved_exactly(self):
        source = "  plain < text & words\nwith spacing  "
        self.assertEqual(source, normalize_upstream_html(source))

    def test_all_dropped_elements_and_nested_dropped_content(self):
        dropped = "style iframe object embed template svg canvas noscript"
        source = "visible " + " ".join(
            f"<{tag}>drop <b>nested</b></{tag}>" for tag in dropped.split()
        ) + " <script><style>also drop</style></script> after"
        self.assertEqual("visible after", normalize_upstream_html(source))

    def test_deep_list_is_rejected_before_amplified_indent_is_built(self):
        source = "<ul>" * (MAX_NORMALIZED_LIST_DEPTH + 1)
        with self.assertRaises(TelegramRenderLimitError):
            normalize_upstream_html(source)

    def test_valid_nested_list_remains_readable(self):
        source = "<ol><li>Parent<ul><li>Child</li></ul></li></ol>"
        self.assertEqual("1. Parent\n  • Child", normalize_upstream_html(source))

    def test_normalized_output_budget_allows_trimmed_block_boundary(self):
        source = "<p>" + ("x" * rendering.MAX_NORMALIZED_TEXT_LENGTH) + "</p>"
        self.assertEqual("x" * rendering.MAX_NORMALIZED_TEXT_LENGTH, normalize_upstream_html(source))

    def test_crossing_dropped_closure_never_exposes_markup_or_partial_content(self):
        source = "before<iframe>secret<style>also secret</iframe>after"
        self.assertEqual("before", normalize_upstream_html(source))

    def test_entity_decoded_telegram_tag_is_escaped_in_final_chunks(self):
        normalized = normalize_upstream_html("<p>&lt;b&gt;not bold&lt;/b&gt;</p>")
        self.assertEqual(["&lt;b&gt;not bold&lt;/b&gt;"], render_message_chunks(normalized))

    def test_parser_failure_is_an_explicit_controlled_failure(self):
        with patch.object(rendering._UpstreamHTMLTextParser, "feed", side_effect=RuntimeError("bad")):
            with self.assertRaises(TelegramRenderLimitError):
                normalize_upstream_html("<p>text</p>")


def _item(description: str) -> dict[str, Any]:
    return {
        "NewsID": 1,
        "Title": "Headline",
        "Description": description,
        "PostedLong": "10:00",
        "EURL": "https://example.test/news",
        "Breaking": True,
        "Level": "active",
    }


class ProcessorRenderingTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_english_send_uses_bullets_and_excludes_script(self):
        sent = AsyncMock(return_value=[101])
        with tempfile.TemporaryDirectory() as tempdir, patch(
            "src.core.news_processor.tg_send_group", new=sent
        ):
            processor = NewsProcessor(state_path=os.path.join(tempdir, "state.json"))
            await processor.process([_item("<ul><li>First</li><li>Second</li></ul><script>evil()</script>")])

        send_call = sent.await_args
        self.assertIsNotNone(send_call)
        if send_call is None:
            return
        message = str(send_call.args[0][0])
        self.assertIn("• First\n• Second", html.unescape(message))
        self.assertNotIn("script", message.lower())
        self.assertNotIn("evil", message)

    async def test_translation_job_receives_normalized_text(self):
        class Worker:
            def __init__(self):
                self.jobs: list[TranslationJob] = []

            def submit(self, job: TranslationJob) -> bool:
                self.jobs.append(job)
                return True

        worker = Worker()
        with tempfile.TemporaryDirectory() as tempdir, patch(
            "src.core.news_processor.tg_send_group", new=AsyncMock(return_value=[101])
        ), patch("src.core.news_processor.TRANSLATE_ENABLED", True):
            processor = NewsProcessor(
                translation_worker=worker,
                state_path=os.path.join(tempdir, "state.json"),
            )
            await processor.process([_item("<ul><li>Translate me</li></ul><script>evil()</script>")])

        self.assertEqual(1, len(worker.jobs))
        original = worker.jobs[0].original_text
        self.assertIn("• Translate me", original)
        self.assertNotIn("<ul>", original)
        self.assertNotIn("script", original.lower())
        self.assertNotIn("evil", original)

    async def test_oversized_normalization_is_rejected_without_partial_send(self):
        sent = AsyncMock(return_value=[101])
        deeply_nested = "<ul>" * (MAX_NORMALIZED_LIST_DEPTH + 1) + "content"
        with tempfile.TemporaryDirectory() as tempdir, patch(
            "src.core.news_processor.tg_send_group", new=sent
        ):
            processor = NewsProcessor(state_path=os.path.join(tempdir, "state.json"))
            await processor.process([_item(deeply_nested)])

        sent.assert_not_awaited()
        self.assertEqual(0, processor._state["1"]["notification_revision"])
        self.assertEqual(deeply_nested, processor._state["1"]["description"])
