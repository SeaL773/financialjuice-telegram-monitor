import json
import asyncio
import unittest
from typing import Never
from unittest.mock import Mock, patch

from src.api.centrifugo_client import CentrifugoClient
from src.api.ws_common import WSConnectionError


class CentrifugoIngressTestCase(unittest.TestCase):
    def _frame(self, message: object, channel: str = "feed:lite_rid:0") -> str:
        return json.dumps({"push": {"channel": channel, "pub": {"data": {"msg": message}}}})

    def test_wrapped_message_parses_with_channel_metadata(self) -> None:
        items = CentrifugoClient._parse_frame(
            self._frame(json.dumps([{"NewsID": 7, "Title": "Headline"}]))
        )
        self.assertEqual(1, len(items))
        self.assertEqual("feed:lite_rid:0", items[0]["__ws_channel__"])

    def test_non_feed_channel_is_ignored(self) -> None:
        self.assertEqual([], CentrifugoClient._parse_frame(self._frame({}, "calendar:all")))

    def test_feedmain_channel_is_accepted(self) -> None:
        items = CentrifugoClient._parse_frame(
            self._frame(json.dumps([{"NewsID": 11}]), "feedmain:lite_rid:0")
        )
        self.assertEqual([11], [i["NewsID"] for i in items])
        self.assertEqual("feedmain:lite_rid:0", items[0]["__ws_channel__"])

    def test_newline_batched_frame_yields_every_object(self) -> None:
        batched = "\n".join(
            (
                self._frame(json.dumps([{"NewsID": 1}]), "feed:all"),
                self._frame(json.dumps([{"NewsID": 2}]), "feedmain:lite_rid:0"),
            )
        )
        self.assertEqual([1, 2], [i["NewsID"] for i in CentrifugoClient._parse_frame(batched)])

    def test_unparsable_object_is_skipped_without_dropping_batch(self) -> None:
        batched = "\n".join(
            ("{not json", self._frame(json.dumps([{"NewsID": 3}])), "12345")
        )
        self.assertEqual([3], [i["NewsID"] for i in CentrifugoClient._parse_frame(batched)])

    def test_oversized_frame_rejected_before_json_parse(self) -> None:
        with patch("src.api.centrifugo_client.MAX_WS_TEXT_FRAME_BYTES", 4), patch(
            "src.api.centrifugo_client.json.loads"
        ) as loads:
            with self.assertRaises(Exception):
                CentrifugoClient._parse_frame("12345")
        loads.assert_not_called()

    def test_oversized_nested_message_and_item_count_are_rejected(self) -> None:
        with patch("src.api.centrifugo_client.MAX_WS_NESTED_PAYLOAD_BYTES", 2):
            self.assertEqual([], CentrifugoClient._parse_frame(self._frame("[]")))

        with patch("src.api.centrifugo_client.MAX_ITEMS_PER_PROCESS_BATCH", 1):
            self.assertEqual([], CentrifugoClient._parse_frame(self._frame([{}, {}])))


class CentrifugoReceiveTestCase(unittest.IsolatedAsyncioTestCase):
    async def test_stale_receive_raises_connection_error(self) -> None:
        class StaleWebSocket:
            closed: bool = False

            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

            async def close(self):
                self.closed = True

            async def send_json(self, data: object):
                return None

            async def send_str(self, data: str):
                return None

            async def receive(self) -> Never:
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

            def exception(self):
                return None

        session = Mock()
        client = CentrifugoClient(
            "wss://example.test",
            "token",
            session,
            receive_timeout=0.01,
        )
        client._ws = StaleWebSocket()

        with self.assertRaisesRegex(WSConnectionError, "No Centrifugo frame"):
            await anext(client.listen())


if __name__ == "__main__":
    unittest.main()
