import json
import unittest
from unittest.mock import patch

from src.api.centrifugo_client import CentrifugoClient


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


if __name__ == "__main__":
    unittest.main()
