import asyncio
import os
import sys
import tempfile
from unittest.mock import AsyncMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.core.news_processor import NewsProcessor


async def main() -> None:
    with tempfile.TemporaryDirectory() as tempdir:
        state_path = os.path.join(tempdir, "state.json")
        send = AsyncMock(return_value=[9001])
        edit = AsyncMock(return_value=True)
        base = {
            "NewsID": 42,
            "Title": "Headline",
            "Description": "short",
            "PostedLong": "12:34 source",
            "EURL": "",
            "Breaking": True,
            "Level": "active",
        }
        expanded = {
            **base,
            "Description": "short plus the complete expanded description",
        }
        with patch(
            "src.core.news_processor.save_news_items_batch", return_value=[{}]
        ), patch(
            "src.core.news_processor.save_breaking_item", return_value=True
        ), patch("src.core.news_processor.tg_send_group", new=send), patch(
            "src.core.news_processor.tg_edit_message", new=edit
        ):
            processor = NewsProcessor(state_path=state_path)
            await processor.process([base], source="MANUAL")
            await processor.process([expanded], source="MANUAL")

        if edit.await_count != 1:
            raise AssertionError("same-NewsID expansion was not edited exactly once")
        edit_call = edit.await_args
        if edit_call is None or "complete expanded description" not in edit_call.args[1]:
            raise AssertionError("expanded content was not preserved in the edit")
        if processor._state["42"]["revision"] != 2:
            raise AssertionError("revision did not advance to 2")
        print("manual expansion: edited complete revision 2")


if __name__ == "__main__":
    asyncio.run(main())
