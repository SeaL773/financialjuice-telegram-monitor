"""Safe Telegram HTML rendering without content truncation."""

from typing import List


TELEGRAM_TEXT_LIMIT = 4096
MAX_TELEGRAM_CHUNKS = 384
MAX_RENDERED_HTML_LENGTH = TELEGRAM_TEXT_LIMIT * MAX_TELEGRAM_CHUNKS


class TelegramRenderLimitError(ValueError):
    """Rendered content exceeds the bounded complete Telegram representation."""


def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _escaped_length(text: str) -> int:
    return len(escape_html(text))


def split_html_text(text: str, max_length: int = TELEGRAM_TEXT_LIMIT) -> List[str]:
    """Return escaped, independently valid HTML chunks preserving every character."""
    if max_length <= 0:
        raise ValueError("max_length must be positive")
    if not text:
        return [""]

    chunks: List[str] = []
    cursor = 0
    text_length = len(text)
    while cursor < text_length:
        used = 0
        end = cursor
        last_newline = -1
        for index in range(cursor, text_length):
            char = text[index]
            char_length = _escaped_length(char)
            if used + char_length > max_length:
                break
            used += char_length
            end = index + 1
            if char == "\n":
                last_newline = end
        else:
            end = text_length

        if end == cursor:
            raise ValueError("max_length is too small for an escaped character")
        if end < text_length and last_newline > cursor:
            end = last_newline

        chunks.append(escape_html(text[cursor:end]))
        cursor = end
    return chunks


def render_message_chunks(text: str, group_label: str = "UPDATE") -> List[str]:
    """Render one message, or a clearly numbered complete message group."""
    escaped = escape_html(text)
    if len(escaped) > MAX_RENDERED_HTML_LENGTH:
        raise TelegramRenderLimitError(
            f"rendered HTML length {len(escaped)} exceeds {MAX_RENDERED_HTML_LENGTH}"
        )
    if len(escaped) <= TELEGRAM_TEXT_LIMIT:
        return [escaped]

    # Reserve enough room for labels and practical chunk counts. Re-split if the
    # actual numbering ever exceeds the reserve rather than truncating content.
    reserve = 64
    while True:
        bodies = split_html_text(text, TELEGRAM_TEXT_LIMIT - reserve)
        count = len(bodies)
        if count > MAX_TELEGRAM_CHUNKS:
            raise TelegramRenderLimitError(
                f"Telegram chunk count {count} exceeds {MAX_TELEGRAM_CHUNKS}"
            )
        rendered = [
            f"<b>{escape_html(group_label)} ({index}/{count})</b>\n{body}"
            for index, body in enumerate(bodies, start=1)
        ]
        if all(len(chunk) <= TELEGRAM_TEXT_LIMIT for chunk in rendered):
            return rendered
        reserve *= 2
        if reserve >= TELEGRAM_TEXT_LIMIT:
            raise ValueError("Unable to render Telegram message group")
