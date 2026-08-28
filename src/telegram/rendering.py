"""Safe Telegram HTML rendering without content truncation."""

from html import unescape
from html.parser import HTMLParser
from typing import List


TELEGRAM_TEXT_LIMIT = 4096
MAX_TELEGRAM_CHUNKS = 384
MAX_RENDERED_HTML_LENGTH = TELEGRAM_TEXT_LIMIT * MAX_TELEGRAM_CHUNKS
MAX_NORMALIZED_TEXT_LENGTH = MAX_RENDERED_HTML_LENGTH
MAX_NORMALIZED_LIST_DEPTH = 1024
_MAX_PARSER_BUFFER_LENGTH = MAX_NORMALIZED_TEXT_LENGTH + 1


class TelegramRenderLimitError(ValueError):
    """Rendered content exceeds the bounded complete Telegram representation."""


_DROPPED_ELEMENTS = {
    "script", "style", "iframe", "object", "embed", "template", "svg",
    "canvas", "noscript",
}
_BLOCK_ELEMENTS = {
    "p", "div", "section", "article", "header", "footer", "h1", "h2", "h3",
    "h4", "h5", "h6", "blockquote",
}
_BREAK_ELEMENTS = {"br", "hr"}


class _UpstreamHTMLTextParser(HTMLParser):
    """Extract readable text while treating every upstream element as untrusted."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.parts: List[str] = []
        self._dropped_stack: List[str] = []
        self._lists: List[tuple[str, int]] = []
        self._pending_space: bool = False
        self.markup_seen: bool = False
        self._output_length: int = 0

    def _dropping(self) -> bool:
        return bool(self._dropped_stack)

    def _append(self, value: str) -> None:
        if not value:
            return
        next_length = self._output_length + len(value)
        if next_length > _MAX_PARSER_BUFFER_LENGTH:
            raise TelegramRenderLimitError(
                f"normalized text length {next_length} exceeds "
                f"{_MAX_PARSER_BUFFER_LENGTH}"
            )
        self.parts.append(value)
        self._output_length = next_length

    def _newline(self) -> None:
        self._pending_space = False
        while self.parts and self.parts[-1] == " ":
            self.parts.pop()
            self._output_length -= 1
        if self.parts and self.parts[-1] != "\n":
            self._append("\n")

    def _text(self, data: str) -> None:
        words = data.split()
        if not words:
            if data:
                self._pending_space = True
            return
        for index, word in enumerate(words):
            if index or (
                self.parts
                and self.parts[-1] not in {"\n", " "}
                and (self._pending_space or data[0].isspace())
            ):
                self._append(" ")
            self._append(word)
        self._pending_space = data[-1].isspace()

    def handle_starttag(self, tag: str, attrs: List[tuple[str, str | None]]) -> None:
        del attrs
        self.markup_seen = True
        tag = tag.lower()
        if self._dropping():
            if tag in _DROPPED_ELEMENTS:
                self._dropped_stack.append(tag)
            return
        if tag in _DROPPED_ELEMENTS:
            self._dropped_stack.append(tag)
        elif tag in _BLOCK_ELEMENTS or tag in _BREAK_ELEMENTS:
            self._newline()
        elif tag in {"ul", "ol"}:
            self._newline()
            if len(self._lists) >= MAX_NORMALIZED_LIST_DEPTH:
                raise TelegramRenderLimitError(
                    f"normalized list depth exceeds {MAX_NORMALIZED_LIST_DEPTH}"
                )
            self._lists.append((tag, 0))
        elif tag == "li":
            self._newline()
            if self._lists:
                list_tag, count = self._lists[-1]
                count += 1
                self._lists[-1] = (list_tag, count)
                marker = f"{count}. " if list_tag == "ol" else "• "
                indent_length = 2 * (len(self._lists) - 1)
                if self._output_length + indent_length + len(marker) > MAX_NORMALIZED_TEXT_LENGTH:
                    raise TelegramRenderLimitError(
                        "normalized list marker exceeds output budget"
                    )
                self._append("  " * (len(self._lists) - 1))
                self._append(marker)

    def handle_startendtag(self, tag: str, attrs: List[tuple[str, str | None]]) -> None:
        self.markup_seen = True
        tag = tag.lower()
        if self._dropping() or tag in _DROPPED_ELEMENTS:
            return
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_endtag(self, tag: str) -> None:
        self.markup_seen = True
        tag = tag.lower()
        if self._dropping():
            if tag == self._dropped_stack[-1]:
                self._dropped_stack.pop()
            return
        if tag in _BLOCK_ELEMENTS or tag == "li":
            self._newline()
        elif tag in {"ul", "ol"}:
            self._newline()
            if self._lists:
                self._lists.pop()

    def handle_data(self, data: str) -> None:
        if not self._dropping():
            self._text(data)

    def handle_entityref(self, name: str) -> None:
        self.markup_seen = True
        if not self._dropping():
            self._text(unescape(f"&{name};"))

    def handle_charref(self, name: str) -> None:
        self.markup_seen = True
        if not self._dropping():
            self._text(unescape(f"&#{name};"))

    def handle_comment(self, data: str) -> None:
        del data
        self.markup_seen = True

    def handle_decl(self, decl: str) -> None:
        del decl
        self.markup_seen = True

    def unknown_decl(self, data: str) -> None:
        del data
        self.markup_seen = True

    def text(self) -> str:
        result = "".join(self.parts).strip()
        if len(result) > MAX_NORMALIZED_TEXT_LENGTH:
            raise TelegramRenderLimitError(
                f"normalized text length {len(result)} exceeds "
                f"{MAX_NORMALIZED_TEXT_LENGTH}"
            )
        return result


def normalize_upstream_html(text: str) -> str:
    """Convert upstream HTML to normalized text, preserving non-HTML input exactly."""
    parser = _UpstreamHTMLTextParser()
    try:
        parser.feed(text)
        parser.close()
    except TelegramRenderLimitError:
        raise
    except Exception as exc:
        # Never send parser output accumulated before an error: it could be a
        # silently partial editorial item. Callers handle this explicit failure.
        raise TelegramRenderLimitError("unable to normalize upstream HTML") from exc
    return parser.text() if parser.markup_seen else text


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
