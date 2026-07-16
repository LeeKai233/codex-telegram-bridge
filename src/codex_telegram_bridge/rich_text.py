from __future__ import annotations

import html
import re
import unicodedata
from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urlsplit

from markdown_it import MarkdownIt
from markdown_it.token import Token

TELEGRAM_TEXT_LIMIT = 4096

_MARKDOWN = MarkdownIt("commonmark", {"html": False})
_HTML_ATOM = re.compile(r"(</?[a-z]+(?:\s+[^<>]*)?>|&(?:#\d+|#x[0-9a-fA-F]+|[a-zA-Z]+);)")
_OPEN_TAG = re.compile(r"^<([a-z]+)(?:\s|>)")
_CLOSE_TAG = re.compile(r"^</([a-z]+)>$")
_LANGUAGE = re.compile(r"^[A-Za-z0-9_+.-]{1,40}$")


@dataclass(frozen=True, slots=True)
class TelegramHtmlChunk:
    html: str
    plain: str


@dataclass(slots=True)
class _ListState:
    ordered: bool
    next_index: int = 1


class _PlainTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def plain_text_from_html(value: str) -> str:
    parser = _PlainTextParser()
    parser.feed(value)
    parser.close()
    return "".join(parser.parts)


def _safe_link(value: str) -> str | None:
    if not value or len(value) > 2048:
        return None
    if any(
        character.isspace() or unicodedata.category(character) == "Cc"
        for character in value
    ):
        return None
    parsed = urlsplit(value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return None
    return value


class _TelegramHtmlRenderer:
    def __init__(self) -> None:
        self.parts: list[str] = []
        self.lists: list[_ListState] = []
        self.blockquote_depth = 0

    def render(self, tokens: list[Token]) -> str:
        for token in tokens:
            self._render_block(token)
        return "".join(self.parts).strip()

    def _trailing_newlines(self) -> int:
        count = 0
        for part in reversed(self.parts):
            for character in reversed(part):
                if character != "\n":
                    return count
                count += 1
                if count >= 2:
                    return count
        return count

    def _newline(self, count: int = 1) -> None:
        missing = count - self._trailing_newlines()
        if missing > 0:
            self.parts.append("\n" * missing)

    def _strip_trailing_newlines(self) -> None:
        while self.parts:
            trimmed = self.parts[-1].rstrip("\n")
            if trimmed:
                self.parts[-1] = trimmed
                return
            self.parts.pop()

    def _render_block(self, token: Token) -> None:
        token_type = token.type
        if token_type == "inline":
            self._render_inline(token.children or [])
        elif token_type == "paragraph_close":
            self._newline(1 if token.hidden or self.lists else 2)
        elif token_type == "heading_open":
            self.parts.append("<b>")
        elif token_type == "heading_close":
            self.parts.append("</b>")
            self._newline(2)
        elif token_type == "bullet_list_open":
            self.lists.append(_ListState(ordered=False))
        elif token_type == "ordered_list_open":
            start = token.attrGet("start")
            self.lists.append(_ListState(ordered=True, next_index=int(start or 1)))
        elif token_type in {"bullet_list_close", "ordered_list_close"}:
            if self.lists:
                self.lists.pop()
            self._newline(1 if self.lists else 2)
        elif token_type == "list_item_open":
            self._newline(1 if self.parts else 0)
            state = self.lists[-1]
            prefix = f"{state.next_index}. " if state.ordered else "• "
            state.next_index += 1
            self.parts.append(f"{'  ' * max(0, len(self.lists) - 1)}{prefix}")
        elif token_type == "list_item_close":
            self._newline(1)
        elif token_type == "blockquote_open":
            if self.blockquote_depth == 0:
                self.parts.append("<blockquote>")
            self.blockquote_depth += 1
        elif token_type == "blockquote_close":
            self.blockquote_depth = max(0, self.blockquote_depth - 1)
            if self.blockquote_depth == 0:
                self._strip_trailing_newlines()
                self.parts.append("</blockquote>")
                self._newline(2)
        elif token_type in {"fence", "code_block"}:
            language = token.info.strip().split(maxsplit=1)[0] if token.info.strip() else ""
            class_name = (
                f' class="language-{html.escape(language, quote=True)}"'
                if _LANGUAGE.fullmatch(language)
                else ""
            )
            self.parts.append(
                f"<pre><code{class_name}>{html.escape(token.content, quote=False)}</code></pre>"
            )
            self._newline(2)
        elif token_type == "hr":
            self.parts.append("────────")
            self._newline(2)
        elif token_type in {"html_block", "html_inline"}:
            self.parts.append(html.escape(token.content, quote=False))
            self._newline(2)
        elif token.content:
            self.parts.append(html.escape(token.content, quote=False))

    def _render_inline(self, tokens: list[Token]) -> None:
        link_stack: list[bool] = []
        for token in tokens:
            token_type = token.type
            if token_type == "text":
                self.parts.append(html.escape(token.content, quote=False))
            elif token_type == "code_inline":
                self.parts.append(f"<code>{html.escape(token.content, quote=False)}</code>")
            elif token_type in {"softbreak", "hardbreak"}:
                self.parts.append("\n")
            elif token_type == "strong_open":
                self.parts.append("<b>")
            elif token_type == "strong_close":
                self.parts.append("</b>")
            elif token_type == "em_open":
                self.parts.append("<i>")
            elif token_type == "em_close":
                self.parts.append("</i>")
            elif token_type == "s_open":
                self.parts.append("<s>")
            elif token_type == "s_close":
                self.parts.append("</s>")
            elif token_type == "link_open":
                href = _safe_link(token.attrGet("href") or "")
                link_stack.append(href is not None)
                if href is not None:
                    self.parts.append(f'<a href="{html.escape(href, quote=True)}">')
            elif token_type == "link_close":
                if link_stack and link_stack.pop():
                    self.parts.append("</a>")
            elif token_type == "image":
                self.parts.append(html.escape(token.content or "image", quote=False))
            elif token_type == "html_inline" or token.content:
                self.parts.append(html.escape(token.content, quote=False))


def render_commonmark_html(markdown: str) -> str:
    return _TelegramHtmlRenderer().render(_MARKDOWN.parse(markdown))


def _semantic_source_blocks(markdown: str) -> list[str]:
    lines = markdown.splitlines(keepends=True)
    tokens = _MARKDOWN.parse(markdown)
    blocks: list[str] = []
    cursor = 0
    for token in tokens:
        if token.level != 0 or token.map is None:
            continue
        start, end = token.map
        if start < cursor or end <= start:
            continue
        prefix = "".join(lines[cursor:start])
        if prefix.strip():
            blocks.append(prefix)
        block = "".join(lines[start:end])
        if block.strip():
            blocks.append(block)
        cursor = end
    suffix = "".join(lines[cursor:])
    if suffix.strip():
        blocks.append(suffix)
    if not blocks and markdown.strip():
        blocks.append(markdown)
    return blocks


def _closing_tags(stack: list[tuple[str, str]]) -> str:
    return "".join(f"</{name}>" for name, _opening in reversed(stack))


def _opening_tags(stack: list[tuple[str, str]]) -> str:
    return "".join(opening for _name, opening in stack)


def _text_pieces(value: str) -> list[str]:
    return re.findall(r"\n+|[^\S\n]+|[^\s]+", value)


def _split_long_piece(value: str, size: int) -> tuple[str, str]:
    if size <= 0:
        return "", value
    return value[:size], value[size:]


def _split_rendered_html(value: str, limit: int) -> list[str]:
    atoms = [part for part in _HTML_ATOM.split(value) if part]
    chunks: list[str] = []
    stack: list[tuple[str, str]] = []
    current = ""

    def flush() -> None:
        nonlocal current
        closed = current + _closing_tags(stack)
        if closed.strip():
            chunks.append(closed)
        current = _opening_tags(stack)

    for atom in atoms:
        closing = _CLOSE_TAG.fullmatch(atom)
        opening = _OPEN_TAG.match(atom) if closing is None else None
        if closing is not None:
            if len(current) + len(atom) + len(_closing_tags(stack[:-1])) > limit:
                flush()
            current += atom
            if stack and stack[-1][0] == closing.group(1):
                stack.pop()
            continue
        if opening is not None:
            prospective = [*stack, (opening.group(1), atom)]
            if len(current) + len(atom) + len(_closing_tags(prospective)) > limit:
                flush()
            current += atom
            stack.append((opening.group(1), atom))
            continue

        pieces = [atom] if atom.startswith("&") and atom.endswith(";") else _text_pieces(atom)
        for piece in pieces:
            remainder = piece
            while remainder:
                room = limit - len(current) - len(_closing_tags(stack))
                if room <= 0:
                    flush()
                    room = limit - len(current) - len(_closing_tags(stack))
                if remainder.startswith("&") and remainder.endswith(";") and len(remainder) > room:
                    flush()
                    room = limit - len(current) - len(_closing_tags(stack))
                head, remainder = _split_long_piece(remainder, room)
                if not head:
                    raise ValueError("Telegram HTML formatting exceeds the message limit")
                current += head
                if remainder:
                    flush()
    if current and current != _opening_tags(stack):
        flush()
    return chunks


def render_commonmark_chunks(
    markdown: str,
    *,
    limit: int = TELEGRAM_TEXT_LIMIT,
) -> list[TelegramHtmlChunk]:
    if limit < 64:
        raise ValueError("Telegram HTML chunk limit must be at least 64 characters")

    rendered_blocks = [
        rendered
        for source in _semantic_source_blocks(markdown)
        if (rendered := render_commonmark_html(source))
    ]
    html_chunks: list[str] = []
    current = ""
    for block in rendered_blocks:
        separator = "\n\n" if current else ""
        if len(current) + len(separator) + len(block) <= limit:
            current += separator + block
            continue
        if current:
            html_chunks.append(current)
            current = ""
        if len(block) <= limit:
            current = block
        else:
            html_chunks.extend(_split_rendered_html(block, limit))
    if current:
        html_chunks.append(current)

    return [
        TelegramHtmlChunk(html=value, plain=plain_text_from_html(value))
        for value in html_chunks
    ]
