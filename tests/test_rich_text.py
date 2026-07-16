from __future__ import annotations

import pytest

from codex_telegram_bridge.rich_text import (
    plain_text_from_html,
    render_commonmark_chunks,
    render_commonmark_html,
)


def test_commonmark_renders_only_safe_telegram_html() -> None:
    rendered = render_commonmark_html(
        """# Result

Text with **bold**, *italic*, `code`, and <b>raw HTML</b>.

- first
- second

> quoted

[safe](https://example.com/path?a=1&b=2)

[unsafe](javascript:alert(1))

![diagram](https://example.com/image.png)
"""
    )

    assert rendered.startswith("<b>Result</b>")
    assert "<b>bold</b>" in rendered
    assert "<i>italic</i>" in rendered
    assert "<code>code</code>" in rendered
    assert "&lt;b&gt;raw HTML&lt;/b&gt;" in rendered
    assert "• first\n• second" in rendered
    assert "<blockquote>quoted</blockquote>" in rendered
    assert '<a href="https://example.com/path?a=1&amp;b=2">safe</a>' in rendered
    assert '<a href="javascript:' not in rendered
    assert "diagram" in rendered
    assert "<img" not in rendered
    assert all(tag not in rendered for tag in ("<p>", "<h1>", "<ul>", "<li>"))


def test_commonmark_chunks_preserve_balanced_formatting_and_plain_fallback() -> None:
    chunks = render_commonmark_chunks(f"**{'x' * 500}**", limit=96)

    assert len(chunks) > 1
    assert all(len(chunk.html) <= 96 for chunk in chunks)
    assert all(chunk.html.count("<b>") == chunk.html.count("</b>") for chunk in chunks)
    assert "".join(chunk.plain for chunk in chunks) == "x" * 500
    assert all(chunk.plain == plain_text_from_html(chunk.html) for chunk in chunks)


def test_commonmark_chunks_keep_code_blocks_valid() -> None:
    source = "```python\n" + "\n".join(f"value_{index} = {index}" for index in range(80)) + "\n```"

    chunks = render_commonmark_chunks(source, limit=160)

    assert len(chunks) > 1
    assert all(len(chunk.html) <= 160 for chunk in chunks)
    assert all(chunk.html.count("<pre>") == chunk.html.count("</pre>") for chunk in chunks)
    assert all(chunk.html.count("<code") == chunk.html.count("</code>") for chunk in chunks)
    assert "".join(chunk.plain for chunk in chunks).startswith("value_0 = 0\n")
    assert "value_79 = 79" in "".join(chunk.plain for chunk in chunks)


def test_commonmark_chunks_pack_semantic_blocks_without_exceeding_limit() -> None:
    source = "\n\n".join(f"Paragraph {index}: " + "word " * 12 for index in range(12))

    chunks = render_commonmark_chunks(source, limit=180)

    assert len(chunks) > 1
    assert all(0 < len(chunk.html) <= 180 for chunk in chunks)
    plain = "\n\n".join(chunk.plain for chunk in chunks)
    for index in range(12):
        assert f"Paragraph {index}:" in plain


def test_commonmark_chunks_handle_empty_input_and_reject_tiny_limit() -> None:
    assert render_commonmark_chunks("  \n") == []
    with pytest.raises(ValueError, match="at least 64"):
        render_commonmark_chunks("text", limit=63)
