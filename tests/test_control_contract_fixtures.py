from __future__ import annotations

import json
import time
from pathlib import Path
from types import SimpleNamespace

from codex_telegram_bridge.views import render_help, render_sessions_page

FIXTURE_PATH = Path(__file__).parents[1] / "fixtures" / "control_contract" / "9527.json"


def _effects(case: dict[str, object]) -> list[dict[str, object]]:
    return list(case["effects"])


def _render(case: dict[str, object]) -> dict[str, object]:
    return next(effect for effect in _effects(case) if effect["type"] == "render")


def test_9527_fixture_is_consumable_by_python_oracle(monkeypatch) -> None:
    monkeypatch.setenv("TZ", "Asia/Shanghai")
    time.tzset()
    fixtures = json.loads(FIXTURE_PATH.read_text())
    cases = {case["name"]: case for case in fixtures}

    help_case = cases["help_paired_markdown_and_plain"]
    help_render = _render(help_case)
    rendered_help = render_help("9527", label=help_case["request"]["label"], paired=True)
    assert (rendered_help.markdown, rendered_help.plain) == (
        help_render["markdown"],
        help_render["plain"],
    )

    sessions_case = cases["sessions_search_deadline_and_logical_callbacks"]
    request = sessions_case["request"]
    states = [SimpleNamespace(**state) for state in request["sessions"]]
    rendered_sessions = render_sessions_page(
        states,
        page=request["page"],
        query=request["query"],
        now=request["now"],
    )
    sessions_render = _render(sessions_case)
    assert (rendered_sessions.message.markdown, rendered_sessions.message.plain) == (
        sessions_render["markdown"],
        sessions_render["plain"],
    )
    keyboard = sessions_render["keyboard"]
    assert [[button["label"] for button in row] for row in keyboard] == [["①"], ["1"]]


def test_9527_fixture_keeps_only_transport_volatile_values_out_of_the_contract() -> None:
    fixtures = json.loads(FIXTURE_PATH.read_text())
    serialized = json.dumps(fixtures, ensure_ascii=False)
    assert "cb:" not in serialized
    assert "message_id" not in serialized
    assert "nonce" not in serialized
    assert "delete_at" not in serialized
    assert "deadline_seconds" in serialized
