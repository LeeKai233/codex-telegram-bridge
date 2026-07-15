from __future__ import annotations

from codex_telegram_bridge.markdown import _step_line
from codex_telegram_bridge.models import PlanStep


def test_completed_plan_step_uses_one_markdown_v2_escape_and_strikethrough() -> None:
    rendered = _step_line(2, PlanStep("done", "completed"))

    assert rendered == r"~2\. done~ ✅"
