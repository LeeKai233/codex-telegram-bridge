from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from codex_telegram_bridge.tmux import TmuxManager


@pytest.mark.asyncio
async def test_dismiss_plan_prompt_sends_escape_only_when_prompt_is_visible(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    manager = TmuxManager("codex", Path("codex"), tmp_path / "codex.sock")
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(manager, "_find_window", lambda _thread_id: "@window")

    def run(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        del check
        calls.append(args)
        if args[0] == "display-message":
            return subprocess.CompletedProcess(["tmux", *args], 0, "0\n", "")
        if args[0] == "capture-pane":
            return subprocess.CompletedProcess(
                ["tmux", *args], 0, "Implement this plan?\n", ""
            )
        return subprocess.CompletedProcess(["tmux", *args], 0, "", "")

    monkeypatch.setattr(manager, "_run", run)

    assert await manager.dismiss_plan_prompt("thread") is True
    assert calls[-1] == ("send-keys", "-t", "@window", "Escape")
    assert await manager.plan_prompt_visible("thread") is True


@pytest.mark.asyncio
async def test_dismiss_plan_prompt_leaves_other_tmux_content_untouched(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    manager = TmuxManager("codex", Path("codex"), tmp_path / "codex.sock")
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(manager, "_find_window", lambda _thread_id: "@window")

    def run(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        del check
        calls.append(args)
        if args[0] == "display-message":
            return subprocess.CompletedProcess(["tmux", *args], 0, "0\n", "")
        return subprocess.CompletedProcess(["tmux", *args], 0, "Codex is working\n", "")

    monkeypatch.setattr(manager, "_run", run)

    assert await manager.dismiss_plan_prompt("thread") is False
    assert await manager.plan_prompt_visible("thread") is False
    assert not any(call[0] == "send-keys" for call in calls)


@pytest.mark.asyncio
async def test_plan_prompt_visibility_is_unknown_without_a_window(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    manager = TmuxManager("codex", Path("codex"), tmp_path / "codex.sock")
    monkeypatch.setattr(manager, "_find_window", lambda _thread_id: None)

    assert await manager.plan_prompt_visible("thread") is None
    assert await manager.dismiss_plan_prompt("thread") is False


@pytest.mark.asyncio
async def test_plan_prompt_visibility_ignores_dead_tmux_pane(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    manager = TmuxManager("codex", Path("codex"), tmp_path / "codex.sock")
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(manager, "_find_window", lambda _thread_id: "@window")

    def run(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        del check
        calls.append(args)
        return subprocess.CompletedProcess(["tmux", *args], 0, "1\n", "")

    monkeypatch.setattr(manager, "_run", run)

    assert await manager.plan_prompt_visible("thread") is None
    assert await manager.dismiss_plan_prompt("thread") is False
    assert not any(call[0] in {"capture-pane", "send-keys"} for call in calls)
