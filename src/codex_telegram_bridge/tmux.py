from __future__ import annotations

import asyncio
import re
import shlex
import subprocess
from pathlib import Path


class TmuxError(RuntimeError):
    pass


def _window_name(title: str, thread_id: str) -> str:
    cleaned = re.sub(r"[^\w\-\u4e00-\u9fff]+", "-", title, flags=re.UNICODE).strip("-")
    return (cleaned[:24] or "codex") + "-" + thread_id[:6]


class TmuxManager:
    def __init__(self, session_name: str, codex_binary: Path, codex_socket: Path) -> None:
        self.session_name = session_name
        self.codex_binary = codex_binary
        self.codex_socket = codex_socket

    async def ensure_window(self, thread_id: str, title: str, cwd: Path) -> str:
        return await asyncio.to_thread(self._ensure_window, thread_id, title, cwd)

    def _run(self, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(["tmux", *args], text=True, capture_output=True, timeout=10, check=False)
        if check and result.returncode:
            raise TmuxError(result.stderr.strip() or result.stdout.strip() or "tmux command failed")
        return result

    def _session_exists(self) -> bool:
        return self._run("has-session", "-t", self.session_name, check=False).returncode == 0

    def _find_window(self, thread_id: str) -> str | None:
        if not self._session_exists():
            return None
        result = self._run("list-windows", "-t", self.session_name, "-F", "#{window_id}\t#{@codex_thread_id}")
        for line in result.stdout.splitlines():
            window_id, _, value = line.partition("\t")
            if value == thread_id:
                return window_id
        return None

    def _ensure_window(self, thread_id: str, title: str, cwd: Path) -> str:
        existing = self._find_window(thread_id)
        if existing:
            return existing
        name = _window_name(title, thread_id)
        command = shlex.join(
            [
                str(self.codex_binary),
                "--remote",
                f"unix://{self.codex_socket}",
                "-C",
                str(cwd),
                "resume",
                thread_id,
            ]
        )
        if self._session_exists():
            result = self._run(
                "new-window",
                "-d",
                "-P",
                "-F",
                "#{window_id}",
                "-t",
                self.session_name,
                "-n",
                name,
                "-c",
                str(cwd),
                command,
            )
        else:
            result = self._run(
                "new-session",
                "-d",
                "-P",
                "-F",
                "#{window_id}",
                "-s",
                self.session_name,
                "-n",
                name,
                "-c",
                str(cwd),
                command,
            )
        window_id = result.stdout.strip()
        self._run("set-option", "-w", "-t", window_id, "@codex_thread_id", thread_id)
        return window_id

    async def window_for(self, thread_id: str) -> str | None:
        return await asyncio.to_thread(self._find_window, thread_id)

    async def dismiss_plan_prompt(self, thread_id: str) -> bool:
        return await asyncio.to_thread(self._dismiss_plan_prompt, thread_id)

    async def plan_prompt_visible(self, thread_id: str) -> bool | None:
        return await asyncio.to_thread(self._plan_prompt_visible, thread_id)

    def _capture_plan_prompt(self, thread_id: str) -> tuple[str, bool] | None:
        window = self._find_window(thread_id)
        if not window:
            return None
        pane = self._run(
            "display-message", "-p", "-t", window, "#{pane_dead}", check=False
        )
        if pane.returncode != 0 or pane.stdout.strip() == "1":
            return None
        captured = self._run(
            "capture-pane",
            "-p",
            "-t",
            window,
            "-S",
            "-40",
            check=False,
        )
        if captured.returncode != 0:
            return None
        return window, "Implement this plan?" in captured.stdout

    def _plan_prompt_visible(self, thread_id: str) -> bool | None:
        captured = self._capture_plan_prompt(thread_id)
        return captured[1] if captured is not None else None

    def _dismiss_plan_prompt(self, thread_id: str) -> bool:
        captured = self._capture_plan_prompt(thread_id)
        if captured is None or not captured[1]:
            return False
        window = captured[0]
        self._run("send-keys", "-t", window, "Escape", check=False)
        return True
