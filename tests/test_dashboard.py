from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from telegram.error import NetworkError

from codex_telegram_bridge.dashboard import DashboardManager
from codex_telegram_bridge.models import ThreadState
from codex_telegram_bridge.space_dashboard import _IMMEDIATE_REASONS
from codex_telegram_bridge.store import Store


class DirectMessenger:
    async def call(self, operation: Any, *, priority: int = 10) -> Any:
        del priority
        return await operation()


class RecordingBot:
    def __init__(self) -> None:
        self.send_calls: list[dict[str, Any]] = []
        self.edit_calls: list[dict[str, Any]] = []

    async def send_message(self, **kwargs: Any) -> Any:
        self.send_calls.append(kwargs)
        return SimpleNamespace(message_id=42)

    async def edit_message_text(self, **kwargs: Any) -> None:
        self.edit_calls.append(kwargs)


class BlockingBot(RecordingBot):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.edited = asyncio.Event()
        self.send_cancelled = False

    async def send_message(self, **kwargs: Any) -> Any:
        self.send_calls.append(kwargs)
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.send_cancelled = True
            raise
        return SimpleNamespace(message_id=42)

    async def edit_message_text(self, **kwargs: Any) -> None:
        self.edit_calls.append(kwargs)
        self.edited.set()


class RetryBot(RecordingBot):
    def __init__(self) -> None:
        super().__init__()
        self.delivered = asyncio.Event()

    async def send_message(self, **kwargs: Any) -> Any:
        self.send_calls.append(kwargs)
        if len(self.send_calls) == 1:
            raise NetworkError("request failed at https://api.telegram.org/botSUPER-SECRET/sendMessage")
        self.delivered.set()
        return SimpleNamespace(message_id=42)


def subscribed_state(store: Store, *, activity: str = "first") -> ThreadState:
    state = ThreadState(
        thread_id="thread-12345678",
        title="Dashboard test",
        cwd="/home/example",
        status="idle",
        latest_activity=activity,
        subscribed=True,
    )
    store.save_thread(state)
    store.subscribe(state.thread_id)
    return state


@pytest.mark.asyncio
async def test_immediate_update_does_not_cancel_inflight_dashboard_send(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    state = subscribed_state(store)
    bot = BlockingBot()
    manager = DashboardManager(
        bot,  # type: ignore[arg-type]
        store,
        DirectMessenger(),  # type: ignore[arg-type]
        owner_chat_id=lambda: 100,
        debounce_seconds=0.01,
        retry_seconds=0.01,
    )

    await manager.schedule(state, immediate=True)
    await asyncio.wait_for(bot.started.wait(), timeout=1)
    await manager.schedule(replace(state, latest_activity="second"), immediate=True)
    await asyncio.sleep(0)

    assert not bot.send_cancelled
    assert len(bot.send_calls) == 1

    bot.release.set()
    await asyncio.wait_for(bot.edited.wait(), timeout=1)
    assert store.subscriptions()[state.thread_id] == 42
    assert len(bot.send_calls) == 1
    assert len(bot.edit_calls) == 1

    await manager.stop()
    store.close()


@pytest.mark.asyncio
async def test_failed_dashboard_send_keeps_dirty_state_and_redacts_exception(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    state = subscribed_state(store)
    bot = RetryBot()
    manager = DashboardManager(
        bot,  # type: ignore[arg-type]
        store,
        DirectMessenger(),  # type: ignore[arg-type]
        owner_chat_id=lambda: 100,
        retry_seconds=0.01,
    )

    await manager.schedule(state, immediate=True)
    await asyncio.wait_for(bot.delivered.wait(), timeout=1)

    assert len(bot.send_calls) == 2
    assert store.subscriptions()[state.thread_id] == 42
    assert "SUPER-SECRET" not in caplog.text
    assert "NetworkError" in caplog.text

    await manager.stop()
    store.close()


@pytest.mark.asyncio
async def test_heartbeat_refreshes_idle_subscriptions(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    state = subscribed_state(store)
    bot = RecordingBot()
    manager = DashboardManager(
        bot,  # type: ignore[arg-type]
        store,
        DirectMessenger(),  # type: ignore[arg-type]
        owner_chat_id=lambda: 100,
        heartbeat_seconds=0.01,  # type: ignore[arg-type]
    )

    manager.start()
    for _ in range(100):
        if store.subscriptions()[state.thread_id] == 42:
            break
        await asyncio.sleep(0.01)

    assert store.subscriptions()[state.thread_id] == 42
    assert len(bot.send_calls) == 1

    await manager.stop()
    store.close()


def test_subagent_item_events_refresh_space_dashboard_immediately() -> None:
    assert {"item/started", "item/completed"} <= _IMMEDIATE_REASONS
