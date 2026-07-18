from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from telegram.error import BadRequest, NetworkError

import codex_telegram_bridge.space_dashboard as space_dashboard_module
from codex_telegram_bridge.config import Config
from codex_telegram_bridge.dashboard import DashboardManager
from codex_telegram_bridge.models import TaskState, ThreadState
from codex_telegram_bridge.space_dashboard import _IMMEDIATE_REASONS, SpaceDashboardManager
from codex_telegram_bridge.store import Store
from codex_telegram_bridge.views import RenderedMessage


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


class StaticSecurity:
    def space_unlock_remaining(self, _space_id: str) -> int:
        return 0


class RecordingEndpoint:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.edit_calls: list[tuple[int, int]] = []

    async def edit_text(
        self,
        chat_id: int,
        message_id: int,
        _markdown: str,
        **_kwargs: Any,
    ) -> None:
        self.edit_calls.append((chat_id, message_id))
        if self.error:
            raise self.error


class ConcurrentEndpoint(RecordingEndpoint):
    def __init__(self, started: list[str], ready: asyncio.Event, label: str) -> None:
        super().__init__()
        self.started = started
        self.ready = ready
        self.label = label

    async def edit_text(
        self,
        chat_id: int,
        message_id: int,
        _markdown: str,
        **_kwargs: Any,
    ) -> None:
        self.edit_calls.append((chat_id, message_id))
        self.started.append(self.label)
        if len(self.started) == 2:
            self.ready.set()
        await asyncio.wait_for(self.ready.wait(), timeout=0.2)


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


def test_subagent_item_events_are_debounced_until_a_semantic_boundary() -> None:
    assert {"item/started", "item/completed"}.isdisjoint(_IMMEDIATE_REASONS)
    assert {"turn/completed", "turn/plan/updated"} <= _IMMEDIATE_REASONS


@pytest.mark.asyncio
async def test_space_dashboard_failure_logs_safe_target_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        allowed_root=tmp_path,
    )
    store = Store(config.database_path)
    store.create_space(
        {
            "space_id": "space-log-context",
            "lifecycle": "active",
            "thread_id": "thread-log-context",
            "discussion_chat_id": -100123,
            "discussion_root_id": 40,
            "status_message_id": 41,
        }
    )
    store.save_thread(
        ThreadState(
            thread_id="thread-log-context",
            title="Logging test",
            cwd=str(tmp_path),
            status="idle",
        )
    )
    control = RecordingEndpoint()
    discussion = RecordingEndpoint(
        BadRequest(
            "invalid dashboard target at https://api.telegram.org/bot123456789:SUPER-SECRET/editMessageText"
        )
    )
    manager = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        control,  # type: ignore[arg-type]
        discussion,  # type: ignore[arg-type]
    )

    async def stop_after_retry(delay: float) -> None:
        assert delay == 5
        manager._stopping = True

    monkeypatch.setattr(space_dashboard_module.asyncio, "sleep", stop_after_retry)
    manager._dirty.add("space-log-context")
    manager._immediate.add("space-log-context")
    await manager._worker("space-log-context")

    assert discussion.edit_calls == [(-100123, 41)]
    assert "event=space_dashboard_target_failed" in caplog.text
    assert "event=space_dashboard_update_failed" in caplog.text
    assert "space_id=space-log-context" in caplog.text
    assert "bot_role=discussion" in caplog.text
    assert "chat_id=-100123" in caplog.text
    assert "message_id=41" in caplog.text
    assert "error_type=BadRequest" in caplog.text
    assert "invalid dashboard target" in caplog.text
    assert "SUPER-SECRET" not in caplog.text
    store.close()


@pytest.mark.asyncio
async def test_space_dashboard_passes_mode_profiles_tasks_and_advancing_animation_frame(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        allowed_root=tmp_path,
    )
    store = Store(config.database_path)
    store.create_space(
        {
            "space_id": "space-animation",
            "lifecycle": "active",
            "thread_id": "thread-animation",
            "channel_chat_id": -100122,
            "channel_post_id": 39,
            "discussion_chat_id": -100123,
            "discussion_root_id": 40,
            "status_message_id": 41,
            "current_mode": "plan",
            "normal_model": "gpt-5.6-sol",
            "normal_effort": "xhigh",
            "plan_model": "gpt-5.6-luna",
            "plan_effort": "max",
        }
    )
    state = ThreadState(
        thread_id="thread-animation",
        title="Animation test",
        cwd=str(tmp_path),
        status="active",
        tasks=[
            TaskState(
                task_id="subagent-1",
                title="Review",
                status="inProgress",
                model="gpt-5.6-luna",
                reasoning_effort="max",
            )
        ],
    )
    store.save_thread(state)
    received: list[tuple[dict[str, Any], ThreadState, int | None]] = []
    channel_received: list[tuple[dict[str, Any], ThreadState, int | None]] = []

    def render_channel(
        rendered_state: ThreadState,
        *,
        space: dict[str, Any],
        lifecycle: str,
        heartbeat_seconds: int,
        animation_frame: int | None = None,
    ) -> RenderedMessage:
        del lifecycle, heartbeat_seconds
        channel_received.append((space, rendered_state, animation_frame))
        return RenderedMessage("channel", "channel")

    def render_status(
        rendered_state: ThreadState,
        *,
        space: dict[str, Any],
        lifecycle: str,
        auth_expires_at: int | None,
        heartbeat_seconds: int,
        animation_frame: int | None = None,
    ) -> RenderedMessage:
        del lifecycle, auth_expires_at, heartbeat_seconds
        received.append((space, rendered_state, animation_frame))
        return RenderedMessage("status", "status")

    monkeypatch.setattr(space_dashboard_module, "render_channel_post", render_channel)
    monkeypatch.setattr(space_dashboard_module, "render_status_comment", render_status)
    control = RecordingEndpoint()
    discussion = RecordingEndpoint()
    manager = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        control,  # type: ignore[arg-type]
        discussion,  # type: ignore[arg-type]
    )

    await manager._flush("space-animation")
    await manager._flush("space-animation")

    assert [frame for _space, _state, frame in received] == [0, 1]
    assert [frame for _space, _state, frame in channel_received] == [0, 1]
    assert received[0][0]["current_mode"] == "plan"
    assert received[0][0]["normal_model"] == "gpt-5.6-sol"
    assert received[0][0]["plan_effort"] == "max"
    assert received[0][1].tasks[0].model == "gpt-5.6-luna"
    assert received[0][1].tasks[0].reasoning_effort == "max"
    assert discussion.edit_calls == [(-100123, 41), (-100123, 41)]
    assert control.edit_calls == [(-100122, 39), (-100122, 39)]
    store.close()


@pytest.mark.asyncio
async def test_space_dashboard_edits_channel_and_discussion_concurrently(tmp_path: Path) -> None:
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        allowed_root=tmp_path,
    )
    store = Store(config.database_path)
    store.create_space(
        {
            "space_id": "space-concurrent",
            "lifecycle": "active",
            "thread_id": "thread-concurrent",
            "channel_chat_id": -100122,
            "channel_post_id": 39,
            "discussion_chat_id": -100123,
            "discussion_root_id": 40,
            "status_message_id": 41,
        }
    )
    store.save_thread(ThreadState(thread_id="thread-concurrent", status="active"))
    started: list[str] = []
    ready = asyncio.Event()
    manager = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        ConcurrentEndpoint(started, ready, "channel"),  # type: ignore[arg-type]
        ConcurrentEndpoint(started, ready, "discussion"),  # type: ignore[arg-type]
    )

    await manager._flush("space-concurrent")

    assert set(started) == {"channel", "discussion"}
    store.close()


@pytest.mark.asyncio
async def test_resynced_thread_refreshes_space_dashboard_immediately(tmp_path: Path) -> None:
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        allowed_root=tmp_path,
    )
    store = Store(config.database_path)
    store.create_space(
        {
            "space_id": "space-resynced",
            "lifecycle": "active",
            "thread_id": "thread-resynced",
        }
    )
    manager = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
    )
    scheduled: list[tuple[str, bool]] = []

    async def schedule_space(space_id: str, *, immediate: bool = False) -> None:
        scheduled.append((space_id, immediate))

    manager.schedule_space = schedule_space  # type: ignore[method-assign]

    await manager.on_thread_change(
        ThreadState(thread_id="thread-resynced", status="idle"),
        "thread/resynced",
    )

    assert scheduled == [("space-resynced", True)]
    store.close()


@pytest.mark.asyncio
async def test_space_dashboard_start_has_no_periodic_animation_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        allowed_root=tmp_path,
    )
    store = Store(config.database_path)
    store.create_space({"space_id": "active-space", "lifecycle": "active"})
    store.create_space({"space_id": "pending-space", "lifecycle": "pending"})
    manager = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
    )
    scheduled: list[tuple[str, bool]] = []

    async def schedule(space_id: str, *, immediate: bool = False) -> None:
        scheduled.append((space_id, immediate))

    monkeypatch.setattr(manager, "schedule_space", schedule)
    await manager.start()
    await asyncio.sleep(0.03)
    await manager.stop()

    assert scheduled == [("active-space", True), ("pending-space", True)]
    store.close()
