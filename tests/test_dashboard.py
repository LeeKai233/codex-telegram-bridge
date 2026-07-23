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
from codex_telegram_bridge.delivery import (
    DeliveryIntent,
    DeliveryKey,
    DeliveryOutcome,
    TelegramDeliveryEngine,
)
from codex_telegram_bridge.models import TaskState, ThreadState
from codex_telegram_bridge.space_dashboard import (
    _IMMEDIATE_REASONS,
    SpaceDashboardManager,
    _AnimationBatch,
)
from codex_telegram_bridge.store import Store
from codex_telegram_bridge.views import RenderedMessage


class BotEndpoint:
    def __init__(self, bot: Any) -> None:
        self.bot = bot

    async def send_text(
        self,
        chat_id: int,
        markdown: str,
        **kwargs: Any,
    ) -> Any:
        kwargs.pop("plain", None)
        kwargs.pop("priority", None)
        return await self.bot.send_message(chat_id=chat_id, text=markdown, **kwargs)


class RecordingDelivery:
    def __init__(self, performed_by_role: dict[str, bool] | None = None) -> None:
        self.intents: list[DeliveryIntent] = []
        self.outcomes: list[DeliveryOutcome] = []
        self.fingerprints: dict[object, str] = {}
        self.performed_by_role = performed_by_role or {}

    def submit(self, intent: DeliveryIntent) -> asyncio.Future[DeliveryOutcome]:
        self.intents.append(intent)
        performed = self.performed_by_role.get(
            intent.key.bot_role,
            self.fingerprints.get(intent.key) != intent.fingerprint,
        )
        self.fingerprints[intent.key] = intent.fingerprint
        future = asyncio.get_running_loop().create_future()
        outcome = DeliveryOutcome(
            key=intent.key,
            revision=len(self.intents),
            status="delivered",
            attempts=1 if performed else 0,
            performed=performed,
        )
        self.outcomes.append(outcome)
        future.set_result(outcome)
        return future


class SequencedDelivery:
    def __init__(self, statuses: list[str]) -> None:
        self.statuses = list(statuses)
        self.intents: list[DeliveryIntent] = []

    def submit(self, intent: DeliveryIntent) -> asyncio.Future[DeliveryOutcome]:
        self.intents.append(intent)
        status = self.statuses.pop(0)
        future = asyncio.get_running_loop().create_future()
        future.set_result(
            DeliveryOutcome(
                key=intent.key,
                revision=len(self.intents),
                status=status,  # type: ignore[arg-type]
                attempts=1,
                performed=status == "delivered",
            )
        )
        return future


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
    delivery = RecordingDelivery()
    manager = DashboardManager(
        BotEndpoint(bot),  # type: ignore[arg-type]
        store,
        delivery,  # type: ignore[arg-type]
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
    for _ in range(100):
        if delivery.intents:
            break
        await asyncio.sleep(0.01)
    assert store.subscriptions()[state.thread_id] == 42
    assert len(bot.send_calls) == 1
    assert len(delivery.intents) == 1
    assert "second" in delivery.intents[0].markdown

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
        BotEndpoint(bot),  # type: ignore[arg-type]
        store,
        RecordingDelivery(),  # type: ignore[arg-type]
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
        BotEndpoint(bot),  # type: ignore[arg-type]
        store,
        RecordingDelivery(),  # type: ignore[arg-type]
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


@pytest.mark.asyncio
async def test_terminal_subscription_retries_after_transport_exhaustion(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    state = subscribed_state(store)
    state.goal = {"status": "complete", "objective": "Done"}
    store.save_thread(state)
    store.set_dashboard_message(state.thread_id, 42)
    delivery = SequencedDelivery(["transient_failure", "delivered"])
    manager = DashboardManager(
        BotEndpoint(RecordingBot()),  # type: ignore[arg-type]
        store,
        delivery,  # type: ignore[arg-type]
        owner_chat_id=lambda: 100,
        debounce_seconds=0.01,
        retry_seconds=0.01,
    )

    await manager.schedule(state, immediate=True)
    for _ in range(100):
        if len(delivery.intents) == 2:
            break
        await asyncio.sleep(0.01)

    assert len(delivery.intents) == 2
    assert all(intent.terminal for intent in delivery.intents)
    await manager.stop()
    store.close()


def test_subagent_item_events_are_debounced_until_a_semantic_boundary() -> None:
    assert {"item/started", "item/completed"}.isdisjoint(_IMMEDIATE_REASONS)
    assert {"turn/completed", "turn/plan/updated"} <= _IMMEDIATE_REASONS


@pytest.mark.asyncio
async def test_space_dashboard_failure_logs_safe_target_context(
    tmp_path: Path,
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
    delivery = TelegramDeliveryEngine(
        {"control": control, "discussion": discussion}  # type: ignore[dict-item]
    )
    delivery.start()
    manager = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        control,  # type: ignore[arg-type]
        discussion,  # type: ignore[arg-type]
        delivery,
    )

    await manager._flush("space-log-context")
    tickets = list(manager._delivery_tickets.values())
    assert tickets
    await asyncio.gather(*tickets)

    assert discussion.edit_calls == [(-100123, 41)]
    assert "event=telegram_delivery_permanent_failure" in caplog.text
    assert "context=space:space-log-context" in caplog.text
    assert "bot_role=discussion" in caplog.text
    assert "chat_id=-100123" in caplog.text
    assert "message_id=41" in caplog.text
    assert "error_type=BadRequest" in caplog.text
    assert "SUPER-SECRET" not in caplog.text
    await delivery.stop(drain_timeout=0)
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
        text = f"channel-{rendered_state.latest_activity}-{animation_frame}"
        return RenderedMessage(text, text)

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
        text = f"status-{rendered_state.latest_activity}-{animation_frame}"
        return RenderedMessage(text, text)

    monkeypatch.setattr(space_dashboard_module, "render_channel_post", render_channel)
    monkeypatch.setattr(space_dashboard_module, "render_status_comment", render_status)
    control = RecordingEndpoint()
    discussion = RecordingEndpoint()
    delivery = RecordingDelivery()
    manager = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        control,  # type: ignore[arg-type]
        discussion,  # type: ignore[arg-type]
        delivery,  # type: ignore[arg-type]
    )

    await manager._flush("space-animation")
    await asyncio.sleep(0)
    state.latest_activity = "second"
    store.save_thread(state)
    await manager._flush("space-animation")
    await asyncio.sleep(0)
    await manager._flush("space-animation")
    await asyncio.sleep(0)

    assert [frame for _space, _state, frame in received if frame is not None] == [0, 0, 1, 1, 2, 2]
    assert [frame for _space, _state, frame in channel_received if frame is not None] == [
        0,
        0,
        1,
        1,
        2,
        2,
    ]
    assert received[0][0]["current_mode"] == "plan"
    assert received[0][0]["normal_model"] == "gpt-5.6-sol"
    assert received[0][0]["plan_effort"] == "max"
    assert received[0][1].tasks[0].model == "gpt-5.6-luna"
    assert received[0][1].tasks[0].reasoning_effort == "max"
    assert [item.key.bot_role for item in delivery.intents] == [
        "control",
        "discussion",
        "control",
        "discussion",
        "control",
        "discussion",
    ]
    assert all(outcome.performed for outcome in delivery.outcomes)
    assert delivery.intents[0].fingerprint != delivery.intents[2].fingerprint
    assert delivery.intents[2].fingerprint != delivery.intents[4].fingerprint
    store.close()


def test_complete_goal_does_not_freeze_moon_during_review() -> None:
    manager = object.__new__(SpaceDashboardManager)
    manager._animation_indices = {}
    space = {"space_id": "space-review", "lifecycle": "active"}
    state = ThreadState(
        thread_id="thread-review",
        status="idle",
        turn_status="completed",
        review_status="inProgress",
        goal={"status": "complete", "objective": "Review changes"},
    )

    assert manager._is_terminal(space, state) is False
    assert manager._is_animated(space, state) is True
    assert manager._frame_for("space-review", terminal=False) == 0
    assert manager._frame_for("space-review", terminal=True) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("performed_by_role", "expected_frame"),
    [
        ({"control": False, "discussion": False}, 0),
        ({"control": False, "discussion": True}, 1),
    ],
)
async def test_animation_batch_requires_a_real_delivery_to_advance(
    tmp_path: Path,
    performed_by_role: dict[str, bool],
    expected_frame: int,
) -> None:
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        allowed_root=tmp_path,
    )
    store = Store(config.database_path)
    delivery = RecordingDelivery(performed_by_role)
    manager = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        delivery,  # type: ignore[arg-type]
    )
    space_id = "space-animation-batch"
    keys = {
        DeliveryKey("control", -100122, 39),
        DeliveryKey("discussion", -100123, 41),
    }
    manager._animation_batches[space_id] = _AnimationBatch(
        frame=0,
        targets=keys,
        advance=True,
    )

    for revision, key in enumerate(keys, 1):
        ticket: asyncio.Future[DeliveryOutcome] = asyncio.get_running_loop().create_future()
        manager._delivery_tickets[key] = ticket
        ticket.set_result(
            DeliveryOutcome(
                key=key,
                revision=revision,
                status="delivered",
                attempts=0 if not performed_by_role[key.bot_role] else 1,
                performed=performed_by_role[key.bot_role],
            )
        )
        manager._delivery_finished(key, space_id, ticket, 0)

    assert manager._animation_indices.get(space_id, 0) == expected_frame
    assert space_id not in manager._animation_batches
    store.close()


@pytest.mark.asyncio
async def test_superseded_dashboard_ticket_releases_its_fingerprint(tmp_path: Path) -> None:
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        allowed_root=tmp_path,
    )
    store = Store(config.database_path)
    manager = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        RecordingDelivery(),  # type: ignore[arg-type]
    )
    key = DeliveryKey("discussion", -100123, 41)
    old_ticket: asyncio.Future[DeliveryOutcome] = asyncio.get_running_loop().create_future()
    current_ticket: asyncio.Future[DeliveryOutcome] = asyncio.get_running_loop().create_future()
    manager._delivery_tickets[key] = current_ticket
    manager._delivery_fingerprints[old_ticket] = "old"
    manager._delivery_fingerprints[current_ticket] = "current"
    old_ticket.set_result(
        DeliveryOutcome(
            key=key,
            revision=1,
            status="delivered",
            attempts=1,
            performed=True,
        )
    )

    manager._delivery_finished(key, "space-superseded", old_ticket, 0)

    assert old_ticket not in manager._delivery_fingerprints
    assert manager._delivery_tickets[key] is current_ticket
    assert manager._delivery_fingerprints[current_ticket] == "current"
    store.close()


@pytest.mark.asyncio
async def test_space_dashboard_submits_channel_and_discussion_independently(tmp_path: Path) -> None:
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
    delivery = RecordingDelivery()
    manager = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        delivery,  # type: ignore[arg-type]
    )

    await manager._flush("space-concurrent")

    assert {(item.key.bot_role, item.key.message_id) for item in delivery.intents} == {
        ("control", 39),
        ("discussion", 41),
    }
    store.close()


@pytest.mark.asyncio
async def test_space_dashboard_restart_uses_persisted_message_state_for_noop(
    tmp_path: Path,
) -> None:
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        allowed_root=tmp_path,
    )
    store = Store(config.database_path)
    message_states: dict[str, dict[str, Any]] = {}

    def get_message_state(message_key: str) -> dict[str, Any] | None:
        return message_states.get(message_key)

    def put_message_state(
        message_key: str,
        *,
        bot_role: str,
        chat_id: int,
        message_id: int,
        semantic_fingerprint: str,
        state: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        row = {
            "message_key": message_key,
            "bot_role": bot_role,
            "chat_id": chat_id,
            "message_id": message_id,
            "semantic_fingerprint": semantic_fingerprint,
            "state": state,
            "payload": dict(payload or {}),
        }
        message_states[message_key] = row
        return row

    store.get_telegram_message_state = get_message_state  # type: ignore[attr-defined]
    store.put_telegram_message_state = put_message_state  # type: ignore[attr-defined]
    store.create_space(
        {
            "space_id": "space-restart-noop",
            "lifecycle": "active",
            "thread_id": "thread-restart-noop",
            "channel_chat_id": -100122,
            "channel_post_id": 39,
        }
    )
    store.save_thread(
        ThreadState(
            thread_id="thread-restart-noop",
            title="Stable dashboard",
            status="idle",
            updated_at=1_700_000_000,
        )
    )

    first_delivery = RecordingDelivery()
    first = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        first_delivery,  # type: ignore[arg-type]
    )
    await first._flush("space-restart-noop")
    await asyncio.sleep(0)

    assert len(first_delivery.intents) == 1
    assert message_states["dashboard:control:-100122:39"]["state"] == "delivered"

    restarted_delivery = RecordingDelivery()
    restarted = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        restarted_delivery,  # type: ignore[arg-type]
    )
    await restarted._flush("space-restart-noop")

    assert restarted_delivery.intents == []
    store.close()


@pytest.mark.asyncio
async def test_complete_goal_uses_full_moon_terminal_delivery(tmp_path: Path) -> None:
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        allowed_root=tmp_path,
    )
    store = Store(config.database_path)
    store.create_space(
        {
            "space_id": "space-complete",
            "lifecycle": "active",
            "thread_id": "thread-complete",
            "discussion_chat_id": -100123,
            "status_message_id": 41,
            "current_mode": "plan",
        }
    )
    store.save_thread(
        ThreadState(
            thread_id="thread-complete",
            status="idle",
            goal={"status": "complete", "objective": "Done"},
        )
    )
    delivery = RecordingDelivery()
    manager = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        delivery,  # type: ignore[arg-type]
    )

    await manager._flush("space-complete")
    await asyncio.sleep(0)
    await manager._flush("space-complete")

    assert all(item.terminal for item in delivery.intents)
    assert all(item.priority == 5 for item in delivery.intents)
    assert all(item.markdown.startswith("🌕") for item in delivery.intents)
    store.close()


@pytest.mark.asyncio
async def test_terminal_space_requeues_after_transport_exhaustion(tmp_path: Path) -> None:
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        allowed_root=tmp_path,
        dashboard_debounce_seconds=0.01,
    )
    store = Store(config.database_path)
    store.create_space(
        {
            "space_id": "space-terminal-retry",
            "lifecycle": "active",
            "thread_id": "thread-terminal-retry",
            "discussion_chat_id": -100123,
            "status_message_id": 41,
        }
    )
    store.save_thread(
        ThreadState(
            thread_id="thread-terminal-retry",
            status="idle",
            goal={"status": "complete", "objective": "Done"},
        )
    )
    delivery = SequencedDelivery(["transient_failure", "delivered"])
    manager = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        delivery,  # type: ignore[arg-type]
    )

    await manager.schedule_space("space-terminal-retry", immediate=True)
    for _ in range(100):
        if len(delivery.intents) == 2:
            break
        await asyncio.sleep(0.01)

    assert len(delivery.intents) == 2
    assert all(intent.terminal for intent in delivery.intents)
    await manager.stop()
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
        RecordingDelivery(),  # type: ignore[arg-type]
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
    store.create_space(
        {"space_id": "active-space", "lifecycle": "active", "channel_post_id": 1}
    )
    store.create_space(
        {"space_id": "pending-space", "lifecycle": "pending", "status_message_id": 2}
    )
    store.create_space(
        {"space_id": "closed-space", "lifecycle": "closed", "status_message_id": 3}
    )
    manager = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        RecordingDelivery(),  # type: ignore[arg-type]
    )
    scheduled: list[tuple[str, bool, str]] = []

    async def schedule(
        space_id: str,
        *,
        immediate: bool = False,
        lane: str = "live",
    ) -> None:
        scheduled.append((space_id, immediate, lane))

    monkeypatch.setattr(manager, "schedule_space", schedule)
    await manager.start()
    await asyncio.sleep(0.03)
    await manager.stop()

    assert set(scheduled) == {
        ("active-space", True, "maintenance"),
        ("pending-space", True, "maintenance"),
    }
    assert ("closed-space", True, "maintenance") not in scheduled
    store.close()


@pytest.mark.asyncio
async def test_space_dashboard_routes_status_role_and_refresh_lanes(tmp_path: Path) -> None:
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        allowed_root=tmp_path,
    )
    store = Store(config.database_path)
    space = {
        "space_id": "space-status-role",
        "generation": 1,
        "lifecycle": "active",
        "thread_id": "thread-status-role",
        "discussion_chat_id": -100123,
        "status_message_id": 41,
        "status_bot_role": "status",
    }
    store.get_space = lambda _space_id: dict(space)  # type: ignore[method-assign]
    store.save_thread(ThreadState(thread_id="thread-status-role", status="active"))
    delivery = RecordingDelivery()
    discussion = RecordingEndpoint()
    status = RecordingEndpoint()
    manager = SpaceDashboardManager(
        config,
        store,
        StaticSecurity(),  # type: ignore[arg-type]
        RecordingEndpoint(),  # type: ignore[arg-type]
        discussion,  # type: ignore[arg-type]
        delivery,  # type: ignore[arg-type]
        status=status,  # type: ignore[arg-type]
    )

    await manager._flush("space-status-role", lane="maintenance")
    await asyncio.sleep(0)

    assert manager.status is status
    assert [(item.key.bot_role, item.lane) for item in delivery.intents] == [
        ("status", "maintenance")
    ]
    keyboard = delivery.intents[0].reply_markup
    assert keyboard is not None
    callback_data = keyboard.inline_keyboard[0][0].callback_data
    assert callback_data is not None
    nonce = callback_data.removeprefix("cb:")
    callback = store.peek_callback(nonce, 0, bot_role="status")
    assert callback is not None
    assert store.peek_callback(nonce, 0, bot_role="discussion") is None

    terminal = ThreadState(
        thread_id="thread-status-role",
        status="idle",
        goal={"status": "complete", "objective": "Done"},
    )
    store.save_thread(terminal)
    await manager._flush("space-status-role", lane="live")

    assert delivery.intents[-1].lane == "interactive"
    assert delivery.intents[-1].terminal is True
    store.close()
