from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from telegram import Chat, MessageOriginChannel
from telegram.constants import ChatType
from telegram.error import TelegramError

from codex_telegram_bridge.models import SessionSpace, ThreadState
from codex_telegram_bridge.space_coordinator import SessionSpaceCoordinator
from codex_telegram_bridge.store import Store


class BridgeStub:
    def __init__(self, store: Store, state: ThreadState) -> None:
        self.store = store
        self.state = state
        self.activations: list[tuple[str, str]] = []
        self.closures: list[tuple[str, int]] = []
        self.subscriptions: list[str] = []

    async def subscribe_space_thread(self, thread_id: str) -> ThreadState:
        self.subscriptions.append(thread_id)
        return self.state

    async def activate_pending_session(
        self, space_id: str, *, client_message_id: str
    ) -> ThreadState:
        self.activations.append((space_id, client_message_id))
        return self.state

    async def close_session_space(self, space_id: str, generation: int) -> SessionSpace:
        self.closures.append((space_id, generation))
        self.store.close_space(space_id, expected_generation=generation)
        closed = self.store.get_session_space(space_id)
        assert closed is not None
        return closed


class Dashboards:
    def __init__(self) -> None:
        self.scheduled: list[tuple[str, bool]] = []

    async def schedule_space(self, space_id: str, *, immediate: bool = False) -> None:
        self.scheduled.append((space_id, immediate))


class Endpoint:
    def __init__(self, message_id: int) -> None:
        self.message_id = message_id
        self.sent: list[dict[str, Any]] = []

    async def send_text(self, chat_id: int, markdown: str, **kwargs: Any) -> Any:
        self.sent.append({"chat_id": chat_id, "markdown": markdown, **kwargs})
        return SimpleNamespace(message_id=self.message_id)


class FlakyEndpoint(Endpoint):
    def __init__(self, message_id: int, *, failures: int) -> None:
        super().__init__(message_id)
        self.failures = failures

    async def send_text(self, chat_id: int, markdown: str, **kwargs: Any) -> Any:
        self.sent.append({"chat_id": chat_id, "markdown": markdown, **kwargs})
        if self.failures > 0:
            self.failures -= 1
            raise TelegramError("temporary provisioning failure")
        return SimpleNamespace(message_id=self.message_id)


def make_coordinator(
    tmp_path: Path,
    *,
    control: Endpoint | None = None,
    discussion: Endpoint | None = None,
    provision_max_attempts: int = 3,
    provision_retry_seconds: float = 30.0,
) -> tuple[SessionSpaceCoordinator, Store, BridgeStub, Dashboards]:
    store = Store(tmp_path / "state.sqlite3")
    state = ThreadState(thread_id="thread-1", cwd=str(tmp_path), status="active")
    store.save_thread(state)
    bridge = BridgeStub(store, state)
    dashboards = Dashboards()
    coordinator = SessionSpaceCoordinator(
        store,
        bridge,  # type: ignore[arg-type]
        control or Endpoint(100),  # type: ignore[arg-type]
        discussion or Endpoint(200),  # type: ignore[arg-type]
        dashboards,  # type: ignore[arg-type]
        provision_max_attempts=provision_max_attempts,
        provision_retry_seconds=provision_retry_seconds,
    )
    return coordinator, store, bridge, dashboards


@pytest.mark.asyncio
async def test_repair_required_pending_space_retries_bridge_activation(tmp_path: Path) -> None:
    coordinator, store, bridge, dashboards = make_coordinator(tmp_path)
    store.create_space(
        {
            "space_id": "space-repair",
            "space_type": "pending_new",
            "lifecycle": "repair_required",
            "thread_id": "thread-1",
            "pending_cwd": str(tmp_path),
            "pending_prompt": "Initial prompt",
        }
    )

    state = await coordinator.activate_pending("space-repair")

    assert state.thread_id == "thread-1"
    assert bridge.activations == [
        ("space-repair", "telegram-new-space-repair-1")
    ]
    assert store.get_space("space-repair")["lifecycle"] == "active"  # type: ignore[index]
    assert dashboards.scheduled == [("space-repair", True)]
    store.close()


@pytest.mark.asyncio
async def test_follow_existing_thread_subscribes_before_creating_channel_post(
    tmp_path: Path,
) -> None:
    coordinator, store, bridge, _dashboards = make_coordinator(tmp_path)
    bridge.state.model = "gpt-5.6-sol"
    bridge.state.reasoning_effort = "xhigh"
    store.set_telegram_binding({"channel_chat_id": -1001, "discussion_chat_id": -1002})

    space = await coordinator.follow_thread("thread-1")

    assert bridge.subscriptions == ["thread-1"]
    assert space["thread_id"] == "thread-1"
    assert space["lifecycle"] == "pending"
    assert space["channel_post_id"] == 100
    assert (space["normal_model"], space["normal_effort"]) == (
        "gpt-5.6-sol",
        "xhigh",
    )
    store.close()


@pytest.mark.asyncio
async def test_reset_transport_reconciles_one_new_post_root_and_status(tmp_path: Path) -> None:
    control = Endpoint(100)
    discussion = Endpoint(200)
    coordinator, store, _bridge, _dashboards = make_coordinator(
        tmp_path,
        control=control,
        discussion=discussion,
    )
    store.set_telegram_binding({"channel_chat_id": -1001, "discussion_chat_id": -1002})
    store.create_space(
        {
            "space_id": "space-rebuild",
            "space_type": "existing",
            "lifecycle": "active",
            "thread_id": "thread-1",
            "channel_chat_id": -1001,
            "channel_post_id": 90,
            "discussion_chat_id": -1002,
            "discussion_root_id": 190,
            "status_message_id": 191,
        }
    )
    store.record_discussion_root(-1001, 90, -1002, 190)
    reset = store.reset_space_transport("space-rebuild", expected_generation=1)
    assert reset is not None and reset["generation"] == 2

    await coordinator.reconcile()

    posted = store.get_space("space-rebuild")
    assert posted is not None
    assert posted["channel_post_id"] == 100
    assert posted["status_message_id"] is None
    assert len(control.sent) == 1
    assert control.sent[0].get("reply_markup") is None

    channel = Chat(-1001, ChatType.CHANNEL, title="Example Channel")
    forwarded = SimpleNamespace(
        chat_id=-1002,
        message_id=201,
        forward_origin=MessageOriginChannel(
            date=datetime.now(UTC),
            chat=channel,
            message_id=100,
        ),
        sender_chat=channel,
    )
    first = await coordinator.handle_automatic_forward(forwarded)
    second = await coordinator.handle_automatic_forward(forwarded)

    assert first is not None and second is not None
    current = store.get_space("space-rebuild")
    assert current is not None
    assert current["lifecycle"] == "active"
    assert current["discussion_root_id"] == 201
    assert current["status_message_id"] == 200
    assert len(discussion.sent) == 1
    assert discussion.sent[0]["reply_parameters"].message_id == 201
    store.close()


@pytest.mark.asyncio
async def test_reconcile_retries_failed_channel_post_and_clears_attempt_state(
    tmp_path: Path,
) -> None:
    control = FlakyEndpoint(100, failures=1)
    coordinator, store, _bridge, _dashboards = make_coordinator(
        tmp_path,
        control=control,
        provision_retry_seconds=0,
    )
    store.set_telegram_binding({"channel_chat_id": -1001, "discussion_chat_id": -1002})

    with pytest.raises(TelegramError):
        await coordinator.follow_thread("thread-1")

    failed = store.get_space_by_thread("thread-1")
    assert failed is not None
    assert failed["lifecycle"] == "repair_required"
    assert failed["channel_post_id"] is None
    assert (failed["provision_stage"], failed["provision_attempts"]) == ("channel_post", 1)

    await coordinator.reconcile()

    repaired = store.get_space_by_thread("thread-1")
    assert repaired is not None
    assert repaired["channel_post_id"] == 100
    assert repaired["lifecycle"] == "pending"
    assert (repaired["provision_stage"], repaired["provision_attempts"]) == ("", 0)
    assert len(control.sent) == 2
    store.close()


@pytest.mark.asyncio
async def test_channel_post_reconciliation_stops_after_bounded_attempts(tmp_path: Path) -> None:
    control = FlakyEndpoint(100, failures=10)
    coordinator, store, _bridge, _dashboards = make_coordinator(
        tmp_path,
        control=control,
        provision_max_attempts=3,
        provision_retry_seconds=0,
    )
    store.set_telegram_binding({"channel_chat_id": -1001, "discussion_chat_id": -1002})

    with pytest.raises(TelegramError):
        await coordinator.follow_thread("thread-1")
    for _ in range(4):
        await coordinator.reconcile()

    failed = store.get_space_by_thread("thread-1")
    assert failed is not None
    assert failed["channel_post_id"] is None
    assert failed["provision_attempts"] == 3
    assert len(control.sent) == 3
    store.close()


@pytest.mark.asyncio
async def test_status_comment_reconciliation_stops_after_bounded_attempts(tmp_path: Path) -> None:
    discussion = FlakyEndpoint(200, failures=10)
    coordinator, store, _bridge, _dashboards = make_coordinator(
        tmp_path,
        discussion=discussion,
        provision_max_attempts=2,
        provision_retry_seconds=0,
    )
    store.create_space(
        {
            "space_id": "space-status-retry",
            "space_type": "existing",
            "lifecycle": "pending",
            "thread_id": "thread-1",
            "channel_chat_id": -1001,
            "channel_post_id": 101,
            "discussion_chat_id": -1002,
            "discussion_root_id": 201,
        }
    )
    store.record_discussion_root(-1001, 101, -1002, 201)

    for _ in range(4):
        await coordinator.reconcile()

    failed = store.get_space("space-status-retry")
    assert failed is not None
    assert failed["status_message_id"] is None
    assert (failed["provision_stage"], failed["provision_attempts"]) == ("status_comment", 2)
    assert len(discussion.sent) == 2
    store.close()


@pytest.mark.asyncio
async def test_coordinator_close_uses_bridge_without_stopping_running_turn(tmp_path: Path) -> None:
    coordinator, store, bridge, dashboards = make_coordinator(tmp_path)
    store.create_space(
        {
            "space_id": "space-active",
            "space_type": "existing",
            "lifecycle": "active",
            "thread_id": "thread-1",
        }
    )

    closed = await coordinator.close("space-active", 1)

    assert bridge.closures == [("space-active", 1)]
    assert (closed["lifecycle"], closed["generation"]) == ("closed", 2)
    assert store.get_thread("thread-1").status == "active"  # type: ignore[union-attr]
    assert dashboards.scheduled == [("space-active", True)]
    store.close()


@pytest.mark.asyncio
async def test_startup_reconciles_crash_after_root_without_duplicate_status(
    tmp_path: Path,
) -> None:
    coordinator, store, bridge, dashboards = make_coordinator(tmp_path)
    store.create_space(
        {
            "space_id": "space-crashed",
            "space_type": "existing",
            "lifecycle": "repair_required",
            "thread_id": "thread-1",
            "channel_chat_id": -1001,
            "channel_post_id": 101,
            "discussion_chat_id": -1002,
            "discussion_root_id": 201,
        }
    )
    store.record_discussion_root(-1001, 101, -1002, 201)

    await coordinator.start()
    for _ in range(100):
        if (current := store.get_space("space-crashed")) and current.get("status_message_id"):
            break
        await asyncio.sleep(0)
    await coordinator.stop()

    space = store.get_space("space-crashed")
    assert space is not None
    assert (space["status_message_id"], space["lifecycle"]) == (200, "active")
    assert bridge.subscriptions == ["thread-1"]
    assert len(coordinator.discussion.sent) == 1  # type: ignore[attr-defined]

    restarted = SessionSpaceCoordinator(
        store,
        bridge,  # type: ignore[arg-type]
        coordinator.control,
        coordinator.discussion,
        dashboards,  # type: ignore[arg-type]
    )
    await restarted.start()
    await asyncio.sleep(0)
    await restarted.stop()

    assert len(coordinator.discussion.sent) == 1  # type: ignore[attr-defined]
    store.close()


@pytest.mark.asyncio
async def test_repair_with_persisted_status_schedules_edit_instead_of_resending(
    tmp_path: Path,
) -> None:
    coordinator, store, _bridge, dashboards = make_coordinator(tmp_path)
    store.create_space(
        {
            "space_id": "space-bound",
            "space_type": "existing",
            "lifecycle": "repair_required",
            "thread_id": "thread-1",
            "channel_chat_id": -1001,
            "channel_post_id": 102,
            "discussion_chat_id": -1002,
            "discussion_root_id": 202,
            "status_message_id": 302,
            "provision_stage": "status_comment",
            "provision_attempts": 3,
            "provision_retry_at": 4_000_000_000.0,
        }
    )

    await coordinator.reconcile()

    space = store.get_space("space-bound")
    assert space is not None and space["lifecycle"] == "active"
    assert (space["provision_stage"], space["provision_attempts"]) == ("", 0)
    assert dashboards.scheduled == [("space-bound", True)]
    assert coordinator.discussion.sent == []  # type: ignore[attr-defined]
    store.close()
