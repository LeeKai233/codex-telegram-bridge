from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from telegram import Chat, ForceReply, MessageOriginChannel
from telegram.constants import ChatType
from telegram.ext import ApplicationHandlerStop

from codex_telegram_bridge.config import Config
from codex_telegram_bridge.control_bot import ControlBotController
from codex_telegram_bridge.deletions import MessageDeletionManager
from codex_telegram_bridge.discussion_bot import DiscussionBotController
from codex_telegram_bridge.metrics import MetricsSnapshot
from codex_telegram_bridge.models import Owner, SessionSpace, ThreadState
from codex_telegram_bridge.space_coordinator import SessionSpaceCoordinator
from codex_telegram_bridge.space_dashboard import SpaceDashboardManager
from codex_telegram_bridge.store import Store
from codex_telegram_bridge.telegram_common import CONTROL_ROLE, DISCUSSION_ROLE

OWNER_ID = 7
OWNER_CHAT_ID = 70
CHANNEL_CHAT_ID = -100111
DISCUSSION_CHAT_ID = -100222


class RecordingEndpoint:
    def __init__(self, role: str, *, first_message_id: int) -> None:
        self.role = role
        self.bot = SimpleNamespace()
        self.next_message_id = first_message_id
        self.sent: list[dict[str, Any]] = []
        self.edited: list[dict[str, Any]] = []
        self.deleted: list[tuple[int, int]] = []
        self.callback_answers: list[dict[str, Any]] = []
        self.before_send_returns: Callable[[dict[str, Any], Any], Awaitable[None]] | None = None

    async def send_text(
        self,
        chat_id: int,
        markdown: str,
        *,
        plain: str | None = None,
        reply_markup: Any = None,
        reply_parameters: Any = None,
        priority: int = 10,
    ) -> Any:
        payload = {
            "chat_id": chat_id,
            "markdown": markdown,
            "plain": plain,
            "reply_markup": reply_markup,
            "reply_parameters": reply_parameters,
            "priority": priority,
        }
        self.sent.append(payload)
        message = SimpleNamespace(message_id=self.next_message_id)
        self.next_message_id += 1
        callback = self.before_send_returns
        if callback is not None:
            self.before_send_returns = None
            await callback(payload, message)
        return message

    async def edit_text(self, chat_id: int, message_id: int, markdown: str, **kwargs: Any) -> Any:
        self.edited.append(
            {"chat_id": chat_id, "message_id": message_id, "markdown": markdown, **kwargs}
        )
        return True

    async def delete_message(self, chat_id: int, message_id: int, *, priority: int = 0) -> bool:
        del priority
        self.deleted.append((chat_id, message_id))
        return True

    async def answer_callback(
        self, query: Any, text: str | None = None, *, show_alert: bool = False
    ) -> None:
        self.callback_answers.append({"query": query, "text": text, "show_alert": show_alert})


class FakePathPolicy:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def validate_directory(self, path: str | Path) -> Path:
        candidate = Path(path).resolve()
        if not candidate.is_dir() or not candidate.is_relative_to(self.root):
            raise ValueError("directory is outside the allowed root")
        return candidate


class FakeMetrics:
    def __init__(self) -> None:
        gib = 1024**3
        self.snapshot = MetricsSnapshot(
            sampled_at=1_700_000_000,
            uptime_seconds=3600,
            load=(0.1, 0.2, 0.3),
            cpu_percent=25.0,
            memory_total=8 * gib,
            memory_available=4 * gib,
            memory_percent=50.0,
            swap_total=2 * gib,
            swap_used=0,
            swap_percent=0.0,
            disk_total=100 * gib,
            disk_free=75 * gib,
            disk_percent=25.0,
            codex_processes=2,
            codex_rss=gib,
            codex_cpu=10.0,
        )

    async def with_gpu(self) -> MetricsSnapshot:
        return self.snapshot


class FakeBridge:
    def __init__(self, store: Store, root: Path) -> None:
        self.store = store
        self.path_policy = FakePathPolicy(root)
        self.metrics = FakeMetrics()
        self.directory_candidates = [root]
        self.activation_calls: list[tuple[str, str]] = []
        self.close_calls: list[tuple[str, int]] = []
        self.prompt_calls: list[dict[str, Any]] = []
        self.answers: list[tuple[str, dict[str, list[str]]]] = []
        self.ask_calls: list[dict[str, Any]] = []
        self.ask_waiters: dict[str, asyncio.Future[str]] = {}
        self.on_question: Any = None
        self.on_notice: Any = None
        self.on_question_resolved: Any = None

    async def resolve_directory(self, description: str) -> list[Path]:
        assert description
        return self.directory_candidates

    async def list_sessions(
        self, *, search_term: str | None = None, limit: int = 200
    ) -> list[ThreadState]:
        states = self.store.list_threads()
        if search_term:
            term = search_term.casefold()
            states = [
                state
                for state in states
                if term in state.thread_id.casefold() or term in state.title.casefold()
            ]
        return states[:limit]

    async def refresh(self, thread_id: str) -> ThreadState:
        state = self.store.get_thread(thread_id)
        if state is None:
            state = ThreadState(
                thread_id=thread_id,
                title=f"Session {thread_id}",
                cwd=str(self.path_policy.root),
                status="idle",
            )
            self.store.save_thread(state)
        return state

    async def activate_pending_session(
        self, space_id: str, *, client_message_id: str
    ) -> ThreadState:
        self.activation_calls.append((space_id, client_message_id))
        space = self.store.get_session_space(space_id)
        assert space is not None
        state = ThreadState(
            thread_id=f"created-{len(self.activation_calls)}",
            title=space.pending_prompt[:80],
            cwd=space.pending_cwd,
            status="active",
        )
        self.store.save_thread(state)
        space.thread_id = state.thread_id
        space.lifecycle = "active"
        space.pending_cwd = ""
        space.pending_prompt = ""
        self.store.save_session_space(space)
        return state

    async def close_session_space(self, space_id: str, generation: int) -> SessionSpace:
        self.close_calls.append((space_id, generation))
        closed = self.store.close_space(space_id, expected_generation=generation)
        if closed is None:
            raise RuntimeError("stale generation")
        model = self.store.get_session_space(space_id)
        assert model is not None
        return model

    async def send_space_prompt(
        self,
        space_id: str,
        prompt: str,
        *,
        mode: str,
        client_message_id: str,
    ) -> str:
        self.prompt_calls.append(
            {
                "space_id": space_id,
                "prompt": prompt,
                "mode": mode,
                "client_message_id": client_message_id,
            }
        )
        return "started"

    async def answer_question(
        self, request_key: str, answers: dict[str, list[str]]
    ) -> None:
        self.answers.append((request_key, answers))

    async def ask_space_question(
        self,
        space_id: str,
        question: str,
        *,
        client_message_id: str,
    ) -> str:
        future = asyncio.get_running_loop().create_future()
        self.ask_calls.append(
            {
                "space_id": space_id,
                "question": question,
                "client_message_id": client_message_id,
            }
        )
        self.ask_waiters[client_message_id] = future
        return await future


class FakeSecurity:
    def __init__(self) -> None:
        self.unlocked: set[str] = set()
        self.verifications: list[tuple[str, str]] = []

    def verify_for_space(self, space_id: str, code: str) -> bool:
        self.verifications.append((space_id, code))
        if code != "123456":
            return False
        self.unlocked.add(space_id)
        return True

    def is_space_unlocked(self, space_id: str) -> bool:
        return space_id in self.unlocked

    def lock_space(self, space_id: str) -> None:
        self.unlocked.discard(space_id)

    def space_unlock_remaining(self, space_id: str) -> int:
        return 60 if space_id in self.unlocked else 0


class FakeDashboards:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bool]] = []

    async def schedule_space(self, space_id: str, *, immediate: bool = False) -> None:
        self.calls.append((space_id, immediate))


@dataclass
class Rig:
    config: Config
    store: Store
    bridge: FakeBridge
    security: FakeSecurity
    dashboards: FakeDashboards
    control_endpoint: RecordingEndpoint
    discussion_endpoint: RecordingEndpoint
    deletions: MessageDeletionManager
    coordinator: SessionSpaceCoordinator
    control: ControlBotController
    discussion: DiscussionBotController


@pytest.fixture
def rig(tmp_path: Path) -> Iterator[Rig]:
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        allowed_root=tmp_path,
        callback_seconds=60,
    )
    store = Store(config.database_path)
    store.set_owner(Owner(OWNER_ID, OWNER_CHAT_ID, "owner"))
    store.set_telegram_binding(
        {
            "channel_chat_id": CHANNEL_CHAT_ID,
            "discussion_chat_id": DISCUSSION_CHAT_ID,
        }
    )
    bridge = FakeBridge(store, tmp_path)
    security = FakeSecurity()
    dashboards = FakeDashboards()
    control_endpoint = RecordingEndpoint(CONTROL_ROLE, first_message_id=1000)
    discussion_endpoint = RecordingEndpoint(DISCUSSION_ROLE, first_message_id=2000)
    deletions = MessageDeletionManager(
        store,
        {CONTROL_ROLE: control_endpoint, DISCUSSION_ROLE: discussion_endpoint},
        poll_seconds=0.01,
    )
    coordinator = SessionSpaceCoordinator(
        store,
        bridge,  # type: ignore[arg-type]
        control_endpoint,  # type: ignore[arg-type]
        discussion_endpoint,  # type: ignore[arg-type]
        dashboards,  # type: ignore[arg-type]
    )
    control = ControlBotController(
        config,
        store,
        security,  # type: ignore[arg-type]
        bridge,  # type: ignore[arg-type]
        control_endpoint,  # type: ignore[arg-type]
        coordinator,
        deletions,
    )
    discussion = DiscussionBotController(
        config,
        store,
        security,  # type: ignore[arg-type]
        bridge,  # type: ignore[arg-type]
        control_endpoint,  # type: ignore[arg-type]
        discussion_endpoint,  # type: ignore[arg-type]
        coordinator,
        dashboards,  # type: ignore[arg-type]
        deletions,
    )
    value = Rig(
        config,
        store,
        bridge,
        security,
        dashboards,
        control_endpoint,
        discussion_endpoint,
        deletions,
        coordinator,
        control,
        discussion,
    )
    try:
        yield value
    finally:
        store.close()


def automatic_forward(channel_post_id: int, root_message_id: int) -> Any:
    channel = Chat(CHANNEL_CHAT_ID, ChatType.CHANNEL, title="Example Channel")
    origin = MessageOriginChannel(
        date=datetime.now(UTC),
        chat=channel,
        message_id=channel_post_id,
    )
    return SimpleNamespace(
        chat_id=DISCUSSION_CHAT_ID,
        message_id=root_message_id,
        forward_origin=origin,
        sender_chat=channel,
        is_automatic_forward=True,
        message_thread_id=None,
        reply_to_message=None,
        text=None,
        caption=None,
        photo=[],
        document=None,
    )


def update_for_message(
    text: str,
    *,
    update_id: int,
    message_id: int,
    chat_id: int,
    chat_type: str,
    user_id: int = OWNER_ID,
    message_thread_id: int | None = None,
    reply_to_message: Any = None,
    sender_chat: Any = None,
) -> Any:
    message = SimpleNamespace(
        text=text,
        caption=None,
        message_id=message_id,
        chat_id=chat_id,
        message_thread_id=message_thread_id,
        reply_to_message=reply_to_message,
        is_automatic_forward=False,
        sender_chat=sender_chat,
        photo=[],
        document=None,
    )
    return SimpleNamespace(
        update_id=update_id,
        effective_chat=SimpleNamespace(id=chat_id, type=chat_type),
        effective_user=SimpleNamespace(id=user_id, username="owner"),
        effective_message=message,
        callback_query=None,
    )


def add_active_space(
    rig: Rig,
    *,
    space_id: str,
    thread_id: str,
    root_message_id: int,
    channel_post_id: int,
) -> dict[str, Any]:
    rig.store.save_thread(
        ThreadState(
            thread_id=thread_id,
            title=f"Session {thread_id}",
            cwd=str(rig.config.allowed_root),
            status="idle",
        )
    )
    space = rig.store.create_space(
        {
            "space_id": space_id,
            "space_type": "existing",
            "lifecycle": "active",
            "thread_id": thread_id,
            "channel_chat_id": CHANNEL_CHAT_ID,
            "channel_post_id": channel_post_id,
            "discussion_chat_id": DISCUSSION_CHAT_ID,
            "discussion_root_id": root_message_id,
            "status_message_id": root_message_id + 1000,
        }
    )
    rig.store.record_discussion_root(
        CHANNEL_CHAT_ID,
        channel_post_id,
        DISCUSSION_CHAT_ID,
        root_message_id,
    )
    rig.store.record_discussion_message(
        DISCUSSION_CHAT_ID,
        root_message_id,
        root_message_id,
        space_id,
    )
    return space


@pytest.mark.asyncio
async def test_update_deduplication_is_role_scoped_and_guards_enforce_owner(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-a",
        thread_id="thread-a",
        root_message_id=501,
        channel_post_id=101,
    )
    private = update_for_message(
        "/sessions",
        update_id=77,
        message_id=11,
        chat_id=OWNER_CHAT_ID,
        chat_type=ChatType.PRIVATE,
    )
    discussion = update_for_message(
        "/status",
        update_id=77,
        message_id=12,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=int(space["discussion_root_id"]),
    )

    await rig.control._guard(private, SimpleNamespace())
    await rig.discussion._guard(discussion, SimpleNamespace())
    assert rig.store.telegram_update_seen(77, CONTROL_ROLE)
    assert rig.store.telegram_update_seen(77, DISCUSSION_ROLE)

    with pytest.raises(ApplicationHandlerStop):
        await rig.control._guard(private, SimpleNamespace())
    with pytest.raises(ApplicationHandlerStop):
        await rig.discussion._guard(discussion, SimpleNamespace())

    outsider = update_for_message(
        "/status",
        update_id=78,
        message_id=13,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        user_id=999,
        message_thread_id=501,
    )
    with pytest.raises(ApplicationHandlerStop):
        await rig.discussion._guard(outsider, SimpleNamespace())
    assert rig.bridge.prompt_calls == []


@pytest.mark.asyncio
async def test_unbound_bind_rejects_invalid_identities_with_actionable_reply(rig: Rig) -> None:
    rig.store.clear_telegram_binding()
    rig.discussion.config = replace(rig.config, control_bot_label="控制_[Bot]")
    rig.discussion_endpoint.bot.username = "session_discussion_bot"
    channel_identity = update_for_message(
        "/bind@session_discussion_bot ABC123",
        update_id=79,
        message_id=14,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        user_id=999_001,
        sender_chat=SimpleNamespace(id=CHANNEL_CHAT_ID),
    )

    with pytest.raises(ApplicationHandlerStop):
        await rig.discussion._guard(channel_identity, SimpleNamespace())

    assert len(rig.discussion_endpoint.sent) == 1
    assert "发送身份切换为个人账号" in rig.discussion_endpoint.sent[0]["markdown"]
    assert "控制\\_\\[Bot\\]" in rig.discussion_endpoint.sent[0]["markdown"]
    assert "控制_[Bot]" in rig.discussion_endpoint.sent[0]["plain"]
    assert "ABC123" not in rig.discussion_endpoint.sent[0]["markdown"]
    assert rig.store.get_telegram_binding() is None
    assert rig.store.get_meta("bind_code_failures", 0) == 0

    wrong_account = update_for_message(
        "/bind@session_discussion_bot ABC123",
        update_id=80,
        message_id=15,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        user_id=999,
    )
    with pytest.raises(ApplicationHandlerStop):
        await rig.discussion._guard(wrong_account, SimpleNamespace())
    assert len(rig.discussion_endpoint.sent) == 2
    assert "ABC123" not in rig.discussion_endpoint.sent[1]["markdown"]
    assert rig.store.get_telegram_binding() is None
    assert rig.store.get_meta("bind_code_failures", 0) == 0

    forwarded = update_for_message(
        "/bind channel post",
        update_id=81,
        message_id=16,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        user_id=999_001,
        sender_chat=SimpleNamespace(id=CHANNEL_CHAT_ID),
    )
    forwarded.effective_message.is_automatic_forward = True
    with pytest.raises(ApplicationHandlerStop):
        await rig.discussion._guard(forwarded, SimpleNamespace())
    assert len(rig.discussion_endpoint.sent) == 2

    personal_identity = update_for_message(
        "/bind@session_discussion_bot ABC123",
        update_id=82,
        message_id=17,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
    )
    await rig.discussion._guard(personal_identity, SimpleNamespace())


@pytest.mark.asyncio
async def test_early_automatic_forward_is_reconciled_after_channel_post_returns(rig: Rig) -> None:
    async def forward_before_send_returns(payload: dict[str, Any], message: Any) -> None:
        assert payload["chat_id"] == CHANNEL_CHAT_ID
        result = await rig.coordinator.handle_automatic_forward(
            automatic_forward(message.message_id, 501)
        )
        assert result is None

    rig.control_endpoint.before_send_returns = forward_before_send_returns

    space = await rig.coordinator.create_pending(rig.config.allowed_root, "Initial prompt")

    assert space["discussion_root_id"] == 501
    assert space["status_message_id"] == 2000
    assert len(rig.discussion_endpoint.sent) == 1
    reply = rig.discussion_endpoint.sent[0]["reply_parameters"]
    assert reply.message_id == 501
    assert rig.store.resolve_discussion_root(DISCUSSION_CHAT_ID, 2000) == {
        "root_message_id": 501,
        "space_id": space["space_id"],
    }


@pytest.mark.asyncio
async def test_late_duplicate_automatic_forwards_provision_one_status_message(rig: Rig) -> None:
    space = await rig.coordinator.create_pending(rig.config.allowed_root, "Initial prompt")
    assert space["status_message_id"] is None
    forwarded = automatic_forward(int(space["channel_post_id"]), 502)

    first, second = await __import__("asyncio").gather(
        rig.coordinator.handle_automatic_forward(forwarded),
        rig.coordinator.handle_automatic_forward(forwarded),
    )

    assert first is not None and second is not None
    assert first["status_message_id"] == second["status_message_id"] == 2000
    assert len(rig.discussion_endpoint.sent) == 1


@pytest.mark.asyncio
async def test_two_sessions_create_distinct_native_comment_threads(rig: Rig) -> None:
    first = await rig.coordinator.create_pending(rig.config.allowed_root, "First session")
    second = await rig.coordinator.create_pending(rig.config.allowed_root, "Second session")
    assert first["channel_post_id"] != second["channel_post_id"]
    assert all(item["reply_markup"] is None for item in rig.control_endpoint.sent)

    await rig.coordinator.handle_automatic_forward(
        automatic_forward(int(first["channel_post_id"]), 511)
    )
    await rig.coordinator.handle_automatic_forward(
        automatic_forward(int(second["channel_post_id"]), 512)
    )

    first_current = rig.store.get_space(str(first["space_id"]))
    second_current = rig.store.get_space(str(second["space_id"]))
    assert first_current is not None and second_current is not None
    assert first_current["discussion_root_id"] == 511
    assert second_current["discussion_root_id"] == 512
    assert first_current["status_message_id"] != second_current["status_message_id"]
    assert [item["reply_parameters"].message_id for item in rig.discussion_endpoint.sent] == [
        511,
        512,
    ]


@pytest.mark.asyncio
async def test_space_dashboard_keeps_channel_native_comments_and_status_controls(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-native-comments",
        thread_id="thread-native-comments",
        root_message_id=513,
        channel_post_id=113,
    )
    manager = SpaceDashboardManager(
        rig.config,
        rig.store,
        rig.security,  # type: ignore[arg-type]
        rig.control_endpoint,  # type: ignore[arg-type]
        rig.discussion_endpoint,  # type: ignore[arg-type]
    )

    await manager._flush(str(space["space_id"]))

    [channel_edit] = rig.control_endpoint.edited
    assert "reply_markup" not in channel_edit
    [status_edit] = rig.discussion_endpoint.edited
    labels = [
        button.text
        for row in status_edit["reply_markup"].inline_keyboard
        for button in row
    ]
    assert labels == ["刷新", "取消关注", "返回帖子"]


@pytest.mark.asyncio
async def test_new_session_is_activated_only_after_totp_inside_its_comment_thread(rig: Rig) -> None:
    new_update = update_for_message(
        "/new project | Build the feature",
        update_id=80,
        message_id=20,
        chat_id=OWNER_CHAT_ID,
        chat_type=ChatType.PRIVATE,
    )
    await rig.control.new(new_update, SimpleNamespace())

    [space] = rig.store.list_spaces()
    assert space["space_type"] == "pending_new"
    assert space["thread_id"] is None
    assert rig.bridge.activation_calls == []

    root = automatic_forward(int(space["channel_post_id"]), 503)
    await rig.coordinator.handle_automatic_forward(root)
    command = update_for_message(
        "/totp 123456",
        update_id=81,
        message_id=21,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        reply_to_message=root,
    )
    await rig.discussion._guard(command, SimpleNamespace())
    await rig.discussion.observe_message(command, SimpleNamespace())
    await rig.discussion.totp(command, SimpleNamespace())

    current = rig.store.get_space(str(space["space_id"]))
    assert current is not None
    assert (current["lifecycle"], current["thread_id"]) == ("active", "created-1")
    assert (current["pending_cwd"], current["pending_prompt"]) == ("", "")
    assert rig.bridge.activation_calls == [
        (str(space["space_id"]), f"telegram-new-{space['space_id']}-1")
    ]
    assert rig.security.verifications == [(str(space["space_id"]), "123456")]
    assert rig.discussion_endpoint.deleted == [(DISCUSSION_CHAT_ID, 21)]
    assert rig.discussion_endpoint.sent[-1]["reply_parameters"].message_id == 503


@pytest.mark.asyncio
async def test_perf_schedules_command_and_reply_for_same_60_second_deadline(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("codex_telegram_bridge.control_bot.time.time", lambda: 10_000)
    update = update_for_message(
        "/perf",
        update_id=90,
        message_id=30,
        chat_id=OWNER_CHAT_ID,
        chat_type=ChatType.PRIVATE,
    )

    await rig.control.perf(update, SimpleNamespace())

    due = rig.store.due_message_deletions(now=10_060)
    assert [(item["bot_role"], item["chat_id"], item["message_id"]) for item in due] == [
        (CONTROL_ROLE, OWNER_CHAT_ID, 30),
        (CONTROL_ROLE, OWNER_CHAT_ID, 1000),
    ]
    assert {item["delete_at"] for item in due} == {10_060}
    assert {item["group_key"] for item in due} == {"perf:90"}


@pytest.mark.asyncio
async def test_sessions_uses_one_fixed_deadline_without_extending_on_pagination(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("codex_telegram_bridge.control_bot.time.time", lambda: 10_000)
    for index in range(6):
        rig.store.save_thread(
            ThreadState(
                thread_id=f"thread-{index}",
                title=f"Session {index}",
                cwd=str(rig.config.allowed_root),
                status="idle",
            )
        )
    update = update_for_message(
        "/sessions",
        update_id=91,
        message_id=31,
        chat_id=OWNER_CHAT_ID,
        chat_type=ChatType.PRIVATE,
    )

    async def assert_command_scheduled_before_reply(_payload: dict[str, Any], _reply: Any) -> None:
        due = rig.store.due_message_deletions(now=10_900)
        assert [(item["message_id"], item["delete_at"]) for item in due] == [
            (31, 10_900)
        ]

    rig.control_endpoint.before_send_returns = assert_command_scheduled_before_reply
    await rig.control.sessions(update, SimpleNamespace())

    due_before_page = rig.store.due_message_deletions(now=10_900)
    assert [(item["message_id"], item["delete_at"]) for item in due_before_page] == [
        (31, 10_900),
        (1000, 10_900),
    ]
    assert {item["group_key"] for item in due_before_page} == {"sessions:91"}

    first_markup = rig.control_endpoint.sent[-1]["reply_markup"]
    next_page = first_markup.inline_keyboard[-1][-1]
    assert next_page.text == ">>"
    monkeypatch.setattr("codex_telegram_bridge.store.time.time", lambda: 10_061)
    assert rig.store.peek_callback(
        str(next_page.callback_data)[3:],
        OWNER_ID,
        bot_role=CONTROL_ROLE,
        chat_id=OWNER_CHAT_ID,
    ) is None

    page_update = update_for_message(
        "",
        update_id=92,
        message_id=1000,
        chat_id=OWNER_CHAT_ID,
        chat_type=ChatType.PRIVATE,
    )
    await rig.control._show_sessions(page_update, query="", page=2, edit=True)

    assert rig.control_endpoint.edited[-1]["message_id"] == 1000
    due_after_page = rig.store.due_message_deletions(now=10_900)
    assert [
        (item["message_id"], item["delete_at"], item["group_key"])
        for item in due_after_page
    ] == [
        (31, 10_900, "sessions:91"),
        (1000, 10_900, "sessions:91"),
    ]


@pytest.mark.asyncio
async def test_unwatch_freezes_space_cancels_work_and_deletes_future_commands(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-freeze",
        thread_id="thread-freeze",
        root_message_id=504,
        channel_post_id=104,
    )
    queue_id = rig.store.enqueue_prompt(
        "thread-freeze",
        "later",
        [],
        "queued-message",
        space_id="space-freeze",
        generation=1,
    )
    button = rig.discussion._button("Refresh", "space_refresh", {}, space)
    nonce = str(button.callback_data)[3:]

    await rig.discussion._dispatch_callback("unwatch_execute", {}, space)

    closed = rig.store.get_space("space-freeze")
    assert closed is not None
    assert (closed["lifecycle"], closed["generation"]) == ("closed", 2)
    assert rig.store.space_queue_entries("space-freeze", 1) == []
    assert not rig.store.cancel_space_prompt("space-freeze", queue_id, 1)
    assert rig.store.consume_callback(
        nonce,
        OWNER_ID,
        bot_role=DISCUSSION_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
    ) is None
    assert rig.bridge.close_calls == [("space-freeze", 1)]

    command = update_for_message(
        "/prompt must not run",
        update_id=91,
        message_id=31,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=504,
    )
    with pytest.raises(ApplicationHandlerStop):
        await rig.discussion._guard(command, SimpleNamespace())
    assert rig.discussion_endpoint.deleted == [(DISCUSSION_CHAT_ID, 31)]
    assert rig.bridge.prompt_calls == []


@pytest.mark.asyncio
async def test_message_routing_stays_bound_to_the_comment_root(rig: Rig) -> None:
    first = add_active_space(
        rig,
        space_id="space-one",
        thread_id="thread-one",
        root_message_id=505,
        channel_post_id=105,
    )
    second = add_active_space(
        rig,
        space_id="space-two",
        thread_id="thread-two",
        root_message_id=506,
        channel_post_id=106,
    )
    rig.store.record_discussion_message(DISCUSSION_CHAT_ID, 2505, 505, "space-one")
    rig.store.record_discussion_message(DISCUSSION_CHAT_ID, 2506, 506, "space-two")

    direct = update_for_message(
        "/status",
        update_id=92,
        message_id=32,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=505,
    ).effective_message
    nested_reply = update_for_message(
        "/status",
        update_id=93,
        message_id=33,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        reply_to_message=SimpleNamespace(
            message_id=2506,
            message_thread_id=None,
            is_automatic_forward=False,
        ),
    ).effective_message
    mapped_current = update_for_message(
        "/status",
        update_id=94,
        message_id=2505,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
    ).effective_message

    assert rig.discussion._space_for_message(direct)["space_id"] == first["space_id"]
    assert rig.discussion._space_for_message(nested_reply)["space_id"] == second["space_id"]
    assert rig.discussion._space_for_message(mapped_current)["space_id"] == first["space_id"]

    rig.security.unlocked.add("space-two")
    prompt_update = update_for_message(
        "/prompt run only here",
        update_id=95,
        message_id=34,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        reply_to_message=nested_reply.reply_to_message,
    )
    await rig.discussion.prompt(prompt_update, SimpleNamespace())
    assert rig.bridge.prompt_calls[0]["space_id"] == "space-two"
    assert rig.bridge.prompt_calls[0]["prompt"] == "run only here"
    assert rig.discussion_endpoint.sent[-1]["reply_parameters"].message_id == 506


@pytest.mark.asyncio
async def test_tmux_question_resolution_deletes_all_forwarded_question_messages(rig: Rig) -> None:
    add_active_space(
        rig,
        space_id="space-question",
        thread_id="thread-question",
        root_message_id=507,
        channel_post_id=107,
    )
    questions = [
        {
            "id": "choice",
            "header": "Choose",
            "question": "Which mode?",
            "options": [{"label": "Queue", "description": "Later"}],
        }
    ]
    rig.store.put_pending_input(
        "request-1",
        "1",
        1,
        "thread-question",
        "turn-1",
        "item-1",
        questions,
        None,
    )

    await rig.discussion.forward_question(
        "request-1",
        {"threadId": "thread-question", "questions": questions},
    )
    assert [item["message_id"] for item in rig.store.question_messages("request-1")] == [
        2000,
        2001,
    ]

    await rig.discussion.question_resolved("request-1")

    assert rig.discussion_endpoint.deleted == [
        (DISCUSSION_CHAT_ID, 2000),
        (DISCUSSION_CHAT_ID, 2001),
    ]
    assert rig.store.question_messages("request-1") == []
    assert rig.store.due_message_deletions() == []
    assert "request-1" not in rig.discussion._question_answers


@pytest.mark.asyncio
async def test_question_offers_force_reply_custom_answer_scoped_to_exact_root(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-custom",
        thread_id="thread-custom",
        root_message_id=508,
        channel_post_id=108,
    )
    add_active_space(
        rig,
        space_id="space-other",
        thread_id="thread-other",
        root_message_id=509,
        channel_post_id=109,
    )
    questions = [
        {
            "id": "delivery",
            "header": "Delivery",
            "question": "How should this be delivered?",
            "options": [{"label": "Queue"}],
        }
    ]
    rig.store.put_pending_input(
        "request-custom",
        "2",
        1,
        "thread-custom",
        "turn-1",
        "item-1",
        questions,
        None,
    )
    rig.security.unlocked.add("space-custom")

    await rig.discussion.forward_question(
        "request-custom",
        {"threadId": "thread-custom", "questions": questions},
    )

    keyboard = rig.discussion_endpoint.sent[1]["reply_markup"].inline_keyboard
    assert [button.text for row in keyboard for button in row] == [
        "Queue",
        "✍️ 自定义回答",
        "❓ 反问 Codex",
    ]
    await rig.discussion._dispatch_callback(
        "question_custom",
        {"request_key": "request-custom", "question_id": "delivery"},
        space,
    )
    force_prompt = rig.discussion_endpoint.sent[-1]
    assert isinstance(force_prompt["reply_markup"], ForceReply)
    assert force_prompt["reply_markup"].selective is True
    assert force_prompt["reply_markup"].input_field_placeholder == "输入自定义回答"
    prompt_message_id = rig.discussion_endpoint.next_message_id - 1
    reply_to = SimpleNamespace(
        message_id=prompt_message_id,
        message_thread_id=508,
        is_automatic_forward=False,
    )

    wrong_root = update_for_message(
        "/not-an-option | exact answer",
        update_id=110,
        message_id=40,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=509,
        reply_to_message=reply_to,
    )
    await rig.discussion.reply_to_intent(wrong_root, SimpleNamespace())
    assert rig.bridge.answers == []

    correct_root = update_for_message(
        "/not-an-option | exact answer",
        update_id=111,
        message_id=41,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=508,
        reply_to_message=reply_to,
    )
    with pytest.raises(ApplicationHandlerStop):
        await rig.discussion.reply_to_intent(correct_root, SimpleNamespace())
    assert rig.bridge.answers == [
        ("request-custom", {"delivery": ["/not-an-option | exact answer"]})
    ]


@pytest.mark.asyncio
async def test_question_clarification_is_isolated_and_keeps_original_question(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-clarify",
        thread_id="thread-clarify",
        root_message_id=510,
        channel_post_id=110,
    )
    questions = [
        {
            "id": "mode",
            "question": "Which execution mode?",
            "options": [{"label": "Queue"}],
        }
    ]
    rig.store.put_pending_input(
        "request-clarify",
        "3",
        1,
        "thread-clarify",
        "turn-1",
        "item-1",
        questions,
        None,
    )
    rig.security.unlocked.add("space-clarify")
    await rig.discussion.forward_question(
        "request-clarify",
        {"threadId": "thread-clarify", "questions": questions},
    )
    await rig.discussion._dispatch_callback(
        "question_clarify",
        {"request_key": "request-clarify", "question_id": "mode"},
        space,
    )
    prompt_message_id = rig.discussion_endpoint.next_message_id - 1
    force_prompt = rig.discussion_endpoint.sent[-1]
    assert isinstance(force_prompt["reply_markup"], ForceReply)
    assert force_prompt["reply_markup"].input_field_placeholder == "输入要向 Codex 反问的问题"
    clarification = update_for_message(
        "/why is Queue safer?",
        update_id=112,
        message_id=42,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=510,
        reply_to_message=SimpleNamespace(
            message_id=prompt_message_id,
            message_thread_id=510,
            is_automatic_forward=False,
        ),
    )

    with pytest.raises(ApplicationHandlerStop):
        await rig.discussion.reply_to_intent(clarification, SimpleNamespace())
    await asyncio.sleep(0)

    assert len(rig.bridge.ask_calls) == 1
    call = rig.bridge.ask_calls[0]
    assert call["space_id"] == "space-clarify"
    assert call["question"] == "/why is Queue safer?"
    assert rig.bridge.prompt_calls == []
    assert [item["message_id"] for item in rig.store.question_messages("request-clarify")] == [
        2000,
        2001,
        2002,
    ]
    waiting = rig.discussion_endpoint.sent[-1]
    ask_id = waiting["markdown"].split("`")[1]
    tasks = list(rig.discussion._ask_tasks)
    rig.bridge.ask_waiters[call["client_message_id"]].set_result("Use *Queue* for isolation.")
    await asyncio.gather(*tasks)

    [edited] = rig.discussion_endpoint.edited
    assert edited["message_id"] == 2003
    assert f"`{ask_id}`" in edited["markdown"]
    assert r"Use \*Queue\* for isolation\." in edited["markdown"]
    assert rig.store.get_pending_input("request-clarify") is not None


@pytest.mark.asyncio
async def test_two_ask_commands_run_concurrently_and_correlate_out_of_order(rig: Rig) -> None:
    add_active_space(
        rig,
        space_id="space-ask",
        thread_id="thread-ask",
        root_message_id=511,
        channel_post_id=111,
    )
    rig.security.unlocked.add("space-ask")
    first = update_for_message(
        "/ask first question",
        update_id=113,
        message_id=43,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=511,
    )
    second = update_for_message(
        "/ask second question",
        update_id=114,
        message_id=44,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=511,
    )

    await rig.discussion.ask(first, SimpleNamespace())
    await rig.discussion.ask(second, SimpleNamespace())
    await asyncio.sleep(0)

    assert [call["question"] for call in rig.bridge.ask_calls] == [
        "first question",
        "second question",
    ]
    assert rig.discussion_endpoint.edited == []
    waiting_ids = [payload["markdown"].split("`")[1] for payload in rig.discussion_endpoint.sent]
    tasks = list(rig.discussion._ask_tasks)
    second_call = rig.bridge.ask_calls[1]
    rig.bridge.ask_waiters[second_call["client_message_id"]].set_result("second answer")
    await asyncio.sleep(0)
    assert rig.discussion_endpoint.edited[0]["message_id"] == 2001
    assert f"`{waiting_ids[1]}`" in rig.discussion_endpoint.edited[0]["markdown"]
    first_call = rig.bridge.ask_calls[0]
    rig.bridge.ask_waiters[first_call["client_message_id"]].set_result("first answer")
    await asyncio.gather(*tasks)

    by_message = {item["message_id"]: item["markdown"] for item in rig.discussion_endpoint.edited}
    assert f"`{waiting_ids[0]}`" in by_message[2000]
    assert "first answer" in by_message[2000]
    assert "second answer" in by_message[2001]
    assert rig.bridge.prompt_calls == []


@pytest.mark.asyncio
async def test_running_ask_does_not_publish_after_space_is_closed(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-closing-ask",
        thread_id="thread-closing-ask",
        root_message_id=512,
        channel_post_id=112,
    )
    rig.security.unlocked.add("space-closing-ask")
    update = update_for_message(
        "/ask should this still appear?",
        update_id=115,
        message_id=45,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=512,
    )
    await rig.discussion.ask(update, SimpleNamespace())
    await asyncio.sleep(0)
    call = rig.bridge.ask_calls[0]
    tasks = list(rig.discussion._ask_tasks)
    assert rig.store.close_space("space-closing-ask", expected_generation=space["generation"])
    rig.bridge.ask_waiters[call["client_message_id"]].set_result("must not be posted")
    await asyncio.gather(*tasks)

    assert rig.discussion_endpoint.edited == []
    assert rig.discussion_endpoint.deleted == [(DISCUSSION_CHAT_ID, 2000)]
