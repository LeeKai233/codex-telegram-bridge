from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from telegram import Chat, ForceReply, MessageOriginChannel
from telegram.constants import ChatType, ParseMode
from telegram.error import TelegramError
from telegram.ext import ApplicationHandlerStop

from codex_telegram_bridge.approval import (
    approval_response_payload,
    interactive_approval_decisions,
)
from codex_telegram_bridge.config import Config
from codex_telegram_bridge.control_bot import ControlBotController
from codex_telegram_bridge.deletions import MessageDeletionManager
from codex_telegram_bridge.delivery import TelegramDeliveryEngine
from codex_telegram_bridge.discussion_bot import (
    DiscussionBotController,
    _callback_workload_space,
)
from codex_telegram_bridge.files import FileCandidate
from codex_telegram_bridge.metrics import MetricsSnapshot
from codex_telegram_bridge.models import (
    ModelOption,
    ModelProfile,
    Owner,
    SessionSpace,
    ThreadState,
)
from codex_telegram_bridge.space_coordinator import SessionSpaceCoordinator
from codex_telegram_bridge.space_dashboard import SpaceDashboardManager
from codex_telegram_bridge.store import Store
from codex_telegram_bridge.telegram_common import CONTROL_ROLE, DISCUSSION_ROLE, STATUS_ROLE
from codex_telegram_bridge.workloads import (
    FILE_IO_SPACE,
    MAINTENANCE_SPACE,
    PROMPT_ACTION_SPACE,
)

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
        parse_mode: str | None = ParseMode.MARKDOWN_V2,
        reply_markup: Any = None,
        reply_parameters: Any = None,
        priority: int = 10,
    ) -> Any:
        payload = {
            "chat_id": chat_id,
            "markdown": markdown,
            "plain": plain,
            "parse_mode": parse_mode,
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

    async def edit_reply_markup(self, chat_id: int, message_id: int, **kwargs: Any) -> Any:
        self.edited.append(
            {"chat_id": chat_id, "message_id": message_id, "reply_markup": kwargs.get("reply_markup")}
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
        self.collaboration_calls: list[dict[str, Any]] = []
        self.collaboration_error: RuntimeError | None = None
        self.reconcile_status = "unknown"
        self.plan_gate_result: dict[str, str] = {"status": "safe_to_submit"}
        self.plan_gate_calls: list[dict[str, Any]] = []
        self.on_question: Any = None
        self.on_notice: Any = None
        self.on_question_resolved: Any = None
        self.on_plan_completed: Any = None
        self.on_prompt_completed: Any = None

    async def resolve_directory(self, description: str) -> list[Path]:
        assert description
        return self.directory_candidates

    async def list_model_options(self) -> list[ModelOption]:
        return [
            ModelOption(
                model="gpt-5.6-luna",
                display_name="GPT-5.6 Luna",
                supported_efforts=("high", "max"),
                default_effort="high",
                is_default=True,
            )
        ]

    async def resolve_model_profile(self, model: str, effort: str) -> ModelProfile:
        if model not in {"gpt-5.6-luna", "luna"} or effort not in {"high", "max"}:
            raise ValueError("invalid model profile")
        return ModelProfile("gpt-5.6-luna", effort)

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

    async def start_space_collaboration_turn(
        self,
        space_id: str,
        prompt: str,
        *,
        mode: str,
        client_message_id: str,
    ) -> dict[str, str]:
        call = {
            "space_id": space_id,
            "prompt": prompt,
            "mode": mode,
            "client_message_id": client_message_id,
        }
        self.collaboration_calls.append(call)
        if self.collaboration_error is not None:
            raise self.collaboration_error
        return {"id": f"turn-{len(self.collaboration_calls)}"}

    async def reconcile_plan_execution(
        self,
        _space_id: str,
        _generation: int,
        _item_id: str,
        _revision_key: str,
        _client_message_id: str,
    ) -> str:
        return self.reconcile_status

    async def wait_for_plan_decision_gate(
        self,
        space_id: str,
        generation: int,
        item_id: str,
        revision_key: str,
        client_message_id: str,
        *,
        timeout: float,
    ) -> dict[str, str]:
        self.plan_gate_calls.append(
            {
                "space_id": space_id,
                "generation": generation,
                "item_id": item_id,
                "revision_key": revision_key,
                "client_message_id": client_message_id,
                "timeout": timeout,
            }
        )
        return dict(self.plan_gate_result)


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
    telegram_message_states: dict[str, dict[str, Any]] = {}
    prompt_receipt_links: list[tuple[str, str]] = []

    def put_telegram_message_state(
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
            "updated_at": int(time.time()),
        }
        telegram_message_states[message_key] = row
        return dict(row)

    def get_telegram_message_state(message_key: str) -> dict[str, Any] | None:
        row = telegram_message_states.get(message_key)
        return dict(row) if row is not None else None

    def link_prompt_intent_receipt(client_message_id: str, receipt_key: str) -> None:
        prompt_receipt_links.append((client_message_id, receipt_key))

    store.put_telegram_message_state = put_telegram_message_state  # type: ignore[attr-defined]
    store.get_telegram_message_state = get_telegram_message_state  # type: ignore[attr-defined]
    store.link_prompt_intent_receipt = link_prompt_intent_receipt  # type: ignore[attr-defined]
    store._test_prompt_receipt_links = prompt_receipt_links  # type: ignore[attr-defined]
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


def record_tui_plan_approval(rig: Rig, thread_id: str, turn_id: str) -> None:
    assert rig.store.record_event(
        f"tui-plan-approval:{turn_id}",
        thread_id,
        "item/started",
        {
            "threadId": thread_id,
            "turnId": turn_id,
            "item": {
                "id": f"item-{turn_id}",
                "type": "userMessage",
                "clientId": None,
                "content": [{"type": "text", "text": "Implement the plan."}],
            },
        },
    )


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
    rig.security.unlocked.add("space-a")
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
async def test_locked_space_allows_status_and_auth_commands_but_blocks_mutation(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-locked",
        thread_id="thread-locked",
        root_message_id=525,
        channel_post_id=125,
    )
    blocked = update_for_message(
        "/planmode",
        update_id=140,
        message_id=60,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=int(space["discussion_root_id"]),
    )
    with pytest.raises(ApplicationHandlerStop):
        await rig.discussion._guard(blocked, SimpleNamespace())
    assert "写操作已锁定" in rig.discussion_endpoint.sent[-1]["markdown"]

    status = update_for_message(
        "/status",
        update_id=141,
        message_id=141,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=int(space["discussion_root_id"]),
    )
    await rig.discussion._guard(status, SimpleNamespace())
    await rig.discussion.status(status, SimpleNamespace())
    await asyncio.sleep(0)
    assert ("space-locked", True) in rig.dashboards.calls
    assert "状态快照已显示" in rig.discussion_endpoint.sent[-1]["markdown"]

    for update_id, command in enumerate(("/totp 123456", "/help", "/lock"), 142):
        allowed = update_for_message(
            command,
            update_id=update_id,
            message_id=update_id,
            chat_id=DISCUSSION_CHAT_ID,
            chat_type=ChatType.SUPERGROUP,
            message_thread_id=int(space["discussion_root_id"]),
        )
        await rig.discussion._guard(allowed, SimpleNamespace())


@pytest.mark.asyncio
async def test_locked_status_bot_refresh_callback_is_read_only(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-locked-refresh",
        thread_id="thread-locked-refresh",
        root_message_id=526,
        channel_post_id=126,
    )
    nonce = rig.store.ensure_callback(
        "status-refresh-nonce",
        "space_refresh",
        {"space_id": space["space_id"], "generation": space["generation"]},
        OWNER_ID,
        int(time.time()) + 60,
        bot_role=STATUS_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
        space_id=space["space_id"],
        generation=int(space["generation"]),
    )
    query = SimpleNamespace(
        data=f"cb:{nonce}",
        message=SimpleNamespace(
            chat=SimpleNamespace(id=DISCUSSION_CHAT_ID, type=ChatType.SUPERGROUP),
            message_id=space["status_message_id"],
        ),
    )
    update = SimpleNamespace(
        callback_query=query,
        effective_user=SimpleNamespace(id=OWNER_ID),
        effective_chat=SimpleNamespace(id=DISCUSSION_CHAT_ID, type=ChatType.SUPERGROUP),
    )

    await rig.discussion.callback_for_role(
        update,
        SimpleNamespace(),
        bot_role=STATUS_ROLE,
        endpoint=rig.discussion_endpoint,  # type: ignore[arg-type]
        allowed_actions=frozenset({"space_refresh"}),
    )

    assert (space["space_id"], True) in rig.dashboards.calls
    assert rig.store.peek_callback(
        nonce,
        OWNER_ID,
        bot_role=STATUS_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
        space_id=space["space_id"],
        generation=int(space["generation"]),
    ) is None


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
    delivery = TelegramDeliveryEngine(
        {
            CONTROL_ROLE: rig.control_endpoint,
            DISCUSSION_ROLE: rig.discussion_endpoint,
        }  # type: ignore[dict-item]
    )
    delivery.start()
    manager = SpaceDashboardManager(
        rig.config,
        rig.store,
        rig.security,  # type: ignore[arg-type]
        rig.control_endpoint,  # type: ignore[arg-type]
        rig.discussion_endpoint,  # type: ignore[arg-type]
        delivery,
    )

    await manager._flush(str(space["space_id"]))
    tickets = list(manager._delivery_tickets.values())
    await asyncio.gather(*tickets)

    [channel_edit] = rig.control_endpoint.edited
    assert "reply_markup" not in channel_edit
    [status_edit] = rig.discussion_endpoint.edited
    labels = [
        button.text
        for row in status_edit["reply_markup"].inline_keyboard
        for button in row
    ]
    assert labels == ["刷新", "取消关注", "返回帖子"]
    await delivery.stop(drain_timeout=0)


@pytest.mark.asyncio
async def test_new_session_is_activated_only_after_totp_inside_its_comment_thread(rig: Rig) -> None:
    new_update = update_for_message(
        "/new gpt-5.6-luna | max | noplan | project | Build the feature",
        update_id=80,
        message_id=20,
        chat_id=OWNER_CHAT_ID,
        chat_type=ChatType.PRIVATE,
    )
    await rig.control.new(new_update, SimpleNamespace())

    [space] = rig.store.list_spaces()
    assert space["space_type"] == "pending_new"
    assert space["thread_id"] is None
    assert (space["normal_model"], space["normal_effort"], space["current_mode"]) == (
        "gpt-5.6-luna",
        "max",
        "default",
    )
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
async def test_perf_updates_then_deletes_command_and_reply_after_30_seconds(
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

    due = rig.store.due_message_deletions(now=10_030)
    assert [(item["bot_role"], item["chat_id"], item["message_id"]) for item in due] == [
        (CONTROL_ROLE, OWNER_CHAT_ID, 30),
        (CONTROL_ROLE, OWNER_CHAT_ID, 1000),
    ]
    assert {item["delete_at"] for item in due} == {10_030}
    assert {item["group_key"] for item in due} == {"perf:90"}
    assert "动态性能" in rig.control_endpoint.sent[-1]["markdown"]
    await rig.control.stop()


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
    await asyncio.gather(*list(rig.discussion._background_tasks))

    assert rig.discussion_endpoint.deleted == [(DISCUSSION_CHAT_ID, 2001)]
    [summary] = rig.discussion_endpoint.edited
    assert summary["message_id"] == 2000
    assert summary["parse_mode"] == ParseMode.HTML
    assert "已在终端处理；具体答案不可用" in summary["markdown"]
    assert "Later" in summary["markdown"]
    assert rig.store.question_messages("request-1") == []
    assert rig.store.due_message_deletions() == []
    assert "request-1" not in rig.discussion._question_answers


@pytest.mark.asyncio
async def test_telegram_question_answer_is_persisted_before_rpc_and_archived(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-answer",
        thread_id="thread-answer",
        root_message_id=520,
        channel_post_id=120,
    )
    questions = [
        {
            "id": "delivery",
            "header": "Delivery",
            "question": "How should this run?",
            "options": [
                {"label": "Queue", "description": "Run after the active turn"},
                {"label": "Steer", "description": "Inject into the active turn"},
            ],
        }
    ]
    rig.store.put_pending_input(
        "request-answer",
        "4",
        1,
        "thread-answer",
        "turn-1",
        "item-1",
        questions,
        None,
    )
    observed: list[dict[str, Any]] = []

    async def answer_question(
        request_key: str, answers: dict[str, list[str]]
    ) -> None:
        persisted = rig.store.pop_question_resolution(request_key)
        assert persisted is not None
        observed.append(persisted)
        rig.store.save_question_resolution(
            request_key,
            answers,
            source=str(persisted["source"]),
        )
        await rig.discussion.question_resolved(request_key)

    rig.bridge.answer_question = answer_question  # type: ignore[method-assign]
    await rig.discussion.forward_question(
        "request-answer",
        {"threadId": "thread-answer", "questions": questions},
    )
    assert "Run after the active turn" in rig.discussion_endpoint.sent[1]["markdown"]

    await rig.discussion._record_question_answer(
        space,
        {
            "request_key": "request-answer",
            "question_id": "delivery",
            "answer": "Queue",
        },
    )
    await asyncio.gather(*list(rig.discussion._background_tasks))

    assert observed == [
        {
            "answers": {"delivery": ["Queue"]},
            "source": "telegram",
            "resolved_at": observed[0]["resolved_at"],
        }
    ]
    [summary] = rig.discussion_endpoint.edited
    assert summary["message_id"] == 2000
    assert "<b>选择：</b>Queue" in summary["markdown"]
    assert "来源：Telegram" in summary["markdown"]
    assert rig.discussion_endpoint.deleted == [(DISCUSSION_CHAT_ID, 2001)]


@pytest.mark.asyncio
async def test_locked_question_button_survives_totp_authentication_window(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    add_active_space(
        rig,
        space_id="space-locked-question",
        thread_id="thread-locked-question",
        root_message_id=525,
        channel_post_id=125,
    )
    questions = [
        {
            "id": "delivery",
            "question": "How should this run?",
            "options": [{"label": "Queue", "description": "Run later"}],
        }
    ]
    rig.store.put_pending_input(
        "request-locked",
        "5",
        1,
        "thread-locked-question",
        "turn-1",
        "item-1",
        questions,
        None,
    )
    clock = [10_000]
    monkeypatch.setattr("codex_telegram_bridge.discussion_bot.time.time", lambda: clock[0])
    monkeypatch.setattr("codex_telegram_bridge.store.time.time", lambda: clock[0])

    await rig.discussion.forward_question(
        "request-locked",
        {"threadId": "thread-locked-question", "questions": questions},
    )
    button = rig.discussion_endpoint.sent[1]["reply_markup"].inline_keyboard[0][0]
    nonce = str(button.callback_data)[3:]
    expiry = rig.store._connection.execute(  # noqa: SLF001
        "SELECT expires_at FROM callbacks WHERE nonce=?", (nonce,)
    ).fetchone()
    assert expiry is not None and int(expiry[0]) == 10_000 + rig.config.totp_unlock_seconds

    clock[0] += 10 * 60
    update = SimpleNamespace(
        callback_query=SimpleNamespace(data=button.callback_data),
        effective_user=SimpleNamespace(id=OWNER_ID),
        effective_chat=SimpleNamespace(id=DISCUSSION_CHAT_ID),
    )
    await rig.discussion.callback(update, SimpleNamespace())
    assert rig.store.peek_callback(
        nonce,
        OWNER_ID,
        bot_role=DISCUSSION_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
    ) is not None

    rig.security.unlocked.add("space-locked-question")
    await rig.discussion.callback(update, SimpleNamespace())
    assert rig.bridge.answers == [
        ("request-locked", {"delivery": ["Queue"]})
    ]
    assert rig.store.peek_callback(
        nonce,
        OWNER_ID,
        bot_role=DISCUSSION_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
    ) is None


@pytest.mark.asyncio
async def test_question_rejects_second_answer_after_first_rpc_failure(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-answer-race",
        thread_id="thread-answer-race",
        root_message_id=526,
        channel_post_id=126,
    )
    questions = [
        {
            "id": "delivery",
            "question": "How should this run?",
            "options": [{"label": "Queue"}, {"label": "Steer"}],
        }
    ]
    rig.store.put_pending_input(
        "request-answer-race",
        "6",
        1,
        "thread-answer-race",
        "turn-1",
        "item-1",
        questions,
        None,
    )
    calls: list[dict[str, list[str]]] = []

    async def fail_answer(_request_key: str, answers: dict[str, list[str]]) -> None:
        calls.append(answers)
        raise RuntimeError("transport failed")

    rig.bridge.answer_question = fail_answer  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="transport failed"):
        await rig.discussion._record_question_answer(
            space,
            {
                "request_key": "request-answer-race",
                "question_id": "delivery",
                "answer": "Queue",
            },
        )
    with pytest.raises(RuntimeError, match="不能更改答案"):
        await rig.discussion._record_question_answer(
            space,
            {
                "request_key": "request-answer-race",
                "question_id": "delivery",
                "answer": "Steer",
            },
        )

    assert calls == [{"delivery": ["Queue"]}]
    assert rig.discussion._question_answers["request-answer-race"] == {
        "delivery": ["Queue"]
    }
    resolution = rig.store.pop_question_resolution("request-answer-race")
    assert resolution is not None
    assert resolution["answers"] == {"delivery": ["Queue"]}


@pytest.mark.asyncio
async def test_plan_article_buttons_preserve_nonce_while_locked_then_execute_once(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    add_active_space(
        rig,
        space_id="space-plan",
        thread_id="thread-plan",
        root_message_id=521,
        channel_post_id=121,
    )
    monkeypatch.setattr("codex_telegram_bridge.discussion_bot.time.time", lambda: 10_000)
    await rig.discussion.plan_completed(
        "thread-plan",
        "turn-plan",
        "item-plan",
        "# Build plan\n\n- Keep `<unsafe>` escaped\n- Run tests",
    )

    article = rig.discussion_endpoint.sent[-1]
    article_message_id = rig.discussion_endpoint.next_message_id - 1
    assert article["parse_mode"] == ParseMode.HTML
    assert "<b>Build plan</b>" in article["markdown"]
    assert "<unsafe>" not in article["markdown"]
    execute, revise = (
        article["reply_markup"].inline_keyboard[0][0],
        article["reply_markup"].inline_keyboard[1][0],
    )
    assert [execute.text, revise.text] == ["批准并执行", "继续完善计划"]
    execute_nonce = str(execute.callback_data)[3:]
    expiry = rig.store._connection.execute(  # noqa: SLF001
        "SELECT expires_at FROM callbacks WHERE nonce=?", (execute_nonce,)
    ).fetchone()
    assert expiry is not None and int(expiry[0]) == 10_000 + 24 * 60 * 60

    query = SimpleNamespace(data=execute.callback_data)
    update = SimpleNamespace(
        callback_query=query,
        effective_user=SimpleNamespace(id=OWNER_ID),
        effective_chat=SimpleNamespace(id=DISCUSSION_CHAT_ID),
    )
    await rig.discussion.callback(update, SimpleNamespace())
    assert rig.bridge.collaboration_calls == []
    assert rig.store.peek_callback(
        execute_nonce,
        OWNER_ID,
        bot_role=DISCUSSION_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
    ) is not None
    assert "认证后可再次点击原按钮" in rig.discussion_endpoint.callback_answers[-1]["text"]

    rig.security.unlocked.add("space-plan")
    await rig.discussion.callback(update, SimpleNamespace())
    assert len(rig.bridge.collaboration_calls) == 1
    assert rig.bridge.collaboration_calls[0]["mode"] == "default"
    assert "Use goal" in rig.bridge.collaboration_calls[0]["prompt"]
    assert rig.store.peek_callback(
        execute_nonce,
        OWNER_ID,
        bot_role=DISCUSSION_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
    ) is None
    assert rig.store.latest_plan_publication("space-plan", 1)["status"] == "executed"
    assert any(
        edit["reply_markup"] is None
        and edit["message_id"] == article_message_id
        for edit in rig.discussion_endpoint.edited
        if "reply_markup" in edit
    )

    await rig.discussion.callback(update, SimpleNamespace())
    assert len(rig.bridge.collaboration_calls) == 1
    assert "已使用或过期" in rig.discussion_endpoint.callback_answers[-1]["text"]


@pytest.mark.asyncio
async def test_plan_callback_uses_callback_message_chat_for_real_telegram_updates(rig: Rig) -> None:
    add_active_space(
        rig,
        space_id="space-plan-callback-chat",
        thread_id="thread-plan-callback-chat",
        root_message_id=528,
        channel_post_id=128,
    )
    rig.security.unlocked.add("space-plan-callback-chat")
    await rig.discussion.plan_completed(
        "thread-plan-callback-chat",
        "turn-plan",
        "item-plan",
        "Execute from the callback message chat.",
    )
    publication = rig.store.latest_plan_publication("space-plan-callback-chat", 1)
    assert publication is not None
    markup = rig.discussion_endpoint.sent[-1]["reply_markup"]
    execute = markup.inline_keyboard[0][0]
    callback_message = SimpleNamespace(
        chat=SimpleNamespace(id=DISCUSSION_CHAT_ID, type=ChatType.SUPERGROUP),
        chat_id=DISCUSSION_CHAT_ID,
        message_id=2028,
        message_thread_id=528,
        reply_to_message=None,
        text=None,
        caption=None,
        is_automatic_forward=False,
        sender_chat=None,
    )
    query = SimpleNamespace(data=execute.callback_data, message=callback_message)
    update = SimpleNamespace(
        update_id=8128,
        callback_query=query,
        effective_user=SimpleNamespace(id=OWNER_ID),
        effective_chat=SimpleNamespace(id=OWNER_CHAT_ID, type=ChatType.PRIVATE),
        effective_message=callback_message,
    )

    await rig.discussion._guard(update, SimpleNamespace())
    await rig.discussion.callback(update, SimpleNamespace())

    assert len(rig.bridge.collaboration_calls) == 1
    assert rig.store.latest_plan_publication("space-plan-callback-chat", 1)["status"] == "executed"


@pytest.mark.asyncio
async def test_tui_plan_selection_retires_telegram_actions(rig: Rig) -> None:
    add_active_space(
        rig,
        space_id="space-plan-tui",
        thread_id="thread-plan-tui",
        root_message_id=527,
        channel_post_id=127,
    )
    await rig.discussion.plan_completed(
        "thread-plan-tui",
        "turn-plan",
        "item-plan",
        "Execute this from the TUI.",
    )
    article = rig.discussion_endpoint.sent[-1]
    article_message_id = rig.discussion_endpoint.next_message_id - 1
    execute = article["reply_markup"].inline_keyboard[0][0]

    await rig.discussion.plan_turn_started("thread-plan-tui", "turn-tui")

    publication = rig.store.latest_plan_publication("space-plan-tui", 1)
    assert publication is not None and publication["status"] == "executed"
    assert rig.store.peek_callback(
        str(execute.callback_data)[3:],
        OWNER_ID,
        bot_role=DISCUSSION_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
    ) is None
    assert any(
        edit.get("message_id") == article_message_id
        and edit.get("reply_markup") is None
        and "已批准并开始执行" in edit.get("markdown", "")
        for edit in rig.discussion_endpoint.edited
    )


@pytest.mark.asyncio
async def test_tui_plan_prompt_disappearance_deletes_every_plan_chunk(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    class PromptSequence:
        def __init__(self) -> None:
            self.states = iter((True, False))

        async def plan_prompt_visible(self, _thread_id: str) -> bool:
            return next(self.states, False)

    rig.bridge.tmux = PromptSequence()
    monkeypatch.setattr(
        "codex_telegram_bridge.discussion_bot._PLAN_PROMPT_POLL_SECONDS", 0
    )
    add_active_space(
        rig,
        space_id="space-plan-dismissed",
        thread_id="thread-plan-dismissed",
        root_message_id=529,
        channel_post_id=129,
    )
    text = "\n\n".join(
        f"{index}. Verify deletion for chunk {index}: " + "detail " * 80
        for index in range(1, 16)
    )

    await rig.discussion.plan_completed(
        "thread-plan-dismissed",
        "turn-plan",
        "item-plan",
        text,
    )
    tasks = list(rig.discussion._plan_prompt_tasks.values())  # noqa: SLF001
    assert tasks
    await asyncio.gather(*tasks)

    publication = rig.store.latest_plan_publication("space-plan-dismissed", 1)
    assert publication is not None and publication["status"] == "dismissed"
    assert len(publication["message_ids"]) > 1
    assert set(publication["message_ids"]) == {
        message_id
        for chat_id, message_id in rig.discussion_endpoint.deleted
        if chat_id == DISCUSSION_CHAT_ID
    }


@pytest.mark.asyncio
async def test_unobserved_absent_tui_prompt_keeps_plan_actions(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    checked = asyncio.Event()

    class MissingPrompt:
        async def plan_prompt_visible(self, _thread_id: str) -> bool:
            checked.set()
            return False

    rig.bridge.tmux = MissingPrompt()
    monkeypatch.setattr(
        "codex_telegram_bridge.discussion_bot._PLAN_PROMPT_POLL_SECONDS", 0.01
    )
    add_active_space(
        rig,
        space_id="space-plan-unobserved",
        thread_id="thread-plan-unobserved",
        root_message_id=530,
        channel_post_id=130,
    )
    await rig.discussion.plan_completed(
        "thread-plan-unobserved",
        "turn-plan",
        "item-plan",
        "Keep this actionable.",
    )
    await asyncio.wait_for(checked.wait(), timeout=1)
    await asyncio.sleep(0)

    publication = rig.store.latest_plan_publication("space-plan-unobserved", 1)
    assert publication is not None and publication["status"] == "published"
    assert publication["tui_prompt_seen_at"] is None
    assert rig.discussion_endpoint.deleted == []
    await rig.discussion.stop()


@pytest.mark.asyncio
async def test_plan_prompt_monitor_backs_off_and_stops_after_ten_minutes(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    add_active_space(
        rig,
        space_id="space-plan-monitor-deadline",
        thread_id="thread-plan-monitor-deadline",
        root_message_id=531,
        channel_post_id=131,
    )
    await rig.discussion.plan_completed(
        "thread-plan-monitor-deadline",
        "turn-plan",
        "item-plan",
        "Keep this plan actionable after the monitor expires.",
    )
    publication = rig.store.latest_plan_publication("space-plan-monitor-deadline", 1)
    assert publication is not None
    await rig.discussion._cancel_plan_prompt_monitor(publication)  # noqa: SLF001

    clock = [0.0]
    sleeps: list[float] = []

    async def absent(_thread_id: str) -> None:
        return None

    async def advance(delay: float) -> None:
        sleeps.append(delay)
        clock[0] += delay

    monkeypatch.setattr(rig.discussion, "_plan_prompt_visibility", absent)
    monkeypatch.setattr(
        "codex_telegram_bridge.discussion_bot.time.monotonic", lambda: clock[0]
    )
    monkeypatch.setattr("codex_telegram_bridge.discussion_bot.asyncio.sleep", advance)

    await rig.discussion._watch_plan_prompt(  # noqa: SLF001
        rig.store.get_space("space-plan-monitor-deadline"),
        publication,
    )

    assert sleeps[:15] == [2.0] * 15
    assert sleeps[15:] == [10.0] * 57
    assert sum(sleeps) == 600.0
    latest = rig.store.latest_plan_publication("space-plan-monitor-deadline", 1)
    assert latest is not None and latest["status"] == "published"


@pytest.mark.asyncio
async def test_telegram_plan_choices_update_the_original_articles(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    del monkeypatch
    execute_space = add_active_space(
        rig,
        space_id="space-plan-article-execute",
        thread_id="thread-plan-article-execute",
        root_message_id=534,
        channel_post_id=134,
    )
    await rig.discussion.plan_completed(
        "thread-plan-article-execute",
        "turn-plan",
        "item-plan",
        "Execute this article plan.",
    )
    execute_article_id = rig.discussion_endpoint.next_message_id - 1
    execute_publication = rig.store.latest_plan_publication(
        "space-plan-article-execute", 1
    )
    assert execute_publication is not None

    await rig.discussion._execute_plan(
        execute_space,
        {
            "item_id": "item-plan",
            "revision_key": execute_publication["revision_key"],
            "thread_id": "thread-plan-article-execute",
            "turn_id": "turn-plan",
        },
    )

    assert any(
        edit.get("message_id") == execute_article_id
        and edit.get("reply_markup") is None
        and "已批准并开始执行" in edit.get("markdown", "")
        for edit in rig.discussion_endpoint.edited
    )

    revise_space = add_active_space(
        rig,
        space_id="space-plan-article-revise",
        thread_id="thread-plan-article-revise",
        root_message_id=535,
        channel_post_id=135,
    )
    await rig.discussion.plan_completed(
        "thread-plan-article-revise",
        "turn-plan",
        "item-plan",
        "Revise this article plan.",
    )
    revise_article_id = rig.discussion_endpoint.next_message_id - 1
    revise_publication = rig.store.latest_plan_publication(
        "space-plan-article-revise", 1
    )
    assert revise_publication is not None

    await rig.discussion._begin_plan_revision(
        revise_space,
        {
            "item_id": "item-plan",
            "revision_key": revise_publication["revision_key"],
            "thread_id": "thread-plan-article-revise",
            "turn_id": "turn-plan",
        },
    )

    assert any(
        edit.get("message_id") == revise_article_id
        and edit.get("reply_markup") is None
        and "已选择继续完善计划" in edit.get("markdown", "")
        for edit in rig.discussion_endpoint.edited
    )
    await rig.discussion.stop()


@pytest.mark.asyncio
async def test_new_plan_marks_superseded_article_and_retires_old_callback(
    rig: Rig,
) -> None:
    add_active_space(
        rig,
        space_id="space-plan-superseded-ui",
        thread_id="thread-plan-superseded-ui",
        root_message_id=536,
        channel_post_id=136,
    )
    await rig.discussion.plan_completed(
        "thread-plan-superseded-ui",
        "turn-plan",
        "item-plan",
        "First article plan.",
    )
    first_article_id = rig.discussion_endpoint.next_message_id - 1
    first_button = rig.discussion_endpoint.sent[-1]["reply_markup"].inline_keyboard[0][0]

    await rig.discussion.plan_completed(
        "thread-plan-superseded-ui",
        "turn-plan",
        "item-plan",
        "Second article plan.",
    )

    assert any(
        edit.get("message_id") == first_article_id
        and edit.get("reply_markup") is None
        and "已被更新版本替代" in edit.get("markdown", "")
        for edit in rig.discussion_endpoint.edited
    )
    assert rig.store.peek_callback(
        str(first_button.callback_data)[3:],
        OWNER_ID,
        bot_role=DISCUSSION_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
    ) is None
    await rig.discussion.stop()


@pytest.mark.asyncio
async def test_startup_repairs_plan_from_historical_exact_tui_approval(rig: Rig) -> None:
    add_active_space(
        rig,
        space_id="space-plan-startup-tui",
        thread_id="thread-plan-startup-tui",
        root_message_id=537,
        channel_post_id=137,
    )
    await rig.discussion.plan_completed(
        "thread-plan-startup-tui",
        "turn-plan",
        "item-plan",
        "Repair this plan on startup.",
    )
    article_id = rig.discussion_endpoint.next_message_id - 1
    execute_button = rig.discussion_endpoint.sent[-1]["reply_markup"].inline_keyboard[0][0]
    record_tui_plan_approval(
        rig,
        "thread-plan-startup-tui",
        "turn-tui-startup",
    )

    await rig.discussion._repair_plan_publications()

    publication = rig.store.latest_plan_publication("space-plan-startup-tui", 1)
    assert publication is not None
    assert publication["status"] == "executed"
    assert publication["decision_turn_id"] == "turn-tui-startup"
    assert rig.bridge.collaboration_calls == []
    assert rig.store.peek_callback(
        str(execute_button.callback_data)[3:],
        OWNER_ID,
        bot_role=DISCUSSION_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
    ) is None
    assert any(
        edit.get("message_id") == article_id
        and edit.get("reply_markup") is None
        and "已批准并开始执行" in edit.get("markdown", "")
        for edit in rig.discussion_endpoint.edited
    )


@pytest.mark.asyncio
async def test_tg_tui_plan_race_does_not_start_a_second_collaboration_turn(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    space = add_active_space(
        rig,
        space_id="space-plan-tg-tui-race",
        thread_id="thread-plan-tg-tui-race",
        root_message_id=538,
        channel_post_id=138,
    )
    await rig.discussion.plan_completed(
        "thread-plan-tg-tui-race",
        "turn-plan",
        "item-plan",
        "Approve this plan once.",
    )
    publication = rig.store.latest_plan_publication("space-plan-tg-tui-race", 1)
    assert publication is not None

    async def approve_from_tui(thread_id: str) -> None:
        record_tui_plan_approval(rig, thread_id, "turn-tui-race")
        await rig.discussion.plan_turn_started(thread_id, "turn-tui-race")

    monkeypatch.setattr(rig.discussion, "_dismiss_tmux_plan_prompt", approve_from_tui)
    rig.bridge.plan_gate_result = {
        "status": "tui_approval_observed",
        "turn_id": "turn-tui-race",
    }

    await rig.discussion._execute_plan(
        space,
        {
            "item_id": "item-plan",
            "revision_key": publication["revision_key"],
            "thread_id": "thread-plan-tg-tui-race",
            "turn_id": "turn-plan",
        },
    )

    current = rig.store.latest_plan_publication("space-plan-tg-tui-race", 1)
    assert current is not None
    assert current["status"] == "executed"
    assert current["decision_turn_id"] == "turn-tui-race"
    assert rig.bridge.collaboration_calls == []


@pytest.mark.asyncio
async def test_startup_replays_failed_dismissed_plan_message_deletion(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    space = add_active_space(
        rig,
        space_id="space-plan-dismiss-replay",
        thread_id="thread-plan-dismiss-replay",
        root_message_id=539,
        channel_post_id=139,
    )
    plan_text = "\n\n".join(
        f"{index}. Replay deletion chunk {index}: " + "detail " * 80
        for index in range(1, 16)
    )
    await rig.discussion.plan_completed(
        "thread-plan-dismiss-replay",
        "turn-plan",
        "item-plan",
        plan_text,
    )
    publication = rig.store.latest_plan_publication("space-plan-dismiss-replay", 1)
    assert publication is not None
    assert rig.store.mark_external_plan_action(
        "space-plan-dismiss-replay",
        1,
        "item-plan",
        revision_key=publication["revision_key"],
        status="dismissed",
    )
    await rig.discussion._cancel_plan_prompt_monitor(publication)
    failed_ids: list[int] = []

    async def fail_delete(
        _chat_id: int, message_id: int, *, priority: int = 0
    ) -> bool:
        del priority
        failed_ids.append(message_id)
        return False

    original_delete = rig.discussion_endpoint.delete_message
    monkeypatch.setattr(rig.discussion_endpoint, "delete_message", fail_delete)
    await rig.discussion._repair_plan_publications()

    assert set(failed_ids) == set(publication["message_ids"])
    assert rig.discussion_endpoint.deleted == []

    monkeypatch.setattr(rig.discussion_endpoint, "delete_message", original_delete)
    await rig.discussion._repair_plan_publications()

    assert set(publication["message_ids"]) == {
        message_id
        for chat_id, message_id in rig.discussion_endpoint.deleted
        if chat_id == int(space["discussion_chat_id"])
    }


@pytest.mark.asyncio
async def test_getfile_reports_resolver_timeout_in_the_discussion(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-getfile-timeout",
        thread_id="thread-getfile-timeout",
        root_message_id=526,
        channel_post_id=126,
    )

    async def resolve_files(_thread_id: str, _description: str) -> list[Any]:
        raise TimeoutError

    rig.bridge.resolve_files = resolve_files  # type: ignore[attr-defined]
    rig.discussion._require_active_unlocked = lambda _update: space  # type: ignore[method-assign]

    await rig.discussion.getfile(
        SimpleNamespace(
            effective_message=SimpleNamespace(text="/getfile report", caption=None)
        ),
        SimpleNamespace(),
    )

    assert "正在查找并校验文件" in rig.discussion_endpoint.sent[-1]["markdown"]
    assert "文件搜索超时" in rig.discussion_endpoint.edited[-1]["markdown"]


@pytest.mark.asyncio
async def test_getfile_sends_progress_before_resolver_finishes(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-getfile-progress",
        thread_id="thread-getfile-progress",
        root_message_id=527,
        channel_post_id=127,
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def resolve_files(_thread_id: str, _description: str) -> list[Any]:
        started.set()
        await release.wait()
        return []

    rig.bridge.resolve_files = resolve_files  # type: ignore[attr-defined]
    rig.discussion._require_active_unlocked = lambda _update: space  # type: ignore[method-assign]
    task = asyncio.create_task(
        rig.discussion.getfile(
            SimpleNamespace(
                effective_message=SimpleNamespace(text="/getfile report", caption=None)
            ),
            SimpleNamespace(),
        )
    )

    await started.wait()
    assert "正在查找并校验文件" in rig.discussion_endpoint.sent[-1]["markdown"]
    assert rig.discussion_endpoint.edited == []
    release.set()
    await task

    assert "没有找到" in rig.discussion_endpoint.edited[-1]["markdown"]


@pytest.mark.asyncio
async def test_getfile_edit_failure_resends_result_and_deletes_progress(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-getfile-fallback",
        thread_id="thread-getfile-fallback",
        root_message_id=528,
        channel_post_id=128,
    )

    async def resolve_files(_thread_id: str, _description: str) -> list[Any]:
        return []

    async def edit_text(_chat_id: int, _message_id: int, _markdown: str, **_kwargs: Any) -> Any:
        raise TelegramError("edit failed")

    rig.bridge.resolve_files = resolve_files  # type: ignore[attr-defined]
    rig.discussion._require_active_unlocked = lambda _update: space  # type: ignore[method-assign]
    rig.discussion_endpoint.edit_text = edit_text  # type: ignore[method-assign]

    await rig.discussion.getfile(
        SimpleNamespace(
            effective_message=SimpleNamespace(text="/getfile report", caption=None)
        ),
        SimpleNamespace(),
    )

    assert "正在查找并校验文件" in rig.discussion_endpoint.sent[-2]["markdown"]
    assert "没有找到" in rig.discussion_endpoint.sent[-1]["markdown"]
    assert rig.discussion_endpoint.deleted[-1] == (
        int(space["discussion_chat_id"]),
        2000,
    )


@pytest.mark.asyncio
async def test_getfile_edits_progress_into_scoped_file_choice(rig: Rig, tmp_path: Path) -> None:
    space = add_active_space(
        rig,
        space_id="space-getfile-success",
        thread_id="thread-getfile-success",
        root_message_id=529,
        channel_post_id=129,
    )
    report = tmp_path / "report.pdf"
    report.write_bytes(b"pdf")
    metadata = report.stat()
    candidate = FileCandidate(
        path=report,
        size=metadata.st_size,
        modified_at=int(metadata.st_mtime),
        device=metadata.st_dev,
        inode=metadata.st_ino,
        modified_ns=metadata.st_mtime_ns,
    )

    async def resolve_files(_thread_id: str, _description: str) -> list[FileCandidate]:
        return [candidate]

    rig.bridge.resolve_files = resolve_files  # type: ignore[attr-defined]
    rig.discussion._require_active_unlocked = lambda _update: space  # type: ignore[method-assign]

    await rig.discussion.getfile(
        SimpleNamespace(
            effective_message=SimpleNamespace(text="/getfile report", caption=None)
        ),
        SimpleNamespace(),
    )

    edit = rig.discussion_endpoint.edited[-1]
    assert "请选择要发送的文件" in edit["markdown"]
    button = edit["reply_markup"].inline_keyboard[0][0]
    assert button.text == "①"
    callback = rig.store.peek_callback(
        str(button.callback_data)[3:],
        OWNER_ID,
        bot_role=DISCUSSION_ROLE,
        chat_id=int(space["discussion_chat_id"]),
        space_id=str(space["space_id"]),
        generation=int(space["generation"]),
    )
    assert callback is not None
    action, payload = callback
    assert action == "send_file"
    assert payload["path"] == str(report)
    assert payload["inode"] == metadata.st_ino
    assert payload["modified_ns"] == metadata.st_mtime_ns


@pytest.mark.asyncio
async def test_getfile_paginates_all_candidates_and_researches_on_next_page(
    rig: Rig, tmp_path: Path
) -> None:
    space = add_active_space(
        rig,
        space_id="space-getfile-pages",
        thread_id="thread-getfile-pages",
        root_message_id=531,
        channel_post_id=131,
    )
    candidates: list[FileCandidate] = []
    for index in range(9):
        path = tmp_path / f"report-{index}.pdf"
        path.write_bytes(f"pdf-{index}".encode())
        metadata = path.stat()
        candidates.append(
            FileCandidate(
                path=path,
                size=metadata.st_size,
                modified_at=int(metadata.st_mtime),
                device=metadata.st_dev,
                inode=metadata.st_ino,
                modified_ns=metadata.st_mtime_ns,
            )
        )
    queries: list[str] = []

    async def resolve_files(_thread_id: str, description: str) -> list[FileCandidate]:
        queries.append(description)
        return candidates

    rig.bridge.resolve_files = resolve_files  # type: ignore[attr-defined]
    rig.security.unlocked.add(str(space["space_id"]))
    rig.discussion._require_active_unlocked = lambda _update: space  # type: ignore[method-assign]

    await rig.discussion.getfile(
        SimpleNamespace(
            effective_message=SimpleNamespace(text="/getfile pdf", caption=None)
        ),
        SimpleNamespace(),
    )

    first_page = rig.discussion_endpoint.edited[-1]
    assert "共 9 个，第 1/2 页" in first_page["markdown"]
    assert [len(row) for row in first_page["reply_markup"].inline_keyboard] == [4, 4, 1]
    assert [
        button.text
        for row in first_page["reply_markup"].inline_keyboard[:-1]
        for button in row
    ] == list("①②③④⑤⑥⑦⑧")
    next_button = first_page["reply_markup"].inline_keyboard[-1][0]
    assert next_button.text == "下一页"

    callback_message = SimpleNamespace(message_id=int(first_page["message_id"]))
    await rig.discussion.callback(
        SimpleNamespace(
            callback_query=SimpleNamespace(data=next_button.callback_data, message=callback_message),
            effective_user=SimpleNamespace(id=OWNER_ID),
            effective_chat=SimpleNamespace(id=DISCUSSION_CHAT_ID),
            effective_message=callback_message,
        ),
        SimpleNamespace(),
    )

    assert queries == ["pdf", "pdf"]
    second_page = rig.discussion_endpoint.edited[-1]
    assert "共 9 个，第 2/2 页" in second_page["markdown"]
    assert "9\\." in second_page["markdown"]
    assert [len(row) for row in second_page["reply_markup"].inline_keyboard] == [1, 1]
    assert second_page["reply_markup"].inline_keyboard[0][0].text == "①"
    assert second_page["reply_markup"].inline_keyboard[-1][0].text == "上一页"


@pytest.mark.asyncio
async def test_getfile_discards_result_after_space_generation_changes(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-getfile-stale",
        thread_id="thread-getfile-stale",
        root_message_id=530,
        channel_post_id=130,
    )

    async def resolve_files(_thread_id: str, _description: str) -> list[Any]:
        assert rig.store.close_space(
            str(space["space_id"]), expected_generation=int(space["generation"])
        ) is not None
        return []

    rig.bridge.resolve_files = resolve_files  # type: ignore[attr-defined]
    rig.discussion._require_active_unlocked = lambda _update: space  # type: ignore[method-assign]

    await rig.discussion.getfile(
        SimpleNamespace(
            effective_message=SimpleNamespace(text="/getfile report", caption=None)
        ),
        SimpleNamespace(),
    )

    assert rig.discussion_endpoint.edited == []
    assert rig.discussion_endpoint.deleted[-1] == (
        int(space["discussion_chat_id"]),
        2000,
    )


@pytest.mark.asyncio
async def test_command_approval_is_forwarded_and_consumed_from_discussion_callback(
    rig: Rig,
) -> None:
    add_active_space(
        rig,
        space_id="space-command-approval",
        thread_id="thread-command-approval",
        root_message_id=529,
        channel_post_id=129,
    )
    request_key = "approval:request-1"
    rig.store.put_pending_input(
        request_key,
        "91",
        1,
        "thread-command-approval",
        "turn-command-approval",
        "item-command-approval",
        [
            {
                "_bridge_request_kind": "command_approval",
                "_bridge_approval_method": "item/commandExecution/requestApproval",
            }
        ],
        int(time.time()) + 300,
    )
    decisions: list[tuple[str, str]] = []

    async def answer(request: str, decision: str) -> None:
        decisions.append((request, decision))

    rig.bridge.answer_command_approval = answer  # type: ignore[attr-defined]
    await rig.discussion.forward_command_approval(
        request_key,
        {
            "threadId": "thread-command-approval",
            "turnId": "turn-command-approval",
            "itemId": "item-command-approval",
            "command": "rm -i example.txt",
            "cwd": str(rig.config.allowed_root),
            "reason": "needs confirmation",
        },
    )

    message = rig.discussion_endpoint.sent[-1]
    assert "请求执行命令" in message["markdown"]
    assert [
        button.text
        for row in message["reply_markup"].inline_keyboard
        for button in row
    ] == [
        "批准执行",
        "本 Session 放行",
        "拒绝",
    ]
    approval_messages = rig.store.question_messages(request_key)
    assert len(approval_messages) == 1
    assert approval_messages[0]["message_kind"] == "summary_anchor"

    rig.security.unlocked.add("space-command-approval")
    button = message["reply_markup"].inline_keyboard[1][0]
    callback_message = SimpleNamespace(
        chat=SimpleNamespace(id=DISCUSSION_CHAT_ID, type=ChatType.SUPERGROUP),
        chat_id=DISCUSSION_CHAT_ID,
        message_id=rig.discussion_endpoint.next_message_id - 1,
        message_thread_id=529,
        reply_to_message=None,
        text=None,
        caption=None,
        is_automatic_forward=False,
        sender_chat=None,
    )
    query = SimpleNamespace(data=button.callback_data, message=callback_message)
    update = SimpleNamespace(
        callback_query=query,
        effective_user=SimpleNamespace(id=OWNER_ID),
        effective_chat=SimpleNamespace(id=DISCUSSION_CHAT_ID, type=ChatType.SUPERGROUP),
        effective_message=callback_message,
    )

    await rig.discussion.callback(update, SimpleNamespace())
    await rig.discussion.callback(update, SimpleNamespace())

    assert decisions == [(request_key, "acceptForSession")]
    assert rig.store.peek_callback(
        str(button.callback_data)[3:],
        OWNER_ID,
        bot_role=DISCUSSION_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
    ) is None
    assert "已批准本 Session" in rig.discussion_endpoint.sent[-1]["markdown"]
    await rig.discussion.question_resolved(request_key)
    await asyncio.gather(*list(rig.discussion._background_tasks))
    assert "Codex 命令审批 · 已处理" in rig.discussion_endpoint.edited[-1]["markdown"]
    assert rig.discussion_endpoint.deleted == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "method",
        "decision",
        "heading",
        "button_label",
        "confirmation",
        "summary_heading",
    ),
    [
        (
            "item/fileChange/requestApproval",
            "acceptForSession",
            "请求修改文件",
            "本 Session 放行",
            "已批准本 Session 后续文件变更",
            "Codex 文件变更审批 · 已处理",
        ),
        (
            "item/permissions/requestApproval",
            {
                "permissions": {"fileSystem": {"read": ["/workspace"]}},
                "scope": "session",
            },
            "请求授予临时权限",
            "本 Session 授权",
            "已授予请求的权限",
            "Codex 权限审批 · 已处理",
        ),
        (
            "applyPatchApproval",
            "cancel",
            "请求应用文件补丁",
            "拒绝并中止 Turn",
            "已拒绝文件变更并中止当前 Turn",
            "Codex 文件变更审批 · 已处理",
        ),
    ],
)
async def test_generic_approvals_are_forwarded_answered_and_summarized(
    rig: Rig,
    method: str,
    decision: Any,
    heading: str,
    button_label: str,
    confirmation: str,
    summary_heading: str,
) -> None:
    space = add_active_space(
        rig,
        space_id="space-generic-approval",
        thread_id="thread-generic-approval",
        root_message_id=539,
        channel_post_id=139,
    )
    params: dict[str, Any] = {
        "threadId": "thread-generic-approval",
        "turnId": "turn-generic-approval",
        "itemId": "item-generic-approval",
        "cwd": str(rig.config.allowed_root),
        "reason": "needs confirmation",
    }
    if method == "item/fileChange/requestApproval":
        params["availableDecisions"] = ["accept", "acceptForSession", "decline"]
        params["grantRoot"] = str(rig.config.allowed_root)
    elif method == "item/permissions/requestApproval":
        params["permissions"] = {"fileSystem": {"read": ["/workspace"]}}
    available = interactive_approval_decisions(method, params)
    request_key = f"approval:generic-{method.split('/')[0]}"
    rig.store.put_pending_input(
        request_key,
        "94",
        1,
        "thread-generic-approval",
        "turn-generic-approval",
        "item-generic-approval",
        [
            {
                "_bridge_request_kind": "generic_approval",
                "_bridge_approval_method": method,
                "_bridge_available_decisions": available,
                "params": params,
            }
        ],
        int(time.time()) + 300,
    )
    decisions: list[tuple[str, Any]] = []

    async def answer(request: str, selected: Any) -> None:
        decisions.append((request, selected))

    rig.bridge.answer_command_approval = answer  # type: ignore[attr-defined]
    await rig.discussion.forward_command_approval(request_key, params)

    message = rig.discussion_endpoint.sent[-1]
    assert heading in message["markdown"]
    assert button_label in [
        button.text
        for row in message["reply_markup"].inline_keyboard
        for button in row
    ]

    await rig.discussion._answer_command_approval(  # noqa: SLF001
        space,
        {"request_key": request_key, "decision": decision},
    )

    assert decisions == [(request_key, decision)]
    assert confirmation in rig.discussion_endpoint.sent[-1]["markdown"]
    await rig.discussion.question_resolved(request_key)
    await asyncio.gather(*list(rig.discussion._background_tasks))
    assert summary_heading in rig.discussion_endpoint.edited[-1]["markdown"]
    assert "Codex 请求输入" not in rig.discussion_endpoint.edited[-1]["markdown"]


@pytest.mark.asyncio
async def test_command_approval_buttons_follow_available_decision_order(rig: Rig) -> None:
    add_active_space(
        rig,
        space_id="space-command-choices",
        thread_id="thread-command-choices",
        root_message_id=530,
        channel_post_id=130,
    )
    request_key = "approval:request-choices"
    exec_amendment = {
        "acceptWithExecpolicyAmendment": {
            "execpolicy_amendment": ["bash", "-lc", "echo hello"],
        }
    }
    network_amendment = {
        "applyNetworkPolicyAmendment": {
            "network_policy_amendment": {"action": "allow", "host": "example.com"},
        }
    }
    available = ["cancel", exec_amendment, network_amendment, "accept"]
    params = {
        "threadId": "thread-command-choices",
        "turnId": "turn-command-choices",
        "itemId": "item-command-choices",
        "command": ["bash", "-lc", "echo hello"],
        "cwd": str(rig.config.allowed_root),
        "availableDecisions": available,
    }
    rig.store.put_pending_input(
        request_key,
        "92",
        1,
        "thread-command-choices",
        "turn-command-choices",
        "item-command-choices",
        [
            {
                "_bridge_request_kind": "command_approval",
                "_bridge_approval_method": "item/commandExecution/requestApproval",
                "_bridge_available_decisions": available,
                "params": params,
            }
        ],
        int(time.time()) + 300,
    )

    await rig.discussion.forward_command_approval(request_key, params)

    message = rig.discussion_endpoint.sent[-1]
    buttons = [button for row in message["reply_markup"].inline_keyboard for button in row]
    assert [button.text for button in buttons] == [
        "拒绝并中止 Turn",
        "批准并应用命令规则",
        "应用网络允许规则",
        "批准执行",
    ]
    assert "['bash'" not in message["markdown"]
    callback_decisions: list[Any] = []
    for button in buttons:
        callback = rig.store.peek_callback(
            str(button.callback_data)[3:],
            OWNER_ID,
            bot_role=DISCUSSION_ROLE,
            chat_id=DISCUSSION_CHAT_ID,
        )
        assert callback is not None
        callback_decisions.append(callback[1]["decision"])
    assert callback_decisions == available


@pytest.mark.asyncio
async def test_failed_command_approval_response_creates_fresh_retry_callback(rig: Rig) -> None:
    add_active_space(
        rig,
        space_id="space-command-retry",
        thread_id="thread-command-retry",
        root_message_id=531,
        channel_post_id=131,
    )
    request_key = "approval:request-retry"
    params = {
        "threadId": "thread-command-retry",
        "turnId": "turn-command-retry",
        "itemId": "item-command-retry",
        "command": "git status",
        "cwd": str(rig.config.allowed_root),
        "availableDecisions": ["accept"],
    }
    rig.store.put_pending_input(
        request_key,
        "93",
        1,
        "thread-command-retry",
        "turn-command-retry",
        "item-command-retry",
        [
            {
                "_bridge_request_kind": "command_approval",
                "_bridge_approval_method": "item/commandExecution/requestApproval",
                "_bridge_available_decisions": ["accept"],
                "params": params,
            }
        ],
        int(time.time()) + 300,
    )
    attempts: list[tuple[str, Any]] = []

    async def answer(request: str, decision: Any) -> None:
        attempts.append((request, decision))
        if len(attempts) == 1:
            raise OSError("app-server send failed")

    rig.bridge.answer_command_approval = answer  # type: ignore[attr-defined]
    await rig.discussion.forward_command_approval(request_key, params)
    first = rig.discussion_endpoint.sent[-1]["reply_markup"].inline_keyboard[0][0]
    rig.security.unlocked.add("space-command-retry")
    callback_message = SimpleNamespace(
        chat=SimpleNamespace(id=DISCUSSION_CHAT_ID, type=ChatType.SUPERGROUP),
        chat_id=DISCUSSION_CHAT_ID,
        message_id=2031,
        message_thread_id=531,
        reply_to_message=None,
        text=None,
        caption=None,
        is_automatic_forward=False,
        sender_chat=None,
    )

    await rig.discussion.callback(
        SimpleNamespace(
            update_id=8131,
            callback_query=SimpleNamespace(data=first.callback_data, message=callback_message),
            effective_user=SimpleNamespace(id=OWNER_ID),
            effective_chat=SimpleNamespace(id=DISCUSSION_CHAT_ID, type=ChatType.SUPERGROUP),
            effective_message=callback_message,
        ),
        SimpleNamespace(),
    )

    retry_message = rig.discussion_endpoint.sent[-1]
    retry_button = retry_message["reply_markup"].inline_keyboard[0][0]
    assert "审批需重试" in retry_message["markdown"]
    assert retry_button.callback_data != first.callback_data
    assert rig.store.peek_callback(
        str(first.callback_data)[3:],
        OWNER_ID,
        bot_role=DISCUSSION_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
    ) is None
    assert rig.store.pop_question_resolution(request_key) is None

    await rig.discussion.callback(
        SimpleNamespace(
            update_id=8132,
            callback_query=SimpleNamespace(data=retry_button.callback_data, message=callback_message),
            effective_user=SimpleNamespace(id=OWNER_ID),
            effective_chat=SimpleNamespace(id=DISCUSSION_CHAT_ID, type=ChatType.SUPERGROUP),
            effective_message=callback_message,
        ),
        SimpleNamespace(),
    )

    assert attempts == [(request_key, "accept"), (request_key, "accept")]
    resolution = rig.store.pop_question_resolution(request_key)
    assert resolution is not None
    assert resolution["answers"] == {"decision": ["accept"]}
    assert "已批准本次命令" in rig.discussion_endpoint.sent[-1]["markdown"]


@pytest.mark.asyncio
async def test_plan_revision_force_reply_starts_plan_turn(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-revise",
        thread_id="thread-revise",
        root_message_id=522,
        channel_post_id=122,
    )
    rig.security.unlocked.add("space-revise")
    await rig.discussion.plan_completed(
        "thread-revise",
        "turn-plan",
        "item-plan",
        "Review the implementation plan.",
    )
    publication = rig.store.latest_plan_publication("space-revise", 1)
    assert publication is not None
    await rig.discussion._dispatch_callback(
        "plan_continue",
        {
            "item_id": "item-plan",
            "revision_key": publication["revision_key"],
            "thread_id": "thread-revise",
            "turn_id": "turn-plan",
        },
        space,
    )
    force_prompt = rig.discussion_endpoint.sent[-1]
    assert isinstance(force_prompt["reply_markup"], ForceReply)
    assert force_prompt["reply_markup"].input_field_placeholder == "输入 Plan 修改意见"
    prompt_message_id = rig.discussion_endpoint.next_message_id - 1
    reply = update_for_message(
        "Add a rollback verification step.",
        update_id=130,
        message_id=50,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=522,
        reply_to_message=SimpleNamespace(
            message_id=prompt_message_id,
            message_thread_id=522,
            is_automatic_forward=False,
        ),
    )

    with pytest.raises(ApplicationHandlerStop):
        await rig.discussion.reply_to_intent(reply, SimpleNamespace())

    [call] = rig.bridge.collaboration_calls
    assert call["mode"] == "plan"
    assert "Do not implement it yet" in call["prompt"]
    assert "rollback verification" in call["prompt"]
    assert (
        rig.store.latest_plan_publication("space-revise", 1)["status"]
        == "revision_started"
    )


@pytest.mark.asyncio
async def test_plan_revision_gate_reconciles_tui_approval_without_force_reply(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-revise-tui-gate",
        thread_id="thread-revise-tui-gate",
        root_message_id=551,
        channel_post_id=151,
    )
    await rig.discussion.plan_completed(
        "thread-revise-tui-gate",
        "turn-plan",
        "item-plan",
        "Review this plan in TUI.",
    )
    publication = rig.store.latest_plan_publication("space-revise-tui-gate", 1)
    assert publication is not None
    sent_before = len(rig.discussion_endpoint.sent)
    rig.bridge.plan_gate_result = {
        "status": "tui_approval_observed",
        "turn_id": "turn-tui-approval",
    }

    await rig.discussion._begin_plan_revision(
        space,
        {
            "item_id": "item-plan",
            "revision_key": publication["revision_key"],
            "thread_id": "thread-revise-tui-gate",
            "turn_id": "turn-plan",
        },
    )

    current = rig.store.latest_plan_publication("space-revise-tui-gate", 1)
    assert current is not None and current["status"] == "executed"
    assert current["decision_turn_id"] == "turn-tui-approval"
    assert len(rig.discussion_endpoint.sent) == sent_before
    assert rig.bridge.collaboration_calls == []


@pytest.mark.asyncio
async def test_plan_revision_gate_reconciles_existing_delivery_without_force_reply(
    rig: Rig,
) -> None:
    space = add_active_space(
        rig,
        space_id="space-revise-delivery-gate",
        thread_id="thread-revise-delivery-gate",
        root_message_id=552,
        channel_post_id=152,
    )
    await rig.discussion.plan_completed(
        "thread-revise-delivery-gate",
        "turn-plan",
        "item-plan",
        "Review this delivered revision.",
    )
    publication = rig.store.latest_plan_publication("space-revise-delivery-gate", 1)
    assert publication is not None
    sent_before = len(rig.discussion_endpoint.sent)
    rig.bridge.plan_gate_result = {"status": "already_delivered"}

    await rig.discussion._begin_plan_revision(
        space,
        {
            "item_id": "item-plan",
            "revision_key": publication["revision_key"],
            "thread_id": "thread-revise-delivery-gate",
            "turn_id": "turn-plan",
        },
    )

    current = rig.store.latest_plan_publication("space-revise-delivery-gate", 1)
    assert current is not None and current["status"] == "revision_started"
    assert not current.get("decision_turn_id")
    assert len(rig.discussion_endpoint.sent) == sent_before
    [gate_call] = rig.bridge.plan_gate_calls
    assert gate_call == {
        "space_id": "space-revise-delivery-gate",
        "generation": 1,
        "item_id": "item-plan",
        "revision_key": publication["revision_key"],
        "client_message_id": (
            "telegram-plan-revise-space-revise-delivery-gate-1-item-plan-"
            f"{publication['revision_key']}"
        ),
        "timeout": 1.0,
    }


@pytest.mark.asyncio
async def test_plan_revision_gate_uncertain_keeps_buttons_removed_without_force_reply(
    rig: Rig,
) -> None:
    space = add_active_space(
        rig,
        space_id="space-revise-uncertain-gate",
        thread_id="thread-revise-uncertain-gate",
        root_message_id=553,
        channel_post_id=153,
    )
    await rig.discussion.plan_completed(
        "thread-revise-uncertain-gate",
        "turn-plan",
        "item-plan",
        "Review this uncertain revision.",
    )
    publication = rig.store.latest_plan_publication("space-revise-uncertain-gate", 1)
    assert publication is not None
    sent_before = len(rig.discussion_endpoint.sent)
    rig.bridge.plan_gate_result = {"status": "uncertain"}

    await rig.discussion._begin_plan_revision(
        space,
        {
            "item_id": "item-plan",
            "revision_key": publication["revision_key"],
            "thread_id": "thread-revise-uncertain-gate",
            "turn_id": "turn-plan",
        },
    )

    current = rig.store.latest_plan_publication("space-revise-uncertain-gate", 1)
    assert current is not None and current["status"] == "revising"
    assert len(rig.discussion_endpoint.sent) == sent_before
    assert any(
        "修改请求送达状态待确认" in edit.get("markdown", "")
        and edit.get("reply_markup") is None
        for edit in rig.discussion_endpoint.edited
    )


@pytest.mark.asyncio
async def test_ambiguous_plan_revision_reconciles_without_duplicate(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-revise-delivered",
        thread_id="thread-revise-delivered",
        root_message_id=526,
        channel_post_id=126,
    )
    rig.security.unlocked.add("space-revise-delivered")
    await rig.discussion.plan_completed(
        "thread-revise-delivered",
        "turn-plan",
        "item-plan",
        "Review this plan once.",
    )
    publication = rig.store.latest_plan_publication("space-revise-delivered", 1)
    assert publication is not None
    await rig.discussion._dispatch_callback(
        "plan_continue",
        {
            "item_id": "item-plan",
            "revision_key": publication["revision_key"],
            "thread_id": "thread-revise-delivered",
            "turn_id": "turn-plan",
        },
        space,
    )
    prompt_message_id = rig.discussion_endpoint.next_message_id - 1
    rig.bridge.collaboration_error = RuntimeError("response lost")
    rig.bridge.reconcile_status = "delivered"
    reply = update_for_message(
        "Add rollback checks.",
        update_id=131,
        message_id=51,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=526,
        reply_to_message=SimpleNamespace(
            message_id=prompt_message_id,
            message_thread_id=526,
            is_automatic_forward=False,
        ),
    )

    with pytest.raises(ApplicationHandlerStop):
        await rig.discussion.reply_to_intent(reply, SimpleNamespace())

    publication = rig.store.latest_plan_publication("space-revise-delivered", 1)
    assert publication is not None and publication["status"] == "revision_started"
    assert rig.store.recoverable_plan_publications() == []
    assert len(rig.bridge.collaboration_calls) == 1


@pytest.mark.asyncio
async def test_plan_revision_prompt_failure_releases_fresh_action_buttons(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    space = add_active_space(
        rig,
        space_id="space-revise-send-fail",
        thread_id="thread-revise-send-fail",
        root_message_id=527,
        channel_post_id=127,
    )
    await rig.discussion.plan_completed(
        "thread-revise-send-fail",
        "turn-plan",
        "item-plan",
        "Plan can be revised.",
    )
    publication = rig.store.latest_plan_publication("space-revise-send-fail", 1)
    assert publication is not None
    original_send = rig.discussion._send_space
    attempts = 0

    async def fail_once(*args: Any, **kwargs: Any) -> Any:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("Telegram send failed")
        return await original_send(*args, **kwargs)

    monkeypatch.setattr(rig.discussion, "_send_space", fail_once)
    await rig.discussion._begin_plan_revision(
        space,
        {
            "item_id": "item-plan",
            "revision_key": publication["revision_key"],
            "thread_id": "thread-revise-send-fail",
            "turn_id": "turn-plan",
        },
    )

    publication = rig.store.latest_plan_publication("space-revise-send-fail", 1)
    assert publication is not None and publication["status"] == "published"
    assert rig.discussion_endpoint.sent[-1]["reply_markup"] is not None


@pytest.mark.asyncio
async def test_failed_plan_execute_is_not_dispatched_twice(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-plan-fail",
        thread_id="thread-plan-fail",
        root_message_id=523,
        channel_post_id=123,
    )
    rig.security.unlocked.add("space-plan-fail")
    await rig.discussion.plan_completed(
        "thread-plan-fail",
        "turn-plan",
        "item-plan",
        "Execute safely.",
    )
    publication = rig.store.latest_plan_publication("space-plan-fail", 1)
    assert publication is not None
    payload = {
        "item_id": "item-plan",
        "revision_key": publication["revision_key"],
        "thread_id": "thread-plan-fail",
        "turn_id": "turn-plan",
    }
    rig.bridge.collaboration_error = RuntimeError("start failed")

    await rig.discussion._execute_plan(space, payload)
    with pytest.raises(RuntimeError, match="已处理"):
        await rig.discussion._execute_plan(space, payload)

    assert len(rig.bridge.collaboration_calls) == 1
    assert any(
        "送达状态待确认" in edit.get("markdown", "")
        for edit in rig.discussion_endpoint.edited
    )


@pytest.mark.asyncio
async def test_ambiguous_plan_execute_marks_delivered_action_terminal(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-plan-delivered",
        thread_id="thread-plan-delivered",
        root_message_id=525,
        channel_post_id=125,
    )
    await rig.discussion.plan_completed(
        "thread-plan-delivered",
        "turn-plan",
        "item-plan",
        "Execute once.",
    )
    publication = rig.store.latest_plan_publication("space-plan-delivered", 1)
    assert publication is not None
    rig.bridge.collaboration_error = RuntimeError("response lost")
    rig.bridge.reconcile_status = "delivered"

    await rig.discussion._execute_plan(
        space,
        {
            "item_id": "item-plan",
            "revision_key": publication["revision_key"],
            "thread_id": "thread-plan-delivered",
            "turn_id": "turn-plan",
        },
    )

    publication = rig.store.latest_plan_publication("space-plan-delivered", 1)
    assert publication is not None and publication["status"] == "executed"
    assert rig.store.recoverable_plan_publications() == []
    assert len(rig.bridge.collaboration_calls) == 1


@pytest.mark.asyncio
async def test_republished_plan_with_reused_item_rejects_old_button_without_consuming_it(
    rig: Rig,
) -> None:
    add_active_space(
        rig,
        space_id="space-plan-republished",
        thread_id="thread-plan-republished",
        root_message_id=528,
        channel_post_id=128,
    )
    rig.security.unlocked.add("space-plan-republished")

    await rig.discussion.plan_completed(
        "thread-plan-republished",
        "turn-plan",
        "item-plan",
        "First version of the plan.",
    )
    first_article = rig.discussion_endpoint.sent[-1]
    old_button = first_article["reply_markup"].inline_keyboard[0][0]
    old_nonce = str(old_button.callback_data)[3:]
    first = rig.store.latest_plan_publication("space-plan-republished", 1)
    assert first is not None

    await rig.discussion.plan_completed(
        "thread-plan-republished",
        "turn-plan",
        "item-plan",
        "Second version after BTW feedback.",
    )
    second_article = rig.discussion_endpoint.sent[-1]
    new_button = second_article["reply_markup"].inline_keyboard[0][0]
    second = rig.store.latest_plan_publication("space-plan-republished", 1)
    assert second is not None
    assert second["revision_key"] != first["revision_key"]
    assert second["status"] == "published"

    old_query = SimpleNamespace(data=old_button.callback_data)
    old_update = SimpleNamespace(
        callback_query=old_query,
        effective_user=SimpleNamespace(id=OWNER_ID),
        effective_chat=SimpleNamespace(id=DISCUSSION_CHAT_ID),
    )
    await rig.discussion.callback(old_update, SimpleNamespace())

    assert rig.bridge.collaboration_calls == []
    assert "过期" in rig.discussion_endpoint.callback_answers[-1]["text"]
    assert rig.store.peek_callback(
        old_nonce,
        OWNER_ID,
        bot_role=DISCUSSION_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
    ) is None

    new_update = SimpleNamespace(
        callback_query=SimpleNamespace(data=new_button.callback_data),
        effective_user=SimpleNamespace(id=OWNER_ID),
        effective_chat=SimpleNamespace(id=DISCUSSION_CHAT_ID),
    )
    await rig.discussion.callback(new_update, SimpleNamespace())

    assert len(rig.bridge.collaboration_calls) == 1
    assert second["revision_key"] in rig.bridge.collaboration_calls[0]["client_message_id"]
    latest = rig.store.latest_plan_publication("space-plan-republished", 1)
    assert latest is not None and latest["status"] == "executed"


@pytest.mark.asyncio
async def test_plan_button_rechecks_revision_after_readiness_await(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    add_active_space(
        rig,
        space_id="space-plan-race",
        thread_id="thread-plan-race",
        root_message_id=532,
        channel_post_id=132,
    )
    rig.security.unlocked.add("space-plan-race")
    await rig.discussion.plan_completed(
        "thread-plan-race",
        "turn-plan",
        "item-plan",
        "First plan revision.",
    )
    old_button = rig.discussion_endpoint.sent[-1]["reply_markup"].inline_keyboard[0][0]
    old_nonce = str(old_button.callback_data)[3:]
    entered = asyncio.Event()
    release = asyncio.Event()

    async def pause_readiness(_space: dict[str, Any]) -> None:
        entered.set()
        await release.wait()

    monkeypatch.setattr(rig.discussion, "_ensure_plan_ready", pause_readiness)
    update = SimpleNamespace(
        callback_query=SimpleNamespace(data=old_button.callback_data),
        effective_user=SimpleNamespace(id=OWNER_ID),
        effective_chat=SimpleNamespace(id=DISCUSSION_CHAT_ID),
    )
    callback_task = asyncio.create_task(rig.discussion.callback(update, SimpleNamespace()))
    await entered.wait()
    await rig.discussion.plan_completed(
        "thread-plan-race",
        "turn-plan",
        "item-plan",
        "Second plan revision published during readiness.",
    )
    release.set()
    await callback_task

    assert rig.bridge.collaboration_calls == []
    assert "已过期" in rig.discussion_endpoint.sent[-1]["markdown"]
    assert rig.store.peek_callback(
        old_nonce,
        OWNER_ID,
        bot_role=DISCUSSION_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
    ) is None


@pytest.mark.asyncio
async def test_plan_revision_reply_rechecks_revision_after_readiness_await(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
) -> None:
    space = add_active_space(
        rig,
        space_id="space-plan-reply-race",
        thread_id="thread-plan-reply-race",
        root_message_id=533,
        channel_post_id=133,
    )
    rig.security.unlocked.add("space-plan-reply-race")
    await rig.discussion.plan_completed(
        "thread-plan-reply-race",
        "turn-plan",
        "item-plan",
        "First plan revision.",
    )
    first = rig.store.latest_plan_publication("space-plan-reply-race", 1)
    assert first is not None
    await rig.discussion._begin_plan_revision(
        space,
        {
            "item_id": "item-plan",
            "revision_key": first["revision_key"],
            "thread_id": "thread-plan-reply-race",
            "turn_id": "turn-plan",
        },
    )
    prompt_message_id = rig.discussion_endpoint.next_message_id - 1
    reply_nonce = rig.discussion._reply_nonce(DISCUSSION_CHAT_ID, prompt_message_id)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def pause_readiness(_space: dict[str, Any]) -> None:
        entered.set()
        await release.wait()

    monkeypatch.setattr(rig.discussion, "_ensure_plan_ready", pause_readiness)
    reply = update_for_message(
        "Add rollback checks.",
        update_id=155,
        message_id=75,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=533,
        reply_to_message=SimpleNamespace(
            message_id=prompt_message_id,
            message_thread_id=533,
            is_automatic_forward=False,
        ),
    )
    reply_task = asyncio.create_task(rig.discussion.reply_to_intent(reply, SimpleNamespace()))
    await entered.wait()
    await rig.discussion.plan_completed(
        "thread-plan-reply-race",
        "turn-plan",
        "item-plan",
        "Second revision published while the reply was waiting.",
    )
    release.set()
    with pytest.raises(ApplicationHandlerStop):
        await reply_task

    assert rig.bridge.collaboration_calls == []
    assert "已过期" in rig.discussion_endpoint.sent[-1]["markdown"]
    assert rig.store.peek_callback(
        reply_nonce,
        OWNER_ID,
        bot_role=DISCUSSION_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
        space_id="space-plan-reply-race",
        generation=1,
    ) is not None


@pytest.mark.asyncio
async def test_prompt_completion_receipt_is_scoped_to_active_generation(rig: Rig) -> None:
    add_active_space(
        rig,
        space_id="space-receipt",
        thread_id="thread-receipt",
        root_message_id=524,
        channel_post_id=124,
    )
    await rig.discussion.prompt_completed(
        {
            "space_id": "space-receipt",
            "generation": 1,
            "thread_id": "thread-receipt",
            "turn_id": "turn-receipt",
            "status": "completed",
        }
    )
    assert "任务已完成" in rig.discussion_endpoint.sent[-1]["markdown"]
    count = len(rig.discussion_endpoint.sent)

    await rig.discussion.prompt_completed(
        {
            "space_id": "space-receipt",
            "generation": 2,
            "thread_id": "thread-receipt",
            "turn_id": "stale-turn",
            "status": "failed",
        }
    )
    assert len(rig.discussion_endpoint.sent) == count


@pytest.mark.asyncio
async def test_prompt_receipt_survives_restart_and_completion_edits_same_message(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-receipt-restart",
        thread_id="thread-receipt-restart",
        root_message_id=554,
        channel_post_id=154,
    )
    client_message_id = "telegram-receipt-restart"

    await rig.discussion._send_prompt(
        space,
        "Run focused tests.",
        "auto",
        client_message_id=client_message_id,
    )

    receipt_message_id = rig.discussion_endpoint.next_message_id - 1
    assert rig.store._test_prompt_receipt_links == [  # type: ignore[attr-defined]
        (client_message_id, f"prompt:{client_message_id}")
    ]
    stored = rig.store.get_telegram_message_state(f"prompt:{client_message_id}")  # type: ignore[attr-defined]
    assert stored is not None and stored["state"] == "started"
    rig.discussion._prompt_receipts.clear()

    await rig.discussion.prompt_completed(
        {
            "space_id": "space-receipt-restart",
            "generation": 1,
            "thread_id": "thread-receipt-restart",
            "client_message_id": client_message_id,
            "turn_id": "turn-receipt-restart",
            "status": "completed",
        }
    )

    assert rig.discussion_endpoint.edited[-1]["message_id"] == receipt_message_id
    assert "任务已完成" in rig.discussion_endpoint.edited[-1]["markdown"]
    terminal = rig.store.get_telegram_message_state(f"prompt:{client_message_id}")  # type: ignore[attr-defined]
    assert terminal is not None and terminal["state"] == "completed"
    assert terminal["message_id"] == receipt_message_id


@pytest.mark.asyncio
async def test_prompt_choose_callback_reuses_client_id_and_receipt_message(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-receipt-choose",
        thread_id="thread-receipt-choose",
        root_message_id=555,
        channel_post_id=155,
    )
    rig.security.unlocked.add("space-receipt-choose")
    outcomes = iter(("choose", "queued"))

    async def send_prompt(
        space_id: str,
        prompt: str,
        *,
        mode: str,
        client_message_id: str,
    ) -> str:
        rig.bridge.prompt_calls.append(
            {
                "space_id": space_id,
                "prompt": prompt,
                "mode": mode,
                "client_message_id": client_message_id,
            }
        )
        return next(outcomes)

    rig.bridge.send_space_prompt = send_prompt  # type: ignore[method-assign]
    client_message_id = "telegram-receipt-choose"

    await rig.discussion._send_prompt(
        space,
        "Queue after choice.",
        "auto",
        client_message_id=client_message_id,
    )
    receipt_message_id = rig.discussion_endpoint.next_message_id - 1
    markup = rig.discussion_endpoint.edited[-1]["reply_markup"]
    queue_button = markup.inline_keyboard[1][0]
    nonce = str(queue_button.callback_data)[3:]
    callback = rig.store.peek_callback(
        nonce,
        OWNER_ID,
        bot_role=DISCUSSION_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
        space_id="space-receipt-choose",
        generation=1,
    )
    assert callback is not None

    replacement_message_id = receipt_message_id + 100
    rig.store.put_telegram_message_state(  # type: ignore[attr-defined]
        f"prompt:{client_message_id}",
        bot_role=DISCUSSION_ROLE,
        chat_id=DISCUSSION_CHAT_ID,
        message_id=replacement_message_id,
        semantic_fingerprint="replacement",
        state="choose",
        payload={
            "space_id": "space-receipt-choose",
            "generation": 1,
            "client_message_id": client_message_id,
        },
    )
    rig.discussion._prompt_receipts.clear()
    action, payload = callback
    await rig.discussion._dispatch_callback(action, payload, space)

    assert rig.bridge.prompt_calls[-1]["client_message_id"] == client_message_id
    assert rig.discussion_endpoint.edited[-1]["message_id"] == replacement_message_id
    assert "已加入队列" in rig.discussion_endpoint.edited[-1]["markdown"]


def test_interactive_approval_payloads_cover_file_permissions_and_legacy_patch() -> None:
    file_choices = interactive_approval_decisions(
        "item/fileChange/requestApproval",
        {"availableDecisions": ["accept", "acceptForSession", "decline"]},
    )
    assert file_choices == ["accept", "acceptForSession", "decline"]
    assert approval_response_payload(
        "item/fileChange/requestApproval", "acceptForSession"
    ) == {"decision": "acceptForSession"}

    requested = {"fileSystem": {"read": ["/workspace"]}}
    assert interactive_approval_decisions(
        "item/permissions/requestApproval", {"permissions": requested}
    ) == [
        {"permissions": requested, "scope": "turn"},
        {"permissions": requested, "scope": "session"},
        {"permissions": {}, "scope": "turn"},
    ]
    assert approval_response_payload(
        "item/permissions/requestApproval",
        {"permissions": requested, "scope": "turn", "strictAutoReview": True},
    ) == {"permissions": requested, "scope": "turn", "strictAutoReview": True}
    assert DiscussionBotController._command_approval_button_label(  # noqa: SLF001
        {"permissions": {}, "scope": "turn"}
    ) == "拒绝权限"
    with pytest.raises(ValueError, match="Session"):
        approval_response_payload(
            "item/permissions/requestApproval",
            {"permissions": requested, "scope": "session", "strictAutoReview": True},
        )

    assert approval_response_payload("applyPatchApproval", "accept") == {
        "decision": "approved"
    }


def test_callback_workload_spaces_isolate_file_prompt_and_maintenance_actions() -> None:
    assert _callback_workload_space("send_file") == FILE_IO_SPACE
    assert _callback_workload_space("plan_execute") == PROMPT_ACTION_SPACE
    assert _callback_workload_space("space_refresh") == MAINTENANCE_SPACE
    assert _callback_workload_space("unwatch_cancel") == "default"


def test_controllers_register_named_workload_spaces(rig: Rig) -> None:
    assert set(rig.control._workloads.snapshot()["spaces"]) == {  # noqa: SLF001
        "default",
        "prompt_action",
        "maintenance",
    }
    assert set(rig.discussion._workloads.snapshot()["spaces"]) == {  # noqa: SLF001
        "default",
        "file_io",
        "prompt_action",
        "maintenance",
    }


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
async def test_question_custom_answer_can_be_sent_directly_in_topic(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-custom-direct",
        thread_id="thread-custom-direct",
        root_message_id=529,
        channel_post_id=129,
    )
    questions = [
        {
            "id": "delivery",
            "question": "How should this be delivered?",
            "options": [{"label": "Queue"}],
        }
    ]
    rig.store.put_pending_input(
        "request-custom-direct",
        "7",
        1,
        "thread-custom-direct",
        "turn-1",
        "item-1",
        questions,
        None,
    )
    rig.security.unlocked.add("space-custom-direct")
    await rig.discussion._begin_question_reply(
        space,
        {"request_key": "request-custom-direct", "question_id": "delivery"},
        clarification=False,
    )

    direct = update_for_message(
        "Ship it through the queue.",
        update_id=150,
        message_id=70,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=529,
    )
    with pytest.raises(ApplicationHandlerStop):
        await rig.discussion.reply_to_intent(direct, SimpleNamespace())

    assert rig.bridge.answers == [
        ("request-custom-direct", {"delivery": ["Ship it through the queue."]})
    ]


@pytest.mark.asyncio
async def test_multiple_direct_question_targets_require_exact_force_reply(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-custom-ambiguous",
        thread_id="thread-custom-ambiguous",
        root_message_id=530,
        channel_post_id=130,
    )
    rig.security.unlocked.add("space-custom-ambiguous")
    prompt_ids: list[int] = []
    for index in (1, 2):
        request_key = f"request-custom-{index}"
        questions = [
            {
                "id": "choice",
                "question": f"Choose delivery {index}",
                "options": [{"label": "Queue"}],
            }
        ]
        rig.store.put_pending_input(
            request_key,
            str(7 + index),
            1,
            "thread-custom-ambiguous",
            f"turn-{index}",
            f"item-{index}",
            questions,
            None,
        )
        await rig.discussion._begin_question_reply(
            space,
            {"request_key": request_key, "question_id": "choice"},
            clarification=False,
        )
        prompt_ids.append(rig.discussion_endpoint.next_message_id - 1)

    ambiguous = update_for_message(
        "Queue this one.",
        update_id=151,
        message_id=71,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=530,
    )
    with pytest.raises(ApplicationHandlerStop):
        await rig.discussion.reply_to_intent(ambiguous, SimpleNamespace())
    assert rig.bridge.answers == []
    assert "多个问题" in rig.discussion_endpoint.sent[-1]["markdown"]

    exact = update_for_message(
        "Queue the first request.",
        update_id=152,
        message_id=72,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=530,
        reply_to_message=SimpleNamespace(
            message_id=prompt_ids[0],
            message_thread_id=530,
            is_automatic_forward=False,
        ),
    )
    with pytest.raises(ApplicationHandlerStop):
        await rig.discussion.reply_to_intent(exact, SimpleNamespace())
    assert rig.bridge.answers == [
        ("request-custom-1", {"choice": ["Queue the first request."]})
    ]


@pytest.mark.asyncio
async def test_known_commands_keep_precedence_but_unknown_slash_can_answer(rig: Rig) -> None:
    space = add_active_space(
        rig,
        space_id="space-custom-command",
        thread_id="thread-custom-command",
        root_message_id=531,
        channel_post_id=131,
    )
    questions = [
        {
            "id": "choice",
            "question": "Choose a value",
            "options": [{"label": "Queue"}],
        }
    ]
    rig.store.put_pending_input(
        "request-custom-command",
        "10",
        1,
        "thread-custom-command",
        "turn-1",
        "item-1",
        questions,
        None,
    )
    rig.security.unlocked.add("space-custom-command")
    await rig.discussion._begin_question_reply(
        space,
        {"request_key": "request-custom-command", "question_id": "choice"},
        clarification=False,
    )

    known = update_for_message(
        "/status",
        update_id=153,
        message_id=73,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=531,
    )
    await rig.discussion.reply_to_intent(known, SimpleNamespace())
    assert rig.bridge.answers == []
    assert len(
        rig.store.live_question_reply_callbacks(
            OWNER_ID,
            bot_role=DISCUSSION_ROLE,
            chat_id=DISCUSSION_CHAT_ID,
            space_id="space-custom-command",
            generation=1,
        )
    ) == 1

    unknown = update_for_message(
        "/use-queue",
        update_id=154,
        message_id=74,
        chat_id=DISCUSSION_CHAT_ID,
        chat_type=ChatType.SUPERGROUP,
        message_thread_id=531,
    )
    with pytest.raises(ApplicationHandlerStop):
        await rig.discussion.reply_to_intent(unknown, SimpleNamespace())
    assert rig.bridge.answers == [
        ("request-custom-command", {"choice": ["/use-queue"]})
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
    assert f"<code>{ask_id}</code>" in edited["markdown"]
    assert "Use <i>Queue</i> for isolation." in edited["markdown"]
    assert edited["parse_mode"] == ParseMode.HTML
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
    assert f"<code>{waiting_ids[1]}</code>" in rig.discussion_endpoint.edited[0]["markdown"]
    first_call = rig.bridge.ask_calls[0]
    rig.bridge.ask_waiters[first_call["client_message_id"]].set_result("first answer")
    await asyncio.gather(*tasks)

    by_message = {item["message_id"]: item["markdown"] for item in rig.discussion_endpoint.edited}
    assert f"<code>{waiting_ids[0]}</code>" in by_message[2000]
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
