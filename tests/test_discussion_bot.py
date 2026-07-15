from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from telegram import Chat, Message, Update, User
from telegram.constants import ChatMemberStatus, ChatType
from telegram.ext import CommandHandler, MessageHandler

import codex_telegram_bridge.discussion_bot as discussion_bot_module
from codex_telegram_bridge.bridge import Bridge
from codex_telegram_bridge.config import Config
from codex_telegram_bridge.deletions import MessageDeletionManager
from codex_telegram_bridge.discussion_bot import DiscussionBotController
from codex_telegram_bridge.models import Owner, ThreadState
from codex_telegram_bridge.store import Store
from codex_telegram_bridge.telegram_common import (
    CONTROL_ROLE,
    DISCUSSION_ROLE,
    build_application,
)


class Messenger:
    def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


class DeletingEndpoint:
    def __init__(self) -> None:
        self.deleted: list[tuple[int, int]] = []

    async def delete_message(self, chat_id: int, message_id: int) -> bool:
        self.deleted.append((chat_id, message_id))
        return True


def make_runtime(
    tmp_path: Path,
) -> tuple[Bridge, Store, DiscussionBotController, DeletingEndpoint, DeletingEndpoint]:
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        codex_home=tmp_path / ".codex",
        codex_socket=tmp_path / ".codex" / "control.sock",
        codex_binary=tmp_path / "codex",
        allowed_root=tmp_path,
    )
    store = Store(config.database_path)
    bridge = Bridge(config, store, object(), Messenger())  # type: ignore[arg-type]
    control = DeletingEndpoint()
    discussion = DeletingEndpoint()
    deletions = MessageDeletionManager(
        store,
        {CONTROL_ROLE: control, DISCUSSION_ROLE: discussion},  # type: ignore[dict-item]
    )
    controller = DiscussionBotController(
        config,
        store,
        object(),  # type: ignore[arg-type]
        bridge,
        object(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        deletions,
    )
    bridge.on_question_resolved = controller.question_resolved
    return bridge, store, controller, control, discussion


def put_pending(store: Store, request_key: str, request_id: str = "42") -> None:
    store.put_pending_input(
        request_key,
        request_id,
        1,
        "thread-1",
        "turn-1",
        "item-1",
        [{"id": "answer", "question": "Continue?"}],
        None,
    )


@pytest.mark.asyncio
async def test_bind_text_fallback_handles_command_without_telegram_entity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _bridge, store, controller, _control, _discussion = make_runtime(tmp_path)
    controller.discussion = SimpleNamespace(
        bot=SimpleNamespace(username="session_discussion_bot")
    )
    application = build_application("123456:TESTTOKEN")
    controller.install(application)

    bind_handler = next(
        handler
        for handler in application.handlers[0]
        if isinstance(handler, CommandHandler) and "bind" in handler.commands
    )
    fallback = next(
        handler
        for handler in application.handlers[0]
        if isinstance(handler, MessageHandler)
        and handler.callback == controller._bind_text_fallback
    )
    assert application.handlers[0].index(bind_handler) < application.handlers[0].index(fallback)
    message = Message(
        message_id=8,
        date=datetime.now(UTC),
        chat=Chat(-1001, ChatType.SUPERGROUP, title="Example Discussion"),
        from_user=User(7, "owner", False),
        text="/bind@session_discussion_bot ABC123",
        entities=[],
    )
    update = Update(99, message=message)

    assert bind_handler.check_update(update) is None
    assert fallback.check_update(update)
    calls: list[tuple[Update, Any]] = []

    async def bind(candidate: Update, context: Any) -> None:
        calls.append((candidate, context))

    monkeypatch.setattr(controller, "bind", bind)
    context = SimpleNamespace()
    await fallback.callback(update, context)
    assert calls == [(update, context)]

    other_update = Update(
        100,
        message=Message(
            message_id=9,
            date=datetime.now(UTC),
            chat=Chat(-1001, ChatType.SUPERGROUP, title="Example Discussion"),
            from_user=User(7, "owner", False),
            text="/bind@another_bot ABC123",
            entities=[],
        ),
    )
    await fallback.callback(other_update, context)
    assert calls == [(update, context)]
    store.close()


@pytest.mark.asyncio
async def test_unbound_bind_runtime_error_is_reported_to_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _bridge, store, controller, _control, _discussion = make_runtime(tmp_path)
    store.set_owner(Owner(user_id=7, chat_id=7, username="owner"))
    sent: list[tuple[int, str, str | None]] = []

    async def send_unscoped(
        chat_id: int, markdown: str, *, plain: str | None = None
    ) -> SimpleNamespace:
        sent.append((chat_id, markdown, plain))
        return SimpleNamespace(message_id=1)

    monkeypatch.setattr(controller, "_send_unscoped", send_unscoped)
    monkeypatch.setattr(discussion_bot_module, "Update", SimpleNamespace)
    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=-1001),
        effective_user=SimpleNamespace(id=7),
        effective_message=SimpleNamespace(
            text="/bind ABC123",
            caption=None,
            message_thread_id=None,
            reply_to_message=None,
            chat_id=-1001,
            message_id=8,
        ),
    )

    await controller.error(
        update,
        SimpleNamespace(error=RuntimeError("426 Bot 缺少删除消息权限")),
    )

    assert sent == [
        (-1001, "绑定失败：426 Bot 缺少删除消息权限", "绑定失败：426 Bot 缺少删除消息权限")
    ]
    store.close()


@pytest.mark.asyncio
async def test_bind_permission_errors_use_custom_bot_labels(tmp_path: Path) -> None:
    _bridge, store, controller, _control, _discussion = make_runtime(tmp_path)
    controller.config = replace(
        controller.config,
        control_bot_label="频道_[Bot]",
        discussion_bot_label="评论*Bot",
    )
    group = SimpleNamespace(
        type=ChatType.SUPERGROUP,
        linked_chat_id=-1002,
        is_forum=False,
        username=None,
    )
    channel = SimpleNamespace(
        type=ChatType.CHANNEL,
        linked_chat_id=-1001,
        username=None,
    )
    control_member = SimpleNamespace(
        status=ChatMemberStatus.MEMBER,
        can_post_messages=True,
        can_edit_messages=True,
    )
    discussion_member = SimpleNamespace(
        status=ChatMemberStatus.MEMBER,
        can_delete_messages=True,
    )

    async def get_group(_chat_id: int) -> object:
        return group

    async def get_channel(_chat_id: int) -> object:
        return channel

    async def get_control_me() -> object:
        return SimpleNamespace(id=11)

    async def get_discussion_me() -> object:
        return SimpleNamespace(id=22)

    async def get_control_member(_chat_id: int, _user_id: int) -> object:
        return control_member

    async def get_discussion_member(_chat_id: int, _user_id: int) -> object:
        return discussion_member

    controller.control = SimpleNamespace(
        bot=SimpleNamespace(
            get_chat=get_channel,
            get_me=get_control_me,
            get_chat_member=get_control_member,
        )
    )
    controller.discussion = SimpleNamespace(
        bot=SimpleNamespace(
            get_chat=get_group,
            get_me=get_discussion_me,
            get_chat_member=get_discussion_member,
        )
    )
    update = SimpleNamespace(
        effective_chat=SimpleNamespace(id=-1001),
        effective_message=SimpleNamespace(text="/bind ABC123", caption=None),
    )

    with pytest.raises(RuntimeError) as control_error:
        await controller.bind(update, SimpleNamespace())
    assert str(control_error.value) == "频道_[Bot] 不是频道管理员"

    control_member.status = ChatMemberStatus.ADMINISTRATOR
    with pytest.raises(RuntimeError) as discussion_error:
        await controller.bind(update, SimpleNamespace())
    assert str(discussion_error.value) == "评论*Bot 不是讨论组管理员"
    store.close()


@pytest.mark.asyncio
async def test_stop_cancels_inflight_ask_tasks(tmp_path: Path) -> None:
    bridge, store, controller, _control, discussion = make_runtime(tmp_path)
    started = asyncio.Event()

    async def pending_ask(*_args: Any, **_kwargs: Any) -> str:
        started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    bridge.ask_space_question = pending_ask  # type: ignore[method-assign]
    task = asyncio.create_task(
        controller._complete_ask(
            {
                "space_id": "space-ask",
                "generation": 1,
                "discussion_chat_id": -1001,
            },
            "question",
            "ask-stop",
            77,
            client_message_id="telegram-ask-stop",
        )
    )
    controller._ask_tasks.add(task)
    await started.wait()

    await controller.stop()

    assert task.cancelled()
    assert controller._ask_tasks == set()
    assert discussion.deleted == [(-1001, 77)]
    assert store.due_message_deletions() == []
    store.close()


@pytest.mark.asyncio
async def test_stop_deletes_waiting_message_before_ask_task_first_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store, controller, _control, discussion = make_runtime(tmp_path)
    started = False

    async def pending_ask(*_args: Any, **_kwargs: Any) -> str:
        nonlocal started
        started = True
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    async def send_space(*_args: Any, **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(message_id=76)

    bridge.ask_space_question = pending_ask  # type: ignore[method-assign]
    monkeypatch.setattr(controller, "_send_space", send_space)
    await controller._launch_ask(
        {
            "space_id": "space-prestart",
            "generation": 1,
            "discussion_chat_id": -1001,
        },
        "question",
        clarification=False,
    )

    await controller.stop()

    assert not started
    assert discussion.deleted == [(-1001, 76)]
    assert controller._ask_waiting_messages == {}
    assert store.due_message_deletions() == []
    store.close()


@pytest.mark.asyncio
async def test_deletion_start_reconciles_questions_left_by_previous_runtime(tmp_path: Path) -> None:
    _bridge, store, _controller, _control, discussion = make_runtime(tmp_path)
    put_pending(store, "previous-generation")
    store.record_question_message("previous-generation", DISCUSSION_ROLE, -1001, 78)
    deletions = MessageDeletionManager(
        store,
        {DISCUSSION_ROLE: discussion},  # type: ignore[dict-item]
    )

    await deletions.start()

    assert discussion.deleted == [(-1001, 78)]
    assert store.get_pending_input("previous-generation") is None
    assert store.question_messages("previous-generation") == []
    assert store.due_message_deletions() == []

    put_pending(store, "current-generation")
    store.record_question_message("current-generation", DISCUSSION_ROLE, -1001, 79)
    await deletions.start()

    assert store.get_pending_input("current-generation") is not None
    assert [
        item["message_id"] for item in store.question_messages("current-generation")
    ] == [79]
    await deletions.stop()
    store.close()


@pytest.mark.asyncio
async def test_tmux_resolution_deletes_every_persisted_question_message(tmp_path: Path) -> None:
    bridge, store, controller, control, discussion = make_runtime(tmp_path)
    put_pending(store, "request-key")
    bridge._pending_requests["request-key"] = (42, 1)
    controller._question_answers["request-key"] = {"answer": ["yes"]}
    store.record_question_message("request-key", DISCUSSION_ROLE, -1001, 11)
    store.record_question_message("request-key", DISCUSSION_ROLE, -1001, 12)
    store.record_question_message("request-key", CONTROL_ROLE, 9527, 13)

    await bridge._on_notification("serverRequest/resolved", {"requestId": 42})

    assert discussion.deleted == [(-1001, 11), (-1001, 12)]
    assert control.deleted == [(9527, 13)]
    assert store.question_messages("request-key") == []
    assert store.get_pending_input("request-key") is None
    assert "request-key" not in controller._question_answers
    assert store.due_message_deletions() == []
    store.close()


@pytest.mark.asyncio
async def test_tmux_resolution_after_restart_uses_persisted_request_key(tmp_path: Path) -> None:
    bridge, store, _controller, _control, discussion = make_runtime(tmp_path)
    put_pending(store, "request-after-restart", json.dumps("rpc-string-id"))
    store.record_question_message(
        "request-after-restart", DISCUSSION_ROLE, -1001, 15
    )
    assert bridge._pending_requests == {}

    await bridge._on_notification(
        "serverRequest/resolved", {"requestId": "rpc-string-id"}
    )

    assert discussion.deleted == [(-1001, 15)]
    assert store.question_messages("request-after-restart") == []
    assert store.get_pending_input("request-after-restart") is None
    store.close()


@pytest.mark.asyncio
async def test_telegram_answer_uses_the_same_question_cleanup_hook(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store, _controller, _control, discussion = make_runtime(tmp_path)
    put_pending(store, "request-key", "77")
    bridge._pending_requests["request-key"] = (77, bridge.client.generation)
    store.record_question_message("request-key", DISCUSSION_ROLE, -1001, 21)
    responses: list[tuple[int | str, dict[str, Any]]] = []

    async def respond(request_id: int | str, result: dict[str, Any]) -> None:
        responses.append((request_id, result))

    monkeypatch.setattr(bridge.client, "respond", respond)

    await bridge.answer_question("request-key", {"answer": ["yes"]})

    assert responses == [(77, {"answers": {"answer": {"answers": ["yes"]}}})]
    assert discussion.deleted == [(-1001, 21)]
    assert store.question_messages("request-key") == []
    assert store.get_pending_input("request-key") is None
    store.close()


@pytest.mark.asyncio
async def test_question_header_is_removed_when_tmux_resolves_during_send(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _bridge, store, controller, _control, discussion = make_runtime(tmp_path)
    put_pending(store, "request-key")
    store.create_space(
        {
            "space_id": "space-1",
            "space_type": "existing",
            "lifecycle": "active",
            "thread_id": "thread-1",
            "channel_chat_id": -1002,
            "channel_post_id": 5,
            "discussion_chat_id": -1001,
            "discussion_root_id": 10,
        }
    )

    async def send_space(*_args: Any, **_kwargs: Any) -> SimpleNamespace:
        store.delete_pending_input("request-key")
        return SimpleNamespace(message_id=31)

    monkeypatch.setattr(controller, "_send_space", send_space)

    await controller.forward_question(
        "request-key",
        {
            "threadId": "thread-1",
            "questions": [{"id": "answer", "question": "Continue?"}],
        },
    )

    assert discussion.deleted == [(-1001, 31)]
    assert store.question_messages("request-key") == []
    store.close()


@pytest.mark.asyncio
async def test_question_sent_during_resolution_is_deleted_before_it_can_be_recorded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _bridge, store, controller, _control, discussion = make_runtime(tmp_path)
    put_pending(store, "request-key")
    space = {
        "space_id": "space-1",
        "thread_id": "thread-1",
        "discussion_chat_id": -1001,
        "discussion_root_id": 10,
    }

    async def send_space(*_args: Any, **_kwargs: Any) -> SimpleNamespace:
        await controller.question_resolved("request-key")
        return SimpleNamespace(message_id=32)

    monkeypatch.setattr(controller, "_send_space", send_space)

    await controller._present_question(space, "request-key", 0)

    assert store.get_pending_input("request-key") is not None
    assert discussion.deleted == [(-1001, 32)]
    assert store.question_messages("request-key") == []
    store.close()


@pytest.mark.asyncio
async def test_duplicate_resolution_notifications_do_not_delete_twice(tmp_path: Path) -> None:
    _bridge, store, controller, _control, discussion = make_runtime(tmp_path)
    store.record_question_message("request-key", DISCUSSION_ROLE, -1001, 41)

    await asyncio.gather(
        controller.question_resolved("request-key"),
        controller.question_resolved("request-key"),
    )

    assert discussion.deleted == [(-1001, 41)]
    assert store.question_messages("request-key") == []
    store.close()


@pytest.mark.asyncio
async def test_resolved_question_tombstones_and_locks_are_bounded(tmp_path: Path) -> None:
    _bridge, store, controller, _control, _discussion = make_runtime(tmp_path)

    for index in range(600):
        await controller.question_resolved(f"request-{index}")

    assert len(controller._resolved_questions) == 512
    assert "request-87" not in controller._resolved_questions
    assert "request-88" in controller._resolved_questions
    assert len(controller._question_locks) == 0
    store.close()


@pytest.mark.asyncio
async def test_totp_retries_repair_required_pending_activation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _bridge, store, controller, _control, _discussion = make_runtime(tmp_path)
    space = {
        "space_id": "space-repair",
        "space_type": "pending_new",
        "lifecycle": "repair_required",
        "thread_id": "thread-partial",
    }
    activations: list[str] = []

    class Security:
        def verify_for_space(self, space_id: str, code: str) -> bool:
            return (space_id, code) == ("space-repair", "123456")

    class Coordinator:
        async def activate_pending(self, space_id: str) -> ThreadState:
            activations.append(space_id)
            return ThreadState(thread_id="thread-partial")

    class Dashboards:
        async def schedule_space(self, space_id: str, *, immediate: bool = False) -> None:
            assert (space_id, immediate) == ("space-repair", True)

    async def send_space(*_args: Any, **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(message_id=99)

    monkeypatch.setattr(controller, "_require_space", lambda _update: space)
    monkeypatch.setattr(controller, "_send_space", send_space)
    controller.security = Security()  # type: ignore[assignment]
    controller.coordinator = Coordinator()  # type: ignore[assignment]
    controller.dashboards = Dashboards()  # type: ignore[assignment]
    controller.discussion = _discussion  # type: ignore[assignment]
    update = SimpleNamespace(
        effective_message=SimpleNamespace(
            chat_id=-1001,
            message_id=88,
            text="/totp 123456",
        )
    )

    await controller.totp(update, SimpleNamespace())  # type: ignore[arg-type]

    assert activations == ["space-repair"]
    store.close()
