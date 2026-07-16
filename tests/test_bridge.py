from __future__ import annotations

import asyncio
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

import codex_telegram_bridge.bridge as bridge_module
from codex_telegram_bridge.bridge import Bridge
from codex_telegram_bridge.codex import CodexRpcError
from codex_telegram_bridge.config import Config
from codex_telegram_bridge.models import SessionSpace, ThreadState
from codex_telegram_bridge.store import Store


class Messenger:
    def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


def make_bridge(tmp_path: Path) -> tuple[Bridge, Store]:
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
    return Bridge(config, store, object(), Messenger()), store  # type: ignore[arg-type]


async def cancel_queue_retries(bridge: Bridge) -> None:
    bridge._started = False
    tasks = list(bridge._queue_retry_tasks.values())
    bridge._queue_retry_tasks.clear()
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_start_protects_files_referenced_by_queued_prompts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    upload = tmp_path / "queued.txt"
    upload.write_text("queued", encoding="utf-8")
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
    store.enqueue_prompt(
        "thread-1",
        "use file",
        [{"type": "mention", "name": upload.name, "path": str(upload)}],
        "client-1",
    )
    bridge = Bridge(config, store, object(), Messenger())  # type: ignore[arg-type]
    protected: set[Path] = set()

    def cleanup(_inbox: Path, _days: int, *, protected_paths: set[Path]) -> int:
        protected.update(protected_paths)
        return 0

    async def no_op(*_args: Any, **_kwargs: Any) -> None:
        pass

    monkeypatch.setattr(bridge_module, "cleanup_inbox", cleanup)
    monkeypatch.setattr(bridge.directory_index, "refresh", no_op)
    monkeypatch.setattr(bridge.client, "start", lambda: None)
    monkeypatch.setattr(bridge.client, "wait_connected", no_op)
    monkeypatch.setattr(bridge.metrics, "start", lambda: None)
    monkeypatch.setattr(bridge.dashboard, "start", lambda: None)

    await bridge.start()

    assert protected == {upload.resolve()}
    await bridge.stop()
    store.close()


@pytest.mark.asyncio
async def test_ambiguous_queue_delivery_requires_reconciliation_before_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    thread_id = "thread-ambiguous"
    store.save_thread(ThreadState(thread_id=thread_id, cwd=str(tmp_path), status="idle", queue_count=1))
    store.enqueue_prompt(thread_id, "first", [{"type": "text", "text": "first"}], "client-1")
    calls: list[str] = []
    read_attempt = 0

    async def read_thread(_thread_id: str, *, include_turns: bool) -> dict[str, Any]:
        nonlocal read_attempt
        assert include_turns
        read_attempt += 1
        calls.append(f"read-{read_attempt}")
        if read_attempt == 1:
            return {"turns": []}
        raise RuntimeError("connection lost while reconciling")

    async def start_turn(
        _thread_id: str,
        _inputs: list[dict[str, Any]],
        *,
        client_message_id: str,
        **kwargs: Any,
    ) -> None:
        assert kwargs["cwd"] == tmp_path
        assert kwargs["sandbox_policy"]["type"] == "workspaceWrite"
        assert kwargs["approval_policy"] == "never"
        calls.append(f"start-{client_message_id}")
        raise TimeoutError("turn/start result is unknown")

    monkeypatch.setattr(bridge.client, "read_thread", read_thread)
    monkeypatch.setattr(bridge.client, "start_turn", start_turn)
    bridge._started = True
    try:
        await bridge.dispatch_queue(thread_id)
        retry_task = bridge._queue_retry_tasks[thread_id]
        assert calls == ["read-1", "start-client-1", "read-2"]
        assert store.queue_count(thread_id) == 1

        await bridge.dispatch_queue(thread_id)

        assert calls == ["read-1", "start-client-1", "read-2", "read-3"]
        assert bridge._queue_retry_tasks[thread_id] is retry_task
        assert store.queue_count(thread_id) == 1
    finally:
        await cancel_queue_retries(bridge)
        store.close()


@pytest.mark.asyncio
async def test_queue_retry_is_unique_per_session_and_stop_cancels_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)

    async def no_op() -> None:
        pass

    monkeypatch.setattr(bridge.dashboard, "stop", no_op)
    monkeypatch.setattr(bridge.metrics, "stop", no_op)
    monkeypatch.setattr(bridge.client, "stop", no_op)
    bridge._started = True
    bridge._schedule_queue_retry("thread-1", delay=3600)
    retry_task = bridge._queue_retry_tasks["thread-1"]
    bridge._schedule_queue_retry("thread-1", delay=3600)

    assert bridge._queue_retry_tasks["thread-1"] is retry_task
    assert not retry_task.done()

    await bridge.stop()

    assert retry_task.cancelled()
    assert bridge._queue_retry_tasks == {}
    store.close()


@pytest.mark.asyncio
async def test_delivered_queue_entry_updates_count_and_continues_fifo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    thread_id = "thread-fifo"
    store.enqueue_prompt(thread_id, "first", [{"type": "text", "text": "first"}], "client-1")
    store.enqueue_prompt(thread_id, "second", [{"type": "text", "text": "second"}], "client-2")
    store.save_thread(ThreadState(thread_id=thread_id, cwd=str(tmp_path), status="idle", queue_count=2))
    read_attempt = 0
    started: list[str] = []
    dashboard_counts: list[int] = []

    async def read_thread(_thread_id: str, *, include_turns: bool) -> dict[str, Any]:
        nonlocal read_attempt
        assert include_turns
        read_attempt += 1
        if read_attempt == 1:
            return {
                "turns": [
                    {"items": [{"type": "userMessage", "clientId": "client-1"}]},
                ]
            }
        return {"turns": []}

    async def start_turn(
        _thread_id: str,
        _inputs: list[dict[str, Any]],
        *,
        client_message_id: str,
        **kwargs: Any,
    ) -> None:
        assert kwargs["cwd"] == tmp_path
        assert kwargs["sandbox_policy"]["networkAccess"] is False
        started.append(client_message_id)

    async def schedule(state: ThreadState, *, immediate: bool = False) -> None:
        del immediate
        dashboard_counts.append(state.queue_count)

    original_schedule_retry = bridge._schedule_queue_retry

    def schedule_retry_now(_thread_id: str, *, delay: float = 5.0) -> None:
        del delay
        original_schedule_retry(_thread_id, delay=0)

    monkeypatch.setattr(bridge.client, "read_thread", read_thread)
    monkeypatch.setattr(bridge.client, "start_turn", start_turn)
    monkeypatch.setattr(bridge.dashboard, "schedule", schedule)
    monkeypatch.setattr(bridge, "_schedule_queue_retry", schedule_retry_now)
    bridge._started = True
    try:
        await bridge.dispatch_queue(thread_id)
        for _ in range(100):
            if store.queue_count(thread_id) == 0 and not bridge._queue_retry_tasks:
                break
            await asyncio.sleep(0)

        assert started == ["client-2"]
        assert dashboard_counts == [1, 0]
        assert store.queue_count(thread_id) == 0
        assert store.get_thread(thread_id).queue_count == 0  # type: ignore[union-attr]
        assert store.next_prompt(thread_id) is None
    finally:
        await cancel_queue_retries(bridge)
        store.close()


@pytest.mark.asyncio
async def test_queue_on_idle_session_dispatches_without_waiting_for_future_event(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    thread_id = "thread-idle"
    dispatched = asyncio.Event()

    async def resume_thread(_thread_id: str) -> dict[str, Any]:
        return {
            "id": thread_id,
            "cwd": str(tmp_path),
            "status": {"type": "idle"},
        }

    async def schedule(_state: ThreadState, *, immediate: bool = False) -> None:
        del immediate

    async def dispatch(_thread_id: str) -> None:
        assert _thread_id == thread_id
        dispatched.set()

    monkeypatch.setattr(bridge.client, "resume_thread", resume_thread)
    monkeypatch.setattr(bridge.dashboard, "schedule", schedule)
    monkeypatch.setattr(bridge, "dispatch_queue", dispatch)
    bridge._started = True
    try:
        result = await bridge.send_prompt(thread_id, "queued", mode="queue")
        await asyncio.wait_for(dispatched.wait(), timeout=1)

        assert result == "queued"
        assert store.queue_count(thread_id) == 1
    finally:
        await cancel_queue_retries(bridge)
        store.close()


@pytest.mark.asyncio
async def test_permanent_queue_rpc_error_is_failed_instead_of_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    thread_id = "thread-rejected"
    store.enqueue_prompt(thread_id, "bad", [{"type": "text", "text": "bad"}], "client-bad")
    store.save_thread(ThreadState(thread_id=thread_id, cwd=str(tmp_path), status="idle", queue_count=1))
    notices: list[tuple[str, str | None]] = []

    async def read_thread(_thread_id: str, *, include_turns: bool) -> dict[str, Any]:
        assert include_turns
        return {"turns": []}

    async def start_turn(*_args: Any, **_kwargs: Any) -> None:
        raise CodexRpcError("turn/start", {"message": "invalid input"})

    async def schedule(_state: ThreadState, *, immediate: bool = False) -> None:
        del immediate

    async def notice(message: str, target: str | None) -> None:
        notices.append((message, target))

    monkeypatch.setattr(bridge.client, "read_thread", read_thread)
    monkeypatch.setattr(bridge.client, "start_turn", start_turn)
    monkeypatch.setattr(bridge.dashboard, "schedule", schedule)
    bridge.on_notice = notice
    bridge._started = True
    try:
        await bridge.dispatch_queue(thread_id)

        assert store.queue_count(thread_id) == 0
        assert store.get_thread(thread_id).last_error == "Queued prompt rejected (CodexRpcError)"  # type: ignore[union-attr]
        assert notices and notices[0][1] == thread_id
        assert bridge._queue_retry_tasks == {}
    finally:
        await cancel_queue_retries(bridge)
        store.close()


@pytest.mark.asyncio
async def test_activate_pending_session_does_not_create_legacy_subscription(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_session_space(
        SessionSpace(
            space_id="space-pending",
            space_type="pending",
            pending_cwd=str(tmp_path),
            pending_prompt="Build the requested feature",
        )
    )
    turns: list[tuple[str, str]] = []
    changes: list[tuple[str, str]] = []

    async def start_thread(cwd: Path) -> dict[str, Any]:
        assert cwd == tmp_path
        return {
            "id": "thread-new",
            "cwd": str(cwd),
            "createdAt": 100,
            "updatedAt": 100,
            "status": {"type": "idle"},
        }

    async def ensure_window(thread_id: str, title: str, cwd: Path) -> str:
        assert (thread_id, title, cwd) == (
            "thread-new",
            "Build the requested feature",
            tmp_path,
        )
        return "codex-thread-new"

    async def start_turn(
        thread_id: str,
        inputs: list[dict[str, Any]],
        *,
        client_message_id: str,
        **kwargs: Any,
    ) -> None:
        assert inputs[0]["text"] == "Build the requested feature"
        assert kwargs["cwd"] == tmp_path
        turns.append((thread_id, client_message_id))

    async def state_changed(state: ThreadState, reason: str) -> None:
        changes.append((state.thread_id, reason))

    monkeypatch.setattr(bridge.client, "start_thread", start_thread)
    monkeypatch.setattr(bridge.client, "start_turn", start_turn)
    monkeypatch.setattr(bridge.tmux, "ensure_window", ensure_window)
    bridge.on_state_change = state_changed

    state = await bridge.activate_pending_session(
        "space-pending", client_message_id="telegram-initial"
    )

    space = store.get_session_space("space-pending")
    assert state.thread_id == "thread-new"
    assert turns == [("thread-new", "telegram-initial")]
    assert changes == [("thread-new", "session/activated")]
    assert store.subscriptions() == {}
    assert space is not None
    assert (space.lifecycle, space.thread_id) == ("active", "thread-new")
    assert (space.pending_cwd, space.pending_prompt) == ("", "")
    store.close()


@pytest.mark.asyncio
async def test_space_prompt_queue_is_scoped_to_current_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_session_space(
        SessionSpace(
            space_id="space-active",
            generation=3,
            lifecycle="active",
            thread_id="thread-space",
        )
    )

    async def resume_thread(_thread_id: str) -> dict[str, Any]:
        return {
            "id": "thread-space",
            "cwd": str(tmp_path),
            "status": {"type": "idle"},
        }

    async def schedule(_state: ThreadState, *, immediate: bool = False) -> None:
        del immediate

    monkeypatch.setattr(bridge.client, "resume_thread", resume_thread)
    monkeypatch.setattr(bridge.dashboard, "schedule", schedule)

    assert await bridge.send_space_prompt(
        "space-active", "queued", mode="queue", client_message_id="space-client"
    ) == "queued"
    entries = store.space_queue_entries("space-active", 3)
    assert [(entry["prompt"], entry["generation"]) for entry in entries] == [("queued", 3)]

    space = store.get_session_space("space-active")
    assert space is not None
    space.generation = 4
    store.save_session_space(space)
    with pytest.raises(RuntimeError, match="generation is stale"):
        await bridge.send_prompt(
            "thread-space",
            "stale",
            mode="queue",
            space_id="space-active",
            generation=3,
        )
    store.close()


@pytest.mark.asyncio
async def test_resolved_server_request_notifies_before_pending_input_is_deleted(tmp_path: Path) -> None:
    bridge, store = make_bridge(tmp_path)
    bridge._pending_requests["request-key"] = (42, 1)
    store.put_pending_input(
        "request-key", "42", 1, "thread-1", "turn-1", "item-1", [], None
    )
    existed_during_hook: list[bool] = []

    async def resolved(request_key: str) -> None:
        existed_during_hook.append(store.get_pending_input(request_key) is not None)

    bridge.on_question_resolved = resolved

    await bridge._on_notification("serverRequest/resolved", {"requestId": 42})

    assert existed_during_hook == [True]
    assert store.get_pending_input("request-key") is None
    assert "request-key" not in bridge._pending_requests
    store.close()


@pytest.mark.asyncio
async def test_resolved_notification_tombstones_request_before_async_registration(
    tmp_path: Path,
) -> None:
    bridge, store = make_bridge(tmp_path)
    forwarded: list[str] = []

    async def forward(request_key: str, _params: dict[str, Any]) -> None:
        forwarded.append(request_key)

    bridge.on_question = forward

    await bridge._on_notification("serverRequest/resolved", {"requestId": 42})
    await bridge._on_server_request(
        42,
        "item/tool/requestUserInput",
        {
            "threadId": "thread-1",
            "turnId": "turn-1",
            "itemId": "item-1",
            "questions": [{"id": "answer", "question": "Continue?"}],
        },
        bridge.client.generation,
    )

    assert forwarded == []
    assert bridge._pending_requests == {}
    assert store.pending_input_keys_for_request(42) == []
    store.close()


@pytest.mark.asyncio
async def test_resync_uses_active_space_as_subscription_without_legacy_dashboard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.create_space(
        {
            "space_id": "space-active",
            "space_type": "existing",
            "lifecycle": "active",
            "thread_id": "thread-space",
        }
    )
    resumed: list[str] = []
    dashboards: list[str] = []
    space_updates: list[tuple[str, str]] = []

    async def list_threads(*, limit: int, **_kwargs: Any) -> list[dict[str, Any]]:
        assert limit == 200
        return []

    async def resume_thread(thread_id: str) -> dict[str, Any]:
        resumed.append(thread_id)
        return {
            "id": thread_id,
            "cwd": str(tmp_path),
            "status": {"type": "active"},
        }

    async def get_goal(_thread_id: str) -> None:
        return None

    async def schedule(state: ThreadState, *, immediate: bool = False) -> None:
        assert immediate
        dashboards.append(state.thread_id)

    async def space_update(state: ThreadState, reason: str) -> None:
        space_updates.append((state.thread_id, reason))

    monkeypatch.setattr(bridge.client, "list_threads", list_threads)
    monkeypatch.setattr(bridge.client, "resume_thread", resume_thread)
    monkeypatch.setattr(bridge.client, "get_goal", get_goal)
    monkeypatch.setattr(bridge.dashboard, "schedule", schedule)
    bridge.on_state_change = space_update

    await bridge.resync()

    assert resumed == ["thread-space"]
    assert dashboards == []
    assert space_updates == [("thread-space", "thread/resynced")]
    state = store.get_thread("thread-space")
    assert state is not None and state.subscribed
    assert store.subscriptions() == {}
    store.close()


@pytest.mark.asyncio
async def test_closing_spaces_unsubscribes_only_after_last_space_closes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_thread(
        ThreadState(
            thread_id="thread-shared",
            cwd=str(tmp_path),
            status="active",
            turn_id="turn-running",
            turn_status="inProgress",
            subscribed=True,
        )
    )
    for space_id in ("space-one", "space-two"):
        store.create_space(
            {
                "space_id": space_id,
                "space_type": "existing",
                "lifecycle": "active",
                "thread_id": "thread-shared",
            }
        )
    unsubscribed: list[str] = []

    async def request(method: str, params: dict[str, Any]) -> None:
        assert method == "thread/unsubscribe"
        unsubscribed.append(str(params["threadId"]))

    monkeypatch.setattr(bridge.client, "request", request)

    await bridge.close_session_space("space-one", 1)
    assert unsubscribed == []
    assert store.get_thread("thread-shared").subscribed  # type: ignore[union-attr]

    await bridge.close_session_space("space-two", 1)
    assert unsubscribed == ["thread-shared"]
    state = store.get_thread("thread-shared")
    assert state is not None
    assert not state.subscribed
    assert (state.status, state.turn_status, state.turn_id) == (
        "active",
        "inProgress",
        "turn-running",
    )
    store.close()


@pytest.mark.asyncio
async def test_legacy_unwatch_keeps_subscription_needed_by_space(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_thread(ThreadState(thread_id="thread-space", subscribed=True))
    store.subscribe("thread-space")
    store.create_space(
        {
            "space_id": "space-active",
            "space_type": "existing",
            "lifecycle": "active",
            "thread_id": "thread-space",
        }
    )
    calls: list[str] = []

    async def request(method: str, _params: dict[str, Any]) -> None:
        calls.append(method)

    monkeypatch.setattr(bridge.client, "request", request)

    await bridge.unwatch("thread-space")

    assert calls == []
    assert store.get_thread("thread-space").subscribed  # type: ignore[union-attr]
    assert store.subscriptions() == {}
    store.close()


@pytest.mark.asyncio
async def test_close_session_space_cancels_queue_without_interrupting_active_turn(tmp_path: Path) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_thread(
        ThreadState(
            thread_id="thread-running",
            cwd=str(tmp_path),
            status="active",
            turn_id="turn-running",
            turn_status="inProgress",
        )
    )
    store.save_session_space(
        SessionSpace(
            space_id="space-running",
            lifecycle="active",
            thread_id="thread-running",
        )
    )
    store.enqueue_prompt(
        "thread-running",
        "later",
        [{"type": "text", "text": "later"}],
        "client-later",
        space_id="space-running",
        generation=1,
    )

    closed = await bridge.close_session_space("space-running", 1)

    state = store.get_thread("thread-running")
    assert (closed.lifecycle, closed.generation) == ("closed", 2)
    assert store.space_queue_entries("space-running", 1) == []
    assert state is not None
    assert (state.status, state.turn_status, state.turn_id) == (
        "active",
        "inProgress",
        "turn-running",
    )
    store.close()


@pytest.mark.asyncio
async def test_ask_space_question_uses_isolated_client_without_mutating_primary_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_thread(
        ThreadState(
            thread_id="thread-primary",
            cwd=str(tmp_path),
            status="active",
            turn_id="turn-primary",
            turn_status="inProgress",
        )
    )
    store.save_session_space(
        SessionSpace(
            space_id="space-primary",
            lifecycle="active",
            thread_id="thread-primary",
        )
    )
    bridge.config = replace(
        bridge.config,
        ask_model="gpt-5.6-luna",
        ask_reasoning_effort="max",
    )
    calls: list[tuple[str, Path, str, str, str | None, str | None]] = []

    async def ask_fork_question(
        thread_id: str,
        cwd: Path,
        question: str,
        *,
        client_message_id: str,
        model: str | None,
        effort: str | None,
    ) -> str:
        calls.append((thread_id, cwd, question, client_message_id, model, effort))
        return "isolated answer"

    monkeypatch.setattr(bridge.client, "ask_fork_question", ask_fork_question)

    answer = await bridge.ask_space_question(
        "space-primary", "  What does this error mean?  ", client_message_id="tg-ask-1"
    )

    state = store.get_thread("thread-primary")
    assert answer == "isolated answer"
    assert calls == [
        (
            "thread-primary",
            tmp_path.resolve(),
            "What does this error mean?",
            "tg-ask-1",
            "gpt-5.6-luna",
            "max",
        )
    ]
    assert state is not None
    assert (state.turn_id, state.turn_status, state.status) == (
        "turn-primary",
        "inProgress",
        "active",
    )
    assert store.space_queue_entries("space-primary", 1) == []
    store.close()


@pytest.mark.asyncio
async def test_ask_space_question_rejects_empty_or_closed_space(tmp_path: Path) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_session_space(
        SessionSpace(space_id="space-closed", lifecycle="closed", thread_id="thread-primary")
    )

    with pytest.raises(ValueError, match="cannot be empty"):
        await bridge.ask_space_question("space-closed", " ", client_message_id="tg-empty")
    with pytest.raises(RuntimeError, match="not active"):
        await bridge.ask_space_question("space-closed", "question", client_message_id="tg-closed")
    store.close()


@pytest.mark.asyncio
async def test_side_fork_events_and_input_requests_never_reach_bridge_hooks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    projected: list[str] = []
    questions: list[str] = []
    rejected: list[tuple[int | str, int]] = []

    async def ingest(method: str, params: dict[str, Any]) -> None:
        del params
        projected.append(method)

    async def question(request_key: str, params: dict[str, Any]) -> None:
        del params
        questions.append(request_key)

    async def respond_error(request_id: int | str, code: int, message: str) -> None:
        del message
        rejected.append((request_id, code))

    monkeypatch.setattr(bridge.projector, "ingest", ingest)
    monkeypatch.setattr(bridge.client, "respond_error", respond_error)
    bridge.on_question = question
    bridge.client._ephemeral_thread_ids.add("side-fork")

    await bridge.client._dispatch_notification(
        "item/completed",
        {
            "threadId": "side-fork",
            "turnId": "side-turn",
            "item": {"id": "answer", "type": "agentMessage", "text": "private answer"},
        },
    )
    await bridge.client._dispatch_notification(
        "turn/completed",
        {
            "threadId": "side-fork",
            "turn": {"id": "side-turn", "status": "completed", "items": []},
        },
    )
    await bridge.client._dispatch_server_request(
        99,
        "item/tool/requestUserInput",
        {"threadId": "side-fork", "turnId": "side-turn", "questions": []},
        1,
    )

    assert projected == []
    assert questions == []
    assert rejected == [(99, -32600)]
    assert store.get_thread("side-fork") is None
    store.close()
