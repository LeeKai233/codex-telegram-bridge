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
from codex_telegram_bridge.models import (
    ModelOption,
    ModelProfile,
    SessionSpace,
    TaskState,
    ThreadState,
)
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
    cleaned = asyncio.Event()
    loop = asyncio.get_running_loop()

    def cleanup(_inbox: Path, _days: int, *, protected_paths: set[Path]) -> int:
        protected.update(protected_paths)
        loop.call_soon_threadsafe(cleaned.set)
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
    await asyncio.wait_for(cleaned.wait(), timeout=1)

    assert protected == {upload.resolve()}
    await bridge.stop()
    store.close()


@pytest.mark.asyncio
async def test_start_does_not_wait_for_local_maintenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    release = asyncio.Event()
    loop = asyncio.get_running_loop()

    def blocked_cleanup() -> None:
        future = asyncio.run_coroutine_threadsafe(release.wait(), loop)
        future.result(timeout=2)

    async def no_op(*_args: Any, **_kwargs: Any) -> None:
        pass

    monkeypatch.setattr(bridge, "_cleanup_local_state", blocked_cleanup)
    monkeypatch.setattr(bridge.directory_index, "refresh", no_op)
    monkeypatch.setattr(bridge.client, "start", lambda: None)
    monkeypatch.setattr(bridge.client, "wait_connected", no_op)
    monkeypatch.setattr(bridge.metrics, "start", lambda: None)
    monkeypatch.setattr(bridge.dashboard, "start", lambda: None)

    await asyncio.wait_for(bridge.start(), timeout=0.2)
    release.set()
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
        assert kwargs["approval_policy"] == "on-request"
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
        payload = {
            "id": "thread-new",
            "cwd": str(cwd),
            "createdAt": 100,
            "updatedAt": 100,
            "status": {"type": "idle"},
        }
        await bridge._on_notification("thread/started", {"thread": payload})
        return payload

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
    assert changes == [
        ("thread-new", "thread/started"),
        ("thread-new", "session/activated"),
    ]
    assert store.subscriptions() == {}
    assert space is not None
    assert (space.lifecycle, space.thread_id) == ("active", "thread-new")
    assert (space.pending_cwd, space.pending_prompt) == ("", "")
    assert [event["kind"] for event in store.timeline("thread-new")] == ["thread/started"]
    store.close()


@pytest.mark.asyncio
async def test_model_profile_alias_and_effort_are_resolved_from_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)

    async def list_options() -> list[ModelOption]:
        return [
            ModelOption(
                model="gpt-5.6-luna",
                display_name="GPT-5.6 Luna",
                supported_efforts=("high", "max"),
                default_effort="high",
                is_default=True,
            )
        ]

    monkeypatch.setattr(bridge.client, "list_model_options", list_options)

    assert await bridge.resolve_model_profile("luna", "MAX") == ModelProfile(
        "gpt-5.6-luna", "max"
    )
    with pytest.raises(ValueError, match="unavailable for model"):
        await bridge.resolve_model_profile("luna", "xhigh")
    store.close()


@pytest.mark.asyncio
async def test_activate_pending_session_starts_with_explicit_plan_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_session_space(
        SessionSpace(
            space_id="space-profiled",
            space_type="pending_new",
            pending_cwd=str(tmp_path),
            pending_prompt="Plan first",
            normal_model="gpt-5.6-luna",
            normal_effort="max",
            plan_model="gpt-5.6-luna",
            plan_effort="low",
            current_mode="plan",
        )
    )
    started: list[dict[str, Any]] = []

    async def resolve_profile(model: str, effort: str) -> ModelProfile:
        return ModelProfile(model, effort)

    async def resolve_mode(
        mode: str, *, model: str | None = None, effort: str | None = None
    ) -> dict[str, Any]:
        return {
            "mode": mode,
            "settings": {"model": model, "reasoning_effort": effort},
        }

    async def start_thread(cwd: Path) -> dict[str, Any]:
        return {
            "id": "thread-profiled",
            "cwd": str(cwd),
            "status": {"type": "idle"},
        }

    async def start_turn(*_args: Any, **kwargs: Any) -> dict[str, Any]:
        started.append(kwargs)
        return {"id": "turn-plan"}

    async def ensure_window(*_args: Any) -> str:
        return "window"

    monkeypatch.setattr(bridge, "resolve_model_profile", resolve_profile)
    monkeypatch.setattr(bridge.client, "resolve_collaboration_mode", resolve_mode)
    monkeypatch.setattr(bridge.client, "start_thread", start_thread)
    monkeypatch.setattr(bridge.client, "start_turn", start_turn)
    monkeypatch.setattr(bridge.tmux, "ensure_window", ensure_window)

    await bridge.activate_pending_session(
        "space-profiled", client_message_id="telegram-profiled"
    )

    assert started[0]["collaboration_mode"] == {
        "mode": "plan",
        "settings": {"model": "gpt-5.6-luna", "reasoning_effort": "low"},
    }
    space = store.get_session_space("space-profiled")
    assert space is not None and (space.current_mode, space.lifecycle) == ("plan", "active")
    store.close()


@pytest.mark.asyncio
async def test_change_space_model_updates_current_mode_for_subsequent_turns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_session_space(
        SessionSpace(
            space_id="space-change",
            lifecycle="active",
            thread_id="thread-change",
            normal_model="old",
            normal_effort="high",
            plan_model="plan-old",
            plan_effort="low",
            current_mode="default",
        )
    )
    store.save_thread(ThreadState(thread_id="thread-change", cwd=str(tmp_path), status="active"))
    updates: list[tuple[str, dict[str, Any]]] = []

    async def resolve_profile(_model: str, _effort: str) -> ModelProfile:
        return ModelProfile("gpt-5.6-luna", "max")

    async def resolve_mode(
        mode: str, *, model: str | None = None, effort: str | None = None
    ) -> dict[str, Any]:
        return {
            "mode": mode,
            "settings": {"model": model, "reasoning_effort": effort},
        }

    async def update_settings(
        thread_id: str, *, collaboration_mode: dict[str, Any]
    ) -> None:
        updates.append((thread_id, collaboration_mode))

    monkeypatch.setattr(bridge, "resolve_model_profile", resolve_profile)
    monkeypatch.setattr(bridge.client, "resolve_collaboration_mode", resolve_mode)
    monkeypatch.setattr(bridge.client, "update_thread_settings", update_settings)

    changed = await bridge.change_space_model("space-change", "luna", "max")

    assert (changed.normal_model, changed.normal_effort) == ("gpt-5.6-luna", "max")
    assert (changed.plan_model, changed.plan_effort) == ("plan-old", "low")
    assert updates[0][0] == "thread-change"
    assert updates[0][1]["mode"] == "default"
    store.close()


@pytest.mark.asyncio
async def test_change_space_model_preserves_permission_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_session_space(
        SessionSpace(
            space_id="space-security",
            lifecycle="active",
            thread_id="thread-security",
            normal_model="old",
            normal_effort="high",
        )
    )
    store.save_thread(
        ThreadState(
            thread_id="thread-security",
            cwd=str(tmp_path),
            status="active",
            permissions="workspace-safe",
            approval_policy="on-request",
            approvals_reviewer="auto_review",
            sandbox_policy={"type": "workspaceWrite", "networkAccess": False},
        )
    )
    updates: list[dict[str, Any]] = []

    async def resolve_profile(_model: str, _effort: str) -> ModelProfile:
        return ModelProfile("gpt-5.6-luna", "max")

    async def resolve_mode(
        mode: str, *, model: str | None = None, effort: str | None = None
    ) -> dict[str, Any]:
        return {
            "mode": mode,
            "settings": {"model": model, "reasoning_effort": effort},
        }

    async def update_settings(_thread_id: str, **kwargs: Any) -> None:
        updates.append(kwargs)

    monkeypatch.setattr(bridge, "resolve_model_profile", resolve_profile)
    monkeypatch.setattr(bridge.client, "resolve_collaboration_mode", resolve_mode)
    monkeypatch.setattr(bridge.client, "update_thread_settings", update_settings)

    await bridge.change_space_model("space-security", "luna", "max")

    assert updates == [
        {
            "collaboration_mode": {
                "mode": "default",
                "settings": {
                    "model": "gpt-5.6-luna",
                    "reasoning_effort": "max",
                },
            },
            "permissions": "workspace-safe",
            "approval_policy": "on-request",
            "approvals_reviewer": "auto_review",
        }
    ]
    store.close()


@pytest.mark.asyncio
async def test_send_prompt_reuses_persisted_permission_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    thread_id = "thread-security-turn"
    store.save_thread(
        ThreadState(
            thread_id=thread_id,
            cwd=str(tmp_path),
            status="idle",
            permissions="workspace-safe",
            approval_policy="on-request",
            approvals_reviewer="user",
        )
    )
    captured: list[dict[str, Any]] = []

    async def resume_thread(_thread_id: str) -> dict[str, Any]:
        return {"id": thread_id, "cwd": str(tmp_path), "status": {"type": "idle"}}

    async def start_turn(*_args: Any, **kwargs: Any) -> dict[str, Any]:
        captured.append(kwargs)
        return {"id": "turn-security"}

    monkeypatch.setattr(bridge.client, "resume_thread", resume_thread)
    monkeypatch.setattr(bridge.client, "start_turn", start_turn)

    assert await bridge.send_prompt(thread_id, "use the inherited profile") == "started"

    assert captured == [
        {
            "client_message_id": captured[0]["client_message_id"],
            "cwd": tmp_path,
            "permissions": "workspace-safe",
            "approval_policy": "on-request",
            "approvals_reviewer": "user",
        }
    ]
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

    assert existed_during_hook == [False]
    assert store.get_pending_input("request-key") is None
    assert "request-key" not in bridge._pending_requests
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "decision", "wire_decision"),
    (
        ("item/commandExecution/requestApproval", "accept", "accept"),
        (
            "item/commandExecution/requestApproval",
            "acceptForSession",
            "acceptForSession",
        ),
        ("item/commandExecution/requestApproval", "decline", "decline"),
        ("execCommandApproval", "accept", "approved"),
        ("execCommandApproval", "acceptForSession", "approved_for_session"),
        ("execCommandApproval", "decline", "denied"),
    ),
)
async def test_command_approval_persists_and_responds_with_protocol_decision(
    tmp_path: Path,
    method: str,
    decision: str,
    wire_decision: str,
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.subscribe("thread-approval")
    request_keys: list[str] = []
    responses: list[tuple[int | str, dict[str, Any]]] = []

    async def forward(request_key: str, _params: dict[str, Any]) -> None:
        request_keys.append(request_key)

    async def respond(
        request_id: int | str, result: dict[str, Any], **_kwargs: Any
    ) -> None:
        responses.append((request_id, result))

    bridge.on_command_approval = forward
    bridge.client.respond = respond  # type: ignore[method-assign]
    params = {
        "threadId": "thread-approval",
        "turnId": "turn-approval",
        "itemId": "item-approval",
        "command": "git status",
        "cwd": str(tmp_path),
        "reason": "command needs approval",
    }

    await bridge._on_server_request(42, method, params, bridge.client.generation)

    assert len(request_keys) == 1
    request_key = request_keys[0]
    assert request_key.startswith("approval:")
    stored = store.get_pending_input(request_key)
    assert stored is not None
    assert stored["request_id"] == "42"
    assert stored["questions"][0]["_bridge_approval_method"] == method

    await bridge.answer_command_approval(request_key, decision)

    assert responses == [(42, {"decision": wire_decision})]
    assert store.get_pending_input(request_key)["status"] == "awaiting_resolved"  # type: ignore[index]
    assert request_key in bridge._pending_requests
    store.close()


@pytest.mark.asyncio
async def test_command_approval_validates_and_preserves_available_decisions(tmp_path: Path) -> None:
    bridge, store = make_bridge(tmp_path)
    store.subscribe("thread-approval")
    request_keys: list[str] = []
    responses: list[tuple[int | str, dict[str, Any]]] = []
    amendment = {
        "acceptWithExecpolicyAmendment": {
            "execpolicy_amendment": ["git", "status"],
        }
    }

    async def forward(request_key: str, _params: dict[str, Any]) -> None:
        request_keys.append(request_key)

    async def respond(
        request_id: int | str, result: dict[str, Any], **_kwargs: Any
    ) -> None:
        responses.append((request_id, result))

    bridge.on_command_approval = forward
    bridge.client.respond = respond  # type: ignore[method-assign]
    await bridge._on_server_request(
        "approval-rpc-id",
        "item/commandExecution/requestApproval",
        {
            "threadId": "thread-approval",
            "turnId": "turn-approval",
            "itemId": "item-approval",
            "command": "git status",
            "availableDecisions": ["cancel", amendment],
        },
        bridge.client.generation,
    )

    [request_key] = request_keys
    stored = store.get_pending_input(request_key)
    assert stored is not None
    assert stored["questions"][0]["_bridge_available_decisions"] == ["cancel", amendment]

    with pytest.raises(ValueError, match="不在当前请求允许"):
        await bridge.answer_command_approval(request_key, "acceptForSession")

    assert responses == []
    assert store.get_pending_input(request_key) is not None

    await bridge.answer_command_approval(request_key, amendment)

    assert responses == [("approval-rpc-id", {"decision": amendment})]
    assert store.get_pending_input(request_key)["status"] == "awaiting_resolved"  # type: ignore[index]
    store.close()


@pytest.mark.asyncio
async def test_legacy_command_approval_normalizes_conversation_and_call_ids(tmp_path: Path) -> None:
    bridge, store = make_bridge(tmp_path)
    store.subscribe("legacy-conversation")
    forwarded: list[tuple[str, dict[str, Any]]] = []
    responses: list[tuple[int | str, dict[str, Any]]] = []

    async def forward(request_key: str, params: dict[str, Any]) -> None:
        forwarded.append((request_key, params))

    async def respond(
        request_id: int | str, result: dict[str, Any], **_kwargs: Any
    ) -> None:
        responses.append((request_id, result))

    bridge.on_command_approval = forward
    bridge.client.respond = respond  # type: ignore[method-assign]
    await bridge._on_server_request(
        73,
        "execCommandApproval",
        {
            "conversationId": "legacy-conversation",
            "callId": "legacy-call",
            "command": ["git", "status", "--short"],
            "cwd": str(tmp_path),
            "parsedCmd": [],
        },
        bridge.client.generation,
    )

    [(request_key, params)] = forwarded
    assert params["threadId"] == "legacy-conversation"
    assert params["itemId"] == "legacy-call"
    stored = store.get_pending_input(request_key)
    assert stored is not None
    assert stored["thread_id"] == "legacy-conversation"
    assert stored["item_id"] == "legacy-call"

    await bridge.answer_command_approval(request_key, "cancel")

    assert responses == [(73, {"decision": "abort"})]
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "params", "decision", "wire_response"),
    (
        (
            "item/fileChange/requestApproval",
            {
                "threadId": "thread-generic",
                "turnId": "turn-file",
                "itemId": "item-file",
                "availableDecisions": ["acceptForSession", "decline"],
            },
            "acceptForSession",
            {"decision": "acceptForSession"},
        ),
        (
            "item/permissions/requestApproval",
            {
                "threadId": "thread-generic",
                "turnId": "turn-permissions",
                "itemId": "item-permissions",
                "permissions": {"fileSystem": "workspaceWrite"},
            },
            {
                "permissions": {"fileSystem": "workspaceWrite"},
                "scope": "turn",
                "strictAutoReview": True,
            },
            {
                "permissions": {"fileSystem": "workspaceWrite"},
                "scope": "turn",
                "strictAutoReview": True,
            },
        ),
        (
            "item/permissions/requestApproval",
            {
                "threadId": "thread-generic",
                "turnId": "turn-permissions-deny",
                "itemId": "item-permissions-deny",
                "permissions": {"network": {"enabled": True}},
            },
            {"permissions": {}, "scope": "turn"},
            {"permissions": {}, "scope": "turn"},
        ),
        (
            "applyPatchApproval",
            {
                "conversationId": "thread-generic",
                "turnId": "turn-patch",
                "callId": "item-patch",
            },
            "accept",
            {"decision": "approved"},
        ),
    ),
)
async def test_generic_approval_methods_emit_exact_wire_payloads(
    tmp_path: Path,
    method: str,
    params: dict[str, Any],
    decision: Any,
    wire_response: dict[str, Any],
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.subscribe("thread-generic")
    request_keys: list[str] = []
    responses: list[tuple[int | str, dict[str, Any], int | None]] = []

    async def forward(request_key: str, _params: dict[str, Any]) -> None:
        request_keys.append(request_key)

    async def respond(
        request_id: int | str,
        result: dict[str, Any],
        *,
        generation: int | None = None,
    ) -> None:
        responses.append((request_id, result, generation))

    bridge.on_command_approval = forward
    bridge.client.respond = respond  # type: ignore[method-assign]

    await bridge._on_server_request(84, method, params, bridge.client.generation)
    [request_key] = request_keys
    if method == "item/permissions/requestApproval":
        stored = store.get_pending_input(request_key)
        assert stored is not None
        assert stored["questions"][0]["_bridge_available_decisions"] == [
            {"permissions": params.get("permissions", {}), "scope": "turn"},
            {"permissions": params.get("permissions", {}), "scope": "session"},
            {"permissions": {}, "scope": "turn"},
        ]
    await bridge.answer_command_approval(request_key, decision)

    assert responses == [(84, wire_response, bridge.client.generation)]
    assert store.get_pending_input(request_key)["status"] == "awaiting_resolved"  # type: ignore[index]
    store.close()


@pytest.mark.asyncio
async def test_permissions_approval_rejects_session_scoped_strict_review(tmp_path: Path) -> None:
    bridge, store = make_bridge(tmp_path)
    store.subscribe("thread-permissions")
    request_keys: list[str] = []

    async def forward(request_key: str, _params: dict[str, Any]) -> None:
        request_keys.append(request_key)

    bridge.on_command_approval = forward
    await bridge._on_server_request(
        85,
        "item/permissions/requestApproval",
        {
            "threadId": "thread-permissions",
            "turnId": "turn-permissions",
            "itemId": "item-permissions",
            "permissions": {"network": {"enabled": True}},
        },
        bridge.client.generation,
    )

    [request_key] = request_keys
    with pytest.raises(ValueError, match="Session"):
        await bridge.answer_command_approval(
            request_key,
            {
                "permissions": {"network": {"enabled": True}},
                "scope": "session",
                "strictAutoReview": True,
            },
        )

    assert store.get_pending_input(request_key)["status"] == "pending"  # type: ignore[index]
    store.close()


@pytest.mark.asyncio
async def test_command_approval_retires_after_transport_generation_changes(tmp_path: Path) -> None:
    bridge, store = make_bridge(tmp_path)
    store.subscribe("thread-reconnect")
    request_keys: list[str] = []
    resolved: list[str] = []

    async def forward(request_key: str, _params: dict[str, Any]) -> None:
        request_keys.append(request_key)

    async def question_resolved(request_key: str) -> None:
        resolved.append(request_key)

    async def fail_after_reconnect(
        _request_id: int | str, _result: dict[str, Any], **_kwargs: Any
    ) -> None:
        bridge.client.generation += 1
        raise OSError("connection replaced")

    bridge.on_command_approval = forward
    bridge.on_question_resolved = question_resolved
    bridge.client.respond = fail_after_reconnect  # type: ignore[method-assign]
    await bridge._on_server_request(
        74,
        "item/commandExecution/requestApproval",
        {
            "threadId": "thread-reconnect",
            "turnId": "turn-reconnect",
            "itemId": "item-reconnect",
            "command": "git status",
        },
        bridge.client.generation,
    )

    [request_key] = request_keys
    with pytest.raises(OSError, match="connection replaced"):
        await bridge.answer_command_approval(request_key, "accept")

    assert resolved == [request_key]
    assert store.get_pending_input(request_key) is None
    assert request_key not in bridge._pending_requests
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
async def test_resync_never_loads_global_thread_list_and_isolates_root_timeouts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.subscribe("a-timeout")
    store.subscribe("b-healthy")
    resumed: list[str] = []

    async def list_threads(**_kwargs: Any) -> list[dict[str, Any]]:
        raise AssertionError("global thread inventory must not be projected during resync")

    async def resume_thread(thread_id: str) -> dict[str, Any]:
        resumed.append(thread_id)
        if thread_id == "a-timeout":
            raise TimeoutError("busy root")
        return {
            "id": thread_id,
            "cwd": str(tmp_path),
            "status": {"type": "active"},
        }

    async def get_goal(_thread_id: str) -> None:
        return None

    async def schedule(_state: ThreadState, *, immediate: bool = False) -> None:
        assert immediate

    monkeypatch.setattr(bridge.client, "list_threads", list_threads)
    monkeypatch.setattr(bridge.client, "resume_thread", resume_thread)
    monkeypatch.setattr(bridge.client, "get_goal", get_goal)
    monkeypatch.setattr(bridge.dashboard, "schedule", schedule)

    await bridge.resync()

    assert resumed == ["a-timeout", "b-healthy"]
    assert store.get_thread("b-healthy") is not None
    assert store.get_thread("a-timeout") is None
    store.close()


@pytest.mark.asyncio
async def test_reconnect_resync_releases_idle_turn_gate_and_retries_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    thread_id = "thread-reconnect-gate"
    store.create_space(
        {
            "space_id": "space-reconnect-gate",
            "space_type": "existing",
            "lifecycle": "active",
            "thread_id": thread_id,
        }
    )
    retries: list[tuple[str, str | None, int | None]] = []

    async def resume_thread(value: str) -> dict[str, Any]:
        assert value == thread_id
        return {
            "id": thread_id,
            "cwd": str(tmp_path),
            "status": {"type": "idle"},
            "turns": [{"id": "turn-before-reconnect", "status": "completed"}],
        }

    async def get_goal(_thread_id: str) -> None:
        return None

    def retry(
        value: str,
        *,
        delay: float = 0.0,
        space_id: str | None = None,
        generation: int | None = None,
    ) -> None:
        del delay
        retries.append((value, space_id, generation))

    monkeypatch.setattr(bridge.client, "resume_thread", resume_thread)
    monkeypatch.setattr(bridge.client, "get_goal", get_goal)
    monkeypatch.setattr(bridge, "_request_queue_retry", retry)
    assert await bridge._begin_turn_gate(thread_id, "client-before-reconnect")
    bridge._bind_turn_gate(
        thread_id,
        "client-before-reconnect",
        "turn-before-reconnect",
    )

    await bridge._on_codex_connection(True, 2, None)

    assert thread_id not in bridge._turn_gates
    assert not bridge._turn_locks[thread_id].locked()
    assert retries == [(thread_id, "space-reconnect-gate", 1)]
    assert await bridge._begin_turn_gate(thread_id, "client-after-reconnect")
    assert bridge._release_turn_gate(thread_id, None)
    store.close()


@pytest.mark.asyncio
async def test_unmanaged_codex_traffic_is_rejected_before_projection_or_telegram(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    projected: list[str] = []
    questions: list[str] = []
    rejected: list[tuple[int | str, int, int | None]] = []

    async def ingest(method: str, _params: dict[str, Any]) -> None:
        projected.append(method)

    async def question(request_key: str, _params: dict[str, Any]) -> None:
        questions.append(request_key)

    async def respond_error(
        request_id: int | str,
        code: int,
        _message: str,
        *,
        generation: int | None = None,
    ) -> None:
        rejected.append((request_id, code, generation))

    monkeypatch.setattr(bridge.projector, "ingest", ingest)
    monkeypatch.setattr(bridge.client, "respond_error", respond_error)
    bridge.on_question = question

    await bridge._on_notification(
        "item/completed",
        {
            "threadId": "foreign-thread",
            "turnId": "foreign-turn",
            "item": {"id": "answer", "type": "agentMessage", "text": "private"},
        },
    )
    await bridge._on_server_request(
        91,
        "item/tool/requestUserInput",
        {
            "threadId": "foreign-thread",
            "turnId": "foreign-turn",
            "questions": [],
        },
        7,
    )

    assert projected == []
    assert questions == []
    assert rejected == [(91, -32600, 7)]
    assert store.get_thread("foreign-thread") is None
    store.close()


@pytest.mark.asyncio
async def test_persisted_subagent_descendant_remains_in_managed_interest_graph(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_thread(
        ThreadState(
            thread_id="managed-parent",
            tasks=[
                TaskState(
                    task_id="managed-child",
                    agent_thread_id="managed-child",
                    title="child",
                )
            ],
        )
    )
    store.subscribe("managed-parent")
    projected: list[str] = []

    async def ingest(_method: str, params: dict[str, Any]) -> None:
        projected.append(str(params.get("threadId") or ""))

    monkeypatch.setattr(bridge.projector, "ingest", ingest)

    await bridge._on_notification(
        "thread/status/changed",
        {"threadId": "managed-child", "status": {"type": "active"}},
    )

    assert projected == ["managed-child"]
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
        ask_reasoning_effort="medium",
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
            "medium",
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
async def test_resolve_files_uses_session_cwd_without_model(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_thread(ThreadState(thread_id="thread-files", cwd=str(tmp_path), status="idle"))
    calls: list[tuple[Path, str]] = []

    async def resolve_files(
        cwd: Path,
        description: str,
    ) -> list[Any]:
        calls.append((cwd, description))
        return []

    monkeypatch.setattr(bridge.resolver, "resolve_files", resolve_files)

    assert await bridge.resolve_files("thread-files", "the report") == []
    assert calls == [(tmp_path.resolve(), "the report")]
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

    async def respond_error(
        request_id: int | str, code: int, message: str, **_kwargs: Any
    ) -> None:
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


@pytest.mark.asyncio
async def test_list_sessions_hides_subagents_and_ephemeral_forks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)

    async def list_threads(**_kwargs: Any) -> list[dict[str, Any]]:
        return [
            {"id": "primary", "status": {"type": "idle"}},
            {
                "id": "child",
                "source": {
                    "subagent": {"threadSpawn": {"parentThreadId": "primary"}}
                },
                "status": {"type": "idle"},
            },
            {"id": "ask-fork", "ephemeral": True, "status": {"type": "idle"}},
        ]

    monkeypatch.setattr(bridge.client, "list_threads", list_threads)

    sessions = await bridge.list_sessions()

    assert [state.thread_id for state in sessions] == ["primary"]
    assert store.get_thread("child").is_subagent  # type: ignore[union-attr]
    assert store.get_thread("ask-fork").ephemeral  # type: ignore[union-attr]
    store.close()


@pytest.mark.asyncio
async def test_collaboration_turn_validates_space_generation_before_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_session_space(
        SessionSpace(
            space_id="space-plan",
            generation=3,
            lifecycle="active",
            thread_id="thread-plan",
        )
    )
    started: list[dict[str, Any]] = []

    async def resume_thread(_thread_id: str) -> dict[str, Any]:
        return {
            "id": "thread-plan",
            "cwd": str(tmp_path),
            "model": "gpt-5.6-luna",
            "reasoningEffort": "max",
            "status": {"type": "idle"},
        }

    async def resolve_mode(
        mode: str, *, model: str, effort: str
    ) -> dict[str, Any]:
        assert mode == "default"
        assert (model, effort) == ("gpt-5.6-luna", "max")
        space = store.get_session_space("space-plan")
        assert space is not None
        space.generation = 4
        store.save_session_space(space)
        return {"mode": mode, "settings": {"model": "gpt-test"}}

    async def start_turn(*_args: Any, **kwargs: Any) -> dict[str, Any]:
        started.append(kwargs)
        return {"id": "turn-plan"}

    async def resolve_profile(model: str, effort: str) -> ModelProfile:
        return ModelProfile(model, effort)

    monkeypatch.setattr(bridge.client, "resume_thread", resume_thread)
    monkeypatch.setattr(bridge.client, "resolve_collaboration_mode", resolve_mode)
    monkeypatch.setattr(bridge.client, "start_turn", start_turn)
    monkeypatch.setattr(bridge, "resolve_model_profile", resolve_profile)

    with pytest.raises(ValueError, match="cannot be empty"):
        await bridge.start_space_collaboration_turn(
            "space-plan", " ", mode="default", client_message_id="tg-empty"
        )
    with pytest.raises(ValueError, match="Unsupported collaboration mode"):
        await bridge.start_space_collaboration_turn(
            "space-plan", "Refine", mode="review", client_message_id="tg-review"
        )
    with pytest.raises(RuntimeError, match="generation is stale"):
        await bridge.start_space_collaboration_turn(
            "space-plan", "Implement the plan", mode="default", client_message_id="tg-plan"
        )

    assert started == []
    store.close()


@pytest.mark.asyncio
async def test_collaboration_turn_rejects_missing_effective_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_session_space(
        SessionSpace(
            space_id="space-no-profile",
            lifecycle="active",
            thread_id="thread-no-profile",
        )
    )

    async def resume_thread(_thread_id: str) -> dict[str, Any]:
        return {
            "id": "thread-no-profile",
            "cwd": str(tmp_path),
            "status": {"type": "idle"},
        }

    monkeypatch.setattr(bridge.client, "resume_thread", resume_thread)
    with pytest.raises(RuntimeError, match="model/effort"):
        await bridge.start_space_collaboration_turn(
            "space-no-profile",
            "Implement the plan",
            mode="default",
            client_message_id="tg-no-profile",
        )
    store.close()


@pytest.mark.asyncio
async def test_plan_completion_hook_uses_authoritative_item_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.subscribe("thread-plan")
    received: list[tuple[str, str, str, str]] = []

    async def ingest(_method: str, _params: dict[str, Any]) -> None:
        pass

    async def plan_completed(
        thread_id: str, turn_id: str, item_id: str, text: str
    ) -> None:
        received.append((thread_id, turn_id, item_id, text))

    monkeypatch.setattr(bridge.projector, "ingest", ingest)
    bridge.on_plan_completed = plan_completed
    plan = {"id": "plan-1", "type": "plan", "text": "1. Inspect\n2. Implement"}

    await bridge._on_notification(
        "item/completed",
        {"threadId": "thread-plan", "turnId": "turn-plan", "item": plan},
    )
    await bridge._on_notification(
        "turn/completed",
        {
            "threadId": "thread-plan",
            "turn": {"id": "turn-plan", "status": "completed", "items": [plan]},
        },
    )

    assert received == [
        ("thread-plan", "turn-plan", "plan-1", "1. Inspect\n2. Implement")
    ]

    updated = {"id": "plan-1", "type": "plan", "text": "1. Inspect\n2. Verify"}
    await bridge._on_notification(
        "item/completed",
        {"threadId": "thread-plan", "turnId": "turn-plan", "item": updated},
    )
    await bridge._on_notification(
        "item/completed",
        {"threadId": "thread-plan", "turnId": "turn-next", "item": updated},
    )

    assert received[-2:] == [
        ("thread-plan", "turn-plan", "plan-1", "1. Inspect\n2. Verify"),
        ("thread-plan", "turn-next", "plan-1", "1. Inspect\n2. Verify"),
    ]
    store.close()


@pytest.mark.asyncio
async def test_tui_plan_approval_hook_uses_live_item_started_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.subscribe("thread-tui")
    received: list[tuple[str, str]] = []

    async def ingest(_method: str, _params: dict[str, Any]) -> None:
        pass

    async def turn_started(thread_id: str, turn_id: str) -> None:
        received.append((thread_id, turn_id))

    monkeypatch.setattr(bridge.projector, "ingest", ingest)
    bridge.on_tui_plan_approved = turn_started

    await bridge._on_notification(
        "turn/started",
        {
            "threadId": "thread-tui",
            "turn": {
                "id": "turn-tui",
                "status": "inProgress",
                "items": [],
            },
        },
    )
    assert received == []

    await bridge._on_notification(
        "item/started",
        {
            "threadId": "thread-tui",
            "turnId": "turn-tui",
            "item": {
                "id": "message-tui",
                "type": "userMessage",
                "clientId": None,
                "content": [
                    {
                        "type": "text",
                        "text": "Implement the plan.",
                        "text_elements": [],
                    }
                ],
            },
        },
    )

    assert received == [("thread-tui", "turn-tui")]
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("client_id", "text"),
    (
        ("telegram-prompt", "Implement the plan."),
        ("telegram-queued", "Run queued work"),
        (None, "Run unrelated work"),
        ("", "Implement the plan."),
        (None, " Implement the plan. "),
    ),
)
async def test_tui_plan_approval_hook_ignores_unrelated_items(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    client_id: str | None,
    text: str,
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.subscribe("thread-plan")
    received: list[tuple[str, str]] = []

    async def ingest(_method: str, _params: dict[str, Any]) -> None:
        pass

    async def turn_started(thread_id: str, turn_id: str) -> None:
        received.append((thread_id, turn_id))

    monkeypatch.setattr(bridge.projector, "ingest", ingest)
    bridge.on_tui_plan_approved = turn_started

    await bridge._on_notification(
        "item/started",
        {
            "threadId": "thread-plan",
            "turnId": "turn-unrelated",
            "item": {
                "id": "message-unrelated",
                "type": "userMessage",
                "clientId": client_id,
                "content": [{"type": "text", "text": text, "text_elements": []}],
            },
        },
    )

    assert received == []
    store.close()


@pytest.mark.asyncio
async def test_space_prompt_completion_receipt_is_routed_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_session_space(
        SessionSpace(
            space_id="space-prompt",
            generation=7,
            lifecycle="active",
            thread_id="thread-prompt",
        )
    )
    receipts: list[dict[str, Any]] = []
    notification = {
        "threadId": "thread-prompt",
        "turn": {
            "id": "turn-prompt",
            "status": "failed",
            "error": {
                "message": "request failed",
                "codexErrorInfo": "usageLimitExceeded",
            },
            "items": [],
        },
    }

    async def resume_thread(_thread_id: str) -> dict[str, Any]:
        return {
            "id": "thread-prompt",
            "cwd": str(tmp_path),
            "status": {"type": "idle"},
        }

    async def start_turn(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        await bridge._on_notification("turn/completed", notification)
        return {"id": "turn-prompt"}

    async def ingest(_method: str, _params: dict[str, Any]) -> None:
        pass

    async def completed(run: dict[str, Any]) -> None:
        receipts.append(run)

    monkeypatch.setattr(bridge.client, "resume_thread", resume_thread)
    monkeypatch.setattr(bridge.client, "start_turn", start_turn)
    monkeypatch.setattr(bridge.projector, "ingest", ingest)
    bridge.on_prompt_completed = completed

    assert await bridge.send_space_prompt(
        "space-prompt",
        "Do the work",
        client_message_id="tg-prompt",
    ) == "started"
    await bridge._on_notification("turn/completed", notification)
    await bridge._on_notification("turn/completed", notification)

    assert len(receipts) == 1
    expected = {
        "space_id": "space-prompt",
        "generation": 7,
        "thread_id": "thread-prompt",
        "turn_id": "turn-prompt",
        "client_message_id": "tg-prompt",
        "status": "failed",
        "error_kind": "usageLimitExceeded",
    }
    assert {key: receipts[0][key] for key in expected} == expected
    store.close()


@pytest.mark.asyncio
async def test_queued_space_prompt_registers_completion_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_session_space(
        SessionSpace(
            space_id="space-queued",
            generation=2,
            lifecycle="active",
            thread_id="thread-queued",
        )
    )
    store.save_thread(
        ThreadState(thread_id="thread-queued", cwd=str(tmp_path), status="idle")
    )
    store.enqueue_prompt(
        "thread-queued",
        "Queued work",
        [{"type": "text", "text": "Queued work"}],
        "tg-queued",
        space_id="space-queued",
        generation=2,
    )
    receipts: list[dict[str, Any]] = []
    notification = {
        "threadId": "thread-queued",
        "turn": {"id": "turn-queued", "status": "completed", "items": []},
    }

    async def not_delivered(_thread_id: str, _client_message_id: str) -> bool:
        return False

    async def start_turn(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        await bridge._on_notification("turn/completed", notification)
        return {"id": "turn-queued"}

    async def ingest(_method: str, _params: dict[str, Any]) -> None:
        pass

    async def completed(run: dict[str, Any]) -> None:
        receipts.append(run)

    monkeypatch.setattr(bridge, "_client_message_exists", not_delivered)
    monkeypatch.setattr(bridge.client, "start_turn", start_turn)
    monkeypatch.setattr(bridge.projector, "ingest", ingest)
    bridge.on_prompt_completed = completed

    await bridge.dispatch_space_queue("space-queued", generation=2)
    await bridge._on_notification("turn/completed", notification)

    assert len(receipts) == 1
    assert (receipts[0]["space_id"], receipts[0]["generation"]) == ("space-queued", 2)
    assert receipts[0]["status"] == "completed"
    assert store.space_queue_entries("space-queued", 2) == []
    store.close()


@pytest.mark.asyncio
async def test_subagent_activity_hydrates_effective_child_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_thread(ThreadState(thread_id="parent", status="active"))
    store.subscribe("parent")

    async def resume_thread(thread_id: str) -> dict[str, Any]:
        assert thread_id == "child-luna"
        return {
            "id": thread_id,
            "parentThreadId": "parent",
            "model": "gpt-5.6-luna",
            "reasoningEffort": "max",
            "status": {"type": "active"},
        }

    monkeypatch.setattr(bridge.client, "resume_thread", resume_thread)
    await bridge._on_notification(
        "item/started",
        {
            "threadId": "parent",
            "item": {
                "id": "activity-child-luna",
                "type": "subAgentActivity",
                "agentThreadId": "child-luna",
                "agentPath": "/root/reviewer",
                "kind": "started",
            },
        },
    )
    task = bridge._subagent_profile_tasks["child-luna"]
    await task

    parent = store.get_thread("parent")
    assert parent is not None
    assert [(item.model, item.reasoning_effort) for item in parent.tasks] == [
        ("gpt-5.6-luna", "max")
    ]
    store.close()


@pytest.mark.asyncio
async def test_restart_hydrates_profiles_for_persisted_subagent_tasks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_thread(
        ThreadState(
            thread_id="parent",
            status="active",
            tasks=[
                TaskState(
                    task_id="child-sol",
                    agent_thread_id="child-sol",
                    agent_path="/root/worker",
                    title="Difficult task",
                    status="inProgress",
                )
            ],
        )
    )
    store.subscribe("parent")

    async def resume_thread(thread_id: str) -> dict[str, Any]:
        assert thread_id == "child-sol"
        return {
            "id": thread_id,
            "parentThreadId": "parent",
            "model": "gpt-5.6-sol",
            "reasoningEffort": "xhigh",
            "status": {"type": "active"},
        }

    monkeypatch.setattr(bridge.client, "resume_thread", resume_thread)
    await bridge._hydrate_missing_subagent_profiles()
    await bridge._subagent_profile_tasks["child-sol"]

    parent = store.get_thread("parent")
    assert parent is not None
    assert [(item.model, item.reasoning_effort) for item in parent.tasks] == [
        ("gpt-5.6-sol", "xhigh")
    ]
    store.close()


@pytest.mark.asyncio
async def test_subagent_hydration_skips_persisted_not_found_tasks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_thread(
        ThreadState(
            thread_id="parent",
            tasks=[
                TaskState(
                    task_id="missing-child",
                    agent_thread_id="missing-child",
                    title="missing",
                    status="notFound",
                )
            ],
        )
    )
    store.subscribe("parent")
    resumed: list[str] = []

    async def resume_thread(thread_id: str) -> dict[str, Any]:
        resumed.append(thread_id)
        raise AssertionError("notFound child must not be resumed")

    monkeypatch.setattr(bridge.client, "resume_thread", resume_thread)

    await bridge._hydrate_missing_subagent_profiles()

    assert resumed == []
    assert bridge._subagent_profile_tasks == {}
    store.close()


@pytest.mark.asyncio
async def test_no_rollout_found_marks_subagent_terminal_without_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_thread(
        ThreadState(
            thread_id="parent",
            tasks=[
                TaskState(
                    task_id="missing-child",
                    agent_thread_id="missing-child",
                    title="missing",
                    status="inProgress",
                )
            ],
        )
    )
    store.subscribe("parent")
    attempts = 0

    async def resume_thread(_thread_id: str) -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        raise CodexRpcError("thread/resume", {"message": "no rollout found for thread"})

    monkeypatch.setattr(bridge.client, "resume_thread", resume_thread)

    await bridge._hydrate_missing_subagent_profiles()
    await bridge._subagent_profile_tasks["missing-child"]
    await bridge._hydrate_missing_subagent_profiles()

    parent = store.get_thread("parent")
    assert attempts == 1
    assert parent is not None and parent.tasks[0].status == "notFound"
    assert bridge._subagent_profile_tasks == {}
    store.close()


@pytest.mark.asyncio
async def test_subagent_hydration_retries_transient_failures_in_one_coalesced_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_thread(
        ThreadState(
            thread_id="parent",
            tasks=[
                TaskState(
                    task_id="eventual-child",
                    agent_thread_id="eventual-child",
                    title="eventual",
                    status="inProgress",
                )
            ],
        )
    )
    store.subscribe("parent")
    attempts = 0

    async def resume_thread(thread_id: str) -> dict[str, Any]:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise TimeoutError("temporary app-server stall")
        return {
            "id": thread_id,
            "parentThreadId": "parent",
            "model": "gpt-5.6-luna",
            "reasoningEffort": "max",
            "status": {"type": "active"},
        }

    monkeypatch.setattr(bridge.client, "resume_thread", resume_thread)
    monkeypatch.setattr(bridge_module, "_SUBAGENT_PROFILE_RETRY_DELAYS", (0.0, 0.0, 0.0, 0.0))

    await bridge._hydrate_missing_subagent_profiles()
    task = bridge._subagent_profile_tasks["eventual-child"]
    await bridge._hydrate_missing_subagent_profiles()
    assert bridge._subagent_profile_tasks["eventual-child"] is task
    await task

    parent = store.get_thread("parent")
    assert attempts == 3
    assert parent is not None
    assert (parent.tasks[0].model, parent.tasks[0].reasoning_effort) == (
        "gpt-5.6-luna",
        "max",
    )
    store.close()


@pytest.mark.asyncio
async def test_prompt_choice_reuses_client_id_for_steer_and_rejects_racing_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_thread(
        ThreadState(
            thread_id="thread-choice", cwd=str(tmp_path), status="active",
            turn_id="turn-active", turn_status="inProgress",
        )
    )

    async def snapshot(_thread_id: str) -> dict[str, Any]:
        return {
            "id": "thread-choice", "cwd": str(tmp_path), "status": {"type": "active"},
            "turns": [{"id": "turn-active", "status": "inProgress"}],
        }

    async def steer(*_args: Any, **_kwargs: Any) -> None:
        pass

    monkeypatch.setattr(bridge, "_live_thread_snapshot", snapshot)
    monkeypatch.setattr(bridge.client, "steer_turn", steer)

    assert await bridge.send_prompt(
        "thread-choice", "work", client_message_id="choice-1"
    ) == "choose"
    assert await bridge.send_prompt(
        "thread-choice", "work", mode="steer", client_message_id="choice-1"
    ) == "steered"
    with pytest.raises(ValueError, match="already resolved"):
        await bridge.send_prompt(
            "thread-choice", "work", mode="queue", client_message_id="choice-1"
        )
    assert store.get_prompt_intent("choice-1").state == "steered"  # type: ignore[union-attr]
    store.close()


@pytest.mark.asyncio
async def test_turn_gate_ignores_mismatched_completion_and_releases_matching_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_thread(ThreadState(thread_id="thread-gate", cwd=str(tmp_path), status="idle"))
    store.subscribe("thread-gate")
    retries: list[str] = []
    async def no_schedule(*_args: Any, **_kwargs: Any) -> None:
        pass

    monkeypatch.setattr(bridge.dashboard, "schedule", no_schedule)
    monkeypatch.setattr(
        bridge, "_request_queue_after_completion", lambda thread_id: retries.append(thread_id)
    )
    assert await bridge._begin_turn_gate("thread-gate", "client-gate")
    bridge._bind_turn_gate("thread-gate", "client-gate", "turn-right")

    await bridge._on_notification(
        "turn/completed",
        {"threadId": "thread-gate", "turn": {"id": "turn-wrong", "status": "completed", "items": []}},
    )
    assert "thread-gate" in bridge._turn_gates
    await bridge._on_notification(
        "turn/completed",
        {"threadId": "thread-gate", "turn": {"id": "turn-right", "status": "completed", "items": []}},
    )
    assert "thread-gate" not in bridge._turn_gates
    assert retries == ["thread-gate"]
    store.close()


@pytest.mark.asyncio
async def test_permanent_direct_rpc_error_releases_turn_gate_without_reconciliation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    thread_id = "thread-direct-rejected"

    async def snapshot(_thread_id: str) -> dict[str, Any]:
        return {
            "id": thread_id,
            "cwd": str(tmp_path),
            "status": {"type": "idle"},
        }

    async def read_thread(_thread_id: str, *, include_turns: bool) -> dict[str, Any]:
        assert include_turns
        return {"turns": []}

    async def start_turn(*_args: Any, **_kwargs: Any) -> None:
        raise CodexRpcError("turn/start", {"message": "invalid input"})

    monkeypatch.setattr(bridge, "_live_thread_snapshot", snapshot)
    monkeypatch.setattr(bridge.client, "read_thread", read_thread)
    monkeypatch.setattr(bridge.client, "start_turn", start_turn)

    with pytest.raises(CodexRpcError):
        await bridge.send_prompt(
            thread_id,
            "rejected",
            client_message_id="client-direct-rejected",
        )

    assert thread_id not in bridge._turn_gates
    assert not bridge._turn_locks[thread_id].locked()
    assert bridge._turn_reconcile_tasks == {}
    intent = store.get_prompt_intent("client-direct-rejected")
    assert intent is not None and intent.state == "failed"
    store.close()


@pytest.mark.asyncio
async def test_started_notification_is_not_blocked_by_slow_state_hook(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    store.save_thread(ThreadState(thread_id="thread-effects", status="idle"))
    store.subscribe("thread-effects")
    release = asyncio.Event()
    started = asyncio.Event()

    async def slow_change(_state: ThreadState, _reason: str) -> None:
        started.set()
        await release.wait()

    bridge.on_state_change = slow_change
    bridge._accept_notification_effects = True
    bridge._notification_effect_task = asyncio.create_task(bridge._run_notification_effects())
    try:
        await bridge._on_notification(
            "turn/started",
            {"threadId": "thread-effects", "turn": {"id": "turn-1", "status": "inProgress"}},
        )
        await asyncio.wait_for(started.wait(), timeout=1)
        await asyncio.wait_for(
            bridge._on_notification(
                "item/started",
                {
                    "threadId": "thread-effects",
                    "turnId": "turn-1",
                    "item": {"id": "i", "type": "agentMessage", "text": "x"},
                },
            ),
            timeout=0.1,
        )
    finally:
        release.set()
        await bridge._stop_notification_effects()
        store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("winning_mode", "losing_mode", "winning_result"),
    (("steer", "queue", "steered"), ("queue", "steer", "queued")),
)
async def test_awaiting_choice_has_one_atomic_steer_or_queue_winner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    winning_mode: str,
    losing_mode: str,
    winning_result: str,
) -> None:
    bridge, store = make_bridge(tmp_path)
    thread_id = "thread-choice-race"
    client_id = f"choice-{winning_mode}"
    store.save_thread(
        ThreadState(
            thread_id=thread_id,
            cwd=str(tmp_path),
            status="active",
            turn_id="turn-active",
            turn_status="inProgress",
        )
    )
    snapshot_started = asyncio.Event()
    release_snapshot = asyncio.Event()
    steer_calls: list[str] = []

    async def snapshot(_thread_id: str) -> dict[str, Any]:
        snapshot_started.set()
        await release_snapshot.wait()
        return {
            "id": thread_id,
            "cwd": str(tmp_path),
            "status": {"type": "active"},
            "turns": [{"id": "turn-active", "status": "inProgress"}],
        }

    async def steer(
        _thread_id: str,
        _turn_id: str,
        _inputs: list[dict[str, Any]],
        *,
        client_message_id: str,
    ) -> None:
        steer_calls.append(client_message_id)

    async def no_schedule(*_args: Any, **_kwargs: Any) -> None:
        pass

    monkeypatch.setattr(bridge, "_live_thread_snapshot", snapshot)
    monkeypatch.setattr(bridge.client, "steer_turn", steer)
    monkeypatch.setattr(bridge.dashboard, "schedule", no_schedule)

    initial = store.create_prompt_intent(
        client_id,
        "legacy",
        "work",
        "auto",
        thread_id=thread_id,
    )
    assert store.transition_prompt_intent(
        client_id,
        expected_states={"received"},
        to_state="awaiting_choice",
    )
    assert initial.intent_id == store.get_prompt_intent(client_id).intent_id  # type: ignore[union-attr]

    winner = asyncio.create_task(
        bridge.send_prompt(
            thread_id,
            "work",
            mode=winning_mode,
            client_message_id=client_id,
        )
    )
    await asyncio.wait_for(snapshot_started.wait(), timeout=1)
    with pytest.raises(ValueError, match="already resolved"):
        await bridge.send_prompt(
            thread_id,
            "work",
            mode=losing_mode,
            client_message_id=client_id,
        )
    release_snapshot.set()

    assert await winner == winning_result
    intent = store.get_prompt_intent(client_id)
    assert intent is not None and intent.mode == winning_mode
    assert steer_calls == ([client_id] if winning_mode == "steer" else [])
    assert store.queue_count(thread_id) == (1 if winning_mode == "queue" else 0)
    store.close()


@pytest.mark.asyncio
async def test_thread_snapshot_coalesces_same_generation_and_refreshes_after_reconnect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    calls = 0
    started = asyncio.Event()
    release = asyncio.Event()

    async def resume_thread(thread_id: str) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return {"id": thread_id, "generation": bridge.client.generation}

    monkeypatch.setattr(bridge.client, "resume_thread", resume_thread)
    first = asyncio.create_task(bridge._live_thread_snapshot("thread-snapshot"))
    second = asyncio.create_task(bridge._live_thread_snapshot("thread-snapshot"))
    await asyncio.wait_for(started.wait(), timeout=1)
    release.set()

    assert await first == await second == {"id": "thread-snapshot", "generation": 0}
    assert await bridge._live_thread_snapshot("thread-snapshot") == {
        "id": "thread-snapshot",
        "generation": 0,
    }
    assert calls == 1

    bridge.client.generation = 1
    assert await bridge._live_thread_snapshot("thread-snapshot") == {
        "id": "thread-snapshot",
        "generation": 1,
    }
    assert calls == 2
    store.close()


@pytest.mark.asyncio
async def test_idle_reconciliation_retries_until_confirmed_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    thread_id = "thread-reconcile"
    store.save_thread(ThreadState(thread_id=thread_id, cwd=str(tmp_path), status="idle"))
    retries: list[str] = []
    receipts: list[tuple[bool | None, dict[str, Any] | None]] = [
        (None, None),
        (False, None),
    ]

    async def receipt(
        _thread_id: str, _client_message_id: str
    ) -> tuple[bool | None, dict[str, Any] | None]:
        return receipts.pop(0)

    monkeypatch.setattr(bridge_module, "_TURN_RECONCILE_SECONDS", 0.0)
    monkeypatch.setattr(bridge, "_client_message_receipt", receipt)
    monkeypatch.setattr(
        bridge, "_request_queue_after_completion", lambda value: retries.append(value)
    )
    bridge.client._connected.set()
    assert await bridge._begin_turn_gate(thread_id, "client-reconcile")

    bridge._schedule_turn_reconciliation(thread_id)
    await bridge._turn_reconcile_tasks[thread_id]
    assert thread_id not in bridge._turn_gates
    assert retries == [thread_id]
    assert receipts == []
    store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("connected", "expected"),
    ((True, "safe_to_submit"), (False, "uncertain")),
)
async def test_plan_decision_gate_requires_connected_idle_absence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    connected: bool,
    expected: str,
) -> None:
    bridge, store = make_bridge(tmp_path)
    thread_id = "thread-plan-gate"
    store.save_thread(ThreadState(thread_id=thread_id, cwd=str(tmp_path), status="idle"))
    store.save_session_space(
        SessionSpace(space_id="space-plan-gate", lifecycle="active", thread_id=thread_id)
    )
    assert store.claim_plan_publication(
        space_id="space-plan-gate",
        generation=1,
        item_id="plan-item",
        revision_key="revision",
        thread_id=thread_id,
        turn_id="turn-plan",
        plan_text="Plan",
    )

    async def absent(
        _thread_id: str, _client_message_id: str
    ) -> tuple[bool, None]:
        return False, None

    monkeypatch.setattr(bridge, "_client_message_receipt", absent)
    if connected:
        bridge.client._connected.set()

    result = await bridge.wait_for_plan_decision_gate(
        "space-plan-gate",
        1,
        "plan-item",
        "revision",
        "client-plan",
        timeout=0,
    )

    assert result == {"status": expected}
    store.close()


@pytest.mark.asyncio
async def test_notification_effects_run_fifo_and_shutdown_accounts_for_dropped_items(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bridge, store = make_bridge(tmp_path)
    order: list[int] = []
    bridge._accept_notification_effects = True
    bridge._notification_effect_task = asyncio.create_task(bridge._run_notification_effects())

    for value in range(3):
        async def effect(item: int = value) -> None:
            await asyncio.sleep(0)
            order.append(item)

        await bridge._dispatch_notification_effect(str(value), effect)
    await asyncio.wait_for(bridge._notification_effects.join(), timeout=1)
    assert order == [0, 1, 2]
    await bridge._stop_notification_effects()

    blocked = asyncio.Event()
    second_ran = False

    async def blocking_effect() -> None:
        blocked.set()
        await asyncio.Event().wait()

    async def dropped_effect() -> None:
        nonlocal second_ran
        second_ran = True

    monkeypatch.setattr(bridge_module, "_NOTIFICATION_EFFECT_DRAIN_SECONDS", 0.01)
    bridge._accept_notification_effects = True
    bridge._notification_effect_task = asyncio.create_task(bridge._run_notification_effects())
    await bridge._dispatch_notification_effect("blocking", blocking_effect)
    await bridge._dispatch_notification_effect("dropped", dropped_effect)
    await asyncio.wait_for(blocked.wait(), timeout=1)

    await bridge._stop_notification_effects()

    await asyncio.wait_for(bridge._notification_effects.join(), timeout=0.1)
    assert bridge._notification_effects.empty()
    assert second_ran is False
    store.close()
