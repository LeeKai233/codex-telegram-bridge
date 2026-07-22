from __future__ import annotations

from pathlib import Path

import pytest

import codex_telegram_bridge.projector as projector_module
from codex_telegram_bridge.models import (
    LifecycleActivity,
    PlanStep,
    SessionSpace,
    TaskState,
    ThreadState,
)
from codex_telegram_bridge.projector import EventProjector
from codex_telegram_bridge.store import Store


def managed_projector(store: Store, on_change=None):  # type: ignore[no-untyped-def]
    options = {"is_managed": lambda _method, _params: True}
    return EventProjector(store, **options) if on_change is None else EventProjector(
        store, on_change, **options
    )


@pytest.mark.asyncio
async def test_projector_rejects_global_and_unmanaged_events_by_default(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    projector = EventProjector(store)

    await projector.ingest("account/rateLimits/updated", {"limit": 1})
    await projector.ingest(
        "thread/status/changed",
        {"threadId": "unmanaged", "status": {"type": "idle"}},
    )

    assert store.get_thread("unmanaged") is None
    assert store.timeline("unmanaged") == []
    store.close()


@pytest.mark.asyncio
async def test_repeated_status_transition_is_processed_after_an_intervening_event(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    changes: list[tuple[str, str]] = []

    async def changed(state, reason: str) -> None:  # type: ignore[no-untyped-def]
        changes.append((state.status, reason))

    projector = managed_projector(store, changed)
    idle = {"threadId": "thread-1", "status": {"type": "idle"}}

    await projector.ingest("thread/status/changed", idle)
    await projector.ingest("thread/status/changed", idle)
    await projector.ingest(
        "turn/started",
        {"threadId": "thread-1", "turn": {"id": "turn-2", "status": "inProgress", "items": []}},
    )
    await projector.ingest("thread/status/changed", idle)

    assert changes == [
        ("idle", "thread/status/changed"),
        ("active", "turn/started"),
        ("idle", "thread/status/changed"),
    ]
    assert [event["kind"] for event in store.timeline("thread-1")] == [
        "thread/status/changed",
        "turn/started",
        "thread/status/changed",
    ]
    store.close()


def test_old_thread_state_without_recovery_flag_loads_as_not_recoverable() -> None:
    state = ThreadState.from_dict({"thread_id": "legacy", "last_error": "old failure"})

    assert state.last_error == "old failure"
    assert state.last_error_recoverable is False


@pytest.mark.asyncio
async def test_retryable_error_clears_on_healthy_status_but_remains_in_timeline(
    tmp_path: Path,
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    projector = managed_projector(store)

    await projector.ingest(
        "error",
        {
            "threadId": "thread-recovery",
            "willRetry": True,
            "error": {"message": "Reconnecting"},
        },
    )
    retrying = store.get_thread("thread-recovery")
    assert retrying is not None
    assert retrying.last_error == "Reconnecting"
    assert retrying.last_error_recoverable is True
    assert retrying.recent_activity[-1].status == "retrying"

    await projector.ingest(
        "thread/status/changed",
        {"threadId": "thread-recovery", "status": {"type": "idle"}},
    )

    recovered = store.get_thread("thread-recovery")
    assert recovered is not None
    assert recovered.last_error == ""
    assert recovered.last_error_recoverable is False
    assert recovered.latest_activity == "连接已恢复"
    assert all(activity.status != "retrying" for activity in recovered.recent_activity)
    assert [event["kind"] for event in store.timeline("thread-recovery")] == [
        "error",
        "thread/status/changed",
    ]
    store.close()


@pytest.mark.asyncio
async def test_terminal_error_supersedes_older_retrying_activity(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    projector = managed_projector(store)

    await projector.ingest(
        "error",
        {
            "threadId": "thread-terminal-error",
            "willRetry": True,
            "error": {"message": "Temporary transport failure"},
        },
    )
    await projector.ingest(
        "error",
        {
            "threadId": "thread-terminal-error",
            "willRetry": False,
            "error": {"message": "Reconnecting"},
        },
    )
    await projector.ingest(
        "thread/status/changed",
        {"threadId": "thread-terminal-error", "status": {"type": "idle"}},
    )

    state = store.get_thread("thread-terminal-error")
    assert state is not None
    assert state.last_error == "Reconnecting"
    assert state.last_error_recoverable is False
    assert state.latest_activity == "Session idle"
    store.close()


@pytest.mark.parametrize(
    ("case", "updated_at", "expected_error", "expected_status"),
    [
        ("missing-timestamp", None, "", "idle"),
        ("older-snapshot", 99, "Reconnecting", "systemError"),
        ("same-timestamp", 100, "", "idle"),
    ],
)
def test_healthy_snapshot_recovery_respects_timestamp_case(
    tmp_path: Path,
    case: str,
    updated_at: int | None,
    expected_error: str,
    expected_status: str,
) -> None:
    store = Store(tmp_path / f"state-{case}.sqlite3")
    projector = managed_projector(store)
    store.save_thread(
        ThreadState(
            thread_id="thread-snapshot",
            status="systemError",
            updated_at=100,
            last_error="Reconnecting",
            last_error_recoverable=True,
            recent_activity=[
                LifecycleActivity(
                    kind="error", text="Reconnecting", status="retrying", timestamp=100
                )
            ],
        )
    )

    payload: dict[str, object] = {
        "id": "thread-snapshot",
        "status": {"type": "idle"},
    }
    if updated_at is not None:
        payload["updatedAt"] = updated_at
    state = projector.apply_thread(payload)

    assert state.last_error == expected_error
    assert state.status == expected_status
    if expected_error:
        assert state.latest_activity == ""
        assert state.last_error_recoverable is True
        assert any(activity.status == "retrying" for activity in state.recent_activity)
    else:
        assert state.last_error_recoverable is False
        assert state.latest_activity == "连接已恢复"
        assert all(activity.status != "retrying" for activity in state.recent_activity)
    store.close()


def test_reconnecting_text_is_recoverable_even_without_flag(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    projector = managed_projector(store)
    store.save_thread(
        ThreadState(
            thread_id="thread-reconnecting-text",
            status="systemError",
            last_error="Reconnecting",
            recent_activity=[
                LifecycleActivity(
                    kind="error", text="Reconnecting", status="retrying", timestamp=100
                )
            ],
        )
    )

    recovered = projector.apply_thread(
        {"id": "thread-reconnecting-text", "status": {"type": "idle"}}
    )

    assert recovered.last_error == ""
    assert recovered.last_error_recoverable is False
    assert recovered.latest_activity == "连接已恢复"
    assert all(activity.status != "retrying" for activity in recovered.recent_activity)
    store.close()


@pytest.mark.asyncio
async def test_failed_turn_ends_retry_state_but_keeps_terminal_error(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    projector = managed_projector(store)
    await projector.ingest(
        "error",
        {
            "threadId": "thread-failed",
            "willRetry": True,
            "error": {"message": "Temporary transport failure"},
        },
    )

    await projector.ingest(
        "turn/completed",
        {
            "threadId": "thread-failed",
            "turn": {
                "id": "turn-failed",
                "status": "failed",
                "error": {"message": "Terminal failure"},
                "items": [],
            },
        },
    )

    state = store.get_thread("thread-failed")
    assert state is not None
    assert state.last_error == "Terminal failure"
    assert state.last_error_recoverable is False
    assert state.turn_status == "failed"
    assert all(activity.status != "retrying" for activity in state.recent_activity)

    await projector.ingest(
        "turn/started",
        {
            "threadId": "thread-failed",
            "turn": {"id": "turn-retry", "status": "inProgress", "items": []},
        },
    )
    started = store.get_thread("thread-failed")
    assert started is not None
    assert started.last_error == "Terminal failure"

    await projector.ingest(
        "turn/completed",
        {
            "threadId": "thread-failed",
            "turn": {"id": "turn-retry", "status": "completed", "items": []},
        },
    )
    recovered = store.get_thread("thread-failed")
    assert recovered is not None
    assert recovered.last_error == ""
    assert recovered.last_error_recoverable is False
    store.close()


@pytest.mark.asyncio
async def test_thread_settings_notification_updates_space_mode_and_profile(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    changes: list[str] = []

    async def changed(_state: ThreadState, reason: str) -> None:
        changes.append(reason)

    store.save_thread(ThreadState(thread_id="thread-settings", status="idle"))
    store.save_session_space(
        SessionSpace(
            space_id="space-settings",
            lifecycle="active",
            thread_id="thread-settings",
            normal_model="gpt-5.6-luna",
            normal_effort="max",
        )
    )
    projector = managed_projector(store, changed)

    await projector.ingest(
        "thread/settings/updated",
        {
            "threadId": "thread-settings",
            "threadSettings": {
                "model": "gpt-5.6-sol",
                "effort": "xhigh",
                "activePermissionProfile": {"id": "workspace-safe", "extends": "default"},
                "approvalPolicy": "on-request",
                "approvalsReviewer": "user",
                "sandboxPolicy": {"type": "workspaceWrite", "networkAccess": False},
                "collaborationMode": {
                    "mode": "plan",
                    "settings": {
                        "model": "gpt-5.6-sol",
                        "reasoning_effort": "xhigh",
                    },
                },
            },
        },
    )

    space = store.get_session_space("space-settings")
    state = store.get_thread("thread-settings")
    assert space is not None
    assert (space.observed_mode, space.plan_model, space.plan_effort) == (
        "plan",
        "gpt-5.6-sol",
        "xhigh",
    )
    assert space.current_mode == space.desired_mode == "default"
    assert state is not None and state.latest_activity == "Session settings updated"
    assert (state.permissions, state.approval_policy, state.approvals_reviewer) == (
        "workspace-safe",
        "on-request",
        "user",
    )
    assert state.sandbox_policy == {"type": "workspaceWrite", "networkAccess": False}
    assert changes == ["thread/settings/updated"]
    store.close()


@pytest.mark.asyncio
async def test_child_status_change_notifies_parent_dashboard(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    changes: list[tuple[str, str]] = []

    async def changed(state: ThreadState, reason: str) -> None:
        changes.append((state.thread_id, reason))

    store.save_thread(
        ThreadState(
            thread_id="parent",
            tasks=[TaskState(task_id="child", title="Child", status="pending")],
        )
    )
    store.save_thread(
        ThreadState(thread_id="child", parent_thread_id="parent", status="idle")
    )
    projector = managed_projector(store, changed)

    await projector.ingest(
        "thread/status/changed",
        {"threadId": "child", "status": {"type": "active"}},
    )

    assert changes == [("child", "thread/status/changed"), ("parent", "subagent/updated")]
    parent = store.get_thread("parent")
    assert parent is not None and parent.tasks[0].status == "inProgress"
    store.close()


def test_apply_thread_projects_server_timestamps_and_turn_durations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    projector = managed_projector(store)
    monkeypatch.setattr(projector_module.time, "time", lambda: 150)

    state = projector.apply_thread(
        {
            "id": "thread-duration",
            "preview": "Duration test",
            "cwd": str(tmp_path),
            "createdAt": 100,
            "updatedAt": 130,
            "status": {"type": "active"},
            "turns": [
                {
                    "id": "turn-completed",
                    "status": "completed",
                    "startedAt": 101,
                    "durationMs": 2500,
                    "items": [],
                },
                {
                    "id": "turn-active",
                    "status": "inProgress",
                    "startedAt": 140,
                    "durationMs": 200,
                    "items": [],
                },
            ],
        }
    )

    assert state.created_at == 100
    assert state.updated_at == 130
    assert state.completed_duration_ms == 2500
    assert state.current_duration_ms == 10_000
    assert state.total_duration_ms == 12_500
    assert store.get_thread("thread-duration").updated_at == 130  # type: ignore[union-attr]
    store.close()


@pytest.mark.asyncio
async def test_collaboration_items_project_semantic_tasks_and_recent_activity(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    projector = managed_projector(store)
    projector.apply_thread({"id": "thread-tasks", "status": {"type": "active"}})

    await projector.ingest(
        "item/completed",
        {
            "threadId": "thread-tasks",
            "item": {
                "id": "collab-1",
                "type": "collabAgentToolCall",
                "tool": "spawn_agent",
                "status": "completed",
                "prompt": "Implement the metrics collector",
                "model": "gpt-5.6-terra",
                "reasoningEffort": "high",
                "receiverThreadIds": ["agent-active"],
                "agentsStates": {
                    "agent-complete": {"status": "completed"},
                    "agent-active": {"status": "running", "message": "testing"},
                    "agent-failed": {"status": "errored", "message": "failed"},
                },
            },
        },
    )

    state = store.get_thread("thread-tasks")
    assert state is not None
    assert [(task.task_id, task.status) for task in state.tasks] == [
        ("agent-active", "inProgress"),
        ("agent-complete", "completed"),
        ("agent-failed", "failed"),
    ]
    assert all(task.title == "Implement the metrics collector" for task in state.tasks)
    profiles = {
        task.task_id: (task.model, task.reasoning_effort) for task in state.tasks
    }
    assert profiles == {
        "agent-active": ("gpt-5.6-terra", "high"),
        "agent-complete": ("", ""),
        "agent-failed": ("", ""),
    }
    assert (state.agents_active, state.agents_completed, state.agents_failed) == (1, 1, 1)
    assert state.recent_activity[-1].kind == "collabAgentToolCall"
    store.close()


@pytest.mark.asyncio
async def test_child_thread_status_reconciles_historical_subagent_activity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    projector = managed_projector(store)
    monkeypatch.setattr(projector_module.time, "time", lambda: 500)
    parent_payload = {
        "id": "parent",
        "status": {"type": "idle"},
        "turns": [
            {
                "id": "turn-old",
                "status": "completed",
                "items": [
                    {
                        "type": "subAgentActivity",
                        "agentThreadId": child_id,
                        "agentPath": f"/root/{child_id}",
                        "kind": "started",
                    }
                    for child_id in ("child-done", "child-closed", "child-error")
                ],
            }
        ],
    }

    projector.apply_thread(parent_payload)
    assert store.get_thread("parent").agents_active == 3  # type: ignore[union-attr]

    for child_id, status in (
        ("child-done", "idle"),
        ("child-closed", "notLoaded"),
        ("child-error", "systemError"),
    ):
        projector.apply_thread(
            {
                "id": child_id,
                "parentThreadId": "parent",
                "createdAt": 100,
                "updatedAt": 300,
                "status": {"type": status},
            }
        )

    parent = store.get_thread("parent")
    assert parent is not None
    assert {task.task_id: task.status for task in parent.tasks} == {
        "child-done": "completed",
        "child-closed": "shutdown",
        "child-error": "failed",
    }
    assert (parent.agents_active, parent.agents_completed, parent.agents_failed) == (0, 2, 1)
    assert all(task.started_at == 100 for task in parent.tasks)

    projector.apply_thread(parent_payload)
    replayed = store.get_thread("parent")
    assert replayed is not None
    assert {task.task_id: task.status for task in replayed.tasks} == {
        "child-done": "completed",
        "child-closed": "shutdown",
        "child-error": "failed",
    }

    await projector.ingest(
        "thread/status/changed",
        {"threadId": "child-done", "status": {"type": "active"}},
    )
    assert {
        task.task_id: task.status for task in store.get_thread("parent").tasks  # type: ignore[union-attr]
    }["child-done"] == "inProgress"
    await projector.ingest(
        "thread/status/changed",
        {"threadId": "child-done", "status": {"type": "idle"}},
    )
    assert {
        task.task_id: task.status for task in store.get_thread("parent").tasks  # type: ignore[union-attr]
    }["child-done"] == "completed"
    await projector.ingest(
        "turn/completed",
        {
            "threadId": "parent",
            "turn": {
                "id": "turn-old",
                "status": "completed",
                "items": [
                    {
                        "type": "subAgentActivity",
                        "agentThreadId": "child-done",
                        "agentPath": "/root/child-done",
                        "kind": "started",
                    }
                ],
            },
        },
    )
    assert {
        task.task_id: task.status for task in store.get_thread("parent").tasks  # type: ignore[union-attr]
    }["child-done"] == "completed"
    store.close()


def test_complete_goal_finalizes_unresolved_tasks_without_rewriting_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    projector = managed_projector(store)
    monkeypatch.setattr(projector_module.time, "time", lambda: 500)
    state = ThreadState(
        thread_id="parent",
        plan=[PlanStep("Deploy", "inProgress")],
        tasks=[TaskState(task_id="missing-child", title="Old agent", status="inProgress")],
    )
    store.save_thread(state)

    projector.apply_goal(state, {"status": "complete", "objective": "Done"})

    persisted = store.get_thread("parent")
    assert persisted is not None
    assert persisted.tasks[0].status == "shutdown"
    assert persisted.tasks[0].finished_at == 500
    assert persisted.plan == [PlanStep("Deploy", "inProgress")]
    store.close()


@pytest.mark.asyncio
async def test_goal_progress_persists_without_republishing_dashboard(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    changes: list[tuple[str, str]] = []

    async def changed(state: ThreadState, reason: str) -> None:
        changes.append((str((state.goal or {}).get("status") or ""), reason))

    projector = managed_projector(store, changed)
    await projector.ingest(
        "thread/goal/updated",
        {
            "threadId": "thread-goal-progress",
            "goal": {
                "status": "active",
                "objective": "Finish",
                "tokensUsed": 10,
                "timeUsedSeconds": 1,
                "updatedAt": 100,
            },
        },
    )
    await projector.ingest(
        "thread/goal/updated",
        {
            "threadId": "thread-goal-progress",
            "goal": {
                "status": "active",
                "objective": "Finish",
                "tokensUsed": 20,
                "timeUsedSeconds": 2,
                "updatedAt": 101,
            },
        },
    )

    state = store.get_thread("thread-goal-progress")
    assert state is not None
    assert state.goal is not None and state.goal["tokensUsed"] == 20
    assert changes == [("active", "thread/goal/updated")]
    store.close()


@pytest.mark.asyncio
async def test_equal_timestamp_goal_update_cannot_regress_complete_state(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    changes: list[str] = []

    async def changed(state: ThreadState, _reason: str) -> None:
        changes.append(str((state.goal or {}).get("status") or ""))

    projector = managed_projector(store, changed)
    for status, updated_at in (("active", 100), ("complete", 101), ("active", 101)):
        await projector.ingest(
            "thread/goal/updated",
            {
                "threadId": "thread-goal-order",
                "goal": {
                    "status": status,
                    "objective": "Finish",
                    "updatedAt": updated_at,
                },
            },
        )

    complete = store.get_thread("thread-goal-order")
    assert complete is not None and complete.goal is not None
    assert complete.goal["status"] == "complete"
    assert changes == ["active", "complete"]

    await projector.ingest(
        "thread/goal/updated",
        {
            "threadId": "thread-goal-order",
            "goal": {"status": "active", "objective": "Resume", "updatedAt": 102},
        },
    )
    resumed = store.get_thread("thread-goal-order")
    assert resumed is not None and resumed.goal is not None
    assert resumed.goal["status"] == "active"
    assert changes == ["active", "complete", "active"]
    store.close()


def test_complete_goal_preserves_authoritative_active_child(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    projector = managed_projector(store)
    parent = ThreadState(
        thread_id="parent",
        tasks=[TaskState(task_id="active-child", title="Live agent", status="inProgress")],
    )
    child = ThreadState(
        thread_id="active-child",
        parent_thread_id="parent",
        status="active",
        created_at=100,
        updated_at=500,
    )
    store.save_thread(parent)
    store.save_thread(child)

    projector.apply_goal(parent, {"status": "complete", "objective": "Premature"})

    persisted = store.get_thread("parent")
    assert persisted is not None
    assert persisted.tasks[0].status == "inProgress"
    assert persisted.tasks[0].finished_at == 0
    assert persisted.agents_active == 1
    store.close()


def test_stale_child_snapshot_cannot_reopen_terminal_task(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    projector = managed_projector(store)
    parent = ThreadState(
        thread_id="parent",
        tasks=[
            TaskState(
                task_id="child",
                title="Finished agent",
                status="completed",
                started_at=100,
                finished_at=500,
                updated_at=500,
            )
        ],
    )
    child = ThreadState(
        thread_id="child",
        parent_thread_id="parent",
        status="idle",
        created_at=100,
        updated_at=500,
    )
    store.save_thread(parent)
    store.save_thread(child)

    projector.apply_thread(
        {
            "id": "child",
            "parentThreadId": "parent",
            "updatedAt": 300,
            "status": {"type": "active"},
        }
    )

    persisted_child = store.get_thread("child")
    persisted_parent = store.get_thread("parent")
    assert persisted_child is not None and persisted_child.status == "idle"
    assert persisted_parent is not None
    assert persisted_parent.tasks[0].status == "completed"
    assert persisted_parent.tasks[0].finished_at == 500

    projector.apply_thread(
        {
            "id": "child",
            "parentThreadId": "parent",
            "updatedAt": 600,
            "status": {"type": "active"},
        }
    )
    reopened = store.get_thread("parent")
    assert reopened is not None
    assert reopened.tasks[0].status == "inProgress"
    assert reopened.tasks[0].finished_at == 0
    store.close()


@pytest.mark.asyncio
async def test_subagent_protocol_metadata_is_linked_without_exposing_activity_payload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    projector = managed_projector(store)
    monkeypatch.setattr(projector_module.time, "time", lambda: 500)
    projector.apply_thread({"id": "parent", "status": {"type": "active"}})
    child = projector.apply_thread(
        {
            "id": "child-agent",
            "sessionId": "session-tree",
            "parentThreadId": "parent",
            "agentNickname": "Ada",
            "agentRole": "reviewer",
            "model": "gpt-5.6-luna",
            "reasoningEffort": "max",
            "ephemeral": False,
            "source": {
                "subAgent": {
                    "thread_spawn": {
                        "parent_thread_id": "parent",
                        "depth": 1,
                        "agent_path": "/root/reviewer",
                    }
                }
            },
            "status": {"type": "active"},
        }
    )

    assert child.is_subagent is True
    assert child.parent_thread_id == "parent"
    assert child.agent_path == "/root/reviewer"
    assert child.agent_nickname == "Ada"

    camel_child = projector.apply_thread(
        {
            "id": "child-camel",
            "source": {
                "subagent": {
                    "threadSpawn": {
                        "parentThreadId": "parent",
                        "agentPath": "/root/camel",
                        "agentNickname": "Grace",
                        "agentRole": "implementer",
                    }
                }
            },
            "status": {"type": "idle"},
        }
    )
    assert camel_child.parent_thread_id == "parent"
    assert camel_child.agent_path == "/root/camel"
    assert camel_child.agent_nickname == "Grace"
    assert camel_child.agent_role == "implementer"

    await projector.ingest(
        "item/started",
        {
            "threadId": "parent",
            "item": {
                "id": "collab-2",
                "type": "collabAgentToolCall",
                "tool": "spawnAgent",
                "status": "inProgress",
                "prompt": "Review the integration. Internal details must not become status text.",
                "receiverThreadIds": ["child-agent"],
                "agentsStates": {
                    "child-agent": {"status": "running", "message": "PRIVATE TOOL OUTPUT"}
                },
            },
        },
    )
    await projector.ingest(
        "item/completed",
        {
            "threadId": "parent",
            "item": {
                "id": "activity-1",
                "type": "subAgentActivity",
                "agentThreadId": "child-agent",
                "agentPath": "/root/reviewer",
                "kind": "interrupted",
            },
        },
    )

    parent = store.get_thread("parent")
    assert parent is not None
    assert len(parent.tasks) == 1
    task = parent.tasks[0]
    assert task.agent_nickname == "Ada"
    assert task.agent_role == "reviewer"
    assert task.agent_path == "/root/reviewer"
    assert task.model == "gpt-5.6-luna"
    assert task.reasoning_effort == "max"
    assert task.status == "interrupted"
    assert task.finished_at == 500

    ephemeral = projector.apply_thread(
        {"id": "ask-fork", "ephemeral": True, "forkedFromId": "parent", "status": {"type": "idle"}}
    )
    assert ephemeral.ephemeral is True
    assert ephemeral.is_subagent is False
    store.close()


@pytest.mark.asyncio
async def test_mixed_subagent_profiles_are_not_overwritten_by_group_activity(
    tmp_path: Path,
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    projector = managed_projector(store)
    projector.apply_thread({"id": "parent", "status": {"type": "active"}})
    projector.apply_thread(
        {
            "id": "child-luna",
            "parentThreadId": "parent",
            "model": "gpt-5.6-luna",
            "reasoningEffort": "max",
            "status": {"type": "active"},
        }
    )
    projector.apply_thread(
        {
            "id": "child-sol",
            "parentThreadId": "parent",
            "model": "gpt-5.6-sol",
            "reasoningEffort": "xhigh",
            "status": {"type": "active"},
        }
    )

    await projector.ingest(
        "item/started",
        {
            "threadId": "parent",
            "item": {
                "id": "group-message",
                "type": "collabAgentToolCall",
                "tool": "sendMessage",
                "status": "inProgress",
                "reasoningEffort": "low",
                "receiverThreadIds": ["child-luna"],
                "agentsStates": {
                    "child-luna": {"status": "running"},
                    "child-sol": {"status": "running"},
                },
            },
        },
    )

    parent = store.get_thread("parent")
    assert parent is not None
    profiles = {
        task.task_id: (task.model, task.reasoning_effort) for task in parent.tasks
    }
    assert profiles == {
        "child-luna": ("gpt-5.6-luna", "max"),
        "child-sol": ("gpt-5.6-sol", "xhigh"),
    }
    store.close()


@pytest.mark.asyncio
async def test_resume_snapshot_does_not_prove_mode_but_settings_notification_does(
    tmp_path: Path,
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    store.save_session_space(
        SessionSpace(
            space_id="space-mode", lifecycle="active", thread_id="thread-mode",
            desired_mode="plan", current_mode="plan", observed_mode="unknown",
        )
    )
    projector = managed_projector(store)
    projector.apply_thread(
        {"id": "thread-mode", "status": {"type": "idle"}, "collaborationMode": {"mode": "plan"}}
    )
    assert store.get_session_space("space-mode").observed_mode == "unknown"  # type: ignore[union-attr]

    await projector.ingest(
        "thread/settings/updated",
        {
            "threadId": "thread-mode",
            "threadSettings": {
                "collaborationMode": {
                    "mode": "plan",
                    "settings": {"model": "gpt-test", "reasoning_effort": "high"},
                }
            },
        },
    )
    space = store.get_session_space("space-mode")
    assert space is not None
    assert (space.desired_mode, space.observed_mode) == ("plan", "plan")
    store.close()
