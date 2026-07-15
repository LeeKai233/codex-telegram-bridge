from __future__ import annotations

from pathlib import Path

import pytest

import codex_telegram_bridge.projector as projector_module
from codex_telegram_bridge.projector import EventProjector
from codex_telegram_bridge.store import Store


@pytest.mark.asyncio
async def test_repeated_status_transition_is_processed_after_an_intervening_event(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    changes: list[tuple[str, str]] = []

    async def changed(state, reason: str) -> None:  # type: ignore[no-untyped-def]
        changes.append((state.status, reason))

    projector = EventProjector(store, changed)
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


def test_apply_thread_projects_server_timestamps_and_turn_durations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    projector = EventProjector(store)
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
    projector = EventProjector(store)
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
    assert (state.agents_active, state.agents_completed, state.agents_failed) == (1, 1, 1)
    assert state.recent_activity[-1].kind == "collabAgentToolCall"
    store.close()
