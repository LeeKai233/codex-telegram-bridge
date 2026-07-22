from __future__ import annotations

import asyncio

import pytest

from codex_telegram_bridge.workloads import (
    FILE_IO_SPACE,
    MAINTENANCE_SPACE,
    PROMPT_ACTION_SPACE,
    KeyedWorkScheduler,
)


@pytest.mark.asyncio
async def test_keyed_scheduler_preserves_per_key_order_and_allows_cross_key_progress() -> None:
    scheduler = KeyedWorkScheduler("test", max_pending=8, max_running=2)
    release = asyncio.Event()
    first_started = asyncio.Event()
    events: list[str] = []

    async def first() -> None:
        events.append("a1-start")
        first_started.set()
        await release.wait()
        events.append("a1-end")

    async def second() -> None:
        events.append("a2")

    async def other() -> None:
        events.append("b1")

    assert scheduler.submit("a", first)
    assert scheduler.submit("a", second)
    assert scheduler.submit("b", other)
    await first_started.wait()
    await asyncio.sleep(0)

    assert events == ["a1-start", "b1"]
    release.set()
    await scheduler.join()
    assert events == ["a1-start", "b1", "a1-end", "a2"]
    await scheduler.stop()


@pytest.mark.asyncio
async def test_keyed_scheduler_rejects_overflow_and_cancels_tracked_workers() -> None:
    scheduler = KeyedWorkScheduler("test", max_pending=1, max_running=1)
    started = asyncio.Event()

    async def blocked() -> None:
        started.set()
        await asyncio.Event().wait()

    assert scheduler.submit("a", blocked)
    assert not scheduler.submit("b", blocked)
    await started.wait()
    assert scheduler.snapshot()["pending"] == 1

    await scheduler.stop()

    assert scheduler.snapshot()["pending"] == 0
    assert scheduler.snapshot()["running_keys"] == 0


@pytest.mark.asyncio
async def test_scheduler_spaces_isolate_capacity_without_breaking_cross_space_key_order() -> None:
    scheduler = KeyedWorkScheduler(
        "test",
        max_pending=8,
        max_running=1,
        spaces=(FILE_IO_SPACE, PROMPT_ACTION_SPACE, MAINTENANCE_SPACE),
    )
    release = asyncio.Event()
    maintenance_started = asyncio.Event()
    file_started = asyncio.Event()
    events: list[str] = []

    async def first_for_key() -> None:
        events.append("key-maintenance-start")
        maintenance_started.set()
        await release.wait()
        events.append("key-maintenance-end")

    async def second_for_key() -> None:
        events.append("key-prompt")

    async def other_maintenance() -> None:
        events.append("other-maintenance")

    async def file_io() -> None:
        events.append("file-io")
        file_started.set()

    assert scheduler.submit("key", first_for_key, space=MAINTENANCE_SPACE)
    assert scheduler.submit("key", second_for_key, space=PROMPT_ACTION_SPACE)
    assert scheduler.submit("other-maintenance", other_maintenance, space=MAINTENANCE_SPACE)
    assert scheduler.submit("file", file_io, space=FILE_IO_SPACE)
    await maintenance_started.wait()
    await asyncio.wait_for(file_started.wait(), timeout=0.2)
    await asyncio.sleep(0)

    assert events == ["key-maintenance-start", "file-io"]
    snapshot = scheduler.snapshot()["spaces"]
    assert snapshot["maintenance"]["running"] == 1
    assert snapshot["file_io"]["running_capacity"] == 2
    assert snapshot["prompt_action"]["running_capacity"] == 4

    release.set()
    await scheduler.join()
    await scheduler.stop()

    assert events.index("key-maintenance-end") < events.index("key-prompt")
    assert "other-maintenance" in events
