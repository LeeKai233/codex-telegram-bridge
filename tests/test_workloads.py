from __future__ import annotations

import asyncio

import pytest

from codex_telegram_bridge.workloads import KeyedWorkScheduler


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
