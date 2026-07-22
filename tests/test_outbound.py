from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

import pytest
from telegram.error import BadRequest, RetryAfter, TimedOut

from codex_telegram_bridge.outbound import OutboundMessenger, TelegramOutcomeUncertain
from codex_telegram_bridge.store import Store


class FailingJournal:
    def __init__(self) -> None:
        self.updates = 0

    def create_outbound_intent(self, **_kwargs: object) -> str:
        return "intent-1"

    def update_outbound_intent(self, _intent_id: str, **_kwargs: object) -> None:
        self.updates += 1
        raise OSError("journal unavailable")


def test_private_chat_scheduler_default_interval() -> None:
    assert OutboundMessenger().minimum_interval == pytest.approx(1.05)


@pytest.mark.asyncio
async def test_scheduler_enforces_configured_interval() -> None:
    messenger = OutboundMessenger(minimum_interval=0.05)
    called_at: list[float] = []

    async def operation() -> int:
        called_at.append(time.monotonic())
        return len(called_at)

    messenger.start()
    try:
        assert await messenger.call(operation) == 1
        assert await messenger.call(operation) == 2
    finally:
        await messenger.stop()

    assert called_at[1] - called_at[0] >= 0.04


@pytest.mark.asyncio
async def test_worker_logs_only_exception_type(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "123456789:SECRET_TOKEN"
    request_url = f"https://api.telegram.invalid/bot{secret}/sendMessage"
    messenger = OutboundMessenger(minimum_interval=0, retries=0)

    async def operation() -> None:
        raise RuntimeError(request_url)

    messenger.start()
    try:
        with (
            caplog.at_level(logging.ERROR, logger="codex_telegram_bridge.outbound"),
            pytest.raises(RuntimeError),
        ):
            await messenger.call(operation)
    finally:
        await messenger.stop()

    assert "RuntimeError" in caplog.text
    assert secret not in caplog.text
    assert request_url not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)


@pytest.mark.asyncio
async def test_scheduler_retries_timed_out_requests_with_bounded_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def operation() -> str:
        nonlocal calls
        calls += 1
        if calls < 4:
            raise TimedOut
        return "delivered"

    messenger = OutboundMessenger(minimum_interval=0, retries=3)
    monkeypatch.setattr(messenger, "_retry_delay", lambda _attempt: 0.0)
    messenger.start()
    try:
        assert await messenger.call(operation) == "delivered"
    finally:
        await messenger.stop()

    assert calls == 4


@pytest.mark.asyncio
async def test_scheduler_bounds_retry_after_responses() -> None:
    calls = 0

    async def operation() -> None:
        nonlocal calls
        calls += 1
        raise RetryAfter(0)

    messenger = OutboundMessenger(minimum_interval=0, retries=1)
    messenger.start()
    try:
        with pytest.raises(RetryAfter):
            await asyncio.wait_for(messenger.call(operation), timeout=1)
    finally:
        await messenger.stop()

    assert calls == 2


@pytest.mark.asyncio
async def test_journal_failure_cannot_stall_successful_delivery() -> None:
    journal = FailingJournal()
    messenger = OutboundMessenger(minimum_interval=0, journal=journal)  # type: ignore[arg-type]
    messenger.start()
    try:
        assert await asyncio.wait_for(
            messenger.call(
                lambda: asyncio.sleep(0, result="delivered"),
                semantics="non_idempotent",
            ),
            timeout=1,
        ) == "delivered"
        assert await asyncio.wait_for(
            messenger.call(lambda: asyncio.sleep(0, result="next")),
            timeout=1,
        ) == "next"
    finally:
        await messenger.stop()

    assert journal.updates == 1


@pytest.mark.asyncio
async def test_journal_failure_cannot_break_retry_scheduling() -> None:
    journal = FailingJournal()
    calls = 0

    async def operation() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RetryAfter(0) from None
        return "delivered"

    messenger = OutboundMessenger(minimum_interval=0, retries=1, journal=journal)  # type: ignore[arg-type]
    messenger.start()
    try:
        assert await asyncio.wait_for(
            messenger.call(operation, semantics="non_idempotent"),
            timeout=1,
        ) == "delivered"
    finally:
        await messenger.stop()

    assert calls == 2
    assert journal.updates == 2


@pytest.mark.asyncio
async def test_scheduler_rejects_work_before_durable_intent_when_queue_is_full() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    journal = FailingJournal()

    async def blocker() -> None:
        started.set()
        await release.wait()

    messenger = OutboundMessenger(
        minimum_interval=0,
        journal=journal,  # type: ignore[arg-type]
        max_queue_size=1,
    )
    messenger.start()
    active = asyncio.create_task(messenger.call(blocker))
    await started.wait()
    queued = asyncio.create_task(messenger.call(lambda: asyncio.sleep(0)))
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="1-item limit"):
        await messenger.call(
            lambda: asyncio.sleep(0),
            semantics="non_idempotent",
        )
    release.set()
    await asyncio.gather(active, queued)
    await messenger.stop()

    assert journal.updates == 0


@pytest.mark.asyncio
async def test_stop_releases_active_and_queued_callers() -> None:
    started = asyncio.Event()

    async def blocker() -> None:
        started.set()
        await asyncio.Event().wait()

    messenger = OutboundMessenger(minimum_interval=0)
    messenger.start()
    active = asyncio.create_task(messenger.call(blocker))
    await started.wait()
    queued = asyncio.create_task(messenger.call(lambda: asyncio.sleep(0)))
    await asyncio.sleep(0)

    await messenger.stop()

    with pytest.raises(RuntimeError, match="stopping"):
        await asyncio.wait_for(active, timeout=1)
    with pytest.raises(RuntimeError, match="stopping"):
        await asyncio.wait_for(queued, timeout=1)


@pytest.mark.asyncio
async def test_stop_is_bounded_when_operation_swallows_cancellation() -> None:
    started = asyncio.Event()
    cancellation_seen = asyncio.Event()
    release = asyncio.Event()

    async def stubborn_operation() -> None:
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancellation_seen.set()
            await release.wait()

    messenger = OutboundMessenger(minimum_interval=0)
    messenger.start()
    caller = asyncio.create_task(messenger.call(stubborn_operation))
    await started.wait()

    await asyncio.wait_for(messenger.stop(), timeout=1.5)

    assert cancellation_seen.is_set()
    with pytest.raises(RuntimeError, match="stopping"):
        await asyncio.wait_for(caller, timeout=1)
    release.set()
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_stale_worker_cannot_requeue_into_restarted_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    cancellation_seen = asyncio.Event()
    release = asyncio.Event()
    operation_finished = asyncio.Event()
    calls = 0

    async def stale_operation() -> None:
        nonlocal calls
        calls += 1
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            cancellation_seen.set()
            await release.wait()
            operation_finished.set()
            raise RetryAfter(0) from None

    monkeypatch.setattr(
        "codex_telegram_bridge.outbound.OUTBOUND_STOP_GRACE_SECONDS",
        0.01,
    )
    messenger = OutboundMessenger(minimum_interval=0, retries=1)
    messenger.start()
    old_worker = messenger._task
    assert old_worker is not None
    caller = asyncio.create_task(messenger.call(stale_operation))
    await started.wait()
    await messenger.stop()
    assert cancellation_seen.is_set()
    with pytest.raises(RuntimeError, match="stopping"):
        await caller

    messenger.start()
    release.set()
    await asyncio.wait_for(operation_finished.wait(), timeout=1)
    await asyncio.wait_for(asyncio.shield(old_worker), timeout=1)

    assert calls == 1
    assert sum(messenger.snapshot()["queues"].values()) == 0
    await messenger.stop()


@pytest.mark.asyncio
async def test_stale_worker_cannot_mutate_restarted_transport_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    operation_finished = asyncio.Event()

    async def stale_operation() -> None:
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            await release.wait()
            operation_finished.set()
            raise TimedOut("stale transport failure") from None

    recycle_calls = 0

    async def recycle_transport() -> None:
        nonlocal recycle_calls
        recycle_calls += 1

    monkeypatch.setattr(
        "codex_telegram_bridge.outbound.OUTBOUND_STOP_GRACE_SECONDS",
        0.01,
    )
    messenger = OutboundMessenger(
        minimum_interval=0,
        retries=1,
        recycle_transport=recycle_transport,
    )
    messenger.start()
    old_worker = messenger._task
    assert old_worker is not None
    caller = asyncio.create_task(messenger.call(stale_operation))
    await started.wait()
    await messenger.stop()
    with pytest.raises(RuntimeError, match="stopping"):
        await caller

    messenger.start()
    messenger._consecutive_transport_failures = 0
    initial_last_request = messenger._last_request
    release.set()
    await asyncio.wait_for(operation_finished.wait(), timeout=1)
    await asyncio.wait_for(asyncio.shield(old_worker), timeout=1)

    assert messenger._consecutive_transport_failures == 0
    assert messenger._last_request == initial_last_request
    assert recycle_calls == 0
    assert sum(messenger.snapshot()["queues"].values()) == 0
    await messenger.stop()


@pytest.mark.asyncio
async def test_non_idempotent_ambiguous_timeout_is_not_retried(tmp_path: Path) -> None:
    calls = 0

    async def operation() -> None:
        nonlocal calls
        calls += 1
        error = TimedOut("read timed out")
        error.__cause__ = TimeoutError("response not received")
        raise error

    store = Store(tmp_path / "state.sqlite3")
    messenger = OutboundMessenger(minimum_interval=0, retries=3, journal=store)
    messenger.start()
    try:
        with pytest.raises(TelegramOutcomeUncertain):
            await messenger.call(
                operation,
                semantics="non_idempotent",
                audit={"operation": "sendMessage", "chat_id": 20, "payload_fingerprint": "abc"},
            )
    finally:
        await messenger.stop()

    assert calls == 1
    intents = store.outbound_intents(status="uncertain")
    assert len(intents) == 1
    assert intents[0]["operation"] == "sendMessage"
    assert intents[0]["attempts"] == 1
    store.close()


@pytest.mark.asyncio
async def test_non_idempotent_known_unsent_pool_timeout_can_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PoolTimeout(Exception):
        pass

    calls = 0

    async def operation() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            error = TimedOut("Request was *not* sent to Telegram")
            error.__cause__ = PoolTimeout()
            raise error
        return "delivered"

    messenger = OutboundMessenger(minimum_interval=0, retries=3)
    monkeypatch.setattr(messenger, "_retry_delay", lambda _attempt: 0.0)
    messenger.start()
    try:
        assert await messenger.call(operation, semantics="non_idempotent") == "delivered"
    finally:
        await messenger.stop()

    assert calls == 2


@pytest.mark.asyncio
async def test_scheduler_does_not_retry_permanent_bad_request() -> None:
    calls = 0

    async def operation() -> None:
        nonlocal calls
        calls += 1
        raise BadRequest("message to edit not found")

    messenger = OutboundMessenger(minimum_interval=0, retries=3)
    messenger.start()
    try:
        with pytest.raises(BadRequest):
            await messenger.call(operation)
    finally:
        await messenger.stop()

    assert calls == 1


@pytest.mark.asyncio
async def test_interactive_lane_is_not_starved_by_maintenance_backlog() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    order: list[str] = []

    async def blocker() -> None:
        order.append("blocker")
        started.set()
        await release.wait()

    async def record(name: str) -> None:
        order.append(name)

    messenger = OutboundMessenger(minimum_interval=0)
    messenger.start()
    first = asyncio.create_task(messenger.call(blocker, lane="live"))
    await started.wait()
    maintenance = [
        asyncio.create_task(
            messenger.call(lambda name=f"maintenance-{index}": record(name), lane="maintenance")
        )
        for index in range(6)
    ]
    interactive = asyncio.create_task(
        messenger.call(lambda: record("interactive"), lane="interactive")
    )
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, interactive, *maintenance)
    await messenger.stop()

    assert order.index("interactive") <= 2


@pytest.mark.asyncio
async def test_maintenance_chat_pacing_preserves_interactive_latency() -> None:
    messenger = OutboundMessenger(
        minimum_interval=0,
        maintenance_chat_interval=0.05,
        group_chat_interval=0,
    )
    events: list[tuple[str, float]] = []

    async def record(name: str) -> None:
        events.append((name, time.monotonic()))

    messenger.start()
    await messenger.call(
        lambda: record("maintenance-1"),
        lane="maintenance",
        traffic_class="maintenance",
        chat_key="chat:20",
    )
    second = asyncio.create_task(
        messenger.call(
            lambda: record("maintenance-2"),
            lane="maintenance",
            traffic_class="maintenance",
            chat_key="chat:20",
        )
    )
    interactive = asyncio.create_task(
        messenger.call(
            lambda: record("interactive"),
            lane="interactive",
            traffic_class="interactive",
            chat_key="chat:20",
        )
    )

    await asyncio.wait_for(interactive, timeout=0.04)
    await second
    await messenger.stop()

    assert [name for name, _timestamp in events] == [
        "maintenance-1",
        "interactive",
        "maintenance-2",
    ]
    assert events[2][1] - events[0][1] >= 0.04


@pytest.mark.asyncio
async def test_group_chat_pacing_survives_messenger_restart() -> None:
    class RateStore:
        def __init__(self) -> None:
            self.values: dict[str, float] = {}

        def get_meta(self, key: str, default: Any = None) -> Any:
            return self.values.get(key, default)

        def set_meta(self, key: str, value: str | int | float | bool) -> None:
            self.values[key] = float(value)

    store = RateStore()
    first = OutboundMessenger(
        journal=store,  # type: ignore[arg-type]
        maintenance_chat_interval=0,
        group_chat_interval=0.05,
    )
    first.start()
    await first.call(lambda: asyncio.sleep(0), chat_key="chat:-10020")
    await first.stop()

    restarted = OutboundMessenger(
        journal=store,  # type: ignore[arg-type]
        maintenance_chat_interval=0,
        group_chat_interval=0.05,
    )
    restarted.start()
    started_at = time.monotonic()
    await restarted.call(lambda: asyncio.sleep(0), chat_key="chat:-10020")
    elapsed = time.monotonic() - started_at
    await restarted.stop()

    assert elapsed >= 0.04
    assert store.values.keys() == {"telegram-group-rate:control:-10020"}


@pytest.mark.asyncio
async def test_default_scheduler_runs_each_traffic_class_at_its_own_concurrency() -> None:
    messenger = OutboundMessenger()
    release = asyncio.Event()
    all_started = asyncio.Event()
    started = 0

    async def blocked() -> None:
        nonlocal started
        started += 1
        if started == 8:
            all_started.set()
        await release.wait()

    messenger.start()
    tasks = [
        *(
            asyncio.create_task(
                messenger.call(
                    blocked,
                    traffic_class="callback_ack",
                    chat_key="chat:20",
                )
            )
            for _ in range(4)
        ),
        *(asyncio.create_task(messenger.call(blocked, chat_key=f"chat:{chat_id}")) for chat_id in (1, 2)),
        asyncio.create_task(messenger.call(blocked, traffic_class="media")),
        asyncio.create_task(messenger.call(blocked, traffic_class="maintenance")),
    ]
    try:
        await asyncio.wait_for(all_started.wait(), timeout=0.2)
        snapshot = messenger.snapshot()["traffic_classes"]
        assert {name: values["active"] for name, values in snapshot.items()} == {
            "callback_ack": 4,
            "interactive": 2,
            "media": 1,
            "maintenance": 1,
        }
        assert {name: values["concurrency"] for name, values in snapshot.items()} == {
            "callback_ack": 4,
            "interactive": 2,
            "media": 1,
            "maintenance": 1,
        }
    finally:
        release.set()
        await asyncio.gather(*tasks)
        await messenger.stop()


@pytest.mark.asyncio
async def test_interactive_traffic_preserves_chat_fifo_while_other_chat_progresses() -> None:
    messenger = OutboundMessenger()
    release = asyncio.Event()
    first_started = asyncio.Event()
    other_started = asyncio.Event()
    events: list[str] = []

    async def first() -> None:
        events.append("same-1-start")
        first_started.set()
        await release.wait()
        events.append("same-1-end")

    async def second() -> None:
        events.append("same-2")

    async def other() -> None:
        events.append("other")
        other_started.set()

    messenger.start()
    first_task = asyncio.create_task(messenger.call(first, chat_key="chat:1"))
    await first_started.wait()
    second_task = asyncio.create_task(messenger.call(second, chat_key="chat:1"))
    other_task = asyncio.create_task(messenger.call(other, chat_key="chat:2"))
    await asyncio.wait_for(other_started.wait(), timeout=0.2)
    await asyncio.sleep(0)
    assert events == ["same-1-start", "other"]

    release.set()
    await asyncio.gather(first_task, second_task, other_task)
    await messenger.stop()

    assert events == ["same-1-start", "other", "same-1-end", "same-2"]


@pytest.mark.asyncio
async def test_interactive_retry_cannot_be_overtaken_within_chat() -> None:
    messenger = OutboundMessenger(retries=1)
    events: list[str] = []
    attempts = 0

    async def first() -> None:
        nonlocal attempts
        attempts += 1
        events.append(f"first-{attempts}")
        if attempts == 1:
            raise RetryAfter(0)

    async def second() -> None:
        events.append("second")

    async def other() -> None:
        events.append("other")

    messenger.start()
    tasks = [
        asyncio.create_task(messenger.call(first, chat_key="chat:1")),
        asyncio.create_task(messenger.call(second, chat_key="chat:1")),
        asyncio.create_task(messenger.call(other, chat_key="chat:2")),
    ]
    await asyncio.gather(*tasks)
    await messenger.stop()

    assert events == ["first-1", "other", "first-2", "second"]


@pytest.mark.asyncio
async def test_callback_ack_bypasses_blocked_interactive_work_for_same_chat() -> None:
    messenger = OutboundMessenger()
    interactive_started = asyncio.Event()
    callback_started = asyncio.Event()
    release = asyncio.Event()

    async def interactive() -> None:
        interactive_started.set()
        await release.wait()

    async def callback() -> None:
        callback_started.set()

    messenger.start()
    interactive_task = asyncio.create_task(
        messenger.call(interactive, traffic_class="interactive", chat_key="chat:20")
    )
    await interactive_started.wait()
    callback_task = asyncio.create_task(
        messenger.call(callback, traffic_class="callback_ack", chat_key="callback:1")
    )
    await asyncio.wait_for(callback_started.wait(), timeout=0.2)

    release.set()
    await asyncio.gather(interactive_task, callback_task)
    await messenger.stop()


@pytest.mark.asyncio
async def test_idempotent_edit_coalesces_while_queued_and_reports_health() -> None:
    messenger = OutboundMessenger(minimum_interval=0, group_chat_interval=0)
    release = asyncio.Event()
    both_started = asyncio.Event()
    started = 0
    performed: list[str] = []

    async def blocker() -> None:
        nonlocal started
        started += 1
        if started == 2:
            both_started.set()
        await release.wait()

    async def record(value: str) -> None:
        performed.append(value)

    key = ("discussion", -10020, 41)
    messenger.start()
    blockers = [asyncio.create_task(messenger.call(blocker)) for _ in range(2)]
    await both_started.wait()
    old = asyncio.create_task(
        messenger.call(lambda: record("old"), coalesce_key=key, chat_key="chat:-10020")
    )
    await asyncio.sleep(0.01)
    latest = asyncio.create_task(
        messenger.call(lambda: record("latest"), coalesce_key=key, chat_key="chat:-10020")
    )
    with pytest.raises(asyncio.CancelledError):
        await old

    health = messenger.snapshot()
    assert health["coalesced"] == 1
    assert health["superseded"] == 1
    assert health["oldest_queued_seconds"] > 0
    assert health["traffic_classes"]["interactive"]["coalesced"] == 1
    assert health["traffic_classes"]["interactive"]["superseded"] == 1

    release.set()
    await asyncio.gather(*blockers, latest)
    await messenger.stop()

    assert performed == ["latest"]


@pytest.mark.asyncio
async def test_idempotent_edit_coalesces_during_throttle_and_retry_waits() -> None:
    messenger = OutboundMessenger(
        minimum_interval=0,
        retries=1,
        maintenance_chat_interval=0.2,
        group_chat_interval=0,
    )
    performed: list[str] = []
    throttle_key = ("discussion", 20, 41)
    retry_key = ("discussion", 20, 42)

    async def retrying() -> None:
        performed.append("retry-attempt")
        raise RetryAfter(30)

    async def record(value: str) -> None:
        performed.append(value)

    messenger.start()
    await messenger.call(
        lambda: asyncio.sleep(0),
        lane="maintenance",
        chat_key="chat:20",
    )
    throttled = asyncio.create_task(
        messenger.call(
            lambda: record("throttled-old"),
            lane="maintenance",
            chat_key="chat:20",
            coalesce_key=throttle_key,
        )
    )
    for _ in range(100):
        if messenger.snapshot()["traffic_classes"]["maintenance"]["active"]:
            break
        await asyncio.sleep(0.001)
    throttle_latest = asyncio.create_task(
        messenger.call(
            lambda: record("throttle-latest"),
            lane="maintenance",
            chat_key="chat:20",
            coalesce_key=throttle_key,
        )
    )
    with pytest.raises(asyncio.CancelledError):
        await throttled
    await asyncio.wait_for(throttle_latest, timeout=1)

    retrying_task = asyncio.create_task(
        messenger.call(retrying, chat_key="chat:20", coalesce_key=retry_key)
    )
    for _ in range(100):
        if messenger.snapshot()["traffic_classes"]["interactive"]["retries"]:
            break
        await asyncio.sleep(0.001)
    retry_latest = asyncio.create_task(
        messenger.call(
            lambda: record("retry-latest"),
            chat_key="chat:20",
            coalesce_key=retry_key,
        )
    )
    with pytest.raises(asyncio.CancelledError):
        await retrying_task
    await asyncio.wait_for(retry_latest, timeout=1)
    await messenger.stop()

    assert "throttled-old" not in performed
    assert performed.count("retry-attempt") == 1
    assert performed[-1] == "retry-latest"


@pytest.mark.asyncio
async def test_coalescing_preserves_in_transport_and_non_idempotent_operations() -> None:
    messenger = OutboundMessenger(minimum_interval=0, group_chat_interval=0)
    transport_started = asyncio.Event()
    release_transport = asyncio.Event()
    performed: list[str] = []
    key = ("discussion", 20, 41)

    async def in_transport() -> None:
        performed.append("transport-old")
        transport_started.set()
        await release_transport.wait()

    async def record(value: str) -> None:
        performed.append(value)

    messenger.start()
    old = asyncio.create_task(
        messenger.call(in_transport, chat_key="chat:20", coalesce_key=key)
    )
    await transport_started.wait()
    latest = asyncio.create_task(
        messenger.call(lambda: record("transport-latest"), chat_key="chat:20", coalesce_key=key)
    )
    await asyncio.sleep(0)
    assert not old.done()
    assert messenger.snapshot()["superseded"] == 0
    release_transport.set()
    await asyncio.gather(old, latest)

    non_idempotent = [
        asyncio.create_task(
            messenger.call(
                lambda value=value: record(value),
                semantics="non_idempotent",
                coalesce_key=key,
            )
        )
        for value in ("send-one", "send-two")
    ]
    await asyncio.gather(*non_idempotent)
    await messenger.stop()

    assert performed == ["transport-old", "transport-latest", "send-one", "send-two"]
