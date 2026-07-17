from __future__ import annotations

import time
from typing import Any

import pytest
from telegram.request import HTTPXRequest

from codex_telegram_bridge.main import PollingSupervisor
from codex_telegram_bridge.telegram_common import (
    PollingHealth,
    PollingHealthRequest,
    build_application,
)


class FakeUpdater:
    def __init__(self) -> None:
        self.running = True
        self.events: list[Any] = []

    async def stop(self) -> None:
        self.running = False
        self.events.append("stop")

    async def start_polling(self, **kwargs: Any) -> None:
        self.running = True
        self.events.append(("start", kwargs))


class FakeApplication:
    def __init__(self) -> None:
        self.updater = FakeUpdater()


def test_polling_health_tracks_success_and_consecutive_failures() -> None:
    health = PollingHealth("control")
    health.mark_failure("TimedOut")
    health.mark_failure("TimedOut")

    assert health.consecutive_failures == 2
    assert health.failure_count == 2
    assert health.last_error_type == "TimedOut"
    assert health.stale_for(0.0)

    health.mark_success()

    assert health.consecutive_failures == 0
    assert not health.stale_for(1.0)


@pytest.mark.asyncio
async def test_polling_health_request_records_http_success_and_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    health = PollingHealth("control")

    async def successful_request(_self: HTTPXRequest, *_args: Any, **_kwargs: Any) -> tuple[int, bytes]:
        return 200, b"{}"

    monkeypatch.setattr(HTTPXRequest, "do_request", successful_request)
    request = PollingHealthRequest(health, connection_pool_size=2)
    await request.do_request("https://example.invalid", "POST")
    assert health.consecutive_failures == 0

    async def failed_request(_self: HTTPXRequest, *_args: Any, **_kwargs: Any) -> tuple[int, bytes]:
        raise TimeoutError

    monkeypatch.setattr(HTTPXRequest, "do_request", failed_request)
    with pytest.raises(TimeoutError):
        await request.do_request("https://example.invalid", "POST")
    assert health.consecutive_failures == 1
    assert health.last_error_type == "TimeoutError"
    await request.shutdown()


@pytest.mark.asyncio
async def test_build_application_uses_a_separate_healthful_polling_request() -> None:
    health = PollingHealth("control")
    application = build_application(
        "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi",
        health,
    )

    polling_request = application.bot._request[0]
    assert isinstance(polling_request, PollingHealthRequest)
    assert polling_request.read_timeout == 30.0
    await application.bot._request[0].shutdown()
    await application.bot._request[1].shutdown()


@pytest.mark.asyncio
async def test_polling_supervisor_restarts_only_stale_updater_without_dropping_pending() -> None:
    application = FakeApplication()
    health = PollingHealth("control")
    health.last_success_at = time.monotonic() - 100
    supervisor = PollingSupervisor(
        (application,),
        (health,),
        ("message", "callback_query"),
        stale_after=60.0,
        restart_cooldown=0.0,
    )

    await supervisor.check_once()

    assert application.updater.events == [
        "stop",
        (
            "start",
            {"allowed_updates": ["message", "callback_query"], "drop_pending_updates": False},
        ),
    ]
    assert not health.stale_for(1.0)


@pytest.mark.asyncio
async def test_polling_supervisor_retries_failed_restart_after_cooldown() -> None:
    health = PollingHealth("control")
    stale_since = time.monotonic() - 100
    health.last_success_at = stale_since

    class FailingStartUpdater(FakeUpdater):
        async def stop(self) -> None:
            await super().stop()
            health.mark_success()

        async def start_polling(self, **kwargs: Any) -> None:
            self.events.append(("start", kwargs))
            raise RuntimeError("bootstrap failed")

    application = FakeApplication()
    application.updater = FailingStartUpdater()
    supervisor = PollingSupervisor(
        (application,),
        (health,),
        ("message",),
        stale_after=60.0,
        restart_cooldown=30.0,
    )

    await supervisor.check_once()

    assert health.last_success_at == stale_since
    assert sum(event[0] == "start" for event in application.updater.events if isinstance(event, tuple)) == 1

    await supervisor.check_once()
    assert sum(event[0] == "start" for event in application.updater.events if isinstance(event, tuple)) == 1

    supervisor._cooldown_until[health.role] = time.monotonic() - 1
    await supervisor.check_once()
    assert sum(event[0] == "start" for event in application.updater.events if isinstance(event, tuple)) == 2


@pytest.mark.asyncio
async def test_polling_supervisor_does_not_restart_a_healthy_updater() -> None:
    application = FakeApplication()
    health = PollingHealth("control")
    supervisor = PollingSupervisor(
        (application,),
        (health,),
        ("message",),
        stale_after=60.0,
    )

    await supervisor.check_once()

    assert application.updater.events == []
