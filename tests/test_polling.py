from __future__ import annotations

import time
from typing import Any, cast

import pytest
from telegram.request import HTTPXRequest

from codex_telegram_bridge.main import PollingSupervisor
from codex_telegram_bridge.telegram_common import (
    PollingHealth,
    PollingHealthRequest,
    build_application,
)


class FakeUpdater:
    def __init__(self, events: list[Any] | None = None) -> None:
        self.running = True
        self.events = events if events is not None else []

    async def stop(self) -> None:
        self.running = False
        self.events.append("stop")

    async def start_polling(self, **kwargs: Any) -> None:
        self.running = True
        self.events.append(("start", kwargs))


class FakePollingRequest:
    def __init__(self, events: list[Any], *, fail_initialize: bool = False) -> None:
        self.events = events
        self.fail_initialize = fail_initialize

    async def shutdown(self) -> None:
        self.events.append("request.shutdown")

    async def initialize(self) -> None:
        self.events.append("request.initialize")
        if self.fail_initialize:
            raise RuntimeError("initialize failed")


class FakeApplication:
    def __init__(self) -> None:
        self.events: list[Any] = []
        self.updater = FakeUpdater(self.events)


def attach_polling_request(
    application: FakeApplication,
    health: PollingHealth,
    *,
    fail_initialize: bool = False,
) -> FakePollingRequest:
    request = FakePollingRequest(application.events, fail_initialize=fail_initialize)
    health.polling_request = cast(Any, request)
    return request


def test_polling_health_tracks_success_and_consecutive_failures() -> None:
    health = PollingHealth("control")
    health.mark_failure("TimedOut")
    health.mark_failure("TimedOut")

    assert health.consecutive_failures == 2
    assert health.failure_count == 2
    assert health.last_error_type == "TimedOut"
    assert health.success_count == 0
    assert health.stale_for(0.0)

    health.mark_success()

    assert health.consecutive_failures == 0
    assert health.success_count == 1
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
    assert health.polling_request is request
    await request.do_request("https://example.invalid", "POST")
    assert health.consecutive_failures == 0
    assert health.success_count == 1

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
    limiter = application.bot.rate_limiter
    assert isinstance(polling_request, PollingHealthRequest)
    assert health.polling_request is polling_request
    assert polling_request.read_timeout == 30.0
    assert limiter is not None
    assert limiter._group_max_rate == 15
    assert limiter._group_time_period == 60
    assert limiter._max_retries == 0
    assert application.bot.request.read_timeout == 15.0
    assert application.bot.request._client.timeout.write == 15.0
    assert application.bot.request._client.timeout.connect == 10.0
    assert application.bot.request._client.timeout.pool == 5.0
    await application.bot._request[0].shutdown()
    await application.bot._request[1].shutdown()


@pytest.mark.asyncio
async def test_polling_supervisor_restarts_only_stale_updater_without_dropping_pending() -> None:
    application = FakeApplication()
    health = PollingHealth("control")
    attach_polling_request(application, health)
    health.last_success_at = time.monotonic() - 100
    supervisor = PollingSupervisor(
        (application,),
        (health,),
        ("message", "callback_query"),
        stale_after=60.0,
        restart_cooldown=0.0,
    )

    await supervisor.check_once()

    assert application.events == [
        "stop",
        "request.shutdown",
        "request.initialize",
        (
            "start",
            {"allowed_updates": ["message", "callback_query"], "drop_pending_updates": False},
        ),
    ]
    assert not health.stale_for(1.0)
    assert not supervisor.fatal_event.is_set()

    health.mark_success()
    await supervisor.check_once()

    assert health.role not in supervisor._recovery_pending


@pytest.mark.asyncio
async def test_polling_supervisor_retries_locally_when_restart_fails() -> None:
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
    application.updater = FailingStartUpdater(application.events)
    attach_polling_request(application, health)
    supervisor = PollingSupervisor(
        (application,),
        (health,),
        ("message",),
        stale_after=60.0,
        restart_cooldown=30.0,
    )

    await supervisor.check_once()

    assert health.last_success_at == stale_since
    assert not supervisor.fatal_event.is_set()
    assert supervisor.fatal_error is None
    assert supervisor.recovery_failures == {"control": 1}
    assert supervisor.last_recovery_error == {"control": "restart_failed:RuntimeError"}
    assert application.events == [
        "stop",
        "request.shutdown",
        "request.initialize",
        ("start", {"allowed_updates": ["message"], "drop_pending_updates": False}),
    ]


@pytest.mark.asyncio
async def test_polling_supervisor_retries_locally_when_transport_recycle_fails() -> None:
    application = FakeApplication()
    health = PollingHealth("control")
    attach_polling_request(application, health, fail_initialize=True)
    stale_since = time.monotonic() - 100
    health.last_success_at = stale_since
    supervisor = PollingSupervisor(
        (application,),
        (health,),
        ("message",),
        stale_after=60.0,
    )

    await supervisor.check_once()

    assert health.last_success_at == stale_since
    assert not supervisor.fatal_event.is_set()
    assert supervisor.recovery_failures == {"control": 1}
    assert application.events == ["stop", "request.shutdown", "request.initialize"]


@pytest.mark.asyncio
async def test_polling_supervisor_retries_locally_after_recovery_verification_timeout() -> None:
    application = FakeApplication()
    health = PollingHealth("control")
    attach_polling_request(application, health)
    health.last_success_at = time.monotonic() - 100
    supervisor = PollingSupervisor(
        (application,),
        (health,),
        ("message",),
        stale_after=60.0,
        recovery_verify_after=45.0,
    )

    await supervisor.check_once()
    success_count, _deadline = supervisor._recovery_pending[health.role]
    supervisor._recovery_pending[health.role] = (success_count, time.monotonic() - 1)
    await supervisor.check_once()

    assert not supervisor.fatal_event.is_set()
    assert health.role not in supervisor._recovery_pending
    assert supervisor.recovery_failures == {"control": 1}
    assert supervisor.last_recovery_error == {
        "control": "verification_timeout:PollingRecoveryVerificationTimeout"
    }


@pytest.mark.asyncio
async def test_polling_supervisor_can_recover_on_retry_after_local_transport_failure() -> None:
    application = FakeApplication()
    health = PollingHealth("control")
    request = attach_polling_request(application, health, fail_initialize=True)
    health.last_success_at = time.monotonic() - 100
    supervisor = PollingSupervisor(
        (application,),
        (health,),
        ("message",),
        stale_after=60.0,
        restart_cooldown=0.0,
    )

    await supervisor.check_once()
    request.fail_initialize = False
    await supervisor.check_once()

    assert not supervisor.fatal_event.is_set()
    assert health.role in supervisor._recovery_pending
    assert application.events == [
        "stop",
        "request.shutdown",
        "request.initialize",
        "request.shutdown",
        "request.initialize",
        ("start", {"allowed_updates": ["message"], "drop_pending_updates": False}),
    ]


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


@pytest.mark.asyncio
async def test_polling_supervisor_isolates_healthy_discussion_bot() -> None:
    control = FakeApplication()
    discussion = FakeApplication()
    control_health = PollingHealth("control")
    discussion_health = PollingHealth("discussion")
    attach_polling_request(control, control_health)
    attach_polling_request(discussion, discussion_health)
    control_health.last_success_at = time.monotonic() - 100
    supervisor = PollingSupervisor(
        (control, discussion),
        (control_health, discussion_health),
        ("message",),
        stale_after=60.0,
    )

    await supervisor.check_once()

    assert control.events
    assert discussion.events == []
