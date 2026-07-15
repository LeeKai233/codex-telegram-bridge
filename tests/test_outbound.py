from __future__ import annotations

import logging
import time

import pytest

from codex_telegram_bridge.outbound import OutboundMessenger


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
