from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any

import pytest
from telegram.error import BadRequest, RetryAfter, TimedOut

from codex_telegram_bridge.delivery import (
    DeliveryIntent,
    DeliveryKey,
    TelegramDeliveryEngine,
)


class RecordingEndpoint:
    def __init__(self, behaviors: list[BaseException | Awaitable[None] | None] | None = None) -> None:
        self.behaviors = list(behaviors or [])
        self.calls: list[tuple[int, int, str, int]] = []

    async def edit_text(
        self,
        chat_id: int,
        message_id: int,
        markdown: str,
        *,
        priority: int,
        **_kwargs: Any,
    ) -> None:
        self.calls.append((chat_id, message_id, markdown, priority))
        behavior = self.behaviors.pop(0) if self.behaviors else None
        if isinstance(behavior, BaseException):
            raise behavior
        if behavior is not None:
            await behavior


def intent(
    text: str,
    *,
    fingerprint: str | None = None,
    terminal: bool = False,
) -> DeliveryIntent:
    return DeliveryIntent(
        key=DeliveryKey("discussion", -100123, 390),
        markdown=text,
        plain=text,
        fingerprint=fingerprint or text,
        priority=5 if terminal else 10,
        terminal=terminal,
        context="test",
    )


@pytest.mark.asyncio
async def test_delivery_engine_keeps_only_latest_intent_per_target() -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def block() -> None:
        started.set()
        await release.wait()

    endpoint = RecordingEndpoint([block()])
    engine = TelegramDeliveryEngine({"discussion": endpoint})  # type: ignore[dict-item]
    engine.start()
    first = engine.submit(intent("old"))
    await asyncio.wait_for(started.wait(), timeout=1)
    latest = engine.submit(intent("latest"))
    release.set()

    assert (await asyncio.wait_for(first, timeout=1)).status == "superseded"
    assert (await asyncio.wait_for(latest, timeout=1)).status == "delivered"
    assert [call[2] for call in endpoint.calls] == ["old", "latest"]
    await engine.stop(drain_timeout=0)


@pytest.mark.asyncio
async def test_delivery_engine_deduplicates_by_semantic_fingerprint() -> None:
    endpoint = RecordingEndpoint()
    engine = TelegramDeliveryEngine({"discussion": endpoint})  # type: ignore[dict-item]
    engine.start()

    delivered = await engine.submit(intent("moon-one", fingerprint="same-state"))
    duplicate = await engine.submit(intent("moon-two", fingerprint="same-state"))

    assert delivered.performed is True
    assert duplicate.performed is False
    assert [call[2] for call in endpoint.calls] == ["moon-one"]
    await engine.stop(drain_timeout=0)


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RetryAfter(0), TimedOut()])
async def test_delivery_engine_does_not_add_a_second_retry_layer(
    error: BaseException,
) -> None:
    endpoint = RecordingEndpoint([error, None])
    engine = TelegramDeliveryEngine({"discussion": endpoint})  # type: ignore[dict-item]
    engine.start()

    outcome = await asyncio.wait_for(engine.submit(intent("final", terminal=True)), timeout=1)

    assert outcome.status == "transient_failure"
    assert outcome.attempts == 1
    assert len(endpoint.calls) == 1
    await engine.stop(drain_timeout=0)


@pytest.mark.asyncio
async def test_delivery_engine_stops_retrying_permanent_errors() -> None:
    endpoint = RecordingEndpoint([BadRequest("message to edit not found")])
    engine = TelegramDeliveryEngine({"discussion": endpoint})  # type: ignore[dict-item]
    engine.start()

    outcome = await asyncio.wait_for(engine.submit(intent("missing", terminal=True)), timeout=1)

    assert outcome.status == "permanent_failure"
    assert outcome.attempts == 1
    assert len(endpoint.calls) == 1
    await engine.stop(drain_timeout=0)
