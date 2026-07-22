from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import time
from dataclasses import dataclass
from typing import Literal

from telegram import InlineKeyboardMarkup
from telegram.error import BadRequest, Forbidden, TelegramError

from .telegram_common import TelegramEndpoint

LOGGER = logging.getLogger(__name__)

DeliveryStatus = Literal[
    "delivered",
    "superseded",
    "transient_failure",
    "permanent_failure",
]


@dataclass(frozen=True, slots=True)
class DeliveryKey:
    bot_role: str
    chat_id: int
    message_id: int


@dataclass(frozen=True, slots=True)
class DeliveryIntent:
    key: DeliveryKey
    markdown: str
    plain: str
    fingerprint: str
    reply_markup: InlineKeyboardMarkup | None = None
    priority: int = 10
    terminal: bool = False
    context: str = ""


@dataclass(frozen=True, slots=True)
class DeliveryOutcome:
    key: DeliveryKey
    revision: int
    status: DeliveryStatus
    attempts: int
    performed: bool


@dataclass(slots=True)
class _PendingDelivery:
    intent: DeliveryIntent
    revision: int
    future: asyncio.Future[DeliveryOutcome]
    attempts: int
    due_at: float


def delivery_fingerprint(
    markdown: str,
    plain: str,
    reply_markup: InlineKeyboardMarkup | None = None,
) -> str:
    markup = reply_markup.to_dict() if reply_markup is not None else None
    raw = json.dumps(
        [markdown, plain, markup],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class TelegramDeliveryEngine:
    """Reconcile editable Telegram messages to their latest desired payload."""

    def __init__(self, endpoints: dict[str, TelegramEndpoint]) -> None:
        self.endpoints = dict(endpoints)
        self._pending: dict[DeliveryKey, _PendingDelivery] = {}
        self._delivered: dict[DeliveryKey, str] = {}
        self._tasks: dict[DeliveryKey, asyncio.Task[None]] = {}
        self._wake_events: dict[DeliveryKey, asyncio.Event] = {}
        self._revision = 0
        self._started = False
        self._accepting = False
        self._stopping = False

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._accepting = True
        self._stopping = False

    def snapshot(self) -> dict[str, object]:
        return {
            "started": self._started,
            "accepting": self._accepting,
            "pending": len(self._pending),
            "workers": len(self._tasks),
            "terminal_pending": sum(
                pending.intent.terminal for pending in self._pending.values()
            ),
        }

    def submit(self, intent: DeliveryIntent) -> asyncio.Future[DeliveryOutcome]:
        if not self._started or not self._accepting:
            raise RuntimeError("Telegram delivery engine is not accepting work")
        loop = asyncio.get_running_loop()
        pending = self._pending.get(intent.key)
        if pending is not None and pending.intent.fingerprint == intent.fingerprint:
            if intent.terminal and not pending.intent.terminal:
                pending.intent = intent
                pending.due_at = min(pending.due_at, time.monotonic())
                self._wake_events.setdefault(intent.key, asyncio.Event()).set()
            return pending.future
        if self._delivered.get(intent.key) == intent.fingerprint:
            future: asyncio.Future[DeliveryOutcome] = loop.create_future()
            future.set_result(
                DeliveryOutcome(
                    key=intent.key,
                    revision=self._revision,
                    status="delivered",
                    attempts=0,
                    performed=False,
                )
            )
            return future

        self._revision += 1
        revision = self._revision
        future = loop.create_future()
        if pending is not None and not pending.future.done():
            pending.future.set_result(
                DeliveryOutcome(
                    key=intent.key,
                    revision=pending.revision,
                    status="superseded",
                    attempts=pending.attempts,
                    performed=False,
                )
            )
        self._pending[intent.key] = _PendingDelivery(
            intent=intent,
            revision=revision,
            future=future,
            attempts=0,
            due_at=time.monotonic(),
        )
        wake = self._wake_events.setdefault(intent.key, asyncio.Event())
        wake.set()
        self._ensure_worker(intent.key)
        return future

    async def stop(self, *, drain_timeout: float = 10.0) -> None:
        if not self._started:
            return
        self._accepting = False
        for key, pending in list(self._pending.items()):
            if pending.intent.terminal:
                continue
            self._pending.pop(key, None)
            self._finish(pending, "superseded", performed=False)
        for wake in self._wake_events.values():
            wake.set()

        terminal_tasks = [
            task
            for key, task in self._tasks.items()
            if key in self._pending and self._pending[key].intent.terminal
        ]
        if terminal_tasks and drain_timeout > 0:
            await asyncio.wait(terminal_tasks, timeout=drain_timeout)

        self._stopping = True
        tasks = list(self._tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for pending in list(self._pending.values()):
            self._finish(pending, "superseded", performed=False)
        self._pending.clear()
        self._tasks.clear()
        self._wake_events.clear()
        self._started = False

    async def _worker(self, key: DeliveryKey) -> None:
        wake = self._wake_events.setdefault(key, asyncio.Event())
        try:
            while not self._stopping:
                wake.clear()
                pending = self._pending.get(key)
                if pending is None:
                    return
                delay = max(0.0, pending.due_at - time.monotonic())
                if delay > 0:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(wake.wait(), timeout=delay)
                    continue

                pending.attempts += 1
                try:
                    await self._deliver(pending.intent)
                except (BadRequest, Forbidden) as exc:
                    if self._is_current(key, pending):
                        self._pending.pop(key, None)
                        self._finish(pending, "permanent_failure", performed=False)
                        LOGGER.error(
                            "event=telegram_delivery_permanent_failure bot_role=%s chat_id=%s "
                            "message_id=%s revision=%s error_type=%s context=%s",
                            key.bot_role,
                            key.chat_id,
                            key.message_id,
                            pending.revision,
                            type(exc).__name__,
                            pending.intent.context or "none",
                        )
                except TelegramError as exc:
                    if self._is_current(key, pending):
                        self._pending.pop(key, None)
                        self._finish(pending, "transient_failure", performed=False)
                        LOGGER.error(
                            "event=telegram_delivery_transport_exhausted bot_role=%s chat_id=%s "
                            "message_id=%s revision=%s attempts=%s error_type=%s context=%s",
                            key.bot_role,
                            key.chat_id,
                            key.message_id,
                            pending.revision,
                            pending.attempts,
                            type(exc).__name__,
                            pending.intent.context or "none",
                        )
                except Exception as exc:
                    if self._is_current(key, pending):
                        self._pending.pop(key, None)
                        self._finish(pending, "permanent_failure", performed=False)
                        LOGGER.error(
                            "event=telegram_delivery_unexpected_failure bot_role=%s chat_id=%s "
                            "message_id=%s revision=%s error_type=%s context=%s",
                            key.bot_role,
                            key.chat_id,
                            key.message_id,
                            pending.revision,
                            type(exc).__name__,
                            pending.intent.context or "none",
                        )
                else:
                    if self._is_current(key, pending):
                        self._pending.pop(key, None)
                        self._delivered[key] = pending.intent.fingerprint
                        self._finish(pending, "delivered", performed=True)
                        if pending.intent.terminal or pending.attempts > 1:
                            LOGGER.info(
                                "event=telegram_delivery_ack bot_role=%s chat_id=%s message_id=%s "
                                "revision=%s attempts=%s terminal=%s context=%s",
                                key.bot_role,
                                key.chat_id,
                                key.message_id,
                                pending.revision,
                                pending.attempts,
                                pending.intent.terminal,
                                pending.intent.context or "none",
                            )
        finally:
            if self._tasks.get(key) is asyncio.current_task():
                self._tasks.pop(key, None)
            if key not in self._pending:
                self._wake_events.pop(key, None)
            elif not self._stopping:
                self._ensure_worker(key)

    async def _deliver(self, intent: DeliveryIntent) -> None:
        endpoint = self.endpoints.get(intent.key.bot_role)
        if endpoint is None:
            raise ValueError(f"Unknown Telegram bot role: {intent.key.bot_role}")
        options: dict[str, object] = {
            "plain": intent.plain,
            "priority": intent.priority,
            "lane": "interactive" if intent.terminal else "maintenance",
        }
        if intent.reply_markup is not None:
            options["reply_markup"] = intent.reply_markup
        await endpoint.edit_text(
            intent.key.chat_id,
            intent.key.message_id,
            intent.markdown,
            **options,
        )

    def _is_current(self, key: DeliveryKey, pending: _PendingDelivery) -> bool:
        return self._pending.get(key) is pending

    def _ensure_worker(self, key: DeliveryKey) -> None:
        task = self._tasks.get(key)
        if task is not None and not task.done():
            return
        self._tasks[key] = asyncio.create_task(
            self._worker(key),
            name=f"telegram-delivery-{key.bot_role}-{key.chat_id}-{key.message_id}",
        )

    @staticmethod
    def _finish(
        pending: _PendingDelivery,
        status: DeliveryStatus,
        *,
        performed: bool,
    ) -> None:
        if pending.future.done():
            return
        pending.future.set_result(
            DeliveryOutcome(
                key=pending.intent.key,
                revision=pending.revision,
                status=status,
                attempts=pending.attempts,
                performed=performed,
            )
        )
