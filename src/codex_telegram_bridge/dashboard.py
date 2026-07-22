from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from .delivery import DeliveryIntent, DeliveryKey, TelegramDeliveryEngine, delivery_fingerprint
from .markdown import render_dashboard, render_dashboard_plain
from .models import ThreadState
from .store import Store
from .telegram_common import CONTROL_ROLE, TelegramEndpoint

LOGGER = logging.getLogger(__name__)


class DashboardManager:
    def __init__(
        self,
        endpoint: TelegramEndpoint,
        store: Store,
        delivery: TelegramDeliveryEngine,
        owner_chat_id: Callable[[], int | None],
        *,
        debounce_seconds: float = 2.0,
        heartbeat_seconds: int = 60,
        retry_seconds: float = 5.0,
    ) -> None:
        self.endpoint = endpoint
        self.store = store
        self.delivery = delivery
        self.owner_chat_id = owner_chat_id
        self.debounce_seconds = debounce_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.retry_seconds = retry_seconds
        self._pending: dict[str, ThreadState] = {}
        self._flush_tasks: dict[str, asyncio.Task[None]] = {}
        self._wake_events: dict[str, asyncio.Event] = {}
        self._inflight: set[str] = set()
        self._locks: dict[str, asyncio.Lock] = {}
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._stopping = False

    def start(self) -> None:
        self._stopping = False
        if not self._heartbeat_task or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="dashboard-heartbeat")

    async def stop(self) -> None:
        self._stopping = True
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
        tasks = dict(self._flush_tasks)
        for thread_id, task in tasks.items():
            if thread_id not in self._inflight:
                task.cancel()
        inflight = [task for thread_id, task in tasks.items() if thread_id in self._inflight]
        if inflight:
            done, pending = await asyncio.wait(inflight, timeout=10)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    task.result()
        remaining = [task for task in tasks.values() if not task.done()]
        if remaining:
            await asyncio.gather(*remaining, return_exceptions=True)

    async def schedule(self, state: ThreadState, *, immediate: bool = False) -> None:
        if self._stopping or state.thread_id not in self.store.subscriptions():
            return
        self._pending[state.thread_id] = state
        current = self._flush_tasks.get(state.thread_id)
        wake = self._wake_events.setdefault(state.thread_id, asyncio.Event())
        if immediate:
            wake.set()
        if not current or current.done():
            self._flush_tasks[state.thread_id] = asyncio.create_task(
                self._delayed_flush(state.thread_id, 0 if immediate else self.debounce_seconds),
                name=f"dashboard-flush-{state.short_id}",
            )

    async def refresh(self, thread_id: str) -> None:
        state = self.store.get_thread(thread_id)
        if state:
            await self.schedule(state, immediate=True)

    async def _delayed_flush(self, thread_id: str, delay: float) -> None:
        wake = self._wake_events.setdefault(thread_id, asyncio.Event())
        try:
            while not self._stopping:
                if delay:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(wake.wait(), timeout=delay)
                wake.clear()
                state = self._pending.pop(thread_id, None)
                if state is None:
                    return
                self._inflight.add(thread_id)
                try:
                    delivered = await self._flush(state)
                finally:
                    self._inflight.discard(thread_id)
                if self._stopping:
                    return
                if not delivered:
                    self._pending.setdefault(thread_id, state)
                    delay = self.retry_seconds
                elif thread_id in self._pending:
                    delay = 0 if wake.is_set() else self.debounce_seconds
                else:
                    return
        finally:
            current = asyncio.current_task()
            if self._flush_tasks.get(thread_id) is current:
                self._flush_tasks.pop(thread_id, None)
            if thread_id not in self._pending:
                self._wake_events.pop(thread_id, None)

    async def _flush(self, state: ThreadState) -> bool:
        chat_id = self.owner_chat_id()
        if chat_id is None:
            return False
        lock = self._locks.setdefault(state.thread_id, asyncio.Lock())
        async with lock:
            terminal = str((state.goal or {}).get("status") or "") == "complete"
            render_time = state.updated_at if terminal and state.updated_at else None
            rendered = render_dashboard(state, now=render_time)
            plain = render_dashboard_plain(state, now=render_time)
            message_id = self.store.subscriptions().get(state.thread_id)
            keyboard = self._keyboard(state)
            try:
                if message_id:
                    key = DeliveryKey(CONTROL_ROLE, chat_id, int(message_id))
                    outcome = await self.delivery.submit(
                        DeliveryIntent(
                            key=key,
                            markdown=rendered,
                            plain=plain,
                            reply_markup=keyboard,
                            fingerprint=delivery_fingerprint(rendered, plain, keyboard),
                            lane="interactive" if terminal else "live",
                            priority=5 if terminal else 10,
                            terminal=terminal,
                            context=f"subscription:{state.thread_id}",
                        )
                    )
                    return outcome.status != "transient_failure"
                else:
                    message = await self.endpoint.send_text(
                        chat_id,
                        rendered,
                        plain=plain,
                        reply_markup=keyboard,
                        priority=20,
                    )
                    self.store.set_dashboard_message(state.thread_id, int(message.message_id))
            except Exception as exc:
                LOGGER.warning(
                    "Dashboard update failed for %s (%s)",
                    state.thread_id,
                    type(exc).__name__,
                )
                return False
            return True

    @staticmethod
    def _keyboard(state: ThreadState) -> InlineKeyboardMarkup:
        prefix = state.short_id
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton("刷新", callback_data=f"ds:{prefix}:status"),
                    InlineKeyboardButton("Prompt", callback_data=f"ds:{prefix}:prompt"),
                    InlineKeyboardButton("Queue", callback_data=f"ds:{prefix}:queue"),
                ]
            ]
        )

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_seconds)
            for thread_id in self.store.subscriptions():
                state = self.store.get_thread(thread_id)
                if state and str((state.goal or {}).get("status") or "") != "complete":
                    await self.schedule(state, immediate=True)
