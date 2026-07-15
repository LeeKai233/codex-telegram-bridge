from __future__ import annotations

import asyncio
import contextlib
import logging
import time

from .store import Store
from .telegram_common import TelegramEndpoint

LOGGER = logging.getLogger(__name__)


class MessageDeletionManager:
    """Durable Telegram deletion queue used for /perf and resolved questions."""

    def __init__(
        self,
        store: Store,
        endpoints: dict[str, TelegramEndpoint],
        *,
        poll_seconds: float = 1.0,
    ) -> None:
        self.store = store
        self.endpoints = endpoints
        self.poll_seconds = max(0.1, poll_seconds)
        self._wake = asyncio.Event()
        self._drain_lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self.store.retire_question_requests(include_unexpired=True)
            await self._drain()
            self._task = asyncio.create_task(self._run(), name="telegram-message-deletions")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    def schedule(
        self,
        bot_role: str,
        chat_id: int,
        message_ids: list[int] | tuple[int, ...],
        *,
        delete_at: int,
        group_key: str | None = None,
    ) -> None:
        self.store.schedule_message_deletions(
            bot_role,
            chat_id,
            message_ids,
            delete_at,
            group_key=group_key,
        )
        self._wake.set()

    async def delete_now(
        self,
        bot_role: str,
        chat_id: int,
        message_ids: list[int] | tuple[int, ...],
        *,
        group_key: str | None = None,
    ) -> None:
        self.schedule(
            bot_role,
            chat_id,
            message_ids,
            delete_at=int(time.time()),
            group_key=group_key,
        )
        await self._drain()

    async def flush(self) -> None:
        await self._drain()

    async def _run(self) -> None:
        while True:
            await self._drain()
            self._wake.clear()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self.poll_seconds)

    async def _drain(self) -> None:
        async with self._drain_lock:
            while True:
                due = self.store.due_message_deletions(limit=100)
                if not due:
                    return
                for item in due:
                    await self._delete(item)
                if len(due) < 100:
                    return

    async def _delete(self, item: dict[str, object]) -> None:
        deletion_id = int(item["deletion_id"])
        role = str(item["bot_role"])
        endpoint = self.endpoints.get(role)
        if endpoint is None:
            self.store.reschedule_message_deletion(
                deletion_id,
                int(time.time()) + 60,
                "unknown bot role",
            )
            return
        deleted = await endpoint.delete_message(
            int(item["chat_id"]), int(item["message_id"])
        )
        if deleted:
            self.store.complete_message_deletion(deletion_id)
            return
        attempts = int(item.get("attempts") or 0) + 1
        if attempts >= 5:
            LOGGER.warning(
                "Giving up Telegram message deletion after %s attempts (%s)",
                attempts,
                role,
            )
            self.store.complete_message_deletion(deletion_id)
            return
        delay = min(300, 2**attempts)
        self.store.reschedule_message_deletion(
            deletion_id,
            int(time.time()) + delay,
            "Telegram delete failed",
        )
