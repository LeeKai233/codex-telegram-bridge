from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any

LOGGER = logging.getLogger(__name__)
WorkFactory = Callable[[], Awaitable[None]]


class KeyedWorkScheduler:
    """Run bounded work concurrently while preserving FIFO order for each key."""

    def __init__(self, name: str, *, max_pending: int = 256, max_running: int = 8) -> None:
        if max_pending < 1 or max_running < 1:
            raise ValueError("scheduler limits must be positive")
        self.name = name
        self.max_pending = max_pending
        self.max_running = max_running
        self._queues: dict[str, deque[WorkFactory]] = {}
        self._workers: dict[str, asyncio.Task[None]] = {}
        self._semaphore = asyncio.Semaphore(max_running)
        self._pending = 0
        self._stopping = False
        self._idle = asyncio.Event()
        self._idle.set()

    def submit(self, key: str, operation: WorkFactory) -> bool:
        if not self.can_submit():
            return False
        work_key = key or "global"
        self._queues.setdefault(work_key, deque()).append(operation)
        self._pending += 1
        self._idle.clear()
        worker = self._workers.get(work_key)
        if worker is None or worker.done():
            worker = asyncio.create_task(
                self._run_key(work_key),
                name=f"{self.name}-{work_key[:24]}",
            )
            self._workers[work_key] = worker
            worker.add_done_callback(
                lambda completed, queue_key=work_key: self._worker_done(
                    queue_key, completed
                )
            )
        return True

    def can_submit(self) -> bool:
        return not self._stopping and self._pending < self.max_pending

    async def join(self) -> None:
        await self._idle.wait()

    async def stop(self) -> None:
        self._stopping = True
        tasks = list(self._workers.values())
        self._workers.clear()
        self._queues.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._pending = 0
        self._idle.set()

    def snapshot(self) -> dict[str, Any]:
        return {
            "pending": self._pending,
            "capacity": self.max_pending,
            "running_keys": len(self._workers),
            "running_capacity": self.max_running,
        }

    async def _run_key(self, key: str) -> None:
        while True:
            queue = self._queues.get(key)
            if not queue:
                self._queues.pop(key, None)
                return
            operation = queue.popleft()
            try:
                async with self._semaphore:
                    await operation()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("event=keyed_work_failed scheduler=%s key=%s", self.name, key[:80])
            finally:
                self._pending -= 1
                if self._pending == 0:
                    self._idle.set()

    def _worker_done(self, key: str, task: asyncio.Task[None]) -> None:
        if self._workers.get(key) is task:
            self._workers.pop(key, None)
        if task.cancelled():
            return
        exception = task.exception()
        if exception is not None:
            LOGGER.error(
                "event=keyed_worker_failed scheduler=%s key=%s",
                self.name,
                key[:80],
                exc_info=(type(exception), exception, exception.__traceback__),
            )
