from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Any

LOGGER = logging.getLogger(__name__)
WorkFactory = Callable[[], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class Space:
    """An independently bounded workload category."""

    name: str
    max_running: int

    def __post_init__(self) -> None:
        if not self.name or self.max_running < 1:
            raise ValueError("space name and concurrency must be positive")


FILE_IO_SPACE = Space("file_io", 2)
PROMPT_ACTION_SPACE = Space("prompt_action", 4)
MAINTENANCE_SPACE = Space("maintenance", 1)


@dataclass(slots=True)
class _Work:
    operation: WorkFactory
    space: str


class KeyedWorkScheduler:
    """Run bounded work concurrently while preserving FIFO order for each key."""

    def __init__(
        self,
        name: str,
        *,
        max_pending: int = 256,
        max_running: int = 8,
        spaces: Sequence[Space] = (),
    ) -> None:
        if max_pending < 1 or max_running < 1:
            raise ValueError("scheduler limits must be positive")
        self.name = name
        self.max_pending = max_pending
        self.max_running = max_running
        self._queues: dict[str, deque[_Work]] = {}
        self._workers: dict[str, asyncio.Task[None]] = {}
        self._spaces: dict[str, Space] = {}
        self._semaphores: dict[str, asyncio.Semaphore] = {}
        self._space_pending: dict[str, int] = {}
        self._space_running: dict[str, int] = {}
        self._register_space(Space("default", max_running))
        for space in spaces:
            self._register_space(space)
        self._pending = 0
        self._stopping = False
        self._idle = asyncio.Event()
        self._idle.set()

    def submit(
        self,
        key: str,
        operation: WorkFactory,
        *,
        space: str | Space = "default",
    ) -> bool:
        if not self.can_submit():
            return False
        if isinstance(space, Space):
            self._register_space(space)
            space_name = space.name
        else:
            space_name = space
            if space_name not in self._spaces:
                raise ValueError(f"unknown scheduler space: {space_name}")
        work_key = key or "global"
        self._queues.setdefault(work_key, deque()).append(_Work(operation, space_name))
        self._pending += 1
        self._space_pending[space_name] += 1
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
        for name in self._space_pending:
            self._space_pending[name] = 0
            self._space_running[name] = 0
        self._idle.set()

    def snapshot(self) -> dict[str, Any]:
        return {
            "pending": self._pending,
            "capacity": self.max_pending,
            "running_keys": len(self._workers),
            "running_capacity": self.max_running,
            "spaces": {
                name: {
                    "pending": self._space_pending[name],
                    "running": self._space_running[name],
                    "running_capacity": space.max_running,
                }
                for name, space in self._spaces.items()
            },
        }

    async def _run_key(self, key: str) -> None:
        while True:
            queue = self._queues.get(key)
            if not queue:
                self._queues.pop(key, None)
                return
            work = queue.popleft()
            try:
                async with self._semaphores[work.space]:
                    self._space_running[work.space] += 1
                    try:
                        await work.operation()
                    finally:
                        self._space_running[work.space] -= 1
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("event=keyed_work_failed scheduler=%s key=%s", self.name, key[:80])
            finally:
                self._pending -= 1
                self._space_pending[work.space] -= 1
                if self._pending == 0:
                    self._idle.set()

    def _register_space(self, space: Space) -> None:
        current = self._spaces.get(space.name)
        if current is not None:
            if current.max_running != space.max_running:
                raise ValueError(
                    f"scheduler space {space.name!r} already has concurrency "
                    f"{current.max_running}"
                )
            return
        self._spaces[space.name] = space
        self._semaphores[space.name] = asyncio.Semaphore(space.max_running)
        self._space_pending[space.name] = 0
        self._space_running[space.name] = 0

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
