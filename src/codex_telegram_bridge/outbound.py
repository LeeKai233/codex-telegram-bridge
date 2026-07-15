from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import itertools
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from telegram.error import RetryAfter, TelegramError

LOGGER = logging.getLogger(__name__)


@dataclass(order=True, slots=True)
class _Job:
    priority: int
    sequence: int
    operation: Callable[[], Awaitable[Any]] = field(compare=False)
    future: asyncio.Future[Any] = field(compare=False)


class OutboundMessenger:
    """Single private-chat scheduler; PTB doesn't per-chat throttle positive chat IDs."""

    def __init__(self, minimum_interval: float = 1.05, retries: int = 3) -> None:
        self.minimum_interval = minimum_interval
        self.retries = retries
        self._queue: asyncio.PriorityQueue[_Job] = asyncio.PriorityQueue()
        self._counter = itertools.count()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._last_request = 0.0

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(self._worker(), name="telegram-outbound-scheduler")

    async def stop(self) -> None:
        self._stopping = True
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        while not self._queue.empty():
            job = self._queue.get_nowait()
            if not job.future.done():
                job.future.set_exception(RuntimeError("Telegram bridge is stopping"))

    async def call(self, operation: Callable[[], Awaitable[Any]], *, priority: int = 10) -> Any:
        if not self._task or self._task.done():
            raise RuntimeError("Telegram outbound scheduler is not running")
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        await self._queue.put(_Job(priority, next(self._counter), operation, future))
        return await future

    async def _worker(self) -> None:
        while True:
            job = await self._queue.get()
            if job.future.cancelled():
                continue
            result: Any = None
            error: Exception | None = None
            for attempt in range(self.retries + 1):
                delay = self.minimum_interval - (time.monotonic() - self._last_request)
                if delay > 0:
                    await asyncio.sleep(delay)
                try:
                    result = await job.operation()
                    self._last_request = time.monotonic()
                    error = None
                    break
                except RetryAfter as exc:
                    error = exc
                    retry_after = exc.retry_after
                    seconds = (
                        retry_after.total_seconds()
                        if isinstance(retry_after, dt.timedelta)
                        else float(retry_after)
                    )
                    if attempt >= self.retries:
                        break
                    await asyncio.sleep(seconds + 0.1)
                except TelegramError as exc:
                    error = exc
                    break
                except Exception as exc:
                    error = exc
                    LOGGER.error("Unexpected Telegram outbound error (%s)", type(exc).__name__)
                    break
            if job.future.done():
                continue
            if error:
                job.future.set_exception(error)
            else:
                job.future.set_result(result)
