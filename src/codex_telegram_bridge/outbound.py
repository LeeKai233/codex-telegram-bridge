from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import itertools
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TelegramError

LOGGER = logging.getLogger(__name__)

OutboundLane = Literal["urgent", "interactive", "live", "maintenance"]
OperationSemantics = Literal["query", "idempotent", "non_idempotent"]
LANE_WEIGHTS: dict[OutboundLane, int] = {
    "urgent": 4,
    "interactive": 3,
    "live": 2,
    "maintenance": 1,
}
_LANE_CYCLE = tuple(lane for lane, weight in LANE_WEIGHTS.items() for _ in range(weight))
OUTBOUND_STOP_GRACE_SECONDS = 1.0


def _consume_task_result(task: asyncio.Task[None]) -> None:
    with contextlib.suppress(asyncio.CancelledError, Exception):
        task.result()


class OutboundJournal(Protocol):
    def create_outbound_intent(
        self,
        *,
        bot_role: str,
        operation: str,
        lane: str,
        chat_id: int | None,
        payload_fingerprint: str,
    ) -> str: ...

    def update_outbound_intent(
        self,
        intent_id: str,
        *,
        status: str,
        attempts: int,
        error_type: str | None = None,
    ) -> None: ...


class TelegramOutcomeUncertain(NetworkError):
    """A non-idempotent Bot API request may have reached Telegram."""


@dataclass(slots=True)
class _Job:
    sequence: int
    operation: Callable[[], Awaitable[Any]]
    future: asyncio.Future[Any]
    lane: OutboundLane
    semantics: OperationSemantics
    due_at: float
    attempts: int = 0
    intent_id: str | None = None
    generation: int = 0


def lane_for_priority(priority: int) -> OutboundLane:
    if priority <= 0:
        return "urgent"
    if priority <= 5:
        return "interactive"
    if priority <= 15:
        return "live"
    return "maintenance"


def _retry_after_seconds(exc: RetryAfter) -> float:
    value = exc.retry_after
    seconds = value.total_seconds() if isinstance(value, dt.timedelta) else float(value)
    return max(0.0, seconds) + 0.1


def _known_unsent(exc: NetworkError) -> bool:
    cause = exc.__cause__
    return type(cause).__name__ in {"ConnectError", "ConnectTimeout", "PoolTimeout"} or (
        "request was *not* sent" in str(exc).casefold()
    )


class OutboundMessenger:
    """Weighted Bot API scheduler and the sole authority for transport retries."""

    def __init__(
        self,
        minimum_interval: float = 1.05,
        retries: int = 3,
        *,
        bot_role: str = "control",
        journal: OutboundJournal | None = None,
        recycle_transport: Callable[[], Awaitable[None]] | None = None,
        max_queue_size: int = 1_000,
    ) -> None:
        self.minimum_interval = minimum_interval
        self.retries = max(0, retries)
        self.bot_role = bot_role
        self.journal = journal
        self.recycle_transport = recycle_transport
        self.max_queue_size = max(1, int(max_queue_size))
        self._queues: dict[OutboundLane, deque[_Job]] = {
            lane: deque() for lane in LANE_WEIGHTS
        }
        self._counter = itertools.count()
        self._task: asyncio.Task[None] | None = None
        self._generation = 0
        self._wake = asyncio.Event()
        self._stopping = False
        self._last_request = 0.0
        self._lane_cursor = 0
        self._active_job: _Job | None = None
        self._consecutive_transport_failures = 0
        self._recycle_count = 0

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stopping = False
        self._generation += 1
        generation = self._generation
        self._task = asyncio.create_task(
            self._worker(generation), name=f"telegram-{self.bot_role}-outbound"
        )

    async def stop(self) -> None:
        self._stopping = True
        self._generation += 1
        self._wake.set()
        active_job = self._active_job
        error = RuntimeError("Telegram bridge is stopping")
        if active_job and not active_job.future.done():
            active_job.future.set_exception(error)
        self._active_job = None
        for queue in self._queues.values():
            while queue:
                job = queue.popleft()
                if not job.future.done():
                    job.future.set_exception(error)
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        done, _pending = await asyncio.wait((task,), timeout=OUTBOUND_STOP_GRACE_SECONDS)
        if task in done:
            _consume_task_result(task)
            return
        LOGGER.warning(
            "event=telegram_outbound_stop_timeout bot_role=%s timeout_seconds=%.1f",
            self.bot_role,
            OUTBOUND_STOP_GRACE_SECONDS,
        )
        task.add_done_callback(_consume_task_result)

    async def call(
        self,
        operation: Callable[[], Awaitable[Any]],
        *,
        priority: int = 10,
        lane: OutboundLane | None = None,
        semantics: OperationSemantics = "idempotent",
        audit: Mapping[str, Any] | None = None,
    ) -> Any:
        if not self._task or self._task.done() or self._stopping:
            raise RuntimeError("Telegram outbound scheduler is not running")
        if sum(len(queue) for queue in self._queues.values()) >= self.max_queue_size:
            raise RuntimeError(
                f"Telegram outbound queue reached its {self.max_queue_size}-item limit"
            )
        selected_lane = lane or lane_for_priority(priority)
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        intent_id: str | None = None
        if semantics == "non_idempotent" and self.journal is not None:
            metadata = dict(audit or {})
            intent_id = self.journal.create_outbound_intent(
                bot_role=self.bot_role,
                operation=str(metadata.get("operation") or "unknown"),
                lane=selected_lane,
                chat_id=(int(metadata["chat_id"]) if metadata.get("chat_id") is not None else None),
                payload_fingerprint=str(metadata.get("payload_fingerprint") or ""),
            )
        self._queues[selected_lane].append(
            _Job(
                sequence=next(self._counter),
                operation=operation,
                future=future,
                lane=selected_lane,
                semantics=semantics,
                due_at=time.monotonic(),
                intent_id=intent_id,
            )
        )
        self._wake.set()
        return await future

    def snapshot(self) -> dict[str, Any]:
        return {
            "bot_role": self.bot_role,
            "queues": {lane: len(queue) for lane, queue in self._queues.items()},
            "active": self._active_job is not None,
            "queue_capacity": self.max_queue_size,
            "consecutive_transport_failures": self._consecutive_transport_failures,
            "transport_recycles": self._recycle_count,
        }

    async def _worker(self, generation: int) -> None:
        while generation == self._generation and not self._stopping:
            job = await self._next_job()
            if generation != self._generation or self._stopping:
                if not job.future.done():
                    job.future.set_exception(RuntimeError("Telegram bridge is stopping"))
                return
            if job.future.cancelled():
                continue
            job.generation = generation
            self._active_job = job
            try:
                try:
                    await self._execute(job)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    LOGGER.error(
                        "event=telegram_outbound_worker_error bot_role=%s error_type=%s",
                        self.bot_role,
                        type(exc).__name__,
                    )
                    if not job.future.done():
                        job.future.set_exception(exc)
            finally:
                if self._active_job is job:
                    self._active_job = None

    async def _next_job(self) -> _Job:
        while True:
            now = time.monotonic()
            job = self._take_due(now)
            if job is not None:
                return job
            due_at = min(
                (job.due_at for queue in self._queues.values() for job in queue),
                default=None,
            )
            self._wake.clear()
            if due_at is None:
                await self._wake.wait()
                continue
            timeout = max(0.0, due_at - time.monotonic())
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=timeout)

    def _take_due(self, now: float) -> _Job | None:
        for _ in range(len(_LANE_CYCLE)):
            lane = _LANE_CYCLE[self._lane_cursor]
            self._lane_cursor = (self._lane_cursor + 1) % len(_LANE_CYCLE)
            queue = self._queues[lane]
            for index, candidate in enumerate(queue):
                if candidate.due_at <= now:
                    del queue[index]
                    return candidate
        return None

    async def _execute(self, job: _Job) -> None:
        delay = self.minimum_interval - (time.monotonic() - self._last_request)
        if delay > 0:
            await asyncio.sleep(delay)
        job.attempts += 1
        try:
            result = await job.operation()
        except (BadRequest, Forbidden) as exc:
            self._finish_error(job, exc)
        except RetryAfter as exc:
            if job.attempts <= self.retries:
                self._requeue(job, _retry_after_seconds(exc))
            else:
                self._finish_error(job, exc)
        except NetworkError as exc:
            await self._handle_network_error(job, exc)
        except TelegramError as exc:
            self._finish_error(job, exc)
        except Exception as exc:
            LOGGER.error("Unexpected Telegram outbound error (%s)", type(exc).__name__)
            self._finish_error(job, exc)
        else:
            if self._job_is_current(job):
                self._consecutive_transport_failures = 0
            self._journal(job, "delivered")
            if not job.future.done():
                job.future.set_result(result)
        finally:
            if job.generation == self._generation and not self._stopping:
                self._last_request = time.monotonic()

    async def _handle_network_error(self, job: _Job, exc: NetworkError) -> None:
        if not self._job_is_current(job):
            return
        self._consecutive_transport_failures += 1
        await self._maybe_recycle_transport(job)
        if not self._job_is_current(job):
            return
        if job.semantics == "non_idempotent" and not _known_unsent(exc):
            uncertain = TelegramOutcomeUncertain(
                "Telegram may have accepted a non-idempotent request; automatic retry was suppressed"
            )
            uncertain.__cause__ = exc
            self._journal(job, "uncertain", type(exc).__name__)
            if not job.future.done():
                job.future.set_exception(uncertain)
            return
        if job.attempts <= self.retries:
            self._requeue(job, self._retry_delay(job.attempts))
            return
        self._finish_error(job, exc)

    async def _maybe_recycle_transport(self, job: _Job) -> None:
        if (
            not self._job_is_current(job)
            or self._consecutive_transport_failures < 2
            or self.recycle_transport is None
        ):
            return
        try:
            await self.recycle_transport()
        except Exception as exc:
            if self._job_is_current(job):
                LOGGER.warning(
                    "event=telegram_transport_recycle_failed bot_role=%s error_type=%s",
                    self.bot_role,
                    type(exc).__name__,
                )
        else:
            if self._job_is_current(job):
                self._recycle_count += 1
                LOGGER.warning("event=telegram_transport_recycled bot_role=%s", self.bot_role)
        finally:
            if self._job_is_current(job):
                self._consecutive_transport_failures = 0

    @staticmethod
    def _retry_delay(attempt: int) -> float:
        return min(60.0, float(2 ** max(0, attempt - 1)))

    def _requeue(self, job: _Job, delay: float) -> None:
        if not self._job_is_current(job):
            return
        job.due_at = time.monotonic() + max(0.0, delay)
        self._queues[job.lane].append(job)
        self._journal(job, "retrying")
        self._wake.set()

    def _job_is_current(self, job: _Job) -> bool:
        return (
            job.generation == self._generation
            and not self._stopping
            and not job.future.done()
        )

    def _finish_error(self, job: _Job, exc: Exception) -> None:
        self._journal(job, "failed", type(exc).__name__)
        if not job.future.done():
            job.future.set_exception(exc)

    def _journal(self, job: _Job, status: str, error_type: str | None = None) -> None:
        if job.intent_id is None or self.journal is None:
            return
        try:
            self.journal.update_outbound_intent(
                job.intent_id,
                status=status,
                attempts=job.attempts,
                error_type=error_type,
            )
        except Exception as exc:
            LOGGER.error(
                "event=telegram_outbound_journal_failed bot_role=%s status=%s error_type=%s",
                self.bot_role,
                status,
                type(exc).__name__,
            )
