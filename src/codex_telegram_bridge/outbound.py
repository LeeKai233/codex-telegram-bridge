from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import itertools
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from telegram.error import BadRequest, Forbidden, NetworkError, RetryAfter, TelegramError

LOGGER = logging.getLogger(__name__)

OutboundLane = Literal["urgent", "interactive", "live", "maintenance"]
TrafficClass = Literal["callback_ack", "interactive", "media", "maintenance"]
OperationSemantics = Literal["query", "idempotent", "non_idempotent"]
CoalesceKey = tuple[str, int, int]
LANE_WEIGHTS: dict[OutboundLane, int] = {
    "urgent": 4,
    "interactive": 3,
    "live": 2,
    "maintenance": 1,
}
_LANE_CYCLE = tuple(lane for lane, weight in LANE_WEIGHTS.items() for _ in range(weight))
TRAFFIC_CLASS_CONCURRENCY: dict[TrafficClass, int] = {
    "callback_ack": 4,
    "interactive": 2,
    "media": 1,
    "maintenance": 1,
}
_LEGACY_TRAFFIC_CLASSES: dict[OutboundLane, TrafficClass] = {
    "urgent": "interactive",
    "interactive": "interactive",
    "live": "interactive",
    "maintenance": "maintenance",
}
_KEYED_TRAFFIC_CLASSES = frozenset({"interactive"})
OUTBOUND_STOP_GRACE_SECONDS = 1.0
MAINTENANCE_CHAT_MINIMUM_INTERVAL_SECONDS = 6.0
GROUP_CHAT_MINIMUM_INTERVAL_SECONDS = 4.1


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


class RateLimitStateStore(Protocol):
    def get_meta(self, key: str, default: Any = None) -> Any: ...

    def set_meta(self, key: str, value: str | int | float | bool) -> None: ...


class TelegramOutcomeUncertain(NetworkError):
    """A non-idempotent Bot API request may have reached Telegram."""


@dataclass(slots=True)
class _Job:
    sequence: int
    operation: Callable[[], Awaitable[Any]]
    future: asyncio.Future[Any]
    lane: OutboundLane
    traffic_class: TrafficClass
    chat_key: str
    semantics: OperationSemantics
    due_at: float
    enqueued_at: float
    coalesce_key: CoalesceKey | None = None
    attempts: int = 0
    intent_id: str | None = None
    generation: int = 0
    transport_started: bool = False
    superseded: asyncio.Event = field(default_factory=asyncio.Event)


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
    """Class-isolated Bot API scheduler and transport retry authority."""

    def __init__(
        self,
        minimum_interval: float | None = None,
        retries: int = 3,
        *,
        bot_role: str = "control",
        journal: OutboundJournal | None = None,
        recycle_transport: Callable[[], Awaitable[None]] | None = None,
        max_queue_size: int = 1_000,
        maintenance_chat_interval: float = MAINTENANCE_CHAT_MINIMUM_INTERVAL_SECONDS,
        group_chat_interval: float = GROUP_CHAT_MINIMUM_INTERVAL_SECONDS,
    ) -> None:
        # Keep the legacy observable value for callers that inspect it. A global
        # interval is only enabled when explicitly configured.
        self.minimum_interval = 1.05 if minimum_interval is None else max(0.0, minimum_interval)
        self._interval_enabled = minimum_interval is not None and minimum_interval > 0
        self.retries = max(0, retries)
        self.bot_role = bot_role
        self.journal = journal
        self.recycle_transport = recycle_transport
        self.max_queue_size = max(1, int(max_queue_size))
        self.maintenance_chat_interval = max(0.0, maintenance_chat_interval)
        self.group_chat_interval = max(0.0, group_chat_interval)
        self._rate_limit_store: RateLimitStateStore | None = None
        if journal is not None and callable(getattr(journal, "get_meta", None)) and callable(
            getattr(journal, "set_meta", None)
        ):
            self._rate_limit_store = journal  # type: ignore[assignment]
        self._queues: dict[TrafficClass, deque[_Job]] = {
            traffic_class: deque() for traffic_class in TRAFFIC_CLASS_CONCURRENCY
        }
        self._counter = itertools.count()
        self._task: asyncio.Task[None] | None = None
        self._worker_tasks: set[asyncio.Task[None]] = set()
        self._generation = 0
        self._wake = asyncio.Event()
        self._stopping = False
        self._last_request = 0.0
        self._maintenance_request_at: dict[str, float] = {}
        self._group_request_at: dict[str, float] = {}
        self._group_interval_locks: dict[str, asyncio.Lock] = {}
        self._interval_lock = asyncio.Lock()
        self._active_jobs: dict[asyncio.Task[Any], _Job] = {}
        self._active_keys: set[tuple[int, str]] = set()
        self._metrics: dict[TrafficClass, dict[str, int]] = {
            traffic_class: {
                "completed": 0,
                "failed": 0,
                "uncertain": 0,
                "retries": 0,
                "coalesced": 0,
                "superseded": 0,
            }
            for traffic_class in TRAFFIC_CLASS_CONCURRENCY
        }
        self._consecutive_transport_failures = 0
        self._recycle_count = 0
        self._transport_condition = asyncio.Condition()
        self._active_transport_requests = 0
        self._recycling_transport = False

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stopping = False
        self._generation += 1
        generation = self._generation
        self._last_request = 0.0
        self._task = asyncio.create_task(
            self._supervise_workers(generation), name=f"telegram-{self.bot_role}-outbound"
        )

    async def stop(self) -> None:
        self._stopping = True
        self._generation += 1
        self._wake.set()
        error = RuntimeError("Telegram bridge is stopping")
        for job in tuple(self._active_jobs.values()):
            if not job.future.done():
                job.future.set_exception(error)
        for queue in self._queues.values():
            while queue:
                job = queue.popleft()
                if not job.future.done():
                    job.future.set_exception(error)
        task = self._task
        self._task = None
        workers = tuple(self._worker_tasks)
        self._worker_tasks.clear()
        if task is None:
            self._active_keys.clear()
            return
        for worker in workers:
            worker.cancel()
        done, _pending = await asyncio.wait((task,), timeout=OUTBOUND_STOP_GRACE_SECONDS)
        if task in done:
            _consume_task_result(task)
            self._active_keys.clear()
            return
        LOGGER.warning(
            "event=telegram_outbound_stop_timeout bot_role=%s timeout_seconds=%.1f",
            self.bot_role,
            OUTBOUND_STOP_GRACE_SECONDS,
        )
        task.add_done_callback(_consume_task_result)
        self._active_keys.clear()

    async def call(
        self,
        operation: Callable[[], Awaitable[Any]],
        *,
        priority: int = 10,
        lane: OutboundLane | None = None,
        traffic_class: TrafficClass | None = None,
        chat_key: str | int | None = None,
        semantics: OperationSemantics = "idempotent",
        coalesce_key: CoalesceKey | None = None,
        audit: Mapping[str, Any] | None = None,
    ) -> Any:
        if not self._task or self._task.done() or self._stopping:
            raise RuntimeError("Telegram outbound scheduler is not running")
        selected_lane = lane or lane_for_priority(priority)
        selected_traffic_class = traffic_class or _LEGACY_TRAFFIC_CLASSES[selected_lane]
        selected_coalesce_key = coalesce_key if semantics == "idempotent" else None
        queued_count = sum(len(queue) for queue in self._queues.values())
        replaceable_count = self._queued_coalesce_count(selected_coalesce_key)
        if queued_count - replaceable_count >= self.max_queue_size:
            raise RuntimeError(
                f"Telegram outbound queue reached its {self.max_queue_size}-item limit"
            )
        superseded = self._supersede_waiting_edits(selected_coalesce_key)
        if superseded:
            self._metrics[selected_traffic_class]["coalesced"] += 1
        future: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        sequence = next(self._counter)
        selected_chat_key = str(chat_key) if chat_key is not None else f"job:{sequence}"
        enqueued_at = time.monotonic()
        intent_id: str | None = None
        if semantics == "non_idempotent" and self.journal is not None:
            metadata = dict(audit or {})
            intent_id = self.journal.create_outbound_intent(
                bot_role=self.bot_role,
                operation=str(metadata.get("operation") or "unknown"),
                lane=selected_traffic_class,
                chat_id=(int(metadata["chat_id"]) if metadata.get("chat_id") is not None else None),
                payload_fingerprint=str(metadata.get("payload_fingerprint") or ""),
            )
        self._queues[selected_traffic_class].append(
            _Job(
                sequence=sequence,
                operation=operation,
                future=future,
                lane=selected_lane,
                traffic_class=selected_traffic_class,
                chat_key=selected_chat_key,
                semantics=semantics,
                due_at=enqueued_at,
                enqueued_at=enqueued_at,
                coalesce_key=selected_coalesce_key,
                intent_id=intent_id,
            )
        )
        self._wake.set()
        return await future

    def snapshot(self) -> dict[str, Any]:
        now = time.monotonic()
        active_counts = {traffic_class: 0 for traffic_class in TRAFFIC_CLASS_CONCURRENCY}
        for job in self._active_jobs.values():
            active_counts[job.traffic_class] += 1
        traffic_classes = {
            traffic_class: {
                "queued": len(self._queues[traffic_class]),
                "oldest_queued_seconds": self._oldest_queued_seconds(
                    self._queues[traffic_class], now=now
                ),
                "active": active_counts[traffic_class],
                "concurrency": concurrency,
                **self._metrics[traffic_class],
            }
            for traffic_class, concurrency in TRAFFIC_CLASS_CONCURRENCY.items()
        }
        return {
            "bot_role": self.bot_role,
            "queues": {
                traffic_class: values["queued"]
                for traffic_class, values in traffic_classes.items()
            },
            "traffic_classes": traffic_classes,
            "active": bool(self._active_jobs),
            "queue_capacity": self.max_queue_size,
            "coalesced": sum(values["coalesced"] for values in self._metrics.values()),
            "superseded": sum(values["superseded"] for values in self._metrics.values()),
            "oldest_queued_seconds": self._oldest_queued_seconds(
                (job for queue in self._queues.values() for job in queue),
                now=now,
            ),
            "consecutive_transport_failures": self._consecutive_transport_failures,
            "transport_recycles": self._recycle_count,
        }

    def _supersede_waiting_edits(self, coalesce_key: CoalesceKey | None) -> int:
        if coalesce_key is None:
            return 0
        superseded = 0
        seen: set[int] = set()
        for queue in self._queues.values():
            for job in tuple(queue):
                if job.coalesce_key != coalesce_key:
                    continue
                queue.remove(job)
                seen.add(id(job))
                superseded += int(self._cancel_superseded(job))
        for job in tuple(self._active_jobs.values()):
            if id(job) in seen or job.coalesce_key != coalesce_key:
                continue
            superseded += int(self._cancel_superseded(job))
        if superseded:
            self._wake.set()
        return superseded

    def _queued_coalesce_count(self, coalesce_key: CoalesceKey | None) -> int:
        if coalesce_key is None:
            return 0
        return sum(
            job.coalesce_key == coalesce_key and not job.future.done()
            for queue in self._queues.values()
            for job in queue
        )

    def _cancel_superseded(self, job: _Job) -> bool:
        if job.transport_started or job.future.done():
            return False
        job.superseded.set()
        job.future.cancel()
        self._metrics[job.traffic_class]["superseded"] += 1
        return True

    @staticmethod
    def _oldest_queued_seconds(
        jobs: Iterable[_Job],
        *,
        now: float,
    ) -> float:
        enqueued = [job.enqueued_at for job in jobs]
        return max(0.0, now - min(enqueued)) if enqueued else 0.0

    async def _supervise_workers(self, generation: int) -> None:
        workers = [
            asyncio.create_task(
                self._worker(generation, traffic_class, slot),
                name=f"telegram-{self.bot_role}-{traffic_class}-{slot + 1}",
            )
            for traffic_class, concurrency in TRAFFIC_CLASS_CONCURRENCY.items()
            for slot in range(concurrency)
        ]
        if generation == self._generation:
            self._worker_tasks = set(workers)
        await asyncio.gather(*workers, return_exceptions=True)

    async def _worker(
        self,
        generation: int,
        traffic_class: TrafficClass,
        slot: int,
    ) -> None:
        del slot
        while generation == self._generation and not self._stopping:
            job = await self._next_job(generation, traffic_class)
            job.generation = generation
            if generation != self._generation or self._stopping:
                if not job.future.done():
                    job.future.set_exception(RuntimeError("Telegram bridge is stopping"))
                return
            if job.future.cancelled():
                self._release_key(job)
                continue
            worker = asyncio.current_task()
            assert worker is not None
            self._active_jobs[worker] = job
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
                if self._active_jobs.get(worker) is job:
                    self._active_jobs.pop(worker, None)
                self._release_key(job)
                self._wake.set()

    async def _next_job(
        self,
        generation: int,
        traffic_class: TrafficClass,
    ) -> _Job:
        while True:
            now = time.monotonic()
            job = self._take_due(generation, traffic_class, now)
            if job is not None:
                return job
            due_at = self._next_due_at(generation, traffic_class)
            self._wake.clear()
            if due_at is None:
                await self._wake.wait()
                continue
            timeout = max(0.0, due_at - time.monotonic())
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=timeout)

    def _take_due(
        self,
        generation: int,
        traffic_class: TrafficClass,
        now: float,
    ) -> _Job | None:
        queue = self._queues[traffic_class]
        for index, candidate in enumerate(queue):
            if candidate.due_at > now:
                continue
            if traffic_class in _KEYED_TRAFFIC_CLASSES:
                key = (generation, candidate.chat_key)
                if key in self._active_keys:
                    continue
                if any(
                    queued.chat_key == candidate.chat_key
                    and queued.sequence < candidate.sequence
                    for queued in queue
                ):
                    continue
                self._active_keys.add(key)
            del queue[index]
            return candidate
        return None

    def _next_due_at(
        self,
        generation: int,
        traffic_class: TrafficClass,
    ) -> float | None:
        queue = self._queues[traffic_class]
        due: list[float] = []
        for candidate in queue:
            if traffic_class in _KEYED_TRAFFIC_CLASSES:
                if (generation, candidate.chat_key) in self._active_keys:
                    continue
                if any(
                    queued.chat_key == candidate.chat_key
                    and queued.sequence < candidate.sequence
                    for queued in queue
                ):
                    continue
            due.append(candidate.due_at)
        return min(due, default=None)

    def _release_key(self, job: _Job) -> None:
        if job.traffic_class in _KEYED_TRAFFIC_CLASSES:
            self._active_keys.discard((job.generation, job.chat_key))

    async def _execute(self, job: _Job) -> None:
        await self._pace_maintenance_chat(job)
        if not self._job_is_current(job):
            return
        await self._pace_group_chat(job)
        if not self._job_is_current(job):
            return
        if self._interval_enabled:
            async with self._interval_lock:
                delay = self.minimum_interval - (time.monotonic() - self._last_request)
                if delay > 0:
                    await self._wait_for_supersession(job, delay)
                if self._job_is_current(job):
                    self._last_request = time.monotonic()
        job.attempts += 1
        try:
            job.transport_started = True
            try:
                result = await self._run_transport_operation(job.operation)
            finally:
                job.transport_started = False
        except (BadRequest, Forbidden) as exc:
            self._finish_error(job, exc)
        except RetryAfter as exc:
            retry_delay = _retry_after_seconds(exc)
            LOGGER.warning(
                "event=telegram_outbound_retry_after bot_role=%s traffic_class=%s "
                "chat_key=%s retry_seconds=%.1f attempt=%s",
                self.bot_role,
                job.traffic_class,
                job.chat_key,
                retry_delay,
                job.attempts,
            )
            if job.attempts <= self.retries:
                self._requeue(job, retry_delay)
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
            if not self._job_is_current(job):
                return
            self._consecutive_transport_failures = 0
            self._metrics[job.traffic_class]["completed"] += 1
            self._journal(job, "delivered")
            job.future.set_result(result)

    async def _pace_maintenance_chat(self, job: _Job) -> None:
        if (
            job.traffic_class != "maintenance"
            or not job.chat_key.startswith("chat:")
            or self.maintenance_chat_interval <= 0
        ):
            return
        last_request = self._maintenance_request_at.get(job.chat_key, 0.0)
        delay = self.maintenance_chat_interval - (time.monotonic() - last_request)
        if delay > 0:
            await self._wait_for_supersession(job, delay)
        if self._job_is_current(job):
            self._maintenance_request_at[job.chat_key] = time.monotonic()

    async def _pace_group_chat(self, job: _Job) -> None:
        if self.group_chat_interval <= 0 or not job.chat_key.startswith("chat:"):
            return
        try:
            chat_id = int(job.chat_key.removeprefix("chat:"))
        except ValueError:
            return
        if chat_id >= 0:
            return

        lock = self._group_interval_locks.setdefault(job.chat_key, asyncio.Lock())
        while self._job_is_current(job):
            async with lock:
                last_request = self._group_request_at.get(job.chat_key)
                if last_request is None:
                    last_request = self._load_group_request_at(chat_id)
                    self._group_request_at[job.chat_key] = last_request
                delay = self.group_chat_interval - (time.time() - last_request)
                if delay <= 0:
                    dispatched_at = time.time()
                    self._group_request_at[job.chat_key] = dispatched_at
                    self._save_group_request_at(chat_id, dispatched_at)
                    return
            await self._wait_for_supersession(
                job,
                min(self.group_chat_interval, max(0.01, delay)),
            )

    @staticmethod
    async def _wait_for_supersession(job: _Job, delay: float) -> None:
        if delay <= 0 or job.superseded.is_set():
            return
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(job.superseded.wait(), timeout=delay)

    def _group_rate_limit_key(self, chat_id: int) -> str:
        return f"telegram-group-rate:{self.bot_role}:{chat_id}"

    def _load_group_request_at(self, chat_id: int) -> float:
        if self._rate_limit_store is None:
            return 0.0
        try:
            return max(
                0.0,
                float(self._rate_limit_store.get_meta(self._group_rate_limit_key(chat_id), 0.0)),
            )
        except Exception as exc:
            LOGGER.warning(
                "event=telegram_rate_limit_state_read_failed bot_role=%s error_type=%s",
                self.bot_role,
                type(exc).__name__,
            )
            return 0.0

    def _save_group_request_at(self, chat_id: int, dispatched_at: float) -> None:
        if self._rate_limit_store is None:
            return
        try:
            self._rate_limit_store.set_meta(
                self._group_rate_limit_key(chat_id),
                dispatched_at,
            )
        except Exception as exc:
            LOGGER.warning(
                "event=telegram_rate_limit_state_write_failed bot_role=%s error_type=%s",
                self.bot_role,
                type(exc).__name__,
            )

    async def _run_transport_operation(
        self,
        operation: Callable[[], Awaitable[Any]],
    ) -> Any:
        async with self._transport_condition:
            while self._recycling_transport:
                await self._transport_condition.wait()
            self._active_transport_requests += 1
        try:
            return await operation()
        finally:
            async with self._transport_condition:
                self._active_transport_requests -= 1
                self._transport_condition.notify_all()

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
            self._metrics[job.traffic_class]["uncertain"] += 1
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
        async with self._transport_condition:
            if self._recycling_transport:
                return
            self._recycling_transport = True
        try:
            async with self._transport_condition:
                while self._active_transport_requests:
                    await self._transport_condition.wait()
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
            async with self._transport_condition:
                self._recycling_transport = False
                self._transport_condition.notify_all()

    @staticmethod
    def _retry_delay(attempt: int) -> float:
        return min(60.0, float(2 ** max(0, attempt - 1)))

    def _requeue(self, job: _Job, delay: float) -> None:
        if not self._job_is_current(job):
            return
        job.due_at = time.monotonic() + max(0.0, delay)
        self._queues[job.traffic_class].append(job)
        self._metrics[job.traffic_class]["retries"] += 1
        self._journal(job, "retrying")
        self._wake.set()

    def _job_is_current(self, job: _Job) -> bool:
        return (
            job.generation == self._generation
            and not self._stopping
            and not job.future.done()
        )

    def _finish_error(self, job: _Job, exc: Exception) -> None:
        if not self._job_is_current(job):
            return
        self._metrics[job.traffic_class]["failed"] += 1
        self._journal(job, "failed", type(exc).__name__)
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
