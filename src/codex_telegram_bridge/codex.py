from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from websockets.asyncio.client import ClientConnection, unix_connect
from websockets.exceptions import ConnectionClosed

from .models import ModelOption

LOGGER = logging.getLogger(__name__)
Json = dict[str, Any]
NotificationHandler = Callable[[str, Json], Awaitable[None]]
ServerRequestHandler = Callable[[int | str, str, Json, int], Awaitable[None]]
ConnectionHandler = Callable[[bool, int, str | None], Awaitable[None]]


def _merge_thread_response(result: Json) -> Json:
    thread = dict(result.get("thread") or {})
    for key in (
        "activePermissionProfile",
        "approvalPolicy",
        "approvalsReviewer",
        "permissions",
        "sandbox",
        "sandboxPolicy",
    ):
        if key in result:
            thread[key] = result[key]
    for key in ("model", "reasoningEffort", "cwd"):
        value = result.get(key)
        if isinstance(value, str) and value.strip():
            thread[key] = value.strip() if key != "cwd" else value
    return thread


@dataclass(frozen=True, slots=True)
class ThreadPage:
    data: list[Json]
    next_cursor: str | None = None
    backwards_cursor: str | None = None


@dataclass(slots=True)
class _PendingRpc:
    future: asyncio.Future[Json]
    connection_token: object
    permit_released: bool = False


@dataclass(frozen=True, slots=True)
class _NotificationEnvelope:
    method: str
    params: Json
    connection_token: object


@dataclass(frozen=True, slots=True)
class _ServerRequestEnvelope:
    request_id: int | str
    method: str
    params: Json
    generation: int
    connection_token: object
    thread_key: str


@dataclass(slots=True)
class _CapturedTurn:
    messages: dict[str, tuple[str | None, str]] = field(default_factory=dict)
    status: str | None = None
    error: str | None = None

    def record_items(self, items: list[Any]) -> None:
        for item in items:
            if not isinstance(item, dict) or item.get("type") != "agentMessage":
                continue
            text = str(item.get("text") or "").strip()
            if not text:
                continue
            item_id = str(item.get("id") or f"message-{len(self.messages)}")
            phase = item.get("phase")
            self.messages[item_id] = (str(phase) if phase is not None else None, text)

    def final_answer(self) -> str | None:
        values = list(self.messages.values())
        final = [text for phase, text in values if phase == "final_answer"]
        if final:
            return final[-1]
        legacy = [text for phase, text in values if phase is None]
        return legacy[-1] if legacy else None


@dataclass(slots=True)
class _IsolatedQuestion:
    thread_id: str
    future: asyncio.Future[str]
    turn_id: str | None = None
    turns: dict[str, _CapturedTurn] = field(default_factory=dict)

    def bind_turn(self, turn_id: str) -> None:
        if self.turn_id and self.turn_id != turn_id:
            raise RuntimeError("Codex side question returned an unexpected turn ID")
        self.turn_id = turn_id
        self._finish_if_ready()

    def ingest(self, method: str, params: Json) -> None:
        turn = params.get("turn")
        turn = turn if isinstance(turn, dict) else {}
        turn_id = str(params.get("turnId") or turn.get("id") or "")
        if not turn_id:
            return
        capture = self.turns.setdefault(turn_id, _CapturedTurn())
        if method == "item/completed":
            capture.record_items([params.get("item")])
        elif method == "error" and not params.get("willRetry"):
            error = params.get("error")
            if isinstance(error, dict):
                capture.error = str(error.get("message") or "Codex side question failed")
        elif method == "turn/completed":
            capture.record_items(list(turn.get("items") or []))
            capture.status = str(turn.get("status") or "completed")
            error = turn.get("error")
            if isinstance(error, dict):
                capture.error = str(error.get("message") or "Codex side question failed")
        self._finish_if_ready()

    def _finish_if_ready(self) -> None:
        if not self.turn_id or self.future.done():
            return
        capture = self.turns.get(self.turn_id)
        if not capture or capture.status not in {"completed", "failed", "interrupted"}:
            return
        if capture.status == "failed":
            self.future.set_exception(RuntimeError(capture.error or "Codex side question failed"))
            return
        if capture.status == "interrupted":
            self.future.set_exception(RuntimeError("Codex side question was interrupted"))
            return
        answer = capture.final_answer()
        if answer is None:
            self.future.set_exception(
                RuntimeError("Codex side question completed without a final agent message")
            )
            return
        self.future.set_result(answer)

    def turn_is_terminal(self, turn_id: str) -> bool:
        capture = self.turns.get(turn_id)
        return bool(capture and capture.status in {"completed", "failed", "interrupted"})


SIDE_QUESTION_INSTRUCTIONS = (
    "Answer this one-off side question using the inherited session context. Do not modify files, "
    "change the session goal, plan, tasks, or queue, contact the parent thread, use the network, "
    "or request interactive input or approval. Return only the answer to the side question."
)


OPT_OUT_NOTIFICATIONS = [
    "item/agentMessage/delta",
    "item/plan/delta",
    "item/commandExecution/outputDelta",
    "item/fileChange/outputDelta",
    "item/fileChange/patchUpdated",
    "item/reasoning/summaryTextDelta",
    "item/reasoning/summaryPartAdded",
    "item/reasoning/textDelta",
    "command/exec/outputDelta",
    "process/outputDelta",
    "thread/tokenUsage/updated",
    "account/rateLimits/updated",
]


class CodexRpcError(RuntimeError):
    def __init__(self, method: str, error: Json) -> None:
        self.method = method
        self.error = error
        super().__init__(f"{method}: {error.get('message', error)}")


class CodexDisconnected(RuntimeError):
    pass


async def _noop_notification(method: str, params: Json) -> None:
    del method, params


async def _noop_server_request(request_id: int | str, method: str, params: Json, generation: int) -> None:
    del request_id, method, params, generation


async def _noop_connection(connected: bool, generation: int, reason: str | None) -> None:
    del connected, generation, reason


class CodexClient:
    def __init__(
        self,
        socket_path: Path,
        *,
        on_notification: NotificationHandler = _noop_notification,
        on_server_request: ServerRequestHandler = _noop_server_request,
        on_connection: ConnectionHandler = _noop_connection,
        notification_capacity: int = 1024,
        server_request_capacity: int = 128,
        server_request_concurrency: int = 8,
        pending_rpc_limit: int = 128,
        admission_timeout: float = 10.0,
        send_timeout: float = 10.0,
    ) -> None:
        if min(
            notification_capacity,
            server_request_capacity,
            server_request_concurrency,
            pending_rpc_limit,
        ) <= 0:
            raise ValueError("Codex queue limits must be positive")
        self.socket_path = socket_path
        self.on_notification = on_notification
        self.on_server_request = on_server_request
        self.on_connection = on_connection
        self._websocket: ClientConnection | None = None
        self._connection_token: object | None = None
        self._pending: dict[int, _PendingRpc] = {}
        self._pending_slots = asyncio.Semaphore(pending_rpc_limit)
        self._pending_rpc_limit = pending_rpc_limit
        self.admission_timeout = admission_timeout
        self.send_timeout = send_timeout
        self._ephemeral_request_ids: set[int] = set()
        self._ephemeral_thread_ids: set[str] = set()
        self._isolated_questions: dict[str, _IsolatedQuestion] = {}
        self.notification_capacity = notification_capacity
        self._notification_queue: asyncio.Queue[_NotificationEnvelope] | None = None
        self._notification_task: asyncio.Task[None] | None = None
        self.server_request_capacity = server_request_capacity
        self.server_request_concurrency = server_request_concurrency
        self._server_request_queue: deque[_ServerRequestEnvelope] = deque()
        self._server_request_tasks: set[asyncio.Task[None]] = set()
        self._server_request_envelopes: dict[asyncio.Task[None], _ServerRequestEnvelope] = {}
        self._server_active_threads: set[str] = set()
        self._connection_callback_tasks: set[asyncio.Task[None]] = set()
        self._connected_callback_task: asyncio.Task[None] | None = None
        self._request_id = 0
        self._send_lock = asyncio.Lock()
        self._connected = asyncio.Event()
        self._stopping = asyncio.Event()
        self._runner: asyncio.Task[None] | None = None
        self.generation = 0

    @property
    def connected(self) -> bool:
        return self._connected.is_set()

    def start(self) -> None:
        if self._runner and not self._runner.done():
            return
        self._stopping.clear()
        self._runner = asyncio.create_task(self._run(), name="codex-app-server-client")

    async def stop(self) -> None:
        self._stopping.set()
        try:
            websocket = self._websocket
            if websocket:
                with contextlib.suppress(Exception):
                    await websocket.close(code=1000, reason="bridge shutdown")
            if self._runner:
                self._runner.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._runner
        finally:
            self._runner = None
            self._connected.clear()
            await self._fail_pending(CodexDisconnected("Codex app-server client stopped"))
            await self._stop_notification_consumer()
            await self._cancel_connection_callbacks()
            self._connection_token = None

    async def wait_connected(self, timeout: float = 15.0) -> None:
        try:
            await asyncio.wait_for(self._connected.wait(), timeout)
        except TimeoutError as exc:
            raise CodexDisconnected(f"Codex app-server unavailable at {self.socket_path}") from exc

    async def _run(self) -> None:
        delay = 1.0
        while not self._stopping.is_set():
            reason: str | None = None
            reader: asyncio.Task[None] | None = None
            notification_task: asyncio.Task[None] | None = None
            websocket: ClientConnection | None = None
            connection_token = object()
            next_generation = self.generation + 1
            try:
                websocket = await unix_connect(
                    path=str(self.socket_path),
                    uri="ws://localhost/",
                    compression=None,
                    user_agent_header=None,
                    open_timeout=10,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_size=16 * 1024 * 1024,
                    max_queue=64,
                )
                self._websocket = websocket
                self._connection_token = connection_token
                notification_queue: asyncio.Queue[_NotificationEnvelope] = asyncio.Queue(
                    maxsize=self.notification_capacity
                )
                self._notification_queue = notification_queue
                notification_task = asyncio.create_task(
                    self._notification_consumer(notification_queue, connection_token),
                    name="codex-notification-consumer",
                )
                self._notification_task = notification_task
                self.generation = next_generation
                reader = asyncio.create_task(
                    self._reader(
                        websocket,
                        connection_token=connection_token,
                        generation=next_generation,
                        notification_queue=notification_queue,
                    ),
                    name="codex-app-server-reader",
                )
                await self._initialize()
                self._connected.set()
                delay = 1.0
                self._connected_callback_task = self._schedule_connection_callback(
                    True,
                    self.generation,
                    None,
                )
                await reader
                reason = "connection closed"
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reason = f"{type(exc).__name__}: {exc}"
                LOGGER.warning("Codex app-server connection failed: %s", reason)
            finally:
                self._connected.clear()
                callback = self._connected_callback_task
                if callback is not None and not callback.done():
                    callback.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await callback
                self._connected_callback_task = None
                if reader and not reader.done():
                    reader.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await reader
                if notification_task and not notification_task.done():
                    notification_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await notification_task
                if self._notification_task is notification_task:
                    self._notification_task = None
                    self._notification_queue = None
                await self._cancel_server_request_tasks()
                await self._fail_pending(
                    CodexDisconnected(reason or "Codex app-server disconnected")
                )
                if websocket is not None:
                    with contextlib.suppress(Exception):
                        await websocket.close(code=1001, reason="connection cycle ended")
                if self._websocket is websocket:
                    self._websocket = None
                if self._connection_token is connection_token:
                    self._connection_token = None
                self._schedule_connection_callback(False, self.generation, reason)
                await asyncio.sleep(0)
            if not self._stopping.is_set():
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._stopping.wait(), timeout=delay)
                delay = min(delay * 2, 30.0)

    async def _initialize(self) -> None:
        result = await self._request_on_connection(
            "initialize",
            {
                "clientInfo": {
                    "name": "codex_telegram_bridge",
                    "title": "Codex Telegram Bridge",
                    "version": "0.1.0",
                },
                "capabilities": {
                    "experimentalApi": True,
                    "optOutNotificationMethods": OPT_OUT_NOTIFICATIONS,
                },
            },
            timeout=15,
        )
        if not isinstance(result, dict):
            raise RuntimeError("Invalid initialize response")
        await self._send({"method": "initialized", "params": {}})

    async def _reader(
        self,
        websocket: ClientConnection,
        *,
        connection_token: object | None = None,
        generation: int | None = None,
        notification_queue: asyncio.Queue[_NotificationEnvelope] | None = None,
    ) -> None:
        token = connection_token or self._connection_token or object()
        current_generation = self.generation if generation is None else generation
        if self._websocket is None:
            self._websocket = websocket
        if self._connection_token is None:
            self._connection_token = token
        owns_queue = notification_queue is None
        queue = notification_queue or asyncio.Queue(maxsize=self.notification_capacity)
        consumer: asyncio.Task[None] | None = None
        if owns_queue:
            consumer = asyncio.create_task(
                self._notification_consumer(queue, token),
                name="codex-notification-consumer-local",
            )
        try:
            async for raw in websocket:
                if not isinstance(raw, str):
                    continue
                try:
                    message = json.loads(raw)
                except json.JSONDecodeError:
                    LOGGER.warning("Ignoring non-JSON app-server frame")
                    continue
                if not isinstance(message, dict):
                    continue
                request_id = message.get("id")
                method = message.get("method")
                if request_id is not None and method is None:
                    pending = self._pending.get(request_id)
                    if pending is None or pending.connection_token is not token:
                        continue
                    self._register_ephemeral_response(request_id, message)
                    if not pending.future.done():
                        pending.future.set_result(message)
                    continue
                params = message.get("params")
                params = params if isinstance(params, dict) else {}
                if request_id is not None and isinstance(method, str):
                    self._enqueue_server_request(
                        request_id,
                        method,
                        params,
                        current_generation,
                        token,
                    )
                elif isinstance(method, str):
                    try:
                        queue.put_nowait(_NotificationEnvelope(method, params, token))
                    except asyncio.QueueFull as exc:
                        raise CodexDisconnected(
                            f"Codex notification queue exceeded {self.notification_capacity} entries"
                        ) from exc
        except ConnectionClosed as exc:
            if not self._stopping.is_set():
                raise CodexDisconnected(str(exc)) from exc
        finally:
            if owns_queue and consumer is not None:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(queue.join(), timeout=1.0)
                consumer.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await consumer

    async def _notification_consumer(
        self,
        queue: asyncio.Queue[_NotificationEnvelope],
        connection_token: object,
    ) -> None:
        while True:
            envelope = await queue.get()
            try:
                if envelope.connection_token is not connection_token:
                    continue
                try:
                    await self._dispatch_notification(envelope.method, envelope.params)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    LOGGER.error(
                        "Codex notification handler failed (%s)",
                        type(exc).__name__,
                    )
            finally:
                queue.task_done()

    async def _stop_notification_consumer(self) -> None:
        task = self._notification_task
        self._notification_task = None
        self._notification_queue = None
        if task is None or task.done():
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    def _schedule_connection_callback(
        self,
        connected: bool,
        generation: int,
        reason: str | None,
    ) -> asyncio.Task[None]:
        task = asyncio.create_task(
            self._run_connection_callback(connected, generation, reason),
            name=f"codex-connection-callback-{'up' if connected else 'down'}-{generation}",
        )
        self._connection_callback_tasks.add(task)
        task.add_done_callback(self._connection_callback_finished)
        return task

    async def _run_connection_callback(
        self,
        connected: bool,
        generation: int,
        reason: str | None,
    ) -> None:
        try:
            await self.on_connection(connected, generation, reason)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOGGER.error(
                "Codex connection callback failed connected=%s generation=%s error_type=%s",
                connected,
                generation,
                type(exc).__name__,
            )

    def _connection_callback_finished(self, task: asyncio.Task[None]) -> None:
        self._connection_callback_tasks.discard(task)

    async def _cancel_connection_callbacks(self) -> None:
        tasks = tuple(self._connection_callback_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._connection_callback_tasks.clear()

    async def _send(
        self,
        message: Json,
        *,
        websocket: ClientConnection | None = None,
        connection_token: object | None = None,
    ) -> None:
        target = websocket or self._websocket
        token = connection_token or self._connection_token
        if target is None or token is None:
            raise CodexDisconnected("Codex app-server is not connected")
        if self._websocket is not target or self._connection_token is not token:
            raise CodexDisconnected("Codex app-server connection generation is stale")
        encoded = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        try:
            async with self._send_lock:
                if self._websocket is not target or self._connection_token is not token:
                    raise CodexDisconnected("Codex app-server connection generation is stale")
                await asyncio.wait_for(target.send(encoded), timeout=self.send_timeout)
        except TimeoutError as exc:
            with contextlib.suppress(Exception):
                await target.close(code=1011, reason="send timeout")
            raise CodexDisconnected("Codex app-server send timed out") from exc

    def _next_request_id(self) -> int:
        self._request_id += 1
        return self._request_id

    async def _request_on_connection(self, method: str, params: Json, timeout: float = 30.0) -> Any:
        websocket = self._websocket
        connection_token = self._connection_token
        if websocket is None or connection_token is None:
            raise CodexDisconnected("Codex app-server is not connected")
        try:
            await asyncio.wait_for(
                self._pending_slots.acquire(),
                timeout=self.admission_timeout,
            )
        except TimeoutError as exc:
            raise TimeoutError("Codex RPC admission queue is full") from exc
        request_id = self._next_request_id()
        future: asyncio.Future[Json] = asyncio.get_running_loop().create_future()
        pending = _PendingRpc(future, connection_token)
        self._pending[request_id] = pending
        if method in {"thread/start", "thread/fork"} and params.get("ephemeral") is True:
            self._ephemeral_request_ids.add(request_id)
        try:
            await self._send(
                {"id": request_id, "method": method, "params": params},
                websocket=websocket,
                connection_token=connection_token,
            )
            response = await asyncio.wait_for(future, timeout)
        finally:
            self._pending.pop(request_id, None)
            self._ephemeral_request_ids.discard(request_id)
            self._release_rpc_slot(pending)
        if isinstance(response.get("error"), dict):
            raise CodexRpcError(method, response["error"])
        return response.get("result")

    async def request(self, method: str, params: Json | None = None, timeout: float = 30.0) -> Any:
        await self.wait_connected()
        return await self._request_on_connection(method, params or {}, timeout)

    async def respond(
        self,
        request_id: int | str,
        result: Json,
        *,
        generation: int | None = None,
    ) -> None:
        if generation is not None and generation != self.generation:
            raise CodexDisconnected("Codex server request belongs to a stale generation")
        await self._send({"id": request_id, "result": result})

    async def respond_error(
        self,
        request_id: int | str,
        code: int,
        message: str,
        *,
        generation: int | None = None,
    ) -> None:
        if generation is not None and generation != self.generation:
            raise CodexDisconnected("Codex server request belongs to a stale generation")
        await self._send({"id": request_id, "error": {"code": code, "message": message}})

    def _enqueue_server_request(
        self,
        request_id: int | str,
        method: str,
        params: Json,
        generation: int,
        connection_token: object,
    ) -> None:
        if len(self._server_request_queue) >= self.server_request_capacity:
            raise CodexDisconnected(
                f"Codex server request queue exceeded {self.server_request_capacity} entries"
            )
        thread_id = self._notification_thread_id(params)
        thread_key = thread_id or f"request:{request_id}"
        self._server_request_queue.append(
            _ServerRequestEnvelope(
                request_id,
                method,
                params,
                generation,
                connection_token,
                thread_key,
            )
        )
        self._pump_server_requests()

    def _pump_server_requests(self) -> None:
        while (
            self._server_request_queue
            and len(self._server_request_tasks) < self.server_request_concurrency
        ):
            selected_index = next(
                (
                    index
                    for index, envelope in enumerate(self._server_request_queue)
                    if envelope.thread_key not in self._server_active_threads
                ),
                None,
            )
            if selected_index is None:
                return
            envelope = self._server_request_queue[selected_index]
            del self._server_request_queue[selected_index]
            self._server_active_threads.add(envelope.thread_key)
            task = asyncio.create_task(
                self._dispatch_server_request(
                    envelope.request_id,
                    envelope.method,
                    envelope.params,
                    envelope.generation,
                    connection_token=envelope.connection_token,
                ),
                name=f"codex-server-request-{envelope.method}",
            )
            self._server_request_tasks.add(task)
            self._server_request_envelopes[task] = envelope
            task.add_done_callback(self._server_request_finished)

    def _server_request_finished(self, task: asyncio.Task[None]) -> None:
        self._server_request_tasks.discard(task)
        envelope = self._server_request_envelopes.pop(task, None)
        if envelope is not None:
            self._server_active_threads.discard(envelope.thread_key)
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                LOGGER.error(
                    "Codex server request handler failed (%s)",
                    type(error).__name__,
                )
        if not self._stopping.is_set():
            self._pump_server_requests()

    async def _cancel_server_request_tasks(self) -> None:
        self._server_request_queue.clear()
        while self._server_request_tasks:
            tasks = tuple(self._server_request_tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._server_request_tasks.difference_update(tasks)
        self._server_request_envelopes.clear()
        self._server_active_threads.clear()

    async def _fail_pending(self, error: Exception) -> None:
        await self._cancel_server_request_tasks()
        pending, self._pending = self._pending, {}
        self._ephemeral_request_ids.clear()
        for request in pending.values():
            if not request.future.done():
                request.future.set_exception(error)
            self._release_rpc_slot(request)
        isolated, self._isolated_questions = self._isolated_questions, {}
        self._ephemeral_thread_ids.clear()
        for question in isolated.values():
            if not question.future.done():
                question.future.set_exception(error)

    def _release_rpc_slot(self, pending: _PendingRpc) -> None:
        if pending.permit_released:
            return
        pending.permit_released = True
        self._pending_slots.release()

    def health_snapshot(self) -> dict[str, Any]:
        queue = self._notification_queue
        return {
            "connected": self.connected,
            "generation": self.generation,
            "pending_rpc": len(self._pending),
            "pending_rpc_capacity": self._pending_rpc_limit,
            "notification_queued": queue.qsize() if queue is not None else 0,
            "notification_capacity": self.notification_capacity,
            "server_request_queued": len(self._server_request_queue),
            "server_request_running": len(self._server_request_tasks),
            "server_request_capacity": self.server_request_capacity,
        }

    def _register_ephemeral_response(self, request_id: Any, message: Json) -> None:
        if request_id not in self._ephemeral_request_ids or isinstance(message.get("error"), dict):
            return
        result = message.get("result")
        result = result if isinstance(result, dict) else {}
        thread = result.get("thread")
        if isinstance(thread, dict) and thread.get("id"):
            self._ephemeral_thread_ids.add(str(thread["id"]))

    @staticmethod
    def _notification_thread_id(params: Json) -> str | None:
        if params.get("threadId"):
            return str(params["threadId"])
        thread = params.get("thread")
        if isinstance(thread, dict) and thread.get("id"):
            return str(thread["id"])
        return None

    async def _dispatch_notification(self, method: str, params: Json) -> None:
        thread = params.get("thread")
        if isinstance(thread, dict) and thread.get("ephemeral") is True and thread.get("id"):
            self._ephemeral_thread_ids.add(str(thread["id"]))
        thread_id = self._notification_thread_id(params)
        if thread_id and thread_id in self._ephemeral_thread_ids:
            question = self._isolated_questions.get(thread_id)
            if question:
                question.ingest(method, params)
            if method in {"thread/closed", "thread/deleted"}:
                if question and not question.future.done():
                    question.future.set_exception(
                        RuntimeError("Codex side-question thread closed before completion")
                    )
                self._ephemeral_thread_ids.discard(thread_id)
            return
        await self.on_notification(method, params)

    async def _dispatch_server_request(
        self,
        request_id: int | str,
        method: str,
        params: Json,
        generation: int,
        *,
        connection_token: object | None = None,
    ) -> None:
        if connection_token is not None and connection_token is not self._connection_token:
            raise CodexDisconnected("Codex server request belongs to a stale connection")
        thread_id = self._notification_thread_id(params)
        if thread_id and thread_id in self._ephemeral_thread_ids:
            await self.respond_error(
                request_id,
                -32600,
                "Interactive requests are disabled for isolated side questions",
                generation=generation,
            )
            return
        await self.on_server_request(request_id, method, params, generation)

    async def list_thread_page(
        self,
        *,
        limit: int = 100,
        cursor: str | None = None,
        search_term: str | None = None,
    ) -> ThreadPage:
        params: Json = {
            "limit": max(1, limit),
            "sortKey": "recency_at",
            "sortDirection": "desc",
            "useStateDbOnly": True,
        }
        if cursor:
            params["cursor"] = cursor
        if search_term and search_term.strip():
            params["searchTerm"] = search_term.strip()
        result = await self.request(
            "thread/list",
            params,
        )
        value = result if isinstance(result, dict) else {}
        return ThreadPage(
            data=[item for item in value.get("data") or [] if isinstance(item, dict)],
            next_cursor=str(value["nextCursor"]) if value.get("nextCursor") else None,
            backwards_cursor=str(value["backwardsCursor"]) if value.get("backwardsCursor") else None,
        )

    async def list_threads(
        self,
        limit: int = 100,
        *,
        cursor: str | None = None,
        search_term: str | None = None,
    ) -> list[Json]:
        if limit <= 0:
            return []
        threads: list[Json] = []
        next_cursor = cursor
        seen_cursors: set[str] = set()
        while len(threads) < limit:
            page = await self.list_thread_page(
                limit=limit - len(threads),
                cursor=next_cursor,
                search_term=search_term,
            )
            threads.extend(page.data[: limit - len(threads)])
            if not page.next_cursor or page.next_cursor in seen_cursors:
                break
            seen_cursors.add(page.next_cursor)
            next_cursor = page.next_cursor
        return threads

    async def loaded_threads(self) -> list[str]:
        result = await self.request("thread/loaded/list", {"limit": 1000})
        return [str(value) for value in (result or {}).get("data") or []]

    async def read_thread(self, thread_id: str, include_turns: bool = True) -> Json:
        result = await self.request("thread/read", {"threadId": thread_id, "includeTurns": include_turns})
        return dict((result or {}).get("thread") or {})

    async def resume_thread(self, thread_id: str) -> Json:
        result = await self.request(
            "thread/resume",
            {
                "threadId": thread_id,
                "excludeTurns": True,
                "initialTurnsPage": {
                    "limit": 1,
                    "sortDirection": "desc",
                    "itemsView": "notLoaded",
                },
            },
            timeout=60,
        )
        thread = _merge_thread_response(dict(result or {}))
        initial_page = (result or {}).get("initialTurnsPage") or {}
        turns = initial_page.get("data") if isinstance(initial_page, dict) else None
        if isinstance(turns, list) and turns:
            # thread/resume excludes history, but steering still needs the active turn ID.
            thread["turns"] = [turns[0]]
        return thread

    async def get_goal(self, thread_id: str) -> Json | None:
        result = await self.request("thread/goal/get", {"threadId": thread_id})
        goal = (result or {}).get("goal")
        return dict(goal) if isinstance(goal, dict) else None

    async def list_collaboration_modes(self) -> list[Json]:
        """Return validated collaboration-mode masks advertised by app-server."""
        try:
            result = await self.request("collaborationMode/list", {})
        except CodexRpcError as exc:
            raise RuntimeError(
                "This Codex app-server does not provide collaboration modes"
            ) from exc
        if not isinstance(result, dict) or not isinstance(result.get("data"), list):
            raise RuntimeError("Codex returned an invalid collaboration-mode response")

        modes: list[Json] = []
        for item in result["data"]:
            if not isinstance(item, dict):
                raise RuntimeError("Codex returned an invalid collaboration-mode entry")
            name = item.get("name")
            mode = item.get("mode")
            model = item.get("model")
            effort = item.get("reasoning_effort")
            if not isinstance(name, str) or not name.strip():
                raise RuntimeError("Codex returned a collaboration mode without a name")
            if mode is not None and mode not in {"default", "plan"}:
                raise RuntimeError(f"Codex returned an unknown collaboration mode: {mode!r}")
            if model is not None and not isinstance(model, str):
                raise RuntimeError("Codex returned an invalid collaboration-mode model")
            if effort is not None and not isinstance(effort, str):
                raise RuntimeError("Codex returned an invalid collaboration-mode effort")
            modes.append(dict(item))
        return modes

    async def list_model_options(self, *, page_size: int = 100) -> list[ModelOption]:
        """Return the complete validated model picker advertised by app-server."""
        if page_size <= 0:
            raise ValueError("Model page size must be positive")
        options: list[ModelOption] = []
        seen_models: set[str] = set()
        seen_cursors: set[str] = set()
        cursor: str | None = None
        while True:
            params: Json = {"limit": page_size}
            if cursor is not None:
                params["cursor"] = cursor
            result = await self.request("model/list", params)
            if not isinstance(result, dict) or not isinstance(result.get("data"), list):
                raise RuntimeError("Codex returned an invalid model-list response")
            for item in result["data"]:
                if not isinstance(item, dict):
                    raise RuntimeError("Codex returned an invalid model-list entry")
                model = item.get("model")
                display_name = item.get("displayName")
                default_effort = item.get("defaultReasoningEffort")
                is_default = item.get("isDefault")
                raw_efforts = item.get("supportedReasoningEfforts")
                if not isinstance(model, str) or not model.strip():
                    raise RuntimeError("Codex returned a model without an identifier")
                if not isinstance(display_name, str) or not display_name.strip():
                    raise RuntimeError(f"Codex model {model!r} has no display name")
                if not isinstance(default_effort, str) or not default_effort.strip():
                    raise RuntimeError(f"Codex model {model!r} has no default effort")
                if not isinstance(is_default, bool) or not isinstance(raw_efforts, list):
                    raise RuntimeError(f"Codex model {model!r} has invalid picker metadata")
                efforts: list[str] = []
                for value in raw_efforts:
                    effort = value.get("reasoningEffort") if isinstance(value, dict) else None
                    if not isinstance(effort, str) or not effort.strip():
                        raise RuntimeError(f"Codex model {model!r} has an invalid effort")
                    normalized = effort.strip()
                    if normalized not in efforts:
                        efforts.append(normalized)
                normalized_model = model.strip()
                normalized_default = default_effort.strip()
                if not efforts or normalized_default not in efforts:
                    raise RuntimeError(f"Codex model {model!r} has inconsistent effort metadata")
                if normalized_model in seen_models:
                    raise RuntimeError(f"Codex returned duplicate model {normalized_model!r}")
                seen_models.add(normalized_model)
                options.append(
                    ModelOption(
                        model=normalized_model,
                        display_name=display_name.strip(),
                        supported_efforts=tuple(efforts),
                        default_effort=normalized_default,
                        is_default=is_default,
                    )
                )
            next_cursor = result.get("nextCursor")
            if next_cursor is None:
                break
            if not isinstance(next_cursor, str) or not next_cursor.strip():
                raise RuntimeError("Codex returned an invalid model-list cursor")
            cursor = next_cursor.strip()
            if cursor in seen_cursors:
                raise RuntimeError("Codex returned a repeated model-list cursor")
            seen_cursors.add(cursor)
        return options

    async def resolve_collaboration_mode(
        self,
        mode: str,
        *,
        model: str | None = None,
        effort: str | None = None,
    ) -> Json:
        """Build a turn/start collaborationMode payload or fail closed."""
        if mode not in {"default", "plan"}:
            raise ValueError(f"Unsupported collaboration mode: {mode!r}")
        if (model is None) != (effort is None):
            raise ValueError("Collaboration model and effort must be provided together")
        if model is not None and effort is not None:
            normalized_model = model.strip()
            normalized_effort = effort.strip()
            if not normalized_model or not normalized_effort:
                raise ValueError("Collaboration model and effort must not be empty")
            return {
                "mode": mode,
                "settings": {
                    "model": normalized_model,
                    "reasoning_effort": normalized_effort,
                    "developer_instructions": None,
                },
            }
        for item in await self.list_collaboration_modes():
            if item.get("mode") != mode:
                continue
            model = item.get("model")
            effort = item.get("reasoning_effort")
            if not isinstance(model, str) or not model.strip():
                raise RuntimeError(f"Codex collaboration mode {mode!r} has no model")
            if effort is not None and (not isinstance(effort, str) or not effort.strip()):
                raise RuntimeError(f"Codex collaboration mode {mode!r} has invalid effort")
            return {
                "mode": mode,
                "settings": {
                    "model": model,
                    "reasoning_effort": effort,
                    "developer_instructions": None,
                },
            }
        raise RuntimeError(f"Codex collaboration mode {mode!r} is unavailable")

    async def update_thread_settings(
        self,
        thread_id: str,
        *,
        model: str | None = None,
        effort: str | None = None,
        collaboration_mode: Json | None = None,
        permissions: str | None = None,
        sandbox_policy: Json | None = None,
        approval_policy: str | Json | None = None,
        approvals_reviewer: str | None = None,
    ) -> None:
        """Update sticky settings used by subsequent turns in a thread."""
        normalized_thread_id = thread_id.strip()
        if not normalized_thread_id:
            raise ValueError("Thread ID must not be empty")
        if collaboration_mode is not None and (model is not None or effort is not None):
            raise ValueError("collaboration_mode cannot be combined with model or effort")
        if permissions is not None and sandbox_policy is not None:
            raise ValueError("permissions cannot be combined with sandbox_policy")
        params: Json = {"threadId": normalized_thread_id}
        if collaboration_mode is not None:
            mode = collaboration_mode.get("mode")
            settings = collaboration_mode.get("settings")
            profile_model = settings.get("model") if isinstance(settings, dict) else None
            profile_effort = settings.get("reasoning_effort") if isinstance(settings, dict) else None
            if mode not in {"default", "plan"}:
                raise ValueError("Collaboration mode is invalid")
            if not isinstance(profile_model, str) or not profile_model.strip():
                raise ValueError("Collaboration mode model is invalid")
            if profile_effort is not None and (
                not isinstance(profile_effort, str) or not profile_effort.strip()
            ):
                raise ValueError("Collaboration mode effort is invalid")
            params["collaborationMode"] = collaboration_mode
        else:
            if model is not None:
                if not model.strip():
                    raise ValueError("Model must not be empty")
                params["model"] = model.strip()
            if effort is not None:
                if not effort.strip():
                    raise ValueError("Effort must not be empty")
                params["effort"] = effort.strip()
        if permissions is not None:
            if not permissions.strip():
                raise ValueError("Permissions must not be empty")
            params["permissions"] = permissions.strip()
        if sandbox_policy is not None:
            params["sandboxPolicy"] = sandbox_policy
        if approval_policy is not None:
            params["approvalPolicy"] = approval_policy
        if approvals_reviewer is not None:
            if not approvals_reviewer.strip():
                raise ValueError("Approvals reviewer must not be empty")
            params["approvalsReviewer"] = approvals_reviewer.strip()
        if len(params) == 1:
            raise ValueError("At least one thread setting must be provided")
        await self.request("thread/settings/update", params, timeout=30)

    async def start_thread(self, cwd: Path, *, ephemeral: bool = False, read_only: bool = False) -> Json:
        params: Json = {
            "cwd": str(cwd),
            "ephemeral": ephemeral,
            "sandbox": "read-only" if read_only else "workspace-write",
            "approvalPolicy": "never" if read_only else "on-request",
        }
        result = await self.request("thread/start", params, timeout=60)
        return _merge_thread_response(dict(result or {}))

    async def fork_thread(
        self,
        thread_id: str,
        cwd: Path,
        *,
        developer_instructions: str | None = None,
    ) -> Json:
        params: Json = {
            "threadId": thread_id,
            "cwd": str(cwd),
            "ephemeral": True,
            "excludeTurns": True,
            "sandbox": "read-only",
            "approvalPolicy": "never",
        }
        if developer_instructions is not None:
            params["developerInstructions"] = developer_instructions
        result = await self.request(
            "thread/fork",
            params,
            timeout=60,
        )
        thread = dict((result or {}).get("thread") or {})
        if thread.get("id"):
            self._ephemeral_thread_ids.add(str(thread["id"]))
        return thread

    async def delete_thread(self, thread_id: str) -> None:
        with contextlib.suppress(CodexRpcError):
            await self.request("thread/delete", {"threadId": thread_id}, timeout=30)

    async def run_ephemeral_turn(
        self,
        cwd: Path,
        prompt: str,
        *,
        client_message_id: str | None = None,
        base_thread_id: str | None = None,
        developer_instructions: str | None = None,
        output_schema: Json | None = None,
        model: str | None = None,
        effort: str | None = None,
        timeout: float = 300.0,
    ) -> str:
        thread_id: str | None = None
        turn_id: str | None = None
        isolated: _IsolatedQuestion | None = None
        try:
            async with asyncio.timeout(timeout):
                if base_thread_id:
                    thread = await self.fork_thread(
                        base_thread_id,
                        cwd,
                        developer_instructions=developer_instructions,
                    )
                else:
                    thread = await self.start_thread(cwd, ephemeral=True, read_only=True)
                thread_id = str(thread.get("id") or "")
                if not thread_id:
                    raise RuntimeError("Codex did not return an ID for the ephemeral thread")
                self._ephemeral_thread_ids.add(thread_id)

                future: asyncio.Future[str] = asyncio.get_running_loop().create_future()
                isolated = _IsolatedQuestion(thread_id=thread_id, future=future)
                self._isolated_questions[thread_id] = isolated
                try:
                    turn = await self.start_turn(
                        thread_id,
                        [text_input(prompt)],
                        client_message_id=client_message_id or f"ephemeral-{thread_id}",
                        output_schema=output_schema,
                        cwd=cwd,
                        sandbox_policy={"type": "readOnly", "networkAccess": False},
                        approval_policy="never",
                        model=model,
                        effort=effort,
                    )
                except CodexRpcError as exc:
                    if model is not None or effort is not None:
                        model_label = model or "inherited model"
                        effort_label = effort or "inherited effort"
                        raise RuntimeError(
                            "Configured utility model or effort was rejected by Codex "
                            f"({model_label}, {effort_label}): "
                            f"{exc.error.get('message', 'unknown app-server error')}"
                        ) from exc
                    raise
                turn_id = str(turn.get("id") or "")
                if not turn_id:
                    raise RuntimeError("Codex did not return an ID for the ephemeral turn")
                isolated.bind_turn(turn_id)
                if str(turn.get("status") or "inProgress") != "inProgress":
                    isolated.ingest("turn/completed", {"threadId": thread_id, "turn": turn})
                return await future
        finally:
            if thread_id and self._isolated_questions.get(thread_id) is isolated:
                self._isolated_questions.pop(thread_id, None)
            if isolated:
                if not isolated.future.done():
                    isolated.future.cancel()
                elif not isolated.future.cancelled():
                    isolated.future.exception()
            interrupt_id = turn_id
            if not interrupt_id and isolated and len(isolated.turns) == 1:
                interrupt_id = next(iter(isolated.turns))
            if thread_id and interrupt_id and (
                not isolated or not isolated.turn_is_terminal(interrupt_id)
            ):
                await self._best_effort_request(
                    "turn/interrupt",
                    {"threadId": thread_id, "turnId": interrupt_id},
                )
            if thread_id:
                try:
                    await self._best_effort_request("thread/delete", {"threadId": thread_id})
                finally:
                    self._ephemeral_thread_ids.discard(thread_id)

    async def ask_fork_question(
        self,
        thread_id: str,
        cwd: Path,
        question: str,
        *,
        client_message_id: str,
        model: str | None = None,
        effort: str | None = None,
        timeout: float = 300.0,
    ) -> str:
        try:
            return await self.run_ephemeral_turn(
                cwd,
                question,
                client_message_id=client_message_id,
                base_thread_id=thread_id,
                developer_instructions=SIDE_QUESTION_INSTRUCTIONS,
                model=model,
                effort=effort,
                timeout=timeout,
            )
        except TimeoutError as exc:
            raise TimeoutError(f"Codex side question timed out after {timeout:g} seconds") from exc

    async def _best_effort_request(self, method: str, params: Json) -> None:
        with contextlib.suppress(Exception):
            async with asyncio.timeout(5):
                await self.request(method, params, timeout=5)

    async def start_turn(
        self,
        thread_id: str,
        inputs: list[Json],
        *,
        client_message_id: str | None = None,
        output_schema: Json | None = None,
        cwd: Path | None = None,
        sandbox_policy: Json | None = None,
        permissions: str | None = None,
        approval_policy: str | Json | None = None,
        approvals_reviewer: str | None = None,
        model: str | None = None,
        effort: str | None = None,
        collaboration_mode: Json | None = None,
    ) -> Json:
        if collaboration_mode is not None and (model is not None or effort is not None):
            raise ValueError("collaboration_mode cannot be combined with model or effort")
        if permissions is not None and sandbox_policy is not None:
            raise ValueError("permissions cannot be combined with sandbox_policy")
        params: Json = {"threadId": thread_id, "input": inputs}
        if client_message_id:
            params["clientUserMessageId"] = client_message_id
        if output_schema is not None:
            params["outputSchema"] = output_schema
        if cwd is not None:
            params["cwd"] = str(cwd)
        if sandbox_policy is not None:
            params["sandboxPolicy"] = sandbox_policy
        if permissions is not None:
            params["permissions"] = permissions
        if approval_policy is not None:
            params["approvalPolicy"] = approval_policy
        if approvals_reviewer is not None:
            params["approvalsReviewer"] = approvals_reviewer
        if model is not None:
            params["model"] = model
        if effort is not None:
            params["effort"] = effort
        if collaboration_mode is not None:
            params["collaborationMode"] = collaboration_mode
        result = await self.request("turn/start", params, timeout=60)
        return dict((result or {}).get("turn") or {})

    async def steer_turn(
        self, thread_id: str, turn_id: str, inputs: list[Json], *, client_message_id: str | None = None
    ) -> str:
        params: Json = {"threadId": thread_id, "expectedTurnId": turn_id, "input": inputs}
        if client_message_id:
            params["clientUserMessageId"] = client_message_id
        result = await self.request("turn/steer", params, timeout=30)
        return str((result or {}).get("turnId") or turn_id)


def text_input(text: str) -> Json:
    return {"type": "text", "text": text, "text_elements": []}


def file_input(path: Path, *, image: bool = False) -> Json:
    if image:
        return {"type": "localImage", "path": str(path), "detail": "auto"}
    return {"type": "mention", "name": path.name, "path": str(path)}
