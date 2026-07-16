from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from websockets.asyncio.client import ClientConnection, unix_connect
from websockets.exceptions import ConnectionClosed

LOGGER = logging.getLogger(__name__)
Json = dict[str, Any]
NotificationHandler = Callable[[str, Json], Awaitable[None]]
ServerRequestHandler = Callable[[int | str, str, Json, int], Awaitable[None]]
ConnectionHandler = Callable[[bool, int, str | None], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class ThreadPage:
    data: list[Json]
    next_cursor: str | None = None
    backwards_cursor: str | None = None


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
    ) -> None:
        self.socket_path = socket_path
        self.on_notification = on_notification
        self.on_server_request = on_server_request
        self.on_connection = on_connection
        self._websocket: ClientConnection | None = None
        self._pending: dict[int, asyncio.Future[Json]] = {}
        self._ephemeral_request_ids: set[int] = set()
        self._ephemeral_thread_ids: set[str] = set()
        self._isolated_questions: dict[str, _IsolatedQuestion] = {}
        self._server_request_tasks: set[asyncio.Task[None]] = set()
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
                reader = asyncio.create_task(self._reader(websocket), name="codex-app-server-reader")
                await self._initialize()
                self.generation += 1
                self._connected.set()
                delay = 1.0
                await self.on_connection(True, self.generation, None)
                await reader
                reason = "connection closed"
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reason = f"{type(exc).__name__}: {exc}"
                LOGGER.warning("Codex app-server connection failed: %s", reason)
            finally:
                self._connected.clear()
                self._websocket = None
                if reader and not reader.done():
                    reader.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await reader
                await self._fail_pending(
                    CodexDisconnected(reason or "Codex app-server disconnected")
                )
                await self.on_connection(False, self.generation, reason)
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

    async def _reader(self, websocket: ClientConnection) -> None:
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
                    self._register_ephemeral_response(request_id, message)
                    future = self._pending.pop(request_id, None)
                    if future and not future.done():
                        future.set_result(message)
                    continue
                params = message.get("params")
                params = params if isinstance(params, dict) else {}
                if request_id is not None and isinstance(method, str):
                    task = asyncio.create_task(
                        self._dispatch_server_request(
                            request_id, method, params, self.generation
                        ),
                        name=f"codex-server-request-{method}",
                    )
                    self._server_request_tasks.add(task)
                    task.add_done_callback(self._server_request_finished)
                elif isinstance(method, str):
                    # Lifecycle notifications are ordered on the wire. Preserve that order so a
                    # late item event cannot overwrite a newer plan or thread status.
                    await self._dispatch_notification(method, params)
        except ConnectionClosed as exc:
            if not self._stopping.is_set():
                raise CodexDisconnected(str(exc)) from exc

    async def _send(self, message: Json) -> None:
        websocket = self._websocket
        if not websocket:
            raise CodexDisconnected("Codex app-server is not connected")
        encoded = json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        async with self._send_lock:
            await websocket.send(encoded)

    def _next_request_id(self) -> int:
        self._request_id += 1
        return self._request_id

    async def _request_on_connection(self, method: str, params: Json, timeout: float = 30.0) -> Any:
        request_id = self._next_request_id()
        future: asyncio.Future[Json] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        if method in {"thread/start", "thread/fork"} and params.get("ephemeral") is True:
            self._ephemeral_request_ids.add(request_id)
        try:
            await self._send({"id": request_id, "method": method, "params": params})
            response = await asyncio.wait_for(future, timeout)
        finally:
            self._pending.pop(request_id, None)
            self._ephemeral_request_ids.discard(request_id)
        if isinstance(response.get("error"), dict):
            raise CodexRpcError(method, response["error"])
        return response.get("result")

    async def request(self, method: str, params: Json | None = None, timeout: float = 30.0) -> Any:
        await self.wait_connected()
        return await self._request_on_connection(method, params or {}, timeout)

    async def respond(self, request_id: int | str, result: Json) -> None:
        await self._send({"id": request_id, "result": result})

    async def respond_error(self, request_id: int | str, code: int, message: str) -> None:
        await self._send({"id": request_id, "error": {"code": code, "message": message}})

    def _server_request_finished(self, task: asyncio.Task[None]) -> None:
        self._server_request_tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            LOGGER.error("Codex server request handler failed: %s", error)

    async def _cancel_server_request_tasks(self) -> None:
        while self._server_request_tasks:
            tasks = tuple(self._server_request_tasks)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._server_request_tasks.difference_update(tasks)

    async def _fail_pending(self, error: Exception) -> None:
        await self._cancel_server_request_tasks()
        pending, self._pending = self._pending, {}
        self._ephemeral_request_ids.clear()
        for future in pending.values():
            if not future.done():
                future.set_exception(error)
        isolated, self._isolated_questions = self._isolated_questions, {}
        self._ephemeral_thread_ids.clear()
        for question in isolated.values():
            if not question.future.done():
                question.future.set_exception(error)

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
        self, request_id: int | str, method: str, params: Json, generation: int
    ) -> None:
        thread_id = self._notification_thread_id(params)
        if thread_id and thread_id in self._ephemeral_thread_ids:
            await self.respond_error(
                request_id,
                -32600,
                "Interactive requests are disabled for isolated side questions",
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
        thread = dict((result or {}).get("thread") or {})
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

    async def resolve_collaboration_mode(self, mode: str) -> Json:
        """Build a turn/start collaborationMode payload or fail closed."""
        if mode not in {"default", "plan"}:
            raise ValueError(f"Unsupported collaboration mode: {mode!r}")
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

    async def start_thread(self, cwd: Path, *, ephemeral: bool = False, read_only: bool = False) -> Json:
        params: Json = {
            "cwd": str(cwd),
            "ephemeral": ephemeral,
            "sandbox": "read-only" if read_only else "workspace-write",
            "approvalPolicy": "never",
        }
        result = await self.request("thread/start", params, timeout=60)
        return dict((result or {}).get("thread") or {})

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

    async def ask_fork_question(
        self,
        thread_id: str,
        cwd: Path,
        question: str,
        *,
        client_message_id: str,
        model: str | None = None,
        effort: str | None = None,
        timeout: float = 180.0,
    ) -> str:
        fork_id: str | None = None
        turn_id: str | None = None
        isolated: _IsolatedQuestion | None = None
        try:
            async with asyncio.timeout(timeout):
                fork = await self.fork_thread(
                    thread_id,
                    cwd,
                    developer_instructions=SIDE_QUESTION_INSTRUCTIONS,
                )
                fork_id = str(fork.get("id") or "")
                if not fork_id:
                    raise RuntimeError("Codex did not return an ID for the side-question fork")
                future: asyncio.Future[str] = asyncio.get_running_loop().create_future()
                isolated = _IsolatedQuestion(thread_id=fork_id, future=future)
                self._isolated_questions[fork_id] = isolated

                try:
                    turn = await self.start_turn(
                        fork_id,
                        [text_input(question)],
                        client_message_id=client_message_id,
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
                            "Configured /ask model or effort was rejected by Codex "
                            f"({model_label}, {effort_label}): "
                            f"{exc.error.get('message', 'unknown app-server error')}"
                        ) from exc
                    raise
                turn_id = str(turn.get("id") or "")
                if not turn_id:
                    raise RuntimeError("Codex did not return an ID for the side-question turn")
                isolated.bind_turn(turn_id)
                if str(turn.get("status") or "inProgress") != "inProgress":
                    isolated.ingest("turn/completed", {"threadId": fork_id, "turn": turn})
                return await future
        except TimeoutError as exc:
            raise TimeoutError(f"Codex side question timed out after {timeout:g} seconds") from exc
        finally:
            if fork_id and self._isolated_questions.get(fork_id) is isolated:
                self._isolated_questions.pop(fork_id, None)
            if isolated:
                if not isolated.future.done():
                    isolated.future.cancel()
                elif not isolated.future.cancelled():
                    isolated.future.exception()
            interrupt_id = turn_id
            if not interrupt_id and isolated and len(isolated.turns) == 1:
                interrupt_id = next(iter(isolated.turns))
            if fork_id and interrupt_id and (
                not isolated or not isolated.turn_is_terminal(interrupt_id)
            ):
                await self._best_effort_request(
                    "turn/interrupt",
                    {"threadId": fork_id, "turnId": interrupt_id},
                )
            if fork_id:
                try:
                    await self._best_effort_request("thread/delete", {"threadId": fork_id})
                finally:
                    self._ephemeral_thread_ids.discard(fork_id)

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
        approval_policy: str | None = None,
        model: str | None = None,
        effort: str | None = None,
        collaboration_mode: Json | None = None,
    ) -> Json:
        if collaboration_mode is not None and (model is not None or effort is not None):
            raise ValueError("collaboration_mode cannot be combined with model or effort")
        params: Json = {"threadId": thread_id, "input": inputs}
        if client_message_id:
            params["clientUserMessageId"] = client_message_id
        if output_schema is not None:
            params["outputSchema"] = output_schema
        if cwd is not None:
            params["cwd"] = str(cwd)
        if sandbox_policy is not None:
            params["sandboxPolicy"] = sandbox_policy
        if approval_policy is not None:
            params["approvalPolicy"] = approval_policy
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
