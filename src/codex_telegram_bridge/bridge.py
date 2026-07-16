from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from telegram import Bot

from .codex import CodexClient, CodexRpcError, file_input, text_input
from .config import Config
from .dashboard import DashboardManager
from .files import FileCandidate, PathPolicy, cleanup_inbox
from .metrics import MetricsSampler
from .models import SessionSpace, ThreadState
from .outbound import OutboundMessenger
from .projector import EventProjector
from .resolver import CodexResolver, DirectoryIndex
from .store import Store
from .tmux import TmuxManager

LOGGER = logging.getLogger(__name__)
Json = dict[str, Any]
QuestionHandler = Callable[[str, Json], Awaitable[None]]
NoticeHandler = Callable[[str, str | None], Awaitable[None]]
StateChangeHandler = Callable[[ThreadState, str], Awaitable[None]]
QuestionResolvedHandler = Callable[[str], Awaitable[None]]


def _workspace_write_policy() -> Json:
    return {
        "type": "workspaceWrite",
        "writableRoots": [],
        "networkAccess": False,
        "excludeSlashTmp": True,
        "excludeTmpdirEnvVar": True,
    }


async def _noop_question(request_key: str, params: Json) -> None:
    del request_key, params


async def _noop_notice(message: str, thread_id: str | None) -> None:
    del message, thread_id


async def _noop_state_change(state: ThreadState, reason: str) -> None:
    del state, reason


async def _noop_question_resolved(request_key: str) -> None:
    del request_key


class Bridge:
    def __init__(self, config: Config, store: Store, bot: Bot, messenger: OutboundMessenger) -> None:
        self.config = config
        self.store = store
        self.bot = bot
        self.messenger = messenger
        self.on_question: QuestionHandler = _noop_question
        self.on_notice: NoticeHandler = _noop_notice
        self.on_state_change: StateChangeHandler = _noop_state_change
        self.on_question_resolved: QuestionResolvedHandler = _noop_question_resolved
        self.dashboard = DashboardManager(
            bot,
            store,
            messenger,
            owner_chat_id=lambda: owner.chat_id if (owner := store.get_owner()) else None,
            debounce_seconds=config.dashboard_debounce_seconds,
            heartbeat_seconds=config.heartbeat_seconds,
        )
        self.projector = EventProjector(store, self._on_state_change)
        self.client = CodexClient(
            config.codex_socket,
            on_notification=self._on_notification,
            on_server_request=self._on_server_request,
            on_connection=self._on_codex_connection,
        )
        self.tmux = TmuxManager(config.tmux_session, config.codex_binary, config.codex_socket)
        self.metrics = MetricsSampler(config.allowed_root)
        self.path_policy = PathPolicy(config.allowed_root, config.telegram_upload_limit)
        self.directory_index = DirectoryIndex(config.allowed_root)
        self.resolver = CodexResolver(self.client, self.path_policy, self.directory_index)
        self._queue_locks: dict[str, asyncio.Lock] = {}
        self._queue_retry_tasks: dict[str, asyncio.Task[None]] = {}
        self._space_locks: dict[str, asyncio.Lock] = {}
        self._pending_requests: dict[str, tuple[int | str, int]] = {}
        self._resolved_request_ids: dict[tuple[int, str], None] = {}
        self._maintenance_task: asyncio.Task[None] | None = None
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._cleanup_local_state()
        self.messenger.start()
        self.metrics.start()
        self.dashboard.start()
        await self.directory_index.refresh()
        self.client.start()
        await self.client.wait_connected(timeout=20)
        self._maintenance_task = asyncio.create_task(
            self._maintenance_loop(), name="bridge-local-maintenance"
        )

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        if self._maintenance_task:
            self._maintenance_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._maintenance_task
            self._maintenance_task = None
        retry_tasks = list(self._queue_retry_tasks.values())
        self._queue_retry_tasks.clear()
        for task in retry_tasks:
            task.cancel()
        if retry_tasks:
            await asyncio.gather(*retry_tasks, return_exceptions=True)
        await self.dashboard.stop()
        await self.metrics.stop()
        await self.client.stop()
        await self.messenger.stop()

    def _cleanup_local_state(self) -> None:
        protected = self.store.queued_file_paths() | self.store.pending_callback_file_paths()
        cleanup_inbox(
            self.config.inbox_dir,
            self.config.upload_retention_days,
            protected_paths=protected,
        )
        self.store.cleanup()

    async def _maintenance_loop(self) -> None:
        while True:
            await asyncio.sleep(3600)
            try:
                await asyncio.to_thread(self._cleanup_local_state)
            except Exception as exc:
                LOGGER.warning("Local maintenance failed (%s)", type(exc).__name__)

    async def _on_codex_connection(self, connected: bool, generation: int, reason: str | None) -> None:
        if connected:
            LOGGER.info("Connected to Codex app-server generation %s", generation)
            await self.resync()
        else:
            LOGGER.warning("Codex app-server disconnected: %s", reason)
            if self._started:
                await self.on_notice("Codex app-server 已断开，正在重连", None)

    async def resync(self) -> None:
        threads = await self.client.list_threads(limit=200)
        for payload in threads:
            if isinstance(payload, dict) and payload.get("id"):
                self.projector.apply_thread(payload)
        legacy_subscriptions = set(self.store.subscriptions())
        space_subscriptions = self._space_subscription_thread_ids()
        for thread_id in sorted(legacy_subscriptions | space_subscriptions):
            try:
                state = await self.subscribe_space_thread(thread_id)
                if thread_id in legacy_subscriptions:
                    await self.dashboard.schedule(state, immediate=True)
                if thread_id in space_subscriptions:
                    await self._notify_state_change(state, "thread/resynced")
                if state.status == "idle":
                    active_spaces = self._active_spaces_for_thread(thread_id)
                    for space in active_spaces:
                        asyncio.create_task(
                            self.dispatch_space_queue(
                                str(space["space_id"]), generation=int(space["generation"])
                            )
                        )
                    if thread_id in legacy_subscriptions:
                        asyncio.create_task(self.dispatch_queue(thread_id))
            except CodexRpcError, RuntimeError:
                LOGGER.exception("Failed to resync thread %s", thread_id)

    def _space_subscription_thread_ids(self) -> set[str]:
        return {
            str(space["thread_id"])
            for space in self.store.list_spaces()
            if space.get("thread_id") and space.get("lifecycle") != "closed"
        }

    def _active_spaces_for_thread(self, thread_id: str) -> list[Json]:
        return [
            space
            for space in self.store.list_spaces("active")
            if str(space.get("thread_id") or "") == thread_id
        ]

    async def subscribe_space_thread(self, thread_id: str) -> ThreadState:
        """Resume a thread for live events without creating a legacy private dashboard."""
        payload = await self.client.resume_thread(thread_id)
        state = self.projector.apply_thread(payload)
        state.goal = await self.client.get_goal(thread_id)
        state.subscribed = True
        self.store.save_thread(state)
        return state

    async def list_sessions(
        self, *, search_term: str | None = None, limit: int = 200
    ) -> list[ThreadState]:
        states: list[ThreadState] = []
        for payload in await self.client.list_threads(limit=limit, search_term=search_term):
            if isinstance(payload, dict) and payload.get("id"):
                states.append(self.projector.apply_thread(payload))
        return states

    async def resolve_thread(self, selector: str) -> ThreadState:
        selector = selector.strip().casefold()
        sessions = await self.list_sessions()
        exact = [state for state in sessions if state.thread_id.casefold() == selector]
        if exact:
            return exact[0]
        prefixes = [state for state in sessions if state.thread_id.casefold().startswith(selector)]
        if len(prefixes) == 1:
            return prefixes[0]
        title_matches = [state for state in sessions if selector in state.title.casefold()]
        if len(title_matches) == 1:
            return title_matches[0]
        if not prefixes and not title_matches:
            raise ValueError(f"没有找到 session: {selector}")
        raise ValueError(f"session 选择不唯一: {selector}")

    async def watch(self, thread_id: str) -> ThreadState:
        state = await self.subscribe_space_thread(thread_id)
        self.store.subscribe(thread_id)
        await self.dashboard.schedule(state, immediate=True)
        return state

    async def unwatch(self, thread_id: str) -> None:
        self.store.unsubscribe(thread_id)
        await self._unsubscribe_thread_if_unused(thread_id)

    async def _unsubscribe_thread_if_unused(self, thread_id: str) -> None:
        required = thread_id in self.store.subscriptions() or any(
            str(space.get("thread_id") or "") == thread_id
            and space.get("lifecycle") != "closed"
            for space in self.store.list_spaces()
        )
        state = self.store.get_thread(thread_id)
        if required:
            if state and not state.subscribed:
                state.subscribed = True
                self.store.save_thread(state)
            return
        if state:
            state.subscribed = False
            self.store.save_thread(state)
        with contextlib.suppress(CodexRpcError, RuntimeError):
            await self.client.request("thread/unsubscribe", {"threadId": thread_id})

    async def refresh(self, thread_id: str) -> ThreadState:
        payload = await self.client.read_thread(thread_id, include_turns=True)
        state = self.projector.apply_thread(payload)
        state.goal = await self.client.get_goal(thread_id)
        self.store.save_thread(state)
        await self.dashboard.schedule(state, immediate=True)
        return state

    async def new_session(self, cwd: Path, prompt: str, client_message_id: str) -> ThreadState:
        cwd = self.path_policy.validate_directory(cwd)
        payload = await self.client.start_thread(cwd)
        state = self.projector.apply_thread(payload)
        state.title = prompt[:80] or state.title
        self.store.subscribe(state.thread_id)
        state.subscribed = True
        self.store.save_thread(state)
        await self.tmux.ensure_window(state.thread_id, state.title, cwd)
        await self.client.start_turn(
            state.thread_id,
            [text_input(prompt)],
            client_message_id=client_message_id,
            cwd=cwd,
            sandbox_policy=_workspace_write_policy(),
            approval_policy="never",
        )
        await self.dashboard.schedule(state, immediate=True)
        return state

    async def activate_pending_session(
        self,
        space_id: str,
        *,
        client_message_id: str,
    ) -> ThreadState:
        lock = self._space_locks.setdefault(space_id, asyncio.Lock())
        async with lock:
            space = self._require_space(space_id)
            if space.active:
                return self.store.get_thread(space.thread_id or "") or await self.refresh(
                    space.thread_id or ""
                )
            if space.lifecycle not in {"pending", "repair_required"}:
                raise RuntimeError("Session space is no longer activatable")
            if not space.pending_cwd or not space.pending_prompt.strip():
                raise ValueError("Pending session requires a directory and initial prompt")

            created_now = False
            if space.thread_id:
                state = self.store.get_thread(space.thread_id) or await self.refresh(space.thread_id)
                cwd = self.path_policy.validate_directory(Path(state.cwd or space.pending_cwd))
            else:
                cwd = self.path_policy.validate_directory(Path(space.pending_cwd))
                payload = await self.client.start_thread(cwd)
                state = self.projector.apply_thread(payload)
                state.title = space.pending_prompt[:80] or state.title
                self.store.save_thread(state)
                space.thread_id = state.thread_id
                space.lifecycle = "repair_required"
                space.last_error = "activation in progress"
                self.store.save_session_space(space)
                created_now = True

            await self.tmux.ensure_window(state.thread_id, state.title, cwd)
            delivered = False if created_now else await self._client_message_exists(
                state.thread_id, client_message_id
            )
            if delivered is None:
                space.last_error = "initial prompt delivery could not be reconciled"
                self.store.save_session_space(space)
                raise RuntimeError(space.last_error)
            if not delivered:
                try:
                    await self.client.start_turn(
                        state.thread_id,
                        [text_input(space.pending_prompt)],
                        client_message_id=client_message_id,
                        cwd=cwd,
                        sandbox_policy=_workspace_write_policy(),
                        approval_policy="never",
                    )
                except Exception as exc:
                    delivered = await self._client_message_exists(state.thread_id, client_message_id)
                    if not delivered:
                        space.last_error = f"initial prompt delivery failed ({type(exc).__name__})"
                        self.store.save_session_space(space)
                        raise

            space.lifecycle = "active"
            space.last_error = ""
            space.pending_cwd = ""
            space.pending_prompt = ""
            self.store.save_session_space(space)
            await self._notify_state_change(state, "session/activated")
            return state

    async def send_space_prompt(
        self,
        space_id: str,
        prompt: str,
        *,
        mode: str = "auto",
        inputs: list[Json] | None = None,
        client_message_id: str | None = None,
    ) -> str:
        lock = self._space_locks.setdefault(space_id, asyncio.Lock())
        async with lock:
            space = self._require_active_space(space_id)
            return await self.send_prompt(
                space.thread_id or "",
                prompt,
                mode=mode,
                inputs=inputs,
                client_message_id=client_message_id,
                space_id=space.space_id,
                generation=space.generation,
            )

    async def ask_space_question(
        self,
        space_id: str,
        question: str,
        *,
        client_message_id: str,
    ) -> str:
        question = question.strip()
        if not question:
            raise ValueError("Side question cannot be empty")
        space = self._require_active_space(space_id)
        thread_id = space.thread_id or ""
        state = self.store.get_thread(thread_id)
        cwd_value = state.cwd if state else ""
        if not cwd_value:
            payload = await self.client.read_thread(thread_id, include_turns=False)
            cwd_value = str(payload.get("cwd") or "")
        if not cwd_value:
            raise ValueError("Session does not report a working directory")
        cwd = self.path_policy.validate_directory(Path(cwd_value))
        return await self.client.ask_fork_question(
            thread_id,
            cwd,
            question,
            client_message_id=client_message_id,
            model=self.config.ask_model,
            effort=self.config.ask_reasoning_effort,
        )

    async def dispatch_space_queue(
        self,
        space_id: str,
        *,
        generation: int | None = None,
    ) -> None:
        lock = self._space_locks.setdefault(space_id, asyncio.Lock())
        async with lock:
            try:
                space = self._require_active_space(space_id)
            except (RuntimeError, ValueError):
                return
            if generation is not None and space.generation != generation:
                return
            await self.dispatch_queue(
                space.thread_id or "",
                space_id=space.space_id,
                generation=space.generation,
            )

    async def close_session_space(self, space_id: str, generation: int) -> SessionSpace:
        lock = self._space_locks.setdefault(space_id, asyncio.Lock())
        async with lock:
            current = self._require_space(space_id)
            if current.generation != generation:
                raise RuntimeError("Session space generation is stale")
            if not self.store.close_space(space_id, expected_generation=generation):
                raise RuntimeError("Session space could not be closed")
            closed = self._require_space(space_id)
            if closed.thread_id:
                await self._unsubscribe_thread_if_unused(closed.thread_id)
                if state := self.store.get_thread(closed.thread_id):
                    state.queue_count = self.store.queue_count(closed.thread_id)
                    self.store.save_thread(state)
                    await self._notify_state_change(state, "session/closed")
            return closed

    def _require_space(self, space_id: str) -> SessionSpace:
        space = self.store.get_session_space(space_id)
        if not space:
            raise ValueError(f"Unknown session space: {space_id}")
        return space

    def _require_active_space(self, space_id: str) -> SessionSpace:
        space = self._require_space(space_id)
        if not space.active:
            raise RuntimeError("Session space is not active")
        return space

    async def attach(self, thread_id: str) -> str:
        state = self.store.get_thread(thread_id) or await self.refresh(thread_id)
        cwd = self.path_policy.validate_directory(state.cwd)
        return await self.tmux.ensure_window(thread_id, state.title, cwd)

    async def send_prompt(
        self,
        thread_id: str,
        prompt: str,
        *,
        mode: str = "auto",
        inputs: list[Json] | None = None,
        client_message_id: str | None = None,
        space_id: str | None = None,
        generation: int | None = None,
    ) -> str:
        if (space_id is None) != (generation is None):
            raise ValueError("space_id and generation must be supplied together")
        if space_id is not None:
            space = self._require_active_space(space_id)
            if space.generation != generation or space.thread_id != thread_id:
                raise RuntimeError("Session space generation is stale")
        payload = await self.client.resume_thread(thread_id)
        state = self.projector.apply_thread(payload)
        if not state.cwd:
            raise ValueError("Session does not report a working directory")
        cwd = self.path_policy.validate_directory(Path(state.cwd))
        client_message_id = client_message_id or f"telegram-{uuid.uuid4()}"
        values = list(inputs or [text_input(prompt)])
        if mode == "queue":
            self.store.enqueue_prompt(
                thread_id,
                prompt,
                values,
                client_message_id,
                space_id=space_id,
                generation=generation or 0,
            )
            state.queue_count = self._queue_count(thread_id, space_id, generation)
            self.store.save_thread(state)
            await self.dashboard.schedule(state)
            await self._notify_state_change(state, "queue/updated")
            if state.status == "idle":
                self._request_queue_retry(
                    thread_id,
                    delay=0,
                    space_id=space_id,
                    generation=generation,
                )
            return "queued"
        if state.status == "active" or state.turn_status == "inProgress":
            if mode != "steer":
                return "choose"
            if not state.turn_id:
                raise RuntimeError("Active turn ID is unavailable")
            await self.client.steer_turn(
                thread_id, state.turn_id, values, client_message_id=client_message_id
            )
            return "steered"
        await self.client.start_turn(
            thread_id,
            values,
            client_message_id=client_message_id,
            cwd=cwd,
            sandbox_policy=_workspace_write_policy(),
            approval_policy="never",
        )
        return "started"

    async def dispatch_queue(
        self,
        thread_id: str,
        *,
        space_id: str | None = None,
        generation: int | None = None,
    ) -> None:
        if (space_id is None) != (generation is None):
            raise ValueError("space_id and generation must be supplied together")
        queue_key = self._queue_key(thread_id, space_id, generation)
        lock = self._queue_locks.setdefault(queue_key, asyncio.Lock())
        async with lock:
            if space_id is not None:
                try:
                    space = self._require_active_space(space_id)
                except (RuntimeError, ValueError):
                    return
                if space.generation != generation or space.thread_id != thread_id:
                    return
            state = self.store.get_thread(thread_id)
            if not state or state.status != "idle":
                return
            queued = (
                self.store.next_space_prompt(space_id, generation)
                if space_id is not None and generation is not None
                else self.store.next_prompt(thread_id)
            )
            if not queued:
                return
            if isinstance(queued, dict):
                queue_id = int(queued["queue_id"])
                inputs = list(queued["inputs"])
                client_message_id = str(queued["client_message_id"])
            else:
                queue_id = queued.queue_id
                inputs = queued.inputs
                client_message_id = queued.client_message_id
            if not state.cwd:
                await self._mark_prompt_failed(
                    state,
                    queue_id,
                    "session cwd is unavailable",
                    space_id=space_id,
                    generation=generation,
                )
                return
            try:
                cwd = self.path_policy.validate_directory(Path(state.cwd))
            except (OSError, ValueError) as exc:
                await self._mark_prompt_failed(
                    state,
                    queue_id,
                    type(exc).__name__,
                    space_id=space_id,
                    generation=generation,
                )
                return
            delivered = await self._client_message_exists(thread_id, client_message_id)
            if delivered is None:
                LOGGER.warning("Cannot reconcile queued prompt %s; postponing delivery", queue_id)
                self._request_queue_retry(thread_id, space_id=space_id, generation=generation)
                return
            if delivered:
                await self._mark_prompt_dispatched(
                    state, queue_id, space_id=space_id, generation=generation
                )
                return
            try:
                await self.client.start_turn(
                    thread_id,
                    inputs,
                    client_message_id=client_message_id,
                    cwd=cwd,
                    sandbox_policy=_workspace_write_policy(),
                    approval_policy="never",
                )
            except Exception as exc:
                delivered = await self._client_message_exists(thread_id, client_message_id)
                if delivered:
                    await self._mark_prompt_dispatched(
                        state, queue_id, space_id=space_id, generation=generation
                    )
                elif delivered is False and isinstance(exc, CodexRpcError):
                    await self._mark_prompt_failed(
                        state,
                        queue_id,
                        type(exc).__name__,
                        space_id=space_id,
                        generation=generation,
                    )
                    return
                elif delivered is None:
                    LOGGER.warning(
                        "Prompt %s has uncertain delivery; reconciliation is required before retry",
                        queue_id,
                    )
                else:
                    LOGGER.exception("Failed to dispatch queued prompt %s", queue_id)
                self._request_queue_retry(thread_id, space_id=space_id, generation=generation)
                return
            await self._mark_prompt_dispatched(
                state, queue_id, space_id=space_id, generation=generation
            )

    async def _mark_prompt_dispatched(
        self,
        state: ThreadState,
        queue_id: int,
        *,
        space_id: str | None = None,
        generation: int | None = None,
    ) -> None:
        self.store.mark_prompt_dispatched(queue_id)
        thread_id = state.thread_id
        state.queue_count = self._queue_count(thread_id, space_id, generation)
        self.store.save_thread(state)
        await self.dashboard.schedule(state)
        await self._notify_state_change(state, "queue/updated")
        if state.status == "idle" and state.queue_count:
            self._request_queue_retry(
                thread_id,
                delay=1.0,
                space_id=space_id,
                generation=generation,
            )

    async def _mark_prompt_failed(
        self,
        state: ThreadState,
        queue_id: int,
        reason: str,
        *,
        space_id: str | None = None,
        generation: int | None = None,
    ) -> None:
        self.store.mark_prompt_failed(queue_id)
        thread_id = state.thread_id
        state.queue_count = self._queue_count(thread_id, space_id, generation)
        state.last_error = f"Queued prompt rejected ({reason})"
        self.store.save_thread(state)
        await self.dashboard.schedule(state)
        await self._notify_state_change(state, "queue/failed")
        await self.on_notice("一个 queued prompt 无法安全投递，已标记失败并继续队列。", thread_id)
        if state.status == "idle" and state.queue_count:
            self._request_queue_retry(
                thread_id,
                delay=1.0,
                space_id=space_id,
                generation=generation,
            )

    def _schedule_queue_retry(
        self,
        thread_id: str,
        *,
        delay: float = 5.0,
        space_id: str | None = None,
        generation: int | None = None,
    ) -> None:
        if not self._started:
            return
        queue_key = self._queue_key(thread_id, space_id, generation)
        current = self._queue_retry_tasks.get(queue_key)
        running = asyncio.current_task()
        if current and current is not running and not current.done():
            return

        async def retry() -> None:
            try:
                await asyncio.sleep(delay)
                if self._started:
                    if space_id is None:
                        await self.dispatch_queue(thread_id)
                    else:
                        await self.dispatch_space_queue(space_id, generation=generation)
            finally:
                if self._queue_retry_tasks.get(queue_key) is asyncio.current_task():
                    self._queue_retry_tasks.pop(queue_key, None)

        self._queue_retry_tasks[queue_key] = asyncio.create_task(
            retry(), name=f"queue-retry-{queue_key[:32]}"
        )

    def _request_queue_retry(
        self,
        thread_id: str,
        *,
        delay: float = 5.0,
        space_id: str | None = None,
        generation: int | None = None,
    ) -> None:
        if space_id is None:
            self._schedule_queue_retry(thread_id, delay=delay)
        else:
            self._schedule_queue_retry(
                thread_id,
                delay=delay,
                space_id=space_id,
                generation=generation,
            )

    def _queue_count(
        self,
        thread_id: str,
        space_id: str | None,
        generation: int | None,
    ) -> int:
        if space_id is not None and generation is not None:
            return len(self.store.space_queue_entries(space_id, generation))
        return self.store.queue_count(thread_id)

    @staticmethod
    def _queue_key(
        thread_id: str,
        space_id: str | None,
        generation: int | None,
    ) -> str:
        if space_id is not None and generation is not None:
            return f"space:{space_id}:{generation}"
        return thread_id

    async def _client_message_exists(self, thread_id: str, client_message_id: str) -> bool | None:
        try:
            payload = await self.client.read_thread(thread_id, include_turns=True)
        except Exception:
            return None
        for turn in payload.get("turns") or []:
            for item in (turn or {}).get("items") or []:
                if (
                    isinstance(item, dict)
                    and item.get("type") == "userMessage"
                    and str(item.get("clientId") or "") == client_message_id
                ):
                    return True
        return False

    async def resolve_directory(self, description: str) -> list[Path]:
        return await self.resolver.resolve_directory(description)

    async def resolve_files(self, thread_id: str, description: str) -> list[FileCandidate]:
        state = self.store.get_thread(thread_id) or await self.refresh(thread_id)
        return await self.resolver.resolve_files(thread_id, Path(state.cwd), description)

    async def send_upload(
        self,
        thread_id: str,
        path: Path,
        caption: str,
        *,
        mode: str,
        image: bool,
        client_message_id: str,
    ) -> str:
        explanation = caption.strip() or f"请读取并处理 Telegram 上传的文件：{path.name}"
        inputs = [text_input(explanation), file_input(path, image=image)]
        return await self.send_prompt(
            thread_id,
            explanation,
            mode=mode,
            inputs=inputs,
            client_message_id=client_message_id,
        )

    async def send_space_upload(
        self,
        space_id: str,
        path: Path,
        caption: str,
        *,
        mode: str,
        image: bool,
        client_message_id: str,
    ) -> str:
        explanation = caption.strip() or f"请读取并处理 Telegram 上传的文件：{path.name}"
        inputs = [text_input(explanation), file_input(path, image=image)]
        return await self.send_space_prompt(
            space_id,
            explanation,
            mode=mode,
            inputs=inputs,
            client_message_id=client_message_id,
        )

    async def answer_question(self, request_key: str, answers: dict[str, list[str]]) -> None:
        pending = self._pending_requests.get(request_key)
        stored = self.store.get_pending_input(request_key)
        if not pending or not stored:
            raise RuntimeError("该问题已过期或已由其他客户端回答")
        request_id, generation = pending
        if generation != self.client.generation:
            raise RuntimeError("Codex 连接已经重建，原问题已失效")
        result = {"answers": {key: {"answers": values} for key, values in answers.items()}}
        await self.client.respond(request_id, result)
        await self._notify_question_resolved(request_key)
        self._pending_requests.pop(request_key, None)
        self.store.delete_pending_input(request_key)

    async def _on_notification(self, method: str, params: Json) -> None:
        if method == "serverRequest/resolved":
            raw_request_id = params.get("requestId")
            request_id = "" if raw_request_id is None else str(raw_request_id)
            tombstone = (self.client.generation, request_id)
            self._resolved_request_ids[tombstone] = None
            while len(self._resolved_request_ids) > 512:
                self._resolved_request_ids.pop(next(iter(self._resolved_request_ids)))
            lookup_id = raw_request_id if isinstance(raw_request_id, int | str) else request_id
            stale = self.store.pending_input_keys_for_request(lookup_id)
            stale.extend(
                key
                for key, (value, _) in self._pending_requests.items()
                if str(value) == request_id and key not in stale
            )
            for key in stale:
                await self._notify_question_resolved(key)
                self._pending_requests.pop(key, None)
                self.store.delete_pending_input(key)
            return
        await self.projector.ingest(method, params)

    async def _on_state_change(self, state: ThreadState, reason: str) -> None:
        immediate = reason in {"error", "turn/completed", "thread/goal/updated", "thread/status/changed"}
        await self.dashboard.schedule(state, immediate=immediate)
        await self._notify_state_change(state, reason)
        if reason == "thread/status/changed" and state.status == "idle":
            spaces = self._active_spaces_for_thread(state.thread_id)
            if spaces:
                for space in spaces:
                    asyncio.create_task(
                        self.dispatch_space_queue(
                            str(space["space_id"]), generation=int(space["generation"])
                        )
                    )
            else:
                asyncio.create_task(self.dispatch_queue(state.thread_id))

    async def _notify_state_change(self, state: ThreadState, reason: str) -> None:
        try:
            await self.on_state_change(state, reason)
        except Exception:
            LOGGER.exception("Session state hook failed for %s (%s)", state.thread_id, reason)

    async def _notify_question_resolved(self, request_key: str) -> None:
        try:
            await self.on_question_resolved(request_key)
        except Exception:
            LOGGER.exception("Failed to remove resolved Telegram question %s", request_key)

    async def _on_server_request(
        self, request_id: int | str, method: str, params: Json, generation: int
    ) -> None:
        tombstone = (generation, str(request_id))
        if tombstone in self._resolved_request_ids:
            self._resolved_request_ids.pop(tombstone, None)
            return
        thread_id = str(params.get("threadId") or "") or None
        if method == "item/tool/requestUserInput":
            questions = [value for value in params.get("questions") or [] if isinstance(value, dict)]
            if any(bool(question.get("isSecret")) for question in questions):
                await self.on_notice("Codex 正在请求敏感输入；请回到本机 tmux 回答", thread_id)
                return
            request_key = uuid.uuid4().hex[:16]
            auto_ms = params.get("autoResolutionMs")
            expires = int(time.time() + int(auto_ms) / 1000) if auto_ms else int(time.time()) + 900
            self._pending_requests[request_key] = (request_id, generation)
            self.store.put_pending_input(
                request_key,
                json.dumps(request_id),
                generation,
                str(params.get("threadId") or ""),
                str(params.get("turnId") or ""),
                str(params.get("itemId") or ""),
                questions,
                expires,
            )
            await self.on_question(request_key, params)
            return
        if method in {
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
            "item/permissions/requestApproval",
            "applyPatchApproval",
            "execCommandApproval",
        }:
            await self.on_notice("Codex 正在等待本机审批；Bot 不会自动批准", thread_id)
            return
        LOGGER.info("Ignoring unsupported app-server request %s", method)
