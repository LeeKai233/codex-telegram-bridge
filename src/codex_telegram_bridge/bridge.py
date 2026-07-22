from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

from telegram import Bot

from .approval import (
    ApprovalDecision,
    approval_decision_is_available,
    approval_response_payload,
    command_approval_decisions,
    interactive_approval_decisions,
    normalize_command_approval_params,
)
from .codex import CodexClient, CodexRpcError, file_input, text_input
from .config import Config
from .dashboard import DashboardManager
from .delivery import TelegramDeliveryEngine
from .files import FileCandidate, PathPolicy, cleanup_inbox
from .metrics import MetricsSampler
from .models import ModelOption, ModelProfile, SessionSpace, ThreadState, plan_revision_key
from .outbound import OutboundMessenger
from .projector import EventProjector
from .resolver import CodexResolver, DirectoryIndex
from .store import Store
from .telegram_common import CONTROL_ROLE, TelegramEndpoint
from .tmux import TmuxManager

LOGGER = logging.getLogger(__name__)
Json = dict[str, Any]
QuestionHandler = Callable[[str, Json], Awaitable[None]]
CommandApprovalHandler = Callable[[str, Json], Awaitable[None]]
NoticeHandler = Callable[[str, str | None], Awaitable[None]]
StateChangeHandler = Callable[[ThreadState, str], Awaitable[None]]
QuestionResolvedHandler = Callable[[str], Awaitable[None]]
PlanCompletedHandler = Callable[[str, str, str, str], Awaitable[None]]
PromptCompletedHandler = Callable[[Json], Awaitable[None]]
TuiPlanApprovedHandler = Callable[[str, str], Awaitable[None]]

_FINAL_TURN_STATUSES = {"completed", "failed", "interrupted"}
_SUBAGENT_PROFILE_RETRY_DELAYS = (1.0, 5.0, 30.0, 120.0)
_THREAD_SNAPSHOT_SECONDS = 5.0
_TURN_RECONCILE_SECONDS = 1.0
_NOTIFICATION_EFFECT_CAPACITY = 256
_NOTIFICATION_EFFECT_DRAIN_SECONDS = 5.0
# The Codex TUI emits this fixed input after "Yes, implement this plan" is selected.
TUI_PLAN_APPROVAL_PROMPT = "Implement the plan."


@dataclass(slots=True)
class _ThreadSnapshot:
    generation: int
    payload: Json
    expires_at: float


@dataclass(slots=True)
class _TurnGate:
    client_message_id: str
    expected_turn_id: str | None = None


NotificationEffect = tuple[str, Callable[[], Awaitable[None]]]


def _workspace_write_policy() -> Json:
    return {
        "type": "workspaceWrite",
        "writableRoots": [],
        "networkAccess": False,
        "excludeSlashTmp": True,
        "excludeTmpdirEnvVar": True,
    }


def _writable_turn_security(state: ThreadState) -> Json:
    kwargs: Json = {
        "approval_policy": state.approval_policy
        if state.approval_policy is not None
        else "on-request"
    }
    if state.permissions:
        kwargs["permissions"] = state.permissions
    else:
        kwargs["sandbox_policy"] = state.sandbox_policy or _workspace_write_policy()
    if state.approvals_reviewer:
        kwargs["approvals_reviewer"] = state.approvals_reviewer
    return kwargs


def _thread_settings_security(state: ThreadState) -> Json:
    kwargs: Json = {}
    if state.permissions:
        kwargs["permissions"] = state.permissions
    elif state.sandbox_policy is not None:
        kwargs["sandbox_policy"] = state.sandbox_policy
    if state.approval_policy is not None:
        kwargs["approval_policy"] = state.approval_policy
    if state.approvals_reviewer:
        kwargs["approvals_reviewer"] = state.approvals_reviewer
    return kwargs


def _turn_error_kind(error: object) -> str:
    if not isinstance(error, dict):
        return ""
    info = error.get("codexErrorInfo")
    if isinstance(info, str):
        return info
    if isinstance(info, dict) and info:
        return str(next(iter(info)))
    return "turnFailed" if error else ""


def _is_tui_plan_approval_item(item: object) -> bool:
    if not isinstance(item, dict) or item.get("type") != "userMessage":
        return False
    if item.get("clientId") is not None:
        return False
    content = item.get("content")
    if not isinstance(content, list) or len(content) != 1:
        return False
    item = content[0]
    return (
        isinstance(item, dict)
        and item.get("type") == "text"
        and item.get("text") == TUI_PLAN_APPROVAL_PROMPT
    )


async def _noop_question(request_key: str, params: Json) -> None:
    del request_key, params


async def _noop_command_approval(request_key: str, params: Json) -> None:
    del request_key, params


async def _noop_notice(message: str, thread_id: str | None) -> None:
    del message, thread_id


async def _noop_state_change(state: ThreadState, reason: str) -> None:
    del state, reason


async def _noop_question_resolved(request_key: str) -> None:
    del request_key


async def _noop_plan_completed(thread_id: str, turn_id: str, item_id: str, text: str) -> None:
    del thread_id, turn_id, item_id, text


async def _noop_prompt_completed(run: Json) -> None:
    del run


async def _noop_tui_plan_approved(thread_id: str, turn_id: str) -> None:
    del thread_id, turn_id


class Bridge:
    def __init__(
        self,
        config: Config,
        store: Store,
        bot: Bot,
        messenger: OutboundMessenger,
        *,
        control_endpoint: TelegramEndpoint | None = None,
        delivery: TelegramDeliveryEngine | None = None,
        manage_messenger: bool = True,
    ) -> None:
        self.config = config
        self.store = store
        self.bot = bot
        self.messenger = messenger
        self.control_endpoint = control_endpoint or TelegramEndpoint(
            CONTROL_ROLE,
            bot,
            messenger,
        )
        self.delivery = delivery or TelegramDeliveryEngine(
            {CONTROL_ROLE: self.control_endpoint}
        )
        self._owns_delivery = delivery is None
        self._manage_messenger = manage_messenger
        self.on_question: QuestionHandler = _noop_question
        self.on_command_approval: CommandApprovalHandler = _noop_command_approval
        self.on_notice: NoticeHandler = _noop_notice
        self.on_state_change: StateChangeHandler = _noop_state_change
        self.on_question_resolved: QuestionResolvedHandler = _noop_question_resolved
        self.on_plan_completed: PlanCompletedHandler = _noop_plan_completed
        self.on_prompt_completed: PromptCompletedHandler = _noop_prompt_completed
        self.on_tui_plan_approved: TuiPlanApprovedHandler = _noop_tui_plan_approved
        self.dashboard = DashboardManager(
            self.control_endpoint,
            store,
            self.delivery,
            owner_chat_id=lambda: owner.chat_id if (owner := store.get_owner()) else None,
            debounce_seconds=config.dashboard_debounce_seconds,
            heartbeat_seconds=config.heartbeat_seconds,
        )
        self.projector = EventProjector(
            store,
            self._on_state_change,
            is_managed=self._notification_is_managed,
        )
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
        self._subagent_profile_tasks: dict[str, asyncio.Task[None]] = {}
        self._interest_cache: set[str] = set()
        self._interest_roots_snapshot: frozenset[str] | None = None
        self._provisional_interest: dict[str, int] = {}
        self._owned_thread_starts: dict[str, tuple[str, set[str]]] = {}
        self._space_locks: dict[str, asyncio.Lock] = {}
        self._pending_requests: dict[str, tuple[int | str, int]] = {}
        self._resolved_request_ids: dict[tuple[int, str], None] = {}
        self._notified_plan_items: dict[tuple[str, str, str], None] = {}
        self._terminal_turns: dict[tuple[str, str], tuple[str, str]] = {}
        self._thread_snapshots: dict[str, _ThreadSnapshot] = {}
        self._thread_refresh_tasks: dict[str, asyncio.Task[Json]] = {}
        self._turn_locks: dict[str, asyncio.Lock] = {}
        self._turn_gates: dict[str, _TurnGate] = {}
        self._turn_reconcile_tasks: dict[str, asyncio.Task[None]] = {}
        self._plan_decision_events: dict[tuple[str, int], asyncio.Event] = {}
        self._notification_effects: asyncio.Queue[NotificationEffect] = asyncio.Queue(
            maxsize=_NOTIFICATION_EFFECT_CAPACITY
        )
        self._notification_effect_task: asyncio.Task[None] | None = None
        self._accept_notification_effects = False
        self._maintenance_task: asyncio.Task[None] | None = None
        self._resync_started_at: int | None = None
        self._resync_finished_at: int | None = None
        self._resync_failures = 0
        self._resync_last_error: str | None = None
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._accept_notification_effects = True
        self._notification_effect_task = asyncio.create_task(
            self._run_notification_effects(), name="bridge-notification-effects"
        )
        if self._manage_messenger:
            self.messenger.start()
        if self._owns_delivery:
            self.delivery.start()
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
        profile_tasks = list(self._subagent_profile_tasks.values())
        self._subagent_profile_tasks.clear()
        for task in profile_tasks:
            task.cancel()
        if profile_tasks:
            await asyncio.gather(*profile_tasks, return_exceptions=True)
        reconcile_tasks = list(self._turn_reconcile_tasks.values())
        self._turn_reconcile_tasks.clear()
        for task in reconcile_tasks:
            task.cancel()
        if reconcile_tasks:
            await asyncio.gather(*reconcile_tasks, return_exceptions=True)
        refresh_tasks = list(self._thread_refresh_tasks.values())
        self._thread_refresh_tasks.clear()
        for task in refresh_tasks:
            task.cancel()
        if refresh_tasks:
            await asyncio.gather(*refresh_tasks, return_exceptions=True)
        self._thread_snapshots.clear()
        for thread_id in list(self._turn_gates):
            self._release_turn_gate(thread_id, None)
        await self.client.stop()
        await self._stop_notification_effects()
        await self.dashboard.stop()
        if self._owns_delivery:
            await self.delivery.stop()
        await self.metrics.stop()
        if self._manage_messenger:
            await self.messenger.stop()

    async def _run_notification_effects(self) -> None:
        while True:
            name, effect = await self._notification_effects.get()
            try:
                await effect()
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("event=notification_effect_failed effect=%s", name)
            finally:
                self._notification_effects.task_done()

    async def _stop_notification_effects(self) -> None:
        self._accept_notification_effects = False
        task = self._notification_effect_task
        self._notification_effect_task = None
        if task is None:
            return
        try:
            await asyncio.wait_for(
                self._notification_effects.join(),
                timeout=_NOTIFICATION_EFFECT_DRAIN_SECONDS,
            )
        except TimeoutError:
            LOGGER.warning(
                "event=notification_effect_shutdown_timeout pending=%s",
                self._notification_effects.qsize(),
            )
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        while True:
            try:
                self._notification_effects.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._notification_effects.task_done()

    async def _dispatch_notification_effect(self, name: str, effect: Callable[[], Awaitable[None]]) -> None:
        task = self._notification_effect_task
        if not self._accept_notification_effects or task is None or task.done():
            await effect()
            return
        if self._notification_effects.full():
            LOGGER.warning(
                "event=notification_effect_backpressure effect=%s capacity=%s",
                name,
                _NOTIFICATION_EFFECT_CAPACITY,
            )
        await self._notification_effects.put((name, effect))

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
            try:
                await asyncio.to_thread(self._cleanup_local_state)
            except Exception as exc:
                LOGGER.warning(
                    "event=bridge_maintenance_failed error_type=%s",
                    type(exc).__name__,
                )
            await asyncio.sleep(3600)

    async def _on_codex_connection(self, connected: bool, generation: int, reason: str | None) -> None:
        if connected:
            LOGGER.info("Connected to Codex app-server generation %s", generation)
            await self.resync()
        else:
            LOGGER.warning("Codex app-server disconnected: %s", reason)
            if self._started:
                await self.on_notice("Codex app-server 已断开，正在重连", None)

    async def resync(self) -> None:
        self._resync_started_at = int(time.time())
        self._resync_last_error = None
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
                        self._request_queue_retry(
                            thread_id,
                            delay=0.0,
                            space_id=str(space["space_id"]),
                            generation=int(space["generation"]),
                        )
                    if thread_id in legacy_subscriptions:
                        self._request_queue_retry(thread_id, delay=0.0)
            except (CodexRpcError, RuntimeError, TimeoutError) as exc:
                self._resync_failures += 1
                self._resync_last_error = type(exc).__name__
                LOGGER.exception("Failed to resync thread %s", thread_id)
        await self._hydrate_missing_subagent_profiles()
        self._resync_finished_at = int(time.time())

    def health_snapshot(self) -> dict[str, Any]:
        return {
            "codex": self.client.health_snapshot(),
            "queue_retry_tasks": len(self._queue_retry_tasks),
            "subagent_profile_tasks": len(self._subagent_profile_tasks),
            "managed_threads": len(self._managed_thread_ids()),
            "resync": {
                "started_at": self._resync_started_at,
                "finished_at": self._resync_finished_at,
                "failures": self._resync_failures,
                "last_error_type": self._resync_last_error,
            },
        }

    async def _hydrate_missing_subagent_profiles(self) -> None:
        seen: set[str] = set()
        for parent_id in self._managed_thread_ids():
            parent = self.store.get_thread(parent_id)
            if parent is None:
                continue
            for task in parent.tasks:
                child_id = str(task.agent_thread_id or task.task_id or "")
                if not child_id or child_id in seen:
                    continue
                seen.add(child_id)
                if task.status == "notFound":
                    continue
                child = self.store.get_thread(child_id)
                if child is not None and child.model and child.reasoning_effort:
                    continue
                self._ensure_subagent_profile_refresh(
                    child_id,
                    parent.thread_id,
                    task.agent_path,
                )

    def _interest_roots(self) -> set[str]:
        return set(self.store.subscriptions()) | self._space_subscription_thread_ids()

    def _managed_thread_ids(self) -> set[str]:
        roots = frozenset(self._interest_roots())
        if self._interest_roots_snapshot != roots:
            states = {state.thread_id: state for state in self.store.list_threads()}
            managed = set(roots)
            pending = list(roots)
            while pending:
                parent = states.get(pending.pop())
                if parent is None:
                    continue
                for task in parent.tasks:
                    child_id = str(task.agent_thread_id or task.task_id or "")
                    if not child_id or child_id in managed:
                        continue
                    managed.add(child_id)
                    pending.append(child_id)
            self._interest_cache = managed
            self._interest_roots_snapshot = roots
        return self._interest_cache | set(self._provisional_interest)

    def _invalidate_interest_cache(self) -> None:
        self._interest_roots_snapshot = None

    def _add_provisional_interest(self, thread_id: str) -> None:
        if thread_id:
            self._provisional_interest[thread_id] = self._provisional_interest.get(thread_id, 0) + 1

    def _remove_provisional_interest(self, thread_id: str) -> None:
        count = self._provisional_interest.get(thread_id, 0)
        if count <= 1:
            self._provisional_interest.pop(thread_id, None)
        else:
            self._provisional_interest[thread_id] = count - 1

    @staticmethod
    def _normalized_cwd(value: object) -> str:
        if not value:
            return ""
        return str(Path(str(value)).resolve(strict=False))

    def _begin_owned_thread_start(self, cwd: Path) -> str:
        token = uuid.uuid4().hex
        self._owned_thread_starts[token] = (self._normalized_cwd(cwd), set())
        return token

    def _claim_owned_thread(self, token: str, thread_id: str) -> None:
        entry = self._owned_thread_starts.get(token)
        if entry is None or not thread_id or thread_id in entry[1]:
            return
        entry[1].add(thread_id)
        self._add_provisional_interest(thread_id)

    def _claim_owned_thread_start(self, method: str, params: Json) -> str | None:
        if method != "thread/started":
            return None
        thread = params.get("thread")
        if not isinstance(thread, dict):
            return None
        thread_id = str(thread.get("id") or "")
        cwd = self._normalized_cwd(thread.get("cwd"))
        if not thread_id or not cwd:
            return None
        matched = False
        for token, (expected_cwd, _) in tuple(self._owned_thread_starts.items()):
            if cwd == expected_cwd:
                self._claim_owned_thread(token, thread_id)
                matched = True
        return thread_id if matched else None

    def _finish_owned_thread_start(self, token: str) -> None:
        entry = self._owned_thread_starts.pop(token, None)
        if entry is None:
            return
        for thread_id in entry[1]:
            self._remove_provisional_interest(thread_id)

    @staticmethod
    def _notification_thread_id(params: Json) -> str:
        if params.get("threadId"):
            return str(params["threadId"])
        thread = params.get("thread")
        if isinstance(thread, dict) and thread.get("id"):
            return str(thread["id"])
        return ""

    def _notification_is_managed(self, method: str, params: Json) -> bool:
        thread_id = self._notification_thread_id(params)
        if thread_id and thread_id in self._managed_thread_ids():
            return True
        claimed = self._claim_owned_thread_start(method, params)
        return bool(claimed and claimed == thread_id)

    def _space_subscription_thread_ids(self) -> set[str]:
        return {
            str(space["thread_id"])
            for space in self.store.list_spaces()
            if space.get("thread_id") and space.get("lifecycle") != "closed"
        }

    async def _notify_projector_parent_changes(self, reason: str) -> None:
        for parent in self.projector.take_parent_changes():
            await self._on_state_change(parent, reason)

    def _active_spaces_for_thread(self, thread_id: str) -> list[Json]:
        return [
            space
            for space in self.store.list_spaces("active")
            if str(space.get("thread_id") or "") == thread_id
        ]

    async def subscribe_space_thread(self, thread_id: str) -> ThreadState:
        """Resume a thread for live events without creating a legacy private dashboard."""
        payload = await self.client.resume_thread(thread_id)
        self._backfill_space_profiles(thread_id, payload)
        state = self.projector.apply_thread(payload)
        state = self.projector.apply_goal(state, await self.client.get_goal(thread_id))
        await self._notify_projector_parent_changes("subagent/resync")
        state.subscribed = True
        self.store.save_thread(state)
        return state

    def _backfill_space_profiles(self, thread_id: str, payload: Json) -> None:
        model = str(payload.get("model") or "").strip()
        effort = str(payload.get("reasoningEffort") or "").strip()
        if not model or not effort:
            return
        for raw in self.store.list_spaces():
            if str(raw.get("thread_id") or "") != thread_id:
                continue
            space = self.store.get_session_space(str(raw["space_id"]))
            if space is None or (space.normal_model and space.normal_effort):
                continue
            space.normal_model = space.normal_model or model
            space.normal_effort = space.normal_effort or effort
            space.plan_model = space.plan_model or model
            space.plan_effort = space.plan_effort or effort
            latest = self.store.latest_plan_publication(space.space_id, space.generation)
            if latest and latest.get("status") in {"published", "executing", "revising"}:
                space.current_mode = "plan"
            self.store.save_session_space(space)

    async def list_model_options(self) -> list[ModelOption]:
        return await self.client.list_model_options()

    async def resolve_model_profile(self, model: str, effort: str) -> ModelProfile:
        requested_model = model.strip().casefold()
        requested_effort = effort.strip().casefold()
        if not requested_model or not requested_effort:
            raise ValueError("Model and effort must not be empty")
        options = await self.list_model_options()
        matches: list[ModelOption] = []
        for option in options:
            aliases = {
                option.model.casefold(),
                option.display_name.casefold(),
                option.model.rsplit("-", 1)[-1].casefold(),
            }
            if requested_model in aliases:
                matches.append(option)
        unique = {option.model: option for option in matches}
        if len(unique) != 1:
            detail = "ambiguous" if unique else "unavailable"
            raise ValueError(f"Model {model!r} is {detail}")
        selected = next(iter(unique.values()))
        efforts = {value.casefold(): value for value in selected.supported_efforts}
        if requested_effort not in efforts:
            raise ValueError(f"Effort {effort!r} is unavailable for model {selected.model!r}")
        return ModelProfile(selected.model, efforts[requested_effort])

    async def prepare_directory_creation(self, value: str) -> Path | None:
        return await asyncio.to_thread(self.path_policy.prepare_directory_creation, value)

    async def create_project_directory(self, target: Path) -> Path:
        created = await asyncio.to_thread(self.path_policy.create_directory, target)
        await self.directory_index.refresh()
        return created

    async def list_sessions(self, *, search_term: str | None = None, limit: int = 200) -> list[ThreadState]:
        states: list[ThreadState] = []
        for payload in await self.client.list_threads(limit=limit, search_term=search_term):
            if isinstance(payload, dict) and payload.get("id"):
                state = self.projector.apply_thread(payload)
                await self._notify_projector_parent_changes("subagent/snapshot")
                if not state.is_subagent and not state.ephemeral:
                    states.append(state)
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
        self._add_provisional_interest(thread_id)
        try:
            state = await self.subscribe_space_thread(thread_id)
            self.store.subscribe(thread_id)
            self._invalidate_interest_cache()
        finally:
            self._remove_provisional_interest(thread_id)
        await self.dashboard.schedule(state, immediate=True)
        return state

    async def unwatch(self, thread_id: str) -> None:
        self.store.unsubscribe(thread_id)
        await self._unsubscribe_thread_if_unused(thread_id)

    async def _unsubscribe_thread_if_unused(self, thread_id: str) -> None:
        required = thread_id in self.store.subscriptions() or any(
            str(space.get("thread_id") or "") == thread_id and space.get("lifecycle") != "closed"
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

    async def _live_thread_snapshot(self, thread_id: str, *, force: bool = False) -> Json:
        generation = self.client.generation
        cached = self._thread_snapshots.get(thread_id)
        if (
            not force
            and cached is not None
            and cached.generation == generation
            and cached.expires_at > time.monotonic()
        ):
            return dict(cached.payload)
        running = self._thread_refresh_tasks.get(thread_id)
        if running is None or running.done():

            async def refresh_snapshot() -> Json:
                payload = await self.client.resume_thread(thread_id)
                self._thread_snapshots[thread_id] = _ThreadSnapshot(
                    generation=self.client.generation,
                    payload=dict(payload),
                    expires_at=time.monotonic() + _THREAD_SNAPSHOT_SECONDS,
                )
                return dict(payload)

            running = asyncio.create_task(
                refresh_snapshot(), name=f"thread-snapshot-{thread_id[:8]}"
            )
            self._thread_refresh_tasks[thread_id] = running
            running.add_done_callback(
                lambda completed, key=thread_id: (
                    self._thread_refresh_tasks.pop(key, None)
                    if self._thread_refresh_tasks.get(key) is completed
                    else None
                )
            )
        return dict(await asyncio.shield(running))

    def _invalidate_thread_snapshot(self, thread_id: str) -> None:
        if thread_id:
            self._thread_snapshots.pop(thread_id, None)

    async def _client_message_receipt(
        self, thread_id: str, client_message_id: str
    ) -> tuple[bool | None, Json | None]:
        try:
            payload = await self.client.read_thread(thread_id, include_turns=True)
            receipt = await self.client.find_user_message(
                thread_id, client_message_id, payload=payload
            )
        except Exception:
            return None, None
        return receipt is not None, receipt

    async def _reconcile_prompt_delivery(
        self,
        thread_id: str,
        client_message_id: str,
        *,
        error: Exception | None = None,
        expected_turn_id: str | None = None,
    ) -> tuple[bool | None, Json | None]:
        delivered, receipt = await self._client_message_receipt(thread_id, client_message_id)
        receipt_turn_id = str((receipt or {}).get("turn_id") or "") or None
        if delivered and expected_turn_id and receipt_turn_id != expected_turn_id:
            self.store.transition_prompt_intent(
                client_message_id,
                expected_states={"submitting"},
                to_state="uncertain",
                turn_id=receipt_turn_id,
                error="steer target mismatch",
            )
            return None, receipt
        if delivered is False and isinstance(error, CodexRpcError):
            self.store.transition_prompt_intent(
                client_message_id,
                expected_states={"submitting", "queued"},
                to_state="failed",
                error=type(error).__name__,
            )
        else:
            self.store.reconcile_prompt_intent(
                client_message_id,
                delivered=delivered,
                turn_id=receipt_turn_id,
                error=type(error).__name__ if error is not None else None,
            )
        return delivered, receipt

    async def _begin_turn_gate(self, thread_id: str, client_message_id: str) -> bool:
        lock = self._turn_locks.setdefault(thread_id, asyncio.Lock())
        if lock.locked():
            return False
        await lock.acquire()
        self._turn_gates[thread_id] = _TurnGate(client_message_id=client_message_id)
        return True

    def _bind_turn_gate(self, thread_id: str, client_message_id: str, turn_id: str) -> None:
        gate = self._turn_gates.get(thread_id)
        if gate is None or gate.client_message_id != client_message_id or not turn_id:
            return
        gate.expected_turn_id = turn_id
        if (thread_id, turn_id) in self._terminal_turns and self._release_turn_gate(
            thread_id, turn_id
        ):
            self._request_queue_after_completion(thread_id)

    def _release_turn_gate(self, thread_id: str, completed_turn_id: str | None) -> bool:
        gate = self._turn_gates.get(thread_id)
        if gate is None:
            return False
        if completed_turn_id is not None and gate.expected_turn_id != completed_turn_id:
            return False
        self._turn_gates.pop(thread_id, None)
        lock = self._turn_locks.get(thread_id)
        if lock is not None and lock.locked():
            lock.release()
        return True

    def _schedule_turn_reconciliation(self, thread_id: str) -> None:
        gate = self._turn_gates.get(thread_id)
        if gate is None:
            return
        current = self._turn_reconcile_tasks.get(thread_id)
        if current is not None and not current.done():
            return

        async def reconcile() -> None:
            try:
                await asyncio.sleep(_TURN_RECONCILE_SECONDS)
                current_gate = self._turn_gates.get(thread_id)
                if current_gate is None:
                    return
                delivered, receipt = await self._client_message_receipt(
                    thread_id, current_gate.client_message_id
                )
                if delivered and receipt is not None:
                    turn_id = str(receipt.get("turn_id") or "")
                    self._bind_turn_gate(thread_id, current_gate.client_message_id, turn_id)
                    if (
                        str(receipt.get("turn_status") or "") in _FINAL_TURN_STATUSES
                        and self._release_turn_gate(thread_id, turn_id)
                    ):
                        self._request_queue_after_completion(thread_id)
                    return
                state = self.store.get_thread(thread_id)
                if (
                    delivered is False
                    and self.client.connected
                    and state
                    and state.status == "idle"
                    and self._release_turn_gate(thread_id, None)
                ):
                    self._request_queue_after_completion(thread_id)
            finally:
                if self._turn_reconcile_tasks.get(thread_id) is asyncio.current_task():
                    self._turn_reconcile_tasks.pop(thread_id, None)

        self._turn_reconcile_tasks[thread_id] = asyncio.create_task(
            reconcile(), name=f"turn-reconcile-{thread_id[:8]}"
        )

    def _request_queue_after_completion(self, thread_id: str) -> None:
        spaces = self._active_spaces_for_thread(thread_id)
        if spaces:
            for space in spaces:
                self._request_queue_retry(
                    thread_id,
                    delay=0.0,
                    space_id=str(space["space_id"]),
                    generation=int(space["generation"]),
                )
            return
        self._request_queue_retry(thread_id, delay=0.0)

    async def refresh(self, thread_id: str) -> ThreadState:
        payload = await self.client.read_thread(thread_id, include_turns=True)
        state = self.projector.apply_thread(payload)
        state = self.projector.apply_goal(state, await self.client.get_goal(thread_id))
        await self._notify_projector_parent_changes("subagent/refresh")
        self.store.save_thread(state)
        await self.dashboard.schedule(state, immediate=True)
        return state

    async def new_session(self, cwd: Path, prompt: str, client_message_id: str) -> ThreadState:
        cwd = self.path_policy.validate_directory(cwd)
        start_token = self._begin_owned_thread_start(cwd)
        try:
            payload = await self.client.start_thread(cwd)
            self._claim_owned_thread(start_token, str(payload.get("id") or ""))
            state = self.projector.apply_thread(payload)
            await self._notify_projector_parent_changes("subagent/snapshot")
            state.title = prompt[:80] or state.title
            self.store.subscribe(state.thread_id)
            self._invalidate_interest_cache()
            state.subscribed = True
            self.store.save_thread(state)
        finally:
            self._finish_owned_thread_start(start_token)
        await self.tmux.ensure_window(state.thread_id, state.title, cwd)
        self.store.create_prompt_intent(
            client_message_id,
            "new_session",
            prompt,
            "default",
            thread_id=state.thread_id,
        )
        if not await self._begin_turn_gate(state.thread_id, client_message_id):
            raise RuntimeError("A turn is already active for the new session")
        self.store.transition_prompt_intent(
            client_message_id,
            expected_states={"received"},
            to_state="submitting",
        )
        try:
            turn = await self.client.start_turn(
                state.thread_id,
                [text_input(prompt)],
                client_message_id=client_message_id,
                cwd=cwd,
                **_writable_turn_security(state),
            )
            turn_id = str((turn or {}).get("id") or "")
            if not turn_id:
                raise RuntimeError("Codex did not return an ID for the new-session turn")
            self._bind_turn_gate(state.thread_id, client_message_id, turn_id)
            self.store.transition_prompt_intent(
                client_message_id,
                expected_states={"submitting"},
                to_state="started",
                turn_id=turn_id,
            )
        except Exception as exc:
            delivered, receipt = await self._reconcile_prompt_delivery(
                state.thread_id, client_message_id, error=exc
            )
            if delivered:
                self._bind_turn_gate(
                    state.thread_id,
                    client_message_id,
                    str((receipt or {}).get("turn_id") or ""),
                )
            else:
                self._schedule_turn_reconciliation(state.thread_id)
            if delivered is not True:
                raise
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
            collaboration_mode: Json | None = None
            profile_model, profile_effort = self._space_profile_values(space, space.current_mode)
            if profile_model or profile_effort:
                profile = await self.resolve_model_profile(profile_model, profile_effort)
                collaboration_mode = await self.client.resolve_collaboration_mode(
                    space.current_mode,
                    model=profile.model,
                    effort=profile.effort,
                )

            created_now = False
            if space.thread_id:
                state = self.store.get_thread(space.thread_id) or await self.refresh(space.thread_id)
                cwd = self.path_policy.validate_directory(Path(state.cwd or space.pending_cwd))
            else:
                cwd = self.path_policy.validate_directory(Path(space.pending_cwd))
                start_token = self._begin_owned_thread_start(cwd)
                try:
                    payload = await self.client.start_thread(cwd)
                    self._claim_owned_thread(start_token, str(payload.get("id") or ""))
                    state = self.projector.apply_thread(payload)
                    await self._notify_projector_parent_changes("subagent/snapshot")
                    state.title = space.pending_prompt[:80] or state.title
                    self.store.save_thread(state)
                    space.thread_id = state.thread_id
                    space.lifecycle = "repair_required"
                    space.last_error = "activation in progress"
                    self.store.save_session_space(space)
                    self._invalidate_interest_cache()
                finally:
                    self._finish_owned_thread_start(start_token)
                created_now = True

            await self.tmux.ensure_window(state.thread_id, state.title, cwd)
            intent = self.store.create_prompt_intent(
                client_message_id,
                "session_activation",
                space.pending_prompt,
                space.desired_mode,
                thread_id=state.thread_id,
                space_id=space.space_id,
                generation=space.generation,
            )
            delivered = (
                False
                if created_now
                else await self._client_message_exists(state.thread_id, client_message_id)
            )
            if delivered is None:
                self.store.reconcile_prompt_intent(client_message_id, delivered=None)
                space.last_error = "initial prompt delivery could not be reconciled"
                self.store.save_session_space(space)
                raise RuntimeError(space.last_error)
            if not delivered:
                if not await self._begin_turn_gate(state.thread_id, client_message_id):
                    raise RuntimeError("A turn is already active for this session")
                self.store.transition_prompt_intent(
                    client_message_id,
                    expected_states={"received", "awaiting_choice"},
                    to_state="submitting",
                )
                try:
                    turn = await self.client.start_turn(
                        state.thread_id,
                        [text_input(space.pending_prompt)],
                        client_message_id=client_message_id,
                        cwd=cwd,
                        **_writable_turn_security(state),
                        collaboration_mode=collaboration_mode,
                    )
                    turn_id = str((turn or {}).get("id") or "")
                    if turn_id:
                        self._bind_turn_gate(state.thread_id, client_message_id, turn_id)
                        self.store.transition_prompt_intent(
                            client_message_id,
                            expected_states={"submitting"},
                            to_state="started",
                            turn_id=turn_id,
                        )
                    else:
                        self._schedule_turn_reconciliation(state.thread_id)
                except Exception as exc:
                    delivered, receipt = await self._reconcile_prompt_delivery(
                        state.thread_id, client_message_id, error=exc
                    )
                    if delivered:
                        self._bind_turn_gate(
                            state.thread_id,
                            client_message_id,
                            str((receipt or {}).get("turn_id") or ""),
                        )
                    else:
                        self._schedule_turn_reconciliation(state.thread_id)
                    if not delivered:
                        space.last_error = f"initial prompt delivery failed ({type(exc).__name__})"
                        self.store.save_session_space(space)
                        raise
            elif intent.state not in {"started", "completed"}:
                _, receipt = await self._client_message_receipt(state.thread_id, client_message_id)
                self.store.reconcile_prompt_intent(
                    client_message_id,
                    delivered=True,
                    turn_id=str((receipt or {}).get("turn_id") or "") or None,
                )

            space.lifecycle = "active"
            space.last_error = ""
            space.pending_cwd = ""
            space.pending_prompt = ""
            if collaboration_mode is not None:
                space.observed_mode = space.desired_mode
            self.store.save_session_space(space)
            await self._notify_state_change(state, "session/activated")
            return state

    @staticmethod
    def _space_profile_values(space: SessionSpace, mode: str) -> tuple[str, str]:
        if mode == "plan":
            return space.plan_model, space.plan_effort
        if mode == "default":
            return space.normal_model, space.normal_effort
        raise ValueError(f"Unsupported collaboration mode: {mode!r}")

    async def set_space_profile(
        self,
        space_id: str,
        mode: str,
        model: str,
        effort: str,
    ) -> SessionSpace:
        profile = await self.resolve_model_profile(model, effort)
        lock = self._space_locks.setdefault(space_id, asyncio.Lock())
        async with lock:
            space = self._require_active_space(space_id)
            if mode == "plan":
                space.plan_model = profile.model
                space.plan_effort = profile.effort
            elif mode == "default":
                space.normal_model = profile.model
                space.normal_effort = profile.effort
            else:
                raise ValueError(f"Unsupported collaboration mode: {mode!r}")
            self.store.save_session_space(space)
            return space

    async def change_space_model(
        self,
        space_id: str,
        model: str,
        effort: str,
    ) -> SessionSpace:
        profile = await self.resolve_model_profile(model, effort)
        lock = self._space_locks.setdefault(space_id, asyncio.Lock())
        async with lock:
            space = self._require_active_space(space_id)
            mode = space.current_mode
            collaboration_mode = await self.client.resolve_collaboration_mode(
                mode,
                model=profile.model,
                effort=profile.effort,
            )
            settings_kwargs: Json = {"collaboration_mode": collaboration_mode}
            state = self.store.get_thread(space.thread_id or "")
            if state is not None:
                settings_kwargs.update(_thread_settings_security(state))
            await self.client.update_thread_settings(
                space.thread_id or "",
                **settings_kwargs,
            )
            current = self._require_active_space(space_id)
            if current.generation != space.generation or current.thread_id != space.thread_id:
                raise RuntimeError("Session space generation is stale")
            if mode == "plan":
                current.plan_model = profile.model
                current.plan_effort = profile.effort
            else:
                current.normal_model = profile.model
                current.normal_effort = profile.effort
            current.current_mode = mode
            current.desired_mode = mode
            current.observed_mode = mode
            self.store.save_session_space(current)
            if state := self.store.get_thread(current.thread_id or ""):
                await self._notify_state_change(state, "thread/settings/updated")
            return current

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

    async def start_space_collaboration_turn(
        self,
        space_id: str,
        prompt: str,
        *,
        mode: str,
        client_message_id: str,
        profile: ModelProfile | None = None,
    ) -> Json:
        prompt = prompt.strip()
        if not prompt:
            raise ValueError("Collaboration prompt cannot be empty")
        if mode not in {"default", "plan"}:
            raise ValueError(f"Unsupported collaboration mode: {mode!r}")
        lock = self._space_locks.setdefault(space_id, asyncio.Lock())
        async with lock:
            space = self._require_active_space(space_id)
            generation = space.generation
            thread_id = space.thread_id or ""
            self.store.create_prompt_intent(
                client_message_id,
                "collaboration",
                prompt,
                mode,
                thread_id=thread_id,
                space_id=space.space_id,
                generation=generation,
            )
            payload = await self._live_thread_snapshot(space.thread_id or "")
            state = self.projector.apply_thread(payload)
            await self._notify_projector_parent_changes("subagent/resync")
            if state.thread_id != thread_id:
                raise RuntimeError("Codex resumed a different session")
            if state.is_subagent or state.ephemeral:
                raise RuntimeError("Collaboration turns require a primary session")
            if state.status == "active" or state.turn_status == "inProgress":
                raise RuntimeError("当前 turn 正在运行，请稍后重试")
            if self.store.space_queue_entries(space.space_id, generation):
                raise RuntimeError("当前 Session 仍有排队 prompt，请先处理队列")
            if not state.cwd:
                raise ValueError("Session does not report a working directory")
            cwd = self.path_policy.validate_directory(Path(state.cwd))
            if profile is None:
                stored_model, stored_effort = self._space_profile_values(space, mode)
                if stored_model and stored_effort:
                    profile = await self.resolve_model_profile(stored_model, stored_effort)
                else:
                    effective_model = str(payload.get("model") or "")
                    effective_effort = str(payload.get("reasoningEffort") or "")
                    if effective_model and effective_effort:
                        profile = await self.resolve_model_profile(effective_model, effective_effort)
            else:
                profile = await self.resolve_model_profile(profile.model, profile.effort)
            if profile is None:
                raise RuntimeError(
                    "Codex 没有返回当前 Session 的 model/effort，已阻止使用空 profile 启动 turn"
                )
            collaboration_mode = await self.client.resolve_collaboration_mode(
                mode,
                model=profile.model,
                effort=profile.effort,
            )
            current = self._require_active_space(space_id)
            if current.generation != generation or current.thread_id != thread_id:
                raise RuntimeError("Session space generation is stale")
            if not await self._begin_turn_gate(thread_id, client_message_id):
                raise RuntimeError("当前 turn 正在运行，请稍后重试")
            self.store.transition_prompt_intent(
                client_message_id,
                expected_states={"received"},
                to_state="submitting",
            )
            try:
                turn = await self.client.start_turn(
                    state.thread_id,
                    [text_input(prompt)],
                    client_message_id=client_message_id,
                    cwd=cwd,
                    **_writable_turn_security(state),
                    collaboration_mode=collaboration_mode,
                )
                turn_id = str((turn or {}).get("id") or "")
                if not turn_id:
                    raise RuntimeError("Codex did not return an ID for the collaboration turn")
                self._bind_turn_gate(thread_id, client_message_id, turn_id)
                self.store.transition_prompt_intent(
                    client_message_id,
                    expected_states={"submitting"},
                    to_state="started",
                    turn_id=turn_id,
                )
            except Exception as exc:
                delivered, receipt = await self._reconcile_prompt_delivery(
                    thread_id, client_message_id, error=exc
                )
                if delivered:
                    self._bind_turn_gate(
                        thread_id,
                        client_message_id,
                        str((receipt or {}).get("turn_id") or ""),
                    )
                else:
                    self._schedule_turn_reconciliation(thread_id)
                if delivered is not True:
                    raise
            current = self._require_active_space(space_id)
            if current.generation != generation or current.thread_id != thread_id:
                raise RuntimeError("Session space generation is stale")
            current.current_mode = mode
            current.desired_mode = mode
            current.observed_mode = mode
            if profile is not None:
                if mode == "plan":
                    current.plan_model = profile.model
                    current.plan_effort = profile.effort
                else:
                    current.normal_model = profile.model
                    current.normal_effort = profile.effort
            self.store.save_session_space(current)
            LOGGER.info(
                "event=collaboration_turn_started space_id=%s thread_id=%s turn_id=%s mode=%s",
                space.space_id,
                state.short_id,
                str((turn or {})["id"])[:8],
                mode,
            )
            return turn or {}

    async def reconcile_plan_execution(
        self,
        space_id: str,
        generation: int,
        item_id: str,
        revision_key: str,
        client_message_id: str,
    ) -> str:
        space = self._require_active_space(space_id)
        if space.generation != generation:
            raise RuntimeError("Session space generation is stale")
        latest = self.store.latest_plan_publication(space_id, generation)
        if (
            latest is None
            or str(latest.get("item_id") or "") != item_id
            or str(latest.get("revision_key") or "") != revision_key
        ):
            raise RuntimeError("Plan publication is stale")
        delivered = await self._client_message_exists(space.thread_id or "", client_message_id)
        if delivered is None:
            return "unknown"
        if delivered:
            space.current_mode = "default"
            space.desired_mode = "default"
            space.observed_mode = "default"
            self.store.save_session_space(space)
            return "delivered"
        return "absent"

    async def wait_for_plan_decision_gate(
        self,
        space_id: str,
        generation: int,
        item_id: str,
        revision_key: str,
        client_message_id: str,
        *,
        timeout: float = 1.0,
    ) -> dict[str, str]:
        space = self._require_active_space(space_id)
        if space.generation != generation:
            raise RuntimeError("Session space generation is stale")
        publication = self.store.latest_plan_publication(space_id, generation)
        if (
            publication is None
            or str(publication.get("item_id") or "") != item_id
            or str(publication.get("revision_key") or "") != revision_key
        ):
            raise RuntimeError("Plan publication is stale")
        thread_id = space.thread_id or ""
        event = self._plan_decision_events.setdefault((space_id, generation), asyncio.Event())
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            approval_turn = self.store.find_tui_plan_approval_turn(
                thread_id,
                after=int(publication.get("created_at") or 0),
                prompt=TUI_PLAN_APPROVAL_PROMPT,
            )
            if approval_turn:
                return {"status": "tui_approval_observed", "turn_id": approval_turn}
            delivered, receipt = await self._client_message_receipt(
                thread_id, client_message_id
            )
            if delivered:
                result = {"status": "already_delivered"}
                turn_id = str((receipt or {}).get("turn_id") or "")
                if turn_id:
                    result["turn_id"] = turn_id
                return result
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                state = self.store.get_thread(thread_id)
                gate = self._turn_gates.get(thread_id)
                safe = (
                    delivered is False
                    and self.client.connected
                    and gate is None
                    and state is not None
                    and state.status == "idle"
                    and state.turn_status != "inProgress"
                )
                return {"status": "safe_to_submit" if safe else "uncertain"}
            if event.is_set():
                event.clear()
                continue
            try:
                await asyncio.wait_for(event.wait(), timeout=remaining)
            except TimeoutError:
                continue
            event.clear()

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
            except RuntimeError, ValueError:
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
        client_message_id = client_message_id or f"telegram-{uuid.uuid4()}"
        source = "space" if space_id is not None else "legacy"
        intent = self.store.get_prompt_intent(client_message_id)
        choice_resolved = False
        if intent is not None:
            compatible = (
                intent.source == source
                and intent.prompt == prompt
                and intent.thread_id == thread_id
                and intent.space_id == space_id
                and intent.generation == (generation or 0)
            )
            if not compatible:
                raise ValueError(f"Prompt client_message_id collision: {client_message_id}")
        if intent is not None and intent.mode != mode:
            intent = self.store.resolve_prompt_intent_choice(
                client_message_id,
                mode=mode,
            )
            if intent is None:
                raise ValueError(f"Prompt choice already resolved: {client_message_id}")
            choice_resolved = True
        if intent is None:
            intent = self.store.create_prompt_intent(
                client_message_id,
                source,
                prompt,
                mode,
                thread_id=thread_id,
                space_id=space_id,
                generation=generation or 0,
            )
        prior_results = {
            "queued": "queued",
            "awaiting_choice": "choose",
            "started": "started",
            "steered": "steered",
        }
        if intent.state in prior_results and not (
            choice_resolved and intent.state == "awaiting_choice"
        ):
            return prior_results[intent.state]
        if intent.state in {"completed", "failed", "uncertain", "cancelled"}:
            raise RuntimeError(f"Prompt intent is already terminal: {intent.state}")
        payload = await self._live_thread_snapshot(thread_id)
        state = self.projector.apply_thread(payload)
        await self._notify_projector_parent_changes("subagent/resync")
        if not state.cwd:
            raise ValueError("Session does not report a working directory")
        cwd = self.path_policy.validate_directory(Path(state.cwd))
        values = list(inputs or [text_input(prompt)])
        if mode == "queue":
            queue_id = self.store.enqueue_prompt(
                thread_id,
                prompt,
                values,
                client_message_id,
                space_id=space_id,
                generation=generation or 0,
                prompt_intent_id=intent.intent_id,
            )
            self.store.transition_prompt_intent(
                client_message_id,
                expected_states={"received", "awaiting_choice"},
                to_state="queued",
                queue_id=queue_id,
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
                self.store.transition_prompt_intent(
                    client_message_id,
                    expected_states={"received"},
                    to_state="awaiting_choice",
                )
                return "choose"
            if not state.turn_id:
                raise RuntimeError("Active turn ID is unavailable")
            self.store.transition_prompt_intent(
                client_message_id,
                expected_states={"received", "awaiting_choice"},
                to_state="submitting",
            )
            try:
                await self.client.steer_turn(
                    thread_id, state.turn_id, values, client_message_id=client_message_id
                )
            except Exception as exc:
                delivered, _ = await self._reconcile_prompt_delivery(
                    thread_id,
                    client_message_id,
                    error=exc,
                    expected_turn_id=state.turn_id,
                )
                if delivered is not True:
                    raise
            else:
                self.store.transition_prompt_intent(
                    client_message_id,
                    expected_states={"submitting"},
                    to_state="steered",
                    turn_id=state.turn_id,
                )
            LOGGER.info(
                "event=prompt_injected space_id=%s thread_id=%s turn_id=%s",
                space_id or "legacy",
                thread_id[:8],
                state.turn_id[:8],
            )
            return "steered"
        if not await self._begin_turn_gate(thread_id, client_message_id):
            self.store.transition_prompt_intent(
                client_message_id,
                expected_states={"received"},
                to_state="awaiting_choice",
            )
            return "choose"
        self.store.transition_prompt_intent(
            client_message_id,
            expected_states={"received", "awaiting_choice"},
            to_state="submitting",
        )
        try:
            turn = await self.client.start_turn(
                thread_id,
                values,
                client_message_id=client_message_id,
                cwd=cwd,
                **_writable_turn_security(state),
            )
            turn_id = str((turn or {}).get("id") or "")
            if not turn_id:
                raise RuntimeError("Codex did not return an ID for the turn")
            self._bind_turn_gate(thread_id, client_message_id, turn_id)
            self.store.transition_prompt_intent(
                client_message_id,
                expected_states={"submitting"},
                to_state="started",
                turn_id=turn_id,
            )
        except Exception as exc:
            delivered, receipt = await self._reconcile_prompt_delivery(
                thread_id, client_message_id, error=exc
            )
            if delivered:
                turn_id = str((receipt or {}).get("turn_id") or "")
                self._bind_turn_gate(thread_id, client_message_id, turn_id)
            else:
                self._schedule_turn_reconciliation(thread_id)
            if delivered is not True:
                raise
        if space_id is not None and generation is not None:
            await self._track_prompt_run(
                space_id=space_id,
                generation=generation,
                thread_id=thread_id,
                turn_id=turn_id,
                client_message_id=client_message_id,
            )
        LOGGER.info(
            "event=prompt_started space_id=%s thread_id=%s turn_id=%s",
            space_id or "legacy",
            thread_id[:8],
            turn_id[:8] or "unknown",
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
                except RuntimeError, ValueError:
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
                prompt_text = str(queued.get("prompt") or "")
            else:
                queue_id = queued.queue_id
                inputs = queued.inputs
                client_message_id = queued.client_message_id
                prompt_text = queued.prompt
            intent = self.store.get_prompt_intent(client_message_id)
            if intent is None:
                intent = self.store.create_prompt_intent(
                    client_message_id,
                    "queue",
                    prompt_text,
                    "queue",
                    thread_id=thread_id,
                    space_id=space_id,
                    generation=generation or 0,
                )
                intent = self.store.transition_prompt_intent(
                    client_message_id,
                    expected_states={"received"},
                    to_state="queued",
                    queue_id=queue_id,
                ) or intent
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
            receipt = None
            if delivered:
                _, receipt = await self._client_message_receipt(thread_id, client_message_id)
            if delivered is None:
                LOGGER.warning("Cannot reconcile queued prompt %s; postponing delivery", queue_id)
                self._request_queue_retry(thread_id, space_id=space_id, generation=generation)
                return
            if delivered:
                self.store.reconcile_prompt_intent(
                    client_message_id,
                    delivered=True,
                    turn_id=str((receipt or {}).get("turn_id") or "") or None,
                )
                receipt_turn_id = str((receipt or {}).get("turn_id") or "")
                if (
                    receipt_turn_id
                    and
                    str((receipt or {}).get("turn_status") or "") not in _FINAL_TURN_STATUSES
                    and await self._begin_turn_gate(thread_id, client_message_id)
                ):
                    self._bind_turn_gate(
                        thread_id,
                        client_message_id,
                        receipt_turn_id,
                    )
                await self._mark_prompt_dispatched(state, queue_id, space_id=space_id, generation=generation)
                return
            if not await self._begin_turn_gate(thread_id, client_message_id):
                return
            self.store.transition_prompt_intent(
                client_message_id,
                expected_states={"queued"},
                to_state="submitting",
                queue_id=queue_id,
            )
            try:
                turn = await self.client.start_turn(
                    thread_id,
                    inputs,
                    client_message_id=client_message_id,
                    cwd=cwd,
                    **_writable_turn_security(state),
                )
                turn_id = str((turn or {}).get("id") or "")
                if turn_id:
                    self._bind_turn_gate(thread_id, client_message_id, turn_id)
                    self.store.transition_prompt_intent(
                        client_message_id,
                        expected_states={"submitting"},
                        to_state="started",
                        turn_id=turn_id,
                    )
                else:
                    self._schedule_turn_reconciliation(thread_id)
                if turn_id and space_id is not None and generation is not None:
                    await self._track_prompt_run(
                        space_id=space_id,
                        generation=generation,
                        thread_id=thread_id,
                        turn_id=turn_id,
                        client_message_id=client_message_id,
                    )
            except Exception as exc:
                delivered, receipt = await self._reconcile_prompt_delivery(
                    thread_id, client_message_id, error=exc
                )
                if delivered:
                    self._bind_turn_gate(
                        thread_id,
                        client_message_id,
                        str((receipt or {}).get("turn_id") or ""),
                    )
                    await self._mark_prompt_dispatched(
                        state, queue_id, space_id=space_id, generation=generation
                    )
                elif delivered is False and isinstance(exc, CodexRpcError):
                    self._release_turn_gate(thread_id, None)
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
                    self._schedule_turn_reconciliation(thread_id)
                else:
                    LOGGER.exception("Failed to dispatch queued prompt %s", queue_id)
                    self._schedule_turn_reconciliation(thread_id)
                self._request_queue_retry(thread_id, space_id=space_id, generation=generation)
                return
            await self._mark_prompt_dispatched(state, queue_id, space_id=space_id, generation=generation)

    async def _track_prompt_run(
        self,
        *,
        space_id: str,
        generation: int,
        thread_id: str,
        turn_id: str,
        client_message_id: str,
    ) -> None:
        if not turn_id:
            LOGGER.warning(
                "event=prompt_run_tracking_skipped space_id=%s thread_id=%s reason=missing_turn_id",
                space_id[:12],
                thread_id[:8],
            )
            return
        try:
            inserted = self.store.put_prompt_run(
                uuid.uuid4().hex,
                space_id=space_id,
                generation=generation,
                thread_id=thread_id,
                turn_id=turn_id,
                client_message_id=client_message_id,
            )
        except Exception:
            LOGGER.exception(
                "event=prompt_run_tracking_failed space_id=%s thread_id=%s turn_id=%s",
                space_id[:12],
                thread_id[:8],
                turn_id[:8],
            )
            return
        if not inserted:
            return
        terminal = self._terminal_turns.get((thread_id, turn_id))
        if terminal is not None:
            await self._finish_prompt_runs(
                thread_id,
                turn_id,
                status=terminal[0],
                error_kind=terminal[1],
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
        delivered, _ = await self._client_message_receipt(thread_id, client_message_id)
        return delivered

    async def resolve_directory(self, description: str) -> list[Path]:
        return await self.resolver.resolve_directory(description)

    async def resolve_files(self, thread_id: str, description: str) -> list[FileCandidate]:
        state = self.store.get_thread(thread_id) or await self.refresh(thread_id)
        return await self.resolver.resolve_files(
            Path(state.cwd),
            description,
        )

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
        stored = self.store.claim_pending_input(request_key)
        if not pending or not stored:
            raise RuntimeError("该问题已过期或已由其他客户端回答")
        request_id, generation = pending
        if generation != self.client.generation:
            self.store.resolve_pending_input(request_key, source="connection_generation_changed")
            raise RuntimeError("Codex 连接已经重建，原问题已失效")
        result = {"answers": {key: {"answers": values} for key, values in answers.items()}}
        try:
            await self.client.respond(request_id, result, generation=generation)
        except Exception:
            self.store.release_pending_input_claim(request_key)
            raise
        self.store.mark_pending_input_responded(request_key, result)

    async def answer_command_approval(
        self,
        request_key: str,
        decision: ApprovalDecision,
    ) -> None:
        stored = self.store.get_pending_input(request_key)
        if not stored:
            raise RuntimeError("该命令审批已过期或已由其他客户端处理")
        generation = int(stored["generation"])
        if generation != self.client.generation:
            await self._retire_command_approval(request_key)
            raise RuntimeError("Codex 连接已经重建，原命令审批已失效")
        metadata = next(
            (
                value
                for value in stored["questions"]
                if isinstance(value, dict)
                and value.get("_bridge_request_kind") in {"command_approval", "generic_approval"}
            ),
            None,
        )
        if metadata is None:
            raise RuntimeError("该请求不是可由 Telegram 处理的命令审批")
        method = str(metadata.get("_bridge_approval_method") or "")
        raw_available = metadata.get("_bridge_available_decisions")
        available = raw_available if isinstance(raw_available, list) else []
        if method in {
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
        }:
            if not approval_decision_is_available(decision, available):
                raise ValueError("审批决定不在当前请求允许的选项中")
            response = approval_response_payload(method, decision)
        elif method == "item/permissions/requestApproval":
            response = approval_response_payload(method, decision)
        elif method in {"execCommandApproval", "applyPatchApproval"}:
            if available and not approval_decision_is_available(decision, available):
                raise ValueError("审批决定不在当前请求允许的选项中")
            response = approval_response_payload(method, decision)
        else:
            raise RuntimeError("未知的审批协议")
        raw_request_id = stored["request_id"]
        try:
            request_id = json.loads(str(raw_request_id))
        except json.JSONDecodeError:
            request_id = raw_request_id
        if not isinstance(request_id, int | str):
            request_id = str(raw_request_id)
        claimed = self.store.claim_pending_input(request_key)
        if claimed is None:
            raise RuntimeError("该审批已由其他客户端处理")
        try:
            await self.client.respond(request_id, response, generation=generation)
        except Exception:
            if generation != self.client.generation:
                await self._retire_command_approval(request_key)
            else:
                self.store.release_pending_input_claim(request_key)
            raise
        self.store.mark_pending_input_responded(request_key, response)

    async def _retire_command_approval(self, request_key: str) -> None:
        await self._notify_question_resolved(request_key)
        self._pending_requests.pop(request_key, None)
        self.store.resolve_pending_input(request_key, source="connection_generation_changed")

    async def _on_notification(self, method: str, params: Json) -> None:
        effects: list[NotificationEffect] = []
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
                self.store.resolve_pending_input(key, source="serverRequest/resolved")
                self._pending_requests.pop(key, None)
                effects.append(
                    ("question_resolved", partial(self._notify_question_resolved, key))
                )
            for name, effect in effects:
                await self._dispatch_notification_effect(name, effect)
            return
        if not self._notification_is_managed(method, params):
            LOGGER.debug(
                "event=codex_notification_ignored method=%s thread_id=%s",
                method,
                self._notification_thread_id(params)[:8] or "missing",
            )
            return
        if "ingest" in self.projector.__dict__:
            await self.projector.ingest(method, params)
            changes = []
        else:
            changes = self.projector.project(method, params)
        thread_id = self._notification_thread_id(params)
        self._invalidate_thread_snapshot(thread_id)
        effects.extend(
            ("state_change", partial(self._on_state_change, state, reason))
            for state, reason in changes
        )
        if method in {"item/started", "item/completed", "turn/completed"}:
            self._invalidate_interest_cache()
        if method == "item/started":
            item = params.get("item") or {}
            thread_id = str(params.get("threadId") or "")
            turn_id = str(params.get("turnId") or "")
            if thread_id and turn_id and _is_tui_plan_approval_item(item):
                effects.append(
                    (
                        "tui_plan_approved",
                        partial(self.on_tui_plan_approved, thread_id, turn_id),
                    )
                )
        self._schedule_subagent_profile_refresh(method, params)
        if method == "item/completed":
            item = params.get("item") or {}
            if isinstance(item, dict) and item.get("type") == "plan":
                effects.append(
                    (
                        "plan_completed",
                        partial(
                            self._notify_plan_completed,
                            str(params.get("threadId") or ""),
                            str(params.get("turnId") or ""),
                            str(item.get("id") or ""),
                            str(item.get("text") or ""),
                        ),
                    )
                )
        elif method == "turn/completed":
            thread_id = str(params.get("threadId") or "")
            turn = params.get("turn") or {}
            if not isinstance(turn, dict):
                return
            turn_id = str(turn.get("id") or "")
            for item in turn.get("items") or []:
                if isinstance(item, dict) and item.get("type") == "plan":
                    effects.append(
                        (
                            "plan_completed",
                            partial(
                                self._notify_plan_completed,
                                thread_id,
                                turn_id,
                                str(item.get("id") or ""),
                                str(item.get("text") or ""),
                            ),
                        )
                    )
            status = str(turn.get("status") or "")
            if not thread_id or not turn_id or status not in _FINAL_TURN_STATUSES:
                LOGGER.warning(
                    "event=invalid_turn_completion thread_id=%s turn_id=%s status=%s",
                    thread_id[:8] or "missing",
                    turn_id[:8] or "missing",
                    status or "missing",
                )
            else:
                error_kind = _turn_error_kind(turn.get("error"))
                marker = (thread_id, turn_id)
                self._terminal_turns[marker] = (status, error_kind)
                while len(self._terminal_turns) > 512:
                    self._terminal_turns.pop(next(iter(self._terminal_turns)))
                self.store.finish_prompt_intents(
                    thread_id,
                    turn_id,
                    status=status,
                    error=error_kind or None,
                )
                if self._release_turn_gate(thread_id, turn_id):
                    self._request_queue_after_completion(thread_id)
                runs = self.store.finish_prompt_runs(
                    thread_id,
                    turn_id,
                    status=status,
                    error_kind=error_kind,
                )
                effects.extend(
                    ("prompt_completed", partial(self._notify_prompt_completed, run))
                    for run in runs
                )
        if method == "thread/status/changed":
            status = params.get("status")
            if isinstance(status, dict) and status.get("type") == "idle":
                self._schedule_turn_reconciliation(thread_id)
        if method in {"item/started", "turn/started", "turn/completed", "thread/status/changed"}:
            for raw_space in self._active_spaces_for_thread(thread_id):
                event = self._plan_decision_events.get(
                    (str(raw_space["space_id"]), int(raw_space["generation"]))
                )
                if event is not None:
                    event.set()
        for name, effect in effects:
            await self._dispatch_notification_effect(name, effect)

    def _schedule_subagent_profile_refresh(self, method: str, params: Json) -> None:
        if method not in {"item/started", "item/completed"}:
            return
        item = params.get("item")
        if not isinstance(item, dict) or item.get("type") != "subAgentActivity":
            return
        child_id = str(item.get("agentThreadId") or "")
        parent_id = str(params.get("threadId") or "")
        if not child_id or not parent_id:
            return
        child = self.store.get_thread(child_id)
        if child is not None and child.model and child.reasoning_effort:
            return
        self._ensure_subagent_profile_refresh(
            child_id,
            parent_id,
            str(item.get("agentPath") or ""),
        )

    def _ensure_subagent_profile_refresh(
        self,
        child_id: str,
        parent_id: str,
        agent_path: str,
    ) -> None:
        running = self._subagent_profile_tasks.get(child_id)
        if running is not None and not running.done():
            return
        task = asyncio.create_task(
            self._refresh_subagent_profile(
                child_id,
                parent_id,
                agent_path,
            ),
            name=f"codex-subagent-profile-{child_id[:8]}",
        )
        self._subagent_profile_tasks[child_id] = task
        task.add_done_callback(
            lambda completed, thread_id=child_id: (
                self._subagent_profile_tasks.pop(thread_id, None)
                if self._subagent_profile_tasks.get(thread_id) is completed
                else None
            )
        )

    async def _refresh_subagent_profile(self, child_id: str, parent_id: str, agent_path: str) -> None:
        payload: Json | None = None
        attempts = len(_SUBAGENT_PROFILE_RETRY_DELAYS) + 1
        for attempt in range(attempts):
            if attempt:
                await asyncio.sleep(_SUBAGENT_PROFILE_RETRY_DELAYS[attempt - 1])
            try:
                payload = await self.client.resume_thread(child_id)
                break
            except CodexRpcError as exc:
                message = str(exc.error.get("message") or exc).casefold()
                if "no rollout found" in message or "not found" in message:
                    parent = self.store.get_thread(parent_id)
                    if parent is not None:
                        now = int(time.time())
                        for task in parent.tasks:
                            if child_id not in {task.agent_thread_id, task.task_id}:
                                continue
                            task.status = "notFound"
                            task.finished_at = task.finished_at or now
                            task.updated_at = now
                            self.store.save_thread(parent)
                            self._invalidate_interest_cache()
                            await self._notify_state_change(parent, "subagent/notFound")
                            break
                    LOGGER.info(
                        "event=subagent_profile_terminal thread_id=%s reason=not_found",
                        child_id[:8],
                    )
                    return
                if attempt + 1 == attempts:
                    LOGGER.warning(
                        "event=subagent_profile_failed thread_id=%s attempts=%s error=%s",
                        child_id[:8],
                        attempts,
                        type(exc).__name__,
                    )
                    return
            except (RuntimeError, TimeoutError) as exc:
                if attempt + 1 == attempts:
                    LOGGER.warning(
                        "event=subagent_profile_failed thread_id=%s attempts=%s error=%s",
                        child_id[:8],
                        attempts,
                        type(exc).__name__,
                    )
                    return
        if payload is None:
            return
        payload.setdefault("parentThreadId", parent_id)
        if agent_path:
            payload.setdefault("agentPath", agent_path)
        self.projector.apply_thread(payload)
        await self._notify_projector_parent_changes("subagent/profile")

    async def _on_state_change(self, state: ThreadState, reason: str) -> None:
        immediate = reason in {"error", "turn/completed", "thread/goal/updated", "thread/status/changed"}
        await self.dashboard.schedule(state, immediate=immediate)
        await self._notify_state_change(state, reason)

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

    async def _notify_plan_completed(self, thread_id: str, turn_id: str, item_id: str, text: str) -> None:
        if not thread_id or not item_id or not text.strip():
            return
        revision_key = plan_revision_key(turn_id, text)
        marker = (thread_id, item_id, revision_key)
        if marker in self._notified_plan_items:
            return
        try:
            await self.on_plan_completed(thread_id, turn_id, item_id, text)
        except Exception:
            LOGGER.exception(
                "event=plan_publish_hook_failed thread_id=%s turn_id=%s item_id=%s",
                thread_id[:8],
                turn_id[:8],
                item_id[:8],
            )
            return
        self._notified_plan_items[marker] = None
        while len(self._notified_plan_items) > 512:
            self._notified_plan_items.pop(next(iter(self._notified_plan_items)))

    async def _notify_prompt_completed(self, run: Json) -> None:
        try:
            await self.on_prompt_completed(run)
        except Exception:
            LOGGER.exception(
                "event=prompt_receipt_hook_failed space_id=%s turn_id=%s",
                str(run.get("space_id") or "")[:12],
                str(run.get("turn_id") or "")[:8],
            )

    async def _finish_prompt_runs(
        self,
        thread_id: str,
        turn_id: str,
        *,
        status: str,
        error_kind: str,
    ) -> None:
        for run in self.store.finish_prompt_runs(
            thread_id,
            turn_id,
            status=status,
            error_kind=error_kind,
        ):
            await self._notify_prompt_completed(run)

    async def _on_server_request(
        self, request_id: int | str, method: str, params: Json, generation: int
    ) -> None:
        tombstone = (generation, str(request_id))
        if tombstone in self._resolved_request_ids:
            self._resolved_request_ids.pop(tombstone, None)
            return
        approval_params = (
            normalize_command_approval_params(method, params)
            if method in {"item/commandExecution/requestApproval", "execCommandApproval"}
            else params
        )
        if method == "applyPatchApproval":
            approval_params = {
                **params,
                "threadId": params.get("conversationId") or params.get("threadId") or "",
                "turnId": params.get("turnId") or "",
                "itemId": params.get("callId") or params.get("itemId") or "",
            }
        thread_id = self._notification_thread_id(approval_params) or None
        if thread_id is None or thread_id not in self._managed_thread_ids():
            LOGGER.info(
                "event=codex_server_request_rejected method=%s thread_id=%s reason=unmanaged",
                method,
                str(thread_id or "")[:8] or "missing",
            )
            await self.client.respond_error(
                request_id,
                -32600,
                "Interactive requests are disabled for sessions not managed by this bridge",
                generation=generation,
            )
            return
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
            "execCommandApproval",
            "applyPatchApproval",
        }:
            thread_id = str(approval_params.get("threadId") or "") or None
            if method in {"item/commandExecution/requestApproval", "execCommandApproval"}:
                available_decisions = command_approval_decisions(method, approval_params)
            elif method == "item/fileChange/requestApproval":
                raw_available = approval_params.get("availableDecisions")
                available_decisions = (
                    [
                        value
                        for value in raw_available
                        if approval_decision_is_available(value, list(raw_available))
                    ]
                    if isinstance(raw_available, list)
                    else ["accept", "acceptForSession", "decline"]
                )
            elif method == "applyPatchApproval":
                available_decisions = ["accept", "acceptForSession", "decline", "cancel"]
            elif method == "item/permissions/requestApproval":
                available_decisions = interactive_approval_decisions(method, approval_params)
            else:
                available_decisions = []
            request_key = f"approval:{uuid.uuid4().hex[:16]}"
            expires = int(time.time()) + max(
                self.config.callback_seconds,
                self.config.totp_unlock_seconds,
            )
            self._pending_requests[request_key] = (request_id, generation)
            approval_metadata = {
                "_bridge_request_kind": "generic_approval",
                "_bridge_approval_method": method,
                "_bridge_available_decisions": available_decisions,
                "params": approval_params,
            }
            self.store.put_pending_input(
                request_key,
                json.dumps(request_id),
                generation,
                str(approval_params.get("threadId") or ""),
                str(approval_params.get("turnId") or ""),
                str(approval_params.get("itemId") or ""),
                [approval_metadata],
                expires,
            )
            LOGGER.info(
                "event=approval_requested request_key=%s thread_id=%s method=%s",
                request_key,
                str(thread_id or "")[:8],
                method,
            )
            await self.on_command_approval(request_key, approval_params)
            return
        LOGGER.info("Ignoring unsupported app-server request %s", method)
