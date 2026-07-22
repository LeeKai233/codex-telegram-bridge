from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import re
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError

from .config import Config
from .delivery import (
    DeliveryIntent,
    DeliveryKey,
    DeliveryOutcome,
    TelegramDeliveryEngine,
    delivery_fingerprint,
)
from .models import ThreadState
from .security import SecurityManager
from .store import Store
from .telegram_common import CONTROL_ROLE, DISCUSSION_ROLE, TelegramEndpoint
from .views import (
    ANIMATION_FRAMES,
    RenderedMessage,
    render_channel_post,
    render_closed_space,
    render_pending_space,
    render_status_comment,
)

LOGGER = logging.getLogger(__name__)

_IMMEDIATE_REASONS = {
    "error",
    "turn/completed",
    "thread/goal/updated",
    "thread/goal/cleared",
    "thread/status/changed",
    "thread/settings/updated",
    "thread/resynced",
    "turn/plan/updated",
}

_BOT_URL_TOKEN = re.compile(r"(https://api\.telegram\.org/bot)[^/\s]+", re.IGNORECASE)
_BOT_TOKEN = re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{16,}\b")


@dataclass(slots=True)
class _AnimationBatch:
    frame: int
    targets: set[DeliveryKey]
    advance: bool
    acknowledged: set[DeliveryKey] = field(default_factory=set)
    performed: bool = False


def _safe_error_text(exc: BaseException) -> str:
    detail = " ".join(str(exc).split()) or type(exc).__name__
    detail = _BOT_URL_TOKEN.sub(r"\1<redacted>", detail)
    return _BOT_TOKEN.sub("<redacted>", detail)[:500]


def private_message_link(chat_id: int, message_id: int) -> str:
    value = str(chat_id)
    internal = value[4:] if value.startswith("-100") else value.lstrip("-")
    return f"https://t.me/c/{internal}/{message_id}"


def channel_comment_link(space: dict[str, Any]) -> str | None:
    chat_id = space.get("channel_chat_id")
    post_id = space.get("channel_post_id")
    status_id = space.get("status_message_id")
    if not chat_id or not post_id or not status_id:
        return None
    return f"{private_message_link(int(chat_id), int(post_id))}?comment={int(status_id)}"


class SpaceDashboardManager:
    def __init__(
        self,
        config: Config,
        store: Store,
        security: SecurityManager,
        control: TelegramEndpoint,
        discussion: TelegramEndpoint,
        delivery: TelegramDeliveryEngine,
    ) -> None:
        self.config = config
        self.store = store
        self.security = security
        self.control = control
        self.discussion = discussion
        self.delivery = delivery
        self._dirty: set[str] = set()
        self._immediate: set[str] = set()
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._animation_indices: dict[str, int] = {}
        self._animation_batches: dict[str, _AnimationBatch] = {}
        self._delivery_tickets: dict[DeliveryKey, asyncio.Future[DeliveryOutcome]] = {}
        self._delivery_fingerprints: dict[asyncio.Future[DeliveryOutcome], str] = {}
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._stopping = False

    async def start(self) -> None:
        self._stopping = False
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(), name="telegram-space-heartbeat"
            )
        for space in self.store.list_spaces():
            if any(
                space.get(name)
                for name in ("channel_post_id", "status_message_id")
            ):
                await self.schedule_space(str(space["space_id"]), immediate=True)

    async def stop(self) -> None:
        self._stopping = True
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None
        tasks = list(self._tasks.values())
        self._tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._delivery_tickets.clear()
        self._delivery_fingerprints.clear()

    async def on_thread_change(self, state: ThreadState, reason: str) -> None:
        for space in self.store.list_spaces():
            if space.get("thread_id") != state.thread_id:
                continue
            if space.get("lifecycle") not in {"active", "repair_required"}:
                continue
            await self.schedule_space(str(space["space_id"]), immediate=reason in _IMMEDIATE_REASONS)

    async def schedule_space(self, space_id: str, *, immediate: bool = False) -> None:
        self._schedule_space(space_id, immediate=immediate)

    def _schedule_space(self, space_id: str, *, immediate: bool) -> None:
        self._dirty.add(space_id)
        if immediate:
            self._immediate.add(space_id)
        task = self._tasks.get(space_id)
        if task is None or task.done():
            self._tasks[space_id] = asyncio.create_task(
                self._worker(space_id), name=f"space-dashboard-{space_id[:8]}"
            )

    async def _worker(self, space_id: str) -> None:
        try:
            while space_id in self._dirty and not self._stopping:
                immediate = space_id in self._immediate
                self._immediate.discard(space_id)
                if not immediate:
                    await asyncio.sleep(self.config.dashboard_debounce_seconds)
                self._dirty.discard(space_id)
                try:
                    await self._flush(space_id)
                except TelegramError as exc:
                    LOGGER.warning(
                        "event=space_dashboard_update_failed space_id=%s error_type=%s error=%s",
                        space_id,
                        type(exc).__name__,
                        _safe_error_text(exc),
                    )
                    self._dirty.add(space_id)
                    await asyncio.sleep(5)
                except Exception as exc:
                    LOGGER.error(
                        "event=space_dashboard_unexpected_error space_id=%s error_type=%s error=%s",
                        space_id,
                        type(exc).__name__,
                        _safe_error_text(exc),
                    )
                    self._dirty.add(space_id)
                    await asyncio.sleep(5)
        finally:
            if self._tasks.get(space_id) is asyncio.current_task():
                self._tasks.pop(space_id, None)

    async def _flush(self, space_id: str) -> None:
        space = self.store.get_space(space_id)
        if not space:
            return
        state = self._state_for_space(space)
        terminal = self._is_terminal(space, state)
        animated = self._is_animated(space, state)
        status_keyboard = self._status_keyboard(space, terminal=terminal)
        targets: list[
            tuple[DeliveryKey, RenderedMessage, InlineKeyboardMarkup | None]
        ] = []
        frame = self._frame_for(space_id, terminal=terminal) if animated or terminal else 0
        if space.get("channel_chat_id") and space.get("channel_post_id"):
            key = DeliveryKey(
                CONTROL_ROLE,
                int(space["channel_chat_id"]),
                int(space["channel_post_id"]),
            )
            channel_rendered, _ = self._render(space, state, animation_frame=frame)
            targets.append((key, channel_rendered, None))
        if space.get("discussion_chat_id") and space.get("status_message_id"):
            key = DeliveryKey(
                DISCUSSION_ROLE,
                int(space["discussion_chat_id"]),
                int(space["status_message_id"]),
            )
            _, status_rendered = self._render(space, state, animation_frame=frame)
            targets.append((key, status_rendered, status_keyboard))
        if not targets:
            self._animation_batches.pop(space_id, None)
            return
        self._animation_batches[space_id] = _AnimationBatch(
            frame=frame,
            targets={key for key, *_ in targets},
            advance=animated,
        )
        for key, rendered, reply_markup in targets:
            self._submit_target(
                key,
                rendered,
                space_id=space_id,
                terminal=terminal,
                reply_markup=reply_markup,
                animation_frame=frame,
            )

    def _submit_target(
        self,
        key: DeliveryKey,
        rendered: RenderedMessage,
        *,
        space_id: str,
        terminal: bool,
        reply_markup: InlineKeyboardMarkup | None = None,
        animation_frame: int = 0,
    ) -> None:
        fingerprint = delivery_fingerprint(
            rendered.markdown,
            rendered.plain,
            reply_markup,
        )
        if self._persisted_fingerprint(key) == fingerprint:
            batch = self._animation_batches.get(space_id)
            if batch is not None and batch.frame == animation_frame:
                batch.acknowledged.add(key)
                if batch.targets <= batch.acknowledged:
                    self._animation_batches.pop(space_id, None)
            return
        ticket = self.delivery.submit(
            DeliveryIntent(
                key=key,
                markdown=rendered.markdown,
                plain=rendered.plain,
                reply_markup=reply_markup,
                fingerprint=fingerprint,
                priority=5 if terminal else 10,
                terminal=terminal,
                context=f"space:{space_id}",
            )
        )
        if self._delivery_tickets.get(key) is ticket:
            return
        self._delivery_tickets[key] = ticket
        self._delivery_fingerprints[ticket] = fingerprint

        def on_done(completed: asyncio.Future[DeliveryOutcome]) -> None:
            self._delivery_finished(key, space_id, completed, animation_frame)

        ticket.add_done_callback(on_done)

    def _delivery_finished(
        self,
        key: DeliveryKey,
        space_id: str,
        ticket: asyncio.Future[DeliveryOutcome],
        animation_frame: int,
    ) -> None:
        if self._delivery_tickets.get(key) is not ticket:
            return
        self._delivery_tickets.pop(key, None)
        fingerprint = self._delivery_fingerprints.pop(ticket, "")
        if ticket.cancelled():
            return
        try:
            outcome = ticket.result()
        except Exception:
            return
        if outcome.status == "delivered":
            if fingerprint:
                self._save_persisted_fingerprint(key, fingerprint)
            batch = self._animation_batches.get(space_id)
            if batch is not None and batch.frame == animation_frame:
                batch.performed = batch.performed or outcome.performed
                batch.acknowledged.add(key)
                if batch.targets <= batch.acknowledged:
                    self._animation_batches.pop(space_id, None)
                    if batch.advance and batch.performed:
                        self._animation_indices[space_id] = (
                            batch.frame + 1
                        ) % len(ANIMATION_FRAMES)
        elif outcome.status == "transient_failure" and not self._stopping:
            LOGGER.warning(
                "event=space_dashboard_delivery_retry space_id=%s bot_role=%s "
                "chat_id=%s message_id=%s attempts=%s",
                space_id,
                key.bot_role,
                key.chat_id,
                key.message_id,
                outcome.attempts,
            )
            self._schedule_space(space_id, immediate=False)

    def _frame_for(self, space_id: str, *, terminal: bool) -> int:
        if terminal:
            return ANIMATION_FRAMES.index("🌕")
        return self._animation_indices.get(space_id, 0)

    @staticmethod
    def _is_terminal(space: dict[str, Any], state: ThreadState | dict[str, Any]) -> bool:
        if str(space.get("lifecycle") or "") == "closed":
            return True
        goal = state.goal if isinstance(state, ThreadState) else state.get("goal")
        return isinstance(goal, dict) and str(goal.get("status") or "") == "complete"

    @staticmethod
    def _is_animated(space: dict[str, Any], state: ThreadState | dict[str, Any]) -> bool:
        if str(space.get("lifecycle") or "") != "active":
            return False
        status = state.status if isinstance(state, ThreadState) else state.get("status")
        turn_status = (
            state.turn_status if isinstance(state, ThreadState) else state.get("turn_status")
        )
        return str(status or "") == "active" or str(turn_status or "") == "inProgress"

    @staticmethod
    def _fingerprint_key(key: DeliveryKey) -> str:
        return f"dashboard:{key.bot_role}:{key.chat_id}:{key.message_id}"

    @staticmethod
    def _legacy_fingerprint_key(key: DeliveryKey) -> str:
        return f"telegram-message:dashboard:{key.bot_role}:{key.chat_id}:{key.message_id}"

    def _persisted_fingerprint(self, key: DeliveryKey) -> str:
        getter = getattr(self.store, "get_telegram_message_state", None)
        if callable(getter):
            stored = getter(self._fingerprint_key(key))
            if stored is not None:
                return str(stored.get("semantic_fingerprint") or "")
        value = self.store.get_meta(self._legacy_fingerprint_key(key))
        return str(value or "")

    def _save_persisted_fingerprint(self, key: DeliveryKey, fingerprint: str) -> None:
        putter = getattr(self.store, "put_telegram_message_state", None)
        if callable(putter):
            putter(
                self._fingerprint_key(key),
                bot_role=key.bot_role,
                chat_id=key.chat_id,
                message_id=key.message_id,
                semantic_fingerprint=fingerprint,
                state="delivered",
                payload={"surface": "dashboard"},
            )
            return
        self.store.set_meta(self._legacy_fingerprint_key(key), fingerprint)

    def _state_for_space(self, space: dict[str, Any]) -> ThreadState | dict[str, Any]:
        thread_id = str(space.get("thread_id") or "")
        if thread_id:
            state = self.store.get_thread(thread_id)
            if state:
                return state
        return {
            "thread_id": thread_id,
            "title": space.get("title") or "New Codex session",
            "cwd": space.get("pending_cwd") or "",
            "status": "pending",
            "updated_at": int(space.get("updated_at") or time.time()),
        }

    def _render(
        self,
        space: dict[str, Any],
        state: ThreadState | dict[str, Any],
        *,
        animation_frame: int | None = None,
    ) -> tuple[RenderedMessage, RenderedMessage]:
        lifecycle = str(space.get("lifecycle") or "pending")
        if lifecycle == "pending":
            pending = render_pending_space(space)
            return pending, pending
        if lifecycle == "closed":
            closed = render_closed_space(state, closed_at=space.get("closed_at"))
            return closed, closed
        remaining = self.security.space_unlock_remaining(str(space["space_id"]))
        auth_expires_at = int(time.time()) + remaining if remaining > 0 else None
        render_now: int | None = None
        if self._is_terminal(space, state) or not self._is_animated(space, state):
            updated = state.updated_at if isinstance(state, ThreadState) else state.get("updated_at")
            render_now = int(updated or space.get("updated_at") or time.time())
            auth_expires_at = None
        channel_options: dict[str, Any] = {
            "space": space,
            "lifecycle": lifecycle,
            "heartbeat_seconds": self.config.heartbeat_seconds,
            "animation_frame": animation_frame,
        }
        if "now" in inspect.signature(render_channel_post).parameters:
            channel_options["now"] = render_now
        channel = render_channel_post(state, **channel_options)
        status_options: dict[str, Any] = {
            "space": space,
            "lifecycle": lifecycle,
            "auth_expires_at": auth_expires_at,
            "heartbeat_seconds": self.config.heartbeat_seconds,
        }
        if "now" in inspect.signature(render_status_comment).parameters:
            status_options["now"] = render_now
        if "animation_frame" in inspect.signature(render_status_comment).parameters:
            status_options["animation_frame"] = animation_frame
        status = render_status_comment(state, **status_options)
        return channel, status

    def _callback_button(
        self,
        label: str,
        action: str,
        space: dict[str, Any],
    ) -> InlineKeyboardButton:
        owner = self.store.get_owner()
        nonce = self.store.ensure_callback(
            secrets.token_urlsafe(12),
            action,
            {"space_id": space["space_id"], "generation": space["generation"]},
            owner.user_id if owner else 0,
            int(time.time()) + self.config.callback_seconds,
            bot_role=DISCUSSION_ROLE,
            chat_id=int(space.get("discussion_chat_id") or 0),
            space_id=str(space["space_id"]),
            generation=int(space["generation"]),
        )
        return InlineKeyboardButton(label, callback_data=f"cb:{nonce}")

    def _status_keyboard(
        self,
        space: dict[str, Any],
        *,
        terminal: bool = False,
    ) -> InlineKeyboardMarkup | None:
        post_id = space.get("channel_post_id")
        channel_id = space.get("channel_chat_id")
        rows: list[list[InlineKeyboardButton]] = []
        if not terminal and space.get("lifecycle") in {"pending", "active", "repair_required"}:
            rows.append(
                [
                    self._callback_button("刷新", "space_refresh", space),
                    self._callback_button("取消关注", "space_unwatch", space),
                ]
            )
        if post_id and channel_id:
            rows.append(
                [InlineKeyboardButton("返回帖子", url=private_message_link(int(channel_id), int(post_id)))]
            )
        return InlineKeyboardMarkup(rows) if rows else None

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_seconds)
            for space in self.store.list_spaces():
                if space.get("lifecycle") in {"pending", "active"}:
                    state = self._state_for_space(space)
                    if self._is_animated(space, state):
                        await self.schedule_space(str(space["space_id"]), immediate=True)
