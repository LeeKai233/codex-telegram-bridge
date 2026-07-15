from __future__ import annotations

import asyncio
import contextlib
import logging
import secrets
import time
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError

from .config import Config
from .models import ThreadState
from .security import SecurityManager
from .store import Store
from .telegram_common import DISCUSSION_ROLE, TelegramEndpoint
from .views import (
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
    "turn/plan/updated",
}


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
    ) -> None:
        self.config = config
        self.store = store
        self.security = security
        self.control = control
        self.discussion = discussion
        self._dirty: set[str] = set()
        self._immediate: set[str] = set()
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._stopping = False

    async def start(self) -> None:
        self._stopping = False
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(), name="telegram-space-heartbeat"
            )
        for space in self.store.list_spaces():
            if space.get("lifecycle") in {"pending", "active"}:
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

    async def on_thread_change(self, state: ThreadState, reason: str) -> None:
        for space in self.store.list_spaces():
            if space.get("thread_id") != state.thread_id:
                continue
            if space.get("lifecycle") not in {"active", "repair_required"}:
                continue
            await self.schedule_space(
                str(space["space_id"]), immediate=reason in _IMMEDIATE_REASONS
            )

    async def schedule_space(self, space_id: str, *, immediate: bool = False) -> None:
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
                        "Space dashboard update failed for %s (%s)",
                        space_id[:8],
                        type(exc).__name__,
                    )
                    self._dirty.add(space_id)
                    await asyncio.sleep(5)
                except Exception as exc:
                    LOGGER.error(
                        "Unexpected space dashboard error for %s (%s)",
                        space_id[:8],
                        type(exc).__name__,
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
        channel_rendered, status_rendered = self._render(space, state)
        status_keyboard = self._status_keyboard(space)

        if space.get("channel_chat_id") and space.get("channel_post_id"):
            await self.control.edit_text(
                int(space["channel_chat_id"]),
                int(space["channel_post_id"]),
                channel_rendered.markdown,
                plain=channel_rendered.plain,
            )
        if space.get("discussion_chat_id") and space.get("status_message_id"):
            await self.discussion.edit_text(
                int(space["discussion_chat_id"]),
                int(space["status_message_id"]),
                status_rendered.markdown,
                plain=status_rendered.plain,
                reply_markup=status_keyboard,
            )

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
        self, space: dict[str, Any], state: ThreadState | dict[str, Any]
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
        channel = render_channel_post(
            state,
            lifecycle=lifecycle,
            heartbeat_seconds=self.config.heartbeat_seconds,
        )
        status = render_status_comment(
            state,
            space=space,
            lifecycle=lifecycle,
            auth_expires_at=auth_expires_at,
            heartbeat_seconds=self.config.heartbeat_seconds,
        )
        return channel, status

    def _callback_button(
        self,
        label: str,
        action: str,
        space: dict[str, Any],
    ) -> InlineKeyboardButton:
        owner = self.store.get_owner()
        nonce = secrets.token_urlsafe(12)
        self.store.put_callback(
            nonce,
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

    def _status_keyboard(self, space: dict[str, Any]) -> InlineKeyboardMarkup | None:
        post_id = space.get("channel_post_id")
        channel_id = space.get("channel_chat_id")
        rows: list[list[InlineKeyboardButton]] = []
        if space.get("lifecycle") in {"pending", "active", "repair_required"}:
            rows.append(
                [
                    self._callback_button("刷新", "space_refresh", space),
                    self._callback_button("取消关注", "space_unwatch", space),
                ]
            )
        if post_id and channel_id:
            rows.append(
                [
                    InlineKeyboardButton(
                        "返回帖子", url=private_message_link(int(channel_id), int(post_id))
                    )
                ]
            )
        return InlineKeyboardMarkup(rows) if rows else None

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.heartbeat_seconds)
            for space in self.store.list_spaces():
                if space.get("lifecycle") in {"pending", "active"}:
                    await self.schedule_space(str(space["space_id"]), immediate=True)
