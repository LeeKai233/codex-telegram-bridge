from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from pathlib import Path
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, MessageOriginChannel, ReplyParameters

from .bridge import Bridge
from .models import ThreadState
from .space_dashboard import SpaceDashboardManager, channel_comment_link
from .store import Store
from .telegram_common import TelegramEndpoint
from .views import render_channel_post, render_pending_space, render_status_comment

LOGGER = logging.getLogger(__name__)


class SessionSpaceCoordinator:
    def __init__(
        self,
        store: Store,
        bridge: Bridge,
        control: TelegramEndpoint,
        discussion: TelegramEndpoint,
        dashboards: SpaceDashboardManager,
        *,
        reconcile_seconds: float = 30.0,
        provision_max_attempts: int = 3,
        provision_retry_seconds: float = 30.0,
    ) -> None:
        self.store = store
        self.bridge = bridge
        self.control = control
        self.discussion = discussion
        self.dashboards = dashboards
        self._locks: dict[str, asyncio.Lock] = {}
        self._reconcile_seconds = max(1.0, reconcile_seconds)
        self._provision_max_attempts = max(1, int(provision_max_attempts))
        self._provision_retry_seconds = max(0.0, float(provision_retry_seconds))
        self._reconcile_wakeup = asyncio.Event()
        self._reconcile_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._reconcile_task and not self._reconcile_task.done():
            return
        self._reconcile_wakeup.set()
        self._reconcile_task = asyncio.create_task(
            self._reconcile_loop(), name="telegram-space-reconciliation"
        )

    async def stop(self) -> None:
        task, self._reconcile_task = self._reconcile_task, None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    def request_reconcile(self) -> None:
        self._reconcile_wakeup.set()

    async def _reconcile_loop(self) -> None:
        while True:
            self._reconcile_wakeup.clear()
            await self.reconcile()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._reconcile_wakeup.wait(), timeout=self._reconcile_seconds)

    async def reconcile(self) -> None:
        for space in self.store.list_spaces():
            if space.get("lifecycle") == "closed":
                continue
            try:
                missing_channel_post = space.get("lifecycle") == "repair_required" and not space.get(
                    "channel_post_id"
                )
                missing_status_with_root = (
                    not space.get("status_message_id") and self._discussion_root(space) is not None
                )
                bound_status_needs_repair = space.get("lifecycle") == "repair_required" and bool(
                    space.get("status_message_id")
                )
                if missing_channel_post:
                    await self._provision_channel_post(str(space["space_id"]))
                elif missing_status_with_root or bound_status_needs_repair:
                    await self.provision_status(str(space["space_id"]))
            except asyncio.CancelledError:
                raise
            except Exception:
                LOGGER.exception("Failed to reconcile SessionSpace %s", space.get("space_id"))

    async def _repair_bound_status(self, space: dict[str, Any]) -> dict[str, Any]:
        lifecycle = "pending" if space.get("space_type") == "pending_new" else "active"
        space = (
            self.store.update_space(
                str(space["space_id"]),
                {
                    "lifecycle": lifecycle,
                    "last_error": "",
                    **self._clear_provisioning(),
                },
                expected_generation=int(space["generation"]),
            )
            or space
        )
        await self.dashboards.schedule_space(str(space["space_id"]), immediate=True)
        return space

    def _discussion_root(self, space: dict[str, Any]) -> dict[str, int] | None:
        if space.get("channel_chat_id") is not None and space.get("channel_post_id") is not None:
            root = self.store.get_discussion_root(
                int(space["channel_chat_id"]), int(space["channel_post_id"])
            )
            if root:
                return root
        if space.get("discussion_chat_id") is None or space.get("discussion_root_id") is None:
            return None
        return {
            "discussion_chat_id": int(space["discussion_chat_id"]),
            "root_message_id": int(space["discussion_root_id"]),
        }

    def binding(self) -> dict[str, Any]:
        binding = self.store.get_telegram_binding()
        if not binding:
            raise RuntimeError("Telegram 频道与讨论组尚未绑定")
        return binding

    async def follow_thread(self, thread_id: str) -> dict[str, Any]:
        existing = self.store.get_space_by_thread(thread_id)
        if existing and existing.get("lifecycle") in {"pending", "active", "repair_required"}:
            await self.bridge.subscribe_space_thread(thread_id)
            if existing.get("lifecycle") == "repair_required" and not existing.get("channel_post_id"):
                self.request_reconcile()
            return existing
        state = await self.bridge.subscribe_space_thread(thread_id)
        value = {
            "space_id": uuid.uuid4().hex,
            "space_type": "existing",
            "lifecycle": "pending",
            "thread_id": thread_id,
            "title": state.title,
            "pending_cwd": state.cwd,
            "normal_model": state.model,
            "normal_effort": state.reasoning_effort,
            "plan_model": state.model,
            "plan_effort": state.reasoning_effort,
            "current_mode": "default",
        }
        return await self._create_space(
            value,
            render_channel_post(
                state,
                space=value,
                lifecycle="active",
                animation_frame=0,
            ),
        )

    async def create_pending(
        self,
        cwd: Path,
        prompt: str,
        *,
        normal_model: str = "",
        normal_effort: str = "",
        plan_model: str | None = None,
        plan_effort: str | None = None,
        current_mode: str = "default",
    ) -> dict[str, Any]:
        cwd = self.bridge.path_policy.validate_directory(cwd)
        if current_mode not in {"default", "plan"}:
            raise ValueError(f"Unsupported collaboration mode: {current_mode!r}")
        if bool(normal_model) != bool(normal_effort):
            raise ValueError("Normal model and effort must be provided together")
        if bool(plan_model) != bool(plan_effort):
            raise ValueError("Plan model and effort must be provided together")
        if current_mode == "plan" and not (plan_model and plan_effort):
            raise ValueError("Plan mode requires a model and effort")
        value = {
            "space_id": uuid.uuid4().hex,
            "space_type": "pending_new",
            "lifecycle": "pending",
            "thread_id": None,
            "title": prompt[:80] or "New Codex session",
            "pending_cwd": str(cwd),
            "pending_prompt": prompt,
            "normal_model": normal_model,
            "normal_effort": normal_effort,
            "plan_model": plan_model or "",
            "plan_effort": plan_effort or "",
            "current_mode": current_mode,
        }
        return await self._create_space(value, render_pending_space(value))

    async def _create_space(
        self,
        value: dict[str, Any],
        rendered: Any,
    ) -> dict[str, Any]:
        binding = self.binding()
        value["channel_chat_id"] = int(binding["channel_chat_id"])
        value["discussion_chat_id"] = int(binding["discussion_chat_id"])
        space = self.store.create_space(value)
        return await self._provision_channel_post(str(space["space_id"]), rendered=rendered)

    def _retry_at(self, attempt: int) -> float:
        if attempt >= self._provision_max_attempts:
            return 0.0
        exponent = min(10, max(0, attempt - 1))
        delay = min(3600.0, self._provision_retry_seconds * (2**exponent))
        return time.time() + delay

    def _record_provision_failure(
        self,
        space: dict[str, Any],
        stage: str,
        exc: Exception,
    ) -> None:
        attempts = int(space.get("provision_attempts", 1))
        self.store.update_space(
            str(space["space_id"]),
            {
                "lifecycle": "repair_required",
                "last_error": type(exc).__name__,
                "provision_stage": stage,
                "provision_attempts": attempts,
                "provision_retry_at": self._retry_at(attempts),
            },
            expected_generation=int(space["generation"]),
        )

    @staticmethod
    def _clear_provisioning() -> dict[str, Any]:
        return {
            "provision_stage": "",
            "provision_attempts": 0,
            "provision_retry_at": 0.0,
        }

    async def _provision_channel_post(
        self,
        space_id: str,
        *,
        rendered: Any | None = None,
    ) -> dict[str, Any]:
        lock = self._locks.setdefault(space_id, asyncio.Lock())
        root_exists = False
        async with lock:
            current = self.store.get_space(space_id)
            if not current:
                raise RuntimeError("Session space 不存在")
            if current.get("channel_post_id"):
                return current
            space = self.store.claim_space_provision_attempt(
                space_id,
                "channel_post",
                max_attempts=self._provision_max_attempts,
            )
            if space is None:
                return self.store.get_space(space_id) or current
            try:
                if rendered is None:
                    if space.get("space_type") == "pending_new":
                        rendered = render_pending_space(space)
                    else:
                        state = await self.bridge.subscribe_space_thread(str(space["thread_id"]))
                        rendered = render_channel_post(
                            state,
                            space=space,
                            lifecycle="active",
                            animation_frame=0,
                        )
                message = await self.control.send_text(
                    int(space["channel_chat_id"]),
                    rendered.markdown,
                    plain=rendered.plain,
                )
            except Exception as exc:
                self._record_provision_failure(space, "channel_post", exc)
                raise

            bound = self.store.bind_space_messages(
                space_id,
                channel_chat_id=int(space["channel_chat_id"]),
                channel_post_id=int(message.message_id),
                discussion_chat_id=int(space["discussion_chat_id"]),
                expected_generation=int(space["generation"]),
            )
            if bound is None:
                # Telegram sendMessage has no idempotency key or history lookup. A crash after
                # acceptance but before this bind is an unavoidable exactly-once gap; never
                # manufacture a replacement message ID during reconciliation.
                LOGGER.error("Channel post was accepted but SessionSpace %s could not be bound", space_id)
                return self.store.get_space(space_id) or space
            space = (
                self.store.update_space(
                    space_id,
                    {
                        "lifecycle": "pending",
                        "last_error": "",
                        **self._clear_provisioning(),
                    },
                    expected_generation=int(bound["generation"]),
                )
                or bound
            )
            root_exists = (
                self.store.get_discussion_root(int(space["channel_chat_id"]), int(space["channel_post_id"]))
                is not None
            )

        if root_exists:
            await self.provision_status(space_id)
        return self.store.get_space(space_id) or space

    async def handle_automatic_forward(self, message: Any) -> dict[str, Any] | None:
        binding = self.binding()
        origin = message.forward_origin
        if not isinstance(origin, MessageOriginChannel):
            return None
        if int(origin.chat.id) != int(binding["channel_chat_id"]):
            return None
        sender_chat = message.sender_chat
        if sender_chat is None or int(sender_chat.id) != int(binding["channel_chat_id"]):
            return None
        self.store.record_discussion_root(
            int(origin.chat.id),
            int(origin.message_id),
            int(message.chat_id),
            int(message.message_id),
        )
        self.request_reconcile()
        space = self.store.get_space_by_channel_post(int(origin.chat.id), int(origin.message_id))
        if not space:
            return None
        return await self.provision_status(str(space["space_id"]))

    async def provision_status(self, space_id: str) -> dict[str, Any]:
        lock = self._locks.setdefault(space_id, asyncio.Lock())
        async with lock:
            space = self.store.get_space(space_id)
            if not space:
                raise RuntimeError("Session space 不存在")
            if space.get("status_message_id"):
                if space.get("lifecycle") == "repair_required":
                    return await self._repair_bound_status(space)
                return space
            root = self._discussion_root(space)
            if not root:
                return space
            claimed = self.store.claim_space_provision_attempt(
                space_id,
                "status_comment",
                max_attempts=self._provision_max_attempts,
            )
            if claimed is None:
                return self.store.get_space(space_id) or space
            space = claimed
            lifecycle = "pending" if space.get("space_type") == "pending_new" else "active"
            try:
                if space.get("space_type") == "pending_new":
                    rendered = render_pending_space(space)
                else:
                    state = await self.bridge.subscribe_space_thread(str(space["thread_id"]))
                    rendered = render_status_comment(state, space={**space, "lifecycle": "active"})
                message = await self.discussion.send_text(
                    int(root["discussion_chat_id"]),
                    rendered.markdown,
                    plain=rendered.plain,
                    reply_parameters=ReplyParameters(message_id=int(root["root_message_id"])),
                )
            except Exception as exc:
                self._record_provision_failure(space, "status_comment", exc)
                raise
            space = (
                self.store.bind_space_messages(
                    space_id,
                    channel_chat_id=int(space["channel_chat_id"]),
                    channel_post_id=int(space["channel_post_id"]),
                    discussion_chat_id=int(root["discussion_chat_id"]),
                    discussion_root_id=int(root["root_message_id"]),
                    status_message_id=int(message.message_id),
                    expected_generation=int(space["generation"]),
                )
                or space
            )
            # The same sendMessage crash gap applies here: Telegram cannot deduplicate a
            # status comment accepted before status_message_id reaches SQLite.
            space = (
                self.store.update_space(
                    space_id,
                    {
                        "lifecycle": lifecycle,
                        "last_error": "",
                        **self._clear_provisioning(),
                    },
                    expected_generation=int(space["generation"]),
                )
                or space
            )
            self.store.record_discussion_message(
                int(root["discussion_chat_id"]),
                int(root["root_message_id"]),
                int(root["root_message_id"]),
                space_id,
            )
            self.store.record_discussion_message(
                int(root["discussion_chat_id"]),
                int(message.message_id),
                int(root["root_message_id"]),
                space_id,
            )
            await self.dashboards.schedule_space(space_id, immediate=True)
            return self.store.get_space(space_id) or space

    async def activate_pending(self, space_id: str) -> ThreadState:
        lock = self._locks.setdefault(space_id, asyncio.Lock())
        async with lock:
            space = self.store.get_space(space_id)
            if not space:
                raise RuntimeError("Session space 不存在")
            if space.get("thread_id") and space.get("lifecycle") == "active":
                state = self.store.get_thread(str(space["thread_id"]))
                if state:
                    return state
            if space.get("space_type") != "pending_new" or space.get("lifecycle") not in {
                "pending",
                "repair_required",
            }:
                raise RuntimeError("该评论串不是待创建 session")
            state = await self.bridge.activate_pending_session(
                space_id,
                client_message_id=f"telegram-new-{space_id}-{space['generation']}",
            )
            current = self.store.get_space(space_id) or space
            self.store.update_space(
                space_id,
                {
                    "thread_id": state.thread_id,
                    "lifecycle": "active",
                    "title": state.title,
                    "last_error": "",
                },
                expected_generation=int(current["generation"]),
            )
            await self.dashboards.schedule_space(space_id, immediate=True)
            return state

    async def close(self, space_id: str, generation: int) -> dict[str, Any]:
        lock = self._locks.setdefault(space_id, asyncio.Lock())
        async with lock:
            try:
                closed = await self.bridge.close_session_space(space_id, generation)
            except (RuntimeError, ValueError) as exc:
                raise RuntimeError("该 Session 帖子已变化，请刷新后重试") from exc
            closed_value = closed.to_dict()
            await self.dashboards.schedule_space(space_id, immediate=True)
            return closed_value

    def status_link(self, space: dict[str, Any]) -> str | None:
        return channel_comment_link(space)

    def open_status_keyboard(self, space: dict[str, Any]) -> InlineKeyboardMarkup | None:
        link = self.status_link(space)
        if not link:
            return None
        return InlineKeyboardMarkup([[InlineKeyboardButton("打开实时状态", url=link)]])
