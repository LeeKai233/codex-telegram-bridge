from __future__ import annotations

import asyncio
import contextlib
import difflib
import html
import json
import logging
import re
import secrets
import shlex
import time
import uuid
from collections.abc import Mapping
from io import BytesIO
from pathlib import Path
from typing import Any
from weakref import WeakValueDictionary

from telegram import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeChatMember,
    ForceReply,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyParameters,
    Update,
)
from telegram.constants import ChatMemberStatus, ChatType, ParseMode
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    ApplicationHandlerStop,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    TypeHandler,
    filters,
)

from .approval import (
    ApprovalDecision,
    approval_decision_kind,
    interactive_approval_decisions,
    interactive_approval_is_available,
)
from .bridge import TUI_PLAN_APPROVAL_PROMPT, Bridge
from .config import Config
from .deletions import MessageDeletionManager
from .delivery import delivery_fingerprint
from .files import FileCandidate, PathPolicyError, prepare_inbox_path
from .markdown import clip, compact_path, escape, inline_code
from .models import plan_revision_key
from .rich_text import TelegramHtmlChunk, render_commonmark_chunks
from .security import SecurityManager
from .space_coordinator import SessionSpaceCoordinator
from .space_dashboard import SpaceDashboardManager
from .store import Store
from .telegram_common import (
    DISCUSSION_ROLE,
    TelegramEndpoint,
    balanced_button_rows,
    command_name,
    human_bytes,
    raw_arguments,
)
from .views import (
    RenderedMessage,
    render_ask_error,
    render_ask_waiting,
    render_help,
)
from .workloads import (
    FILE_IO_SPACE,
    MAINTENANCE_SPACE,
    PROMPT_ACTION_SPACE,
    KeyedWorkScheduler,
    Space,
)

LOGGER = logging.getLogger(__name__)

_MAX_RESOLVED_QUESTION_TOMBSTONES = 512
_LOCKED_COMMAND_ALLOWLIST = {"/totp", "/help", "/lock"}
_PLAN_ACTION_SECONDS = 24 * 60 * 60
_INTERACTION_SECONDS = 5 * 60
_PROMPT_WAIT_SECONDS = 30
_PLAN_PROMPT_POLL_SECONDS = 2.0
_PLAN_PROMPT_FAST_WINDOW_SECONDS = 30.0
_PLAN_PROMPT_SLOW_POLL_SECONDS = 10.0
_PLAN_PROMPT_MONITOR_SECONDS = 10 * 60.0
_GETFILE_PAGE_SIZE = 8
_CIRCLED_FILE_BUTTON_LABELS = "①②③④⑤⑥⑦⑧"
_BOT_URL_TOKEN = re.compile(r"(https://api\.telegram\.org/bot)[^/\s]+", re.IGNORECASE)
_BOT_TOKEN = re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{16,}\b")
_STATUS_REFRESH_SECONDS = 5.0
_APPROVAL_REQUEST_KINDS = frozenset({"command_approval", "generic_approval"})

_FILE_IO_CALLBACK_ACTIONS = frozenset({"send_file", "getfile_page", "send_upload"})
_PROMPT_CALLBACK_ACTIONS = frozenset(
    {
        "prompt_mode",
        "queue_cancel",
        "profile_model",
        "profile_effort",
        "profile_cancel",
        "question",
        "command_approval",
        "question_custom",
        "question_clarify",
        "plan_execute",
        "plan_continue",
    }
)


def _callback_workload_space(action: str) -> str | Space:
    if action in _FILE_IO_CALLBACK_ACTIONS:
        return FILE_IO_SPACE
    if action in _PROMPT_CALLBACK_ACTIONS:
        return PROMPT_ACTION_SPACE
    if action == "space_refresh":
        return MAINTENANCE_SPACE
    return "default"


def _redacted_error(error: object) -> str:
    detail = " ".join(str(error).split()) or type(error).__name__
    detail = _BOT_URL_TOKEN.sub(r"\1<redacted>", detail)
    return _BOT_TOKEN.sub("<redacted>", detail)[:240]


def _value(source: object, name: str, default: Any = None) -> Any:
    if isinstance(source, Mapping):
        return source.get(name, default)
    return getattr(source, name, default)


def _pipe_arguments(raw: str, *, limit: int) -> list[str]:
    return [part.strip() for part in raw.split("|", max(0, limit - 1))]


_SESSION_COMMANDS = (
    ("status", "刷新当前 Session 状态"),
    ("totp", "认证当前 Session"),
    ("lock", "锁定当前 Session"),
    ("prompt", "发送 prompt"),
    ("ask", "独立询问 Codex"),
    ("queue", "查看队列或加入 prompt"),
    ("planmode", "进入 Plan Mode"),
    ("changemodel", "切换当前模式的模型"),
    ("plan", "查看完整计划"),
    ("timeline", "查看近期事件"),
    ("attach", "接入 tmux"),
    ("getfile", "获取本机文件"),
    ("unwatch", "取消关注"),
    ("help", "显示帮助"),
)
_KNOWN_COMMANDS = {f"/{command}" for command, _description in _SESSION_COMMANDS} | {
    "/answer",
    "/bind",
}


class DiscussionBotController:
    def __init__(
        self,
        config: Config,
        store: Store,
        security: SecurityManager,
        bridge: Bridge,
        control: TelegramEndpoint,
        discussion: TelegramEndpoint,
        coordinator: SessionSpaceCoordinator,
        dashboards: SpaceDashboardManager,
        deletions: MessageDeletionManager,
    ) -> None:
        self.config = config
        self.store = store
        self.security = security
        self.bridge = bridge
        self.control = control
        self.discussion = discussion
        self.coordinator = coordinator
        self.dashboards = dashboards
        self.deletions = deletions
        self._question_answers: dict[str, dict[str, list[str]]] = {}
        self._question_locks: WeakValueDictionary[str, asyncio.Lock] = WeakValueDictionary()
        self._resolved_questions: dict[str, None] = {}
        self._application: Application | None = None
        self._ask_tasks: set[asyncio.Task[Any]] = set()
        self._ask_waiting_messages: dict[asyncio.Task[Any], tuple[int, int, str]] = {}
        self._interaction_tasks: dict[str, asyncio.Task[None]] = {}
        self._plan_prompt_tasks: dict[str, asyncio.Task[None]] = {}
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._status_refresh_tasks: dict[str, asyncio.Task[None]] = {}
        self._status_refreshed_at: dict[str, float] = {}
        self._prompt_receipts: dict[str, dict[str, Any]] = {}
        self._plan_recovery_done = False
        self._plan_recovery_task: asyncio.Task[None] | None = None
        self._workloads = KeyedWorkScheduler(
            "discussion-work",
            max_pending=256,
            max_running=8,
            spaces=(FILE_IO_SPACE, PROMPT_ACTION_SPACE, MAINTENANCE_SPACE),
        )

    def install(self, application: Application) -> None:
        self._application = application
        application.add_handler(TypeHandler(Update, self._guard), group=-100)
        application.add_handler(MessageHandler(filters.ALL, self.observe_message), group=-50)
        application.add_handler(
            MessageHandler(
                filters.TEXT,
                self._defer_handler(
                    self.reply_to_intent,
                    workload_space=PROMPT_ACTION_SPACE,
                ),
            ),
            group=-25,
        )
        for command, callback, workload_space in (
            ("bind", self.bind, "default"),
            ("help", self.help, "default"),
            ("status", self.status, MAINTENANCE_SPACE),
            ("totp", self.totp, "default"),
            ("lock", self.lock, "default"),
            ("prompt", self.prompt, PROMPT_ACTION_SPACE),
            ("ask", self.ask, PROMPT_ACTION_SPACE),
            ("queue", self.queue, PROMPT_ACTION_SPACE),
            ("planmode", self.planmode, PROMPT_ACTION_SPACE),
            ("changemodel", self.changemodel, PROMPT_ACTION_SPACE),
            ("plan", self.plan, PROMPT_ACTION_SPACE),
            ("timeline", self.timeline, "default"),
            ("attach", self.attach, "default"),
            ("getfile", self.getfile, FILE_IO_SPACE),
            ("unwatch", self.unwatch, "default"),
            ("answer", self.answer, PROMPT_ACTION_SPACE),
        ):
            application.add_handler(
                CommandHandler(
                    command,
                    self._defer_handler(callback, workload_space=workload_space),
                )
            )
        application.add_handler(
            MessageHandler(
                filters.TEXT & filters.Regex(r"(?i)^/bind(?:@[a-z0-9_]+)?(?:\s|$)"),
                self._defer_handler(self._bind_text_fallback),
            )
        )
        application.add_handler(CallbackQueryHandler(self.callback, pattern=r"^cb:"))
        application.add_handler(
            MessageHandler(
                filters.Document.ALL | filters.PHOTO,
                self._defer_handler(self.upload, workload_space=FILE_IO_SPACE),
            ),
            group=1,
        )
        application.add_error_handler(self.error)
        self.bridge.on_question = self.forward_question
        self.bridge.on_command_approval = self.forward_command_approval
        self.bridge.on_notice = self.forward_notice
        self.bridge.on_question_resolved = self.question_resolved
        self.bridge.on_plan_completed = self.plan_completed
        self.bridge.on_prompt_completed = self.prompt_completed
        self.bridge.on_tui_plan_approved = self.plan_turn_started

    def _defer_handler(
        self,
        callback: Any,
        *,
        workload_space: str | Space = "default",
    ) -> Any:
        async def deferred(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
            message = update.effective_message
            space = self._space_for_message(message) if message is not None else None
            if space is not None:
                key = f"space:{space['space_id']}:{space['generation']}"
            else:
                chat = self._chat_for_update(update)
                key = f"chat:{chat.id}" if chat is not None else "discussion"

            async def run() -> None:
                with contextlib.suppress(ApplicationHandlerStop):
                    await callback(update, context)

            if self._workloads.submit(key, run, space=workload_space):
                return
            if space is not None:
                await self._send_space(space, "请求队列已满，请稍后重试。")
            else:
                chat = self._chat_for_update(update)
                if chat is not None:
                    await self._send_unscoped(chat.id, "请求队列已满，请稍后重试。")

        return deferred

    async def stop(self) -> None:
        await self._workloads.stop()
        background = list(self._background_tasks)
        self._background_tasks.clear()
        for task in self._status_refresh_tasks.values():
            if task not in background:
                background.append(task)
        self._status_refresh_tasks.clear()
        for task in background:
            task.cancel()
        if background:
            await asyncio.gather(*background, return_exceptions=True)
        plan_prompt_tasks = list(self._plan_prompt_tasks.values())
        self._plan_prompt_tasks.clear()
        for task in plan_prompt_tasks:
            task.cancel()
        if plan_prompt_tasks:
            await asyncio.gather(*plan_prompt_tasks, return_exceptions=True)
        if self._plan_recovery_task is not None:
            self._plan_recovery_task.cancel()
            await asyncio.gather(self._plan_recovery_task, return_exceptions=True)
            self._plan_recovery_task = None
        interaction_tasks = list(self._interaction_tasks.values())
        self._interaction_tasks.clear()
        for task in interaction_tasks:
            task.cancel()
        if interaction_tasks:
            await asyncio.gather(*interaction_tasks, return_exceptions=True)
        tasks = list(self._ask_tasks)
        self._ask_tasks.clear()
        scheduled = False
        for task in tasks:
            waiting = self._ask_waiting_messages.pop(task, None)
            if waiting is not None and not task.done():
                try:
                    self._schedule_ask_deletion(*waiting)
                    scheduled = True
                except Exception:
                    LOGGER.warning("Unable to schedule waiting ask message during shutdown")
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._ask_waiting_messages.clear()
        if scheduled:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await asyncio.shield(self.deletions.flush())

    async def set_commands(self) -> None:
        await self.discussion.set_my_commands(
            [BotCommand("bind", "绑定频道讨论组"), BotCommand("help", "显示帮助")],
            scope=BotCommandScopeAllGroupChats(),
        )
        binding = self.store.get_telegram_binding()
        owner = self.store.get_owner()
        if binding and owner:
            await self.discussion.set_my_commands(
                [BotCommand(command, description) for command, description in _SESSION_COMMANDS],
                scope=BotCommandScopeChatMember(
                    chat_id=int(binding["discussion_chat_id"]), user_id=owner.user_id
                ),
            )
        await self._recover_interactions()
        await self._ensure_plan_recovery()

    async def _ensure_plan_recovery(self) -> None:
        if self._plan_recovery_done:
            return
        await self._repair_plan_publications()
        client = getattr(self.bridge, "client", None)
        if client is None or bool(getattr(client, "connected", True)):
            await self._recover_plan_executions()
            self._plan_recovery_done = True
            return
        if self._plan_recovery_task is not None and not self._plan_recovery_task.done():
            return
        self._plan_recovery_task = asyncio.create_task(
            self._recover_plan_executions_when_connected(client),
            name="discussion-plan-recovery",
        )

    async def _recover_plan_executions_when_connected(self, client: object) -> None:
        try:
            await client.wait_connected(timeout=120)
            await self._recover_plan_executions()
            self._plan_recovery_done = True
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("event=plan_execution_startup_recovery_failed")
        finally:
            if self._plan_recovery_task is asyncio.current_task():
                self._plan_recovery_task = None

    @staticmethod
    def _chat_for_update(update: Update) -> Any:
        callback = update.callback_query
        callback_message = getattr(callback, "message", None) if callback else None
        callback_chat = getattr(callback_message, "chat", None)
        return callback_chat or update.effective_chat

    async def _guard(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat = self._chat_for_update(update)
        user = update.effective_user
        message = update.effective_message
        if not chat or chat.type != ChatType.SUPERGROUP:
            raise ApplicationHandlerStop
        if not self.store.claim_telegram_update(update.update_id, bot_role=DISCUSSION_ROLE):
            raise ApplicationHandlerStop
        binding = self.store.get_telegram_binding()
        owner = self.store.get_owner()
        command = command_name(update)
        if binding is None:
            if command in {"/bind", "/help"} and self._command_targets_this_bot(update):
                if (
                    owner
                    and user
                    and user.id == owner.user_id
                    and (not message or message.sender_chat is None)
                ):
                    return
                if command == "/bind" and message and not message.is_automatic_forward:
                    control_label = self.config.control_bot_label
                    await self._send_unscoped(
                        chat.id,
                        f"绑定请求未获授权。请使用已与 {escape(control_label)} 配对的个人账号发送，"
                        "并将发送身份切换为个人账号；若启用了匿名管理员，请先关闭。",
                        plain=(
                            f"绑定请求未获授权。请使用已与 {control_label} 配对的个人账号发送，"
                            "并将发送身份切换为个人账号；若启用了匿名管理员，请先关闭。"
                        ),
                    )
            raise ApplicationHandlerStop
        if chat.id != int(binding["discussion_chat_id"]):
            raise ApplicationHandlerStop
        if message and message.is_automatic_forward:
            return
        if not owner or not user or user.id != owner.user_id:
            raise ApplicationHandlerStop
        if message and message.sender_chat is not None:
            raise ApplicationHandlerStop
        if update.callback_query:
            return
        space = self._space_for_message(message) if message else None
        if space and space.get("lifecycle") == "closed":
            if message:
                await self.discussion.delete_message(chat.id, message.message_id)
            raise ApplicationHandlerStop
        if space is None and command not in {"/help", "/bind"}:
            raise ApplicationHandlerStop
        if (
            space is not None
            and command not in _LOCKED_COMMAND_ALLOWLIST
            and not self.security.is_space_unlocked(str(space["space_id"]))
        ):
            await self._send_space(
                space,
                "写操作已锁定，请先在当前评论串发送 `/totp <验证码>`。",
            )
            LOGGER.info(
                "event=locked_update_rejected space_id=%s command=%s",
                str(space["space_id"])[:12],
                command or "message",
            )
            raise ApplicationHandlerStop

    def _command_targets_this_bot(self, update: Update) -> bool:
        message = update.effective_message
        raw = (message.text or message.caption or "") if message else ""
        first = raw.lstrip().split(maxsplit=1)[0] if raw.strip() else ""
        _, separator, target = first.partition("@")
        if not separator:
            return True
        with contextlib.suppress(RuntimeError):
            username = self.discussion.bot.username
            return bool(username and target.casefold() == username.casefold())
        return False

    async def observe_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        message = update.effective_message
        if not message:
            return
        if message.is_automatic_forward:
            await self.coordinator.handle_automatic_forward(message)
            raise ApplicationHandlerStop
        space = self._space_for_message(message)
        if space:
            root = int(space["discussion_root_id"])
            self.store.record_discussion_message(
                int(message.chat_id), int(message.message_id), root, str(space["space_id"])
            )

    async def bind(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat = update.effective_chat
        if not chat:
            return
        if self.store.get_telegram_binding():
            await self._send_unscoped(chat.id, "讨论组已经绑定。")
            return
        code = raw_arguments(update)
        if not code:
            await self._send_unscoped(chat.id, "用法：`/bind <本机 bind code>`")
            return
        group = await self.discussion.query(lambda: self.discussion.bot.get_chat(chat.id))
        channel_id = getattr(group, "linked_chat_id", None)
        if group.type != ChatType.SUPERGROUP or not channel_id:
            raise RuntimeError("当前群不是已关联频道的讨论超级群")
        if bool(getattr(group, "is_forum", False)):
            raise RuntimeError("讨论组启用了 Forum Topics，请先关闭 Topics")
        if getattr(group, "username", None):
            raise RuntimeError("讨论组必须保持私有")
        channel = await self.control.query(lambda: self.control.bot.get_chat(int(channel_id)))
        if channel.type != ChatType.CHANNEL or getattr(channel, "username", None):
            raise RuntimeError("关联频道必须保持私有")
        if int(getattr(channel, "linked_chat_id", 0) or 0) != chat.id:
            raise RuntimeError("频道与讨论组的 linked_chat_id 不是双向一致")
        control_me, discussion_me = await asyncio.gather(
            self.control.get_me(lane="interactive"),
            self.discussion.get_me(lane="interactive"),
        )
        control_member, discussion_member = await asyncio.gather(
            self.control.query(
                lambda: self.control.bot.get_chat_member(int(channel_id), control_me.id)
            ),
            self.discussion.query(
                lambda: self.discussion.bot.get_chat_member(chat.id, discussion_me.id)
            ),
        )
        if control_member.status != ChatMemberStatus.ADMINISTRATOR:
            raise RuntimeError(f"{self.config.control_bot_label} 不是频道管理员")
        if not getattr(control_member, "can_post_messages", False):
            raise RuntimeError(f"{self.config.control_bot_label} 缺少发布消息权限")
        if not getattr(control_member, "can_edit_messages", False):
            raise RuntimeError(f"{self.config.control_bot_label} 缺少编辑消息权限")
        if discussion_member.status != ChatMemberStatus.ADMINISTRATOR:
            raise RuntimeError(f"{self.config.discussion_bot_label} 不是讨论组管理员")
        if not getattr(discussion_member, "can_delete_messages", False):
            raise RuntimeError(f"{self.config.discussion_bot_label} 缺少删除消息权限")
        owner = self.store.get_owner()
        if not owner:
            raise RuntimeError(f"请先在 {self.config.control_bot_label} 私聊完成 owner 配对")
        owner_member = await self.discussion.query(
            lambda: self.discussion.bot.get_chat_member(chat.id, owner.user_id)
        )
        if bool(getattr(owner_member, "is_anonymous", False)):
            raise RuntimeError("owner 的匿名管理员模式必须关闭")
        if not self.store.consume_bind_code(code):
            raise RuntimeError("bind code 无效、过期或尝试次数过多")
        extra_rights = self._extra_rights(discussion_member)
        self.store.set_telegram_binding(
            {
                "channel_chat_id": int(channel_id),
                "discussion_chat_id": chat.id,
                "control_bot_id": control_me.id,
                "forum_bot_id": discussion_me.id,
                "is_forum": False,
                "control_can_post_messages": True,
                "control_can_edit_messages": True,
                "discussion_can_delete_messages": True,
                "extra_discussion_rights": extra_rights,
            }
        )
        await self.set_commands()
        text = "频道与讨论组绑定成功。"
        plain = text
        if extra_rights:
            extra_rights_text = "、".join(extra_rights)
            text += (
                f"\n安全提示：{escape(self.config.discussion_bot_label)}"
                f" 仍有额外管理员权限：{extra_rights_text}"
            )
            plain += f"\n安全提示：{self.config.discussion_bot_label} 仍有额外管理员权限：{extra_rights_text}"
        await self._send_unscoped(chat.id, text, plain=plain)

    async def _bind_text_fallback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not self._command_targets_this_bot(update):
            return
        await self.bind(update, context)

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat = update.effective_chat
        if not chat:
            return
        binding = self.store.get_telegram_binding()
        space = self._space_for_message(update.effective_message)
        rendered = render_help(
            "426",
            label=self.config.discussion_bot_label,
            bound=binding is not None,
            in_session_thread=space is not None,
        )
        if space:
            await self._send_space(space, rendered.markdown, plain=rendered.plain)
        else:
            await self._send_unscoped(chat.id, rendered.markdown, plain=rendered.plain)

    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        space = self._require_space(update)
        await self.dashboards.schedule_space(str(space["space_id"]), immediate=True)
        link = self.coordinator.status_link(space)
        await self._send_space(
            space,
            "状态快照已显示，后台刷新中。",
            reply_markup=(
                InlineKeyboardMarkup([[InlineKeyboardButton("打开状态", url=link)]]) if link else None
            ),
        )
        if space.get("thread_id"):
            self._schedule_status_refresh(space)

    async def totp(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        space = self._require_space(update)
        message = update.effective_message
        if message:
            await self.discussion.delete_message(message.chat_id, message.message_id)
        code = raw_arguments(update)
        if not code:
            await self._send_space(space, "用法：`/totp <6 位验证码或恢复码>`")
            return
        space_id = str(space["space_id"])
        if not await asyncio.to_thread(self.security.verify_for_space, space_id, code):
            await self._send_space(space, "验证码无效、已使用，或验证暂时锁定。")
            return
        if space.get("space_type") == "pending_new" and space.get("lifecycle") in {
            "pending",
            "repair_required",
        }:
            state = await self.coordinator.activate_pending(space_id)
            await self._send_space(space, f"已创建 Session {inline_code(state.short_id)}。")
        else:
            minutes = max(1, self.config.totp_unlock_seconds // 60)
            await self._send_space(space, f"当前 Session 已解锁 {inline_code(minutes)} 分钟。")
        await self.dashboards.schedule_space(space_id, immediate=True)

    async def lock(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        space = self._require_space(update)
        self.security.lock_space(str(space["space_id"]))
        await self.dashboards.schedule_space(str(space["space_id"]), immediate=True)
        await self._send_space(space, "当前 Session 已锁定。")

    async def prompt(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        space = self._require_active_unlocked(update)
        prompt = raw_arguments(update)
        if not prompt:
            await self._send_space(space, "用法：`/prompt <内容>`")
            return
        await self._send_prompt(space, prompt, "steer")

    async def ask(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        space = self._require_active_unlocked(update)
        question = raw_arguments(update)
        if not question:
            await self._send_space(space, "用法：`/ask <问题>`")
            return
        await self._launch_ask(space, question, clarification=False, update=update)

    async def _launch_ask(
        self,
        space: dict[str, Any],
        question: str,
        *,
        clarification: bool,
        update: Update | None = None,
    ) -> None:
        token = uuid.uuid4().hex
        ask_id = token[:8]
        rendered = render_ask_waiting(question, ask_id, clarification=clarification)
        waiting = await self._send_space(
            space,
            rendered.markdown,
            plain=rendered.plain,
            priority=5,
        )
        coroutine = self._complete_ask(
            space,
            question,
            ask_id,
            int(waiting.message_id),
            client_message_id=f"telegram-ask-{space['space_id']}-{token}",
        )
        if self._application is not None:
            task = self._application.create_task(
                coroutine,
                update=update,
                name=f"codex-tg-ask-{ask_id}",
            )
        else:
            task = asyncio.create_task(coroutine, name=f"codex-tg-ask-{ask_id}")
        self._ask_tasks.add(task)
        self._ask_waiting_messages[task] = (
            int(space["discussion_chat_id"]),
            int(waiting.message_id),
            ask_id,
        )
        task.add_done_callback(self._ask_completed)

    def _ask_completed(self, task: asyncio.Task[Any]) -> None:
        self._ask_tasks.discard(task)
        self._ask_waiting_messages.pop(task, None)

    def _schedule_ask_deletion(self, chat_id: int, message_id: int, ask_id: str) -> None:
        self.deletions.schedule(
            DISCUSSION_ROLE,
            chat_id,
            [message_id],
            delete_at=int(time.time()),
            group_key=f"ask:{ask_id}",
        )

    async def _complete_ask(
        self,
        space: dict[str, Any],
        question: str,
        ask_id: str,
        waiting_message_id: int,
        *,
        client_message_id: str,
    ) -> None:
        rich_answer: str | None = None
        try:
            answer = await self.bridge.ask_space_question(
                str(space["space_id"]),
                question,
                client_message_id=client_message_id,
            )
        except TimeoutError:
            rendered = render_ask_error(ask_id, "等待 Codex 回答超时（300 秒）。")
        except asyncio.CancelledError:
            try:
                self._schedule_ask_deletion(int(space["discussion_chat_id"]), waiting_message_id, ask_id)
                await asyncio.shield(self.deletions.flush())
            except asyncio.CancelledError, Exception:
                LOGGER.warning("Unable to flush cancelled ask message %s", ask_id)
            raise
        except Exception as exc:
            LOGGER.exception("event=ask_failed ask_id=%s", ask_id)
            detail = clip(str(exc) or type(exc).__name__, 500)
            rendered = render_ask_error(ask_id, f"独立提问失败：{detail}")
        else:
            rich_answer = answer
            rendered = None
        current = self.store.get_space(str(space["space_id"]))
        if (
            current is None
            or current.get("lifecycle") != "active"
            or int(current["generation"]) != int(space["generation"])
        ):
            await self.discussion.delete_message(
                int(space["discussion_chat_id"]),
                waiting_message_id,
            )
            return
        if rich_answer is not None:
            await self._edit_or_resend_rich_ask(
                current,
                waiting_message_id,
                question,
                rich_answer,
                ask_id,
            )
            return
        if rendered is not None:
            await self._edit_or_resend_ask(current, waiting_message_id, rendered)

    async def _edit_or_resend_rich_ask(
        self,
        space: dict[str, Any],
        message_id: int,
        question: str,
        answer: str,
        ask_id: str,
    ) -> None:
        chunks = render_commonmark_chunks(answer, limit=3400)
        if not chunks:
            chunks = [
                TelegramHtmlChunk(
                    html="<i>Codex 没有返回文本回答。</i>",
                    plain="Codex 没有返回文本回答。",
                )
            ]
        question_text = clip(" ".join(question.split()), 500)
        header_html = (
            f"<b>💬 Codex 回答 · <code>{html.escape(ask_id)}</code></b>\n"
            f"<b>❓ {html.escape(question_text)}</b>\n\n"
        )
        header_plain = f"💬 Codex 回答 · {ask_id}\n❓ {question_text}\n\n"
        first = chunks[0]
        try:
            await self.discussion.edit_text(
                int(space["discussion_chat_id"]),
                message_id,
                header_html + first.html,
                plain=header_plain + first.plain,
                parse_mode=ParseMode.HTML,
                priority=5,
            )
        except TelegramError:
            await self.discussion.delete_message(int(space["discussion_chat_id"]), message_id)
            await self._send_space_html(
                space,
                header_html + first.html,
                plain=header_plain + first.plain,
                priority=5,
            )
        for chunk in chunks[1:]:
            await self._send_space_html(
                space,
                chunk.html,
                plain=chunk.plain,
                priority=5,
            )
        LOGGER.info(
            "event=ask_completed space_id=%s ask_id=%s chunks=%d",
            str(space["space_id"])[:12],
            ask_id,
            len(chunks),
        )

    async def _edit_or_resend_ask(
        self,
        space: dict[str, Any],
        message_id: int,
        rendered: RenderedMessage,
    ) -> None:
        try:
            await self.discussion.edit_text(
                int(space["discussion_chat_id"]),
                message_id,
                rendered.markdown,
                plain=rendered.plain,
                priority=5,
            )
        except TelegramError:
            await self._send_space(
                space,
                rendered.markdown,
                plain=rendered.plain,
                priority=5,
            )

    async def queue(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        space = self._require_active_unlocked(update)
        prompt = raw_arguments(update)
        if prompt:
            await self._send_prompt(space, prompt, "queue")
            return
        entries = self.store.space_queue_entries(str(space["space_id"]), int(space["generation"]))
        lines = ["*📥 Queue*"]
        rows: list[list[InlineKeyboardButton]] = []
        if not entries:
            lines.append("队列为空。")
        for index, entry in enumerate(entries[:20], 1):
            lines.append(f"{index}\\. {escape(clip(str(entry['prompt']), 180))}")
            rows.append(
                [
                    self._button(
                        f"取消 {index}",
                        "queue_cancel",
                        {"queue_id": entry["queue_id"]},
                        space,
                    )
                ]
            )
        await self._send_space(
            space,
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(rows) if rows else None,
        )

    async def _send_prompt(
        self,
        space: dict[str, Any],
        prompt: str,
        mode: str,
        *,
        client_message_id: str | None = None,
        receipt_message_id: int | None = None,
    ) -> None:
        existing_receipt = (
            self._prompt_receipt(space, client_message_id) if client_message_id else None
        )
        client_message_id = client_message_id or f"telegram-{space['space_id']}-{uuid.uuid4()}"
        receipt_state = existing_receipt
        if receipt_state is None:
            if receipt_message_id is None:
                receipt = await self._send_space(space, "📨 已收到请求。", priority=5)
                receipt_message_id = int(receipt.message_id)
            receipt_state = {
                "space_id": str(space["space_id"]),
                "generation": int(space["generation"]),
                "message_id": receipt_message_id,
                "state": "received",
            }
            self._prompt_receipts[client_message_id] = receipt_state
            self._persist_prompt_receipt(
                space,
                client_message_id,
                receipt_state,
                "📨 已收到请求。",
            )
        receipt_message_id = int(receipt_state["message_id"])
        await self._edit_prompt_receipt(space, client_message_id, "submitting")
        try:
            result = await self.bridge.send_space_prompt(
                str(space["space_id"]),
                prompt,
                mode=mode,
                client_message_id=client_message_id,
            )
            self.store.link_prompt_intent_receipt(
                client_message_id,
                self._prompt_receipt_key(client_message_id),
            )
        except asyncio.CancelledError:
            await asyncio.shield(self._edit_prompt_receipt(space, client_message_id, "cancelled"))
            raise
        except (TimeoutError, OSError):
            await self._edit_prompt_receipt(space, client_message_id, "uncertain")
            return
        except Exception as exc:
            await self._edit_prompt_receipt(
                space,
                client_message_id,
                "failed",
                detail=_redacted_error(exc),
            )
            return
        if result == "choose":
            await self._edit_prompt_receipt(
                space,
                client_message_id,
                "choose",
                text="当前 turn 正在运行，请选择投递方式：",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            self._button(
                                "BTW · 当前 turn",
                                "prompt_mode",
                                {
                                    "prompt": prompt,
                                    "mode": "steer",
                                    "client_message_id": client_message_id,
                                    "receipt_message_id": receipt_message_id,
                                },
                                space,
                            )
                        ],
                        [
                            self._button(
                                "Queue · 稍后",
                                "prompt_mode",
                                {
                                    "prompt": prompt,
                                    "mode": "queue",
                                    "client_message_id": client_message_id,
                                    "receipt_message_id": receipt_message_id,
                                },
                                space,
                            ),
                        ]
                    ]
                ),
            )
            return
        state = result if result in {"started", "steered", "queued"} else "failed"
        await self._edit_prompt_receipt(space, client_message_id, state, detail=str(result))

    async def _edit_prompt_receipt(
        self,
        space: dict[str, Any],
        client_message_id: str,
        state: str,
        *,
        detail: str = "",
        text: str | None = None,
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> bool:
        receipt = self._prompt_receipt(space, client_message_id)
        if receipt is None:
            return False
        labels = {
            "received": "📨 已收到请求。",
            "choose": "当前 turn 正在运行，请选择投递方式：",
            "queued": "📥 已加入队列。",
            "submitting": "⏳ 正在提交给 Codex。",
            "started": "▶️ 已开始执行。",
            "steered": "↪️ 已注入当前 turn。",
            "completed": "✅ `/prompt` 任务已完成。Codex 将继续此前工作；若无待处理指令则进入空闲。",
            "failed": "❌ `/prompt` 任务失败。",
            "uncertain": "⚠️ 请求送达状态待确认；请勿重复提交。",
            "cancelled": "⏹ `/prompt` 任务已取消。",
        }
        message = text or labels.get(state, escape(state))
        if detail and state == "failed":
            message += f"\n{inline_code(clip(detail, 160))}"
        try:
            await self.discussion.edit_text(
                int(space["discussion_chat_id"]),
                int(receipt["message_id"]),
                message,
                reply_markup=reply_markup,
                priority=5,
            )
        except TelegramError:
            replacement = await self._send_space(
                space,
                message,
                reply_markup=reply_markup,
                priority=5,
            )
            receipt["message_id"] = int(replacement.message_id)
        receipt["state"] = state
        self._persist_prompt_receipt(
            space,
            client_message_id,
            receipt,
            message,
            reply_markup=reply_markup,
        )
        return True

    @staticmethod
    def _prompt_receipt_key(client_message_id: str) -> str:
        return f"prompt:{client_message_id}"

    def _prompt_receipt(
        self,
        space: Mapping[str, Any],
        client_message_id: str,
    ) -> dict[str, Any] | None:
        receipt = self._prompt_receipts.get(client_message_id)
        if receipt is not None:
            return receipt
        stored = self.store.get_telegram_message_state(
            self._prompt_receipt_key(client_message_id)
        )
        if stored is None:
            return None
        payload = stored.get("payload")
        if not isinstance(payload, Mapping):
            return None
        if (
            str(stored.get("bot_role") or "") != DISCUSSION_ROLE
            or int(stored.get("chat_id") or 0) != int(space["discussion_chat_id"])
            or str(payload.get("space_id") or "") != str(space["space_id"])
            or int(payload.get("generation") or 0) != int(space["generation"])
            or str(payload.get("client_message_id") or "") != client_message_id
        ):
            return None
        receipt = {
            "space_id": str(space["space_id"]),
            "generation": int(space["generation"]),
            "message_id": int(stored["message_id"]),
            "state": str(stored.get("state") or "received"),
        }
        self._prompt_receipts[client_message_id] = receipt
        return receipt

    def _persist_prompt_receipt(
        self,
        space: Mapping[str, Any],
        client_message_id: str,
        receipt: Mapping[str, Any],
        message: str,
        *,
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> None:
        self.store.put_telegram_message_state(
            self._prompt_receipt_key(client_message_id),
            bot_role=DISCUSSION_ROLE,
            chat_id=int(space["discussion_chat_id"]),
            message_id=int(receipt["message_id"]),
            semantic_fingerprint=delivery_fingerprint(message, message, reply_markup),
            state=str(receipt.get("state") or "received"),
            payload={
                "space_id": str(space["space_id"]),
                "generation": int(space["generation"]),
                "client_message_id": client_message_id,
            },
        )

    async def planmode(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        space = self._require_active_unlocked(update)
        await self._ensure_plan_ready(space)
        raw = raw_arguments(update).strip()
        if not raw:
            await self._begin_profile_interaction(space, "planmode")
            return
        parts = _pipe_arguments(raw, limit=3)
        if len(parts) not in {2, 3} or not parts[0] or not parts[1]:
            await self._send_space(
                space,
                "用法：`/planmode <model> | <effort> [ | <prompt> ]`；全不带参数可进入交互模式。",
            )
            return
        profile = await self._resolve_profile_or_suggest(
            space,
            "planmode",
            parts[0],
            parts[1],
            prompt=parts[2] if len(parts) == 3 else None,
        )
        if profile is None:
            return
        await self.bridge.set_space_profile(
            str(space["space_id"]),
            "plan",
            str(_value(profile, "model")),
            str(_value(profile, "effort")),
        )
        if len(parts) == 3 and parts[2]:
            await self._start_plan_mode(space, parts[2], profile)
            return
        await self._wait_for_plan_prompt(space, profile)

    async def changemodel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        space = self._require_active_unlocked(update)
        raw = raw_arguments(update).strip()
        if not raw:
            await self._begin_profile_interaction(space, "changemodel")
            return
        parts = _pipe_arguments(raw, limit=2)
        if len(parts) != 2 or not parts[0] or not parts[1]:
            await self._send_space(
                space,
                "用法：`/changemodel <model> | <effort>`；全不带参数可进入交互模式。",
            )
            return
        profile = await self._resolve_profile_or_suggest(space, "changemodel", parts[0], parts[1])
        if profile is None:
            return
        updated = await self.bridge.change_space_model(
            str(space["space_id"]),
            str(_value(profile, "model")),
            str(_value(profile, "effort")),
        )
        await self._announce_model_change(updated, profile)

    async def _begin_profile_interaction(self, space: dict[str, Any], kind: str) -> None:
        options = await self._model_options()
        if not options:
            raise RuntimeError("Codex 当前没有返回可用模型")
        draft = self._replace_interaction(
            space,
            kind=kind,
            phase="select_model",
            payload={},
            expires_at=int(time.time()) + _INTERACTION_SECONDS,
        )
        buttons = [
            self._interaction_button(
                clip(
                    str(_value(option, "display_name") or _value(option, "model")),
                    60,
                ),
                "profile_model",
                draft,
                {"model": str(_value(option, "model"))},
                space,
            )
            for option in options
        ]
        label = "Plan Mode" if kind == "planmode" else "当前模式"
        await self._send_space(
            space,
            f"请选择 {label} 使用的模型：",
            reply_markup=InlineKeyboardMarkup(
                [
                    *balanced_button_rows(buttons, columns=2),
                    [self._interaction_button("退出", "profile_cancel", draft, {}, space)],
                ]
            ),
        )

    async def _profile_model_selected(
        self,
        space: dict[str, Any],
        payload: dict[str, Any],
        *,
        message_id: int | None = None,
        chat_id: int | None = None,
    ) -> None:
        draft = self._current_draft(payload, phase="select_model")
        model = str(payload.get("model") or "")
        option = next(
            (
                candidate
                for candidate in await self._model_options()
                if str(_value(candidate, "model")) == model
            ),
            None,
        )
        if option is None:
            raise RuntimeError("模型目录已更新，请重新执行命令")
        efforts = tuple(str(value) for value in (_value(option, "supported_efforts", ()) or ()))
        if not efforts:
            raise RuntimeError("该模型没有可用的 effort")
        advanced = self.store.advance_interaction(
            str(_value(draft, "scope_key")),
            str(_value(draft, "flow_id")),
            int(_value(draft, "revision")),
            phase="select_effort",
            payload={"model": model},
            expires_at=int(time.time()) + _INTERACTION_SECONDS,
        )
        if advanced is None:
            raise RuntimeError("这次交互已被新命令替换")
        self._schedule_interaction_timeout(advanced)
        await self._edit_selection_message(
            space,
            message_id,
            f"已选择模型 {inline_code(model)}。",
            plain=f"已选择模型 {model}。",
            chat_id=chat_id,
        )
        buttons = [
            self._interaction_button(
                effort,
                "profile_effort",
                advanced,
                {"model": model, "effort": effort},
                space,
            )
            for effort in efforts
        ]
        await self._send_space(
            space,
            f"模型 {inline_code(model)} 支持以下 effort：",
            reply_markup=InlineKeyboardMarkup(
                [
                    *balanced_button_rows(buttons, columns=3),
                    [
                        self._interaction_button(
                            "退出", "profile_cancel", advanced, {}, space
                        )
                    ],
                ]
            ),
        )

    async def _profile_effort_selected(
        self,
        space: dict[str, Any],
        payload: dict[str, Any],
        *,
        message_id: int | None = None,
        chat_id: int | None = None,
    ) -> None:
        draft = self._current_draft(payload, phase="select_effort")
        stored_payload = dict(_value(draft, "payload", {}) or {})
        model = str(payload.get("model") or "")
        effort = str(payload.get("effort") or "")
        if model != str(stored_payload.get("model") or ""):
            raise RuntimeError("模型选择已过期")
        kind = str(_value(draft, "kind"))
        profile = await self._resolve_profile_or_suggest(space, kind, model, effort)
        if profile is None:
            return
        await self._edit_selection_message(
            space,
            message_id,
            f"已选择 effort {inline_code(str(_value(profile, 'effort')))}。",
            plain=f"已选择 effort {str(_value(profile, 'effort'))}。",
            chat_id=chat_id,
        )
        if kind == "changemodel":
            claimed = self.store.claim_interaction(
                str(_value(draft, "scope_key")),
                str(_value(draft, "flow_id")),
                int(_value(draft, "revision")),
            )
            if claimed is None:
                raise RuntimeError("这次交互已被新命令替换")
            self._cancel_interaction_timeout(str(_value(draft, "scope_key")))
            updated = await self.bridge.change_space_model(
                str(space["space_id"]),
                str(_value(profile, "model")),
                str(_value(profile, "effort")),
            )
            await self._announce_model_change(updated, profile)
            return
        await self._advance_to_prompt_wait(draft, profile, space)

    async def _cancel_profile_interaction(
        self,
        space: dict[str, Any],
        payload: dict[str, Any],
        *,
        message_id: int | None = None,
        chat_id: int | None = None,
    ) -> None:
        scope_key = str(payload.get("scope_key") or "")
        draft = self.store.get_interaction(scope_key)
        if (
            draft is None
            or str(_value(draft, "flow_id")) != str(payload.get("flow_id") or "")
            or int(_value(draft, "revision")) != int(payload.get("revision") or 0)
            or int(_value(draft, "expires_at", 0)) <= int(time.time())
        ):
            raise RuntimeError("这次交互已被新命令替换或已经过期")
        claimed = self.store.claim_interaction(
            scope_key,
            str(_value(draft, "flow_id")),
            int(_value(draft, "revision")),
        )
        if claimed is None:
            raise RuntimeError("这次交互已被新命令替换")
        self._cancel_interaction_timeout(scope_key)
        command = "/planmode" if str(_value(draft, "kind")) == "planmode" else "/changemodel"
        await self._edit_selection_message(
            space,
            message_id,
            f"已退出 `{command}`。",
            plain=f"已退出 {command}。",
            chat_id=chat_id,
        )

    async def _wait_for_plan_prompt(self, space: dict[str, Any], profile: object) -> None:
        draft = self._replace_interaction(
            space,
            kind="planmode",
            phase="await_prompt",
            payload={
                "model": str(_value(profile, "model")),
                "effort": str(_value(profile, "effort")),
            },
            expires_at=int(time.time()) + _PROMPT_WAIT_SECONDS,
        )
        await self._send_plan_prompt_request(space, draft)

    async def _advance_to_prompt_wait(self, draft: object, profile: object, space: dict[str, Any]) -> None:
        advanced = self.store.advance_interaction(
            str(_value(draft, "scope_key")),
            str(_value(draft, "flow_id")),
            int(_value(draft, "revision")),
            phase="await_prompt",
            payload={
                "model": str(_value(profile, "model")),
                "effort": str(_value(profile, "effort")),
            },
            expires_at=int(time.time()) + _PROMPT_WAIT_SECONDS,
        )
        if advanced is None:
            raise RuntimeError("这次交互已被新命令替换")
        try:
            await self.bridge.set_space_profile(
                str(space["space_id"]),
                "plan",
                str(_value(profile, "model")),
                str(_value(profile, "effort")),
            )
        except Exception:
            self.store.claim_interaction(
                str(_value(advanced, "scope_key")),
                str(_value(advanced, "flow_id")),
                int(_value(advanced, "revision")),
            )
            self._cancel_interaction_timeout(str(_value(advanced, "scope_key")))
            raise
        self._schedule_interaction_timeout(advanced)
        await self._send_plan_prompt_request(space, advanced)

    async def _send_plan_prompt_request(self, space: dict[str, Any], draft: object) -> None:
        await self._send_space(
            space,
            "请在 30 秒内发送进入 Plan Mode 后的第一条 prompt。超时将取消本次模式切换。",
            reply_markup=InlineKeyboardMarkup(
                [[self._interaction_button("退出", "profile_cancel", draft, {}, space)]]
            ),
        )

    async def _consume_plan_prompt(self, space: dict[str, Any], draft: object, prompt: str) -> bool:
        now = int(time.time())
        if int(_value(draft, "expires_at", 0)) <= now:
            return False
        claimed = self.store.claim_interaction(
            str(_value(draft, "scope_key")),
            str(_value(draft, "flow_id")),
            int(_value(draft, "revision")),
        )
        if claimed is None:
            return False
        self._cancel_interaction_timeout(str(_value(draft, "scope_key")))
        payload = dict(_value(claimed, "payload", {}) or {})
        profile = await self.bridge.resolve_model_profile(
            str(payload.get("model") or ""),
            str(payload.get("effort") or ""),
        )
        await self._start_plan_mode(space, prompt, profile)
        return True

    async def _start_plan_mode(self, space: dict[str, Any], prompt: str, profile: object) -> None:
        await self._ensure_plan_ready(space)
        turn = await self.bridge.start_space_collaboration_turn(
            str(space["space_id"]),
            prompt,
            mode="plan",
            client_message_id=f"telegram-planmode-{space['space_id']}-{uuid.uuid4()}",
            profile=profile,
        )
        await self.dashboards.schedule_space(str(space["space_id"]), immediate=True)
        await self._send_space(
            space,
            "已进入 Plan Mode。\n"
            f"{inline_code(str(_value(profile, 'model')))} · "
            f"{inline_code(str(_value(profile, 'effort')))}\n"
            f"Turn {inline_code(str(turn.get('id') or '')[:8])}",
            priority=5,
        )

    async def _ensure_plan_ready(self, space: dict[str, Any]) -> Any:
        state = await self._state(space)
        if state.status == "active" or state.turn_status == "inProgress":
            raise RuntimeError("当前 turn 正在运行，请稍后重试")
        blockers = set(state.active_flags) & {"waitingOnApproval", "waitingOnUserInput"}
        if blockers:
            raise RuntimeError("当前 Session 正在等待审批或用户输入，不能进入 Plan Mode")
        if self.store.space_queue_entries(str(space["space_id"]), int(space["generation"])):
            raise RuntimeError("当前 Session 的 prompt 队列非空，不能进入 Plan Mode")
        return state

    async def _resolve_profile_or_suggest(
        self,
        space: dict[str, Any],
        command: str,
        model: str,
        effort: str,
        *,
        prompt: str | None = None,
    ) -> object | None:
        try:
            return await self.bridge.resolve_model_profile(model, effort)
        except ValueError:
            suggestion = await self._profile_suggestion(command, model, effort, prompt=prompt)
            await self._send_space(
                space,
                f"模型或 effort 无效。你可能想发送：{inline_code(suggestion)}",
            )
            return None

    async def _profile_suggestion(
        self,
        command: str,
        model: str,
        effort: str,
        *,
        prompt: str | None = None,
    ) -> str:
        options = await self._model_options()
        if not options:
            return f"/{command} <model> | <effort>"
        by_model = {str(_value(option, "model")): option for option in options}
        aliases: dict[str, str] = {}
        for candidate in by_model:
            aliases[candidate.casefold()] = candidate
            aliases[candidate.rsplit("-", 1)[-1].casefold()] = candidate
        requested = model.casefold()
        selected_model = aliases.get(requested)
        if selected_model is None:
            match = difflib.get_close_matches(requested, list(aliases), n=1, cutoff=0.25)
            selected_model = aliases[match[0]] if match else next(iter(by_model))
        option = by_model[selected_model]
        efforts = [str(value) for value in (_value(option, "supported_efforts", ()) or ())]
        selected_effort = effort if effort in efforts else ""
        if not selected_effort and efforts:
            effort_match = difflib.get_close_matches(effort, efforts, n=1, cutoff=0.25)
            selected_effort = (
                effort_match[0] if effort_match else str(_value(option, "default_effort") or efforts[0])
            )
        suggestion = f"/{command} {selected_model} | {selected_effort or '<effort>'}"
        if command == "planmode" and prompt:
            suggestion += f" | {prompt}"
        return suggestion

    async def _announce_model_change(self, space: object, profile: object) -> None:
        current_mode = str(_value(space, "current_mode", "normal"))
        label = "Plan Mode" if current_mode == "plan" else "Normal Mode"
        space_id = str(_value(space, "space_id"))
        current = self.store.get_space(space_id)
        if current is None:
            raise RuntimeError("Session 状态已变化")
        await self.dashboards.schedule_space(space_id, immediate=True)
        await self._send_space(
            current,
            f"{label} 的模型已更新为 {inline_code(str(_value(profile, 'model')))} · "
            f"{inline_code(str(_value(profile, 'effort')))}。\n"
            "当前 turn 不变，后续 turn 使用新配置。",
        )

    async def _edit_selection_message(
        self,
        space: dict[str, Any],
        message_id: int | None,
        markdown: str,
        *,
        plain: str,
        chat_id: int | None = None,
    ) -> None:
        if message_id is None:
            return
        try:
            await self.discussion.edit_text(
                int(chat_id if chat_id is not None else space["discussion_chat_id"]),
                message_id,
                markdown,
                plain=plain,
                reply_markup=None,
                priority=5,
            )
        except TelegramError:
            LOGGER.warning(
                "event=profile_choice_cleanup_failed space_id=%s message_id=%s",
                str(space["space_id"])[:12],
                message_id,
            )

    def _replace_interaction(
        self,
        space: dict[str, Any],
        *,
        kind: str,
        phase: str,
        payload: dict[str, Any],
        expires_at: int,
    ) -> object:
        owner = self.store.get_owner()
        if owner is None:
            raise RuntimeError("owner 配对已失效")
        scope_key = self._interaction_scope(space, owner.user_id)
        draft = self.store.replace_interaction(
            scope_key,
            kind=kind,
            phase=phase,
            payload=payload,
            user_id=owner.user_id,
            bot_role=DISCUSSION_ROLE,
            chat_id=int(space["discussion_chat_id"]),
            expires_at=expires_at,
            space_id=str(space["space_id"]),
            generation=int(space["generation"]),
        )
        self._schedule_interaction_timeout(draft)
        return draft

    @staticmethod
    def _interaction_scope(space: dict[str, Any], user_id: int) -> str:
        return (
            f"discussion:{int(space['discussion_chat_id'])}:{space['space_id']}:"
            f"{int(space['generation'])}:{user_id}"
        )

    def _interaction_button(
        self,
        label: str,
        action: str,
        draft: object,
        payload: dict[str, Any],
        space: dict[str, Any],
    ) -> InlineKeyboardButton:
        return self._button(
            label,
            action,
            {
                **payload,
                "scope_key": str(_value(draft, "scope_key")),
                "flow_id": str(_value(draft, "flow_id")),
                "revision": int(_value(draft, "revision")),
            },
            space,
        )

    def _current_draft(self, payload: dict[str, Any], *, phase: str) -> object:
        scope_key = str(payload.get("scope_key") or "")
        draft = self.store.get_interaction(scope_key)
        if (
            draft is None
            or str(_value(draft, "flow_id")) != str(payload.get("flow_id") or "")
            or int(_value(draft, "revision")) != int(payload.get("revision") or 0)
            or str(_value(draft, "phase")) != phase
            or int(_value(draft, "expires_at", 0)) <= int(time.time())
        ):
            raise RuntimeError("这次交互已被新命令替换或已经过期")
        return draft

    def _schedule_interaction_timeout(self, draft: object) -> None:
        scope_key = str(_value(draft, "scope_key"))
        self._cancel_interaction_timeout(scope_key)
        coroutine = self._expire_interaction(
            scope_key,
            str(_value(draft, "flow_id")),
            int(_value(draft, "revision")),
            int(_value(draft, "expires_at")),
        )
        if self._application is not None:
            task = self._application.create_task(
                coroutine,
                name=f"codex-tg-interaction-{str(_value(draft, 'flow_id'))[:8]}",
            )
        else:
            task = asyncio.create_task(
                coroutine,
                name=f"codex-tg-interaction-{str(_value(draft, 'flow_id'))[:8]}",
            )
        self._interaction_tasks[scope_key] = task
        task.add_done_callback(
            lambda completed, key=scope_key: self._interaction_timeout_done(key, completed)
        )

    def _cancel_interaction_timeout(self, scope_key: str) -> None:
        task = self._interaction_tasks.pop(scope_key, None)
        if task is not None and task is not asyncio.current_task():
            task.cancel()

    def _interaction_timeout_done(self, scope_key: str, task: asyncio.Task[Any]) -> None:
        if self._interaction_tasks.get(scope_key) is task:
            self._interaction_tasks.pop(scope_key, None)

    async def _expire_interaction(self, scope_key: str, flow_id: str, revision: int, expires_at: int) -> None:
        await asyncio.sleep(max(0, expires_at - int(time.time())))
        draft = self.store.get_interaction(scope_key)
        if (
            draft is None
            or str(_value(draft, "flow_id")) != flow_id
            or int(_value(draft, "revision")) != revision
        ):
            return
        now = int(time.time())
        if int(_value(draft, "expires_at")) > now:
            self._schedule_interaction_timeout(draft)
            return
        claimed = self.store.claim_interaction(scope_key, flow_id, revision)
        if claimed is None:
            return
        space = self.store.get_space(str(_value(claimed, "space_id") or ""))
        if (
            space is None
            or int(space.get("generation") or 0) != int(_value(claimed, "generation", 0))
            or space.get("lifecycle") == "closed"
        ):
            return
        if str(_value(claimed, "kind")) == "planmode" and str(_value(claimed, "phase")) == "await_prompt":
            message = "30 秒内未收到 prompt，本次 Plan Mode 切换已取消。"
        else:
            message = "模型选择交互已过期，请重新执行命令。"
        await self._send_space(space, message)

    async def _recover_interactions(self) -> None:
        list_interactions = getattr(self.store, "list_interactions", None)
        if list_interactions is None:
            return
        for draft in list_interactions():
            if str(_value(draft, "bot_role")) != DISCUSSION_ROLE:
                continue
            space = self.store.get_space(str(_value(draft, "space_id") or ""))
            if (
                space is None
                or int(space.get("generation") or 0) != int(_value(draft, "generation", 0))
                or space.get("lifecycle") == "closed"
            ):
                self.store.delete_interaction(str(_value(draft, "scope_key")))
                continue
            self._schedule_interaction_timeout(draft)

    async def plan(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        space = self._require_space(update)
        state = await self._state(space)
        lines = [f"*🧭 Plan · {escape(clip(state.title, 70))}*"]
        if not state.plan:
            lines.append("尚未创建计划。")
        for index, step in enumerate(state.plan[:40], 1):
            marker = {
                "completed": "✅",
                "inProgress": "▶",
                "blocked": "⏸",
                "failed": "❌",
            }.get(step.status, "○")
            text = escape(clip(step.step, 220))
            lines.append(f"{marker} {index}\\. {text}")
        await self._send_space(space, "\n".join(lines))

    async def timeline(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        space = self._require_space(update)
        state = await self._state(space)
        events = self.store.timeline(state.thread_id, 20)
        lines = [f"*🕒 Timeline · {escape(clip(state.title, 70))}*"]
        if not events:
            lines.append("尚无事件。")
        for event in reversed(events):
            clock = time.strftime("%H:%M:%S", time.localtime(int(event["created_at"])))
            lines.append(f"{inline_code(clock)} {escape(clip(str(event['kind']), 220))}")
        await self._send_space(space, "\n".join(lines))

    async def attach(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        space = self._require_active_unlocked(update)
        target = await self.bridge.attach(str(space["thread_id"]))
        await self._send_space(space, f"tmux 已就绪：{inline_code(target)}")

    async def getfile(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        space = self._require_active_unlocked(update)
        description = raw_arguments(update)
        if not description:
            await self._send_space(space, "用法：`/getfile <文件描述>`")
            return
        waiting = await self._send_space(
            space,
            "正在查找并校验文件，请稍候。",
            priority=5,
        )
        waiting_message_id = int(waiting.message_id)
        try:
            candidates = await self.bridge.resolve_files(str(space["thread_id"]), description)
        except TimeoutError:
            await self._edit_or_resend_getfile(
                space,
                waiting_message_id,
                "文件搜索超时，请缩小描述范围后重试。",
            )
            return
        except (OSError, PathPolicyError, RuntimeError, ValueError) as exc:
            await self._edit_or_resend_getfile(
                space,
                waiting_message_id,
                f"文件搜索失败：{escape(str(exc))}",
                plain=f"文件搜索失败：{str(exc)}",
            )
            return
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await asyncio.shield(
                    self.discussion.delete_message(
                        int(space["discussion_chat_id"]), waiting_message_id
                    )
                )
            raise
        if not await self._getfile_space_is_current(space, waiting_message_id):
            return
        if not candidates:
            await self._edit_or_resend_getfile(
                space,
                waiting_message_id,
                "没有找到符合描述且允许发送的文件。",
            )
            return
        await self._render_getfile_page(
            space,
            waiting_message_id,
            description,
            candidates,
            page=1,
        )

    async def _getfile_space_is_current(
        self, space: dict[str, Any], waiting_message_id: int
    ) -> bool:
        current = self.store.get_space(str(space["space_id"]))
        if (
            current is not None
            and current.get("lifecycle") == "active"
            and int(current["generation"]) == int(space["generation"])
        ):
            return True
        await self.discussion.delete_message(
            int(space["discussion_chat_id"]), waiting_message_id
        )
        return False

    async def _render_getfile_page(
        self,
        space: dict[str, Any],
        message_id: int | None,
        query: str,
        candidates: list[FileCandidate],
        *,
        page: int,
    ) -> None:
        if not candidates:
            markdown = "没有找到符合描述且允许发送的文件。"
            if message_id is None:
                await self._send_space(space, markdown)
            else:
                await self._edit_or_resend_getfile(space, message_id, markdown)
            return

        total_pages = (len(candidates) + _GETFILE_PAGE_SIZE - 1) // _GETFILE_PAGE_SIZE
        page = min(max(1, page), total_pages)
        start = (page - 1) * _GETFILE_PAGE_SIZE
        visible = candidates[start : start + _GETFILE_PAGE_SIZE]
        lines = [
            f"请选择要发送的文件：共 {len(candidates)} 个，第 {page}/{total_pages} 页。"
        ]
        file_buttons: list[InlineKeyboardButton] = []
        for local_index, candidate in enumerate(visible, 1):
            index = start + local_index
            lines.append(
                f"{index}\\. {inline_code(compact_path(str(candidate.path)), 120)} · "
                f"{inline_code(human_bytes(candidate.size))}"
            )
            file_buttons.append(
                self._button(
                    _CIRCLED_FILE_BUTTON_LABELS[local_index - 1],
                    "send_file",
                    {
                        "path": str(candidate.path),
                        "size": candidate.size,
                        "modified_at": candidate.modified_at,
                        "device": candidate.device,
                        "inode": candidate.inode,
                        "modified_ns": candidate.modified_ns,
                    },
                    space,
                )
            )

        rows = balanced_button_rows(file_buttons, columns=4)

        navigation: list[InlineKeyboardButton] = []
        if page > 1:
            navigation.append(
                self._button(
                    "上一页",
                    "getfile_page",
                    {"query": query, "page": page - 1},
                    space,
                )
            )
        if page < total_pages:
            navigation.append(
                self._button(
                    "下一页",
                    "getfile_page",
                    {"query": query, "page": page + 1},
                    space,
                )
            )
        if navigation:
            rows.append(navigation)

        markdown = "\n".join(lines)
        if message_id is None:
            await self._send_space(
                space,
                markdown,
                reply_markup=InlineKeyboardMarkup(rows),
            )
        else:
            await self._edit_or_resend_getfile(
                space,
                message_id,
                markdown,
                reply_markup=InlineKeyboardMarkup(rows),
            )

    async def _getfile_page(
        self,
        space: dict[str, Any],
        payload: dict[str, Any],
        *,
        message_id: int | None,
    ) -> None:
        query = str(payload["query"]).strip()
        page = int(payload.get("page") or 1)
        candidates = await self.bridge.resolve_files(str(space["thread_id"]), query)
        await self._render_getfile_page(space, message_id, query, candidates, page=page)

    async def _edit_or_resend_getfile(
        self,
        space: dict[str, Any],
        message_id: int,
        markdown: str,
        *,
        plain: str | None = None,
        reply_markup: InlineKeyboardMarkup | None = None,
    ) -> None:
        if not await self._getfile_space_is_current(space, message_id):
            return
        current = self.store.get_space(str(space["space_id"]))
        if current is None:
            return
        try:
            await self.discussion.edit_text(
                int(current["discussion_chat_id"]),
                message_id,
                markdown,
                plain=plain,
                reply_markup=reply_markup,
                priority=5,
            )
        except TelegramError:
            await self._send_space(
                current,
                markdown,
                plain=plain,
                reply_markup=reply_markup,
                priority=5,
            )
            await self.discussion.delete_message(
                int(current["discussion_chat_id"]), message_id
            )

    async def unwatch(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        space = self._require_space(update)
        await self._confirm_unwatch(space)

    async def _confirm_unwatch(self, space: dict[str, Any]) -> None:
        await self._send_space(
            space,
            "确认取消关注？评论历史会保留，但此评论串将永久只读。",
            reply_markup=InlineKeyboardMarkup(
                [
                    [self._button("确认取消关注", "unwatch_execute", {}, space)],
                    [self._button("返回", "unwatch_cancel", {}, space)],
                ]
            ),
        )

    async def upload(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        space = self._require_active_unlocked(update)
        message = update.effective_message
        if not message:
            return
        telegram_file: Any
        image = bool(message.photo)
        if image:
            telegram_file = message.photo[-1]
            file_name = f"telegram-{telegram_file.file_unique_id}.jpg"
        elif message.document:
            telegram_file = message.document
            file_name = message.document.file_name or f"telegram-{message.document.file_unique_id}.bin"
        else:
            return
        expected_size = int(telegram_file.file_size) if telegram_file.file_size is not None else None
        if expected_size is not None and expected_size > self.config.telegram_download_limit:
            await self._send_space(space, "文件超过 Telegram 入站大小限制。")
            return
        destination = prepare_inbox_path(
            self.config.inbox_dir,
            str(space["thread_id"]),
            file_name,
            expected_size=expected_size,
            download_limit=self.config.telegram_download_limit,
            quota_bytes=self.config.inbox_quota_bytes,
            minimum_free_bytes=self.config.minimum_free_bytes,
        )
        try:
            remote = await telegram_file.get_file()
            with BytesIO() as payload:
                await remote.download_to_memory(out=payload)
                payload.seek(0)
                destination.write_from(payload)
            path = destination.commit()
        except Exception:
            destination.abort()
            raise
        payload = {
            "path": str(path),
            "caption": (message.caption or "").strip(),
            "image": image,
            "client_message_id": f"telegram-upload-{update.update_id}-{telegram_file.file_unique_id}",
            "mode": "auto",
        }
        await self._send_space(
            space,
            f"已安全接收 {inline_code(path.name)} · {inline_code(human_bytes(path.stat().st_size))}",
            reply_markup=InlineKeyboardMarkup(
                [[self._button("确认发送给 Codex", "send_upload", payload, space)]]
            ),
        )

    async def callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        query = update.callback_query
        user = update.effective_user
        chat = self._chat_for_update(update)
        if not query or not user or not chat:
            return
        LOGGER.info(
            "event=telegram_callback_received update_id=%s chat_id=%s user_id=%s",
            getattr(update, "update_id", "unknown"),
            chat.id,
            user.id,
        )
        data = str(query.data or "")
        pending = (
            self.store.peek_callback(data[3:], user.id, bot_role=DISCUSSION_ROLE, chat_id=chat.id)
            if data.startswith("cb:")
            else None
        )
        if not pending:
            LOGGER.warning(
                "event=telegram_callback_rejected reason=missing_or_expired chat_id=%s data_prefix=%s",
                chat.id,
                data[:12],
            )
            await self.discussion.answer_callback(
                query, "按钮已使用或过期，请重新执行命令。", show_alert=True
            )
            return
        action, payload = pending
        space = self.store.get_space(str(payload.get("space_id") or ""))
        if not space or int(payload.get("generation") or 0) != int(space["generation"]):
            LOGGER.warning(
                "event=telegram_callback_rejected reason=stale_space action=%s chat_id=%s",
                action,
                chat.id,
            )
            await self.discussion.answer_callback(query, "Session 状态已变化。", show_alert=True)
            return
        if not self.security.is_space_unlocked(str(space["space_id"])):
            LOGGER.info(
                "event=telegram_callback_rejected reason=locked action=%s space_id=%s",
                action,
                str(space["space_id"])[:12],
            )
            await self.discussion.answer_callback(
                query,
                "写操作已锁定，请先发送 /totp <验证码>。认证后可再次点击原按钮。",
                show_alert=True,
            )
            await self.dashboards.schedule_space(str(space["space_id"]), immediate=True)
            LOGGER.info(
                "event=locked_callback_rejected space_id=%s action=%s",
                str(space["space_id"])[:12],
                action,
            )
            return
        if action in {"plan_execute", "plan_continue"}:
            try:
                self._ensure_latest_plan(space, payload)
            except RuntimeError as exc:
                await self.discussion.answer_callback(query, str(exc), show_alert=True)
                return
        if not self._workloads.can_submit():
            await self.discussion.answer_callback(
                query, "请求队列已满，请稍后重试。", show_alert=True
            )
            return
        consumed = self.store.consume_callback(
            data[3:],
            user.id,
            bot_role=DISCUSSION_ROLE,
            chat_id=chat.id,
            space_id=str(space["space_id"]),
            generation=int(space["generation"]),
        )
        if consumed is None:
            LOGGER.warning(
                "event=telegram_callback_rejected reason=consumed action=%s space_id=%s",
                action,
                str(space["space_id"])[:12],
            )
            await self.discussion.answer_callback(
                query, "按钮已使用或过期，请重新执行命令。", show_alert=True
            )
            return
        action, payload = consumed
        await self.discussion.answer_callback(query)
        callback_message = getattr(query, "message", None)
        callback_message_id = getattr(callback_message, "message_id", None)
        submitted = self._workloads.submit(
            f"space:{space['space_id']}:{space['generation']}",
            lambda: self._run_callback_action(
                action,
                payload,
                space,
                callback_message_id=(
                    int(callback_message_id) if callback_message_id is not None else None
                ),
                callback_chat_id=int(chat.id),
            ),
            space=_callback_workload_space(action),
        )
        if not submitted:
            await self._send_space(space, "请求队列已满，请重新执行命令。")
        elif self._application is None:
            await self._workloads.join()

    async def _run_callback_action(
        self,
        action: str,
        payload: dict[str, Any],
        space: dict[str, Any],
        *,
        callback_message_id: int | None,
        callback_chat_id: int,
    ) -> None:
        try:
            if action in {"plan_execute", "plan_continue"}:
                self._ensure_latest_plan(space, payload)
                await self._ensure_plan_ready(space)
                self._ensure_latest_plan(space, payload)
            await self._dispatch_callback(
                action,
                payload,
                space,
                callback_message_id=(
                    int(callback_message_id) if callback_message_id is not None else None
                ),
                callback_chat_id=callback_chat_id,
            )
        except (KeyError, ValueError, RuntimeError, OSError, TelegramError, PathPolicyError) as exc:
            LOGGER.warning(
                "event=telegram_callback_failed action=%s space_id=%s error_type=%s error=%s",
                action,
                str(space["space_id"])[:12],
                type(exc).__name__,
                str(exc)[:240],
            )
            latest = self.store.latest_plan_publication(str(space["space_id"]), int(space["generation"]))
            if (
                action in {"plan_execute", "plan_continue"}
                and latest is not None
                and str(latest.get("status") or "") == "published"
            ):
                await self._send_plan_action_retry(space, latest, str(exc))
            else:
                await self._send_space(space, escape(str(exc)))

    async def _dispatch_callback(
        self,
        action: str,
        payload: dict[str, Any],
        space: dict[str, Any],
        *,
        callback_message_id: int | None = None,
        callback_chat_id: int | None = None,
    ) -> None:
        if action == "space_refresh":
            if space.get("thread_id"):
                await self.bridge.refresh(str(space["thread_id"]))
            await self.dashboards.schedule_space(str(space["space_id"]), immediate=True)
        elif action == "space_unwatch":
            await self._confirm_unwatch(space)
        elif action == "unwatch_cancel":
            await self._send_space(space, "已取消操作。")
        elif action == "unwatch_execute":
            await self.coordinator.close(str(space["space_id"]), int(space["generation"]))
        elif action == "prompt_mode":
            self._ensure_unlocked(space)
            await self._send_prompt(
                space,
                str(payload["prompt"]),
                str(payload["mode"]),
                client_message_id=str(payload.get("client_message_id") or "") or None,
                receipt_message_id=(
                    int(payload["receipt_message_id"])
                    if payload.get("receipt_message_id") is not None
                    else None
                ),
            )
        elif action == "queue_cancel":
            self._ensure_unlocked(space)
            cancelled = self.store.cancel_space_prompt(
                str(space["space_id"]), int(payload["queue_id"]), int(space["generation"])
            )
            await self._send_space(space, "已取消队列项。" if cancelled else "队列项已变化。")
        elif action == "profile_model":
            self._ensure_unlocked(space)
            await self._profile_model_selected(
                space,
                payload,
                message_id=callback_message_id,
                chat_id=callback_chat_id,
            )
        elif action == "profile_effort":
            self._ensure_unlocked(space)
            await self._profile_effort_selected(
                space,
                payload,
                message_id=callback_message_id,
                chat_id=callback_chat_id,
            )
        elif action == "profile_cancel":
            self._ensure_unlocked(space)
            await self._cancel_profile_interaction(
                space,
                payload,
                message_id=callback_message_id,
                chat_id=callback_chat_id,
            )
        elif action == "send_file":
            self._ensure_unlocked(space)
            await self._send_file(space, payload)
        elif action == "getfile_page":
            self._ensure_unlocked(space)
            await self._getfile_page(space, payload, message_id=callback_message_id)
        elif action == "send_upload":
            self._ensure_unlocked(space)
            await self._send_upload(space, payload)
        elif action == "question":
            self._ensure_unlocked(space)
            await self._record_question_answer(space, payload)
        elif action == "command_approval":
            self._ensure_unlocked(space)
            await self._answer_command_approval(
                space,
                payload,
                message_id=callback_message_id,
                chat_id=callback_chat_id,
            )
        elif action in {"question_custom", "question_clarify"}:
            self._ensure_unlocked(space)
            await self._begin_question_reply(
                space,
                payload,
                clarification=action == "question_clarify",
            )
        elif action == "plan_execute":
            self._ensure_unlocked(space)
            await self._execute_plan(
                space,
                payload,
                callback_message_id=callback_message_id,
                callback_chat_id=callback_chat_id,
            )
        elif action == "plan_continue":
            self._ensure_unlocked(space)
            await self._begin_plan_revision(
                space,
                payload,
                callback_message_id=callback_message_id,
                callback_chat_id=callback_chat_id,
            )

    async def _send_file(self, space: dict[str, Any], payload: dict[str, Any]) -> None:
        candidate = FileCandidate(
            path=Path(str(payload["path"])),
            size=int(payload["size"]),
            modified_at=int(payload["modified_at"]),
            device=int(payload.get("device") or 0),
            inode=int(payload.get("inode") or 0),
            modified_ns=int(payload.get("modified_ns") or 0),
        )
        with self.bridge.path_policy.open_outbound(candidate) as handle:
            message = await self.discussion.send_document(
                int(space["discussion_chat_id"]),
                handle,
                filename=candidate.path.name,
                caption=f"{candidate.path.name} · {human_bytes(candidate.size)}",
                reply_parameters=ReplyParameters(message_id=int(space["discussion_root_id"])),
            )
        self.store.record_discussion_message(
            int(space["discussion_chat_id"]),
            int(message.message_id),
            int(space["discussion_root_id"]),
            str(space["space_id"]),
        )

    async def _send_upload(self, space: dict[str, Any], payload: dict[str, Any]) -> None:
        result = await self.bridge.send_space_upload(
            str(space["space_id"]),
            Path(str(payload["path"])),
            str(payload.get("caption") or ""),
            mode=str(payload.get("mode") or "auto"),
            image=bool(payload.get("image")),
            client_message_id=str(payload["client_message_id"]),
        )
        if result == "choose":
            await self._send_space(
                space,
                "当前 turn 正在运行，请选择文件投递方式：",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [self._button("BTW", "send_upload", {**payload, "mode": "steer"}, space)],
                        [self._button("Queue", "send_upload", {**payload, "mode": "queue"}, space)],
                    ]
                ),
            )
            return
        labels = {
            "started": "文件已交给 Codex。",
            "steered": "文件已插入当前 turn。",
            "queued": "文件已加入队列。",
        }
        await self._send_space(space, labels.get(result, escape(result)))

    async def forward_notice(self, message: str, thread_id: str | None) -> None:
        space = self.store.get_space_by_thread(thread_id) if thread_id else None
        if space:
            await self._send_space(space, escape(message), priority=5)
            return
        owner = self.store.get_owner()
        if owner:
            await self.control.send_text(owner.chat_id, escape(message), priority=5)

    async def plan_completed(
        self,
        thread_id: str,
        turn_id: str,
        item_id: str,
        text: str,
    ) -> None:
        space = self.store.get_space_by_thread(thread_id)
        if not space or space.get("lifecycle") != "active" or str(space.get("thread_id") or "") != thread_id:
            LOGGER.info(
                "event=plan_publish_skipped reason=no_active_space thread_id=%s item_id=%s",
                thread_id[:8],
                item_id[:8],
            )
            return
        space_id = str(space["space_id"])
        generation = int(space["generation"])
        revision_key = plan_revision_key(turn_id, text)
        if not self.store.claim_plan_publication(
            space_id=space_id,
            generation=generation,
            item_id=item_id,
            revision_key=revision_key,
            thread_id=thread_id,
            turn_id=turn_id,
            plan_text=text,
        ):
            LOGGER.info(
                "event=plan_publish_deduplicated space_id=%s item_id=%s",
                space_id[:12],
                item_id[:8],
            )
            return
        self.store.retire_stale_plan_callbacks(
            space_id, generation, item_id, revision_key
        )
        await self._repair_superseded_plan_articles(space)

        chunks = render_commonmark_chunks(text, limit=3500)
        if not chunks:
            chunks = [TelegramHtmlChunk(html="<i>Plan 内容为空。</i>", plain="Plan 内容为空。")]
        rows = self._plan_action_markup(
            space,
            item_id,
            revision_key,
            thread_id,
            turn_id,
        )
        message_ids: list[int] = []
        try:
            for index, chunk in enumerate(chunks):
                prefix_html = "<b>📋 Codex Plan</b>\n\n" if index == 0 else ""
                prefix_plain = "📋 Codex Plan\n\n" if index == 0 else ""
                message = await self._send_space_html(
                    space,
                    prefix_html + chunk.html,
                    plain=prefix_plain + chunk.plain,
                    reply_markup=rows if index == len(chunks) - 1 else None,
                    priority=5,
                )
                message_ids.append(int(message.message_id))
        except Exception:
            self.store.finish_plan_publication(
                space_id=space_id,
                generation=generation,
                item_id=item_id,
                revision_key=revision_key,
                status="failed",
                message_ids=message_ids,
            )
            raise
        self.store.finish_plan_publication(
            space_id=space_id,
            generation=generation,
            item_id=item_id,
            revision_key=revision_key,
            status="published",
            message_ids=message_ids,
        )
        publication = self.store.latest_plan_publication(space_id, generation)
        if publication is not None and str(publication.get("status") or "") == "published":
            self._schedule_plan_prompt_monitor(space, publication)
        LOGGER.info(
            "event=plan_published space_id=%s item_id=%s chunks=%d",
            space_id[:12],
            item_id[:8],
            len(message_ids),
        )

    async def plan_turn_started(self, thread_id: str, turn_id: str) -> None:
        space = self.store.get_space_by_thread(thread_id)
        if not space or space.get("lifecycle") != "active" or not turn_id:
            return
        latest = self.store.latest_plan_publication(
            str(space["space_id"]), int(space["generation"])
        )
        if (
            latest is None
            or str(latest.get("turn_id") or "") == turn_id
        ):
            return
        changed = self.store.mark_external_plan_action(
            str(space["space_id"]),
            int(space["generation"]),
            str(latest["item_id"]),
            revision_key=str(latest.get("revision_key") or ""),
            status="executed",
            decision_turn_id=turn_id,
            expected_statuses={"published", "executing", "revising"},
        )
        current = self.store.latest_plan_publication(
            str(space["space_id"]), int(space["generation"])
        )
        if current is None or str(current.get("status") or "") != "executed":
            return
        await self._cancel_plan_prompt_monitor(current)
        await self._finalize_plan_ui(space, current, status="executed")
        if not changed:
            return
        LOGGER.info(
            "event=tui_plan_selection_reconciled space_id=%s turn_id=%s",
            str(space["space_id"])[:12],
            turn_id[:8],
        )

    @staticmethod
    def _plan_publication_key(publication: Mapping[str, Any]) -> str:
        return (
            f"{publication.get('space_id')}:{publication.get('generation')}:"
            f"{publication.get('item_id')}:{publication.get('revision_key')}"
        )

    def _schedule_plan_prompt_monitor(
        self, space: Mapping[str, Any], publication: Mapping[str, Any]
    ) -> None:
        key = self._plan_publication_key(publication)
        existing = self._plan_prompt_tasks.get(key)
        if existing is not None and not existing.done():
            return
        prefix = f"{space.get('space_id')}:{space.get('generation')}:"
        for old_key, old_task in list(self._plan_prompt_tasks.items()):
            if old_key.startswith(prefix) and old_key != key:
                old_task.cancel()
        task = asyncio.create_task(
            self._watch_plan_prompt(dict(space), dict(publication)),
            name=f"discussion-plan-prompt-{str(publication.get('item_id') or '')[:8]}",
        )
        self._plan_prompt_tasks[key] = task
        task.add_done_callback(
            lambda completed, plan_key=key: self._plan_prompt_monitor_done(
                plan_key, completed
            )
        )

    def _plan_prompt_monitor_done(self, key: str, task: asyncio.Task[None]) -> None:
        if self._plan_prompt_tasks.get(key) is task:
            self._plan_prompt_tasks.pop(key, None)
        exception = None if task.cancelled() else task.exception()
        if exception is not None:
            LOGGER.error(
                "event=plan_prompt_monitor_failed key=%s",
                key[:80],
                exc_info=(type(exception), exception, exception.__traceback__),
            )

    async def _cancel_plan_prompt_monitor(self, publication: Mapping[str, Any]) -> None:
        key = self._plan_publication_key(publication)
        task = self._plan_prompt_tasks.pop(key, None)
        if task is None or task is asyncio.current_task():
            return
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def _plan_prompt_visibility(self, thread_id: str) -> bool | None:
        tmux = getattr(self.bridge, "tmux", None)
        visible = getattr(tmux, "plan_prompt_visible", None)
        if not callable(visible):
            return None
        try:
            return await visible(thread_id)
        except Exception:
            LOGGER.warning(
                "event=plan_prompt_visibility_failed thread_id=%s",
                thread_id[:8],
                exc_info=True,
            )
            return None

    async def _watch_plan_prompt(
        self, space: dict[str, Any], publication: dict[str, Any]
    ) -> None:
        space_id = str(space["space_id"])
        generation = int(space["generation"])
        item_id = str(publication["item_id"])
        revision_key = str(publication.get("revision_key") or "")
        thread_id = str(publication["thread_id"])
        seen = publication.get("tui_prompt_seen_at") is not None
        started = time.monotonic()
        deadline = started + _PLAN_PROMPT_MONITOR_SECONDS
        while True:
            latest = self.store.latest_plan_publication(space_id, generation)
            if (
                latest is None
                or str(latest.get("item_id") or "") != item_id
                or str(latest.get("revision_key") or "") != revision_key
                or str(latest.get("status") or "") != "published"
            ):
                return
            now = time.monotonic()
            if now >= deadline:
                LOGGER.info(
                    "event=plan_prompt_monitor_expired space_id=%s item_id=%s",
                    space_id[:12],
                    item_id[:8],
                )
                return
            poll_seconds = (
                _PLAN_PROMPT_POLL_SECONDS
                if now - started < _PLAN_PROMPT_FAST_WINDOW_SECONDS
                else _PLAN_PROMPT_SLOW_POLL_SECONDS
            )
            visible = await self._plan_prompt_visibility(thread_id)
            if visible is True:
                if not seen:
                    if not self.store.mark_tui_plan_prompt_seen(
                        space_id,
                        generation,
                        item_id,
                        revision_key=revision_key,
                    ):
                        return
                    seen = True
                await asyncio.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))
                continue
            if visible is None or not seen:
                await asyncio.sleep(min(poll_seconds, max(0.0, deadline - time.monotonic())))
                continue

            latest = self.store.latest_plan_publication(space_id, generation)
            if latest is None or str(latest.get("status") or "") != "published":
                return
            client_message_id = self._plan_client_message_id(space, latest)
            gate = await self.bridge.wait_for_plan_decision_gate(
                space_id,
                generation,
                item_id,
                revision_key,
                client_message_id,
                timeout=1.0,
            )
            gate_status = str(gate.get("status") or "uncertain")
            if gate_status == "tui_approval_observed":
                approval_turn = str(gate.get("turn_id") or "")
                if approval_turn:
                    await self.plan_turn_started(thread_id, approval_turn)
                return
            if gate_status == "already_delivered":
                self.store.mark_external_plan_action(
                    space_id,
                    generation,
                    item_id,
                    revision_key=revision_key,
                    status="executed",
                    expected_statuses={"published", "executing"},
                )
                delivered = self.store.latest_plan_publication(space_id, generation)
                if delivered is not None:
                    await self._finalize_plan_ui(space, delivered, status="executed")
                return
            if gate_status != "safe_to_submit":
                return
            if not self.store.mark_external_plan_action(
                space_id,
                generation,
                item_id,
                revision_key=revision_key,
                status="dismissed",
                expected_statuses={"published"},
            ):
                return
            dismissed = self.store.latest_plan_publication(space_id, generation)
            if dismissed is not None:
                await self._delete_plan_messages(space, dismissed)
            LOGGER.info(
                "event=tui_plan_dismissed space_id=%s item_id=%s",
                space_id[:12],
                item_id[:8],
            )
            return

    @staticmethod
    def _render_plan_last_chunk(plan_text: str) -> tuple[str, str] | None:
        if not plan_text:
            return None
        chunks = render_commonmark_chunks(plan_text, limit=3500)
        if not chunks:
            chunks = [TelegramHtmlChunk(html="<i>Plan 内容为空。</i>", plain="Plan 内容为空。")]
        chunk = chunks[-1]
        if len(chunks) == 1:
            return f"<b>📋 Codex Plan</b>\n\n{chunk.html}", f"📋 Codex Plan\n\n{chunk.plain}"
        return chunk.html, chunk.plain

    @staticmethod
    def _plan_status_text(
        status: str, decision_turn_id: str = ""
    ) -> tuple[str, str]:
        labels = {
            "executing": (
                "⏳ <b>状态：</b>已在 Telegram 批准，正在启动执行。",
                "⏳ 状态：已在 Telegram 批准，正在启动执行。",
            ),
            "executed": ("✅ <b>状态：</b>已批准并开始执行。", "✅ 状态：已批准并开始执行。"),
            "revising": ("📝 <b>状态：</b>已选择继续完善计划。", "📝 状态：已选择继续完善计划。"),
            "revision_started": ("📝 <b>状态：</b>已提交继续完善请求。", "📝 状态：已提交继续完善请求。"),
            "superseded": ("↪ <b>状态：</b>已被更新版本替代。", "↪ 状态：已被更新版本替代。"),
        }
        html_status, plain_status = labels.get(
            status,
            ("<b>状态：</b>Plan 操作已结束。", "状态：Plan 操作已结束。"),
        )
        if decision_turn_id:
            short = html.escape(decision_turn_id[:8])
            html_status += f" <code>Turn {short}</code>"
            plain_status += f" Turn {decision_turn_id[:8]}"
        return html_status, plain_status

    async def _update_plan_article_status(
        self,
        space: Mapping[str, Any],
        publication: Mapping[str, Any],
        *,
        status: str,
        custom_status: tuple[str, str] | None = None,
    ) -> None:
        message_ids = publication.get("message_ids")
        if not isinstance(message_ids, list) or not message_ids:
            return
        message_id = int(message_ids[-1])
        rendered = self._render_plan_last_chunk(str(publication.get("plan_text") or ""))
        if rendered is None:
            await self._clear_callback_markup(dict(space), message_id)
            return
        html_body, plain_body = rendered
        html_status, plain_status = custom_status or self._plan_status_text(
            status, str(publication.get("decision_turn_id") or "")
        )
        try:
            await self.discussion.edit_text(
                int(space["discussion_chat_id"]),
                message_id,
                f"{html_body}\n\n{html_status}",
                plain=f"{plain_body}\n\n{plain_status}",
                parse_mode=ParseMode.HTML,
                reply_markup=None,
                priority=5,
            )
        except TelegramError:
            LOGGER.warning(
                "event=plan_article_status_update_failed space_id=%s message_id=%s",
                str(space["space_id"])[:12],
                message_id,
            )
            await self._clear_callback_markup(dict(space), message_id)

    async def _clear_plan_action_message_markups(
        self,
        space: Mapping[str, Any],
        publication: Mapping[str, Any],
        *,
        callback_message_id: int | None = None,
        callback_chat_id: int | None = None,
    ) -> None:
        original_ids = {
            int(value) for value in publication.get("message_ids") or []
        }
        action_ids = {
            int(value) for value in publication.get("action_message_ids") or []
        }
        if callback_message_id is not None and callback_message_id not in original_ids:
            action_ids.add(int(callback_message_id))
        for message_id in sorted(action_ids):
            await self._clear_callback_markup(
                dict(space),
                message_id,
                chat_id=callback_chat_id,
            )

    async def _finalize_plan_ui(
        self,
        space: Mapping[str, Any],
        publication: Mapping[str, Any],
        *,
        status: str,
        callback_message_id: int | None = None,
        callback_chat_id: int | None = None,
        custom_status: tuple[str, str] | None = None,
        retire_all_callbacks: bool = True,
    ) -> None:
        if retire_all_callbacks:
            self.store.retire_plan_callbacks(
                str(space["space_id"]), int(space["generation"])
            )
        await self._update_plan_article_status(
            space,
            publication,
            status=status,
            custom_status=custom_status,
        )
        await self._clear_plan_action_message_markups(
            space,
            publication,
            callback_message_id=callback_message_id,
            callback_chat_id=callback_chat_id,
        )

    async def _delete_plan_messages(
        self, space: Mapping[str, Any], publication: Mapping[str, Any]
    ) -> None:
        self.store.retire_plan_callbacks(
            str(space["space_id"]), int(space["generation"])
        )
        message_ids = {
            int(value) for value in publication.get("message_ids") or []
        } | {
            int(value) for value in publication.get("action_message_ids") or []
        }
        for message_id in sorted(message_ids):
            deleted = await self.discussion.delete_message(
                int(space["discussion_chat_id"]), message_id, priority=5
            )
            if not deleted:
                LOGGER.warning(
                    "event=plan_message_delete_failed space_id=%s message_id=%s",
                    str(space["space_id"])[:12],
                    message_id,
                )

    async def _repair_superseded_plan_articles(self, space: Mapping[str, Any]) -> None:
        latest = self.store.latest_plan_publication(
            str(space["space_id"]), int(space["generation"])
        )
        if latest is None:
            return
        self.store.retire_stale_plan_callbacks(
            str(space["space_id"]),
            int(space["generation"]),
            str(latest["item_id"]),
            str(latest.get("revision_key") or ""),
        )
        for publication in self.store.plan_publications_for_ui_repair():
            if (
                str(publication.get("space_id") or "") == str(space["space_id"])
                and int(publication.get("generation") or 0) == int(space["generation"])
                and str(publication.get("status") or "") == "superseded"
            ):
                await self._finalize_plan_ui(
                    space,
                    publication,
                    status="superseded",
                    retire_all_callbacks=False,
                )

    def _plan_action_markup(
        self,
        space: dict[str, Any],
        item_id: str,
        revision_key: str,
        thread_id: str,
        turn_id: str,
    ) -> InlineKeyboardMarkup:
        payload = {
            "item_id": item_id,
            "revision_key": revision_key,
            "thread_id": thread_id,
            "turn_id": turn_id,
        }
        return InlineKeyboardMarkup(
            [
                [
                    self._button(
                        "批准并执行",
                        "plan_execute",
                        payload,
                        space,
                        ttl_seconds=_PLAN_ACTION_SECONDS,
                    )
                ],
                [
                    self._button(
                        "继续完善计划",
                        "plan_continue",
                        payload,
                        space,
                        ttl_seconds=_PLAN_ACTION_SECONDS,
                    )
                ],
            ]
        )

    async def prompt_completed(self, run: dict[str, Any]) -> None:
        space_id = str(run.get("space_id") or "")
        space = self.store.get_space(space_id)
        if (
            not space
            or space.get("lifecycle") != "active"
            or int(space.get("generation") or 0) != int(run.get("generation") or 0)
            or str(space.get("thread_id") or "") != str(run.get("thread_id") or "")
        ):
            LOGGER.info(
                "event=prompt_receipt_skipped reason=stale_space space_id=%s turn_id=%s",
                space_id[:12],
                str(run.get("turn_id") or "")[:8],
            )
            return
        status = str(run.get("status") or "completed")
        turn_id = str(run.get("turn_id") or "")
        receipt_state = "cancelled" if status == "interrupted" else status
        if receipt_state not in {"completed", "failed", "uncertain", "cancelled"}:
            receipt_state = "failed"
        client_message_id = str(run.get("client_message_id") or "")
        if client_message_id and await self._edit_prompt_receipt(
                space,
                client_message_id,
                receipt_state,
                detail=str(run.get("error_kind") or status),
            ):
            self._prompt_receipts.pop(client_message_id, None)
            return
        if status == "completed":
            message = "✅ `/prompt` 任务已完成。Codex 将继续此前工作；若无待处理指令则进入空闲。"
        elif status == "interrupted":
            message = "⏹ `/prompt` 任务已中断。"
        else:
            detail = str(run.get("error_kind") or status)
            message = f"❌ `/prompt` 任务失败：{inline_code(clip(detail, 160))}"
        if turn_id:
            message += f"\nTurn {inline_code(turn_id[:8])}"
        await self._send_space(space, message, priority=5)
        LOGGER.info(
            "event=prompt_receipt_sent space_id=%s turn_id=%s status=%s",
            space_id[:12],
            turn_id[:8],
            status,
        )

    def _ensure_latest_plan(
        self,
        space: dict[str, Any],
        payload: dict[str, Any],
        *,
        allowed_statuses: set[str] | None = None,
    ) -> dict[str, Any]:
        latest = self.store.latest_plan_publication(str(space["space_id"]), int(space["generation"]))
        if (
            latest is None
            or str(latest["item_id"]) != str(payload.get("item_id") or "")
            or str(latest.get("revision_key") or "") != str(payload.get("revision_key") or "")
            or str(latest["thread_id"]) != str(space.get("thread_id") or "")
            or str(payload.get("thread_id") or "") != str(space.get("thread_id") or "")
        ):
            raise RuntimeError("该 Plan 已过期，请使用最新 Plan 的按钮")
        expected = allowed_statuses or {"published"}
        if str(latest["status"]) not in expected:
            raise RuntimeError("该 Plan 操作已处理或已过期")
        return latest

    async def _execute_plan(
        self,
        space: dict[str, Any],
        payload: dict[str, Any],
        *,
        callback_message_id: int | None = None,
        callback_chat_id: int | None = None,
    ) -> None:
        latest = self._ensure_latest_plan(space, payload)
        await self._ensure_plan_ready(space)
        profile = await self._profile_for_mode(space, "default")
        client_message_id = self._plan_client_message_id(space, latest)
        if not self.store.mark_plan_action(
            str(space["space_id"]),
            int(space["generation"]),
            str(latest["item_id"]),
            revision_key=str(latest.get("revision_key") or ""),
            status="executing",
        ):
            raise RuntimeError("该 Plan 操作已处理或已过期")
        await self._cancel_plan_prompt_monitor(latest)
        await self._clear_plan_actions(
            space,
            latest,
            status="executing",
            callback_message_id=callback_message_id,
            callback_chat_id=callback_chat_id,
        )
        await self._dismiss_tmux_plan_prompt(str(space.get("thread_id") or ""))
        wait_for_gate = getattr(self.bridge, "wait_for_plan_decision_gate", None)
        gate = (
            await wait_for_gate(
                str(space["space_id"]),
                int(space["generation"]),
                str(latest["item_id"]),
                str(latest.get("revision_key") or ""),
                client_message_id,
                timeout=1.0,
            )
            if wait_for_gate is not None
            else {"status": "safe_to_submit"}
        )
        gate_status = str(gate.get("status") or "uncertain")
        if gate_status == "tui_approval_observed":
            approval_turn = str(gate.get("turn_id") or "")
            if approval_turn:
                await self.plan_turn_started(str(space.get("thread_id") or ""), approval_turn)
            return
        if gate_status == "already_delivered":
            self.store.complete_plan_action(
                str(space["space_id"]),
                int(space["generation"]),
                str(latest["item_id"]),
                revision_key=str(latest.get("revision_key") or ""),
                expected_status="executing",
                status="executed",
                decision_turn_id=str(gate.get("turn_id") or ""),
            )
            delivered = self.store.latest_plan_publication(
                str(space["space_id"]), int(space["generation"])
            )
            if delivered is not None:
                await self._finalize_plan_ui(space, delivered, status="executed")
            return
        if gate_status != "safe_to_submit":
            uncertain = self.store.latest_plan_publication(
                str(space["space_id"]), int(space["generation"])
            )
            if uncertain is not None:
                await self._update_plan_article_status(
                    space,
                    uncertain,
                    status="executing",
                    custom_status=(
                        "⚠ <b>状态：</b>批准请求送达状态待确认，已移除按钮以防重复执行。",
                        "⚠ 状态：批准请求送达状态待确认，已移除按钮以防重复执行。",
                    ),
                )
            return
        current = self.store.latest_plan_publication(
            str(space["space_id"]), int(space["generation"])
        )
        if current is None or str(current.get("status") or "") != "executing":
            return
        try:
            turn = await self._start_profiled_turn(
                space,
                "Implement the approved plan. Use goal to track and execute it end to end.",
                mode="default",
                client_message_id=client_message_id,
                profile=profile,
            )
        except Exception:
            reconcile = getattr(self.bridge, "reconcile_plan_execution", None)
            if reconcile is None:
                raise
            status = await reconcile(
                str(space["space_id"]),
                int(space["generation"]),
                str(latest["item_id"]),
                str(latest.get("revision_key") or ""),
                client_message_id,
            )
            if status == "delivered":
                self.store.complete_plan_action(
                    str(space["space_id"]),
                    int(space["generation"]),
                    str(latest["item_id"]),
                    revision_key=str(latest.get("revision_key") or ""),
                    expected_status="executing",
                    status="executed",
                )
                completed = self.store.latest_plan_publication(
                    str(space["space_id"]), int(space["generation"])
                )
                if completed is not None:
                    await self._finalize_plan_ui(space, completed, status="executed")
                return
            if status == "absent":
                self.store.release_plan_action(
                    str(space["space_id"]),
                    int(space["generation"]),
                    str(latest["item_id"]),
                    revision_key=str(latest.get("revision_key") or ""),
                    expected_status="executing",
                )
                released = self.store.latest_plan_publication(
                    str(space["space_id"]), int(space["generation"])
                )
                if released is not None:
                    await self._update_plan_article_status(
                        space,
                        released,
                        status="published",
                        custom_status=(
                            "⚠ <b>状态：</b>批准请求未送达；操作已转移到新消息。",
                            "⚠ 状态：批准请求未送达；操作已转移到新消息。",
                        ),
                    )
                await self._send_plan_action_retry(
                    space,
                    released or latest,
                    "批准请求未送达 Codex，请使用新按钮重试。",
                )
                return
            uncertain = self.store.latest_plan_publication(
                str(space["space_id"]), int(space["generation"])
            )
            if uncertain is not None:
                await self._update_plan_article_status(
                    space,
                    uncertain,
                    status="executing",
                    custom_status=(
                        "⚠ <b>状态：</b>批准请求送达状态待确认，已移除按钮以防重复执行。",
                        "⚠ 状态：批准请求送达状态待确认，已移除按钮以防重复执行。",
                    ),
                )
            return
        completed = self.store.complete_plan_action(
            str(space["space_id"]),
            int(space["generation"]),
            str(latest["item_id"]),
            revision_key=str(latest.get("revision_key") or ""),
            expected_status="executing",
            status="executed",
            decision_turn_id=str(turn.get("id") or ""),
        )
        current = self.store.latest_plan_publication(
            str(space["space_id"]), int(space["generation"])
        )
        if not completed and (
            current is None or str(current.get("status") or "") != "executed"
        ):
            raise RuntimeError("Plan 执行状态已变化，已阻止重复提交")
        if current is not None:
            await self._finalize_plan_ui(space, current, status="executed")

    @staticmethod
    def _plan_client_message_id(space: dict[str, Any], publication: Mapping[str, Any]) -> str:
        return (
            f"telegram-plan-execute-{space['space_id']}-{space['generation']}-"
            f"{publication['item_id']}-{publication.get('revision_key') or 'legacy'}"
        )

    @staticmethod
    def _plan_revision_client_message_id(space: Mapping[str, Any], publication: Mapping[str, Any]) -> str:
        return (
            f"telegram-plan-revise-{space['space_id']}-{space['generation']}-"
            f"{publication['item_id']}-{publication.get('revision_key') or 'legacy'}"
        )

    async def _profile_for_mode(self, space: Mapping[str, Any], mode: str) -> object | None:
        prefix = "plan" if mode == "plan" else "normal"
        model = str(space.get(f"{prefix}_model") or "")
        effort = str(space.get(f"{prefix}_effort") or "")
        if not model or not effort:
            return None
        return await self.bridge.resolve_model_profile(model, effort)

    async def _start_profiled_turn(
        self,
        space: dict[str, Any],
        prompt: str,
        *,
        mode: str,
        client_message_id: str,
        profile: object | None = None,
    ) -> dict[str, Any]:
        selected = profile if profile is not None else await self._profile_for_mode(space, mode)
        kwargs: dict[str, Any] = {
            "mode": mode,
            "client_message_id": client_message_id,
        }
        if selected is not None:
            kwargs["profile"] = selected
        return await self.bridge.start_space_collaboration_turn(
            str(space["space_id"]),
            prompt,
            **kwargs,
        )

    async def _send_plan_action_retry(
        self,
        space: dict[str, Any],
        publication: Mapping[str, Any],
        message: str,
    ) -> None:
        sent = await self._send_space(
            space,
            message,
            reply_markup=self._plan_action_markup(
                space,
                str(publication["item_id"]),
                str(publication.get("revision_key") or ""),
                str(publication["thread_id"]),
                str(publication["turn_id"]),
            ),
            priority=5,
        )
        self.store.append_plan_action_message(
            str(space["space_id"]),
            int(space["generation"]),
            str(publication["item_id"]),
            int(sent.message_id),
            revision_key=str(publication.get("revision_key") or ""),
        )

    async def _clear_plan_actions(
        self,
        space: dict[str, Any],
        publication: Mapping[str, Any],
        *,
        status: str,
        callback_message_id: int | None = None,
        callback_chat_id: int | None = None,
        custom_status: tuple[str, str] | None = None,
    ) -> None:
        current = self.store.latest_plan_publication(
            str(space["space_id"]), int(space["generation"])
        )
        target = current if current is not None else publication
        await self._finalize_plan_ui(
            space,
            target,
            status=status,
            callback_message_id=callback_message_id,
            callback_chat_id=callback_chat_id,
            custom_status=custom_status,
        )

    async def _clear_callback_markup(
        self,
        space: dict[str, Any],
        message_id: int | None,
        *,
        chat_id: int | None = None,
    ) -> None:
        if message_id is None:
            return
        try:
            await self.discussion.edit_reply_markup(
                int(chat_id if chat_id is not None else space["discussion_chat_id"]),
                message_id,
                reply_markup=None,
                priority=5,
            )
        except TelegramError:
            LOGGER.warning(
                "event=callback_button_cleanup_failed space_id=%s message_id=%s",
                str(space["space_id"])[:12],
                message_id,
            )

    async def _dismiss_tmux_plan_prompt(self, thread_id: str) -> None:
        if not thread_id:
            return
        tmux = getattr(self.bridge, "tmux", None)
        dismiss = getattr(tmux, "dismiss_plan_prompt", None)
        if not callable(dismiss):
            return
        try:
            dismissed = await dismiss(thread_id)
        except Exception:
            LOGGER.warning(
                "event=tmux_plan_prompt_cleanup_failed thread_id=%s",
                thread_id[:8],
                exc_info=True,
            )
            return
        if dismissed:
            LOGGER.info("event=tmux_plan_prompt_dismissed thread_id=%s", thread_id[:8])

    async def _repair_plan_publications(self) -> None:
        for publication in self.store.plan_publications_for_ui_repair():
            space = self.store.get_space(str(publication.get("space_id") or ""))
            if (
                space is None
                or space.get("lifecycle") != "active"
                or int(space.get("generation") or 0)
                != int(publication.get("generation") or 0)
            ):
                continue
            status = str(publication.get("status") or "")
            if status == "dismissed":
                await self._delete_plan_messages(space, publication)
                continue
            if status == "superseded":
                await self._finalize_plan_ui(
                    space,
                    publication,
                    status=status,
                    retire_all_callbacks=False,
                )
                continue
            if status in {"executed", "revision_started"}:
                await self._finalize_plan_ui(space, publication, status=status)
                continue
            if status == "published":
                approval_turn = self.store.find_tui_plan_approval_turn(
                    str(publication["thread_id"]),
                    after=int(publication.get("created_at") or 0),
                    prompt=TUI_PLAN_APPROVAL_PROMPT,
                )
                if approval_turn:
                    await self.plan_turn_started(
                        str(publication["thread_id"]), approval_turn
                    )
                else:
                    self._schedule_plan_prompt_monitor(space, publication)
                continue
            await self._update_plan_article_status(
                space, publication, status=status
            )

    async def _recover_plan_executions(self) -> None:
        publications = getattr(self.store, "recoverable_plan_publications", None)
        reconcile = getattr(self.bridge, "reconcile_plan_execution", None)
        if publications is None or reconcile is None:
            return
        for publication in publications():
            space = self.store.get_space(str(publication.get("space_id") or ""))
            if (
                space is None
                or space.get("lifecycle") != "active"
                or int(space.get("generation") or 0) != int(publication.get("generation") or 0)
            ):
                continue
            action_status = str(publication.get("status") or "")
            if action_status == "executing":
                client_message_id = self._plan_client_message_id(space, publication)
                terminal_status = "executed"
            else:
                client_message_id = self._plan_revision_client_message_id(space, publication)
                terminal_status = "revision_started"
            try:
                status = await reconcile(
                    str(space["space_id"]),
                    int(space["generation"]),
                    str(publication["item_id"]),
                    str(publication.get("revision_key") or ""),
                    client_message_id,
                )
            except Exception:
                LOGGER.exception(
                    "event=plan_execution_reconcile_failed space_id=%s item_id=%s",
                    str(space["space_id"])[:12],
                    str(publication["item_id"])[:8],
                )
                continue
            if status == "delivered":
                self.store.complete_plan_action(
                    str(space["space_id"]),
                    int(space["generation"]),
                    str(publication["item_id"]),
                    revision_key=str(publication.get("revision_key") or ""),
                    expected_status=action_status,
                    status=terminal_status,
                )
                completed = self.store.latest_plan_publication(
                    str(space["space_id"]), int(space["generation"])
                )
                if completed is not None:
                    await self._finalize_plan_ui(
                        space, completed, status=terminal_status
                    )
                continue
            if status == "absent":
                released = self.store.release_plan_action(
                    str(space["space_id"]),
                    int(space["generation"]),
                    str(publication["item_id"]),
                    revision_key=str(publication.get("revision_key") or ""),
                    expected_status=action_status,
                )
                if released:
                    current = self.store.latest_plan_publication(
                        str(space["space_id"]), int(space["generation"])
                    )
                    if current is not None:
                        await self._update_plan_article_status(
                            space,
                            current,
                            status="published",
                            custom_status=(
                                "⚠ <b>状态：</b>服务重启前的操作未送达；操作已转移到新消息。",
                                "⚠ 状态：服务重启前的操作未送达；操作已转移到新消息。",
                            ),
                        )
                    await self._send_plan_action_retry(
                        space,
                        current or publication,
                        "服务重启前的 Plan 操作没有送达 Codex，请使用新按钮重试。",
                    )
                continue
            current = self.store.latest_plan_publication(
                str(space["space_id"]), int(space["generation"])
            )
            if current is not None:
                await self._update_plan_article_status(
                    space,
                    current,
                    status=action_status,
                    custom_status=(
                        "⚠ <b>状态：</b>服务重启前的操作送达状态待确认，按钮保持禁用。",
                        "⚠ 状态：服务重启前的操作送达状态待确认，按钮保持禁用。",
                    ),
                )

    async def _begin_plan_revision(
        self,
        space: dict[str, Any],
        payload: dict[str, Any],
        *,
        callback_message_id: int | None = None,
        callback_chat_id: int | None = None,
    ) -> None:
        latest = self._ensure_latest_plan(space, payload)
        await self._profile_for_mode(space, "plan")
        owner = self.store.get_owner()
        if owner is None:
            raise RuntimeError("owner 配对已失效")
        if not self.store.mark_plan_action(
            str(space["space_id"]),
            int(space["generation"]),
            str(latest["item_id"]),
            revision_key=str(latest.get("revision_key") or ""),
            status="revising",
        ):
            raise RuntimeError("该 Plan 操作已处理或已过期")
        await self._cancel_plan_prompt_monitor(latest)
        await self._clear_plan_actions(
            space,
            latest,
            status="revising",
            callback_message_id=callback_message_id,
            callback_chat_id=callback_chat_id,
        )
        await self._dismiss_tmux_plan_prompt(str(space.get("thread_id") or ""))
        client_message_id = self._plan_revision_client_message_id(space, latest)
        gate = await self.bridge.wait_for_plan_decision_gate(
            str(space["space_id"]),
            int(space["generation"]),
            str(latest["item_id"]),
            str(latest.get("revision_key") or ""),
            client_message_id,
            timeout=1.0,
        )
        gate_status = str(gate.get("status") or "uncertain")
        if gate_status == "tui_approval_observed":
            approval_turn = str(gate.get("turn_id") or "")
            if approval_turn:
                await self.plan_turn_started(str(space.get("thread_id") or ""), approval_turn)
            return
        if gate_status == "already_delivered":
            self.store.complete_plan_action(
                str(space["space_id"]),
                int(space["generation"]),
                str(latest["item_id"]),
                revision_key=str(latest.get("revision_key") or ""),
                expected_status="revising",
                status="revision_started",
            )
            delivered = self.store.latest_plan_publication(
                str(space["space_id"]), int(space["generation"])
            )
            if delivered is not None:
                await self._finalize_plan_ui(space, delivered, status="revision_started")
            return
        if gate_status != "safe_to_submit":
            uncertain = self.store.latest_plan_publication(
                str(space["space_id"]), int(space["generation"])
            )
            if uncertain is not None:
                await self._update_plan_article_status(
                    space,
                    uncertain,
                    status="revising",
                    custom_status=(
                        "⚠ <b>状态：</b>修改请求送达状态待确认，已移除按钮以防重复提交。",
                        "⚠ 状态：修改请求送达状态待确认，已移除按钮以防重复提交。",
                    ),
                )
            return
        try:
            prompt = await self._send_space(
                space,
                "请回复这条消息，说明需要如何继续完善 Plan。",
                reply_markup=ForceReply(
                    selective=True,
                    input_field_placeholder="输入 Plan 修改意见",
                ),
                priority=5,
            )
            nonce = self._reply_nonce(int(space["discussion_chat_id"]), int(prompt.message_id))
            self.store.put_callback(
                nonce,
                "reply_plan_revision",
                {
                    "item_id": str(latest["item_id"]),
                    "revision_key": str(latest.get("revision_key") or ""),
                    "thread_id": str(latest["thread_id"]),
                    "turn_id": str(latest["turn_id"]),
                },
                owner.user_id,
                int(time.time()) + _PLAN_ACTION_SECONDS,
                bot_role=DISCUSSION_ROLE,
                chat_id=int(space["discussion_chat_id"]),
                space_id=str(space["space_id"]),
                generation=int(space["generation"]),
            )
        except Exception:
            self.store.release_plan_action(
                str(space["space_id"]),
                int(space["generation"]),
                str(latest["item_id"]),
                revision_key=str(latest.get("revision_key") or ""),
                expected_status="revising",
            )
            released = self.store.latest_plan_publication(
                str(space["space_id"]), int(space["generation"])
            )
            if released is not None:
                await self._update_plan_article_status(
                    space,
                    released,
                    status="published",
                    custom_status=(
                        "⚠ <b>状态：</b>无法创建修改请求；操作已转移到新消息。",
                        "⚠ 状态：无法创建修改请求；操作已转移到新消息。",
                    ),
                )
            await self._send_plan_action_retry(
                space,
                released or latest,
                "无法创建 Plan 修改请求，请使用新按钮重试。",
            )

    async def forward_command_approval(
        self,
        request_key: str,
        params: dict[str, Any],
        *,
        retry: bool = False,
    ) -> None:
        thread_id = str(params.get("threadId") or "")
        space = self.store.get_space_by_thread(thread_id)
        if not space or space.get("lifecycle") != "active":
            await self.forward_notice(
                "Codex 正在等待命令审批；当前 Session 没有可用的 Telegram Space，请在本机处理。",
                thread_id or None,
            )
            LOGGER.warning(
                "event=command_approval_unforwarded request_key=%s thread_id=%s",
                request_key,
                thread_id[:8],
            )
            return
        stored = self.store.get_pending_input(request_key)
        metadata = next(
            (
                value
                for value in (stored["questions"] if stored else [])
                if isinstance(value, dict)
                and value.get("_bridge_request_kind") in _APPROVAL_REQUEST_KINDS
            ),
            None,
        )
        method = str(metadata.get("_bridge_approval_method") or "") if metadata else ""
        raw_command = params.get("command")
        if isinstance(raw_command, list):
            command = shlex.join(str(value) for value in raw_command)
        else:
            command = str(raw_command or "未知命令")
        subject_label = "命令"
        if method == "item/permissions/requestApproval":
            requested = params.get("permissions") or params.get("requestedPermissions") or {}
            command = json.dumps(requested, ensure_ascii=False, sort_keys=True)
            subject_label = "权限"
        elif method in {"item/fileChange/requestApproval", "applyPatchApproval"}:
            command = str(params.get("grantRoot") or params.get("cwd") or "当前文件变更")
            subject_label = "变更范围"
        command = clip(command, 1600)
        cwd = clip(str(params.get("cwd") or "未知目录"), 500)
        reason = clip(str(params.get("reason") or "该命令需要额外权限。"), 500)
        if metadata and "_bridge_available_decisions" in metadata:
            raw_available = metadata.get("_bridge_available_decisions")
            available: list[ApprovalDecision] = raw_available if isinstance(raw_available, list) else []
        else:
            available = interactive_approval_decisions(method, params)
        ttl = max(self.config.callback_seconds, self.config.totp_unlock_seconds)
        buttons = [
            self._button(
                self._command_approval_button_label(decision),
                "command_approval",
                {"request_key": request_key, "decision": decision},
                space,
                ttl_seconds=ttl,
            )
            for decision in available
        ]
        markup = InlineKeyboardMarkup([[button] for button in buttons]) if buttons else None
        approval_kind = {
            "item/fileChange/requestApproval": "修改文件",
            "applyPatchApproval": "应用文件补丁",
            "item/permissions/requestApproval": "授予临时权限",
        }.get(method, "执行命令")
        heading = (
            f"*⚠️ Codex {approval_kind}审批需重试*"
            if retry
            else f"*⚠️ Codex 请求{approval_kind}*"
        )
        instruction = (
            "上一选择未送达 Codex，请使用下面的新按钮。"
            if retry
            else f"请确认是否允许本次{subject_label}。"
        )
        if not buttons:
            instruction = "该请求没有可由 Telegram 提交的决定，请在本机处理。"
        message = await self._send_space(
            space,
            "\n".join(
                [
                    heading,
                    f"Session {inline_code(thread_id[:8])}",
                    f"{subject_label}：{inline_code(command)}",
                    f"目录：{inline_code(cwd)}",
                    f"原因：{escape(reason)}",
                    instruction,
                ]
            ),
            reply_markup=markup,
            priority=5,
        )
        lock = self._question_locks.setdefault(request_key, asyncio.Lock())
        async with lock:
            resolved = request_key in self._resolved_questions
            if not resolved and self.store.get_pending_input(request_key):
                self.store.record_question_message(
                    request_key,
                    DISCUSSION_ROLE,
                    int(space["discussion_chat_id"]),
                    int(message.message_id),
                    message_kind="summary_anchor",
                )
            else:
                resolved = True
        if resolved:
            await self._delete_question_message(space, request_key, int(message.message_id))

    @staticmethod
    def _command_approval_button_label(decision: ApprovalDecision) -> str:
        if isinstance(decision, dict) and isinstance(decision.get("permissions"), dict):
            if not decision["permissions"]:
                return "拒绝权限"
            scope = str(decision.get("scope") or "turn")
            return "仅本 Turn 授权" if scope == "turn" else "本 Session 授权"
        kind = approval_decision_kind(decision)
        if kind == "accept":
            return "批准执行"
        if kind == "acceptForSession":
            return "本 Session 放行"
        if kind == "acceptWithExecpolicyAmendment":
            return "批准并应用命令规则"
        if kind == "applyNetworkPolicyAmendment":
            detail = decision.get("applyNetworkPolicyAmendment", {})
            amendment = detail.get("network_policy_amendment", {}) if isinstance(detail, dict) else {}
            return "应用网络允许规则" if amendment.get("action") == "allow" else "应用网络拒绝规则"
        if kind == "cancel":
            return "拒绝并中止 Turn"
        return "拒绝"

    async def forward_question(self, request_key: str, params: dict[str, Any]) -> None:
        thread_id = str(params.get("threadId") or "")
        space = self.store.get_space_by_thread(thread_id)
        questions = [value for value in params.get("questions") or [] if isinstance(value, dict)]
        if not space or not questions:
            return
        self._question_answers[request_key] = {}
        header = await self._send_space(
            space,
            f"*Codex 请求输入*\nSession {inline_code(thread_id[:8])}",
            priority=5,
        )
        lock = self._question_locks.setdefault(request_key, asyncio.Lock())
        async with lock:
            resolved = request_key in self._resolved_questions
            if not resolved and self.store.get_pending_input(request_key):
                self.store.record_question_message(
                    request_key,
                    DISCUSSION_ROLE,
                    int(space["discussion_chat_id"]),
                    int(header.message_id),
                    message_kind="summary_anchor",
                )
            else:
                resolved = True
        if resolved:
            await self._delete_question_message(space, request_key, int(header.message_id))
            return
        await self._present_question(space, request_key, 0)

    async def _present_question(self, space: dict[str, Any], request_key: str, index: int) -> None:
        stored = self.store.get_pending_input(request_key)
        if not stored:
            return
        questions = stored["questions"]
        if index >= len(questions):
            return
        question = questions[index]
        question_id = str(question.get("id") or f"question-{index + 1}")
        question_ttl = max(self.config.callback_seconds, self.config.totp_unlock_seconds)
        lines = [
            f"*{escape(question.get('header') or f'问题 {index + 1}')}*",
            escape(question.get("question") or "请选择"),
        ]
        rows: list[list[InlineKeyboardButton]] = []
        for option in question.get("options") or []:
            if not isinstance(option, dict) or not option.get("label"):
                continue
            label = str(option["label"])
            description = str(option.get("description") or "").strip()
            option_line = f"• {escape(label)}"
            if description:
                option_line += f" — {escape(description)}"
            lines.append(option_line)
            rows.append(
                [
                    self._button(
                        label[:50],
                        "question",
                        {
                            "request_key": request_key,
                            "question_id": question_id,
                            "answer": label,
                        },
                        space,
                        ttl_seconds=question_ttl,
                    )
                ]
            )
        reply_payload = {
            "request_key": request_key,
            "question_id": question_id,
        }
        rows.append(
            [
                self._button(
                    "✍️ 自定义回答",
                    "question_custom",
                    reply_payload,
                    space,
                    ttl_seconds=question_ttl,
                )
            ]
        )
        rows.append(
            [
                self._button(
                    "❓ 反问 Codex",
                    "question_clarify",
                    reply_payload,
                    space,
                    ttl_seconds=question_ttl,
                )
            ]
        )
        message = await self._send_space(
            space,
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(rows),
            priority=5,
        )
        lock = self._question_locks.setdefault(request_key, asyncio.Lock())
        async with lock:
            resolved = request_key in self._resolved_questions
            if not resolved and self.store.get_pending_input(request_key):
                self.store.record_question_message(
                    request_key,
                    DISCUSSION_ROLE,
                    int(space["discussion_chat_id"]),
                    int(message.message_id),
                )
            else:
                resolved = True
        if resolved:
            await self._delete_question_message(space, request_key, int(message.message_id))

    async def _begin_question_reply(
        self,
        space: dict[str, Any],
        payload: dict[str, Any],
        *,
        clarification: bool,
    ) -> None:
        request_key = str(payload["request_key"])
        question_id = str(payload["question_id"])
        question = self._pending_question(space, request_key, question_id)
        owner = self.store.get_owner()
        if owner is None:
            raise RuntimeError("owner 配对已失效")
        label = f"@{owner.username}" if owner.username else "owner"
        mention = f"[{escape(label)}](tg://user?id={owner.user_id})"
        question_text = clip(question.get("question") or "当前问题", 320)
        if clarification:
            markdown = (
                f"{mention} 请回复这条消息，输入你要向 Codex 反问的内容。\n原问题：{escape(question_text)}"
            )
            plain = f"{label} 请回复这条消息，输入你要向 Codex 反问的内容。\n原问题：{question_text}"
            placeholder = "输入要向 Codex 反问的问题"
            action = "reply_question_clarify"
        else:
            markdown = f"{mention} 请回复这条消息，输入你的自定义回答。\n问题：{escape(question_text)}"
            plain = f"{label} 请回复这条消息，输入你的自定义回答。\n问题：{question_text}"
            placeholder = "输入自定义回答"
            action = "reply_question_custom"
        prompt = await self._send_space(
            space,
            markdown,
            plain=plain,
            reply_markup=ForceReply(
                selective=True,
                input_field_placeholder=placeholder,
            ),
            priority=5,
        )
        prompt_message_id = int(prompt.message_id)
        lock = self._question_locks.setdefault(request_key, asyncio.Lock())
        async with lock:
            resolved = request_key in self._resolved_questions
            if not resolved and self.store.get_pending_input(request_key):
                nonce = self._reply_nonce(
                    int(space["discussion_chat_id"]),
                    prompt_message_id,
                )
                self.store.put_callback(
                    nonce,
                    action,
                    {
                        "request_key": request_key,
                        "question_id": question_id,
                    },
                    owner.user_id,
                    int(time.time()) + max(self.config.callback_seconds, self.config.totp_unlock_seconds),
                    bot_role=DISCUSSION_ROLE,
                    chat_id=int(space["discussion_chat_id"]),
                    space_id=str(space["space_id"]),
                    generation=int(space["generation"]),
                )
                self.store.record_question_message(
                    request_key,
                    DISCUSSION_ROLE,
                    int(space["discussion_chat_id"]),
                    prompt_message_id,
                )
            else:
                resolved = True
        if resolved:
            await self._delete_question_message(space, request_key, prompt_message_id)

    async def _consume_direct_question_reply(
        self,
        update: Update,
        space: dict[str, Any],
        user_id: int,
        answer: str,
    ) -> bool:
        message = update.effective_message
        if message is None or not answer:
            return False
        callbacks = self.store.live_question_reply_callbacks(
            user_id,
            bot_role=DISCUSSION_ROLE,
            chat_id=int(message.chat_id),
            space_id=str(space["space_id"]),
            generation=int(space["generation"]),
        )
        candidates: list[dict[str, Any]] = []
        for callback in callbacks:
            payload = callback["payload"]
            try:
                self._pending_question(
                    space,
                    str(payload["request_key"]),
                    str(payload["question_id"]),
                )
            except KeyError, RuntimeError:
                continue
            candidates.append(callback)
        if not candidates:
            return False
        if len(candidates) > 1:
            await self._send_space(
                space,
                "当前有多个问题等待文字输入，请回复对应的 Bot 提示消息以明确目标。",
            )
            return True
        if space.get("lifecycle") != "active" or not space.get("thread_id"):
            await self._send_space(space, "当前 Session 尚未激活。")
            return True
        try:
            self._ensure_unlocked(space)
        except RuntimeError as exc:
            await self._send_space(space, escape(str(exc)))
            return True
        constraints = {
            "bot_role": DISCUSSION_ROLE,
            "chat_id": int(message.chat_id),
            "space_id": str(space["space_id"]),
            "generation": int(space["generation"]),
        }
        selected = candidates[0]
        consumed = self.store.consume_callback(str(selected["nonce"]), user_id, **constraints)
        if consumed is None:
            await self._send_space(space, "这条回复请求已使用或过期。")
            return True
        action, payload = consumed
        try:
            if action == "reply_question_custom":
                await self._record_question_answer(
                    space,
                    {**payload, "answer": answer},
                )
            else:
                self._pending_question(
                    space,
                    str(payload["request_key"]),
                    str(payload["question_id"]),
                )
                await self._launch_ask(
                    space,
                    answer,
                    clarification=True,
                    update=update,
                )
        except RuntimeError as exc:
            await self._send_space(space, escape(str(exc)))
        return True

    async def reply_to_intent(
        self,
        update: Update,
        context: ContextTypes.DEFAULT_TYPE,
    ) -> None:
        del context
        message = update.effective_message
        user = update.effective_user
        if not message or not user:
            return
        space = self._space_for_message(message)
        if not space:
            return
        text = (message.text or "").strip()
        if command_name(update) in _KNOWN_COMMANDS:
            return
        reply = message.reply_to_message
        if text and not text.startswith("/") and reply is None:
            scope_key = self._interaction_scope(space, user.id)
            get_interaction = getattr(self.store, "get_interaction", None)
            draft = get_interaction(scope_key) if get_interaction is not None else None
            if (
                draft is not None
                and int(_value(draft, "user_id", 0)) == user.id
                and int(_value(draft, "chat_id", 0)) == int(message.chat_id)
                and str(_value(draft, "space_id") or "") == str(space["space_id"])
                and int(_value(draft, "generation", 0)) == int(space["generation"])
                and str(_value(draft, "kind")) == "planmode"
                and str(_value(draft, "phase")) == "await_prompt"
            ):
                try:
                    self._ensure_unlocked(space)
                    consumed = await self._consume_plan_prompt(space, draft, text)
                except RuntimeError as exc:
                    await self._send_space(space, escape(str(exc)))
                    raise ApplicationHandlerStop from None
                if consumed:
                    raise ApplicationHandlerStop
        if not reply:
            if await self._consume_direct_question_reply(update, space, user.id, text):
                raise ApplicationHandlerStop
            return
        nonce = self._reply_nonce(int(message.chat_id), int(reply.message_id))
        constraints = {
            "bot_role": DISCUSSION_ROLE,
            "chat_id": int(message.chat_id),
            "space_id": str(space["space_id"]),
            "generation": int(space["generation"]),
        }
        pending = self.store.peek_callback(nonce, user.id, **constraints)
        if pending is None or pending[0] not in {
            "reply_question_custom",
            "reply_question_clarify",
            "reply_plan_revision",
        }:
            return
        if space.get("lifecycle") != "active" or not space.get("thread_id"):
            await self._send_space(space, "当前 Session 尚未激活。")
            raise ApplicationHandlerStop
        try:
            self._ensure_unlocked(space)
        except RuntimeError as exc:
            await self._send_space(space, escape(str(exc)))
            raise ApplicationHandlerStop from None
        answer = (message.text or "").strip()
        if not answer:
            await self._send_space(space, "回复内容不能为空。")
            raise ApplicationHandlerStop
        if pending[0] == "reply_plan_revision":
            try:
                self._ensure_latest_plan(
                    space,
                    pending[1],
                    allowed_statuses={"revising"},
                )
                await self._ensure_plan_ready(space)
                self._ensure_latest_plan(
                    space,
                    pending[1],
                    allowed_statuses={"revising"},
                )
            except RuntimeError as exc:
                await self._send_space(space, escape(str(exc)))
                raise ApplicationHandlerStop from None
        consumed = self.store.consume_callback(nonce, user.id, **constraints)
        if consumed is None:
            await self._send_space(space, "这条回复请求已使用或过期。")
            raise ApplicationHandlerStop
        action, payload = consumed
        try:
            if action == "reply_question_custom":
                await self._record_question_answer(
                    space,
                    {**payload, "answer": answer},
                )
            elif action == "reply_question_clarify":
                self._pending_question(
                    space,
                    str(payload["request_key"]),
                    str(payload["question_id"]),
                )
                await self._launch_ask(
                    space,
                    answer,
                    clarification=True,
                    update=update,
                )
            else:
                publication = self._ensure_latest_plan(
                    space,
                    payload,
                    allowed_statuses={"revising"},
                )
                await self._ensure_plan_ready(space)
                client_message_id = self._plan_revision_client_message_id(space, publication)
                try:
                    turn = await self._start_profiled_turn(
                        space,
                        (
                            "Continue refining the current plan based on this feedback. "
                            "Do not implement it yet.\n\n"
                            f"{answer}"
                        ),
                        mode="plan",
                        client_message_id=client_message_id,
                    )
                except Exception:
                    reconcile = getattr(self.bridge, "reconcile_plan_execution", None)
                    status = (
                        await reconcile(
                            str(space["space_id"]),
                            int(space["generation"]),
                            str(publication["item_id"]),
                            str(publication.get("revision_key") or ""),
                            client_message_id,
                        )
                        if reconcile is not None
                        else "unknown"
                    )
                    if status == "delivered":
                        self.store.complete_plan_action(
                            str(space["space_id"]),
                            int(space["generation"]),
                            str(publication["item_id"]),
                            revision_key=str(publication.get("revision_key") or ""),
                            expected_status="revising",
                            status="revision_started",
                        )
                        current = self.store.latest_plan_publication(
                            str(space["space_id"]), int(space["generation"])
                        )
                        if current is not None:
                            await self._finalize_plan_ui(
                                space, current, status="revision_started"
                            )
                        raise ApplicationHandlerStop from None
                    if status == "absent":
                        self.store.release_plan_action(
                            str(space["space_id"]),
                            int(space["generation"]),
                            str(publication["item_id"]),
                            revision_key=str(publication.get("revision_key") or ""),
                            expected_status="revising",
                        )
                        current = self.store.latest_plan_publication(
                            str(space["space_id"]), int(space["generation"])
                        )
                        if current is not None:
                            await self._update_plan_article_status(
                                space,
                                current,
                                status="published",
                                custom_status=(
                                    "⚠ <b>状态：</b>修改意见未送达；操作已转移到新消息。",
                                    "⚠ 状态：修改意见未送达；操作已转移到新消息。",
                                ),
                            )
                        await self._send_plan_action_retry(
                            space,
                            current or publication,
                            "Plan 修改意见未送达 Codex，请使用新按钮重试。",
                        )
                        raise ApplicationHandlerStop from None
                    current = self.store.latest_plan_publication(
                        str(space["space_id"]), int(space["generation"])
                    )
                    if current is not None:
                        await self._update_plan_article_status(
                            space,
                            current,
                            status="revising",
                            custom_status=(
                                "⚠ <b>状态：</b>修改意见送达状态待确认，按钮保持禁用。",
                                "⚠ 状态：修改意见送达状态待确认，按钮保持禁用。",
                            ),
                        )
                    raise ApplicationHandlerStop from None
                completed = self.store.complete_plan_action(
                    str(space["space_id"]),
                    int(space["generation"]),
                    str(publication["item_id"]),
                    revision_key=str(publication.get("revision_key") or ""),
                    expected_status="revising",
                    status="revision_started",
                    decision_turn_id=str(turn.get("id") or ""),
                )
                if not completed:
                    raise RuntimeError("Plan 修改状态已变化，已阻止重复提交")
                current = self.store.latest_plan_publication(
                    str(space["space_id"]), int(space["generation"])
                )
                if current is not None:
                    await self._finalize_plan_ui(
                        space, current, status="revision_started"
                    )
        except RuntimeError as exc:
            if action == "reply_plan_revision":
                latest = self.store.latest_plan_publication(str(space["space_id"]), int(space["generation"]))
                if latest is not None and str(latest.get("status") or "") == "revising":
                    self.store.release_plan_action(
                        str(space["space_id"]),
                        int(space["generation"]),
                        str(latest["item_id"]),
                        revision_key=str(latest.get("revision_key") or ""),
                        expected_status="revising",
                    )
                    current = self.store.latest_plan_publication(
                        str(space["space_id"]), int(space["generation"])
                    )
                    if current is not None:
                        await self._update_plan_article_status(
                            space,
                            current,
                            status="published",
                            custom_status=(
                                "⚠ <b>状态：</b>修改请求失败；操作已转移到新消息。",
                                "⚠ 状态：修改请求失败；操作已转移到新消息。",
                            ),
                        )
                    await self._send_plan_action_retry(
                        space, current or latest, str(exc)
                    )
                    raise ApplicationHandlerStop from None
            await self._send_space(space, escape(str(exc)))
        raise ApplicationHandlerStop

    def _pending_question(
        self,
        space: dict[str, Any],
        request_key: str,
        question_id: str,
    ) -> dict[str, Any]:
        stored = self.store.get_pending_input(request_key)
        if not stored or str(stored["thread_id"]) != str(space.get("thread_id") or ""):
            raise RuntimeError("该问题已过期或不属于当前 Session")
        for index, question in enumerate(stored["questions"]):
            known_id = str(question.get("id") or f"question-{index + 1}")
            if known_id == question_id:
                return question
        raise RuntimeError("问题 ID 不匹配")

    @staticmethod
    def _reply_nonce(chat_id: int, message_id: int) -> str:
        return f"reply:{DISCUSSION_ROLE}:{chat_id}:{message_id}"

    async def answer(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        space = self._require_active_unlocked(update)
        raw = raw_arguments(update)
        left, separator, answer = raw.partition("|")
        identifiers = left.split()
        if not separator or len(identifiers) != 2 or not answer.strip():
            await self._send_space(space, "用法：`/answer <请求ID> <问题ID> | <回答>`")
            return
        await self._record_question_answer(
            space,
            {
                "request_key": identifiers[0],
                "question_id": identifiers[1],
                "answer": answer.strip(),
            },
        )

    async def _record_question_answer(self, space: dict[str, Any], payload: dict[str, Any]) -> None:
        request_key = str(payload["request_key"])
        question_id = str(payload["question_id"])
        answer = str(payload["answer"])
        stored = self.store.get_pending_input(request_key)
        if not stored or str(stored["thread_id"]) != str(space["thread_id"]):
            raise RuntimeError("该问题已过期或不属于当前 Session")
        questions = stored["questions"]
        known = [str(value.get("id") or f"question-{index + 1}") for index, value in enumerate(questions)]
        if question_id not in known:
            raise RuntimeError("问题 ID 不匹配")
        values = self._question_answers.setdefault(request_key, {})
        previous = values.get(question_id)
        values[question_id] = [answer]
        missing = next((index for index, value in enumerate(known) if value not in values), None)
        if missing is not None:
            await self._present_question(space, request_key, missing)
            return
        persisted = {key: list(value) for key, value in values.items()}
        if not self.store.save_question_resolution(
            request_key,
            persisted,
            source="telegram",
        ):
            if previous is None:
                values.pop(question_id, None)
            else:
                values[question_id] = previous
            raise RuntimeError("该问题已提交，不能更改答案")
        await self.bridge.answer_question(request_key, persisted)
        self._question_answers.pop(request_key, None)

    async def _answer_command_approval(
        self,
        space: dict[str, Any],
        payload: dict[str, Any],
        *,
        message_id: int | None = None,
        chat_id: int | None = None,
    ) -> None:
        request_key = str(payload.get("request_key") or "")
        decision = payload.get("decision")
        stored = self.store.get_pending_input(request_key)
        if not stored or str(stored["thread_id"]) != str(space["thread_id"]):
            raise RuntimeError("该命令审批已过期或不属于当前 Session")
        metadata = next(
            (
                value
                for value in stored["questions"]
                if isinstance(value, dict)
                and value.get("_bridge_request_kind") in _APPROVAL_REQUEST_KINDS
            ),
            None,
        )
        if metadata is None:
            raise RuntimeError("该请求不是可由 Telegram 处理的审批")
        method = str(metadata.get("_bridge_approval_method") or "")
        raw_available = metadata.get("_bridge_available_decisions")
        if "_bridge_available_decisions" in metadata:
            available = raw_available if isinstance(raw_available, list) else []
        else:
            raw_params = metadata.get("params")
            available = interactive_approval_decisions(
                method,
                raw_params if isinstance(raw_params, dict) else {},
            )
        if not interactive_approval_is_available(method, decision, available):
            raise ValueError("命令审批决定不在当前请求允许的选项中")
        if not self.store.save_question_resolution(
            request_key,
            {"decision": [decision]},
            source="telegram",
        ):
            raise RuntimeError("该命令审批已提交，不能重复处理")
        try:
            await self.bridge.answer_command_approval(request_key, decision)
        except Exception as exc:
            self.store.pop_question_resolution(request_key)
            retry_stored = self.store.get_pending_input(request_key)
            retry_params = metadata.get("params")
            if (
                retry_stored
                and str(retry_stored["thread_id"]) == str(space["thread_id"])
                and isinstance(retry_params, dict)
            ):
                LOGGER.warning(
                    "event=command_approval_response_retry request_key=%s error_type=%s error=%s",
                    request_key,
                    type(exc).__name__,
                    _redacted_error(exc),
                )
                await self.forward_command_approval(request_key, retry_params, retry=True)
                return
            raise RuntimeError("Codex 连接已经重建，原命令审批已失效") from exc
        self._track_task(
            asyncio.create_task(
                self._clear_callback_markup(
                    space,
                    message_id,
                    chat_id=chat_id,
                ),
                name=f"approval-cleanup:{request_key[:12]}",
            )
        )
        kind = approval_decision_kind(decision)
        if method == "item/permissions/requestApproval" and isinstance(decision, dict):
            permissions = decision.get("permissions")
            message = "已授予请求的权限。" if permissions else "已拒绝请求的权限。"
            await self._send_space(space, message, priority=5)
            return
        subject = (
            "文件变更"
            if method in {"item/fileChange/requestApproval", "applyPatchApproval"}
            else "命令执行"
        )
        labels = {
            "accept": f"已批准本次{subject}。",
            "acceptForSession": f"已批准本 Session 后续{subject}。",
            "acceptWithExecpolicyAmendment": f"已批准{subject}并应用命令规则。",
            "applyNetworkPolicyAmendment": "已提交网络策略决定。",
            "decline": f"已拒绝本次{subject}。",
            "cancel": f"已拒绝{subject}并中止当前 Turn。",
        }
        await self._send_space(space, labels.get(kind, "审批已提交。"), priority=5)

    async def question_resolved(self, request_key: str) -> None:
        self._track_task(
            asyncio.create_task(
                self._cleanup_question_resolution(request_key),
                name=f"question-resolved:{request_key[:12]}",
            )
        )

    async def _cleanup_question_resolution(self, request_key: str) -> None:
        lock = self._question_locks.setdefault(request_key, asyncio.Lock())
        async with lock:
            self._resolved_questions.pop(request_key, None)
            self._resolved_questions[request_key] = None
            while len(self._resolved_questions) > _MAX_RESOLVED_QUESTION_TOMBSTONES:
                self._resolved_questions.pop(next(iter(self._resolved_questions)))
            stored = self.store.get_pending_input(request_key)
            resolution = self.store.pop_question_resolution(request_key)
            messages = self.store.pop_question_messages(request_key)
            anchor = next(
                (
                    message
                    for message in reversed(messages)
                    if message["bot_role"] == DISCUSSION_ROLE and message["message_kind"] == "summary_anchor"
                ),
                None,
            )
            retained: tuple[str, int, int] | None = None
            if stored and anchor:
                space = self.store.get_space_by_thread(str(stored["thread_id"]))
                if space and int(space["discussion_chat_id"]) == int(anchor["chat_id"]):
                    summary_html, summary_plain = self._question_summary(
                        stored,
                        resolution,
                    )
                    try:
                        await self.discussion.edit_text(
                            int(anchor["chat_id"]),
                            int(anchor["message_id"]),
                            summary_html,
                            plain=summary_plain,
                            parse_mode=ParseMode.HTML,
                            priority=5,
                        )
                    except TelegramError:
                        LOGGER.warning(
                            "event=question_summary_edit_failed request_key=%s",
                            request_key[:16],
                        )
                    else:
                        retained = (
                            DISCUSSION_ROLE,
                            int(anchor["chat_id"]),
                            int(anchor["message_id"]),
                        )
            grouped: dict[tuple[str, int], list[int]] = {}
            for message in messages:
                key = (str(message["bot_role"]), int(message["chat_id"]))
                if retained == (key[0], key[1], int(message["message_id"])):
                    continue
                grouped.setdefault(key, []).append(int(message["message_id"]))
            delete_at = int(time.time())
            for (bot_role, chat_id), message_ids in grouped.items():
                self.deletions.schedule(
                    bot_role,
                    chat_id,
                    message_ids,
                    delete_at=delete_at,
                    group_key=f"question:{request_key}",
                )
            await self.deletions.flush()
            self._question_answers.pop(request_key, None)

    @staticmethod
    def _question_summary(
        stored: dict[str, Any],
        resolution: dict[str, Any] | None,
    ) -> tuple[str, str]:
        answers = resolution.get("answers") if resolution else {}
        source = str(resolution.get("source") or "terminal") if resolution else "terminal"
        metadata = next(
            (
                value
                for value in stored.get("questions") or []
                if isinstance(value, dict)
                and value.get("_bridge_request_kind") in _APPROVAL_REQUEST_KINDS
            ),
            None,
        )
        if metadata is not None:
            params = metadata.get("params") if isinstance(metadata.get("params"), dict) else {}
            method = str(metadata.get("_bridge_approval_method") or "")
            raw_subject = params.get("command")
            subject_label = "命令"
            approval_kind = "命令"
            if method == "item/permissions/requestApproval":
                requested = params.get("permissions") or params.get("requestedPermissions") or {}
                subject = json.dumps(requested, ensure_ascii=False, sort_keys=True)
                subject_label = "权限"
                approval_kind = "权限"
            elif method in {"item/fileChange/requestApproval", "applyPatchApproval"}:
                subject = str(params.get("grantRoot") or params.get("cwd") or "当前文件变更")
                subject_label = "变更范围"
                approval_kind = "文件变更"
            else:
                subject = (
                    shlex.join(str(value) for value in raw_subject)
                    if isinstance(raw_subject, list)
                    else str(raw_subject or "未知命令")
                )
            cwd = str(params.get("cwd") or "未知目录")
            reason = str(params.get("reason") or "该命令需要额外权限。")
            selected = answers.get("decision") if isinstance(answers, dict) else None
            decision = selected[0] if isinstance(selected, list) and selected else None
            decision_label = (
                DiscussionBotController._command_approval_button_label(decision)
                if decision is not None
                else "已处理"
            )
            source_label = "Telegram" if source == "telegram" else "终端"
            thread_id = str(stored.get("thread_id") or "")
            html_lines = [f"<b>Codex {approval_kind}审批 · 已处理</b>"]
            plain_lines = [f"Codex {approval_kind}审批 · 已处理"]
            if thread_id:
                html_lines.append(f"Session <code>{html.escape(thread_id[:8])}</code>")
                plain_lines.append(f"Session {thread_id[:8]}")
            html_lines.extend(
                [
                    f"{subject_label}：<code>{html.escape(clip(subject, 1600))}</code>",
                    f"目录：<code>{html.escape(clip(cwd, 500))}</code>",
                    f"原因：{html.escape(clip(reason, 500))}",
                    f"<b>决定：</b>{html.escape(decision_label)}",
                    f"<i>来源：{source_label}</i>",
                ]
            )
            plain_lines.extend(
                [
                    f"{subject_label}：{clip(subject, 1600)}",
                    f"目录：{clip(cwd, 500)}",
                    f"原因：{clip(reason, 500)}",
                    f"决定：{decision_label}",
                    f"来源：{source_label}",
                ]
            )
            return "\n".join(html_lines), "\n".join(plain_lines)
        html_lines = ["<b>Codex 请求输入 · 已处理</b>"]
        plain_lines = ["Codex 请求输入 · 已处理"]
        thread_id = str(stored.get("thread_id") or "")
        if thread_id:
            html_lines.append(f"Session <code>{html.escape(thread_id[:8])}</code>")
            plain_lines.append(f"Session {thread_id[:8]}")
        for index, question in enumerate(stored.get("questions") or []):
            if not isinstance(question, dict):
                continue
            question_id = str(question.get("id") or f"question-{index + 1}")
            header = clip(str(question.get("header") or f"问题 {index + 1}"), 120)
            question_text = clip(str(question.get("question") or "请选择"), 360)
            html_lines.extend(
                [
                    "",
                    f"<b>{html.escape(header)}</b>",
                    html.escape(question_text),
                ]
            )
            plain_lines.extend(["", header, question_text])
            for option in (question.get("options") or [])[:6]:
                if not isinstance(option, dict) or not option.get("label"):
                    continue
                label = clip(str(option["label"]), 100)
                description = clip(str(option.get("description") or "").strip(), 180)
                suffix = f" — {description}" if description else ""
                html_lines.append(f"• {html.escape(label + suffix)}")
                plain_lines.append(f"• {label}{suffix}")
            selected = answers.get(question_id) if isinstance(answers, dict) else None
            if isinstance(selected, list) and selected:
                selected_text = clip("; ".join(str(value) for value in selected), 360)
                html_lines.append(f"<b>选择：</b>{html.escape(selected_text)}")
                plain_lines.append(f"选择：{selected_text}")
            else:
                fallback = "已在终端处理；具体答案不可用"
                html_lines.append(f"<b>选择：</b>{fallback}")
                plain_lines.append(f"选择：{fallback}")
        source_label = "Telegram" if source == "telegram" else "终端"
        html_lines.extend(["", f"<i>来源：{source_label}</i>"])
        plain_lines.extend(["", f"来源：{source_label}"])
        return "\n".join(html_lines), "\n".join(plain_lines)

    async def _delete_question_message(
        self,
        space: dict[str, Any],
        request_key: str,
        message_id: int,
    ) -> None:
        await self.deletions.delete_now(
            DISCUSSION_ROLE,
            int(space["discussion_chat_id"]),
            [message_id],
            group_key=f"question:{request_key}",
        )

    async def _send_space(
        self,
        space: dict[str, Any],
        markdown: str,
        *,
        plain: str | None = None,
        reply_markup: InlineKeyboardMarkup | ForceReply | None = None,
        priority: int = 10,
    ) -> Any:
        message = await self.discussion.send_text(
            int(space["discussion_chat_id"]),
            markdown,
            plain=plain,
            reply_markup=reply_markup,
            reply_parameters=ReplyParameters(message_id=int(space["discussion_root_id"])),
            priority=priority,
        )
        self.store.record_discussion_message(
            int(space["discussion_chat_id"]),
            int(message.message_id),
            int(space["discussion_root_id"]),
            str(space["space_id"]),
        )
        return message

    async def _send_space_html(
        self,
        space: dict[str, Any],
        html_text: str,
        *,
        plain: str,
        reply_markup: InlineKeyboardMarkup | None = None,
        priority: int = 10,
    ) -> Any:
        message = await self.discussion.send_text(
            int(space["discussion_chat_id"]),
            html_text,
            plain=plain,
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup,
            reply_parameters=ReplyParameters(message_id=int(space["discussion_root_id"])),
            priority=priority,
        )
        self.store.record_discussion_message(
            int(space["discussion_chat_id"]),
            int(message.message_id),
            int(space["discussion_root_id"]),
            str(space["space_id"]),
        )
        return message

    async def _send_unscoped(
        self,
        chat_id: int,
        markdown: str,
        *,
        plain: str | None = None,
    ) -> Any:
        return await self.discussion.send_text(chat_id, markdown, plain=plain)

    def _space_for_message(self, message: Any | None) -> dict[str, Any] | None:
        if message is None:
            return None
        root_id = int(message.message_thread_id) if message.message_thread_id else None
        reply = message.reply_to_message
        if root_id is None and reply:
            root_id = int(reply.message_thread_id) if reply.message_thread_id else None
            if root_id is None and reply.is_automatic_forward:
                root_id = int(reply.message_id)
            if root_id is None:
                mapped = self.store.resolve_discussion_root(message.chat_id, reply.message_id)
                root_id = int(mapped["root_message_id"]) if mapped else None
        if root_id is None:
            mapped = self.store.resolve_discussion_root(message.chat_id, message.message_id)
            root_id = int(mapped["root_message_id"]) if mapped else None
        return self.store.get_space_by_root(message.chat_id, root_id) if root_id else None

    def _require_space(self, update: Update) -> dict[str, Any]:
        space = self._space_for_message(update.effective_message)
        if not space:
            raise RuntimeError("该消息不在已绑定的 Session 评论串中")
        return space

    def _require_active_unlocked(self, update: Update) -> dict[str, Any]:
        space = self._require_space(update)
        if space.get("lifecycle") != "active" or not space.get("thread_id"):
            raise RuntimeError("当前 Session 尚未激活")
        self._ensure_unlocked(space)
        return space

    def _ensure_unlocked(self, space: dict[str, Any]) -> None:
        if not self.security.is_space_unlocked(str(space["space_id"])):
            raise RuntimeError("写操作已锁定，请先在当前评论串发送 /totp <验证码>")

    async def _state(self, space: dict[str, Any]) -> Any:
        if not space.get("thread_id"):
            raise RuntimeError("当前 Session 尚未创建")
        return await self.bridge.refresh(str(space["thread_id"]))

    def _track_task(self, task: asyncio.Task[Any]) -> None:
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _schedule_status_refresh(self, space: dict[str, Any]) -> None:
        space_id = str(space["space_id"])
        current = self._status_refresh_tasks.get(space_id)
        if current is not None and not current.done():
            return
        task = asyncio.create_task(
            self._refresh_status(space_id, int(space["generation"])),
            name=f"discussion-status-refresh:{space_id[:8]}",
        )
        self._status_refresh_tasks[space_id] = task
        self._track_task(task)

    async def _refresh_status(self, space_id: str, generation: int) -> None:
        delay = max(
            0.0,
            self._status_refreshed_at.get(space_id, 0.0)
            + _STATUS_REFRESH_SECONDS
            - time.monotonic(),
        )
        if delay:
            await asyncio.sleep(delay)
        try:
            current = self.store.get_space(space_id)
            if (
                current is None
                or int(current.get("generation") or 0) != generation
                or not current.get("thread_id")
            ):
                return
            await self.bridge.refresh(str(current["thread_id"]))
            self._status_refreshed_at[space_id] = time.monotonic()
            await self.dashboards.schedule_space(space_id, immediate=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.warning("Unable to refresh /status snapshot", exc_info=True)
        finally:
            if self._status_refresh_tasks.get(space_id) is asyncio.current_task():
                self._status_refresh_tasks.pop(space_id, None)

    async def _model_options(self) -> list[Any]:
        return await self.bridge.list_model_options()

    def _button(
        self,
        label: str,
        action: str,
        payload: dict[str, Any],
        space: dict[str, Any],
        *,
        ttl_seconds: int | None = None,
    ) -> InlineKeyboardButton:
        owner = self.store.get_owner()
        nonce = secrets.token_urlsafe(12)
        generation = int(space.get("generation") or 1)
        context = {
            **payload,
            "space_id": str(space["space_id"]),
            "generation": generation,
        }
        self.store.put_callback(
            nonce,
            action,
            context,
            owner.user_id if owner else 0,
            int(time.time()) + (self.config.callback_seconds if ttl_seconds is None else max(1, ttl_seconds)),
            bot_role=DISCUSSION_ROLE,
            chat_id=int(space["discussion_chat_id"]),
            space_id=str(space["space_id"]),
            generation=generation,
        )
        return InlineKeyboardButton(label, callback_data=f"cb:{nonce}")

    @staticmethod
    def _extra_rights(member: Any) -> list[str]:
        names = {
            "can_manage_topics": "管理话题",
            "can_restrict_members": "封禁成员",
            "can_promote_members": "添加管理员",
            "can_pin_messages": "置顶消息",
            "can_manage_video_chats": "管理视频聊天",
            "can_change_info": "修改群信息",
        }
        return [label for name, label in names.items() if bool(getattr(member, name, False))]

    async def error(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        error = context.error
        message = getattr(update, "effective_message", None)
        chat = getattr(update, "effective_chat", None)
        space = None
        command = ""
        if message is not None:
            with contextlib.suppress(Exception):
                space = self._space_for_message(message)
            with contextlib.suppress(Exception):
                command = command_name(update)  # type: ignore[arg-type]
        LOGGER.error(
            "event=discussion_handler_failed error_type=%s error=%r "
            "update_id=%s chat_id=%s command=%s space_id=%s",
            type(error).__name__,
            _redacted_error(error),
            getattr(update, "update_id", None),
            getattr(chat, "id", None),
            command or "none",
            str(space.get("space_id") or "")[:12] if space else "none",
        )
        if not isinstance(update, Update):
            return
        if not space:
            user = update.effective_user
            owner = self.store.get_owner()
            if (
                chat
                and user
                and owner
                and user.id == owner.user_id
                and command_name(update) == "/bind"
                and isinstance(error, RuntimeError)
            ):
                detail = clip(str(error).strip() or "绑定校验失败", 500)
                with contextlib.suppress(TelegramError, RuntimeError):
                    await self._send_unscoped(
                        chat.id,
                        f"绑定失败：{escape(detail)}",
                        plain=f"绑定失败：{detail}",
                    )
            return
        with contextlib.suppress(TelegramError, RuntimeError):
            await self._send_space(space, "处理指令时发生错误；详情已写入本机日志。")
