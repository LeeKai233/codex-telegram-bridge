from __future__ import annotations

import asyncio
import contextlib
import difflib
import html
import logging
import re
import secrets
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

from .bridge import Bridge
from .config import Config
from .deletions import MessageDeletionManager
from .files import FileCandidate, PathPolicyError, prepare_inbox_path
from .markdown import clip, compact_path, escape, inline_code
from .rich_text import TelegramHtmlChunk, render_commonmark_chunks
from .security import SecurityManager
from .space_coordinator import SessionSpaceCoordinator
from .space_dashboard import SpaceDashboardManager
from .store import Store
from .telegram_common import (
    DISCUSSION_ROLE,
    TelegramEndpoint,
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

LOGGER = logging.getLogger(__name__)

_MAX_RESOLVED_QUESTION_TOMBSTONES = 512
_LOCKED_COMMAND_ALLOWLIST = {"/totp", "/help", "/lock"}
_PLAN_ACTION_SECONDS = 24 * 60 * 60
_INTERACTION_SECONDS = 5 * 60
_PROMPT_WAIT_SECONDS = 30
_BOT_URL_TOKEN = re.compile(r"(https://api\.telegram\.org/bot)[^/\s]+", re.IGNORECASE)
_BOT_TOKEN = re.compile(r"\b\d{6,}:[A-Za-z0-9_-]{16,}\b")


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
        self._plan_recovery_done = False

    def install(self, application: Application) -> None:
        self._application = application
        application.add_handler(TypeHandler(Update, self._guard), group=-100)
        application.add_handler(MessageHandler(filters.ALL, self.observe_message), group=-50)
        application.add_handler(MessageHandler(filters.TEXT, self.reply_to_intent), group=-25)
        for command, callback in (
            ("bind", self.bind),
            ("help", self.help),
            ("status", self.status),
            ("totp", self.totp),
            ("lock", self.lock),
            ("prompt", self.prompt),
            ("ask", self.ask),
            ("queue", self.queue),
            ("planmode", self.planmode),
            ("changemodel", self.changemodel),
            ("plan", self.plan),
            ("timeline", self.timeline),
            ("attach", self.attach),
            ("getfile", self.getfile),
            ("unwatch", self.unwatch),
            ("answer", self.answer),
        ):
            application.add_handler(CommandHandler(command, callback))
        application.add_handler(
            MessageHandler(
                filters.TEXT
                & filters.Regex(r"(?i)^/bind(?:@[a-z0-9_]+)?(?:\s|$)"),
                self._bind_text_fallback,
            )
        )
        application.add_handler(CallbackQueryHandler(self.callback, pattern=r"^cb:"))
        application.add_handler(
            MessageHandler(filters.Document.ALL | filters.PHOTO, self.upload), group=1
        )
        application.add_error_handler(self.error)
        self.bridge.on_question = self.forward_question
        self.bridge.on_notice = self.forward_notice
        self.bridge.on_question_resolved = self.question_resolved
        self.bridge.on_plan_completed = self.plan_completed
        self.bridge.on_prompt_completed = self.prompt_completed

    async def stop(self) -> None:
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
        await self.discussion.bot.set_my_commands(
            [BotCommand("bind", "绑定频道讨论组"), BotCommand("help", "显示帮助")],
            scope=BotCommandScopeAllGroupChats(),
        )
        binding = self.store.get_telegram_binding()
        owner = self.store.get_owner()
        if binding and owner:
            await self.discussion.bot.set_my_commands(
                [BotCommand(command, description) for command, description in _SESSION_COMMANDS],
                scope=BotCommandScopeChatMember(
                    chat_id=int(binding["discussion_chat_id"]), user_id=owner.user_id
                ),
            )
        await self._recover_interactions()
        if not self._plan_recovery_done:
            self._plan_recovery_done = True
            await self._recover_plan_executions()

    async def _guard(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat = update.effective_chat
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
        group = await self.discussion.bot.get_chat(chat.id)
        channel_id = getattr(group, "linked_chat_id", None)
        if group.type != ChatType.SUPERGROUP or not channel_id:
            raise RuntimeError("当前群不是已关联频道的讨论超级群")
        if bool(getattr(group, "is_forum", False)):
            raise RuntimeError("讨论组启用了 Forum Topics，请先关闭 Topics")
        if getattr(group, "username", None):
            raise RuntimeError("讨论组必须保持私有")
        channel = await self.control.bot.get_chat(int(channel_id))
        if channel.type != ChatType.CHANNEL or getattr(channel, "username", None):
            raise RuntimeError("关联频道必须保持私有")
        if int(getattr(channel, "linked_chat_id", 0) or 0) != chat.id:
            raise RuntimeError("频道与讨论组的 linked_chat_id 不是双向一致")
        control_me, discussion_me = await asyncio.gather(
            self.control.bot.get_me(), self.discussion.bot.get_me()
        )
        control_member, discussion_member = await asyncio.gather(
            self.control.bot.get_chat_member(int(channel_id), control_me.id),
            self.discussion.bot.get_chat_member(chat.id, discussion_me.id),
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
        owner_member = await self.discussion.bot.get_chat_member(chat.id, owner.user_id)
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
            plain += (
                f"\n安全提示：{self.config.discussion_bot_label}"
                f" 仍有额外管理员权限：{extra_rights_text}"
            )
        await self._send_unscoped(chat.id, text, plain=plain)

    async def _bind_text_fallback(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
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
        if space.get("thread_id"):
            await self.bridge.refresh(str(space["thread_id"]))
        await self.dashboards.schedule_space(str(space["space_id"]), immediate=True)
        link = self.coordinator.status_link(space)
        await self._send_space(
            space,
            "实时状态已刷新。",
            reply_markup=(
                InlineKeyboardMarkup([[InlineKeyboardButton("打开状态", url=link)]]) if link else None
            ),
        )

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
            rendered = render_ask_error(ask_id, "等待 Codex 回答超时（180 秒）。")
        except asyncio.CancelledError:
            try:
                self._schedule_ask_deletion(
                    int(space["discussion_chat_id"]), waiting_message_id, ask_id
                )
                await asyncio.shield(self.deletions.flush())
            except (asyncio.CancelledError, Exception):
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
            await self.discussion.delete_message(
                int(space["discussion_chat_id"]), message_id
            )
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
        entries = self.store.space_queue_entries(
            str(space["space_id"]), int(space["generation"])
        )
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

    async def _send_prompt(self, space: dict[str, Any], prompt: str, mode: str) -> None:
        result = await self.bridge.send_space_prompt(
            str(space["space_id"]),
            prompt,
            mode=mode,
            client_message_id=f"telegram-{space['space_id']}-{uuid.uuid4()}",
        )
        if result == "choose":
            await self._send_space(
                space,
                "当前 turn 正在运行，请选择投递方式：",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            self._button(
                                "BTW · 当前 turn",
                                "prompt_mode",
                                {"prompt": prompt, "mode": "steer"},
                                space,
                            ),
                            self._button(
                                "Queue · 稍后",
                                "prompt_mode",
                                {"prompt": prompt, "mode": "queue"},
                                space,
                            ),
                        ]
                    ]
                ),
            )
            return
        labels = {
            "started": "已开始执行。",
            "steered": "已注入当前 turn。",
            "queued": "已加入队列。",
        }
        await self._send_space(space, labels.get(result, escape(result)))

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
                "用法：`/planmode <model> | <effort> [ | <prompt> ]`；"
                "全不带参数可进入交互模式。",
            )
            return
        profile = await self._resolve_profile_or_suggest(
            space, "planmode", parts[0], parts[1]
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
        profile = await self._resolve_profile_or_suggest(
            space, "changemodel", parts[0], parts[1]
        )
        if profile is None:
            return
        updated = await self.bridge.change_space_model(
            str(space["space_id"]),
            str(_value(profile, "model")),
            str(_value(profile, "effort")),
        )
        await self._announce_model_change(updated, profile)

    async def _begin_profile_interaction(
        self, space: dict[str, Any], kind: str
    ) -> None:
        options = await self.bridge.list_model_options()
        if not options:
            raise RuntimeError("Codex 当前没有返回可用模型")
        draft = self._replace_interaction(
            space,
            kind=kind,
            phase="select_model",
            payload={},
            expires_at=int(time.time()) + _INTERACTION_SECONDS,
        )
        rows = [
            [
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
            ]
            for option in options
        ]
        label = "Plan Mode" if kind == "planmode" else "当前模式"
        await self._send_space(
            space,
            f"请选择 {label} 使用的模型：",
            reply_markup=InlineKeyboardMarkup(rows),
        )

    async def _profile_model_selected(
        self, space: dict[str, Any], payload: dict[str, Any]
    ) -> None:
        draft = self._current_draft(payload, phase="select_model")
        model = str(payload.get("model") or "")
        option = next(
            (
                candidate
                for candidate in await self.bridge.list_model_options()
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
        rows = [
            [
                self._interaction_button(
                    effort,
                    "profile_effort",
                    advanced,
                    {"model": model, "effort": effort},
                    space,
                )
            ]
            for effort in efforts
        ]
        await self._send_space(
            space,
            f"模型 {inline_code(model)} 支持以下 effort：",
            reply_markup=InlineKeyboardMarkup(rows),
        )

    async def _profile_effort_selected(
        self, space: dict[str, Any], payload: dict[str, Any]
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
        if kind == "changemodel":
            updated = await self.bridge.change_space_model(
                str(space["space_id"]),
                str(_value(profile, "model")),
                str(_value(profile, "effort")),
            )
            claimed = self.store.claim_interaction(
                str(_value(draft, "scope_key")),
                str(_value(draft, "flow_id")),
                int(_value(draft, "revision")),
            )
            if claimed is None:
                raise RuntimeError("这次交互已被新命令替换")
            self._cancel_interaction_timeout(str(_value(draft, "scope_key")))
            await self._announce_model_change(updated, profile)
            return
        await self.bridge.set_space_profile(
            str(space["space_id"]),
            "plan",
            str(_value(profile, "model")),
            str(_value(profile, "effort")),
        )
        await self._advance_to_prompt_wait(draft, profile, space)

    async def _wait_for_plan_prompt(
        self, space: dict[str, Any], profile: object
    ) -> None:
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

    async def _advance_to_prompt_wait(
        self, draft: object, profile: object, space: dict[str, Any]
    ) -> None:
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
        self._schedule_interaction_timeout(advanced)
        await self._send_plan_prompt_request(space, advanced)

    async def _send_plan_prompt_request(
        self, space: dict[str, Any], draft: object
    ) -> None:
        del draft
        await self._send_space(
            space,
            "请在 30 秒内发送进入 Plan Mode 后的第一条 prompt。"
            "超时将取消本次模式切换。",
        )

    async def _consume_plan_prompt(
        self, space: dict[str, Any], draft: object, prompt: str
    ) -> bool:
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

    async def _start_plan_mode(
        self, space: dict[str, Any], prompt: str, profile: object
    ) -> None:
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
    ) -> object | None:
        try:
            return await self.bridge.resolve_model_profile(model, effort)
        except ValueError:
            suggestion = await self._profile_suggestion(command, model, effort)
            await self._send_space(
                space,
                f"模型或 effort 无效。你可能想发送：{inline_code(suggestion)}",
            )
            return None

    async def _profile_suggestion(self, command: str, model: str, effort: str) -> str:
        options = await self.bridge.list_model_options()
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
                effort_match[0]
                if effort_match
                else str(_value(option, "default_effort") or efforts[0])
            )
        return f"/{command} {selected_model} | {selected_effort or '<effort>'}"

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

    def _interaction_timeout_done(
        self, scope_key: str, task: asyncio.Task[Any]
    ) -> None:
        if self._interaction_tasks.get(scope_key) is task:
            self._interaction_tasks.pop(scope_key, None)

    async def _expire_interaction(
        self, scope_key: str, flow_id: str, revision: int, expires_at: int
    ) -> None:
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
        if str(_value(claimed, "kind")) == "planmode" and str(
            _value(claimed, "phase")
        ) == "await_prompt":
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
        candidates = await self.bridge.resolve_files(str(space["thread_id"]), description)
        if not candidates:
            await self._send_space(space, "没有找到符合描述且允许发送的文件。")
            return
        lines = ["请选择要发送的文件："]
        rows: list[list[InlineKeyboardButton]] = []
        for index, candidate in enumerate(candidates[:8], 1):
            lines.append(
                f"{index}\\. {inline_code(compact_path(str(candidate.path)), 120)} · "
                f"{inline_code(human_bytes(candidate.size))}"
            )
            rows.append(
                [
                    self._button(
                        f"发送 {index}. {candidate.path.name[:32]}",
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
                ]
            )
        await self._send_space(space, "\n".join(lines), reply_markup=InlineKeyboardMarkup(rows))

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
                    [
                        self._button("确认取消关注", "unwatch_execute", {}, space),
                        self._button("返回", "unwatch_cancel", {}, space),
                    ]
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
        chat = update.effective_chat
        if not query or not user or not chat:
            return
        data = str(query.data or "")
        pending = self.store.peek_callback(
            data[3:], user.id, bot_role=DISCUSSION_ROLE, chat_id=chat.id
        ) if data.startswith("cb:") else None
        if not pending:
            await self.discussion.answer_callback(
                query, "按钮已使用或过期，请重新执行命令。", show_alert=True
            )
            return
        action, payload = pending
        space = self.store.get_space(str(payload.get("space_id") or ""))
        if not space or int(payload.get("generation") or 0) != int(space["generation"]):
            await self.discussion.answer_callback(query, "Session 状态已变化。", show_alert=True)
            return
        if not self.security.is_space_unlocked(str(space["space_id"])):
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
                state = self.store.get_thread(str(space.get("thread_id") or ""))
                if state and (state.status == "active" or state.turn_status == "inProgress"):
                    raise RuntimeError("当前 turn 正在运行，请稍后重试")
            except RuntimeError as exc:
                await self.discussion.answer_callback(query, str(exc), show_alert=True)
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
            await self.discussion.answer_callback(
                query, "按钮已使用或过期，请重新执行命令。", show_alert=True
            )
            return
        action, payload = consumed
        await self.discussion.answer_callback(query)
        try:
            await self._dispatch_callback(action, payload, space)
        except (KeyError, ValueError, RuntimeError, OSError, TelegramError, PathPolicyError) as exc:
            await self._send_space(space, escape(str(exc)))

    async def _dispatch_callback(
        self, action: str, payload: dict[str, Any], space: dict[str, Any]
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
            await self._send_prompt(space, str(payload["prompt"]), str(payload["mode"]))
        elif action == "queue_cancel":
            self._ensure_unlocked(space)
            cancelled = self.store.cancel_space_prompt(
                str(space["space_id"]), int(payload["queue_id"]), int(space["generation"])
            )
            await self._send_space(space, "已取消队列项。" if cancelled else "队列项已变化。")
        elif action == "profile_model":
            self._ensure_unlocked(space)
            await self._profile_model_selected(space, payload)
        elif action == "profile_effort":
            self._ensure_unlocked(space)
            await self._profile_effort_selected(space, payload)
        elif action == "send_file":
            self._ensure_unlocked(space)
            await self._send_file(space, payload)
        elif action == "send_upload":
            self._ensure_unlocked(space)
            await self._send_upload(space, payload)
        elif action == "question":
            self._ensure_unlocked(space)
            await self._record_question_answer(space, payload)
        elif action in {"question_custom", "question_clarify"}:
            self._ensure_unlocked(space)
            await self._begin_question_reply(
                space,
                payload,
                clarification=action == "question_clarify",
            )
        elif action == "plan_execute":
            self._ensure_unlocked(space)
            await self._execute_plan(space, payload)
        elif action == "plan_continue":
            self._ensure_unlocked(space)
            await self._begin_plan_revision(space, payload)

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
                        [
                            self._button("BTW", "send_upload", {**payload, "mode": "steer"}, space),
                            self._button("Queue", "send_upload", {**payload, "mode": "queue"}, space),
                        ]
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
        if (
            not space
            or space.get("lifecycle") != "active"
            or str(space.get("thread_id") or "") != thread_id
        ):
            LOGGER.info(
                "event=plan_publish_skipped reason=no_active_space thread_id=%s item_id=%s",
                thread_id[:8],
                item_id[:8],
            )
            return
        space_id = str(space["space_id"])
        generation = int(space["generation"])
        if not self.store.claim_plan_publication(
            space_id=space_id,
            generation=generation,
            item_id=item_id,
            thread_id=thread_id,
            turn_id=turn_id,
        ):
            LOGGER.info(
                "event=plan_publish_deduplicated space_id=%s item_id=%s",
                space_id[:12],
                item_id[:8],
            )
            return

        chunks = render_commonmark_chunks(text, limit=3500)
        if not chunks:
            chunks = [TelegramHtmlChunk(html="<i>Plan 内容为空。</i>", plain="Plan 内容为空。")]
        rows = self._plan_action_markup(space, item_id, thread_id, turn_id)
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
                status="failed",
                message_ids=message_ids,
            )
            raise
        self.store.finish_plan_publication(
            space_id=space_id,
            generation=generation,
            item_id=item_id,
            status="published",
            message_ids=message_ids,
        )
        LOGGER.info(
            "event=plan_published space_id=%s item_id=%s chunks=%d",
            space_id[:12],
            item_id[:8],
            len(message_ids),
        )

    def _plan_action_markup(
        self,
        space: dict[str, Any],
        item_id: str,
        thread_id: str,
        turn_id: str,
    ) -> InlineKeyboardMarkup:
        payload = {"item_id": item_id, "thread_id": thread_id, "turn_id": turn_id}
        return InlineKeyboardMarkup(
            [
                [
                    self._button(
                        "批准并执行",
                        "plan_execute",
                        payload,
                        space,
                        ttl_seconds=_PLAN_ACTION_SECONDS,
                    ),
                    self._button(
                        "继续完善计划",
                        "plan_continue",
                        payload,
                        space,
                        ttl_seconds=_PLAN_ACTION_SECONDS,
                    ),
                ]
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
        latest = self.store.latest_plan_publication(
            str(space["space_id"]), int(space["generation"])
        )
        if (
            latest is None
            or str(latest["item_id"]) != str(payload.get("item_id") or "")
            or str(latest["thread_id"]) != str(space.get("thread_id") or "")
            or str(payload.get("thread_id") or "") != str(space.get("thread_id") or "")
        ):
            raise RuntimeError("该 Plan 已过期，请使用最新 Plan 的按钮")
        expected = allowed_statuses or {"published"}
        if str(latest["status"]) not in expected:
            raise RuntimeError("该 Plan 操作已处理或已过期")
        return latest

    async def _execute_plan(
        self, space: dict[str, Any], payload: dict[str, Any]
    ) -> None:
        latest = self._ensure_latest_plan(space, payload)
        await self._ensure_plan_ready(space)
        profile = await self._profile_for_mode(space, "default")
        client_message_id = self._plan_client_message_id(space, latest)
        if not self.store.mark_plan_action(
            str(space["space_id"]),
            int(space["generation"]),
            str(latest["item_id"]),
            status="executing",
        ):
            raise RuntimeError("该 Plan 操作已处理或已过期")
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
                client_message_id,
            )
            if status == "delivered":
                await self._send_space(
                    space,
                    "已确认批准请求送达 Codex；当前 turn 不会重复创建。",
                    priority=5,
                )
                return
            if status == "absent":
                self.store.release_plan_action(
                    str(space["space_id"]),
                    int(space["generation"]),
                    str(latest["item_id"]),
                    expected_status="executing",
                )
                await self._send_plan_action_retry(
                    space,
                    latest,
                    "批准请求未送达 Codex，请使用新按钮重试。",
                )
                return
            await self._send_space(
                space,
                "批准请求的送达状态暂时无法确认。为避免重复执行，已暂停自动重试。",
                priority=5,
            )
            return
        await self._send_space(
            space,
            f"已批准 Plan 并开始执行。\nTurn {inline_code(str(turn.get('id') or '')[:8])}",
            priority=5,
        )

    @staticmethod
    def _plan_client_message_id(
        space: dict[str, Any], publication: Mapping[str, Any]
    ) -> str:
        return (
            f"telegram-plan-execute-{space['space_id']}-{space['generation']}-"
            f"{publication['item_id']}"
        )

    async def _profile_for_mode(
        self, space: Mapping[str, Any], mode: str
    ) -> object | None:
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
        await self._send_space(
            space,
            message,
            reply_markup=self._plan_action_markup(
                space,
                str(publication["item_id"]),
                str(publication["thread_id"]),
                str(publication["turn_id"]),
            ),
            priority=5,
        )

    async def _recover_plan_executions(self) -> None:
        publications = getattr(self.store, "executing_plan_publications", None)
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
            client_message_id = self._plan_client_message_id(space, publication)
            try:
                status = await reconcile(
                    str(space["space_id"]),
                    int(space["generation"]),
                    str(publication["item_id"]),
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
                continue
            if status == "absent":
                released = self.store.release_plan_action(
                    str(space["space_id"]),
                    int(space["generation"]),
                    str(publication["item_id"]),
                    expected_status="executing",
                )
                if released:
                    await self._send_plan_action_retry(
                        space,
                        publication,
                        "服务重启前的批准请求没有送达 Codex，请使用新按钮重试。",
                    )
                continue
            await self._send_space(
                space,
                "服务重启前的批准请求状态暂时无法确认。为避免重复执行，已暂停自动重试。",
                priority=5,
            )

    async def _begin_plan_revision(
        self, space: dict[str, Any], payload: dict[str, Any]
    ) -> None:
        latest = self._ensure_latest_plan(space, payload)
        await self._profile_for_mode(space, "plan")
        if not self.store.mark_plan_action(
            str(space["space_id"]),
            int(space["generation"]),
            str(latest["item_id"]),
            status="revising",
        ):
            raise RuntimeError("该 Plan 操作已处理或已过期")
        owner = self.store.get_owner()
        if owner is None:
            raise RuntimeError("owner 配对已失效")
        prompt = await self._send_space(
            space,
            "请回复这条消息，说明需要如何继续完善 Plan。",
            reply_markup=ForceReply(
                selective=True,
                input_field_placeholder="输入 Plan 修改意见",
            ),
            priority=5,
        )
        nonce = self._reply_nonce(
            int(space["discussion_chat_id"]), int(prompt.message_id)
        )
        self.store.put_callback(
            nonce,
            "reply_plan_revision",
            {
                "item_id": str(latest["item_id"]),
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

    async def _present_question(
        self, space: dict[str, Any], request_key: str, index: int
    ) -> None:
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
                ),
                self._button(
                    "❓ 反问 Codex",
                    "question_clarify",
                    reply_payload,
                    space,
                    ttl_seconds=question_ttl,
                ),
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
                f"{mention} 请回复这条消息，输入你要向 Codex 反问的内容。\n"
                f"原问题：{escape(question_text)}"
            )
            plain = f"{label} 请回复这条消息，输入你要向 Codex 反问的内容。\n原问题：{question_text}"
            placeholder = "输入要向 Codex 反问的问题"
            action = "reply_question_clarify"
        else:
            markdown = (
                f"{mention} 请回复这条消息，输入你的自定义回答。\n"
                f"问题：{escape(question_text)}"
            )
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
                    int(time.time())
                    + max(self.config.callback_seconds, self.config.totp_unlock_seconds),
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
        if text and not text.startswith("/"):
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
        reply = message.reply_to_message
        if not reply:
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
                self._ensure_latest_plan(
                    space,
                    payload,
                    allowed_statuses={"revising"},
                )
                turn = await self._start_profiled_turn(
                    space,
                    (
                        "Continue refining the current plan based on this feedback. "
                        "Do not implement it yet.\n\n"
                        f"{answer}"
                    ),
                    mode="plan",
                    client_message_id=(
                        f"telegram-plan-revise-{space['space_id']}-{space['generation']}-"
                        f"{payload['item_id']}"
                    ),
                )
                await self._send_space(
                    space,
                    f"已提交 Plan 修改意见。\nTurn {inline_code(str(turn.get('id') or '')[:8])}",
                    priority=5,
                )
        except RuntimeError as exc:
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

    async def _record_question_answer(
        self, space: dict[str, Any], payload: dict[str, Any]
    ) -> None:
        request_key = str(payload["request_key"])
        question_id = str(payload["question_id"])
        answer = str(payload["answer"])
        stored = self.store.get_pending_input(request_key)
        if not stored or str(stored["thread_id"]) != str(space["thread_id"]):
            raise RuntimeError("该问题已过期或不属于当前 Session")
        questions = stored["questions"]
        known = [
            str(value.get("id") or f"question-{index + 1}")
            for index, value in enumerate(questions)
        ]
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

    async def question_resolved(self, request_key: str) -> None:
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
                    for message in messages
                    if message["bot_role"] == DISCUSSION_ROLE
                    and message["message_kind"] == "summary_anchor"
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
            int(time.time())
            + (self.config.callback_seconds if ttl_seconds is None else max(1, ttl_seconds)),
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
