from __future__ import annotations

import asyncio
import contextlib
import logging
import secrets
import time
import uuid
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
from telegram.constants import ChatMemberStatus, ChatType
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
    render_ask_answer,
    render_ask_error,
    render_ask_waiting,
    render_help,
)

LOGGER = logging.getLogger(__name__)

_MAX_RESOLVED_QUESTION_TOMBSTONES = 512

_SESSION_COMMANDS = (
    ("status", "刷新当前 Session 状态"),
    ("totp", "认证当前 Session"),
    ("lock", "锁定当前 Session"),
    ("prompt", "发送 prompt"),
    ("ask", "独立询问 Codex"),
    ("queue", "查看队列或加入 prompt"),
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

    async def stop(self) -> None:
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
        await self._send_prompt(space, prompt, "auto")

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
            LOGGER.exception("Isolated Telegram ask %s failed", ask_id)
            detail = clip(str(exc) or type(exc).__name__, 500)
            rendered = render_ask_error(ask_id, f"独立提问失败：{detail}")
        else:
            rendered = render_ask_answer(question, answer, ask_id)
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
        await self._edit_or_resend_ask(current, waiting_message_id, rendered)

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
        labels = {"started": "已开始执行。", "steered": "已作为 BTW prompt 插入。", "queued": "已加入队列。"}
        await self._send_space(space, labels.get(result, escape(result)))

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
        consumed = self.store.consume_callback(
            data[3:], user.id, bot_role=DISCUSSION_ROLE, chat_id=chat.id
        ) if data.startswith("cb:") else None
        if not consumed:
            await self.discussion.answer_callback(
                query, "按钮已使用或过期，请重新执行命令。", show_alert=True
            )
            return
        action, payload = consumed
        space = self.store.get_space(str(payload.get("space_id") or ""))
        if not space or int(payload.get("generation") or 0) != int(space["generation"]):
            await self.discussion.answer_callback(query, "Session 状态已变化。", show_alert=True)
            return
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
        lines = [
            f"*{escape(question.get('header') or f'问题 {index + 1}')}*",
            escape(question.get("question") or "请选择"),
        ]
        rows: list[list[InlineKeyboardButton]] = []
        for option in question.get("options") or []:
            if not isinstance(option, dict) or not option.get("label"):
                continue
            label = str(option["label"])
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
                    )
                ]
            )
        reply_payload = {
            "request_key": request_key,
            "question_id": question_id,
        }
        rows.append(
            [
                self._button("✍️ 自定义回答", "question_custom", reply_payload, space),
                self._button("❓ 反问 Codex", "question_clarify", reply_payload, space),
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
        reply = message.reply_to_message if message else None
        if not message or not user or not reply:
            return
        space = self._space_for_message(message)
        if not space:
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
        values[question_id] = [answer]
        missing = next((index for index, value in enumerate(known) if value not in values), None)
        if missing is not None:
            await self._present_question(space, request_key, missing)
            return
        await self.bridge.answer_question(request_key, values)
        self._question_answers.pop(request_key, None)

    async def question_resolved(self, request_key: str) -> None:
        lock = self._question_locks.setdefault(request_key, asyncio.Lock())
        async with lock:
            self._resolved_questions.pop(request_key, None)
            self._resolved_questions[request_key] = None
            while len(self._resolved_questions) > _MAX_RESOLVED_QUESTION_TOMBSTONES:
                self._resolved_questions.pop(next(iter(self._resolved_questions)))
            messages = self.store.question_messages(request_key)
            grouped: dict[tuple[str, int], list[int]] = {}
            for message in messages:
                key = (str(message["bot_role"]), int(message["chat_id"]))
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
            self.store.pop_question_messages(request_key)
            await self.deletions.flush()
            self._question_answers.pop(request_key, None)

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
            int(time.time()) + self.config.callback_seconds,
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
        LOGGER.error("Discussion Bot handler failed (%s)", type(error).__name__)
        if not isinstance(update, Update):
            return
        message = update.effective_message
        space = self._space_for_message(message)
        if not space:
            chat = update.effective_chat
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
