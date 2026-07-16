from __future__ import annotations

import asyncio
import contextlib
import logging
import secrets
import time
from pathlib import Path
from typing import Any

from telegram import (
    BotCommand,
    BotCommandScopeAllPrivateChats,
    BotCommandScopeChat,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ChatType
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    ApplicationHandlerStop,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    TypeHandler,
)

from .bridge import Bridge
from .config import Config
from .deletions import MessageDeletionManager
from .markdown import clip, compact_path, escape, inline_code
from .metrics import render_metrics, render_metrics_plain
from .models import Owner
from .security import SecurityManager
from .space_coordinator import SessionSpaceCoordinator
from .space_dashboard import private_message_link
from .store import Store
from .telegram_common import CONTROL_ROLE, TelegramEndpoint, command_name, raw_arguments
from .views import render_help, render_sessions_page, render_status_comment

LOGGER = logging.getLogger(__name__)

_PRIVATE_COMMANDS = (
    ("sessions", "查找 Codex sessions"),
    ("topics", "查看 Session 帖子"),
    ("new", "创建待认证 Session 帖子"),
    ("perf", "查看 WSL 与 GPU 性能"),
    ("help", "显示帮助"),
)

_SESSIONS_DELETE_SECONDS = 15 * 60


class ControlBotController:
    def __init__(
        self,
        config: Config,
        store: Store,
        security: SecurityManager,
        bridge: Bridge,
        endpoint: TelegramEndpoint,
        coordinator: SessionSpaceCoordinator,
        deletions: MessageDeletionManager,
    ) -> None:
        self.config = config
        self.store = store
        self.security = security
        self.bridge = bridge
        self.endpoint = endpoint
        self.coordinator = coordinator
        self.deletions = deletions

    def install(self, application: Application) -> None:
        application.add_handler(TypeHandler(Update, self._guard), group=-100)
        for command, callback in (
            ("pair", self.pair),
            ("help", self.help),
            ("sessions", self.sessions),
            ("topics", self.topics),
            ("new", self.new),
            ("perf", self.perf),
        ):
            application.add_handler(CommandHandler(command, callback))
        application.add_handler(CallbackQueryHandler(self.callback, pattern=r"^cb:"))
        application.add_error_handler(self.error)

    async def set_commands(self) -> None:
        await self.endpoint.bot.set_my_commands(
            [BotCommand("pair", "完成 owner 配对"), BotCommand("help", "显示帮助")],
            scope=BotCommandScopeAllPrivateChats(),
        )
        owner = self.store.get_owner()
        if owner:
            await self.endpoint.bot.set_my_commands(
                [BotCommand(command, description) for command, description in _PRIVATE_COMMANDS],
                scope=BotCommandScopeChat(chat_id=owner.chat_id),
            )

    async def _guard(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat = update.effective_chat
        user = update.effective_user
        if not chat or not user or chat.type != ChatType.PRIVATE:
            raise ApplicationHandlerStop
        if not self.store.claim_telegram_update(update.update_id, bot_role=CONTROL_ROLE):
            raise ApplicationHandlerStop
        owner = self.store.get_owner()
        if owner is None:
            if command_name(update) in {"/pair", "/help"}:
                return
            await self.endpoint.send_text(chat.id, "Bot 尚未配对，请先生成本机配对码。")
            raise ApplicationHandlerStop
        if user.id != owner.user_id or chat.id != owner.chat_id:
            raise ApplicationHandlerStop

    async def pair(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat = update.effective_chat
        user = update.effective_user
        if not chat or not user:
            return
        if self.store.get_owner() is not None:
            await self.endpoint.send_text(chat.id, "Bot 已配对；更换 owner 只能使用本机 owner-reset。")
            return
        code = raw_arguments(update)
        if not code:
            await self.endpoint.send_text(chat.id, "用法：`/pair <本机配对码>`")
            return
        if not await asyncio.to_thread(self.security.consume_pair_code, code):
            await self.endpoint.send_text(chat.id, "配对码无效、过期或尝试次数过多。")
            return
        self.store.set_owner(Owner(user.id, chat.id, user.username))
        self.store.set_meta("telegram_runtime_chat_id", chat.id)
        await self.endpoint.send_text(chat.id, "🤝", plain="🤝", priority=0)
        await self.endpoint.send_text(chat.id, "配对成功。Session 写操作在评论串内使用 TOTP 认证。")
        await self.set_commands()

    async def help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat = update.effective_chat
        if not chat:
            return
        rendered = render_help(
            "9527",
            label=self.config.control_bot_label,
            paired=self.store.get_owner() is not None,
        )
        await self.endpoint.send_text(chat.id, rendered.markdown, plain=rendered.plain)

    async def sessions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat = update.effective_chat
        message = update.effective_message
        if not chat or not message:
            return
        deadline = int(time.time()) + _SESSIONS_DELETE_SECONDS
        group_key = f"sessions:{update.update_id}"
        self.deletions.schedule(
            CONTROL_ROLE,
            chat.id,
            [message.message_id],
            delete_at=deadline,
            group_key=group_key,
        )
        reply = await self._show_sessions(
            update, query=raw_arguments(update), page=1, edit=False
        )
        if reply is not None:
            self.deletions.schedule(
                CONTROL_ROLE,
                chat.id,
                [int(reply.message_id)],
                delete_at=deadline,
                group_key=group_key,
            )

    async def _show_sessions(
        self,
        update: Update,
        *,
        query: str,
        page: int,
        edit: bool,
    ) -> Any | None:
        chat = update.effective_chat
        if not chat:
            return
        states = await self.bridge.list_sessions(search_term=query or None, limit=1000)
        view = render_sessions_page(states, page=page, query=query)
        rows: list[list[InlineKeyboardButton]] = []
        if view.details:
            rows.append(
                [
                    self._button(
                        detail.label,
                        "session_detail",
                        {"thread_id": detail.thread_id},
                        chat.id,
                    )
                    for detail in view.details
                ]
            )
        rows.append(
            [
                self._button(
                    button.label,
                    "sessions_current" if button.current else "sessions_page",
                    {"query": query, "page": button.page},
                    chat.id,
                )
                for button in view.navigation
            ]
        )
        markup = InlineKeyboardMarkup(rows)
        message = update.effective_message
        if edit and message:
            return await self.endpoint.edit_text(
                chat.id,
                message.message_id,
                view.message.markdown,
                plain=view.message.plain,
                reply_markup=markup,
            )
        return await self.endpoint.send_text(
            chat.id,
            view.message.markdown,
            plain=view.message.plain,
            reply_markup=markup,
        )

    async def topics(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat = update.effective_chat
        if not chat:
            return
        spaces = self.store.list_spaces()
        if not spaces:
            await self.endpoint.send_text(chat.id, "当前没有 Session 帖子。")
            return
        lines = ["*🤖 Session 帖子*"]
        rows: list[list[InlineKeyboardButton]] = []
        for index, space in enumerate(spaces[:30], 1):
            lifecycle = str(space.get("lifecycle") or "pending")
            title = clip(str(space.get("title") or space.get("thread_id") or "Pending"), 80)
            lines.append(
                f"{index}\\. {escape(title)} · {inline_code(lifecycle)}"
            )
            link = self.coordinator.status_link(space)
            if not link and space.get("channel_chat_id") and space.get("channel_post_id"):
                link = private_message_link(
                    int(space["channel_chat_id"]), int(space["channel_post_id"])
                )
            if link:
                rows.append([InlineKeyboardButton(f"打开 {index}", url=link)])
        await self.endpoint.send_text(
            chat.id,
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(rows) if rows else None,
        )

    async def new(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat = update.effective_chat
        if not chat:
            return
        parsed = self._split_new(raw_arguments(update))
        if not parsed:
            await self.endpoint.send_text(chat.id, "用法：`/new <目录描述> | <prompt>`")
            return
        description, prompt = parsed
        candidates = await self.bridge.resolve_directory(description)
        if not candidates:
            await self.endpoint.send_text(chat.id, "没有找到符合描述且允许访问的目录。")
            return
        if len(candidates) == 1:
            await self._create_pending(chat.id, candidates[0], prompt)
            return
        rows = [
            [
                self._button(
                    compact_path(str(path))[:50],
                    "new_space",
                    {"cwd": str(path), "prompt": prompt},
                    chat.id,
                )
            ]
            for path in candidates[:8]
        ]
        await self.endpoint.send_text(
            chat.id,
            "请选择新 Session 的工作目录：",
            reply_markup=InlineKeyboardMarkup(rows),
        )

    async def _create_pending(self, chat_id: int, cwd: Path, prompt: str) -> None:
        space = await self.coordinator.create_pending(cwd, prompt)
        post_link = private_message_link(
            int(space["channel_chat_id"]), int(space["channel_post_id"])
        )
        await self.endpoint.send_text(
            chat_id,
            "待认证 Session 帖子已创建。进入评论串并发送 `/totp <验证码>`。",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("打开帖子", url=post_link)]]
            ),
        )

    async def perf(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat = update.effective_chat
        message = update.effective_message
        if not chat or not message:
            return
        snapshot = await self.bridge.metrics.with_gpu()
        reply = await self.endpoint.send_text(
            chat.id,
            render_metrics(snapshot),
            plain=render_metrics_plain(snapshot),
        )
        deadline = int(time.time()) + 60
        self.deletions.schedule(
            CONTROL_ROLE,
            chat.id,
            [message.message_id, int(reply.message_id)],
            delete_at=deadline,
            group_key=f"perf:{update.update_id}",
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
            data[3:], user.id, bot_role=CONTROL_ROLE, chat_id=chat.id
        ) if data.startswith("cb:") else None
        if not consumed:
            await self.endpoint.answer_callback(
                query, "按钮已使用或过期，请重新执行命令。", show_alert=True
            )
            return
        action, payload = consumed
        if action == "sessions_current":
            await self.endpoint.answer_callback(query)
            return
        await self.endpoint.answer_callback(query)
        try:
            if action == "sessions_page":
                await self._show_sessions(
                    update,
                    query=str(payload.get("query") or ""),
                    page=int(payload.get("page") or 1),
                    edit=True,
                )
            elif action == "session_detail":
                await self._session_detail(chat.id, str(payload["thread_id"]))
            elif action == "follow_space":
                await self._follow(chat.id, str(payload["thread_id"]))
            elif action == "new_space":
                await self._create_pending(
                    chat.id, Path(str(payload["cwd"])), str(payload["prompt"])
                )
        except (KeyError, ValueError, RuntimeError, OSError, TelegramError) as exc:
            await self.endpoint.send_text(chat.id, escape(str(exc)))

    async def _session_detail(self, chat_id: int, thread_id: str) -> None:
        state = await self.bridge.refresh(thread_id)
        rendered = render_status_comment(state)
        space = self.store.get_space_by_thread(thread_id)
        if space:
            markup = self.coordinator.open_status_keyboard(space)
        else:
            markup = InlineKeyboardMarkup(
                [[self._button("关注", "follow_space", {"thread_id": thread_id}, chat_id)]]
            )
        await self.endpoint.send_text(
            chat_id,
            rendered.markdown,
            plain=rendered.plain,
            reply_markup=markup,
        )

    async def _follow(self, chat_id: int, thread_id: str) -> None:
        space = await self.coordinator.follow_thread(thread_id)
        link = self.coordinator.status_link(space)
        if not link:
            link = private_message_link(
                int(space["channel_chat_id"]), int(space["channel_post_id"])
            )
        await self.endpoint.send_text(
            chat_id,
            f"已关注 {inline_code(thread_id[:8])}。",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("打开 Session 帖子", url=link)]]
            ),
        )

    def _button(
        self,
        label: str,
        action: str,
        payload: dict[str, Any],
        chat_id: int,
    ) -> InlineKeyboardButton:
        owner = self.store.get_owner()
        nonce = secrets.token_urlsafe(12)
        self.store.put_callback(
            nonce,
            action,
            payload,
            owner.user_id if owner else 0,
            int(time.time()) + self.config.callback_seconds,
            bot_role=CONTROL_ROLE,
            chat_id=chat_id,
        )
        return InlineKeyboardButton(label, callback_data=f"cb:{nonce}")

    @staticmethod
    def _split_new(value: str) -> tuple[str, str] | None:
        for separator in (" | ", " :: ", "\n"):
            if separator in value:
                directory, prompt = value.split(separator, 1)
                if directory.strip() and prompt.strip():
                    return directory.strip(), prompt.strip()
        return None

    async def error(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        LOGGER.error("Control Bot handler failed (%s)", type(context.error).__name__)
        if not isinstance(update, Update):
            return
        chat = update.effective_chat
        owner = self.store.get_owner()
        if not chat or not owner or chat.id != owner.chat_id:
            return
        with contextlib.suppress(TelegramError):
            await self.endpoint.send_text(chat.id, "处理指令时发生错误；详情已写入本机日志。")
