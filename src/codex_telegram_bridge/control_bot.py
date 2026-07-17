from __future__ import annotations

import asyncio
import contextlib
import difflib
import logging
import math
import secrets
import time
from dataclasses import dataclass
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
    MessageHandler,
    TypeHandler,
    filters,
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
from .telegram_common import (
    CONTROL_ROLE,
    TelegramEndpoint,
    balanced_button_rows,
    command_name,
    raw_arguments,
)
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
_PERF_LIFETIME_SECONDS = 30.0
_PERF_UPDATE_SECONDS = 1.05
_PERF_FRAMES = ("🕛", "🕒", "🕕", "🕘")
_NEW_INTERACTION_SECONDS = 5 * 60
_NEW_PROMPT_SECONDS = 30
_NEW_INTERACTION_KIND = "control_new"


@dataclass(slots=True)
class _PerfRun:
    task: asyncio.Task[None]
    message_ids: tuple[int, int]
    group_key: str


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
        self._perf_runs: dict[int, _PerfRun] = {}
        self._new_timeouts: dict[str, asyncio.Task[None]] = {}

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
        application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self.observe_message)
        )
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
        self._restore_new_interactions()

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
        user = update.effective_user
        if not chat or not user:
            return
        arguments = raw_arguments(update)
        scope_key = self._new_scope(chat.id, user.id)
        if not arguments:
            draft = self.store.replace_interaction(
                scope_key,
                kind=_NEW_INTERACTION_KIND,
                phase="normal_model",
                payload={},
                user_id=user.id,
                bot_role=CONTROL_ROLE,
                chat_id=chat.id,
                expires_at=int(time.time()) + _NEW_INTERACTION_SECONDS,
            )
            self._schedule_new_timeout(draft)
            await self._show_model_choices(chat.id, draft, plan=False)
            return

        parsed = self._parse_new_arguments(arguments)
        if parsed is None:
            await self._send_new_parse_suggestion(chat.id, arguments)
            return
        normal_model, normal_effort, mode, plan_model, plan_effort, cwd, prompt = parsed
        if mode not in {None, "planmode", "noplan"}:
            await self._send_new_parse_suggestion(chat.id, arguments)
            return
        try:
            normal = await self.bridge.resolve_model_profile(normal_model, normal_effort)
            plan = (
                await self.bridge.resolve_model_profile(str(plan_model), str(plan_effort))
                if mode == "planmode"
                else None
            )
        except ValueError:
            await self._send_new_suggestion(chat.id, parsed)
            return

        payload: dict[str, Any] = {
            "normal_model": normal.model,
            "normal_effort": normal.effort,
        }
        if plan is not None:
            payload.update({"plan_model": plan.model, "plan_effort": plan.effort})
        phase = "plan_choice" if mode is None else "project"
        draft = self.store.replace_interaction(
            scope_key,
            kind=_NEW_INTERACTION_KIND,
            phase=phase,
            payload=payload,
            user_id=user.id,
            bot_role=CONTROL_ROLE,
            chat_id=chat.id,
            expires_at=int(time.time()) + _NEW_INTERACTION_SECONDS,
        )
        self._schedule_new_timeout(draft)
        if mode is None:
            await self._show_plan_choice(chat.id, draft)
            return
        if cwd is None:
            await self._ask_for_project(chat.id)
            return
        await self._handle_project_value(draft, cwd, initial_prompt=prompt)

    async def _create_pending(
        self,
        chat_id: int,
        cwd: Path,
        prompt: str,
        payload: dict[str, Any],
    ) -> None:
        space = await self.coordinator.create_pending(
            cwd,
            prompt,
            normal_model=str(payload["normal_model"]),
            normal_effort=str(payload["normal_effort"]),
            plan_model=self._optional_text(payload.get("plan_model")),
            plan_effort=self._optional_text(payload.get("plan_effort")),
            current_mode="plan" if payload.get("plan_model") else "default",
        )
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
        await self._cancel_perf(chat.id, delete=True)
        group_key = f"perf:{update.update_id}"
        snapshot = await self.bridge.metrics.with_gpu()
        reply = await self.endpoint.send_text(
            chat.id,
            self._render_perf(snapshot, 0, plain=False),
            plain=self._render_perf(snapshot, 0, plain=True),
        )
        started = time.monotonic()
        deadline = math.ceil(time.time() + _PERF_LIFETIME_SECONDS)
        reply_id = int(reply.message_id)
        self.deletions.schedule(
            CONTROL_ROLE,
            chat.id,
            [message.message_id, reply_id],
            delete_at=deadline,
            group_key=group_key,
        )
        task = asyncio.create_task(
            self._run_perf(chat.id, reply_id, started),
            name=f"control-perf:{chat.id}",
        )
        run = _PerfRun(task, (message.message_id, reply_id), group_key)
        self._perf_runs[chat.id] = run
        task.add_done_callback(lambda completed: self._finish_perf(chat.id, completed))

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
            elif action == "new_flow":
                await self._handle_new_callback(chat.id, payload)
        except (KeyError, ValueError, RuntimeError, OSError, TelegramError) as exc:
            await self.endpoint.send_text(chat.id, escape(str(exc)))

    async def observe_message(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        del context
        chat = update.effective_chat
        user = update.effective_user
        message = update.effective_message
        text = str(getattr(message, "text", "") or "").strip() if message else ""
        if not chat or not user or not text or text.startswith("/"):
            return
        scope_key = self._new_scope(chat.id, user.id)
        draft = self.store.get_interaction(scope_key)
        if draft is None or draft.claimed_at is not None:
            return
        if draft.expires_at <= int(time.time()):
            await self._expire_new_draft(draft)
            return
        if draft.phase == "prompt":
            await self._finish_new_prompt(draft, text)
        elif draft.phase in {"project", "project_choice", "project_confirmation"}:
            await self._handle_project_value(draft, text)

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

    async def _show_model_choices(self, chat_id: int, draft: Any, *, plan: bool) -> None:
        options = await self.bridge.list_model_options()
        if not options:
            raise RuntimeError("当前没有可用模型。")
        event = "plan_model" if plan else "normal_model"
        buttons = [
            self._new_button(
                chat_id,
                draft,
                event,
                str(option.model),
                self._model_label(option),
            )
            for option in options
        ]
        mode = "Plan Mode" if plan else "Normal Mode"
        await self.endpoint.send_text(
            chat_id,
            f"请选择 {mode} 使用的模型：",
            reply_markup=InlineKeyboardMarkup(balanced_button_rows(buttons)),
        )

    async def _show_effort_choices(self, chat_id: int, draft: Any, *, plan: bool) -> None:
        key = "plan_model" if plan else "normal_model"
        model = str(draft.payload[key])
        option = await self._model_option(model)
        event = "plan_effort" if plan else "normal_effort"
        buttons = [
            self._new_button(
                chat_id,
                draft,
                event,
                effort,
                f"{effort}{'（默认）' if effort == option.default_effort else ''}",
            )
            for effort in option.supported_efforts
        ]
        await self.endpoint.send_text(
            chat_id,
            f"请选择 {inline_code(model)} 的 effort：",
            reply_markup=InlineKeyboardMarkup(balanced_button_rows(buttons)),
        )

    async def _show_plan_choice(self, chat_id: int, draft: Any) -> None:
        rows = [
            [
                self._new_button(chat_id, draft, "plan_choice", "yes", "是"),
                self._new_button(chat_id, draft, "plan_choice", "no", "否"),
            ]
        ]
        await self.endpoint.send_text(
            chat_id,
            "新 Session 是否先进入 Plan Mode？",
            reply_markup=InlineKeyboardMarkup(rows),
        )

    async def _handle_new_callback(self, chat_id: int, callback: dict[str, Any]) -> None:
        scope_key = str(callback["scope_key"])
        draft = self.store.get_interaction(scope_key)
        if (
            draft is None
            or draft.claimed_at is not None
            or draft.flow_id != str(callback["flow_id"])
            or draft.revision != int(callback["revision"])
        ):
            await self.endpoint.send_text(chat_id, "该选择已失效，请重新执行 `/new`。")
            return
        if draft.expires_at <= int(time.time()):
            await self._expire_new_draft(draft)
            await self.endpoint.send_text(chat_id, "该选择已过期，请重新执行 `/new`。")
            return
        event = str(callback["event"])
        value = str(callback.get("value") or "")
        payload = dict(draft.payload)
        if event == "normal_model" and draft.phase == "normal_model":
            payload["normal_model"] = value
            updated = self._advance_new(draft, "normal_effort", payload)
            if updated:
                await self._show_effort_choices(chat_id, updated, plan=False)
            return
        if event == "normal_effort" and draft.phase == "normal_effort":
            profile = await self.bridge.resolve_model_profile(
                str(payload["normal_model"]), value
            )
            payload.update({"normal_model": profile.model, "normal_effort": profile.effort})
            updated = self._advance_new(draft, "plan_choice", payload)
            if updated:
                await self._show_plan_choice(chat_id, updated)
            return
        if event == "plan_choice" and draft.phase == "plan_choice":
            if value == "yes":
                updated = self._advance_new(draft, "plan_model", payload)
                if updated:
                    await self._show_model_choices(chat_id, updated, plan=True)
            elif value == "no":
                updated = self._advance_new(draft, "project", payload)
                if updated:
                    await self._ask_for_project(chat_id)
            return
        if event == "plan_model" and draft.phase == "plan_model":
            payload["plan_model"] = value
            updated = self._advance_new(draft, "plan_effort", payload)
            if updated:
                await self._show_effort_choices(chat_id, updated, plan=True)
            return
        if event == "plan_effort" and draft.phase == "plan_effort":
            profile = await self.bridge.resolve_model_profile(
                str(payload["plan_model"]), value
            )
            payload.update({"plan_model": profile.model, "plan_effort": profile.effort})
            updated = self._advance_new(draft, "project", payload)
            if updated:
                await self._ask_for_project(chat_id)
            return
        if event == "project" and draft.phase == "project_choice":
            await self._handle_project_value(
                draft,
                value,
                initial_prompt=self._optional_text(payload.get("initial_prompt")),
            )
            return
        if event == "create_project" and draft.phase == "project_confirmation":
            applying = self._advance_new(draft, "creating_project", payload)
            if applying is None:
                return
            try:
                cwd = await self.bridge.create_project_directory(Path(value))
            except (ValueError, OSError) as exc:
                self.store.claim_interaction(
                    applying.scope_key,
                    applying.flow_id,
                    applying.revision,
                )
                self._cancel_new_timeout(applying.scope_key)
                await self.endpoint.send_text(chat_id, escape(str(exc)))
                return
            await self._accept_project(
                applying,
                cwd,
                initial_prompt=self._optional_text(payload.get("initial_prompt")),
            )
            return
        if event == "hello" and draft.phase == "prompt":
            await self._finish_new_prompt(draft, "Hello")
            return
        await self.endpoint.send_text(chat_id, "该选择与当前步骤不匹配，请重新执行 `/new`。")

    async def _handle_project_value(
        self,
        draft: Any,
        value: str,
        *,
        initial_prompt: str | None = None,
    ) -> None:
        payload = dict(draft.payload)
        if initial_prompt:
            payload["initial_prompt"] = initial_prompt
        else:
            payload.pop("initial_prompt", None)
        try:
            candidates = await self.bridge.resolve_directory(value)
        except (ValueError, OSError) as exc:
            await self.endpoint.send_text(draft.chat_id, escape(str(exc)))
            return
        if len(candidates) == 1:
            await self._accept_project(draft, candidates[0], initial_prompt=initial_prompt)
            return
        if len(candidates) > 1:
            updated = self._advance_new(draft, "project_choice", payload)
            if updated is None:
                return
            rows = [
                [
                    self._new_button(
                        draft.chat_id,
                        updated,
                        "project",
                        str(path),
                        compact_path(str(path))[:50],
                    )
                ]
                for path in candidates[:8]
            ]
            await self.endpoint.send_text(
                draft.chat_id,
                "找到多个项目，请选择工作目录：",
                reply_markup=InlineKeyboardMarkup(rows),
            )
            return
        try:
            target = await self.bridge.prepare_directory_creation(value)
        except (ValueError, OSError) as exc:
            await self.endpoint.send_text(draft.chat_id, escape(str(exc)))
            return
        if target is None:
            updated = self._advance_new(draft, "project", payload)
            if updated:
                await self.endpoint.send_text(
                    draft.chat_id,
                    "没有找到匹配项目。请发送允许目录中的明确路径。",
                )
            return
        payload["project_target"] = str(target)
        updated = self._advance_new(draft, "project_confirmation", payload)
        if updated is None:
            return
        button = self._new_button(
            draft.chat_id,
            updated,
            "create_project",
            str(target),
            "创建目录",
        )
        await self.endpoint.send_text(
            draft.chat_id,
            f"目录 {inline_code(compact_path(str(target)))} 不存在，是否创建？",
            reply_markup=InlineKeyboardMarkup([[button]]),
        )

    async def _accept_project(
        self,
        draft: Any,
        cwd: Path,
        *,
        initial_prompt: str | None,
    ) -> None:
        payload = dict(draft.payload)
        payload["cwd"] = str(cwd)
        payload.pop("project_target", None)
        payload.pop("initial_prompt", None)
        updated = self._advance_new(
            draft,
            "prompt",
            payload,
            seconds=_NEW_PROMPT_SECONDS,
        )
        if updated is None:
            return
        if initial_prompt:
            await self._finish_new_prompt(updated, initial_prompt)
            return
        hello = self._new_button(draft.chat_id, updated, "hello", "Hello", "Hello")
        await self.endpoint.send_text(
            draft.chat_id,
            "请发送第一条 prompt。30 秒内未发送时将使用 `Hello`。",
            reply_markup=InlineKeyboardMarkup([[hello]]),
        )

    async def _finish_new_prompt(
        self, draft: Any, prompt: str, *, expired: bool = False
    ) -> None:
        claim = (
            self.store.claim_expired_interaction
            if expired
            else self.store.claim_live_interaction
        )
        claimed = claim(draft.scope_key, draft.flow_id, draft.revision)
        if claimed is None:
            return
        self._cancel_new_timeout(draft.scope_key)
        payload = dict(claimed.payload)
        try:
            await self._create_pending(
                claimed.chat_id,
                Path(str(payload["cwd"])),
                prompt,
                payload,
            )
        except (KeyError, ValueError, RuntimeError, OSError, TelegramError) as exc:
            await self.endpoint.send_text(
                claimed.chat_id,
                f"Session 创建失败，请重新执行 `/new`：{escape(str(exc))}",
            )
        finally:
            self.store.delete_interaction(claimed.scope_key)

    async def _ask_for_project(self, chat_id: int) -> None:
        await self.endpoint.send_text(
            chat_id,
            "请发送项目地址或项目描述；下一条文本消息会被识别为项目。",
        )

    def _advance_new(
        self,
        draft: Any,
        phase: str,
        payload: dict[str, Any],
        *,
        seconds: int = _NEW_INTERACTION_SECONDS,
    ) -> Any | None:
        updated = self.store.advance_interaction(
            draft.scope_key,
            draft.flow_id,
            draft.revision,
            phase=phase,
            payload=payload,
            expires_at=int(time.time()) + seconds,
        )
        if updated is not None:
            self._schedule_new_timeout(updated)
        return updated

    def _schedule_new_timeout(self, draft: Any) -> None:
        self._cancel_new_timeout(draft.scope_key)
        task = asyncio.create_task(
            self._run_new_timeout(draft.scope_key, draft.flow_id, draft.revision),
            name=f"control-new-timeout:{draft.chat_id}",
        )
        self._new_timeouts[draft.scope_key] = task

    async def _run_new_timeout(
        self, scope_key: str, flow_id: str, revision: int
    ) -> None:
        try:
            draft = self.store.get_interaction(scope_key)
            if draft is None:
                return
            await asyncio.sleep(max(0.0, float(draft.expires_at - time.time())))
            current = self.store.get_interaction(scope_key)
            if (
                current is None
                or current.claimed_at is not None
                or current.flow_id != flow_id
                or current.revision != revision
            ):
                return
            await self._expire_new_draft(current)
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("Failed to expire /new interaction")
        finally:
            current_task = asyncio.current_task()
            if self._new_timeouts.get(scope_key) is current_task:
                self._new_timeouts.pop(scope_key, None)

    async def _expire_new_draft(self, draft: Any) -> None:
        if draft.phase == "prompt":
            await self._finish_new_prompt(draft, "Hello", expired=True)
        else:
            self.store.delete_interaction(draft.scope_key)

    def _restore_new_interactions(self) -> None:
        for draft in self.store.list_interactions(kind=_NEW_INTERACTION_KIND):
            if draft.claimed_at is None:
                self._schedule_new_timeout(draft)

    def _cancel_new_timeout(self, scope_key: str) -> None:
        task = self._new_timeouts.pop(scope_key, None)
        if task is not None and task is not asyncio.current_task():
            task.cancel()

    async def _run_perf(self, chat_id: int, message_id: int, started: float) -> None:
        expires = started + _PERF_LIFETIME_SECONDS
        tick = 1
        while True:
            target = started + tick * _PERF_UPDATE_SECONDS
            if target >= expires:
                return
            await asyncio.sleep(max(0.0, target - time.monotonic()))
            elapsed = max(0.0, time.monotonic() - started)
            tick = max(tick, int(elapsed // _PERF_UPDATE_SECONDS))
            if time.monotonic() >= expires:
                return
            try:
                snapshot = await self.bridge.metrics.with_gpu()
                if time.monotonic() >= expires:
                    return
                frame = tick % len(_PERF_FRAMES)
                await self.endpoint.edit_text(
                    chat_id,
                    message_id,
                    self._render_perf(snapshot, frame, plain=False),
                    plain=self._render_perf(snapshot, frame, plain=True),
                    priority=50,
                )
            except TelegramError:
                LOGGER.debug("Unable to update dynamic /perf message", exc_info=True)
                return
            except Exception:
                LOGGER.warning("Unable to sample dynamic /perf message", exc_info=True)
                return
            tick += 1

    async def _cancel_perf(self, chat_id: int, *, delete: bool) -> None:
        run = self._perf_runs.pop(chat_id, None)
        if run is None:
            return
        run.task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run.task
        if delete:
            await self.deletions.delete_now(
                CONTROL_ROLE,
                chat_id,
                run.message_ids,
                group_key=run.group_key,
            )

    def _finish_perf(self, chat_id: int, task: asyncio.Task[None]) -> None:
        run = self._perf_runs.get(chat_id)
        if run is not None and run.task is task:
            self._perf_runs.pop(chat_id, None)
        if not task.cancelled():
            with contextlib.suppress(Exception):
                task.result()

    async def stop(self) -> None:
        for chat_id in list(self._perf_runs):
            await self._cancel_perf(chat_id, delete=False)
        tasks = list(self._new_timeouts.values())
        self._new_timeouts.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _new_button(
        self,
        chat_id: int,
        draft: Any,
        event: str,
        value: str,
        label: str,
    ) -> InlineKeyboardButton:
        return self._button(
            label,
            "new_flow",
            {
                "scope_key": draft.scope_key,
                "flow_id": draft.flow_id,
                "revision": draft.revision,
                "event": event,
                "value": value,
            },
            chat_id,
        )

    async def _model_option(self, model: str) -> Any:
        for option in await self.bridge.list_model_options():
            if str(option.model) == model:
                return option
        raise ValueError(f"模型 {model!r} 已不可用，请重新执行 /new")

    @staticmethod
    def _model_label(option: Any) -> str:
        label = str(option.display_name or option.model)
        if option.is_default:
            label += "（默认）"
        return label[:60]

    @staticmethod
    def _new_scope(chat_id: int, user_id: int) -> str:
        return f"control:{chat_id}:{user_id}:new"

    @staticmethod
    def _render_perf(snapshot: Any, frame: int, *, plain: bool) -> str:
        rendered = render_metrics_plain(snapshot) if plain else render_metrics(snapshot)
        header = f"{_PERF_FRAMES[frame]} 动态性能"
        return f"{header}\n{rendered}" if plain else f"*{header}*\n{rendered}"

    @staticmethod
    def _optional_text(value: Any) -> str | None:
        normalized = str(value or "").strip()
        return normalized or None

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
    def _parse_new_arguments(
        value: str,
    ) -> tuple[
        str,
        str,
        str | None,
        str | None,
        str | None,
        str | None,
        str | None,
    ] | None:
        leading = value.split("|", 2)
        if len(leading) < 2:
            return None
        model = leading[0].strip()
        effort = leading[1].strip()
        if not model or not effort:
            return None
        if len(leading) == 2 or not leading[2].strip():
            return model, effort, None, None, None, None, None
        mode_and_tail = leading[2].split("|", 1)
        mode = mode_and_tail[0].strip().casefold()
        tail = mode_and_tail[1] if len(mode_and_tail) == 2 else ""
        if mode == "planmode":
            fields = tail.split("|", 3)
            if len(fields) < 2 or not fields[0].strip() or not fields[1].strip():
                return None
            cwd = fields[2].strip() if len(fields) >= 3 and fields[2].strip() else None
            prompt = fields[3].strip() if len(fields) >= 4 and fields[3].strip() else None
            return (
                model,
                effort,
                mode,
                fields[0].strip(),
                fields[1].strip(),
                cwd,
                prompt,
            )
        if mode == "noplan":
            fields = tail.split("|", 1)
            cwd = fields[0].strip() if fields and fields[0].strip() else None
            prompt = fields[1].strip() if len(fields) == 2 and fields[1].strip() else None
            return model, effort, mode, None, None, cwd, prompt
        return model, effort, mode, None, None, None, None

    async def _send_new_usage(self, chat_id: int) -> None:
        await self.endpoint.send_text(
            chat_id,
            "参数不完整。可用格式：\n"
            "`/new <model> | <effort>`\n"
            "`/new <model> | <effort> | noplan [ | <cwd> [ | <prompt> ] ]`\n"
            "`/new <model> | <effort> | planmode | <plan_model> | <plan_effort> "
            "[ | <cwd> [ | <prompt> ] ]`",
        )

    async def _send_new_parse_suggestion(self, chat_id: int, arguments: str) -> None:
        parts = [part.strip() for part in arguments.split("|")]
        model = parts[0] if parts else ""
        effort = parts[1] if len(parts) > 1 else ""
        mode = parts[2].casefold() if len(parts) > 2 and parts[2] else None
        plan_model: str | None = None
        plan_effort: str | None = None
        cwd: str | None = None
        prompt: str | None = None
        if mode is not None and self._nearest_value(mode, ("planmode", "noplan")) == "planmode":
            plan_model = parts[3] if len(parts) > 3 else ""
            plan_effort = parts[4] if len(parts) > 4 else ""
            cwd = parts[5] if len(parts) > 5 and parts[5] else None
            prompt = " | ".join(parts[6:]).strip() or None
        elif mode is not None:
            cwd = parts[3] if len(parts) > 3 and parts[3] else None
            prompt = " | ".join(parts[4:]).strip() or None
        await self._send_new_suggestion(
            chat_id,
            (model, effort, mode, plan_model, plan_effort, cwd, prompt),
        )

    async def _send_new_suggestion(
        self,
        chat_id: int,
        parsed: tuple[
            str,
            str,
            str | None,
            str | None,
            str | None,
            str | None,
            str | None,
        ],
    ) -> None:
        options = await self.bridge.list_model_options()
        if not options:
            await self.endpoint.send_text(chat_id, "当前没有可用模型。")
            return
        model, effort, mode, plan_model, plan_effort, cwd, prompt = parsed
        normal_option = self._nearest_model_option(model, options)
        normal_effort = self._nearest_effort(effort, normal_option)
        fields = ["/new " + str(normal_option.model), normal_effort]
        if mode is not None:
            normalized_mode = self._nearest_value(mode, ("planmode", "noplan"))
            fields.append(normalized_mode)
            if normalized_mode == "planmode":
                plan_option = self._nearest_model_option(plan_model or model, options)
                fields.extend(
                    [
                        str(plan_option.model),
                        self._nearest_effort(plan_effort or effort, plan_option),
                    ]
                )
            if cwd:
                fields.append(cwd)
            if prompt:
                fields.append(prompt)
        suggestion = " | ".join(fields)
        await self.endpoint.send_text(
            chat_id,
            f"模型、effort 或模式无效。你可能想发送：\n{inline_code(suggestion)}",
        )

    @classmethod
    def _nearest_model_option(cls, value: str, options: list[Any]) -> Any:
        normalized = value.strip().casefold()
        aliases: dict[str, Any] = {}
        for option in options:
            model = str(option.model)
            aliases[model.casefold()] = option
            aliases[model.rsplit("-", 1)[-1].casefold()] = option
            aliases[str(option.display_name).casefold()] = option
        if normalized in aliases:
            return aliases[normalized]
        nearest = cls._nearest_value(normalized, tuple(aliases))
        if nearest in aliases:
            return aliases[nearest]
        return next((option for option in options if option.is_default), options[0])

    @classmethod
    def _nearest_effort(cls, value: str, option: Any) -> str:
        efforts = tuple(str(item) for item in option.supported_efforts)
        if not efforts:
            return str(option.default_effort)
        return cls._nearest_value(value, efforts, fallback=str(option.default_effort))

    @staticmethod
    def _nearest_value(
        value: str,
        choices: tuple[str, ...],
        *,
        fallback: str | None = None,
    ) -> str:
        if not choices:
            return fallback or value
        normalized = value.strip().casefold()
        mapping = {choice.casefold(): choice for choice in choices}
        if normalized in mapping:
            return mapping[normalized]
        matches = difflib.get_close_matches(normalized, tuple(mapping), n=1, cutoff=0.35)
        return mapping[matches[0]] if matches else (fallback or choices[0])

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
