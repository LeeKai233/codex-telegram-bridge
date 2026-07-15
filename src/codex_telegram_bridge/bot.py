from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import secrets
import time
import uuid
from io import BytesIO
from pathlib import Path
from typing import Any

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ChatType, ParseMode
from telegram.error import BadRequest, TelegramError
from telegram.ext import (
    AIORateLimiter,
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
from .files import FileCandidate, PathPolicyError, prepare_inbox_path
from .markdown import (
    MAX_MESSAGE_LENGTH,
    clip,
    compact_path,
    escape,
    inline_code,
    render_dashboard,
    render_dashboard_plain,
)
from .metrics import render_metrics
from .models import Owner, ThreadState
from .security import SecurityManager
from .store import Store

LOGGER = logging.getLogger(__name__)
ALLOWED_UPDATES = ["message", "callback_query"]
_MAX_LISTED_SESSIONS = 30
_MAX_TIMELINE_EVENTS = 30


def build_application(token: str) -> Application:
    """Build a PTB application; lifecycle and polling are owned by ``main``."""
    return Application.builder().token(token).concurrent_updates(False).rate_limiter(AIORateLimiter()).build()


def _plain(markdown: str) -> str:
    value = re.sub(r"\\([_\-*\[\]()~`>#+=|{}.!\\])", r"\1", markdown)
    return value.replace("`", "").replace("*", "").replace("~", "")


def _human_bytes(value: int) -> str:
    number = float(value)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if number < 1024 or unit == "GiB":
            return f"{number:.1f} {unit}"
        number /= 1024
    return f"{number:.1f} GiB"


def _command_name(update: Update) -> str:
    message = update.effective_message
    raw = (message.text or message.caption or "") if message else ""
    first = raw.lstrip().split(maxsplit=1)[0] if raw.strip() else ""
    return first.split("@", 1)[0].casefold()


class BotController:
    """Owner-only Telegram command and callback controller."""

    def __init__(
        self,
        config: Config,
        store: Store,
        security: SecurityManager,
        bridge: Bridge,
    ) -> None:
        self.config = config
        self.store = store
        self.security = security
        self.bridge = bridge
        self._question_answers: dict[str, dict[str, list[str]]] = {}

    def install(self, application: Application) -> None:
        """Register sequential handlers and attach app-server callbacks."""
        application.add_handler(TypeHandler(Update, self._guard), group=-100)
        for command, callback in (
            ("pair", self.pair),
            ("totp", self.totp),
            ("sessions", self.sessions),
            ("watch", self.watch),
            ("unwatch", self.unwatch),
            ("status", self.status),
            ("new", self.new),
            ("attach", self.attach),
            ("prompt", self.prompt),
            ("queue", self.queue),
            ("perf", self.perf),
            ("getfile", self.getfile),
            ("plan", self.plan),
            ("timeline", self.timeline),
            ("lock", self.lock),
            ("answer", self.answer),
        ):
            application.add_handler(CommandHandler(command, callback), group=0)
        application.add_handler(CallbackQueryHandler(self.callback, pattern=r"^(?:cb:|ds:)"), group=0)
        application.add_handler(MessageHandler(filters.Document.ALL | filters.PHOTO, self.upload), group=1)
        application.add_error_handler(self.error)
        self.bridge.on_question = self.forward_question
        self.bridge.on_notice = self.forward_notice

    async def _guard(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat = update.effective_chat
        user = update.effective_user
        if not chat or not user or chat.type != ChatType.PRIVATE:
            raise ApplicationHandlerStop
        owner = self.store.get_owner()
        if owner is None:
            if _command_name(update) == "/pair":
                return
            await self._send_text(chat.id, "Bot 尚未配对。请先在本机生成配对码。")
            raise ApplicationHandlerStop
        if user.id != owner.user_id or chat.id != owner.chat_id:
            raise ApplicationHandlerStop
        if not self.store.claim_telegram_update(update.update_id):
            raise ApplicationHandlerStop

    async def _send_text(
        self,
        chat_id: int,
        markdown: str,
        *,
        plain: str | None = None,
        reply_markup: InlineKeyboardMarkup | None = None,
        priority: int = 10,
    ) -> Any:
        markdown = markdown[:MAX_MESSAGE_LENGTH]
        fallback = (plain or _plain(markdown))[:MAX_MESSAGE_LENGTH]
        try:
            return await self.bridge.messenger.call(
                lambda: self.bridge.bot.send_message(
                    chat_id=chat_id,
                    text=markdown,
                    parse_mode=ParseMode.MARKDOWN_V2,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True,
                ),
                priority=priority,
            )
        except BadRequest:
            return await self.bridge.messenger.call(
                lambda: self.bridge.bot.send_message(
                    chat_id=chat_id,
                    text=fallback,
                    reply_markup=reply_markup,
                    disable_web_page_preview=True,
                ),
                priority=priority,
            )

    async def _answer_callback(
        self, query: Any, text: str | None = None, *, show_alert: bool = False
    ) -> None:
        try:
            await self.bridge.messenger.call(
                lambda: query.answer(text=text, show_alert=show_alert), priority=0
            )
        except TelegramError as exc:
            LOGGER.debug("Unable to answer callback query (%s)", type(exc).__name__)

    def _button(
        self,
        label: str,
        action: str,
        payload: dict[str, Any],
        user_id: int | None = None,
    ) -> InlineKeyboardButton:
        owner = self.store.get_owner()
        owner_id = user_id if user_id is not None else (owner.user_id if owner else 0)
        nonce = secrets.token_urlsafe(12)
        self.store.put_callback(
            nonce,
            action,
            payload,
            owner_id,
            int(time.time()) + self.config.callback_seconds,
        )
        return InlineKeyboardButton(label, callback_data=f"cb:{nonce}")

    async def _require_unlocked(self, chat_id: int) -> bool:
        if not self.security.configured:
            await self._send_text(chat_id, "本机尚未配置 TOTP，请先运行 `codex-tg totp-enroll`。")
            return False
        if self.security.is_unlocked():
            return True
        await self._send_text(chat_id, "写操作已锁定。请先发送 `/totp 123456` 解锁。")
        return False

    @staticmethod
    def _chat_id(update: Update) -> int:
        if not update.effective_chat:
            raise RuntimeError("Telegram update has no chat")
        return int(update.effective_chat.id)

    @staticmethod
    def _raw_arguments(update: Update) -> str:
        message = update.effective_message
        raw = (message.text or message.caption or "") if message else ""
        return raw.split(maxsplit=1)[1].strip() if len(raw.split(maxsplit=1)) == 2 else ""

    async def pair(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat_id = self._chat_id(update)
        if self.store.get_owner() is not None:
            await self._send_text(chat_id, "Bot 已配对；如需更换 owner，请在本机执行 owner reset。")
            return
        user = update.effective_user
        if not user:
            return
        code = self._raw_arguments(update)
        if not code:
            await self._send_text(chat_id, "用法：`/pair 本机生成的配对码`")
            return
        if not await asyncio.to_thread(self.security.consume_pair_code, code):
            await self._send_text(chat_id, "配对码无效、过期或尝试次数过多。")
            return
        self.store.set_owner(Owner(user.id, chat_id, user.username))
        self.store.set_meta("telegram_runtime_chat_id", chat_id)
        suffix = "写操作仍需 `/totp` 解锁。" if self.security.configured else "请先在本机配置 TOTP。"
        await self._send_text(chat_id, f"🤝 配对成功。{suffix}")

    async def totp(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat_id = self._chat_id(update)
        code = self._raw_arguments(update)
        message = update.effective_message
        if message:
            with contextlib.suppress(TelegramError):
                await self.bridge.messenger.call(
                    lambda: self.bridge.bot.delete_message(chat_id=chat_id, message_id=message.message_id),
                    priority=0,
                )
        if not code:
            await self._send_text(chat_id, "用法：`/totp 123456`；消息会尽力立即删除。")
            return
        if await asyncio.to_thread(self.security.verify, code):
            minutes = max(1, self.config.totp_unlock_seconds // 60)
            await self._send_text(chat_id, f"已解锁 {inline_code(minutes)} 分钟。")
        else:
            await self._send_text(chat_id, "验证码无效、已使用，或验证暂时锁定。")

    async def sessions(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat_id = self._chat_id(update)
        states = (await self.bridge.list_sessions())[:_MAX_LISTED_SESSIONS]
        if not states:
            await self._send_text(chat_id, "当前没有 Codex session。")
            return
        subscriptions = self.store.subscriptions()
        lines = ["*Codex sessions*"]
        rows: list[list[InlineKeyboardButton]] = []
        for state in states:
            watching = state.thread_id in subscriptions
            marker = "🟢" if state.status == "active" else "⚪" if state.status == "idle" else "⚫"
            lines.append(
                f"{marker} {inline_code(state.short_id)} {escape(clip(state.title, 65))}"
                + (" · 👁" if watching else "")
            )
            rows.append(
                [
                    self._button(f"{state.short_id} 状态", "status", {"thread_id": state.thread_id}),
                    self._button(
                        "取消关注" if watching else "关注",
                        "unwatch" if watching else "watch",
                        {"thread_id": state.thread_id},
                    ),
                ]
            )
        if len(states) == _MAX_LISTED_SESSIONS:
            lines.append(escape("仅显示最近 30 个 session"))
        await self._send_text(chat_id, "\n".join(lines), reply_markup=InlineKeyboardMarkup(rows))

    async def _resolve_many(self, selectors: list[str]) -> list[ThreadState]:
        if not selectors:
            selectors = list(self.store.subscriptions())
        states: list[ThreadState] = []
        seen: set[str] = set()
        for selector in selectors:
            state = await self.bridge.resolve_thread(selector)
            if state.thread_id not in seen:
                states.append(state)
                seen.add(state.thread_id)
        return states

    async def watch(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = self._chat_id(update)
        if not await self._require_unlocked(chat_id):
            return
        if not context.args:
            await self._send_text(chat_id, "用法：`/watch <session> [session ...]`")
            return
        messages: list[str] = []
        for state in await self._resolve_many(context.args):
            watched = await self.bridge.watch(state.thread_id)
            messages.append(f"已关注 {inline_code(watched.short_id)} {escape(clip(watched.title, 70))}")
        await self._send_text(chat_id, "\n".join(messages))

    async def unwatch(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = self._chat_id(update)
        if not await self._require_unlocked(chat_id):
            return
        if not context.args:
            await self._send_text(chat_id, "用法：`/unwatch <session> [session ...]`")
            return
        messages: list[str] = []
        for state in await self._resolve_many(context.args):
            await self.bridge.unwatch(state.thread_id)
            messages.append(f"已取消关注 {inline_code(state.short_id)}")
        await self._send_text(chat_id, "\n".join(messages))

    async def status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = self._chat_id(update)
        await self._show_status(chat_id, list(context.args))

    async def _show_status(self, chat_id: int, selectors: list[str]) -> None:
        states = await self._resolve_many(selectors)
        if not states:
            await self._send_text(chat_id, "尚未关注 session；可使用 `/sessions` 和 `/watch`。")
            return
        for state in states:
            refreshed = await self.bridge.refresh(state.thread_id)
            await self._send_text(
                chat_id,
                render_dashboard(refreshed),
                plain=render_dashboard_plain(refreshed),
            )

    @staticmethod
    def _split_new(value: str) -> tuple[str, str] | None:
        for separator in (" | ", " :: ", "\n"):
            if separator in value:
                directory, prompt = value.split(separator, 1)
                if directory.strip() and prompt.strip():
                    return directory.strip(), prompt.strip()
        return None

    async def new(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat_id = self._chat_id(update)
        if not await self._require_unlocked(chat_id):
            return
        parsed = self._split_new(self._raw_arguments(update))
        if not parsed:
            await self._send_text(chat_id, "用法：`/new <目录描述> | <prompt>`")
            return
        description, prompt = parsed
        candidates = await self.bridge.resolve_directory(description)
        if not candidates:
            await self._send_text(chat_id, "没有找到符合描述且允许访问的目录。")
            return
        if len(candidates) == 1:
            await self._start_session(chat_id, candidates[0], prompt)
            return
        rows = [
            [
                self._button(
                    compact_path(str(path))[:50],
                    "new",
                    {"cwd": str(path), "prompt": prompt},
                )
            ]
            for path in candidates[:8]
        ]
        await self._send_text(
            chat_id,
            "请选择新 session 的工作目录：",
            reply_markup=InlineKeyboardMarkup(rows),
        )

    async def _start_session(self, chat_id: int, cwd: Path, prompt: str) -> None:
        state = await self.bridge.new_session(cwd, prompt, f"telegram-new-{uuid.uuid4()}")
        await self._send_text(
            chat_id,
            f"已创建并关注 {inline_code(state.short_id)} {escape(clip(state.title, 80))}",
        )

    async def attach(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = self._chat_id(update)
        if not await self._require_unlocked(chat_id):
            return
        if len(context.args) != 1:
            await self._send_text(chat_id, "用法：`/attach <session>`")
            return
        state = await self.bridge.resolve_thread(context.args[0])
        target = await self.bridge.attach(state.thread_id)
        await self._send_text(chat_id, f"tmux 已就绪：{inline_code(target)}")

    @staticmethod
    def _split_session_text(value: str) -> tuple[str, str] | None:
        parts = value.split(maxsplit=1)
        if len(parts) != 2 or not parts[1].strip():
            return None
        return parts[0], parts[1].strip()

    async def prompt(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat_id = self._chat_id(update)
        if not await self._require_unlocked(chat_id):
            return
        parsed = self._split_session_text(self._raw_arguments(update))
        if not parsed:
            await self._send_text(chat_id, "用法：`/prompt <session> <prompt>`")
            return
        selector, prompt = parsed
        state = await self.bridge.resolve_thread(selector)
        await self._send_prompt(chat_id, state.thread_id, prompt, "auto")

    async def queue(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat_id = self._chat_id(update)
        if not await self._require_unlocked(chat_id):
            return
        parsed = self._split_session_text(self._raw_arguments(update))
        if not parsed:
            await self._send_text(chat_id, "用法：`/queue <session> <prompt>`")
            return
        selector, prompt = parsed
        state = await self.bridge.resolve_thread(selector)
        await self._send_prompt(chat_id, state.thread_id, prompt, "queue")

    async def _send_prompt(self, chat_id: int, thread_id: str, prompt: str, mode: str) -> None:
        result = await self.bridge.send_prompt(
            thread_id,
            prompt,
            mode=mode,
            client_message_id=f"telegram-{uuid.uuid4()}",
        )
        if result == "choose":
            keyboard = InlineKeyboardMarkup(
                [
                    [
                        self._button(
                            "BTW · 插入当前 turn",
                            "prompt",
                            {"thread_id": thread_id, "prompt": prompt, "mode": "steer"},
                        ),
                        self._button(
                            "Queue · 稍后执行",
                            "prompt",
                            {"thread_id": thread_id, "prompt": prompt, "mode": "queue"},
                        ),
                    ]
                ]
            )
            await self._send_text(
                chat_id,
                "该 session 正在运行。请选择 prompt 的投递方式：",
                reply_markup=keyboard,
            )
            return
        labels = {"started": "已开始执行", "steered": "已作为 BTW prompt 插入", "queued": "已加入队列"}
        await self._send_text(chat_id, labels.get(result, escape(result)))

    async def perf(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        snapshot = await self.bridge.metrics.with_gpu()
        rendered = render_metrics(snapshot)
        await self._send_text(self._chat_id(update), rendered, plain=_plain(rendered))

    async def getfile(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat_id = self._chat_id(update)
        if not await self._require_unlocked(chat_id):
            return
        parsed = self._split_session_text(self._raw_arguments(update))
        if not parsed:
            await self._send_text(chat_id, "用法：`/getfile <session> <文件描述>`")
            return
        selector, description = parsed
        state = await self.bridge.resolve_thread(selector)
        candidates = await self.bridge.resolve_files(state.thread_id, description)
        if not candidates:
            await self._send_text(chat_id, "没有找到允许发送的匹配文件。")
            return
        rows: list[list[InlineKeyboardButton]] = []
        lines = ["*请选择要发送的文件*", "每个候选都需要一次性确认。"]
        for index, candidate in enumerate(candidates[:8], 1):
            lines.append(
                f"{index}\\. {inline_code(compact_path(str(candidate.path)), 100)} · "
                f"{inline_code(_human_bytes(candidate.size))}"
            )
            rows.append(
                [
                    self._button(
                        f"发送 {index}. {candidate.path.name[:35]}",
                        "file",
                        {
                            "path": str(candidate.path),
                            "size": candidate.size,
                            "modified_at": candidate.modified_at,
                            "device": candidate.device,
                            "inode": candidate.inode,
                            "modified_ns": candidate.modified_ns,
                        },
                    )
                ]
            )
        await self._send_text(chat_id, "\n".join(lines), reply_markup=InlineKeyboardMarkup(rows))

    async def _send_file(self, chat_id: int, payload: dict[str, Any]) -> None:
        path = Path(str(payload["path"]))
        candidate = FileCandidate(
            path=path,
            size=int(payload["size"]),
            modified_at=int(payload["modified_at"]),
            device=int(payload.get("device") or 0),
            inode=int(payload.get("inode") or 0),
            modified_ns=int(payload.get("modified_ns") or 0),
        )
        with self.bridge.path_policy.open_outbound(candidate) as handle:
            await self.bridge.messenger.call(
                lambda: self.bridge.bot.send_document(
                    chat_id=chat_id,
                    document=handle,
                    filename=candidate.path.name,
                    caption=f"{candidate.path.name} · {_human_bytes(candidate.size)}",
                ),
                priority=5,
            )

    async def plan(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = self._chat_id(update)
        states = await self._resolve_many(list(context.args))
        if not states:
            await self._send_text(chat_id, "尚未关注 session。")
            return
        for current in states:
            state = await self.bridge.refresh(current.thread_id)
            lines = [
                f"*Plan · {escape(clip(state.title, 70))}*",
                f"{inline_code(state.short_id)} · r{state.plan_revision} · "
                f"{inline_code(f'{state.completed_steps}/{len(state.plan)}')}",
            ]
            if not state.plan:
                lines.append("尚未创建计划。")
            for index, step in enumerate(state.plan[:30], 1):
                marker = {"completed": "✅", "inProgress": "▶", "blocked": "⏸", "failed": "❌"}.get(
                    step.status, "○"
                )
                label = escape(clip(step.step, 220))
                lines.append(f"{marker} {index}\\. {label}")
            if len(state.plan) > 30:
                lines.append(escape(f"另有 {len(state.plan) - 30} 项未显示"))
            await self._send_text(chat_id, "\n".join(lines))

    async def timeline(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        chat_id = self._chat_id(update)
        args = list(context.args)
        limit = 15
        if args and args[-1].isdigit():
            limit = min(_MAX_TIMELINE_EVENTS, max(1, int(args.pop())))
        states = await self._resolve_many(args)
        if not states:
            await self._send_text(chat_id, "用法：`/timeline <session> [条数]`")
            return
        for state in states:
            events = self.store.timeline(state.thread_id, limit)
            lines = [f"*Timeline · {escape(clip(state.title, 70))}*", inline_code(state.short_id)]
            if not events:
                lines.append("尚无事件。")
            for event in reversed(events):
                timestamp = time.strftime("%H:%M:%S", time.localtime(int(event["created_at"])))
                summary = self._event_summary(str(event["kind"]), event["payload"])
                lines.append(f"{inline_code(timestamp)} {escape(clip(summary, 240))}")
            await self._send_text(chat_id, "\n".join(lines))

    @staticmethod
    def _event_summary(kind: str, payload: dict[str, Any]) -> str:
        for key in ("explanation", "message", "threadName"):
            if payload.get(key):
                return f"{kind}: {payload[key]}"
        for key in ("item", "turn", "status", "goal"):
            value = payload.get(key)
            if isinstance(value, dict):
                detail = value.get("type") or value.get("status") or value.get("id")
                if detail:
                    return f"{kind}: {detail}"
        return kind

    async def lock(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        self.security.lock()
        await self._send_text(self._chat_id(update), "写操作已锁定。")

    async def upload(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat_id = self._chat_id(update)
        if not await self._require_unlocked(chat_id):
            return
        message = update.effective_message
        if not message:
            return
        caption_parts = (message.caption or "").strip().split(maxsplit=1)
        if not caption_parts:
            await self._send_text(chat_id, "发送文件时请使用 caption：`<session> [处理说明]`")
            return
        selector = caption_parts[0]
        caption = caption_parts[1].strip() if len(caption_parts) == 2 else ""
        state = await self.bridge.resolve_thread(selector)
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
        raw_size = telegram_file.file_size
        expected_size = int(raw_size) if raw_size is not None else None
        if expected_size is not None and expected_size > self.config.telegram_download_limit:
            await self._send_text(chat_id, "文件超过 Telegram 入站大小限制。")
            return
        destination = prepare_inbox_path(
            self.config.inbox_dir,
            state.thread_id,
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
        keyboard = InlineKeyboardMarkup(
            [
                [
                    self._button(
                        "确认发送给 Codex",
                        "upload",
                        {
                            "thread_id": state.thread_id,
                            "path": str(path),
                            "caption": caption,
                            "image": image,
                            "client_message_id": (
                                f"telegram-upload-{update.update_id}-{telegram_file.file_unique_id}"
                            ),
                            "mode": "auto",
                        },
                    )
                ]
            ]
        )
        await self._send_text(
            chat_id,
            f"已安全接收 {inline_code(path.name)} · {inline_code(_human_bytes(path.stat().st_size))}",
            reply_markup=keyboard,
        )

    async def _send_upload(self, chat_id: int, payload: dict[str, Any]) -> None:
        result = await self.bridge.send_upload(
            str(payload["thread_id"]),
            Path(str(payload["path"])),
            str(payload["caption"]),
            mode=str(payload.get("mode") or "auto"),
            image=bool(payload.get("image")),
            client_message_id=str(payload["client_message_id"]),
        )
        if result == "choose":
            base = dict(payload)
            steer = {**base, "mode": "steer"}
            queued = {**base, "mode": "queue"}
            await self._send_text(
                chat_id,
                "该 session 正在运行。请选择上传文件的投递方式：",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            self._button("BTW · 当前 turn", "upload", steer),
                            self._button("Queue · 稍后", "upload", queued),
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
        await self._send_text(chat_id, labels.get(result, escape(result)))

    async def callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        query = update.callback_query
        user = update.effective_user
        if not query or not user:
            return
        data = str(query.data or "")
        if data.startswith("ds:"):
            await self._answer_callback(query)
            await self._dashboard_callback(self._chat_id(update), data)
            return
        if not data.startswith("cb:"):
            await self._answer_callback(query, "不支持的操作", show_alert=True)
            return
        consumed = self.store.consume_callback(data[3:], user.id)
        if not consumed:
            await self._answer_callback(query, "按钮已使用或过期，请重新执行命令。", show_alert=True)
            return
        action, payload = consumed
        await self._answer_callback(query)
        try:
            await self._dispatch_callback(self._chat_id(update), action, payload)
        except (KeyError, ValueError, RuntimeError, OSError, PathPolicyError) as exc:
            await self._send_text(self._chat_id(update), escape(str(exc)))

    async def _dashboard_callback(self, chat_id: int, data: str) -> None:
        parts = data.split(":", 2)
        if len(parts) != 3:
            return
        _, selector, action = parts
        if action == "status":
            await self._show_status(chat_id, [selector])
        elif action == "prompt":
            await self._send_text(chat_id, f"使用 `/prompt {escape(selector)} <prompt>`。")
        elif action == "queue":
            await self._send_text(chat_id, f"使用 `/queue {escape(selector)} <prompt>`。")

    async def _dispatch_callback(self, chat_id: int, action: str, payload: dict[str, Any]) -> None:
        if action == "status":
            await self._show_status(chat_id, [str(payload["thread_id"])])
            return
        if action in {"watch", "unwatch", "new", "prompt", "file", "upload", "question"} and not (
            await self._require_unlocked(chat_id)
        ):
            return
        if action == "watch":
            state = await self.bridge.watch(str(payload["thread_id"]))
            await self._send_text(chat_id, f"已关注 {inline_code(state.short_id)}")
        elif action == "unwatch":
            thread_id = str(payload["thread_id"])
            await self.bridge.unwatch(thread_id)
            await self._send_text(chat_id, f"已取消关注 {inline_code(thread_id[:8])}")
        elif action == "new":
            await self._start_session(chat_id, Path(str(payload["cwd"])), str(payload["prompt"]))
        elif action == "prompt":
            await self._send_prompt(
                chat_id,
                str(payload["thread_id"]),
                str(payload["prompt"]),
                str(payload["mode"]),
            )
        elif action == "file":
            await self._send_file(chat_id, payload)
        elif action == "upload":
            await self._send_upload(chat_id, payload)
        elif action == "question":
            await self._record_question_answer(chat_id, payload)
        else:
            await self._send_text(chat_id, "未知或已经失效的操作。")

    async def forward_notice(self, message: str, thread_id: str | None) -> None:
        owner = self.store.get_owner()
        if not owner:
            return
        suffix = f"\nSession {inline_code(thread_id[:8])}" if thread_id else ""
        await self._send_text(owner.chat_id, f"{escape(message)}{suffix}", priority=5)

    async def forward_question(self, request_key: str, params: dict[str, Any]) -> None:
        owner = self.store.get_owner()
        if not owner:
            return
        questions = [value for value in params.get("questions") or [] if isinstance(value, dict)]
        if not questions or any(bool(value.get("isSecret")) for value in questions):
            return
        self._question_answers[request_key] = {}
        thread_id = str(params.get("threadId") or "")
        await self._send_text(
            owner.chat_id,
            f"*Codex 请求输入*\nSession {inline_code(thread_id[:8])}",
            priority=5,
        )
        await self._present_question(owner.chat_id, request_key, 0)

    async def _present_question(self, chat_id: int, request_key: str, index: int) -> None:
        stored = self.store.get_pending_input(request_key)
        if not stored:
            await self._send_text(chat_id, "该问题已经失效。")
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
            description = str(option.get("description") or "")
            if description:
                lines.append(f"• {escape(label)}：{escape(clip(description, 180))}")
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
                    )
                ]
            )
        lines.append(f"自定义回答：{inline_code(f'/answer {request_key} {question_id} | <回答>')}")
        await self._send_text(
            chat_id,
            "\n".join(lines),
            reply_markup=InlineKeyboardMarkup(rows) if rows else None,
            priority=5,
        )

    async def answer(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        del context
        chat_id = self._chat_id(update)
        if not await self._require_unlocked(chat_id):
            return
        raw = self._raw_arguments(update)
        left, separator, answer = raw.partition("|")
        identifiers = left.split()
        if not separator or len(identifiers) != 2 or not answer.strip():
            await self._send_text(chat_id, "用法：`/answer <请求ID> <问题ID> | <回答>`")
            return
        await self._record_question_answer(
            chat_id,
            {
                "request_key": identifiers[0],
                "question_id": identifiers[1],
                "answer": answer.strip(),
            },
        )

    async def _record_question_answer(self, chat_id: int, payload: dict[str, Any]) -> None:
        request_key = str(payload["request_key"])
        question_id = str(payload["question_id"])
        answer = str(payload["answer"])
        stored = self.store.get_pending_input(request_key)
        if not stored:
            raise RuntimeError("该问题已过期或已由其他客户端回答")
        questions = stored["questions"]
        known_ids = [str(value.get("id") or f"question-{index + 1}") for index, value in enumerate(questions)]
        if question_id not in known_ids:
            raise RuntimeError("问题 ID 不匹配")
        values = self._question_answers.setdefault(request_key, {})
        values[question_id] = [answer]
        missing = next((index for index, value in enumerate(known_ids) if value not in values), None)
        if missing is not None:
            await self._present_question(chat_id, request_key, missing)
            return
        await self.bridge.answer_question(request_key, values)
        self._question_answers.pop(request_key, None)
        await self._send_text(chat_id, "回答已提交给 Codex。")

    async def error(self, update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        LOGGER.error("Telegram handler failed (%s)", type(context.error).__name__)
        if not isinstance(update, Update):
            return
        chat = update.effective_chat
        owner = self.store.get_owner()
        if not chat or not owner or chat.id != owner.chat_id:
            return
        with contextlib.suppress(TelegramError, RuntimeError):
            await self._send_text(chat.id, "处理指令时发生错误；详细信息已写入本机日志。")
