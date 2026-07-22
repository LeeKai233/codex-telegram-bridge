from __future__ import annotations

import contextlib
import hashlib
import logging
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO

from telegram import (
    Bot,
    ForceReply,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    ReplyParameters,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, TelegramError
from telegram.ext import AIORateLimiter, Application
from telegram.request import HTTPXRequest

from .markdown import MAX_MESSAGE_LENGTH
from .outbound import OperationSemantics, OutboundLane, OutboundMessenger, TrafficClass

LOGGER = logging.getLogger(__name__)

CONTROL_ROLE = "control"
DISCUSSION_ROLE = "discussion"
ALLOWED_UPDATES = ["message", "callback_query"]
POLLING_CONNECTION_POOL_SIZE = 2
POLLING_READ_TIMEOUT_SECONDS = 30.0
POLLING_CONNECT_TIMEOUT_SECONDS = 10.0
POLLING_POOL_TIMEOUT_SECONDS = 10.0
OUTBOUND_CONNECTION_POOL_SIZE = 8
OUTBOUND_READ_TIMEOUT_SECONDS = 15.0
OUTBOUND_WRITE_TIMEOUT_SECONDS = 15.0
OUTBOUND_CONNECT_TIMEOUT_SECONDS = 10.0
OUTBOUND_POOL_TIMEOUT_SECONDS = 5.0
OUTBOUND_MEDIA_WRITE_TIMEOUT_SECONDS = 60.0


def _traffic_class_for_lane(lane: OutboundLane, *, media: bool = False) -> TrafficClass:
    if media:
        return "media"
    if lane == "maintenance":
        return "maintenance"
    return "interactive"


def _chat_key(chat_id: int) -> str:
    return f"chat:{chat_id}"


def balanced_button_rows(
    buttons: Sequence[InlineKeyboardButton], *, columns: int = 3
) -> list[list[InlineKeyboardButton]]:
    """Group buttons into rows and avoid a singleton trailing row when possible."""
    if columns < 1:
        raise ValueError("columns must be positive")
    values = list(buttons)
    if not values:
        return []
    if columns == 1:
        return [[button] for button in values]
    rows = [values[index : index + columns] for index in range(0, len(values), columns)]
    if len(rows) > 1 and len(rows[-1]) == 1:
        rows[-1].insert(0, rows[-2].pop())
    return rows


@dataclass(slots=True)
class PollingHealth:
    role: str
    last_success_at: float = field(default_factory=time.monotonic)
    last_error_type: str | None = None
    failure_count: int = 0
    consecutive_failures: int = 0
    success_count: int = 0
    polling_request: PollingHealthRequest | None = field(default=None, init=False, repr=False)

    def mark_started(self) -> None:
        self.last_success_at = time.monotonic()

    def mark_success(self) -> None:
        self.last_success_at = time.monotonic()
        self.consecutive_failures = 0
        self.success_count += 1

    def mark_failure(self, error_type: str) -> None:
        self.last_error_type = error_type
        self.failure_count += 1
        self.consecutive_failures += 1

    def stale_for(self, seconds: float, *, now: float | None = None) -> bool:
        current = time.monotonic() if now is None else now
        return current - self.last_success_at >= seconds

    def snapshot(self, *, now: float | None = None) -> dict[str, object]:
        current = time.monotonic() if now is None else now
        return {
            "role": self.role,
            "seconds_since_success": max(0.0, current - self.last_success_at),
            "last_error_type": self.last_error_type,
            "failure_count": self.failure_count,
            "consecutive_failures": self.consecutive_failures,
            "success_count": self.success_count,
        }


class PollingHealthRequest(HTTPXRequest):
    """Record successful getUpdates responses without touching outbound requests."""

    __slots__ = ("health",)

    def __init__(self, health: PollingHealth, **kwargs: Any) -> None:
        self.health = health
        super().__init__(**kwargs)
        health.polling_request = self

    async def do_request(self, *args: Any, **kwargs: Any) -> tuple[int, bytes]:
        try:
            result = await super().do_request(*args, **kwargs)
        except Exception as exc:
            self.health.mark_failure(type(exc).__name__)
            raise
        if result[0] == 200:
            self.health.mark_success()
        else:
            self.health.mark_failure(f"http_{result[0]}")
        return result


def build_application(token: str, polling_health: PollingHealth | None = None) -> Application:
    """Build a sequential PTB application whose lifecycle is owned by ``main``."""
    builder = (
        Application.builder()
        .token(token)
        .concurrent_updates(False)
        .request(
            HTTPXRequest(
                connection_pool_size=OUTBOUND_CONNECTION_POOL_SIZE,
                read_timeout=OUTBOUND_READ_TIMEOUT_SECONDS,
                write_timeout=OUTBOUND_WRITE_TIMEOUT_SECONDS,
                connect_timeout=OUTBOUND_CONNECT_TIMEOUT_SECONDS,
                pool_timeout=OUTBOUND_POOL_TIMEOUT_SECONDS,
                media_write_timeout=OUTBOUND_MEDIA_WRITE_TIMEOUT_SECONDS,
            )
        )
        .rate_limiter(
            AIORateLimiter(
                group_max_rate=15,
                group_time_period=60,
                max_retries=0,
            )
        )
    )
    if polling_health is not None:
        builder = builder.get_updates_request(
            PollingHealthRequest(
                polling_health,
                connection_pool_size=POLLING_CONNECTION_POOL_SIZE,
                read_timeout=POLLING_READ_TIMEOUT_SECONDS,
                connect_timeout=POLLING_CONNECT_TIMEOUT_SECONDS,
                pool_timeout=POLLING_POOL_TIMEOUT_SECONDS,
            )
        )
    return builder.build()


def plain_from_markdown(markdown: str) -> str:
    value = re.sub(r"\\([_\-*\[\]()~`>#+=|{}.!\\])", r"\1", markdown)
    return value.replace("`", "").replace("*", "").replace("~", "")


def command_name(update: Update) -> str:
    message = update.effective_message
    raw = (message.text or message.caption or "") if message else ""
    first = raw.lstrip().split(maxsplit=1)[0] if raw.strip() else ""
    return first.split("@", 1)[0].casefold()


def raw_arguments(update: Update) -> str:
    message = update.effective_message
    raw = (message.text or message.caption or "") if message else ""
    parts = raw.split(maxsplit=1)
    return parts[1].strip() if len(parts) == 2 else ""


def human_bytes(value: int) -> str:
    number = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if number < 1024 or unit == "TiB":
            return f"{number:.1f} {unit}"
        number /= 1024
    return f"{number:.1f} TiB"


@dataclass(slots=True)
class TelegramEndpoint:
    role: str
    bot: Bot
    messenger: OutboundMessenger

    async def send_text(
        self,
        chat_id: int,
        markdown: str,
        *,
        plain: str | None = None,
        parse_mode: str | None = ParseMode.MARKDOWN_V2,
        reply_markup: InlineKeyboardMarkup | ForceReply | None = None,
        reply_parameters: ReplyParameters | None = None,
        priority: int = 10,
        lane: OutboundLane = "interactive",
        semantics: OperationSemantics = "non_idempotent",
    ) -> Any:
        markdown = markdown[:MAX_MESSAGE_LENGTH]
        fallback = (plain or plain_from_markdown(markdown))[:MAX_MESSAGE_LENGTH]
        kwargs = {
            "chat_id": chat_id,
            "reply_markup": reply_markup,
            "reply_parameters": reply_parameters,
            "link_preview_options": LinkPreviewOptions(is_disabled=True),
        }
        try:
            return await self.messenger.call(
                lambda: self.bot.send_message(
                    text=markdown,
                    parse_mode=parse_mode,
                    **kwargs,
                ),
                priority=priority,
                lane=lane,
                traffic_class=_traffic_class_for_lane(lane),
                chat_key=_chat_key(chat_id),
                semantics=semantics,
                audit={
                    "operation": "sendMessage",
                    "chat_id": chat_id,
                    "payload_fingerprint": hashlib.sha256(
                        markdown.encode("utf-8")
                    ).hexdigest(),
                },
            )
        except BadRequest:
            LOGGER.warning(
                "event=telegram_format_fallback bot_role=%s operation=send_text",
                self.role,
            )
            return await self.messenger.call(
                lambda: self.bot.send_message(text=fallback, **kwargs),
                priority=priority,
                lane=lane,
                traffic_class=_traffic_class_for_lane(lane),
                chat_key=_chat_key(chat_id),
                semantics=semantics,
                audit={
                    "operation": "sendMessage",
                    "chat_id": chat_id,
                    "payload_fingerprint": hashlib.sha256(
                        fallback.encode("utf-8")
                    ).hexdigest(),
                },
            )

    async def edit_text(
        self,
        chat_id: int,
        message_id: int,
        markdown: str,
        *,
        plain: str | None = None,
        parse_mode: str | None = ParseMode.MARKDOWN_V2,
        reply_markup: InlineKeyboardMarkup | None = None,
        priority: int = 10,
        lane: OutboundLane = "live",
    ) -> Any:
        markdown = markdown[:MAX_MESSAGE_LENGTH]
        fallback = (plain or plain_from_markdown(markdown))[:MAX_MESSAGE_LENGTH]
        kwargs = {
            "chat_id": chat_id,
            "message_id": message_id,
            "reply_markup": reply_markup,
            "link_preview_options": LinkPreviewOptions(is_disabled=True),
        }
        try:
            return await self.messenger.call(
                lambda: self.bot.edit_message_text(
                    text=markdown,
                    parse_mode=parse_mode,
                    **kwargs,
                ),
                priority=priority,
                lane=lane,
                traffic_class=_traffic_class_for_lane(lane),
                chat_key=_chat_key(chat_id),
                semantics="idempotent",
            )
        except BadRequest as exc:
            if "message is not modified" in str(exc).casefold():
                return True
            LOGGER.warning(
                "event=telegram_format_fallback bot_role=%s operation=edit_text",
                self.role,
            )
            return await self.messenger.call(
                lambda: self.bot.edit_message_text(text=fallback, **kwargs),
                priority=priority,
                lane=lane,
                traffic_class=_traffic_class_for_lane(lane),
                chat_key=_chat_key(chat_id),
                semantics="idempotent",
            )

    async def delete_message(
        self,
        chat_id: int,
        message_id: int,
        *,
        priority: int = 20,
        lane: OutboundLane = "maintenance",
    ) -> bool:
        try:
            return bool(
                await self.messenger.call(
                    lambda: self.bot.delete_message(chat_id=chat_id, message_id=message_id),
                    priority=priority,
                    lane=lane,
                    traffic_class=_traffic_class_for_lane(lane),
                    chat_key=_chat_key(chat_id),
                    semantics="idempotent",
                )
            )
        except TelegramError as exc:
            LOGGER.debug("Unable to delete Telegram message (%s)", type(exc).__name__)
            return False

    async def edit_reply_markup(
        self,
        chat_id: int,
        message_id: int,
        *,
        reply_markup: InlineKeyboardMarkup | None = None,
        priority: int = 10,
        lane: OutboundLane = "live",
    ) -> Any:
        try:
            return await self.messenger.call(
                lambda: self.bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=message_id,
                    reply_markup=reply_markup,
                ),
                priority=priority,
                lane=lane,
                traffic_class=_traffic_class_for_lane(lane),
                chat_key=_chat_key(chat_id),
                semantics="idempotent",
            )
        except BadRequest as exc:
            if "message is not modified" in str(exc).casefold():
                return True
            raise

    async def send_document(
        self,
        chat_id: int,
        handle: BinaryIO,
        *,
        filename: str,
        caption: str,
        reply_parameters: ReplyParameters | None = None,
        priority: int = 5,
        lane: OutboundLane = "interactive",
    ) -> Any:
        return await self.messenger.call(
            lambda: self.bot.send_document(
                chat_id=chat_id,
                document=handle,
                filename=Path(filename).name,
                caption=caption,
                reply_parameters=reply_parameters,
            ),
            priority=priority,
            lane=lane,
            traffic_class=_traffic_class_for_lane(lane, media=True),
            chat_key=_chat_key(chat_id),
            semantics="non_idempotent",
            audit={
                "operation": "sendDocument",
                "chat_id": chat_id,
                "payload_fingerprint": hashlib.sha256(
                    f"{Path(filename).name}\0{caption}".encode()
                ).hexdigest(),
            },
        )

    async def get_me(self, *, lane: OutboundLane = "maintenance") -> Any:
        return await self.messenger.call(
            self.bot.get_me,
            lane=lane,
            traffic_class=_traffic_class_for_lane(lane),
            chat_key="bot:get_me",
            semantics="query",
        )

    async def query(
        self,
        operation: Any,
        *,
        lane: OutboundLane = "interactive",
        chat_key: str | int | None = None,
    ) -> Any:
        return await self.messenger.call(
            operation,
            lane=lane,
            traffic_class=_traffic_class_for_lane(lane),
            chat_key=chat_key if chat_key is not None else "bot:query",
            semantics="query",
        )

    async def set_my_commands(self, commands: Sequence[Any], **kwargs: Any) -> Any:
        return await self.messenger.call(
            lambda: self.bot.set_my_commands(commands, **kwargs),
            lane="maintenance",
            traffic_class="maintenance",
            chat_key=f"bot:commands:{kwargs.get('scope', 'default')}",
            semantics="idempotent",
        )

    async def answer_callback(
        self,
        query: Any,
        text: str | None = None,
        *,
        show_alert: bool = False,
    ) -> None:
        message = getattr(query, "message", None)
        chat = getattr(message, "chat", None)
        chat_id = getattr(chat, "id", None)
        callback_id = getattr(query, "id", None)
        callback_key = (
            f"callback:{callback_id}"
            if callback_id is not None
            else _chat_key(int(chat_id))
            if chat_id is not None
            else f"callback:{id(query)}"
        )
        with contextlib.suppress(TelegramError):
            await self.messenger.call(
                lambda: query.answer(text=text, show_alert=show_alert),
                priority=0,
                lane="urgent",
                traffic_class="callback_ack",
                chat_key=callback_key,
                semantics="idempotent",
            )
