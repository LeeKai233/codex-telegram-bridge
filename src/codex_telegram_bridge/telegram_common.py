from __future__ import annotations

import contextlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

from telegram import (
    Bot,
    ForceReply,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    ReplyParameters,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest, TelegramError
from telegram.ext import AIORateLimiter, Application

from .markdown import MAX_MESSAGE_LENGTH
from .outbound import OutboundMessenger

LOGGER = logging.getLogger(__name__)

CONTROL_ROLE = "control"
DISCUSSION_ROLE = "discussion"
ALLOWED_UPDATES = ["message", "callback_query"]


def build_application(token: str) -> Application:
    """Build a sequential PTB application whose lifecycle is owned by ``main``."""
    return (
        Application.builder()
        .token(token)
        .concurrent_updates(False)
        .rate_limiter(AIORateLimiter())
        .build()
    )


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
        reply_markup: InlineKeyboardMarkup | ForceReply | None = None,
        reply_parameters: ReplyParameters | None = None,
        priority: int = 10,
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
                    parse_mode=ParseMode.MARKDOWN_V2,
                    **kwargs,
                ),
                priority=priority,
            )
        except BadRequest:
            return await self.messenger.call(
                lambda: self.bot.send_message(text=fallback, **kwargs),
                priority=priority,
            )

    async def edit_text(
        self,
        chat_id: int,
        message_id: int,
        markdown: str,
        *,
        plain: str | None = None,
        reply_markup: InlineKeyboardMarkup | None = None,
        priority: int = 10,
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
                    parse_mode=ParseMode.MARKDOWN_V2,
                    **kwargs,
                ),
                priority=priority,
            )
        except BadRequest as exc:
            if "message is not modified" in str(exc).casefold():
                return True
            return await self.messenger.call(
                lambda: self.bot.edit_message_text(text=fallback, **kwargs),
                priority=priority,
            )

    async def delete_message(self, chat_id: int, message_id: int, *, priority: int = 0) -> bool:
        try:
            return bool(
                await self.messenger.call(
                    lambda: self.bot.delete_message(chat_id=chat_id, message_id=message_id),
                    priority=priority,
                )
            )
        except TelegramError as exc:
            LOGGER.debug("Unable to delete Telegram message (%s)", type(exc).__name__)
            return False

    async def send_document(
        self,
        chat_id: int,
        handle: BinaryIO,
        *,
        filename: str,
        caption: str,
        reply_parameters: ReplyParameters | None = None,
        priority: int = 5,
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
        )

    async def answer_callback(
        self,
        query: Any,
        text: str | None = None,
        *,
        show_alert: bool = False,
    ) -> None:
        with contextlib.suppress(TelegramError):
            await self.messenger.call(
                lambda: query.answer(text=text, show_alert=show_alert),
                priority=0,
            )
