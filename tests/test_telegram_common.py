from __future__ import annotations

from io import BytesIO
from types import SimpleNamespace
from typing import Any

import pytest
from telegram import InlineKeyboardButton
from telegram.ext import AIORateLimiter

from codex_telegram_bridge.telegram_common import (
    TelegramEndpoint,
    balanced_button_rows,
    build_application,
)


@pytest.mark.parametrize(
    ("count", "row_lengths"),
    (
        (1, [1]),
        (3, [3]),
        (4, [2, 2]),
        (5, [3, 2]),
        (7, [3, 2, 2]),
        (10, [3, 3, 2, 2]),
    ),
)
def test_balanced_button_rows_avoids_singleton_trailing_rows(
    count: int, row_lengths: list[int]
) -> None:
    buttons = [InlineKeyboardButton(str(index), callback_data=str(index)) for index in range(count)]

    rows = balanced_button_rows(buttons)

    assert [len(row) for row in rows] == row_lengths
    assert [button.text for row in rows for button in row] == [str(index) for index in range(count)]


def test_balanced_button_rows_preserves_single_column_rows() -> None:
    buttons = [InlineKeyboardButton(str(index), callback_data=str(index)) for index in range(3)]

    rows = balanced_button_rows(buttons, columns=1)

    assert [len(row) for row in rows] == [1, 1, 1]


def test_application_keeps_ptb_rate_limiter_and_sequential_update_processing() -> None:
    application = build_application("123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi")

    assert application.update_processor.max_concurrent_updates == 1
    assert isinstance(application.bot.rate_limiter, AIORateLimiter)


@pytest.mark.asyncio
async def test_endpoint_assigns_explicit_traffic_classes_and_chat_keys() -> None:
    class Messenger:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        async def call(self, operation: Any, **kwargs: Any) -> Any:
            self.calls.append(kwargs)
            return await operation()

    class Bot:
        async def send_message(self, **_kwargs: Any) -> object:
            return object()

        async def edit_message_text(self, **_kwargs: Any) -> object:
            return object()

        async def send_document(self, **_kwargs: Any) -> object:
            return object()

        async def delete_message(self, **_kwargs: Any) -> bool:
            return True

    class Query:
        id = "callback-1"
        message = SimpleNamespace(chat=SimpleNamespace(id=20))

        async def answer(self, **_kwargs: Any) -> None:
            pass

    messenger = Messenger()
    endpoint = TelegramEndpoint("control", Bot(), messenger)  # type: ignore[arg-type]

    await endpoint.send_text(20, "text", parse_mode=None)
    await endpoint.edit_text(20, 1, "edit", parse_mode=None)
    await endpoint.send_document(20, BytesIO(b"data"), filename="file.txt", caption="file")
    await endpoint.delete_message(20, 1)
    await endpoint.answer_callback(Query())

    assert [call["traffic_class"] for call in messenger.calls] == [
        "interactive",
        "interactive",
        "media",
        "maintenance",
        "callback_ack",
    ]
    assert [call["chat_key"] for call in messenger.calls] == [
        "chat:20",
        "chat:20",
        "chat:20",
        "chat:20",
        "callback:callback-1",
    ]
