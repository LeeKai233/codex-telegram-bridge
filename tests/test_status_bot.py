from __future__ import annotations

from types import SimpleNamespace

import pytest
from telegram.constants import ChatType
from telegram.ext import ApplicationHandlerStop

from codex_telegram_bridge.models import Owner
from codex_telegram_bridge.status_bot import StatusBotController
from codex_telegram_bridge.store import Store
from codex_telegram_bridge.telegram_common import STATUS_ROLE


class RecordingDiscussionController:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def callback_for_role(self, update, context, **kwargs):
        self.calls.append({"update": update, "context": context, **kwargs})


class Endpoint:
    role = STATUS_ROLE


def callback_update(
    update_id: int,
    *,
    chat_id: int = -100222,
    chat_type: str = ChatType.SUPERGROUP,
    user_id: int = 7,
) -> object:
    query = SimpleNamespace(
        data="cb:status-nonce",
        message=SimpleNamespace(chat=SimpleNamespace(id=chat_id, type=chat_type)),
    )
    return SimpleNamespace(
        update_id=update_id,
        callback_query=query,
        effective_user=SimpleNamespace(id=user_id),
    )


@pytest.mark.asyncio
async def test_status_bot_is_callback_only_and_role_scoped(tmp_path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        store.set_owner(Owner(user_id=7, chat_id=70, username="owner"))
        store.set_telegram_binding({"channel_chat_id": -100111, "discussion_chat_id": -100222})
        discussion = RecordingDiscussionController()
        controller = StatusBotController(store, discussion, Endpoint())
        update = callback_update(42)

        await controller._guard(update, SimpleNamespace())
        await controller.callback(update, SimpleNamespace())

        assert discussion.calls[0]["bot_role"] == STATUS_ROLE
        assert discussion.calls[0]["endpoint"] is controller.endpoint
        assert discussion.calls[0]["allowed_actions"] == frozenset(
            {"space_refresh", "space_unwatch"}
        )
        assert store.claim_telegram_update(42, bot_role="discussion") is True
        assert store.claim_telegram_update(42, bot_role=STATUS_ROLE) is False
    finally:
        store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "update",
    [
        SimpleNamespace(
            update_id=1,
            callback_query=None,
            effective_user=SimpleNamespace(id=7),
        ),
        callback_update(2, chat_id=-100999),
        callback_update(3, user_id=8),
        callback_update(4, chat_type=ChatType.PRIVATE),
    ],
)
async def test_status_bot_stops_non_status_updates(tmp_path, update) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        store.set_owner(Owner(user_id=7, chat_id=70, username="owner"))
        store.set_telegram_binding({"channel_chat_id": -100111, "discussion_chat_id": -100222})
        controller = StatusBotController(store, RecordingDiscussionController(), Endpoint())

        with pytest.raises(ApplicationHandlerStop):
            await controller._guard(update, SimpleNamespace())
    finally:
        store.close()
