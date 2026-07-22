from __future__ import annotations

import time
from dataclasses import replace
from types import SimpleNamespace

import pytest

from codex_telegram_bridge.config import Config
from codex_telegram_bridge.models import Owner, ThreadState
from codex_telegram_bridge.space_coordinator import SessionSpaceCoordinator
from codex_telegram_bridge.store import Store
from codex_telegram_bridge.telegram_common import CONTROL_ROLE, DISCUSSION_ROLE, STATUS_ROLE


class Endpoint:
    def __init__(self, role: str, message_id: int) -> None:
        self.role = role
        self.message_id = message_id
        self.sent: list[dict[str, object]] = []

    async def send_text(self, chat_id: int, markdown: str, **kwargs):
        self.sent.append({"chat_id": chat_id, "markdown": markdown, **kwargs})
        return SimpleNamespace(message_id=self.message_id)


class Bridge:
    def __init__(self, state: ThreadState) -> None:
        self.state = state

    async def subscribe_space_thread(self, thread_id: str) -> ThreadState:
        assert thread_id == self.state.thread_id
        return self.state


class Dashboards:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bool]] = []

    async def schedule_space(self, space_id: str, *, immediate: bool = False) -> None:
        self.calls.append((space_id, immediate))


class Deletions:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def schedule(self, bot_role: str, chat_id: int, message_ids, **kwargs) -> None:
        self.calls.append(
            {
                "bot_role": bot_role,
                "chat_id": chat_id,
                "message_ids": list(message_ids),
                **kwargs,
            }
        )


@pytest.mark.asyncio
async def test_reconcile_migrates_legacy_status_message_atomically(tmp_path) -> None:
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        allowed_root=tmp_path,
    )
    store = Store(config.database_path)
    try:
        store.set_owner(Owner(user_id=7, chat_id=70, username="owner"))
        store.set_telegram_binding({"channel_chat_id": -100111, "discussion_chat_id": -100222})
        state = ThreadState(
            thread_id="thread-migrate",
            title="Migrated session",
            cwd=str(tmp_path),
            status="active",
        )
        store.save_thread(state)
        store.create_space(
            {
                "space_id": "space-migrate",
                "space_type": "existing",
                "lifecycle": "active",
                "thread_id": state.thread_id,
                "channel_chat_id": -100111,
                "channel_post_id": 11,
                "discussion_chat_id": -100222,
                "discussion_root_id": 22,
                "status_message_id": 33,
            }
        )
        store.record_discussion_root(-100111, 11, -100222, 22)
        store.ensure_callback(
            "legacy-nonce",
            "space_refresh",
            {"space_id": "space-migrate", "generation": 1},
            7,
            4_000_000_000,
            bot_role=DISCUSSION_ROLE,
            chat_id=-100222,
            space_id="space-migrate",
            generation=1,
        )

        status = Endpoint(STATUS_ROLE, 44)
        deletions = Deletions()
        dashboards = Dashboards()
        coordinator = SessionSpaceCoordinator(
            store,
            Bridge(state),  # type: ignore[arg-type]
            Endpoint(CONTROL_ROLE, 55),  # type: ignore[arg-type]
            Endpoint(DISCUSSION_ROLE, 66),  # type: ignore[arg-type]
            dashboards,  # type: ignore[arg-type]
            status=status,  # type: ignore[arg-type]
            deletions=deletions,
        )

        await coordinator.reconcile()

        space = store.get_space("space-migrate")
        assert space is not None
        assert space["status_message_id"] == 44
        assert space["status_bot_role"] == STATUS_ROLE
        assert space["legacy_status_message_id"] == 33
        assert space["legacy_status_bot_role"] == DISCUSSION_ROLE
        assert deletions.calls[0]["bot_role"] == DISCUSSION_ROLE
        assert deletions.calls[0]["chat_id"] == -100222
        assert deletions.calls[0]["message_ids"] == [33]
        assert int(deletions.calls[0]["delete_at"]) >= 600 + int(time.time()) - 2
        assert dashboards.calls == [("space-migrate", True)]
        assert store.peek_callback(
            "legacy-nonce",
            7,
            bot_role=DISCUSSION_ROLE,
            chat_id=-100222,
            space_id="space-migrate",
            generation=1,
        ) is None
    finally:
        store.close()
