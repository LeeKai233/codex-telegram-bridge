from __future__ import annotations

import base64
import hashlib
import json
import sqlite3
import stat
import time
from pathlib import Path

import pytest

import codex_telegram_bridge.store as store_module
from codex_telegram_bridge.models import (
    Owner,
    SessionSpace,
    ThreadState,
    plan_revision_key,
)
from codex_telegram_bridge.store import SCHEMA_VERSION, Store


def test_atomic_totp_replay_and_update_claims_across_connections(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    first = Store(path)
    second = Store(path)
    try:
        assert first.accept_totp_timecode(100)
        assert not second.accept_totp_timecode(100)
        assert second.accept_totp_timecode(101)
        assert first.claim_telegram_update(10)
        assert not second.claim_telegram_update(10)
        assert second.claim_telegram_update(5)
        assert first.telegram_update_seen(10)
        assert first.telegram_update_seen(5)
    finally:
        first.close()
        second.close()


def test_owner_and_callback_claims_are_first_writer_wins(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    first = Store(path)
    second = Store(path)
    try:
        owner = Owner(1, 11, "first")
        assert first.set_owner(owner)
        assert not second.set_owner(Owner(2, 22, "second"))
        assert second.get_owner() == owner
        first.put_callback("nonce", "run", {"value": 1}, 1, 4_000_000_000)
        assert second.consume_callback("nonce", 1) == ("run", {"value": 1})
        assert first.consume_callback("nonce", 1) is None
    finally:
        first.close()
        second.close()


def test_ensure_callback_reuses_live_scope_and_replaces_consumed_nonce(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    payload = {"space_id": "space-1", "generation": 1}

    first = store.ensure_callback(
        "first",
        "space_refresh",
        payload,
        7,
        4_000_000_000,
        bot_role="discussion",
        chat_id=-100123,
        space_id="space-1",
        generation=1,
    )
    reused = store.ensure_callback(
        "second",
        "space_refresh",
        payload,
        7,
        4_000_000_100,
        bot_role="discussion",
        chat_id=-100123,
        space_id="space-1",
        generation=1,
    )

    assert first == reused == "first"
    assert store.consume_callback(
        "first",
        7,
        bot_role="discussion",
        chat_id=-100123,
        space_id="space-1",
        generation=1,
    ) == ("space_refresh", payload)

    replacement = store.ensure_callback(
        "third",
        "space_refresh",
        payload,
        7,
        4_000_000_200,
        bot_role="discussion",
        chat_id=-100123,
        space_id="space-1",
        generation=1,
    )
    next_generation = store.ensure_callback(
        "fourth",
        "space_refresh",
        {"space_id": "space-1", "generation": 2},
        7,
        4_000_000_200,
        bot_role="discussion",
        chat_id=-100123,
        space_id="space-1",
        generation=2,
    )

    assert replacement == "third"
    assert next_generation == "fourth"
    store.close()


def test_pair_code_is_consumed_atomically_and_locked_after_failures(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    salt = b"0123456789abcdef"
    code = "ABCDEF0123"
    store.set_meta_many(
        {
            "pair_code_salt": base64.b64encode(salt).decode("ascii"),
            "pair_code_digest": hashlib.sha256(salt + code.encode("ascii")).hexdigest(),
            "pair_code_expires": 200,
            "pair_code_failures": 0,
        }
    )
    assert store.consume_pair_code(code, now=100)
    assert not store.consume_pair_code(code, now=100)

    store.set_meta_many(
        {
            "pair_code_salt": base64.b64encode(salt).decode("ascii"),
            "pair_code_digest": hashlib.sha256(salt + code.encode("ascii")).hexdigest(),
            "pair_code_expires": 200,
            "pair_code_failures": 0,
        }
    )
    for _ in range(5):
        assert not store.consume_pair_code("WRONG", now=100)
    assert store.get_meta("pair_code_expires") == 0
    assert not store.consume_pair_code(code, now=100)
    store.close()


def test_reset_owner_revokes_authority_and_queued_work(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    upload = tmp_path / "inbox" / "upload.txt"
    upload.parent.mkdir()
    upload.write_text("data", encoding="utf-8")
    assert store.set_owner(Owner(1, 11, "owner"))
    store.subscribe("thread", message_id=99)
    store.put_callback("nonce", "run", {}, 1, 4_000_000_000)
    store.put_pending_input("request", "1", 1, "thread", "turn", "item", [], None)
    store.enqueue_prompt(
        "thread",
        "read",
        [{"type": "mention", "name": upload.name, "path": str(upload)}],
        "message-1",
    )
    assert store.save_question_resolution("request", {"question": ["answer"]}, source="telegram")
    assert store.put_prompt_run(
        "run-1",
        space_id="space-1",
        generation=1,
        thread_id="thread",
        turn_id="turn",
        client_message_id="prompt-1",
    )
    assert store.claim_plan_publication(
        space_id="space-1",
        generation=1,
        item_id="plan-1",
        thread_id="thread",
        turn_id="turn",
    )
    store.set_meta_many({"totp_force_locked": False, "totp_unlocked_until": 4_000_000_000})
    assert store.queued_file_paths() == {upload.resolve()}

    store.reset_owner()

    assert store.get_owner() is None
    assert store.consume_callback("nonce", 1) is None
    assert store.get_pending_input("request") is None
    assert store.queue_count("thread") == 0
    assert store.queued_file_paths() == set()
    assert store.subscriptions() == {"thread": None}
    assert store.get_meta("totp_force_locked") is True
    assert store.get_meta("totp_unlocked_until") == 0
    with store._lock:
        for table in ("question_resolutions", "prompt_runs", "plan_publications"):
            assert store._connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
    store.close()


def test_store_database_and_directory_are_private(tmp_path: Path) -> None:
    path = tmp_path / "private" / "state.sqlite3"
    store = Store(path)
    try:
        assert path.stat().st_mode & 0o777 == 0o600
        assert path.parent.stat().st_mode & 0o777 == 0o700
        with store._lock:
            row = store._connection.execute("PRAGMA journal_mode").fetchone()
            assert str(row[0]).casefold() == "wal"
            assert int(store._connection.execute("PRAGMA busy_timeout").fetchone()[0]) == 30_000
    finally:
        store.close()


def test_update_cleanup_keeps_recent_ids_without_a_high_water_mark(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        assert store.claim_telegram_update(100)
        assert store.claim_telegram_update(5)
        with store._lock, store._connection:
            store._connection.execute("UPDATE telegram_updates SET received_at=1")
        store.cleanup(update_days=1, keep_recent_updates=1)
        assert not store.telegram_update_seen(100)
        assert store.telegram_update_seen(5)
        assert store.claim_telegram_update(100)
    finally:
        store.close()


def test_update_claim_enforces_hard_limit_by_arrival_order(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        assert store.claim_telegram_update(100, max_tracked=2)
        assert store.claim_telegram_update(200, max_tracked=2)
        assert store.claim_telegram_update(5, max_tracked=2)

        count = store._connection.execute("SELECT COUNT(*) FROM telegram_updates").fetchone()[0]
        assert count == 2
        assert not store.telegram_update_seen(100)
        assert store.telegram_update_seen(200)
        assert store.telegram_update_seen(5)
        assert not store.claim_telegram_update(5, max_tracked=2)
    finally:
        store.close()


def test_pending_callback_file_paths_only_returns_live_unconsumed_paths(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    live = tmp_path / "live.txt"
    consumed = tmp_path / "consumed.txt"
    expired = tmp_path / "expired.txt"
    now = int(time.time())
    try:
        store.put_callback("live", "upload", {"path": str(live)}, 7, now + 60)
        store.put_callback("used", "upload", {"path": str(consumed)}, 7, now + 60)
        store.put_callback("old", "upload", {"path": str(expired)}, 7, now - 1)
        assert store.consume_callback("used", 7) is not None

        assert store.pending_callback_file_paths() == {live.resolve(strict=False)}
    finally:
        store.close()


def test_legacy_database_is_backed_up_and_migrated_idempotently(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE subscriptions(
            thread_id TEXT PRIMARY KEY,
            dashboard_message_id INTEGER,
            active INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL
        );
        INSERT INTO subscriptions VALUES('old-thread', NULL, 0, 1);
        CREATE TABLE telegram_updates(
            update_id INTEGER PRIMARY KEY,
            received_at INTEGER NOT NULL,
            sequence INTEGER
        );
        INSERT INTO telegram_updates VALUES(7, 10, 1);
        """
    )
    connection.close()

    store = Store(path)
    backup = store.last_backup_path
    assert store.schema_version == SCHEMA_VERSION
    assert backup is not None and backup.is_file()
    assert stat.S_IMODE(backup.stat().st_mode) == 0o600
    assert store.telegram_update_seen(7, "control")
    assert store.subscriptions() == {}
    store.close()

    reopened = Store(path)
    assert reopened.last_backup_path is None
    assert reopened.schema_version == SCHEMA_VERSION
    reopened.close()


def test_v4_migration_adds_interaction_state_tables_and_preserves_messages(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE question_messages (
            request_key TEXT NOT NULL,
            bot_role TEXT NOT NULL,
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            PRIMARY KEY(request_key, bot_role, chat_id, message_id)
        );
        INSERT INTO question_messages VALUES('request-1', 'forum', -1002, 42, 1);
        PRAGMA user_version=4;
        """
    )
    connection.close()

    store = Store(path)
    try:
        assert store.schema_version == SCHEMA_VERSION
        assert store.question_messages("request-1") == [
            {
                "bot_role": "forum",
                "chat_id": -1002,
                "message_id": 42,
                "message_kind": "interaction",
            }
        ]
        tables = {
            str(row[0])
            for row in store._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert {
            "interaction_drafts",
            "question_resolutions",
            "prompt_runs",
            "plan_publications",
        } <= tables
    finally:
        store.close()


def test_v6_plan_publications_migrate_and_accept_same_item_new_revision(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE plan_publications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            space_id TEXT NOT NULL,
            generation INTEGER NOT NULL,
            item_id TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            turn_id TEXT NOT NULL,
            status TEXT NOT NULL,
            message_ids_json TEXT NOT NULL DEFAULT '[]',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(space_id, generation, item_id)
        );
        CREATE INDEX idx_plan_publications_latest
            ON plan_publications(space_id, generation, id DESC);
        INSERT INTO plan_publications(
            space_id, generation, item_id, thread_id, turn_id, status,
            message_ids_json, created_at, updated_at
        ) VALUES('space-1', 2, 'item-1', 'thread-1', 'turn-1', 'published', '[41]', 1, 1);
        PRAGMA user_version=6;
        """
    )
    connection.close()

    store = Store(path)
    try:
        assert store.schema_version == SCHEMA_VERSION
        assert store.last_backup_path is not None
        migrated = store.latest_plan_publication("space-1", 2)
        assert migrated is not None
        assert migrated["revision_key"] == ""
        revision = plan_revision_key("turn-1", "Updated plan")
        assert store.claim_plan_publication(
            space_id="space-1",
            generation=2,
            item_id="item-1",
            revision_key=revision,
            thread_id="thread-1",
            turn_id="turn-1",
        )
        latest = store.latest_plan_publication("space-1", 2)
        assert latest is not None and latest["revision_key"] == revision
        old_status = store._connection.execute(
            "SELECT status FROM plan_publications WHERE revision_key=''"
        ).fetchone()
        assert old_status is not None and old_status[0] == "superseded"
    finally:
        store.close()


def test_v7_plan_publication_migration_backfills_text_and_tui_state(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE events (
            event_key TEXT PRIMARY KEY,
            thread_id TEXT,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE TABLE plan_publications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            space_id TEXT NOT NULL,
            generation INTEGER NOT NULL,
            item_id TEXT NOT NULL,
            revision_key TEXT NOT NULL DEFAULT '',
            thread_id TEXT NOT NULL,
            turn_id TEXT NOT NULL,
            status TEXT NOT NULL,
            message_ids_json TEXT NOT NULL DEFAULT '[]',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE(space_id, generation, item_id, revision_key)
        );
        INSERT INTO events VALUES(
            'plan-event', 'thread-1', 'item/completed',
            '{"threadId":"thread-1","turnId":"turn-plan",' ||
            '"item":{"id":"item-plan","type":"plan",' ||
            '"text":"Persist this plan."}}',
            10
        );
        INSERT INTO plan_publications(
            space_id, generation, item_id, revision_key, thread_id, turn_id,
            status, message_ids_json, created_at, updated_at
        ) VALUES(
            'space-1', 1, 'item-plan', 'revision-1', 'thread-1', 'turn-plan',
            'published', '[101,102]', 10, 10
        );
        PRAGMA user_version=7;
        """
    )
    connection.close()

    store = Store(path)
    try:
        assert store.schema_version == SCHEMA_VERSION
        assert store.last_backup_path is not None
        publication = store.latest_plan_publication("space-1", 1)
        assert publication is not None
        assert publication["plan_text"] == "Persist this plan."
        assert publication["action_message_ids"] == []
        assert publication["tui_prompt_seen_at"] is None
        assert publication["decision_turn_id"] == ""

        assert store.append_plan_action_message(
            "space-1", 1, "item-plan", 103, revision_key="revision-1"
        )
        assert store.mark_tui_plan_prompt_seen(
            "space-1", 1, "item-plan", revision_key="revision-1"
        )
        assert store.record_event(
            "approval-event",
            "thread-1",
            "item/started",
            {
                "threadId": "thread-1",
                "turnId": "turn-execute",
                "item": {
                    "id": "message-1",
                    "type": "userMessage",
                    "clientId": None,
                    "content": [{"type": "text", "text": "Implement the plan."}],
                },
            },
            managed=True,
        )
        assert store.find_tui_plan_approval_turn(
            "thread-1", after=10, prompt="Implement the plan."
        ) == "turn-execute"
        assert store.mark_external_plan_action(
            "space-1",
            1,
            "item-plan",
            revision_key="revision-1",
            status="executed",
            decision_turn_id="turn-execute",
            expected_statuses={"published"},
        )
        publication = store.latest_plan_publication("space-1", 1)
        assert publication is not None
        assert publication["action_message_ids"] == [103]
        assert publication["tui_prompt_seen_at"] is not None
        assert publication["decision_turn_id"] == "turn-execute"
    finally:
        store.close()


def test_plan_publication_revision_key_deduplicates_events_but_allows_updates(
    tmp_path: Path,
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        common = {
            "space_id": "space-1",
            "generation": 1,
            "item_id": "item-plan",
            "thread_id": "thread-1",
            "turn_id": "turn-1",
        }
        first = plan_revision_key("turn-1", "First plan")
        second = plan_revision_key("turn-1", "Second plan")
        assert store.claim_plan_publication(**common, revision_key=first)
        assert store.finish_plan_publication(
            space_id="space-1",
            generation=1,
            item_id="item-plan",
            revision_key=first,
            status="published",
            message_ids=[41],
        )
        assert not store.claim_plan_publication(**common, revision_key=first)
        assert store.claim_plan_publication(**common, revision_key=second)
        assert store.finish_plan_publication(
            space_id="space-1",
            generation=1,
            item_id="item-plan",
            revision_key=second,
            status="published",
            message_ids=[42],
        )
        assert not store.mark_plan_action(
            "space-1",
            1,
            "item-plan",
            revision_key=first,
            status="executing",
        )
        assert store.mark_plan_action(
            "space-1",
            1,
            "item-plan",
            revision_key=second,
            status="executing",
        )
    finally:
        store.close()


def test_interaction_draft_progress_and_claim_are_atomic_across_connections(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    first = Store(path)
    second = Store(path)
    try:
        draft = first.replace_interaction(
            "control:11:7:new",
            kind="new",
            phase="model",
            payload={"seed": "value"},
            user_id=7,
            bot_role="control",
            chat_id=11,
            expires_at=4_000_000_000,
        )
        assert draft.revision == 1
        assert draft.claimed_at is None

        advanced = second.advance_interaction(
            draft.scope_key,
            draft.flow_id,
            draft.revision,
            phase="effort",
            payload={"model": "gpt-5.6-luna"},
            expires_at=4_000_000_001,
        )
        assert advanced is not None
        assert advanced.revision == 2
        assert advanced.phase == "effort"
        assert advanced.payload == {"model": "gpt-5.6-luna"}
        assert (
            first.advance_interaction(
                draft.scope_key,
                draft.flow_id,
                draft.revision,
                phase="stale",
                payload={},
                expires_at=4_000_000_002,
            )
            is None
        )

        claimed = first.claim_interaction(
            advanced.scope_key,
            advanced.flow_id,
            advanced.revision,
        )
        assert claimed is not None
        assert claimed.claimed_at is not None
        assert (
            second.claim_interaction(
                advanced.scope_key,
                advanced.flow_id,
                advanced.revision,
            )
            is None
        )
        assert second.get_interaction(advanced.scope_key) is None
    finally:
        first.close()
        second.close()


def test_interaction_claims_enforce_expiry_inside_the_atomic_update(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    first = Store(path)
    second = Store(path)
    now = int(time.time())
    try:
        expired = first.replace_interaction(
            "control:11:7:expired",
            kind="new",
            phase="prompt",
            payload={"cwd": str(tmp_path)},
            user_id=7,
            bot_role="control",
            chat_id=11,
            expires_at=now - 1,
        )
        assert (
            first.advance_interaction(
                expired.scope_key,
                expired.flow_id,
                expired.revision,
                phase="revived",
                payload={},
                expires_at=now + 60,
            )
            is None
        )
        assert (
            first.claim_live_interaction(
                expired.scope_key, expired.flow_id, expired.revision
            )
            is None
        )
        assert (
            second.claim_expired_interaction(
                expired.scope_key, expired.flow_id, expired.revision
            )
            is not None
        )

        live = first.replace_interaction(
            "control:11:7:live",
            kind="new",
            phase="prompt",
            payload={"cwd": str(tmp_path)},
            user_id=7,
            bot_role="control",
            chat_id=11,
            expires_at=now + 60,
        )
        assert (
            second.claim_expired_interaction(
                live.scope_key, live.flow_id, live.revision
            )
            is None
        )
        assert (
            first.claim_live_interaction(live.scope_key, live.flow_id, live.revision)
            is not None
        )
    finally:
        first.close()
        second.close()


def test_reset_owner_deletes_interaction_drafts(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        store.replace_interaction(
            "discussion:22:8:planmode",
            kind="planmode",
            phase="prompt",
            payload={"model": "gpt-5.6-luna", "effort": "max"},
            user_id=8,
            bot_role="discussion",
            chat_id=22,
            expires_at=4_000_000_000,
            space_id="space-1",
            generation=2,
        )
        assert len(store.list_interactions("planmode")) == 1

        store.reset_owner()

        assert store.list_interactions() == []
    finally:
        store.close()


def test_question_resolution_is_first_writer_wins_and_popped_once(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    first = Store(path)
    second = Store(path)
    try:
        assert first.save_question_resolution(
            "request-1", {"question": ["Telegram answer"]}, source="telegram"
        )
        assert not second.save_question_resolution(
            "request-1", {"question": ["Terminal answer"]}, source="terminal"
        )

        resolution = second.pop_question_resolution("request-1")
        assert resolution is not None
        assert resolution["answers"] == {"question": ["Telegram answer"]}
        assert resolution["source"] == "telegram"
        assert first.pop_question_resolution("request-1") is None
    finally:
        first.close()
        second.close()


def test_prompt_run_claim_and_completion_are_idempotent_across_connections(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    first = Store(path)
    second = Store(path)
    try:
        assert first.put_prompt_run(
            "run-1",
            space_id="space-1",
            generation=2,
            thread_id="thread-1",
            turn_id="turn-1",
            client_message_id="client-1",
        )
        assert not second.put_prompt_run(
            "run-2",
            space_id="space-1",
            generation=2,
            thread_id="thread-1",
            turn_id="turn-1",
            client_message_id="client-1",
        )

        completed = second.finish_prompt_runs(
            "thread-1", "turn-1", status="completed"
        )
        assert len(completed) == 1
        assert completed[0] == {
            "run_id": "run-1",
            "space_id": "space-1",
            "generation": 2,
            "thread_id": "thread-1",
            "turn_id": "turn-1",
            "client_message_id": "client-1",
            "status": "completed",
            "error_kind": "",
            "created_at": completed[0]["created_at"],
            "updated_at": completed[0]["updated_at"],
        }
        assert first.finish_prompt_runs("thread-1", "turn-1", status="completed") == []
        with pytest.raises(ValueError, match="terminal status"):
            first.finish_prompt_runs("thread-1", "turn-1", status="running")
    finally:
        first.close()
        second.close()


def test_plan_publication_state_machine_rejects_stale_and_duplicate_actions(
    tmp_path: Path,
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        common = {"space_id": "space-1", "generation": 3, "thread_id": "thread-1"}
        assert store.claim_plan_publication(
            **common, item_id="plan-1", turn_id="turn-1"
        )
        assert not store.claim_plan_publication(
            **common, item_id="plan-1", turn_id="turn-1"
        )
        assert store.finish_plan_publication(
            space_id="space-1",
            generation=3,
            item_id="plan-1",
            status="published",
            message_ids=[101, 102],
        )
        assert not store.finish_plan_publication(
            space_id="space-1",
            generation=3,
            item_id="plan-1",
            status="failed",
            message_ids=[],
        )

        assert store.claim_plan_publication(
            **common, item_id="plan-2", turn_id="turn-2"
        )
        assert not store.claim_plan_publication(
            **common, item_id="plan-1", turn_id="turn-1"
        )
        latest = store.latest_plan_publication("space-1", 3)
        assert latest is not None
        assert (latest["item_id"], latest["status"]) == ("plan-2", "publishing")
        assert store.finish_plan_publication(
            space_id="space-1",
            generation=3,
            item_id="plan-2",
            status="published",
            message_ids=[103],
        )
        assert not store.mark_plan_action("space-1", 3, "plan-1", status="executing")
        assert store.mark_plan_action("space-1", 3, "plan-2", status="revising")
        assert not store.mark_plan_action("space-1", 3, "plan-2", status="executing")
        assert store.complete_plan_action(
            "space-1",
            3,
            "plan-2",
            expected_status="revising",
            status="revision_started",
        )
        assert store.recoverable_plan_publications() == []
        with pytest.raises(ValueError, match="action status"):
            store.mark_plan_action("space-1", 3, "plan-2", status="published")

        assert store.claim_plan_publication(
            **common, item_id="plan-3", turn_id="turn-3"
        )
        assert store.finish_plan_publication(
            space_id="space-1",
            generation=3,
            item_id="plan-3",
            status="failed",
            message_ids=[104],
        )
        assert store.claim_plan_publication(
            **common, item_id="plan-3", turn_id="turn-3-retry"
        )
        assert not store.claim_plan_publication(
            **common, item_id="plan-3", turn_id="turn-3-retry"
        )
        assert store.claim_plan_publication(
            **common,
            item_id="plan-3",
            turn_id="turn-3-stale-retry",
            stale_after=0,
        )
    finally:
        store.close()


def test_cleanup_removes_expired_interaction_state_but_keeps_live_plan(
    tmp_path: Path,
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        store.create_space({"space_id": "active-space"})
        assert store.save_question_resolution("old-resolution", {}, source="terminal")
        assert store.save_question_resolution("new-resolution", {}, source="telegram")
        assert store.put_prompt_run(
            "old-run",
            space_id="missing-space",
            generation=1,
            thread_id="thread-old",
            turn_id="turn-old",
            client_message_id="client-old",
        )
        assert store.claim_plan_publication(
            space_id="missing-space",
            generation=1,
            item_id="old-plan",
            thread_id="thread-old",
            turn_id="turn-old",
        )
        assert store.finish_plan_publication(
            space_id="missing-space",
            generation=1,
            item_id="old-plan",
            status="published",
            message_ids=[1],
        )
        assert store.claim_plan_publication(
            space_id="active-space",
            generation=1,
            item_id="old-active-plan",
            thread_id="thread-live",
            turn_id="turn-old-active",
        )
        assert store.finish_plan_publication(
            space_id="active-space",
            generation=1,
            item_id="old-active-plan",
            status="published",
            message_ids=[2],
        )
        assert store.mark_plan_action(
            "active-space", 1, "old-active-plan", status="executing"
        )
        assert store.claim_plan_publication(
            space_id="active-space",
            generation=1,
            item_id="live-plan",
            thread_id="thread-live",
            turn_id="turn-live",
        )
        assert store.finish_plan_publication(
            space_id="active-space",
            generation=1,
            item_id="live-plan",
            status="published",
            message_ids=[3],
        )
        with store._lock, store._connection:
            store._connection.execute(
                "UPDATE question_resolutions SET resolved_at=1 WHERE request_key='old-resolution'"
            )
            store._connection.execute(
                "UPDATE prompt_runs SET updated_at=1 WHERE run_id='old-run'"
            )
            store._connection.execute("UPDATE plan_publications SET updated_at=1")

        store.cleanup(event_days=1)

        assert store.pop_question_resolution("old-resolution") is None
        assert store.pop_question_resolution("new-resolution") is not None
        assert store.latest_plan_publication("missing-space", 1) is None
        live = store.latest_plan_publication("active-space", 1)
        assert live is not None and live["status"] == "published"
        assert (
            store._connection.execute(
                "SELECT COUNT(*) FROM plan_publications WHERE item_id='old-active-plan'"
            ).fetchone()[0]
            == 0
        )
        assert store.finish_prompt_runs("thread-old", "turn-old", status="completed") == []
    finally:
        store.close()


def test_bot_update_ids_are_isolated_by_role(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        assert store.claim_telegram_update(42, "control")
        assert store.claim_telegram_update(42, "forum")
        assert not store.claim_telegram_update(42, "control")
        assert store.telegram_update_seen(42, "forum")
    finally:
        store.close()


def test_session_space_reconciles_early_discussion_root_and_freezes_generation(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        store.record_discussion_root(-1001, 9, -1002, 19)
        created = store.create_space(
            {
                "space_id": "space-1",
                "thread_id": "thread-1",
                "channel_chat_id": -1001,
                "channel_post_id": 9,
            }
        )
        bound = store.bind_space_messages(
            "space-1", channel_chat_id=-1001, channel_post_id=9, expected_generation=1
        )
        assert bound is not None
        assert bound["discussion_chat_id"] == -1002
        assert bound["discussion_root_id"] == 19
        assert store.get_space_by_thread("thread-1") == bound

        store.record_discussion_message(-1002, 25, 19, "space-1")
        assert store.resolve_discussion_root(-1002, 25) == {
            "root_message_id": 19,
            "space_id": "space-1",
        }

        store.put_callback(
            "space-action",
            "run",
            {},
            7,
            4_000_000_000,
            bot_role="forum",
            chat_id=-1002,
            space_id="space-1",
            generation=1,
        )
        assert store.peek_callback(
            "space-action",
            7,
            bot_role="forum",
            chat_id=-1002,
            space_id="space-1",
            generation=1,
        ) == ("run", {})
        assert store.peek_callback(
            "space-action",
            7,
            bot_role="forum",
            chat_id=-1002,
            space_id="wrong-space",
            generation=1,
        ) is None
        store.enqueue_prompt(
            "thread-1", "queued", [], "space-client", space_id="space-1", generation=1
        )
        closed = store.close_space("space-1", expected_generation=1)
        assert closed is not None and closed["generation"] == 2
        assert store.consume_callback(
            "space-action", 7, bot_role="forum", space_id="space-1", generation=1
        ) is None
        assert store.space_queue_entries("space-1", 1) == []

        model = store.get_session_space("space-1")
        assert isinstance(model, SessionSpace)
        assert model.lifecycle == "closed"
        assert created["generation"] == 1
    finally:
        store.close()


def test_reset_space_transport_preserves_thread_and_invalidates_old_messages(
    tmp_path: Path,
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        state = ThreadState(
            thread_id="thread-1",
            status="active",
            goal={"objective": "Preserve me", "status": "active"},
        )
        store.save_thread(state)
        store.subscribe("thread-1")
        store.create_space(
            {
                "space_id": "space-reset",
                "space_type": "existing",
                "lifecycle": "active",
                "thread_id": "thread-1",
                "channel_chat_id": -1001,
                "channel_post_id": 9,
                "discussion_chat_id": -1002,
                "discussion_root_id": 19,
                "status_message_id": 20,
            }
        )
        store.record_discussion_root(-1001, 9, -1002, 19)
        store.record_discussion_message(-1002, 19, 19, "space-reset")
        store.record_discussion_message(-1002, 20, 19, "space-reset")
        store.record_question_message("request-1", "discussion", -1002, 20)
        store.schedule_message_deletions("discussion", -1002, [20], 4_000_000_000)
        store.schedule_message_deletions("control", -1001, [9], 4_000_000_000)
        store.put_callback(
            "old-space-action",
            "run",
            {},
            7,
            4_000_000_000,
            bot_role="discussion",
            chat_id=-1002,
            space_id="space-reset",
            generation=1,
        )
        store.record_discussion_root(-2001, 29, -2002, 39)
        store.record_discussion_message(-2002, 40, 39, "space-other")
        store.put_callback(
            "other-space-action",
            "run",
            {},
            7,
            4_000_000_000,
            bot_role="discussion",
            chat_id=-2002,
            space_id="space-other",
            generation=1,
        )

        assert store.reset_space_transport("space-reset", expected_generation=2) is None
        assert store.reset_space_transport("missing", expected_generation=1) is None

        reset = store.reset_space_transport("space-reset", expected_generation=1)

        assert reset is not None
        assert reset["thread_id"] == "thread-1"
        assert reset["lifecycle"] == "repair_required"
        assert reset["generation"] == 2
        assert reset["channel_chat_id"] == -1001
        assert reset["discussion_chat_id"] == -1002
        assert reset["channel_post_id"] is None
        assert reset["discussion_root_id"] is None
        assert reset["status_message_id"] is None
        assert store.get_thread("thread-1") == state
        assert store.subscriptions() == {"thread-1": None}
        assert store.get_discussion_root(-1001, 9) is None
        assert store.resolve_discussion_root(-1002, 19) is None
        assert store.resolve_discussion_root(-1002, 20) is None
        assert store.question_messages("request-1") == []
        assert store.due_message_deletions(now=4_000_000_000) == []
        assert store.peek_callback(
            "old-space-action",
            7,
            bot_role="discussion",
            chat_id=-1002,
            space_id="space-reset",
            generation=1,
        ) is None
        assert store.get_discussion_root(-2001, 29) == {
            "discussion_chat_id": -2002,
            "root_message_id": 39,
        }
        assert store.resolve_discussion_root(-2002, 40) == {
            "root_message_id": 39,
            "space_id": "space-other",
        }
        assert store.peek_callback(
            "other-space-action",
            7,
            bot_role="discussion",
            chat_id=-2002,
            space_id="space-other",
            generation=1,
        ) == ("run", {})
    finally:
        store.close()


def test_reset_space_transport_blocks_queued_prompts_and_pending_inputs(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        store.create_space(
            {
                "space_id": "space-busy",
                "space_type": "existing",
                "lifecycle": "active",
                "thread_id": "thread-busy",
                "channel_chat_id": -1001,
                "channel_post_id": 9,
                "discussion_chat_id": -1002,
                "discussion_root_id": 19,
                "status_message_id": 20,
            }
        )
        queue_id = store.enqueue_prompt(
            "thread-busy",
            "queued",
            [],
            "queued-client",
        )

        with pytest.raises(RuntimeError, match="排队 prompt"):
            store.reset_space_transport("space-busy", expected_generation=1)
        assert store.cancel_prompt(queue_id)

        store.put_pending_input(
            "request-busy",
            "1",
            1,
            "thread-busy",
            "turn-1",
            "item-1",
            [{"id": "answer", "question": "Continue?"}],
            None,
        )
        with pytest.raises(RuntimeError, match="待回答问题"):
            store.reset_space_transport("space-busy", expected_generation=1)

        current = store.get_space("space-busy")
        assert current is not None
        assert (current["generation"], current["channel_post_id"]) == (1, 9)
    finally:
        store.close()


@pytest.mark.parametrize("flag", ["waitingOnApproval", "waitingOnUserInput"])
def test_reset_space_transport_blocks_interactive_thread_flags(
    tmp_path: Path, flag: str
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        store.save_thread(ThreadState(thread_id="thread-interactive", active_flags=[flag]))
        store.create_space(
            {
                "space_id": "space-interactive",
                "space_type": "existing",
                "lifecycle": "active",
                "thread_id": "thread-interactive",
                "channel_chat_id": -1001,
                "channel_post_id": 9,
                "discussion_chat_id": -1002,
                "discussion_root_id": 19,
                "status_message_id": 20,
            }
        )

        with pytest.raises(RuntimeError, match="等待审批或回答"):
            store.reset_space_transport("space-interactive", expected_generation=1)

        current = store.get_space("space-interactive")
        assert current is not None
        assert (current["generation"], current["channel_post_id"]) == (1, 9)
    finally:
        store.close()


def test_reset_space_transport_removes_root_only_mapping(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        store.create_space(
            {
                "space_id": "space-root-only",
                "space_type": "existing",
                "lifecycle": "repair_required",
                "thread_id": "thread-root-only",
                "channel_chat_id": -1001,
                "discussion_chat_id": -1002,
                "discussion_root_id": 19,
            }
        )
        store.record_discussion_root(-1001, 9, -1002, 19)

        reset = store.reset_space_transport("space-root-only", expected_generation=1)

        assert reset is not None and reset["generation"] == 2
        assert store.get_discussion_root(-1001, 9) is None
    finally:
        store.close()


def test_space_provision_attempt_claim_is_due_bounded_and_model_safe(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    now = time.time()
    try:
        store.create_space({"space_id": "space-retry", "thread_id": "thread-1"})

        first = store.claim_space_provision_attempt(
            "space-retry", "channel_post", max_attempts=2, now=now
        )
        assert first is not None and first["provision_attempts"] == 1
        store.update_space(
            "space-retry",
            {"provision_retry_at": now + 30},
            expected_generation=int(first["generation"]),
        )

        assert (
            store.claim_space_provision_attempt(
                "space-retry", "channel_post", max_attempts=2, now=now + 29
            )
            is None
        )
        second = store.claim_space_provision_attempt(
            "space-retry", "channel_post", max_attempts=2, now=now + 30
        )
        assert second is not None and second["provision_attempts"] == 2
        assert (
            store.claim_space_provision_attempt(
                "space-retry", "channel_post", max_attempts=2, now=now + 31
            )
            is None
        )

        model = store.get_session_space("space-retry")
        assert model is not None
        assert (model.provision_stage, model.provision_attempts) == ("channel_post", 2)
    finally:
        store.close()


def test_persistent_deletions_and_question_messages(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        identifiers = store.schedule_message_deletions(
            "control", 11, [101, 102], 50, group_key="perf:1"
        )
        assert len(identifiers) == 2
        assert [item["message_id"] for item in store.due_message_deletions(now=50)] == [101, 102]
        assert store.complete_message_deletion(identifiers[0])
        assert store.reschedule_message_deletion(identifiers[1], 70, "temporary")
        assert store.due_message_deletions(now=69) == []

        store.record_question_message("request-1", "forum", -1002, 201)
        store.record_question_message("request-1", "forum", -1002, 202)
        assert len(store.question_messages("request-1")) == 2
        assert len(store.pop_question_messages("request-1")) == 2
        assert store.question_messages("request-1") == []
    finally:
        store.close()


def test_cleanup_durably_schedules_expired_and_orphan_question_messages(
    tmp_path: Path,
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        store.put_pending_input(
            "expired", "1", 4, "thread-1", "turn-1", "item-1", [], 50
        )
        store.record_question_message("expired", "forum", -1002, 301)
        store.record_question_message("orphan", "control", 9527, 302)
        store.put_pending_input(
            "live", "2", 5, "thread-1", "turn-2", "item-2", [], int(time.time()) + 60
        )
        store.record_question_message("live", "forum", -1002, 303)
        store.put_pending_input(
            "approval:expired", "3", 6, "thread-2", "turn-3", "item-3", [], 50
        )
        store.record_question_message("approval:expired", "forum", -1002, 304, message_kind="approval")
        store.put_pending_input(
            "approval:live", "4", 7, "thread-2", "turn-4", "item-4", [], int(time.time()) + 60
        )
        store.record_question_message("approval:live", "forum", -1002, 305, message_kind="approval")

        store.cleanup()

        due = store.due_message_deletions()
        assert {item["message_id"] for item in due} == {301, 302, 304}
        assert {item["group_key"] for item in due} == {
            "question:expired",
            "question:orphan",
            "question:approval:expired",
        }
        assert store.get_pending_input("expired") is None
        assert store.question_messages("expired") == []
        assert store.question_messages("orphan") == []
        assert store.get_pending_input("live") is not None
        assert [item["message_id"] for item in store.question_messages("live")] == [303]
        assert store.get_pending_input("approval:expired") is None
        assert store.question_messages("approval:expired") == []
        assert store.get_pending_input("approval:live") is not None
        assert [item["message_id"] for item in store.question_messages("approval:live")] == [305]
    finally:
        store.close()


def test_startup_retirement_includes_unexpired_command_approvals(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        request_key = "approval:previous-runtime"
        store.put_pending_input(
            request_key,
            "91",
            8,
            "thread-approval",
            "turn-approval",
            "item-approval",
            [],
            10_000,
        )
        store.record_question_message(
            request_key,
            "discussion",
            -1002,
            306,
            message_kind="approval",
        )
        assert store.save_question_resolution(
            request_key,
            {"decision": ["accept"]},
            source="telegram",
        )

        retired = store.retire_question_requests(include_unexpired=True, now=100)

        assert retired == [request_key]
        assert store.get_pending_input(request_key) is None
        assert store.question_messages(request_key) == []
        assert store.pop_question_resolution(request_key) is None
        due = store.due_message_deletions(now=100)
        assert [(item["message_id"], item["group_key"]) for item in due] == [
            (306, f"question:{request_key}")
        ]
    finally:
        store.close()


def _create_v9_event_database(path: Path) -> None:
    now = int(time.time())
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE events (
            event_key TEXT PRIMARY KEY,
            thread_id TEXT,
            kind TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        CREATE INDEX idx_events_thread_created ON events(thread_id, created_at DESC);
        CREATE TABLE subscriptions (
            thread_id TEXT PRIMARY KEY,
            dashboard_message_id INTEGER,
            active INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL
        );
        INSERT INTO subscriptions VALUES('managed-thread', NULL, 1, 1);
        PRAGMA user_version=9;
        """
    )
    connection.executemany(
        "INSERT INTO events(event_key, thread_id, kind, payload_json, created_at) "
        "VALUES(?, ?, ?, ?, ?)",
        (
            (
                "managed-large",
                "managed-thread",
                "warning",
                json.dumps({"threadId": "managed-thread", "message": "x" * 100_000}),
                now,
            ),
            (
                "managed-approval",
                "managed-thread",
                "item/started",
                json.dumps(
                    {
                        "threadId": "managed-thread",
                        "turnId": "turn-execute",
                        "item": {
                            "type": "userMessage",
                            "clientId": None,
                            "content": [{"type": "text", "text": "Implement the plan."}],
                        },
                    }
                ),
                now,
            ),
            (
                "managed-large-nonapproval",
                "managed-thread",
                "item/started",
                json.dumps(
                    {
                        "threadId": "managed-thread",
                        "turnId": "turn-unrelated",
                        "item": {
                            "type": "userMessage",
                            "clientId": None,
                            "content": [{"type": "text", "text": "z" * 100_000}],
                        },
                    }
                ),
                now,
            ),
            (
                "unmanaged-large",
                "unmanaged-thread",
                "warning",
                json.dumps({"threadId": "unmanaged-thread", "message": "y" * 100_000}),
                now,
            ),
        ),
    )
    connection.commit()
    connection.close()


def test_v10_migration_compacts_managed_events_and_preserves_tui_fact(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    _create_v9_event_database(path)

    store = Store(path)
    try:
        assert store.schema_version == SCHEMA_VERSION
        assert store.last_backup_path is not None
        assert store._connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        tables = {
            str(row[0])
            for row in store._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "events" not in tables
        assert {
            "event_receipts",
            "timeline_events",
            "tui_plan_approvals",
        } <= tables
        assert store._connection.execute(
            "SELECT COUNT(*) FROM event_receipts"
        ).fetchone()[0] == 3
        assert store._connection.execute(
            "SELECT COUNT(*) FROM timeline_events"
        ).fetchone()[0] == 3
        assert store._connection.execute(
            "SELECT COUNT(*) FROM tui_plan_approvals"
        ).fetchone()[0] == 1
        assert store._connection.execute(
            "SELECT MAX(length(payload_json)) FROM timeline_events"
        ).fetchone()[0] < 1_000
        assert store.find_tui_plan_approval_turn(
            "managed-thread", after=0, prompt="Implement the plan."
        ) == "turn-execute"
        assert store.timeline("unmanaged-thread") == []
    finally:
        store.close()


def test_v10_migration_failure_rolls_back_and_closes_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state.sqlite3"
    _create_v9_event_database(path)

    def fail(_store: Store) -> None:
        raise RuntimeError("injected migration failure")

    monkeypatch.setattr(Store, "_migrate_events_v10", fail)
    with pytest.raises(RuntimeError, match="injected migration failure"):
        Store(path)

    connection = sqlite3.connect(path)
    try:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 9
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute(
            "SELECT COUNT(*) FROM events"
        ).fetchone()[0] == 4
    finally:
        connection.close()


def test_live_event_storage_rejects_large_nonapproval_tui_message(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        store.subscribe("managed-thread")
        assert store.record_event(
            "large-tui-message",
            "managed-thread",
            "item/started",
            {
                "threadId": "managed-thread",
                "turnId": "turn-unrelated",
                "item": {
                    "type": "userMessage",
                    "clientId": None,
                    "content": [{"type": "text", "text": "x" * 100_000}],
                },
            },
        )

        assert store.find_tui_plan_approval_turn(
            "managed-thread", after=0, prompt="Implement the plan."
        ) is None
        assert store._connection.execute(
            "SELECT COUNT(*) FROM tui_plan_approvals"
        ).fetchone()[0] == 0
        assert store._connection.execute(
            "SELECT MAX(length(payload_json)) FROM timeline_events"
        ).fetchone()[0] < 1_000
    finally:
        store.close()


def test_settings_timeline_retains_mode_profile_without_developer_instructions(
    tmp_path: Path,
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        assert store.record_event(
            "settings-event",
            "managed-thread",
            "thread/settings/updated",
            {
                "threadId": "managed-thread",
                "threadSettings": {
                    "model": "gpt-normal",
                    "effort": "high",
                    "developer_instructions": "top-level secret",
                    "collaborationMode": {
                        "mode": "plan",
                        "settings": {
                            "model": "gpt-plan",
                            "reasoning_effort": "xhigh",
                            "developer_instructions": "nested secret",
                        },
                    },
                },
            },
            managed=True,
        )

        payload = store.timeline("managed-thread")[0]["payload"]
        assert payload == {
            "threadId": "managed-thread",
            "threadSettings": {
                "model": "gpt-normal",
                "effort": "high",
                "collaborationMode": {
                    "mode": "plan",
                    "settings": {
                        "model": "gpt-plan",
                        "reasoning_effort": "xhigh",
                    },
                },
            },
        }
        assert "developer_instructions" not in json.dumps(payload)
    finally:
        store.close()


def test_cleanup_enforces_receipt_and_per_thread_timeline_caps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(store_module, "EVENT_RECEIPT_LIMIT", 5)
    monkeypatch.setattr(store_module, "TIMELINE_PER_THREAD_LIMIT", 3)
    assert store_module.MAINTENANCE_BATCH_SIZE == 500
    store = Store(tmp_path / "state.sqlite3")
    try:
        store.subscribe("managed-thread")
        for index in range(8):
            assert store.record_event(
                f"event-{index}",
                "managed-thread",
                "warning",
                {"threadId": "managed-thread", "message": str(index)},
            )

        deleted = store.cleanup()

        assert deleted["event_receipts"] == 3
        assert deleted["timeline"] == 5
        assert store._connection.execute(
            "SELECT COUNT(*) FROM event_receipts"
        ).fetchone()[0] == 5
        assert len(store.timeline("managed-thread", 20)) == 3
    finally:
        store.close()


def test_cleanup_caps_terminal_outbound_intents_without_dropping_live_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(store_module, "OUTBOUND_TERMINAL_LIMIT", 3)
    store = Store(tmp_path / "state.sqlite3")
    try:
        terminal_ids: list[str] = []
        for index in range(5):
            intent_id = store.create_outbound_intent(
                bot_role="control",
                operation=f"send-{index}",
                lane="interactive",
                chat_id=20,
                payload_fingerprint=f"fingerprint-{index}",
            )
            store.update_outbound_intent(intent_id, status="delivered", attempts=1)
            terminal_ids.append(intent_id)
        pending_id = store.create_outbound_intent(
            bot_role="control",
            operation="pending",
            lane="interactive",
            chat_id=20,
            payload_fingerprint="pending-fingerprint",
        )

        deleted = store.cleanup()

        assert deleted["outbound_intents"] == 2
        assert {row["intent_id"] for row in store.outbound_intents(limit=20)} == {
            *terminal_ids[-3:],
            pending_id,
        }
        assert store.outbound_intents(status="pending")[0]["intent_id"] == pending_id
    finally:
        store.close()


def test_startup_recovery_marks_incomplete_outbound_intents_uncertain(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        pending_id = store.create_outbound_intent(
            bot_role="control",
            operation="sendMessage",
            lane="urgent",
            chat_id=20,
            payload_fingerprint="pending",
        )
        retrying_id = store.create_outbound_intent(
            bot_role="discussion",
            operation="sendMessage",
            lane="interactive",
            chat_id=20,
            payload_fingerprint="retrying",
        )
        store.update_outbound_intent(retrying_id, status="retrying", attempts=1)

        assert store.recover_outbound_intents() == 2

        recovered = {row["intent_id"]: row for row in store.outbound_intents(limit=10)}
        assert recovered[pending_id]["status"] == "uncertain"
        assert recovered[retrying_id]["status"] == "uncertain"
        assert {row["error_type"] for row in recovered.values()} == {"ProcessRestart"}
        assert store.recover_outbound_intents() == 0
    finally:
        store.close()


def test_cleanup_lock_contention_returns_after_one_second(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    store = Store(path)
    blocker = sqlite3.connect(path)
    blocker.execute("BEGIN IMMEDIATE")
    started = time.monotonic()
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            store.cleanup()
        assert time.monotonic() - started < 2.5
    finally:
        blocker.rollback()
        blocker.close()
        store.close()


def test_health_snapshot_lock_contention_returns_after_one_second(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    store = Store(path)
    blocker = sqlite3.connect(path)
    blocker.execute("BEGIN IMMEDIATE")
    started = time.monotonic()
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            store.write_health_snapshot({"service_state": "running"})
        assert time.monotonic() - started < 2.5
    finally:
        blocker.rollback()
        blocker.close()
        store.close()


def test_v11_migrates_legacy_space_modes_and_adds_state_tables(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE session_spaces(
            space_id TEXT PRIMARY KEY, thread_id TEXT, lifecycle TEXT NOT NULL,
            generation INTEGER NOT NULL, channel_chat_id INTEGER, channel_post_id INTEGER,
            discussion_chat_id INTEGER, discussion_root_id INTEGER, status_message_id INTEGER,
            state_json TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
        );
        CREATE TABLE prompt_queue(
            id INTEGER PRIMARY KEY AUTOINCREMENT, thread_id TEXT NOT NULL, prompt TEXT NOT NULL,
            inputs_json TEXT NOT NULL, client_message_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'queued', created_at INTEGER NOT NULL,
            dispatched_at INTEGER, space_id TEXT, generation INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE pending_inputs(
            request_key TEXT PRIMARY KEY, request_id TEXT NOT NULL, generation INTEGER NOT NULL,
            thread_id TEXT NOT NULL, turn_id TEXT NOT NULL, item_id TEXT NOT NULL,
            questions_json TEXT NOT NULL, expires_at INTEGER, created_at INTEGER NOT NULL
        );
        PRAGMA user_version=10;
        """
    )
    for index, lifecycle in enumerate(("active", "pending", "closed"), start=1):
        value = {
            "space_id": f"space-{lifecycle}",
            "thread_id": "thread" if lifecycle == "active" else None,
            "lifecycle": lifecycle,
            "generation": index,
            "current_mode": "plan" if lifecycle == "active" else "default",
            "created_at": 1,
            "updated_at": 1,
        }
        connection.execute(
            "INSERT INTO session_spaces(space_id, thread_id, lifecycle, generation, state_json, "
            "created_at, updated_at) VALUES(?, ?, ?, ?, ?, 1, 1)",
            (value["space_id"], value["thread_id"], lifecycle, index, json.dumps(value)),
        )
    connection.commit()
    connection.close()

    store = Store(path)
    try:
        assert store.schema_version == 11
        assert store.last_backup_path is not None
        for lifecycle in ("active", "pending", "closed"):
            space = store.get_space(f"space-{lifecycle}")
            assert space is not None
            assert space["desired_mode"] == space["current_mode"]
            assert space["observed_mode"] == "unknown"
        tables = {
            row[0]
            for row in store._connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert {"prompt_intents", "telegram_message_state"} <= tables
        assert {"status", "claimed_at", "responded_at", "resolved_at"} <= {
            row[1] for row in store._connection.execute("PRAGMA table_info(pending_inputs)")
        }
    finally:
        store.close()


def test_space_mode_roundtrip_honors_legacy_current_mode_writer(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        created = store.create_space(
            {
                "space_id": "space-mode",
                "lifecycle": "pending",
                "generation": 1,
                "current_mode": "default",
            }
        )
        assert (created["desired_mode"], created["observed_mode"]) == ("default", "unknown")
        updated = store.update_space("space-mode", {"current_mode": "plan"})
        assert updated is not None
        assert (updated["current_mode"], updated["desired_mode"]) == ("plan", "plan")
        assert store.get_session_space("space-mode").observed_mode == "unknown"  # type: ignore[union-attr]
    finally:
        store.close()


def test_prompt_intent_cas_choice_collision_and_queue_linkage(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        intent = store.create_prompt_intent(
            "client-1", "space", "work", "auto", thread_id="thread", space_id="space", generation=2
        )
        assert intent.state == "received"
        same = store.create_prompt_intent(
            "client-1", "space", "work", "auto", thread_id="thread", space_id="space", generation=2
        )
        assert same.intent_id == intent.intent_id
        with pytest.raises(ValueError, match="collision"):
            store.create_prompt_intent(
                "client-1", "space", "other", "auto", thread_id="thread", space_id="space", generation=2
            )
        assert store.transition_prompt_intent(
            "client-1", expected_states={"received"}, to_state="awaiting_choice"
        )
        chosen = store.resolve_prompt_intent_choice("client-1", mode="queue")
        assert chosen is not None and chosen.mode == "queue"
        assert store.resolve_prompt_intent_choice("client-1", mode="steer") is None
        queue_id = store.enqueue_prompt(
            "thread", "work", [{"type": "text", "text": "work"}], "client-1",
            space_id="space", generation=2, prompt_intent_id=intent.intent_id,
        )
        queued = store.transition_prompt_intent(
            "client-1", expected_states={"awaiting_choice"}, to_state="queued", queue_id=queue_id
        )
        assert queued is not None and queued.queue_id == queue_id
        assert store.next_prompt("thread").prompt_intent_id == intent.intent_id  # type: ignore[union-attr]
    finally:
        store.close()


def test_pending_input_lifecycle_and_telegram_message_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = Store(tmp_path / "state.sqlite3")
    try:
        store.put_pending_input("request", "1", 1, "thread", "turn", "item", [], None)
        assert store.claim_pending_input("request")["status"] == "claimed"  # type: ignore[index]
        assert store.claim_pending_input("request") is None
        assert store.mark_pending_input_responded("request", {"decision": "accept"})
        assert store.get_pending_input("request")["status"] == "awaiting_resolved"  # type: ignore[index]
        assert store.resolve_pending_input("request", source="serverRequest/resolved")
        assert store.get_pending_input("request") is None

        old = int(time.time()) - 8 * 86400
        monkeypatch.setattr(time, "time", lambda: old)
        store.put_telegram_message_state(
            "message", bot_role="discussion", chat_id=1, message_id=2,
            semantic_fingerprint="abc", state="sent", payload={"kind": "prompt"},
        )
        monkeypatch.undo()
        deleted = store.cleanup(event_days=7)
        assert deleted["telegram_message_state"] == 1
        assert store.get_telegram_message_state("message") is None
    finally:
        store.close()
