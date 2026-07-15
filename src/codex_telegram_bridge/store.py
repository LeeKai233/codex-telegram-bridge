from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .config import ensure_private_directory
from .models import Owner, QueuedPrompt, SessionSpace, ThreadState

SCHEMA_VERSION = 4
CONTROL_BOT_ROLE = "control"

SCHEMA = """
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS owner (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    user_id INTEGER NOT NULL,
    chat_id INTEGER NOT NULL,
    username TEXT,
    paired_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS threads (
    thread_id TEXT PRIMARY KEY,
    state_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS subscriptions (
    thread_id TEXT PRIMARY KEY,
    dashboard_message_id INTEGER,
    active INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    event_key TEXT PRIMARY KEY,
    thread_id TEXT,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_thread_created ON events(thread_id, created_at DESC);
CREATE TABLE IF NOT EXISTS prompt_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    thread_id TEXT NOT NULL,
    prompt TEXT NOT NULL,
    inputs_json TEXT NOT NULL,
    client_message_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'queued',
    created_at INTEGER NOT NULL,
    dispatched_at INTEGER,
    space_id TEXT,
    generation INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_prompt_queue_thread ON prompt_queue(thread_id, status, id);
CREATE TABLE IF NOT EXISTS callbacks (
    nonce TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    used_at INTEGER,
    bot_role TEXT NOT NULL DEFAULT 'control',
    chat_id INTEGER,
    space_id TEXT,
    generation INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS pending_inputs (
    request_key TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    questions_json TEXT NOT NULL,
    expires_at INTEGER,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS recovery_codes (
    code_hash TEXT PRIMARY KEY,
    salt TEXT NOT NULL,
    used_at INTEGER
);
CREATE TABLE IF NOT EXISTS telegram_updates (
    bot_role TEXT NOT NULL DEFAULT 'control',
    update_id INTEGER NOT NULL,
    received_at INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    PRIMARY KEY(bot_role, update_id)
);
CREATE INDEX IF NOT EXISTS idx_telegram_updates_received ON telegram_updates(received_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_telegram_updates_sequence ON telegram_updates(sequence);
CREATE TABLE IF NOT EXISTS telegram_binding (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    binding_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS session_spaces (
    space_id TEXT PRIMARY KEY,
    thread_id TEXT,
    lifecycle TEXT NOT NULL,
    generation INTEGER NOT NULL,
    channel_chat_id INTEGER,
    channel_post_id INTEGER,
    discussion_chat_id INTEGER,
    discussion_root_id INTEGER,
    status_message_id INTEGER,
    state_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_session_spaces_thread ON session_spaces(thread_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_session_spaces_channel_post
    ON session_spaces(channel_chat_id, channel_post_id);
CREATE INDEX IF NOT EXISTS idx_session_spaces_discussion_root
    ON session_spaces(discussion_chat_id, discussion_root_id);
CREATE TABLE IF NOT EXISTS discussion_roots (
    channel_chat_id INTEGER NOT NULL,
    channel_post_id INTEGER NOT NULL,
    discussion_chat_id INTEGER NOT NULL,
    root_message_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY(channel_chat_id, channel_post_id),
    UNIQUE(discussion_chat_id, root_message_id)
);
CREATE TABLE IF NOT EXISTS discussion_messages (
    discussion_chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    root_message_id INTEGER NOT NULL,
    space_id TEXT,
    created_at INTEGER NOT NULL,
    PRIMARY KEY(discussion_chat_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_discussion_messages_root
    ON discussion_messages(discussion_chat_id, root_message_id);
CREATE TABLE IF NOT EXISTS scheduled_deletions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bot_role TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    delete_at INTEGER NOT NULL,
    group_key TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    UNIQUE(bot_role, chat_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_scheduled_deletions_due ON scheduled_deletions(delete_at, id);
CREATE TABLE IF NOT EXISTS question_messages (
    request_key TEXT NOT NULL,
    bot_role TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY(request_key, bot_role, chat_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_question_messages_request ON question_messages(request_key);
"""


def _schema_statements() -> list[str]:
    return [statement.strip() for statement in SCHEMA.split(";") if statement.strip()]


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def _json_mapping(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), ensure_ascii=False, separators=(",", ":"))


class Store:
    def __init__(self, path: Path) -> None:
        ensure_private_directory(path.parent)
        self.path = path
        self.last_backup_path: Path | None = None
        self._lock = threading.RLock()
        self._closed = False
        had_database = path.is_file() and path.stat().st_size > 0
        self._connection = sqlite3.connect(path, check_same_thread=False, timeout=30.0)
        self._connection.row_factory = sqlite3.Row
        with self._lock:
            self._connection.execute("PRAGMA busy_timeout=30000")
            self._connection.execute("PRAGMA foreign_keys=ON")
            self._migrate(had_database=had_database)
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute("PRAGMA synchronous=FULL")
            self._connection.commit()
        os.chmod(path, 0o600)

    @property
    def schema_version(self) -> int:
        with self._lock:
            return int(self._connection.execute("PRAGMA user_version").fetchone()[0])

    def _backup_before_migration(self, old_version: int) -> Path:
        timestamp = time.strftime("%Y%m%dT%H%M%S", time.localtime())
        backup = self.path.with_name(
            f"{self.path.name}.pre-v{old_version}-to-v{SCHEMA_VERSION}.{timestamp}.{time.time_ns()}.bak"
        )
        descriptor = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(descriptor)
        destination = sqlite3.connect(backup)
        try:
            self._connection.backup(destination)
            destination.commit()
        except BaseException:
            backup.unlink(missing_ok=True)
            raise
        finally:
            destination.close()
        os.chmod(backup, 0o600)
        self.last_backup_path = backup
        return backup

    def _migrate(self, *, had_database: bool) -> None:
        current = int(self._connection.execute("PRAGMA user_version").fetchone()[0])
        if current > SCHEMA_VERSION:
            raise RuntimeError(
                f"State database schema {current} is newer than supported schema {SCHEMA_VERSION}"
            )
        if current == SCHEMA_VERSION:
            return
        has_tables = self._connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1"
        ).fetchone()
        if had_database and has_tables:
            self._backup_before_migration(current)

        self._connection.execute("BEGIN IMMEDIATE")
        try:
            update_columns = _columns(self._connection, "telegram_updates")
            if update_columns and "bot_role" not in update_columns:
                self._connection.execute("ALTER TABLE telegram_updates RENAME TO telegram_updates_legacy")
            for statement in _schema_statements():
                self._connection.execute(statement)
            if _columns(self._connection, "telegram_updates_legacy"):
                legacy_columns = _columns(self._connection, "telegram_updates_legacy")
                sequence = "COALESCE(sequence, rowid)" if "sequence" in legacy_columns else "rowid"
                self._connection.execute(
                    "INSERT OR IGNORE INTO telegram_updates(bot_role, update_id, received_at, sequence) "
                    f"SELECT ?, update_id, received_at, {sequence} FROM telegram_updates_legacy",
                    (CONTROL_BOT_ROLE,),
                )
                self._connection.execute("DROP TABLE telegram_updates_legacy")
                self._connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_telegram_updates_received "
                    "ON telegram_updates(received_at DESC)"
                )
                self._connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_telegram_updates_sequence "
                    "ON telegram_updates(sequence)"
                )
            prompt_columns = _columns(self._connection, "prompt_queue")
            if "space_id" not in prompt_columns:
                self._connection.execute("ALTER TABLE prompt_queue ADD COLUMN space_id TEXT")
            if "generation" not in prompt_columns:
                self._connection.execute(
                    "ALTER TABLE prompt_queue ADD COLUMN generation INTEGER NOT NULL DEFAULT 0"
                )
            callback_columns = _columns(self._connection, "callbacks")
            additions = {
                "bot_role": "TEXT NOT NULL DEFAULT 'control'",
                "chat_id": "INTEGER",
                "space_id": "TEXT",
                "generation": "INTEGER NOT NULL DEFAULT 0",
            }
            for name, declaration in additions.items():
                if name not in callback_columns:
                    self._connection.execute(f"ALTER TABLE callbacks ADD COLUMN {name} {declaration}")
            self._connection.execute(
                "INSERT INTO metadata(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (json.dumps(SCHEMA_VERSION),),
            )
            self._connection.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        except BaseException:
            self._connection.rollback()
            raise
        else:
            self._connection.commit()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._connection.close()
            self._closed = True

    @contextmanager
    def _immediate_transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                yield self._connection
            except BaseException:
                self._connection.rollback()
                raise
            else:
                self._connection.commit()

    def set_meta(self, key: str, value: str | int | float | bool) -> None:
        encoded = json.dumps(value, ensure_ascii=False)
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT INTO metadata(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, encoded),
            )

    def set_meta_many(self, values: Mapping[str, str | int | float | bool]) -> None:
        encoded = [(key, json.dumps(value, ensure_ascii=False)) for key, value in values.items()]
        with self._immediate_transaction() as connection:
            connection.executemany(
                "INSERT INTO metadata(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                encoded,
            )

    def get_meta(self, key: str, default: Any = None) -> Any:
        with self._lock:
            row = self._connection.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_owner(self, owner: Owner) -> bool:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "INSERT OR IGNORE INTO owner(singleton, user_id, chat_id, username, paired_at) "
                "VALUES(1, ?, ?, ?, ?)",
                (owner.user_id, owner.chat_id, owner.username, int(time.time())),
            )
        return cursor.rowcount == 1

    def get_owner(self) -> Owner | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT user_id, chat_id, username FROM owner WHERE singleton=1"
            ).fetchone()
        return Owner(int(row[0]), int(row[1]), row[2]) if row else None

    def set_telegram_binding(self, binding: Mapping[str, Any]) -> None:
        payload = dict(binding)
        payload["updated_at"] = int(time.time())
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT INTO telegram_binding(singleton, binding_json, updated_at) VALUES(1, ?, ?) "
                "ON CONFLICT(singleton) DO UPDATE SET "
                "binding_json=excluded.binding_json, updated_at=excluded.updated_at",
                (_json_mapping(payload), payload["updated_at"]),
            )

    def get_telegram_binding(self) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT binding_json FROM telegram_binding WHERE singleton=1"
            ).fetchone()
        if not row:
            return None
        value = json.loads(row[0])
        return value if isinstance(value, dict) else None

    def clear_telegram_binding(self) -> None:
        with self._lock, self._connection:
            self._connection.execute("DELETE FROM telegram_binding")

    @staticmethod
    def _space_from_row(row: sqlite3.Row) -> dict[str, Any]:
        value = json.loads(row[0])
        if not isinstance(value, dict):
            value = {}
        return value

    @staticmethod
    def _canonical_space(
        value: Mapping[str, Any], *, now: int, created_at: int | None = None
    ) -> dict[str, Any]:
        space = dict(value)
        space_id = str(space.get("space_id") or uuid.uuid4())
        generation = int(space.get("generation", 1))
        if generation < 1:
            raise ValueError("Session space generation must be positive")
        lifecycle = str(space.get("lifecycle") or "pending")
        if lifecycle not in {"pending", "active", "closed", "repair_required"}:
            raise ValueError(f"Unsupported session space lifecycle: {lifecycle}")
        space.update(
            {
                "space_id": space_id,
                "thread_id": str(space["thread_id"]) if space.get("thread_id") else None,
                "lifecycle": lifecycle,
                "generation": generation,
                "created_at": int(created_at if created_at is not None else space.get("created_at", now)),
                "updated_at": now,
            }
        )
        for name in (
            "channel_chat_id",
            "channel_post_id",
            "discussion_chat_id",
            "discussion_root_id",
            "status_message_id",
        ):
            space[name] = int(space[name]) if space.get(name) is not None else None
        return space

    @staticmethod
    def _write_space(connection: sqlite3.Connection, space: Mapping[str, Any]) -> None:
        connection.execute(
            "INSERT INTO session_spaces("
            "space_id, thread_id, lifecycle, generation, channel_chat_id, channel_post_id, "
            "discussion_chat_id, discussion_root_id, status_message_id, state_json, created_at, updated_at"
            ") VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(space_id) DO UPDATE SET "
            "thread_id=excluded.thread_id, lifecycle=excluded.lifecycle, generation=excluded.generation, "
            "channel_chat_id=excluded.channel_chat_id, channel_post_id=excluded.channel_post_id, "
            "discussion_chat_id=excluded.discussion_chat_id, "
            "discussion_root_id=excluded.discussion_root_id, "
            "status_message_id=excluded.status_message_id, state_json=excluded.state_json, "
            "updated_at=excluded.updated_at",
            (
                space["space_id"],
                space.get("thread_id"),
                space["lifecycle"],
                space["generation"],
                space.get("channel_chat_id"),
                space.get("channel_post_id"),
                space.get("discussion_chat_id"),
                space.get("discussion_root_id"),
                space.get("status_message_id"),
                _json_mapping(space),
                space["created_at"],
                space["updated_at"],
            ),
        )

    def create_space(self, space: Mapping[str, Any]) -> dict[str, Any]:
        now = int(time.time())
        value = self._canonical_space(space, now=now)
        with self._immediate_transaction() as connection:
            if connection.execute(
                "SELECT 1 FROM session_spaces WHERE space_id=?", (value["space_id"],)
            ).fetchone():
                raise ValueError(f"Session space already exists: {value['space_id']}")
            self._write_space(connection, value)
        return value

    def get_space(self, space_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT state_json FROM session_spaces WHERE space_id=?", (space_id,)
            ).fetchone()
        return self._space_from_row(row) if row else None

    def update_space(
        self,
        space_id: str,
        updates: Mapping[str, Any],
        *,
        expected_generation: int | None = None,
    ) -> dict[str, Any] | None:
        with self._immediate_transaction() as connection:
            row = connection.execute(
                "SELECT state_json FROM session_spaces WHERE space_id=?", (space_id,)
            ).fetchone()
            if not row:
                return None
            current = self._space_from_row(row)
            if expected_generation is not None and int(current["generation"]) != expected_generation:
                return None
            merged = dict(current)
            merged.update(updates)
            merged["space_id"] = space_id
            value = self._canonical_space(
                merged, now=int(time.time()), created_at=int(current["created_at"])
            )
            self._write_space(connection, value)
        return value

    def claim_space_provision_attempt(
        self,
        space_id: str,
        stage: str,
        *,
        max_attempts: int,
        now: float | None = None,
    ) -> dict[str, Any] | None:
        if stage not in {"channel_post", "status_comment"}:
            raise ValueError(f"Unsupported provisioning stage: {stage}")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        claimed_at = time.time() if now is None else float(now)
        with self._immediate_transaction() as connection:
            row = connection.execute(
                "SELECT state_json FROM session_spaces WHERE space_id=?", (space_id,)
            ).fetchone()
            if not row:
                return None
            current = self._space_from_row(row)
            if current.get("lifecycle") == "closed":
                return None
            if stage == "channel_post" and current.get("channel_post_id") is not None:
                return None
            if stage == "status_comment" and current.get("status_message_id") is not None:
                return None

            same_stage = current.get("provision_stage") == stage
            attempts = int(current.get("provision_attempts", 0)) if same_stage else 0
            retry_at = float(current.get("provision_retry_at", 0.0)) if same_stage else 0.0
            if attempts >= max_attempts or retry_at > claimed_at:
                return None

            merged = dict(current)
            merged.update(
                {
                    "provision_stage": stage,
                    "provision_attempts": attempts + 1,
                    "provision_retry_at": 0.0,
                }
            )
            value = self._canonical_space(
                merged,
                now=int(claimed_at),
                created_at=int(current["created_at"]),
            )
            self._write_space(connection, value)
        return value

    def list_spaces(self, lifecycle: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if lifecycle is None:
                rows = self._connection.execute(
                    "SELECT state_json FROM session_spaces ORDER BY updated_at DESC, space_id"
                ).fetchall()
            else:
                rows = self._connection.execute(
                    "SELECT state_json FROM session_spaces WHERE lifecycle=? "
                    "ORDER BY updated_at DESC, space_id",
                    (lifecycle,),
                ).fetchall()
        return [self._space_from_row(row) for row in rows]

    def get_space_by_thread(self, thread_id: str, *, include_closed: bool = False) -> dict[str, Any] | None:
        suffix = "" if include_closed else " AND lifecycle != 'closed'"
        with self._lock:
            row = self._connection.execute(
                "SELECT state_json FROM session_spaces WHERE thread_id=?" + suffix
                + " ORDER BY updated_at DESC LIMIT 1",
                (thread_id,),
            ).fetchone()
        return self._space_from_row(row) if row else None

    def get_space_by_channel_post(self, channel_chat_id: int, channel_post_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT state_json FROM session_spaces "
                "WHERE channel_chat_id=? AND channel_post_id=? ORDER BY updated_at DESC LIMIT 1",
                (int(channel_chat_id), int(channel_post_id)),
            ).fetchone()
        return self._space_from_row(row) if row else None

    def get_space_by_root(self, discussion_chat_id: int, root_message_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT state_json FROM session_spaces "
                "WHERE discussion_chat_id=? AND discussion_root_id=? ORDER BY updated_at DESC LIMIT 1",
                (int(discussion_chat_id), int(root_message_id)),
            ).fetchone()
        return self._space_from_row(row) if row else None

    def bind_space_messages(
        self,
        space_id: str,
        *,
        channel_chat_id: int,
        channel_post_id: int,
        discussion_chat_id: int | None = None,
        discussion_root_id: int | None = None,
        status_message_id: int | None = None,
        expected_generation: int | None = None,
    ) -> dict[str, Any] | None:
        if discussion_root_id is None:
            root = self.get_discussion_root(channel_chat_id, channel_post_id)
            if root:
                discussion_chat_id = int(root["discussion_chat_id"])
                discussion_root_id = int(root["root_message_id"])
        updates: dict[str, Any] = {
            "channel_chat_id": channel_chat_id,
            "channel_post_id": channel_post_id,
        }
        if discussion_chat_id is not None:
            updates["discussion_chat_id"] = discussion_chat_id
        if discussion_root_id is not None:
            updates["discussion_root_id"] = discussion_root_id
        if status_message_id is not None:
            updates["status_message_id"] = status_message_id
        return self.update_space(space_id, updates, expected_generation=expected_generation)

    def reset_space_transport(
        self, space_id: str, *, expected_generation: int
    ) -> dict[str, Any] | None:
        """Invalidate one space's Telegram messages while preserving its Codex thread."""
        with self._immediate_transaction() as connection:
            row = connection.execute(
                "SELECT state_json FROM session_spaces WHERE space_id=?", (space_id,)
            ).fetchone()
            if not row:
                return None
            current = self._space_from_row(row)
            generation = int(current["generation"])
            if generation != expected_generation:
                return None
            if current.get("lifecycle") == "closed":
                raise RuntimeError("已关闭的 SessionSpace 不能重建 Telegram 帖子")

            channel_chat_id = current.get("channel_chat_id")
            discussion_chat_id = current.get("discussion_chat_id")
            if channel_chat_id is None or discussion_chat_id is None:
                raise RuntimeError("SessionSpace 缺少已绑定的频道或讨论组")

            thread_id = current.get("thread_id")
            queue_parameters: list[Any] = [space_id]
            queue_filter = "space_id=?"
            if thread_id:
                queue_filter = "(space_id=? OR thread_id=?)"
                queue_parameters.append(str(thread_id))
            if connection.execute(
                f"SELECT 1 FROM prompt_queue WHERE {queue_filter} "
                "AND status='queued' LIMIT 1",
                queue_parameters,
            ).fetchone():
                raise RuntimeError("当前 SessionSpace 仍有排队 prompt，不能重建 Telegram 帖子")
            if thread_id and connection.execute(
                "SELECT 1 FROM pending_inputs WHERE thread_id=? LIMIT 1", (thread_id,)
            ).fetchone():
                raise RuntimeError("当前 Session 仍有待回答问题，不能重建 Telegram 帖子")
            if thread_id:
                thread_row = connection.execute(
                    "SELECT state_json FROM threads WHERE thread_id=?", (thread_id,)
                ).fetchone()
                if thread_row:
                    thread = json.loads(thread_row[0])
                    flags = {str(flag) for flag in thread.get("active_flags") or []}
                    blockers = flags & {"waitingOnApproval", "waitingOnUserInput"}
                    if blockers:
                        raise RuntimeError("当前 Session 正在等待审批或回答，不能重建 Telegram 帖子")

            discussion_root_id = current.get("discussion_root_id")
            parameters: list[Any] = [int(discussion_chat_id), space_id]
            message_filter = "discussion_chat_id=? AND space_id=?"
            if discussion_root_id is not None:
                message_filter = "discussion_chat_id=? AND (space_id=? OR root_message_id=?)"
                parameters.append(int(discussion_root_id))
            connection.execute(
                "DELETE FROM question_messages WHERE chat_id=? AND message_id IN ("
                f"SELECT message_id FROM discussion_messages WHERE {message_filter})",
                [int(discussion_chat_id), *parameters],
            )
            connection.execute(
                "DELETE FROM scheduled_deletions WHERE chat_id=? AND message_id IN ("
                f"SELECT message_id FROM discussion_messages WHERE {message_filter})",
                [int(discussion_chat_id), *parameters],
            )
            connection.execute(
                f"DELETE FROM discussion_messages WHERE {message_filter}", parameters
            )

            channel_post_id = current.get("channel_post_id")
            root_filters: list[str] = []
            root_parameters: list[int] = []
            if channel_post_id is not None:
                root_filters.append("(channel_chat_id=? AND channel_post_id=?)")
                root_parameters.extend((int(channel_chat_id), int(channel_post_id)))
                connection.execute(
                    "DELETE FROM scheduled_deletions WHERE chat_id=? AND message_id=?",
                    (int(channel_chat_id), int(channel_post_id)),
                )
            if discussion_root_id is not None:
                root_filters.append("(discussion_chat_id=? AND root_message_id=?)")
                root_parameters.extend((int(discussion_chat_id), int(discussion_root_id)))
            if root_filters:
                connection.execute(
                    "DELETE FROM discussion_roots WHERE " + " OR ".join(root_filters),
                    root_parameters,
                )

            now = int(time.time())
            connection.execute(
                "UPDATE callbacks SET used_at=? WHERE space_id=? AND used_at IS NULL",
                (now, space_id),
            )
            value = self._canonical_space(
                {
                    **current,
                    "lifecycle": "repair_required",
                    "generation": generation + 1,
                    "channel_post_id": None,
                    "discussion_root_id": None,
                    "status_message_id": None,
                    "last_error": "",
                    "provision_stage": "",
                    "provision_attempts": 0,
                    "provision_retry_at": 0.0,
                },
                now=now,
                created_at=int(current["created_at"]),
            )
            self._write_space(connection, value)
        return value

    def close_space(
        self, space_id: str, *, expected_generation: int | None = None
    ) -> dict[str, Any] | None:
        with self._immediate_transaction() as connection:
            row = connection.execute(
                "SELECT state_json FROM session_spaces WHERE space_id=?", (space_id,)
            ).fetchone()
            if not row:
                return None
            current = self._space_from_row(row)
            generation = int(current["generation"])
            if expected_generation is not None and generation != expected_generation:
                return None
            value = self._canonical_space(
                {**current, "lifecycle": "closed", "generation": generation + 1},
                now=int(time.time()),
                created_at=int(current["created_at"]),
            )
            self._write_space(connection, value)
            connection.execute(
                "UPDATE callbacks SET used_at=? "
                "WHERE space_id=? AND generation=? AND used_at IS NULL",
                (int(time.time()), space_id, generation),
            )
            connection.execute(
                "UPDATE prompt_queue SET status='cancelled' "
                "WHERE space_id=? AND generation=? AND status='queued'",
                (space_id, generation),
            )
        return value

    def record_discussion_root(
        self,
        channel_chat_id: int,
        channel_post_id: int,
        discussion_chat_id: int,
        root_message_id: int,
    ) -> None:
        now = int(time.time())
        with self._immediate_transaction() as connection:
            connection.execute(
                "INSERT INTO discussion_roots(channel_chat_id, channel_post_id, discussion_chat_id, "
                "root_message_id, created_at) VALUES(?, ?, ?, ?, ?) "
                "ON CONFLICT(channel_chat_id, channel_post_id) DO UPDATE SET "
                "discussion_chat_id=excluded.discussion_chat_id, "
                "root_message_id=excluded.root_message_id",
                (
                    int(channel_chat_id),
                    int(channel_post_id),
                    int(discussion_chat_id),
                    int(root_message_id),
                    now,
                ),
            )
            rows = connection.execute(
                "SELECT state_json FROM session_spaces WHERE channel_chat_id=? AND channel_post_id=?",
                (int(channel_chat_id), int(channel_post_id)),
            ).fetchall()
            for row in rows:
                current = self._space_from_row(row)
                value = self._canonical_space(
                    {
                        **current,
                        "discussion_chat_id": int(discussion_chat_id),
                        "discussion_root_id": int(root_message_id),
                    },
                    now=now,
                    created_at=int(current["created_at"]),
                )
                self._write_space(connection, value)

    def get_discussion_root(self, channel_chat_id: int, channel_post_id: int) -> dict[str, int] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT discussion_chat_id, root_message_id FROM discussion_roots "
                "WHERE channel_chat_id=? AND channel_post_id=?",
                (int(channel_chat_id), int(channel_post_id)),
            ).fetchone()
        if not row:
            return None
        return {"discussion_chat_id": int(row[0]), "root_message_id": int(row[1])}

    def record_discussion_message(
        self,
        discussion_chat_id: int,
        message_id: int,
        root_message_id: int,
        space_id: str | None = None,
    ) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT INTO discussion_messages(discussion_chat_id, message_id, root_message_id, "
                "space_id, created_at) VALUES(?, ?, ?, ?, ?) "
                "ON CONFLICT(discussion_chat_id, message_id) DO UPDATE SET "
                "root_message_id=excluded.root_message_id, "
                "space_id=COALESCE(excluded.space_id, discussion_messages.space_id)",
                (
                    int(discussion_chat_id),
                    int(message_id),
                    int(root_message_id),
                    space_id,
                    int(time.time()),
                ),
            )

    def resolve_discussion_root(self, discussion_chat_id: int, message_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT root_message_id, space_id FROM discussion_messages "
                "WHERE discussion_chat_id=? AND message_id=?",
                (int(discussion_chat_id), int(message_id)),
            ).fetchone()
            if not row:
                row = self._connection.execute(
                    "SELECT root_message_id, NULL FROM discussion_roots "
                    "WHERE discussion_chat_id=? AND root_message_id=?",
                    (int(discussion_chat_id), int(message_id)),
                ).fetchone()
        if not row:
            return None
        root_message_id = int(row[0])
        space_id = str(row[1]) if row[1] is not None else None
        if space_id is None:
            space = self.get_space_by_root(discussion_chat_id, root_message_id)
            space_id = str(space["space_id"]) if space else None
        return {"root_message_id": root_message_id, "space_id": space_id}

    @staticmethod
    def _session_space_model(value: Mapping[str, Any]) -> SessionSpace:
        names = {
            "space_id",
            "generation",
            "space_type",
            "lifecycle",
            "thread_id",
            "channel_chat_id",
            "channel_post_id",
            "discussion_chat_id",
            "discussion_root_id",
            "status_message_id",
            "pending_cwd",
            "pending_prompt",
            "created_at",
            "updated_at",
            "last_error",
            "provision_stage",
            "provision_attempts",
            "provision_retry_at",
        }
        return SessionSpace.from_dict({key: item for key, item in value.items() if key in names})

    def save_session_space(self, space: SessionSpace | Mapping[str, Any]) -> None:
        value = space.to_dict() if isinstance(space, SessionSpace) else dict(space)
        existing = self.get_space(str(value["space_id"]))
        if existing is None:
            self.create_space(value)
        else:
            self.update_space(str(value["space_id"]), value)

    def get_session_space(self, space_id: str) -> SessionSpace | None:
        value = self.get_space(space_id)
        return self._session_space_model(value) if value else None

    def session_space_for_thread(self, thread_id: str) -> SessionSpace | None:
        value = self.get_space_by_thread(thread_id)
        return self._session_space_model(value) if value else None

    def save_thread(self, state: ThreadState) -> None:
        payload = json.dumps(state.to_dict(), ensure_ascii=False, separators=(",", ":"))
        persisted_at = int(time.time())
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT INTO threads(thread_id, state_json, updated_at) VALUES(?, ?, ?) "
                "ON CONFLICT(thread_id) DO UPDATE SET "
                "state_json=excluded.state_json, updated_at=excluded.updated_at",
                (state.thread_id, payload, persisted_at),
            )

    def get_thread(self, thread_id: str) -> ThreadState | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT state_json FROM threads WHERE thread_id=?", (thread_id,)
            ).fetchone()
        return ThreadState.from_dict(json.loads(row[0])) if row else None

    def list_threads(self) -> list[ThreadState]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT state_json FROM threads ORDER BY updated_at DESC"
            ).fetchall()
        return [ThreadState.from_dict(json.loads(row[0])) for row in rows]

    def subscribe(self, thread_id: str, message_id: int | None = None) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT INTO subscriptions(thread_id, dashboard_message_id, active, created_at) "
                "VALUES(?, ?, 1, ?) "
                "ON CONFLICT(thread_id) DO UPDATE SET active=1, "
                "dashboard_message_id="
                "COALESCE(excluded.dashboard_message_id, subscriptions.dashboard_message_id)",
                (thread_id, message_id, int(time.time())),
            )

    def unsubscribe(self, thread_id: str) -> None:
        with self._lock, self._connection:
            self._connection.execute("UPDATE subscriptions SET active=0 WHERE thread_id=?", (thread_id,))

    def set_dashboard_message(self, thread_id: str, message_id: int) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE subscriptions SET dashboard_message_id=? WHERE thread_id=?", (message_id, thread_id)
            )

    def subscriptions(self) -> dict[str, int | None]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT thread_id, dashboard_message_id FROM subscriptions WHERE active=1"
            ).fetchall()
        return {str(row[0]): int(row[1]) if row[1] is not None else None for row in rows}

    def record_event(self, event_key: str, thread_id: str | None, kind: str, payload: dict[str, Any]) -> bool:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "INSERT OR IGNORE INTO events(event_key, thread_id, kind, payload_json, created_at) "
                "VALUES(?, ?, ?, ?, ?)",
                (event_key, thread_id, kind, json.dumps(payload, ensure_ascii=False), int(time.time())),
            )
        return cursor.rowcount == 1

    def timeline(self, thread_id: str, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT kind, payload_json, created_at FROM events WHERE thread_id=? "
                "ORDER BY created_at DESC LIMIT ?",
                (thread_id, limit),
            ).fetchall()
        return [{"kind": row[0], "payload": json.loads(row[1]), "created_at": int(row[2])} for row in rows]

    def enqueue_prompt(
        self,
        thread_id: str,
        prompt: str,
        inputs: list[dict[str, Any]],
        client_message_id: str,
        *,
        space_id: str | None = None,
        generation: int = 0,
    ) -> int:
        if generation < 0:
            raise ValueError("generation must not be negative")
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "INSERT INTO prompt_queue(thread_id, prompt, inputs_json, client_message_id, created_at, "
                "space_id, generation) VALUES(?, ?, ?, ?, ?, ?, ?)",
                (
                    thread_id,
                    prompt,
                    json.dumps(inputs, ensure_ascii=False),
                    client_message_id,
                    int(time.time()),
                    space_id,
                    generation,
                ),
            )
        return int(cursor.lastrowid)

    def next_prompt(self, thread_id: str) -> QueuedPrompt | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT id, thread_id, prompt, inputs_json, client_message_id, created_at "
                "FROM prompt_queue WHERE thread_id=? AND status='queued' ORDER BY id LIMIT 1",
                (thread_id,),
            ).fetchone()
        if not row:
            return None
        return QueuedPrompt(
            queue_id=int(row[0]),
            thread_id=str(row[1]),
            prompt=str(row[2]),
            inputs=json.loads(row[3]),
            client_message_id=str(row[4]),
            created_at=int(row[5]),
        )

    def mark_prompt_dispatched(self, queue_id: int) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE prompt_queue SET status='dispatched', dispatched_at=? WHERE id=? AND status='queued'",
                (int(time.time()), queue_id),
            )

    def mark_prompt_failed(self, queue_id: int) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE prompt_queue SET status='failed' WHERE id=? AND status='queued'", (queue_id,)
            )

    def cancel_prompt(self, queue_id: int) -> bool:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE prompt_queue SET status='cancelled' WHERE id=? AND status='queued'", (queue_id,)
            )
        return cursor.rowcount == 1

    def queue_count(self, thread_id: str) -> int:
        with self._lock:
            row = self._connection.execute(
                "SELECT COUNT(*) FROM prompt_queue WHERE thread_id=? AND status='queued'", (thread_id,)
            ).fetchone()
        return int(row[0])

    def put_callback(
        self,
        nonce: str,
        action: str,
        payload: dict[str, Any],
        user_id: int,
        expires_at: int,
        *,
        bot_role: str = CONTROL_BOT_ROLE,
        chat_id: int | None = None,
        space_id: str | None = None,
        generation: int = 0,
    ) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT INTO callbacks(nonce, action, payload_json, user_id, expires_at, bot_role, "
                "chat_id, space_id, generation) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    nonce,
                    action,
                    json.dumps(payload, ensure_ascii=False),
                    user_id,
                    expires_at,
                    bot_role,
                    chat_id,
                    space_id,
                    generation,
                ),
            )

    def consume_callback(
        self,
        nonce: str,
        user_id: int,
        *,
        bot_role: str | None = None,
        chat_id: int | None = None,
        space_id: str | None = None,
        generation: int | None = None,
    ) -> tuple[str, dict[str, Any]] | None:
        now = int(time.time())
        with self._immediate_transaction() as connection:
            row = connection.execute(
                "SELECT action, payload_json, expires_at, used_at, user_id, bot_role, chat_id, "
                "space_id, generation FROM callbacks WHERE nonce=?",
                (nonce,),
            ).fetchone()
            if not row or row[3] is not None or int(row[2]) < now or int(row[4]) != user_id:
                return None
            if bot_role is not None and str(row[5]) != bot_role:
                return None
            if chat_id is not None and (row[6] is None or int(row[6]) != int(chat_id)):
                return None
            if space_id is not None and str(row[7]) != space_id:
                return None
            if generation is not None and int(row[8]) != generation:
                return None
            cursor = connection.execute(
                "UPDATE callbacks SET used_at=? WHERE nonce=? AND used_at IS NULL", (now, nonce)
            )
            if cursor.rowcount != 1:
                return None
        return str(row[0]), json.loads(row[1])

    def peek_callback(
        self,
        nonce: str,
        user_id: int,
        *,
        bot_role: str | None = None,
        chat_id: int | None = None,
        space_id: str | None = None,
        generation: int | None = None,
    ) -> tuple[str, dict[str, Any]] | None:
        """Read a live callback without consuming it, applying the same scope checks."""
        now = int(time.time())
        with self._lock:
            row = self._connection.execute(
                "SELECT action, payload_json, expires_at, used_at, user_id, bot_role, chat_id, "
                "space_id, generation FROM callbacks WHERE nonce=?",
                (nonce,),
            ).fetchone()
        if not row or row[3] is not None or int(row[2]) < now or int(row[4]) != user_id:
            return None
        if bot_role is not None and str(row[5]) != bot_role:
            return None
        if chat_id is not None and (row[6] is None or int(row[6]) != int(chat_id)):
            return None
        if space_id is not None and str(row[7]) != space_id:
            return None
        if generation is not None and int(row[8]) != generation:
            return None
        return str(row[0]), json.loads(row[1])

    def replace_recovery_codes(self, entries: list[tuple[str, str]]) -> None:
        with self._immediate_transaction() as connection:
            connection.execute("DELETE FROM recovery_codes")
            connection.executemany("INSERT INTO recovery_codes(code_hash, salt) VALUES(?, ?)", entries)

    def unused_recovery_codes(self) -> list[tuple[str, str]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT code_hash, salt FROM recovery_codes WHERE used_at IS NULL"
            ).fetchall()
        return [(str(row[0]), str(row[1])) for row in rows]

    def consume_recovery_code(self, code_hash: str) -> bool:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE recovery_codes SET used_at=? WHERE code_hash=? AND used_at IS NULL",
                (int(time.time()), code_hash),
            )
        return cursor.rowcount == 1

    def accept_totp_timecode(self, timecode: int) -> bool:
        encoded = json.dumps(int(timecode))
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "INSERT INTO metadata(key, value) VALUES('totp_last_timecode', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value "
                "WHERE CAST(metadata.value AS INTEGER) < CAST(excluded.value AS INTEGER)",
                (encoded,),
            )
        return cursor.rowcount == 1

    def record_totp_failure(
        self,
        *,
        now: int,
        max_failures: int = 5,
        lock_seconds: int = 600,
    ) -> int:
        with self._immediate_transaction() as connection:
            rows = connection.execute(
                "SELECT key, value FROM metadata WHERE key IN ('totp_failures', 'totp_locked_until')"
            ).fetchall()
            values = {str(row[0]): json.loads(row[1]) for row in rows}
            locked_until = int(values.get("totp_locked_until", 0))
            if locked_until > now:
                return locked_until
            failures = int(values.get("totp_failures", 0)) + 1
            if failures >= max_failures:
                failures = 0
                locked_until = now + lock_seconds
            connection.executemany(
                "INSERT INTO metadata(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (
                    ("totp_failures", json.dumps(failures)),
                    ("totp_locked_until", json.dumps(locked_until)),
                ),
            )
            return locked_until

    def record_totp_success(self, *, now: int, unlock_seconds: int) -> None:
        del now, unlock_seconds
        self.set_meta_many(
            {
                "totp_failures": 0,
                "totp_locked_until": 0,
                # Space leases are deliberately process-local and never recoverable from SQLite.
                "totp_unlocked_until": 0,
                "totp_force_locked": False,
            }
        )

    def consume_pair_code(self, code: str, *, now: int | None = None, max_failures: int = 5) -> bool:
        timestamp = int(time.time()) if now is None else int(now)
        with self._immediate_transaction() as connection:
            if connection.execute("SELECT 1 FROM owner WHERE singleton=1").fetchone():
                return False
            rows = connection.execute(
                "SELECT key, value FROM metadata WHERE key IN "
                "('pair_code_salt', 'pair_code_digest', 'pair_code_expires', 'pair_code_failures')"
            ).fetchall()
            values = {str(row[0]): json.loads(row[1]) for row in rows}
            expires = int(values.get("pair_code_expires", 0))
            encoded_salt = str(values.get("pair_code_salt", ""))
            expected = str(values.get("pair_code_digest", ""))
            if not encoded_salt or not expected or expires <= timestamp:
                return False
            try:
                salt = base64.b64decode(encoded_salt, validate=True)
            except ValueError, TypeError:
                return False
            candidate = hashlib.sha256(salt + code.strip().encode("ascii", errors="ignore")).hexdigest()
            valid = hmac.compare_digest(candidate, expected)
            failures = int(values.get("pair_code_failures", 0)) + (not valid)
            updates: dict[str, str | int] = {
                "pair_code_failures": int(failures),
            }
            if valid or failures >= max_failures:
                updates.update(
                    {
                        "pair_code_digest": "",
                        "pair_code_salt": "",
                        "pair_code_expires": 0,
                        "pair_code_failures": 0,
                    }
                )
            connection.executemany(
                "INSERT INTO metadata(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [(key, json.dumps(value)) for key, value in updates.items()],
            )
            return valid

    def consume_bind_code(self, code: str, *, now: int | None = None, max_failures: int = 5) -> bool:
        timestamp = int(time.time()) if now is None else int(now)
        with self._immediate_transaction() as connection:
            rows = connection.execute(
                "SELECT key, value FROM metadata WHERE key IN "
                "('bind_code_salt', 'bind_code_digest', 'bind_code_expires', 'bind_code_failures')"
            ).fetchall()
            values = {str(row[0]): json.loads(row[1]) for row in rows}
            expires = int(values.get("bind_code_expires", 0))
            encoded_salt = str(values.get("bind_code_salt", ""))
            expected = str(values.get("bind_code_digest", ""))
            if not encoded_salt or not expected or expires <= timestamp:
                return False
            try:
                salt = base64.b64decode(encoded_salt, validate=True)
            except ValueError, TypeError:
                return False
            candidate = hashlib.sha256(salt + code.strip().encode("ascii", errors="ignore")).hexdigest()
            valid = hmac.compare_digest(candidate, expected)
            failures = int(values.get("bind_code_failures", 0)) + (not valid)
            updates: dict[str, str | int] = {"bind_code_failures": int(failures)}
            if valid or failures >= max_failures:
                updates.update(
                    {
                        "bind_code_digest": "",
                        "bind_code_salt": "",
                        "bind_code_expires": 0,
                        "bind_code_failures": 0,
                    }
                )
            connection.executemany(
                "INSERT INTO metadata(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [(key, json.dumps(value)) for key, value in updates.items()],
            )
            return valid

    def auth_epoch(self) -> int:
        return int(self.get_meta("totp_auth_epoch", 0))

    def increment_auth_epoch(self) -> int:
        with self._immediate_transaction() as connection:
            row = connection.execute(
                "SELECT value FROM metadata WHERE key='totp_auth_epoch'"
            ).fetchone()
            epoch = int(json.loads(row[0])) + 1 if row else 1
            connection.executemany(
                "INSERT INTO metadata(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (
                    ("totp_auth_epoch", json.dumps(epoch)),
                    ("totp_force_locked", json.dumps(True)),
                    ("totp_unlocked_until", json.dumps(0)),
                ),
            )
        return epoch

    def cleanup(
        self,
        *,
        event_days: int = 30,
        update_days: int = 30,
        keep_recent_updates: int = 1000,
    ) -> None:
        if event_days <= 0 or update_days <= 0 or keep_recent_updates < 0:
            raise ValueError("Cleanup limits are invalid")
        now = int(time.time())
        self.retire_question_requests(now=now)
        with self._lock, self._connection:
            self._connection.execute(
                "DELETE FROM callbacks WHERE expires_at < ? OR used_at IS NOT NULL", (now,)
            )
            self._connection.execute("DELETE FROM events WHERE created_at < ?", (now - event_days * 86400,))
            self._connection.execute(
                "DELETE FROM telegram_updates WHERE received_at < ? "
                "AND sequence NOT IN ("
                "SELECT sequence FROM telegram_updates "
                "ORDER BY sequence DESC LIMIT ?)",
                (now - update_days * 86400, keep_recent_updates),
            )
            self._connection.execute(
                "DELETE FROM scheduled_deletions WHERE delete_at < ? AND attempts >= 20",
                (now - 7 * 86400,),
            )

    def telegram_update_seen(self, update_id: int, bot_role: str = CONTROL_BOT_ROLE) -> bool:
        with self._lock:
            row = self._connection.execute(
                "SELECT 1 FROM telegram_updates WHERE bot_role=? AND update_id=?",
                (bot_role, int(update_id)),
            ).fetchone()
        return row is not None

    def mark_telegram_update(self, update_id: int, bot_role: str = CONTROL_BOT_ROLE) -> None:
        self.claim_telegram_update(update_id, bot_role)

    def claim_telegram_update(
        self,
        update_id: int,
        bot_role: str = CONTROL_BOT_ROLE,
        *,
        max_tracked: int = 10_000,
    ) -> bool:
        if max_tracked <= 0:
            raise ValueError("max_tracked must be positive")
        if not bot_role.strip():
            raise ValueError("bot_role must not be empty")
        with self._immediate_transaction() as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO telegram_updates(bot_role, update_id, received_at, sequence) "
                "SELECT ?, ?, ?, COALESCE(MAX(sequence), 0) + 1 FROM telegram_updates",
                (bot_role, int(update_id), int(time.time())),
            )
            if cursor.rowcount == 1:
                connection.execute(
                    "DELETE FROM telegram_updates WHERE bot_role=? AND sequence NOT IN ("
                    "SELECT sequence FROM telegram_updates WHERE bot_role=? "
                    "ORDER BY sequence DESC LIMIT ?)",
                    (bot_role, bot_role, max_tracked),
                )
        return cursor.rowcount == 1

    def queue_entries(self, thread_id: str) -> list[QueuedPrompt]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT id, thread_id, prompt, inputs_json, client_message_id, created_at "
                "FROM prompt_queue WHERE thread_id=? AND status='queued' ORDER BY id",
                (thread_id,),
            ).fetchall()
        return [
            QueuedPrompt(
                queue_id=int(row[0]),
                thread_id=str(row[1]),
                prompt=str(row[2]),
                inputs=json.loads(row[3]),
                client_message_id=str(row[4]),
                created_at=int(row[5]),
            )
            for row in rows
        ]

    def space_queue_entries(
        self, space_id: str, generation: int | None = None
    ) -> list[dict[str, Any]]:
        parameters: list[Any] = [space_id]
        generation_filter = ""
        if generation is not None:
            generation_filter = " AND generation=?"
            parameters.append(generation)
        with self._lock:
            rows = self._connection.execute(
                "SELECT id, thread_id, prompt, inputs_json, client_message_id, created_at, generation "
                "FROM prompt_queue WHERE space_id=? AND status='queued'"
                + generation_filter
                + " ORDER BY id",
                parameters,
            ).fetchall()
        return [
            {
                "queue_id": int(row[0]),
                "thread_id": str(row[1]),
                "prompt": str(row[2]),
                "inputs": json.loads(row[3]),
                "client_message_id": str(row[4]),
                "created_at": int(row[5]),
                "space_id": space_id,
                "generation": int(row[6]),
            }
            for row in rows
        ]

    def next_space_prompt(self, space_id: str, generation: int) -> dict[str, Any] | None:
        entries = self.space_queue_entries(space_id, generation)
        return entries[0] if entries else None

    def cancel_space_prompt(
        self, space_id: str, queue_id: int, generation: int | None = None
    ) -> bool:
        parameters: list[Any] = [int(queue_id), space_id]
        generation_filter = ""
        if generation is not None:
            generation_filter = " AND generation=?"
            parameters.append(generation)
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE prompt_queue SET status='cancelled' "
                "WHERE id=? AND space_id=? AND status='queued'" + generation_filter,
                parameters,
            )
        return cursor.rowcount == 1

    def cancel_space_queue(self, space_id: str, generation: int | None = None) -> int:
        parameters: list[Any] = [space_id]
        generation_filter = ""
        if generation is not None:
            generation_filter = " AND generation=?"
            parameters.append(generation)
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE prompt_queue SET status='cancelled' "
                "WHERE space_id=? AND status='queued'" + generation_filter,
                parameters,
            )
        return cursor.rowcount

    def queued_file_paths(self) -> set[Path]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT inputs_json FROM prompt_queue WHERE status='queued'"
            ).fetchall()
        paths: set[Path] = set()
        for row in rows:
            try:
                inputs = json.loads(row[0])
            except TypeError, json.JSONDecodeError:
                continue
            for value in inputs if isinstance(inputs, list) else []:
                if not isinstance(value, dict) or value.get("type") not in {
                    "localImage",
                    "mention",
                    "localFile",
                }:
                    continue
                path = value.get("path")
                if isinstance(path, str) and path:
                    paths.add(Path(path).expanduser().resolve(strict=False))
        return paths

    def pending_callback_file_paths(self) -> set[Path]:
        now = int(time.time())
        with self._lock:
            rows = self._connection.execute(
                "SELECT payload_json FROM callbacks WHERE used_at IS NULL AND expires_at >= ?",
                (now,),
            ).fetchall()
        paths: set[Path] = set()
        for row in rows:
            try:
                payload = json.loads(row[0])
            except TypeError, json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                path = payload.get("path")
                if isinstance(path, str) and path:
                    paths.add(Path(path).expanduser().resolve(strict=False))
        return paths

    def schedule_message_deletions(
        self,
        bot_role: str,
        chat_id: int,
        message_ids: list[int] | tuple[int, ...],
        delete_at: int,
        *,
        group_key: str | None = None,
    ) -> list[int]:
        if not bot_role.strip():
            raise ValueError("bot_role must not be empty")
        if not message_ids:
            return []
        now = int(time.time())
        identifiers: list[int] = []
        with self._immediate_transaction() as connection:
            for message_id in dict.fromkeys(int(value) for value in message_ids):
                connection.execute(
                    "INSERT INTO scheduled_deletions(bot_role, chat_id, message_id, delete_at, "
                    "group_key, created_at) VALUES(?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(bot_role, chat_id, message_id) DO UPDATE SET "
                    "delete_at=excluded.delete_at, group_key=excluded.group_key, "
                    "attempts=0, last_error=NULL",
                    (bot_role, int(chat_id), message_id, int(delete_at), group_key, now),
                )
                row = connection.execute(
                    "SELECT id FROM scheduled_deletions "
                    "WHERE bot_role=? AND chat_id=? AND message_id=?",
                    (bot_role, int(chat_id), message_id),
                ).fetchone()
                identifiers.append(int(row[0]))
        return identifiers

    def due_message_deletions(
        self, *, now: int | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            raise ValueError("limit must be positive")
        timestamp = int(time.time()) if now is None else int(now)
        with self._lock:
            rows = self._connection.execute(
                "SELECT id, bot_role, chat_id, message_id, delete_at, group_key, attempts "
                "FROM scheduled_deletions WHERE delete_at<=? ORDER BY delete_at, id LIMIT ?",
                (timestamp, limit),
            ).fetchall()
        return [
            {
                "deletion_id": int(row[0]),
                "bot_role": str(row[1]),
                "chat_id": int(row[2]),
                "message_id": int(row[3]),
                "delete_at": int(row[4]),
                "group_key": str(row[5]) if row[5] is not None else None,
                "attempts": int(row[6]),
            }
            for row in rows
        ]

    def complete_message_deletion(self, deletion_id: int) -> bool:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "DELETE FROM scheduled_deletions WHERE id=?", (int(deletion_id),)
            )
        return cursor.rowcount == 1

    def delete_scheduled_message(self, deletion_id: int) -> bool:
        return self.complete_message_deletion(deletion_id)

    def reschedule_message_deletion(
        self, deletion_id: int, delete_at: int, error: str = ""
    ) -> bool:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE scheduled_deletions SET delete_at=?, attempts=attempts+1, last_error=? "
                "WHERE id=?",
                (int(delete_at), error[:500] or None, int(deletion_id)),
            )
        return cursor.rowcount == 1

    def put_pending_input(
        self,
        request_key: str,
        request_id: str,
        generation: int,
        thread_id: str,
        turn_id: str,
        item_id: str,
        questions: list[dict[str, Any]],
        expires_at: int | None,
    ) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT OR REPLACE INTO pending_inputs("
                "request_key, request_id, generation, thread_id, turn_id, "
                "item_id, questions_json, expires_at, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    request_key,
                    request_id,
                    generation,
                    thread_id,
                    turn_id,
                    item_id,
                    json.dumps(questions, ensure_ascii=False),
                    expires_at,
                    int(time.time()),
                ),
            )

    def record_question_message(
        self, request_key: str, bot_role: str, chat_id: int, message_id: int
    ) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT OR IGNORE INTO question_messages(request_key, bot_role, chat_id, message_id, "
                "created_at) VALUES(?, ?, ?, ?, ?)",
                (request_key, bot_role, int(chat_id), int(message_id), int(time.time())),
            )

    def question_messages(self, request_key: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT bot_role, chat_id, message_id FROM question_messages "
                "WHERE request_key=? ORDER BY created_at, message_id",
                (request_key,),
            ).fetchall()
        return [
            {"bot_role": str(row[0]), "chat_id": int(row[1]), "message_id": int(row[2])}
            for row in rows
        ]

    def pop_question_messages(self, request_key: str) -> list[dict[str, Any]]:
        with self._immediate_transaction() as connection:
            rows = connection.execute(
                "SELECT bot_role, chat_id, message_id FROM question_messages "
                "WHERE request_key=? ORDER BY created_at, message_id",
                (request_key,),
            ).fetchall()
            connection.execute("DELETE FROM question_messages WHERE request_key=?", (request_key,))
        return [
            {"bot_role": str(row[0]), "chat_id": int(row[1]), "message_id": int(row[2])}
            for row in rows
        ]

    def retire_question_requests(
        self,
        *,
        include_unexpired: bool = False,
        now: int | None = None,
    ) -> list[str]:
        """Atomically move stale question messages into the durable deletion queue."""
        timestamp = int(time.time()) if now is None else int(now)
        with self._immediate_transaction() as connection:
            if include_unexpired:
                rows = connection.execute(
                    "SELECT request_key FROM pending_inputs "
                    "UNION SELECT request_key FROM question_messages ORDER BY request_key"
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT request_key FROM pending_inputs "
                    "WHERE expires_at IS NOT NULL AND expires_at < ? "
                    "UNION SELECT messages.request_key FROM question_messages AS messages "
                    "LEFT JOIN pending_inputs AS pending ON pending.request_key=messages.request_key "
                    "WHERE pending.request_key IS NULL ORDER BY request_key",
                    (timestamp,),
                ).fetchall()
            request_keys = [str(row[0]) for row in rows]
            for request_key in request_keys:
                messages = connection.execute(
                    "SELECT bot_role, chat_id, message_id FROM question_messages "
                    "WHERE request_key=? ORDER BY created_at, message_id",
                    (request_key,),
                ).fetchall()
                for bot_role, chat_id, message_id in messages:
                    connection.execute(
                        "INSERT INTO scheduled_deletions(bot_role, chat_id, message_id, delete_at, "
                        "group_key, created_at) VALUES(?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT(bot_role, chat_id, message_id) DO UPDATE SET "
                        "delete_at=excluded.delete_at, group_key=excluded.group_key, "
                        "attempts=0, last_error=NULL",
                        (
                            str(bot_role),
                            int(chat_id),
                            int(message_id),
                            timestamp,
                            f"question:{request_key}",
                            timestamp,
                        ),
                    )
                connection.execute(
                    "DELETE FROM question_messages WHERE request_key=?", (request_key,)
                )
                connection.execute(
                    "DELETE FROM pending_inputs WHERE request_key=?", (request_key,)
                )
        return request_keys

    def get_pending_input(self, request_key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT request_id, generation, thread_id, turn_id, item_id, questions_json, expires_at "
                "FROM pending_inputs WHERE request_key=?",
                (request_key,),
            ).fetchone()
        if not row:
            return None
        return {
            "request_id": row[0],
            "generation": int(row[1]),
            "thread_id": row[2],
            "turn_id": row[3],
            "item_id": row[4],
            "questions": json.loads(row[5]),
            "expires_at": int(row[6]) if row[6] is not None else None,
        }

    def pending_input_keys_for_request(self, request_id: int | str) -> list[str]:
        encoded = json.dumps(request_id)
        compact = json.dumps(request_id, ensure_ascii=False, separators=(",", ":"))
        candidates = tuple(dict.fromkeys((str(request_id), encoded, compact)))
        placeholders = ",".join("?" for _ in candidates)
        with self._lock:
            rows = self._connection.execute(
                f"SELECT request_key FROM pending_inputs WHERE request_id IN ({placeholders}) "
                "ORDER BY created_at, request_key",
                candidates,
            ).fetchall()
        return [str(row[0]) for row in rows]

    def delete_pending_input(self, request_key: str) -> None:
        with self._lock, self._connection:
            self._connection.execute("DELETE FROM pending_inputs WHERE request_key=?", (request_key,))

    def delete_pending_input_by_request(self, request_id: str) -> None:
        with self._lock, self._connection:
            self._connection.execute("DELETE FROM pending_inputs WHERE request_id=?", (request_id,))

    def reset_owner(self) -> None:
        with self._immediate_transaction() as connection:
            connection.execute("DELETE FROM owner")
            connection.execute("DELETE FROM telegram_binding")
            connection.execute("DELETE FROM callbacks")
            connection.execute("DELETE FROM pending_inputs")
            connection.execute("UPDATE subscriptions SET dashboard_message_id=NULL")
            connection.execute("UPDATE prompt_queue SET status='cancelled' WHERE status='queued'")
            rows = connection.execute(
                "SELECT state_json FROM session_spaces WHERE lifecycle!='closed'"
            ).fetchall()
            now = int(time.time())
            for row in rows:
                current = self._space_from_row(row)
                value = self._canonical_space(
                    {
                        **current,
                        "lifecycle": "closed",
                        "generation": int(current["generation"]) + 1,
                    },
                    now=now,
                    created_at=int(current["created_at"]),
                )
                self._write_space(connection, value)
            auth_epoch = int(self.get_meta("totp_auth_epoch", 0)) + 1
            values: dict[str, str | int | bool] = {
                "pair_code_salt": "",
                "pair_code_digest": "",
                "pair_code_expires": 0,
                "pair_code_failures": 0,
                "bind_code_salt": "",
                "bind_code_digest": "",
                "bind_code_expires": 0,
                "bind_code_failures": 0,
                "totp_unlocked_until": 0,
                "totp_force_locked": True,
                "totp_auth_epoch": auth_epoch,
                "totp_failures": 0,
                "totp_locked_until": 0,
            }
            connection.executemany(
                "INSERT INTO metadata(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [(key, json.dumps(value)) for key, value in values.items()],
            )
