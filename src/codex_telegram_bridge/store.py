from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
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
from .models import InteractionDraft, Owner, PromptIntent, QueuedPrompt, SessionSpace, ThreadState

LOGGER = logging.getLogger(__name__)
SCHEMA_VERSION = 11
CONTROL_BOT_ROLE = "control"
EVENT_RETENTION_DAYS = 7
EVENT_RECEIPT_LIMIT = 100_000
TIMELINE_PER_THREAD_LIMIT = 500
OUTBOUND_TERMINAL_LIMIT = 10_000
MAINTENANCE_BATCH_SIZE = 500
MAINTENANCE_BUSY_TIMEOUT_MS = 1_000
_PLAN_PUBLICATION_RESULTS = frozenset({"published", "failed"})
_PLAN_ACTION_STATUSES = frozenset({"executing", "revising"})
_PLAN_ACTION_TERMINAL_STATUSES = frozenset({"executed", "revision_started", "dismissed"})
_PLAN_UI_REPAIR_STATUSES = frozenset(
    {"published", "executing", "revising", "executed", "revision_started", "dismissed", "superseded"}
)
_PROMPT_INTENT_TRANSITIONS: dict[str, frozenset[str]] = {
    "received": frozenset({"awaiting_choice", "queued", "submitting", "failed", "cancelled"}),
    "awaiting_choice": frozenset({"queued", "submitting", "cancelled"}),
    "queued": frozenset({"submitting", "failed", "uncertain", "cancelled"}),
    "submitting": frozenset({"started", "steered", "failed", "uncertain", "cancelled"}),
    "started": frozenset({"completed", "failed", "cancelled"}),
    "steered": frozenset({"completed", "failed", "cancelled"}),
    "completed": frozenset(),
    "failed": frozenset(),
    "uncertain": frozenset(),
    "cancelled": frozenset(),
}

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
CREATE TABLE IF NOT EXISTS event_receipts (
    event_key TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_event_receipts_created
    ON event_receipts(created_at DESC);
CREATE TABLE IF NOT EXISTS timeline_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key TEXT NOT NULL UNIQUE,
    thread_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_timeline_thread_created
    ON timeline_events(thread_id, created_at DESC, id DESC);
CREATE TABLE IF NOT EXISTS tui_plan_approvals (
    event_key TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    prompt TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tui_plan_approvals_lookup
    ON tui_plan_approvals(thread_id, prompt, created_at, event_key);
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
    generation INTEGER NOT NULL DEFAULT 0,
    prompt_intent_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_prompt_queue_thread ON prompt_queue(thread_id, status, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_prompt_queue_intent
    ON prompt_queue(prompt_intent_id) WHERE prompt_intent_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS prompt_intents (
    intent_id TEXT PRIMARY KEY,
    client_message_id TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    prompt TEXT NOT NULL,
    mode TEXT NOT NULL,
    thread_id TEXT,
    space_id TEXT,
    generation INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'received',
    turn_id TEXT,
    queue_id INTEGER,
    error TEXT,
    receipt_key TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_prompt_intents_thread_state
    ON prompt_intents(thread_id, state, updated_at);
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
CREATE TABLE IF NOT EXISTS interaction_drafts (
    scope_key TEXT PRIMARY KEY,
    flow_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    kind TEXT NOT NULL,
    phase TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    user_id INTEGER NOT NULL,
    bot_role TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    space_id TEXT,
    generation INTEGER NOT NULL DEFAULT 0,
    expires_at INTEGER NOT NULL,
    claimed_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_interaction_drafts_kind
    ON interaction_drafts(kind, claimed_at, expires_at);
CREATE TABLE IF NOT EXISTS pending_inputs (
    request_key TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    questions_json TEXT NOT NULL,
    expires_at INTEGER,
    created_at INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    claimed_at INTEGER,
    responded_at INTEGER,
    resolved_at INTEGER,
    response_json TEXT,
    resolution_source TEXT
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
    message_kind TEXT NOT NULL DEFAULT 'interaction',
    created_at INTEGER NOT NULL,
    PRIMARY KEY(request_key, bot_role, chat_id, message_id)
);
CREATE INDEX IF NOT EXISTS idx_question_messages_request ON question_messages(request_key);
CREATE TABLE IF NOT EXISTS question_resolutions (
    request_key TEXT PRIMARY KEY,
    answers_json TEXT NOT NULL,
    source TEXT NOT NULL,
    resolved_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS prompt_runs (
    run_id TEXT PRIMARY KEY,
    space_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    client_message_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL,
    error_kind TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_prompt_runs_turn ON prompt_runs(thread_id, turn_id, status);
CREATE TABLE IF NOT EXISTS plan_publications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    space_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    item_id TEXT NOT NULL,
    revision_key TEXT NOT NULL DEFAULT '',
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    status TEXT NOT NULL,
    plan_text TEXT NOT NULL DEFAULT '',
    message_ids_json TEXT NOT NULL DEFAULT '[]',
    action_message_ids_json TEXT NOT NULL DEFAULT '[]',
    tui_prompt_seen_at INTEGER,
    decision_turn_id TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(space_id, generation, item_id, revision_key)
);
CREATE INDEX IF NOT EXISTS idx_plan_publications_latest
    ON plan_publications(space_id, generation, id DESC);
CREATE TABLE IF NOT EXISTS outbound_intents (
    intent_id TEXT PRIMARY KEY,
    bot_role TEXT NOT NULL,
    operation TEXT NOT NULL,
    lane TEXT NOT NULL,
    chat_id INTEGER,
    payload_fingerprint TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    error_type TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outbound_intents_status
    ON outbound_intents(status, updated_at DESC);
CREATE TABLE IF NOT EXISTS runtime_lifecycle (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    runtime_id TEXT NOT NULL,
    state TEXT NOT NULL,
    owner_chat_id INTEGER,
    startup_disconnect_state TEXT NOT NULL,
    handshake_state TEXT NOT NULL,
    shutdown_disconnect_state TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    ready_at INTEGER,
    stopped_at INTEGER,
    updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS health_snapshots (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    snapshot_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS telegram_message_state (
    message_key TEXT PRIMARY KEY,
    bot_role TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    semantic_fingerprint TEXT NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_telegram_message_state_message
    ON telegram_message_state(bot_role, chat_id, message_id);
"""


def _schema_statements() -> list[str]:
    return [statement.strip() for statement in SCHEMA.split(";") if statement.strip()]


def _columns(connection: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in connection.execute(f"PRAGMA table_info({table})")}


def _json_mapping(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), ensure_ascii=False, separators=(",", ":"))


def _managed_thread_ids(connection: sqlite3.Connection) -> set[str]:
    roots = {
        str(row[0])
        for row in connection.execute(
            "SELECT thread_id FROM subscriptions WHERE active=1 "
            "UNION SELECT thread_id FROM session_spaces "
            "WHERE lifecycle!='closed' AND thread_id IS NOT NULL"
        )
        if row[0]
    }
    states: dict[str, dict[str, Any]] = {}
    if _columns(connection, "threads"):
        for thread_id, raw in connection.execute("SELECT thread_id, state_json FROM threads"):
            try:
                value = json.loads(str(raw))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
            if isinstance(value, dict):
                states[str(thread_id)] = value
    managed = set(roots)
    pending = list(roots)
    while pending:
        state = states.get(pending.pop())
        if state is None:
            continue
        for task in state.get("tasks") or []:
            if not isinstance(task, dict):
                continue
            child_id = str(
                task.get("agent_thread_id")
                or task.get("agentThreadId")
                or task.get("task_id")
                or task.get("taskId")
                or ""
            )
            if child_id and child_id not in managed:
                managed.add(child_id)
                pending.append(child_id)
    return managed


def _compact_event_payload(kind: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for name in ("threadId", "turnId"):
        value = payload.get(name)
        if value:
            compact[name] = str(value)[:160]
    status = payload.get("status")
    if isinstance(status, dict):
        compact["status"] = {
            "type": str(status.get("type") or "")[:80],
            "activeFlags": [str(value)[:80] for value in status.get("activeFlags") or []][:16],
        }
    elif status is not None:
        compact["status"] = str(status)[:80]
    turn = payload.get("turn")
    if isinstance(turn, dict):
        compact["turn"] = {
            "id": str(turn.get("id") or "")[:160],
            "status": str(turn.get("status") or "")[:80],
        }
    item = payload.get("item")
    if isinstance(item, dict):
        compact["item"] = {
            "id": str(item.get("id") or "")[:160],
            "type": str(item.get("type") or "")[:80],
            "status": str(item.get("status") or "")[:80],
        }
    error = payload.get("error")
    if isinstance(error, dict):
        compact["error"] = {"message": " ".join(str(error.get("message") or "").split())[:360]}
    elif error:
        compact["error"] = {"message": " ".join(str(error).split())[:360]}
    if "willRetry" in payload:
        compact["willRetry"] = bool(payload.get("willRetry"))
    if kind in {"warning", "guardianWarning", "deprecationNotice", "configWarning"}:
        message = payload.get("message") or payload.get("text")
        if message:
            compact["message"] = " ".join(str(message).split())[:360]
    return compact


def _tui_plan_approval(payload: Mapping[str, Any]) -> tuple[str, str] | None:
    item = payload.get("item")
    if (
        not isinstance(item, dict)
        or item.get("type") != "userMessage"
        or item.get("clientId") is not None
    ):
        return None
    content = item.get("content")
    if not isinstance(content, list) or len(content) != 1 or not isinstance(content[0], dict):
        return None
    first = content[0]
    if first.get("type") != "text" or first.get("text") != "Implement the plan.":
        return None
    turn_id = str(payload.get("turnId") or "")
    return (turn_id, "Implement the plan.") if turn_id else None


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
        try:
            with self._lock:
                self._connection.execute("PRAGMA busy_timeout=30000")
                self._connection.execute("PRAGMA foreign_keys=ON")
                self._migrate(had_database=had_database)
                self._connection.execute("PRAGMA journal_mode=WAL")
                self._connection.execute("PRAGMA synchronous=FULL")
                self._connection.commit()
        except BaseException:
            self._connection.close()
            self._closed = True
            raise
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

    def _migrate_events_v10(self) -> None:
        managed = sorted(_managed_thread_ids(self._connection))
        self._connection.execute(
            "CREATE TEMP TABLE IF NOT EXISTS migration_managed_threads("
            "thread_id TEXT PRIMARY KEY) WITHOUT ROWID"
        )
        self._connection.execute("DELETE FROM migration_managed_threads")
        self._connection.executemany(
            "INSERT INTO migration_managed_threads(thread_id) VALUES(?)",
            ((thread_id,) for thread_id in managed),
        )
        cutoff = int(time.time()) - EVENT_RETENTION_DAYS * 86400
        self._connection.execute(
            "INSERT OR IGNORE INTO event_receipts(event_key, thread_id, kind, created_at) "
            "SELECT events.event_key, events.thread_id, events.kind, events.created_at "
            "FROM events JOIN migration_managed_threads AS managed "
            "ON managed.thread_id=events.thread_id WHERE events.created_at>=? "
            "ORDER BY events.created_at DESC, events.rowid DESC LIMIT ?",
            (cutoff, EVENT_RECEIPT_LIMIT),
        )
        for thread_id in managed:
            rows = self._connection.execute(
                "SELECT event_key, kind, payload_json, created_at FROM events "
                "WHERE thread_id=? AND created_at>=? "
                "ORDER BY created_at DESC, rowid DESC LIMIT ?",
                (thread_id, cutoff, TIMELINE_PER_THREAD_LIMIT),
            ).fetchall()
            timeline_rows: list[tuple[str, str, str, str, int]] = []
            for row in reversed(rows):
                try:
                    raw = json.loads(str(row[2]))
                except (TypeError, ValueError):
                    raw = {}
                payload = raw if isinstance(raw, dict) else {}
                timeline_rows.append(
                    (
                        str(row[0]),
                        thread_id,
                        str(row[1]),
                        _json_mapping(_compact_event_payload(str(row[1]), payload)),
                        int(row[3]),
                    )
                )
            self._connection.executemany(
                "INSERT OR IGNORE INTO timeline_events("
                "event_key, thread_id, kind, payload_json, created_at) VALUES(?, ?, ?, ?, ?)",
                timeline_rows,
            )
        cursor = self._connection.execute(
            "SELECT events.event_key, events.thread_id, events.payload_json, events.created_at "
            "FROM events JOIN migration_managed_threads AS managed "
            "ON managed.thread_id=events.thread_id "
            "WHERE events.kind='item/started' AND events.created_at>=? "
            "ORDER BY events.created_at, events.rowid",
            (cutoff,),
        )
        while True:
            rows = cursor.fetchmany(MAINTENANCE_BATCH_SIZE)
            if not rows:
                break
            approvals: list[tuple[str, str, str, str, int]] = []
            for row in rows:
                try:
                    raw = json.loads(str(row[2]))
                except (TypeError, ValueError):
                    continue
                semantic = _tui_plan_approval(raw if isinstance(raw, dict) else {})
                if semantic is not None:
                    approvals.append(
                        (str(row[0]), str(row[1]), semantic[0], semantic[1], int(row[3]))
                    )
            self._connection.executemany(
                "INSERT OR IGNORE INTO tui_plan_approvals("
                "event_key, thread_id, turn_id, prompt, created_at) VALUES(?, ?, ?, ?, ?)",
                approvals,
            )
        self._connection.execute("DROP INDEX IF EXISTS idx_events_thread_created")
        self._connection.execute("DROP TABLE events")
        self._connection.execute("DROP TABLE migration_managed_threads")

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
            plan_columns = _columns(self._connection, "plan_publications")
            if plan_columns and "revision_key" not in plan_columns:
                self._connection.execute("DROP INDEX IF EXISTS idx_plan_publications_latest")
                self._connection.execute("ALTER TABLE plan_publications RENAME TO plan_publications_v6")
            update_columns = _columns(self._connection, "telegram_updates")
            if update_columns and "bot_role" not in update_columns:
                self._connection.execute("ALTER TABLE telegram_updates RENAME TO telegram_updates_legacy")
            prompt_columns = _columns(self._connection, "prompt_queue")
            if prompt_columns and "prompt_intent_id" not in prompt_columns:
                self._connection.execute("ALTER TABLE prompt_queue ADD COLUMN prompt_intent_id TEXT")
            pending_columns = _columns(self._connection, "pending_inputs")
            pending_additions = {
                "status": "TEXT NOT NULL DEFAULT 'pending'",
                "claimed_at": "INTEGER",
                "responded_at": "INTEGER",
                "resolved_at": "INTEGER",
                "response_json": "TEXT",
                "resolution_source": "TEXT",
            }
            if pending_columns:
                for name, declaration in pending_additions.items():
                    if name not in pending_columns:
                        self._connection.execute(
                            f"ALTER TABLE pending_inputs ADD COLUMN {name} {declaration}"
                        )
            for statement in _schema_statements():
                self._connection.execute(statement)
            if _columns(self._connection, "plan_publications_v6"):
                self._connection.execute(
                    "INSERT INTO plan_publications(id, space_id, generation, item_id, revision_key, "
                    "thread_id, turn_id, status, message_ids_json, created_at, updated_at) "
                    "SELECT id, space_id, generation, item_id, '', thread_id, turn_id, status, "
                    "message_ids_json, created_at, updated_at FROM plan_publications_v6"
                )
                self._connection.execute("DROP TABLE plan_publications_v6")
            plan_columns = _columns(self._connection, "plan_publications")
            plan_additions = {
                "plan_text": "TEXT NOT NULL DEFAULT ''",
                "action_message_ids_json": "TEXT NOT NULL DEFAULT '[]'",
                "tui_prompt_seen_at": "INTEGER",
                "decision_turn_id": "TEXT NOT NULL DEFAULT ''",
            }
            for name, declaration in plan_additions.items():
                if name not in plan_columns:
                    self._connection.execute(
                        f"ALTER TABLE plan_publications ADD COLUMN {name} {declaration}"
                    )
            if _columns(self._connection, "events"):
                self._connection.execute(
                    "UPDATE plan_publications SET plan_text=COALESCE(("
                    "SELECT json_extract(events.payload_json, '$.item.text') FROM events "
                    "WHERE events.thread_id=plan_publications.thread_id "
                    "AND events.kind IN ('item/completed', 'item/started') "
                    "AND json_extract(events.payload_json, '$.turnId')=plan_publications.turn_id "
                    "AND json_extract(events.payload_json, '$.item.id')=plan_publications.item_id "
                    "AND json_extract(events.payload_json, '$.item.type')='plan' "
                    "ORDER BY events.created_at DESC LIMIT 1), '') WHERE plan_text=''"
                )
                self._migrate_events_v10()
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
            rows = self._connection.execute(
                "SELECT space_id, state_json FROM session_spaces"
            ).fetchall()
            for row in rows:
                try:
                    raw_space = json.loads(str(row[1]))
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
                if not isinstance(raw_space, dict):
                    continue
                current_mode = str(raw_space.get("current_mode") or "default")
                raw_space.setdefault("desired_mode", current_mode)
                raw_space.setdefault("observed_mode", "unknown")
                raw_space["current_mode"] = str(raw_space.get("desired_mode") or current_mode)
                self._connection.execute(
                    "UPDATE session_spaces SET state_json=? WHERE space_id=?",
                    (_json_mapping(raw_space), str(row[0])),
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
            question_message_columns = _columns(self._connection, "question_messages")
            if "message_kind" not in question_message_columns:
                self._connection.execute(
                    "ALTER TABLE question_messages ADD COLUMN "
                    "message_kind TEXT NOT NULL DEFAULT 'interaction'"
                )
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

    def create_outbound_intent(
        self,
        *,
        bot_role: str,
        operation: str,
        lane: str,
        chat_id: int | None,
        payload_fingerprint: str,
    ) -> str:
        intent_id = str(uuid.uuid4())
        now = int(time.time())
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT INTO outbound_intents(intent_id, bot_role, operation, lane, chat_id, "
                "payload_fingerprint, status, created_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
                (
                    intent_id,
                    bot_role,
                    operation,
                    lane,
                    chat_id,
                    payload_fingerprint,
                    now,
                    now,
                ),
            )
        return intent_id

    def update_outbound_intent(
        self,
        intent_id: str,
        *,
        status: str,
        attempts: int,
        error_type: str | None = None,
    ) -> None:
        if status not in {"pending", "retrying", "delivered", "uncertain", "failed"}:
            raise ValueError("Outbound intent status is invalid")
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE outbound_intents SET status=?, attempts=?, error_type=?, updated_at=? "
                "WHERE intent_id=?",
                (status, attempts, error_type, int(time.time()), intent_id),
            )

    def outbound_intents(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        sql = (
            "SELECT intent_id, bot_role, operation, lane, chat_id, payload_fingerprint, status, "
            "attempts, error_type, created_at, updated_at FROM outbound_intents"
        )
        params: tuple[Any, ...]
        if status is None:
            sql += " ORDER BY updated_at DESC LIMIT ?"
            params = (limit,)
        else:
            sql += " WHERE status=? ORDER BY updated_at DESC LIMIT ?"
            params = (status, limit)
        with self._lock:
            rows = self._connection.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def recover_outbound_intents(self) -> int:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE outbound_intents SET status='uncertain', error_type='ProcessRestart', "
                "updated_at=? WHERE status IN ('pending', 'retrying')",
                (int(time.time()),),
            )
        return max(0, int(cursor.rowcount))

    def begin_runtime_lifecycle(self, owner_chat_id: int | None) -> dict[str, Any]:
        runtime_id = str(uuid.uuid4())
        now = int(time.time())
        with self._immediate_transaction() as connection:
            row = connection.execute(
                "SELECT runtime_id, state, owner_chat_id, shutdown_disconnect_state "
                "FROM runtime_lifecycle WHERE singleton=1"
            ).fetchone()
            legacy_active = bool(self._meta_on(connection, "telegram_runtime_active", False))
            legacy_chat_id = int(self._meta_on(connection, "telegram_runtime_chat_id", 0)) or None
            previous_active = bool(row and str(row[1]) != "stopped") or (row is None and legacy_active)
            previous_chat_id = int(row[2]) if row and row[2] is not None else legacy_chat_id
            previous_shutdown = str(row[3]) if row else "pending"
            startup_disconnect = (
                "pending"
                if owner_chat_id is not None
                and previous_active
                and previous_chat_id == owner_chat_id
                and previous_shutdown == "pending"
                else "skipped"
            )
            connection.execute(
                "INSERT INTO runtime_lifecycle(singleton, runtime_id, state, owner_chat_id, "
                "startup_disconnect_state, handshake_state, shutdown_disconnect_state, "
                "started_at, ready_at, stopped_at, updated_at) "
                "VALUES(1, ?, 'starting', ?, ?, 'pending', 'pending', ?, NULL, NULL, ?) "
                "ON CONFLICT(singleton) DO UPDATE SET runtime_id=excluded.runtime_id, "
                "state=excluded.state, owner_chat_id=excluded.owner_chat_id, "
                "startup_disconnect_state=excluded.startup_disconnect_state, "
                "handshake_state=excluded.handshake_state, "
                "shutdown_disconnect_state=excluded.shutdown_disconnect_state, "
                "started_at=excluded.started_at, ready_at=NULL, stopped_at=NULL, "
                "updated_at=excluded.updated_at",
                (runtime_id, owner_chat_id, startup_disconnect, now, now),
            )
            self._set_meta_on(connection, "telegram_runtime_active", True)
            self._set_meta_on(connection, "telegram_runtime_chat_id", owner_chat_id or 0)
            self._set_meta_on(connection, "telegram_disconnect_pending", startup_disconnect == "pending")
        return self.runtime_lifecycle() or {}

    def bind_runtime_owner(self, runtime_id: str, owner_chat_id: int) -> bool:
        now = int(time.time())
        with self._immediate_transaction() as connection:
            cursor = connection.execute(
                "UPDATE runtime_lifecycle SET owner_chat_id=?, handshake_state='pending', "
                "updated_at=? WHERE singleton=1 AND runtime_id=? "
                "AND (owner_chat_id IS NULL OR owner_chat_id!=?)",
                (owner_chat_id, now, runtime_id, owner_chat_id),
            )
            if cursor.rowcount:
                self._set_meta_on(connection, "telegram_runtime_chat_id", owner_chat_id)
        return bool(cursor.rowcount)

    def claim_runtime_notice(self, runtime_id: str, notice: str) -> bool:
        column = self._runtime_notice_column(notice)
        with self._lock, self._connection:
            cursor = self._connection.execute(
                f"UPDATE runtime_lifecycle SET {column}='attempting', updated_at=? "
                "WHERE singleton=1 AND runtime_id=? "
                f"AND {column}='pending'",
                (int(time.time()), runtime_id),
            )
        return bool(cursor.rowcount)

    def complete_runtime_notice(self, runtime_id: str, notice: str, outcome: str) -> None:
        if outcome not in {"delivered", "uncertain", "failed", "skipped"}:
            raise ValueError("Runtime notice outcome is invalid")
        column = self._runtime_notice_column(notice)
        with self._immediate_transaction() as connection:
            connection.execute(
                f"UPDATE runtime_lifecycle SET {column}=?, updated_at=? "
                "WHERE singleton=1 AND runtime_id=?",
                (outcome, int(time.time()), runtime_id),
            )
            if notice == "startup_disconnect":
                self._set_meta_on(connection, "telegram_disconnect_pending", False)

    def mark_runtime_ready(self, runtime_id: str) -> None:
        now = int(time.time())
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE runtime_lifecycle SET state='ready', ready_at=?, updated_at=? "
                "WHERE singleton=1 AND runtime_id=?",
                (now, now, runtime_id),
            )

    def finish_runtime_lifecycle(self, runtime_id: str) -> None:
        now = int(time.time())
        with self._immediate_transaction() as connection:
            connection.execute(
                "UPDATE runtime_lifecycle SET state='stopped', stopped_at=?, updated_at=? "
                "WHERE singleton=1 AND runtime_id=?",
                (now, now, runtime_id),
            )
            self._set_meta_on(connection, "telegram_runtime_active", False)
            self._set_meta_on(connection, "telegram_disconnect_pending", False)

    def runtime_lifecycle(self) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT runtime_id, state, owner_chat_id, startup_disconnect_state, "
                "handshake_state, shutdown_disconnect_state, started_at, ready_at, stopped_at, "
                "updated_at FROM runtime_lifecycle WHERE singleton=1"
            ).fetchone()
        return dict(row) if row else None

    def write_health_snapshot(self, snapshot: Mapping[str, Any]) -> None:
        now = int(time.time())
        payload = _json_mapping(snapshot)
        connection = sqlite3.connect(self.path, timeout=1.0)
        connection.execute(f"PRAGMA busy_timeout={MAINTENANCE_BUSY_TIMEOUT_MS}")
        try:
            with connection:
                connection.execute(
                "INSERT INTO health_snapshots(singleton, snapshot_json, updated_at) "
                "VALUES(1, ?, ?) ON CONFLICT(singleton) DO UPDATE SET "
                "snapshot_json=excluded.snapshot_json, updated_at=excluded.updated_at",
                (payload, now),
            )
        finally:
            connection.close()

    def health_snapshot(self) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT snapshot_json, updated_at FROM health_snapshots WHERE singleton=1"
            ).fetchone()
        if row is None:
            return None
        value = json.loads(str(row[0]))
        snapshot = value if isinstance(value, dict) else {}
        snapshot["updated_at"] = int(row[1])
        return snapshot

    @staticmethod
    def _runtime_notice_column(notice: str) -> str:
        columns = {
            "startup_disconnect": "startup_disconnect_state",
            "handshake": "handshake_state",
            "shutdown_disconnect": "shutdown_disconnect_state",
        }
        try:
            return columns[notice]
        except KeyError as exc:
            raise ValueError("Runtime notice is invalid") from exc

    @staticmethod
    def _meta_on(connection: sqlite3.Connection, key: str, default: Any) -> Any:
        row = connection.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    @staticmethod
    def _set_meta_on(connection: sqlite3.Connection, key: str, value: Any) -> None:
        connection.execute(
            "INSERT INTO metadata(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value, ensure_ascii=False)),
        )

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
        for name in ("normal_model", "normal_effort", "plan_model", "plan_effort"):
            space[name] = str(space.get(name) or "").strip()
        desired_mode = str(space.get("desired_mode") or space.get("current_mode") or "default")
        legacy_mode = str(space.get("current_mode") or desired_mode)
        if legacy_mode != desired_mode:
            LOGGER.info(
                "event=session_space_legacy_mode_override space_id=%s desired=%s current=%s",
                space_id,
                desired_mode,
                legacy_mode,
            )
            desired_mode = legacy_mode
        if desired_mode not in {"default", "plan"}:
            raise ValueError(f"Unsupported desired collaboration mode: {desired_mode}")
        observed_mode = str(space.get("observed_mode") or "unknown")
        if observed_mode not in {"unknown", "default", "plan"}:
            raise ValueError(f"Unsupported observed collaboration mode: {observed_mode}")
        space["desired_mode"] = desired_mode
        space["observed_mode"] = observed_mode
        space["current_mode"] = desired_mode
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
            value = self._canonical_space(merged, now=int(time.time()), created_at=int(current["created_at"]))
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
                "SELECT state_json FROM session_spaces WHERE thread_id=?"
                + suffix
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

    def reset_space_transport(self, space_id: str, *, expected_generation: int) -> dict[str, Any] | None:
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
                f"SELECT 1 FROM prompt_queue WHERE {queue_filter} AND status='queued' LIMIT 1",
                queue_parameters,
            ).fetchone():
                raise RuntimeError("当前 SessionSpace 仍有排队 prompt，不能重建 Telegram 帖子")
            if (
                thread_id
                and connection.execute(
                    "SELECT 1 FROM pending_inputs WHERE thread_id=? AND status!='resolved' LIMIT 1",
                    (thread_id,),
                ).fetchone()
            ):
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
            connection.execute(f"DELETE FROM discussion_messages WHERE {message_filter}", parameters)

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
                    "observed_mode": "unknown",
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

    def close_space(self, space_id: str, *, expected_generation: int | None = None) -> dict[str, Any] | None:
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
                {
                    **current,
                    "lifecycle": "closed",
                    "generation": generation + 1,
                    "observed_mode": "unknown",
                },
                now=int(time.time()),
                created_at=int(current["created_at"]),
            )
            self._write_space(connection, value)
            connection.execute(
                "UPDATE callbacks SET used_at=? WHERE space_id=? AND generation=? AND used_at IS NULL",
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
            "normal_model",
            "normal_effort",
            "plan_model",
            "plan_effort",
            "current_mode",
            "desired_mode",
            "observed_mode",
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

    def is_managed_thread(self, thread_id: str | None) -> bool:
        if not thread_id:
            return False
        with self._lock:
            return thread_id in _managed_thread_ids(self._connection)

    def record_event(
        self,
        event_key: str,
        thread_id: str | None,
        kind: str,
        payload: dict[str, Any],
        *,
        managed: bool = False,
    ) -> bool:
        if not thread_id or (not managed and not self.is_managed_thread(thread_id)):
            return False
        created_at = int(time.time())
        compact = _json_mapping(_compact_event_payload(kind, payload))
        approval = _tui_plan_approval(payload) if kind == "item/started" else None
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "INSERT OR IGNORE INTO event_receipts(event_key, thread_id, kind, created_at) "
                "VALUES(?, ?, ?, ?)",
                (event_key, thread_id, kind, created_at),
            )
            if cursor.rowcount != 1:
                return False
            self._connection.execute(
                "INSERT INTO timeline_events(event_key, thread_id, kind, payload_json, created_at) "
                "VALUES(?, ?, ?, ?, ?)",
                (event_key, thread_id, kind, compact, created_at),
            )
            if approval is not None:
                self._connection.execute(
                    "INSERT OR IGNORE INTO tui_plan_approvals("
                    "event_key, thread_id, turn_id, prompt, created_at) VALUES(?, ?, ?, ?, ?)",
                    (event_key, thread_id, approval[0], approval[1], created_at),
                )
        return True

    def timeline(self, thread_id: str, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT kind, payload_json, created_at FROM timeline_events WHERE thread_id=? "
                "ORDER BY created_at DESC, id ASC LIMIT ?",
                (thread_id, limit),
            ).fetchall()
        return [{"kind": row[0], "payload": json.loads(row[1]), "created_at": int(row[2])} for row in rows]

    @staticmethod
    def _prompt_intent_from_row(row: sqlite3.Row) -> PromptIntent:
        return PromptIntent(
            intent_id=str(row[0]),
            client_message_id=str(row[1]),
            source=str(row[2]),
            prompt=str(row[3]),
            mode=str(row[4]),
            thread_id=str(row[5]) if row[5] else None,
            space_id=str(row[6]) if row[6] else None,
            generation=int(row[7]),
            state=str(row[8]),
            turn_id=str(row[9]) if row[9] else None,
            queue_id=int(row[10]) if row[10] is not None else None,
            error=str(row[11]) if row[11] else None,
            receipt_key=str(row[12]) if row[12] else None,
            created_at=int(row[13]),
            updated_at=int(row[14]),
        )

    def create_prompt_intent(
        self,
        client_message_id: str,
        source: str,
        prompt: str,
        mode: str,
        *,
        thread_id: str | None = None,
        space_id: str | None = None,
        generation: int = 0,
        receipt_key: str | None = None,
    ) -> PromptIntent:
        if not client_message_id.strip() or not source.strip() or not mode.strip():
            raise ValueError("Prompt intent identifiers must not be empty")
        if generation < 0:
            raise ValueError("generation must not be negative")
        now = int(time.time())
        with self._immediate_transaction() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO prompt_intents("
                "intent_id, client_message_id, source, prompt, mode, thread_id, space_id, generation, "
                "state, receipt_key, created_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'received', ?, ?, ?)",
                (
                    uuid.uuid4().hex,
                    client_message_id,
                    source,
                    prompt,
                    mode,
                    thread_id,
                    space_id,
                    generation,
                    receipt_key,
                    now,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT intent_id, client_message_id, source, prompt, mode, thread_id, space_id, "
                "generation, state, turn_id, queue_id, error, receipt_key, created_at, updated_at "
                "FROM prompt_intents WHERE client_message_id=?",
                (client_message_id,),
            ).fetchone()
        assert row is not None
        intent = self._prompt_intent_from_row(row)
        requested = (source, prompt, mode, thread_id, space_id, generation)
        persisted = (
            intent.source,
            intent.prompt,
            intent.mode,
            intent.thread_id,
            intent.space_id,
            intent.generation,
        )
        if persisted != requested:
            raise ValueError(f"Prompt client_message_id collision: {client_message_id}")
        return intent

    def get_prompt_intent(self, client_message_id: str) -> PromptIntent | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT intent_id, client_message_id, source, prompt, mode, thread_id, space_id, "
                "generation, state, turn_id, queue_id, error, receipt_key, created_at, updated_at "
                "FROM prompt_intents WHERE client_message_id=?",
                (client_message_id,),
            ).fetchone()
        return self._prompt_intent_from_row(row) if row is not None else None

    def resolve_prompt_intent_choice(
        self,
        client_message_id: str,
        *,
        mode: str,
    ) -> PromptIntent | None:
        if mode not in {"queue", "steer"}:
            raise ValueError(f"Unsupported prompt choice: {mode}")
        with self._immediate_transaction() as connection:
            cursor = connection.execute(
                "UPDATE prompt_intents SET mode=?, updated_at=? WHERE client_message_id=? "
                "AND state='awaiting_choice' AND mode='auto'",
                (mode, int(time.time()), client_message_id),
            )
            if cursor.rowcount != 1:
                return None
            row = connection.execute(
                "SELECT intent_id, client_message_id, source, prompt, mode, thread_id, space_id, "
                "generation, state, turn_id, queue_id, error, receipt_key, created_at, updated_at "
                "FROM prompt_intents WHERE client_message_id=?",
                (client_message_id,),
            ).fetchone()
        return self._prompt_intent_from_row(row) if row is not None else None

    def link_prompt_intent_receipt(
        self,
        client_message_id: str,
        receipt_key: str,
    ) -> PromptIntent | None:
        if not receipt_key.strip():
            raise ValueError("receipt_key must not be empty")
        with self._immediate_transaction() as connection:
            current = connection.execute(
                "SELECT receipt_key FROM prompt_intents WHERE client_message_id=?",
                (client_message_id,),
            ).fetchone()
            if current is None:
                return None
            if current[0] not in {None, receipt_key}:
                raise ValueError(f"Prompt intent receipt collision: {client_message_id}")
            connection.execute(
                "UPDATE prompt_intents SET receipt_key=?, updated_at=? WHERE client_message_id=?",
                (receipt_key, int(time.time()), client_message_id),
            )
            row = connection.execute(
                "SELECT intent_id, client_message_id, source, prompt, mode, thread_id, space_id, "
                "generation, state, turn_id, queue_id, error, receipt_key, created_at, updated_at "
                "FROM prompt_intents WHERE client_message_id=?",
                (client_message_id,),
            ).fetchone()
        return self._prompt_intent_from_row(row) if row is not None else None

    def transition_prompt_intent(
        self,
        client_message_id: str,
        *,
        expected_states: set[str] | None,
        to_state: str,
        turn_id: str | None = None,
        queue_id: int | None = None,
        error: str | None = None,
        receipt_key: str | None = None,
    ) -> PromptIntent | None:
        if to_state not in _PROMPT_INTENT_TRANSITIONS:
            raise ValueError(f"Unsupported prompt intent state: {to_state}")
        with self._immediate_transaction() as connection:
            current = connection.execute(
                "SELECT state FROM prompt_intents WHERE client_message_id=?",
                (client_message_id,),
            ).fetchone()
            if current is None:
                return None
            current_state = str(current[0])
            if expected_states is not None and current_state not in expected_states:
                return None
            if current_state != to_state and to_state not in _PROMPT_INTENT_TRANSITIONS[current_state]:
                raise ValueError(f"Invalid prompt intent transition: {current_state} -> {to_state}")
            connection.execute(
                "UPDATE prompt_intents SET state=?, turn_id=COALESCE(?, turn_id), "
                "queue_id=COALESCE(?, queue_id), error=COALESCE(?, error), "
                "receipt_key=COALESCE(?, receipt_key), updated_at=? WHERE client_message_id=?",
                (
                    to_state,
                    turn_id,
                    queue_id,
                    error,
                    receipt_key,
                    int(time.time()),
                    client_message_id,
                ),
            )
            row = connection.execute(
                "SELECT intent_id, client_message_id, source, prompt, mode, thread_id, space_id, "
                "generation, state, turn_id, queue_id, error, receipt_key, created_at, updated_at "
                "FROM prompt_intents WHERE client_message_id=?",
                (client_message_id,),
            ).fetchone()
        return self._prompt_intent_from_row(row) if row is not None else None

    def reconcile_prompt_intent(
        self,
        client_message_id: str,
        *,
        delivered: bool | None,
        turn_id: str | None = None,
        error: str | None = None,
    ) -> PromptIntent | None:
        current = self.get_prompt_intent(client_message_id)
        if current is None or current.state in {"completed", "failed", "uncertain", "cancelled"}:
            return current
        if delivered:
            target = "steered" if current.mode == "steer" else "started"
            if current.state == "queued":
                current = self.transition_prompt_intent(
                    client_message_id,
                    expected_states={"queued"},
                    to_state="submitting",
                    turn_id=turn_id,
                    error=error,
                ) or current
            return self.transition_prompt_intent(
                client_message_id,
                expected_states={"received", "awaiting_choice", "submitting"},
                to_state=target,
                turn_id=turn_id,
                error=error,
            )
        return self.transition_prompt_intent(
            client_message_id,
            expected_states={"submitting", "queued"},
            to_state="uncertain",
            turn_id=turn_id,
            error=error or "delivery could not be reconciled",
        )

    def finish_prompt_intents(
        self,
        thread_id: str,
        turn_id: str,
        *,
        status: str,
        error: str | None = None,
    ) -> int:
        target = {"completed": "completed", "interrupted": "cancelled", "failed": "failed"}.get(
            status
        )
        if target is None:
            raise ValueError(f"Unsupported terminal turn status: {status}")
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE prompt_intents SET state=?, error=COALESCE(?, error), updated_at=? "
                "WHERE thread_id=? AND turn_id=? AND state IN ('started', 'steered')",
                (target, error, int(time.time()), thread_id, turn_id),
            )
        return int(cursor.rowcount)

    def put_telegram_message_state(
        self,
        message_key: str,
        *,
        bot_role: str,
        chat_id: int,
        message_id: int,
        semantic_fingerprint: str,
        state: str,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if not message_key.strip() or not bot_role.strip() or not state.strip():
            raise ValueError("Telegram message state identifiers must not be empty")
        now = int(time.time())
        encoded = _json_mapping(payload or {})
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT INTO telegram_message_state(message_key, bot_role, chat_id, message_id, "
                "semantic_fingerprint, state, payload_json, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(message_key) DO UPDATE SET bot_role=excluded.bot_role, "
                "chat_id=excluded.chat_id, message_id=excluded.message_id, "
                "semantic_fingerprint=excluded.semantic_fingerprint, state=excluded.state, "
                "payload_json=excluded.payload_json, updated_at=excluded.updated_at",
                (
                    message_key,
                    bot_role,
                    int(chat_id),
                    int(message_id),
                    semantic_fingerprint,
                    state,
                    encoded,
                    now,
                ),
            )
        return self.get_telegram_message_state(message_key) or {}

    def get_telegram_message_state(self, message_key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT bot_role, chat_id, message_id, semantic_fingerprint, state, payload_json, "
                "updated_at FROM telegram_message_state WHERE message_key=?",
                (message_key,),
            ).fetchone()
        if row is None:
            return None
        payload = json.loads(str(row[5]))
        return {
            "message_key": message_key,
            "bot_role": str(row[0]),
            "chat_id": int(row[1]),
            "message_id": int(row[2]),
            "semantic_fingerprint": str(row[3]),
            "state": str(row[4]),
            "payload": payload if isinstance(payload, dict) else {},
            "updated_at": int(row[6]),
        }

    def delete_telegram_message_state(self, message_key: str) -> bool:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "DELETE FROM telegram_message_state WHERE message_key=?", (message_key,)
            )
        return cursor.rowcount == 1

    def enqueue_prompt(
        self,
        thread_id: str,
        prompt: str,
        inputs: list[dict[str, Any]],
        client_message_id: str,
        *,
        space_id: str | None = None,
        generation: int = 0,
        prompt_intent_id: str | None = None,
    ) -> int:
        if generation < 0:
            raise ValueError("generation must not be negative")
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "INSERT INTO prompt_queue(thread_id, prompt, inputs_json, client_message_id, created_at, "
                "space_id, generation, prompt_intent_id) VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    thread_id,
                    prompt,
                    json.dumps(inputs, ensure_ascii=False),
                    client_message_id,
                    int(time.time()),
                    space_id,
                    generation,
                    prompt_intent_id,
                ),
            )
        return int(cursor.lastrowid)

    def next_prompt(self, thread_id: str) -> QueuedPrompt | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT id, thread_id, prompt, inputs_json, client_message_id, created_at, "
                "prompt_intent_id "
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
            prompt_intent_id=str(row[6]) if row[6] else None,
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

    def put_prompt_run(
        self,
        run_id: str,
        *,
        space_id: str,
        generation: int,
        thread_id: str,
        turn_id: str,
        client_message_id: str,
    ) -> bool:
        if not all(value.strip() for value in (run_id, space_id, thread_id, turn_id, client_message_id)):
            raise ValueError("Prompt run identifiers must not be empty")
        if generation < 0:
            raise ValueError("generation must not be negative")
        now = int(time.time())
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "INSERT OR IGNORE INTO prompt_runs(run_id, space_id, generation, thread_id, turn_id, "
                "client_message_id, status, created_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?, 'running', ?, ?)",
                (
                    run_id,
                    space_id,
                    int(generation),
                    thread_id,
                    turn_id,
                    client_message_id,
                    now,
                    now,
                ),
            )
        return cursor.rowcount == 1

    def finish_prompt_runs(
        self,
        thread_id: str,
        turn_id: str,
        *,
        status: str,
        error_kind: str = "",
    ) -> list[dict[str, Any]]:
        if not thread_id.strip() or not turn_id.strip():
            raise ValueError("Prompt run thread and turn IDs must not be empty")
        if not status.strip() or status == "running":
            raise ValueError("Prompt run terminal status is invalid")
        now = int(time.time())
        with self._immediate_transaction() as connection:
            rows = connection.execute(
                "SELECT run_id, space_id, generation, client_message_id, created_at "
                "FROM prompt_runs WHERE thread_id=? AND turn_id=? AND status='running' "
                "ORDER BY created_at, run_id",
                (thread_id, turn_id),
            ).fetchall()
            connection.execute(
                "UPDATE prompt_runs SET status=?, error_kind=?, updated_at=? "
                "WHERE thread_id=? AND turn_id=? AND status='running'",
                (status, error_kind or None, now, thread_id, turn_id),
            )
        return [
            {
                "run_id": str(row[0]),
                "space_id": str(row[1]),
                "generation": int(row[2]),
                "thread_id": thread_id,
                "turn_id": turn_id,
                "client_message_id": str(row[3]),
                "status": status,
                "error_kind": error_kind,
                "created_at": int(row[4]),
                "updated_at": now,
            }
            for row in rows
        ]

    def claim_plan_publication(
        self,
        *,
        space_id: str,
        generation: int,
        item_id: str,
        revision_key: str = "",
        thread_id: str,
        turn_id: str,
        plan_text: str = "",
        stale_after: int = 300,
    ) -> bool:
        if not all(value.strip() for value in (space_id, item_id, thread_id, turn_id)):
            raise ValueError("Plan publication identifiers must not be empty")
        if generation < 0:
            raise ValueError("generation must not be negative")
        if stale_after < 0:
            raise ValueError("stale_after must not be negative")
        now = int(time.time())
        with self._immediate_transaction() as connection:
            cursor = connection.execute(
                "INSERT OR IGNORE INTO plan_publications(space_id, generation, item_id, "
                "revision_key, thread_id, turn_id, status, plan_text, created_at, updated_at) "
                "VALUES(?, ?, ?, ?, ?, ?, 'publishing', ?, ?, ?)",
                (
                    space_id,
                    int(generation),
                    item_id,
                    revision_key,
                    thread_id,
                    turn_id,
                    plan_text,
                    now,
                    now,
                ),
            )
            if cursor.rowcount == 1:
                publication_id = int(cursor.lastrowid)
            else:
                row = connection.execute(
                    "SELECT id, status, updated_at FROM plan_publications "
                    "WHERE space_id=? AND generation=? AND item_id=? AND revision_key=?",
                    (space_id, int(generation), item_id, revision_key),
                ).fetchone()
                if row is None:
                    return False
                publication_id = int(row[0])
                latest = connection.execute(
                    "SELECT MAX(id) FROM plan_publications WHERE space_id=? AND generation=?",
                    (space_id, int(generation)),
                ).fetchone()
                if latest is None or publication_id != int(latest[0]):
                    return False
                retryable = str(row[1]) == "failed" or (
                    str(row[1]) == "publishing" and int(row[2]) <= now - stale_after
                )
                if not retryable:
                    return False
                retry = connection.execute(
                    "UPDATE plan_publications SET status='publishing', message_ids_json='[]', "
                    "action_message_ids_json='[]', tui_prompt_seen_at=NULL, decision_turn_id='', "
                    "thread_id=?, turn_id=?, plan_text=?, updated_at=? "
                    "WHERE id=? AND status=? AND updated_at=?",
                    (thread_id, turn_id, plan_text, now, publication_id, str(row[1]), int(row[2])),
                )
                if retry.rowcount != 1:
                    return False
            connection.execute(
                "UPDATE plan_publications SET status='superseded', updated_at=? "
                "WHERE space_id=? AND generation=? AND id<? "
                "AND status<>'superseded'",
                (now, space_id, int(generation), publication_id),
            )
            return True

    def finish_plan_publication(
        self,
        *,
        space_id: str,
        generation: int,
        item_id: str,
        revision_key: str = "",
        status: str,
        message_ids: list[int],
    ) -> bool:
        if status not in _PLAN_PUBLICATION_RESULTS:
            raise ValueError("Plan publication result is invalid")
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE plan_publications SET status=?, message_ids_json=?, updated_at=? "
                "WHERE space_id=? AND generation=? AND item_id=? AND revision_key=? "
                "AND status='publishing'",
                (
                    status,
                    json.dumps([int(value) for value in message_ids]),
                    int(time.time()),
                    space_id,
                    int(generation),
                    item_id,
                    revision_key,
                ),
            )
        return cursor.rowcount == 1

    def latest_plan_publication(self, space_id: str, generation: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT item_id, revision_key, thread_id, turn_id, status, plan_text, "
                "message_ids_json, action_message_ids_json, tui_prompt_seen_at, decision_turn_id, "
                "created_at, updated_at FROM plan_publications WHERE space_id=? AND generation=? "
                "ORDER BY id DESC LIMIT 1",
                (space_id, int(generation)),
            ).fetchone()
        if row is None:
            return None
        return self._plan_publication_from_row(row, space_id=space_id, generation=generation)

    @staticmethod
    def _plan_publication_from_row(
        row: sqlite3.Row | tuple[Any, ...], *, space_id: str | None = None, generation: int | None = None
    ) -> dict[str, Any]:
        offset = 0
        if space_id is None or generation is None:
            space_id = str(row[0])
            generation = int(row[1])
            offset = 2
        return {
            "space_id": space_id,
            "generation": generation,
            "item_id": str(row[offset]),
            "revision_key": str(row[offset + 1]),
            "thread_id": str(row[offset + 2]),
            "turn_id": str(row[offset + 3]),
            "status": str(row[offset + 4]),
            "plan_text": str(row[offset + 5]),
            "message_ids": json.loads(str(row[offset + 6])),
            "action_message_ids": json.loads(str(row[offset + 7])),
            "tui_prompt_seen_at": (
                int(row[offset + 8]) if row[offset + 8] is not None else None
            ),
            "decision_turn_id": str(row[offset + 9]),
            "created_at": int(row[offset + 10]),
            "updated_at": int(row[offset + 11]),
        }

    def mark_plan_action(
        self,
        space_id: str,
        generation: int,
        item_id: str,
        *,
        revision_key: str = "",
        status: str,
    ) -> bool:
        if status not in _PLAN_ACTION_STATUSES:
            raise ValueError("Plan action status is invalid")
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE plan_publications SET status=?, updated_at=? "
                "WHERE space_id=? AND generation=? AND item_id=? AND revision_key=? "
                "AND status='published' "
                "AND id=(SELECT MAX(id) FROM plan_publications "
                "WHERE space_id=? AND generation=?)",
                (
                    status,
                    int(time.time()),
                    space_id,
                    int(generation),
                    item_id,
                    revision_key,
                    space_id,
                    int(generation),
                ),
            )
        return cursor.rowcount == 1

    def release_plan_action(
        self,
        space_id: str,
        generation: int,
        item_id: str,
        *,
        revision_key: str = "",
        expected_status: str = "executing",
    ) -> bool:
        if expected_status not in _PLAN_ACTION_STATUSES:
            raise ValueError("Plan action status is invalid")
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE plan_publications SET status='published', updated_at=? "
                "WHERE space_id=? AND generation=? AND item_id=? AND revision_key=? AND status=? "
                "AND id=(SELECT MAX(id) FROM plan_publications "
                "WHERE space_id=? AND generation=?)",
                (
                    int(time.time()),
                    space_id,
                    int(generation),
                    item_id,
                    revision_key,
                    expected_status,
                    space_id,
                    int(generation),
                ),
            )
        return cursor.rowcount == 1

    def complete_plan_action(
        self,
        space_id: str,
        generation: int,
        item_id: str,
        *,
        revision_key: str = "",
        expected_status: str,
        status: str,
        decision_turn_id: str = "",
    ) -> bool:
        if expected_status not in _PLAN_ACTION_STATUSES:
            raise ValueError("Plan action status is invalid")
        if status not in _PLAN_ACTION_TERMINAL_STATUSES:
            raise ValueError("Plan action terminal status is invalid")
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE plan_publications SET status=?, decision_turn_id=?, updated_at=? "
                "WHERE space_id=? AND generation=? AND item_id=? AND revision_key=? AND status=? "
                "AND id=(SELECT MAX(id) FROM plan_publications "
                "WHERE space_id=? AND generation=?)",
                (
                    status,
                    decision_turn_id,
                    int(time.time()),
                    space_id,
                    int(generation),
                    item_id,
                    revision_key,
                    expected_status,
                    space_id,
                    int(generation),
                ),
            )
        return cursor.rowcount == 1

    def mark_external_plan_action(
        self,
        space_id: str,
        generation: int,
        item_id: str,
        *,
        revision_key: str = "",
        status: str = "executed",
        decision_turn_id: str = "",
        expected_statuses: set[str] | frozenset[str] = frozenset({"published"}),
    ) -> bool:
        if status not in _PLAN_ACTION_TERMINAL_STATUSES:
            raise ValueError("Plan action terminal status is invalid")
        selected = frozenset(str(value) for value in expected_statuses)
        allowed = frozenset({"published"}) | _PLAN_ACTION_STATUSES
        if not selected or not selected <= allowed:
            raise ValueError("Plan action expected status is invalid")
        placeholders = ",".join("?" for _ in selected)
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE plan_publications SET status=?, decision_turn_id=?, updated_at=? "
                "WHERE space_id=? AND generation=? AND item_id=? AND revision_key=? "
                f"AND status IN ({placeholders}) AND id=(SELECT MAX(id) FROM plan_publications "
                "WHERE space_id=? AND generation=?)",
                (
                    status,
                    decision_turn_id,
                    int(time.time()),
                    space_id,
                    int(generation),
                    item_id,
                    revision_key,
                    *sorted(selected),
                    space_id,
                    int(generation),
                ),
            )
        return cursor.rowcount == 1

    def mark_tui_plan_prompt_seen(
        self, space_id: str, generation: int, item_id: str, *, revision_key: str = ""
    ) -> bool:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE plan_publications SET tui_prompt_seen_at=COALESCE(tui_prompt_seen_at, ?), "
                "updated_at=? WHERE space_id=? AND generation=? AND item_id=? AND revision_key=? "
                "AND status='published' AND id=(SELECT MAX(id) FROM plan_publications "
                "WHERE space_id=? AND generation=?)",
                (
                    int(time.time()),
                    int(time.time()),
                    space_id,
                    int(generation),
                    item_id,
                    revision_key,
                    space_id,
                    int(generation),
                ),
            )
        return cursor.rowcount == 1

    def append_plan_action_message(
        self,
        space_id: str,
        generation: int,
        item_id: str,
        message_id: int,
        *,
        revision_key: str = "",
    ) -> bool:
        with self._lock, self._connection:
            row = self._connection.execute(
                "SELECT action_message_ids_json FROM plan_publications WHERE space_id=? "
                "AND generation=? AND item_id=? AND revision_key=?",
                (space_id, int(generation), item_id, revision_key),
            ).fetchone()
            if row is None:
                return False
            values = [int(value) for value in json.loads(str(row[0]))]
            if int(message_id) not in values:
                values.append(int(message_id))
            cursor = self._connection.execute(
                "UPDATE plan_publications SET action_message_ids_json=?, updated_at=? "
                "WHERE space_id=? AND generation=? AND item_id=? AND revision_key=?",
                (
                    json.dumps(values),
                    int(time.time()),
                    space_id,
                    int(generation),
                    item_id,
                    revision_key,
                ),
            )
        return cursor.rowcount == 1

    def retire_plan_callbacks(self, space_id: str, generation: int) -> int:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE callbacks SET used_at=? WHERE space_id=? AND generation=? "
                "AND used_at IS NULL AND action IN ('plan_execute', 'plan_continue')",
                (int(time.time()), space_id, int(generation)),
            )
        return int(cursor.rowcount)

    def retire_stale_plan_callbacks(
        self, space_id: str, generation: int, item_id: str, revision_key: str
    ) -> int:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE callbacks SET used_at=? WHERE space_id=? AND generation=? "
                "AND used_at IS NULL AND action IN ('plan_execute', 'plan_continue') "
                "AND (json_extract(payload_json, '$.item_id')<>? "
                "OR json_extract(payload_json, '$.revision_key')<>?)",
                (int(time.time()), space_id, int(generation), item_id, revision_key),
            )
        return int(cursor.rowcount)

    def find_tui_plan_approval_turn(
        self, thread_id: str, *, after: int, prompt: str
    ) -> str | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT turn_id FROM tui_plan_approvals WHERE thread_id=? "
                "AND created_at>=? AND prompt=? ORDER BY created_at, event_key LIMIT 1",
                (thread_id, int(after), prompt),
            ).fetchone()
        return str(row[0]) if row is not None and row[0] else None

    def plan_publications_for_ui_repair(self) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in _PLAN_UI_REPAIR_STATUSES)
        with self._lock:
            rows = self._connection.execute(
                "SELECT publications.space_id, publications.generation, publications.item_id, "
                "publications.revision_key, publications.thread_id, publications.turn_id, "
                "publications.status, publications.plan_text, publications.message_ids_json, "
                "publications.action_message_ids_json, publications.tui_prompt_seen_at, "
                "publications.decision_turn_id, publications.created_at, publications.updated_at "
                "FROM plan_publications AS publications JOIN session_spaces AS spaces "
                "ON spaces.space_id=publications.space_id "
                "AND spaces.generation=publications.generation "
                "WHERE spaces.lifecycle='active' "
                f"AND publications.status IN ({placeholders}) ORDER BY publications.id",
                tuple(sorted(_PLAN_UI_REPAIR_STATUSES)),
            ).fetchall()
        return [self._plan_publication_from_row(row) for row in rows]

    def executing_plan_publications(self) -> list[dict[str, Any]]:
        return self.recoverable_plan_publications({"executing"})

    def recoverable_plan_publications(
        self, statuses: set[str] | frozenset[str] = _PLAN_ACTION_STATUSES
    ) -> list[dict[str, Any]]:
        selected = frozenset(str(value) for value in statuses)
        if not selected or not selected <= _PLAN_ACTION_STATUSES:
            raise ValueError("Plan recovery statuses are invalid")
        placeholders = ",".join("?" for _ in selected)
        with self._lock:
            rows = self._connection.execute(
                "SELECT space_id, generation, item_id, revision_key, thread_id, turn_id, status, "
                "plan_text, message_ids_json, action_message_ids_json, tui_prompt_seen_at, "
                "decision_turn_id, created_at, updated_at FROM plan_publications "
                f"WHERE status IN ({placeholders}) ORDER BY updated_at, id",
                tuple(sorted(selected)),
            ).fetchall()
        return [self._plan_publication_from_row(row) for row in rows]

    @staticmethod
    def _interaction_from_row(row: sqlite3.Row) -> InteractionDraft:
        payload = json.loads(str(row[5]))
        if not isinstance(payload, dict):
            payload = {}
        return InteractionDraft(
            scope_key=str(row[0]),
            flow_id=str(row[1]),
            revision=int(row[2]),
            kind=str(row[3]),
            phase=str(row[4]),
            payload=payload,
            user_id=int(row[6]),
            bot_role=str(row[7]),
            chat_id=int(row[8]),
            space_id=str(row[9]) if row[9] is not None else None,
            generation=int(row[10]),
            expires_at=int(row[11]),
            claimed_at=int(row[12]) if row[12] is not None else None,
            created_at=int(row[13]),
            updated_at=int(row[14]),
        )

    def replace_interaction(
        self,
        scope_key: str,
        *,
        kind: str,
        phase: str,
        payload: Mapping[str, Any],
        user_id: int,
        bot_role: str,
        chat_id: int,
        expires_at: int,
        space_id: str | None = None,
        generation: int = 0,
    ) -> InteractionDraft:
        if not all(value.strip() for value in (scope_key, kind, phase, bot_role)):
            raise ValueError("Interaction identifiers must not be empty")
        if generation < 0:
            raise ValueError("Interaction generation must not be negative")
        now = int(time.time())
        flow_id = uuid.uuid4().hex
        encoded = _json_mapping(payload)
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT INTO interaction_drafts(scope_key, flow_id, revision, kind, phase, "
                "payload_json, user_id, bot_role, chat_id, space_id, generation, expires_at, "
                "claimed_at, created_at, updated_at) VALUES(?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                "NULL, ?, ?) ON CONFLICT(scope_key) DO UPDATE SET flow_id=excluded.flow_id, "
                "revision=1, kind=excluded.kind, phase=excluded.phase, "
                "payload_json=excluded.payload_json, user_id=excluded.user_id, "
                "bot_role=excluded.bot_role, chat_id=excluded.chat_id, space_id=excluded.space_id, "
                "generation=excluded.generation, expires_at=excluded.expires_at, claimed_at=NULL, "
                "created_at=excluded.created_at, updated_at=excluded.updated_at",
                (
                    scope_key,
                    flow_id,
                    kind,
                    phase,
                    encoded,
                    int(user_id),
                    bot_role,
                    int(chat_id),
                    space_id,
                    int(generation),
                    int(expires_at),
                    now,
                    now,
                ),
            )
        draft = self.get_interaction(scope_key)
        if draft is None:
            raise RuntimeError("Interaction draft could not be persisted")
        return draft

    def get_interaction(self, scope_key: str) -> InteractionDraft | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT scope_key, flow_id, revision, kind, phase, payload_json, user_id, "
                "bot_role, chat_id, space_id, generation, expires_at, claimed_at, created_at, "
                "updated_at FROM interaction_drafts WHERE scope_key=? AND claimed_at IS NULL",
                (scope_key,),
            ).fetchone()
        return self._interaction_from_row(row) if row else None

    def advance_interaction(
        self,
        scope_key: str,
        flow_id: str,
        revision: int,
        *,
        phase: str,
        payload: Mapping[str, Any],
        expires_at: int,
    ) -> InteractionDraft | None:
        now = int(time.time())
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE interaction_drafts SET revision=revision+1, phase=?, payload_json=?, "
                "expires_at=?, updated_at=? WHERE scope_key=? AND flow_id=? AND revision=? "
                "AND claimed_at IS NULL AND expires_at>?",
                (
                    phase,
                    _json_mapping(payload),
                    int(expires_at),
                    now,
                    scope_key,
                    flow_id,
                    int(revision),
                    now,
                ),
            )
        return self.get_interaction(scope_key) if cursor.rowcount == 1 else None

    def claim_interaction(self, scope_key: str, flow_id: str, revision: int) -> InteractionDraft | None:
        return self._claim_interaction(scope_key, flow_id, revision)

    def claim_live_interaction(self, scope_key: str, flow_id: str, revision: int) -> InteractionDraft | None:
        return self._claim_interaction(scope_key, flow_id, revision, expiry="live")

    def claim_expired_interaction(
        self, scope_key: str, flow_id: str, revision: int
    ) -> InteractionDraft | None:
        return self._claim_interaction(scope_key, flow_id, revision, expiry="expired")

    def _claim_interaction(
        self,
        scope_key: str,
        flow_id: str,
        revision: int,
        *,
        expiry: str | None = None,
    ) -> InteractionDraft | None:
        if expiry not in {None, "live", "expired"}:
            raise ValueError("Interaction expiry condition is invalid")
        now = int(time.time())
        expiry_sql = (
            " AND expires_at>?" if expiry == "live" else " AND expires_at<=?" if expiry == "expired" else ""
        )
        params: tuple[object, ...] = (scope_key, flow_id, int(revision))
        if expiry is not None:
            params = (*params, now)
        with self._immediate_transaction() as connection:
            row = connection.execute(
                "SELECT scope_key, flow_id, revision, kind, phase, payload_json, user_id, "
                "bot_role, chat_id, space_id, generation, expires_at, claimed_at, created_at, "
                "updated_at FROM interaction_drafts WHERE scope_key=? AND flow_id=? "
                f"AND revision=? AND claimed_at IS NULL{expiry_sql}",
                params,
            ).fetchone()
            if row is None:
                return None
            cursor = connection.execute(
                "UPDATE interaction_drafts SET claimed_at=?, updated_at=? WHERE scope_key=? "
                f"AND flow_id=? AND revision=? AND claimed_at IS NULL{expiry_sql}",
                (now, now, *params),
            )
            if cursor.rowcount != 1:
                return None
        claimed = self._interaction_from_row(row)
        claimed.claimed_at = now
        claimed.updated_at = now
        return claimed

    def delete_interaction(self, scope_key: str) -> None:
        with self._lock, self._connection:
            self._connection.execute("DELETE FROM interaction_drafts WHERE scope_key=?", (scope_key,))

    def list_interactions(self, kind: str | None = None) -> list[InteractionDraft]:
        query = (
            "SELECT scope_key, flow_id, revision, kind, phase, payload_json, user_id, bot_role, "
            "chat_id, space_id, generation, expires_at, claimed_at, created_at, updated_at "
            "FROM interaction_drafts WHERE claimed_at IS NULL"
        )
        params: tuple[object, ...] = ()
        if kind is not None:
            query += " AND kind=?"
            params = (kind,)
        query += " ORDER BY updated_at, scope_key"
        with self._lock:
            rows = self._connection.execute(query, params).fetchall()
        return [self._interaction_from_row(row) for row in rows]

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

    def ensure_callback(
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
    ) -> str:
        payload_json = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        now = int(time.time())
        with self._immediate_transaction() as connection:
            row = connection.execute(
                "SELECT nonce FROM callbacks WHERE action=? AND payload_json=? AND user_id=? "
                "AND bot_role=? AND chat_id IS ? AND space_id IS ? AND generation=? "
                "AND used_at IS NULL AND expires_at>=? ORDER BY expires_at DESC LIMIT 1",
                (
                    action,
                    payload_json,
                    user_id,
                    bot_role,
                    chat_id,
                    space_id,
                    generation,
                    now,
                ),
            ).fetchone()
            if row is not None:
                existing = str(row[0])
                connection.execute(
                    "UPDATE callbacks SET expires_at=MAX(expires_at, ?) WHERE nonce=?",
                    (expires_at, existing),
                )
                return existing
            connection.execute(
                "INSERT INTO callbacks(nonce, action, payload_json, user_id, expires_at, bot_role, "
                "chat_id, space_id, generation) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    nonce,
                    action,
                    payload_json,
                    user_id,
                    expires_at,
                    bot_role,
                    chat_id,
                    space_id,
                    generation,
                ),
            )
        return nonce

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

    def live_question_reply_callbacks(
        self,
        user_id: int,
        *,
        bot_role: str,
        chat_id: int,
        space_id: str,
        generation: int,
    ) -> list[dict[str, Any]]:
        now = int(time.time())
        with self._lock:
            rows = self._connection.execute(
                "SELECT nonce, action, payload_json FROM callbacks "
                "WHERE user_id=? AND bot_role=? AND chat_id=? AND space_id=? AND generation=? "
                "AND used_at IS NULL AND expires_at>=? "
                "AND action IN ('reply_question_custom', 'reply_question_clarify') "
                "ORDER BY rowid",
                (user_id, bot_role, int(chat_id), space_id, int(generation), now),
            ).fetchall()
        callbacks: list[dict[str, Any]] = []
        for row in rows:
            payload = json.loads(str(row[2]))
            if not isinstance(payload, dict):
                continue
            callbacks.append(
                {
                    "nonce": str(row[0]),
                    "action": str(row[1]),
                    "payload": payload,
                }
            )
        return callbacks

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
            row = connection.execute("SELECT value FROM metadata WHERE key='totp_auth_epoch'").fetchone()
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
        event_days: int = EVENT_RETENTION_DAYS,
        update_days: int = 30,
        keep_recent_updates: int = 1000,
    ) -> dict[str, int]:
        if event_days <= 0 or update_days <= 0 or keep_recent_updates < 0:
            raise ValueError("Cleanup limits are invalid")
        connection = sqlite3.connect(self.path, timeout=1.0)
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={MAINTENANCE_BUSY_TIMEOUT_MS}")
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            return self._cleanup_on_connection(
                connection,
                event_days=event_days,
                update_days=update_days,
                keep_recent_updates=keep_recent_updates,
            )
        finally:
            connection.close()

    @staticmethod
    def _delete_in_batches(
        connection: sqlite3.Connection,
        table: str,
        where: str,
        params: tuple[Any, ...],
    ) -> int:
        deleted = 0
        while True:
            connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = connection.execute(
                    f"DELETE FROM {table} WHERE rowid IN ("
                    f"SELECT rowid FROM {table} WHERE {where} LIMIT ?)",
                    (*params, MAINTENANCE_BATCH_SIZE),
                )
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()
            count = max(0, int(cursor.rowcount))
            deleted += count
            if count < MAINTENANCE_BATCH_SIZE:
                return deleted

    @staticmethod
    def _delete_ranked_timeline_batches(connection: sqlite3.Connection) -> int:
        deleted = 0
        while True:
            connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = connection.execute(
                    "DELETE FROM timeline_events WHERE id IN (SELECT id FROM ("
                    "SELECT id, ROW_NUMBER() OVER(PARTITION BY thread_id "
                    "ORDER BY created_at DESC, id DESC) AS ordinal FROM timeline_events"
                    ") WHERE ordinal>? LIMIT ?)",
                    (TIMELINE_PER_THREAD_LIMIT, MAINTENANCE_BATCH_SIZE),
                )
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()
            count = max(0, int(cursor.rowcount))
            deleted += count
            if count < MAINTENANCE_BATCH_SIZE:
                return deleted

    @staticmethod
    def _retire_questions_on(connection: sqlite3.Connection, now: int) -> int:
        retired = 0
        while True:
            rows = connection.execute(
                "SELECT request_key FROM pending_inputs "
                "WHERE status='resolved' OR (expires_at IS NOT NULL AND expires_at < ?) "
                "UNION SELECT messages.request_key FROM question_messages AS messages "
                "LEFT JOIN pending_inputs AS pending ON pending.request_key=messages.request_key "
                "AND pending.status!='resolved' "
                "WHERE pending.request_key IS NULL ORDER BY request_key LIMIT ?",
                (now, MAINTENANCE_BATCH_SIZE),
            ).fetchall()
            if not rows:
                return retired
            connection.execute("BEGIN IMMEDIATE")
            try:
                for row in rows:
                    request_key = str(row[0])
                    messages = connection.execute(
                        "SELECT bot_role, chat_id, message_id FROM question_messages "
                        "WHERE request_key=? ORDER BY created_at, message_id",
                        (request_key,),
                    ).fetchall()
                    for bot_role, chat_id, message_id in messages:
                        connection.execute(
                            "INSERT INTO scheduled_deletions("
                            "bot_role, chat_id, message_id, delete_at, group_key, created_at) "
                            "VALUES(?, ?, ?, ?, ?, ?) "
                            "ON CONFLICT(bot_role, chat_id, message_id) DO UPDATE SET "
                            "delete_at=excluded.delete_at, group_key=excluded.group_key, "
                            "attempts=0, last_error=NULL",
                            (
                                str(bot_role),
                                int(chat_id),
                                int(message_id),
                                now,
                                f"question:{request_key}",
                                now,
                            ),
                        )
                    connection.execute(
                        "DELETE FROM question_messages WHERE request_key=?", (request_key,)
                    )
                    connection.execute("DELETE FROM pending_inputs WHERE request_key=?", (request_key,))
                    connection.execute(
                        "DELETE FROM question_resolutions WHERE request_key=?", (request_key,)
                    )
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()
            retired += len(rows)

    @classmethod
    def _cleanup_on_connection(
        cls,
        connection: sqlite3.Connection,
        *,
        event_days: int,
        update_days: int,
        keep_recent_updates: int,
    ) -> dict[str, int]:
        now = int(time.time())
        retention_cutoff = now - event_days * 86400
        connection.execute(
            "CREATE TEMP TABLE IF NOT EXISTS maintenance_managed_threads("
            "thread_id TEXT PRIMARY KEY) WITHOUT ROWID"
        )
        connection.execute("DELETE FROM maintenance_managed_threads")
        connection.executemany(
            "INSERT INTO maintenance_managed_threads(thread_id) VALUES(?)",
            ((thread_id,) for thread_id in sorted(_managed_thread_ids(connection))),
        )
        connection.commit()
        deleted: dict[str, int] = {
            "retired_questions": cls._retire_questions_on(connection, now),
            "callbacks": cls._delete_in_batches(
                connection, "callbacks", "expires_at < ? OR used_at IS NOT NULL", (now,)
            ),
            "interactions": cls._delete_in_batches(
                connection,
                "interaction_drafts",
                "claimed_at IS NOT NULL AND updated_at < ?",
                (now - 86400,),
            ),
            "event_receipts": cls._delete_in_batches(
                connection, "event_receipts", "created_at < ?", (retention_cutoff,)
            ),
            "timeline": cls._delete_in_batches(
                connection,
                "timeline_events",
                "created_at < ? OR thread_id NOT IN ("
                "SELECT thread_id FROM maintenance_managed_threads)",
                (retention_cutoff,),
            ),
            "tui_approvals": cls._delete_in_batches(
                connection, "tui_plan_approvals", "created_at < ?", (retention_cutoff,)
            ),
            "telegram_updates": cls._delete_in_batches(
                connection,
                "telegram_updates",
                "received_at < ? AND sequence NOT IN ("
                "SELECT sequence FROM telegram_updates ORDER BY sequence DESC LIMIT ?)",
                (now - update_days * 86400, keep_recent_updates),
            ),
            "scheduled_deletions": cls._delete_in_batches(
                connection,
                "scheduled_deletions",
                "delete_at < ? AND attempts >= 20",
                (now - 7 * 86400,),
            ),
            "question_resolutions": cls._delete_in_batches(
                connection, "question_resolutions", "resolved_at < ?", (retention_cutoff,)
            ),
            "prompt_runs": cls._delete_in_batches(
                connection, "prompt_runs", "updated_at < ?", (retention_cutoff,)
            ),
            "prompt_intents": cls._delete_in_batches(
                connection,
                "prompt_intents",
                "state IN ('completed', 'failed', 'uncertain', 'cancelled') AND updated_at < ?",
                (retention_cutoff,),
            ),
            "telegram_message_state": cls._delete_in_batches(
                connection, "telegram_message_state", "updated_at < ?", (retention_cutoff,)
            ),
            "plan_publications": cls._delete_in_batches(
                connection,
                "plan_publications",
                "updated_at < ? AND (status IN ('superseded', 'failed') OR EXISTS ("
                "SELECT 1 FROM plan_publications AS newer "
                "WHERE newer.space_id=plan_publications.space_id "
                "AND newer.generation=plan_publications.generation "
                "AND newer.id>plan_publications.id) OR NOT EXISTS ("
                "SELECT 1 FROM session_spaces AS spaces "
                "WHERE spaces.space_id=plan_publications.space_id "
                "AND spaces.generation=plan_publications.generation "
                "AND spaces.lifecycle!='closed'))",
                (retention_cutoff,),
            ),
            "outbound_intents": cls._delete_in_batches(
                connection,
                "outbound_intents",
                "status IN ('delivered', 'uncertain', 'failed') AND updated_at < ?",
                (retention_cutoff,),
            ),
        }
        deleted["event_receipts"] += cls._delete_in_batches(
            connection,
            "event_receipts",
            "rowid NOT IN (SELECT rowid FROM event_receipts "
            "ORDER BY created_at DESC, rowid DESC LIMIT ?)",
            (EVENT_RECEIPT_LIMIT,),
        )
        deleted["timeline"] += cls._delete_ranked_timeline_batches(connection)
        deleted["outbound_intents"] += cls._delete_in_batches(
            connection,
            "outbound_intents",
            "status IN ('delivered', 'uncertain', 'failed') AND rowid NOT IN ("
            "SELECT rowid FROM outbound_intents "
            "WHERE status IN ('delivered', 'uncertain', 'failed') "
            "ORDER BY updated_at DESC, rowid DESC LIMIT ?)",
            (OUTBOUND_TERMINAL_LIMIT,),
        )
        connection.execute("DROP TABLE maintenance_managed_threads")
        connection.commit()
        return deleted

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
                "SELECT id, thread_id, prompt, inputs_json, client_message_id, created_at, "
                "prompt_intent_id "
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
                prompt_intent_id=str(row[6]) if row[6] else None,
            )
            for row in rows
        ]

    def space_queue_entries(self, space_id: str, generation: int | None = None) -> list[dict[str, Any]]:
        parameters: list[Any] = [space_id]
        generation_filter = ""
        if generation is not None:
            generation_filter = " AND generation=?"
            parameters.append(generation)
        with self._lock:
            rows = self._connection.execute(
                "SELECT id, thread_id, prompt, inputs_json, client_message_id, created_at, generation, "
                "prompt_intent_id "
                "FROM prompt_queue WHERE space_id=? AND status='queued'" + generation_filter + " ORDER BY id",
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
                "prompt_intent_id": str(row[7]) if row[7] else None,
            }
            for row in rows
        ]

    def next_space_prompt(self, space_id: str, generation: int) -> dict[str, Any] | None:
        entries = self.space_queue_entries(space_id, generation)
        return entries[0] if entries else None

    def cancel_space_prompt(self, space_id: str, queue_id: int, generation: int | None = None) -> bool:
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
                    "SELECT id FROM scheduled_deletions WHERE bot_role=? AND chat_id=? AND message_id=?",
                    (bot_role, int(chat_id), message_id),
                ).fetchone()
                identifiers.append(int(row[0]))
        return identifiers

    def due_message_deletions(self, *, now: int | None = None, limit: int = 100) -> list[dict[str, Any]]:
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

    def reschedule_message_deletion(self, deletion_id: int, delete_at: int, error: str = "") -> bool:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE scheduled_deletions SET delete_at=?, attempts=attempts+1, last_error=? WHERE id=?",
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
                "item_id, questions_json, expires_at, created_at, status) "
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')",
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

    def claim_pending_input(self, request_key: str) -> dict[str, Any] | None:
        with self._immediate_transaction() as connection:
            cursor = connection.execute(
                "UPDATE pending_inputs SET status='claimed', claimed_at=? "
                "WHERE request_key=? AND status='pending'",
                (int(time.time()), request_key),
            )
            if cursor.rowcount != 1:
                return None
        return self.get_pending_input(request_key)

    def release_pending_input_claim(self, request_key: str) -> bool:
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE pending_inputs SET status='pending', claimed_at=NULL "
                "WHERE request_key=? AND status='claimed'",
                (request_key,),
            )
        return cursor.rowcount == 1

    def mark_pending_input_responded(
        self,
        request_key: str,
        response: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        encoded = _json_mapping(response)
        with self._immediate_transaction() as connection:
            cursor = connection.execute(
                "UPDATE pending_inputs SET status='awaiting_resolved', responded_at=?, response_json=? "
                "WHERE request_key=? AND status='claimed'",
                (int(time.time()), encoded, request_key),
            )
            if cursor.rowcount != 1:
                return None
        return self.get_pending_input(request_key)

    def resolve_pending_input(
        self,
        request_key: str,
        *,
        source: str,
        response: Mapping[str, Any] | None = None,
    ) -> bool:
        if not source.strip():
            raise ValueError("Pending input resolution source must not be empty")
        encoded = _json_mapping(response) if response is not None else None
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "UPDATE pending_inputs SET status='resolved', resolved_at=?, "
                "response_json=COALESCE(?, response_json), resolution_source=? "
                "WHERE request_key=? AND status!='resolved'",
                (int(time.time()), encoded, source, request_key),
            )
        return cursor.rowcount == 1

    def record_question_message(
        self,
        request_key: str,
        bot_role: str,
        chat_id: int,
        message_id: int,
        *,
        message_kind: str = "interaction",
    ) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT OR IGNORE INTO question_messages(request_key, bot_role, chat_id, message_id, "
                "message_kind, created_at) VALUES(?, ?, ?, ?, ?, ?)",
                (
                    request_key,
                    bot_role,
                    int(chat_id),
                    int(message_id),
                    message_kind,
                    int(time.time()),
                ),
            )

    def question_messages(self, request_key: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT bot_role, chat_id, message_id, message_kind FROM question_messages "
                "WHERE request_key=? ORDER BY created_at, message_id",
                (request_key,),
            ).fetchall()
        return [
            {
                "bot_role": str(row[0]),
                "chat_id": int(row[1]),
                "message_id": int(row[2]),
                "message_kind": str(row[3]),
            }
            for row in rows
        ]

    def pop_question_messages(self, request_key: str) -> list[dict[str, Any]]:
        with self._immediate_transaction() as connection:
            rows = connection.execute(
                "SELECT bot_role, chat_id, message_id, message_kind FROM question_messages "
                "WHERE request_key=? ORDER BY created_at, message_id",
                (request_key,),
            ).fetchall()
            connection.execute("DELETE FROM question_messages WHERE request_key=?", (request_key,))
        return [
            {
                "bot_role": str(row[0]),
                "chat_id": int(row[1]),
                "message_id": int(row[2]),
                "message_kind": str(row[3]),
            }
            for row in rows
        ]

    def save_question_resolution(
        self,
        request_key: str,
        answers: Mapping[str, list[str]],
        *,
        source: str,
    ) -> bool:
        if not request_key.strip() or not source.strip():
            raise ValueError("Question resolution identifiers must not be empty")
        now = int(time.time())
        encoded = json.dumps(dict(answers), ensure_ascii=False, separators=(",", ":"))
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "INSERT OR IGNORE INTO question_resolutions("
                "request_key, answers_json, source, resolved_at) VALUES(?, ?, ?, ?)",
                (request_key, encoded, source, now),
            )
        return cursor.rowcount == 1

    def pop_question_resolution(self, request_key: str) -> dict[str, Any] | None:
        with self._immediate_transaction() as connection:
            row = connection.execute(
                "SELECT answers_json, source, resolved_at FROM question_resolutions WHERE request_key=?",
                (request_key,),
            ).fetchone()
            connection.execute("DELETE FROM question_resolutions WHERE request_key=?", (request_key,))
        if row is None:
            return None
        answers = json.loads(str(row[0]))
        return {
            "answers": answers if isinstance(answers, dict) else {},
            "source": str(row[1]),
            "resolved_at": int(row[2]),
        }

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
                    "WHERE status='resolved' OR (expires_at IS NOT NULL AND expires_at < ?) "
                    "UNION SELECT messages.request_key FROM question_messages AS messages "
                    "LEFT JOIN pending_inputs AS pending ON pending.request_key=messages.request_key "
                    "AND pending.status!='resolved' "
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
                connection.execute("DELETE FROM question_messages WHERE request_key=?", (request_key,))
                connection.execute("DELETE FROM pending_inputs WHERE request_key=?", (request_key,))
                connection.execute("DELETE FROM question_resolutions WHERE request_key=?", (request_key,))
        return request_keys

    def get_pending_input(self, request_key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT request_id, generation, thread_id, turn_id, item_id, questions_json, expires_at, "
                "status, claimed_at, responded_at, resolved_at, response_json, resolution_source "
                "FROM pending_inputs WHERE request_key=? AND status!='resolved'",
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
            "status": str(row[7]),
            "claimed_at": int(row[8]) if row[8] is not None else None,
            "responded_at": int(row[9]) if row[9] is not None else None,
            "resolved_at": int(row[10]) if row[10] is not None else None,
            "response": json.loads(str(row[11])) if row[11] else None,
            "resolution_source": str(row[12]) if row[12] else None,
        }

    def pending_input_keys_for_request(self, request_id: int | str) -> list[str]:
        encoded = json.dumps(request_id)
        compact = json.dumps(request_id, ensure_ascii=False, separators=(",", ":"))
        candidates = tuple(dict.fromkeys((str(request_id), encoded, compact)))
        placeholders = ",".join("?" for _ in candidates)
        with self._lock:
            rows = self._connection.execute(
                f"SELECT request_key FROM pending_inputs WHERE request_id IN ({placeholders}) "
                "AND status!='resolved' "
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
            connection.execute("DELETE FROM interaction_drafts")
            connection.execute("DELETE FROM pending_inputs")
            connection.execute("DELETE FROM question_resolutions")
            connection.execute("DELETE FROM prompt_runs")
            connection.execute("DELETE FROM prompt_intents")
            connection.execute("DELETE FROM telegram_message_state")
            connection.execute("DELETE FROM plan_publications")
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
                        "observed_mode": "unknown",
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
