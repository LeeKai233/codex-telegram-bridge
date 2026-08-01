//! SQLite adapter with WAL mode and transactional business-record/event writes.

use ctg_domain::{
    ApprovalDecision, ApprovalId, ApprovalRequest, Artifact, ArtifactId, DomainEvent, Session,
    SessionId, SessionStatus,
};
use ctg_ports::{ApprovalStore, ArtifactStore, EventLog, PortError, PortResult, SessionRepository};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::path::Path;
use std::sync::Mutex;

/// This is deliberately independent from the Python bridge schema. Rust
/// deployments receive a new database path and never migrate Python state.
const SCHEMA_VERSION: i64 = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RustSessionSpace {
    pub space_id: String,
    pub thread_id: Option<String>,
    pub lifecycle: String,
    pub generation: i64,
    pub channel_chat_id: i64,
    pub channel_post_id: i64,
    pub discussion_chat_id: Option<i64>,
    pub discussion_root_message_id: Option<i64>,
    pub status_message_id: Option<i64>,
    pub status_bot_instance: Option<String>,
    pub owner_chat_id: Option<i64>,
    pub plan_mode: bool,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeCommentRoot {
    pub channel_chat_id: i64,
    pub channel_post_id: i64,
    pub discussion_chat_id: i64,
    pub root_message_id: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredCallback {
    pub nonce: String,
    pub space_id: String,
    pub generation: i64,
    pub action: String,
    pub expires_at_ms: i64,
}

pub struct SqliteStore {
    connection: Mutex<Connection>,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> PortResult<Self> {
        let connection = Connection::open(path).map_err(sql_error)?;
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(sql_error)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(sql_error)?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(sql_error)?;
        connection
            .pragma_update(None, "synchronous", "NORMAL")
            .map_err(sql_error)?;
        let store = Self {
            connection: Mutex::new(connection),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn in_memory() -> PortResult<Self> {
        let connection = Connection::open_in_memory().map_err(sql_error)?;
        connection
            .pragma_update(None, "foreign_keys", "ON")
            .map_err(sql_error)?;
        let store = Self {
            connection: Mutex::new(connection),
        };
        store.migrate()?;
        Ok(store)
    }

    pub fn schema_version(&self) -> PortResult<i64> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(sql_error)
    }

    /// Idempotently records an update before it reaches a controller. The
    /// returned boolean is false for a replay, which callers must not dispatch.
    pub fn record_processed_update(
        &self,
        bot_instance_id: &str,
        update_id: i64,
        received_at_ms: i64,
    ) -> PortResult<bool> {
        if bot_instance_id.trim().is_empty() {
            return Err(PortError::Adapter("bot instance id cannot be empty".into()));
        }
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        let inserted = transaction
            .execute(
                "INSERT OR IGNORE INTO rust_processed_updates(bot_instance_id, update_id, received_at_ms) VALUES (?1, ?2, ?3)",
                params![bot_instance_id, update_id, received_at_ms],
            )
            .map_err(sql_error)?
            == 1;
        if inserted {
            transaction
                .execute(
                    "INSERT INTO rust_poll_offsets(bot_instance_id, next_update_id, updated_at_ms) VALUES (?1, ?2, ?3) ON CONFLICT(bot_instance_id) DO UPDATE SET next_update_id=MAX(rust_poll_offsets.next_update_id, excluded.next_update_id), updated_at_ms=excluded.updated_at_ms",
                    params![bot_instance_id, update_id.saturating_add(1), received_at_ms],
                )
                .map_err(sql_error)?;
        }
        transaction.commit().map_err(sql_error)?;
        Ok(inserted)
    }

    pub fn next_update_offset(&self, bot_instance_id: &str) -> PortResult<Option<i64>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT next_update_id FROM rust_poll_offsets WHERE bot_instance_id=?1",
                params![bot_instance_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)
    }

    pub fn upsert_session_space(&self, space: &RustSessionSpace) -> PortResult<()> {
        validate_space(space)?;
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .execute(
                "INSERT INTO rust_session_spaces(space_id, thread_id, lifecycle, generation, channel_chat_id, channel_post_id, discussion_chat_id, discussion_root_message_id, status_message_id, status_bot_instance, owner_chat_id, plan_mode, created_at_ms, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14) ON CONFLICT(space_id) DO UPDATE SET thread_id=excluded.thread_id, lifecycle=excluded.lifecycle, generation=excluded.generation, channel_chat_id=excluded.channel_chat_id, channel_post_id=excluded.channel_post_id, discussion_chat_id=excluded.discussion_chat_id, discussion_root_message_id=excluded.discussion_root_message_id, status_message_id=excluded.status_message_id, status_bot_instance=excluded.status_bot_instance, owner_chat_id=excluded.owner_chat_id, plan_mode=excluded.plan_mode, updated_at_ms=excluded.updated_at_ms",
                params![
                    space.space_id,
                    space.thread_id,
                    space.lifecycle,
                    space.generation,
                    space.channel_chat_id,
                    space.channel_post_id,
                    space.discussion_chat_id,
                    space.discussion_root_message_id,
                    space.status_message_id,
                    space.status_bot_instance,
                    space.owner_chat_id,
                    i64::from(space.plan_mode),
                    space.created_at_ms,
                    space.updated_at_ms,
                ],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    pub fn get_session_space(&self, space_id: &str) -> PortResult<Option<RustSessionSpace>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT space_id, thread_id, lifecycle, generation, channel_chat_id, channel_post_id, discussion_chat_id, discussion_root_message_id, status_message_id, status_bot_instance, owner_chat_id, plan_mode, created_at_ms, updated_at_ms FROM rust_session_spaces WHERE space_id=?1",
                params![space_id],
                row_to_space,
            )
            .optional()
            .map_err(sql_error)
    }

    /// Saves the immutable channel-post to discussion-root relationship. It
    /// neither requires nor writes `message_thread_id`, because linked channel
    /// discussion groups are commonly ordinary supergroups rather than Topics.
    pub fn bind_native_comment_root(
        &self,
        root: &NativeCommentRoot,
        now_ms: i64,
    ) -> PortResult<()> {
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        transaction
            .execute(
                "INSERT INTO rust_native_comment_roots(channel_chat_id, channel_post_id, discussion_chat_id, root_message_id, created_at_ms) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(channel_chat_id, channel_post_id) DO UPDATE SET discussion_chat_id=excluded.discussion_chat_id, root_message_id=excluded.root_message_id",
                params![root.channel_chat_id, root.channel_post_id, root.discussion_chat_id, root.root_message_id, now_ms],
            )
            .map_err(sql_error)?;
        transaction
            .execute(
                "UPDATE rust_session_spaces SET discussion_chat_id=?1, discussion_root_message_id=?2, updated_at_ms=?3 WHERE channel_chat_id=?4 AND channel_post_id=?5",
                params![root.discussion_chat_id, root.root_message_id, now_ms, root.channel_chat_id, root.channel_post_id],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)
    }

    pub fn native_comment_root(
        &self,
        channel_chat_id: i64,
        channel_post_id: i64,
    ) -> PortResult<Option<NativeCommentRoot>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT channel_chat_id, channel_post_id, discussion_chat_id, root_message_id FROM rust_native_comment_roots WHERE channel_chat_id=?1 AND channel_post_id=?2",
                params![channel_chat_id, channel_post_id],
                |row| Ok(NativeCommentRoot { channel_chat_id: row.get(0)?, channel_post_id: row.get(1)?, discussion_chat_id: row.get(2)?, root_message_id: row.get(3)? }),
            )
            .optional()
            .map_err(sql_error)
    }

    pub fn session_space_for_discussion_root(
        &self,
        discussion_chat_id: i64,
        root_message_id: i64,
    ) -> PortResult<Option<RustSessionSpace>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT space_id, thread_id, lifecycle, generation, channel_chat_id, channel_post_id, discussion_chat_id, discussion_root_message_id, status_message_id, status_bot_instance, owner_chat_id, plan_mode, created_at_ms, updated_at_ms FROM rust_session_spaces WHERE discussion_chat_id=?1 AND discussion_root_message_id=?2",
                params![discussion_chat_id, root_message_id],
                row_to_space,
            )
            .optional()
            .map_err(sql_error)
    }

    pub fn create_callback(&self, callback: &StoredCallback) -> PortResult<()> {
        if callback.nonce.trim().is_empty() || callback.action.trim().is_empty() {
            return Err(PortError::Adapter(
                "callback nonce and action cannot be empty".into(),
            ));
        }
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .execute(
                "INSERT INTO rust_callbacks(nonce, space_id, generation, action, expires_at_ms) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![callback.nonce, callback.space_id, callback.generation, callback.action, callback.expires_at_ms],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    /// Atomically consumes a callback once. A stale generation or expired nonce
    /// is treated as absent so it can never affect a rebuilt comment thread.
    pub fn take_callback(&self, nonce: &str, now_ms: i64) -> PortResult<Option<StoredCallback>> {
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        let callback = transaction
            .query_row(
                "SELECT nonce, space_id, generation, action, expires_at_ms FROM rust_callbacks WHERE nonce=?1 AND consumed_at_ms IS NULL AND expires_at_ms>=?2",
                params![nonce, now_ms],
                |row| Ok(StoredCallback { nonce: row.get(0)?, space_id: row.get(1)?, generation: row.get(2)?, action: row.get(3)?, expires_at_ms: row.get(4)? }),
            )
            .optional()
            .map_err(sql_error)?;
        let Some(callback) = callback else {
            transaction.commit().map_err(sql_error)?;
            return Ok(None);
        };
        let consumed = transaction
            .execute(
                "UPDATE rust_callbacks SET consumed_at_ms=?1 WHERE nonce=?2 AND consumed_at_ms IS NULL",
                params![now_ms, nonce],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)?;
        Ok((consumed == 1).then_some(callback))
    }

    fn migrate(&self) -> PortResult<()> {
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        let version: i64 = transaction
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .map_err(sql_error)?;
        if version > SCHEMA_VERSION {
            return Err(PortError::Adapter(format!(
                "database schema version {version} is newer than supported {SCHEMA_VERSION}"
            )));
        }
        if version < 1 {
            transaction
                .execute_batch(
                    "
                    CREATE TABLE sessions (
                        id TEXT PRIMARY KEY NOT NULL,
                        title TEXT NOT NULL,
                        status TEXT NOT NULL,
                        created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL
                    ) STRICT;
                    CREATE TABLE approvals (
                        id TEXT PRIMARY KEY NOT NULL,
                        session_id TEXT NOT NULL REFERENCES sessions(id),
                        action_json TEXT NOT NULL,
                        decision TEXT NOT NULL,
                        requested_at_ms INTEGER NOT NULL,
                        decided_at_ms INTEGER
                    ) STRICT;
                    CREATE TABLE artifacts (
                        id TEXT PRIMARY KEY NOT NULL,
                        session_id TEXT NOT NULL REFERENCES sessions(id),
                        path TEXT NOT NULL,
                        sha256 TEXT NOT NULL,
                        bytes INTEGER NOT NULL,
                        created_at_ms INTEGER NOT NULL
                    ) STRICT;
                    CREATE INDEX artifacts_by_session ON artifacts(session_id, created_at_ms);
                    CREATE TABLE domain_events (
                        id TEXT PRIMARY KEY NOT NULL,
                        event_type TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        occurred_at_ms INTEGER NOT NULL
                    ) STRICT;
                    CREATE INDEX domain_events_by_occurred_at ON domain_events(occurred_at_ms, id);
                    ",
                )
                .map_err(sql_error)?;
        }
        if version < 2 {
            transaction
                .execute_batch(
                    "
                    CREATE TABLE rust_session_spaces (
                        space_id TEXT PRIMARY KEY NOT NULL,
                        thread_id TEXT,
                        lifecycle TEXT NOT NULL,
                        generation INTEGER NOT NULL,
                        channel_chat_id INTEGER NOT NULL,
                        channel_post_id INTEGER NOT NULL,
                        discussion_chat_id INTEGER,
                        discussion_root_message_id INTEGER,
                        status_message_id INTEGER,
                        status_bot_instance TEXT,
                        owner_chat_id INTEGER,
                        plan_mode INTEGER NOT NULL CHECK(plan_mode IN (0, 1)),
                        created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL,
                        UNIQUE(channel_chat_id, channel_post_id)
                    ) STRICT;
                    CREATE INDEX rust_session_spaces_by_thread
                        ON rust_session_spaces(thread_id, updated_at_ms DESC);
                    CREATE INDEX rust_session_spaces_by_discussion_root
                        ON rust_session_spaces(discussion_chat_id, discussion_root_message_id);
                    CREATE TABLE rust_native_comment_roots (
                        channel_chat_id INTEGER NOT NULL,
                        channel_post_id INTEGER NOT NULL,
                        discussion_chat_id INTEGER NOT NULL,
                        root_message_id INTEGER NOT NULL,
                        created_at_ms INTEGER NOT NULL,
                        PRIMARY KEY(channel_chat_id, channel_post_id),
                        UNIQUE(discussion_chat_id, root_message_id)
                    ) STRICT;
                    CREATE TABLE rust_processed_updates (
                        bot_instance_id TEXT NOT NULL,
                        update_id INTEGER NOT NULL,
                        received_at_ms INTEGER NOT NULL,
                        PRIMARY KEY(bot_instance_id, update_id)
                    ) STRICT;
                    CREATE TABLE rust_poll_offsets (
                        bot_instance_id TEXT PRIMARY KEY NOT NULL,
                        next_update_id INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL
                    ) STRICT;
                    CREATE TABLE rust_callbacks (
                        nonce TEXT PRIMARY KEY NOT NULL,
                        space_id TEXT NOT NULL REFERENCES rust_session_spaces(space_id),
                        generation INTEGER NOT NULL,
                        action TEXT NOT NULL,
                        expires_at_ms INTEGER NOT NULL,
                        consumed_at_ms INTEGER
                    ) STRICT;
                    CREATE INDEX rust_callbacks_pending
                        ON rust_callbacks(space_id, generation, expires_at_ms)
                        WHERE consumed_at_ms IS NULL;
                    ",
                )
                .map_err(sql_error)?;
        }
        transaction
            .pragma_update(None, "user_version", SCHEMA_VERSION)
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)
    }

    fn append_event(transaction: &Transaction<'_>, event: &DomainEvent) -> PortResult<()> {
        let payload = serde_json::to_string(&event.kind)
            .map_err(|error| PortError::Adapter(format!("serialize event: {error}")))?;
        transaction
            .execute(
                "INSERT INTO domain_events (id, event_type, payload_json, occurred_at_ms) VALUES (?1, ?2, ?3, ?4)",
                params![event.id.as_str(), event.kind.event_type(), payload, event.occurred_at_ms],
            )
            .map_err(sql_error)?;
        Ok(())
    }
}

impl SessionRepository for SqliteStore {
    fn insert_session(&self, session: &Session, event: &DomainEvent) -> PortResult<()> {
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        transaction
            .execute(
                "INSERT INTO sessions (id, title, status, created_at_ms, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![session.id.as_str(), session.title, session.status.as_str(), session.created_at_ms, session.updated_at_ms],
            )
            .map_err(sql_error)?;
        Self::append_event(&transaction, event)?;
        transaction.commit().map_err(sql_error)
    }

    fn get_session(&self, id: &SessionId) -> PortResult<Option<Session>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT id, title, status, created_at_ms, updated_at_ms FROM sessions WHERE id = ?1",
                params![id.as_str()],
                |row| {
                    Ok(Session {
                        id: SessionId::new(row.get::<_, String>(0)?).map_err(to_from_sql_error)?,
                        title: row.get(1)?,
                        status: parse_status(&row.get::<_, String>(2)?).map_err(to_from_sql_error)?,
                        created_at_ms: row.get(3)?,
                        updated_at_ms: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(sql_error)
    }
}

impl ApprovalStore for SqliteStore {
    fn insert_approval(&self, approval: &ApprovalRequest, event: &DomainEvent) -> PortResult<()> {
        let action = serde_json::to_string(&approval.action)
            .map_err(|error| PortError::Adapter(format!("serialize approval action: {error}")))?;
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        transaction
            .execute(
                "INSERT INTO approvals (id, session_id, action_json, decision, requested_at_ms, decided_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![approval.id.as_str(), approval.session_id.as_str(), action, approval.decision.as_str(), approval.requested_at_ms, approval.decided_at_ms],
            )
            .map_err(sql_error)?;
        Self::append_event(&transaction, event)?;
        transaction.commit().map_err(sql_error)
    }

    fn get_approval(&self, id: &ApprovalId) -> PortResult<Option<ApprovalRequest>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT id, session_id, action_json, decision, requested_at_ms, decided_at_ms FROM approvals WHERE id = ?1",
                params![id.as_str()],
                |row| {
                    let action_json: String = row.get(2)?;
                    let action = serde_json::from_str(&action_json).map_err(to_from_sql_error)?;
                    Ok(ApprovalRequest {
                        id: ApprovalId::new(row.get::<_, String>(0)?).map_err(to_from_sql_error)?,
                        session_id: SessionId::new(row.get::<_, String>(1)?).map_err(to_from_sql_error)?,
                        action,
                        decision: parse_decision(&row.get::<_, String>(3)?).map_err(to_from_sql_error)?,
                        requested_at_ms: row.get(4)?,
                        decided_at_ms: row.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(sql_error)
    }

    fn decide_approval(&self, approval: &ApprovalRequest, event: &DomainEvent) -> PortResult<()> {
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        let changed = transaction
            .execute(
                "UPDATE approvals SET decision = ?1, decided_at_ms = ?2 WHERE id = ?3 AND decision = 'pending'",
                params![approval.decision.as_str(), approval.decided_at_ms, approval.id.as_str()],
            )
            .map_err(sql_error)?;
        if changed != 1 {
            return Err(PortError::Conflict(format!(
                "approval {} is no longer pending",
                approval.id
            )));
        }
        Self::append_event(&transaction, event)?;
        transaction.commit().map_err(sql_error)
    }
}

impl ArtifactStore for SqliteStore {
    fn insert_artifact(&self, artifact: &Artifact, event: &DomainEvent) -> PortResult<()> {
        let bytes = i64::try_from(artifact.bytes)
            .map_err(|_| PortError::Adapter("artifact byte count exceeds SQLite INTEGER".into()))?;
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        transaction
            .execute(
                "INSERT INTO artifacts (id, session_id, path, sha256, bytes, created_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![artifact.id.as_str(), artifact.session_id.as_str(), artifact.path, artifact.sha256, bytes, artifact.created_at_ms],
            )
            .map_err(sql_error)?;
        Self::append_event(&transaction, event)?;
        transaction.commit().map_err(sql_error)
    }

    fn list_artifacts(&self, session_id: &SessionId) -> PortResult<Vec<Artifact>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let mut statement = connection
            .prepare(
                "SELECT id, session_id, path, sha256, bytes, created_at_ms FROM artifacts WHERE session_id = ?1 ORDER BY created_at_ms, id",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![session_id.as_str()], |row| {
                Ok(Artifact {
                    id: ArtifactId::new(row.get::<_, String>(0)?).map_err(to_from_sql_error)?,
                    session_id: SessionId::new(row.get::<_, String>(1)?)
                        .map_err(to_from_sql_error)?,
                    path: row.get(2)?,
                    sha256: row.get(3)?,
                    bytes: u64::try_from(row.get::<_, i64>(4)?).map_err(to_from_sql_error)?,
                    created_at_ms: row.get(5)?,
                })
            })
            .map_err(sql_error)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(sql_error)
    }
}

impl EventLog for SqliteStore {
    fn append(&self, event: &DomainEvent) -> PortResult<()> {
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        Self::append_event(&transaction, event)?;
        transaction.commit().map_err(sql_error)
    }
}

fn parse_status(value: &str) -> Result<SessionStatus, String> {
    match value {
        "active" => Ok(SessionStatus::Active),
        "paused" => Ok(SessionStatus::Paused),
        "closed" => Ok(SessionStatus::Closed),
        _ => Err(format!("unknown session status: {value}")),
    }
}

fn parse_decision(value: &str) -> Result<ApprovalDecision, String> {
    match value {
        "pending" => Ok(ApprovalDecision::Pending),
        "approved" => Ok(ApprovalDecision::Approved),
        "rejected" => Ok(ApprovalDecision::Rejected),
        "expired" => Ok(ApprovalDecision::Expired),
        _ => Err(format!("unknown approval decision: {value}")),
    }
}

fn validate_space(space: &RustSessionSpace) -> PortResult<()> {
    if space.space_id.trim().is_empty() || space.lifecycle.trim().is_empty() {
        return Err(PortError::Adapter(
            "space id and lifecycle cannot be empty".into(),
        ));
    }
    if space.generation < 0 || space.channel_post_id <= 0 {
        return Err(PortError::Adapter(
            "invalid session-space generation or post id".into(),
        ));
    }
    if let Some(root) = space.discussion_root_message_id
        && root <= 0
    {
        return Err(PortError::Adapter("invalid discussion root id".into()));
    }
    Ok(())
}

fn row_to_space(row: &rusqlite::Row<'_>) -> rusqlite::Result<RustSessionSpace> {
    Ok(RustSessionSpace {
        space_id: row.get(0)?,
        thread_id: row.get(1)?,
        lifecycle: row.get(2)?,
        generation: row.get(3)?,
        channel_chat_id: row.get(4)?,
        channel_post_id: row.get(5)?,
        discussion_chat_id: row.get(6)?,
        discussion_root_message_id: row.get(7)?,
        status_message_id: row.get(8)?,
        status_bot_instance: row.get(9)?,
        owner_chat_id: row.get(10)?,
        plan_mode: row.get::<_, i64>(11)? != 0,
        created_at_ms: row.get(12)?,
        updated_at_ms: row.get(13)?,
    })
}

fn to_from_sql_error(error: impl std::fmt::Display) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            error.to_string(),
        )),
    )
}

fn lock_error<T>(_: std::sync::PoisonError<T>) -> PortError {
    PortError::Adapter("SQLite connection lock was poisoned".into())
}

fn sql_error(error: rusqlite::Error) -> PortError {
    match error {
        rusqlite::Error::SqliteFailure(_, Some(message))
            if message.contains("UNIQUE constraint failed") =>
        {
            PortError::Conflict(message)
        }
        other => PortError::Adapter(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ctg_domain::{ApprovalAction, DomainEventKind, EventId};
    use std::fs;

    fn event(id: &str, now: i64, kind: DomainEventKind) -> DomainEvent {
        DomainEvent {
            id: EventId::new(id).unwrap(),
            occurred_at_ms: now,
            kind,
        }
    }

    fn session() -> Session {
        Session::new(SessionId::new("session-1").unwrap(), "Build", 10).unwrap()
    }

    fn space() -> RustSessionSpace {
        RustSessionSpace {
            space_id: "space-1".into(),
            thread_id: Some("thread-1".into()),
            lifecycle: "active".into(),
            generation: 0,
            channel_chat_id: -1004446000549,
            channel_post_id: 81,
            discussion_chat_id: None,
            discussion_root_message_id: None,
            status_message_id: None,
            status_bot_instance: Some("status".into()),
            owner_chat_id: Some(42),
            plan_mode: false,
            created_at_ms: 10,
            updated_at_ms: 10,
        }
    }

    #[test]
    fn persists_domain_writes_and_events_together() {
        let store = SqliteStore::in_memory().unwrap();
        let session = session();
        store
            .insert_session(
                &session,
                &event(
                    "event-1",
                    10,
                    DomainEventKind::SessionCreated {
                        session: session.clone(),
                    },
                ),
            )
            .unwrap();
        let approval = ApprovalRequest::pending(
            ApprovalId::new("approval-1").unwrap(),
            session.id.clone(),
            ApprovalAction::SendPrompt {
                prompt: "ship it".into(),
            },
            11,
        );
        store
            .insert_approval(
                &approval,
                &event(
                    "event-2",
                    11,
                    DomainEventKind::ApprovalRequested {
                        approval: approval.clone(),
                    },
                ),
            )
            .unwrap();

        let fetched = store.get_approval(&approval.id).unwrap().unwrap();
        assert_eq!(fetched, approval);
    }

    #[test]
    fn opens_file_database_in_wal_mode() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-tmp")
            .join(format!("ctg-storage-{}.sqlite", std::process::id()));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let _ = fs::remove_file(&path);
        let store = SqliteStore::open(&path).unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        let journal_mode: String = store
            .connection
            .lock()
            .unwrap()
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .unwrap();
        assert_eq!(journal_mode, "wal");
        drop(store);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn rust_state_is_independent_and_binds_native_comment_roots() {
        let store = SqliteStore::in_memory().unwrap();
        store.upsert_session_space(&space()).unwrap();
        let root = NativeCommentRoot {
            channel_chat_id: -1004446000549,
            channel_post_id: 81,
            discussion_chat_id: -1004290500369,
            root_message_id: 700,
        };
        store.bind_native_comment_root(&root, 20).unwrap();
        assert_eq!(
            store
                .native_comment_root(root.channel_chat_id, root.channel_post_id)
                .unwrap(),
            Some(root)
        );
        let bound = store
            .session_space_for_discussion_root(-1004290500369, 700)
            .unwrap()
            .unwrap();
        assert_eq!(bound.space_id, "space-1");
        assert_eq!(bound.discussion_root_message_id, Some(700));
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
    }

    #[test]
    fn update_deduplication_advances_a_per_bot_offset_once() {
        let store = SqliteStore::in_memory().unwrap();
        assert!(store.record_processed_update("control", 99, 10).unwrap());
        assert!(!store.record_processed_update("control", 99, 11).unwrap());
        assert!(store.record_processed_update("discussion", 4, 12).unwrap());
        assert_eq!(store.next_update_offset("control").unwrap(), Some(100));
        assert_eq!(store.next_update_offset("discussion").unwrap(), Some(5));
    }

    #[test]
    fn callbacks_are_consumed_exactly_once() {
        let store = SqliteStore::in_memory().unwrap();
        store.upsert_session_space(&space()).unwrap();
        let callback = StoredCallback {
            nonce: "nonce-1".into(),
            space_id: "space-1".into(),
            generation: 0,
            action: "status".into(),
            expires_at_ms: 20,
        };
        store.create_callback(&callback).unwrap();
        assert_eq!(store.take_callback("nonce-1", 15).unwrap(), Some(callback));
        assert_eq!(store.take_callback("nonce-1", 16).unwrap(), None);
    }
}
