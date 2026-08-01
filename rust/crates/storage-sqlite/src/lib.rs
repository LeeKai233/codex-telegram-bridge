//! SQLite adapter with WAL mode and transactional business-record/event writes.

use ctg_domain::{
    ApprovalDecision, ApprovalId, ApprovalRequest, Artifact, ArtifactId, DomainEvent, Session,
    SessionId, SessionStatus,
};
use ctg_ports::{ApprovalStore, ArtifactStore, EventLog, PortError, PortResult, SessionRepository};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use std::path::Path;
use std::sync::Mutex;

const SCHEMA_VERSION: i64 = 1;

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
        if version == 0 {
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
            transaction
                .pragma_update(None, "user_version", SCHEMA_VERSION)
                .map_err(sql_error)?;
        }
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
        let path = std::env::temp_dir().join(format!("ctg-storage-{}.sqlite", std::process::id()));
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
}
