//! SQLite adapter with WAL mode and transactional business-record/event writes.

use ctg_domain::{
    ApprovalDecision, ApprovalId, ApprovalRequest, Artifact, ArtifactId, DomainEvent,
    PlanPublication, PromptIntent, QuestionRequest, Session, SessionId, SessionStatus,
};
use ctg_ports::{ApprovalStore, ArtifactStore, EventLog, PortError, PortResult, SessionRepository};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// This is deliberately independent from the Python bridge schema. Rust
/// deployments receive a new database path and never migrate Python state.
const SCHEMA_VERSION: i64 = 10;
const DELETION_CLAIM_LEASE_MS: i64 = 60_000;
/// Failed deletions back off exponentially (2^attempts seconds, capped) and
/// are abandoned after this many attempts so one stuck message cannot block
/// the queue behind it forever.
const DELETION_MAX_ATTEMPTS: i64 = 8;
const DELETION_BACKOFF_CAP_MS: i64 = 300_000;
const CONTROL_CLAIM_LEASE_MS: i64 = 60_000;

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
    pub observed_mode: Option<String>,
    pub normal_model: Option<String>,
    pub normal_effort: Option<String>,
    pub plan_model: Option<String>,
    pub plan_effort: Option<String>,
    pub closed_at_ms: Option<i64>,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TotpState {
    pub last_timecode: i64,
    pub unlocked_until_ms: i64,
    pub force_locked: bool,
    pub auth_epoch: i64,
    pub failures: i64,
    pub locked_until_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ControlInteraction {
    pub scope_key: String,
    pub kind: String,
    pub revision: i64,
    pub phase: String,
    pub payload: Value,
    pub user_id: i64,
    pub chat_id: i64,
    pub message_id: Option<i64>,
    pub expires_at_ms: i64,
    pub claimed_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ControlCallback {
    pub nonce: String,
    pub scope_key: Option<String>,
    pub revision: Option<i64>,
    pub user_id: i64,
    pub chat_id: i64,
    pub action: String,
    pub payload: Value,
    pub expires_at_ms: i64,
    pub consumed_at_ms: Option<i64>,
    pub invalidated_at_ms: Option<i64>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ScheduledDeletion {
    pub bot_instance_id: String,
    pub chat_id: i64,
    pub message_id: i64,
    pub group_key: String,
    pub delete_at_ms: i64,
    pub attempts: i64,
    pub claimed_at_ms: Option<i64>,
    pub last_error_class: Option<String>,
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
            .pragma_update(None, "synchronous", "FULL")
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

    pub fn upsert_prompt_intent(&self, intent: &PromptIntent) -> PortResult<()> {
        let payload =
            serde_json::to_string(intent).map_err(|error| PortError::Adapter(error.to_string()))?;
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .execute(
                "INSERT INTO rust_prompt_intents(intent_id, client_message_id, state, payload_json, created_at_ms, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(intent_id) DO UPDATE SET state=excluded.state, payload_json=excluded.payload_json, updated_at_ms=excluded.updated_at_ms",
                params![intent.intent_id, intent.client_message_id, format!("{:?}", intent.state).to_ascii_lowercase(), payload, intent.created_at_ms, intent.updated_at_ms],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    pub fn prompt_intent_by_client_message_id(
        &self,
        client_message_id: &str,
    ) -> PortResult<Option<PromptIntent>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let payload = connection
            .query_row(
                "SELECT payload_json FROM rust_prompt_intents WHERE client_message_id=?1",
                params![client_message_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?;
        payload
            .map(|value| {
                serde_json::from_str(&value).map_err(|error| PortError::Adapter(error.to_string()))
            })
            .transpose()
    }

    pub fn prompt_intents(&self) -> PortResult<Vec<PromptIntent>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let mut statement = connection
            .prepare(
                "SELECT payload_json FROM rust_prompt_intents ORDER BY updated_at_ms, intent_id",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sql_error)?;
        let mut intents = Vec::new();
        for row in rows {
            let payload = row.map_err(sql_error)?;
            let intent = serde_json::from_str(&payload)
                .map_err(|error| PortError::Adapter(error.to_string()))?;
            intents.push(intent);
        }
        Ok(intents)
    }

    pub fn upsert_question(&self, request: &QuestionRequest) -> PortResult<()> {
        let payload = serde_json::to_string(request)
            .map_err(|error| PortError::Adapter(error.to_string()))?;
        let generation = i64::try_from(request.generation)
            .map_err(|_| PortError::Adapter("question generation exceeds SQLite range".into()))?;
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .execute(
                "INSERT INTO rust_pending_questions(request_key, generation, status, payload_json, response_json, created_at_ms, updated_at_ms) VALUES (?1, ?2, 'pending', ?3, NULL, ?4, ?5) ON CONFLICT(request_key) DO UPDATE SET generation=excluded.generation, payload_json=excluded.payload_json, updated_at_ms=excluded.updated_at_ms",
                params![request.request_key, generation, payload, now_ms(), now_ms()],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    pub fn resolve_question(
        &self,
        request_key: &str,
        response: &serde_json::Value,
        resolved_at_ms: i64,
    ) -> PortResult<()> {
        let payload = serde_json::to_string(response)
            .map_err(|error| PortError::Adapter(error.to_string()))?;
        let connection = self.connection.lock().map_err(lock_error)?;
        let changed = connection
            .execute(
                "UPDATE rust_pending_questions SET status='resolved', response_json=?2, updated_at_ms=?3 WHERE request_key=?1 AND status='pending'",
                params![request_key, payload, resolved_at_ms],
            )
            .map_err(sql_error)?;
        if changed == 0 {
            return Err(PortError::Conflict(
                "question is missing or already resolved".into(),
            ));
        }
        Ok(())
    }

    pub fn upsert_plan_publication(&self, publication: &PlanPublication) -> PortResult<()> {
        let payload = serde_json::to_string(publication)
            .map_err(|error| PortError::Adapter(error.to_string()))?;
        let generation = i64::try_from(publication.generation)
            .map_err(|_| PortError::Adapter("plan generation exceeds SQLite range".into()))?;
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .execute(
                "INSERT INTO rust_plan_publications(space_id, generation, item_id, revision_key, status, payload_json, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(space_id, generation, item_id, revision_key) DO UPDATE SET status=excluded.status, payload_json=excluded.payload_json, updated_at_ms=excluded.updated_at_ms",
                params![publication.space_id, generation, publication.item_id, publication.revision_key, format!("{:?}", publication.status).to_ascii_lowercase(), payload, publication.updated_at_ms],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    /// Stores controller-owned state without coupling the SQLite adapter to
    /// Telegram presentation types.  The v4 legacy-record table is also the
    /// durable handoff boundary used by the Python importer, so workflow
    /// records remain recoverable across a daemon restart without a schema
    /// fork.
    pub fn upsert_workflow_record(
        &self,
        kind: &str,
        key: &str,
        payload: &serde_json::Value,
        updated_at_ms: i64,
    ) -> PortResult<()> {
        if kind.trim().is_empty() || key.trim().is_empty() {
            return Err(PortError::Adapter(
                "workflow record key cannot be empty".into(),
            ));
        }
        let row_json = serde_json::to_string(payload)
            .map_err(|error| PortError::Adapter(format!("serialize workflow record: {error}")))?;
        let mut digest = sha2::Sha256::new();
        use sha2::Digest;
        digest.update(row_json.as_bytes());
        let row_sha256 = format!("{:x}", digest.finalize());
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .execute(
                "INSERT INTO rust_legacy_records(table_name, row_key, source_schema_version, row_json, row_sha256, imported_at_ms) VALUES (?1, ?2, -1, ?3, ?4, ?5) ON CONFLICT(table_name, row_key) DO UPDATE SET row_json=excluded.row_json, row_sha256=excluded.row_sha256, imported_at_ms=excluded.imported_at_ms",
                params![format!("rust_workflow:{kind}"), key, row_json, row_sha256, updated_at_ms],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    pub fn workflow_record(&self, kind: &str, key: &str) -> PortResult<Option<serde_json::Value>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let row_json = connection
            .query_row(
                "SELECT row_json FROM rust_legacy_records WHERE table_name=?1 AND row_key=?2",
                params![format!("rust_workflow:{kind}"), key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?;
        row_json
            .map(|value| {
                serde_json::from_str(&value).map_err(|error| PortError::Adapter(error.to_string()))
            })
            .transpose()
    }

    pub fn workflow_records(&self, kind: &str) -> PortResult<Vec<(String, serde_json::Value)>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let mut statement = connection
            .prepare(
                "SELECT row_key, row_json FROM rust_legacy_records WHERE table_name=?1 ORDER BY imported_at_ms, row_key",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![format!("rust_workflow:{kind}")], |row| {
                let key: String = row.get(0)?;
                let payload: String = row.get(1)?;
                Ok((key, payload))
            })
            .map_err(sql_error)?;
        let mut records = Vec::new();
        for row in rows {
            let (key, payload) = row.map_err(sql_error)?;
            let payload = serde_json::from_str(&payload)
                .map_err(|error| PortError::Adapter(error.to_string()))?;
            records.push((key, payload));
        }
        Ok(records)
    }

    pub fn delete_workflow_record(&self, kind: &str, key: &str) -> PortResult<bool> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let changed = connection
            .execute(
                "DELETE FROM rust_legacy_records WHERE table_name=?1 AND row_key=?2",
                params![format!("rust_workflow:{kind}"), key],
            )
            .map_err(sql_error)?;
        Ok(changed == 1)
    }

    /// Replaces one control interaction and invalidates callbacks issued by
    /// its previous revision in the same transaction.  This is the durable
    /// race boundary for `/new`: a timeout, text message, and callback can all
    /// observe the same revision, but only one conditional claim succeeds.
    #[allow(clippy::too_many_arguments)]
    pub fn replace_control_interaction(
        &self,
        scope_key: &str,
        kind: &str,
        phase: &str,
        payload: &Value,
        user_id: i64,
        chat_id: i64,
        message_id: Option<i64>,
        expires_at_ms: i64,
        now_ms: i64,
    ) -> PortResult<ControlInteraction> {
        if scope_key.trim().is_empty() || kind.trim().is_empty() || phase.trim().is_empty() {
            return Err(PortError::Adapter(
                "control interaction keys cannot be empty".into(),
            ));
        }
        let payload_json = serde_json::to_string(payload).map_err(|error| {
            PortError::Adapter(format!("serialize control interaction: {error}"))
        })?;
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        transaction
            .execute(
                "UPDATE rust_control_callbacks SET invalidated_at_ms=?1 WHERE scope_key=?2 AND consumed_at_ms IS NULL AND invalidated_at_ms IS NULL",
                params![now_ms, scope_key],
            )
            .map_err(sql_error)?;
        let existing = transaction
            .query_row(
                "SELECT revision, created_at_ms FROM rust_control_interactions WHERE scope_key=?1",
                params![scope_key],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        let revision = existing.map_or(1, |(revision, _)| revision.saturating_add(1));
        let created_at_ms = existing.map_or(now_ms, |(_, created_at_ms)| created_at_ms);
        transaction
            .execute(
                "INSERT INTO rust_control_interactions(scope_key, kind, revision, phase, payload_json, user_id, chat_id, message_id, expires_at_ms, claimed_at_ms, created_at_ms, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10, ?10) ON CONFLICT(scope_key) DO UPDATE SET kind=excluded.kind, revision=excluded.revision, phase=excluded.phase, payload_json=excluded.payload_json, user_id=excluded.user_id, chat_id=excluded.chat_id, message_id=excluded.message_id, expires_at_ms=excluded.expires_at_ms, claimed_at_ms=NULL, updated_at_ms=excluded.updated_at_ms",
                params![
                    scope_key,
                    kind,
                    revision,
                    phase,
                    payload_json,
                    user_id,
                    chat_id,
                    message_id,
                    expires_at_ms,
                    created_at_ms,
                ],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)?;
        Ok(ControlInteraction {
            scope_key: scope_key.to_owned(),
            kind: kind.to_owned(),
            revision,
            phase: phase.to_owned(),
            payload: payload.clone(),
            user_id,
            chat_id,
            message_id,
            expires_at_ms,
            claimed_at_ms: None,
            created_at_ms,
            updated_at_ms: now_ms,
        })
    }

    /// Advances an interaction only when the caller still owns the revision it
    /// read.  Callback rows from that revision are invalidated in the same
    /// transaction, so two concurrent `/new` choices cannot overwrite one
    /// another.
    #[allow(clippy::too_many_arguments)]
    pub fn advance_control_interaction(
        &self,
        scope_key: &str,
        expected_revision: i64,
        phase: &str,
        payload: &Value,
        user_id: i64,
        chat_id: i64,
        message_id: Option<i64>,
        expires_at_ms: i64,
        now_ms: i64,
    ) -> PortResult<Option<ControlInteraction>> {
        if scope_key.trim().is_empty() || phase.trim().is_empty() {
            return Err(PortError::Adapter(
                "control interaction keys cannot be empty".into(),
            ));
        }
        let payload_json = serde_json::to_string(payload).map_err(|error| {
            PortError::Adapter(format!("serialize control interaction: {error}"))
        })?;
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        let changed = transaction
            .execute(
                "UPDATE rust_control_interactions SET revision=revision+1, phase=?1, payload_json=?2, user_id=?3, chat_id=?4, message_id=?5, expires_at_ms=?6, claimed_at_ms=NULL, updated_at_ms=?7 WHERE scope_key=?8 AND revision=?9 AND user_id=?3 AND chat_id=?4 AND claimed_at_ms IS NULL AND expires_at_ms>?7",
                params![
                    phase,
                    payload_json,
                    user_id,
                    chat_id,
                    message_id,
                    expires_at_ms,
                    now_ms,
                    scope_key,
                    expected_revision,
                ],
            )
            .map_err(sql_error)?;
        if changed == 0 {
            transaction.commit().map_err(sql_error)?;
            return Ok(None);
        }
        transaction
            .execute(
                "UPDATE rust_control_callbacks SET invalidated_at_ms=?1 WHERE scope_key=?2 AND revision=?3 AND consumed_at_ms IS NULL AND invalidated_at_ms IS NULL",
                params![now_ms, scope_key, expected_revision],
            )
            .map_err(sql_error)?;
        let interaction = transaction
            .query_row(
                "SELECT scope_key, kind, revision, phase, payload_json, user_id, chat_id, message_id, expires_at_ms, claimed_at_ms, created_at_ms, updated_at_ms FROM rust_control_interactions WHERE scope_key=?1",
                params![scope_key],
                control_interaction_from_row,
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)?;
        Ok(Some(interaction))
    }

    pub fn control_interaction(&self, scope_key: &str) -> PortResult<Option<ControlInteraction>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT scope_key, kind, revision, phase, payload_json, user_id, chat_id, message_id, expires_at_ms, claimed_at_ms, created_at_ms, updated_at_ms FROM rust_control_interactions WHERE scope_key=?1",
                params![scope_key],
                control_interaction_from_row,
            )
            .optional()
            .map_err(sql_error)
    }

    pub fn control_interactions(&self, kind: &str) -> PortResult<Vec<ControlInteraction>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let mut statement = connection
            .prepare(
                "SELECT scope_key, kind, revision, phase, payload_json, user_id, chat_id, message_id, expires_at_ms, claimed_at_ms, created_at_ms, updated_at_ms FROM rust_control_interactions WHERE kind=?1 ORDER BY updated_at_ms, scope_key",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![kind], control_interaction_from_row)
            .map_err(sql_error)?;
        rows.map(|row| row.map_err(sql_error)).collect()
    }

    pub fn claim_control_interaction(
        &self,
        scope_key: &str,
        user_id: i64,
        chat_id: i64,
        revision: i64,
        now_ms: i64,
    ) -> PortResult<Option<ControlInteraction>> {
        self.claim_control_interaction_with_expiry(
            scope_key, user_id, chat_id, revision, now_ms, true,
        )
    }

    pub fn claim_expired_control_interaction(
        &self,
        scope_key: &str,
        user_id: i64,
        chat_id: i64,
        revision: i64,
        now_ms: i64,
    ) -> PortResult<Option<ControlInteraction>> {
        self.claim_control_interaction_with_expiry(
            scope_key, user_id, chat_id, revision, now_ms, false,
        )
    }

    fn claim_control_interaction_with_expiry(
        &self,
        scope_key: &str,
        user_id: i64,
        chat_id: i64,
        revision: i64,
        now_ms: i64,
        live: bool,
    ) -> PortResult<Option<ControlInteraction>> {
        let expiry_operator = if live { ">" } else { "<=" };
        let stale_before_ms = now_ms.saturating_sub(CONTROL_CLAIM_LEASE_MS);
        let connection = self.connection.lock().map_err(lock_error)?;
        let changed = connection
            .execute(&format!(
                "UPDATE rust_control_interactions SET claimed_at_ms=?1, updated_at_ms=?1 WHERE scope_key=?2 AND user_id=?3 AND chat_id=?4 AND revision=?5 AND (claimed_at_ms IS NULL OR claimed_at_ms<=?6) AND expires_at_ms{expiry_operator}?1"
            ), params![now_ms, scope_key, user_id, chat_id, revision, stale_before_ms])
            .map_err(sql_error)?;
        if changed == 0 {
            return Ok(None);
        }
        connection
            .query_row(
                "SELECT scope_key, kind, revision, phase, payload_json, user_id, chat_id, message_id, expires_at_ms, claimed_at_ms, created_at_ms, updated_at_ms FROM rust_control_interactions WHERE scope_key=?1",
                params![scope_key],
                control_interaction_from_row,
            )
            .optional()
            .map_err(sql_error)
    }

    pub fn delete_control_interaction(&self, scope_key: &str, now_ms: i64) -> PortResult<bool> {
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        transaction
            .execute(
                "UPDATE rust_control_callbacks SET invalidated_at_ms=?1 WHERE scope_key=?2 AND consumed_at_ms IS NULL AND invalidated_at_ms IS NULL",
                params![now_ms, scope_key],
            )
            .map_err(sql_error)?;
        let changed = transaction
            .execute(
                "DELETE FROM rust_control_interactions WHERE scope_key=?1",
                params![scope_key],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)?;
        Ok(changed == 1)
    }

    pub fn release_control_interaction_claim(
        &self,
        scope_key: &str,
        revision: i64,
        claim_started_ms: i64,
        now_ms: i64,
    ) -> PortResult<bool> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let changed = connection
            .execute(
                "UPDATE rust_control_interactions SET claimed_at_ms=NULL, updated_at_ms=?1 WHERE scope_key=?2 AND revision=?3 AND claimed_at_ms=?4",
                params![now_ms, scope_key, revision, claim_started_ms],
            )
            .map_err(sql_error)?;
        Ok(changed == 1)
    }

    /// Restores a callback that was claimed before an external side effect
    /// failed.  The callback remains scoped and expires normally, allowing a
    /// later Telegram click to retry without opening a second action.
    pub fn restore_callback(&self, nonce: &str) -> PortResult<bool> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let changed = connection
            .execute(
                "UPDATE rust_callbacks SET consumed_at_ms=NULL WHERE nonce=?1 AND consumed_at_ms IS NOT NULL",
                params![nonce],
            )
            .map_err(sql_error)?;
        Ok(changed == 1)
    }

    pub fn upsert_control_callback(&self, callback: &ControlCallback) -> PortResult<()> {
        if callback.nonce.trim().is_empty()
            || callback.action.trim().is_empty()
            || callback
                .scope_key
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Err(PortError::Adapter(
                "control callback fields cannot be empty".into(),
            ));
        }
        let payload_json = serde_json::to_string(&callback.payload)
            .map_err(|error| PortError::Adapter(format!("serialize control callback: {error}")))?;
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .execute(
                "INSERT INTO rust_control_callbacks(nonce, scope_key, revision, user_id, chat_id, action, payload_json, expires_at_ms, consumed_at_ms, invalidated_at_ms, created_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11) ON CONFLICT(nonce) DO UPDATE SET scope_key=excluded.scope_key, revision=excluded.revision, user_id=excluded.user_id, chat_id=excluded.chat_id, action=excluded.action, payload_json=excluded.payload_json, expires_at_ms=excluded.expires_at_ms, consumed_at_ms=excluded.consumed_at_ms, invalidated_at_ms=excluded.invalidated_at_ms",
                params![
                    callback.nonce,
                    callback.scope_key,
                    callback.revision,
                    callback.user_id,
                    callback.chat_id,
                    callback.action,
                    payload_json,
                    callback.expires_at_ms,
                    callback.consumed_at_ms,
                    callback.invalidated_at_ms,
                    callback.created_at_ms,
                ],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    pub fn consume_control_callback(
        &self,
        nonce: &str,
        user_id: i64,
        chat_id: i64,
        now_ms: i64,
    ) -> PortResult<Option<ControlCallback>> {
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        let callback = transaction
            .query_row(
                "SELECT nonce, scope_key, revision, user_id, chat_id, action, payload_json, expires_at_ms, consumed_at_ms, invalidated_at_ms, created_at_ms FROM rust_control_callbacks WHERE nonce=?1 AND user_id=?2 AND chat_id=?3 AND expires_at_ms>=?4 AND consumed_at_ms IS NULL AND invalidated_at_ms IS NULL",
                params![nonce, user_id, chat_id, now_ms],
                control_callback_from_row,
            )
            .optional()
            .map_err(sql_error)?;
        let Some(callback) = callback else {
            transaction.commit().map_err(sql_error)?;
            return Ok(None);
        };
        if let (Some(scope_key), Some(revision)) =
            (callback.scope_key.as_deref(), callback.revision)
        {
            let current_revision = transaction
                .query_row(
                    "SELECT revision FROM rust_control_interactions WHERE scope_key=?1",
                    params![scope_key],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .map_err(sql_error)?;
            if current_revision != Some(revision) {
                transaction.commit().map_err(sql_error)?;
                return Ok(None);
            }
        }
        let changed = transaction
            .execute(
                "UPDATE rust_control_callbacks SET consumed_at_ms=?1 WHERE nonce=?2 AND consumed_at_ms IS NULL AND invalidated_at_ms IS NULL",
                params![now_ms, nonce],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)?;
        Ok((changed == 1).then_some(ControlCallback {
            consumed_at_ms: Some(now_ms),
            ..callback
        }))
    }

    pub fn schedule_deletion(&self, deletion: &ScheduledDeletion) -> PortResult<()> {
        if deletion.bot_instance_id.trim().is_empty()
            || deletion.group_key.trim().is_empty()
            || deletion.message_id <= 0
        {
            return Err(PortError::Adapter(
                "scheduled deletion fields are invalid".into(),
            ));
        }
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .execute(
                "INSERT INTO rust_scheduled_deletions(bot_instance_id, chat_id, message_id, group_key, delete_at_ms, attempts, claimed_at_ms, last_error_class) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) ON CONFLICT(bot_instance_id, chat_id, message_id) DO UPDATE SET group_key=excluded.group_key, delete_at_ms=excluded.delete_at_ms, attempts=excluded.attempts, claimed_at_ms=excluded.claimed_at_ms, last_error_class=excluded.last_error_class, next_attempt_at_ms=0, abandoned_at_ms=NULL",
                params![deletion.bot_instance_id, deletion.chat_id, deletion.message_id, deletion.group_key, deletion.delete_at_ms, deletion.attempts, deletion.claimed_at_ms, deletion.last_error_class],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    pub fn claim_due_deletions(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> PortResult<Vec<ScheduledDeletion>> {
        let limit = i64::try_from(limit)
            .map_err(|_| PortError::Adapter("deletion limit exceeds SQLite range".into()))?;
        let stale_before_ms = now_ms.saturating_sub(DELETION_CLAIM_LEASE_MS);
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        let mut statement = transaction
            .prepare(
                "SELECT bot_instance_id, chat_id, message_id, group_key, delete_at_ms, attempts, claimed_at_ms, last_error_class FROM rust_scheduled_deletions WHERE delete_at_ms<=?1 AND abandoned_at_ms IS NULL AND next_attempt_at_ms<=?1 AND (claimed_at_ms IS NULL OR claimed_at_ms<=?2) ORDER BY delete_at_ms, bot_instance_id, chat_id, message_id LIMIT ?3",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(
                params![now_ms, stale_before_ms, limit],
                scheduled_deletion_from_row,
            )
            .map_err(sql_error)?;
        let mut due = Vec::new();
        for row in rows {
            due.push(row.map_err(sql_error)?);
        }
        drop(statement);
        let mut claimed = Vec::with_capacity(due.len());
        for deletion in due {
            let changed = transaction
                .execute(
                    "UPDATE rust_scheduled_deletions SET claimed_at_ms=?1 WHERE bot_instance_id=?2 AND chat_id=?3 AND message_id=?4 AND (claimed_at_ms IS NULL OR claimed_at_ms<=?5)",
                    params![now_ms, deletion.bot_instance_id, deletion.chat_id, deletion.message_id, stale_before_ms],
                )
                .map_err(sql_error)?;
            if changed == 1 {
                claimed.push(ScheduledDeletion {
                    claimed_at_ms: Some(now_ms),
                    ..deletion
                });
            }
        }
        transaction.commit().map_err(sql_error)?;
        Ok(claimed)
    }

    pub fn complete_deletion(
        &self,
        bot_instance_id: &str,
        chat_id: i64,
        message_id: i64,
    ) -> PortResult<bool> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let changed = connection
            .execute(
                "DELETE FROM rust_scheduled_deletions WHERE bot_instance_id=?1 AND chat_id=?2 AND message_id=?3",
                params![bot_instance_id, chat_id, message_id],
            )
            .map_err(sql_error)?;
        Ok(changed == 1)
    }

    pub fn retry_deletion(
        &self,
        bot_instance_id: &str,
        chat_id: i64,
        message_id: i64,
        error_class: &str,
        now_ms: i64,
    ) -> PortResult<bool> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let changed = connection
            .execute(
                "UPDATE rust_scheduled_deletions SET attempts=attempts+1, claimed_at_ms=NULL, last_error_class=?4, next_attempt_at_ms=?1 + MIN(?6, (1 << MIN(attempts + 1, 20)) * 1000), abandoned_at_ms=CASE WHEN attempts + 1 >= ?7 THEN ?1 ELSE abandoned_at_ms END WHERE bot_instance_id=?2 AND chat_id=?3 AND message_id=?5",
                params![now_ms, bot_instance_id, chat_id, error_class, message_id, DELETION_BACKOFF_CAP_MS, DELETION_MAX_ATTEMPTS],
            )
            .map_err(sql_error)?;
        Ok(changed == 1)
    }

    /// Returns paths retained by durable artifact records so maintenance can
    /// avoid deleting files that are still addressable from a Session.
    pub fn artifact_paths(&self) -> PortResult<Vec<PathBuf>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let mut statement = connection
            .prepare("SELECT path FROM artifacts ORDER BY created_at_ms, id")
            .map_err(sql_error)?;
        let rows = statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(sql_error)?;
        rows.map(|row| row.map(PathBuf::from).map_err(sql_error))
            .collect()
    }

    pub fn update_control_interaction_message(
        &self,
        scope_key: &str,
        revision: i64,
        message_id: i64,
        now_ms: i64,
    ) -> PortResult<bool> {
        if message_id <= 0 {
            return Err(PortError::Adapter(
                "control interaction message id must be positive".into(),
            ));
        }
        let connection = self.connection.lock().map_err(lock_error)?;
        let changed = connection
            .execute(
                "UPDATE rust_control_interactions SET message_id=?1, updated_at_ms=?2 WHERE scope_key=?3 AND revision=?4",
                params![message_id, now_ms, scope_key, revision],
            )
            .map_err(sql_error)?;
        Ok(changed == 1)
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

    pub fn processed_update_exists(
        &self,
        bot_instance_id: &str,
        update_id: i64,
    ) -> PortResult<bool> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM rust_processed_updates WHERE bot_instance_id=?1 AND update_id=?2)",
                params![bot_instance_id, update_id],
                |row| row.get(0),
            )
            .map_err(sql_error)
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
                "INSERT INTO rust_session_spaces(space_id, thread_id, lifecycle, generation, channel_chat_id, channel_post_id, discussion_chat_id, discussion_root_message_id, status_message_id, status_bot_instance, owner_chat_id, plan_mode, observed_mode, normal_model, normal_effort, plan_model, plan_effort, closed_at_ms, created_at_ms, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20) ON CONFLICT(space_id) DO UPDATE SET thread_id=excluded.thread_id, lifecycle=excluded.lifecycle, generation=excluded.generation, channel_chat_id=excluded.channel_chat_id, channel_post_id=excluded.channel_post_id, discussion_chat_id=COALESCE(excluded.discussion_chat_id, rust_session_spaces.discussion_chat_id), discussion_root_message_id=COALESCE(excluded.discussion_root_message_id, rust_session_spaces.discussion_root_message_id), status_message_id=excluded.status_message_id, status_bot_instance=excluded.status_bot_instance, owner_chat_id=excluded.owner_chat_id, plan_mode=excluded.plan_mode, observed_mode=excluded.observed_mode, normal_model=excluded.normal_model, normal_effort=excluded.normal_effort, plan_model=excluded.plan_model, plan_effort=excluded.plan_effort, closed_at_ms=excluded.closed_at_ms, updated_at_ms=excluded.updated_at_ms WHERE (rust_session_spaces.lifecycle != 'closed' AND excluded.generation >= rust_session_spaces.generation) OR excluded.lifecycle='closed'",
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
                    space.observed_mode,
                    space.normal_model,
                    space.normal_effort,
                    space.plan_model,
                    space.plan_effort,
                    space.closed_at_ms,
                    space.created_at_ms,
                    space.updated_at_ms,
                ],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    /// Atomically closes a SessionSpace and invalidates every callback and
    /// queued prompt belonging to its current generation.  The generation is
    /// incremented in the same transaction so a callback racing the close can
    /// never observe a closed row with the old generation.
    pub fn close_session_space(
        &self,
        space_id: &str,
        expected_generation: i64,
        closed_at_ms: i64,
    ) -> PortResult<Option<RustSessionSpace>> {
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        let current = transaction
            .query_row(
                "SELECT space_id, thread_id, lifecycle, generation, channel_chat_id, channel_post_id, discussion_chat_id, discussion_root_message_id, status_message_id, status_bot_instance, owner_chat_id, plan_mode, observed_mode, normal_model, normal_effort, plan_model, plan_effort, closed_at_ms, created_at_ms, updated_at_ms FROM rust_session_spaces WHERE space_id=?1 AND generation=?2",
                params![space_id, expected_generation],
                row_to_space,
            )
            .optional()
            .map_err(sql_error)?;
        let Some(current) = current else {
            transaction.commit().map_err(sql_error)?;
            return Ok(None);
        };
        if current.lifecycle == "closed" {
            transaction.commit().map_err(sql_error)?;
            return Ok(Some(current));
        }
        let mut closed = current.clone();
        closed.lifecycle = "closed".to_owned();
        closed.generation = expected_generation.saturating_add(1);
        closed.closed_at_ms = Some(closed_at_ms);
        closed.updated_at_ms = closed_at_ms;
        transaction
            .execute(
                "UPDATE rust_session_spaces SET lifecycle='closed', generation=?1, closed_at_ms=?2, updated_at_ms=?2 WHERE space_id=?3 AND generation=?4 AND lifecycle!='closed'",
                params![closed.generation, closed_at_ms, space_id, expected_generation],
            )
            .map_err(sql_error)?;
        transaction
            .execute(
                "UPDATE rust_callbacks SET consumed_at_ms=?1 WHERE space_id=?2 AND consumed_at_ms IS NULL",
                params![closed_at_ms, space_id],
            )
            .map_err(sql_error)?;

        let mut statement = transaction
            .prepare(
                "SELECT row_key, row_json FROM rust_legacy_records WHERE table_name='rust_workflow:queue'",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        drop(statement);
        for (row_key, row_json) in rows {
            let Ok(mut value) = serde_json::from_str::<serde_json::Value>(&row_json) else {
                continue;
            };
            let same_space = value.get("space_id").and_then(serde_json::Value::as_str)
                == Some(space_id)
                || (value.get("thread_id").and_then(serde_json::Value::as_str)
                    == current.thread_id.as_deref()
                    && value
                        .get("generation")
                        .and_then(serde_json::Value::as_i64)
                        .is_some_and(|generation| generation <= expected_generation));
            if !same_space
                || value.get("status").and_then(serde_json::Value::as_str) != Some("queued")
            {
                continue;
            }
            value["status"] = serde_json::Value::String("cancelled".to_owned());
            value["cancelled_at_ms"] = serde_json::Value::from(closed_at_ms);
            let updated_json = serde_json::to_string(&value).map_err(|error| {
                PortError::Adapter(format!("serialize cancelled queue: {error}"))
            })?;
            let mut digest = sha2::Sha256::new();
            use sha2::Digest;
            digest.update(updated_json.as_bytes());
            let row_sha256 = format!("{:x}", digest.finalize());
            transaction
                .execute(
                    "UPDATE rust_legacy_records SET row_json=?1, row_sha256=?2, imported_at_ms=?3 WHERE table_name='rust_workflow:queue' AND row_key=?4",
                    params![updated_json, row_sha256, closed_at_ms, row_key],
                )
                .map_err(sql_error)?;
        }
        transaction.commit().map_err(sql_error)?;
        Ok(Some(closed))
    }

    pub fn get_session_space(&self, space_id: &str) -> PortResult<Option<RustSessionSpace>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT space_id, thread_id, lifecycle, generation, channel_chat_id, channel_post_id, discussion_chat_id, discussion_root_message_id, status_message_id, status_bot_instance, owner_chat_id, plan_mode, observed_mode, normal_model, normal_effort, plan_model, plan_effort, closed_at_ms, created_at_ms, updated_at_ms FROM rust_session_spaces WHERE space_id=?1",
                params![space_id],
                row_to_space,
            )
            .optional()
            .map_err(sql_error)
    }

    pub fn session_space_for_channel_post(
        &self,
        channel_chat_id: i64,
        channel_post_id: i64,
    ) -> PortResult<Option<RustSessionSpace>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT space_id, thread_id, lifecycle, generation, channel_chat_id, channel_post_id, discussion_chat_id, discussion_root_message_id, status_message_id, status_bot_instance, owner_chat_id, plan_mode, observed_mode, normal_model, normal_effort, plan_model, plan_effort, closed_at_ms, created_at_ms, updated_at_ms FROM rust_session_spaces WHERE channel_chat_id=?1 AND channel_post_id=?2",
                params![channel_chat_id, channel_post_id],
                row_to_space,
            )
            .optional()
            .map_err(sql_error)
    }

    pub fn active_session_spaces(&self) -> PortResult<Vec<RustSessionSpace>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let mut statement = connection
            .prepare(
                "SELECT space_id, thread_id, lifecycle, generation, channel_chat_id, channel_post_id, discussion_chat_id, discussion_root_message_id, status_message_id, status_bot_instance, owner_chat_id, plan_mode, observed_mode, normal_model, normal_effort, plan_model, plan_effort, closed_at_ms, created_at_ms, updated_at_ms FROM rust_session_spaces WHERE lifecycle='active' ORDER BY updated_at_ms, space_id",
            )
            .map_err(sql_error)?;
        let mut rows = statement.query([]).map_err(sql_error)?;
        let mut spaces = Vec::new();
        while let Some(row) = rows.next().map_err(sql_error)? {
            spaces.push(row_to_space(row).map_err(sql_error)?);
        }
        Ok(spaces)
    }

    /// Returns every non-closed Telegram SessionSpace, including pending
    /// spaces waiting for discussion-thread TOTP activation.
    pub fn session_spaces(&self) -> PortResult<Vec<RustSessionSpace>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let mut statement = connection
            .prepare(
                "SELECT space_id, thread_id, lifecycle, generation, channel_chat_id, channel_post_id, discussion_chat_id, discussion_root_message_id, status_message_id, status_bot_instance, owner_chat_id, plan_mode, observed_mode, normal_model, normal_effort, plan_model, plan_effort, closed_at_ms, created_at_ms, updated_at_ms FROM rust_session_spaces WHERE lifecycle != 'closed' ORDER BY updated_at_ms, space_id",
            )
            .map_err(sql_error)?;
        let mut rows = statement.query([]).map_err(sql_error)?;
        let mut spaces = Vec::new();
        while let Some(row) = rows.next().map_err(sql_error)? {
            spaces.push(row_to_space(row).map_err(sql_error)?);
        }
        Ok(spaces)
    }

    /// Finds the newest pending space in a linked discussion chat.  The
    /// command router supplies the precise comment root when available; this
    /// fallback keeps pending activation recoverable after a process restart.
    pub fn pending_session_space_for_discussion(
        &self,
        discussion_chat_id: i64,
    ) -> PortResult<Option<RustSessionSpace>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT s.space_id, s.thread_id, s.lifecycle, s.generation, s.channel_chat_id, s.channel_post_id, COALESCE(s.discussion_chat_id, r.discussion_chat_id), COALESCE(s.discussion_root_message_id, r.root_message_id), s.status_message_id, s.status_bot_instance, s.owner_chat_id, s.plan_mode, s.observed_mode, s.normal_model, s.normal_effort, s.plan_model, s.plan_effort, s.closed_at_ms, s.created_at_ms, s.updated_at_ms FROM rust_session_spaces AS s LEFT JOIN rust_native_comment_roots AS r ON r.channel_chat_id=s.channel_chat_id AND r.channel_post_id=s.channel_post_id WHERE COALESCE(s.discussion_chat_id, r.discussion_chat_id)=?1 AND s.lifecycle IN ('pending', 'repair_required') ORDER BY s.updated_at_ms DESC, s.space_id DESC LIMIT 1",
                params![discussion_chat_id],
                row_to_space,
            )
            .optional()
            .map_err(sql_error)
    }

    pub fn session_space_for_thread(
        &self,
        thread_id: &str,
    ) -> PortResult<Option<RustSessionSpace>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT space_id, thread_id, lifecycle, generation, channel_chat_id, channel_post_id, discussion_chat_id, discussion_root_message_id, status_message_id, status_bot_instance, owner_chat_id, plan_mode, observed_mode, normal_model, normal_effort, plan_model, plan_effort, closed_at_ms, created_at_ms, updated_at_ms FROM rust_session_spaces WHERE thread_id=?1 ORDER BY updated_at_ms DESC LIMIT 1",
                params![thread_id],
                row_to_space,
            )
            .optional()
            .map_err(sql_error)
    }

    pub fn totp_state(&self) -> PortResult<TotpState> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT last_timecode, unlocked_until_ms, force_locked, auth_epoch, totp_failures, totp_locked_until_ms FROM rust_security_state WHERE id=1",
                [],
                |row| {
                    Ok(TotpState {
                        last_timecode: row.get(0)?,
                        unlocked_until_ms: row.get(1)?,
                        force_locked: row.get::<_, i64>(2)? != 0,
                        auth_epoch: row.get(3)?,
                        failures: row.get(4)?,
                        locked_until_ms: row.get(5)?,
                    })
                },
            )
            .map_err(sql_error)
    }

    pub fn is_totp_unlocked(&self, now_ms: i64) -> PortResult<bool> {
        let state = self.totp_state()?;
        Ok(!state.force_locked && state.unlocked_until_ms > now_ms)
    }

    /// Accepts a timecode once and atomically opens the write lease.
    pub fn accept_totp_timecode(
        &self,
        timecode: i64,
        now_ms: i64,
        unlock_seconds: i64,
    ) -> PortResult<bool> {
        if timecode < 0 || unlock_seconds <= 0 {
            return Err(PortError::Adapter("invalid TOTP state input".into()));
        }
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        let last: i64 = transaction
            .query_row(
                "SELECT last_timecode FROM rust_security_state WHERE id=1",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if timecode <= last {
            transaction.commit().map_err(sql_error)?;
            return Ok(false);
        }
        transaction
            .execute(
                "UPDATE rust_security_state SET last_timecode=?1, unlocked_until_ms=?2, force_locked=0, totp_failures=0, totp_locked_until_ms=0 WHERE id=1",
                params![timecode, now_ms.saturating_add(unlock_seconds.saturating_mul(1000))],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)?;
        Ok(true)
    }

    /// Accepts a timecode for a process-local Session lease.  The durable
    /// state only records single-use/revocation metadata; it deliberately does
    /// not open the legacy global lease.
    pub fn accept_totp_timecode_for_space(&self, timecode: i64) -> PortResult<bool> {
        if timecode < 0 {
            return Err(PortError::Adapter("invalid TOTP timecode".into()));
        }
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        let last: i64 = transaction
            .query_row(
                "SELECT last_timecode FROM rust_security_state WHERE id=1",
                [],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        if timecode <= last {
            transaction.commit().map_err(sql_error)?;
            return Ok(false);
        }
        transaction
            .execute(
                "UPDATE rust_security_state SET last_timecode=?1, unlocked_until_ms=0, force_locked=0, totp_failures=0, totp_locked_until_ms=0 WHERE id=1",
                params![timecode],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)?;
        Ok(true)
    }

    pub fn unlock_totp_after_recovery(&self, now_ms: i64, unlock_seconds: i64) -> PortResult<bool> {
        if unlock_seconds <= 0 {
            return Err(PortError::Adapter("invalid TOTP unlock duration".into()));
        }
        let connection = self.connection.lock().map_err(lock_error)?;
        let changed = connection
            .execute(
                "UPDATE rust_security_state SET unlocked_until_ms=?1, force_locked=0, totp_failures=0, totp_locked_until_ms=0 WHERE id=1",
                params![now_ms.saturating_add(unlock_seconds.saturating_mul(1000))],
            )
            .map_err(sql_error)?;
        Ok(changed == 1)
    }

    pub fn accept_recovery_for_space(&self) -> PortResult<bool> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let changed = connection
            .execute(
                "UPDATE rust_security_state SET force_locked=0, totp_failures=0, totp_locked_until_ms=0 WHERE id=1",
                [],
            )
            .map_err(sql_error)?;
        Ok(changed == 1)
    }

    pub fn lock_totp(&self) -> PortResult<()> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .execute(
                "UPDATE rust_security_state SET unlocked_until_ms=0, force_locked=1, auth_epoch=auth_epoch+1 WHERE id=1",
                [],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    /// Records an invalid TOTP/recovery attempt and applies a short lockout
    /// after repeated failures. The caller checks the returned count only for
    /// diagnostics; authorization remains false until a later valid attempt.
    pub fn record_totp_failure(&self, now_ms: i64) -> PortResult<i64> {
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        let (failures, locked_until_ms): (i64, i64) = transaction
            .query_row(
                "SELECT totp_failures, totp_locked_until_ms FROM rust_security_state WHERE id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(sql_error)?;
        if locked_until_ms > now_ms {
            transaction.commit().map_err(sql_error)?;
            return Ok(failures);
        }
        let failures = failures.saturating_add(1);
        let locked_until_ms = if failures >= 5 {
            now_ms.saturating_add(300_000)
        } else {
            0
        };
        transaction
            .execute(
                "UPDATE rust_security_state SET totp_failures=?1, totp_locked_until_ms=?2 WHERE id=1",
                params![failures, locked_until_ms],
            )
            .map_err(sql_error)?;
        transaction.commit().map_err(sql_error)?;
        Ok(failures)
    }

    pub fn replace_recovery_codes(&self, entries: &[(String, String)]) -> PortResult<()> {
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        transaction
            .execute("DELETE FROM rust_totp_recovery_codes", [])
            .map_err(sql_error)?;
        for (digest, salt) in entries {
            transaction
                .execute(
                    "INSERT INTO rust_totp_recovery_codes(digest, salt, consumed_at_ms) VALUES (?1, ?2, NULL)",
                    params![digest, salt],
                )
                .map_err(sql_error)?;
        }
        transaction.commit().map_err(sql_error)
    }

    pub fn unused_recovery_codes(&self) -> PortResult<Vec<(String, String)>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let mut statement = connection
            .prepare(
                "SELECT digest, salt FROM rust_totp_recovery_codes WHERE consumed_at_ms IS NULL",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .map_err(sql_error)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(sql_error)
    }

    pub fn consume_recovery_code(&self, digest: &str, now_ms: i64) -> PortResult<bool> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let changed = connection
            .execute(
                "UPDATE rust_totp_recovery_codes SET consumed_at_ms=?1 WHERE digest=?2 AND consumed_at_ms IS NULL",
                params![now_ms, digest],
            )
            .map_err(sql_error)?;
        Ok(changed == 1)
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
                "SELECT s.space_id, s.thread_id, s.lifecycle, s.generation, s.channel_chat_id, s.channel_post_id, COALESCE(s.discussion_chat_id, r.discussion_chat_id), COALESCE(s.discussion_root_message_id, r.root_message_id), s.status_message_id, s.status_bot_instance, s.owner_chat_id, s.plan_mode, s.observed_mode, s.normal_model, s.normal_effort, s.plan_model, s.plan_effort, s.closed_at_ms, s.created_at_ms, s.updated_at_ms FROM rust_session_spaces AS s LEFT JOIN rust_native_comment_roots AS r ON r.channel_chat_id=s.channel_chat_id AND r.channel_post_id=s.channel_post_id WHERE COALESCE(s.discussion_chat_id, r.discussion_chat_id)=?1 AND COALESCE(s.discussion_root_message_id, r.root_message_id)=?2",
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
                "INSERT INTO rust_callbacks(nonce, space_id, generation, action, expires_at_ms, surface) VALUES (?1, ?2, ?3, ?4, ?5, 'workflow')",
                params![callback.nonce, callback.space_id, callback.generation, callback.action, callback.expires_at_ms],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    /// Creates a callback owned by the 818 status surface.  The separate
    /// surface column makes retirement durable and independent of action JSON
    /// shape, while old v5 callbacks remain discoverable by the compatibility
    /// predicate below.
    pub fn create_status_callback(&self, callback: &StoredCallback) -> PortResult<()> {
        if callback.nonce.trim().is_empty() || callback.action.trim().is_empty() {
            return Err(PortError::Adapter(
                "callback nonce and action cannot be empty".into(),
            ));
        }
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .execute(
                "INSERT INTO rust_callbacks(nonce, space_id, generation, action, expires_at_ms, surface) VALUES (?1, ?2, ?3, ?4, ?5, 'status')",
                params![callback.nonce, callback.space_id, callback.generation, callback.action, callback.expires_at_ms],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    /// Retire only the status-surface callbacks for one SessionSpace before a
    /// fresh keyboard is rendered. Plan, question, and approval callbacks use
    /// different JSON payloads and remain valid until their own scope expires.
    pub fn retire_status_callbacks(&self, space_id: &str, generation: i64) -> PortResult<usize> {
        let retired_at = self.retire_status_callbacks_at(space_id, generation)?;
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT COUNT(*) FROM rust_callbacks WHERE space_id=?1 AND generation=?2 AND consumed_at_ms=?3",
                params![space_id, generation, retired_at],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| usize::try_from(count).unwrap_or(usize::MAX))
            .map_err(sql_error)
    }

    pub fn retire_status_callbacks_at(&self, space_id: &str, generation: i64) -> PortResult<i64> {
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        let mut statement = transaction
            .prepare(
                "SELECT nonce, action, surface FROM rust_callbacks WHERE space_id=?1 AND generation=?2 AND consumed_at_ms IS NULL",
            )
            .map_err(sql_error)?;
        let callbacks = statement
            .query_map(params![space_id, generation], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(sql_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql_error)?;
        drop(statement);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64;
        for (nonce, action, surface) in callbacks {
            let legacy_status = serde_json::from_str::<serde_json::Value>(&action)
                .ok()
                .and_then(|value| {
                    value
                        .get("action")
                        .and_then(serde_json::Value::as_str)
                        .map(|value| {
                            matches!(
                                value,
                                "space_refresh"
                                    | "space_unwatch"
                                    | "status_unwatch_execute"
                                    | "status_unwatch_cancel"
                            )
                        })
                })
                .unwrap_or(false);
            let is_status = surface == "status" || legacy_status;
            if is_status {
                transaction
                    .execute(
                        "UPDATE rust_callbacks SET consumed_at_ms=?1 WHERE nonce=?2 AND consumed_at_ms IS NULL",
                        params![now, nonce],
                    )
                    .map_err(sql_error)?;
            }
        }
        transaction.commit().map_err(sql_error)?;
        Ok(now)
    }

    pub fn restore_status_callbacks(
        &self,
        space_id: &str,
        generation: i64,
        retired_at_ms: i64,
    ) -> PortResult<usize> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let restored = connection
            .execute(
                "UPDATE rust_callbacks SET consumed_at_ms=NULL WHERE space_id=?1 AND generation=?2 AND surface='status' AND consumed_at_ms=?3",
                params![space_id, generation, retired_at_ms],
            )
            .map_err(sql_error)?;
        Ok(restored)
    }

    pub fn peek_callback(&self, nonce: &str, now_ms: i64) -> PortResult<Option<StoredCallback>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT nonce, space_id, generation, action, expires_at_ms FROM rust_callbacks WHERE nonce=?1 AND consumed_at_ms IS NULL AND expires_at_ms>=?2",
                params![nonce, now_ms],
                |row| Ok(StoredCallback { nonce: row.get(0)?, space_id: row.get(1)?, generation: row.get(2)?, action: row.get(3)?, expires_at_ms: row.get(4)? }),
            )
            .optional()
            .map_err(sql_error)
    }

    /// Returns the durable owner surface for a live callback without
    /// consuming it.  The daemon uses this as a second authorization gate so
    /// a workflow callback can never be replayed through the 818 status Bot.
    pub fn callback_surface(&self, nonce: &str, now_ms: i64) -> PortResult<Option<String>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT surface FROM rust_callbacks WHERE nonce=?1 AND consumed_at_ms IS NULL AND expires_at_ms>=?2",
                params![nonce, now_ms],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)
    }

    /// Atomically consumes a callback once. A stale generation or expired nonce
    /// is treated as absent so it can never affect a rebuilt comment thread.
    pub fn take_callback(&self, nonce: &str, now_ms: i64) -> PortResult<Option<StoredCallback>> {
        self.take_callback_scoped(nonce, now_ms, None, None)
    }

    /// Consumes a callback only when its persisted space and generation still
    /// match the caller.  The scope is part of the SQL predicate so a callback
    /// arriving from another chat cannot be burned before validation.
    pub fn take_callback_scoped(
        &self,
        nonce: &str,
        now_ms: i64,
        expected_space_id: Option<&str>,
        expected_generation: Option<i64>,
    ) -> PortResult<Option<StoredCallback>> {
        let mut connection = self.connection.lock().map_err(lock_error)?;
        let transaction = connection.transaction().map_err(sql_error)?;
        let mut query = String::from(
            "SELECT nonce, space_id, generation, action, expires_at_ms FROM rust_callbacks WHERE nonce=?1 AND consumed_at_ms IS NULL AND expires_at_ms>=?2",
        );
        if expected_space_id.is_some() {
            query.push_str(" AND space_id=?3");
        }
        if expected_generation.is_some() {
            query.push_str(if expected_space_id.is_some() {
                " AND generation=?4"
            } else {
                " AND generation=?3"
            });
        }
        let mut statement = transaction.prepare(&query).map_err(sql_error)?;
        let mut values: Vec<Box<dyn rusqlite::ToSql>> =
            vec![Box::new(nonce.to_owned()), Box::new(now_ms)];
        if let Some(space_id) = expected_space_id {
            values.push(Box::new(space_id.to_owned()));
        }
        if let Some(generation) = expected_generation {
            values.push(Box::new(generation));
        }
        let callback = statement
            .query_row(
                rusqlite::params_from_iter(values.iter().map(|value| value.as_ref())),
                |row| {
                    Ok(StoredCallback {
                        nonce: row.get(0)?,
                        space_id: row.get(1)?,
                        generation: row.get(2)?,
                        action: row.get(3)?,
                        expires_at_ms: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(sql_error)?;
        drop(statement);
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

    pub fn upsert_thread_projection(
        &self,
        thread_id: &str,
        generation: i64,
        projection: &serde_json::Value,
        updated_at_ms: i64,
    ) -> PortResult<()> {
        if thread_id.trim().is_empty() {
            return Err(PortError::Adapter(
                "thread projection id cannot be empty".into(),
            ));
        }
        let payload = serde_json::to_string(projection)
            .map_err(|error| PortError::Adapter(format!("serialize thread projection: {error}")))?;
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .execute(
                "INSERT INTO rust_thread_projections(thread_id, generation, projection_json, updated_at_ms) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(thread_id) DO UPDATE SET generation=excluded.generation, projection_json=excluded.projection_json, updated_at_ms=excluded.updated_at_ms",
                params![thread_id, generation, payload, updated_at_ms],
            )
            .map_err(sql_error)?;
        Ok(())
    }

    /// Single-thread variant of `thread_projections` used by the event loop
    /// to lazily reload a projection that was evicted from memory.
    pub fn thread_projection(
        &self,
        thread_id: &str,
    ) -> PortResult<Option<(String, i64, serde_json::Value, i64)>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT thread_id, generation, projection_json, updated_at_ms FROM rust_thread_projections WHERE thread_id=?1",
                params![thread_id],
                |row| {
                    let payload: String = row.get(2)?;
                    let value = serde_json::from_str(&payload).map_err(to_from_sql_error)?;
                    Ok((row.get(0)?, row.get(1)?, value, row.get(3)?))
                },
            )
            .optional()
            .map_err(sql_error)
    }

    pub fn thread_projections(&self) -> PortResult<Vec<(String, i64, serde_json::Value, i64)>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        let mut statement = connection
            .prepare(
                "SELECT thread_id, generation, projection_json, updated_at_ms FROM rust_thread_projections ORDER BY updated_at_ms, thread_id",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map([], |row| {
                let payload: String = row.get(2)?;
                let value = serde_json::from_str(&payload).map_err(to_from_sql_error)?;
                Ok((row.get(0)?, row.get(1)?, value, row.get(3)?))
            })
            .map_err(sql_error)?;
        rows.map(|row| row.map_err(sql_error)).collect()
    }

    pub fn telegram_fingerprint(
        &self,
        bot_instance_id: &str,
        chat_id: i64,
        message_id: i64,
        semantic_key: &str,
    ) -> PortResult<Option<String>> {
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .query_row(
                "SELECT fingerprint FROM rust_telegram_fingerprints WHERE bot_instance_id=?1 AND chat_id=?2 AND message_id=?3 AND semantic_key=?4",
                params![bot_instance_id, chat_id, message_id, semantic_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)
    }

    pub fn set_telegram_fingerprint(
        &self,
        bot_instance_id: &str,
        chat_id: i64,
        message_id: i64,
        semantic_key: &str,
        fingerprint: &str,
        updated_at_ms: i64,
    ) -> PortResult<()> {
        if bot_instance_id.trim().is_empty() || semantic_key.trim().is_empty() {
            return Err(PortError::Adapter(
                "telegram fingerprint key cannot be empty".into(),
            ));
        }
        let connection = self.connection.lock().map_err(lock_error)?;
        connection
            .execute(
                "INSERT INTO rust_telegram_fingerprints(bot_instance_id, chat_id, message_id, semantic_key, fingerprint, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6) ON CONFLICT(bot_instance_id, chat_id, message_id, semantic_key) DO UPDATE SET fingerprint=excluded.fingerprint, updated_at_ms=excluded.updated_at_ms",
                params![bot_instance_id, chat_id, message_id, semantic_key, fingerprint, updated_at_ms],
            )
            .map_err(sql_error)?;
        Ok(())
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
        if version < 3 {
            transaction
                .execute_batch(
                    "
                    CREATE TABLE rust_security_state (
                        id INTEGER PRIMARY KEY CHECK(id = 1),
                        last_timecode INTEGER NOT NULL,
                        unlocked_until_ms INTEGER NOT NULL,
                        force_locked INTEGER NOT NULL CHECK(force_locked IN (0, 1)),
                        auth_epoch INTEGER NOT NULL
                    ) STRICT;
                    INSERT INTO rust_security_state(id, last_timecode, unlocked_until_ms, force_locked, auth_epoch)
                        VALUES (1, -1, 0, 1, 0);
                    ",
                )
                .map_err(sql_error)?;
        }
        if version < 4 {
            transaction
                .execute_batch(
                    "
                    CREATE TABLE rust_legacy_records (
                        table_name TEXT NOT NULL,
                        row_key TEXT NOT NULL,
                        source_schema_version INTEGER NOT NULL,
                        row_json TEXT NOT NULL,
                        row_sha256 TEXT NOT NULL,
                        imported_at_ms INTEGER NOT NULL,
                        PRIMARY KEY(table_name, row_key)
                    ) STRICT;
                    CREATE INDEX rust_legacy_records_by_table
                        ON rust_legacy_records(table_name, row_key);
                    CREATE TABLE rust_import_metadata (
                        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                        source_schema_version INTEGER NOT NULL,
                        source_path_sha256 TEXT NOT NULL,
                        imported_at_ms INTEGER NOT NULL,
                        report_json TEXT NOT NULL
                    ) STRICT;
                    CREATE TABLE rust_prompt_intents (
                        intent_id TEXT PRIMARY KEY NOT NULL,
                        client_message_id TEXT NOT NULL UNIQUE,
                        state TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL
                    ) STRICT;
                    CREATE INDEX rust_prompt_intents_by_state
                        ON rust_prompt_intents(state, updated_at_ms);
                    CREATE TABLE rust_pending_questions (
                        request_key TEXT PRIMARY KEY NOT NULL,
                        generation INTEGER NOT NULL,
                        status TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        response_json TEXT,
                        created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL
                    ) STRICT;
                    CREATE TABLE rust_plan_publications (
                        space_id TEXT NOT NULL,
                        generation INTEGER NOT NULL,
                        item_id TEXT NOT NULL,
                        revision_key TEXT NOT NULL,
                        status TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        updated_at_ms INTEGER NOT NULL,
                        PRIMARY KEY(space_id, generation, item_id, revision_key)
                    ) STRICT;
                    ",
                )
                .map_err(sql_error)?;
        }
        if version < 5 {
            transaction
                .execute_batch(
                    "
                    CREATE TABLE rust_control_interactions (
                        scope_key TEXT PRIMARY KEY NOT NULL,
                        kind TEXT NOT NULL,
                        revision INTEGER NOT NULL,
                        phase TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        user_id INTEGER NOT NULL,
                        chat_id INTEGER NOT NULL,
                        message_id INTEGER,
                        expires_at_ms INTEGER NOT NULL,
                        claimed_at_ms INTEGER,
                        created_at_ms INTEGER NOT NULL,
                        updated_at_ms INTEGER NOT NULL
                    ) STRICT;
                    CREATE INDEX rust_control_interactions_by_expiry
                        ON rust_control_interactions(expires_at_ms, updated_at_ms);
                    CREATE TABLE rust_control_callbacks (
                        nonce TEXT PRIMARY KEY NOT NULL,
                        scope_key TEXT,
                        revision INTEGER,
                        user_id INTEGER NOT NULL,
                        chat_id INTEGER NOT NULL,
                        action TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        expires_at_ms INTEGER NOT NULL,
                        consumed_at_ms INTEGER,
                        invalidated_at_ms INTEGER,
                        created_at_ms INTEGER NOT NULL
                    ) STRICT;
                    CREATE INDEX rust_control_callbacks_pending
                        ON rust_control_callbacks(scope_key, revision, expires_at_ms)
                        WHERE consumed_at_ms IS NULL AND invalidated_at_ms IS NULL;
                    CREATE TABLE rust_scheduled_deletions (
                        bot_instance_id TEXT NOT NULL,
                        chat_id INTEGER NOT NULL,
                        message_id INTEGER NOT NULL,
                        group_key TEXT NOT NULL,
                        delete_at_ms INTEGER NOT NULL,
                        attempts INTEGER NOT NULL DEFAULT 0,
                        claimed_at_ms INTEGER,
                        last_error_class TEXT,
                        PRIMARY KEY(bot_instance_id, chat_id, message_id)
                    ) STRICT;
                    CREATE INDEX rust_scheduled_deletions_due
                        ON rust_scheduled_deletions(delete_at_ms, claimed_at_ms);
                    ",
                )
                .map_err(sql_error)?;
        }
        if version < 6 {
            transaction
                .execute_batch(
                    "
                    ALTER TABLE rust_session_spaces ADD COLUMN closed_at_ms INTEGER;
                    ALTER TABLE rust_callbacks ADD COLUMN surface TEXT NOT NULL DEFAULT 'workflow';
                    CREATE TABLE rust_thread_projections (
                        thread_id TEXT PRIMARY KEY NOT NULL,
                        generation INTEGER NOT NULL,
                        projection_json TEXT NOT NULL,
                        updated_at_ms INTEGER NOT NULL
                    ) STRICT;
                    CREATE INDEX rust_thread_projections_by_updated
                        ON rust_thread_projections(updated_at_ms, thread_id);
                    CREATE TABLE rust_telegram_fingerprints (
                        bot_instance_id TEXT NOT NULL,
                        chat_id INTEGER NOT NULL,
                        message_id INTEGER NOT NULL,
                        semantic_key TEXT NOT NULL,
                        fingerprint TEXT NOT NULL,
                        updated_at_ms INTEGER NOT NULL,
                        PRIMARY KEY(bot_instance_id, chat_id, message_id, semantic_key)
                    ) STRICT;
                    ",
                )
                .map_err(sql_error)?;
        }
        if version < 7 {
            transaction
                .execute_batch(
                    "
                    ALTER TABLE rust_security_state ADD COLUMN totp_failures INTEGER NOT NULL DEFAULT 0;
                    ALTER TABLE rust_security_state ADD COLUMN totp_locked_until_ms INTEGER NOT NULL DEFAULT 0;
                    CREATE TABLE rust_totp_recovery_codes (
                        digest TEXT PRIMARY KEY NOT NULL,
                        salt TEXT NOT NULL,
                        consumed_at_ms INTEGER
                    ) STRICT;
                    CREATE INDEX rust_totp_recovery_codes_pending
                        ON rust_totp_recovery_codes(consumed_at_ms);
                    ",
                )
                .map_err(sql_error)?;
        }
        if version < 8 {
            transaction
                .execute_batch(
                    "
                    ALTER TABLE rust_session_spaces ADD COLUMN observed_mode TEXT;
                    ALTER TABLE rust_session_spaces ADD COLUMN normal_model TEXT;
                    ALTER TABLE rust_session_spaces ADD COLUMN normal_effort TEXT;
                    ALTER TABLE rust_session_spaces ADD COLUMN plan_model TEXT;
                    ALTER TABLE rust_session_spaces ADD COLUMN plan_effort TEXT;
                    ",
                )
                .map_err(sql_error)?;
        }
        if version < 9 {
            transaction
                .execute_batch(
                    "
                    ALTER TABLE rust_scheduled_deletions ADD COLUMN next_attempt_at_ms INTEGER NOT NULL DEFAULT 0;
                    ALTER TABLE rust_scheduled_deletions ADD COLUMN abandoned_at_ms INTEGER;
                    ",
                )
                .map_err(sql_error)?;
        }
        if version < 10 {
            // Repair pass for deployments where an earlier experimental build
            // already stamped user_version 8/9 with a different column set:
            // add every expected column that is actually missing instead of
            // trusting the version marker alone.
            ensure_column(
                &transaction,
                "rust_session_spaces",
                "observed_mode",
                "ALTER TABLE rust_session_spaces ADD COLUMN observed_mode TEXT",
            )?;
            ensure_column(
                &transaction,
                "rust_session_spaces",
                "normal_model",
                "ALTER TABLE rust_session_spaces ADD COLUMN normal_model TEXT",
            )?;
            ensure_column(
                &transaction,
                "rust_session_spaces",
                "normal_effort",
                "ALTER TABLE rust_session_spaces ADD COLUMN normal_effort TEXT",
            )?;
            ensure_column(
                &transaction,
                "rust_session_spaces",
                "plan_model",
                "ALTER TABLE rust_session_spaces ADD COLUMN plan_model TEXT",
            )?;
            ensure_column(
                &transaction,
                "rust_session_spaces",
                "plan_effort",
                "ALTER TABLE rust_session_spaces ADD COLUMN plan_effort TEXT",
            )?;
            ensure_column(
                &transaction,
                "rust_scheduled_deletions",
                "next_attempt_at_ms",
                "ALTER TABLE rust_scheduled_deletions ADD COLUMN next_attempt_at_ms INTEGER NOT NULL DEFAULT 0",
            )?;
            ensure_column(
                &transaction,
                "rust_scheduled_deletions",
                "abandoned_at_ms",
                "ALTER TABLE rust_scheduled_deletions ADD COLUMN abandoned_at_ms INTEGER",
            )?;
        }
        transaction
            .execute(
                "UPDATE rust_session_spaces AS s SET discussion_chat_id=(SELECT r.discussion_chat_id FROM rust_native_comment_roots AS r WHERE r.channel_chat_id=s.channel_chat_id AND r.channel_post_id=s.channel_post_id), discussion_root_message_id=(SELECT r.root_message_id FROM rust_native_comment_roots AS r WHERE r.channel_chat_id=s.channel_chat_id AND r.channel_post_id=s.channel_post_id), updated_at_ms=MAX(s.updated_at_ms, (SELECT r.created_at_ms FROM rust_native_comment_roots AS r WHERE r.channel_chat_id=s.channel_chat_id AND r.channel_post_id=s.channel_post_id)) WHERE EXISTS (SELECT 1 FROM rust_native_comment_roots AS r WHERE r.channel_chat_id=s.channel_chat_id AND r.channel_post_id=s.channel_post_id)",
                [],
            )
            .map_err(sql_error)?;
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

fn control_interaction_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ControlInteraction> {
    let payload_json: String = row.get(4)?;
    let payload = serde_json::from_str(&payload_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(4, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(ControlInteraction {
        scope_key: row.get(0)?,
        kind: row.get(1)?,
        revision: row.get(2)?,
        phase: row.get(3)?,
        payload,
        user_id: row.get(5)?,
        chat_id: row.get(6)?,
        message_id: row.get(7)?,
        expires_at_ms: row.get(8)?,
        claimed_at_ms: row.get(9)?,
        created_at_ms: row.get(10)?,
        updated_at_ms: row.get(11)?,
    })
}

fn control_callback_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ControlCallback> {
    let payload_json: String = row.get(6)?;
    let payload = serde_json::from_str(&payload_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(ControlCallback {
        nonce: row.get(0)?,
        scope_key: row.get(1)?,
        revision: row.get(2)?,
        user_id: row.get(3)?,
        chat_id: row.get(4)?,
        action: row.get(5)?,
        payload,
        expires_at_ms: row.get(7)?,
        consumed_at_ms: row.get(8)?,
        invalidated_at_ms: row.get(9)?,
        created_at_ms: row.get(10)?,
    })
}

fn scheduled_deletion_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ScheduledDeletion> {
    Ok(ScheduledDeletion {
        bot_instance_id: row.get(0)?,
        chat_id: row.get(1)?,
        message_id: row.get(2)?,
        group_key: row.get(3)?,
        delete_at_ms: row.get(4)?,
        attempts: row.get(5)?,
        claimed_at_ms: row.get(6)?,
        last_error_class: row.get(7)?,
    })
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
        observed_mode: row.get(12)?,
        normal_model: row.get(13)?,
        normal_effort: row.get(14)?,
        plan_model: row.get(15)?,
        plan_effort: row.get(16)?,
        closed_at_ms: row.get(17)?,
        created_at_ms: row.get(18)?,
        updated_at_ms: row.get(19)?,
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

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

/// Runs `ddl` only when `table` does not already have `column`. SQLite has
/// no `ADD COLUMN IF NOT EXISTS`, so migration repair paths introspect
/// `PRAGMA table_info` first and stay idempotent across partially-applied
/// schema versions.
fn ensure_column(
    connection: &rusqlite::Connection,
    table: &str,
    column: &str,
    ddl: &str,
) -> PortResult<()> {
    let mut statement = connection
        .prepare(&format!("PRAGMA table_info({table})"))
        .map_err(sql_error)?;
    let exists = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(sql_error)?
        .filter_map(Result::ok)
        .any(|name| name == column);
    if !exists {
        connection.execute_batch(ddl).map_err(sql_error)?;
    }
    Ok(())
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
            observed_mode: None,
            normal_model: None,
            normal_effort: None,
            plan_model: None,
            plan_effort: None,
            closed_at_ms: None,
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
    fn lists_prompt_intents_in_updated_order() {
        let store = SqliteStore::in_memory().unwrap();
        let thread_id = ctg_domain::ThreadId::new("thread-intents").unwrap();
        for (suffix, updated_at_ms) in [("first", 2), ("second", 3)] {
            store
                .upsert_prompt_intent(&PromptIntent {
                    intent_id: format!("intent-{suffix}"),
                    client_message_id: format!("client-{suffix}"),
                    source: "telegram".into(),
                    prompt: suffix.into(),
                    mode: "default".into(),
                    thread_id: Some(thread_id.clone()),
                    space_id: Some("space-intents".into()),
                    generation: 1,
                    state: ctg_domain::PromptIntentState::Started,
                    turn_id: None,
                    queue_id: None,
                    error: None,
                    created_at_ms: 1,
                    updated_at_ms,
                })
                .unwrap();
        }

        let intents = store.prompt_intents().unwrap();
        assert_eq!(
            intents
                .iter()
                .map(|intent| intent.intent_id.as_str())
                .collect::<Vec<_>>(),
            vec!["intent-first", "intent-second"]
        );
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
    fn migrate_repairs_columns_missing_from_a_prestamped_version() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-tmp")
            .join(format!("ctg-storage-repair-{}.sqlite", std::process::id()));
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let _ = fs::remove_file(&path);
        {
            let store = SqliteStore::open(&path).unwrap();
            // Simulate a deployment whose earlier build stamped user_version
            // 8/9 without adding the columns the current schema expects.
            let connection = store.connection.lock().unwrap();
            connection
                .execute_batch(
                    "
                    ALTER TABLE rust_session_spaces DROP COLUMN observed_mode;
                    ALTER TABLE rust_scheduled_deletions DROP COLUMN abandoned_at_ms;
                    PRAGMA user_version = 9;
                    ",
                )
                .unwrap();
        }
        let store = SqliteStore::open(&path).unwrap();
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
        let connection = store.connection.lock().unwrap();
        let columns = |table: &str| {
            connection
                .prepare(&format!("PRAGMA table_info({table})"))
                .unwrap()
                .query_map([], |row| row.get::<_, String>(1))
                .unwrap()
                .filter_map(Result::ok)
                .collect::<Vec<_>>()
        };
        assert!(
            columns("rust_session_spaces")
                .iter()
                .any(|c| c == "observed_mode")
        );
        assert!(
            columns("rust_scheduled_deletions")
                .iter()
                .any(|c| c == "abandoned_at_ms")
        );
        drop(connection);
        drop(store);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn rust_state_is_independent_and_binds_native_comment_roots() {
        let store = SqliteStore::in_memory().unwrap();
        store.upsert_session_space(&space()).unwrap();
        assert_eq!(store.active_session_spaces().unwrap(), vec![space()]);
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
    fn session_space_profile_fields_roundtrip() {
        let store = SqliteStore::in_memory().unwrap();
        let mut profiled = space();
        profiled.observed_mode = Some("plan".into());
        profiled.normal_model = Some("gpt-5.6-terra".into());
        profiled.normal_effort = Some("low".into());
        profiled.plan_model = Some("gpt-5.6-sol".into());
        profiled.plan_effort = Some("high".into());
        store.upsert_session_space(&profiled).unwrap();
        assert_eq!(store.get_session_space("space-1").unwrap(), Some(profiled));
        assert_eq!(store.schema_version().unwrap(), SCHEMA_VERSION);
    }

    #[test]
    fn native_comment_binding_survives_stale_session_snapshot_and_recovery_lookup() {
        let store = SqliteStore::in_memory().unwrap();
        store.upsert_session_space(&space()).unwrap();
        let root = NativeCommentRoot {
            channel_chat_id: -1004446000549,
            channel_post_id: 81,
            discussion_chat_id: -1004290500369,
            root_message_id: 700,
        };
        store.bind_native_comment_root(&root, 20).unwrap();

        let mut stale = space();
        stale.updated_at_ms = 30;
        store.upsert_session_space(&stale).unwrap();

        let bound = store
            .session_space_for_discussion_root(-1004290500369, 700)
            .unwrap()
            .unwrap();
        assert_eq!(bound.discussion_chat_id, Some(-1004290500369));
        assert_eq!(bound.discussion_root_message_id, Some(700));
        assert_eq!(
            store
                .session_space_for_channel_post(-1004446000549, 81)
                .unwrap()
                .unwrap()
                .space_id,
            "space-1"
        );
    }

    #[test]
    fn pending_lookup_recovers_a_root_recorded_before_space_creation() {
        let store = SqliteStore::in_memory().unwrap();
        let root = NativeCommentRoot {
            channel_chat_id: -1004446000549,
            channel_post_id: 82,
            discussion_chat_id: -1004290500369,
            root_message_id: 701,
        };
        store.bind_native_comment_root(&root, 20).unwrap();
        let mut pending = space();
        pending.space_id = "pending-root-before-space".into();
        pending.thread_id = None;
        pending.lifecycle = "pending".into();
        pending.channel_post_id = 82;
        pending.updated_at_ms = 21;
        store.upsert_session_space(&pending).unwrap();

        let resolved = store
            .pending_session_space_for_discussion(-1004290500369)
            .unwrap()
            .unwrap();
        assert_eq!(resolved.space_id, "pending-root-before-space");
        assert_eq!(resolved.discussion_root_message_id, Some(701));
    }

    #[test]
    fn pending_spaces_are_listed_and_resolved_by_discussion_chat() {
        let store = SqliteStore::in_memory().unwrap();
        store.upsert_session_space(&space()).unwrap();
        let mut pending = space();
        pending.space_id = "pending-1".into();
        pending.thread_id = None;
        pending.lifecycle = "pending".into();
        pending.channel_post_id = 82;
        pending.discussion_chat_id = Some(-1004290500369);
        pending.discussion_root_message_id = Some(700);
        pending.updated_at_ms = 20;
        store.upsert_session_space(&pending).unwrap();

        let spaces = store.session_spaces().unwrap();
        assert_eq!(spaces.len(), 2);
        assert_eq!(
            store
                .pending_session_space_for_discussion(-1004290500369)
                .unwrap()
                .unwrap()
                .space_id,
            "pending-1"
        );
    }

    #[test]
    fn update_deduplication_advances_a_per_bot_offset_once() {
        let store = SqliteStore::in_memory().unwrap();
        assert!(store.record_processed_update("control", 99, 10).unwrap());
        assert!(store.processed_update_exists("control", 99).unwrap());
        assert!(!store.processed_update_exists("control", 100).unwrap());
        assert!(!store.record_processed_update("control", 99, 11).unwrap());
        assert!(store.record_processed_update("discussion", 4, 12).unwrap());
        assert_eq!(store.next_update_offset("control").unwrap(), Some(100));
        assert_eq!(store.next_update_offset("discussion").unwrap(), Some(5));
    }

    #[test]
    fn control_revisions_invalidate_old_callbacks_and_claim_once() {
        let store = SqliteStore::in_memory().unwrap();
        let first = store
            .replace_control_interaction(
                "control:42:7:new",
                "control_new",
                "normal_model",
                &serde_json::json!({"choices": ["gpt"]}),
                7,
                42,
                Some(100),
                5_000,
                1_000,
            )
            .unwrap();
        assert_eq!(first.revision, 1);
        store
            .upsert_control_callback(&ControlCallback {
                nonce: "old".into(),
                scope_key: Some(first.scope_key.clone()),
                revision: Some(first.revision),
                user_id: 7,
                chat_id: 42,
                action: "normal_model".into(),
                payload: serde_json::json!({"value": "gpt"}),
                expires_at_ms: 5_000,
                consumed_at_ms: None,
                invalidated_at_ms: None,
                created_at_ms: 1_000,
            })
            .unwrap();
        let second = store
            .replace_control_interaction(
                &first.scope_key,
                "control_new",
                "normal_effort",
                &serde_json::json!({"choices": ["high"]}),
                7,
                42,
                Some(101),
                6_000,
                1_100,
            )
            .unwrap();
        assert_eq!(second.revision, 2);
        assert_eq!(
            store.consume_control_callback("old", 7, 42, 1_101).unwrap(),
            None
        );
        assert!(
            store
                .claim_control_interaction(&first.scope_key, 7, 42, 1, 1_101)
                .unwrap()
                .is_none()
        );
        let claimed = store
            .claim_control_interaction(&first.scope_key, 7, 42, 2, 1_101)
            .unwrap()
            .unwrap();
        assert_eq!(claimed.claimed_at_ms, Some(1_101));
        assert!(
            store
                .claim_control_interaction(&first.scope_key, 7, 42, 2, 1_102)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn control_advance_is_compare_and_swap_and_expiry_claim_is_exclusive() {
        let store = SqliteStore::in_memory().unwrap();
        let first = store
            .replace_control_interaction(
                "control:42:7:new",
                "control_new",
                "normal_model",
                &serde_json::json!({"choices": ["gpt"]}),
                7,
                42,
                Some(100),
                5_000,
                1_000,
            )
            .unwrap();
        let advanced = store
            .advance_control_interaction(
                &first.scope_key,
                first.revision,
                "normal_effort",
                &serde_json::json!({"normal_model": "gpt"}),
                7,
                42,
                Some(101),
                6_000,
                1_100,
            )
            .unwrap()
            .unwrap();
        assert_eq!(advanced.revision, 2);
        assert!(
            store
                .advance_control_interaction(
                    &first.scope_key,
                    first.revision,
                    "stale",
                    &serde_json::json!({}),
                    7,
                    42,
                    Some(102),
                    7_000,
                    1_101,
                )
                .unwrap()
                .is_none()
        );

        let expired = store
            .replace_control_interaction(
                "control:42:7:expired",
                "control_new",
                "prompt",
                &serde_json::json!({}),
                7,
                42,
                None,
                2_000,
                1_000,
            )
            .unwrap();
        assert!(
            store
                .claim_control_interaction(&expired.scope_key, 7, 42, expired.revision, 2_001,)
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .claim_expired_control_interaction(
                    &expired.scope_key,
                    7,
                    42,
                    expired.revision,
                    2_001,
                )
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .claim_expired_control_interaction(
                    &expired.scope_key,
                    7,
                    42,
                    expired.revision,
                    2_002,
                )
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn scheduled_deletions_have_fixed_deadlines_and_restart_claims() {
        let store = SqliteStore::in_memory().unwrap();
        store
            .schedule_deletion(&ScheduledDeletion {
                bot_instance_id: "control".into(),
                chat_id: 42,
                message_id: 101,
                group_key: "sessions:1".into(),
                delete_at_ms: 1_500,
                attempts: 0,
                claimed_at_ms: None,
                last_error_class: None,
            })
            .unwrap();
        assert!(store.claim_due_deletions(1_499, 10).unwrap().is_empty());
        let due = store.claim_due_deletions(1_500, 10).unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].delete_at_ms, 1_500);
        assert!(store.claim_due_deletions(1_501, 10).unwrap().is_empty());
        assert_eq!(store.claim_due_deletions(61_501, 10).unwrap().len(), 1);
        assert!(
            store
                .retry_deletion("control", 42, 101, "timeout", 61_501)
                .unwrap()
        );
        // The first failure backs off for 2^1 seconds before a reclaim.
        assert!(store.claim_due_deletions(61_501, 10).unwrap().is_empty());
        assert_eq!(store.claim_due_deletions(63_501, 10).unwrap().len(), 1);
        assert!(store.complete_deletion("control", 42, 101).unwrap());
        assert!(store.claim_due_deletions(64_000, 10).unwrap().is_empty());
    }

    #[test]
    fn scheduled_deletion_backoff_abandons_after_max_attempts_and_reschedule_revives() {
        let store = SqliteStore::in_memory().unwrap();
        let deletion = ScheduledDeletion {
            bot_instance_id: "control".into(),
            chat_id: 42,
            message_id: 202,
            group_key: "perf:1".into(),
            delete_at_ms: 1_000,
            attempts: 0,
            claimed_at_ms: None,
            last_error_class: None,
        };
        store.schedule_deletion(&deletion).unwrap();
        let mut now = 1_000;
        for attempt in 1..=DELETION_MAX_ATTEMPTS {
            assert_eq!(
                store.claim_due_deletions(now, 10).unwrap().len(),
                1,
                "attempt {attempt} must still be claimable"
            );
            assert!(
                store
                    .retry_deletion("control", 42, 202, "timeout", now)
                    .unwrap()
            );
            now = now.saturating_add((1i64 << attempt.min(20)).min(300) * 1_000);
        }
        // An abandoned row is never claimed again, even long after its backoff.
        assert!(
            store
                .claim_due_deletions(now.saturating_add(10_000_000), 10)
                .unwrap()
                .is_empty()
        );
        // Re-scheduling the same message resets attempts and revives it.
        store
            .schedule_deletion(&ScheduledDeletion {
                attempts: 0,
                ..deletion
            })
            .unwrap();
        assert_eq!(store.claim_due_deletions(1_000, 10).unwrap().len(), 1);
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
        assert!(store.restore_callback("nonce-1").unwrap());
        assert!(store.take_callback("nonce-1", 17).unwrap().is_some());
    }

    #[test]
    fn status_callback_retirement_preserves_plan_callbacks() {
        let store = SqliteStore::in_memory().unwrap();
        store.upsert_session_space(&space()).unwrap();
        let status = StoredCallback {
            nonce: "status-1".into(),
            space_id: "space-1".into(),
            generation: 0,
            action: serde_json::json!({"action":"space_refresh"}).to_string(),
            expires_at_ms: i64::MAX,
        };
        let plan = StoredCallback {
            nonce: "plan-1".into(),
            space_id: "space-1".into(),
            generation: 0,
            action: serde_json::json!({"decision":"execute"}).to_string(),
            expires_at_ms: i64::MAX,
        };
        store.create_callback(&status).unwrap();
        store.create_callback(&plan).unwrap();

        assert_eq!(store.retire_status_callbacks("space-1", 0).unwrap(), 1);
        assert_eq!(store.peek_callback("status-1", i64::MAX).unwrap(), None);
        assert_eq!(store.peek_callback("plan-1", i64::MAX).unwrap(), Some(plan));
    }

    #[test]
    fn close_session_space_is_atomic_and_invalidates_current_generation() {
        let store = SqliteStore::in_memory().unwrap();
        store.upsert_session_space(&space()).unwrap();
        store
            .create_status_callback(&StoredCallback {
                nonce: "status-close-1".into(),
                space_id: "space-1".into(),
                generation: 0,
                action: serde_json::json!({
                    "space_id": "space-1",
                    "generation": 0,
                    "thread_id": "thread-1",
                    "action": "space_unwatch"
                })
                .to_string(),
                expires_at_ms: i64::MAX,
            })
            .unwrap();
        store
            .create_callback(&StoredCallback {
                nonce: "workflow-close-high-generation".into(),
                space_id: "space-1".into(),
                generation: 99,
                action: "approval".into(),
                expires_at_ms: i64::MAX,
            })
            .unwrap();
        store
            .upsert_workflow_record(
                "queue",
                "queue-close-1",
                &serde_json::json!({
                    "space_id": "space-1",
                    "thread_id": "thread-1",
                    "generation": 0,
                    "status": "queued",
                    "prompt": "queued before close"
                }),
                10,
            )
            .unwrap();

        let closed = store
            .close_session_space("space-1", 0, 100)
            .unwrap()
            .expect("current generation should close");
        assert_eq!(closed.lifecycle, "closed");
        assert_eq!(closed.generation, 1);
        assert_eq!(closed.closed_at_ms, Some(100));
        assert_eq!(store.peek_callback("status-close-1", 100).unwrap(), None);
        assert_eq!(
            store
                .peek_callback("workflow-close-high-generation", 100)
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .workflow_record("queue", "queue-close-1")
                .unwrap()
                .and_then(|value| {
                    value
                        .get("status")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap(),
            "cancelled"
        );

        assert!(
            store
                .close_session_space("space-1", 0, 200)
                .unwrap()
                .is_none()
        );
        let persisted = store
            .get_session_space("space-1")
            .unwrap()
            .expect("closed row remains durable");
        assert_eq!(persisted.generation, 1);
        assert_eq!(persisted.closed_at_ms, Some(100));

        let mut stale = persisted.clone();
        stale.lifecycle = "active".into();
        stale.generation = 0;
        stale.status_message_id = Some(999);
        stale.updated_at_ms = 200;
        store.upsert_session_space(&stale).unwrap();
        let still_closed = store
            .get_session_space("space-1")
            .unwrap()
            .expect("closed row remains durable after stale update");
        assert_eq!(still_closed.lifecycle, "closed");
        assert_eq!(still_closed.generation, 1);
        assert_eq!(still_closed.status_message_id, None);
    }
}
