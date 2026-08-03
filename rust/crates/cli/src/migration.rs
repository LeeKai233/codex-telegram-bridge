//! Read-only inventory of the Python bridge SQLite state before a staged
//! migration. It intentionally emits no message text, paths, chat IDs, or
//! credentials.

use rusqlite::{Connection, OpenFlags, params, types::ValueRef};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct MigrationReport {
    pub schema_version: i64,
    pub tables: Vec<TableSummary>,
    pub binding_shape: BindingShape,
    pub recommendation: &'static str,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct TableSummary {
    pub name: String,
    pub rows: u64,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub enum BindingShape {
    Missing,
    LegacyLinkedDiscussion,
    ForumTopic,
    Unknown,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ImportReport {
    pub source_schema_version: i64,
    pub source_path_sha256: String,
    pub target_schema_version: i64,
    pub dry_run: bool,
    pub tables: Vec<ImportTableReport>,
    pub reconciliation: Vec<ReconciliationItem>,
    pub imported_rows: u64,
    pub blocked: bool,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ImportTableReport {
    pub table: String,
    pub rows: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct ReconciliationItem {
    pub table: String,
    pub state: String,
    pub rows: u64,
    pub action: String,
}

#[derive(Clone, Debug)]
struct LegacyRow {
    table: String,
    row_key: String,
    row_json: String,
    row_sha256: String,
}

pub fn inspect_legacy_database(path: impl AsRef<Path>) -> Result<MigrationReport, MigrationError> {
    let connection = Connection::open_with_flags(
        path.as_ref(),
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(|_| MigrationError::Unreadable)?;
    let schema_version = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|_| MigrationError::Unreadable)?;
    let mut statement = connection
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        .map_err(|_| MigrationError::Unreadable)?;
    let names = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|_| MigrationError::Unreadable)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| MigrationError::Unreadable)?;
    let mut tables = Vec::with_capacity(names.len());
    for name in names {
        if !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(MigrationError::UnexpectedTableName);
        }
        let sql = format!("SELECT COUNT(*) FROM \"{name}\"");
        let rows: i64 = connection
            .query_row(&sql, [], |row| row.get(0))
            .map_err(|_| MigrationError::Unreadable)?;
        tables.push(TableSummary {
            name,
            rows: u64::try_from(rows).map_err(|_| MigrationError::Unreadable)?,
        });
    }
    let binding_shape = inspect_binding_shape(&connection)?;
    Ok(MigrationReport {
        schema_version,
        tables,
        binding_shape,
        recommendation: "export domain records, then import into a new Rust database during a maintenance window",
    })
}

/// Import every row from a Python bridge database into a fresh Rust-native
/// database. The source is opened read-only and is never migrated in place.
/// The generic legacy table is intentional: it preserves business state that
/// has not yet received a typed Rust projection instead of silently dropping it.
pub fn import_python_database(
    source_path: impl AsRef<Path>,
    target_path: impl AsRef<Path>,
    report_path: impl AsRef<Path>,
    dry_run: bool,
) -> Result<ImportReport, MigrationError> {
    let source_path = source_path.as_ref();
    let target_path = target_path.as_ref();
    let report_path = report_path.as_ref();
    if !source_path.is_file() {
        return Err(MigrationError::Unreadable);
    }
    if !dry_run && target_path.exists() {
        return Err(MigrationError::TargetExists);
    }
    let source_bytes = fs::read(source_path).map_err(|_| MigrationError::Unreadable)?;
    let source_path_sha256 = digest_bytes(&source_bytes);
    let connection = Connection::open_with_flags(
        source_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
    )
    .map_err(|_| MigrationError::Unreadable)?;
    integrity_check(&connection)?;
    let source_schema_version = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|_| MigrationError::Unreadable)?;
    if !(0..=11).contains(&source_schema_version) {
        return Err(MigrationError::UnsupportedSchema(source_schema_version));
    }

    let names = table_names(&connection)?;
    let mut rows = Vec::new();
    let mut tables = Vec::with_capacity(names.len());
    for table in names {
        let table_rows = read_table_rows(&connection, &table)?;
        let mut row_hashes = table_rows
            .iter()
            .map(|row| row.row_sha256.as_str())
            .collect::<Vec<_>>();
        row_hashes.sort_unstable();
        tables.push(ImportTableReport {
            table: table.clone(),
            rows: table_rows.len() as u64,
            sha256: digest_text(&row_hashes.join("\n")),
        });
        rows.extend(table_rows);
    }
    let reconciliation = reconcile_connection_bound_work(&connection)?;
    let blocked = reconciliation.iter().any(|item| item.state == "blocked");
    let mut report = ImportReport {
        source_schema_version,
        source_path_sha256,
        target_schema_version: 5,
        dry_run,
        tables,
        reconciliation,
        imported_rows: 0,
        blocked,
    };
    if !dry_run && blocked {
        write_report(report_path, &report)?;
        return Err(MigrationError::BlockedReconciliation);
    }
    if !dry_run {
        let _store = ctg_storage_sqlite::SqliteStore::open(target_path)
            .map_err(|error| MigrationError::Target(error.to_string()))?;
        drop(_store);
        let target = Connection::open(target_path).map_err(|_| MigrationError::TargetExists)?;
        let transaction = target
            .unchecked_transaction()
            .map_err(|_| MigrationError::Target("target transaction failed".into()))?;
        let imported_at_ms = now_ms();
        let owner_chat_id = rows
            .iter()
            .find(|row| row.table == "owner")
            .and_then(|row| serde_json::from_str::<Value>(&row.row_json).ok())
            .and_then(|value| value.get("chat_id").and_then(Value::as_i64));
        for row in &rows {
            transaction
                .execute(
                    "INSERT INTO rust_legacy_records(table_name, row_key, source_schema_version, row_json, row_sha256, imported_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![row.table, row.row_key, source_schema_version, row.row_json, row.row_sha256, imported_at_ms],
                )
                .map_err(|_| MigrationError::Target("legacy row insert failed".into()))?;
            if let Some((kind, key, payload)) = workflow_projection(row) {
                let payload_json = serde_json::to_string(&payload).map_err(|_| {
                    MigrationError::Target("workflow projection serialization failed".into())
                })?;
                let row_sha256 = digest_text(&payload_json);
                transaction
                    .execute(
                        "INSERT OR REPLACE INTO rust_legacy_records(table_name, row_key, source_schema_version, row_json, row_sha256, imported_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                        params![format!("rust_workflow:{kind}"), key, source_schema_version, payload_json, row_sha256, imported_at_ms],
                    )
                    .map_err(|_| MigrationError::Target("workflow projection insert failed".into()))?;
                import_typed_projection(&transaction, row, owner_chat_id, imported_at_ms)?;
            }
        }
        report.imported_rows = rows.len() as u64;
        let report_json = serde_json::to_string(&report)
            .map_err(|_| MigrationError::Target("report serialization failed".into()))?;
        transaction
            .execute(
                "INSERT INTO rust_import_metadata(singleton, source_schema_version, source_path_sha256, imported_at_ms, report_json) VALUES (1, ?1, ?2, ?3, ?4)",
                params![source_schema_version, report.source_path_sha256, imported_at_ms, report_json],
            )
            .map_err(|_| MigrationError::Target("import metadata insert failed".into()))?;
        transaction
            .commit()
            .map_err(|_| MigrationError::Target("target commit failed".into()))?;
        integrity_check(&target)?;
    }
    write_report(report_path, &report)?;
    Ok(report)
}

fn table_names(connection: &Connection) -> Result<Vec<String>, MigrationError> {
    let mut statement = connection
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        .map_err(|_| MigrationError::Unreadable)?;
    statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|_| MigrationError::Unreadable)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| MigrationError::Unreadable)
}

fn read_table_rows(connection: &Connection, table: &str) -> Result<Vec<LegacyRow>, MigrationError> {
    validate_identifier(table)?;
    let query = format!("SELECT * FROM \"{table}\"");
    let mut statement = connection
        .prepare(&query)
        .map_err(|_| MigrationError::Unreadable)?;
    let columns = statement
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mut raw_rows = statement
        .query([])
        .map_err(|_| MigrationError::Unreadable)?;
    let mut values = Vec::new();
    while let Some(row) = raw_rows.next().map_err(|_| MigrationError::Unreadable)? {
        let mut object = BTreeMap::new();
        for (index, column) in columns.iter().enumerate() {
            let value = row.get_ref(index).map_err(|_| MigrationError::Unreadable)?;
            object.insert(column.clone(), value_to_json(value));
        }
        let row_json = serde_json::to_string(&object).map_err(|_| MigrationError::Unreadable)?;
        values.push(row_json);
    }
    values.sort_unstable();
    Ok(values
        .into_iter()
        .enumerate()
        .map(|(index, row_json)| {
            let row_sha256 = digest_text(&row_json);
            LegacyRow {
                table: table.to_owned(),
                row_key: format!("{row_sha256}-{index:08}"),
                row_json,
                row_sha256,
            }
        })
        .collect())
}

fn workflow_projection(row: &LegacyRow) -> Option<(String, String, Value)> {
    let value: Value = serde_json::from_str(&row.row_json).ok()?;
    let object = value.as_object()?;
    match row.table.as_str() {
        "owner" => Some(("onboarding".into(), "owner".into(), value)),
        "telegram_binding" => {
            let binding = object
                .get("binding_json")
                .and_then(Value::as_str)
                .and_then(|text| serde_json::from_str(text).ok())
                .unwrap_or(value);
            Some(("onboarding".into(), "binding".into(), binding))
        }
        "prompt_intents" => {
            let key = object
                .get("client_message_id")
                .or_else(|| object.get("intent_id"))
                .and_then(Value::as_str)?
                .to_owned();
            Some(("prompt".into(), key, value))
        }
        "plan_publications" => {
            let space = object.get("space_id").and_then(Value::as_str)?;
            let generation = object
                .get("generation")
                .and_then(Value::as_i64)
                .unwrap_or(0);
            let item = object
                .get("item_id")
                .and_then(Value::as_str)
                .unwrap_or("plan");
            let revision = object
                .get("revision_key")
                .and_then(Value::as_str)
                .unwrap_or("");
            let mut payload = object.clone();
            for (source, target) in [
                ("message_ids_json", "message_ids"),
                ("action_message_ids_json", "action_message_ids"),
            ] {
                if let Some(raw) = object.get(source).and_then(Value::as_str)
                    && let Ok(decoded) = serde_json::from_str::<Value>(raw)
                {
                    payload.insert(target.into(), decoded);
                }
            }
            if !payload.contains_key("updated_at_ms") {
                payload.insert(
                    "updated_at_ms".into(),
                    Value::from(epoch_ms(object_i64(&value, "updated_at"))),
                );
            }
            if payload
                .get("decision_turn_id")
                .and_then(Value::as_str)
                .is_some_and(str::is_empty)
            {
                payload.insert("decision_turn_id".into(), Value::Null);
            }
            Some((
                "plan".into(),
                format!("{space}:{generation}:{item}:{revision}"),
                Value::Object(payload),
            ))
        }
        "session_spaces" => {
            let key = object.get("space_id").and_then(Value::as_str)?.to_owned();
            Some(("space".into(), key, value))
        }
        _ => None,
    }
}

fn object_i64(value: &Value, key: &str) -> i64 {
    value.get(key).and_then(Value::as_i64).unwrap_or_default()
}

fn epoch_ms(value: i64) -> i64 {
    if value > 0 && value < 10_000_000_000 {
        value.saturating_mul(1000)
    } else {
        value
    }
}

fn object_text(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(Value::as_str).map(str::to_owned)
}

fn import_typed_projection(
    transaction: &rusqlite::Transaction<'_>,
    row: &LegacyRow,
    owner_chat_id: Option<i64>,
    imported_at_ms: i64,
) -> Result<(), MigrationError> {
    let value: Value =
        serde_json::from_str(&row.row_json).map_err(|_| MigrationError::Unreadable)?;
    if row.table != "session_spaces" {
        return Ok(());
    }
    let space_id = object_text(&value, "space_id").ok_or(MigrationError::Unreadable)?;
    let thread_id = object_text(&value, "thread_id");
    let lifecycle = object_text(&value, "lifecycle").unwrap_or_else(|| "active".into());
    let generation = object_i64(&value, "generation");
    let channel_chat_id = object_i64(&value, "channel_chat_id");
    let channel_post_id = object_i64(&value, "channel_post_id").max(1);
    let discussion_chat_id = value.get("discussion_chat_id").and_then(Value::as_i64);
    let discussion_root_id = value
        .get("discussion_root_id")
        .or_else(|| value.get("discussion_root_message_id"))
        .and_then(Value::as_i64);
    let status_message_id = object_text(&value, "status_message_id")
        .and_then(|text| text.parse::<i64>().ok())
        .or_else(|| value.get("status_message_id").and_then(Value::as_i64));
    let state_json = value
        .get("state_json")
        .and_then(Value::as_str)
        .and_then(|text| serde_json::from_str::<Value>(text).ok())
        .unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    let plan_mode = state_json
        .get("current_mode")
        .and_then(Value::as_str)
        .is_some_and(|mode| mode == "plan");
    let created_at_ms = epoch_ms(object_i64(&value, "created_at"));
    let updated_at_value = epoch_ms(object_i64(&value, "updated_at"));
    let updated_at_ms = if updated_at_value > 0 {
        updated_at_value
    } else {
        imported_at_ms
    };
    transaction
        .execute(
            "INSERT OR REPLACE INTO rust_session_spaces(space_id, thread_id, lifecycle, generation, channel_chat_id, channel_post_id, discussion_chat_id, discussion_root_message_id, status_message_id, status_bot_instance, owner_chat_id, plan_mode, created_at_ms, updated_at_ms) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10, ?11, ?12, ?13)",
            rusqlite::params![
                space_id,
                thread_id,
                lifecycle,
                generation,
                channel_chat_id,
                channel_post_id,
                discussion_chat_id,
                discussion_root_id,
                status_message_id,
                owner_chat_id,
                i64::from(plan_mode),
                created_at_ms,
                updated_at_ms,
            ],
        )
        .map_err(|_| MigrationError::Target("session space projection failed".into()))?;
    if matches!(lifecycle.as_str(), "pending" | "repair_required") {
        let mut pending = value.clone();
        if let Some(state) = pending
            .get("state_json")
            .and_then(Value::as_str)
            .and_then(|text| serde_json::from_str::<Value>(text).ok())
            && let (Some(target), Some(source)) = (pending.as_object_mut(), state.as_object())
        {
            for key in [
                "pending_cwd",
                "pending_prompt",
                "normal_model",
                "normal_effort",
                "plan_model",
                "plan_effort",
                "current_mode",
            ] {
                if !target.contains_key(key)
                    && let Some(field) = source.get(key)
                {
                    target.insert(key.to_owned(), field.clone());
                }
            }
        }
        if pending.get("pending_cwd").and_then(Value::as_str).is_some()
            && pending
                .get("pending_prompt")
                .and_then(Value::as_str)
                .is_some()
        {
            pending["space_id"] = Value::String(space_id.clone());
            let payload_json = serde_json::to_string(&pending)
                .map_err(|_| MigrationError::Target("pending space serialization failed".into()))?;
            let row_sha256 = digest_text(&payload_json);
            transaction
                .execute(
                    "INSERT OR REPLACE INTO rust_legacy_records(table_name, row_key, source_schema_version, row_json, row_sha256, imported_at_ms) VALUES ('rust_workflow:pending_space', ?1, -1, ?2, ?3, ?4)",
                    params![space_id, payload_json, row_sha256, imported_at_ms],
                )
                .map_err(|_| MigrationError::Target("pending workflow projection failed".into()))?;
        }
    }
    Ok(())
}

fn reconcile_connection_bound_work(
    connection: &Connection,
) -> Result<Vec<ReconciliationItem>, MigrationError> {
    let mut items = Vec::new();
    let checks = [
        (
            "prompt_queue",
            "status",
            &["dispatched", "submitting"] as &[&str],
            "blocked",
        ),
        (
            "prompt_intents",
            "state",
            &["submitting", "uncertain"] as &[&str],
            "blocked",
        ),
        (
            "pending_inputs",
            "status",
            &["pending", "claimed"] as &[&str],
            "blocked",
        ),
        ("callbacks", "used_at", &["NULL"] as &[&str], "resumable"),
        (
            "plan_publications",
            "status",
            &["published", "executing", "revising"] as &[&str],
            "resumable",
        ),
    ];
    for (table, column, states, action) in checks {
        if !has_table(connection, table)? {
            continue;
        }
        let count = if states == ["NULL"] {
            let query = format!("SELECT COUNT(*) FROM \"{table}\" WHERE \"{column}\" IS NULL");
            connection.query_row(&query, [], |row| row.get::<_, i64>(0))
        } else {
            let placeholders = std::iter::repeat_n("?", states.len())
                .collect::<Vec<_>>()
                .join(",");
            let query =
                format!("SELECT COUNT(*) FROM \"{table}\" WHERE \"{column}\" IN ({placeholders})");
            connection.query_row(&query, rusqlite::params_from_iter(states.iter()), |row| {
                row.get::<_, i64>(0)
            })
        }
        .map_err(|_| MigrationError::Unreadable)?;
        if count > 0 {
            items.push(ReconciliationItem {
                table: table.to_owned(),
                state: action.to_owned(),
                rows: count as u64,
                action: if action == "blocked" {
                    "settle before cutover".into()
                } else {
                    "retain callback/state and verify after reconnect".into()
                },
            });
        }
    }
    Ok(items)
}

fn has_table(connection: &Connection, table: &str) -> Result<bool, MigrationError> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
            params![table],
            |row| row.get::<_, i64>(0),
        )
        .map(|value| value == 1)
        .map_err(|_| MigrationError::Unreadable)
}

fn value_to_json(value: ValueRef<'_>) -> serde_json::Value {
    match value {
        ValueRef::Null => serde_json::Value::Null,
        ValueRef::Integer(value) => serde_json::Value::from(value),
        ValueRef::Real(value) => serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        ValueRef::Text(value) => serde_json::Value::String(String::from_utf8_lossy(value).into()),
        ValueRef::Blob(value) => serde_json::Value::String(format!("hex:{}", hex_bytes(value))),
    }
}

fn validate_identifier(value: &str) -> Result<(), MigrationError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(MigrationError::UnexpectedTableName);
    }
    Ok(())
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn digest_bytes(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}

fn digest_text(text: &str) -> String {
    digest_bytes(text.as_bytes())
}

fn integrity_check(connection: &Connection) -> Result<(), MigrationError> {
    let result: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .map_err(|_| MigrationError::Unreadable)?;
    if result != "ok" {
        return Err(MigrationError::Integrity(result));
    }
    Ok(())
}

fn write_report(path: &Path, report: &ImportReport) -> Result<(), MigrationError> {
    let text = serde_json::to_string_pretty(report).map_err(|_| MigrationError::Report)?;
    fs::write(path, format!("{text}\n")).map_err(|_| MigrationError::Report)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn inspect_binding_shape(connection: &Connection) -> Result<BindingShape, MigrationError> {
    let binding: Option<String> = match connection.query_row(
        "SELECT binding_json FROM telegram_binding LIMIT 1",
        params![],
        |row| row.get(0),
    ) {
        Ok(value) => Some(value),
        Err(rusqlite::Error::SqliteFailure(_, Some(message)))
            if message.contains("no such table") =>
        {
            None
        }
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(_) => return Err(MigrationError::Unreadable),
    };
    let Some(binding) = binding else {
        return Ok(BindingShape::Missing);
    };
    let value: serde_json::Value =
        serde_json::from_str(&binding).map_err(|_| MigrationError::InvalidBinding)?;
    if value.get("is_forum").and_then(serde_json::Value::as_bool) == Some(true) {
        Ok(BindingShape::ForumTopic)
    } else if value.get("discussion_chat_id").is_some() {
        Ok(BindingShape::LegacyLinkedDiscussion)
    } else {
        Ok(BindingShape::Unknown)
    }
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("legacy SQLite database is unreadable")]
    Unreadable,
    #[error("legacy database contains an unexpected table name")]
    UnexpectedTableName,
    #[error("legacy Telegram binding is invalid JSON")]
    InvalidBinding,
    #[error("legacy database schema version {0} is not supported")]
    UnsupportedSchema(i64),
    #[error("target database already exists")]
    TargetExists,
    #[error("target database error: {0}")]
    Target(String),
    #[error("legacy database failed integrity_check: {0}")]
    Integrity(String),
    #[error("connection-bound work must be reconciled before cutover")]
    BlockedReconciliation,
    #[error("could not write migration report")]
    Report,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn inventory_is_read_only_and_classifies_legacy_binding() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../../target/test-tmp")
            .join(format!("codex-migration-{}.sqlite3", std::process::id()));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch("CREATE TABLE telegram_binding (binding_json TEXT); INSERT INTO telegram_binding VALUES ('{\"discussion_chat_id\":1,\"is_forum\":false}'); CREATE TABLE threads (id TEXT);").unwrap();
        drop(connection);
        let report = inspect_legacy_database(&path).unwrap();
        assert_eq!(report.binding_shape, BindingShape::LegacyLinkedDiscussion);
        assert_eq!(report.tables.len(), 2);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn imports_all_legacy_rows_into_a_new_rust_database() {
        let root =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../target/test-tmp");
        std::fs::create_dir_all(&root).unwrap();
        let source = root.join(format!(
            "codex-import-source-{}.sqlite3",
            std::process::id()
        ));
        let target = root.join(format!(
            "codex-import-target-{}.sqlite3",
            std::process::id()
        ));
        let report_path = root.join(format!("codex-import-report-{}.json", std::process::id()));
        let connection = Connection::open(&source).unwrap();
        connection
            .execute_batch("PRAGMA user_version=11; CREATE TABLE metadata (key TEXT, value TEXT); INSERT INTO metadata VALUES ('schema_version','11'); CREATE TABLE threads (thread_id TEXT, state_json TEXT, updated_at INTEGER); INSERT INTO threads VALUES ('thread-1','{}',1);")
            .unwrap();
        drop(connection);
        let report = import_python_database(&source, &target, &report_path, false).unwrap();
        assert_eq!(report.source_schema_version, 11);
        assert_eq!(report.imported_rows, 2);
        assert!(!report.blocked);
        let target_connection = Connection::open(&target).unwrap();
        let count: i64 = target_connection
            .query_row("SELECT COUNT(*) FROM rust_legacy_records", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(count, 2);
        for path in [source, target, report_path] {
            std::fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn imports_pending_session_payload_and_converts_epoch_units() {
        let root =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../target/test-tmp");
        std::fs::create_dir_all(&root).unwrap();
        let suffix = std::process::id();
        let source = root.join(format!("codex-pending-source-{suffix}.sqlite3"));
        let target = root.join(format!("codex-pending-target-{suffix}.sqlite3"));
        let report_path = root.join(format!("codex-pending-report-{suffix}.json"));
        let connection = Connection::open(&source).unwrap();
        connection
            .execute_batch(
                "PRAGMA user_version=11;
                 CREATE TABLE session_spaces (
                   space_id TEXT,
                   space_type TEXT,
                   lifecycle TEXT,
                   thread_id TEXT,
                   channel_chat_id INTEGER,
                   channel_post_id INTEGER,
                   discussion_chat_id INTEGER,
                   discussion_root_id INTEGER,
                   status_message_id INTEGER,
                   generation INTEGER,
                   state_json TEXT,
                   created_at INTEGER,
                   updated_at INTEGER
                 );
                 INSERT INTO session_spaces VALUES
                   ('space-pending','pending_new','pending',NULL,-1004446000549,81,-1004290500369,700,NULL,3,
                    '{\"pending_cwd\":\"/workspace/demo\",\"pending_prompt\":\"Build it\",\"normal_model\":\"gpt-5\",\"normal_effort\":\"high\"}',
                    1700000000,1700000001);",
            )
            .unwrap();
        drop(connection);

        import_python_database(&source, &target, &report_path, false).unwrap();
        let target_connection = Connection::open(&target).unwrap();
        let (lifecycle, created_at_ms, updated_at_ms): (String, i64, i64) = target_connection
            .query_row(
                "SELECT lifecycle, created_at_ms, updated_at_ms FROM rust_session_spaces WHERE space_id='space-pending'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(lifecycle, "pending");
        assert_eq!(created_at_ms, 1_700_000_000_000);
        assert_eq!(updated_at_ms, 1_700_000_001_000);
        let payload: String = target_connection
            .query_row(
                "SELECT row_json FROM rust_legacy_records WHERE table_name='rust_workflow:pending_space' AND row_key='space-pending'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let payload: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(payload["pending_prompt"], "Build it");
        assert_eq!(payload["normal_effort"], "high");

        for path in [source, target, report_path] {
            std::fs::remove_file(path).unwrap();
        }
    }
}
