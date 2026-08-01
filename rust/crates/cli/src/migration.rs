//! Read-only inventory of the Python bridge SQLite state before a staged
//! migration. It intentionally emits no message text, paths, chat IDs, or
//! credentials.

use rusqlite::{Connection, OpenFlags, params};
use serde::Serialize;
use std::path::Path;
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
}
