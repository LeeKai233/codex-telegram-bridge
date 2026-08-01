//! File-backed bot credentials with deliberate secret-redaction boundaries.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum CredentialRole {
    Control,
    Discussion,
    Status,
    ProductionAlert,
    CanaryAlert,
    Approval,
    Artifact,
}

impl CredentialRole {
    pub const ALL: [Self; 7] = [
        Self::Control,
        Self::Discussion,
        Self::Status,
        Self::ProductionAlert,
        Self::CanaryAlert,
        Self::Approval,
        Self::Artifact,
    ];

    pub const RUST_TEST: [Self; 4] = [
        Self::Discussion,
        Self::Status,
        Self::CanaryAlert,
        Self::ProductionAlert,
    ];

    pub const fn file_name(self) -> &'static str {
        match self {
            Self::Control => "control.token",
            Self::Discussion => "discussion.token",
            Self::Status => "status.token",
            Self::ProductionAlert => "production-alert.token",
            Self::CanaryAlert => "canary-alert.token",
            Self::Approval => "approval.token",
            Self::Artifact => "artifact.token",
        }
    }

    /// Stable defaults for the local `.tgrc` convention. These are credential
    /// names, not Telegram numeric identities; deployments may provide their
    /// own key names through `TgrcCredentials::get`.
    pub const fn default_tgrc_key(self) -> &'static str {
        match self {
            Self::Control => "rust_9527_bot_key",
            Self::Discussion => "rust_91_bot_key",
            Self::Status => "rust_818_bot_key",
            Self::ProductionAlert => "rust_826_bot_key",
            Self::CanaryAlert => "rust_411_bot_key",
            Self::Approval => "rust_69_bot_key",
            Self::Artifact => "rust_426_bot_key",
        }
    }
}

impl fmt::Display for CredentialRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Control => "control",
            Self::Discussion => "discussion",
            Self::Status => "status",
            Self::ProductionAlert => "production-alert",
            Self::CanaryAlert => "canary-alert",
            Self::Approval => "approval",
            Self::Artifact => "artifact",
        })
    }
}

/// A token whose standard formatting can never disclose its value.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct BotToken(String);

impl BotToken {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, CredentialError> {
        let value = value.as_ref().trim();
        if value.is_empty() {
            return Err(CredentialError::EmptyToken);
        }
        if value.chars().any(char::is_whitespace) {
            return Err(CredentialError::MalformedToken);
        }
        Ok(Self(value.to_owned()))
    }

    /// Exposes the token only to an HTTP adapter that must authenticate a call.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for BotToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BotToken([REDACTED])")
    }
}

impl fmt::Display for BotToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialFiles {
    root: PathBuf,
}

impl CredentialFiles {
    pub fn discover(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn path_for(&self, role: CredentialRole) -> PathBuf {
        self.root.join(role.file_name())
    }

    pub fn present_roles(&self) -> Vec<CredentialRole> {
        CredentialRole::ALL
            .into_iter()
            .filter(|role| self.path_for(*role).is_file())
            .collect()
    }

    pub fn read(&self, role: CredentialRole) -> Result<BotToken, CredentialError> {
        self.read_path(role, self.path_for(role))
    }

    pub fn read_path(
        &self,
        role: CredentialRole,
        path: impl Into<PathBuf>,
    ) -> Result<BotToken, CredentialError> {
        let path = path.into();
        let metadata = fs::symlink_metadata(&path).map_err(|_| CredentialError::Missing {
            role,
            path: path.clone(),
        })?;
        if !metadata.is_file() {
            return Err(CredentialError::NotRegularFile { role, path });
        }
        enforce_private_permissions(&metadata, role, &path)?;
        let content =
            fs::read_to_string(&path).map_err(|_| CredentialError::Unreadable { role, path })?;
        BotToken::parse(content)
    }

    pub fn read_role_or_tgrc(
        &self,
        role: CredentialRole,
        tgrc: Option<&TgrcCredentials>,
    ) -> Result<BotToken, CredentialError> {
        match self.read(role) {
            Ok(token) => Ok(token),
            Err(CredentialError::Missing { .. }) => tgrc
                .and_then(|credentials| credentials.get(role.default_tgrc_key()).cloned())
                .ok_or_else(|| CredentialError::Missing {
                    role,
                    path: self.path_for(role),
                }),
            Err(error) => Err(error),
        }
    }
}

/// A private, deliberately small parser for the existing `~/.tgrc` format.
/// Only `export NAME=value` entries are accepted; shell evaluation is never
/// performed and values are kept behind `BotToken`'s redacting formatters.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TgrcCredentials {
    entries: BTreeMap<String, BotToken>,
}

impl TgrcCredentials {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, CredentialError> {
        let path = path.as_ref().to_owned();
        let metadata = fs::symlink_metadata(&path)
            .map_err(|_| CredentialError::TgrcMissing { path: path.clone() })?;
        if !metadata.is_file() {
            return Err(CredentialError::TgrcNotRegularFile { path });
        }
        enforce_tgrc_permissions(&metadata, &path)?;
        let text =
            fs::read_to_string(&path).map_err(|_| CredentialError::TgrcUnreadable { path })?;
        Self::parse(&text)
    }

    pub fn parse(text: &str) -> Result<Self, CredentialError> {
        let mut entries = BTreeMap::new();
        for (line_number, raw_line) in text.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let assignment =
                line.strip_prefix("export ")
                    .ok_or_else(|| CredentialError::InvalidTgrcLine {
                        line: line_number + 1,
                    })?;
            let (key, raw_value) =
                assignment
                    .split_once('=')
                    .ok_or_else(|| CredentialError::InvalidTgrcLine {
                        line: line_number + 1,
                    })?;
            if !is_credential_key(key) {
                return Err(CredentialError::InvalidTgrcKey {
                    line: line_number + 1,
                });
            }
            let value = raw_value.trim();
            let value = value
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .or_else(|| {
                    value
                        .strip_prefix('\'')
                        .and_then(|value| value.strip_suffix('\''))
                })
                .unwrap_or(value);
            let token = BotToken::parse(value)?;
            if entries.insert(key.to_owned(), token).is_some() {
                return Err(CredentialError::DuplicateTgrcKey {
                    line: line_number + 1,
                });
            }
        }
        Ok(Self { entries })
    }

    pub fn get(&self, key: &str) -> Option<&BotToken> {
        self.entries.get(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }
}

fn is_credential_key(key: &str) -> bool {
    !key.is_empty()
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn enforce_tgrc_permissions(metadata: &fs::Metadata, path: &Path) -> Result<(), CredentialError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o077 != 0 {
            return Err(CredentialError::TgrcInsecurePermissions {
                path: path.to_owned(),
            });
        }
    }
    #[cfg(not(unix))]
    let _ = (metadata, path);
    Ok(())
}

fn enforce_private_permissions(
    metadata: &fs::Metadata,
    role: CredentialRole,
    path: &Path,
) -> Result<(), CredentialError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o077 != 0 {
            return Err(CredentialError::InsecurePermissions {
                role,
                path: path.to_owned(),
            });
        }
    }
    #[cfg(not(unix))]
    let _ = (metadata, role, path);
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CredentialError {
    Missing { role: CredentialRole, path: PathBuf },
    NotRegularFile { role: CredentialRole, path: PathBuf },
    InsecurePermissions { role: CredentialRole, path: PathBuf },
    Unreadable { role: CredentialRole, path: PathBuf },
    EmptyToken,
    MalformedToken,
    TgrcMissing { path: PathBuf },
    TgrcNotRegularFile { path: PathBuf },
    TgrcInsecurePermissions { path: PathBuf },
    TgrcUnreadable { path: PathBuf },
    InvalidTgrcLine { line: usize },
    InvalidTgrcKey { line: usize },
    DuplicateTgrcKey { line: usize },
}

impl fmt::Display for CredentialError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { role, path } => write!(
                formatter,
                "{role} credential is missing at {}",
                path.display()
            ),
            Self::NotRegularFile { role, path } => {
                write!(
                    formatter,
                    "{role} credential is not a regular file at {}",
                    path.display()
                )
            }
            Self::InsecurePermissions { role, path } => {
                write!(
                    formatter,
                    "{role} credential has insecure permissions at {}",
                    path.display()
                )
            }
            Self::Unreadable { role, path } => write!(
                formatter,
                "{role} credential is unreadable at {}",
                path.display()
            ),
            Self::EmptyToken => formatter.write_str("credential token is empty"),
            Self::MalformedToken => formatter.write_str("credential token has whitespace"),
            Self::TgrcMissing { path } => write!(
                formatter,
                "credential registry is missing at {}",
                path.display()
            ),
            Self::TgrcNotRegularFile { path } => write!(
                formatter,
                "credential registry is not a regular file at {}",
                path.display()
            ),
            Self::TgrcInsecurePermissions { path } => write!(
                formatter,
                "credential registry has insecure permissions at {}",
                path.display()
            ),
            Self::TgrcUnreadable { path } => write!(
                formatter,
                "credential registry is unreadable at {}",
                path.display()
            ),
            Self::InvalidTgrcLine { line } => {
                write!(formatter, "invalid credential registry line {line}")
            }
            Self::InvalidTgrcKey { line } => {
                write!(formatter, "invalid credential registry key at line {line}")
            }
            Self::DuplicateTgrcKey { line } => write!(
                formatter,
                "duplicate credential registry key at line {line}"
            ),
        }
    }
}

impl std::error::Error for CredentialError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    fn temporary_directory() -> PathBuf {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-tmp")
            .join(format!(
                "codex-telegram-credentials-{}-{}",
                std::process::id(),
                NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn token_formatting_is_redacted() {
        let token = BotToken::parse("123:secret-value").unwrap();
        assert!(!format!("{token:?}").contains("secret-value"));
        assert!(!format!("{token}").contains("secret-value"));
    }

    #[test]
    fn discovery_only_reports_existing_files() {
        let directory = temporary_directory();
        let path = directory.join(CredentialRole::Control.file_name());
        fs::write(&path, "123:secret-value").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }

        let files = CredentialFiles::discover(&directory);
        assert_eq!(files.present_roles(), vec![CredentialRole::Control]);
        assert_eq!(
            files.read(CredentialRole::Control).unwrap().as_str(),
            "123:secret-value"
        );
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn tgrc_parser_accepts_exported_tokens_without_shell_evaluation() {
        let registry = TgrcCredentials::parse(
            "# comment\nexport rust_91_bot_key='123:secret-value'\nexport rust_818_bot_key=456:other-value\n",
        )
        .unwrap();
        assert_eq!(registry.keys().count(), 2);
        assert_eq!(
            registry.get("rust_91_bot_key").unwrap().as_str(),
            "123:secret-value"
        );
        assert!(!format!("{registry:?}").contains("secret-value"));
    }
}
