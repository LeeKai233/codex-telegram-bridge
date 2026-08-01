//! File-backed bot credentials with deliberate secret-redaction boundaries.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum CredentialRole {
    Control,
    Discussion,
    Status,
}

impl CredentialRole {
    pub const ALL: [Self; 3] = [Self::Control, Self::Discussion, Self::Status];

    pub const fn file_name(self) -> &'static str {
        match self {
            Self::Control => "control.token",
            Self::Discussion => "discussion.token",
            Self::Status => "status.token",
        }
    }
}

impl fmt::Display for CredentialRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Control => "control",
            Self::Discussion => "discussion",
            Self::Status => "status",
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
        let path = self.path_for(role);
        let metadata = fs::metadata(&path).map_err(|_| CredentialError::Missing {
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
        let path = std::env::temp_dir().join(format!(
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
}
