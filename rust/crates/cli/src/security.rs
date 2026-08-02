//! TOTP verification and the durable Rust write lock.

use ctg_storage_sqlite::SqliteStore;
use data_encoding::BASE32_NOPAD_NOCASE;
use sha1::{Digest, Sha1};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use thiserror::Error;

const TOTP_INTERVAL_SECONDS: i64 = 30;
const TOTP_DIGITS: u32 = 6;

#[derive(Debug, Error)]
pub enum TotpError {
    #[error("TOTP secret is unavailable")]
    SecretUnavailable,
    #[error("TOTP secret has unsafe permissions")]
    UnsafeSecret,
    #[error("TOTP code is invalid")]
    InvalidCode,
    #[error("TOTP state could not be updated: {0}")]
    State(String),
}

pub struct TotpManager {
    store: Arc<SqliteStore>,
    secret_path: PathBuf,
    unlock_seconds: i64,
}

impl TotpManager {
    pub fn new(
        store: Arc<SqliteStore>,
        secret_path: impl Into<PathBuf>,
        unlock_seconds: u64,
    ) -> Self {
        Self {
            store,
            secret_path: secret_path.into(),
            unlock_seconds: i64::try_from(unlock_seconds).unwrap_or(i64::MAX),
        }
    }

    pub fn is_unlocked(&self, now_ms: i64) -> Result<bool, TotpError> {
        self.store
            .is_totp_unlocked(now_ms)
            .map_err(|error| TotpError::State(error.to_string()))
    }

    pub fn lock(&self) -> Result<(), TotpError> {
        self.store
            .lock_totp()
            .map_err(|error| TotpError::State(error.to_string()))
    }

    pub fn verify_and_unlock(&self, value: &str, now_ms: i64) -> Result<bool, TotpError> {
        let value = value.trim();
        if value.len() != TOTP_DIGITS as usize || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Ok(false);
        }
        let secret = read_secret(&self.secret_path)?;
        let secret = secret.trim_end_matches('=').as_bytes();
        let key = BASE32_NOPAD_NOCASE
            .decode(secret)
            .map_err(|_| TotpError::SecretUnavailable)?;
        let current = now_ms.div_euclid(1000).div_euclid(TOTP_INTERVAL_SECONDS);
        for offset in -1_i64..=1_i64 {
            let candidate_timecode = current.saturating_add(offset);
            if candidate_timecode < 0 {
                continue;
            }
            let candidate = hotp(&key, candidate_timecode as u64);
            if candidate.as_bytes() == value.as_bytes() {
                return self
                    .store
                    .accept_totp_timecode(candidate_timecode, now_ms, self.unlock_seconds)
                    .map_err(|error| TotpError::State(error.to_string()));
            }
        }
        Ok(false)
    }
}

pub fn is_private_regular_file(path: &Path) -> bool {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return false;
        }
    }
    true
}

fn read_secret(path: &Path) -> Result<String, TotpError> {
    fs::symlink_metadata(path).map_err(|_| TotpError::SecretUnavailable)?;
    if !is_private_regular_file(path) {
        return Err(TotpError::UnsafeSecret);
    }
    let secret = fs::read_to_string(path).map_err(|_| TotpError::SecretUnavailable)?;
    if secret.len() > 256 || secret.trim().is_empty() {
        return Err(TotpError::SecretUnavailable);
    }
    Ok(secret.trim().to_owned())
}

fn hotp(key: &[u8], counter: u64) -> String {
    let message = counter.to_be_bytes();
    let digest = hmac_sha1(key, &message);
    let offset = usize::from(digest[19] & 0x0f);
    let binary = (u32::from(digest[offset]) & 0x7f) << 24
        | u32::from(digest[offset + 1]) << 16
        | u32::from(digest[offset + 2]) << 8
        | u32::from(digest[offset + 3]);
    format!("{:06}", binary % 10_u32.pow(TOTP_DIGITS))
}

fn hmac_sha1(key: &[u8], message: &[u8]) -> [u8; 20] {
    let mut block = [0_u8; 64];
    if key.len() > block.len() {
        let digest = Sha1::digest(key);
        block[..digest.len()].copy_from_slice(&digest);
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Vec::with_capacity(64 + message.len());
    inner.extend(block.iter().map(|byte| byte ^ 0x36));
    inner.extend_from_slice(message);
    let inner_digest = Sha1::digest(&inner);
    let mut outer = Vec::with_capacity(64 + inner_digest.len());
    outer.extend(block.iter().map(|byte| byte ^ 0x5c));
    outer.extend_from_slice(&inner_digest);
    let digest = Sha1::digest(&outer);
    let mut output = [0_u8; 20];
    output.copy_from_slice(&digest);
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn matches_rfc6238_sha1_vector() {
        let key = b"12345678901234567890";
        assert_eq!(hotp(key, 59 / 30), "287082");
        assert_eq!(hotp(key, 1_111_111_109 / 30), "081804");
        assert_eq!(hotp(key, 1_111_111_111 / 30), "050471");
        assert_eq!(hotp(key, 1_234_567_890 / 30), "005924");
        assert_eq!(hotp(key, 2_000_000_000 / 30), "279037");
    }

    #[test]
    fn consumes_a_timecode_once_and_lock_is_durable() {
        let store = Arc::new(SqliteStore::in_memory().unwrap());
        let directory = std::env::temp_dir().join(format!("ctg-totp-{}", std::process::id()));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("totp_secret");
        fs::write(&path, "JBSWY3DPEHPK3PXP\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        let manager = TotpManager::new(store.clone(), &path, 60);
        let now = 1_700_000_000_000_i64;
        let code = hotp(
            &BASE32_NOPAD_NOCASE.decode(b"JBSWY3DPEHPK3PXP").unwrap(),
            (now / 1000) as u64 / 30,
        );
        assert!(manager.verify_and_unlock(&code, now).unwrap());
        assert!(!manager.verify_and_unlock(&code, now).unwrap());
        assert!(manager.is_unlocked(now + 1000).unwrap());
        manager.lock().unwrap();
        assert!(!manager.is_unlocked(now + 1000).unwrap());
        let _ = fs::remove_file(path);
        let _ = fs::remove_dir(directory);
    }
}
