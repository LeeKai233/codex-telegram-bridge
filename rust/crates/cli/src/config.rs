//! Declarative Rust vNext topology. Bot identities are configuration data;
//! the adapter never derives behaviour from Telegram numeric IDs.

use codex_telegram_adapter::{
    BotCapability, BotInstanceBinding, ChannelBinding, ForumTopicBinding, TelegramSurfaceBinding,
};
use codex_telegram_credentials::{BotToken, CredentialError, CredentialFiles, TgrcCredentials};
use serde::Deserialize;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize)]
pub struct RustConfig {
    #[serde(default = "default_api_base")]
    pub api_base: String,
    #[serde(default = "default_registry_path")]
    pub credential_registry: PathBuf,
    #[serde(default = "default_legacy_control_token")]
    pub legacy_control_token: PathBuf,
    #[serde(default = "default_lock_directory")]
    pub lock_directory: PathBuf,
    #[serde(default = "default_metrics_bind")]
    pub metrics_bind: String,
    #[serde(default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
    #[serde(default = "default_bots")]
    pub bots: Vec<BotConfig>,
    #[serde(default)]
    pub surfaces: Vec<SurfaceConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct BotConfig {
    pub instance_id: String,
    pub capability: BotCapability,
    pub credential_key: String,
    pub update_consumer: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SurfaceConfig {
    pub kind: SurfaceKind,
    pub bot_instance_id: String,
    pub chat_id: i64,
    #[serde(default)]
    pub message_thread_id: Option<i64>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SurfaceKind {
    Channel,
    ForumTopic,
}

impl Default for RustConfig {
    fn default() -> Self {
        Self {
            api_base: default_api_base(),
            credential_registry: default_registry_path(),
            legacy_control_token: default_legacy_control_token(),
            lock_directory: default_lock_directory(),
            metrics_bind: default_metrics_bind(),
            request_timeout_seconds: default_request_timeout_seconds(),
            bots: default_bots(),
            surfaces: Vec::new(),
        }
    }
}

impl RustConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = fs::read_to_string(path).map_err(|_| ConfigError::UnreadableConfig {
            path: path.to_owned(),
        })?;
        let mut config: Self = toml::from_str(&text).map_err(|_| ConfigError::InvalidConfig {
            path: path.to_owned(),
        })?;
        config.normalize(path.parent().unwrap_or_else(|| Path::new(".")))?;
        Ok(config)
    }

    pub fn load_default() -> Result<Self, ConfigError> {
        let path = default_config_path();
        if path.is_file() {
            Self::load(path)
        } else {
            Ok(Self::default())
        }
    }

    pub fn save_template(&self, path: impl AsRef<Path>) -> Result<(), ConfigError> {
        let text = toml::to_string_pretty(self).map_err(|_| ConfigError::InvalidTemplate)?;
        fs::write(path.as_ref(), text).map_err(|_| ConfigError::InvalidTemplate)
    }

    pub fn enabled_bot_bindings(&self) -> Result<Vec<BotInstanceBinding>, ConfigError> {
        self.bots
            .iter()
            .filter(|bot| bot.enabled)
            .map(|bot| {
                BotInstanceBinding::new(&bot.instance_id, bot.capability)
                    .with_update_consumer(&bot.update_consumer)
                    .map_err(|_| ConfigError::InvalidBot {
                        instance_id: bot.instance_id.clone(),
                    })
            })
            .collect()
    }

    pub fn surface_bindings(&self) -> Result<Vec<TelegramSurfaceBinding>, ConfigError> {
        self.surfaces
            .iter()
            .map(|surface| {
                let channel =
                    ChannelBinding::new(&surface.bot_instance_id, surface.chat_id.to_string())
                        .map_err(|_| ConfigError::InvalidSurface {
                            bot_instance_id: surface.bot_instance_id.clone(),
                        })?;
                match surface.kind {
                    SurfaceKind::Channel => Ok(TelegramSurfaceBinding::Channel(channel)),
                    SurfaceKind::ForumTopic => {
                        let thread = surface.message_thread_id.ok_or_else(|| {
                            ConfigError::InvalidSurface {
                                bot_instance_id: surface.bot_instance_id.clone(),
                            }
                        })?;
                        ForumTopicBinding::new(channel, thread)
                            .map(TelegramSurfaceBinding::ForumTopic)
                            .map_err(|_| ConfigError::InvalidSurface {
                                bot_instance_id: surface.bot_instance_id.clone(),
                            })
                    }
                }
            })
            .collect()
    }

    pub fn credentials(&self) -> Result<TgrcCredentials, ConfigError> {
        TgrcCredentials::load(&self.credential_registry).map_err(ConfigError::Credentials)
    }

    pub fn token_for(
        &self,
        bot: &BotConfig,
        registry: &TgrcCredentials,
    ) -> Result<BotToken, ConfigError> {
        if let Some(token) = registry.get(&bot.credential_key) {
            return Ok(token.clone());
        }
        if bot.capability == BotCapability::Control {
            let files = CredentialFiles::discover(
                self.legacy_control_token
                    .parent()
                    .unwrap_or_else(|| Path::new(".")),
            );
            return files
                .read_path(bot.capability.credential_role(), &self.legacy_control_token)
                .map_err(ConfigError::Credentials);
        }
        Err(ConfigError::MissingCredentialKey {
            instance_id: bot.instance_id.clone(),
        })
    }

    fn normalize(&mut self, base: &Path) -> Result<(), ConfigError> {
        if self.api_base.trim().is_empty() || !self.api_base.starts_with("https://") {
            return Err(ConfigError::InsecureApiBase);
        }
        if self.request_timeout_seconds == 0 || self.request_timeout_seconds > 300 {
            return Err(ConfigError::InvalidTimeout);
        }
        if !self.metrics_bind.starts_with("127.0.0.1:") {
            return Err(ConfigError::MetricsMustBeLoopback);
        }
        if self.credential_registry.is_relative() {
            self.credential_registry = base.join(&self.credential_registry);
        }
        if self.lock_directory.is_relative() {
            self.lock_directory = base.join(&self.lock_directory);
        }
        if self.legacy_control_token.is_relative() {
            self.legacy_control_token = base.join(&self.legacy_control_token);
        }
        if self
            .bots
            .iter()
            .any(|bot| bot.instance_id.trim().is_empty())
        {
            return Err(ConfigError::InvalidBot {
                instance_id: "<empty>".into(),
            });
        }
        Ok(())
    }
}

fn default_enabled() -> bool {
    true
}

fn default_api_base() -> String {
    "https://api.telegram.org".into()
}

fn default_metrics_bind() -> String {
    "127.0.0.1:9465".into()
}

fn default_request_timeout_seconds() -> u64 {
    30
}

fn home_directory() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("USERPROFILE").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("."))
}

fn default_registry_path() -> PathBuf {
    home_directory().join(".tgrc")
}

fn default_legacy_control_token() -> PathBuf {
    home_directory().join(".config/codex-telegram-bridge/telegram_9527_bot_token")
}

fn default_lock_directory() -> PathBuf {
    home_directory().join(".local/state/codex-telegram-bridge/rust-vnext/leases")
}

fn default_config_path() -> PathBuf {
    home_directory().join(".config/codex-telegram-bridge/rust-vnext.toml")
}

fn default_bots() -> Vec<BotConfig> {
    vec![
        BotConfig {
            instance_id: "control".into(),
            capability: BotCapability::Control,
            credential_key: "rust_9527_bot_key".into(),
            update_consumer: "rust-vnext-control-outbound".into(),
            enabled: true,
        },
        BotConfig {
            instance_id: "discussion".into(),
            capability: BotCapability::Discussion,
            credential_key: "rust_91_bot_key".into(),
            update_consumer: "rust-vnext-discussion".into(),
            enabled: true,
        },
        BotConfig {
            instance_id: "status".into(),
            capability: BotCapability::Status,
            credential_key: "rust_818_bot_key".into(),
            update_consumer: "rust-vnext-status".into(),
            enabled: true,
        },
        BotConfig {
            instance_id: "production-alert".into(),
            capability: BotCapability::ProductionAlert,
            credential_key: "rust_826_bot_key".into(),
            update_consumer: "rust-vnext-production-alert".into(),
            enabled: true,
        },
        BotConfig {
            instance_id: "canary-alert".into(),
            capability: BotCapability::CanaryAlert,
            credential_key: "rust_411_bot_key".into(),
            update_consumer: "rust-vnext-canary-alert".into(),
            enabled: true,
        },
        BotConfig {
            instance_id: "approval".into(),
            capability: BotCapability::Approval,
            credential_key: "rust_69_bot_key".into(),
            update_consumer: "rust-vnext-approval".into(),
            enabled: false,
        },
        BotConfig {
            instance_id: "artifact".into(),
            capability: BotCapability::Artifact,
            credential_key: "rust_426_bot_key".into(),
            update_consumer: "rust-vnext-artifact".into(),
            enabled: false,
        },
    ]
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Rust vNext config is unreadable: {path}")]
    UnreadableConfig { path: PathBuf },
    #[error("Rust vNext config is invalid: {path}")]
    InvalidConfig { path: PathBuf },
    #[error("Rust vNext config template could not be written")]
    InvalidTemplate,
    #[error("Telegram API base must use HTTPS")]
    InsecureApiBase,
    #[error("metrics listener must bind to loopback")]
    MetricsMustBeLoopback,
    #[error("request timeout must be between 1 and 300 seconds")]
    InvalidTimeout,
    #[error("invalid bot instance: {instance_id}")]
    InvalidBot { instance_id: String },
    #[error("invalid Telegram surface for bot instance: {bot_instance_id}")]
    InvalidSurface { bot_instance_id: String },
    #[error("credential key is missing for bot instance: {instance_id}")]
    MissingCredentialKey { instance_id: String },
    #[error(transparent)]
    Credentials(#[from] CredentialError),
}

impl serde::Serialize for RustConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("RustConfig", 8)?;
        state.serialize_field("api_base", &self.api_base)?;
        state.serialize_field("credential_registry", &self.credential_registry)?;
        state.serialize_field("legacy_control_token", &self.legacy_control_token)?;
        state.serialize_field("lock_directory", &self.lock_directory)?;
        state.serialize_field("metrics_bind", &self.metrics_bind)?;
        state.serialize_field("request_timeout_seconds", &self.request_timeout_seconds)?;
        state.serialize_field(
            "bots",
            &self
                .bots
                .iter()
                .map(BotConfigView::from)
                .collect::<Vec<_>>(),
        )?;
        state.serialize_field("surfaces", &self.surfaces)?;
        state.end()
    }
}

#[derive(serde::Serialize)]
struct BotConfigView<'a> {
    instance_id: &'a str,
    capability: BotCapability,
    credential_key: &'a str,
    update_consumer: &'a str,
    enabled: bool,
}

impl<'a> From<&'a BotConfig> for BotConfigView<'a> {
    fn from(value: &'a BotConfig) -> Self {
        Self {
            instance_id: &value.instance_id,
            capability: value.capability,
            credential_key: &value.credential_key,
            update_consumer: &value.update_consumer,
            enabled: value.enabled,
        }
    }
}

impl serde::Serialize for SurfaceConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("SurfaceConfig", 4)?;
        state.serialize_field(
            "kind",
            match self.kind {
                SurfaceKind::Channel => "channel",
                SurfaceKind::ForumTopic => "forum_topic",
            },
        )?;
        state.serialize_field("bot_instance_id", &self.bot_instance_id)?;
        state.serialize_field("chat_id", &self.chat_id)?;
        state.serialize_field("message_thread_id", &self.message_thread_id)?;
        state.end()
    }
}
