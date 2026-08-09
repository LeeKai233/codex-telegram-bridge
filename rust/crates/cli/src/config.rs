//! Declarative Rust vNext topology. Bot identities are configuration data;
//! the adapter never derives behaviour from Telegram numeric IDs.

use codex_telegram_adapter::{
    BotCapability, BotInstanceBinding, ChannelBinding, ForumTopicBinding, NativeCommentBinding,
    TelegramSurfaceBinding,
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
    #[serde(default = "default_state_directory")]
    pub state_directory: PathBuf,
    #[serde(default = "default_codex_socket")]
    pub codex_socket: PathBuf,
    #[serde(default = "default_workspace_root")]
    pub workspace_root: PathBuf,
    #[serde(default = "default_metrics_bind")]
    pub metrics_bind: String,
    #[serde(default = "default_alert_webhook_bind")]
    pub alert_webhook_bind: String,
    #[serde(default = "default_alert_chat_id")]
    pub alert_chat_id: i64,
    #[serde(default = "default_totp_secret_path")]
    pub totp_secret_path: PathBuf,
    #[serde(default = "default_totp_unlock_seconds")]
    pub totp_unlock_seconds: u64,
    #[serde(default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
    #[serde(default = "default_poll_timeout_seconds")]
    pub poll_timeout_seconds: u16,
    #[serde(default = "default_max_backlog")]
    pub max_backlog: usize,
    #[serde(default = "default_ask_model")]
    pub ask_model: String,
    #[serde(default = "default_ask_reasoning_effort")]
    pub ask_reasoning_effort: String,
    #[serde(default)]
    pub poll_updates: bool,
    #[serde(default = "default_channel_chat_id")]
    pub channel_chat_id: i64,
    #[serde(default = "default_discussion_chat_id")]
    pub discussion_chat_id: i64,
    #[serde(default = "default_control_chat_id")]
    pub control_chat_id: i64,
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
    #[serde(default)]
    pub discussion_chat_id: Option<i64>,
    #[serde(default)]
    pub root_message_id: Option<i64>,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SurfaceKind {
    Channel,
    ForumTopic,
    NativeComment,
}

impl Default for RustConfig {
    fn default() -> Self {
        Self {
            api_base: default_api_base(),
            credential_registry: default_registry_path(),
            legacy_control_token: default_legacy_control_token(),
            lock_directory: default_lock_directory(),
            state_directory: default_state_directory(),
            codex_socket: default_codex_socket(),
            workspace_root: default_workspace_root(),
            metrics_bind: default_metrics_bind(),
            alert_webhook_bind: default_alert_webhook_bind(),
            alert_chat_id: default_alert_chat_id(),
            totp_secret_path: default_totp_secret_path(),
            totp_unlock_seconds: default_totp_unlock_seconds(),
            request_timeout_seconds: default_request_timeout_seconds(),
            poll_timeout_seconds: default_poll_timeout_seconds(),
            max_backlog: default_max_backlog(),
            ask_model: default_ask_model(),
            ask_reasoning_effort: default_ask_reasoning_effort(),
            poll_updates: false,
            channel_chat_id: default_channel_chat_id(),
            discussion_chat_id: default_discussion_chat_id(),
            control_chat_id: default_control_chat_id(),
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
                    SurfaceKind::NativeComment => {
                        let discussion_chat_id = surface.discussion_chat_id.ok_or_else(|| {
                            ConfigError::InvalidSurface {
                                bot_instance_id: surface.bot_instance_id.clone(),
                            }
                        })?;
                        let root_message_id =
                            surface
                                .root_message_id
                                .ok_or_else(|| ConfigError::InvalidSurface {
                                    bot_instance_id: surface.bot_instance_id.clone(),
                                })?;
                        NativeCommentBinding::new(
                            channel,
                            discussion_chat_id.to_string(),
                            root_message_id,
                        )
                        .map(TelegramSurfaceBinding::NativeCommentRoot)
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
        if self.poll_timeout_seconds == 0 || self.poll_timeout_seconds > 50 {
            return Err(ConfigError::InvalidPollTimeout);
        }
        if self.max_backlog == 0 || self.max_backlog > 1000 {
            return Err(ConfigError::InvalidBacklog);
        }
        if self.ask_model.trim().is_empty() || self.ask_reasoning_effort.trim().is_empty() {
            return Err(ConfigError::InvalidAskProfile);
        }
        if !self.metrics_bind.starts_with("127.0.0.1:") {
            return Err(ConfigError::MetricsMustBeLoopback);
        }
        if !self.alert_webhook_bind.starts_with("127.0.0.1:") {
            return Err(ConfigError::AlertWebhookMustBeLoopback);
        }
        if self.alert_chat_id == 0 {
            return Err(ConfigError::InvalidAlertChat);
        }
        if self.totp_unlock_seconds == 0 || self.totp_unlock_seconds > 86_400 {
            return Err(ConfigError::InvalidTotpUnlockSeconds);
        }
        if self.credential_registry.is_relative() {
            self.credential_registry = base.join(&self.credential_registry);
        }
        if self.lock_directory.is_relative() {
            self.lock_directory = base.join(&self.lock_directory);
        }
        if self.state_directory.is_relative() {
            self.state_directory = base.join(&self.state_directory);
        }
        if self.codex_socket.is_relative() {
            self.codex_socket = base.join(&self.codex_socket);
        }
        if self.workspace_root.is_relative() {
            self.workspace_root = base.join(&self.workspace_root);
        }
        if self.totp_secret_path.is_relative() {
            self.totp_secret_path = base.join(&self.totp_secret_path);
        }
        if self.channel_chat_id >= 0 || self.discussion_chat_id >= 0 || self.control_chat_id == 0 {
            return Err(ConfigError::InvalidTopology);
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

fn default_alert_webhook_bind() -> String {
    "127.0.0.1:18091".into()
}

fn default_poll_timeout_seconds() -> u16 {
    30
}

fn default_max_backlog() -> usize {
    100
}

fn default_ask_model() -> String {
    "gpt-5.6-terra".into()
}

fn default_ask_reasoning_effort() -> String {
    "medium".into()
}

fn default_channel_chat_id() -> i64 {
    -1004446000549
}

fn default_discussion_chat_id() -> i64 {
    -1004290500369
}

fn default_control_chat_id() -> i64 {
    default_discussion_chat_id()
}

fn default_alert_chat_id() -> i64 {
    default_control_chat_id()
}

fn default_totp_secret_path() -> PathBuf {
    home_directory().join(".config/codex-telegram-bridge/totp_secret")
}

fn default_totp_unlock_seconds() -> u64 {
    1800
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

fn default_state_directory() -> PathBuf {
    home_directory().join(".local/state/codex-telegram-bridge/rust-vnext-full")
}

fn default_codex_socket() -> PathBuf {
    let codex_home = env::var_os("CODEX_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| home_directory().join(".codex"));
    codex_home.join("app-server-control/app-server-control.sock")
}

fn default_workspace_root() -> PathBuf {
    home_directory()
}

fn default_config_path() -> PathBuf {
    home_directory().join(".config/codex-telegram-bridge/rust-vnext.toml")
}

fn default_bots() -> Vec<BotConfig> {
    vec![
        BotConfig {
            instance_id: "control".into(),
            capability: BotCapability::Control,
            credential_key: "rust_91_bot_key".into(),
            update_consumer: "rust-full-control".into(),
            enabled: true,
        },
        BotConfig {
            instance_id: "status".into(),
            capability: BotCapability::Status,
            credential_key: "rust_818_bot_key".into(),
            update_consumer: "rust-full-status".into(),
            enabled: true,
        },
        BotConfig {
            instance_id: "discussion".into(),
            capability: BotCapability::Discussion,
            credential_key: "rust_411_bot_key".into(),
            update_consumer: "rust-full-discussion".into(),
            enabled: true,
        },
        BotConfig {
            instance_id: "monitoring".into(),
            capability: BotCapability::ProductionAlert,
            credential_key: "rust_826_bot_key".into(),
            update_consumer: "rust-full-monitoring-send-only".into(),
            enabled: true,
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
    #[error("Alertmanager webhook must bind to loopback")]
    AlertWebhookMustBeLoopback,
    #[error("alert chat id must be nonzero")]
    InvalidAlertChat,
    #[error("TOTP unlock duration must be between 1 and 86400 seconds")]
    InvalidTotpUnlockSeconds,
    #[error("request timeout must be between 1 and 300 seconds")]
    InvalidTimeout,
    #[error("poll timeout must be between 1 and 50 seconds")]
    InvalidPollTimeout,
    #[error("poll backlog must be between 1 and 1000 updates")]
    InvalidBacklog,
    #[error("ask model and reasoning effort must not be empty")]
    InvalidAskProfile,
    #[error("channel and discussion chat ids must be negative; control chat id must be nonzero")]
    InvalidTopology,
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
        let mut state = serializer.serialize_struct("RustConfig", 23)?;
        state.serialize_field("api_base", &self.api_base)?;
        state.serialize_field("credential_registry", &self.credential_registry)?;
        state.serialize_field("legacy_control_token", &self.legacy_control_token)?;
        state.serialize_field("lock_directory", &self.lock_directory)?;
        state.serialize_field("state_directory", &self.state_directory)?;
        state.serialize_field("codex_socket", &self.codex_socket)?;
        state.serialize_field("workspace_root", &self.workspace_root)?;
        state.serialize_field("metrics_bind", &self.metrics_bind)?;
        state.serialize_field("alert_webhook_bind", &self.alert_webhook_bind)?;
        state.serialize_field("alert_chat_id", &self.alert_chat_id)?;
        state.serialize_field("totp_secret_path", &self.totp_secret_path)?;
        state.serialize_field("totp_unlock_seconds", &self.totp_unlock_seconds)?;
        state.serialize_field("request_timeout_seconds", &self.request_timeout_seconds)?;
        state.serialize_field("poll_timeout_seconds", &self.poll_timeout_seconds)?;
        state.serialize_field("max_backlog", &self.max_backlog)?;
        state.serialize_field("ask_model", &self.ask_model)?;
        state.serialize_field("ask_reasoning_effort", &self.ask_reasoning_effort)?;
        state.serialize_field("poll_updates", &self.poll_updates)?;
        state.serialize_field("channel_chat_id", &self.channel_chat_id)?;
        state.serialize_field("discussion_chat_id", &self.discussion_chat_id)?;
        state.serialize_field("control_chat_id", &self.control_chat_id)?;
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
                SurfaceKind::NativeComment => "native_comment",
            },
        )?;
        state.serialize_field("bot_instance_id", &self.bot_instance_id)?;
        state.serialize_field("chat_id", &self.chat_id)?;
        state.serialize_field("message_thread_id", &self.message_thread_id)?;
        state.serialize_field("discussion_chat_id", &self.discussion_chat_id)?;
        state.serialize_field("root_message_id", &self.root_message_id)?;
        state.end()
    }
}
