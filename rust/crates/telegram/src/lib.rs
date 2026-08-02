//! Telegram Bot API bindings that keep bot roles, update ownership, and chat surfaces explicit.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use codex_telegram_credentials::{BotToken, CredentialRole};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub mod controllers;

pub const ALLOWED_UPDATES: &[&str] = &[
    "message",
    "edited_message",
    "callback_query",
    "my_chat_member",
];

/// The four production identities have deliberately different update duties.
/// The alert bot is send-only: acquiring a polling lease for it is a bug.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeBotRole {
    Control,
    Status,
    Discussion,
    Alert,
}

impl RuntimeBotRole {
    pub const fn polls_updates(self) -> bool {
        !matches!(self, Self::Alert)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LinkedDiscussion {
    pub channel_chat_id: i64,
    pub discussion_chat_id: i64,
}

impl LinkedDiscussion {
    pub fn new(channel_chat_id: i64, discussion_chat_id: i64) -> Result<Self, RoutingError> {
        if channel_chat_id >= 0 || discussion_chat_id >= 0 {
            return Err(RoutingError::InvalidTopology);
        }
        Ok(Self {
            channel_chat_id,
            discussion_chat_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateRoutingPolicy {
    pub control_owner_chat_id: i64,
    pub linked_discussion: LinkedDiscussion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramMessage {
    pub chat_id: i64,
    pub message_id: i64,
    pub text: Option<String>,
    pub reply_to_message_id: Option<i64>,
    /// The linked-discussion automatic forward from a channel post. Telegram's
    /// `is_automatic_forward` is the source of truth; forum topics are optional.
    pub automatic_forward_from_channel: Option<i64>,
    pub automatic_forward_channel_post_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramCallback {
    pub id: String,
    pub chat_id: i64,
    pub message_id: i64,
    pub data: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncomingUpdate {
    Message(TelegramMessage),
    EditedMessage(TelegramMessage),
    Callback(TelegramCallback),
    Membership,
    Unsupported,
}

impl IncomingUpdate {
    pub fn from_update(update: &Update) -> Self {
        if let Some(message) = update.payload.get("message").and_then(parse_message) {
            return Self::Message(message);
        }
        if let Some(message) = update.payload.get("edited_message").and_then(parse_message) {
            return Self::EditedMessage(message);
        }
        if let Some(callback) = update
            .payload
            .get("callback_query")
            .and_then(parse_callback)
        {
            return Self::Callback(callback);
        }
        if update.payload.get("my_chat_member").is_some() {
            return Self::Membership;
        }
        Self::Unsupported
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkflowCommand {
    New,
    Totp,
    Lock,
    Status,
    Perf,
    Help,
    Sessions,
    Topics,
    PlanMode,
    ChangeModel,
    GetFile,
    Review,
    Cancel,
    Unknown(String),
}

impl WorkflowCommand {
    pub fn parse(text: &str) -> Option<Self> {
        let command = text.split_whitespace().next()?.strip_prefix('/')?;
        let command = command
            .split('@')
            .next()
            .unwrap_or(command)
            .to_ascii_lowercase();
        Some(match command.as_str() {
            "new" => Self::New,
            "totp" => Self::Totp,
            "lock" => Self::Lock,
            "status" => Self::Status,
            "perf" => Self::Perf,
            "help" | "start" => Self::Help,
            "sessions" => Self::Sessions,
            "topics" => Self::Topics,
            "planmode" => Self::PlanMode,
            "changemodel" => Self::ChangeModel,
            "getfile" => Self::GetFile,
            "review" => Self::Review,
            "cancel" => Self::Cancel,
            other => Self::Unknown(other.to_owned()),
        })
    }

    /// Keep the Python bridge's role-local command surface intact. A command
    /// may be understood by the shared parser while still being ignored by a
    /// bot that does not own that business surface.
    pub const fn allowed_for_role(&self, role: RuntimeBotRole) -> bool {
        match role {
            RuntimeBotRole::Control => matches!(
                self,
                Self::New | Self::Perf | Self::Help | Self::Sessions | Self::Topics
            ),
            RuntimeBotRole::Discussion => matches!(
                self,
                Self::Status
                    | Self::Totp
                    | Self::Lock
                    | Self::PlanMode
                    | Self::ChangeModel
                    | Self::GetFile
                    | Self::Review
                    | Self::Cancel
                    | Self::Help
            ),
            RuntimeBotRole::Status | RuntimeBotRole::Alert => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkflowAction {
    Command {
        command: WorkflowCommand,
        chat_id: i64,
        message_id: i64,
        text: String,
    },
    Prompt {
        chat_id: i64,
        message_id: i64,
        text: String,
        root_message_id: Option<i64>,
    },
    Callback(TelegramCallback),
    NativeCommentPost {
        channel_chat_id: i64,
        channel_post_id: i64,
        discussion_chat_id: i64,
        discussion_root_message_id: i64,
    },
    MembershipChanged,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutedUpdate {
    Dispatch(WorkflowAction),
    Ignore,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutingError {
    InvalidTopology,
    AlertBotMustNotPoll,
}

impl fmt::Display for RoutingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTopology => {
                formatter.write_str("linked channel and discussion must be Telegram supergroup ids")
            }
            Self::AlertBotMustNotPoll => {
                formatter.write_str("the alert bot is send-only and must not poll updates")
            }
        }
    }
}

impl std::error::Error for RoutingError {}

pub struct UpdateRouter {
    role: RuntimeBotRole,
    policy: UpdateRoutingPolicy,
}

impl UpdateRouter {
    pub fn new(role: RuntimeBotRole, policy: UpdateRoutingPolicy) -> Result<Self, RoutingError> {
        if !role.polls_updates() {
            return Err(RoutingError::AlertBotMustNotPoll);
        }
        Ok(Self { role, policy })
    }

    pub fn route(&self, update: &Update) -> RoutedUpdate {
        match (self.role, IncomingUpdate::from_update(update)) {
            (RuntimeBotRole::Control, IncomingUpdate::Message(message))
                if message.chat_id == self.policy.control_owner_chat_id =>
            {
                self.route_message(message, None)
            }
            (RuntimeBotRole::Control, IncomingUpdate::Callback(callback))
                if callback.chat_id == self.policy.control_owner_chat_id =>
            {
                RoutedUpdate::Dispatch(WorkflowAction::Callback(callback))
            }
            (RuntimeBotRole::Status, IncomingUpdate::Callback(callback))
                if callback.chat_id == self.policy.linked_discussion.discussion_chat_id =>
            {
                RoutedUpdate::Dispatch(WorkflowAction::Callback(callback))
            }
            (RuntimeBotRole::Discussion, IncomingUpdate::Message(message))
                if message.chat_id == self.policy.linked_discussion.discussion_chat_id =>
            {
                if message.automatic_forward_from_channel
                    == Some(self.policy.linked_discussion.channel_chat_id)
                {
                    RoutedUpdate::Dispatch(WorkflowAction::NativeCommentPost {
                        channel_chat_id: self.policy.linked_discussion.channel_chat_id,
                        channel_post_id: message
                            .automatic_forward_channel_post_id
                            .unwrap_or(message.message_id),
                        discussion_chat_id: message.chat_id,
                        discussion_root_message_id: message.message_id,
                    })
                } else {
                    self.route_message(message.clone(), message.reply_to_message_id)
                }
            }
            (RuntimeBotRole::Discussion, IncomingUpdate::Callback(callback))
                if callback.chat_id == self.policy.linked_discussion.discussion_chat_id =>
            {
                RoutedUpdate::Dispatch(WorkflowAction::Callback(callback))
            }
            (_, IncomingUpdate::Membership) => {
                RoutedUpdate::Dispatch(WorkflowAction::MembershipChanged)
            }
            _ => RoutedUpdate::Ignore,
        }
    }

    fn route_message(
        &self,
        message: TelegramMessage,
        root_message_id: Option<i64>,
    ) -> RoutedUpdate {
        let text = match message.text {
            Some(text) if !text.trim().is_empty() => text,
            _ => return RoutedUpdate::Ignore,
        };
        if let Some(command) = WorkflowCommand::parse(&text) {
            if !command.allowed_for_role(self.role) {
                return RoutedUpdate::Ignore;
            }
            RoutedUpdate::Dispatch(WorkflowAction::Command {
                command,
                chat_id: message.chat_id,
                message_id: message.message_id,
                text,
            })
        } else {
            RoutedUpdate::Dispatch(WorkflowAction::Prompt {
                chat_id: message.chat_id,
                message_id: message.message_id,
                text,
                root_message_id,
            })
        }
    }
}

fn parse_message(value: &Value) -> Option<TelegramMessage> {
    let chat_id = value.pointer("/chat/id")?.as_i64()?;
    let message_id = value.get("message_id")?.as_i64()?;
    let automatic_forward_from_channel = value
        .get("is_automatic_forward")
        .filter(|flag| flag.as_bool() == Some(true))
        .and_then(|_| {
            value
                .pointer("/sender_chat/id")
                .or_else(|| value.pointer("/forward_from_chat/id"))
                .and_then(Value::as_i64)
        });
    Some(TelegramMessage {
        chat_id,
        message_id,
        text: value.get("text").and_then(Value::as_str).map(str::to_owned),
        reply_to_message_id: value
            .pointer("/reply_to_message/message_id")
            .and_then(Value::as_i64),
        automatic_forward_from_channel,
        automatic_forward_channel_post_id: value
            .get("is_automatic_forward")
            .filter(|flag| flag.as_bool() == Some(true))
            .and_then(|_| value.get("forward_from_message_id"))
            .and_then(Value::as_i64),
    })
}

fn parse_callback(value: &Value) -> Option<TelegramCallback> {
    Some(TelegramCallback {
        id: value.get("id")?.as_str()?.to_owned(),
        chat_id: value.pointer("/message/chat/id")?.as_i64()?,
        message_id: value.pointer("/message/message_id")?.as_i64()?,
        data: value.get("data")?.as_str()?.to_owned(),
    })
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BotCapability {
    Control,
    Discussion,
    Status,
    ProductionAlert,
    CanaryAlert,
    Approval,
    Artifact,
}

impl BotCapability {
    pub const fn credential_role(self) -> CredentialRole {
        match self {
            Self::Control => CredentialRole::Control,
            Self::Discussion => CredentialRole::Discussion,
            Self::Status => CredentialRole::Status,
            Self::ProductionAlert => CredentialRole::ProductionAlert,
            Self::CanaryAlert => CredentialRole::CanaryAlert,
            Self::Approval => CredentialRole::Approval,
            Self::Artifact => CredentialRole::Artifact,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BotInstanceBinding {
    pub instance_id: String,
    pub capability: BotCapability,
    pub credential_role: CredentialRole,
    pub update_consumer: Option<UpdateConsumerId>,
}

impl BotInstanceBinding {
    pub fn new(instance_id: impl Into<String>, capability: BotCapability) -> Self {
        Self {
            instance_id: instance_id.into(),
            credential_role: capability.credential_role(),
            capability,
            update_consumer: None,
        }
    }

    pub fn with_update_consumer(
        mut self,
        consumer: impl Into<String>,
    ) -> Result<Self, BindingIssue> {
        self.update_consumer = Some(UpdateConsumerId::parse(consumer)?);
        Ok(self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct UpdateConsumerId(String);

impl UpdateConsumerId {
    pub fn parse(value: impl Into<String>) -> Result<Self, BindingIssue> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(BindingIssue::new(
                "empty-update-consumer",
                "update consumer cannot be empty",
            ));
        }
        Ok(Self(value))
    }
}

impl fmt::Display for UpdateConsumerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChannelBinding {
    pub bot_instance_id: String,
    pub chat_id: String,
}

impl ChannelBinding {
    pub fn new(
        bot_instance_id: impl Into<String>,
        chat_id: impl Into<String>,
    ) -> Result<Self, BindingIssue> {
        let binding = Self {
            bot_instance_id: bot_instance_id.into(),
            chat_id: chat_id.into(),
        };
        if binding.bot_instance_id.trim().is_empty() {
            return Err(BindingIssue::new(
                "empty-bot-instance",
                "channel binding needs a bot instance id",
            ));
        }
        if binding.chat_id.trim().is_empty() {
            return Err(BindingIssue::new(
                "empty-chat-id",
                "channel binding needs a chat id",
            ));
        }
        Ok(binding)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForumTopicBinding {
    pub channel: ChannelBinding,
    pub message_thread_id: i64,
}

impl ForumTopicBinding {
    pub fn new(channel: ChannelBinding, message_thread_id: i64) -> Result<Self, BindingIssue> {
        if message_thread_id <= 0 {
            return Err(BindingIssue::new(
                "invalid-message-thread-id",
                "forum topic needs a positive message thread id",
            ));
        }
        Ok(Self {
            channel,
            message_thread_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeCommentBinding {
    pub channel: ChannelBinding,
    pub discussion_chat_id: String,
    pub root_message_id: i64,
}

impl NativeCommentBinding {
    pub fn new(
        channel: ChannelBinding,
        discussion_chat_id: impl Into<String>,
        root_message_id: i64,
    ) -> Result<Self, BindingIssue> {
        let discussion_chat_id = discussion_chat_id.into();
        if discussion_chat_id.trim().is_empty() || root_message_id <= 0 {
            return Err(BindingIssue::new(
                "invalid-native-comment-root",
                "native comment binding needs a discussion chat and positive root message id",
            ));
        }
        Ok(Self {
            channel,
            discussion_chat_id,
            root_message_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TelegramSurfaceBinding {
    Channel(ChannelBinding),
    ForumTopic(ForumTopicBinding),
    NativeCommentRoot(NativeCommentBinding),
}

impl TelegramSurfaceBinding {
    pub fn bot_instance_id(&self) -> &str {
        match self {
            Self::Channel(channel) => &channel.bot_instance_id,
            Self::ForumTopic(topic) => &topic.channel.bot_instance_id,
            Self::NativeCommentRoot(comment) => &comment.channel.bot_instance_id,
        }
    }

    fn send_message_payload(&self, text: &str) -> Value {
        match self {
            Self::Channel(channel) => json!({ "chat_id": channel.chat_id, "text": text }),
            Self::ForumTopic(topic) => json!({
                "chat_id": topic.channel.chat_id,
                "message_thread_id": topic.message_thread_id,
                "text": text,
            }),
            Self::NativeCommentRoot(comment) => json!({
                "chat_id": comment.discussion_chat_id,
                "text": text,
                "reply_parameters": {
                    "message_id": comment.root_message_id,
                    "allow_sending_without_reply": true,
                },
            }),
        }
    }

    fn document_fields(&self) -> Vec<(String, String)> {
        match self {
            Self::Channel(channel) => vec![("chat_id".into(), channel.chat_id.clone())],
            Self::ForumTopic(topic) => vec![
                ("chat_id".into(), topic.channel.chat_id.clone()),
                (
                    "message_thread_id".into(),
                    topic.message_thread_id.to_string(),
                ),
            ],
            Self::NativeCommentRoot(comment) => vec![
                ("chat_id".into(), comment.discussion_chat_id.clone()),
                (
                    "reply_parameters".into(),
                    json!({
                        "message_id": comment.root_message_id,
                        "allow_sending_without_reply": true,
                    })
                    .to_string(),
                ),
            ],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BindingIssue {
    pub code: &'static str,
    pub message: &'static str,
}

impl BindingIssue {
    const fn new(code: &'static str, message: &'static str) -> Self {
        Self { code, message }
    }
}

pub fn validate_bindings(bindings: &[BotInstanceBinding]) -> Vec<BindingIssue> {
    let mut issues = Vec::new();
    let mut instance_ids = BTreeSet::new();
    let mut capabilities = BTreeSet::new();
    for binding in bindings {
        if binding.instance_id.trim().is_empty() {
            issues.push(BindingIssue::new(
                "empty-bot-instance",
                "bot instance id cannot be empty",
            ));
        } else if !instance_ids.insert(binding.instance_id.as_str()) {
            issues.push(BindingIssue::new(
                "duplicate-bot-instance",
                "bot instance ids must be unique",
            ));
        }
        if !capabilities.insert(binding.capability) {
            issues.push(BindingIssue::new(
                "duplicate-capability",
                "each bot capability can have one instance",
            ));
        }
        if binding.credential_role != binding.capability.credential_role() {
            issues.push(BindingIssue::new(
                "credential-role-mismatch",
                "bot capability must use its matching credential role",
            ));
        }
    }
    issues
}

#[derive(Default, Clone)]
pub struct TokenLeaseRegistry {
    active: Arc<Mutex<HashMap<BotToken, UpdateConsumerId>>>,
}

impl TokenLeaseRegistry {
    pub fn acquire(
        &self,
        token: &BotToken,
        consumer: UpdateConsumerId,
    ) -> Result<TokenLease, TokenLeaseError> {
        let mut active = self
            .active
            .lock()
            .expect("token lease registry mutex poisoned");
        if let Some(owner) = active.get(token) {
            return Err(TokenLeaseError::AlreadyLeased {
                requested: consumer,
                active: owner.clone(),
            });
        }
        active.insert(token.clone(), consumer.clone());
        Ok(TokenLease {
            token: token.clone(),
            consumer,
            registry: self.active.clone(),
            process_lock: None,
        })
    }
}

pub struct TokenLease {
    token: BotToken,
    consumer: UpdateConsumerId,
    registry: Arc<Mutex<HashMap<BotToken, UpdateConsumerId>>>,
    process_lock: Option<TokenLeaseLock>,
}

impl TokenLease {
    pub fn consumer(&self) -> &UpdateConsumerId {
        &self.consumer
    }

    pub fn process_lock_path(&self) -> Option<&Path> {
        self.process_lock.as_ref().map(TokenLeaseLock::path)
    }

    fn token(&self) -> &BotToken {
        &self.token
    }
}

impl Drop for TokenLease {
    fn drop(&mut self) {
        let mut active = self
            .registry
            .lock()
            .expect("token lease registry mutex poisoned");
        if active.get(&self.token) == Some(&self.consumer) {
            active.remove(&self.token);
        }
    }
}

impl TokenLeaseRegistry {
    /// Acquire both the in-process lease and a cross-process advisory lock.
    /// The lock filename contains only a SHA-256 digest, never the token.
    pub fn acquire_with_lock(
        &self,
        token: &BotToken,
        consumer: UpdateConsumerId,
        lock_directory: impl AsRef<Path>,
    ) -> Result<TokenLease, TokenLeaseError> {
        let lease = self.acquire(token, consumer)?;
        match TokenLeaseLock::acquire(lock_directory, token, lease.consumer.clone()) {
            Ok(process_lock) => {
                let mut lease = lease;
                lease.process_lock = Some(process_lock);
                Ok(lease)
            }
            Err(error) => {
                drop(lease);
                Err(error)
            }
        }
    }
}

/// A portable advisory lock that prevents two bridge processes from polling
/// the same Bot API token. `fs2` maps the lock to `flock` on Unix and the
/// equivalent exclusive file lock on Windows.
pub struct TokenLeaseLock {
    file: File,
    path: PathBuf,
}

impl TokenLeaseLock {
    pub fn acquire(
        directory: impl AsRef<Path>,
        token: &BotToken,
        consumer: UpdateConsumerId,
    ) -> Result<Self, TokenLeaseError> {
        let directory = directory.as_ref();
        fs::create_dir_all(directory).map_err(|_| TokenLeaseError::LockUnavailable)?;
        secure_directory_permissions(directory).map_err(|_| TokenLeaseError::LockUnavailable)?;
        let mut digest = Sha256::new();
        digest.update(token.as_str().as_bytes());
        let digest = format!("{:x}", digest.finalize());
        let path = directory.join(format!("telegram-{digest}.lock"));
        let file = open_lock_file(&path).map_err(|_| TokenLeaseError::LockUnavailable)?;
        secure_file_permissions(&file).map_err(|_| TokenLeaseError::LockUnavailable)?;
        if file.try_lock_exclusive().is_err() {
            return Err(TokenLeaseError::AlreadyLeased {
                requested: consumer,
                active: UpdateConsumerId::parse("external-process")
                    .expect("constant consumer id is valid"),
            });
        }
        let mut owner = format!("pid={} consumer={}\n", std::process::id(), consumer);
        owner.truncate(256);
        file.set_len(0)
            .map_err(|_| TokenLeaseError::LockUnavailable)?;
        (&file)
            .write_all(owner.as_bytes())
            .map_err(|_| TokenLeaseError::LockUnavailable)?;
        Ok(Self { file, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn open_lock_file(path: &Path) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path)
}

impl Drop for TokenLeaseLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(unix)]
fn secure_directory_permissions(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn secure_directory_permissions(_: &Path) -> std::io::Result<()> {
    Ok(())
}

fn secure_file_permissions(file: &File) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TokenLeaseError {
    AlreadyLeased {
        requested: UpdateConsumerId,
        active: UpdateConsumerId,
    },
    LockUnavailable,
}

impl fmt::Display for TokenLeaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyLeased { requested, active } => {
                write!(
                    formatter,
                    "update consumer {requested} cannot acquire a token leased by {active}"
                )
            }
            Self::LockUnavailable => formatter.write_str("token ownership lock is unavailable"),
        }
    }
}

impl std::error::Error for TokenLeaseError {}

pub trait TelegramTransport {
    /// Implementations must use the token only to construct the HTTPS request and must not log it.
    fn post_json(
        &self,
        api_base: &str,
        token: &BotToken,
        method: &'static str,
        payload: Value,
    ) -> Result<String, TelegramTransportError>;

    /// Multipart is optional so test transports and non-file adapters remain
    /// intentionally text-only. Production reqwest enables it for bounded
    /// Telegram document uploads.
    fn post_multipart(
        &self,
        _api_base: &str,
        _token: &BotToken,
        _method: &'static str,
        _fields: Vec<(String, String)>,
        _file_name: String,
        _file_bytes: Vec<u8>,
    ) -> Result<String, TelegramTransportError> {
        Err(TelegramTransportError::new("multipart-unsupported"))
    }
}

/// Production Telegram Bot API transport. The client uses rustls only, a
/// bounded timeout, and maps all network failures to token-free error kinds.
#[derive(Clone)]
pub struct ReqwestTransport {
    client: reqwest::blocking::Client,
}

impl ReqwestTransport {
    pub fn new(timeout: Duration) -> Result<Self, TelegramTransportError> {
        let client = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout.min(Duration::from_secs(10)))
            .https_only(true)
            .build()
            .map_err(|_| TelegramTransportError::new("client-build"))?;
        Ok(Self { client })
    }
}

impl TelegramTransport for ReqwestTransport {
    fn post_json(
        &self,
        api_base: &str,
        token: &BotToken,
        method: &'static str,
        payload: Value,
    ) -> Result<String, TelegramTransportError> {
        let url = format!(
            "{}/bot{}/{}",
            api_base.trim_end_matches('/'),
            token.as_str(),
            method
        );
        let response = self
            .client
            .post(url)
            .json(&payload)
            .send()
            .map_err(|error| {
                if error.is_timeout() {
                    TelegramTransportError::new("timeout")
                } else if error.is_connect() {
                    TelegramTransportError::new("connect")
                } else {
                    TelegramTransportError::new("request")
                }
            })?;
        if !response.status().is_success() {
            return Err(TelegramTransportError::new("http-status"));
        }
        response
            .text()
            .map_err(|_| TelegramTransportError::new("response-body"))
    }

    fn post_multipart(
        &self,
        api_base: &str,
        token: &BotToken,
        method: &'static str,
        fields: Vec<(String, String)>,
        file_name: String,
        file_bytes: Vec<u8>,
    ) -> Result<String, TelegramTransportError> {
        let url = format!(
            "{}/bot{}/{}",
            api_base.trim_end_matches('/'),
            token.as_str(),
            method
        );
        let mut form = reqwest::blocking::multipart::Form::new();
        for (name, value) in fields {
            form = form.text(name, value);
        }
        let part = reqwest::blocking::multipart::Part::bytes(file_bytes).file_name(file_name);
        form = form.part("document", part);
        let response = self
            .client
            .post(url)
            .multipart(form)
            .send()
            .map_err(|error| {
                if error.is_timeout() {
                    TelegramTransportError::new("timeout")
                } else if error.is_connect() {
                    TelegramTransportError::new("connect")
                } else {
                    TelegramTransportError::new("request")
                }
            })?;
        if !response.status().is_success() {
            return Err(TelegramTransportError::new("http-status"));
        }
        response
            .text()
            .map_err(|_| TelegramTransportError::new("response-body"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramTransportError {
    pub kind: &'static str,
}

impl TelegramTransportError {
    pub const fn new(kind: &'static str) -> Self {
        Self { kind }
    }
}

impl fmt::Display for TelegramTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Telegram transport failure: {}", self.kind)
    }
}

impl std::error::Error for TelegramTransportError {}

pub struct TelegramBotApi<T> {
    transport: T,
    api_base: String,
}

impl<T> TelegramBotApi<T>
where
    T: TelegramTransport,
{
    pub fn new(transport: T) -> Self {
        Self::with_api_base(transport, "https://api.telegram.org")
    }

    pub fn with_api_base(transport: T, api_base: impl Into<String>) -> Self {
        Self {
            transport,
            api_base: api_base.into().trim_end_matches('/').to_owned(),
        }
    }

    pub fn get_me(&self, token: &BotToken) -> Result<BotProfile, TelegramError> {
        self.call(token, "getMe", json!({}))
    }

    /// `TokenLease` proves this process is the only active long-poll consumer for the token.
    pub fn get_updates(
        &self,
        lease: &TokenLease,
        offset: Option<i64>,
        timeout_seconds: u16,
    ) -> Result<Vec<Update>, TelegramError> {
        self.call(
            lease.token(),
            "getUpdates",
            json!({
                "offset": offset,
                "timeout": timeout_seconds.min(50),
                "allowed_updates": ALLOWED_UPDATES,
            }),
        )
    }

    pub fn send_text(
        &self,
        token: &BotToken,
        surface: &TelegramSurfaceBinding,
        text: &str,
    ) -> Result<SentMessage, TelegramError> {
        self.send_text_with_markup(token, surface, text, None)
    }

    pub fn send_text_with_markup(
        &self,
        token: &BotToken,
        surface: &TelegramSurfaceBinding,
        text: &str,
        reply_markup: Option<Value>,
    ) -> Result<SentMessage, TelegramError> {
        if text.is_empty() {
            return Err(TelegramError::InvalidInput("message text cannot be empty"));
        }
        let mut payload = surface.send_message_payload(text);
        if let Some(reply_markup) = reply_markup {
            payload["reply_markup"] = reply_markup;
        }
        self.call(token, "sendMessage", payload)
    }

    pub fn send_document(
        &self,
        token: &BotToken,
        surface: &TelegramSurfaceBinding,
        file_name: &str,
        file_bytes: Vec<u8>,
        caption: Option<&str>,
    ) -> Result<SentMessage, TelegramError> {
        if file_name.trim().is_empty() {
            return Err(TelegramError::InvalidInput(
                "document file name cannot be empty",
            ));
        }
        if file_bytes.is_empty() {
            return Err(TelegramError::InvalidInput("document cannot be empty"));
        }
        let mut fields = surface.document_fields();
        if let Some(caption) = caption.filter(|caption| !caption.is_empty()) {
            fields.push(("caption".into(), caption.to_owned()));
        }
        let body = self
            .transport
            .post_multipart(
                &self.api_base,
                token,
                "sendDocument",
                fields,
                file_name.to_owned(),
                file_bytes,
            )
            .map_err(TelegramError::Transport)?;
        parse_api_response(&body, "sendDocument")
    }

    pub fn answer_callback_query(
        &self,
        token: &BotToken,
        callback_query_id: &str,
        text: Option<&str>,
    ) -> Result<bool, TelegramError> {
        if callback_query_id.trim().is_empty() {
            return Err(TelegramError::InvalidInput(
                "callback query id cannot be empty",
            ));
        }
        let mut payload = json!({"callback_query_id": callback_query_id});
        if let Some(text) = text.filter(|text| !text.is_empty()) {
            payload["text"] = Value::String(text.to_owned());
        }
        self.call(token, "answerCallbackQuery", payload)
    }

    pub fn get_chat(&self, token: &BotToken, chat_id: i64) -> Result<ChatInfo, TelegramError> {
        self.call(token, "getChat", json!({ "chat_id": chat_id }))
    }

    pub fn get_chat_member(
        &self,
        token: &BotToken,
        chat_id: i64,
        user_id: i64,
    ) -> Result<ChatMemberInfo, TelegramError> {
        self.call(
            token,
            "getChatMember",
            json!({ "chat_id": chat_id, "user_id": user_id }),
        )
    }

    fn call<R: for<'de> Deserialize<'de>>(
        &self,
        token: &BotToken,
        method: &'static str,
        payload: Value,
    ) -> Result<R, TelegramError> {
        let body = self
            .transport
            .post_json(&self.api_base, token, method, payload)
            .map_err(TelegramError::Transport)?;
        parse_api_response(&body, method)
    }
}

fn parse_api_response<R: for<'de> Deserialize<'de>>(
    body: &str,
    method: &'static str,
) -> Result<R, TelegramError> {
    let response: BotApiResponse<R> =
        serde_json::from_str(body).map_err(|_| TelegramError::InvalidResponse { method })?;
    if response.ok {
        response
            .result
            .ok_or(TelegramError::InvalidResponse { method })
    } else {
        Err(TelegramError::ApiRejected {
            method,
            error_code: response.error_code,
        })
    }
}

#[derive(Deserialize)]
struct BotApiResponse<T> {
    ok: bool,
    result: Option<T>,
    error_code: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct BotProfile {
    pub id: i64,
    pub is_bot: bool,
    pub username: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ChatInfo {
    pub id: i64,
    #[serde(rename = "type")]
    pub chat_type: String,
    #[serde(default)]
    pub is_forum: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ChatMemberInfo {
    pub status: String,
    pub user: BotProfile,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Update {
    pub update_id: i64,
    #[serde(flatten)]
    pub payload: Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SentMessage {
    pub message_id: i64,
}

#[derive(Debug)]
pub enum TelegramError {
    Transport(TelegramTransportError),
    InvalidInput(&'static str),
    InvalidResponse {
        method: &'static str,
    },
    ApiRejected {
        method: &'static str,
        error_code: Option<i64>,
    },
}

impl fmt::Display for TelegramError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => error.fmt(formatter),
            Self::InvalidInput(message) => formatter.write_str(message),
            Self::InvalidResponse { method } => write!(
                formatter,
                "Telegram returned an invalid response to {method}"
            ),
            Self::ApiRejected { method, error_code } => match error_code {
                Some(code) => write!(
                    formatter,
                    "Telegram rejected {method} with error code {code}"
                ),
                None => write!(formatter, "Telegram rejected {method}"),
            },
        }
    }
}

impl std::error::Error for TelegramError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    type MultipartCall = (&'static str, Vec<(String, String)>, String, Vec<u8>);

    #[derive(Clone, Default)]
    struct RecordingTransport {
        calls: Arc<Mutex<Vec<(&'static str, Value)>>>,
        multipart_calls: Arc<Mutex<Vec<MultipartCall>>>,
        response: Arc<Mutex<String>>,
    }

    impl RecordingTransport {
        fn responds_with(body: &str) -> Self {
            Self {
                response: Arc::new(Mutex::new(body.to_owned())),
                ..Self::default()
            }
        }
    }

    impl TelegramTransport for RecordingTransport {
        fn post_json(
            &self,
            _api_base: &str,
            _token: &BotToken,
            method: &'static str,
            payload: Value,
        ) -> Result<String, TelegramTransportError> {
            self.calls.lock().unwrap().push((method, payload));
            Ok(self.response.lock().unwrap().clone())
        }

        fn post_multipart(
            &self,
            _api_base: &str,
            _token: &BotToken,
            method: &'static str,
            fields: Vec<(String, String)>,
            file_name: String,
            file_bytes: Vec<u8>,
        ) -> Result<String, TelegramTransportError> {
            self.multipart_calls
                .lock()
                .unwrap()
                .push((method, fields, file_name, file_bytes));
            Ok(self.response.lock().unwrap().clone())
        }
    }

    fn token() -> BotToken {
        BotToken::parse("123:very-secret-token").unwrap()
    }

    #[test]
    fn only_one_consumer_can_lease_a_token() {
        let registry = TokenLeaseRegistry::default();
        let first = registry
            .acquire(&token(), UpdateConsumerId::parse("bridge-a").unwrap())
            .unwrap();
        let error = match registry.acquire(&token(), UpdateConsumerId::parse("bridge-b").unwrap()) {
            Ok(_) => panic!("a second consumer must not acquire the same token"),
            Err(error) => error,
        };
        assert!(matches!(error, TokenLeaseError::AlreadyLeased { .. }));
        drop(first);
        assert!(
            registry
                .acquire(&token(), UpdateConsumerId::parse("bridge-b").unwrap())
                .is_ok()
        );
    }

    #[test]
    fn polling_always_sends_the_allowed_update_policy() {
        let transport = RecordingTransport::responds_with(r#"{"ok":true,"result":[]}"#);
        let calls = transport.calls.clone();
        let api = TelegramBotApi::new(transport);
        let registry = TokenLeaseRegistry::default();
        let lease = registry
            .acquire(&token(), UpdateConsumerId::parse("bridge").unwrap())
            .unwrap();

        assert_eq!(
            api.get_updates(&lease, Some(42), 90).unwrap(),
            Vec::<Update>::new()
        );
        let calls = calls.lock().unwrap();
        assert_eq!(calls[0].0, "getUpdates");
        assert_eq!(calls[0].1["timeout"], 50);
        assert_eq!(calls[0].1["allowed_updates"], json!(ALLOWED_UPDATES));
    }

    #[test]
    fn forum_topics_include_the_thread_id_when_sending() {
        let transport =
            RecordingTransport::responds_with(r#"{"ok":true,"result":{"message_id":9}}"#);
        let calls = transport.calls.clone();
        let api = TelegramBotApi::new(transport);
        let channel = ChannelBinding::new("discussion", "-100123").unwrap();
        let surface =
            TelegramSurfaceBinding::ForumTopic(ForumTopicBinding::new(channel, 17).unwrap());

        assert_eq!(
            api.send_text(&token(), &surface, "hello")
                .unwrap()
                .message_id,
            9
        );
        let calls = calls.lock().unwrap();
        assert_eq!(calls[0].1["message_thread_id"], 17);
    }

    #[test]
    fn native_comments_reply_to_the_linked_discussion_root_without_thread_id() {
        let transport =
            RecordingTransport::responds_with(r#"{"ok":true,"result":{"message_id":10}}"#);
        let calls = transport.calls.clone();
        let api = TelegramBotApi::new(transport);
        let channel = ChannelBinding::new("discussion", "-1004446000549").unwrap();
        let surface = TelegramSurfaceBinding::NativeCommentRoot(
            NativeCommentBinding::new(channel, "-1004290500369", 700).unwrap(),
        );
        api.send_text(&token(), &surface, "reply").unwrap();
        let payload = &calls.lock().unwrap()[0].1;
        assert_eq!(payload["chat_id"], "-1004290500369");
        assert_eq!(payload["reply_parameters"]["message_id"], 700);
        assert!(payload.get("message_thread_id").is_none());
    }

    #[test]
    fn document_upload_preserves_native_comment_reply_parameters() {
        let transport =
            RecordingTransport::responds_with(r#"{"ok":true,"result":{"message_id":11}}"#);
        let multipart_calls = transport.multipart_calls.clone();
        let api = TelegramBotApi::new(transport);
        let channel = ChannelBinding::new("discussion", "-1004446000549").unwrap();
        let surface = TelegramSurfaceBinding::NativeCommentRoot(
            NativeCommentBinding::new(channel, "-1004290500369", 700).unwrap(),
        );
        assert_eq!(
            api.send_document(
                &token(),
                &surface,
                "report.txt",
                b"hello".to_vec(),
                Some("sha256=x"),
            )
            .unwrap()
            .message_id,
            11
        );
        let calls = multipart_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "sendDocument");
        assert_eq!(calls[0].2, "report.txt");
        assert_eq!(calls[0].3, b"hello");
        assert!(calls[0].1.iter().any(|(name, value)| {
            name == "reply_parameters" && value.contains("\"message_id\":700")
        }));
    }

    #[test]
    fn token_is_not_present_in_adapter_errors() {
        let transport = RecordingTransport::responds_with(
            r#"{"ok":false,"error_code":401,"description":"bad token"}"#,
        );
        let error = TelegramBotApi::new(transport).get_me(&token()).unwrap_err();
        assert!(!error.to_string().contains("very-secret-token"));
        assert!(!format!("{error:?}").contains("very-secret-token"));
    }

    #[test]
    fn cross_process_lock_does_not_include_token_in_path() {
        let directory = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-tmp")
            .join(format!("codex-telegram-lock-{}", std::process::id()));
        let first_registry = TokenLeaseRegistry::default();
        let second_registry = TokenLeaseRegistry::default();
        let first = first_registry
            .acquire_with_lock(
                &token(),
                UpdateConsumerId::parse("first").unwrap(),
                &directory,
            )
            .unwrap();
        let error = match second_registry.acquire_with_lock(
            &token(),
            UpdateConsumerId::parse("second").unwrap(),
            &directory,
        ) {
            Ok(_) => panic!("second process must not acquire the lock"),
            Err(error) => error,
        };
        assert!(matches!(error, TokenLeaseError::AlreadyLeased { .. }));
        assert!(
            !first
                .process_lock_path()
                .unwrap()
                .to_string_lossy()
                .contains("very-secret-token")
        );
        drop(first);
        std::fs::remove_dir_all(directory).unwrap();
    }

    fn policy() -> UpdateRoutingPolicy {
        UpdateRoutingPolicy {
            control_owner_chat_id: 42,
            linked_discussion: LinkedDiscussion::new(-1004446000549, -1004290500369).unwrap(),
        }
    }

    fn update(payload: Value) -> Update {
        Update {
            update_id: 12,
            payload,
        }
    }

    #[test]
    fn linked_channel_automatic_forward_binds_native_comment_without_topic_id() {
        let router = UpdateRouter::new(RuntimeBotRole::Discussion, policy()).unwrap();
        let routed = router.route(&update(json!({
            "message": {
                "message_id": 700,
                "chat": {"id": -1004290500369i64},
                "is_automatic_forward": true,
                "sender_chat": {"id": -1004446000549i64},
                "forward_from_message_id": 81
            }
        })));
        assert_eq!(
            routed,
            RoutedUpdate::Dispatch(WorkflowAction::NativeCommentPost {
                channel_chat_id: -1004446000549,
                channel_post_id: 81,
                discussion_chat_id: -1004290500369,
                discussion_root_message_id: 700,
            })
        );
    }

    #[test]
    fn routes_commands_and_rejects_unrelated_chats() {
        let router = UpdateRouter::new(RuntimeBotRole::Control, policy()).unwrap();
        let command = router.route(&update(json!({
            "message": {"message_id": 3, "chat": {"id": 42}, "text": "/perf@RustControlBot"}
        })));
        assert!(matches!(
            command,
            RoutedUpdate::Dispatch(WorkflowAction::Command {
                command: WorkflowCommand::Perf,
                ..
            })
        ));
        let ignored = router.route(&update(json!({
            "message": {"message_id": 4, "chat": {"id": 41}, "text": "/new"}
        })));
        assert_eq!(ignored, RoutedUpdate::Ignore);
    }

    #[test]
    fn preserves_python_role_local_command_surfaces() {
        let control = UpdateRouter::new(RuntimeBotRole::Control, policy()).unwrap();
        let discussion = UpdateRouter::new(RuntimeBotRole::Discussion, policy()).unwrap();

        let sessions = control.route(&update(json!({
            "message": {"message_id": 5, "chat": {"id": 42}, "text": "/sessions"}
        })));
        assert!(matches!(
            sessions,
            RoutedUpdate::Dispatch(WorkflowAction::Command {
                command: WorkflowCommand::Sessions,
                ..
            })
        ));

        let topics = control.route(&update(json!({
            "message": {"message_id": 51, "chat": {"id": 42}, "text": "/topics"}
        })));
        assert!(matches!(
            topics,
            RoutedUpdate::Dispatch(WorkflowAction::Command {
                command: WorkflowCommand::Topics,
                ..
            })
        ));

        let control_getfile = control.route(&update(json!({
            "message": {"message_id": 6, "chat": {"id": 42}, "text": "/getfile report.txt"}
        })));
        assert_eq!(control_getfile, RoutedUpdate::Ignore);

        let discussion_getfile = discussion.route(&update(json!({
            "message": {
                "message_id": 7,
                "chat": {"id": -1004290500369i64},
                "text": "/getfile report.txt"
            }
        })));
        assert!(matches!(
            discussion_getfile,
            RoutedUpdate::Dispatch(WorkflowAction::Command {
                command: WorkflowCommand::GetFile,
                ..
            })
        ));

        let discussion_sessions = discussion.route(&update(json!({
            "message": {
                "message_id": 8,
                "chat": {"id": -1004290500369i64},
                "text": "/sessions"
            }
        })));
        assert_eq!(discussion_sessions, RoutedUpdate::Ignore);
    }

    #[test]
    fn alert_role_cannot_create_an_update_router() {
        assert!(matches!(
            UpdateRouter::new(RuntimeBotRole::Alert, policy()),
            Err(RoutingError::AlertBotMustNotPoll)
        ));
    }
}
