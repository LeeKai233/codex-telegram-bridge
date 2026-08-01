//! Telegram Bot API bindings that keep bot roles, update ownership, and chat surfaces explicit.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};

use codex_telegram_credentials::{BotToken, CredentialRole};
use serde::Deserialize;
use serde_json::{json, Value};

pub const ALLOWED_UPDATES: &[&str] = &[
    "message",
    "edited_message",
    "callback_query",
    "my_chat_member",
];

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum BotCapability {
    Control,
    Discussion,
    Status,
}

impl BotCapability {
    pub const fn credential_role(self) -> CredentialRole {
        match self {
            Self::Control => CredentialRole::Control,
            Self::Discussion => CredentialRole::Discussion,
            Self::Status => CredentialRole::Status,
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
pub enum TelegramSurfaceBinding {
    Channel(ChannelBinding),
    ForumTopic(ForumTopicBinding),
}

impl TelegramSurfaceBinding {
    pub fn bot_instance_id(&self) -> &str {
        match self {
            Self::Channel(channel) => &channel.bot_instance_id,
            Self::ForumTopic(topic) => &topic.channel.bot_instance_id,
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
        })
    }
}

pub struct TokenLease {
    token: BotToken,
    consumer: UpdateConsumerId,
    registry: Arc<Mutex<HashMap<BotToken, UpdateConsumerId>>>,
}

impl TokenLease {
    pub fn consumer(&self) -> &UpdateConsumerId {
        &self.consumer
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TokenLeaseError {
    AlreadyLeased {
        requested: UpdateConsumerId,
        active: UpdateConsumerId,
    },
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
        if text.is_empty() {
            return Err(TelegramError::InvalidInput("message text cannot be empty"));
        }
        self.call(token, "sendMessage", surface.send_message_payload(text))
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
        let response: BotApiResponse<R> =
            serde_json::from_str(&body).map_err(|_| TelegramError::InvalidResponse { method })?;
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

    #[derive(Clone, Default)]
    struct RecordingTransport {
        calls: Arc<Mutex<Vec<(&'static str, Value)>>>,
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
        assert!(registry
            .acquire(&token(), UpdateConsumerId::parse("bridge-b").unwrap())
            .is_ok());
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
    fn token_is_not_present_in_adapter_errors() {
        let transport = RecordingTransport::responds_with(
            r#"{"ok":false,"error_code":401,"description":"bad token"}"#,
        );
        let error = TelegramBotApi::new(transport).get_me(&token()).unwrap_err();
        assert!(!error.to_string().contains("very-secret-token"));
        assert!(!format!("{error:?}").contains("very-secret-token"));
    }
}
