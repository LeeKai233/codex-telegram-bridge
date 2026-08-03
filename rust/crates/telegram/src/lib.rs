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

/// Telegram exposes the chat kind in every message and callback.  Keeping it
/// typed lets the adapter enforce the same private-chat and supergroup gates
/// as the Python controllers without leaking raw Bot API JSON to callers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelegramChatKind {
    Private,
    Group,
    Supergroup,
    Channel,
    Unknown,
}

impl TelegramChatKind {
    fn from_api(value: Option<&str>) -> Self {
        match value {
            Some("private") => Self::Private,
            Some("group") => Self::Group,
            Some("supergroup") => Self::Supergroup,
            Some("channel") => Self::Channel,
            _ => Self::Unknown,
        }
    }
}

/// The human sender and an optional anonymous sender-chat are intentionally
/// separate.  Python rejects anonymous administrators for owner-only flows.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TelegramActor {
    pub user_id: Option<i64>,
    pub sender_chat_id: Option<i64>,
}

impl TelegramActor {
    pub const fn is_personal(&self) -> bool {
        self.sender_chat_id.is_none()
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
    pub chat_kind: TelegramChatKind,
    pub message_id: i64,
    /// Text is normalized from either Bot API `text` or `caption`, matching
    /// the Python command helpers.
    pub text: Option<String>,
    /// The raw caption is retained so media messages can preserve their
    /// Python attachment semantics while `text` remains the command surface.
    pub caption: Option<String>,
    pub document_file_id: Option<String>,
    pub document_file_name: Option<String>,
    pub photo_file_id: Option<String>,
    /// True when the attachment came from Telegram's `photo` array.  Python
    /// treats photos as Codex local-image inputs, while documents remain
    /// ordinary file artifacts.
    pub is_photo: bool,
    pub actor: TelegramActor,
    pub reply_to_message_id: Option<i64>,
    pub message_thread_id: Option<i64>,
    /// The linked-discussion automatic forward from a channel post. Telegram's
    /// `is_automatic_forward` is the source of truth; forum topics are optional.
    pub automatic_forward_from_channel: Option<i64>,
    pub automatic_forward_channel_post_id: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramCallback {
    pub id: String,
    pub chat_id: i64,
    pub chat_kind: TelegramChatKind,
    pub message_id: i64,
    pub data: String,
    pub actor: TelegramActor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramMembership {
    pub chat_id: i64,
    pub chat_kind: TelegramChatKind,
    pub actor: TelegramActor,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncomingUpdate {
    Message(TelegramMessage),
    EditedMessage(TelegramMessage),
    Callback(TelegramCallback),
    Membership(TelegramMembership),
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
        if let Some(membership) = update
            .payload
            .get("my_chat_member")
            .and_then(parse_membership)
        {
            return Self::Membership(membership);
        }
        Self::Unsupported
    }
}

/// A BotCommand menu entry.  Labels are part of the user-facing Telegram
/// contract, so they live beside the role routing policy rather than in a
/// daemon-specific string literal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandMenuEntry {
    pub command: &'static str,
    pub description: &'static str,
}

impl CommandMenuEntry {
    fn to_value(self) -> Value {
        json!({"command": self.command, "description": self.description})
    }
}

/// Telegram's command-menu visibility is a Bot API value, not a role-local
/// routing decision. Keeping every wire variant typed prevents callers from
/// accidentally sending an owner-only control menu to a broad chat scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BotCommandScope {
    Default,
    AllPrivateChats,
    AllGroupChats,
    AllChatAdministrators,
    Chat { chat_id: i64 },
    ChatAdministrators { chat_id: i64 },
    ChatMember { chat_id: i64, user_id: i64 },
}

impl BotCommandScope {
    fn to_value(self) -> Value {
        match self {
            Self::Default => json!({"type": "default"}),
            Self::AllPrivateChats => json!({"type": "all_private_chats"}),
            Self::AllGroupChats => json!({"type": "all_group_chats"}),
            Self::AllChatAdministrators => json!({"type": "all_chat_administrators"}),
            Self::Chat { chat_id } => json!({"type": "chat", "chat_id": chat_id}),
            Self::ChatAdministrators { chat_id } => {
                json!({"type": "chat_administrators", "chat_id": chat_id})
            }
            Self::ChatMember { chat_id, user_id } => json!({
                "type": "chat_member",
                "chat_id": chat_id,
                "user_id": user_id,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandMenuScope {
    /// Commands shown before a control bot is paired or a discussion group is
    /// bound.  These are Telegram's all-private/all-group command menus.
    Bootstrap,
    /// Commands shown to the configured owner after setup succeeds.
    Owner,
}

const CONTROL_BOOTSTRAP_COMMANDS: &[CommandMenuEntry] = &[
    CommandMenuEntry {
        command: "pair",
        description: "完成 owner 配对",
    },
    CommandMenuEntry {
        command: "help",
        description: "显示帮助",
    },
];

const CONTROL_OWNER_COMMANDS: &[CommandMenuEntry] = &[
    CommandMenuEntry {
        command: "sessions",
        description: "查找 Codex sessions",
    },
    CommandMenuEntry {
        command: "topics",
        description: "查看 Session 帖子",
    },
    CommandMenuEntry {
        command: "new",
        description: "创建待认证 Session 帖子",
    },
    CommandMenuEntry {
        command: "perf",
        description: "查看 WSL 与 GPU 性能",
    },
    CommandMenuEntry {
        command: "help",
        description: "显示帮助",
    },
];

const DISCUSSION_BOOTSTRAP_COMMANDS: &[CommandMenuEntry] = &[
    CommandMenuEntry {
        command: "bind",
        description: "绑定频道讨论组",
    },
    CommandMenuEntry {
        command: "help",
        description: "显示帮助",
    },
];

const DISCUSSION_OWNER_COMMANDS: &[CommandMenuEntry] = &[
    CommandMenuEntry {
        command: "status",
        description: "刷新当前 Session 状态",
    },
    CommandMenuEntry {
        command: "totp",
        description: "认证当前 Session",
    },
    CommandMenuEntry {
        command: "lock",
        description: "锁定当前 Session",
    },
    CommandMenuEntry {
        command: "prompt",
        description: "发送 prompt",
    },
    CommandMenuEntry {
        command: "ask",
        description: "独立询问 Codex",
    },
    CommandMenuEntry {
        command: "queue",
        description: "查看队列或加入 prompt",
    },
    CommandMenuEntry {
        command: "planmode",
        description: "进入 Plan Mode",
    },
    CommandMenuEntry {
        command: "review",
        description: "执行一次 Codex Review",
    },
    CommandMenuEntry {
        command: "changemodel",
        description: "切换当前模式的模型",
    },
    CommandMenuEntry {
        command: "plan",
        description: "查看完整计划",
    },
    CommandMenuEntry {
        command: "timeline",
        description: "查看近期事件",
    },
    CommandMenuEntry {
        command: "attach",
        description: "接入 tmux",
    },
    CommandMenuEntry {
        command: "getfile",
        description: "获取本机文件",
    },
    CommandMenuEntry {
        command: "unwatch",
        description: "取消关注",
    },
    CommandMenuEntry {
        command: "help",
        description: "显示帮助",
    },
];

/// Exact command menus installed by the Python control, discussion, and
/// status bots.  `Status` and `Alert` intentionally expose no slash commands.
pub const fn command_menu(
    role: RuntimeBotRole,
    scope: CommandMenuScope,
) -> &'static [CommandMenuEntry] {
    match (role, scope) {
        (RuntimeBotRole::Control, CommandMenuScope::Bootstrap) => CONTROL_BOOTSTRAP_COMMANDS,
        (RuntimeBotRole::Control, CommandMenuScope::Owner) => CONTROL_OWNER_COMMANDS,
        (RuntimeBotRole::Discussion, CommandMenuScope::Bootstrap) => DISCUSSION_BOOTSTRAP_COMMANDS,
        (RuntimeBotRole::Discussion, CommandMenuScope::Owner) => DISCUSSION_OWNER_COMMANDS,
        (RuntimeBotRole::Status | RuntimeBotRole::Alert, _) => &[],
    }
}

/// The full Telegram command vocabulary.  It is richer than the legacy
/// `WorkflowCommand` because the adapter must preserve Python-visible commands
/// even while a downstream daemon incrementally implements their business use.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TelegramCommand {
    Pair,
    Bind,
    New,
    Totp,
    Lock,
    Status,
    Perf,
    Help,
    Sessions,
    Topics,
    Prompt,
    Ask,
    Queue,
    PlanMode,
    ChangeModel,
    Plan,
    Timeline,
    Attach,
    GetFile,
    Unwatch,
    Review,
    Answer,
    Cancel,
    Unknown(String),
}

impl TelegramCommand {
    pub fn name(&self) -> &str {
        match self {
            Self::Pair => "pair",
            Self::Bind => "bind",
            Self::New => "new",
            Self::Totp => "totp",
            Self::Lock => "lock",
            Self::Status => "status",
            Self::Perf => "perf",
            Self::Help => "help",
            Self::Sessions => "sessions",
            Self::Topics => "topics",
            Self::Prompt => "prompt",
            Self::Ask => "ask",
            Self::Queue => "queue",
            Self::PlanMode => "planmode",
            Self::ChangeModel => "changemodel",
            Self::Plan => "plan",
            Self::Timeline => "timeline",
            Self::Attach => "attach",
            Self::GetFile => "getfile",
            Self::Unwatch => "unwatch",
            Self::Review => "review",
            Self::Answer => "answer",
            Self::Cancel => "cancel",
            Self::Unknown(value) => value,
        }
    }

    fn parse_name(command: &str) -> Self {
        match command {
            "pair" => Self::Pair,
            "bind" => Self::Bind,
            "new" => Self::New,
            "totp" => Self::Totp,
            "lock" => Self::Lock,
            "status" => Self::Status,
            "perf" => Self::Perf,
            "help" | "start" => Self::Help,
            "sessions" => Self::Sessions,
            "topics" => Self::Topics,
            "prompt" => Self::Prompt,
            "ask" => Self::Ask,
            "queue" => Self::Queue,
            "planmode" => Self::PlanMode,
            "changemodel" => Self::ChangeModel,
            "plan" => Self::Plan,
            "timeline" => Self::Timeline,
            "attach" => Self::Attach,
            "getfile" => Self::GetFile,
            "unwatch" => Self::Unwatch,
            "review" => Self::Review,
            "answer" => Self::Answer,
            "cancel" => Self::Cancel,
            other => Self::Unknown(other.to_owned()),
        }
    }

    pub const fn allowed_for_role(&self, role: RuntimeBotRole) -> bool {
        match role {
            RuntimeBotRole::Control => matches!(
                self,
                Self::Pair
                    | Self::Help
                    | Self::Sessions
                    | Self::Topics
                    | Self::New
                    | Self::Perf
                    | Self::Unknown(_)
            ),
            RuntimeBotRole::Discussion => matches!(
                self,
                Self::Bind
                    | Self::Help
                    | Self::Status
                    | Self::Totp
                    | Self::Lock
                    | Self::Prompt
                    | Self::Ask
                    | Self::Queue
                    | Self::PlanMode
                    | Self::Review
                    | Self::ChangeModel
                    | Self::Plan
                    | Self::Timeline
                    | Self::Attach
                    | Self::GetFile
                    | Self::Unwatch
                    | Self::Answer
                    | Self::Cancel
                    | Self::Unknown(_)
            ),
            RuntimeBotRole::Status | RuntimeBotRole::Alert => false,
        }
    }

    fn into_legacy(self) -> WorkflowCommand {
        match self {
            Self::Pair => WorkflowCommand::Pair,
            Self::Bind => WorkflowCommand::Bind,
            Self::New => WorkflowCommand::New,
            Self::Totp => WorkflowCommand::Totp,
            Self::Lock => WorkflowCommand::Lock,
            Self::Status => WorkflowCommand::Status,
            Self::Perf => WorkflowCommand::Perf,
            Self::Help => WorkflowCommand::Help,
            Self::Sessions => WorkflowCommand::Sessions,
            Self::Topics => WorkflowCommand::Topics,
            Self::Prompt => WorkflowCommand::Prompt,
            Self::Ask => WorkflowCommand::Ask,
            Self::Queue => WorkflowCommand::Queue,
            Self::PlanMode => WorkflowCommand::PlanMode,
            Self::ChangeModel => WorkflowCommand::ChangeModel,
            Self::Plan => WorkflowCommand::Plan,
            Self::Timeline => WorkflowCommand::Timeline,
            Self::Attach => WorkflowCommand::Attach,
            Self::GetFile => WorkflowCommand::GetFile,
            Self::Unwatch => WorkflowCommand::Unwatch,
            Self::Answer => WorkflowCommand::Answer,
            Self::Review => WorkflowCommand::Review,
            Self::Cancel => WorkflowCommand::Cancel,
            command => WorkflowCommand::Unknown(command.name().to_owned()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedTelegramCommand {
    pub command: TelegramCommand,
    pub addressed_bot_username: Option<String>,
}

impl ParsedTelegramCommand {
    pub fn parse(text: &str) -> Option<Self> {
        let first = text.split_whitespace().next()?;
        let value = first.strip_prefix('/')?;
        let (name, addressed_bot_username) = match value.split_once('@') {
            Some((name, target)) if !target.is_empty() => (name, Some(target.to_ascii_lowercase())),
            Some((name, _)) => (name, None),
            None => (value, None),
        };
        if name.is_empty() {
            return None;
        }
        Some(Self {
            command: TelegramCommand::parse_name(&name.to_ascii_lowercase()),
            addressed_bot_username,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkflowCommand {
    Pair,
    Bind,
    New,
    Totp,
    Lock,
    Status,
    Perf,
    Help,
    Sessions,
    Topics,
    Prompt,
    Ask,
    Queue,
    PlanMode,
    ChangeModel,
    Plan,
    Timeline,
    Attach,
    GetFile,
    Unwatch,
    Answer,
    Review,
    Cancel,
    Unknown(String),
}

impl WorkflowCommand {
    pub fn parse(text: &str) -> Option<Self> {
        ParsedTelegramCommand::parse(text).map(|parsed| parsed.command.into_legacy())
    }

    /// Keep the Python bridge's role-local command surface intact. A command
    /// may be understood by the shared parser while still being ignored by a
    /// bot that does not own that business surface.
    pub const fn allowed_for_role(&self, role: RuntimeBotRole) -> bool {
        match role {
            RuntimeBotRole::Control => matches!(
                self,
                Self::Pair
                    | Self::New
                    | Self::Perf
                    | Self::Help
                    | Self::Sessions
                    | Self::Topics
                    | Self::Unknown(_)
            ),
            RuntimeBotRole::Discussion => matches!(
                self,
                Self::Bind
                    | Self::Status
                    | Self::Totp
                    | Self::Lock
                    | Self::Prompt
                    | Self::Ask
                    | Self::Queue
                    | Self::PlanMode
                    | Self::ChangeModel
                    | Self::Plan
                    | Self::Timeline
                    | Self::Attach
                    | Self::GetFile
                    | Self::Unwatch
                    | Self::Answer
                    | Self::Review
                    | Self::Cancel
                    | Self::Help
                    | Self::Unknown(_)
            ),
            RuntimeBotRole::Status | RuntimeBotRole::Alert => false,
        }
    }
}

/// A typed effect emitted by the adapter before it is converted into the
/// legacy daemon action.  New controllers can consume this form without
/// reparsing Bot API JSON or losing sender and thread context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkflowEffect {
    Command {
        command: TelegramCommand,
        chat_id: i64,
        message_id: i64,
        message_thread_id: Option<i64>,
        root_message_id: Option<i64>,
        text: String,
        actor: TelegramActor,
    },
    Prompt {
        chat_id: i64,
        message_id: i64,
        message_thread_id: Option<i64>,
        text: String,
        root_message_id: Option<i64>,
        actor: TelegramActor,
    },
    Attachment {
        chat_id: i64,
        message_id: i64,
        message_thread_id: Option<i64>,
        caption: Option<String>,
        file_id: String,
        file_name: Option<String>,
        is_photo: bool,
        root_message_id: Option<i64>,
        actor: TelegramActor,
    },
    Callback(TelegramCallback),
    NativeCommentPost {
        channel_chat_id: i64,
        channel_post_id: i64,
        discussion_chat_id: i64,
        discussion_root_message_id: i64,
    },
    MembershipChanged(TelegramMembership),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RoutedEffect {
    Dispatch(WorkflowEffect),
    Ignore,
}

impl RoutedEffect {
    fn into_legacy(self) -> RoutedUpdate {
        match self {
            Self::Ignore => RoutedUpdate::Ignore,
            Self::Dispatch(effect) => RoutedUpdate::Dispatch(match effect {
                WorkflowEffect::Command {
                    command,
                    chat_id,
                    message_id,
                    root_message_id,
                    text,
                    ..
                } => WorkflowAction::Command {
                    command: command.into_legacy(),
                    chat_id,
                    message_id,
                    root_message_id,
                    text,
                },
                WorkflowEffect::Prompt {
                    chat_id,
                    message_id,
                    text,
                    root_message_id,
                    ..
                } => WorkflowAction::Prompt {
                    chat_id,
                    message_id,
                    text,
                    root_message_id,
                },
                WorkflowEffect::Attachment {
                    chat_id,
                    message_id,
                    caption,
                    file_id,
                    file_name,
                    is_photo,
                    root_message_id,
                    ..
                } => WorkflowAction::Attachment {
                    chat_id,
                    message_id,
                    caption,
                    file_id,
                    file_name,
                    is_photo,
                    root_message_id,
                },
                WorkflowEffect::Callback(callback) => WorkflowAction::Callback(callback),
                WorkflowEffect::NativeCommentPost {
                    channel_chat_id,
                    channel_post_id,
                    discussion_chat_id,
                    discussion_root_message_id,
                } => WorkflowAction::NativeCommentPost {
                    channel_chat_id,
                    channel_post_id,
                    discussion_chat_id,
                    discussion_root_message_id,
                },
                WorkflowEffect::MembershipChanged(_) => WorkflowAction::MembershipChanged,
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkflowAction {
    Command {
        command: WorkflowCommand,
        chat_id: i64,
        message_id: i64,
        root_message_id: Option<i64>,
        text: String,
    },
    Prompt {
        chat_id: i64,
        message_id: i64,
        text: String,
        root_message_id: Option<i64>,
    },
    Attachment {
        chat_id: i64,
        message_id: i64,
        caption: Option<String>,
        file_id: String,
        file_name: Option<String>,
        is_photo: bool,
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

/// Optional strict gates mirroring the Python controllers.  `new` keeps the
/// historical chat-id-only behavior for the existing daemon, while new
/// consumers should use `new_with_authorization`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpdateAuthorization {
    pub owner_user_id: Option<i64>,
    pub bot_username: Option<String>,
    pub enforce_chat_kind: bool,
    pub reject_sender_chat: bool,
    /// When no owner exists, only the bootstrap `/pair` and `/help` commands
    /// may pass through the Control Bot's configured chat gate.
    pub bootstrap_only: bool,
}

impl UpdateAuthorization {
    pub fn python_owner(owner_user_id: i64, bot_username: impl Into<String>) -> Self {
        Self {
            owner_user_id: Some(owner_user_id),
            bot_username: Some(bot_username.into().to_ascii_lowercase()),
            enforce_chat_kind: true,
            reject_sender_chat: true,
            bootstrap_only: false,
        }
    }

    fn allows_actor(&self, actor: &TelegramActor) -> bool {
        self.owner_user_id
            .is_none_or(|owner_user_id| actor.user_id == Some(owner_user_id))
            && (!self.reject_sender_chat || actor.is_personal())
    }

    fn targets_this_bot(&self, target: Option<&str>) -> bool {
        match (target, self.bot_username.as_deref()) {
            (Some(target), Some(expected)) => target.eq_ignore_ascii_case(expected),
            _ => true,
        }
    }
}

pub struct UpdateRouter {
    role: RuntimeBotRole,
    policy: UpdateRoutingPolicy,
    authorization: UpdateAuthorization,
}

impl UpdateRouter {
    pub fn new(role: RuntimeBotRole, policy: UpdateRoutingPolicy) -> Result<Self, RoutingError> {
        if !role.polls_updates() {
            return Err(RoutingError::AlertBotMustNotPoll);
        }
        Ok(Self {
            role,
            policy,
            authorization: UpdateAuthorization::default(),
        })
    }

    pub fn new_with_authorization(
        role: RuntimeBotRole,
        policy: UpdateRoutingPolicy,
        authorization: UpdateAuthorization,
    ) -> Result<Self, RoutingError> {
        let mut router = Self::new(role, policy)?;
        router.authorization = authorization;
        Ok(router)
    }

    pub fn route(&self, update: &Update) -> RoutedUpdate {
        self.route_effect(update).into_legacy()
    }

    pub fn route_effect(&self, update: &Update) -> RoutedEffect {
        match (self.role, IncomingUpdate::from_update(update)) {
            (RuntimeBotRole::Control, IncomingUpdate::Message(message))
                if message.chat_id == self.policy.control_owner_chat_id
                    && self.message_is_authorized(&message, false) =>
            {
                self.route_message_effect(message, None)
            }
            (RuntimeBotRole::Control, IncomingUpdate::Callback(callback))
                if callback.chat_id == self.policy.control_owner_chat_id
                    && self.callback_is_authorized(&callback) =>
            {
                RoutedEffect::Dispatch(WorkflowEffect::Callback(callback))
            }
            (RuntimeBotRole::Status, IncomingUpdate::Callback(callback))
                if callback.chat_id == self.policy.linked_discussion.discussion_chat_id
                    && self.callback_is_authorized(&callback) =>
            {
                RoutedEffect::Dispatch(WorkflowEffect::Callback(callback))
            }
            (RuntimeBotRole::Discussion, IncomingUpdate::Message(message))
                if message.chat_id == self.policy.linked_discussion.discussion_chat_id =>
            {
                if message.automatic_forward_from_channel
                    == Some(self.policy.linked_discussion.channel_chat_id)
                {
                    if !self.message_is_authorized(&message, true) {
                        return RoutedEffect::Ignore;
                    }
                    RoutedEffect::Dispatch(WorkflowEffect::NativeCommentPost {
                        channel_chat_id: self.policy.linked_discussion.channel_chat_id,
                        channel_post_id: message
                            .automatic_forward_channel_post_id
                            .unwrap_or(message.message_id),
                        discussion_chat_id: message.chat_id,
                        discussion_root_message_id: message.message_id,
                    })
                } else {
                    if !self.message_is_authorized(&message, false) {
                        return RoutedEffect::Ignore;
                    }
                    self.route_message_effect(message.clone(), message.reply_to_message_id)
                }
            }
            (RuntimeBotRole::Discussion, IncomingUpdate::Callback(callback))
                if callback.chat_id == self.policy.linked_discussion.discussion_chat_id
                    && self.callback_is_authorized(&callback) =>
            {
                RoutedEffect::Dispatch(WorkflowEffect::Callback(callback))
            }
            (_, IncomingUpdate::Membership(membership))
                if self.membership_is_authorized(&membership) =>
            {
                RoutedEffect::Dispatch(WorkflowEffect::MembershipChanged(membership))
            }
            _ => RoutedEffect::Ignore,
        }
    }

    fn route_message_effect(
        &self,
        message: TelegramMessage,
        root_message_id: Option<i64>,
    ) -> RoutedEffect {
        let text = message.text.clone().or_else(|| message.caption.clone());
        let file_id = message
            .document_file_id
            .clone()
            .or_else(|| message.photo_file_id.clone());
        if self.authorization.bootstrap_only
            && self.authorization.owner_user_id.is_none()
            && file_id.is_some()
        {
            return RoutedEffect::Ignore;
        }
        if let Some(file_id) = file_id {
            return RoutedEffect::Dispatch(WorkflowEffect::Attachment {
                chat_id: message.chat_id,
                message_id: message.message_id,
                message_thread_id: message.message_thread_id,
                caption: message.caption.clone().or(text.clone()),
                file_id,
                file_name: message.document_file_name.clone(),
                is_photo: message.is_photo,
                root_message_id,
                actor: message.actor.clone(),
            });
        }
        let Some(text) = text.filter(|text| !text.trim().is_empty()) else {
            return RoutedEffect::Ignore;
        };
        if let Some(parsed) = ParsedTelegramCommand::parse(&text) {
            let bootstrap_command_allowed = match self.role {
                RuntimeBotRole::Control => {
                    matches!(
                        parsed.command,
                        TelegramCommand::Pair | TelegramCommand::Help
                    )
                }
                RuntimeBotRole::Discussion => {
                    matches!(
                        parsed.command,
                        TelegramCommand::Bind | TelegramCommand::Help
                    )
                }
                RuntimeBotRole::Status | RuntimeBotRole::Alert => false,
            };
            if !self
                .authorization
                .targets_this_bot(parsed.addressed_bot_username.as_deref())
                || !parsed.command.allowed_for_role(self.role)
                || (self.authorization.bootstrap_only && !bootstrap_command_allowed)
            {
                return RoutedEffect::Ignore;
            }
            RoutedEffect::Dispatch(WorkflowEffect::Command {
                command: parsed.command,
                chat_id: message.chat_id,
                message_id: message.message_id,
                message_thread_id: message.message_thread_id,
                root_message_id,
                text,
                actor: message.actor,
            })
        } else {
            if self.authorization.bootstrap_only {
                return RoutedEffect::Ignore;
            }
            RoutedEffect::Dispatch(WorkflowEffect::Prompt {
                chat_id: message.chat_id,
                message_id: message.message_id,
                message_thread_id: message.message_thread_id,
                text,
                root_message_id,
                actor: message.actor,
            })
        }
    }

    fn message_is_authorized(&self, message: &TelegramMessage, automatic_forward: bool) -> bool {
        if self.authorization.enforce_chat_kind
            && !matches!(
                (self.role, message.chat_kind),
                (RuntimeBotRole::Control, TelegramChatKind::Private)
                    | (RuntimeBotRole::Discussion, TelegramChatKind::Supergroup)
            )
        {
            return false;
        }
        if self.authorization.bootstrap_only
            && self.authorization.owner_user_id.is_none()
            && self.role == RuntimeBotRole::Status
        {
            return false;
        }
        if automatic_forward
            && self.authorization.bootstrap_only
            && self.authorization.owner_user_id.is_none()
        {
            return false;
        }
        automatic_forward || self.authorization.allows_actor(&message.actor)
    }

    fn callback_is_authorized(&self, callback: &TelegramCallback) -> bool {
        if self.authorization.enforce_chat_kind
            && !matches!(
                (self.role, callback.chat_kind),
                (RuntimeBotRole::Control, TelegramChatKind::Private)
                    | (
                        RuntimeBotRole::Discussion | RuntimeBotRole::Status,
                        TelegramChatKind::Supergroup
                    )
            )
        {
            return false;
        }
        if self.authorization.bootstrap_only && self.authorization.owner_user_id.is_none() {
            return false;
        }
        self.authorization.allows_actor(&callback.actor)
    }

    fn membership_is_authorized(&self, membership: &TelegramMembership) -> bool {
        if !self.authorization.enforce_chat_kind {
            return true;
        }
        match self.role {
            RuntimeBotRole::Control => {
                membership.chat_id == self.policy.control_owner_chat_id
                    && membership.chat_kind == TelegramChatKind::Private
            }
            RuntimeBotRole::Discussion | RuntimeBotRole::Status => {
                membership.chat_id == self.policy.linked_discussion.discussion_chat_id
                    && membership.chat_kind == TelegramChatKind::Supergroup
            }
            RuntimeBotRole::Alert => false,
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
        chat_kind: TelegramChatKind::from_api(value.pointer("/chat/type").and_then(Value::as_str)),
        message_id,
        text: value
            .get("text")
            .or_else(|| value.get("caption"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        caption: value
            .get("caption")
            .and_then(Value::as_str)
            .map(str::to_owned),
        document_file_id: value
            .pointer("/document/file_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        document_file_name: value
            .pointer("/document/file_name")
            .and_then(Value::as_str)
            .map(str::to_owned),
        photo_file_id: value
            .get("photo")
            .and_then(Value::as_array)
            .and_then(|photos| photos.last())
            .and_then(|photo| photo.get("file_id"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        is_photo: value
            .get("photo")
            .and_then(Value::as_array)
            .is_some_and(|photos| !photos.is_empty()),
        actor: TelegramActor {
            user_id: value.pointer("/from/id").and_then(Value::as_i64),
            sender_chat_id: value.pointer("/sender_chat/id").and_then(Value::as_i64),
        },
        reply_to_message_id: value
            .pointer("/reply_to_message/message_id")
            .and_then(Value::as_i64),
        message_thread_id: value.get("message_thread_id").and_then(Value::as_i64),
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
        chat_kind: TelegramChatKind::from_api(
            value.pointer("/message/chat/type").and_then(Value::as_str),
        ),
        message_id: value.pointer("/message/message_id")?.as_i64()?,
        data: value.get("data")?.as_str()?.to_owned(),
        actor: TelegramActor {
            user_id: value.pointer("/from/id").and_then(Value::as_i64),
            sender_chat_id: None,
        },
    })
}

fn parse_membership(value: &Value) -> Option<TelegramMembership> {
    Some(TelegramMembership {
        chat_id: value.pointer("/chat/id")?.as_i64()?,
        chat_kind: TelegramChatKind::from_api(value.pointer("/chat/type").and_then(Value::as_str)),
        actor: TelegramActor {
            user_id: value.pointer("/from/id").and_then(Value::as_i64),
            sender_chat_id: None,
        },
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TelegramParseMode {
    MarkdownV2,
    Html,
}

impl TelegramParseMode {
    const fn as_api_value(self) -> &'static str {
        match self {
            Self::MarkdownV2 => "MarkdownV2",
            Self::Html => "HTML",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplyParameters {
    pub message_id: i64,
    pub allow_sending_without_reply: Option<bool>,
}

impl ReplyParameters {
    pub const fn new(message_id: i64) -> Self {
        Self {
            message_id,
            allow_sending_without_reply: None,
        }
    }

    pub const fn allow_sending_without_reply(mut self, allowed: bool) -> Self {
        self.allow_sending_without_reply = Some(allowed);
        self
    }

    fn to_value(&self) -> Value {
        let mut value = json!({"message_id": self.message_id});
        if let Some(allow_sending_without_reply) = self.allow_sending_without_reply {
            value["allow_sending_without_reply"] = Value::Bool(allow_sending_without_reply);
        }
        value
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InlineKeyboardButton {
    Callback { text: String, data: String },
    Url { text: String, url: String },
}

impl InlineKeyboardButton {
    pub fn callback(
        text: impl Into<String>,
        data: impl Into<String>,
    ) -> Result<Self, TelegramError> {
        let text = text.into();
        let data = data.into();
        if text.trim().is_empty() {
            return Err(TelegramError::InvalidInput("button text cannot be empty"));
        }
        if data.is_empty() {
            return Err(TelegramError::InvalidInput("callback data cannot be empty"));
        }
        if data.len() > 64 {
            return Err(TelegramError::InvalidInput(
                "callback data must be at most 64 bytes",
            ));
        }
        Ok(Self::Callback { text, data })
    }

    pub fn url(text: impl Into<String>, url: impl Into<String>) -> Result<Self, TelegramError> {
        let text = text.into();
        let url = url.into();
        if text.trim().is_empty() {
            return Err(TelegramError::InvalidInput("button text cannot be empty"));
        }
        if url.trim().is_empty() {
            return Err(TelegramError::InvalidInput("button URL cannot be empty"));
        }
        Ok(Self::Url { text, url })
    }

    fn to_value(&self) -> Value {
        match self {
            Self::Callback { text, data } => {
                json!({"text": text, "callback_data": data})
            }
            Self::Url { text, url } => json!({"text": text, "url": url}),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InlineKeyboardMarkup {
    pub rows: Vec<Vec<InlineKeyboardButton>>,
}

impl InlineKeyboardMarkup {
    pub fn new(rows: Vec<Vec<InlineKeyboardButton>>) -> Result<Self, TelegramError> {
        if rows.is_empty() || rows.iter().any(Vec::is_empty) {
            return Err(TelegramError::InvalidInput(
                "inline keyboard rows cannot be empty",
            ));
        }
        Ok(Self { rows })
    }

    fn to_value(&self) -> Value {
        Value::Object(
            [(
                "inline_keyboard".to_owned(),
                Value::Array(
                    self.rows
                        .iter()
                        .map(|row| {
                            Value::Array(row.iter().map(InlineKeyboardButton::to_value).collect())
                        })
                        .collect(),
                ),
            )]
            .into_iter()
            .collect(),
        )
    }
}

/// Rendering options for a Telegram text message.  The Python endpoint always
/// disables previews and retries markup errors with `plain_fallback`; this
/// value preserves both pieces of behavior in a transport-neutral form.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramMessageRequest {
    pub text: String,
    pub plain_fallback: Option<String>,
    pub parse_mode: Option<TelegramParseMode>,
    pub reply_markup: Option<InlineKeyboardMarkup>,
    pub reply_parameters: Option<ReplyParameters>,
    pub disable_link_preview: bool,
}

impl TelegramMessageRequest {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            plain_fallback: None,
            parse_mode: None,
            reply_markup: None,
            reply_parameters: None,
            disable_link_preview: true,
        }
    }

    /// Builds the Python control bot's normal rich-text contract: Telegram
    /// receives MarkdownV2 first and a supplied plain string on a 400 parse
    /// rejection.
    pub fn markdown_v2(markdown: impl Into<String>, plain_fallback: impl Into<String>) -> Self {
        Self::new(markdown)
            .with_plain_fallback(plain_fallback)
            .with_parse_mode(TelegramParseMode::MarkdownV2)
    }

    pub fn with_plain_fallback(mut self, plain_fallback: impl Into<String>) -> Self {
        self.plain_fallback = Some(plain_fallback.into());
        self
    }

    pub const fn with_parse_mode(mut self, parse_mode: TelegramParseMode) -> Self {
        self.parse_mode = Some(parse_mode);
        self
    }

    pub fn with_reply_markup(mut self, reply_markup: InlineKeyboardMarkup) -> Self {
        self.reply_markup = Some(reply_markup);
        self
    }

    pub fn with_reply_markup_option(mut self, reply_markup: Option<InlineKeyboardMarkup>) -> Self {
        self.reply_markup = reply_markup;
        self
    }

    pub const fn reply_to(mut self, reply_parameters: ReplyParameters) -> Self {
        self.reply_parameters = Some(reply_parameters);
        self
    }

    pub const fn with_link_preview_disabled(mut self, disabled: bool) -> Self {
        self.disable_link_preview = disabled;
        self
    }

    fn fallback(&self) -> Option<Self> {
        self.plain_fallback
            .as_ref()
            .filter(|fallback| *fallback != &self.text)
            .map(|fallback| Self {
                text: fallback.clone(),
                plain_fallback: None,
                parse_mode: None,
                reply_markup: self.reply_markup.clone(),
                reply_parameters: self.reply_parameters.clone(),
                disable_link_preview: self.disable_link_preview,
            })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramMessageReference {
    pub chat_id: String,
    pub message_id: i64,
}

impl TelegramMessageReference {
    pub fn new(chat_id: impl Into<String>, message_id: i64) -> Result<Self, TelegramError> {
        let chat_id = chat_id.into();
        if chat_id.trim().is_empty() || message_id <= 0 {
            return Err(TelegramError::InvalidInput(
                "message reference needs a chat id and positive message id",
            ));
        }
        Ok(Self {
            chat_id,
            message_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelegramDocumentRequest {
    pub file_name: String,
    pub file_bytes: Vec<u8>,
    pub caption: Option<String>,
    pub caption_parse_mode: Option<TelegramParseMode>,
    pub reply_markup: Option<InlineKeyboardMarkup>,
}

impl TelegramDocumentRequest {
    pub fn new(file_name: impl Into<String>, file_bytes: Vec<u8>) -> Self {
        Self {
            file_name: file_name.into(),
            file_bytes,
            caption: None,
            caption_parse_mode: None,
            reply_markup: None,
        }
    }

    pub fn with_caption(mut self, caption: impl Into<String>) -> Self {
        self.caption = Some(caption.into());
        self
    }

    pub const fn with_caption_parse_mode(mut self, parse_mode: TelegramParseMode) -> Self {
        self.caption_parse_mode = Some(parse_mode);
        self
    }

    pub fn with_reply_markup(mut self, reply_markup: InlineKeyboardMarkup) -> Self {
        self.reply_markup = Some(reply_markup);
        self
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CallbackAnswer {
    pub text: Option<String>,
    pub show_alert: bool,
}

impl CallbackAnswer {
    pub fn text(value: impl Into<String>) -> Self {
        Self {
            text: Some(value.into()),
            show_alert: false,
        }
    }

    pub const fn show_alert(mut self, show_alert: bool) -> Self {
        self.show_alert = show_alert;
        self
    }
}

/// The persistent callback store owns durability; this typed ticket is the
/// adapter-side contract for creating a button and validating all scope data
/// before a store atomically consumes it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallbackTicket {
    pub nonce: String,
    pub action: String,
    pub bot_role: RuntimeBotRole,
    pub user_id: i64,
    pub chat_id: i64,
    pub space_id: Option<String>,
    pub generation: i64,
    pub expires_at_ms: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallbackTicketValidation {
    Accepted,
    Expired,
    WrongNonce,
    WrongRole,
    WrongUser,
    WrongChat,
    WrongSpace,
    WrongGeneration,
}

impl CallbackTicket {
    pub fn callback_data(&self) -> Result<String, TelegramError> {
        if self.nonce.trim().is_empty() {
            return Err(TelegramError::InvalidInput(
                "callback nonce cannot be empty",
            ));
        }
        let data = format!("cb:{}", self.nonce);
        if data.len() > 64 {
            return Err(TelegramError::InvalidInput(
                "callback data must be at most 64 bytes",
            ));
        }
        Ok(data)
    }

    pub fn button(&self, text: impl Into<String>) -> Result<InlineKeyboardButton, TelegramError> {
        InlineKeyboardButton::callback(text, self.callback_data()?)
    }

    pub fn validate(
        &self,
        role: RuntimeBotRole,
        callback: &TelegramCallback,
        now_ms: i64,
        space_id: Option<&str>,
        generation: i64,
    ) -> CallbackTicketValidation {
        if now_ms > self.expires_at_ms {
            return CallbackTicketValidation::Expired;
        }
        if callback.data != format!("cb:{}", self.nonce) {
            return CallbackTicketValidation::WrongNonce;
        }
        if role != self.bot_role {
            return CallbackTicketValidation::WrongRole;
        }
        if callback.actor.user_id != Some(self.user_id) {
            return CallbackTicketValidation::WrongUser;
        }
        if callback.chat_id != self.chat_id {
            return CallbackTicketValidation::WrongChat;
        }
        if self.space_id.as_deref() != space_id {
            return CallbackTicketValidation::WrongSpace;
        }
        if self.generation != generation {
            return CallbackTicketValidation::WrongGeneration;
        }
        CallbackTicketValidation::Accepted
    }
}

impl TelegramSurfaceBinding {
    pub fn bot_instance_id(&self) -> &str {
        match self {
            Self::Channel(channel) => &channel.bot_instance_id,
            Self::ForumTopic(topic) => &topic.channel.bot_instance_id,
            Self::NativeCommentRoot(comment) => &comment.channel.bot_instance_id,
        }
    }

    pub fn message_reference(
        &self,
        message_id: i64,
    ) -> Result<TelegramMessageReference, TelegramError> {
        let chat_id = match self {
            Self::Channel(channel) => &channel.chat_id,
            Self::ForumTopic(topic) => &topic.channel.chat_id,
            Self::NativeCommentRoot(comment) => &comment.discussion_chat_id,
        };
        TelegramMessageReference::new(chat_id.clone(), message_id)
    }

    fn base_send_message_payload(&self, text: &str) -> Value {
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

    fn render_message_payload(&self, request: &TelegramMessageRequest) -> Value {
        let mut payload = self.base_send_message_payload(&request.text);
        if let Some(parse_mode) = request.parse_mode {
            payload["parse_mode"] = Value::String(parse_mode.as_api_value().to_owned());
        }
        if let Some(reply_markup) = request.reply_markup.as_ref() {
            payload["reply_markup"] = reply_markup.to_value();
        }
        if payload.get("reply_parameters").is_none()
            && let Some(reply_parameters) = request.reply_parameters.as_ref()
        {
            payload["reply_parameters"] = reply_parameters.to_value();
        }
        if request.disable_link_preview {
            payload["link_preview_options"] = json!({"is_disabled": true});
        }
        payload
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

    fn get_bytes(
        &self,
        _api_base: &str,
        _token: &BotToken,
        _file_path: &str,
    ) -> Result<Vec<u8>, TelegramTransportError> {
        Err(TelegramTransportError::new("download-unsupported"))
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
        if !should_parse_bot_api_response(response.status()) {
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
        if !should_parse_bot_api_response(response.status()) {
            return Err(TelegramTransportError::new("http-status"));
        }
        response
            .text()
            .map_err(|_| TelegramTransportError::new("response-body"))
    }

    fn get_bytes(
        &self,
        api_base: &str,
        token: &BotToken,
        file_path: &str,
    ) -> Result<Vec<u8>, TelegramTransportError> {
        let url = format!(
            "{}/file/bot{}/{}",
            api_base.trim_end_matches('/'),
            token.as_str(),
            file_path.trim_start_matches('/')
        );
        let response = self.client.get(url).send().map_err(|error| {
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
            .bytes()
            .map(|bytes| bytes.to_vec())
            .map_err(|_| TelegramTransportError::new("response-body"))
    }
}

fn should_parse_bot_api_response(status: reqwest::StatusCode) -> bool {
    status.is_success() || status == reqwest::StatusCode::BAD_REQUEST
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

    pub fn get_file(&self, token: &BotToken, file_id: &str) -> Result<TelegramFile, TelegramError> {
        if file_id.trim().is_empty() {
            return Err(TelegramError::InvalidInput("file id cannot be empty"));
        }
        self.call(token, "getFile", json!({"file_id": file_id}))
    }

    pub fn download_file(
        &self,
        token: &BotToken,
        file_path: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>, TelegramError> {
        if file_path.trim().is_empty() {
            return Err(TelegramError::InvalidInput("file path cannot be empty"));
        }
        if max_bytes == 0 {
            return Err(TelegramError::InvalidInput("download limit cannot be zero"));
        }
        let bytes = self
            .transport
            .get_bytes(&self.api_base, token, file_path)
            .map_err(TelegramError::Transport)?;
        if bytes.len() > max_bytes {
            return Err(TelegramError::InvalidInput(
                "download exceeds configured limit",
            ));
        }
        Ok(bytes)
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

    /// Install a non-empty command menu for one explicit Bot API scope.
    /// Clearing a menu is deliberately a separate `delete_my_commands` call,
    /// matching Telegram's API and making a broad default clear auditable.
    pub fn set_my_commands(
        &self,
        token: &BotToken,
        commands: &[CommandMenuEntry],
        scope: BotCommandScope,
    ) -> Result<bool, TelegramError> {
        if commands.is_empty() {
            return Err(TelegramError::InvalidInput("command menu cannot be empty"));
        }
        self.call(
            token,
            "setMyCommands",
            json!({
                "commands": commands.iter().copied().map(CommandMenuEntry::to_value).collect::<Vec<_>>(),
                "scope": scope.to_value(),
            }),
        )
    }

    /// Remove the command menu from exactly one scope. Callers must select a
    /// scope explicitly so deleting an owner menu cannot implicitly alter the
    /// all-private bootstrap menu.
    pub fn delete_my_commands(
        &self,
        token: &BotToken,
        scope: BotCommandScope,
    ) -> Result<bool, TelegramError> {
        self.call(
            token,
            "deleteMyCommands",
            json!({"scope": scope.to_value()}),
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

    /// Sends a typed text request and retries a Telegram 400 with the supplied
    /// unformatted fallback. This mirrors the Python endpoint's markdown/HTML
    /// recovery path while retaining the reply target and inline keyboard.
    pub fn send_rendered(
        &self,
        token: &BotToken,
        surface: &TelegramSurfaceBinding,
        request: &TelegramMessageRequest,
    ) -> Result<SentMessage, TelegramError> {
        self.call_rendered(token, "sendMessage", request, |request| {
            surface.render_message_payload(request)
        })
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
        let mut payload = surface.base_send_message_payload(text);
        if let Some(reply_markup) = reply_markup {
            payload["reply_markup"] = reply_markup;
        }
        self.call(token, "sendMessage", payload)
    }

    pub fn edit_text(
        &self,
        token: &BotToken,
        message: &TelegramMessageReference,
        request: &TelegramMessageRequest,
    ) -> Result<SentMessage, TelegramError> {
        self.call_rendered(token, "editMessageText", request, |request| {
            render_edit_text_payload(message, request)
        })
    }

    /// Edit text while preserving the daemon's JSON keyboard contract.  The
    /// typed `edit_text` API remains available for callers that already use
    /// `InlineKeyboardMarkup`; this value-based sibling is used by durable
    /// callback projections whose markup is stored as JSON alongside state.
    pub fn edit_text_with_markup(
        &self,
        token: &BotToken,
        message: &TelegramMessageReference,
        text: &str,
        reply_markup: Option<Value>,
    ) -> Result<SentMessage, TelegramError> {
        if text.is_empty() {
            return Err(TelegramError::InvalidInput("message text cannot be empty"));
        }
        let mut payload = json!({
            "chat_id": message.chat_id,
            "message_id": message.message_id,
            "text": text,
            "link_preview_options": {"is_disabled": true},
        });
        if let Some(reply_markup) = reply_markup {
            payload["reply_markup"] = reply_markup;
        }
        self.call(token, "editMessageText", payload)
    }

    pub fn edit_reply_markup(
        &self,
        token: &BotToken,
        message: &TelegramMessageReference,
        reply_markup: Option<&InlineKeyboardMarkup>,
    ) -> Result<SentMessage, TelegramError> {
        let mut payload = json!({
            "chat_id": message.chat_id,
            "message_id": message.message_id,
        });
        payload["reply_markup"] = reply_markup
            .map(InlineKeyboardMarkup::to_value)
            .unwrap_or(Value::Null);
        self.call(token, "editMessageReplyMarkup", payload)
    }

    pub fn delete_message(
        &self,
        token: &BotToken,
        message: &TelegramMessageReference,
    ) -> Result<bool, TelegramError> {
        self.call(
            token,
            "deleteMessage",
            json!({"chat_id": message.chat_id, "message_id": message.message_id}),
        )
    }

    pub fn send_document(
        &self,
        token: &BotToken,
        surface: &TelegramSurfaceBinding,
        file_name: &str,
        file_bytes: Vec<u8>,
        caption: Option<&str>,
    ) -> Result<SentMessage, TelegramError> {
        let mut request = TelegramDocumentRequest::new(file_name, file_bytes);
        if let Some(caption) = caption.filter(|caption| !caption.is_empty()) {
            request = request.with_caption(caption);
        }
        self.send_document_rendered(token, surface, request)
    }

    pub fn send_document_rendered(
        &self,
        token: &BotToken,
        surface: &TelegramSurfaceBinding,
        request: TelegramDocumentRequest,
    ) -> Result<SentMessage, TelegramError> {
        if request.file_name.trim().is_empty() {
            return Err(TelegramError::InvalidInput(
                "document file name cannot be empty",
            ));
        }
        if request.file_bytes.is_empty() {
            return Err(TelegramError::InvalidInput("document cannot be empty"));
        }
        let mut fields = surface.document_fields();
        if let Some(caption) = request
            .caption
            .as_deref()
            .filter(|caption| !caption.is_empty())
        {
            fields.push(("caption".into(), caption.to_owned()));
        }
        if request.caption.is_some()
            && let Some(parse_mode) = request.caption_parse_mode
        {
            fields.push(("parse_mode".into(), parse_mode.as_api_value().to_owned()));
        }
        if let Some(reply_markup) = request.reply_markup.as_ref() {
            fields.push(("reply_markup".into(), reply_markup.to_value().to_string()));
        }
        let body = self
            .transport
            .post_multipart(
                &self.api_base,
                token,
                "sendDocument",
                fields,
                request.file_name,
                request.file_bytes,
            )
            .map_err(TelegramError::Transport)?;
        parse_api_response(&body, "sendDocument")
    }

    pub fn answer_callback(
        &self,
        token: &BotToken,
        callback_query_id: &str,
        answer: &CallbackAnswer,
    ) -> Result<bool, TelegramError> {
        if callback_query_id.trim().is_empty() {
            return Err(TelegramError::InvalidInput(
                "callback query id cannot be empty",
            ));
        }
        let mut payload = json!({
            "callback_query_id": callback_query_id,
            "show_alert": answer.show_alert,
        });
        if let Some(text) = answer.text.as_deref().filter(|text| !text.is_empty()) {
            payload["text"] = Value::String(text.to_owned());
        }
        self.call(token, "answerCallbackQuery", payload)
    }

    pub fn answer_callback_query(
        &self,
        token: &BotToken,
        callback_query_id: &str,
        text: Option<&str>,
    ) -> Result<bool, TelegramError> {
        let answer = CallbackAnswer {
            text: text.filter(|text| !text.is_empty()).map(str::to_owned),
            show_alert: false,
        };
        self.answer_callback(token, callback_query_id, &answer)
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

    fn call_rendered(
        &self,
        token: &BotToken,
        method: &'static str,
        request: &TelegramMessageRequest,
        render_payload: impl Fn(&TelegramMessageRequest) -> Value,
    ) -> Result<SentMessage, TelegramError> {
        if request.text.is_empty() {
            return Err(TelegramError::InvalidInput("message text cannot be empty"));
        }
        match self.call(token, method, render_payload(request)) {
            Err(
                error @ TelegramError::ApiRejected {
                    error_code: Some(400),
                    ..
                },
            ) => match request.fallback() {
                Some(fallback) => self.call(token, method, render_payload(&fallback)),
                None => Err(error),
            },
            result => result,
        }
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

fn render_edit_text_payload(
    message: &TelegramMessageReference,
    request: &TelegramMessageRequest,
) -> Value {
    let mut payload = json!({
        "chat_id": message.chat_id,
        "message_id": message.message_id,
        "text": request.text,
    });
    if let Some(parse_mode) = request.parse_mode {
        payload["parse_mode"] = Value::String(parse_mode.as_api_value().to_owned());
    }
    if let Some(reply_markup) = request.reply_markup.as_ref() {
        payload["reply_markup"] = reply_markup.to_value();
    }
    if request.disable_link_preview {
        payload["link_preview_options"] = json!({"is_disabled": true});
    }
    payload
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
pub struct TelegramFile {
    pub file_id: String,
    pub file_path: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ChatInfo {
    pub id: i64,
    #[serde(rename = "type")]
    pub chat_type: String,
    #[serde(default)]
    pub is_forum: bool,
    #[serde(default)]
    pub linked_chat_id: Option<i64>,
    #[serde(default)]
    pub username: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct ChatMemberInfo {
    pub status: String,
    pub user: BotProfile,
    #[serde(default)]
    pub can_post_messages: bool,
    #[serde(default)]
    pub can_edit_messages: bool,
    #[serde(default)]
    pub can_delete_messages: bool,
    #[serde(default)]
    pub is_anonymous: bool,
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
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    type MultipartCall = (&'static str, Vec<(String, String)>, String, Vec<u8>);

    #[derive(Clone, Default)]
    struct RecordingTransport {
        calls: Arc<Mutex<Vec<(&'static str, Value)>>>,
        multipart_calls: Arc<Mutex<Vec<MultipartCall>>>,
        response: Arc<Mutex<String>>,
        response_sequence: Arc<Mutex<VecDeque<String>>>,
    }

    impl RecordingTransport {
        fn responds_with(body: &str) -> Self {
            Self {
                response: Arc::new(Mutex::new(body.to_owned())),
                ..Self::default()
            }
        }

        fn responds_in_order(bodies: &[&str]) -> Self {
            assert!(!bodies.is_empty(), "a response sequence cannot be empty");
            Self {
                response: Arc::new(Mutex::new(bodies[bodies.len() - 1].to_owned())),
                response_sequence: Arc::new(Mutex::new(
                    bodies.iter().map(|body| (*body).to_owned()).collect(),
                )),
                ..Self::default()
            }
        }

        fn next_response(&self) -> String {
            let mut sequence = self.response_sequence.lock().unwrap();
            if sequence.len() > 1 {
                return sequence.pop_front().unwrap();
            }
            if let Some(response) = sequence.front() {
                return response.clone();
            }
            self.response.lock().unwrap().clone()
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
            Ok(self.next_response())
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
            Ok(self.next_response())
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
    fn command_scopes_serialize_to_the_exact_bot_api_shapes() {
        assert_eq!(
            BotCommandScope::Default.to_value(),
            json!({"type": "default"})
        );
        assert_eq!(
            BotCommandScope::AllPrivateChats.to_value(),
            json!({"type": "all_private_chats"})
        );
        assert_eq!(
            BotCommandScope::AllGroupChats.to_value(),
            json!({"type": "all_group_chats"})
        );
        assert_eq!(
            BotCommandScope::AllChatAdministrators.to_value(),
            json!({"type": "all_chat_administrators"})
        );
        assert_eq!(
            BotCommandScope::Chat { chat_id: 9527 }.to_value(),
            json!({"type": "chat", "chat_id": 9527})
        );
        assert_eq!(
            BotCommandScope::ChatAdministrators { chat_id: -1001 }.to_value(),
            json!({"type": "chat_administrators", "chat_id": -1001})
        );
        assert_eq!(
            BotCommandScope::ChatMember {
                chat_id: -1001,
                user_id: 9527,
            }
            .to_value(),
            json!({"type": "chat_member", "chat_id": -1001, "user_id": 9527})
        );
    }

    #[test]
    fn command_menu_methods_keep_bootstrap_and_owner_scopes_distinct() {
        let transport = RecordingTransport::responds_in_order(&[
            r#"{"ok":true,"result":true}"#,
            r#"{"ok":true,"result":true}"#,
        ]);
        let calls = transport.calls.clone();
        let api = TelegramBotApi::new(transport);
        let commands = [
            CommandMenuEntry {
                command: "pair",
                description: "完成 owner 配对",
            },
            CommandMenuEntry {
                command: "help",
                description: "显示帮助",
            },
        ];

        assert!(
            api.set_my_commands(&token(), &commands, BotCommandScope::AllPrivateChats)
                .unwrap()
        );
        assert!(
            api.delete_my_commands(&token(), BotCommandScope::Chat { chat_id: 9527 })
                .unwrap()
        );
        assert!(matches!(
            api.set_my_commands(&token(), &[], BotCommandScope::Default),
            Err(TelegramError::InvalidInput("command menu cannot be empty"))
        ));

        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0, "setMyCommands");
        assert_eq!(
            calls[0].1,
            json!({
                "commands": [
                    {"command": "pair", "description": "完成 owner 配对"},
                    {"command": "help", "description": "显示帮助"},
                ],
                "scope": {"type": "all_private_chats"},
            })
        );
        assert_eq!(calls[1].0, "deleteMyCommands");
        assert_eq!(
            calls[1].1,
            json!({"scope": {"type": "chat", "chat_id": 9527}})
        );
    }

    #[test]
    fn production_transport_preserves_bot_api_bad_requests_for_markup_fallback() {
        assert!(should_parse_bot_api_response(reqwest::StatusCode::OK));
        assert!(should_parse_bot_api_response(
            reqwest::StatusCode::BAD_REQUEST
        ));
        assert!(!should_parse_bot_api_response(
            reqwest::StatusCode::UNAUTHORIZED
        ));
        assert!(!should_parse_bot_api_response(
            reqwest::StatusCode::TOO_MANY_REQUESTS
        ));
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
    fn rendered_send_and_edit_retry_plain_text_after_telegram_format_rejection() {
        let markup = InlineKeyboardMarkup::new(vec![vec![
            InlineKeyboardButton::callback("Retry", "cb:retry").unwrap(),
        ]])
        .unwrap();
        let request = TelegramMessageRequest::markdown_v2("*formatted*", "formatted")
            .with_reply_markup(markup.clone())
            .reply_to(ReplyParameters::new(31).allow_sending_without_reply(true));
        let surface = TelegramSurfaceBinding::Channel(
            ChannelBinding::new("discussion", "-1004290500369").unwrap(),
        );

        let transport = RecordingTransport::responds_in_order(&[
            r#"{"ok":false,"error_code":400,"description":"can't parse entities"}"#,
            r#"{"ok":true,"result":{"message_id":24}}"#,
        ]);
        let calls = transport.calls.clone();
        let api = TelegramBotApi::new(transport);
        assert_eq!(
            api.send_rendered(&token(), &surface, &request)
                .unwrap()
                .message_id,
            24
        );
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0, "sendMessage");
        assert_eq!(calls[0].1["text"], "*formatted*");
        assert_eq!(calls[0].1["parse_mode"], "MarkdownV2");
        assert_eq!(
            calls[0].1["link_preview_options"],
            json!({"is_disabled": true})
        );
        assert_eq!(
            calls[0].1["reply_parameters"],
            json!({"message_id": 31, "allow_sending_without_reply": true})
        );
        assert_eq!(
            calls[0].1["reply_markup"],
            json!({"inline_keyboard": [[{"text": "Retry", "callback_data": "cb:retry"}]]})
        );
        assert_eq!(calls[1].0, "sendMessage");
        assert_eq!(calls[1].1["text"], "formatted");
        assert!(calls[1].1.get("parse_mode").is_none());
        assert_eq!(calls[1].1["reply_markup"], calls[0].1["reply_markup"]);
        drop(calls);

        let transport = RecordingTransport::responds_in_order(&[
            r#"{"ok":false,"error_code":400,"description":"can't parse entities"}"#,
            r#"{"ok":true,"result":{"message_id":25}}"#,
        ]);
        let calls = transport.calls.clone();
        let api = TelegramBotApi::new(transport);
        let reference = TelegramMessageReference::new("-1004290500369", 24).unwrap();
        assert_eq!(
            api.edit_text(&token(), &reference, &request)
                .unwrap()
                .message_id,
            25
        );
        let calls = calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0, "editMessageText");
        assert_eq!(calls[0].1["chat_id"], "-1004290500369");
        assert_eq!(calls[0].1["message_id"], 24);
        assert_eq!(calls[0].1["parse_mode"], "MarkdownV2");
        assert!(calls[0].1.get("reply_parameters").is_none());
        assert_eq!(calls[1].1["text"], "formatted");
        assert!(calls[1].1.get("parse_mode").is_none());
    }

    #[test]
    fn typed_message_mutation_payloads_cover_markup_and_deletion() {
        let transport = RecordingTransport::responds_in_order(&[
            r#"{"ok":true,"result":{"message_id":26}}"#,
            r#"{"ok":true,"result":true}"#,
            r#"{"ok":true,"result":{"message_id":26}}"#,
        ]);
        let calls = transport.calls.clone();
        let api = TelegramBotApi::new(transport);
        let reference = TelegramMessageReference::new("-1004290500369", 26).unwrap();
        let markup = InlineKeyboardMarkup::new(vec![vec![
            InlineKeyboardButton::callback("Cancel", "cb:cancel").unwrap(),
        ]])
        .unwrap();

        assert_eq!(
            api.edit_reply_markup(&token(), &reference, Some(&markup))
                .unwrap()
                .message_id,
            26
        );
        assert!(api.delete_message(&token(), &reference).unwrap());
        assert_eq!(
            api.edit_reply_markup(&token(), &reference, None)
                .unwrap()
                .message_id,
            26
        );

        let calls = calls.lock().unwrap();
        assert_eq!(calls[0].0, "editMessageReplyMarkup");
        assert_eq!(
            calls[0].1["reply_markup"],
            json!({"inline_keyboard": [[{"text": "Cancel", "callback_data": "cb:cancel"}]]})
        );
        assert_eq!(calls[1].0, "deleteMessage");
        assert_eq!(
            calls[1].1,
            json!({"chat_id": "-1004290500369", "message_id": 26})
        );
        assert_eq!(calls[2].0, "editMessageReplyMarkup");
        assert_eq!(calls[2].1["reply_markup"], Value::Null);
    }

    #[test]
    fn typed_document_preserves_native_comment_reply_and_serializes_caption_markup() {
        let transport =
            RecordingTransport::responds_with(r#"{"ok":true,"result":{"message_id":27}}"#);
        let multipart_calls = transport.multipart_calls.clone();
        let api = TelegramBotApi::new(transport);
        let channel = ChannelBinding::new("discussion", "-1004446000549").unwrap();
        let surface = TelegramSurfaceBinding::NativeCommentRoot(
            NativeCommentBinding::new(channel, "-1004290500369", 700).unwrap(),
        );
        let markup = InlineKeyboardMarkup::new(vec![vec![
            InlineKeyboardButton::url("Open", "https://example.test/report").unwrap(),
        ]])
        .unwrap();
        let request = TelegramDocumentRequest::new("report.txt", b"report".to_vec())
            .with_caption("<b>report</b>")
            .with_caption_parse_mode(TelegramParseMode::Html)
            .with_reply_markup(markup);

        assert_eq!(
            api.send_document_rendered(&token(), &surface, request)
                .unwrap()
                .message_id,
            27
        );
        let calls = multipart_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "sendDocument");
        assert_eq!(calls[0].2, "report.txt");
        assert_eq!(calls[0].3, b"report");
        let field = |name: &str| {
            calls[0]
                .1
                .iter()
                .find_map(|(field_name, value)| (field_name == name).then_some(value.as_str()))
                .unwrap()
        };
        assert_eq!(field("caption"), "<b>report</b>");
        assert_eq!(field("parse_mode"), "HTML");
        assert_eq!(
            serde_json::from_str::<Value>(field("reply_markup")).unwrap(),
            json!({"inline_keyboard": [[{"text": "Open", "url": "https://example.test/report"}]]})
        );
        assert_eq!(
            serde_json::from_str::<Value>(field("reply_parameters")).unwrap(),
            json!({"message_id": 700, "allow_sending_without_reply": true})
        );
    }

    #[test]
    fn typed_callback_answers_include_alert_and_legacy_default_fields() {
        let transport = RecordingTransport::responds_in_order(&[
            r#"{"ok":true,"result":true}"#,
            r#"{"ok":true,"result":true}"#,
        ]);
        let calls = transport.calls.clone();
        let api = TelegramBotApi::new(transport);
        assert!(
            api.answer_callback(
                &token(),
                "callback-27",
                &CallbackAnswer::text("Locked").show_alert(true),
            )
            .unwrap()
        );
        assert!(
            api.answer_callback_query(&token(), "callback-28", None)
                .unwrap()
        );

        let calls = calls.lock().unwrap();
        assert_eq!(calls[0].0, "answerCallbackQuery");
        assert_eq!(
            calls[0].1,
            json!({"callback_query_id": "callback-27", "text": "Locked", "show_alert": true})
        );
        assert_eq!(
            calls[1].1,
            json!({"callback_query_id": "callback-28", "show_alert": false})
        );
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
    fn python_command_menus_and_typed_role_routing_are_preserved() {
        let entries = |role, scope| {
            command_menu(role, scope)
                .iter()
                .map(|entry| (entry.command, entry.description))
                .collect::<Vec<_>>()
        };

        assert_eq!(
            entries(RuntimeBotRole::Control, CommandMenuScope::Bootstrap),
            vec![("pair", "完成 owner 配对"), ("help", "显示帮助"),]
        );
        assert_eq!(
            entries(RuntimeBotRole::Control, CommandMenuScope::Owner),
            vec![
                ("sessions", "查找 Codex sessions"),
                ("topics", "查看 Session 帖子"),
                ("new", "创建待认证 Session 帖子"),
                ("perf", "查看 WSL 与 GPU 性能"),
                ("help", "显示帮助"),
            ]
        );
        assert_eq!(
            entries(RuntimeBotRole::Discussion, CommandMenuScope::Bootstrap),
            vec![("bind", "绑定频道讨论组"), ("help", "显示帮助"),]
        );
        assert_eq!(
            entries(RuntimeBotRole::Discussion, CommandMenuScope::Owner),
            vec![
                ("status", "刷新当前 Session 状态"),
                ("totp", "认证当前 Session"),
                ("lock", "锁定当前 Session"),
                ("prompt", "发送 prompt"),
                ("ask", "独立询问 Codex"),
                ("queue", "查看队列或加入 prompt"),
                ("planmode", "进入 Plan Mode"),
                ("review", "执行一次 Codex Review"),
                ("changemodel", "切换当前模式的模型"),
                ("plan", "查看完整计划"),
                ("timeline", "查看近期事件"),
                ("attach", "接入 tmux"),
                ("getfile", "获取本机文件"),
                ("unwatch", "取消关注"),
                ("help", "显示帮助"),
            ]
        );
        assert!(entries(RuntimeBotRole::Status, CommandMenuScope::Owner).is_empty());
        assert!(entries(RuntimeBotRole::Alert, CommandMenuScope::Bootstrap).is_empty());

        let control = UpdateRouter::new(RuntimeBotRole::Control, policy()).unwrap();
        assert!(matches!(
            control.route_effect(&update(json!({
                "message": {"message_id": 9, "chat": {"id": 42}, "text": "/pair"}
            }))),
            RoutedEffect::Dispatch(WorkflowEffect::Command {
                command: TelegramCommand::Pair,
                ..
            })
        ));
        assert_eq!(
            control.route_effect(&update(json!({
                "message": {"message_id": 10, "chat": {"id": 42}, "text": "/bind"}
            }))),
            RoutedEffect::Ignore
        );

        let discussion = UpdateRouter::new(RuntimeBotRole::Discussion, policy()).unwrap();
        assert!(matches!(
            discussion.route_effect(&update(json!({
                "message": {
                    "message_id": 11,
                    "chat": {"id": -1004290500369i64},
                    "text": "/bind"
                }
            }))),
            RoutedEffect::Dispatch(WorkflowEffect::Command {
                command: TelegramCommand::Bind,
                ..
            })
        ));
    }

    #[test]
    fn inbound_updates_normalize_caption_thread_actor_and_callback_metadata() {
        assert_eq!(
            IncomingUpdate::from_update(&update(json!({
                "message": {
                    "message_id": 13,
                    "chat": {"id": -1004290500369i64, "type": "supergroup"},
                    "caption": "/status now",
                    "from": {"id": 77},
                    "message_thread_id": 41,
                    "reply_to_message": {"message_id": 12}
                }
            }))),
            IncomingUpdate::Message(TelegramMessage {
                chat_id: -1004290500369,
                chat_kind: TelegramChatKind::Supergroup,
                message_id: 13,
                text: Some("/status now".into()),
                caption: Some("/status now".into()),
                document_file_id: None,
                document_file_name: None,
                photo_file_id: None,
                is_photo: false,
                actor: TelegramActor {
                    user_id: Some(77),
                    sender_chat_id: None,
                },
                reply_to_message_id: Some(12),
                message_thread_id: Some(41),
                automatic_forward_from_channel: None,
                automatic_forward_channel_post_id: None,
            })
        );
        assert_eq!(
            IncomingUpdate::from_update(&update(json!({
                "callback_query": {
                    "id": "callback-13",
                    "from": {"id": 77},
                    "data": "cb:ticket",
                    "message": {
                        "message_id": 14,
                        "chat": {"id": -1004290500369i64, "type": "supergroup"}
                    }
                }
            }))),
            IncomingUpdate::Callback(TelegramCallback {
                id: "callback-13".into(),
                chat_id: -1004290500369,
                chat_kind: TelegramChatKind::Supergroup,
                message_id: 14,
                data: "cb:ticket".into(),
                actor: TelegramActor {
                    user_id: Some(77),
                    sender_chat_id: None,
                },
            })
        );
    }

    #[test]
    fn strict_authorization_enforces_owner_chat_kind_anonymous_and_bot_addressing() {
        let control = UpdateRouter::new_with_authorization(
            RuntimeBotRole::Control,
            policy(),
            UpdateAuthorization::python_owner(77, "RustControlBot"),
        )
        .unwrap();
        assert!(matches!(
            control.route_effect(&update(json!({
                "message": {
                    "message_id": 15,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 77},
                    "text": "/perf@rustcontrolbot"
                }
            }))),
            RoutedEffect::Dispatch(WorkflowEffect::Command {
                command: TelegramCommand::Perf,
                ..
            })
        ));
        for payload in [
            json!({
                "message": {
                    "message_id": 16,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 77},
                    "text": "/perf@otherbot"
                }
            }),
            json!({
                "message": {
                    "message_id": 17,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 77},
                    "sender_chat": {"id": -1001},
                    "text": "/perf"
                }
            }),
            json!({
                "message": {
                    "message_id": 18,
                    "chat": {"id": 42, "type": "group"},
                    "from": {"id": 77},
                    "text": "/perf"
                }
            }),
            json!({
                "message": {
                    "message_id": 19,
                    "chat": {"id": 43, "type": "private"},
                    "from": {"id": 77},
                    "text": "/perf"
                }
            }),
        ] {
            assert_eq!(control.route_effect(&update(payload)), RoutedEffect::Ignore);
        }

        let discussion = UpdateRouter::new_with_authorization(
            RuntimeBotRole::Discussion,
            policy(),
            UpdateAuthorization::python_owner(77, "RustDiscussionBot"),
        )
        .unwrap();
        assert!(matches!(
            discussion.route_effect(&update(json!({
                "message": {
                    "message_id": 20,
                    "chat": {"id": -1004290500369i64, "type": "supergroup"},
                    "from": {"id": 77},
                    "text": "/status@rustdiscussionbot"
                }
            }))),
            RoutedEffect::Dispatch(WorkflowEffect::Command {
                command: TelegramCommand::Status,
                ..
            })
        ));
        assert!(matches!(
            discussion.route_effect(&update(json!({
                "message": {
                    "message_id": 21,
                    "chat": {"id": -1004290500369i64, "type": "supergroup"},
                    "is_automatic_forward": true,
                    "sender_chat": {"id": -1004446000549i64},
                    "forward_from_message_id": 81
                }
            }))),
            RoutedEffect::Dispatch(WorkflowEffect::NativeCommentPost {
                channel_post_id: 81,
                ..
            })
        ));

        let status = UpdateRouter::new_with_authorization(
            RuntimeBotRole::Status,
            policy(),
            UpdateAuthorization::python_owner(77, "RustStatusBot"),
        )
        .unwrap();
        assert_eq!(
            status.route_effect(&update(json!({
                "callback_query": {
                    "id": "foreign",
                    "from": {"id": 78},
                    "data": "cb:ticket",
                    "message": {
                        "message_id": 22,
                        "chat": {"id": -1004290500369i64, "type": "supergroup"}
                    }
                }
            }))),
            RoutedEffect::Ignore
        );
    }

    #[test]
    fn bootstrap_authorization_blocks_discussion_and_status_until_pairing() {
        let bootstrap = UpdateAuthorization {
            enforce_chat_kind: true,
            reject_sender_chat: true,
            bootstrap_only: true,
            ..UpdateAuthorization::default()
        };
        let discussion = UpdateRouter::new_with_authorization(
            RuntimeBotRole::Discussion,
            policy(),
            bootstrap.clone(),
        )
        .unwrap();
        assert_eq!(
            discussion.route_effect(&update(json!({
                "message": {
                    "message_id": 23,
                    "chat": {"id": -1004290500369i64, "type": "supergroup"},
                    "from": {"id": 77},
                    "text": "/totp 123456"
                }
            }))),
            RoutedEffect::Ignore
        );
        assert!(matches!(
            discussion.route_effect(&update(json!({
                "message": {
                    "message_id": 28,
                    "chat": {"id": -1004290500369i64, "type": "supergroup"},
                    "from": {"id": 77},
                    "text": "/bind"
                }
            }))),
            RoutedEffect::Dispatch(WorkflowEffect::Command {
                command: TelegramCommand::Bind,
                ..
            })
        ));
        assert_eq!(
            discussion.route_effect(&update(json!({
                "callback_query": {
                    "id": "pre-pair",
                    "from": {"id": 77},
                    "data": "cb:status",
                    "message": {
                        "message_id": 24,
                        "chat": {"id": -1004290500369i64, "type": "supergroup"}
                    }
                }
            }))),
            RoutedEffect::Ignore
        );

        let status =
            UpdateRouter::new_with_authorization(RuntimeBotRole::Status, policy(), bootstrap)
                .unwrap();
        assert_eq!(
            status.route_effect(&update(json!({
                "callback_query": {
                    "id": "pre-pair-status",
                    "from": {"id": 77},
                    "data": "cb:status",
                    "message": {
                        "message_id": 25,
                        "chat": {"id": -1004290500369i64, "type": "supergroup"}
                    }
                }
            }))),
            RoutedEffect::Ignore
        );

        let control = UpdateRouter::new_with_authorization(
            RuntimeBotRole::Control,
            policy(),
            UpdateAuthorization {
                enforce_chat_kind: true,
                reject_sender_chat: true,
                bootstrap_only: true,
                ..UpdateAuthorization::default()
            },
        )
        .unwrap();
        assert!(matches!(
            control.route_effect(&update(json!({
                "message": {
                    "message_id": 26,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 77},
                    "text": "/pair"
                }
            }))),
            RoutedEffect::Dispatch(WorkflowEffect::Command {
                command: TelegramCommand::Pair,
                ..
            })
        ));
        assert_eq!(
            control.route_effect(&update(json!({
                "message": {
                    "message_id": 27,
                    "chat": {"id": 42, "type": "private"},
                    "from": {"id": 77},
                    "text": "untrusted prompt"
                }
            }))),
            RoutedEffect::Ignore
        );
    }

    #[test]
    fn callback_tickets_scope_buttons_to_the_expected_role_owner_chat_space_and_generation() {
        let ticket = CallbackTicket {
            nonce: "nonce-7".into(),
            action: "plan_execute".into(),
            bot_role: RuntimeBotRole::Discussion,
            user_id: 77,
            chat_id: -1004290500369,
            space_id: Some("space-1".into()),
            generation: 4,
            expires_at_ms: 2_000,
        };
        assert_eq!(ticket.callback_data().unwrap(), "cb:nonce-7");
        let markup = InlineKeyboardMarkup::new(vec![vec![
            ticket.button("Execute").unwrap(),
            InlineKeyboardButton::url("Open", "https://example.test/plan").unwrap(),
        ]])
        .unwrap();
        assert_eq!(
            markup.to_value(),
            json!({
                "inline_keyboard": [[
                    {"text": "Execute", "callback_data": "cb:nonce-7"},
                    {"text": "Open", "url": "https://example.test/plan"}
                ]]
            })
        );

        let callback = TelegramCallback {
            id: "callback-7".into(),
            chat_id: -1004290500369,
            chat_kind: TelegramChatKind::Supergroup,
            message_id: 23,
            data: "cb:nonce-7".into(),
            actor: TelegramActor {
                user_id: Some(77),
                sender_chat_id: None,
            },
        };
        assert_eq!(
            ticket.validate(
                RuntimeBotRole::Discussion,
                &callback,
                2_000,
                Some("space-1"),
                4,
            ),
            CallbackTicketValidation::Accepted
        );
        assert_eq!(
            ticket.validate(RuntimeBotRole::Status, &callback, 2_000, Some("space-1"), 4,),
            CallbackTicketValidation::WrongRole
        );
        assert_eq!(
            ticket.validate(
                RuntimeBotRole::Discussion,
                &callback,
                2_001,
                Some("space-1"),
                4,
            ),
            CallbackTicketValidation::Expired
        );
        let wrong_owner = TelegramCallback {
            actor: TelegramActor {
                user_id: Some(78),
                sender_chat_id: None,
            },
            ..callback.clone()
        };
        assert_eq!(
            ticket.validate(
                RuntimeBotRole::Discussion,
                &wrong_owner,
                2_000,
                Some("space-1"),
                4,
            ),
            CallbackTicketValidation::WrongUser
        );
        assert_eq!(
            ticket.validate(
                RuntimeBotRole::Discussion,
                &callback,
                2_000,
                Some("other-space"),
                4,
            ),
            CallbackTicketValidation::WrongSpace
        );
        assert_eq!(
            ticket.validate(
                RuntimeBotRole::Discussion,
                &callback,
                2_000,
                Some("space-1"),
                5,
            ),
            CallbackTicketValidation::WrongGeneration
        );
    }

    #[test]
    fn alert_role_cannot_create_an_update_router() {
        assert!(matches!(
            UpdateRouter::new(RuntimeBotRole::Alert, policy()),
            Err(RoutingError::AlertBotMustNotPoll)
        ));
    }
}
