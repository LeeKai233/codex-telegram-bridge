//! Ports implemented by adapters such as SQLite, Telegram, and Codex.

use async_trait::async_trait;
use ctg_domain::{
    AgentEvent, AgentServerRequest, AgentThread, AgentTurn, ApprovalAction, ApprovalDecision,
    ApprovalId, ApprovalRequest, Artifact, DomainEvent, PromptInput, Session, SessionId,
    TelegramEffect, ThreadId, TimestampMs, TurnId,
};
use serde_json::Value;
use thiserror::Error;

pub type PortResult<T> = Result<T, PortError>;

#[async_trait]
pub trait TelegramGateway: Send + Sync {
    async fn apply(&self, effect: TelegramEffect) -> PortResult<Option<ctg_domain::MessageRef>>;
}

pub trait PromptIntentStore: Send + Sync {
    fn insert_prompt_intent(&self, intent: &ctg_domain::PromptIntent) -> PortResult<()>;
    fn update_prompt_intent(&self, intent: &ctg_domain::PromptIntent) -> PortResult<()>;
    fn find_by_client_message_id(
        &self,
        client_message_id: &str,
    ) -> PortResult<Option<ctg_domain::PromptIntent>>;
}

pub trait QuestionStore: Send + Sync {
    fn insert_question(&self, request: &ctg_domain::QuestionRequest) -> PortResult<()>;
    fn resolve_question(
        &self,
        request_key: &str,
        response: Value,
        source: &str,
        resolved_at_ms: TimestampMs,
    ) -> PortResult<()>;
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PortError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("policy denied: {0}")]
    Denied(String),
    #[error("adapter failure: {0}")]
    Adapter(String),
}

pub trait Clock: Send + Sync {
    fn now_ms(&self) -> TimestampMs;
}

pub trait SessionRepository: Send + Sync {
    fn insert_session(&self, session: &Session, event: &DomainEvent) -> PortResult<()>;
    fn get_session(&self, id: &SessionId) -> PortResult<Option<Session>>;
}

pub trait Scheduler: Send + Sync {
    fn enqueue(&self, command: &ctg_domain::ScheduledCommand) -> PortResult<()>;
}

pub trait Policy: Send + Sync {
    fn authorize(&self, session: &Session, action: &ApprovalAction) -> PortResult<()>;
}

pub trait ApprovalStore: Send + Sync {
    fn insert_approval(&self, approval: &ApprovalRequest, event: &DomainEvent) -> PortResult<()>;
    fn get_approval(&self, id: &ApprovalId) -> PortResult<Option<ApprovalRequest>>;
    fn decide_approval(&self, approval: &ApprovalRequest, event: &DomainEvent) -> PortResult<()>;
}

pub trait ArtifactStore: Send + Sync {
    fn insert_artifact(&self, artifact: &Artifact, event: &DomainEvent) -> PortResult<()>;
    fn list_artifacts(&self, session_id: &SessionId) -> PortResult<Vec<Artifact>>;
}

pub trait EventLog: Send + Sync {
    fn append(&self, event: &DomainEvent) -> PortResult<()>;
}

pub trait ApprovalIdGenerator: Send + Sync {
    fn next_approval_id(&self) -> ApprovalId;
}

pub trait EventIdGenerator: Send + Sync {
    fn next_event_id(&self) -> ctg_domain::EventId;
}

pub trait SessionIdGenerator: Send + Sync {
    fn next_session_id(&self) -> SessionId;
}

pub trait DecisionValidator: Send + Sync {
    fn terminal_decision(&self, decision: ApprovalDecision) -> PortResult<()>;
}

/// Transport-neutral Codex boundary.  Telegram controllers depend on this
/// trait, so they can use the managed Unix app-server on Linux/WSL or another
/// implementation on Windows without importing transport details.
#[async_trait]
pub trait AgentBackend: Send + Sync {
    async fn start_thread(
        &self,
        cwd: &str,
        ephemeral: bool,
        read_only: bool,
    ) -> PortResult<AgentThread>;
    async fn resume_thread(&self, thread_id: &ThreadId) -> PortResult<AgentThread>;
    async fn read_thread(&self, thread_id: &ThreadId, include_turns: bool) -> PortResult<Value>;
    async fn list_threads(&self, limit: u32, cursor: Option<&str>) -> PortResult<Value>;
    async fn start_turn(
        &self,
        thread_id: &ThreadId,
        input: Vec<PromptInput>,
        client_message_id: Option<&str>,
    ) -> PortResult<AgentTurn>;
    async fn steer_turn(
        &self,
        thread_id: &ThreadId,
        expected_turn_id: &TurnId,
        input: Vec<PromptInput>,
        client_message_id: Option<&str>,
    ) -> PortResult<TurnId>;
    async fn respond(&self, request_id: Value, result: Value) -> PortResult<()>;
    async fn respond_error(&self, request_id: Value, code: i64, message: &str) -> PortResult<()>;
    fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<AgentEvent>;
    fn subscribe_server_requests(&self) -> tokio::sync::broadcast::Receiver<AgentServerRequest>;
}

/// Optional physical approval channel. The core remains usable when no
/// Telegram approval Bot is configured, but high-risk callers must treat
/// `Unavailable` as a hard deny rather than silently executing.
pub trait ApprovalGateway: Send + Sync {
    fn availability(&self) -> ApprovalAvailability;
    fn publish(&self, approval: &ApprovalRequest) -> PortResult<ApprovalDelivery>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ApprovalAvailability {
    Available,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApprovalDelivery {
    pub external_reference: String,
}

/// Optional artifact transport. Durable storage is independent from delivery,
/// so an unavailable Telegram/file adapter retains the artifact for retry.
pub trait ArtifactTransport: Send + Sync {
    fn availability(&self) -> ArtifactAvailability;
    fn retain(&self, artifact: &Artifact) -> PortResult<()>;
    fn transfer(&self, artifact: &Artifact) -> PortResult<ArtifactTransfer>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ArtifactAvailability {
    Available,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactTransfer {
    pub external_reference: String,
}
