//! Business entities and events for the next-generation bridge.
//!
//! This crate deliberately has no I/O dependencies. Its types are the contract
//! shared by the application engine and infrastructure adapters.

use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

pub type TimestampMs = i64;

macro_rules! identifier {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(DomainError::EmptyIdentifier(stringify!($name)));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

identifier!(SessionId);
identifier!(ApprovalId);
identifier!(ArtifactId);
identifier!(CommandId);
identifier!(EventId);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionStatus {
    Active,
    Paused,
    Closed,
}

impl SessionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Closed => "closed",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub id: SessionId,
    pub title: String,
    pub status: SessionStatus,
    pub created_at_ms: TimestampMs,
    pub updated_at_ms: TimestampMs,
}

impl Session {
    pub fn new(
        id: SessionId,
        title: impl Into<String>,
        now_ms: TimestampMs,
    ) -> Result<Self, DomainError> {
        let title = title.into();
        if title.trim().is_empty() {
            return Err(DomainError::EmptyTitle);
        }
        Ok(Self {
            id,
            title,
            status: SessionStatus::Active,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApprovalDecision {
    Pending,
    Approved,
    Rejected,
    Expired,
}

impl ApprovalDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Expired => "expired",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApprovalAction {
    ExecuteCommand { command: String },
    SendPrompt { prompt: String },
    TransferArtifact { artifact_id: ArtifactId },
}

impl ApprovalAction {
    pub fn summary(&self) -> &str {
        match self {
            Self::ExecuteCommand { command } => command,
            Self::SendPrompt { prompt } => prompt,
            Self::TransferArtifact { artifact_id } => artifact_id.as_str(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub id: ApprovalId,
    pub session_id: SessionId,
    pub action: ApprovalAction,
    pub decision: ApprovalDecision,
    pub requested_at_ms: TimestampMs,
    pub decided_at_ms: Option<TimestampMs>,
}

impl ApprovalRequest {
    pub fn pending(
        id: ApprovalId,
        session_id: SessionId,
        action: ApprovalAction,
        requested_at_ms: TimestampMs,
    ) -> Self {
        Self {
            id,
            session_id,
            action,
            decision: ApprovalDecision::Pending,
            requested_at_ms,
            decided_at_ms: None,
        }
    }

    pub fn decide(
        &mut self,
        decision: ApprovalDecision,
        decided_at_ms: TimestampMs,
    ) -> Result<(), DomainError> {
        if decision == ApprovalDecision::Pending {
            return Err(DomainError::InvalidApprovalTransition);
        }
        if self.decision != ApprovalDecision::Pending {
            return Err(DomainError::ApprovalAlreadyDecided);
        }
        self.decision = decision;
        self.decided_at_ms = Some(decided_at_ms);
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    pub id: ArtifactId,
    pub session_id: SessionId,
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
    pub created_at_ms: TimestampMs,
}

impl Artifact {
    pub fn new(
        id: ArtifactId,
        session_id: SessionId,
        path: impl Into<String>,
        sha256: impl Into<String>,
        bytes: u64,
        created_at_ms: TimestampMs,
    ) -> Result<Self, DomainError> {
        let path = path.into();
        let sha256 = sha256.into();
        if path.trim().is_empty() {
            return Err(DomainError::EmptyArtifactPath);
        }
        if sha256.len() != 64 || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(DomainError::InvalidSha256);
        }
        Ok(Self {
            id,
            session_id,
            path,
            sha256: sha256.to_ascii_lowercase(),
            bytes,
            created_at_ms,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledCommand {
    pub id: CommandId,
    pub session_id: SessionId,
    pub prompt: String,
    pub enqueued_at_ms: TimestampMs,
}

impl ScheduledCommand {
    pub fn new(
        id: CommandId,
        session_id: SessionId,
        prompt: impl Into<String>,
        enqueued_at_ms: TimestampMs,
    ) -> Result<Self, DomainError> {
        let prompt = prompt.into();
        if prompt.trim().is_empty() {
            return Err(DomainError::EmptyPrompt);
        }
        Ok(Self {
            id,
            session_id,
            prompt,
            enqueued_at_ms,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DomainEventKind {
    SessionCreated { session: Session },
    ApprovalRequested { approval: ApprovalRequest },
    ApprovalDecided { approval: ApprovalRequest },
    ArtifactRecorded { artifact: Artifact },
    CommandScheduled { command: ScheduledCommand },
}

impl DomainEventKind {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::SessionCreated { .. } => "session.created",
            Self::ApprovalRequested { .. } => "approval.requested",
            Self::ApprovalDecided { .. } => "approval.decided",
            Self::ArtifactRecorded { .. } => "artifact.recorded",
            Self::CommandScheduled { .. } => "command.scheduled",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainEvent {
    pub id: EventId,
    pub occurred_at_ms: TimestampMs,
    pub kind: DomainEventKind,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DomainError {
    #[error("{0} cannot be empty")]
    EmptyIdentifier(&'static str),
    #[error("session title cannot be empty")]
    EmptyTitle,
    #[error("artifact path cannot be empty")]
    EmptyArtifactPath,
    #[error("artifact digest must be a 64-character hexadecimal SHA-256")]
    InvalidSha256,
    #[error("command prompt cannot be empty")]
    EmptyPrompt,
    #[error("approval can only transition from pending to a terminal decision")]
    InvalidApprovalTransition,
    #[error("approval was already decided")]
    ApprovalAlreadyDecided,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn approval_can_only_be_decided_once() {
        let mut approval = ApprovalRequest::pending(
            ApprovalId::new("approval-1").unwrap(),
            SessionId::new("session-1").unwrap(),
            ApprovalAction::SendPrompt {
                prompt: "Implement the plan.".into(),
            },
            10,
        );

        approval.decide(ApprovalDecision::Approved, 20).unwrap();
        assert_eq!(approval.decided_at_ms, Some(20));
        assert_eq!(
            approval.decide(ApprovalDecision::Rejected, 30),
            Err(DomainError::ApprovalAlreadyDecided)
        );
    }

    #[test]
    fn artifact_requires_a_sha256_digest() {
        let artifact = Artifact::new(
            ArtifactId::new("artifact-1").unwrap(),
            SessionId::new("session-1").unwrap(),
            "report.txt",
            "not-a-digest",
            3,
            1,
        );
        assert_eq!(artifact, Err(DomainError::InvalidSha256));
    }
}
