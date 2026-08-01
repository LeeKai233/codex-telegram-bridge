//! Application service orchestration over the domain and adapter ports.

use ctg_domain::{
    ApprovalAction, ApprovalDecision, ApprovalId, ApprovalRequest, Artifact, DomainError,
    DomainEvent, DomainEventKind, ScheduledCommand, Session, SessionId, TimestampMs,
};
use ctg_ports::{
    ApprovalIdGenerator, ApprovalStore, ArtifactStore, Clock, EventIdGenerator, Policy, PortError,
    Scheduler, SessionIdGenerator, SessionRepository,
};
use thiserror::Error;

pub struct Engine<'a> {
    clock: &'a dyn Clock,
    sessions: &'a dyn SessionRepository,
    approvals: &'a dyn ApprovalStore,
    artifacts: &'a dyn ArtifactStore,
    scheduler: &'a dyn Scheduler,
    policy: &'a dyn Policy,
    session_ids: &'a dyn SessionIdGenerator,
    approval_ids: &'a dyn ApprovalIdGenerator,
    event_ids: &'a dyn EventIdGenerator,
}

pub struct EngineDependencies<'a> {
    pub clock: &'a dyn Clock,
    pub sessions: &'a dyn SessionRepository,
    pub approvals: &'a dyn ApprovalStore,
    pub artifacts: &'a dyn ArtifactStore,
    pub scheduler: &'a dyn Scheduler,
    pub policy: &'a dyn Policy,
    pub session_ids: &'a dyn SessionIdGenerator,
    pub approval_ids: &'a dyn ApprovalIdGenerator,
    pub event_ids: &'a dyn EventIdGenerator,
}

impl<'a> Engine<'a> {
    pub fn new(dependencies: EngineDependencies<'a>) -> Self {
        Self {
            clock: dependencies.clock,
            sessions: dependencies.sessions,
            approvals: dependencies.approvals,
            artifacts: dependencies.artifacts,
            scheduler: dependencies.scheduler,
            policy: dependencies.policy,
            session_ids: dependencies.session_ids,
            approval_ids: dependencies.approval_ids,
            event_ids: dependencies.event_ids,
        }
    }

    pub fn create_session(&self, title: impl Into<String>) -> EngineResult<Session> {
        let now = self.clock.now_ms();
        let session = Session::new(self.session_ids.next_session_id(), title, now)?;
        let event = self.event(
            now,
            DomainEventKind::SessionCreated {
                session: session.clone(),
            },
        );
        self.sessions.insert_session(&session, &event)?;
        Ok(session)
    }

    pub fn request_approval(
        &self,
        session_id: &SessionId,
        action: ApprovalAction,
    ) -> EngineResult<ApprovalRequest> {
        let session = self.required_session(session_id)?;
        self.policy.authorize(&session, &action)?;
        let now = self.clock.now_ms();
        let approval = ApprovalRequest::pending(
            self.approval_ids.next_approval_id(),
            session_id.clone(),
            action,
            now,
        );
        let event = self.event(
            now,
            DomainEventKind::ApprovalRequested {
                approval: approval.clone(),
            },
        );
        self.approvals.insert_approval(&approval, &event)?;
        Ok(approval)
    }

    pub fn decide_approval(
        &self,
        approval_id: &ApprovalId,
        decision: ApprovalDecision,
    ) -> EngineResult<ApprovalRequest> {
        let mut approval = self
            .approvals
            .get_approval(approval_id)?
            .ok_or_else(|| EngineError::MissingApproval(approval_id.to_string()))?;
        let now = self.clock.now_ms();
        approval.decide(decision, now)?;
        let event = self.event(
            now,
            DomainEventKind::ApprovalDecided {
                approval: approval.clone(),
            },
        );
        self.approvals.decide_approval(&approval, &event)?;
        Ok(approval)
    }

    pub fn schedule(&self, command: ScheduledCommand) -> EngineResult<()> {
        let session = self.required_session(&command.session_id)?;
        self.policy.authorize(
            &session,
            &ApprovalAction::SendPrompt {
                prompt: command.prompt.clone(),
            },
        )?;
        self.scheduler.enqueue(&command)?;
        Ok(())
    }

    pub fn record_artifact(&self, artifact: Artifact) -> EngineResult<()> {
        self.required_session(&artifact.session_id)?;
        let event = self.event(
            self.clock.now_ms(),
            DomainEventKind::ArtifactRecorded {
                artifact: artifact.clone(),
            },
        );
        self.artifacts.insert_artifact(&artifact, &event)?;
        Ok(())
    }

    fn required_session(&self, session_id: &SessionId) -> EngineResult<Session> {
        self.sessions
            .get_session(session_id)?
            .ok_or_else(|| EngineError::MissingSession(session_id.to_string()))
    }

    fn event(&self, occurred_at_ms: TimestampMs, kind: DomainEventKind) -> DomainEvent {
        DomainEvent {
            id: self.event_ids.next_event_id(),
            occurred_at_ms,
            kind,
        }
    }
}

pub type EngineResult<T> = Result<T, EngineError>;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Port(#[from] PortError),
    #[error("session does not exist: {0}")]
    MissingSession(String),
    #[error("approval does not exist: {0}")]
    MissingApproval(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use ctg_domain::{ApprovalId, EventId};
    use ctg_ports::{ArtifactStore, PortResult};
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct FixedClock;
    impl Clock for FixedClock {
        fn now_ms(&self) -> TimestampMs {
            100
        }
    }

    struct Ids;
    impl SessionIdGenerator for Ids {
        fn next_session_id(&self) -> SessionId {
            SessionId::new("session-1").unwrap()
        }
    }
    impl ApprovalIdGenerator for Ids {
        fn next_approval_id(&self) -> ApprovalId {
            ApprovalId::new("approval-1").unwrap()
        }
    }
    impl EventIdGenerator for Ids {
        fn next_event_id(&self) -> EventId {
            EventId::new("event-1").unwrap()
        }
    }

    #[derive(Default)]
    struct MemoryStore {
        sessions: Mutex<HashMap<String, Session>>,
        approvals: Mutex<HashMap<String, ApprovalRequest>>,
    }
    impl SessionRepository for MemoryStore {
        fn insert_session(&self, session: &Session, _: &DomainEvent) -> PortResult<()> {
            self.sessions
                .lock()
                .unwrap()
                .insert(session.id.to_string(), session.clone());
            Ok(())
        }
        fn get_session(&self, id: &SessionId) -> PortResult<Option<Session>> {
            Ok(self.sessions.lock().unwrap().get(id.as_str()).cloned())
        }
    }
    impl ApprovalStore for MemoryStore {
        fn insert_approval(&self, approval: &ApprovalRequest, _: &DomainEvent) -> PortResult<()> {
            self.approvals
                .lock()
                .unwrap()
                .insert(approval.id.to_string(), approval.clone());
            Ok(())
        }
        fn get_approval(&self, id: &ApprovalId) -> PortResult<Option<ApprovalRequest>> {
            Ok(self.approvals.lock().unwrap().get(id.as_str()).cloned())
        }
        fn decide_approval(&self, approval: &ApprovalRequest, _: &DomainEvent) -> PortResult<()> {
            self.approvals
                .lock()
                .unwrap()
                .insert(approval.id.to_string(), approval.clone());
            Ok(())
        }
    }
    impl ArtifactStore for MemoryStore {
        fn insert_artifact(&self, _: &Artifact, _: &DomainEvent) -> PortResult<()> {
            Ok(())
        }
        fn list_artifacts(&self, _: &SessionId) -> PortResult<Vec<Artifact>> {
            Ok(vec![])
        }
    }
    struct Allow;
    impl Policy for Allow {
        fn authorize(&self, _: &Session, _: &ApprovalAction) -> PortResult<()> {
            Ok(())
        }
    }
    struct NoopScheduler;
    impl Scheduler for NoopScheduler {
        fn enqueue(&self, _: &ScheduledCommand) -> PortResult<()> {
            Ok(())
        }
    }

    #[test]
    fn approval_lifecycle_is_orchestrated_through_ports() {
        let clock = FixedClock;
        let ids = Ids;
        let store = MemoryStore::default();
        let engine = Engine::new(EngineDependencies {
            clock: &clock,
            sessions: &store,
            approvals: &store,
            artifacts: &store,
            scheduler: &NoopScheduler,
            policy: &Allow,
            session_ids: &ids,
            approval_ids: &ids,
            event_ids: &ids,
        });
        let session = engine.create_session("A session").unwrap();
        let approval = engine
            .request_approval(
                &session.id,
                ApprovalAction::SendPrompt {
                    prompt: "continue".into(),
                },
            )
            .unwrap();
        let decided = engine
            .decide_approval(&approval.id, ApprovalDecision::Approved)
            .unwrap();
        assert_eq!(decided.decision, ApprovalDecision::Approved);
    }
}
