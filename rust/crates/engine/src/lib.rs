//! Application service orchestration over the domain and adapter ports.

use ctg_domain::{
    ApprovalAction, ApprovalDecision, ApprovalId, ApprovalRequest, Artifact, DomainError,
    DomainEvent, DomainEventKind, ScheduledCommand, Session, SessionId, TimestampMs,
};
use ctg_ports::{
    ApprovalAvailability, ApprovalGateway, ApprovalIdGenerator, ApprovalStore, ArtifactStore,
    Clock, EventIdGenerator, Policy, PortError, Scheduler, SessionIdGenerator, SessionRepository,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use thiserror::Error;

pub mod projector;

pub use projector::{
    EventProjector, MAX_PROJECTION_ITEMS, ProjectionEffect, ThreadProjection,
    project_item_subagents, truncate_items,
};

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

/// The engine intentionally holds no concrete Codex client.  Controllers can
/// accept an `AgentBackend` alongside this durable business engine, keeping
/// Telegram, Claude, and future transports outside the core.
pub trait EngineAgentBackend: ctg_ports::AgentBackend {}
impl<T: ctg_ports::AgentBackend + ?Sized> EngineAgentBackend for T {}

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

    /// High-risk actions require a configured physical approval gateway. The
    /// absence of Bot 69 (or another adapter) is a deny, never an implicit
    /// approval.
    pub fn request_high_risk_approval(
        &self,
        session_id: &SessionId,
        action: ApprovalAction,
        gateway: &dyn ApprovalGateway,
    ) -> EngineResult<ApprovalRequest> {
        if gateway.availability() != ApprovalAvailability::Available {
            return Err(EngineError::Port(PortError::Denied(
                "high-risk approval gateway is unavailable".into(),
            )));
        }
        let approval = self.request_approval(session_id, action)?;
        gateway.publish(&approval)?;
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

/// The durable identity of a queue.  A SessionSpace generation is part of the
/// key, so work from a closed/reopened Telegram surface cannot be delivered to
/// the replacement space.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct WorkflowScope {
    pub thread_id: String,
    pub space_id: Option<String>,
    pub space_generation: u64,
}

impl WorkflowScope {
    pub fn legacy(thread_id: impl Into<String>) -> Self {
        Self {
            thread_id: thread_id.into(),
            space_id: None,
            space_generation: 0,
        }
    }

    pub fn space(
        thread_id: impl Into<String>,
        space_id: impl Into<String>,
        space_generation: u64,
    ) -> Self {
        Self {
            thread_id: thread_id.into(),
            space_id: Some(space_id.into()),
            space_generation,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromptIntentState {
    Received,
    AwaitingChoice,
    Queued,
    Submitting,
    Started,
    Steered,
    Completed,
    Failed,
    Uncertain,
    Cancelled,
}

impl PromptIntentState {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Uncertain
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromptIntent {
    pub client_message_id: String,
    pub scope: WorkflowScope,
    pub state: PromptIntentState,
    pub turn_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TurnTerminalStatus {
    Completed,
    Failed,
    Interrupted,
}

impl TurnTerminalStatus {
    pub const fn intent_state(self) -> PromptIntentState {
        match self {
            Self::Completed => PromptIntentState::Completed,
            Self::Failed => PromptIntentState::Failed,
            Self::Interrupted => PromptIntentState::Cancelled,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeliveryObservation {
    Present {
        turn_id: String,
        terminal_status: Option<TurnTerminalStatus>,
    },
    Absent,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingRequestKind {
    UserInput,
    Approval,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingRequestState {
    Available,
    Claimed,
    Responded,
    Resolved,
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingServerRequest {
    pub request_key: String,
    pub request_id: String,
    pub generation: u64,
    pub scope: WorkflowScope,
    pub kind: PendingRequestKind,
    pub state: PendingRequestState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanPublication {
    pub scope: WorkflowScope,
    pub turn_id: String,
    pub item_id: String,
    /// The adapter supplies a deterministic content revision key.  The engine
    /// deliberately does not hash Telegram-visible text on its own.
    pub revision_key: String,
    pub text: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodexWorkflowEffect {
    ReconcileThread {
        scope: WorkflowScope,
    },
    RetryQueue {
        scope: WorkflowScope,
    },
    PromptFinished {
        client_message_id: String,
        scope: WorkflowScope,
        turn_id: String,
        status: TurnTerminalStatus,
    },
    RetireServerRequest {
        request_key: String,
    },
    ServerRequestResolved {
        request_key: String,
    },
    PublishPlan {
        publication: PlanPublication,
    },
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum CodexWorkflowError {
    #[error("client_message_id must not be empty")]
    EmptyClientMessageId,
    #[error("workflow scope must include a thread id")]
    EmptyThreadId,
    #[error("prompt intent collides with a different workflow scope: {0}")]
    IntentCollision(String),
    #[error("unknown prompt intent: {0}")]
    UnknownPrompt(String),
    #[error("invalid prompt transition from {from:?} to {to:?}")]
    InvalidPromptTransition {
        from: PromptIntentState,
        to: PromptIntentState,
    },
    #[error("server request belongs to generation {actual}, current generation is {expected}")]
    StaleGeneration { expected: u64, actual: u64 },
    #[error("unknown server request: {0}")]
    UnknownServerRequest(String),
    #[error("server request is no longer claimable: {0}")]
    ServerRequestUnavailable(String),
    #[error("plan publication is missing item id or revision key")]
    InvalidPlanPublication,
}

/// Transport-neutral projection for the non-Telegram half of the bridge.
///
/// It accepts already-normalized app-server events, keeps every recovery
/// decision explicit, and emits effects for the adapter to persist/deliver.
/// Storage, JSON-RPC, and Telegram I/O remain ports owned by their adapters.
#[derive(Debug, Default)]
pub struct CodexWorkflowProjection {
    connection_generation: Option<u64>,
    connected: bool,
    intents: BTreeMap<String, PromptIntent>,
    queues: BTreeMap<WorkflowScope, VecDeque<String>>,
    pending_requests: BTreeMap<String, PendingServerRequest>,
    terminal_turns: BTreeMap<(String, String), TurnTerminalStatus>,
    plans: BTreeMap<(WorkflowScope, String), PlanPublication>,
}

impl CodexWorkflowProjection {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn connection_generation(&self) -> Option<u64> {
        self.connection_generation
    }

    pub fn prompt_intent(&self, client_message_id: &str) -> Option<&PromptIntent> {
        self.intents.get(client_message_id)
    }

    pub fn pending_request(&self, request_key: &str) -> Option<&PendingServerRequest> {
        self.pending_requests.get(request_key)
    }

    pub fn queued_client_message_ids(&self, scope: &WorkflowScope) -> Vec<String> {
        self.queues
            .get(scope)
            .map(|queue| queue.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// A new healthy generation retires interactive callbacks from older
    /// connections and asks the adapter to reconcile every nonterminal turn
    /// before any queue retry.  This matches the Python bridge's fail-closed
    /// behavior for ambiguous delivery.
    pub fn observe_connection(
        &mut self,
        generation: u64,
        connected: bool,
    ) -> Result<Vec<CodexWorkflowEffect>, CodexWorkflowError> {
        if let Some(current) = self.connection_generation
            && generation < current
        {
            return Err(CodexWorkflowError::StaleGeneration {
                expected: current,
                actual: generation,
            });
        }
        let generation_changed = self.connection_generation != Some(generation);
        self.connection_generation = Some(generation);
        self.connected = connected;
        if !connected || !generation_changed {
            return Ok(Vec::new());
        }

        let mut effects = Vec::new();
        for request in self.pending_requests.values_mut() {
            if request.generation != generation
                && matches!(
                    request.state,
                    PendingRequestState::Available
                        | PendingRequestState::Claimed
                        | PendingRequestState::Responded
                )
            {
                request.state = PendingRequestState::Retired;
                effects.push(CodexWorkflowEffect::RetireServerRequest {
                    request_key: request.request_key.clone(),
                });
            }
        }

        let mut scopes = BTreeSet::new();
        for intent in self.intents.values_mut() {
            if intent.state == PromptIntentState::Submitting {
                intent.state = PromptIntentState::Uncertain;
            }
            if !intent.state.is_terminal() || intent.state == PromptIntentState::Uncertain {
                scopes.insert(intent.scope.clone());
            }
        }
        scopes.extend(self.queues.keys().cloned());
        effects.extend(
            scopes
                .into_iter()
                .map(|scope| CodexWorkflowEffect::ReconcileThread { scope }),
        );
        Ok(effects)
    }

    pub fn create_prompt(
        &mut self,
        client_message_id: impl Into<String>,
        scope: WorkflowScope,
    ) -> Result<PromptIntent, CodexWorkflowError> {
        let client_message_id = client_message_id.into();
        if client_message_id.trim().is_empty() {
            return Err(CodexWorkflowError::EmptyClientMessageId);
        }
        validate_scope(&scope)?;
        if let Some(existing) = self.intents.get(&client_message_id) {
            if existing.scope == scope {
                return Ok(existing.clone());
            }
            return Err(CodexWorkflowError::IntentCollision(client_message_id));
        }
        let intent = PromptIntent {
            client_message_id: client_message_id.clone(),
            scope,
            state: PromptIntentState::Received,
            turn_id: None,
        };
        self.intents.insert(client_message_id, intent.clone());
        Ok(intent)
    }

    pub fn queue_prompt(&mut self, client_message_id: &str) -> Result<(), CodexWorkflowError> {
        let scope = {
            let intent = self.required_intent_mut(client_message_id)?;
            transition_prompt(intent, PromptIntentState::Queued)?;
            intent.scope.clone()
        };
        let queue = self.queues.entry(scope).or_default();
        if !queue.iter().any(|value| value == client_message_id) {
            queue.push_back(client_message_id.to_owned());
        }
        Ok(())
    }

    pub fn await_choice(&mut self, client_message_id: &str) -> Result<(), CodexWorkflowError> {
        let intent = self.required_intent_mut(client_message_id)?;
        transition_prompt(intent, PromptIntentState::AwaitingChoice)
    }

    /// Mark a queued prompt as sent to Codex while preserving it in the queue
    /// until a turn ID or a proven absence resolves the delivery ambiguity.
    pub fn begin_dispatch(
        &mut self,
        scope: &WorkflowScope,
    ) -> Result<Option<PromptIntent>, CodexWorkflowError> {
        let next = self.queues.get(scope).and_then(VecDeque::front).cloned();
        let Some(client_message_id) = next else {
            return Ok(None);
        };
        let intent = self.required_intent_mut(&client_message_id)?;
        if intent.state != PromptIntentState::Queued {
            return Ok(None);
        }
        transition_prompt(intent, PromptIntentState::Submitting)?;
        Ok(Some(intent.clone()))
    }

    pub fn observe_delivery(
        &mut self,
        client_message_id: &str,
        observation: DeliveryObservation,
    ) -> Result<Vec<CodexWorkflowEffect>, CodexWorkflowError> {
        let follow_up = match &observation {
            DeliveryObservation::Absent => Some(false),
            DeliveryObservation::Unknown => Some(true),
            DeliveryObservation::Present { .. } => None,
        };
        let mut terminal = None;
        let scope = {
            let intent = self.required_intent_mut(client_message_id)?;
            if matches!(
                intent.state,
                PromptIntentState::Completed
                    | PromptIntentState::Failed
                    | PromptIntentState::Cancelled
            ) {
                return Ok(Vec::new());
            }
            match observation {
                DeliveryObservation::Present {
                    turn_id,
                    terminal_status,
                } => {
                    if turn_id.trim().is_empty() {
                        return Err(CodexWorkflowError::InvalidPromptTransition {
                            from: intent.state,
                            to: PromptIntentState::Started,
                        });
                    }
                    if intent.state == PromptIntentState::Started
                        && intent.turn_id.as_deref() != Some(turn_id.as_str())
                    {
                        return Err(CodexWorkflowError::InvalidPromptTransition {
                            from: intent.state,
                            to: PromptIntentState::Started,
                        });
                    }
                    intent.turn_id = Some(turn_id);
                    if let Some(status) = terminal_status {
                        intent.state = status.intent_state();
                        terminal = Some(status);
                    } else {
                        intent.state = PromptIntentState::Started;
                    }
                }
                DeliveryObservation::Absent => {
                    if matches!(
                        intent.state,
                        PromptIntentState::Submitting | PromptIntentState::Uncertain
                    ) {
                        intent.state = PromptIntentState::Queued;
                    }
                }
                DeliveryObservation::Unknown => {
                    if !intent.state.is_terminal() {
                        intent.state = PromptIntentState::Uncertain;
                    }
                }
            }
            intent.scope.clone()
        };
        if let Some(status) = terminal {
            return self.finish_prompt(client_message_id, status);
        }
        match follow_up {
            Some(false) => Ok(vec![CodexWorkflowEffect::RetryQueue { scope }]),
            Some(true) => Ok(vec![CodexWorkflowEffect::ReconcileThread { scope }]),
            None => Ok(Vec::new()),
        }
    }

    pub fn observe_turn_started(
        &mut self,
        client_message_id: &str,
        turn_id: impl Into<String>,
    ) -> Result<Vec<CodexWorkflowEffect>, CodexWorkflowError> {
        let turn_id = turn_id.into();
        let scope = {
            let intent = self.required_intent_mut(client_message_id)?;
            if turn_id.trim().is_empty() || intent.scope.thread_id.trim().is_empty() {
                return Err(CodexWorkflowError::InvalidPromptTransition {
                    from: intent.state,
                    to: PromptIntentState::Started,
                });
            }
            if intent.state == PromptIntentState::Started
                && intent.turn_id.as_deref() == Some(turn_id.as_str())
            {
                intent.scope.clone()
            } else if !matches!(
                intent.state,
                PromptIntentState::Submitting
                    | PromptIntentState::Queued
                    | PromptIntentState::AwaitingChoice
            ) {
                return Err(CodexWorkflowError::InvalidPromptTransition {
                    from: intent.state,
                    to: PromptIntentState::Started,
                });
            } else {
                intent.turn_id = Some(turn_id.clone());
                intent.state = PromptIntentState::Started;
                intent.scope.clone()
            }
        };
        if let Some(status) = self
            .terminal_turns
            .get(&(scope.thread_id.clone(), turn_id))
            .copied()
        {
            return self.finish_prompt(client_message_id, status);
        }
        Ok(Vec::new())
    }

    pub fn observe_turn_completed(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        status: TurnTerminalStatus,
    ) -> Vec<CodexWorkflowEffect> {
        if thread_id.trim().is_empty() || turn_id.trim().is_empty() {
            return Vec::new();
        }
        self.terminal_turns
            .insert((thread_id.to_owned(), turn_id.to_owned()), status);
        let affected = self
            .intents
            .iter()
            .filter_map(|(key, intent)| {
                (intent.scope.thread_id == thread_id
                    && intent.turn_id.as_deref() == Some(turn_id)
                    && !intent.state.is_terminal())
                .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        affected
            .into_iter()
            .flat_map(|client_message_id| {
                self.finish_prompt(&client_message_id, status)
                    .unwrap_or_default()
            })
            .collect()
    }

    pub fn observe_plan(
        &mut self,
        publication: PlanPublication,
    ) -> Result<Vec<CodexWorkflowEffect>, CodexWorkflowError> {
        validate_scope(&publication.scope)?;
        if publication.item_id.trim().is_empty() || publication.revision_key.trim().is_empty() {
            return Err(CodexWorkflowError::InvalidPlanPublication);
        }
        let key = (publication.scope.clone(), publication.item_id.clone());
        if self
            .plans
            .get(&key)
            .is_some_and(|existing| existing.revision_key == publication.revision_key)
        {
            return Ok(Vec::new());
        }
        self.plans.insert(key, publication.clone());
        Ok(vec![CodexWorkflowEffect::PublishPlan { publication }])
    }

    pub fn register_server_request(
        &mut self,
        mut request: PendingServerRequest,
    ) -> Result<bool, CodexWorkflowError> {
        validate_scope(&request.scope)?;
        let current = self
            .connection_generation
            .ok_or(CodexWorkflowError::StaleGeneration {
                expected: 0,
                actual: request.generation,
            })?;
        if current != request.generation || !self.connected {
            return Err(CodexWorkflowError::StaleGeneration {
                expected: current,
                actual: request.generation,
            });
        }
        if request.request_key.trim().is_empty() {
            return Err(CodexWorkflowError::UnknownServerRequest(String::new()));
        }
        if let Some(existing) = self.pending_requests.get(&request.request_key) {
            if existing.request_id == request.request_id
                && existing.generation == request.generation
                && existing.scope == request.scope
                && existing.kind == request.kind
            {
                return Ok(false);
            }
            return Err(CodexWorkflowError::IntentCollision(request.request_key));
        }
        request.state = PendingRequestState::Available;
        self.pending_requests
            .insert(request.request_key.clone(), request);
        Ok(true)
    }

    pub fn claim_server_request(
        &mut self,
        request_key: &str,
    ) -> Result<PendingServerRequest, CodexWorkflowError> {
        let current = self.connection_generation.unwrap_or_default();
        let request = self
            .pending_requests
            .get_mut(request_key)
            .ok_or_else(|| CodexWorkflowError::UnknownServerRequest(request_key.to_owned()))?;
        if !self.connected || request.generation != current {
            request.state = PendingRequestState::Retired;
            return Err(CodexWorkflowError::StaleGeneration {
                expected: current,
                actual: request.generation,
            });
        }
        if request.state != PendingRequestState::Available {
            return Err(CodexWorkflowError::ServerRequestUnavailable(
                request_key.to_owned(),
            ));
        }
        request.state = PendingRequestState::Claimed;
        Ok(request.clone())
    }

    pub fn mark_server_request_responded(
        &mut self,
        request_key: &str,
    ) -> Result<(), CodexWorkflowError> {
        let request = self
            .pending_requests
            .get_mut(request_key)
            .ok_or_else(|| CodexWorkflowError::UnknownServerRequest(request_key.to_owned()))?;
        if request.state != PendingRequestState::Claimed {
            return Err(CodexWorkflowError::ServerRequestUnavailable(
                request_key.to_owned(),
            ));
        }
        request.state = PendingRequestState::Responded;
        Ok(())
    }

    pub fn observe_server_request_resolved(
        &mut self,
        generation: u64,
        request_id: &str,
    ) -> Vec<CodexWorkflowEffect> {
        self.pending_requests
            .values_mut()
            .filter(|request| {
                request.generation == generation
                    && request.request_id == request_id
                    && request.state != PendingRequestState::Resolved
            })
            .map(|request| {
                request.state = PendingRequestState::Resolved;
                CodexWorkflowEffect::ServerRequestResolved {
                    request_key: request.request_key.clone(),
                }
            })
            .collect()
    }

    fn required_intent_mut(
        &mut self,
        client_message_id: &str,
    ) -> Result<&mut PromptIntent, CodexWorkflowError> {
        self.intents
            .get_mut(client_message_id)
            .ok_or_else(|| CodexWorkflowError::UnknownPrompt(client_message_id.to_owned()))
    }

    fn finish_prompt(
        &mut self,
        client_message_id: &str,
        status: TurnTerminalStatus,
    ) -> Result<Vec<CodexWorkflowEffect>, CodexWorkflowError> {
        let (scope, turn_id) = {
            let intent = self.required_intent_mut(client_message_id)?;
            let turn_id =
                intent
                    .turn_id
                    .clone()
                    .ok_or(CodexWorkflowError::InvalidPromptTransition {
                        from: intent.state,
                        to: status.intent_state(),
                    })?;
            intent.state = status.intent_state();
            (intent.scope.clone(), turn_id)
        };
        if let Some(queue) = self.queues.get_mut(&scope) {
            queue.retain(|value| value != client_message_id);
        }
        let mut effects = vec![CodexWorkflowEffect::PromptFinished {
            client_message_id: client_message_id.to_owned(),
            scope: scope.clone(),
            turn_id,
            status,
        }];
        if self
            .queues
            .get(&scope)
            .is_some_and(|queue| !queue.is_empty())
        {
            effects.push(CodexWorkflowEffect::RetryQueue { scope });
        }
        Ok(effects)
    }
}

fn validate_scope(scope: &WorkflowScope) -> Result<(), CodexWorkflowError> {
    if scope.thread_id.trim().is_empty() {
        Err(CodexWorkflowError::EmptyThreadId)
    } else {
        Ok(())
    }
}

fn transition_prompt(
    intent: &mut PromptIntent,
    target: PromptIntentState,
) -> Result<(), CodexWorkflowError> {
    if intent.state == target {
        return Ok(());
    }
    let valid = matches!(
        (intent.state, target),
        (
            PromptIntentState::Received,
            PromptIntentState::AwaitingChoice
        ) | (PromptIntentState::Received, PromptIntentState::Queued)
            | (PromptIntentState::Received, PromptIntentState::Submitting)
            | (PromptIntentState::AwaitingChoice, PromptIntentState::Queued)
            | (
                PromptIntentState::AwaitingChoice,
                PromptIntentState::Submitting
            )
            | (PromptIntentState::Queued, PromptIntentState::Submitting)
            | (PromptIntentState::Submitting, PromptIntentState::Queued)
            | (PromptIntentState::Submitting, PromptIntentState::Started)
            | (PromptIntentState::Submitting, PromptIntentState::Uncertain)
            | (PromptIntentState::Started, PromptIntentState::Completed)
            | (PromptIntentState::Started, PromptIntentState::Failed)
            | (PromptIntentState::Steered, PromptIntentState::Completed)
            | (PromptIntentState::Steered, PromptIntentState::Failed)
    );
    if !valid {
        return Err(CodexWorkflowError::InvalidPromptTransition {
            from: intent.state,
            to: target,
        });
    }
    intent.state = target;
    Ok(())
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

    #[test]
    fn reconnect_retires_old_interactive_requests_and_requires_reconciliation() {
        let scope = WorkflowScope::space("thread-1", "space-1", 7);
        let mut projection = CodexWorkflowProjection::new();
        projection.observe_connection(1, true).unwrap();
        projection.create_prompt("prompt-1", scope.clone()).unwrap();
        projection.queue_prompt("prompt-1").unwrap();
        projection.begin_dispatch(&scope).unwrap().unwrap();
        assert_eq!(
            projection.prompt_intent("prompt-1").unwrap().state,
            PromptIntentState::Submitting
        );
        projection
            .register_server_request(PendingServerRequest {
                request_key: "question-1".into(),
                request_id: "rpc-1".into(),
                generation: 1,
                scope: scope.clone(),
                kind: PendingRequestKind::UserInput,
                state: PendingRequestState::Available,
            })
            .unwrap();

        let effects = projection.observe_connection(2, true).unwrap();

        assert_eq!(
            projection.prompt_intent("prompt-1").unwrap().state,
            PromptIntentState::Uncertain
        );
        assert_eq!(
            projection.pending_request("question-1").unwrap().state,
            PendingRequestState::Retired
        );
        assert!(effects.contains(&CodexWorkflowEffect::RetireServerRequest {
            request_key: "question-1".into(),
        }));
        assert!(effects.contains(&CodexWorkflowEffect::ReconcileThread { scope }));
    }

    #[test]
    fn queue_retries_only_after_terminal_turn_releases_the_gate() {
        let scope = WorkflowScope::legacy("thread-queue");
        let mut projection = CodexWorkflowProjection::new();
        projection.create_prompt("first", scope.clone()).unwrap();
        projection.create_prompt("second", scope.clone()).unwrap();
        projection.queue_prompt("first").unwrap();
        projection.queue_prompt("second").unwrap();

        assert_eq!(
            projection
                .begin_dispatch(&scope)
                .unwrap()
                .unwrap()
                .client_message_id,
            "first"
        );
        projection.observe_turn_started("first", "turn-1").unwrap();
        let effects = projection.observe_turn_completed(
            "thread-queue",
            "turn-1",
            TurnTerminalStatus::Completed,
        );

        assert_eq!(
            projection.prompt_intent("first").unwrap().state,
            PromptIntentState::Completed
        );
        assert_eq!(projection.queued_client_message_ids(&scope), vec!["second"]);
        assert!(effects.contains(&CodexWorkflowEffect::RetryQueue {
            scope: scope.clone(),
        }));
        assert_eq!(
            projection
                .begin_dispatch(&scope)
                .unwrap()
                .unwrap()
                .client_message_id,
            "second"
        );
    }

    #[test]
    fn duplicate_turn_started_is_idempotent_but_a_conflicting_turn_is_rejected() {
        let scope = WorkflowScope::legacy("thread-duplicate-start");
        let mut projection = CodexWorkflowProjection::new();
        projection.create_prompt("prompt", scope.clone()).unwrap();
        projection.queue_prompt("prompt").unwrap();
        projection.begin_dispatch(&scope).unwrap().unwrap();

        assert!(
            projection
                .observe_turn_started("prompt", "turn-1")
                .unwrap()
                .is_empty()
        );
        assert!(
            projection
                .observe_turn_started("prompt", "turn-1")
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            projection.prompt_intent("prompt").unwrap().state,
            PromptIntentState::Started
        );
        assert_eq!(
            projection
                .prompt_intent("prompt")
                .unwrap()
                .turn_id
                .as_deref(),
            Some("turn-1")
        );
        assert!(matches!(
            projection.observe_turn_started("prompt", "turn-2"),
            Err(CodexWorkflowError::InvalidPromptTransition { .. })
        ));
    }

    #[test]
    fn terminal_delivery_is_idempotent_and_interruption_cancels_the_intent() {
        let scope = WorkflowScope::legacy("thread-terminal-delivery");
        let mut projection = CodexWorkflowProjection::new();
        projection.create_prompt("prompt", scope.clone()).unwrap();
        projection.queue_prompt("prompt").unwrap();
        projection.begin_dispatch(&scope).unwrap().unwrap();

        let observation = DeliveryObservation::Present {
            turn_id: "turn-1".into(),
            terminal_status: Some(TurnTerminalStatus::Interrupted),
        };
        let effects = projection
            .observe_delivery("prompt", observation.clone())
            .unwrap();
        assert_eq!(
            projection.prompt_intent("prompt").unwrap().state,
            PromptIntentState::Cancelled
        );
        assert!(effects.contains(&CodexWorkflowEffect::PromptFinished {
            client_message_id: "prompt".into(),
            scope,
            turn_id: "turn-1".into(),
            status: TurnTerminalStatus::Interrupted,
        }));
        assert!(
            projection
                .observe_delivery("prompt", observation)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn ambiguous_delivery_never_retries_until_a_read_proves_absence() {
        let scope = WorkflowScope::legacy("thread-delivery");
        let mut projection = CodexWorkflowProjection::new();
        projection.create_prompt("prompt", scope.clone()).unwrap();
        projection.queue_prompt("prompt").unwrap();
        projection.begin_dispatch(&scope).unwrap().unwrap();

        let effects = projection
            .observe_delivery("prompt", DeliveryObservation::Unknown)
            .unwrap();
        assert_eq!(
            projection.prompt_intent("prompt").unwrap().state,
            PromptIntentState::Uncertain
        );
        assert_eq!(
            effects,
            vec![CodexWorkflowEffect::ReconcileThread {
                scope: scope.clone()
            }]
        );
        assert!(projection.begin_dispatch(&scope).unwrap().is_none());

        let effects = projection
            .observe_delivery("prompt", DeliveryObservation::Absent)
            .unwrap();
        assert_eq!(
            projection.prompt_intent("prompt").unwrap().state,
            PromptIntentState::Queued
        );
        assert_eq!(effects, vec![CodexWorkflowEffect::RetryQueue { scope }]);
    }

    #[test]
    fn plan_and_server_request_effects_are_idempotent() {
        let scope = WorkflowScope::legacy("thread-effects");
        let mut projection = CodexWorkflowProjection::new();
        projection.observe_connection(3, true).unwrap();
        let publication = PlanPublication {
            scope: scope.clone(),
            turn_id: "turn-plan".into(),
            item_id: "item-plan".into(),
            revision_key: "revision-a".into(),
            text: "Plan text".into(),
        };
        assert_eq!(
            projection.observe_plan(publication.clone()).unwrap(),
            vec![CodexWorkflowEffect::PublishPlan {
                publication: publication.clone()
            }]
        );
        assert!(projection.observe_plan(publication).unwrap().is_empty());

        let request = PendingServerRequest {
            request_key: "approval-1".into(),
            request_id: "rpc-approval".into(),
            generation: 3,
            scope,
            kind: PendingRequestKind::Approval,
            state: PendingRequestState::Retired,
        };
        assert!(projection.register_server_request(request.clone()).unwrap());
        assert!(!projection.register_server_request(request).unwrap());
        projection.claim_server_request("approval-1").unwrap();
        projection
            .mark_server_request_responded("approval-1")
            .unwrap();
        assert_eq!(
            projection.observe_server_request_resolved(3, "rpc-approval"),
            vec![CodexWorkflowEffect::ServerRequestResolved {
                request_key: "approval-1".into()
            }]
        );
        assert!(
            projection
                .observe_server_request_resolved(3, "rpc-approval")
                .is_empty()
        );
    }
}
