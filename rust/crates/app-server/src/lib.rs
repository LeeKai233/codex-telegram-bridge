//! Codex managed app-server adapter.
//!
//! The production daemon speaks JSON-RPC over a WebSocket upgrade on a Unix
//! socket. This crate owns that transport only; application code consumes the
//! `ctg_ports::AgentBackend` trait instead.

use async_trait::async_trait;
use ctg_domain::{
    AgentEvent, AgentServerRequest, AgentThread, AgentTurn, PromptInput, ThreadId, TurnId,
};
use ctg_ports::{AgentBackend, PortError, PortResult};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, Semaphore, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_tungstenite::{
    WebSocketStream, client_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};

type AppSocket = WebSocketStream<UnixStream>;

const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Debug)]
pub struct AppServerConfig {
    pub socket_path: PathBuf,
    pub transport: AppServerTransport,
    pub client_name: String,
    pub client_title: String,
    pub client_version: String,
    pub outbound_capacity: usize,
    pub pending_capacity: usize,
    pub notification_capacity: usize,
    pub reconnect_initial: Duration,
    pub reconnect_max: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppServerTransport {
    UnixSocket(PathBuf),
    Stdio { program: PathBuf, args: Vec<String> },
}

impl AppServerConfig {
    pub fn managed(socket_path: impl Into<PathBuf>) -> Self {
        let socket_path = socket_path.into();
        Self {
            transport: AppServerTransport::UnixSocket(socket_path.clone()),
            socket_path,
            client_name: "codex_telegram_bridge_rust".into(),
            client_title: "Codex Telegram Bridge (Rust)".into(),
            client_version: env!("CARGO_PKG_VERSION").into(),
            outbound_capacity: 128,
            pending_capacity: 128,
            notification_capacity: 256,
            reconnect_initial: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(30),
        }
    }

    /// Use Codex's newline-delimited JSON protocol for native Windows,
    /// sandboxed hosts, and any platform without Unix sockets.
    pub fn stdio(program: impl Into<PathBuf>, args: impl IntoIterator<Item = String>) -> Self {
        let mut config = Self::managed(PathBuf::from("stdio"));
        config.transport = AppServerTransport::Stdio {
            program: program.into(),
            args: args.into_iter().collect(),
        };
        config
    }

    fn validate(&self) -> Result<(), AppServerError> {
        if self.socket_path.as_os_str().is_empty()
            || matches!(
                &self.transport,
                AppServerTransport::UnixSocket(path) if path.as_os_str().is_empty()
            )
            || matches!(
                &self.transport,
                AppServerTransport::Stdio { program, .. } if program.as_os_str().is_empty()
            )
            || self.outbound_capacity == 0
            || self.pending_capacity == 0
            || self.notification_capacity == 0
            || self.reconnect_initial.is_zero()
            || self.reconnect_max < self.reconnect_initial
        {
            return Err(AppServerError::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum AppServerError {
    #[error("invalid app-server configuration")]
    InvalidConfig,
    #[error("app-server is disconnected")]
    Disconnected,
    #[error("app-server client is shut down")]
    Shutdown,
    #[error("app-server outbound queue is full")]
    QueueFull,
    #[error("app-server request timed out")]
    Timeout,
    #[error("app-server generation is stale (expected {expected}, got {actual})")]
    StaleGeneration { expected: u64, actual: u64 },
    #[error("app-server protocol error: {0}")]
    Protocol(String),
    #[error("app-server I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<AppServerError> for PortError {
    fn from(value: AppServerError) -> Self {
        PortError::Adapter(value.to_string())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConnectionState {
    pub generation: u64,
    pub connected: bool,
}

/// A normalized interactive request.  The daemon and Telegram adapter own
/// delivery, but they must use this contract before persisting or replying to
/// a Codex server request.  Keeping the protocol rules at the transport
/// boundary prevents legacy request shapes from leaking into business state.
#[derive(Clone, Debug, PartialEq)]
pub enum InteractiveServerRequest {
    UserInput(UserInputRequest),
    Approval(InteractiveApprovalRequest),
}

#[derive(Clone, Debug, PartialEq)]
pub struct UserInputRequest {
    pub request: AgentServerRequest,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub questions: Vec<UserInputQuestion>,
    pub auto_resolution_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserInputQuestion {
    pub id: String,
    pub question: String,
    pub is_secret: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct InteractiveApprovalRequest {
    pub request: AgentServerRequest,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub method: String,
    pub params: Value,
    /// Values are retained verbatim because Codex supports structured policy
    /// amendments in addition to the simple accept/decline decisions.
    pub available_decisions: Vec<Value>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum InteractiveRequestError {
    #[error("unsupported interactive server request: {0}")]
    UnsupportedMethod(String),
    #[error("interactive server request is missing threadId")]
    MissingThreadId,
    #[error("requestUserInput question is missing a stable id")]
    MissingQuestionId,
    #[error("requestUserInput contains a secret question")]
    SecretInput,
    #[error("requestUserInput answer does not match a pending question: {0}")]
    UnknownQuestion(String),
    #[error("approval decision is not available for this request")]
    UnavailableApprovalDecision,
    #[error("approval decision has an invalid protocol shape")]
    InvalidApprovalDecision,
}

impl InteractiveServerRequest {
    pub fn parse(request: AgentServerRequest) -> Result<Self, InteractiveRequestError> {
        if request.method == "item/tool/requestUserInput" {
            return UserInputRequest::parse(request).map(Self::UserInput);
        }
        InteractiveApprovalRequest::parse(request).map(Self::Approval)
    }
}

impl UserInputRequest {
    fn parse(request: AgentServerRequest) -> Result<Self, InteractiveRequestError> {
        let thread_id = required_thread_id(&request.params)?;
        let questions = request
            .params
            .get("questions")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_object)
                    .map(|question| {
                        let id = question
                            .get("id")
                            .and_then(Value::as_str)
                            .filter(|value| !value.trim().is_empty())
                            .ok_or(InteractiveRequestError::MissingQuestionId)?;
                        Ok(UserInputQuestion {
                            id: id.to_owned(),
                            question: question
                                .get("question")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned(),
                            is_secret: question
                                .get("isSecret")
                                .and_then(Value::as_bool)
                                .unwrap_or(false),
                        })
                    })
                    .collect::<Result<Vec<_>, InteractiveRequestError>>()
            })
            .transpose()?
            .unwrap_or_default();
        Ok(Self {
            turn_id: string_field(&request.params, "turnId"),
            item_id: string_field(&request.params, "itemId"),
            auto_resolution_ms: request
                .params
                .get("autoResolutionMs")
                .and_then(Value::as_u64),
            request,
            thread_id,
            questions,
        })
    }

    pub fn contains_secret(&self) -> bool {
        self.questions.iter().any(|question| question.is_secret)
    }

    /// Encode the exact `item/tool/requestUserInput` JSON-RPC result payload.
    /// Unknown answers are refused so a stale Telegram callback cannot answer a
    /// different request after recovery.
    pub fn response(
        &self,
        answers: &BTreeMap<String, Vec<String>>,
    ) -> Result<Value, InteractiveRequestError> {
        if self.contains_secret() {
            return Err(InteractiveRequestError::SecretInput);
        }
        let known = self
            .questions
            .iter()
            .map(|question| question.id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let mut encoded = serde_json::Map::new();
        for (id, values) in answers {
            if !known.contains(id.as_str()) {
                return Err(InteractiveRequestError::UnknownQuestion(id.clone()));
            }
            encoded.insert(id.clone(), json!({"answers": values}));
        }
        Ok(json!({"answers": Value::Object(encoded)}))
    }
}

impl InteractiveApprovalRequest {
    fn parse(request: AgentServerRequest) -> Result<Self, InteractiveRequestError> {
        let method = request.method.clone();
        if !is_approval_method(&method) {
            return Err(InteractiveRequestError::UnsupportedMethod(method));
        }
        let params = normalize_interactive_approval_params(&method, &request.params);
        let thread_id = required_thread_id(&params)?;
        let available_decisions = interactive_approval_decisions(&method, &params);
        Ok(Self {
            turn_id: string_field(&params, "turnId"),
            item_id: string_field(&params, "itemId"),
            request,
            thread_id,
            method,
            params,
            available_decisions,
        })
    }

    /// Encode an approval response only after checking it against the exact
    /// decisions Codex advertised for this request.
    pub fn response(&self, decision: &Value) -> Result<Value, InteractiveRequestError> {
        if !interactive_approval_is_available(&self.method, decision, &self.available_decisions) {
            return Err(InteractiveRequestError::UnavailableApprovalDecision);
        }
        approval_response_payload(&self.method, decision)
    }
}

fn string_field(params: &Value, name: &str) -> String {
    params
        .get(name)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn required_thread_id(params: &Value) -> Result<String, InteractiveRequestError> {
    let direct = string_field(params, "threadId");
    if !direct.trim().is_empty() {
        return Ok(direct);
    }
    let nested = params
        .get("thread")
        .and_then(Value::as_object)
        .and_then(|thread| thread.get("id"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    if nested.is_empty() {
        Err(InteractiveRequestError::MissingThreadId)
    } else {
        Ok(nested)
    }
}

fn is_approval_method(method: &str) -> bool {
    matches!(
        method,
        "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval"
            | "execCommandApproval"
            | "applyPatchApproval"
    )
}

/// Normalize legacy RPC names into the thread/turn/item shape used by all
/// durable workflow records.
pub fn normalize_interactive_approval_params(method: &str, params: &Value) -> Value {
    let mut normalized = params.clone();
    let Some(values) = normalized.as_object_mut() else {
        return normalized;
    };
    if matches!(method, "execCommandApproval" | "applyPatchApproval") {
        let thread_id = values
            .get("conversationId")
            .cloned()
            .or_else(|| values.get("threadId").cloned())
            .unwrap_or_else(|| Value::String(String::new()));
        let turn_id = values
            .get("turnId")
            .cloned()
            .unwrap_or_else(|| Value::String(String::new()));
        let item_id = values
            .get("callId")
            .cloned()
            .or_else(|| values.get("itemId").cloned())
            .unwrap_or_else(|| Value::String(String::new()));
        values.insert("threadId".into(), thread_id);
        values.insert("turnId".into(), turn_id);
        values.insert("itemId".into(), item_id);
    }
    normalized
}

fn approval_decision_kind(decision: &Value) -> Option<&'static str> {
    if let Some(value) = decision.as_str() {
        return match value {
            "accept" => Some("accept"),
            "acceptForSession" => Some("acceptForSession"),
            "decline" => Some("decline"),
            "cancel" => Some("cancel"),
            _ => None,
        };
    }
    let values = decision.as_object()?;
    if values.len() != 1 {
        return None;
    }
    if let Some(detail) = values
        .get("acceptWithExecpolicyAmendment")
        .and_then(Value::as_object)
    {
        let amendment = detail.get("execpolicy_amendment")?.as_array()?;
        return amendment
            .iter()
            .all(Value::is_string)
            .then_some("acceptWithExecpolicyAmendment");
    }
    if let Some(detail) = values
        .get("applyNetworkPolicyAmendment")
        .and_then(Value::as_object)
    {
        let amendment = detail.get("network_policy_amendment")?.as_object()?;
        let valid_action = matches!(
            amendment.get("action").and_then(Value::as_str),
            Some("allow" | "deny")
        );
        let valid_host = amendment
            .get("host")
            .and_then(Value::as_str)
            .is_some_and(|host| !host.trim().is_empty());
        return (valid_action && valid_host).then_some("applyNetworkPolicyAmendment");
    }
    None
}

fn simple_approval(value: &Value) -> bool {
    matches!(
        approval_decision_kind(value),
        Some("accept" | "acceptForSession" | "decline" | "cancel")
    )
}

/// Return the Telegram-safe approval choices without rewriting structured
/// amendments or permissions.  Callers persist these values verbatim.
pub fn interactive_approval_decisions(method: &str, params: &Value) -> Vec<Value> {
    const MODERN_DEFAULT: [&str; 3] = ["accept", "acceptForSession", "decline"];
    const LEGACY_DEFAULT: [&str; 4] = ["accept", "acceptForSession", "decline", "cancel"];
    let defaults = |values: &[&str]| {
        values
            .iter()
            .map(|value| Value::String((*value).to_owned()))
            .collect::<Vec<_>>()
    };
    match method {
        "item/commandExecution/requestApproval" => match params.get("availableDecisions") {
            None => defaults(&MODERN_DEFAULT),
            Some(Value::Array(values)) => values
                .iter()
                .filter(|value| approval_decision_kind(value).is_some())
                .cloned()
                .collect(),
            Some(_) => Vec::new(),
        },
        "execCommandApproval" => defaults(&LEGACY_DEFAULT),
        "item/fileChange/requestApproval" | "applyPatchApproval" => {
            match params.get("availableDecisions") {
                None => defaults(&LEGACY_DEFAULT),
                Some(Value::Array(values)) => values
                    .iter()
                    .filter(|value| simple_approval(value))
                    .cloned()
                    .collect(),
                Some(_) => Vec::new(),
            }
        }
        "item/permissions/requestApproval" => {
            let Some(permissions) = params
                .get("permissions")
                .or_else(|| params.get("requestedPermissions"))
                .filter(|value| value.is_object())
            else {
                return Vec::new();
            };
            let turn = json!({"permissions": permissions, "scope": "turn"});
            if permissions
                .as_object()
                .is_some_and(serde_json::Map::is_empty)
            {
                vec![turn]
            } else {
                vec![
                    turn,
                    json!({"permissions": permissions, "scope": "session"}),
                    json!({"permissions": {}, "scope": "turn"}),
                ]
            }
        }
        _ => Vec::new(),
    }
}

pub fn interactive_approval_is_available(
    method: &str,
    decision: &Value,
    available: &[Value],
) -> bool {
    if method == "item/permissions/requestApproval" {
        return decision.is_object() && available.iter().any(|candidate| candidate == decision);
    }
    approval_decision_kind(decision).is_some()
        && available.iter().any(|candidate| candidate == decision)
}

/// Build an exact JSON-RPC result for the current Codex approval method.
pub fn approval_response_payload(
    method: &str,
    decision: &Value,
) -> Result<Value, InteractiveRequestError> {
    if method == "item/permissions/requestApproval" {
        let Some(values) = decision.as_object() else {
            return Err(InteractiveRequestError::InvalidApprovalDecision);
        };
        let valid_permissions = values.get("permissions").is_some_and(Value::is_object);
        let scope = values.get("scope").and_then(Value::as_str);
        let strict = values.get("strictAutoReview");
        let valid_strict = strict.is_none_or(Value::is_boolean);
        if !valid_permissions
            || !matches!(scope, Some("turn" | "session"))
            || !valid_strict
            || (scope == Some("session") && strict == Some(&Value::Bool(true)))
        {
            return Err(InteractiveRequestError::InvalidApprovalDecision);
        }
        return Ok(decision.clone());
    }
    if approval_decision_kind(decision).is_none() {
        return Err(InteractiveRequestError::InvalidApprovalDecision);
    }
    match method {
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            Ok(json!({"decision": decision}))
        }
        "execCommandApproval" | "applyPatchApproval" => {
            let Some(value) = decision.as_str() else {
                return Err(InteractiveRequestError::InvalidApprovalDecision);
            };
            let mapped = match value {
                "accept" => "approved",
                "acceptForSession" => "approved_for_session",
                "decline" => "denied",
                "cancel" => "abort",
                _ => return Err(InteractiveRequestError::InvalidApprovalDecision),
            };
            Ok(json!({"decision": mapped}))
        }
        _ => Err(InteractiveRequestError::UnsupportedMethod(
            method.to_owned(),
        )),
    }
}

enum Outbound {
    Request {
        generation: u64,
        id: u64,
        method: String,
        params: Value,
        response: oneshot::Sender<Result<Value, AppServerError>>,
        permit: tokio::sync::OwnedSemaphorePermit,
    },
    Response {
        generation: u64,
        message: Value,
        sent: oneshot::Sender<Result<(), AppServerError>>,
    },
}

struct Pending {
    response: oneshot::Sender<Result<Value, AppServerError>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

struct Inner {
    config: AppServerConfig,
    outbound: mpsc::Sender<Outbound>,
    events: mpsc::Sender<AgentEvent>,
    server_requests: mpsc::Sender<AgentServerRequest>,
    event_receiver: StdMutex<Option<mpsc::Receiver<AgentEvent>>>,
    server_request_receiver: StdMutex<Option<mpsc::Receiver<AgentServerRequest>>>,
    state: watch::Sender<ConnectionState>,
    shutdown: watch::Sender<bool>,
    ids: AtomicU64,
    transport_generation: AtomicU64,
    transport_active: AtomicBool,
    permits: Arc<Semaphore>,
    task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone)]
pub struct AppServerClient {
    inner: Arc<Inner>,
}

impl AppServerClient {
    pub async fn connect(config: AppServerConfig) -> Result<Self, AppServerError> {
        config.validate()?;
        let (outbound, receiver) = mpsc::channel(config.outbound_capacity);
        let (events, event_receiver) = mpsc::channel(config.notification_capacity);
        let (server_requests, server_request_receiver) =
            mpsc::channel(config.notification_capacity);
        let (state, _) = watch::channel(ConnectionState::default());
        let (shutdown, _) = watch::channel(false);
        let inner = Arc::new(Inner {
            permits: Arc::new(Semaphore::new(config.pending_capacity)),
            config,
            outbound,
            events,
            server_requests,
            event_receiver: StdMutex::new(Some(event_receiver)),
            server_request_receiver: StdMutex::new(Some(server_request_receiver)),
            state,
            shutdown,
            ids: AtomicU64::new(1),
            transport_generation: AtomicU64::new(0),
            transport_active: AtomicBool::new(false),
            task: Mutex::new(None),
        });
        let client = Self { inner };
        let supervisor = client.clone();
        let task = tokio::spawn(async move { supervisor.supervise(receiver).await });
        *client.inner.task.lock().await = Some(task);
        Ok(client)
    }

    pub fn connection_state(&self) -> ConnectionState {
        *self.inner.state.borrow()
    }

    pub async fn wait_connected(&self, wait: Duration) -> Result<ConnectionState, AppServerError> {
        let mut state = self.inner.state.subscribe();
        timeout(wait, async move {
            loop {
                let current = *state.borrow_and_update();
                if current.connected {
                    return Ok(current);
                }
                state
                    .changed()
                    .await
                    .map_err(|_| AppServerError::Shutdown)?;
            }
        })
        .await
        .map_err(|_| AppServerError::Timeout)?
    }

    pub async fn shutdown(&self) {
        let _ = self.inner.shutdown.send(true);
        if let Some(task) = self.inner.task.lock().await.take() {
            let _ = task.await;
        }
    }

    pub async fn request(
        &self,
        method: impl Into<String>,
        params: Value,
        request_timeout: Duration,
    ) -> Result<Value, AppServerError> {
        let state = self.wait_connected(request_timeout).await?;
        let permit = timeout(request_timeout, self.inner.permits.clone().acquire_owned())
            .await
            .map_err(|_| AppServerError::Timeout)
            .and_then(|value| value.map_err(|_| AppServerError::Shutdown))?;
        let (response_tx, response_rx) = oneshot::channel();
        let outbound = Outbound::Request {
            generation: state.generation,
            id: self.inner.ids.fetch_add(1, Ordering::Relaxed),
            method: method.into(),
            params,
            response: response_tx,
            permit,
        };
        self.inner
            .outbound
            .try_send(outbound)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => AppServerError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => AppServerError::Shutdown,
            })?;
        timeout(request_timeout, response_rx)
            .await
            .map_err(|_| AppServerError::Timeout)?
            .map_err(|_| AppServerError::Disconnected)?
    }

    async fn send_response(&self, id: Value, payload: Value) -> Result<(), AppServerError> {
        let generation = self.inner.transport_generation.load(Ordering::Acquire);
        self.send_response_for_generation(generation, id, payload)
            .await
    }

    async fn send_response_for_generation(
        &self,
        generation: u64,
        id: Value,
        payload: Value,
    ) -> Result<(), AppServerError> {
        let active_generation = self.inner.transport_generation.load(Ordering::Acquire);
        if !self.inner.transport_active.load(Ordering::Acquire) {
            return Err(AppServerError::Disconnected);
        }
        if generation == 0 || active_generation != generation {
            return Err(AppServerError::StaleGeneration {
                expected: active_generation,
                actual: generation,
            });
        }
        let mut message = payload.as_object().cloned().unwrap_or_default();
        message.insert("id".into(), id);
        let (sent, acknowledged) = oneshot::channel();
        self.inner
            .outbound
            .try_send(Outbound::Response {
                generation,
                message: Value::Object(message),
                sent,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => AppServerError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => AppServerError::Shutdown,
            })?;
        timeout(INITIALIZE_TIMEOUT, acknowledged)
            .await
            .map_err(|_| AppServerError::Timeout)?
            .map_err(|_| AppServerError::Disconnected)?
    }

    /// Reply only on the same transport generation that delivered a server
    /// request.  A reconnect must retire the old Telegram callback instead of
    /// sending its decision to a new Codex connection.
    pub async fn respond_to_server_request(
        &self,
        request: &AgentServerRequest,
        result: Value,
    ) -> Result<(), AppServerError> {
        self.send_response_for_generation(
            request.generation,
            request.id.clone(),
            json!({"result": result}),
        )
        .await
    }

    pub async fn respond_error_to_server_request(
        &self,
        request: &AgentServerRequest,
        code: i64,
        message: &str,
    ) -> Result<(), AppServerError> {
        self.send_response_for_generation(
            request.generation,
            request.id.clone(),
            json!({"error": {"code": code, "message": message}}),
        )
        .await
    }

    async fn supervise(&self, mut outbound: mpsc::Receiver<Outbound>) {
        let mut delay = self.inner.config.reconnect_initial;
        let mut generation = 0_u64;
        let mut shutdown = self.inner.shutdown.subscribe();
        loop {
            if *shutdown.borrow() {
                break;
            }
            generation += 1;
            let result = match &self.inner.config.transport {
                AppServerTransport::UnixSocket(path) => match UnixStream::connect(path).await {
                    Ok(stream) => {
                        self.run_connection(stream, generation, &mut outbound, &mut shutdown)
                            .await
                    }
                    Err(error) => Err(AppServerError::Io(error)),
                },
                AppServerTransport::Stdio { program, args } => {
                    self.run_stdio_connection(
                        program,
                        args,
                        generation,
                        &mut outbound,
                        &mut shutdown,
                    )
                    .await
                }
            };
            if self.inner.transport_generation.load(Ordering::Acquire) == generation {
                self.inner.transport_active.store(false, Ordering::Release);
            }
            let _ = self.inner.state.send(ConnectionState {
                generation,
                connected: false,
            });
            if result.is_ok() {
                delay = self.inner.config.reconnect_initial;
            }
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { break; }
                }
                _ = sleep(delay) => {}
            }
            delay = delay.saturating_mul(2).min(self.inner.config.reconnect_max);
        }
        let _ = self.inner.state.send(ConnectionState {
            generation,
            connected: false,
        });
        while let Ok(outbound) = outbound.try_recv() {
            fail_outbound(outbound, AppServerError::Shutdown);
        }
    }

    async fn run_connection(
        &self,
        stream: UnixStream,
        generation: u64,
        outbound: &mut mpsc::Receiver<Outbound>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), AppServerError> {
        let mut websocket_config = WebSocketConfig::default();
        websocket_config.max_frame_size = Some(16 * 1024 * 1024);
        websocket_config.max_message_size = Some(64 * 1024 * 1024);
        let (mut socket, _) =
            client_async_with_config("ws://localhost/", stream, Some(websocket_config))
                .await
                .map_err(websocket_error)?;
        self.inner
            .transport_generation
            .store(generation, Ordering::Release);
        self.inner.transport_active.store(true, Ordering::Release);
        let _ = self.inner.state.send(ConnectionState {
            generation,
            connected: false,
        });
        let mut pending: HashMap<u64, Pending> = HashMap::new();
        let initialize_id = self.inner.ids.fetch_add(1, Ordering::Relaxed);
        write_message(
            &mut socket,
            &json!({
                "id": initialize_id,
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        "name": self.inner.config.client_name,
                        "title": self.inner.config.client_title,
                        "version": self.inner.config.client_version,
                    },
                    "capabilities": {"experimentalApi": true}
                }
            }),
        )
        .await?;
        let mut initialized = false;
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() { return Ok(()); }
                }
                outbound_message = outbound.recv() => {
                    match outbound_message {
                        Some(message) => self.write_outbound(&mut socket, generation, &mut pending, message).await?,
                        None => return Ok(()),
                    }
                }
                inbound = socket.next() => {
                    let Some(inbound) = inbound else { return Err(AppServerError::Disconnected); };
                    let inbound = inbound.map_err(websocket_error)?;
                    let message = match inbound {
                        Message::Text(text) => serde_json::from_str(text.as_ref())
                            .map_err(|error| AppServerError::Protocol(format!("invalid JSON frame: {error}")))?,
                        Message::Binary(_) => return Err(AppServerError::Protocol("binary WebSocket frame is not supported".into())),
                        Message::Ping(payload) => {
                            socket.send(Message::Pong(payload)).await.map_err(websocket_error)?;
                            continue;
                        }
                        Message::Pong(_) => continue,
                        Message::Close(_) => return Err(AppServerError::Disconnected),
                        _ => continue,
                    };
                    if let Some(result) = handle_inbound(&self.inner, &mut pending, generation, initialize_id, &mut initialized, message).await? {
                        write_message(&mut socket, &result).await?;
                    }
                    if initialized && !self.connection_state().connected {
                        write_message(&mut socket, &json!({"method":"initialized", "params":{}})).await?;
                        let _ = self.inner.state.send(ConnectionState { generation, connected: true });
                    }
                }
            }
        }
    }

    async fn run_stdio_connection(
        &self,
        program: &PathBuf,
        args: &[String],
        generation: u64,
        outbound: &mut mpsc::Receiver<Outbound>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), AppServerError> {
        let mut command = Command::new(program);
        command
            .args(args)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null());
        let mut child = command.spawn()?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| AppServerError::Protocol("stdio app-server has no stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| AppServerError::Protocol("stdio app-server has no stdout".into()))?;
        let result = self
            .run_stdio_stream(stdin, stdout, generation, &mut child, outbound, shutdown)
            .await;
        if child.try_wait()?.is_none() {
            let _ = child.kill().await;
        }
        result
    }

    async fn run_stdio_stream(
        &self,
        mut stdin: ChildStdin,
        stdout: ChildStdout,
        generation: u64,
        child: &mut Child,
        outbound: &mut mpsc::Receiver<Outbound>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<(), AppServerError> {
        let mut reader = BufReader::new(stdout);
        self.inner
            .transport_generation
            .store(generation, Ordering::Release);
        self.inner.transport_active.store(true, Ordering::Release);
        let _ = self.inner.state.send(ConnectionState {
            generation,
            connected: false,
        });
        let mut pending: HashMap<u64, Pending> = HashMap::new();
        let initialize_id = self.inner.ids.fetch_add(1, Ordering::Relaxed);
        write_json_line(
            &mut stdin,
            &json!({
                "id": initialize_id,
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        "name": self.inner.config.client_name,
                        "title": self.inner.config.client_title,
                        "version": self.inner.config.client_version,
                    },
                    "capabilities": {"experimentalApi": true}
                }
            }),
        )
        .await?;
        let mut initialized = false;
        let mut line = String::new();
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        let _ = child.kill().await;
                        return Ok(());
                    }
                }
                outbound_message = outbound.recv() => {
                    match outbound_message {
                        Some(message) => self.write_stdio_outbound(&mut stdin, generation, &mut pending, message).await?,
                        None => return Ok(()),
                    }
                }
                read = reader.read_line(&mut line) => {
                    let read = read?;
                    if read == 0 {
                        return Err(AppServerError::Disconnected);
                    }
                    let message = serde_json::from_str::<Value>(line.trim_end())
                        .map_err(|error| AppServerError::Protocol(format!("invalid JSON line: {error}")))?;
                    line.clear();
                    if let Some(result) = handle_inbound(
                        &self.inner,
                        &mut pending,
                        generation,
                        initialize_id,
                        &mut initialized,
                        message,
                    )
                    .await?
                    {
                        write_json_line(&mut stdin, &result).await?;
                    }
                    if initialized && !self.connection_state().connected {
                        write_json_line(&mut stdin, &json!({"method":"initialized", "params":{}})).await?;
                        let _ = self.inner.state.send(ConnectionState { generation, connected: true });
                    }
                }
            }
        }
    }

    async fn write_outbound(
        &self,
        socket: &mut AppSocket,
        generation: u64,
        pending: &mut HashMap<u64, Pending>,
        outbound: Outbound,
    ) -> Result<(), AppServerError> {
        match outbound {
            Outbound::Request {
                generation: request_generation,
                id,
                method,
                params,
                response,
                permit,
            } => {
                if request_generation != generation || !self.connection_state().connected {
                    let _ = response.send(Err(AppServerError::Disconnected));
                    return Ok(());
                }
                write_message(
                    socket,
                    &json!({"id": id, "method": method, "params": params}),
                )
                .await?;
                pending.insert(
                    id,
                    Pending {
                        response,
                        _permit: permit,
                    },
                );
            }
            Outbound::Response {
                generation: message_generation,
                message,
                sent,
            } => {
                let active_generation = self.inner.transport_generation.load(Ordering::Acquire);
                if !self.inner.transport_active.load(Ordering::Acquire) {
                    let _ = sent.send(Err(AppServerError::Disconnected));
                    return Ok(());
                }
                if message_generation != generation || active_generation != message_generation {
                    let _ = sent.send(Err(AppServerError::StaleGeneration {
                        expected: active_generation,
                        actual: message_generation,
                    }));
                    return Ok(());
                }
                match write_message(socket, &message).await {
                    Ok(()) => {
                        let _ = sent.send(Ok(()));
                    }
                    Err(error) => {
                        let _ = sent.send(Err(AppServerError::Disconnected));
                        return Err(error);
                    }
                }
            }
        }
        Ok(())
    }

    async fn write_stdio_outbound(
        &self,
        stdin: &mut ChildStdin,
        generation: u64,
        pending: &mut HashMap<u64, Pending>,
        outbound: Outbound,
    ) -> Result<(), AppServerError> {
        match outbound {
            Outbound::Request {
                generation: request_generation,
                id,
                method,
                params,
                response,
                permit,
            } => {
                if request_generation != generation || !self.connection_state().connected {
                    let _ = response.send(Err(AppServerError::Disconnected));
                    return Ok(());
                }
                write_json_line(
                    stdin,
                    &json!({"id": id, "method": method, "params": params}),
                )
                .await?;
                pending.insert(
                    id,
                    Pending {
                        response,
                        _permit: permit,
                    },
                );
            }
            Outbound::Response {
                generation: message_generation,
                message,
                sent,
            } => {
                let active_generation = self.inner.transport_generation.load(Ordering::Acquire);
                if !self.inner.transport_active.load(Ordering::Acquire) {
                    let _ = sent.send(Err(AppServerError::Disconnected));
                    return Ok(());
                }
                if message_generation != generation || active_generation != message_generation {
                    let _ = sent.send(Err(AppServerError::StaleGeneration {
                        expected: active_generation,
                        actual: message_generation,
                    }));
                    return Ok(());
                }
                match write_json_line(stdin, &message).await {
                    Ok(()) => {
                        let _ = sent.send(Ok(()));
                    }
                    Err(error) => {
                        let _ = sent.send(Err(AppServerError::Disconnected));
                        return Err(error);
                    }
                }
            }
        }
        Ok(())
    }
}

impl Drop for AppServerClient {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            let _ = self.inner.shutdown.send(true);
        }
    }
}

async fn handle_inbound(
    inner: &Inner,
    pending: &mut HashMap<u64, Pending>,
    generation: u64,
    initialize_id: u64,
    initialized: &mut bool,
    message: Value,
) -> Result<Option<Value>, AppServerError> {
    let object = message
        .as_object()
        .ok_or_else(|| AppServerError::Protocol("frame must be an object".into()))?;
    let id = object.get("id").cloned();
    let method = object.get("method").and_then(Value::as_str);
    match (id, method) {
        (Some(id), None) => {
            let id_number = id.as_u64().ok_or_else(|| {
                AppServerError::Protocol("response id must be an unsigned integer".into())
            })?;
            if id_number == initialize_id {
                if !object.get("result").is_some_and(Value::is_object) {
                    return Err(AppServerError::Protocol(
                        "initialize did not return an object".into(),
                    ));
                }
                *initialized = true;
            } else if let Some(pending) = pending.remove(&id_number) {
                let result = if let Some(error) = object.get("error") {
                    Err(AppServerError::Protocol(error.to_string()))
                } else {
                    Ok(object.get("result").cloned().unwrap_or(Value::Null))
                };
                let _ = pending.response.send(result);
            }
            Ok(None)
        }
        (Some(id), Some(method)) => {
            inner
                .server_requests
                .send(AgentServerRequest {
                    id,
                    method: method.to_owned(),
                    params: object.get("params").cloned().unwrap_or_else(|| json!({})),
                    generation,
                })
                .await
                .map_err(|_| AppServerError::Shutdown)?;
            Ok(None)
        }
        (None, Some(method)) => {
            inner
                .events
                .send(AgentEvent {
                    method: method.to_owned(),
                    params: object.get("params").cloned().unwrap_or_else(|| json!({})),
                    generation,
                })
                .await
                .map_err(|_| AppServerError::Shutdown)?;
            Ok(None)
        }
        (None, None) => Err(AppServerError::Protocol(
            "frame has neither id nor method".into(),
        )),
    }
}

async fn write_message(socket: &mut AppSocket, value: &Value) -> Result<(), AppServerError> {
    let encoded = serde_json::to_string(value)
        .map_err(|error| AppServerError::Protocol(error.to_string()))?;
    socket
        .send(Message::Text(encoded.into()))
        .await
        .map_err(websocket_error)?;
    Ok(())
}

async fn write_json_line(stdin: &mut ChildStdin, value: &Value) -> Result<(), AppServerError> {
    let encoded = serde_json::to_string(value)
        .map_err(|error| AppServerError::Protocol(error.to_string()))?;
    stdin.write_all(encoded.as_bytes()).await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await?;
    Ok(())
}

fn websocket_error(error: tokio_tungstenite::tungstenite::Error) -> AppServerError {
    match error {
        tokio_tungstenite::tungstenite::Error::Io(error) => AppServerError::Io(error),
        other => AppServerError::Protocol(format!("WebSocket transport: {other}")),
    }
}

fn fail_outbound(outbound: Outbound, error: AppServerError) {
    match outbound {
        Outbound::Request { response, .. } => {
            let _ = response.send(Err(error));
        }
        Outbound::Response { sent, .. } => {
            let _ = sent.send(Err(error));
        }
    }
}

fn parse_thread(value: Value) -> PortResult<AgentThread> {
    let thread = value.get("thread").unwrap_or(&value);
    let id = thread
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| PortError::Adapter("Codex response did not include thread.id".into()))?;
    Ok(AgentThread {
        id: ThreadId::new(id).map_err(|error| PortError::Adapter(error.to_string()))?,
        status: thread
            .get("status")
            .and_then(Value::as_str)
            .map(str::to_owned),
        ephemeral: thread
            .get("ephemeral")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn parse_turn(thread_id: &ThreadId, value: Value) -> PortResult<AgentTurn> {
    let turn = value.get("turn").unwrap_or(&value);
    let id = turn
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| PortError::Adapter("Codex response did not include turn.id".into()))?;
    Ok(AgentTurn {
        id: TurnId::new(id).map_err(|error| PortError::Adapter(error.to_string()))?,
        thread_id: thread_id.clone(),
        status: turn
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("inProgress")
            .to_owned(),
    })
}

impl AppServerClient {
    /// Starts a turn while preserving the optional collaboration-mode payload
    /// used by the Python bridge for the first prompt of a new Session.
    pub async fn start_turn_with_collaboration_mode(
        &self,
        thread_id: &ThreadId,
        input: Vec<PromptInput>,
        client_message_id: Option<&str>,
        collaboration_mode: Option<Value>,
    ) -> PortResult<AgentTurn> {
        let mut params = json!({"threadId": thread_id.as_str(), "input": input});
        if let Some(id) = client_message_id.filter(|value| !value.is_empty()) {
            params["clientUserMessageId"] = Value::String(id.to_owned());
        }
        if let Some(mode) = collaboration_mode {
            params["collaborationMode"] = mode;
        }
        parse_turn(
            thread_id,
            self.request("turn/start", params, Duration::from_secs(60))
                .await
                .map_err(PortError::from)?,
        )
    }
}

#[async_trait]
impl AgentBackend for AppServerClient {
    async fn start_thread(
        &self,
        cwd: &str,
        ephemeral: bool,
        read_only: bool,
    ) -> PortResult<AgentThread> {
        parse_thread(
            self.request(
                "thread/start",
                json!({
                    "cwd": cwd,
                    "ephemeral": ephemeral,
                    "sandbox": if read_only { "read-only" } else { "workspace-write" },
                    "approvalPolicy": if read_only { "never" } else { "on-request" },
                }),
                Duration::from_secs(60),
            )
            .await
            .map_err(PortError::from)?,
        )
    }

    async fn resume_thread(&self, thread_id: &ThreadId) -> PortResult<AgentThread> {
        parse_thread(self.request("thread/resume", json!({
            "threadId": thread_id.as_str(),
            "excludeTurns": true,
            "initialTurnsPage": {"limit": 1, "sortDirection": "desc", "itemsView": "notLoaded"}
        }), Duration::from_secs(60)).await.map_err(PortError::from)?)
    }

    async fn read_thread(&self, thread_id: &ThreadId, include_turns: bool) -> PortResult<Value> {
        self.request(
            "thread/read",
            json!({"threadId": thread_id.as_str(), "includeTurns": include_turns}),
            Duration::from_secs(30),
        )
        .await
        .map_err(PortError::from)
    }

    async fn list_threads(&self, limit: u32, cursor: Option<&str>) -> PortResult<Value> {
        let mut params = json!({"limit": limit.max(1), "sortKey": "recency_at", "sortDirection": "desc", "useStateDbOnly": true});
        if let Some(cursor) = cursor.filter(|value| !value.is_empty()) {
            params["cursor"] = Value::String(cursor.to_owned());
        }
        self.request("thread/list", params, Duration::from_secs(30))
            .await
            .map_err(PortError::from)
    }

    async fn start_turn(
        &self,
        thread_id: &ThreadId,
        input: Vec<PromptInput>,
        client_message_id: Option<&str>,
    ) -> PortResult<AgentTurn> {
        self.start_turn_with_collaboration_mode(thread_id, input, client_message_id, None)
            .await
    }

    async fn steer_turn(
        &self,
        thread_id: &ThreadId,
        expected_turn_id: &TurnId,
        input: Vec<PromptInput>,
        client_message_id: Option<&str>,
    ) -> PortResult<TurnId> {
        let mut params = json!({"threadId": thread_id.as_str(), "expectedTurnId": expected_turn_id.as_str(), "input": input});
        if let Some(id) = client_message_id.filter(|value| !value.is_empty()) {
            params["clientUserMessageId"] = Value::String(id.to_owned());
        }
        let result = self
            .request("turn/steer", params, Duration::from_secs(30))
            .await
            .map_err(PortError::from)?;
        let turn_id = result
            .get("turnId")
            .and_then(Value::as_str)
            .unwrap_or(expected_turn_id.as_str());
        TurnId::new(turn_id).map_err(|error| PortError::Adapter(error.to_string()))
    }

    async fn respond(&self, request_id: Value, result: Value) -> PortResult<()> {
        self.send_response(request_id, json!({"result": result}))
            .await
            .map_err(PortError::from)
    }

    async fn respond_error(&self, request_id: Value, code: i64, message: &str) -> PortResult<()> {
        self.send_response(
            request_id,
            json!({"error": {"code": code, "message": message}}),
        )
        .await
        .map_err(PortError::from)
    }

    fn subscribe_events(&self) -> mpsc::Receiver<AgentEvent> {
        self.inner
            .event_receiver
            .lock()
            .expect("event receiver mutex must not be poisoned")
            .take()
            .unwrap_or_else(|| {
                let (_sender, receiver) = mpsc::channel(1);
                receiver
            })
    }
    fn subscribe_server_requests(&self) -> mpsc::Receiver<AgentServerRequest> {
        self.inner
            .server_request_receiver
            .lock()
            .expect("server request receiver mutex must not be poisoned")
            .take()
            .unwrap_or_else(|| {
                let (_sender, receiver) = mpsc::channel(1);
                receiver
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use tokio::net::UnixListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    #[cfg(unix)]
    static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

    fn test_socket(name: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "ctg-{name}-{}-{}.sock",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[test]
    fn stdio_transport_configuration_is_valid_without_unix_socket() {
        let config = AppServerConfig::stdio(
            "codex",
            [
                "app-server".to_owned(),
                "--listen".to_owned(),
                "stdio://".to_owned(),
            ],
        );
        assert!(matches!(config.transport, AppServerTransport::Stdio { .. }));
        assert!(config.validate().is_ok());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stdio_transport_handshakes_with_a_mock_subprocess() {
        let script = std::env::temp_dir().join(format!(
            "ctg-stdio-fixture-{}-{}.sh",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(
            &script,
            br##"#!/bin/sh
while IFS= read -r line; do
    id=$(printf '%s\n' "$line" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
    if [ -z "$id" ]; then
        continue
    fi
    case "$line" in
        *thread/start*)
            printf '{"id":%s,"result":{"thread":{"id":"t-stdio","status":"idle","ephemeral":false}}}\n' "$id"
            ;;
        *)
            printf '{"id":%s,"result":{}}\n' "$id"
            ;;
    esac
done
"##,
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();

        let client = AppServerClient::connect(AppServerConfig {
            reconnect_initial: Duration::from_millis(5),
            reconnect_max: Duration::from_millis(10),
            ..AppServerConfig::stdio(&script, Vec::<String>::new())
        })
        .await
        .unwrap();
        client.wait_connected(Duration::from_secs(1)).await.unwrap();
        let thread = client.start_thread("/tmp", false, false).await.unwrap();
        assert_eq!(thread.id.as_str(), "t-stdio");
        client.shutdown().await;
        let _ = std::fs::remove_file(script);
    }

    async fn recv_json(socket: &mut AppSocket) -> Value {
        loop {
            match socket.next().await.unwrap().unwrap() {
                Message::Text(text) => return serde_json::from_str(text.as_ref()).unwrap(),
                Message::Ping(payload) => socket.send(Message::Pong(payload)).await.unwrap(),
                Message::Pong(_) => {}
                Message::Close(_) => panic!("unexpected WebSocket close"),
                Message::Binary(_) => panic!("unexpected binary frame"),
                _ => {}
            }
        }
    }

    async fn send_json(socket: &mut AppSocket, value: Value) {
        let encoded = serde_json::to_string(&value).unwrap();
        socket.send(Message::Text(encoded.into())).await.unwrap();
    }

    #[tokio::test]
    async fn handshake_correlates_requests_and_routes_events() {
        let path = test_socket("rpc");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let initialize = recv_json(&mut socket).await;
            assert_eq!(initialize["method"], "initialize");
            send_json(
                &mut socket,
                json!({"id": initialize["id"], "result": {"serverInfo": {}}}),
            )
            .await;
            assert_eq!(recv_json(&mut socket).await["method"], "initialized");
            send_json(
                &mut socket,
                json!({"method":"thread/started", "params":{"thread":{"id":"t-1"}}}),
            )
            .await;
            send_json(&mut socket, json!({"id":"request-9", "method":"item/tool/requestUserInput", "params":{"threadId":"t-1"}})).await;
            let request = recv_json(&mut socket).await;
            assert_eq!(request["method"], "thread/start");
            send_json(&mut socket, json!({"id": request["id"], "result":{"thread":{"id":"t-1", "status":"idle", "ephemeral":false}}})).await;
            let response = recv_json(&mut socket).await;
            assert_eq!(response, json!({"id":"request-9", "result":{"answers":[]}}));
            let turn = recv_json(&mut socket).await;
            assert_eq!(turn["method"], "turn/start");
            assert_eq!(turn["params"]["clientUserMessageId"], "client-1");
            assert_eq!(
                turn["params"]["collaborationMode"],
                json!({
                    "mode": "plan",
                    "settings": {
                        "model": "gpt-test",
                        "reasoning_effort": "low",
                        "developer_instructions": null
                    }
                })
            );
            send_json(
                &mut socket,
                json!({"id": turn["id"], "result":{"turn":{"id":"turn-1", "status":"inProgress"}}}),
            )
            .await;
        });
        let client = AppServerClient::connect(AppServerConfig {
            reconnect_initial: Duration::from_millis(5),
            reconnect_max: Duration::from_millis(10),
            ..AppServerConfig::managed(&path)
        })
        .await
        .unwrap();
        let mut events = client.subscribe_events();
        let mut requests = client.subscribe_server_requests();
        client.wait_connected(Duration::from_secs(1)).await.unwrap();
        let thread = client.start_thread("/tmp", false, false).await.unwrap();
        assert_eq!(thread.id.as_str(), "t-1");
        assert_eq!(events.recv().await.unwrap().method, "thread/started");
        let request = requests.recv().await.unwrap();
        assert_eq!(request.id, json!("request-9"));
        client
            .respond(request.id, json!({"answers":[]}))
            .await
            .unwrap();
        let turn = client
            .start_turn_with_collaboration_mode(
                &thread.id,
                vec![PromptInput::text("hello").unwrap()],
                Some("client-1"),
                Some(json!({
                    "mode": "plan",
                    "settings": {
                        "model": "gpt-test",
                        "reasoning_effort": "low",
                        "developer_instructions": null
                    }
                })),
            )
            .await
            .unwrap();
        assert_eq!(turn.id.as_str(), "turn-1");
        server.await.unwrap();
        client.shutdown().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn reconnects_after_a_disconnected_generation() {
        let path = test_socket("reconnect");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            for attempt in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                let initialize = recv_json(&mut socket).await;
                send_json(&mut socket, json!({"id": initialize["id"], "result": {}})).await;
                assert_eq!(recv_json(&mut socket).await["method"], "initialized");
                if attempt == 1 {
                    let request = recv_json(&mut socket).await;
                    send_json(
                        &mut socket,
                        json!({"id": request["id"], "result":{"data":[]}}),
                    )
                    .await;
                }
            }
        });
        let client = AppServerClient::connect(AppServerConfig {
            reconnect_initial: Duration::from_millis(5),
            reconnect_max: Duration::from_millis(10),
            ..AppServerConfig::managed(&path)
        })
        .await
        .unwrap();
        let first = client.wait_connected(Duration::from_secs(1)).await.unwrap();
        let mut state = client.inner.state.subscribe();
        timeout(Duration::from_secs(1), async {
            while state.changed().await.is_ok() {
                if state.borrow().connected && state.borrow().generation > first.generation {
                    return;
                }
            }
        })
        .await
        .unwrap();
        let listed = client.list_threads(10, None).await.unwrap();
        assert_eq!(listed["data"], json!([]));
        server.await.unwrap();
        client.shutdown().await;
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn invalid_config_rejects_zero_capacities() {
        let result = AppServerClient::connect(AppServerConfig {
            outbound_capacity: 0,
            ..AppServerConfig::managed("/tmp/test.sock")
        })
        .await;
        let Err(error) = result else {
            panic!("zero queue capacity must be rejected");
        };
        assert!(matches!(error, AppServerError::InvalidConfig));
    }

    #[test]
    fn interactive_contract_preserves_modern_amendments_and_legacy_wire_shapes() {
        let amendment = json!({
            "acceptWithExecpolicyAmendment": {
                "execpolicy_amendment": ["allow git status"]
            }
        });
        let request = AgentServerRequest {
            id: json!(11),
            method: "item/commandExecution/requestApproval".into(),
            params: json!({
                "threadId": "thread-modern",
                "turnId": "turn-modern",
                "itemId": "item-modern",
                "availableDecisions": [amendment, "decline"]
            }),
            generation: 4,
        };
        let InteractiveServerRequest::Approval(modern) =
            InteractiveServerRequest::parse(request).unwrap()
        else {
            panic!("modern approval must be normalized");
        };
        assert_eq!(modern.thread_id, "thread-modern");
        assert_eq!(modern.available_decisions.len(), 2);
        assert_eq!(
            modern.response(&modern.available_decisions[0]).unwrap(),
            json!({"decision": modern.available_decisions[0].clone()})
        );

        let legacy = AgentServerRequest {
            id: json!("legacy-id"),
            method: "execCommandApproval".into(),
            params: json!({
                "conversationId": "thread-legacy",
                "turnId": "turn-legacy",
                "callId": "call-legacy"
            }),
            generation: 4,
        };
        let InteractiveServerRequest::Approval(legacy) =
            InteractiveServerRequest::parse(legacy).unwrap()
        else {
            panic!("legacy approval must be normalized");
        };
        assert_eq!(legacy.thread_id, "thread-legacy");
        assert_eq!(legacy.item_id, "call-legacy");
        assert_eq!(
            legacy.response(&json!("acceptForSession")).unwrap(),
            json!({"decision": "approved_for_session"})
        );
    }

    #[test]
    fn request_user_input_uses_exact_wire_shape_and_rejects_secret_or_stale_answers() {
        let request = AgentServerRequest {
            id: json!(12),
            method: "item/tool/requestUserInput".into(),
            params: json!({
                "threadId": "thread-question",
                "turnId": "turn-question",
                "itemId": "item-question",
                "autoResolutionMs": 5000,
                "questions": [{"id": "choice", "question": "Continue?"}]
            }),
            generation: 5,
        };
        let InteractiveServerRequest::UserInput(input) =
            InteractiveServerRequest::parse(request).unwrap()
        else {
            panic!("requestUserInput must be normalized");
        };
        let mut answers = BTreeMap::new();
        answers.insert("choice".into(), vec!["continue".into()]);
        assert_eq!(
            input.response(&answers).unwrap(),
            json!({"answers": {"choice": {"answers": ["continue"]}}})
        );
        answers.insert("stale".into(), vec!["no".into()]);
        assert_eq!(
            input.response(&answers),
            Err(InteractiveRequestError::UnknownQuestion("stale".into()))
        );

        let secret = AgentServerRequest {
            id: json!(13),
            method: "item/tool/requestUserInput".into(),
            params: json!({
                "threadId": "thread-question",
                "questions": [{"id": "secret", "isSecret": true}]
            }),
            generation: 5,
        };
        let InteractiveServerRequest::UserInput(secret) =
            InteractiveServerRequest::parse(secret).unwrap()
        else {
            panic!("secret question must still be detectable");
        };
        assert_eq!(
            secret.response(&BTreeMap::new()),
            Err(InteractiveRequestError::SecretInput)
        );
    }

    #[tokio::test]
    async fn server_request_reply_cannot_cross_a_reconnect_generation() {
        let path = test_socket("stale-server-request");
        let listener = UnixListener::bind(&path).unwrap();
        let server = tokio::spawn(async move {
            {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = accept_async(stream).await.unwrap();
                let initialize = recv_json(&mut socket).await;
                send_json(&mut socket, json!({"id": initialize["id"], "result": {}})).await;
                assert_eq!(recv_json(&mut socket).await["method"], "initialized");
                send_json(
                    &mut socket,
                    json!({
                        "id": "old-request",
                        "method": "item/tool/requestUserInput",
                        "params": {"threadId": "thread-old"}
                    }),
                )
                .await;
            }
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let initialize = recv_json(&mut socket).await;
            send_json(&mut socket, json!({"id": initialize["id"], "result": {}})).await;
            assert_eq!(recv_json(&mut socket).await["method"], "initialized");
            assert!(
                timeout(Duration::from_millis(150), socket.next())
                    .await
                    .is_err()
            );
        });
        let client = AppServerClient::connect(AppServerConfig {
            reconnect_initial: Duration::from_millis(5),
            reconnect_max: Duration::from_millis(10),
            ..AppServerConfig::managed(&path)
        })
        .await
        .unwrap();
        let mut requests = client.subscribe_server_requests();
        client.wait_connected(Duration::from_secs(1)).await.unwrap();
        let request = timeout(Duration::from_secs(1), requests.recv())
            .await
            .unwrap()
            .unwrap();
        let mut state = client.inner.state.subscribe();
        timeout(Duration::from_secs(1), async {
            loop {
                let current = *state.borrow_and_update();
                if current.connected && current.generation > request.generation {
                    return;
                }
                state.changed().await.unwrap();
            }
        })
        .await
        .unwrap();

        let result = client
            .respond_to_server_request(&request, json!({"answers": {}}))
            .await;
        assert!(matches!(
            result,
            Err(AppServerError::StaleGeneration { actual: 1, .. })
        ));
        server.await.unwrap();
        client.shutdown().await;
        let _ = std::fs::remove_file(path);
    }
}
