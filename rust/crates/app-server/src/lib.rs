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
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, Semaphore, broadcast, mpsc, oneshot, watch},
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
    },
}

struct Pending {
    response: oneshot::Sender<Result<Value, AppServerError>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

struct Inner {
    config: AppServerConfig,
    outbound: mpsc::Sender<Outbound>,
    events: broadcast::Sender<AgentEvent>,
    server_requests: broadcast::Sender<AgentServerRequest>,
    state: watch::Sender<ConnectionState>,
    shutdown: watch::Sender<bool>,
    ids: AtomicU64,
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
        let (events, _) = broadcast::channel(config.notification_capacity);
        let (server_requests, _) = broadcast::channel(config.notification_capacity);
        let (state, _) = watch::channel(ConnectionState::default());
        let (shutdown, _) = watch::channel(false);
        let inner = Arc::new(Inner {
            permits: Arc::new(Semaphore::new(config.pending_capacity)),
            config,
            outbound,
            events,
            server_requests,
            state,
            shutdown,
            ids: AtomicU64::new(1),
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
        let state = self.wait_connected(INITIALIZE_TIMEOUT).await?;
        let mut message = payload.as_object().cloned().unwrap_or_default();
        message.insert("id".into(), id);
        self.inner
            .outbound
            .try_send(Outbound::Response {
                generation: state.generation,
                message: Value::Object(message),
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => AppServerError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => AppServerError::Shutdown,
            })
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
            } => {
                if message_generation == generation && self.connection_state().connected {
                    write_message(socket, &message).await?;
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
            } => {
                if message_generation == generation && self.connection_state().connected {
                    write_json_line(stdin, &message).await?;
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
            let _ = inner.server_requests.send(AgentServerRequest {
                id,
                method: method.to_owned(),
                params: object.get("params").cloned().unwrap_or_else(|| json!({})),
                generation,
            });
            Ok(None)
        }
        (None, Some(method)) => {
            let _ = inner.events.send(AgentEvent {
                method: method.to_owned(),
                params: object.get("params").cloned().unwrap_or_else(|| json!({})),
                generation,
            });
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
    if let Outbound::Request { response, .. } = outbound {
        let _ = response.send(Err(error));
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
        let mut params = json!({"threadId": thread_id.as_str(), "input": input});
        if let Some(id) = client_message_id.filter(|value| !value.is_empty()) {
            params["clientUserMessageId"] = Value::String(id.to_owned());
        }
        parse_turn(
            thread_id,
            self.request("turn/start", params, Duration::from_secs(60))
                .await
                .map_err(PortError::from)?,
        )
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

    fn subscribe_events(&self) -> broadcast::Receiver<AgentEvent> {
        self.inner.events.subscribe()
    }
    fn subscribe_server_requests(&self) -> broadcast::Receiver<AgentServerRequest> {
        self.inner.server_requests.subscribe()
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
}
