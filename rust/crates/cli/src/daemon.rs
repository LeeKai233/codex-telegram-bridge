//! Full Rust runtime orchestration.
//!
//! Telegram's blocking Bot API client is isolated in one polling thread per
//! update-owning Bot. The dispatcher stays on Tokio so Codex app-server calls,
//! notification projection, and shutdown are asynchronous and bounded.

use crate::alerts::AlertWebhookServer;
use crate::config::{BotConfig, RustConfig};
use crate::control::{
    ButtonTarget, ControlController, ControlEffect, ControlRequest, DeleteTarget,
    ModelOption as ControlModelOption, RenderOperation, RenderedEffect, Session as ControlSession,
    SessionsRequest, Topic as ControlTopic,
};
use crate::metrics::{MetricsRegistry, MetricsServer};
use crate::security::TotpManager;
use crate::status_contract::{
    DASHBOARD_DEBOUNCE_MS, HEARTBEAT_SECONDS, LOCKED_WRITE_MESSAGE, STATUS_CALLBACK_TTL_MS,
    UNWATCH_CANCEL_MESSAGE, UNWATCH_CLOSED_MESSAGE, UNWATCH_CONFIRM_MESSAGE, is_status_action,
};
use codex_telegram_adapter::{
    BotCapability, BotCommandScope, ChannelBinding, CommandMenuScope, IncomingUpdate,
    InlineKeyboardButton, InlineKeyboardMarkup, LinkedDiscussion, NativeCommentBinding,
    ParsedTelegramCommand, ReqwestTransport, RoutedUpdate, RuntimeBotRole, SentMessage,
    TelegramBotApi, TelegramCommand, TelegramMessageReference, TelegramMessageRequest,
    TelegramSurfaceBinding, TokenLeaseRegistry, UpdateAuthorization, UpdateRouter,
    UpdateRoutingPolicy, WorkflowAction, WorkflowCommand, command_menu,
};
use codex_telegram_credentials::BotToken;
use ctg_app_server::{AppServerClient, AppServerConfig};
use ctg_domain::{
    AgentServerRequest, AgentTurn, ApprovalAction, ApprovalDecision, ApprovalId, ApprovalRequest,
    Artifact, ArtifactId, DomainEvent, DomainEventKind, EventId, PlanPublication,
    PlanPublicationState, PromptInput, PromptIntent, PromptIntentState, QuestionRequest, Session,
    SessionId, ThreadId, TurnId,
};
use ctg_engine::{EventProjector, ProjectionEffect, ThreadProjection};
use ctg_ports::{AgentBackend, ApprovalStore, ArtifactStore, SessionRepository};
use ctg_storage_sqlite::{
    ControlCallback, NativeCommentRoot, RustSessionSpace, ScheduledDeletion, SqliteStore,
    StoredCallback,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::mpsc;

const APP_SERVER_WAIT: Duration = Duration::from_secs(30);
const APPROVAL_CALLBACK_TTL_MS: i64 = 15 * 60 * 1000;
const NEW_INTERACTION_TTL_MS: i64 = 5 * 60 * 1000;
const NEW_PROMPT_TTL_MS: i64 = 30 * 1000;
const MAX_ARTIFACT_BYTES: u64 = 10 * 1024 * 1024;
/// Shorter deadline for `/perf` and heartbeat edits so one slow edit cannot
/// stall a refresh loop for the full 30s request timeout.
const PERF_EDIT_TIMEOUT: Duration = Duration::from_secs(10);
const UPLOAD_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;
static NEXT_APPROVAL_NONCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct ControlRuntime {
    perf: Arc<crate::perf::PerfSampler>,
    sessions_cache: Arc<Mutex<SessionsCache>>,
    sessions_dirty: Arc<AtomicBool>,
}

/// Cached `/sessions` list built from durable thread projections. Refresh
/// loops reuse it instead of calling `thread/list` every few seconds; Codex
/// lifecycle events mark it dirty so transitions still show up promptly.
#[derive(Default)]
struct SessionsCache {
    sessions: Vec<crate::control::Session>,
    built_at_ms: i64,
    /// `createdAt` is absent from projections, so it is harvested once from
    /// `thread/list` and kept indefinitely (creation time never changes).
    created_at_ms: HashMap<String, i64>,
    created_backfill_attempted_at_ms: i64,
}

impl Default for ControlRuntime {
    fn default() -> Self {
        Self {
            perf: Arc::new(crate::perf::PerfSampler::new()),
            sessions_cache: Arc::new(Mutex::new(SessionsCache::default())),
            sessions_dirty: Arc::new(AtomicBool::new(true)),
        }
    }
}

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error("Rust daemon requires poll_updates = true")]
    PollingDisabled,
    #[error("no enabled update-owning Bot is configured")]
    NoPollingBot,
    #[error("Rust configuration is unavailable: {0}")]
    Config(String),
    #[error("Rust credentials are unavailable")]
    Credentials,
    #[error("Rust state directory could not be created")]
    StateDirectory,
    #[error("Rust SQLite state could not be opened: {0}")]
    Store(String),
    #[error("Codex app-server could not be started: {0}")]
    AppServer(String),
    #[error("Tokio runtime could not be created")]
    Runtime,
    #[error("daemon task failed: {0}")]
    Task(String),
}

#[derive(Clone)]
struct RuntimeBot {
    config: BotConfig,
    role: RuntimeBotRole,
    token: BotToken,
    api: Arc<TelegramBotApi<ReqwestTransport>>,
    username: String,
}

struct InboundUpdate {
    bot_instance_id: String,
    update: codex_telegram_adapter::Update,
    completion: std::sync::mpsc::Sender<(i64, bool)>,
}

#[derive(Clone, Debug)]
struct SessionRecord {
    thread_id: ThreadId,
    turn_id: Option<TurnId>,
    chat_id: i64,
    root_message_id: Option<i64>,
    sender_instance_id: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredApprovalAction {
    request_id: Value,
    generation: u64,
    method: String,
    thread_id: String,
    approval_id: String,
    decision: Value,
    response: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredQuestionAction {
    request_key: String,
    request_id: Value,
    generation: u64,
    thread_id: String,
    space_id: String,
    question_id: String,
    #[serde(default)]
    question_index: usize,
    answer: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredPlanAction {
    space_id: String,
    generation: u64,
    thread_id: String,
    item_id: String,
    revision_key: String,
    decision: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredStatusAction {
    space_id: String,
    generation: u64,
    thread_id: String,
    action: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StoredWorkflowQuestion {
    request_key: String,
    request_id: Value,
    generation: u64,
    thread_id: String,
    turn_id: String,
    item_id: String,
    questions: Value,
    answers: HashMap<String, Vec<String>>,
    #[serde(default)]
    current_index: usize,
    #[serde(default)]
    message_ids: Vec<i64>,
    #[serde(default)]
    summary_message_id: Option<i64>,
    status: String,
    expires_at_ms: Option<i64>,
}

impl StoredApprovalAction {
    fn response_payload(&self) -> Value {
        self.response.clone()
    }
}

#[derive(Default)]
struct SessionRegistry {
    by_chat: Mutex<HashMap<i64, SessionRecord>>,
    by_thread: Mutex<HashMap<String, SessionRecord>>,
}

impl SessionRegistry {
    fn insert(&self, record: SessionRecord) {
        self.by_thread
            .lock()
            .expect("session registry poisoned")
            .insert(record.thread_id.to_string(), record.clone());
        self.by_chat
            .lock()
            .expect("session registry poisoned")
            .insert(record.chat_id, record);
    }

    fn by_chat(&self, chat_id: i64) -> Option<SessionRecord> {
        self.by_chat
            .lock()
            .expect("session registry poisoned")
            .get(&chat_id)
            .cloned()
    }

    fn by_thread(&self, thread_id: &str) -> Option<SessionRecord> {
        self.by_thread
            .lock()
            .expect("session registry poisoned")
            .get(thread_id)
            .cloned()
    }

    fn set_turn(&self, thread_id: &str, turn_id: Option<TurnId>) {
        let mut by_thread = self.by_thread.lock().expect("session registry poisoned");
        if let Some(record) = by_thread.get_mut(thread_id) {
            record.turn_id = turn_id.clone();
            let copy = record.clone();
            drop(by_thread);
            self.by_chat
                .lock()
                .expect("session registry poisoned")
                .insert(copy.chat_id, copy);
        }
    }

    fn set_root(&self, chat_id: i64, root_message_id: Option<i64>) {
        let mut by_chat = self.by_chat.lock().expect("session registry poisoned");
        if let Some(record) = by_chat.get_mut(&chat_id) {
            record.root_message_id = root_message_id;
            let copy = record.clone();
            drop(by_chat);
            self.by_thread
                .lock()
                .expect("session registry poisoned")
                .insert(copy.thread_id.to_string(), copy);
        }
    }

    fn remove(&self, thread_id: &str) -> Option<SessionRecord> {
        let removed = self
            .by_thread
            .lock()
            .expect("session registry poisoned")
            .remove(thread_id);
        if let Some(record) = removed.as_ref() {
            self.by_chat
                .lock()
                .expect("session registry poisoned")
                .remove(&record.chat_id);
        }
        removed
    }
}

/// Fan-out key for the update dispatcher: updates from the same chat keep
/// their Telegram order, while a slow handler in one chat no longer blocks
/// commands, callbacks, or approvals of other chats or bots.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct DispatchKey {
    bot_instance_id: String,
    chat_id: i64,
}

impl DispatchKey {
    fn for_update(inbound: &InboundUpdate) -> Self {
        let chat_id = match IncomingUpdate::from_update(&inbound.update) {
            IncomingUpdate::Message(message) | IncomingUpdate::EditedMessage(message) => {
                message.chat_id
            }
            IncomingUpdate::Callback(callback) => callback.chat_id,
            IncomingUpdate::Membership(membership) => membership.chat_id,
            IncomingUpdate::Unsupported => 0,
        };
        Self {
            bot_instance_id: inbound.bot_instance_id.clone(),
            chat_id,
        }
    }
}

/// Spawns one serial worker per [`DispatchKey`] on first use.  The polling
/// thread dispatches fire-and-forget and confirms the Telegram offset over
/// the contiguous completed prefix, so a slow handler only queues its own
/// key and never blocks other chats or bots.
struct KeyedDispatcher<T, H> {
    workers: HashMap<DispatchKey, mpsc::UnboundedSender<T>>,
    handler: H,
}

impl<T, H, Fut> KeyedDispatcher<T, H>
where
    T: Send + 'static,
    H: Fn(T) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    fn new(handler: H) -> Self {
        Self {
            workers: HashMap::new(),
            handler,
        }
    }

    fn dispatch(&mut self, key: DispatchKey, item: T) {
        let sender = self.workers.entry(key).or_insert_with(|| {
            let (sender, mut receiver) = mpsc::unbounded_channel::<T>();
            let handler = self.handler.clone();
            tokio::spawn(async move {
                while let Some(item) = receiver.recv().await {
                    handler(item).await;
                }
            });
            sender
        });
        // The receiver only closes when this dispatcher drops (shutdown),
        // in which case losing a queued update is acceptable.
        let _ = sender.send(item);
    }

    #[cfg(test)]
    fn worker_count(&self) -> usize {
        self.workers.len()
    }
}

/// Shared dependencies for processing one inbound update, cloned once per
/// per-key worker instead of being re-captured per update.
struct DispatchContext {
    bots_by_id: HashMap<String, RuntimeBot>,
    policy: UpdateRoutingPolicy,
    config: RustConfig,
    store: Arc<SqliteStore>,
    agent: AppServerClient,
    sessions: Arc<SessionRegistry>,
    metrics: MetricsRegistry,
    totp: Arc<TotpManager>,
    control_runtime: Arc<ControlRuntime>,
}

impl DispatchContext {
    async fn process(&self, inbound: InboundUpdate) {
        let update_id = inbound.update.update_id;
        let Some(bot) = self.bots_by_id.get(&inbound.bot_instance_id).cloned() else {
            let _ = inbound.completion.send((update_id, false));
            return;
        };
        let owner_user_id = self
            .store
            .workflow_record("onboarding", "owner")
            .ok()
            .flatten()
            .and_then(|value| value.get("user_id").and_then(Value::as_i64));
        let authorization = UpdateAuthorization {
            owner_user_id,
            bot_username: Some(bot.username.clone()),
            enforce_chat_kind: true,
            reject_sender_chat: true,
            // Before `/pair`, only the Control Bot's private bootstrap
            // commands are reachable. Discussion/Status updates must not
            // interpret a missing owner as an unrestricted actor.
            bootstrap_only: owner_user_id.is_none(),
        };
        let actor_user_id = match IncomingUpdate::from_update(&inbound.update) {
            IncomingUpdate::Message(message) | IncomingUpdate::EditedMessage(message) => {
                message.actor.user_id
            }
            IncomingUpdate::Callback(callback) => callback.actor.user_id,
            IncomingUpdate::Membership(membership) => membership.actor.user_id,
            IncomingUpdate::Unsupported => None,
        };
        let router = match UpdateRouter::new_with_authorization(
            bot.role,
            self.policy.clone(),
            authorization,
        ) {
            Ok(router) => router,
            Err(error) => {
                eprintln!("rust bridge routing disabled: {error}");
                let _ = inbound.completion.send((update_id, false));
                return;
            }
        };
        let routed = router.route(&inbound.update);
        let bind_identity_warning = match IncomingUpdate::from_update(&inbound.update) {
            IncomingUpdate::Message(message)
                if message.chat_id == self.config.discussion_chat_id
                    && message.actor.sender_chat_id.is_some()
                    && message.automatic_forward_from_channel.is_none()
                    && message.text.as_deref().is_some_and(|text| {
                        ParsedTelegramCommand::parse(text).is_some_and(|parsed| {
                            parsed.command == TelegramCommand::Bind
                                && parsed
                                    .addressed_bot_username
                                    .as_deref()
                                    .is_none_or(|target| target.eq_ignore_ascii_case(&bot.username))
                        })
                    }) =>
            {
                Some(message)
            }
            _ => None,
        };
        let result = if let Some(message) = bind_identity_warning {
            let surface = surface_for(&bot, &self.config, message.chat_id, None);
            send_text(
                &bot,
                &surface,
                "绑定请求未获授权。请使用已配对的个人账号发送，并将发送身份切换为个人账号；若启用了匿名管理员，请先关闭。",
                &self.metrics,
            )
            .await
            .map(|_| ())
        } else {
            handle_action(
                routed,
                actor_user_id,
                bot,
                &self.bots_by_id,
                &self.config,
                &self.store,
                &self.agent,
                &self.sessions,
                &self.metrics,
                &self.totp,
                &self.control_runtime,
            )
            .await
        };
        if let Err(error) = &result {
            eprintln!("rust bridge action failed: {error}");
        }
        // Telegram's update stream is ordered.  Retrying one failed
        // handler forever would therefore starve every later callback,
        // including harmless inline-button acknowledgements.  The
        // handler has already logged the durable failure; advance the
        // update cursor so the next update can still be delivered.
        let _ = inbound.completion.send((update_id, true));
    }
}

pub fn run(config_path: Option<&Path>) -> Result<(), DaemonError> {
    let config = if let Some(path) = config_path {
        RustConfig::load(path).map_err(|error| DaemonError::Config(error.to_string()))?
    } else {
        RustConfig::load_default().map_err(|error| DaemonError::Config(error.to_string()))?
    };
    if !config.poll_updates {
        return Err(DaemonError::PollingDisabled);
    }
    ensure_private_directory(&config.state_directory)?;
    ensure_private_directory(&config.lock_directory)?;

    let credentials = config.credentials().map_err(|_| DaemonError::Credentials)?;
    let transport = ReqwestTransport::new(Duration::from_secs(config.request_timeout_seconds))
        .map_err(|_| DaemonError::Config("Telegram transport could not be built".into()))?;
    let api = Arc::new(TelegramBotApi::with_api_base(transport, &config.api_base));
    let mut bots = Vec::new();
    for bot in config.bots.iter().filter(|bot| bot.enabled) {
        let token = config
            .token_for(bot, &credentials)
            .map_err(|_| DaemonError::Credentials)?;
        let role = runtime_role(bot.capability);
        if role.is_none() && bot.capability != BotCapability::ProductionAlert {
            continue;
        }
        let profile = api
            .get_me(&token)
            .map_err(|error| DaemonError::Config(format!("Telegram getMe failed: {error}")))?;
        bots.push(RuntimeBot {
            config: bot.clone(),
            role: role.unwrap_or(RuntimeBotRole::Alert),
            token,
            api: api.clone(),
            username: profile.username.unwrap_or_default(),
        });
    }
    if !bots.iter().any(|bot| bot.role.polls_updates()) {
        return Err(DaemonError::NoPollingBot);
    }

    let store = Arc::new(
        SqliteStore::open(config.state_directory.join("state.sqlite3"))
            .map_err(|error| DaemonError::Store(error.to_string()))?,
    );
    let metrics = MetricsRegistry::default();
    let metrics_server = MetricsServer::start(&config.metrics_bind, metrics.clone())
        .map_err(|error| DaemonError::Config(error.to_string()))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .map_err(|_| DaemonError::Runtime)?;
    let result = runtime.block_on(run_async(config, bots, store, metrics));
    drop(metrics_server);
    result
}

async fn run_async(
    config: RustConfig,
    bots: Vec<RuntimeBot>,
    store: Arc<SqliteStore>,
    metrics: MetricsRegistry,
) -> Result<(), DaemonError> {
    let agent = AppServerClient::connect(AppServerConfig::managed(config.codex_socket.clone()))
        .await
        .map_err(|error| DaemonError::AppServer(error.to_string()))?;
    agent
        .wait_connected(APP_SERVER_WAIT)
        .await
        .map_err(|error| DaemonError::AppServer(error.to_string()))?;

    let policy = UpdateRoutingPolicy {
        control_owner_chat_id: config.control_chat_id,
        linked_discussion: LinkedDiscussion::new(config.channel_chat_id, config.discussion_chat_id)
            .map_err(|error| DaemonError::Config(error.to_string()))?,
    };
    let sessions = Arc::new(SessionRegistry::default());
    let bots_by_id = bots
        .iter()
        .cloned()
        .map(|bot| (bot.config.instance_id.clone(), bot))
        .collect::<HashMap<_, _>>();
    let control_runtime = Arc::new(ControlRuntime::default());
    refresh_command_menus(&bots_by_id, &config, &store).await;
    let totp = Arc::new(TotpManager::new(
        store.clone(),
        config.totp_secret_path.clone(),
        config.totp_unlock_seconds,
    ));
    let persisted_projections = store
        .thread_projections()
        .map_err(|error| DaemonError::Store(error.to_string()))?
        .into_iter()
        .filter_map(|(thread_id, _, payload, _)| {
            serde_json::from_value::<ThreadProjection>(payload)
                .ok()
                .map(|projection| (thread_id, projection))
        })
        .collect::<HashMap<_, _>>();
    restore_active_sessions(
        &sessions,
        &store,
        &config,
        &bots_by_id,
        &persisted_projections,
    )?;
    for space in store
        .active_session_spaces()
        .map_err(|error| DaemonError::Store(error.to_string()))?
    {
        if let Err(error) = update_status_message(
            &store,
            &bots_by_id,
            &config,
            &metrics,
            totp.as_ref(),
            &space,
            space
                .thread_id
                .as_deref()
                .and_then(|thread_id| persisted_projections.get(thread_id)),
            None,
            true,
            None,
        )
        .await
        {
            eprintln!(
                "rust bridge status message restore failed for {}: {error}",
                space.space_id
            );
        }
    }
    let _alert_webhook = if let Some(bot) = bots_by_id
        .values()
        .find(|bot| bot.role == RuntimeBotRole::Alert)
    {
        let surface = TelegramSurfaceBinding::Channel(
            ChannelBinding::new(
                bot.config.instance_id.clone(),
                config.alert_chat_id.to_string(),
            )
            .map_err(|error| DaemonError::Config(error.message.to_owned()))?,
        );
        Some(
            AlertWebhookServer::start(
                &config.alert_webhook_bind,
                bot.api.clone(),
                bot.token.clone(),
                surface,
                metrics.clone(),
            )
            .map_err(|error| DaemonError::Config(error.to_string()))?,
        )
    } else {
        eprintln!(
            "rust bridge monitoring alert Bot is disabled; local alert webhook is unavailable"
        );
        None
    };

    let leases = TokenLeaseRegistry::default();
    let shutdown = Arc::new(AtomicBool::new(false));
    let (updates_tx, mut updates_rx) = mpsc::channel(config.max_backlog.max(1));
    let mut pollers = Vec::new();
    for bot in bots.iter().filter(|bot| bot.role.polls_updates()) {
        pollers.push(spawn_poller(
            bot.clone(),
            config.clone(),
            store.clone(),
            metrics.clone(),
            leases.clone(),
            updates_tx.clone(),
            shutdown.clone(),
        ));
    }
    drop(updates_tx);

    let event_task = tokio::spawn(forward_codex_events(
        agent.clone(),
        store.clone(),
        sessions.clone(),
        bots_by_id.clone(),
        config.clone(),
        metrics.clone(),
        totp.clone(),
        control_runtime.clone(),
    ));
    let heartbeat_task = tokio::spawn(run_status_heartbeat_worker(
        store.clone(),
        bots_by_id.clone(),
        config.clone(),
        metrics.clone(),
        totp.clone(),
    ));
    let request_task = tokio::spawn(handle_server_requests(
        agent.clone(),
        store.clone(),
        sessions.clone(),
        bots_by_id.clone(),
        config.clone(),
        metrics.clone(),
    ));
    for space in store
        .active_session_spaces()
        .map_err(|error| DaemonError::Store(error.to_string()))?
    {
        let Some(thread_id) = space.thread_id.as_deref() else {
            continue;
        };
        let Some(session) = sessions.by_thread(thread_id) else {
            continue;
        };
        if session.turn_id.is_none()
            && let Err(error) = dispatch_next_queued(
                &store,
                &agent,
                &sessions,
                &session,
                &bots_by_id,
                &config,
                &metrics,
            )
            .await
        {
            eprintln!("rust bridge startup queue dispatch failed: {error}");
        }
    }
    let new_expiry_task = bots_by_id
        .values()
        .find(|bot| bot.role == RuntimeBotRole::Control)
        .cloned()
        .map(|control_bot| {
            tokio::spawn(run_new_interaction_expirer(
                store.clone(),
                agent.clone(),
                sessions.clone(),
                control_bot,
                bots_by_id.clone(),
                config.clone(),
                metrics.clone(),
            ))
        });
    let deletion_task = tokio::spawn(run_scheduled_deletion_worker(
        store.clone(),
        bots_by_id.clone(),
    ));
    let dispatch_context = Arc::new(DispatchContext {
        bots_by_id,
        policy,
        config,
        store,
        agent: agent.clone(),
        sessions,
        metrics,
        totp,
        control_runtime,
    });
    let dispatch_task = tokio::spawn(async move {
        let mut dispatcher = KeyedDispatcher::new({
            let context = dispatch_context.clone();
            move |inbound: InboundUpdate| {
                let context = context.clone();
                async move { context.process(inbound).await }
            }
        });
        while let Some(inbound) = updates_rx.recv().await {
            dispatcher.dispatch(DispatchKey::for_update(&inbound), inbound);
        }
    });

    tokio::pin!(dispatch_task);
    tokio::select! {
        signal = wait_for_shutdown_signal() => {
            signal?;
            shutdown.store(true, Ordering::Release);
            dispatch_task.abort();
            if let Some(task) = &new_expiry_task {
                task.abort();
            }
            deletion_task.abort();
            heartbeat_task.abort();
        }
        result = &mut dispatch_task => {
            result.map_err(|error| DaemonError::Task(error.to_string()))?;
            shutdown.store(true, Ordering::Release);
            if let Some(task) = &new_expiry_task {
                task.abort();
            }
            deletion_task.abort();
            heartbeat_task.abort();
        }
    }
    let _ = tokio::task::spawn_blocking(move || {
        for poller in pollers {
            let _ = poller.join();
        }
    })
    .await;
    event_task.abort();
    heartbeat_task.abort();
    request_task.abort();
    agent.shutdown().await;
    Ok(())
}

fn restore_active_sessions(
    sessions: &SessionRegistry,
    store: &SqliteStore,
    config: &RustConfig,
    bots_by_id: &HashMap<String, RuntimeBot>,
    projections: &HashMap<String, ThreadProjection>,
) -> Result<(), DaemonError> {
    let mut restored = 0usize;
    for space in store
        .active_session_spaces()
        .map_err(|error| DaemonError::Store(error.to_string()))?
    {
        let (Some(thread_id), Some(chat_id)) = (space.thread_id, space.owner_chat_id) else {
            continue;
        };
        let Ok(thread_id) = ThreadId::new(thread_id) else {
            continue;
        };
        if let Some(projection) = projections.get(thread_id.as_str())
            && let Some(status) = projection
                .turn_status
                .as_deref()
                .filter(|status| matches!(*status, "completed" | "failed" | "interrupted"))
        {
            let reconciled = reconcile_terminal_prompt_intents(
                store,
                thread_id.as_str(),
                status,
                projection.turn_id.as_deref(),
                projection.finished_at_ms,
            )
            .map_err(DaemonError::Store)?;
            if reconciled > 0 {
                eprintln!(
                    "rust bridge reconciled {reconciled} terminal prompt intent(s) for {}",
                    thread_id.as_str()
                );
            }
        }
        let sender_instance_id = if chat_id == config.control_chat_id {
            bots_by_id
                .values()
                .find(|bot| bot.role == RuntimeBotRole::Control)
                .map(|bot| bot.config.instance_id.clone())
        } else if chat_id == config.discussion_chat_id {
            bots_by_id
                .values()
                .find(|bot| bot.role == RuntimeBotRole::Discussion)
                .map(|bot| bot.config.instance_id.clone())
        } else {
            None
        };
        let Some(sender_instance_id) = sender_instance_id else {
            continue;
        };
        let restored_turn_id = projections
            .get(thread_id.as_str())
            .filter(|projection| {
                matches!(
                    projection.turn_status.as_deref(),
                    Some("inProgress" | "active" | "running")
                )
            })
            .and_then(|projection| projection.turn_id.clone())
            .and_then(|turn_id| TurnId::new(turn_id).ok());
        sessions.insert(SessionRecord {
            thread_id,
            turn_id: restored_turn_id,
            chat_id,
            root_message_id: space.discussion_root_message_id,
            sender_instance_id,
        });
        restored += 1;
    }
    eprintln!("rust bridge restored {restored} active session(s)");
    Ok(())
}

fn reconcile_terminal_prompt_intents(
    store: &SqliteStore,
    thread_id: &str,
    status: &str,
    turn_id: Option<&str>,
    finished_at_ms: Option<i64>,
) -> Result<usize, String> {
    let target = match status {
        "completed" => PromptIntentState::Completed,
        "failed" => PromptIntentState::Failed,
        "interrupted" => PromptIntentState::Cancelled,
        _ => return Ok(0),
    };
    let Some(finished_at_ms) = finished_at_ms else {
        return Ok(0);
    };
    let mut reconciled = 0usize;
    for mut intent in store.prompt_intents().map_err(|error| error.to_string())? {
        if intent.thread_id.as_ref().map(ThreadId::as_str) != Some(thread_id)
            || !matches!(
                intent.state,
                PromptIntentState::Started | PromptIntentState::Steered
            )
            || intent.updated_at_ms > finished_at_ms
            || turn_id
                .is_some_and(|turn_id| intent.turn_id.as_ref().map(TurnId::as_str) != Some(turn_id))
        {
            continue;
        }
        intent.state = target;
        intent.updated_at_ms = now_ms();
        store
            .upsert_prompt_intent(&intent)
            .map_err(|error| error.to_string())?;
        reconciled += 1;
    }
    Ok(reconciled)
}

async fn wait_for_shutdown_signal() -> Result<(), DaemonError> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut terminate = signal(SignalKind::terminate())
            .map_err(|error| DaemonError::Task(error.to_string()))?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                result.map_err(|error| DaemonError::Task(error.to_string()))
            }
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .map_err(|error| DaemonError::Task(error.to_string()))
    }
}

fn spawn_poller(
    bot: RuntimeBot,
    config: RustConfig,
    store: Arc<SqliteStore>,
    metrics: MetricsRegistry,
    leases: TokenLeaseRegistry,
    updates_tx: mpsc::Sender<InboundUpdate>,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name(format!("rust-poll-{}", bot.config.instance_id))
        .spawn(move || {
            let consumer = match codex_telegram_adapter::UpdateConsumerId::parse(
                bot.config.update_consumer.clone(),
            ) {
                Ok(consumer) => consumer,
                Err(_) => return,
            };
            let lease = match leases.acquire_with_lock(&bot.token, consumer, &config.lock_directory)
            {
                Ok(lease) => lease,
                Err(error) => {
                    eprintln!(
                        "rust bridge could not acquire {} polling lease: {error}",
                        bot.config.instance_id
                    );
                    return;
                }
            };
            let mut offset = store
                .next_update_offset(&bot.config.instance_id)
                .ok()
                .flatten();
            // Updates are dispatched fire-and-forget into the keyed
            // dispatcher so a slow handler never blocks the polling loop.
            // The Telegram offset only advances once every update below it
            // has reported completion (contiguous-prefix confirmation),
            // which preserves the previous at-most-once record semantics.
            let (done_tx, done_rx) = std::sync::mpsc::channel::<(i64, bool)>();
            let mut pending: std::collections::BTreeMap<i64, bool> =
                std::collections::BTreeMap::new();
            loop {
                if shutdown.load(Ordering::Acquire) {
                    return;
                }
                let updates = match bot
                    .api
                    .get_updates(&lease, offset, config.poll_timeout_seconds)
                {
                    Ok(updates) => {
                        metrics.observe_poll_for(role_label(bot.role), true);
                        updates
                    }
                    Err(error) => {
                        metrics.observe_poll_for(role_label(bot.role), false);
                        if shutdown.load(Ordering::Acquire) {
                            return;
                        }
                        eprintln!(
                            "rust bridge poll failed for {}: {error}",
                            bot.config.instance_id
                        );
                        std::thread::sleep(Duration::from_secs(1));
                        continue;
                    }
                };
                let processing_started = Instant::now();
                metrics.set_queue_depth(updates.len() as u64);
                for update in updates.into_iter().take(config.max_backlog) {
                    let update_id = update.update_id;
                    if pending.contains_key(&update_id) {
                        // Already dispatched in an earlier cycle and still
                        // waiting on its completion (or on a lower update to
                        // confirm first).
                        continue;
                    }
                    if store
                        .processed_update_exists(&bot.config.instance_id, update_id)
                        .unwrap_or(false)
                    {
                        pending.insert(update_id, true);
                        continue;
                    }
                    let mut inbound = InboundUpdate {
                        bot_instance_id: bot.config.instance_id.clone(),
                        update,
                        completion: done_tx.clone(),
                    };
                    loop {
                        match updates_tx.try_send(inbound) {
                            Ok(()) => break,
                            Err(mpsc::error::TrySendError::Full(returned)) => {
                                inbound = returned;
                                if shutdown.load(Ordering::Acquire) {
                                    return;
                                }
                                std::thread::sleep(Duration::from_millis(25));
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => return,
                        }
                    }
                    pending.insert(update_id, false);
                }
                // Drain completions.  When unconfirmed updates remain, pace
                // the loop with a short wait so a stalled handler does not
                // spin the poller against Telegram's getUpdates.
                let wait = if pending.values().any(|confirmed| !confirmed) {
                    Duration::from_millis(500)
                } else {
                    Duration::from_millis(0)
                };
                loop {
                    let result = if wait.is_zero() {
                        done_rx.try_recv().map_err(|error| match error {
                            std::sync::mpsc::TryRecvError::Empty => {
                                std::sync::mpsc::RecvTimeoutError::Timeout
                            }
                            std::sync::mpsc::TryRecvError::Disconnected => {
                                std::sync::mpsc::RecvTimeoutError::Disconnected
                            }
                        })
                    } else {
                        done_rx.recv_timeout(wait)
                    };
                    match result {
                        Ok((update_id, handled)) => {
                            if handled {
                                if let Some(confirmed) = pending.get_mut(&update_id) {
                                    *confirmed = true;
                                }
                            } else {
                                // Leave the update unconfirmed and
                                // redeliverable: it stays below the
                                // confirmation prefix and is re-fetched (and
                                // re-dispatched) on a later cycle.
                                pending.remove(&update_id);
                            }
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => break,
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
                    }
                    if shutdown.load(Ordering::Acquire) {
                        return;
                    }
                }
                // Advance the offset over the confirmed prefix.  Pending
                // ids iterate in ascending order; recording stops at the
                // first unconfirmed update (or on a store error, which is
                // retried on the next cycle).
                for update_id in confirmed_update_prefix(&pending) {
                    match store.record_processed_update(
                        &bot.config.instance_id,
                        update_id,
                        now_ms(),
                    ) {
                        Ok(_) => {
                            pending.remove(&update_id);
                            offset = Some(update_id.saturating_add(1));
                        }
                        Err(error) => {
                            eprintln!("rust bridge update state failed: {error}");
                            break;
                        }
                    }
                }
                metrics.set_event_loop_lag_micros(processing_started.elapsed().as_micros() as u64);
            }
        })
        .expect("poller thread must start")
}

/// Leading run of confirmed update ids in ascending order.  The Telegram
/// offset may only advance past a contiguous completed prefix, so polling
/// stops confirming at the first update whose handler has not finished.
fn confirmed_update_prefix(pending: &std::collections::BTreeMap<i64, bool>) -> Vec<i64> {
    pending
        .iter()
        .take_while(|(_, done)| **done)
        .map(|(id, _)| *id)
        .collect()
}

/// First up to 8 chars of an identifier, safe for non-ASCII values where
/// byte slicing would panic.
fn short_id_prefix(id: &str) -> &str {
    let end = id
        .char_indices()
        .nth(8)
        .map_or(id.len(), |(index, _)| index);
    &id[..end]
}

#[allow(clippy::too_many_arguments)]
async fn handle_action(
    routed: RoutedUpdate,
    actor_user_id: Option<i64>,
    inbound_bot: RuntimeBot,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    store: &Arc<SqliteStore>,
    agent: &AppServerClient,
    sessions: &Arc<SessionRegistry>,
    metrics: &MetricsRegistry,
    totp: &Arc<TotpManager>,
    control_runtime: &Arc<ControlRuntime>,
) -> Result<(), String> {
    let RoutedUpdate::Dispatch(action) = routed else {
        return Ok(());
    };
    match action {
        WorkflowAction::Command {
            command,
            chat_id,
            message_id,
            root_message_id,
            text,
        } => {
            if !command.allowed_for_role(inbound_bot.role) {
                return Ok(());
            }
            handle_command(
                command,
                actor_user_id,
                chat_id,
                message_id,
                root_message_id,
                text,
                inbound_bot,
                bots_by_id,
                config,
                store,
                agent,
                sessions,
                metrics,
                totp,
                control_runtime,
            )
            .await
        }
        WorkflowAction::Prompt {
            chat_id,
            message_id,
            text,
            root_message_id,
        } => {
            if inbound_bot.role == RuntimeBotRole::Control
                && handle_new_text(
                    store,
                    agent,
                    sessions,
                    &inbound_bot,
                    bots_by_id,
                    config,
                    metrics,
                    chat_id,
                    actor_user_id,
                    message_id,
                    &text,
                )
                .await?
            {
                return Ok(());
            }
            let discussion_space = if inbound_bot.role == RuntimeBotRole::Discussion {
                root_message_id.and_then(|root| {
                    store
                        .session_space_for_discussion_root(chat_id, root)
                        .ok()
                        .flatten()
                })
            } else {
                None
            };
            if inbound_bot.role == RuntimeBotRole::Discussion && discussion_space.is_none() {
                eprintln!(
                    "rust bridge ignored prompt without an exact discussion root chat_id={chat_id}"
                );
                return Ok(());
            }
            let unlocked = if let Some(space) = discussion_space.as_ref() {
                totp.is_unlocked_for_space(&space.space_id, now_ms())
            } else {
                totp.is_unlocked(now_ms())
            }
            .map_err(|error| error.to_string())?;
            if !unlocked {
                send_text(
                    &inbound_bot,
                    &surface_for(&inbound_bot, config, chat_id, root_message_id),
                    "写操作已锁定，请先发送 /totp <6 位验证码>。",
                    metrics,
                )
                .await?;
                return Ok(());
            }
            let Some(session) = sessions.by_chat(chat_id) else {
                send_text(
                    &inbound_bot,
                    &surface_for(&inbound_bot, config, chat_id, root_message_id),
                    "请先发送 /new 创建一个 Codex Session。",
                    metrics,
                )
                .await?;
                return Ok(());
            };
            sessions.set_root(chat_id, root_message_id);
            if let Some(reply_message_id) = root_message_id
                && let Some(publication) =
                    revising_plan_for_reply(store, &session.thread_id, reply_message_id)?
            {
                return submit_plan_revision_feedback(
                    store,
                    agent,
                    sessions,
                    &inbound_bot,
                    config,
                    metrics,
                    session,
                    publication,
                    &text,
                    message_id,
                )
                .await;
            }
            submit_prompt_intent(
                store,
                agent,
                sessions,
                &inbound_bot,
                config,
                metrics,
                session,
                &text,
                "steer",
                message_id,
            )
            .await
        }
        WorkflowAction::Attachment {
            chat_id,
            message_id,
            caption,
            file_id,
            file_name,
            is_photo,
            root_message_id,
        } => {
            let surface = surface_for(&inbound_bot, config, chat_id, root_message_id);
            let Some(session) = sessions.by_chat(chat_id) else {
                return send_text(
                    &inbound_bot,
                    &surface,
                    "请先发送 /new 创建一个 Codex Session。",
                    metrics,
                )
                .await;
            };
            let write_unlocked = match store
                .session_space_for_thread(session.thread_id.as_str())
                .map_err(|error| error.to_string())?
            {
                Some(space) => totp.is_unlocked_for_space(&space.space_id, now_ms()),
                None => totp.is_unlocked(now_ms()),
            }
            .map_err(|error| error.to_string())?;
            if !write_unlocked {
                return send_text(
                    &inbound_bot,
                    &surface,
                    "写操作已锁定，请先发送 /totp <6 位验证码>。",
                    metrics,
                )
                .await;
            }
            if file_id.trim().is_empty() {
                return send_text(&inbound_bot, &surface, "未识别到可下载的附件。", metrics).await;
            }
            let token = inbound_bot.token.clone();
            let api = inbound_bot.api.clone();
            let result = tokio::task::spawn_blocking(move || {
                let remote = api.get_file(&token, &file_id)?;
                let path =
                    remote
                        .file_path
                        .ok_or(codex_telegram_adapter::TelegramError::InvalidInput(
                            "Telegram file path is missing",
                        ))?;
                let bytes = api.download_file(&token, &path, MAX_ARTIFACT_BYTES as usize)?;
                Ok::<_, codex_telegram_adapter::TelegramError>((bytes, path))
            })
            .await
            .map_err(|error| error.to_string())?;
            match result {
                Ok((bytes, path)) => {
                    let name = file_name
                        .or_else(|| {
                            Path::new(&path)
                                .file_name()
                                .map(|value| value.to_string_lossy().into_owned())
                        })
                        .unwrap_or_else(|| format!("telegram-{message_id}.bin"));
                    let safe_name = safe_attachment_name(&name, message_id);
                    let upload_dir = config
                        .state_directory
                        .join("uploads")
                        .join(chat_id.to_string());
                    ensure_private_directory(&upload_dir).map_err(|error| error.to_string())?;
                    let protected_paths =
                        protected_upload_paths(store, &config.state_directory.join("uploads"));
                    cleanup_upload_directory(
                        &config.state_directory.join("uploads"),
                        now_ms().saturating_sub(UPLOAD_RETENTION_MS),
                        &protected_paths,
                    );
                    let upload_path = upload_dir.join(&safe_name);
                    fs::write(&upload_path, &bytes)
                        .map_err(|error| format!("附件保存失败: {error}"))?;
                    let session_id =
                        ensure_approval_session(store, session.thread_id.as_str(), now_ms())?;
                    let mut digest = Sha256::new();
                    digest.update(&bytes);
                    let artifact = Artifact::new(
                        ArtifactId::new(format!("telegram-artifact-{}", next_approval_nonce()))
                            .map_err(|error| error.to_string())?,
                        session_id,
                        upload_path.to_string_lossy().to_string(),
                        format!("{:x}", digest.finalize()),
                        bytes.len() as u64,
                        now_ms(),
                    )
                    .map_err(|error| error.to_string())?;
                    let event = DomainEvent {
                        id: EventId::new(format!("artifact-recorded-{}", next_approval_nonce()))
                            .map_err(|error| error.to_string())?,
                        occurred_at_ms: now_ms(),
                        kind: DomainEventKind::ArtifactRecorded {
                            artifact: artifact.clone(),
                        },
                    };
                    store
                        .insert_artifact(&artifact, &event)
                        .map_err(|error| error.to_string())?;
                    let explanation = caption
                        .filter(|value| !value.trim().is_empty())
                        .unwrap_or_else(|| {
                            format!("请读取并处理 Telegram 上传的文件：{safe_name}")
                        });
                    let inputs = vec![
                        PromptInput::text(&explanation).map_err(|error| error.to_string())?,
                        if is_photo {
                            PromptInput::LocalImage {
                                path: upload_path.to_string_lossy().into_owned(),
                                detail: "auto".into(),
                            }
                        } else {
                            PromptInput::Mention {
                                name: safe_name.clone(),
                                path: upload_path.to_string_lossy().into_owned(),
                            }
                        },
                    ];
                    submit_prompt_intent_with_inputs(
                        store,
                        agent,
                        sessions,
                        &inbound_bot,
                        config,
                        metrics,
                        session,
                        &explanation,
                        "upload",
                        message_id,
                        Some(inputs),
                    )
                    .await
                }
                Err(error) => {
                    send_text(
                        &inbound_bot,
                        &surface,
                        &format!("附件下载失败：{error}"),
                        metrics,
                    )
                    .await
                }
            }
        }
        WorkflowAction::NativeCommentPost {
            channel_chat_id,
            channel_post_id,
            discussion_chat_id,
            discussion_root_message_id,
        } => {
            if store
                .workflow_record("onboarding", "binding")
                .map_err(|error| error.to_string())?
                .is_none()
            {
                eprintln!(
                    "rust bridge ignored native comment before binding channel_post_id={channel_post_id}"
                );
                return Ok(());
            }
            store
                .bind_native_comment_root(
                    &NativeCommentRoot {
                        channel_chat_id,
                        channel_post_id,
                        discussion_chat_id,
                        root_message_id: discussion_root_message_id,
                    },
                    now_ms(),
                )
                .map_err(|error| error.to_string())?;
            for space in store
                .session_spaces()
                .map_err(|error| error.to_string())?
                .into_iter()
                .filter(|space| {
                    space.channel_chat_id == channel_chat_id
                        && space.channel_post_id == channel_post_id
                        && space.discussion_chat_id == Some(discussion_chat_id)
                        && space.discussion_root_message_id == Some(discussion_root_message_id)
                })
            {
                if let Err(error) = ensure_status_message(
                    store,
                    bots_by_id,
                    config,
                    metrics,
                    totp.as_ref(),
                    &space,
                    None,
                    None,
                )
                .await
                {
                    eprintln!("rust bridge status message provisioning failed: {error}");
                }
            }
            Ok(())
        }
        WorkflowAction::Callback(callback) => {
            handle_callback(
                callback,
                inbound_bot,
                bots_by_id,
                config,
                store,
                agent,
                sessions,
                metrics,
                totp,
                control_runtime,
            )
            .await
        }
        WorkflowAction::MembershipChanged => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_command(
    command: WorkflowCommand,
    actor_user_id: Option<i64>,
    chat_id: i64,
    message_id: i64,
    root_message_id: Option<i64>,
    text: String,
    inbound_bot: RuntimeBot,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    store: &Arc<SqliteStore>,
    agent: &AppServerClient,
    sessions: &Arc<SessionRegistry>,
    metrics: &MetricsRegistry,
    totp: &Arc<TotpManager>,
    control_runtime: &Arc<ControlRuntime>,
) -> Result<(), String> {
    let bound_space = if inbound_bot.role == RuntimeBotRole::Discussion {
        root_message_id.and_then(|root| {
            store
                .session_space_for_discussion_root(chat_id, root)
                .ok()
                .flatten()
        })
    } else {
        None
    };
    if inbound_bot.role == RuntimeBotRole::Discussion
        && !matches!(command, WorkflowCommand::Bind | WorkflowCommand::Help)
        && bound_space.is_none()
    {
        eprintln!("rust bridge ignored discussion command without an exact root chat_id={chat_id}");
        return Ok(());
    }
    let response_root_message_id = bound_space
        .as_ref()
        .and_then(|space| space.discussion_root_message_id)
        .or(root_message_id);
    let surface = surface_for(&inbound_bot, config, chat_id, response_root_message_id);
    let write_unlocked = if let Some(space) = bound_space.as_ref() {
        totp.is_unlocked_for_space(&space.space_id, now_ms())
    } else {
        totp.is_unlocked(now_ms())
    }
    .map_err(|error| error.to_string())?;
    if matches!(
        command,
        WorkflowCommand::PlanMode
            | WorkflowCommand::ChangeModel
            | WorkflowCommand::Review
            | WorkflowCommand::Cancel
            | WorkflowCommand::GetFile
            | WorkflowCommand::Attach
            | WorkflowCommand::Unwatch
            | WorkflowCommand::Answer
    ) && !write_unlocked
    {
        send_text(
            &inbound_bot,
            &surface,
            "该命令是写操作，请先发送 /totp <6 位验证码>。",
            metrics,
        )
        .await?;
        return Ok(());
    }
    match command {
        WorkflowCommand::New => {
            begin_new_interaction(
                store,
                agent,
                sessions,
                &inbound_bot,
                bots_by_id,
                config,
                metrics,
                chat_id,
                actor_user_id,
                message_id,
                &text,
            )
            .await
        }
        WorkflowCommand::Status => {
            let state = agent.connection_state();
            let schema = store.schema_version().map_err(|error| error.to_string())?;
            let unlocked = write_unlocked;
            send_text(
                &inbound_bot,
                &surface,
                &format!(
                    "Rust Bridge 状态：{}\nCodex generation: {}\nRust SQLite schema: {}\nwrite_unlocked={}",
                    if state.connected { "connected" } else { "disconnected" },
                    state.generation,
                    schema,
                    unlocked
                ),
                metrics,
            )
            .await
        }
        WorkflowCommand::Perf => {
            handle_control_perf(
                control_runtime,
                &inbound_bot,
                config,
                store,
                metrics,
                chat_id,
                actor_user_id,
                message_id,
            )
            .await
        }
        WorkflowCommand::Sessions => {
            handle_control_sessions(
                agent,
                &inbound_bot,
                config,
                store,
                metrics,
                control_runtime,
                chat_id,
                actor_user_id,
                message_id,
                &text,
            )
            .await
        }
        WorkflowCommand::Topics => {
            handle_control_topics(
                &inbound_bot,
                config,
                store,
                metrics,
                chat_id,
                actor_user_id,
                message_id,
            )
            .await
        }
        WorkflowCommand::Help => {
            if inbound_bot.role == RuntimeBotRole::Control {
                let paired = store
                    .workflow_record("onboarding", "owner")
                    .map_err(|error| error.to_string())?
                    .is_some();
                return handle_control_help(
                    &inbound_bot,
                    config,
                    metrics,
                    chat_id,
                    actor_user_id,
                    message_id,
                    paired,
                )
                .await;
            }
            let help = match inbound_bot.role {
                RuntimeBotRole::Discussion => {
                    "/status  查看当前 Session 状态\n/totp <code>  认证当前 Session\n/lock  锁定当前 Session\n/planmode on|off  切换 Plan Mode\n/changemodel <model> [effort]  切换模型\n/review [target]  启动 Review\n/cancel  取消当前 turn\n/getfile <relative-path>  发送 workspace 文件\n/help  查看命令\n直接发送文本  提交 Codex Prompt"
                }
                RuntimeBotRole::Status | RuntimeBotRole::Alert => "当前 Bot 没有可用命令。",
                RuntimeBotRole::Control => unreachable!(),
            };
            send_text(&inbound_bot, &surface, help, metrics).await
        }
        WorkflowCommand::Totp => {
            if inbound_bot.role == RuntimeBotRole::Discussion {
                delete_inbound_message(&inbound_bot, chat_id, message_id).await;
            }
            let code = text.split_whitespace().nth(1).unwrap_or_default();
            if code.is_empty() {
                return send_text(
                    &inbound_bot,
                    &surface,
                    "用法：`/totp <6 位验证码或恢复码>`",
                    metrics,
                )
                .await;
            }
            let verified = if let Some(space) = bound_space.as_ref() {
                totp.verify_and_unlock_for_space(&space.space_id, code, now_ms())
            } else {
                totp.verify_and_unlock(code, now_ms())
            }
            .map_err(|error| error.to_string())?;
            if verified
                && let Some(space) = bound_space.as_ref()
                && matches!(space.lifecycle.as_str(), "pending" | "repair_required")
            {
                return activate_pending_session(
                    store,
                    agent,
                    sessions,
                    &inbound_bot,
                    bots_by_id,
                    config,
                    metrics,
                    totp,
                    space,
                )
                .await;
            }
            let message = if verified {
                format!(
                    "当前 Session 已解锁 {} 分钟。",
                    config.totp_unlock_seconds.max(60) / 60
                )
            } else {
                "验证码无效、已使用，或验证暂时锁定。".to_owned()
            };
            send_text(&inbound_bot, &surface, &message, metrics).await
        }
        WorkflowCommand::Lock => {
            if let Some(space) = bound_space.as_ref() {
                totp.lock_space(&space.space_id)
                    .map_err(|error| error.to_string())?;
            } else {
                totp.lock().map_err(|error| error.to_string())?;
            }
            send_text(
                &inbound_bot,
                &surface,
                "Rust Bridge write operations are locked.",
                metrics,
            )
            .await
        }
        WorkflowCommand::Pair => {
            handle_pair_command(
                &text,
                chat_id,
                actor_user_id,
                &inbound_bot,
                bots_by_id,
                config,
                store,
                metrics,
            )
            .await
        }
        WorkflowCommand::Bind => {
            handle_bind_command(
                &text,
                chat_id,
                actor_user_id,
                &inbound_bot,
                bots_by_id,
                config,
                store,
                metrics,
            )
            .await
        }
        command @ (WorkflowCommand::Prompt | WorkflowCommand::Ask) => {
            let Some(session) = sessions.by_chat(chat_id) else {
                return send_text(
                    &inbound_bot,
                    &surface,
                    "请先发送 /new 创建一个 Codex Session。",
                    metrics,
                )
                .await;
            };
            let prompt = text
                .split_once(char::is_whitespace)
                .map(|(_, value)| value.trim())
                .unwrap_or_default();
            if prompt.is_empty() {
                return send_text(
                    &inbound_bot,
                    &surface,
                    if matches!(command, WorkflowCommand::Ask) {
                        "用法：/ask <问题>"
                    } else {
                        "用法：/prompt <内容>"
                    },
                    metrics,
                )
                .await;
            }
            let mode = if matches!(command, WorkflowCommand::Ask) {
                "ask"
            } else {
                "steer"
            };
            submit_prompt_intent(
                store,
                agent,
                sessions,
                &inbound_bot,
                config,
                metrics,
                session,
                prompt,
                mode,
                message_id,
            )
            .await
        }
        WorkflowCommand::Queue => {
            let Some(session) = sessions.by_chat(chat_id) else {
                return send_text(
                    &inbound_bot,
                    &surface,
                    "请先发送 /new 创建一个 Codex Session。",
                    metrics,
                )
                .await;
            };
            let prompt = text
                .split_once(char::is_whitespace)
                .map(|(_, value)| value.trim())
                .unwrap_or_default();
            if prompt.is_empty() {
                return render_queue(store, &inbound_bot, config, metrics, &session).await;
            }
            enqueue_prompt(store, &session, prompt, message_id)?;
            let active = session.turn_id.is_some();
            if !active {
                dispatch_next_queued(
                    store, agent, sessions, &session, bots_by_id, config, metrics,
                )
                .await?;
            }
            send_text(
                &inbound_bot,
                &surface_for(&inbound_bot, config, chat_id, session.root_message_id),
                if active {
                    "📥 已加入队列；当前 turn 完成后会按顺序提交。"
                } else {
                    "📥 已加入队列；Rust Bridge 会在 Session 空闲时提交。"
                },
                metrics,
            )
            .await
        }
        WorkflowCommand::Plan => {
            render_plan_command(
                agent,
                store,
                &inbound_bot,
                config,
                metrics,
                chat_id,
                sessions.by_chat(chat_id),
            )
            .await
        }
        WorkflowCommand::Timeline => {
            render_timeline_command(
                agent,
                &inbound_bot,
                config,
                metrics,
                chat_id,
                sessions.by_chat(chat_id),
            )
            .await
        }
        WorkflowCommand::Attach => {
            handle_attach_command(
                agent,
                &inbound_bot,
                config,
                metrics,
                chat_id,
                sessions.by_chat(chat_id),
            )
            .await
        }
        WorkflowCommand::Unwatch => {
            let Some(session) = sessions.by_chat(chat_id) else {
                return send_text(
                    &inbound_bot,
                    &surface,
                    "当前没有可取消监控的 Session。",
                    metrics,
                )
                .await;
            };
            let Some(space) = store
                .session_space_for_thread(session.thread_id.as_str())
                .map_err(|error| error.to_string())?
            else {
                return send_text(
                    &inbound_bot,
                    &surface,
                    "当前没有可取消监控的 Session。",
                    metrics,
                )
                .await;
            };
            let markup = discussion_callback_markup_rows(
                store,
                &space,
                &[
                    &[("确认取消关注", "status_unwatch_execute")],
                    &[("返回", "status_unwatch_cancel")],
                ],
            )?;
            send_text_with_markup(
                &inbound_bot,
                &surface_for(&inbound_bot, config, chat_id, session.root_message_id),
                UNWATCH_CONFIRM_MESSAGE,
                markup,
                metrics,
            )
            .await
        }
        WorkflowCommand::Answer => {
            handle_answer_command(
                agent,
                store,
                &inbound_bot,
                config,
                metrics,
                chat_id,
                text.split_once(char::is_whitespace)
                    .map(|(_, value)| value.trim())
                    .unwrap_or_default(),
                sessions.by_chat(chat_id),
            )
            .await
        }
        WorkflowCommand::PlanMode => {
            let Some(session) = sessions.by_chat(chat_id) else {
                return send_text(
                    &inbound_bot,
                    &surface,
                    "请先发送 /new 创建一个 Codex Session。",
                    metrics,
                )
                .await;
            };
            let requested = text
                .split_whitespace()
                .nth(1)
                .map(str::to_ascii_lowercase)
                .unwrap_or_else(|| "on".into());
            let enabled = match requested.as_str() {
                "on" | "enable" | "enabled" | "plan" => true,
                "off" | "disable" | "disabled" | "default" => false,
                _ => {
                    return send_text(
                        &inbound_bot,
                        &surface,
                        "用法：/planmode on 或 /planmode off",
                        metrics,
                    )
                    .await;
                }
            };
            let mode = collaboration_mode_payload(
                agent,
                if enabled { "plan" } else { "default" },
                None,
                None,
            )
            .await?;
            agent
                .request(
                    "thread/settings/update",
                    json!({"threadId": session.thread_id.as_str(), "collaborationMode": mode}),
                    Duration::from_secs(30),
                )
                .await
                .map_err(|error| error.to_string())?;
            update_session_plan_mode(store, &session.thread_id, enabled)?;
            send_text(
                &inbound_bot,
                &surface,
                if enabled {
                    "Plan Mode 已开启；后续 turn 将使用 Codex 的 plan collaboration mode。"
                } else {
                    "Plan Mode 已关闭；后续 turn 将使用 default collaboration mode。"
                },
                metrics,
            )
            .await
        }
        WorkflowCommand::ChangeModel => {
            let Some(session) = sessions.by_chat(chat_id) else {
                return send_text(
                    &inbound_bot,
                    &surface,
                    "请先发送 /new 创建一个 Codex Session。",
                    metrics,
                )
                .await;
            };
            let models = list_model_choices(agent).await?;
            let mut fields = text.split_whitespace();
            let _ = fields.next();
            let Some(model) = fields.next() else {
                return send_text(
                    &inbound_bot,
                    &surface,
                    &format!(
                        "可用模型：\n{}\n用法：/changemodel <model> [effort]",
                        models.summary
                    ),
                    metrics,
                )
                .await;
            };
            let model = model.to_owned();
            let Some(entry) = models.entries.iter().find(|entry| entry.model == model) else {
                return send_text(
                    &inbound_bot,
                    &surface,
                    "模型不在 app-server 的当前可用列表中；请先查看 /changemodel。",
                    metrics,
                )
                .await;
            };
            let effort = fields
                .next()
                .map(str::to_owned)
                .unwrap_or_else(|| entry.default_effort.clone());
            if !entry.efforts.iter().any(|value| value == &effort) {
                return send_text(
                    &inbound_bot,
                    &surface,
                    &format!(
                        "effort {} 不适用于 {}；可用：{}",
                        effort,
                        entry.model,
                        entry.efforts.join(", ")
                    ),
                    metrics,
                )
                .await;
            }
            agent
                .request(
                    "thread/settings/update",
                    json!({"threadId": session.thread_id.as_str(), "model": entry.model, "effort": effort}),
                    Duration::from_secs(30),
                )
                .await
                .map_err(|error| error.to_string())?;
            send_text(
                &inbound_bot,
                &surface,
                &format!(
                    "模型已切换为 {} ({})，后续 turn 生效。",
                    entry.model, effort
                ),
                metrics,
            )
            .await
        }
        WorkflowCommand::Review => {
            let Some(session) = sessions.by_chat(chat_id) else {
                return send_text(
                    &inbound_bot,
                    &surface,
                    "请先发送 /new 创建一个 Codex Session。",
                    metrics,
                )
                .await;
            };
            let target = parse_review_target(&text)?;
            let result = agent
                .request(
                    "review/start",
                    json!({
                        "threadId": session.thread_id.as_str(),
                        "delivery": "inline",
                        "target": target,
                    }),
                    Duration::from_secs(60),
                )
                .await
                .map_err(|error| error.to_string())?;
            let turn_id = result
                .get("turn")
                .and_then(|turn| turn.get("id"))
                .and_then(Value::as_str)
                .ok_or_else(|| "Codex review response did not include turn.id".to_owned())?;
            sessions.set_turn(
                session.thread_id.as_str(),
                Some(TurnId::new(turn_id).map_err(|error| error.to_string())?),
            );
            send_text(
                &inbound_bot,
                &surface,
                "Review 已启动，完成后会把结果回传到 Telegram。",
                metrics,
            )
            .await
        }
        WorkflowCommand::Cancel => {
            let Some(session) = sessions.by_chat(chat_id) else {
                return send_text(
                    &inbound_bot,
                    &surface,
                    "当前没有可取消的 Codex Session。",
                    metrics,
                )
                .await;
            };
            let Some(turn_id) = session.turn_id else {
                return send_text(&inbound_bot, &surface, "当前没有正在运行的 turn。", metrics)
                    .await;
            };
            agent
                .request(
                    "turn/interrupt",
                    json!({"threadId": session.thread_id.as_str(), "turnId": turn_id.as_str()}),
                    Duration::from_secs(30),
                )
                .await
                .map_err(|error| error.to_string())?;
            sessions.set_turn(session.thread_id.as_str(), None);
            send_text(
                &inbound_bot,
                &surface,
                "已请求取消当前 Codex turn。",
                metrics,
            )
            .await
        }
        WorkflowCommand::GetFile => {
            let Some(session) = sessions.by_chat(chat_id) else {
                return send_text(
                    &inbound_bot,
                    &surface,
                    "请先发送 /new 创建一个 Codex Session。",
                    metrics,
                )
                .await;
            };
            let requested = text.split_whitespace().nth(1).unwrap_or_default();
            if requested.is_empty() {
                return send_text(
                    &inbound_bot,
                    &surface,
                    "用法：/getfile <workspace 内相对路径>",
                    metrics,
                )
                .await;
            }
            let artifact = read_workspace_artifact(&config.workspace_root, requested)?;
            let session_id = ensure_approval_session(store, session.thread_id.as_str(), now_ms())?;
            let artifact_id = ArtifactId::new(format!("artifact-{}", next_approval_nonce()))
                .map_err(|error| error.to_string())?;
            let artifact_record = Artifact::new(
                artifact_id,
                session_id,
                artifact.relative_path.clone(),
                artifact.sha256.clone(),
                artifact.bytes.len() as u64,
                now_ms(),
            )
            .map_err(|error| error.to_string())?;
            let event = DomainEvent {
                id: EventId::new(format!("artifact-recorded-{}", next_approval_nonce()))
                    .map_err(|error| error.to_string())?,
                occurred_at_ms: now_ms(),
                kind: DomainEventKind::ArtifactRecorded {
                    artifact: artifact_record.clone(),
                },
            };
            store
                .insert_artifact(&artifact_record, &event)
                .map_err(|error| error.to_string())?;
            send_document(
                &inbound_bot,
                &surface_for(&inbound_bot, config, chat_id, session.root_message_id),
                &artifact.file_name,
                artifact.bytes,
                Some(&format!(
                    "{}\nsha256={}",
                    artifact.relative_path, artifact.sha256
                )),
                metrics,
            )
            .await
        }
        WorkflowCommand::Unknown(_) => {
            send_text(&inbound_bot, &surface, "未知命令，请发送 /help。", metrics).await
        }
    }
}

const CONTROL_UTC_OFFSET_SECONDS: i64 = 8 * 60 * 60;

async fn refresh_command_menus(
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    store: &SqliteStore,
) {
    let paired = store
        .workflow_record("onboarding", "owner")
        .ok()
        .flatten()
        .is_some();
    let owner_user_id = store
        .workflow_record("onboarding", "owner")
        .ok()
        .flatten()
        .and_then(|value| value.get("user_id").and_then(Value::as_i64));
    let discussion_bound = store
        .workflow_record("onboarding", "binding")
        .ok()
        .flatten()
        .is_some();
    let mut tasks = Vec::new();
    for bot in bots_by_id.values().filter(|bot| bot.role.polls_updates()) {
        let bot = bot.clone();
        let control_chat_id = config.control_chat_id;
        let discussion_chat_id = config.discussion_chat_id;
        tasks.push(tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                let install = |scope: BotCommandScope,
                               commands: &'static [codex_telegram_adapter::CommandMenuEntry]| {
                    if commands.is_empty() {
                        bot.api.delete_my_commands(&bot.token, scope).map(|_| ())
                    } else {
                        bot.api
                            .set_my_commands(&bot.token, commands, scope)
                            .map(|_| ())
                    }
                };
                match bot.role {
                    RuntimeBotRole::Control => {
                        install(BotCommandScope::Default, &[])?;
                        install(
                            BotCommandScope::AllPrivateChats,
                            command_menu(RuntimeBotRole::Control, CommandMenuScope::Bootstrap),
                        )?;
                        if paired {
                            install(
                                BotCommandScope::Chat {
                                    chat_id: control_chat_id,
                                },
                                command_menu(RuntimeBotRole::Control, CommandMenuScope::Owner),
                            )?;
                        } else {
                            install(
                                BotCommandScope::Chat {
                                    chat_id: control_chat_id,
                                },
                                &[],
                            )?;
                        }
                    }
                    RuntimeBotRole::Discussion => {
                        install(BotCommandScope::Default, &[])?;
                        install(
                            BotCommandScope::AllGroupChats,
                            command_menu(RuntimeBotRole::Discussion, CommandMenuScope::Bootstrap),
                        )?;
                        install(
                            BotCommandScope::Chat {
                                chat_id: discussion_chat_id,
                            },
                            &[],
                        )?;
                        if let Some(owner_user_id) = owner_user_id {
                            install(
                                BotCommandScope::ChatMember {
                                    chat_id: discussion_chat_id,
                                    user_id: owner_user_id,
                                },
                                if paired && discussion_bound {
                                    command_menu(
                                        RuntimeBotRole::Discussion,
                                        CommandMenuScope::Owner,
                                    )
                                } else {
                                    &[]
                                },
                            )?;
                        }
                    }
                    RuntimeBotRole::Status | RuntimeBotRole::Alert => {
                        install(BotCommandScope::Default, &[])?;
                    }
                }
                Ok::<(), codex_telegram_adapter::TelegramError>(())
            })
            .await;
            match result {
                Ok(Ok(())) => Ok::<(), String>(()),
                Ok(Err(error)) => Err(error.to_string()),
                Err(error) => Err(error.to_string()),
            }
        }));
    }
    for task in tasks {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                eprintln!("rust bridge command menu refresh failed: {error}");
            }
            Err(error) => {
                eprintln!("rust bridge command menu refresh task failed: {error}");
            }
        }
    }
}

async fn run_scheduled_deletion_worker(
    store: Arc<SqliteStore>,
    bots_by_id: HashMap<String, RuntimeBot>,
) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let due = match store.claim_due_deletions(now_ms(), 32) {
            Ok(due) => due,
            Err(error) => {
                eprintln!("rust bridge scheduled deletion claim failed: {error}");
                continue;
            }
        };
        for deletion in due {
            let result = delete_scheduled_message(&bots_by_id, &deletion).await;
            match result {
                Ok(()) => {
                    let _ = store.complete_deletion(
                        &deletion.bot_instance_id,
                        deletion.chat_id,
                        deletion.message_id,
                    );
                }
                Err(error) => {
                    let _ = store.retry_deletion(
                        &deletion.bot_instance_id,
                        deletion.chat_id,
                        deletion.message_id,
                        &error,
                        now_ms(),
                    );
                    eprintln!(
                        "rust bridge scheduled deletion retry bot={} chat={} message={} class={error}",
                        deletion.bot_instance_id, deletion.chat_id, deletion.message_id
                    );
                }
            }
        }
    }
}

async fn delete_scheduled_message(
    bots_by_id: &HashMap<String, RuntimeBot>,
    deletion: &ScheduledDeletion,
) -> Result<(), String> {
    let Some(bot) = bots_by_id.get(&deletion.bot_instance_id) else {
        return Err("bot_unavailable".to_owned());
    };
    let reference =
        TelegramMessageReference::new(deletion.chat_id.to_string(), deletion.message_id)
            .map_err(|_| "invalid_reference".to_owned())?;
    let api = bot.api.clone();
    let token = bot.token.clone();
    let result = tokio::task::spawn_blocking(move || api.delete_message(&token, &reference))
        .await
        .map_err(|_| "delete_join".to_owned())?;
    match result {
        Ok(_) => Ok(()),
        Err(codex_telegram_adapter::TelegramError::ApiRejected {
            error_code: Some(400),
            ..
        }) => Ok(()),
        Err(_error) => Err("telegram_delete_failed".to_owned()),
    }
}

async fn delete_inbound_message(bot: &RuntimeBot, chat_id: i64, message_id: i64) {
    let Ok(reference) = TelegramMessageReference::new(chat_id.to_string(), message_id) else {
        return;
    };
    let api = bot.api.clone();
    let token = bot.token.clone();
    let _ = tokio::task::spawn_blocking(move || api.delete_message(&token, &reference)).await;
}

fn control_user(actor_user_id: Option<i64>) -> Result<i64, String> {
    actor_user_id.ok_or_else(|| "无法确认个人发送身份；请关闭匿名管理员后重试。".to_owned())
}

fn workflow_callback_bot_allowed(bot: &RuntimeBot) -> bool {
    matches!(
        bot.role,
        RuntimeBotRole::Control | RuntimeBotRole::Discussion
    )
}

fn workflow_callback_owner_authorized(
    store: &SqliteStore,
    callback: &codex_telegram_adapter::TelegramCallback,
) -> Result<bool, String> {
    let Some(actor_user_id) = callback.actor.user_id else {
        return Ok(false);
    };
    let owner_user_id = store
        .workflow_record("onboarding", "owner")
        .map_err(|error| error.to_string())?
        .and_then(|value| value.get("user_id").and_then(Value::as_i64));
    Ok(owner_user_id == Some(actor_user_id))
}

fn control_now_seconds() -> i64 {
    now_ms().saturating_div(1000)
}

fn control_models(models: &ModelChoices) -> Vec<ControlModelOption> {
    models
        .entries
        .iter()
        .map(|model| ControlModelOption {
            model: model.model.clone(),
            display_name: model.display_name.clone(),
            supported_efforts: model.efforts.clone(),
        })
        .collect()
}

fn normalized_status(value: Option<&Value>, default: &str) -> String {
    let Some(value) = value else {
        return default.to_owned();
    };
    match value {
        Value::String(value) if !value.trim().is_empty() => value.trim().to_owned(),
        Value::Object(object) => {
            for key in ["type", "status", "state", "name"] {
                let normalized = normalized_status(object.get(key), "");
                if !normalized.is_empty() {
                    return normalized;
                }
            }
            default.to_owned()
        }
        _ => default.to_owned(),
    }
}

fn status_active_flags(value: Option<&Value>) -> Vec<String> {
    let Some(object) = value.and_then(Value::as_object) else {
        return Vec::new();
    };
    object
        .get("activeFlags")
        .or_else(|| object.get("active_flags"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn control_epoch_value(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Number(number)) => number
            .as_i64()
            .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok())),
        Some(Value::String(value)) => value.trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn control_epoch_ms(value: Option<&Value>) -> Option<i64> {
    let raw = control_epoch_value(value)?;
    Some(if raw > 100_000_000_000 {
        raw
    } else {
        raw.saturating_mul(1000)
    })
}

fn error_message(value: Option<&Value>) -> Option<String> {
    let value = value?;
    let message = value
        .get("message")
        .or(Some(value))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|message| !message.is_empty())?;
    Some(message.to_owned())
}

fn control_session_from_value(value: &Value) -> Option<ControlSession> {
    let thread_id = value.get("id").and_then(Value::as_str)?.trim();
    if thread_id.is_empty() || value.get("ephemeral").and_then(Value::as_bool) == Some(true) {
        return None;
    }
    let title = ["naturalSummary", "summary", "title", "name", "preview"]
        .iter()
        .find_map(|field| value.get(*field).and_then(Value::as_str))
        .unwrap_or("Codex session")
        .to_owned();
    let epoch = |field: &str| {
        control_epoch_value(value.get(field)).map(|raw| {
            if raw > 100_000_000_000 {
                raw / 1000
            } else {
                raw
            }
        })
    };
    let status = value.get("status");
    Some(ControlSession {
        thread_id: thread_id.to_owned(),
        title,
        status: normalized_status(status, "unknown"),
        turn_status: normalized_status(
            value.get("turnStatus").or_else(|| value.get("turn_status")),
            "idle",
        ),
        lifecycle: value
            .get("lifecycle")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        active_flags: if status_active_flags(status).is_empty() {
            value
                .get("activeFlags")
                .or_else(|| value.get("active_flags"))
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect()
                })
                .unwrap_or_default()
        } else {
            status_active_flags(status)
        },
        error: value
            .get("error")
            .and_then(|error| error.get("message").or(Some(error)))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        created_at: epoch("createdAt").or_else(|| epoch("created_at")),
        updated_at: epoch("updatedAt").or_else(|| epoch("updated_at")),
        cwd: value
            .get("cwd")
            .or_else(|| value.get("path"))
            .and_then(Value::as_str)
            .unwrap_or("-")
            .to_owned(),
    })
}

fn thread_read_source(response: &Value) -> &Value {
    response
        .get("thread")
        .filter(|thread| thread.is_object())
        .unwrap_or(response)
}

fn thread_read_turns(source: &Value, response: &Value) -> Vec<Value> {
    let turns = source
        .get("turns")
        .or_else(|| response.get("turns"))
        .and_then(|value| {
            value
                .as_array()
                .or_else(|| value.get("data").and_then(Value::as_array))
        });
    turns.cloned().unwrap_or_default()
}

fn projection_from_thread_read(thread_id: &str, response: &Value) -> ThreadProjection {
    let source = thread_read_source(response);
    let turns = thread_read_turns(source, response);
    let latest_turn = turns.iter().rev().find(|turn| turn.is_object());
    let status = source.get("status");
    let mut projection = ThreadProjection {
        thread_id: thread_id.to_owned(),
        title: ["naturalSummary", "summary", "title", "name", "preview"]
            .iter()
            .find_map(|field| {
                source
                    .get(*field)
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            }),
        cwd: source
            .get("cwd")
            .or_else(|| source.get("directory"))
            .or_else(|| source.get("path"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        status: Some(normalized_status(status, "unknown")),
        turn_id: latest_turn
            .and_then(|turn| turn.get("id"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        turn_status: latest_turn
            .and_then(|turn| turn.get("status"))
            .map(|value| normalized_status(Some(value), "idle"))
            .or_else(|| {
                source
                    .get("turnStatus")
                    .or_else(|| source.get("turn_status"))
                    .map(|value| normalized_status(Some(value), "idle"))
            }),
        goal: source
            .get("goal")
            .or_else(|| response.get("goal"))
            .cloned()
            .filter(|value| !value.is_null()),
        plan: source
            .get("plan")
            .or_else(|| response.get("plan"))
            .cloned()
            .filter(|value| !value.is_null()),
        review_status: None,
        desired_mode: source
            .get("collaborationMode")
            .or_else(|| source.get("collaboration_mode"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        observed_mode: source
            .get("observedMode")
            .or_else(|| source.get("observed_mode"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        active_flags: status_active_flags(status),
        model: source
            .get("model")
            .and_then(Value::as_str)
            .map(str::to_owned),
        effort: source
            .get("effort")
            .or_else(|| source.get("reasoningEffort"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        started_at_ms: latest_turn
            .and_then(|turn| turn.get("startedAt").or_else(|| turn.get("started_at")))
            .and_then(|value| control_epoch_ms(Some(value))),
        finished_at_ms: latest_turn
            .and_then(|turn| {
                turn.get("completedAt")
                    .or_else(|| turn.get("completed_at"))
                    .or_else(|| turn.get("finishedAt"))
                    .or_else(|| turn.get("finished_at"))
            })
            .and_then(|value| control_epoch_ms(Some(value))),
        items: Default::default(),
        item_order: Default::default(),
        subagents: Default::default(),
        last_error: error_message(
            source
                .get("error")
                .or_else(|| latest_turn.and_then(|turn| turn.get("error"))),
        ),
        last_error_recoverable: false,
        completed_turns_duration_ms: 0,
        completed_turn_ids: Default::default(),
        generation: source
            .get("generation")
            .or_else(|| response.get("generation"))
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        updated_at_ms: control_epoch_ms(
            source
                .get("updatedAt")
                .or_else(|| source.get("updated_at"))
                .or_else(|| response.get("updatedAt")),
        )
        .unwrap_or_default(),
    };
    if projection.finished_at_ms.is_none()
        && matches!(
            projection.turn_status.as_deref(),
            Some("completed" | "failed" | "interrupted")
        )
        && projection.updated_at_ms > 0
    {
        projection.finished_at_ms = Some(projection.updated_at_ms);
    }
    if projection.observed_mode.is_none()
        && let Some(settings) = source
            .get("threadSettings")
            .or_else(|| source.get("settings"))
    {
        projection.observed_mode = settings
            .get("observedMode")
            .or_else(|| settings.get("observed_mode"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        projection.desired_mode = projection.desired_mode.or_else(|| {
            settings
                .get("collaborationMode")
                .or_else(|| settings.get("collaboration_mode"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
        projection.model = projection.model.or_else(|| {
            settings
                .get("model")
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
        projection.effort = projection.effort.or_else(|| {
            settings
                .get("effort")
                .or_else(|| settings.get("reasoning_effort"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
    }

    let mut plan_from_item = None;
    for (turn_index, turn) in turns.iter().enumerate() {
        let turn_status = turn
            .get("status")
            .map(|value| normalized_status(Some(value), ""));
        let duration_ms = turn
            .get("durationMs")
            .or_else(|| turn.get("duration_ms"))
            .and_then(Value::as_i64);
        if let Some(duration_ms) = duration_ms
            && turn_status.as_deref() != Some("inProgress")
        {
            let turn_id = turn.get("id").and_then(Value::as_str);
            let newly_counted =
                turn_id.is_none_or(|id| projection.completed_turn_ids.insert(id.to_owned()));
            if newly_counted {
                projection.completed_turns_duration_ms = projection
                    .completed_turns_duration_ms
                    .saturating_add(duration_ms.max(0));
            }
        }
        let Some(items) = turn.get("items").and_then(Value::as_array) else {
            continue;
        };
        for (item_index, item) in items.iter().enumerate() {
            let item_id = item
                .get("id")
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| format!("item-{turn_index}-{item_index}"));
            if !projection.items.contains_key(&item_id) {
                projection.item_order.push(item_id.clone());
            }
            projection.items.insert(item_id, item.clone());
            ctg_engine::project_item_subagents(&mut projection, item, true);
            if let Some(task) = item
                .get("task")
                .or_else(|| item.get("subagent"))
                .filter(|value| value.is_object())
                && let Some(agent_id) = task.get("id").and_then(Value::as_str)
            {
                projection
                    .subagents
                    .insert(agent_id.to_owned(), task.clone());
            }
            match item.get("type").and_then(Value::as_str) {
                Some("plan") => {
                    plan_from_item = item
                        .get("plan")
                        .or_else(|| item.get("steps"))
                        .cloned()
                        .or_else(|| Some(item.clone()));
                }
                Some("enteredReviewMode") => projection.review_status = Some("inProgress".into()),
                Some("exitedReviewMode") => projection.review_status = Some("completed".into()),
                Some("error" | "turnError") => {
                    projection.last_error = projection
                        .last_error
                        .clone()
                        .or_else(|| error_message(item.get("error").or(Some(item))));
                }
                _ => {}
            }
        }
    }
    if projection.plan.is_none() {
        projection.plan = plan_from_item;
    }
    // A full thread/read can carry the complete item history; keep the same
    // bounded tail the live projector enforces.
    ctg_engine::truncate_items(&mut projection);
    projection
}

fn synthetic_session_space(thread_id: &str, owner_chat_id: i64) -> RustSessionSpace {
    RustSessionSpace {
        space_id: format!("control-detail:{thread_id}"),
        thread_id: Some(thread_id.to_owned()),
        lifecycle: "active".to_owned(),
        generation: 0,
        channel_chat_id: owner_chat_id,
        channel_post_id: 1,
        discussion_chat_id: None,
        discussion_root_message_id: None,
        status_message_id: None,
        status_bot_instance: None,
        owner_chat_id: Some(owner_chat_id),
        plan_mode: false,
        observed_mode: None,
        normal_model: None,
        normal_effort: None,
        plan_model: None,
        plan_effort: None,
        closed_at_ms: None,
        created_at_ms: 0,
        updated_at_ms: 0,
    }
}

fn session_status_markup(space: &RustSessionSpace) -> Option<Value> {
    let url = space
        .status_message_id
        .filter(|message_id| *message_id > 0)
        .filter(|_| space.channel_post_id > 0)
        .map(|message_id| {
            format!(
                "{}?comment={message_id}",
                telegram_message_link(space.channel_chat_id, space.channel_post_id)
            )
        })
        .or_else(|| {
            space
                .discussion_chat_id
                .zip(space.discussion_root_message_id)
                .filter(|(_, message_id)| *message_id > 0)
                .map(|(chat_id, message_id)| telegram_message_link(chat_id, message_id))
        })?;
    Some(json!({
        "inline_keyboard": [[{"text": "打开实时状态", "url": url}]]
    }))
}

const SESSIONS_CACHE_TTL_MS: i64 = 15_000;
const SESSIONS_CREATED_BACKFILL_BACKOFF_MS: i64 = 60_000;

async fn list_control_sessions(agent: &AppServerClient) -> Result<Vec<ControlSession>, String> {
    let response = agent
        .list_threads(200, None)
        .await
        .map_err(|error| error.to_string())?;
    Ok(response
        .get("data")
        .or_else(|| response.get("threads"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(control_session_from_value)
                .collect()
        })
        .unwrap_or_default())
}

/// Builds the `/sessions` rows from durable thread projections, avoiding a
/// full `thread/list` round trip. Rows are sorted by recency descending to
/// match the app-server ordering the panel previously rendered.
fn control_sessions_from_projections(
    projections: Vec<(String, i64, Value, i64)>,
    created_at_ms: &HashMap<String, i64>,
    lifecycle_by_thread: &HashMap<String, String>,
) -> Vec<ControlSession> {
    let mut sessions = projections
        .into_iter()
        .filter_map(|(thread_id, _, payload, _)| {
            let projection = serde_json::from_value::<ThreadProjection>(payload).ok()?;
            Some(ControlSession {
                thread_id: thread_id.clone(),
                title: projection
                    .title
                    .filter(|title| !title.trim().is_empty())
                    .unwrap_or_else(|| "Codex session".to_owned()),
                status: projection.status.unwrap_or_else(|| "unknown".to_owned()),
                turn_status: projection.turn_status.unwrap_or_else(|| "idle".to_owned()),
                lifecycle: lifecycle_by_thread
                    .get(&thread_id)
                    .cloned()
                    .unwrap_or_default(),
                active_flags: projection.active_flags,
                error: if projection.last_error_recoverable {
                    String::new()
                } else {
                    projection.last_error.unwrap_or_default()
                },
                created_at: created_at_ms.get(&thread_id).copied(),
                updated_at: (projection.updated_at_ms > 0)
                    .then_some(projection.updated_at_ms.div_euclid(1000)),
                cwd: projection.cwd.unwrap_or_else(|| "-".to_owned()),
            })
        })
        .collect::<Vec<_>>();
    sessions.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.thread_id.cmp(&right.thread_id))
    });
    sessions
}

/// `/sessions` data source: a TTL cache over durable projections with
/// event-driven invalidation. `thread/list` remains the cold-start fallback
/// (no projections yet) and the one-time `createdAt` backfill.
async fn list_control_sessions_cached(
    agent: &AppServerClient,
    store: &Arc<SqliteStore>,
    control_runtime: &Arc<ControlRuntime>,
) -> Result<Vec<ControlSession>, String> {
    let now = now_ms();
    {
        let cache = control_runtime
            .sessions_cache
            .lock()
            .map_err(|error| error.to_string())?;
        if cache.built_at_ms > 0
            && now.saturating_sub(cache.built_at_ms) < SESSIONS_CACHE_TTL_MS
            && !control_runtime.sessions_dirty.load(Ordering::Acquire)
        {
            return Ok(cache.sessions.clone());
        }
    }
    // Consume the dirty flag before reading the store so an event landing
    // mid-rebuild marks the cache dirty again instead of being lost.
    control_runtime
        .sessions_dirty
        .store(false, Ordering::Release);
    let projection_rows = store.thread_projections().unwrap_or_default();
    let mut sessions = if !projection_rows.is_empty() {
        let lifecycle_by_thread: HashMap<String, String> = store
            .session_spaces()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|space| {
                space
                    .thread_id
                    .map(|thread_id| (thread_id, space.lifecycle))
            })
            .collect();
        let created_at_ms = {
            let cache = control_runtime
                .sessions_cache
                .lock()
                .map_err(|error| error.to_string())?;
            cache.created_at_ms.clone()
        };
        let mut sessions = control_sessions_from_projections(
            projection_rows,
            &created_at_ms,
            &lifecycle_by_thread,
        );
        let created_missing = sessions.iter().any(|session| session.created_at.is_none());
        let backfill_due = {
            let cache = control_runtime
                .sessions_cache
                .lock()
                .map_err(|error| error.to_string())?;
            now.saturating_sub(cache.created_backfill_attempted_at_ms)
                >= SESSIONS_CREATED_BACKFILL_BACKOFF_MS
        };
        if created_missing && backfill_due {
            if let Ok(listed) = list_control_sessions(agent).await {
                let harvested = listed
                    .iter()
                    .filter_map(|session| {
                        session
                            .created_at
                            .map(|created| (session.thread_id.clone(), created))
                    })
                    .collect::<Vec<_>>();
                let mut cache = control_runtime
                    .sessions_cache
                    .lock()
                    .map_err(|error| error.to_string())?;
                for (thread_id, created_at) in harvested {
                    cache.created_at_ms.insert(thread_id, created_at);
                }
                cache.created_backfill_attempted_at_ms = now;
                for session in sessions
                    .iter_mut()
                    .filter(|session| session.created_at.is_none())
                {
                    session.created_at = cache.created_at_ms.get(&session.thread_id).copied();
                }
            } else {
                let mut cache = control_runtime
                    .sessions_cache
                    .lock()
                    .map_err(|error| error.to_string())?;
                cache.created_backfill_attempted_at_ms = now;
            }
        }
        sessions
    } else {
        // Cold start before any projection exists: fall back to one
        // `thread/list` and harvest creation times for later cache builds.
        let listed = list_control_sessions(agent).await?;
        let mut cache = control_runtime
            .sessions_cache
            .lock()
            .map_err(|error| error.to_string())?;
        for session in &listed {
            if let Some(created_at) = session.created_at {
                cache
                    .created_at_ms
                    .insert(session.thread_id.clone(), created_at);
            }
        }
        listed
    };
    let mut cache = control_runtime
        .sessions_cache
        .lock()
        .map_err(|error| error.to_string())?;
    cache.sessions = std::mem::take(&mut sessions);
    cache.built_at_ms = now_ms();
    Ok(cache.sessions.clone())
}

fn control_topic_from_space(store: &SqliteStore, space: &RustSessionSpace) -> ControlTopic {
    let url = space
        .discussion_chat_id
        .zip(space.discussion_root_message_id)
        .map(|(chat_id, message_id)| telegram_message_link(chat_id, message_id))
        .or_else(|| {
            Some(telegram_message_link(
                space.channel_chat_id,
                space.channel_post_id,
            ))
        });
    ControlTopic {
        title: store
            .workflow_record("pending_space", &space.space_id)
            .ok()
            .flatten()
            .and_then(|payload| {
                payload
                    .get("pending_prompt")
                    .or_else(|| payload.get("title"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .or_else(|| space.thread_id.clone())
            .unwrap_or_else(|| "Pending".to_owned()),
        lifecycle: space.lifecycle.clone(),
        url,
    }
}

fn control_button_markup(
    store: &SqliteStore,
    scope_key: &str,
    revision: i64,
    user_id: i64,
    chat_id: i64,
    keyboard: &[Vec<crate::control::ControlButton>],
    expires_at_ms: i64,
) -> Result<Option<InlineKeyboardMarkup>, String> {
    if keyboard.is_empty() {
        return Ok(None);
    }
    let mut rows = Vec::new();
    for row in keyboard {
        let mut buttons = Vec::new();
        for button in row {
            let value = match &button.target {
                ButtonTarget::Callback { action, payload } => {
                    let nonce = next_approval_nonce();
                    store
                        .upsert_control_callback(&ControlCallback {
                            nonce: nonce.clone(),
                            scope_key: Some(scope_key.to_owned()),
                            revision: Some(revision),
                            user_id,
                            chat_id,
                            action: action.clone(),
                            payload: serde_json::to_value(payload)
                                .map_err(|error| error.to_string())?,
                            expires_at_ms,
                            consumed_at_ms: None,
                            invalidated_at_ms: None,
                            created_at_ms: now_ms(),
                        })
                        .map_err(|error| error.to_string())?;
                    InlineKeyboardButton::callback(&button.label, format!("ctl:{nonce}"))
                        .map_err(|error| error.to_string())?
                }
                ButtonTarget::Url { url } => InlineKeyboardButton::url(&button.label, url)
                    .map_err(|error| error.to_string())?,
            };
            buttons.push(value);
        }
        if !buttons.is_empty() {
            rows.push(buttons);
        }
    }
    (!rows.is_empty())
        .then(|| InlineKeyboardMarkup::new(rows).map_err(|error| error.to_string()))
        .transpose()
}

fn typed_markup_from_json(value: Option<Value>) -> Result<Option<InlineKeyboardMarkup>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(rows) = value.get("inline_keyboard").and_then(Value::as_array) else {
        return Ok(None);
    };
    let mut typed_rows = Vec::new();
    for row in rows {
        let Some(buttons) = row.as_array() else {
            continue;
        };
        let mut typed_buttons = Vec::new();
        for button in buttons {
            let label = button
                .get("text")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if let Some(data) = button.get("callback_data").and_then(Value::as_str) {
                typed_buttons.push(
                    InlineKeyboardButton::callback(label, data)
                        .map_err(|error| error.to_string())?,
                );
            } else if let Some(url) = button.get("url").and_then(Value::as_str) {
                typed_buttons.push(
                    InlineKeyboardButton::url(label, url).map_err(|error| error.to_string())?,
                );
            }
        }
        if !typed_buttons.is_empty() {
            typed_rows.push(typed_buttons);
        }
    }
    (!typed_rows.is_empty())
        .then(|| InlineKeyboardMarkup::new(typed_rows).map_err(|error| error.to_string()))
        .transpose()
}

async fn send_control_rendered(
    bot: &RuntimeBot,
    surface: &TelegramSurfaceBinding,
    rendered: &RenderedEffect,
    markup: Option<InlineKeyboardMarkup>,
    metrics: &MetricsRegistry,
) -> Result<SentMessage, String> {
    let plain = rendered
        .plain
        .clone()
        .unwrap_or_else(|| rendered.markdown.clone());
    let request = TelegramMessageRequest::markdown_v2(rendered.markdown.clone(), plain)
        .with_reply_markup_option(markup);
    let api = bot.api.clone();
    let token = bot.token.clone();
    let surface = surface.clone();
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || api.send_rendered(&token, &surface, &request))
        .await
        .map_err(|error| error.to_string())?;
    match result {
        Ok(message) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                true,
                started.elapsed().as_micros() as u64,
            );
            Ok(message)
        }
        Err(error) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                false,
                started.elapsed().as_micros() as u64,
            );
            Err(error.to_string())
        }
    }
}

async fn edit_control_rendered(
    bot: &RuntimeBot,
    reference: &TelegramMessageReference,
    rendered: &RenderedEffect,
    markup: Option<InlineKeyboardMarkup>,
    metrics: &MetricsRegistry,
    timeout: Option<Duration>,
) -> Result<(), String> {
    let plain = rendered
        .plain
        .clone()
        .unwrap_or_else(|| rendered.markdown.clone());
    let request = TelegramMessageRequest::markdown_v2(rendered.markdown.clone(), plain)
        .with_reply_markup_option(markup);
    let api = bot.api.clone();
    let token = bot.token.clone();
    let reference = reference.clone();
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        api.edit_text_with_timeout(&token, &reference, &request, timeout)
    })
    .await
    .map_err(|error| error.to_string())?;
    match result {
        Ok(_) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                true,
                started.elapsed().as_micros() as u64,
            );
            Ok(())
        }
        Err(error) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                false,
                started.elapsed().as_micros() as u64,
            );
            Err(error.to_string())
        }
    }
}

fn schedule_control_deletions(
    store: &SqliteStore,
    bot: &RuntimeBot,
    chat_id: i64,
    command_id: i64,
    reply_id: i64,
    effect: &ControlEffect,
) -> Result<(), String> {
    let ControlEffect::DeleteDeadline {
        targets,
        deadline_seconds,
        group_key,
    } = effect
    else {
        return Ok(());
    };
    let delete_at_ms = now_ms().saturating_add((*deadline_seconds as i64).saturating_mul(1000));
    for (target, message_id) in [
        (DeleteTarget::Command, command_id),
        (DeleteTarget::Reply, reply_id),
    ] {
        if targets.contains(&target) {
            store
                .schedule_deletion(&ScheduledDeletion {
                    bot_instance_id: bot.config.instance_id.clone(),
                    chat_id,
                    message_id,
                    group_key: group_key.clone(),
                    delete_at_ms,
                    attempts: 0,
                    claimed_at_ms: None,
                    last_error_class: None,
                })
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

async fn handle_control_help(
    bot: &RuntimeBot,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    chat_id: i64,
    actor_user_id: Option<i64>,
    _message_id: i64,
    paired: bool,
) -> Result<(), String> {
    let user_id = control_user(actor_user_id)?;
    let effects = ControlController
        .dispatch(ControlRequest::Help {
            label: bot.config.instance_id.clone(),
            paired,
        })
        .map_err(|error| format!("control help failed: {error:?}"))?;
    for effect in effects {
        if let ControlEffect::Render(rendered) = effect {
            let surface = surface_for(bot, config, chat_id, None);
            let _ = user_id;
            send_control_rendered(bot, &surface, &rendered, None, metrics).await?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_control_sessions(
    agent: &AppServerClient,
    bot: &RuntimeBot,
    config: &RustConfig,
    store: &Arc<SqliteStore>,
    metrics: &MetricsRegistry,
    control_runtime: &Arc<ControlRuntime>,
    chat_id: i64,
    actor_user_id: Option<i64>,
    command_id: i64,
    text: &str,
) -> Result<(), String> {
    let user_id = control_user(actor_user_id)?;
    let query = text
        .split_whitespace()
        .skip(1)
        .collect::<Vec<_>>()
        .join(" ");
    let sessions = list_control_sessions_cached(agent, store, control_runtime).await?;
    let scope_key = format!("sessions:{chat_id}");
    let interaction = store
        .replace_control_interaction(
            &scope_key,
            "sessions",
            "list",
            &json!({"query": query, "page": 1}),
            user_id,
            chat_id,
            Some(command_id),
            now_ms().saturating_add(15 * 60 * 1000),
            now_ms(),
        )
        .map_err(|error| error.to_string())?;
    let effects = ControlController
        .dispatch(ControlRequest::Sessions(SessionsRequest {
            query: query.clone(),
            page: 1,
            now: control_now_seconds(),
            utc_offset_seconds: CONTROL_UTC_OFFSET_SECONDS,
            sessions,
        }))
        .map_err(|error| format!("control sessions failed: {error:?}"))?;
    let surface = surface_for(bot, config, chat_id, None);
    let mut reply_id = None;
    for effect in &effects {
        if let ControlEffect::Render(rendered) = effect {
            let markup = control_button_markup(
                store,
                &scope_key,
                interaction.revision,
                user_id,
                chat_id,
                rendered.keyboard.as_deref().unwrap_or_default(),
                interaction.expires_at_ms,
            )?;
            let message = send_control_rendered(bot, &surface, rendered, markup, metrics).await?;
            reply_id = Some(message.message_id);
            store
                .update_control_interaction_message(
                    &scope_key,
                    interaction.revision,
                    message.message_id,
                    now_ms(),
                )
                .map_err(|error| error.to_string())?;
        }
    }
    if let Some(reply_id) = reply_id {
        for effect in &effects {
            schedule_control_deletions(store, bot, chat_id, command_id, reply_id, effect)?;
        }
        if let Some(refresh_seconds) = effects.iter().find_map(|effect| match effect {
            ControlEffect::SessionRefresh { after_seconds } => Some(*after_seconds),
            _ => None,
        }) {
            tokio::spawn(run_sessions_refresh(
                agent.clone(),
                store.clone(),
                bot.clone(),
                metrics.clone(),
                control_runtime.clone(),
                scope_key,
                query,
                1,
                reply_id,
                refresh_seconds,
            ));
        }
    }
    Ok(())
}

/// Periodically re-renders the `/sessions` panel from the cached projection
/// listing. A single list/render/edit failure is logged and retried on the
/// next tick; the loop ends only when the interaction expires (panel TTL) or
/// a newer revision (re-issued command or page change) replaces it.
#[allow(clippy::too_many_arguments)]
async fn run_sessions_refresh(
    agent: AppServerClient,
    store: Arc<SqliteStore>,
    bot: RuntimeBot,
    metrics: MetricsRegistry,
    control_runtime: Arc<ControlRuntime>,
    scope_key: String,
    query: String,
    page: usize,
    reply_id: i64,
    refresh_seconds: u64,
) {
    let revision = match store.control_interaction(&scope_key).ok().flatten() {
        Some(interaction) => interaction.revision,
        None => return,
    };
    let reference = store
        .control_interaction(&scope_key)
        .ok()
        .flatten()
        .and_then(|current| {
            TelegramMessageReference::new(current.chat_id.to_string(), reply_id).ok()
        });
    let Some(reference) = reference else {
        return;
    };
    let mut last_content: Option<(String, String)> = None;
    loop {
        tokio::time::sleep(Duration::from_secs(refresh_seconds)).await;
        let Some(current) = store.control_interaction(&scope_key).ok().flatten() else {
            return;
        };
        if current.revision != revision || current.expires_at_ms <= now_ms() {
            return;
        }
        let sessions = match list_control_sessions_cached(&agent, &store, &control_runtime).await {
            Ok(sessions) => sessions,
            Err(error) => {
                eprintln!("rust bridge sessions refresh list failed: {error}");
                continue;
            }
        };
        let effects = match ControlController.dispatch(ControlRequest::Sessions(SessionsRequest {
            query: query.clone(),
            page,
            now: control_now_seconds(),
            utc_offset_seconds: CONTROL_UTC_OFFSET_SECONDS,
            sessions,
        })) {
            Ok(effects) => effects,
            Err(error) => {
                eprintln!("rust bridge sessions refresh render failed: {error:?}");
                continue;
            }
        };
        let Some(rendered) = effects.iter().find_map(|effect| match effect {
            ControlEffect::Render(rendered) => Some(rendered),
            _ => None,
        }) else {
            continue;
        };
        let content = (
            rendered.markdown.clone(),
            rendered.plain.clone().unwrap_or_default(),
        );
        if last_content.as_ref() == Some(&content) {
            continue;
        }
        let markup = match control_button_markup(
            &store,
            &scope_key,
            current.revision,
            current.user_id,
            current.chat_id,
            rendered.keyboard.as_deref().unwrap_or_default(),
            current.expires_at_ms,
        ) {
            Ok(markup) => markup,
            Err(error) => {
                eprintln!("rust bridge sessions refresh markup failed: {error}");
                continue;
            }
        };
        if let Err(error) =
            edit_control_rendered(&bot, &reference, rendered, markup, &metrics, None).await
        {
            eprintln!("rust bridge sessions refresh edit failed: {error}");
            continue;
        }
        last_content = Some(content);
    }
}

async fn handle_control_topics(
    bot: &RuntimeBot,
    config: &RustConfig,
    store: &Arc<SqliteStore>,
    metrics: &MetricsRegistry,
    chat_id: i64,
    actor_user_id: Option<i64>,
    command_id: i64,
) -> Result<(), String> {
    let user_id = control_user(actor_user_id)?;
    let topics = store
        .session_spaces()
        .map_err(|error| error.to_string())?
        .iter()
        .filter(|space| space.lifecycle != "closed")
        .take(30)
        .map(|space| control_topic_from_space(store, space))
        .collect::<Vec<_>>();
    let scope_key = format!("topics:{chat_id}");
    let interaction = store
        .replace_control_interaction(
            &scope_key,
            "topics",
            "list",
            &json!({"count": topics.len()}),
            user_id,
            chat_id,
            Some(command_id),
            now_ms().saturating_add(15 * 60 * 1000),
            now_ms(),
        )
        .map_err(|error| error.to_string())?;
    let effects = ControlController
        .dispatch(ControlRequest::Topics { topics })
        .map_err(|error| format!("control topics failed: {error:?}"))?;
    let surface = surface_for(bot, config, chat_id, None);
    for effect in effects {
        if let ControlEffect::Render(rendered) = effect {
            let markup = control_button_markup(
                store,
                &scope_key,
                interaction.revision,
                user_id,
                chat_id,
                rendered.keyboard.as_deref().unwrap_or_default(),
                interaction.expires_at_ms,
            )?;
            let _ = send_control_rendered(bot, &surface, &rendered, markup, metrics).await?;
        }
    }
    Ok(())
}

fn format_perf_snapshot(snapshot: &crate::perf::PerfSnapshot) -> (String, String) {
    let human_bytes = |value: u64| {
        let mut number = value as f64;
        for unit in ["B", "KiB", "MiB", "GiB", "TiB"] {
            if number < 1024.0 || unit == "TiB" {
                return format!("{number:.1} {unit}");
            }
            number /= 1024.0;
        }
        unreachable!("the TiB branch always returns")
    };
    let percent = |used: u64, total: u64| {
        if total == 0 {
            0.0
        } else {
            (used as f64 * 100.0 / total as f64).clamp(0.0, 100.0)
        }
    };
    let bar = |value: f64| {
        let filled = ((value.clamp(0.0, 100.0) * 10.0 / 100.0) + 0.5) as usize;
        "#".repeat(filled.min(10)) + &"-".repeat(10usize.saturating_sub(filled.min(10)))
    };
    let escape = |value: &str| {
        let mut escaped = String::with_capacity(value.len());
        for character in value.chars() {
            if matches!(
                character,
                '\\' | '_'
                    | '*'
                    | '['
                    | ']'
                    | '('
                    | ')'
                    | '~'
                    | '`'
                    | '>'
                    | '#'
                    | '+'
                    | '-'
                    | '='
                    | '|'
                    | '{'
                    | '}'
                    | '.'
                    | '!'
            ) {
                escaped.push('\\');
            }
            escaped.push(character);
        }
        escaped
    };
    let memory_used = snapshot.memory_used_bytes;
    let disk_used = snapshot.disk_used_bytes;
    let memory_percent = percent(memory_used, snapshot.memory_total_bytes);
    let swap_percent = percent(snapshot.swap_used_bytes, snapshot.swap_total_bytes);
    let disk_percent = percent(disk_used, snapshot.disk_total_bytes);
    let uptime_minutes = snapshot.uptime_seconds / 60;
    let uptime_hours = uptime_minutes / 60;
    let days = uptime_hours / 24;
    let hours = uptime_hours % 24;
    let minutes = uptime_minutes % 60;
    let local_seconds = snapshot
        .sampled_at_ms
        .div_euclid(1000)
        .saturating_add(CONTROL_UTC_OFFSET_SECONDS)
        .rem_euclid(24 * 60 * 60);
    let sampled_clock = format!(
        "{:02}:{:02}:{:02}",
        local_seconds / 3600,
        (local_seconds % 3600) / 60,
        local_seconds % 60
    );
    let mut lines = vec![
        "*🟠 Ubuntu · WSL*".to_owned(),
        format!(
            "CPU  `{:.1}%` `{}`",
            snapshot.cpu_percent,
            bar(snapshot.cpu_percent as f64)
        ),
        format!("RAM  `{memory_percent:.1}%` `{}`", bar(memory_percent)),
        format!("Swap `{swap_percent:.1}%` `{}`", bar(swap_percent)),
        format!("Disk `{disk_percent:.1}%` `{}`", bar(disk_percent)),
        format!(
            "内存 `{}`",
            format!(
                "{} / {}",
                human_bytes(memory_used),
                human_bytes(snapshot.memory_total_bytes)
            )
        ),
        format!(
            "交换 `{}`",
            format!(
                "{} / {}",
                human_bytes(snapshot.swap_used_bytes),
                human_bytes(snapshot.swap_total_bytes)
            )
        ),
        format!(
            "磁盘 `{}`",
            format!(
                "{} / {}",
                human_bytes(disk_used),
                human_bytes(snapshot.disk_total_bytes)
            )
        ),
        format!(
            "负载 `{:.2} / {:.2} / {:.2}`",
            snapshot.load[0], snapshot.load[1], snapshot.load[2]
        ),
        format!("运行 `{days}d {hours}h {minutes}m`"),
        String::new(),
        "*⚙️ Codex*".to_owned(),
        format!(
            "进程 `{}` · RSS `{}`",
            snapshot.codex_process_count,
            human_bytes(snapshot.codex_memory_bytes)
        ),
        format!(
            "CPU  `{:.1}%` `{}`",
            snapshot.codex_cpu_percent,
            bar(snapshot.codex_cpu_percent as f64)
        ),
    ];
    if let Some(gpu) = snapshot.gpu.as_ref() {
        lines.push(String::new());
        lines.push(format!("*🟩 NVIDIA · {}*", escape(&gpu.name)));
        let utilization = gpu
            .utilization_percent
            .map_or_else(|| "N/A".to_owned(), |value| format!("{value:.1}%"));
        let utilization_bar = gpu
            .utilization_percent
            .map_or_else(|| "----------".to_owned(), |value| bar(value as f64));
        lines.push(format!("GPU  `{utilization}` `{utilization_bar}`"));
        match (gpu.memory_used_mib, gpu.memory_total_mib) {
            (Some(used), Some(total)) if total > 0.0 => {
                let ratio = (used * 100.0 / total).clamp(0.0, 100.0);
                let memory = if total >= 1024.0 {
                    format!("{:.1} / {:.1} GiB", used / 1024.0, total / 1024.0)
                } else {
                    format!("{used:.0} / {total:.0} MiB")
                };
                lines.push(format!("VRAM `{memory}` `{}`", bar(ratio as f64)));
            }
            _ => lines.push("VRAM `N/A`".to_owned()),
        }
        let temperature = gpu
            .temperature_c
            .map_or_else(|| "N/A".to_owned(), |value| format!("{value:.1}°C"));
        let power = gpu
            .power_w
            .map_or_else(|| "N/A".to_owned(), |value| format!("{value:.1} W"));
        lines.push(format!("温度 `{temperature}` · 功耗 `{power}`"));
    } else {
        lines.extend([
            String::new(),
            "*🟩 NVIDIA*".to_owned(),
            "GPU  `N/A`".to_owned(),
        ]);
    }
    lines.push(String::new());
    lines.push(format!("采样 `{sampled_clock}`"));
    let markdown = lines.join("\n");
    let plain = markdown.replace(['*', '`', '\\'], "");
    (markdown, plain)
}

#[allow(clippy::too_many_arguments)]
async fn handle_control_perf(
    control_runtime: &Arc<ControlRuntime>,
    bot: &RuntimeBot,
    config: &RustConfig,
    store: &Arc<SqliteStore>,
    metrics: &MetricsRegistry,
    chat_id: i64,
    actor_user_id: Option<i64>,
    command_id: i64,
) -> Result<(), String> {
    let user_id = control_user(actor_user_id)?;
    let sampler = control_runtime.perf.clone();
    let snapshot = tokio::task::spawn_blocking(move || sampler.sample(true))
        .await
        .map_err(|error| error.to_string())?;
    let (markdown_body, plain_body) = format_perf_snapshot(&snapshot);
    let initial_content = (markdown_body.clone(), plain_body.clone());
    let effects = ControlController
        .dispatch(ControlRequest::Perf {
            frame: 0,
            markdown_body,
            plain_body,
        })
        .map_err(|error| format!("control perf failed: {error:?}"))?;
    let scope_key = format!("perf:{chat_id}");
    let interaction = store
        .replace_control_interaction(
            &scope_key,
            "perf",
            "running",
            &json!({"command_id": command_id}),
            user_id,
            chat_id,
            Some(command_id),
            now_ms().saturating_add(30 * 1000),
            now_ms(),
        )
        .map_err(|error| error.to_string())?;
    let surface = surface_for(bot, config, chat_id, None);
    let mut reply_id = None;
    for effect in &effects {
        if let ControlEffect::Render(rendered) = effect {
            let message = send_control_rendered(bot, &surface, rendered, None, metrics).await?;
            reply_id = Some(message.message_id);
            store
                .update_control_interaction_message(
                    &scope_key,
                    interaction.revision,
                    message.message_id,
                    now_ms(),
                )
                .map_err(|error| error.to_string())?;
        }
    }
    let Some(reply_id) = reply_id else {
        return Ok(());
    };
    for effect in &effects {
        schedule_control_deletions(store, bot, chat_id, command_id, reply_id, effect)?;
    }
    tokio::spawn(run_perf_ticker(
        control_runtime.perf.clone(),
        store.clone(),
        bot.clone(),
        metrics.clone(),
        scope_key,
        interaction.revision,
        chat_id,
        reply_id,
        initial_content,
    ));
    Ok(())
}

/// `/perf` dynamic panel, aligned with Python `control_bot._run_perf`:
/// absolute tick schedule anchored at the first-frame send with catch-up
/// (`tick = elapsed // interval`), edits skipped when the rendered content is
/// unchanged, and single sample/edit failures logged and retried — the loop
/// ends only at the lifetime deadline or on a revision change.
#[allow(clippy::too_many_arguments)]
async fn run_perf_ticker(
    sampler: Arc<crate::perf::PerfSampler>,
    store: Arc<SqliteStore>,
    bot: RuntimeBot,
    metrics: MetricsRegistry,
    scope_key: String,
    revision: i64,
    chat_id: i64,
    reply_id: i64,
    initial_content: (String, String),
) {
    // Anchored after the first frame landed, matching the Python lifetime
    // anchor; the fixture-locked constants keep 30s lifetime / 5s updates.
    let started = Instant::now();
    let update_seconds = crate::control::PERF_UPDATE_SECONDS;
    let interval = Duration::from_secs(update_seconds);
    let expires = started + Duration::from_secs(crate::control::PERF_LIFETIME_SECONDS);
    let mut tick: u64 = 1;
    let mut last_content = Some(initial_content);
    loop {
        let target = started + interval * tick as u32;
        if target >= expires {
            return;
        }
        tokio::time::sleep(target.saturating_duration_since(Instant::now())).await;
        let elapsed_seconds = Instant::now().saturating_duration_since(started).as_secs();
        tick = tick.max(elapsed_seconds / update_seconds.max(1));
        if Instant::now() >= expires {
            return;
        }
        let Some(interaction) = store.control_interaction(&scope_key).ok().flatten() else {
            return;
        };
        if interaction.revision != revision {
            return;
        }
        let snapshot = match tokio::task::spawn_blocking({
            let sampler = sampler.clone();
            move || sampler.sample(true)
        })
        .await
        {
            Ok(snapshot) => snapshot,
            Err(error) => {
                eprintln!("rust bridge perf sample failed: {error}");
                tick += 1;
                continue;
            }
        };
        if Instant::now() >= expires {
            return;
        }
        let content = format_perf_snapshot(&snapshot);
        if last_content.as_ref() == Some(&content) {
            tick += 1;
            continue;
        }
        let Ok(effects) = ControlController.dispatch(ControlRequest::Perf {
            // Python keeps the dynamic-performance clock anchored at the
            // initial frame while only the sampled values change.
            frame: 0,
            markdown_body: content.0.clone(),
            plain_body: content.1.clone(),
        }) else {
            tick += 1;
            continue;
        };
        let Some(rendered) = effects.iter().find_map(|effect| match effect {
            ControlEffect::Render(rendered) => Some(rendered),
            _ => None,
        }) else {
            tick += 1;
            continue;
        };
        let Ok(reference) = TelegramMessageReference::new(chat_id.to_string(), reply_id) else {
            return;
        };
        if let Err(error) = edit_control_rendered(
            &bot,
            &reference,
            rendered,
            None,
            &metrics,
            Some(PERF_EDIT_TIMEOUT),
        )
        .await
        {
            eprintln!("rust bridge perf edit failed: {error}");
            tick += 1;
            continue;
        }
        last_content = Some(content);
        tick += 1;
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_control_callback(
    callback: codex_telegram_adapter::TelegramCallback,
    bot: RuntimeBot,
    _bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    store: &Arc<SqliteStore>,
    agent: &AppServerClient,
    _sessions: &Arc<SessionRegistry>,
    metrics: &MetricsRegistry,
    totp: &Arc<TotpManager>,
    control_runtime: &Arc<ControlRuntime>,
) -> Result<(), String> {
    let Some(user_id) = callback.actor.user_id else {
        acknowledge_callback(&bot, &callback, Some("无法确认发送者")).await;
        return Ok(());
    };
    let nonce = callback.data.strip_prefix("ctl:").unwrap_or_default();
    let Some(stored) = store
        .consume_control_callback(nonce, user_id, callback.chat_id, now_ms())
        .map_err(|error| error.to_string())?
    else {
        acknowledge_callback(&bot, &callback, Some("按钮已使用或过期，请重新执行命令。")).await;
        return Ok(());
    };
    let action = stored.action.as_str();
    if !matches!(
        action,
        "session_detail" | "sessions_current" | "sessions_page"
    ) {
        acknowledge_callback(&bot, &callback, Some("按钮已处理")).await;
        return Ok(());
    }
    acknowledge_callback(&bot, &callback, None).await;
    if action == "sessions_current" {
        return Ok(());
    }
    let Some(scope_key) = stored.scope_key.as_deref() else {
        return Ok(());
    };
    let query = stored
        .payload
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    if action == "session_detail" {
        let thread_id = stored
            .payload
            .get("thread_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let thread = ThreadId::new(thread_id.to_owned()).map_err(|error| error.to_string())?;
        let response = match agent.read_thread(&thread, true).await {
            Ok(response) => response,
            Err(error) => {
                return send_text(
                    &bot,
                    &surface_for(&bot, config, callback.chat_id, None),
                    &format!("无法读取 Session {thread_id} 的实时状态：{error}"),
                    metrics,
                )
                .await
                .map(|_| ());
            }
        };
        let projection = projection_from_thread_read(thread_id, &response);
        let space = store
            .session_space_for_thread(thread_id)
            .map_err(|error| error.to_string())?
            .unwrap_or_else(|| synthetic_session_space(thread_id, callback.chat_id));
        let detail = status_text(store, &space, Some(&projection), None, totp.as_ref());
        return send_text_with_markup(
            &bot,
            &surface_for(&bot, config, callback.chat_id, None),
            &detail,
            session_status_markup(&space),
            metrics,
        )
        .await
        .map(|_| ());
    }
    let page = stored
        .payload
        .get("page")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1);
    let refresh_query = query.clone();
    let sessions = list_control_sessions_cached(agent, store, control_runtime).await?;
    let interaction = store
        .control_interaction(scope_key)
        .map_err(|error| error.to_string())?;
    let Some(interaction) = interaction else {
        return Ok(());
    };
    let next = store
        .replace_control_interaction(
            scope_key,
            "sessions",
            "list",
            &json!({"query": query, "page": page}),
            user_id,
            callback.chat_id,
            Some(callback.message_id),
            now_ms().saturating_add(15 * 60 * 1000),
            now_ms(),
        )
        .map_err(|error| error.to_string())?;
    let effects = ControlController
        .dispatch(ControlRequest::Sessions(SessionsRequest {
            query,
            page,
            now: control_now_seconds(),
            utc_offset_seconds: CONTROL_UTC_OFFSET_SECONDS,
            sessions,
        }))
        .map_err(|error| format!("control sessions callback failed: {error:?}"))?;
    let reference =
        TelegramMessageReference::new(callback.chat_id.to_string(), callback.message_id)
            .map_err(|error| error.to_string())?;
    for effect in &effects {
        if let ControlEffect::Render(rendered) = effect {
            let markup = control_button_markup(
                store,
                scope_key,
                next.revision,
                user_id,
                callback.chat_id,
                rendered.keyboard.as_deref().unwrap_or_default(),
                next.expires_at_ms,
            )?;
            edit_control_rendered(&bot, &reference, rendered, markup, metrics, None).await?;
        }
    }
    if let Some(refresh_seconds) = effects.iter().find_map(|effect| match effect {
        ControlEffect::SessionRefresh { after_seconds } => Some(*after_seconds),
        _ => None,
    }) {
        // The interaction revision bump retires the previous refresh loop at
        // its next wake-up, so each panel keeps exactly one active loop.
        tokio::spawn(run_sessions_refresh(
            agent.clone(),
            store.clone(),
            bot.clone(),
            metrics.clone(),
            control_runtime.clone(),
            scope_key.to_owned(),
            refresh_query,
            page,
            callback.message_id,
            refresh_seconds,
        ));
    }
    let _ = interaction;
    Ok(())
}

fn new_draft_key(chat_id: i64) -> String {
    format!("{chat_id}")
}

fn new_command_arguments(text: &str) -> &str {
    text.split_once(char::is_whitespace)
        .map(|(_, value)| value.trim())
        .unwrap_or_default()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NewArguments {
    model: String,
    effort: String,
    mode: Option<String>,
    plan_model: Option<String>,
    plan_effort: Option<String>,
    cwd: Option<String>,
    prompt: Option<String>,
}

fn parse_new_arguments(text: &str) -> Result<NewArguments, String> {
    let value = new_command_arguments(text);
    let fields = value.split('|').map(str::trim).collect::<Vec<_>>();
    if fields.len() < 2 || fields[0].is_empty() || fields[1].is_empty() {
        return Err(
            "参数不完整。可用格式：/new <model> | <effort> | noplan [ | <cwd> [ | <prompt> ] ] 或 /new <model> | <effort> | planmode | <plan_model> | <plan_effort> [ | <cwd> [ | <prompt> ] ]".into(),
        );
    }
    let model = fields[0].to_owned();
    let effort = fields[1].to_owned();
    let Some(mode) = fields.get(2).filter(|value| !value.is_empty()) else {
        return Ok(NewArguments {
            model,
            effort,
            mode: None,
            plan_model: None,
            plan_effort: None,
            cwd: None,
            prompt: None,
        });
    };
    let mode = mode.to_ascii_lowercase();
    match mode.as_str() {
        "noplan" => Ok(NewArguments {
            model,
            effort,
            mode: Some(mode),
            plan_model: None,
            plan_effort: None,
            cwd: fields
                .get(3)
                .filter(|value| !value.is_empty())
                .map(|value| (*value).to_owned()),
            prompt: (fields.len() > 4)
                .then(|| fields[4..].join(" | "))
                .filter(|value| !value.is_empty()),
        }),
        "planmode" => {
            if fields.len() < 5 || fields[3].is_empty() || fields[4].is_empty() {
                return Err("Plan Mode 需要同时提供 plan model 和 plan effort。".into());
            }
            Ok(NewArguments {
                model,
                effort,
                mode: Some(mode),
                plan_model: Some(fields[3].to_owned()),
                plan_effort: Some(fields[4].to_owned()),
                cwd: fields
                    .get(5)
                    .filter(|value| !value.is_empty())
                    .map(|value| (*value).to_owned()),
                prompt: (fields.len() > 6)
                    .then(|| fields[6..].join(" | "))
                    .filter(|value| !value.is_empty()),
            })
        }
        _ => Err("模式只能是 planmode 或 noplan。".into()),
    }
}

fn new_argument_suggestion(models: &ModelChoices, arguments: &str) -> String {
    let fields = arguments.split('|').map(str::trim).collect::<Vec<_>>();
    let normal = fields
        .first()
        .and_then(|value| model_choice(models, value))
        .or_else(|| models.entries.first());
    let Some(normal) = normal else {
        return "/new <model> | <effort>".to_owned();
    };
    let effort = fields
        .get(1)
        .filter(|value| normal.efforts.iter().any(|candidate| candidate == *value))
        .copied()
        .unwrap_or(normal.default_effort.as_str());
    let mut suggestion = vec![format!("/new {}", normal.model), effort.to_owned()];
    let Some(raw_mode) = fields.get(2).filter(|value| !value.is_empty()) else {
        return suggestion.join(" | ");
    };
    let plan = raw_mode.to_ascii_lowercase().contains("plan");
    suggestion.push(if plan { "planmode" } else { "noplan" }.to_owned());
    if plan {
        let plan_model = fields
            .get(3)
            .and_then(|value| model_choice(models, value))
            .or_else(|| model_choice(models, fields.first().copied().unwrap_or_default()))
            .or_else(|| models.entries.first());
        if let Some(plan_model) = plan_model {
            let plan_effort = fields
                .get(4)
                .filter(|value| {
                    plan_model
                        .efforts
                        .iter()
                        .any(|candidate| candidate == *value)
                })
                .copied()
                .unwrap_or(plan_model.default_effort.as_str());
            suggestion.push(plan_model.model.clone());
            suggestion.push(plan_effort.to_owned());
        }
        if let Some(cwd) = fields.get(5).filter(|value| !value.is_empty()) {
            suggestion.push((*cwd).to_owned());
            if fields.len() > 6 {
                suggestion.push(fields[6..].join(" | "));
            }
        }
    } else {
        if let Some(cwd) = fields.get(3).filter(|value| !value.is_empty()) {
            suggestion.push((*cwd).to_owned());
            if fields.len() > 4 {
                suggestion.push(fields[4..].join(" | "));
            }
        }
    }
    suggestion.join(" | ")
}

fn model_choice<'a>(models: &'a ModelChoices, requested: &str) -> Option<&'a ModelChoice> {
    let normalized = requested.trim().to_ascii_lowercase();
    models.entries.iter().find(|entry| {
        entry.model.eq_ignore_ascii_case(requested)
            || entry.display_name.eq_ignore_ascii_case(requested)
            || entry
                .model
                .rsplit('-')
                .next()
                .is_some_and(|alias| alias.eq_ignore_ascii_case(&normalized))
    })
}

fn new_draft(chat_id: i64, user_id: i64, phase: &str, payload: Value) -> Value {
    json!({
        "chat_id": chat_id,
        "user_id": user_id,
        "phase": phase,
        "revision": 0,
        "payload": payload,
        "choices": {},
        "expires_at_ms": now_ms().saturating_add(NEW_INTERACTION_TTL_MS),
    })
}

fn new_draft_expired(draft: &Value) -> bool {
    draft
        .get("expires_at_ms")
        .and_then(Value::as_i64)
        .is_some_and(|expires| expires < now_ms())
}

fn persist_new_draft(store: &SqliteStore, key: &str, draft: &Value) -> Result<(), String> {
    let chat_id = draft
        .get("chat_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| "new draft chat_id missing".to_owned())?;
    let user_id = draft
        .get("user_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| "new draft user_id missing".to_owned())?;
    let phase = draft
        .get("phase")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let expires_at_ms = draft
        .get("expires_at_ms")
        .and_then(Value::as_i64)
        .unwrap_or_else(|| now_ms().saturating_add(NEW_INTERACTION_TTL_MS));
    let interaction = store
        .replace_control_interaction(
            key,
            "new",
            phase,
            draft.get("payload").unwrap_or(&Value::Null),
            user_id,
            chat_id,
            None,
            expires_at_ms,
            now_ms(),
        )
        .map_err(|error| error.to_string())?;
    let mut persisted = draft.clone();
    persisted["revision"] = Value::from(interaction.revision);
    store
        .upsert_workflow_record("new", key, &persisted, now_ms())
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn new_interaction_revision(
    store: &SqliteStore,
    key: &str,
    draft: &Value,
) -> Result<(ctg_storage_sqlite::ControlInteraction, i64), String> {
    let interaction = store
        .control_interaction(key)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "new interaction is no longer active".to_owned())?;
    let draft_revision = draft
        .get("revision")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    if draft_revision != interaction.revision {
        let legacy_snapshot_matches = draft_revision.saturating_add(1) == interaction.revision
            && draft.get("phase").and_then(Value::as_str) == Some(interaction.phase.as_str())
            && draft.get("payload") == Some(&interaction.payload);
        if !legacy_snapshot_matches {
            return Err("new interaction changed; selection was already handled".to_owned());
        }
    }
    let revision = interaction.revision;
    Ok((interaction, revision))
}

fn advance_new_draft(
    store: &SqliteStore,
    key: &str,
    draft: &Value,
    phase: &str,
    payload: Value,
    prompt_phase: bool,
) -> Result<Value, String> {
    let mut updated = draft.clone();
    updated["phase"] = Value::String(phase.to_owned());
    updated["revision"] = Value::from(
        draft
            .get("revision")
            .and_then(Value::as_u64)
            .unwrap_or_default()
            .saturating_add(1),
    );
    updated["payload"] = payload;
    updated["choices"] = json!({});
    updated["expires_at_ms"] = Value::from(now_ms().saturating_add(if prompt_phase {
        NEW_PROMPT_TTL_MS
    } else {
        NEW_INTERACTION_TTL_MS
    }));
    let (current, expected_revision) = new_interaction_revision(store, key, draft)?;
    let next = store
        .advance_control_interaction(
            key,
            expected_revision,
            phase,
            updated.get("payload").unwrap_or(&Value::Null),
            current.user_id,
            current.chat_id,
            current.message_id,
            updated["expires_at_ms"].as_i64().unwrap_or(now_ms()),
            now_ms(),
        )
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "new interaction changed; selection was already handled".to_owned())?;
    updated["revision"] = Value::from(next.revision);
    store
        .upsert_workflow_record("new", key, &updated, now_ms())
        .map_err(|error| error.to_string())?;
    Ok(updated)
}

fn new_choice_markup(
    store: &SqliteStore,
    key: &str,
    draft: &mut Value,
    choices: &[(String, String, String)],
) -> Result<Option<Value>, String> {
    let chat_id = draft
        .get("chat_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| "new draft chat_id missing".to_owned())?;
    let user_id = draft
        .get("user_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| "new draft user_id missing".to_owned())?;
    let revision = store
        .control_interaction(key)
        .map_err(|error| error.to_string())?
        .map(|interaction| interaction.revision)
        .or_else(|| draft.get("revision").and_then(Value::as_i64))
        .unwrap_or_default();
    let expires_at_ms = draft
        .get("expires_at_ms")
        .and_then(Value::as_i64)
        .unwrap_or_else(|| now_ms().saturating_add(NEW_INTERACTION_TTL_MS));
    let mut stored = serde_json::Map::new();
    let mut buttons = Vec::new();
    for (event, value, label) in choices {
        let nonce = next_approval_nonce();
        stored.insert(nonce.clone(), json!({"event": event, "value": value}));
        store
            .upsert_control_callback(&ControlCallback {
                nonce: nonce.clone(),
                scope_key: Some(key.to_owned()),
                revision: Some(revision),
                user_id,
                chat_id,
                action: event.clone(),
                payload: json!({"value": value}),
                expires_at_ms,
                consumed_at_ms: None,
                invalidated_at_ms: None,
                created_at_ms: now_ms(),
            })
            .map_err(|error| error.to_string())?;
        buttons.push(json!({
            "text": truncate_text(label),
            "callback_data": format!("new:{nonce}"),
        }));
    }
    let cancel_nonce = next_approval_nonce();
    store
        .upsert_control_callback(&ControlCallback {
            nonce: cancel_nonce.clone(),
            scope_key: Some(key.to_owned()),
            revision: Some(revision),
            user_id,
            chat_id,
            action: "cancel".to_owned(),
            payload: json!({"value": ""}),
            expires_at_ms,
            consumed_at_ms: None,
            invalidated_at_ms: None,
            created_at_ms: now_ms(),
        })
        .map_err(|error| error.to_string())?;
    stored.insert(
        cancel_nonce.clone(),
        json!({"event": "cancel", "value": ""}),
    );
    draft["choices"] = Value::Object(stored);
    store
        .upsert_workflow_record("new", key, draft, now_ms())
        .map_err(|error| error.to_string())?;
    let event = choices
        .first()
        .map(|choice| choice.0.as_str())
        .unwrap_or_default();
    let columns = if event.contains("model") {
        2
    } else if event.contains("effort") {
        3
    } else {
        1
    };
    let mut rows = buttons
        .chunks(columns)
        .map(|row| Value::Array(row.to_vec()))
        .collect::<Vec<_>>();
    if columns > 1
        && rows.len() > 1
        && rows
            .last()
            .is_some_and(|row| row.as_array().is_some_and(|buttons| buttons.len() == 1))
    {
        let previous = rows.len().saturating_sub(2);
        let moved = rows
            .get_mut(previous)
            .and_then(Value::as_array_mut)
            .and_then(Vec::pop);
        if let Some(button) = moved {
            rows.last_mut()
                .and_then(Value::as_array_mut)
                .expect("rows is non-empty")
                .insert(0, button);
        }
    }
    rows.push(json!([{
        "text": "退出",
        "callback_data": format!("new:{cancel_nonce}"),
    }]));
    Ok(Some(json!({"inline_keyboard": rows})))
}

fn new_control_phase(value: &str) -> Option<crate::control::NewPhase> {
    match value {
        "normal_model" => Some(crate::control::NewPhase::NormalModel),
        "normal_effort" => Some(crate::control::NewPhase::NormalEffort),
        "plan_choice" => Some(crate::control::NewPhase::PlanChoice),
        "plan_model" => Some(crate::control::NewPhase::PlanModel),
        "plan_effort" => Some(crate::control::NewPhase::PlanEffort),
        "project" => Some(crate::control::NewPhase::Project),
        "prompt" => Some(crate::control::NewPhase::Prompt),
        _ => None,
    }
}

fn new_control_event(value: &str) -> Option<crate::control::NewEvent> {
    match value {
        "cancel" => Some(crate::control::NewEvent::Cancel),
        "normal_model" => Some(crate::control::NewEvent::NormalModel),
        "normal_effort" => Some(crate::control::NewEvent::NormalEffort),
        "plan_choice" => Some(crate::control::NewEvent::PlanChoice),
        "plan_model" => Some(crate::control::NewEvent::PlanModel),
        "plan_effort" => Some(crate::control::NewEvent::PlanEffort),
        "hello" => Some(crate::control::NewEvent::Hello),
        _ => None,
    }
}

fn dispatch_new_control_effects(
    draft: &Value,
    event: &str,
    value: &str,
    models: &ModelChoices,
) -> Result<Vec<ControlEffect>, String> {
    let Some(phase_name) = draft.get("phase").and_then(Value::as_str) else {
        return Ok(Vec::new());
    };
    let Some(phase) = new_control_phase(phase_name) else {
        return Ok(Vec::new());
    };
    let Some(event) = new_control_event(event) else {
        return Ok(Vec::new());
    };
    let payload = draft.get("payload").cloned().unwrap_or_else(|| json!({}));
    ControlController
        .dispatch(ControlRequest::NewCallback {
            draft: crate::control::NewDraft {
                phase,
                normal_model: payload
                    .get("normal_model")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                plan_model: payload
                    .get("plan_model")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            },
            event,
            value: value.to_owned(),
            models: control_models(models),
        })
        .map_err(|error| format!("control /new transition failed: {error:?}"))
}

fn new_render_effect(
    effects: &[ControlEffect],
    operation: RenderOperation,
) -> Option<RenderedEffect> {
    effects.iter().find_map(|effect| match effect {
        ControlEffect::Render(rendered) if rendered.operation == operation => {
            Some(rendered.clone())
        }
        _ => None,
    })
}

async fn send_new_render_effect(
    bot: &RuntimeBot,
    surface: &TelegramSurfaceBinding,
    rendered: Option<&RenderedEffect>,
    fallback_markdown: &str,
    fallback_plain: &str,
    markup: Option<Value>,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    let rendered = rendered.cloned().unwrap_or_else(|| RenderedEffect {
        operation: RenderOperation::Send,
        markdown: fallback_markdown.to_owned(),
        plain: Some(fallback_plain.to_owned()),
        keyboard: None,
    });
    send_control_rendered(
        bot,
        surface,
        &rendered,
        typed_markup_from_json(markup)?,
        metrics,
    )
    .await
    .map(|_| ())
}

#[allow(clippy::too_many_arguments)]
async fn begin_new_interaction(
    store: &Arc<SqliteStore>,
    agent: &AppServerClient,
    sessions: &Arc<SessionRegistry>,
    bot: &RuntimeBot,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    chat_id: i64,
    actor_user_id: Option<i64>,
    message_id: i64,
    text: &str,
) -> Result<(), String> {
    let Some(user_id) = actor_user_id else {
        return send_text(
            bot,
            &surface_for(bot, config, chat_id, None),
            "无法确认个人发送身份；请关闭匿名管理员后重试。",
            metrics,
        )
        .await;
    };
    let key = new_draft_key(chat_id);
    if let Some(existing) = store
        .workflow_record("new", &key)
        .map_err(|error| error.to_string())?
        && !new_draft_expired(&existing)
        && existing.get("user_id").and_then(Value::as_i64) != Some(user_id)
    {
        return send_text(
            bot,
            &surface_for(bot, config, chat_id, None),
            "当前已有另一条 /new 交互，请先完成或等待它过期。",
            metrics,
        )
        .await;
    }
    if let Some(existing) = store
        .workflow_record("new", &key)
        .map_err(|error| error.to_string())?
        && new_draft_expired(&existing)
    {
        let _ = store.delete_workflow_record("new", &key);
        let _ = store.delete_control_interaction(&key, now_ms());
    }
    let args = new_command_arguments(text);
    if args.is_empty() {
        let models = list_model_choices(agent).await?;
        let contract_models = control_models(&models);
        let effects = ControlController
            .dispatch(ControlRequest::New {
                draft: crate::control::NewDraft {
                    phase: crate::control::NewPhase::NormalModel,
                    normal_model: None,
                    plan_model: None,
                },
                models: contract_models,
            })
            .map_err(|error| format!("control /new contract failed: {error:?}"))?;
        let rendered = new_render_effect(&effects, RenderOperation::Send);
        let mut draft = new_draft(
            chat_id,
            user_id,
            "normal_model",
            json!({"channel_post_id": message_id.max(1)}),
        );
        persist_new_draft(store, &key, &draft)?;
        let choices = models
            .entries
            .iter()
            .map(|entry| {
                (
                    "normal_model".to_owned(),
                    entry.model.clone(),
                    entry.display_name.clone(),
                )
            })
            .collect::<Vec<_>>();
        let markup = new_choice_markup(store, &key, &mut draft, &choices)?;
        return send_new_render_effect(
            bot,
            &surface_for(bot, config, chat_id, None),
            rendered.as_ref(),
            "请选择 当前模式 使用的模型：",
            "请选择 当前模式 使用的模型：",
            markup,
            metrics,
        )
        .await;
    }
    let models = list_model_choices(agent).await?;
    let parsed = match parse_new_arguments(text) {
        Ok(parsed) => parsed,
        Err(error) => {
            let suggestion = new_argument_suggestion(&models, new_command_arguments(text));
            return send_text(
                bot,
                &surface_for(bot, config, chat_id, None),
                &format!("{error}\n你可能想发送：`{suggestion}`"),
                metrics,
            )
            .await;
        }
    };
    let Some(normal) = model_choice(&models, &parsed.model) else {
        let suggestion = new_argument_suggestion(&models, new_command_arguments(text));
        return send_text(
            bot,
            &surface_for(bot, config, chat_id, None),
            &format!("模型、effort 或模式无效。你可能想发送：\n`{suggestion}`"),
            metrics,
        )
        .await;
    };
    if !normal.efforts.iter().any(|value| value == &parsed.effort) {
        let suggestion = new_argument_suggestion(&models, new_command_arguments(text));
        return send_text(
            bot,
            &surface_for(bot, config, chat_id, None),
            &format!("模型、effort 或模式无效。你可能想发送：\n`{suggestion}`"),
            metrics,
        )
        .await;
    }
    let mut payload = json!({
        "normal_model": normal.model,
        "normal_effort": parsed.effort,
        "channel_post_id": message_id.max(1),
    });
    if parsed.mode.as_deref() == Some("planmode") {
        let plan_model = parsed.plan_model.as_deref().unwrap_or_default();
        let plan_effort = parsed.plan_effort.as_deref().unwrap_or_default();
        let Some(plan) = model_choice(&models, plan_model) else {
            let suggestion = new_argument_suggestion(&models, new_command_arguments(text));
            return send_text(
                bot,
                &surface_for(bot, config, chat_id, None),
                &format!("模型、effort 或模式无效。你可能想发送：\n`{suggestion}`"),
                metrics,
            )
            .await;
        };
        if !plan.efforts.iter().any(|value| value == plan_effort) {
            let suggestion = new_argument_suggestion(&models, new_command_arguments(text));
            return send_text(
                bot,
                &surface_for(bot, config, chat_id, None),
                &format!("模型、effort 或模式无效。你可能想发送：\n`{suggestion}`"),
                metrics,
            )
            .await;
        }
        payload["plan_model"] = Value::String(plan.model.clone());
        payload["plan_effort"] = Value::String(plan_effort.to_owned());
    }
    let phase = if parsed.mode.is_none() {
        "plan_choice"
    } else {
        "project"
    };
    let mut draft = new_draft(chat_id, user_id, phase, payload);
    if let Some(cwd) = parsed.cwd {
        draft["payload"]["cwd"] = Value::String(cwd);
        if let Some(prompt) = parsed.prompt {
            draft["payload"]["initial_prompt"] = Value::String(prompt);
        }
        persist_new_draft(store, &key, &draft)?;
        return finish_new_project(
            store, agent, sessions, bot, bots_by_id, config, metrics, &key, draft,
        )
        .await;
    }
    if phase == "plan_choice" {
        persist_new_draft(store, &key, &draft)?;
        let choices = vec![
            ("plan_choice".into(), "yes".into(), "是".into()),
            ("plan_choice".into(), "no".into(), "否".into()),
        ];
        let markup = new_choice_markup(store, &key, &mut draft, &choices)?;
        send_text_with_markup(
            bot,
            &surface_for(bot, config, chat_id, None),
            "新 Session 是否先进入 Plan Mode？",
            markup,
            metrics,
        )
        .await
    } else {
        persist_new_draft(store, &key, &draft)?;
        send_text(
            bot,
            &surface_for(bot, config, chat_id, None),
            "请发送项目地址或项目描述；下一条文本消息会被识别为项目。",
            metrics,
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_new_callback(
    callback: codex_telegram_adapter::TelegramCallback,
    bot: RuntimeBot,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    store: &Arc<SqliteStore>,
    agent: &AppServerClient,
    sessions: &Arc<SessionRegistry>,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    let nonce = callback.data.strip_prefix("new:").unwrap_or_default();
    let key = new_draft_key(callback.chat_id);
    let Some(mut draft) = store
        .workflow_record("new", &key)
        .map_err(|error| error.to_string())?
    else {
        acknowledge_callback(&bot, &callback, Some("选择已失效")).await;
        return Ok(());
    };
    if new_draft_expired(&draft)
        || draft.get("user_id").and_then(Value::as_i64) != callback.actor.user_id
    {
        acknowledge_callback(&bot, &callback, Some("选择已过期或无权操作")).await;
        return Ok(());
    }
    let Some(user_id) = callback.actor.user_id else {
        acknowledge_callback(&bot, &callback, Some("无法确认发送者")).await;
        return Ok(());
    };
    let Some(stored) = store
        .consume_control_callback(nonce, user_id, callback.chat_id, now_ms())
        .map_err(|error| error.to_string())?
    else {
        acknowledge_callback(&bot, &callback, Some("选择已处理或已过期")).await;
        return Ok(());
    };
    let event = stored.action.as_str();
    let value = stored
        .payload
        .get("value")
        .and_then(Value::as_str)
        .unwrap_or_default();
    acknowledge_callback(&bot, &callback, Some("已收到")).await;
    draft["choices"] = json!({});
    let payload = draft.get("payload").cloned().unwrap_or_else(|| json!({}));
    let logical_effects = if new_control_event(event).is_some() {
        let models = if event == "cancel" {
            ModelChoices {
                entries: Vec::new(),
                summary: String::new(),
            }
        } else {
            list_model_choices(agent).await.unwrap_or(ModelChoices {
                entries: Vec::new(),
                summary: String::new(),
            })
        };
        dispatch_new_control_effects(&draft, event, value, &models).unwrap_or_default()
    } else {
        Vec::new()
    };
    let logical_send = new_render_effect(&logical_effects, RenderOperation::Send);
    if let Some(rendered) = new_render_effect(&logical_effects, RenderOperation::Edit) {
        let reference =
            TelegramMessageReference::new(callback.chat_id.to_string(), callback.message_id)
                .map_err(|error| error.to_string())?;
        edit_control_rendered(&bot, &reference, &rendered, None, metrics, None).await?;
    }
    match event {
        "cancel" => {
            store
                .delete_workflow_record("new", &key)
                .map_err(|error| error.to_string())?;
            store
                .delete_control_interaction(&key, now_ms())
                .map_err(|error| error.to_string())
                .map(|_| ())
        }
        "normal_model" => {
            let models = list_model_choices(agent).await?;
            let Some(model) = model_choice(&models, value) else {
                return send_text(&bot, &surface_for(&bot, config, callback.chat_id, None), "模型已不可用，请重新执行 /new。", metrics).await;
            };
            let next = advance_new_draft(store, &key, &draft, "normal_effort", json!({"normal_model": model.model}), false)?;
            let mut next = next;
            let choices = model
                .efforts
                .iter()
                .map(|effort| ("normal_effort".into(), effort.clone(), effort.clone()))
                .collect::<Vec<_>>();
            let markup = new_choice_markup(store, &key, &mut next, &choices)?;
            send_new_render_effect(
                &bot,
                &surface_for(&bot, config, callback.chat_id, None),
                logical_send.as_ref(),
                "模型支持以下 effort：",
                "模型支持以下 effort：",
                markup,
                metrics,
            )
            .await
        }
        "normal_effort" => {
            let mut next_payload = payload;
            next_payload["normal_effort"] = Value::String(value.to_owned());
            let next = advance_new_draft(store, &key, &draft, "plan_choice", next_payload, false)?;
            let mut next = next;
            let choices = vec![("plan_choice".into(), "yes".into(), "是".into()), ("plan_choice".into(), "no".into(), "否".into())];
            let markup = new_choice_markup(store, &key, &mut next, &choices)?;
            send_new_render_effect(
                &bot,
                &surface_for(&bot, config, callback.chat_id, None),
                logical_send.as_ref(),
                "新 Session 是否先进入 Plan Mode？",
                "新 Session 是否先进入 Plan Mode？",
                markup,
                metrics,
            )
            .await
        }
        "plan_choice" => {
            if value == "yes" {
                let next = advance_new_draft(store, &key, &draft, "plan_model", payload, false)?;
                let models = list_model_choices(agent).await?;
                let mut next = next;
                let choices = models
                    .entries
                    .iter()
                    .map(|entry| ("plan_model".into(), entry.model.clone(), entry.display_name.clone()))
                    .collect::<Vec<_>>();
                let markup = new_choice_markup(store, &key, &mut next, &choices)?;
                send_new_render_effect(
                    &bot,
                    &surface_for(&bot, config, callback.chat_id, None),
                    logical_send.as_ref(),
                    "请选择 Plan Mode 使用的模型：",
                    "请选择 Plan Mode 使用的模型：",
                    markup,
                    metrics,
                )
                .await
            } else {
                let mut next = advance_new_draft(store, &key, &draft, "project", payload, false)?;
                let markup = new_choice_markup(store, &key, &mut next, &[])?;
                send_new_render_effect(
                    &bot,
                    &surface_for(&bot, config, callback.chat_id, None),
                    logical_send.as_ref(),
                    "请发送项目地址或项目描述；下一条文本消息会被识别为项目。",
                    "请发送项目地址或项目描述；下一条文本消息会被识别为项目。",
                    markup,
                    metrics,
                )
                .await
            }
        }
        "plan_model" => {
            let models = list_model_choices(agent).await?;
            let Some(model) = model_choice(&models, value) else {
                return send_text(&bot, &surface_for(&bot, config, callback.chat_id, None), "Plan model 已不可用，请重新执行 /new。", metrics).await;
            };
            let next = advance_new_draft(store, &key, &draft, "plan_effort", json!({"normal_model": payload.get("normal_model").cloned().unwrap_or(Value::Null), "normal_effort": payload.get("normal_effort").cloned().unwrap_or(Value::Null), "plan_model": model.model}), false)?;
            let mut next = next;
            let choices = model
                .efforts
                .iter()
                .map(|effort| ("plan_effort".into(), effort.clone(), effort.clone()))
                .collect::<Vec<_>>();
            let markup = new_choice_markup(store, &key, &mut next, &choices)?;
            send_new_render_effect(
                &bot,
                &surface_for(&bot, config, callback.chat_id, None),
                logical_send.as_ref(),
                "模型支持以下 effort：",
                "模型支持以下 effort：",
                markup,
                metrics,
            )
            .await
        }
        "plan_effort" => {
            let mut next_payload = payload;
            next_payload["plan_effort"] = Value::String(value.to_owned());
            let mut next = advance_new_draft(store, &key, &draft, "project", next_payload, false)?;
            let markup = new_choice_markup(store, &key, &mut next, &[])?;
            send_new_render_effect(
                &bot,
                &surface_for(&bot, config, callback.chat_id, None),
                logical_send.as_ref(),
                "请发送项目地址或项目描述；下一条文本消息会被识别为项目。",
                "请发送项目地址或项目描述；下一条文本消息会被识别为项目。",
                markup,
                metrics,
            )
            .await
        }
        "project" => handle_new_project_value(store, agent, sessions, &bot, bots_by_id, config, metrics, &key, draft, value.to_owned()).await,
        "create_project" => {
            let target = payload.get("project_target").and_then(Value::as_str).unwrap_or_default();
            let path = PathBuf::from(target);
            ensure_private_directory(&path).map_err(|error| error.to_string())?;
            let mut next_payload = payload;
            next_payload["cwd"] = Value::String(path.to_string_lossy().into_owned());
            if let Some(object) = next_payload.as_object_mut() {
                object.remove("project_target");
            }
            let next = advance_new_draft(store, &key, &draft, "prompt", next_payload, true)?;
            send_new_prompt_or_finish(store, agent, sessions, &bot, bots_by_id, config, metrics, &key, next).await
        }
        "hello" => finish_new_prompt(store, agent, sessions, &bot, bots_by_id, config, metrics, &key, draft, "Hello".into(), false).await,
        _ => send_text(&bot, &surface_for(&bot, config, callback.chat_id, None), "该选择与当前步骤不匹配，请重新执行 /new。", metrics).await,
    }
    .map(|_| {
        let _ = sessions;
    })
}

#[allow(clippy::too_many_arguments)]
async fn handle_new_text(
    store: &Arc<SqliteStore>,
    agent: &AppServerClient,
    sessions: &Arc<SessionRegistry>,
    bot: &RuntimeBot,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    chat_id: i64,
    actor_user_id: Option<i64>,
    _message_id: i64,
    text: &str,
) -> Result<bool, String> {
    let key = new_draft_key(chat_id);
    let Some(draft) = store
        .workflow_record("new", &key)
        .map_err(|error| error.to_string())?
    else {
        return Ok(false);
    };
    if draft.get("user_id").and_then(Value::as_i64) != actor_user_id {
        return Ok(false);
    }
    if new_draft_expired(&draft) {
        store
            .delete_workflow_record("new", &key)
            .map_err(|error| error.to_string())?;
        let _ = store.delete_control_interaction(&key, now_ms());
        send_text(
            bot,
            &surface_for(bot, config, chat_id, None),
            " /new 交互已过期，请重新执行 /new。",
            metrics,
        )
        .await?;
        return Ok(true);
    }
    match draft
        .get("phase")
        .and_then(Value::as_str)
        .unwrap_or_default()
    {
        "project" | "project_choice" | "project_confirmation" => {
            handle_new_project_value(
                store,
                agent,
                sessions,
                bot,
                bots_by_id,
                config,
                metrics,
                &key,
                draft,
                text.trim().to_owned(),
            )
            .await?;
        }
        "prompt" => {
            finish_new_prompt(
                store,
                agent,
                sessions,
                bot,
                bots_by_id,
                config,
                metrics,
                &key,
                draft,
                text.trim().to_owned(),
                false,
            )
            .await?;
        }
        _ => {
            send_text(
                bot,
                &surface_for(bot, config, chat_id, None),
                "请使用当前按钮完成 /new 选择。",
                metrics,
            )
            .await?;
        }
    }
    let _ = sessions;
    Ok(true)
}

fn expand_user_path(value: &str) -> PathBuf {
    let value = value.trim();
    let home = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    if value == "~" {
        return home;
    }
    if let Some(relative) = value.strip_prefix("~/") {
        return home.join(relative);
    }
    if let Some(relative) = value.strip_prefix("~\\") {
        return home.join(relative);
    }
    PathBuf::from(value)
}

fn missing_path_has_safe_ancestor(root: &Path, target: &Path) -> bool {
    let mut ancestor = target;
    loop {
        if ancestor.exists() {
            return fs::canonicalize(ancestor)
                .ok()
                .is_some_and(|resolved| resolved == root || resolved.strip_prefix(root).is_ok());
        }
        let Some(parent) = ancestor.parent() else {
            return false;
        };
        if parent == ancestor {
            return false;
        }
        ancestor = parent;
    }
}

fn new_existing_projects(root: &Path, value: &str) -> Result<Vec<PathBuf>, String> {
    let root =
        fs::canonicalize(root).map_err(|error| format!("workspace 根目录不可用: {error}"))?;
    let value = value.trim();
    if value.is_empty() {
        return Ok(Vec::new());
    }
    let candidate = expand_user_path(value);
    if candidate.is_absolute() {
        if !candidate.exists() {
            return Ok(Vec::new());
        }
        let resolved = fs::canonicalize(candidate).map_err(|error| error.to_string())?;
        if resolved.is_dir() && resolved.strip_prefix(&root).is_ok() {
            return Ok(vec![resolved]);
        }
        return Err("项目路径必须位于 workspace 根目录内。".into());
    }
    let direct = root.join(value);
    if direct.is_dir() {
        let resolved = fs::canonicalize(direct).map_err(|error| error.to_string())?;
        if resolved.strip_prefix(&root).is_err() {
            return Err("项目路径必须位于 workspace 根目录内。".into());
        }
        return Ok(vec![resolved]);
    }
    let needle = value.to_ascii_lowercase();
    let mut matches = Vec::new();
    let mut stack = vec![(root.clone(), 0usize)];
    while let Some((directory, depth)) = stack.pop() {
        if depth >= 3 {
            continue;
        }
        let entries = fs::read_dir(&directory).map_err(|error| error.to_string())?;
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            if name.contains(&needle)
                || path
                    .to_string_lossy()
                    .to_ascii_lowercase()
                    .contains(&needle)
            {
                matches.push(path.clone());
            }
            stack.push((path, depth + 1));
            if matches.len() >= 8 {
                return Ok(matches);
            }
        }
    }
    Ok(matches)
}

#[allow(clippy::too_many_arguments)]
async fn handle_new_project_value(
    store: &Arc<SqliteStore>,
    agent: &AppServerClient,
    sessions: &Arc<SessionRegistry>,
    bot: &RuntimeBot,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    key: &str,
    draft: Value,
    value: String,
) -> Result<(), String> {
    let candidates = new_existing_projects(&config.workspace_root, &value)?;
    if candidates.len() == 1 {
        let mut payload = draft.get("payload").cloned().unwrap_or_else(|| json!({}));
        payload["cwd"] = Value::String(candidates[0].to_string_lossy().into_owned());
        return send_new_prompt_or_finish(
            store,
            agent,
            sessions,
            bot,
            bots_by_id,
            config,
            metrics,
            key,
            advance_new_draft(store, key, &draft, "prompt", payload, true)?,
        )
        .await;
    }
    if candidates.len() > 1 {
        let next = advance_new_draft(
            store,
            key,
            &draft,
            "project_choice",
            draft.get("payload").cloned().unwrap_or_else(|| json!({})),
            false,
        )?;
        let mut next = next;
        let choices = candidates
            .iter()
            .map(|path| {
                (
                    "project".into(),
                    path.to_string_lossy().into_owned(),
                    path.to_string_lossy().into_owned(),
                )
            })
            .collect::<Vec<_>>();
        let markup = new_choice_markup(store, key, &mut next, &choices)?;
        return send_text_with_markup(
            bot,
            &surface_for(
                bot,
                config,
                next["chat_id"].as_i64().unwrap_or_default(),
                None,
            ),
            "找到多个项目，请选择工作目录：",
            markup,
            metrics,
        )
        .await;
    }
    let raw = expand_user_path(value.trim());
    if raw.is_absolute() && !raw.exists() {
        let root = fs::canonicalize(&config.workspace_root).map_err(|error| error.to_string())?;
        if raw.strip_prefix(&root).is_ok() && missing_path_has_safe_ancestor(&root, &raw) {
            let mut payload = draft.get("payload").cloned().unwrap_or_else(|| json!({}));
            payload["project_target"] = Value::String(raw.to_string_lossy().into_owned());
            let mut next =
                advance_new_draft(store, key, &draft, "project_confirmation", payload, false)?;
            let choices = vec![(
                "create_project".into(),
                raw.to_string_lossy().into_owned(),
                "创建目录".into(),
            )];
            let markup = new_choice_markup(store, key, &mut next, &choices)?;
            return send_text_with_markup(
                bot,
                &surface_for(
                    bot,
                    config,
                    next["chat_id"].as_i64().unwrap_or_default(),
                    None,
                ),
                &format!(
                    "目录 `{}` 不存在，是否创建？",
                    truncate_text(&raw.to_string_lossy())
                ),
                markup,
                metrics,
            )
            .await;
        }
    }
    send_text(
        bot,
        &surface_for(
            bot,
            config,
            draft["chat_id"].as_i64().unwrap_or_default(),
            None,
        ),
        "没有找到匹配项目。请发送允许目录中的明确路径。",
        metrics,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn send_new_prompt_or_finish(
    store: &Arc<SqliteStore>,
    agent: &AppServerClient,
    sessions: &Arc<SessionRegistry>,
    bot: &RuntimeBot,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    key: &str,
    draft: Value,
) -> Result<(), String> {
    if let Some(prompt) = draft["payload"]
        .get("initial_prompt")
        .and_then(Value::as_str)
        .map(str::to_owned)
    {
        return finish_new_prompt(
            store, agent, sessions, bot, bots_by_id, config, metrics, key, draft, prompt, false,
        )
        .await;
    }
    let mut next = draft;
    let choices = vec![("hello".into(), "Hello".into(), "Hello".into())];
    let markup = new_choice_markup(store, key, &mut next, &choices)?;
    send_text_with_markup(
        bot,
        &surface_for(
            bot,
            config,
            next["chat_id"].as_i64().unwrap_or_default(),
            None,
        ),
        "请发送第一条 prompt。30 秒内未发送时将使用 `Hello`。",
        markup,
        metrics,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn finish_new_project(
    store: &Arc<SqliteStore>,
    agent: &AppServerClient,
    sessions: &Arc<SessionRegistry>,
    bot: &RuntimeBot,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    key: &str,
    draft: Value,
) -> Result<(), String> {
    send_new_prompt_or_finish(
        store, agent, sessions, bot, bots_by_id, config, metrics, key, draft,
    )
    .await
}

fn pending_space_payload(store: &SqliteStore, space_id: &str) -> Result<Value, String> {
    if let Some(payload) = store
        .workflow_record("pending_space", space_id)
        .map_err(|error| error.to_string())?
    {
        return Ok(payload);
    }
    let Some(mut payload) = store
        .workflow_record("space", space_id)
        .map_err(|error| error.to_string())?
    else {
        return Err("待认证 Session 的持久化草稿不存在".into());
    };
    if let Some(state) = payload
        .get("state_json")
        .and_then(Value::as_str)
        .and_then(|text| serde_json::from_str::<Value>(text).ok())
        && let (Some(target), Some(source)) = (payload.as_object_mut(), state.as_object())
    {
        for key in [
            "pending_cwd",
            "pending_prompt",
            "normal_model",
            "normal_effort",
            "plan_model",
            "plan_effort",
            "current_mode",
        ] {
            if !target.contains_key(key)
                && let Some(value) = source.get(key)
            {
                target.insert(key.to_owned(), value.clone());
            }
        }
    }
    Ok(payload)
}

fn space_profile_text(payload: &Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

#[allow(clippy::too_many_arguments)]
async fn create_pending_session_space(
    store: &SqliteStore,
    bot: &RuntimeBot,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    owner_chat_id: i64,
    generation: i64,
    payload: &Value,
    prompt: &str,
    space_id: &str,
) -> Result<(RustSessionSpace, SentMessage), String> {
    let cwd = payload
        .get("cwd")
        .and_then(Value::as_str)
        .ok_or_else(|| "项目目录未选择".to_owned())?;
    if store
        .workflow_record("onboarding", "binding")
        .map_err(|error| error.to_string())?
        .is_none()
    {
        return Err("频道与讨论组尚未绑定，请先完成 /bind。".to_owned());
    }
    let plan_model = payload
        .get("plan_model")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty());
    let plan_effort = payload
        .get("plan_effort")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty());
    if plan_model.is_some() != plan_effort.is_some() {
        return Err("Plan model 和 effort 必须同时存在".into());
    }
    if let Some(existing) = store
        .get_session_space(space_id)
        .map_err(|error| error.to_string())?
        && matches!(existing.lifecycle.as_str(), "pending" | "repair_required")
    {
        return Ok((
            existing.clone(),
            SentMessage {
                message_id: existing.channel_post_id,
            },
        ));
    }
    if let Some(previous) = store
        .workflow_record("pending_space", space_id)
        .map_err(|error| error.to_string())?
        && let Some(post_id) = previous.get("channel_post_id").and_then(Value::as_i64)
        && post_id > 0
    {
        let native_root = store
            .native_comment_root(config.channel_chat_id, post_id)
            .map_err(|error| error.to_string())?;
        let recovered = RustSessionSpace {
            space_id: space_id.to_owned(),
            thread_id: None,
            lifecycle: "pending".into(),
            generation,
            channel_chat_id: config.channel_chat_id,
            channel_post_id: post_id,
            discussion_chat_id: native_root.as_ref().map(|root| root.discussion_chat_id),
            discussion_root_message_id: native_root.as_ref().map(|root| root.root_message_id),
            status_message_id: None,
            status_bot_instance: None,
            owner_chat_id: Some(owner_chat_id),
            plan_mode: plan_model.is_some(),
            observed_mode: None,
            normal_model: space_profile_text(payload, "normal_model"),
            normal_effort: space_profile_text(payload, "normal_effort"),
            plan_model: plan_model.map(str::to_owned),
            plan_effort: plan_effort.map(str::to_owned),
            closed_at_ms: None,
            created_at_ms: now_ms(),
            updated_at_ms: now_ms(),
        };
        match store.upsert_session_space(&recovered) {
            Ok(()) => {
                return Ok((
                    recovered.clone(),
                    SentMessage {
                        message_id: post_id,
                    },
                ));
            }
            Err(error) => {
                if let Some(existing) = store
                    .session_space_for_channel_post(config.channel_chat_id, post_id)
                    .map_err(|lookup| lookup.to_string())?
                    .filter(|existing| existing.lifecycle != "closed")
                {
                    return Ok((
                        existing.clone(),
                        SentMessage {
                            message_id: existing.channel_post_id,
                        },
                    ));
                }
                return Err(error.to_string());
            }
        }
    }
    let channel = ChannelBinding::new(
        bot.config.instance_id.clone(),
        config.channel_chat_id.to_string(),
    )
    .map_err(|error| error.message.to_owned())?;
    let mode = if plan_model.is_some() {
        "Plan"
    } else {
        "普通"
    };
    let channel_text = format!(
        "🆕 待认证 Codex Session\n📁 {}\n📝 {}\n模式：{}\n\n请在评论串发送 /totp <验证码> 完成激活。",
        truncate_text(cwd),
        truncate_text(prompt),
        mode,
    );
    // Persist the activation intent before the Telegram side effect.  The
    // message id is filled in after send, but a restart can still find the
    // exact payload and retry instead of losing the claimed `/new` request.
    let now = now_ms();
    let mut pending = payload.clone();
    pending["space_id"] = Value::String(space_id.to_owned());
    pending["pending_cwd"] = Value::String(cwd.to_owned());
    pending["pending_prompt"] = Value::String(prompt.to_owned());
    store
        .upsert_workflow_record("pending_space", space_id, &pending, now)
        .map_err(|error| error.to_string())?;
    let message = send_text_message(
        bot,
        &TelegramSurfaceBinding::Channel(channel),
        &channel_text,
        metrics,
    )
    .await?;
    pending["channel_post_id"] = Value::from(message.message_id.max(1));
    store
        .upsert_workflow_record("pending_space", space_id, &pending, now_ms())
        .map_err(|error| error.to_string())?;
    let native_root = store
        .native_comment_root(config.channel_chat_id, message.message_id)
        .map_err(|error| error.to_string())?;
    let space = RustSessionSpace {
        space_id: space_id.to_owned(),
        thread_id: None,
        lifecycle: "pending".into(),
        generation,
        channel_chat_id: config.channel_chat_id,
        channel_post_id: message.message_id.max(1),
        discussion_chat_id: native_root.as_ref().map(|root| root.discussion_chat_id),
        discussion_root_message_id: native_root.as_ref().map(|root| root.root_message_id),
        status_message_id: None,
        status_bot_instance: None,
        owner_chat_id: Some(owner_chat_id),
        plan_mode: plan_model.is_some(),
        observed_mode: None,
        normal_model: space_profile_text(payload, "normal_model"),
        normal_effort: space_profile_text(payload, "normal_effort"),
        plan_model: plan_model.map(str::to_owned),
        plan_effort: plan_effort.map(str::to_owned),
        closed_at_ms: None,
        created_at_ms: now,
        updated_at_ms: now,
    };
    if let Err(error) = store.upsert_session_space(&space) {
        if let Some(existing) = store
            .session_space_for_channel_post(space.channel_chat_id, space.channel_post_id)
            .map_err(|lookup| lookup.to_string())?
            .filter(|existing| existing.lifecycle != "closed")
        {
            return Ok((
                existing.clone(),
                SentMessage {
                    message_id: existing.channel_post_id,
                },
            ));
        }
        return Err(error.to_string());
    }
    Ok((space, message))
}

#[allow(clippy::too_many_arguments)]
async fn activate_pending_session(
    store: &Arc<SqliteStore>,
    agent: &AppServerClient,
    sessions: &Arc<SessionRegistry>,
    bot: &RuntimeBot,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    totp: &TotpManager,
    space: &RustSessionSpace,
) -> Result<(), String> {
    if space.lifecycle == "active" && space.thread_id.is_some() {
        return send_text(
            bot,
            &surface_for(
                bot,
                config,
                space
                    .discussion_chat_id
                    .unwrap_or(config.discussion_chat_id),
                space.discussion_root_message_id,
            ),
            "当前 Session 已经激活。",
            metrics,
        )
        .await;
    }
    let payload = pending_space_payload(store, &space.space_id)?;
    let cwd = payload
        .get("pending_cwd")
        .or_else(|| payload.get("cwd"))
        .and_then(Value::as_str)
        .ok_or_else(|| "待认证 Session 缺少项目目录".to_owned())?;
    let prompt = payload
        .get("pending_prompt")
        .or_else(|| payload.get("prompt"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "待认证 Session 缺少首条 prompt".to_owned())?;
    let root = fs::canonicalize(&config.workspace_root)
        .map_err(|error| format!("workspace 根目录不可用: {error}"))?;
    let cwd_path = fs::canonicalize(cwd).map_err(|error| format!("项目目录不可用: {error}"))?;
    if !cwd_path.is_dir() || cwd_path.strip_prefix(&root).is_err() {
        return Err("项目目录必须是 workspace 根目录内的现有目录。".into());
    }
    let cwd = cwd_path.to_string_lossy().into_owned();
    let plan_mode = space.plan_mode
        || payload
            .get("plan_model")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty());
    let mode_key = if plan_mode { "plan" } else { "default" };
    let model_key = if plan_mode {
        "plan_model"
    } else {
        "normal_model"
    };
    let effort_key = if plan_mode {
        "plan_effort"
    } else {
        "normal_effort"
    };
    let model = payload.get(model_key).and_then(Value::as_str);
    let effort = payload.get(effort_key).and_then(Value::as_str);
    if model.is_some() != effort.is_some() {
        return Err("Session model 和 effort 必须同时存在。".into());
    }
    let collaboration_mode = if model.is_some() {
        Some(
            collaboration_mode_payload(agent, mode_key, model, effort)
                .await
                .map_err(|error| error.to_string())?,
        )
    } else {
        None
    };
    let thread = if let Some(thread_id) = space
        .thread_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        let thread_id = ThreadId::new(thread_id).map_err(|error| error.to_string())?;
        agent
            .resume_thread(&thread_id)
            .await
            .map_err(|error| error.to_string())?
    } else {
        let thread = agent
            .start_thread(&cwd, false, false)
            .await
            .map_err(|error| error.to_string())?;
        let repair = RustSessionSpace {
            thread_id: Some(thread.id.to_string()),
            lifecycle: "repair_required".into(),
            updated_at_ms: now_ms(),
            ..space.clone()
        };
        store
            .upsert_session_space(&repair)
            .map_err(|error| error.to_string())?;
        thread
    };
    let discussion_chat_id = space
        .discussion_chat_id
        .unwrap_or(config.discussion_chat_id);
    let repair = RustSessionSpace {
        thread_id: Some(thread.id.to_string()),
        lifecycle: "repair_required".into(),
        owner_chat_id: Some(discussion_chat_id),
        updated_at_ms: now_ms(),
        ..space.clone()
    };
    store
        .upsert_session_space(&repair)
        .map_err(|error| error.to_string())?;
    sessions.insert(SessionRecord {
        thread_id: thread.id.clone(),
        turn_id: None,
        chat_id: discussion_chat_id,
        root_message_id: repair.discussion_root_message_id,
        sender_instance_id: bot.config.instance_id.clone(),
    });
    // The space id is the idempotency key for the initial prompt.  A retry
    // after a transport timeout must reuse it so app-server can deduplicate
    // the request instead of starting a second turn.
    let client_message_id = format!("telegram-new-{}", repair.space_id);
    let existing_intent = store
        .prompt_intent_by_client_message_id(&client_message_id)
        .map_err(|error| error.to_string())?;
    if let Some(existing) = existing_intent.as_ref()
        && let Some(turn_id) = existing.turn_id.clone()
        && matches!(
            existing.state,
            PromptIntentState::Started | PromptIntentState::Steered | PromptIntentState::Completed
        )
    {
        sessions.set_turn(thread.id.as_str(), Some(turn_id.clone()));
        let active = RustSessionSpace {
            lifecycle: "active".into(),
            updated_at_ms: now_ms(),
            ..repair
        };
        store
            .upsert_session_space(&active)
            .map_err(|error| error.to_string())?;
        store
            .delete_workflow_record("pending_space", &active.space_id)
            .map_err(|error| error.to_string())?;
        update_status_message(
            store, bots_by_id, config, metrics, totp, &active, None, None, false, None,
        )
        .await?;
        let short_id = thread.id.as_str().chars().take(8).collect::<String>();
        return send_text(
            bot,
            &surface_for(
                bot,
                config,
                discussion_chat_id,
                active.discussion_root_message_id,
            ),
            &format!("已创建 Session `{short_id}`。"),
            metrics,
        )
        .await;
    }
    let mut intent = existing_intent.unwrap_or_else(|| PromptIntent {
        intent_id: format!("intent-{client_message_id}"),
        client_message_id: client_message_id.clone(),
        source: "session_activation".into(),
        prompt: prompt.to_owned(),
        mode: if plan_mode {
            "plan".into()
        } else {
            "default".into()
        },
        thread_id: Some(thread.id.clone()),
        space_id: Some(repair.space_id.clone()),
        generation: agent.connection_state().generation,
        state: PromptIntentState::Submitting,
        turn_id: None,
        queue_id: None,
        error: None,
        created_at_ms: now_ms(),
        updated_at_ms: now_ms(),
    });
    intent.thread_id = Some(thread.id.clone());
    intent.space_id = Some(repair.space_id.clone());
    intent.prompt = prompt.to_owned();
    intent.state = PromptIntentState::Submitting;
    intent.error = None;
    intent.updated_at_ms = now_ms();
    store
        .upsert_prompt_intent(&intent)
        .map_err(|error| error.to_string())?;
    let turn = match agent
        .start_turn_with_collaboration_mode(
            &thread.id,
            vec![PromptInput::text(prompt).map_err(|error| error.to_string())?],
            Some(&client_message_id),
            collaboration_mode,
        )
        .await
    {
        Ok(turn) => turn,
        Err(error) => {
            intent.state = PromptIntentState::Uncertain;
            intent.error = Some(error.to_string());
            intent.updated_at_ms = now_ms();
            store
                .upsert_prompt_intent(&intent)
                .map_err(|store_error| store_error.to_string())?;
            let mut repair = repair.clone();
            repair.lifecycle = "repair_required".into();
            repair.updated_at_ms = now_ms();
            store
                .upsert_session_space(&repair)
                .map_err(|store_error| store_error.to_string())?;
            return send_text(
                bot,
                &surface_for(
                    bot,
                    config,
                    discussion_chat_id,
                    repair.discussion_root_message_id,
                ),
                &format!("Session 已创建但首条 prompt 送达待确认：{error}"),
                metrics,
            )
            .await;
        }
    };
    sessions.set_turn(thread.id.as_str(), Some(turn.id.clone()));
    intent.state = PromptIntentState::Started;
    intent.turn_id = Some(turn.id.clone());
    intent.updated_at_ms = now_ms();
    store
        .upsert_prompt_intent(&intent)
        .map_err(|error| error.to_string())?;
    let activation_record = json!({
        "intent_id": intent.intent_id,
        "client_message_id": client_message_id,
        "source": "session_activation",
        "thread_id": turn.thread_id.as_str(),
        "turn_id": turn.id.as_str(),
        "space_id": repair.space_id,
        "generation": repair.generation,
        "state": "started",
    });
    store
        .upsert_workflow_record(
            "prompt",
            &intent.client_message_id,
            &activation_record,
            now_ms(),
        )
        .map_err(|error| error.to_string())?;
    let active = RustSessionSpace {
        lifecycle: "active".into(),
        updated_at_ms: now_ms(),
        ..repair
    };
    store
        .upsert_session_space(&active)
        .map_err(|error| error.to_string())?;
    store
        .delete_workflow_record("pending_space", &active.space_id)
        .map_err(|error| error.to_string())?;
    update_status_message(
        store, bots_by_id, config, metrics, totp, &active, None, None, false, None,
    )
    .await?;
    let short_id = thread.id.as_str().chars().take(8).collect::<String>();
    send_text(
        bot,
        &surface_for(
            bot,
            config,
            discussion_chat_id,
            active.discussion_root_message_id,
        ),
        &format!("已创建 Session `{short_id}`。"),
        metrics,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn finish_new_prompt(
    store: &Arc<SqliteStore>,
    agent: &AppServerClient,
    _sessions: &Arc<SessionRegistry>,
    bot: &RuntimeBot,
    _bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    key: &str,
    draft: Value,
    prompt: String,
    expired: bool,
) -> Result<(), String> {
    let (current, revision) = new_interaction_revision(store, key, &draft)?;
    let claimed = if expired {
        store
            .claim_expired_control_interaction(
                key,
                current.user_id,
                current.chat_id,
                revision,
                now_ms(),
            )
            .map_err(|error| error.to_string())?
    } else {
        store
            .claim_control_interaction(key, current.user_id, current.chat_id, revision, now_ms())
            .map_err(|error| error.to_string())?
    };
    let Some(claimed) = claimed else {
        return Ok(());
    };
    let chat_id = claimed.chat_id;
    let payload = claimed.payload;
    let space_id = format!("telegram-pending-{}-{}", chat_id, claimed.created_at_ms);
    let (space, channel_post) = match create_pending_session_space(
        store,
        bot,
        config,
        metrics,
        claimed.chat_id,
        i64::try_from(agent.connection_state().generation).unwrap_or(0),
        &payload,
        &prompt,
        &space_id,
    )
    .await
    {
        Ok(value) => value,
        Err(error) => {
            // A claimed /new interaction is terminal once creation has
            // reached this point. Releasing the claim lets the expiry worker
            // submit a second implicit `Hello`, producing "failed" followed
            // by a later successful Session. The user can explicitly retry
            // with a fresh /new instead.
            let _ = store.delete_workflow_record("new", key);
            let _ = store.delete_control_interaction(key, now_ms());
            send_text(
                bot,
                &surface_for(bot, config, chat_id, None),
                &format!("Session 创建失败，请重新执行 /new：{error}"),
                metrics,
            )
            .await?;
            return Err(error);
        }
    };
    store
        .delete_workflow_record("new", key)
        .map_err(|error| error.to_string())?;
    store
        .delete_control_interaction(key, now_ms())
        .map_err(|error| error.to_string())?;
    let post_link = telegram_message_link(space.channel_chat_id, channel_post.message_id);
    let (rendered, markup) = pending_session_confirmation(&post_link)?;
    send_control_rendered(
        bot,
        &surface_for(bot, config, chat_id, None),
        &rendered,
        Some(markup),
        metrics,
    )
    .await
    .map(|_| ())
}

fn pending_session_confirmation(
    post_link: &str,
) -> Result<(RenderedEffect, InlineKeyboardMarkup), String> {
    let markup = InlineKeyboardMarkup::new(vec![vec![
        InlineKeyboardButton::url("打开帖子", post_link).map_err(|error| error.to_string())?,
    ]])
    .map_err(|error| error.to_string())?;
    Ok((
        RenderedEffect {
            operation: RenderOperation::Send,
            markdown: "待认证 Session 帖子已创建。进入评论串并发送 `/totp <验证码>`。".to_owned(),
            plain: Some("待认证 Session 帖子已创建。进入评论串并发送 /totp <验证码>。".to_owned()),
            keyboard: None,
        },
        markup,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn run_new_interaction_expirer(
    store: Arc<SqliteStore>,
    agent: AppServerClient,
    sessions: Arc<SessionRegistry>,
    bot: RuntimeBot,
    bots_by_id: HashMap<String, RuntimeBot>,
    config: RustConfig,
    metrics: MetricsRegistry,
) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let records = match store.workflow_records("new") {
            Ok(records) => records,
            Err(error) => {
                eprintln!("rust bridge /new expiry scan failed: {error}");
                continue;
            }
        };
        for (key, draft) in records {
            let expires_at = draft
                .get("expires_at_ms")
                .and_then(Value::as_i64)
                .unwrap_or(i64::MAX);
            if expires_at > now_ms() {
                continue;
            }
            let phase = draft
                .get("phase")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if phase == "prompt" {
                if let Err(error) = finish_new_prompt(
                    &store,
                    &agent,
                    &sessions,
                    &bot,
                    &bots_by_id,
                    &config,
                    &metrics,
                    &key,
                    draft,
                    "Hello".into(),
                    true,
                )
                .await
                {
                    eprintln!("rust bridge /new Hello expiry failed: {error}");
                }
            } else {
                let Some(user_id) = draft.get("user_id").and_then(Value::as_i64) else {
                    continue;
                };
                let Some(chat_id) = draft.get("chat_id").and_then(Value::as_i64) else {
                    continue;
                };
                let Ok((_, revision)) = new_interaction_revision(&store, &key, &draft) else {
                    continue;
                };
                match store.claim_expired_control_interaction(
                    &key,
                    user_id,
                    chat_id,
                    revision,
                    now_ms(),
                ) {
                    Ok(Some(_)) => {
                        let _ = store.delete_workflow_record("new", &key);
                        let _ = store.delete_control_interaction(&key, now_ms());
                    }
                    Ok(None) => {}
                    Err(error) => eprintln!("rust bridge /new draft cleanup failed: {error}"),
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_pair_command(
    text: &str,
    chat_id: i64,
    actor_user_id: Option<i64>,
    bot: &RuntimeBot,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    store: &Arc<SqliteStore>,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    let surface = surface_for(bot, config, chat_id, None);
    let Some(actor_user_id) = actor_user_id else {
        return send_text(
            bot,
            &surface,
            "无法确认个人发送身份；请关闭匿名管理员后重试。",
            metrics,
        )
        .await;
    };
    if store
        .workflow_record("onboarding", "owner")
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return send_text(
            bot,
            &surface,
            "Bot 已配对；更换 owner 只能使用本机 owner-reset。",
            metrics,
        )
        .await;
    }
    let code = text
        .split_once(char::is_whitespace)
        .map(|(_, value)| value.trim())
        .unwrap_or_default();
    if code.is_empty() {
        return send_text(bot, &surface, "用法：/pair <本机配对码>", metrics).await;
    }
    if !consume_onboarding_code(store, "pair", code)? {
        return send_text(bot, &surface, "配对码无效、过期或尝试次数过多。", metrics).await;
    }
    store
        .upsert_workflow_record(
            "onboarding",
            "owner",
            &json!({
                "chat_id": chat_id,
                "user_id": actor_user_id,
                "paired_at_ms": now_ms()
            }),
            now_ms(),
        )
        .map_err(|error| error.to_string())?;
    refresh_command_menus(bots_by_id, config, store).await;
    send_text(
        bot,
        &surface,
        "配对成功。Session 写操作在评论串内使用 TOTP 认证。",
        metrics,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn handle_bind_command(
    text: &str,
    chat_id: i64,
    actor_user_id: Option<i64>,
    bot: &RuntimeBot,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    store: &Arc<SqliteStore>,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    let surface = surface_for(bot, config, chat_id, None);
    if store
        .workflow_record("onboarding", "binding")
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return send_text(bot, &surface, "讨论组已经绑定。", metrics).await;
    }
    if store
        .workflow_record("onboarding", "owner")
        .map_err(|error| error.to_string())?
        .is_none()
    {
        return send_text(
            bot,
            &surface,
            "请先在 Control Bot 私聊完成 owner 配对。",
            metrics,
        )
        .await;
    }
    let owner_user_id = store
        .workflow_record("onboarding", "owner")
        .map_err(|error| error.to_string())?
        .and_then(|value| value.get("user_id").and_then(Value::as_i64));
    if owner_user_id.is_none() || actor_user_id != owner_user_id {
        return send_text(
            bot,
            &surface,
            "绑定请求未获授权；请使用已配对的个人账号发送。",
            metrics,
        )
        .await;
    }
    let code = text
        .split_once(char::is_whitespace)
        .map(|(_, value)| value.trim())
        .unwrap_or_default();
    if code.is_empty() {
        return send_text(bot, &surface, "用法：/bind <本机 bind code>", metrics).await;
    }

    let Some(control_bot) = bots_by_id
        .values()
        .find(|candidate| candidate.role == RuntimeBotRole::Control)
        .cloned()
    else {
        return send_text(
            bot,
            &surface,
            "Control Bot 不可用，暂时无法校验频道拓扑。",
            metrics,
        )
        .await;
    };
    let discussion_api = bot.api.clone();
    let discussion_token = bot.token.clone();
    let control_api = control_bot.api.clone();
    let control_token = control_bot.token.clone();
    let expected_channel_id = config.channel_chat_id;
    let topology = tokio::task::spawn_blocking(move || {
        let group = discussion_api
            .get_chat(&discussion_token, chat_id)
            .map_err(|error| error.to_string())?;
        if group.chat_type != "supergroup"
            || group.linked_chat_id != Some(expected_channel_id)
            || group.is_forum
            || group.username.is_some()
        {
            return Err("当前群不是已关联频道的私有讨论超级群，或启用了 Forum Topics。".to_owned());
        }
        let channel = control_api
            .get_chat(&control_token, expected_channel_id)
            .map_err(|error| error.to_string())?;
        if channel.chat_type != "channel"
            || channel.linked_chat_id != Some(chat_id)
            || channel.username.is_some()
        {
            return Err("关联频道必须是与讨论组双向关联的私有频道。".to_owned());
        }
        let control_me = control_api
            .get_me(&control_token)
            .map_err(|error| error.to_string())?;
        let discussion_me = discussion_api
            .get_me(&discussion_token)
            .map_err(|error| error.to_string())?;
        let control_member = control_api
            .get_chat_member(&control_token, expected_channel_id, control_me.id)
            .map_err(|error| error.to_string())?;
        if control_member.status != "administrator"
            || !control_member.can_post_messages
            || !control_member.can_edit_messages
        {
            return Err("Control Bot 不是频道管理员，或缺少发布/编辑消息权限。".to_owned());
        }
        let discussion_member = discussion_api
            .get_chat_member(&discussion_token, chat_id, discussion_me.id)
            .map_err(|error| error.to_string())?;
        if discussion_member.status != "administrator" || !discussion_member.can_delete_messages {
            return Err("Discussion Bot 不是讨论组管理员，或缺少删除消息权限。".to_owned());
        }
        let owner_member = discussion_api
            .get_chat_member(
                &discussion_token,
                chat_id,
                owner_user_id.expect("checked above"),
            )
            .map_err(|error| error.to_string())?;
        if owner_member.is_anonymous {
            return Err("owner 的匿名管理员模式必须关闭。".to_owned());
        }
        Ok::<(), String>(())
    })
    .await
    .map_err(|error| format!("绑定拓扑校验任务失败: {error}"))?;
    if let Err(error) = topology {
        return send_text(bot, &surface, &error, metrics).await;
    }
    if !consume_onboarding_code(store, "bind", code)? {
        return send_text(
            bot,
            &surface,
            "bind code 无效、过期或尝试次数过多。",
            metrics,
        )
        .await;
    }
    store
        .upsert_workflow_record(
            "onboarding",
            "binding",
            &json!({
                "channel_chat_id": config.channel_chat_id,
                "discussion_chat_id": config.discussion_chat_id,
                "chat_id": chat_id,
                "bound_at_ms": now_ms(),
            }),
            now_ms(),
        )
        .map_err(|error| error.to_string())?;
    refresh_command_menus(bots_by_id, config, store).await;
    send_text(bot, &surface, "频道与讨论组绑定成功。", metrics).await
}

fn consume_onboarding_code(store: &SqliteStore, kind: &str, code: &str) -> Result<bool, String> {
    let Some(mut record) = store
        .workflow_record("onboarding_code", kind)
        .map_err(|error| error.to_string())?
    else {
        return Ok(false);
    };
    let now = now_ms();
    let expires = record
        .get("expires_at_ms")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let failures = record.get("failures").and_then(Value::as_u64).unwrap_or(0);
    if expires < now || failures >= 5 {
        return Ok(false);
    }
    let expected = record
        .get("sha256")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let mut digest = Sha256::new();
    digest.update(code.trim().as_bytes());
    let actual = format!("{:x}", digest.finalize());
    if expected.is_empty() || !constant_time_equal(expected.as_bytes(), actual.as_bytes()) {
        record["failures"] = Value::from(failures.saturating_add(1));
        store
            .upsert_workflow_record("onboarding_code", kind, &record, now)
            .map_err(|error| error.to_string())?;
        return Ok(false);
    }
    store
        .delete_workflow_record("onboarding_code", kind)
        .map_err(|error| error.to_string())?;
    Ok(true)
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0_u8;
    for (a, b) in left.iter().zip(right.iter()) {
        difference |= a ^ b;
    }
    difference == 0
}

fn revising_plan_for_reply(
    store: &SqliteStore,
    thread_id: &ThreadId,
    reply_message_id: i64,
) -> Result<Option<PlanPublication>, String> {
    for (_, value) in store
        .workflow_records("plan")
        .map_err(|error| error.to_string())?
    {
        let Ok(publication) = serde_json::from_value::<PlanPublication>(value) else {
            continue;
        };
        if publication.status == PlanPublicationState::Revising
            && publication.thread_id.as_str() == thread_id.as_str()
            && publication.revision_prompt_message_id == Some(reply_message_id)
        {
            return Ok(Some(publication));
        }
    }
    Ok(None)
}

#[allow(clippy::too_many_arguments)]
async fn submit_plan_revision_feedback(
    store: &SqliteStore,
    agent: &AppServerClient,
    sessions: &SessionRegistry,
    bot: &RuntimeBot,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    session: SessionRecord,
    mut publication: PlanPublication,
    feedback: &str,
    message_id: i64,
) -> Result<(), String> {
    let client_message_id = format!(
        "telegram-plan-revise-{}-{}-{}-{}",
        publication.space_id, publication.generation, publication.item_id, publication.revision_key
    );
    if store
        .prompt_intent_by_client_message_id(&client_message_id)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return send_text(
            bot,
            &surface_for(bot, config, session.chat_id, session.root_message_id),
            "这条 Plan 修改意见已经提交，未重复发送。",
            metrics,
        )
        .await;
    }
    let prompt = format!(
        "Continue refining the current plan based on this feedback. Do not implement it yet.\n\n{feedback}"
    );
    let now = now_ms();
    let mut intent = PromptIntent {
        intent_id: format!("intent-{}", next_approval_nonce()),
        client_message_id: client_message_id.clone(),
        source: "telegram".into(),
        prompt: prompt.clone(),
        mode: "plan".into(),
        thread_id: Some(session.thread_id.clone()),
        space_id: Some(publication.space_id.clone()),
        generation: publication.generation,
        state: PromptIntentState::Received,
        turn_id: None,
        queue_id: None,
        error: None,
        created_at_ms: now,
        updated_at_ms: now,
    };
    store
        .upsert_prompt_intent(&intent)
        .map_err(|error| error.to_string())?;
    let surface = surface_for(bot, config, session.chat_id, session.root_message_id);
    let _ = send_text(
        bot,
        &surface,
        "📨 已收到 Plan 修改意见，正在提交给 Codex。",
        metrics,
    )
    .await;
    intent.state = PromptIntentState::Submitting;
    intent.updated_at_ms = now_ms();
    store
        .upsert_prompt_intent(&intent)
        .map_err(|error| error.to_string())?;
    let input = vec![PromptInput::text(&prompt).map_err(|error| error.to_string())?];
    let steered = session.turn_id.is_some();
    let result = if let Some(turn_id) = session.turn_id.as_ref() {
        agent
            .steer_turn(&session.thread_id, turn_id, input, Some(&client_message_id))
            .await
            .map(|id| AgentTurn {
                id,
                thread_id: session.thread_id.clone(),
                status: "inProgress".into(),
            })
    } else {
        agent
            .start_turn(&session.thread_id, input, Some(&client_message_id))
            .await
    };
    match result {
        Ok(turn) => {
            sessions.set_turn(turn.thread_id.as_str(), Some(turn.id.clone()));
            intent.turn_id = Some(turn.id.clone());
            intent.state = if steered {
                PromptIntentState::Steered
            } else {
                PromptIntentState::Started
            };
            intent.updated_at_ms = now_ms();
            store
                .upsert_prompt_intent(&intent)
                .map_err(|error| error.to_string())?;
            store
                .upsert_workflow_record(
                    "prompt",
                    &client_message_id,
                    &json!({
                        "intent_id": intent.intent_id,
                        "client_message_id": client_message_id,
                        "thread_id": turn.thread_id.as_str(),
                        "turn_id": turn.id.as_str(),
                        "chat_id": session.chat_id,
                        "message_id": message_id,
                        "state": if steered { "steered" } else { "started" },
                        "plan_revision": true,
                    }),
                    now_ms(),
                )
                .map_err(|error| error.to_string())?;
            publication.status = PlanPublicationState::RevisionStarted;
            publication.decision_turn_id = Some(turn.id.clone());
            publication.revision_prompt_message_id = None;
            update_plan_publication(
                store,
                &mut publication,
                PlanPublicationState::RevisionStarted,
                Some(turn.id),
            )?;
            let _ = edit_plan_publication(bot, &session, config, metrics, &publication).await;
            send_text(
                bot,
                &surface,
                if steered {
                    "📝 Plan 修改意见已注入当前 turn。"
                } else {
                    "📝 Plan 修改意见已提交，正在继续完善。"
                },
                metrics,
            )
            .await
        }
        Err(error) => {
            intent.state = PromptIntentState::Uncertain;
            intent.error = Some(error.to_string());
            intent.updated_at_ms = now_ms();
            store
                .upsert_prompt_intent(&intent)
                .map_err(|store_error| store_error.to_string())?;
            send_text(
                bot,
                &surface,
                "⚠️ Plan 修改意见送达状态待确认；请等待 Codex 状态更新后再重试。",
                metrics,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn submit_prompt_intent(
    store: &SqliteStore,
    agent: &AppServerClient,
    sessions: &SessionRegistry,
    bot: &RuntimeBot,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    session: SessionRecord,
    prompt: &str,
    mode: &str,
    message_id: i64,
) -> Result<(), String> {
    submit_prompt_intent_with_inputs(
        store, agent, sessions, bot, config, metrics, session, prompt, mode, message_id, None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn submit_prompt_intent_with_inputs(
    store: &SqliteStore,
    agent: &AppServerClient,
    sessions: &SessionRegistry,
    bot: &RuntimeBot,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    session: SessionRecord,
    prompt: &str,
    mode: &str,
    message_id: i64,
    custom_inputs: Option<Vec<PromptInput>>,
) -> Result<(), String> {
    let client_message_id = format!(
        "telegram-{}-{}-{}",
        session.chat_id,
        message_id,
        next_approval_nonce()
    );
    if store
        .prompt_intent_by_client_message_id(&client_message_id)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return send_text(
            bot,
            &surface_for(bot, config, session.chat_id, session.root_message_id),
            "该请求已经提交，未重复发送。",
            metrics,
        )
        .await;
    }
    let now = now_ms();
    let mut intent = PromptIntent {
        intent_id: format!("intent-{}", next_approval_nonce()),
        client_message_id: client_message_id.clone(),
        source: "telegram".into(),
        prompt: prompt.to_owned(),
        mode: mode.to_owned(),
        thread_id: Some(session.thread_id.clone()),
        space_id: Some(format!("telegram-{}", session.chat_id)),
        generation: agent.connection_state().generation,
        state: PromptIntentState::Received,
        turn_id: None,
        queue_id: None,
        error: None,
        created_at_ms: now,
        updated_at_ms: now,
    };
    store
        .upsert_prompt_intent(&intent)
        .map_err(|error| error.to_string())?;
    let surface = surface_for(bot, config, session.chat_id, session.root_message_id);
    let receipt = send_text(bot, &surface, "📨 已收到请求，正在提交给 Codex。", metrics).await;
    intent.state = PromptIntentState::Submitting;
    intent.updated_at_ms = now_ms();
    store
        .upsert_prompt_intent(&intent)
        .map_err(|error| error.to_string())?;
    let input = match custom_inputs {
        Some(inputs) => inputs,
        None => vec![
            PromptInput::text(if mode == "ask" {
                format!("请直接回答以下问题，并在回答中保留必要的上下文：\n\n{prompt}")
            } else {
                prompt.to_owned()
            })
            .map_err(|error| error.to_string())?,
        ],
    };
    let result = if let Some(turn_id) = session.turn_id.as_ref() {
        agent
            .steer_turn(&session.thread_id, turn_id, input, Some(&client_message_id))
            .await
            .map(|turn_id| AgentTurn {
                id: turn_id,
                thread_id: session.thread_id.clone(),
                status: "inProgress".into(),
            })
    } else if mode == "ask" {
        agent
            .start_turn_with_model(
                &session.thread_id,
                input,
                Some(&client_message_id),
                Some(config.ask_model.as_str()),
                Some(config.ask_reasoning_effort.as_str()),
            )
            .await
    } else {
        agent
            .start_turn(&session.thread_id, input, Some(&client_message_id))
            .await
    };
    match result {
        Ok(turn) => {
            let steered = session.turn_id.is_some();
            sessions.set_turn(turn.thread_id.as_str(), Some(turn.id.clone()));
            intent.turn_id = Some(turn.id.clone());
            intent.state = if steered {
                PromptIntentState::Steered
            } else {
                PromptIntentState::Started
            };
            intent.updated_at_ms = now_ms();
            store
                .upsert_prompt_intent(&intent)
                .map_err(|error| error.to_string())?;
            let record = json!({
                "intent_id": intent.intent_id,
                "client_message_id": client_message_id,
                "thread_id": turn.thread_id.as_str(),
                "turn_id": turn.id.as_str(),
                "chat_id": session.chat_id,
                "receipt_sent": receipt.is_ok(),
                "state": if steered { "steered" } else { "started" },
            });
            store
                .upsert_workflow_record("prompt", &intent.client_message_id, &record, now_ms())
                .map_err(|error| error.to_string())?;
            send_text(
                bot,
                &surface,
                if steered {
                    "↪️ 已注入当前 Codex turn。"
                } else {
                    "▶️ 已开始执行。"
                },
                metrics,
            )
            .await
        }
        Err(error) => {
            intent.state = PromptIntentState::Uncertain;
            intent.error = Some(error.to_string());
            intent.updated_at_ms = now_ms();
            store
                .upsert_prompt_intent(&intent)
                .map_err(|error| error.to_string())?;
            send_text(
                bot,
                &surface,
                "⚠️ 请求送达状态待确认；请等待 Codex 状态更新后再重试。",
                metrics,
            )
            .await
        }
    }
}

fn enqueue_prompt(
    store: &SqliteStore,
    session: &SessionRecord,
    prompt: &str,
    message_id: i64,
) -> Result<String, String> {
    let id = next_approval_nonce();
    let space = store
        .session_space_for_thread(session.thread_id.as_str())
        .map_err(|error| error.to_string())?;
    store
        .upsert_workflow_record(
            "queue",
            &id,
            &json!({
                "queue_id": id,
                "thread_id": session.thread_id.as_str(),
                "space_id": space.as_ref().map(|value| value.space_id.as_str()),
                "generation": space.as_ref().map_or(0, |value| value.generation),
                "chat_id": session.chat_id,
                "message_id": message_id,
                "prompt": prompt,
                "status": "queued",
                "created_at_ms": now_ms(),
            }),
            now_ms(),
        )
        .map_err(|error| error.to_string())?;
    Ok(id)
}

async fn render_queue(
    store: &SqliteStore,
    bot: &RuntimeBot,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    session: &SessionRecord,
) -> Result<(), String> {
    let records = store
        .workflow_records("queue")
        .map_err(|error| error.to_string())?;
    let session_space = store
        .session_space_for_thread(session.thread_id.as_str())
        .map_err(|error| error.to_string())?;
    let mut lines = vec!["📥 Queue".to_owned()];
    let mut rows = Vec::new();
    let mut visible = 0;
    for (key, value) in records {
        if value.get("thread_id").and_then(Value::as_str) != Some(session.thread_id.as_str())
            || value.get("status").and_then(Value::as_str) != Some("queued")
        {
            continue;
        }
        visible += 1;
        let prompt = value.get("prompt").and_then(Value::as_str).unwrap_or("-");
        lines.push(format!("{}. {}", visible, truncate_text(prompt)));
        let callback_nonce = format!("qc:{}", next_approval_nonce());
        if let Some((callback_space_id, callback_generation)) =
            queue_callback_scope(&value, session_space.as_ref())
        {
            store
                .create_callback(&StoredCallback {
                    nonce: callback_nonce.clone(),
                    space_id: callback_space_id,
                    generation: callback_generation,
                    action: key.clone(),
                    expires_at_ms: now_ms() + APPROVAL_CALLBACK_TTL_MS,
                })
                .map_err(|error| error.to_string())?;
            rows.push(vec![json!({"text": format!("取消 {visible}"), "callback_data": format!("qcancel:{callback_nonce}")})]);
        }
        if visible >= 20 {
            break;
        }
    }
    if visible == 0 {
        lines.push("队列为空。".into());
    }
    let markup = (!rows.is_empty()).then(|| json!({"inline_keyboard": rows}));
    send_text_with_markup(
        bot,
        &surface_for(bot, config, session.chat_id, session.root_message_id),
        &lines.join("\n"),
        markup,
        metrics,
    )
    .await
}

fn queue_callback_scope(
    value: &Value,
    session_space: Option<&RustSessionSpace>,
) -> Option<(String, i64)> {
    let space_id = value
        .get("space_id")
        .and_then(Value::as_str)
        .filter(|space_id| !space_id.trim().is_empty())
        .map(str::to_owned);
    match (space_id, session_space) {
        (Some(space_id), space) => Some((
            space_id,
            value
                .get("generation")
                .and_then(Value::as_i64)
                .or_else(|| space.map(|space| space.generation))
                .unwrap_or(0),
        )),
        (None, Some(space)) => Some((space.space_id.clone(), space.generation)),
        (None, None) => None,
    }
}

fn plan_text_from_value(value: &Value) -> Option<String> {
    if let Some(text) = value
        .as_str()
        .map(str::trim)
        .filter(|text| !text.is_empty())
    {
        return Some(text.to_owned());
    }
    for key in ["text", "content", "plan", "summary"] {
        if let Some(text) = value
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
        {
            return Some(text.to_owned());
        }
    }
    value.as_array().and_then(|items| {
        let lines = items
            .iter()
            .enumerate()
            .filter_map(|(index, item)| {
                let text = item
                    .get("step")
                    .or_else(|| item.get("title"))
                    .or_else(|| item.get("text"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|text| !text.is_empty())?;
                let status = item
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("pending");
                Some(format!("{}. [{}] {}", index + 1, status, text))
            })
            .collect::<Vec<_>>();
        (!lines.is_empty()).then(|| lines.join("\n"))
    })
}

fn plan_markup(
    store: &SqliteStore,
    space_id: &str,
    generation: u64,
    thread_id: &ThreadId,
    item_id: &str,
    revision_key: &str,
) -> Result<Value, String> {
    let mut buttons = Vec::new();
    for decision in ["execute", "revise"] {
        let nonce = format!("plan-{}", next_approval_nonce());
        let action = StoredPlanAction {
            space_id: space_id.to_owned(),
            generation,
            thread_id: thread_id.to_string(),
            item_id: item_id.to_owned(),
            revision_key: revision_key.to_owned(),
            decision: decision.to_owned(),
        };
        store
            .create_callback(&StoredCallback {
                nonce: nonce.clone(),
                space_id: space_id.to_owned(),
                generation: i64::try_from(generation)
                    .map_err(|_| "plan generation exceeds SQLite range".to_owned())?,
                action: serde_json::to_string(&action).map_err(|error| error.to_string())?,
                expires_at_ms: now_ms() + APPROVAL_CALLBACK_TTL_MS,
            })
            .map_err(|error| error.to_string())?;
        buttons.push(json!({
            "text": if decision == "execute" { "批准并执行" } else { "继续完善计划" },
            "callback_data": format!("rp:{nonce}:{decision}"),
        }));
    }
    Ok(json!({"inline_keyboard": [buttons]}))
}

fn plan_publication_key(
    space_id: &str,
    generation: u64,
    item_id: &str,
    revision_key: &str,
) -> String {
    format!("{space_id}:{generation}:{item_id}:{revision_key}")
}

#[allow(clippy::too_many_arguments)]
async fn publish_plan_message(
    agent: &AppServerClient,
    store: &SqliteStore,
    bot: &RuntimeBot,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    session: &SessionRecord,
    space: &RustSessionSpace,
    item_id: &str,
    turn_id: &TurnId,
    plan_text: &str,
) -> Result<(), String> {
    let generation = agent.connection_state().generation;
    let mut digest = Sha256::new();
    digest.update(turn_id.as_str().as_bytes());
    digest.update([0]);
    digest.update(plan_text.as_bytes());
    let revision_key = format!("{:x}", digest.finalize());
    let key = plan_publication_key(&space.space_id, generation, item_id, &revision_key);
    if store
        .workflow_record("plan", &key)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Ok(());
    }
    for (old_key, mut old) in store
        .workflow_records("plan")
        .map_err(|error| error.to_string())?
    {
        if old.get("space_id").and_then(Value::as_str) == Some(space.space_id.as_str())
            && old.get("generation").and_then(Value::as_u64) == Some(generation)
            && old.get("status").and_then(Value::as_str) == Some("published")
            && old_key != key
        {
            old["status"] = Value::String("superseded".into());
            store
                .upsert_workflow_record("plan", &old_key, &old, now_ms())
                .map_err(|error| error.to_string())?;
        }
    }
    let markup = plan_markup(
        store,
        &space.space_id,
        generation,
        &session.thread_id,
        item_id,
        &revision_key,
    )?;
    let body = format!(
        "📋 Codex Plan\n\n{}\n\n状态：等待 Telegram 选择。",
        truncate_text(plan_text)
    );
    let make_publication = |status, message_ids| PlanPublication {
        space_id: space.space_id.clone(),
        generation,
        item_id: item_id.to_owned(),
        revision_key: revision_key.clone(),
        thread_id: session.thread_id.clone(),
        turn_id: turn_id.clone(),
        status,
        plan_text: plan_text.to_owned(),
        message_ids,
        action_message_ids: Vec::new(),
        revision_prompt_message_id: None,
        decision_turn_id: None,
        updated_at_ms: now_ms(),
    };
    let message = match send_text_with_markup_message(
        bot,
        &surface_for(bot, config, session.chat_id, session.root_message_id),
        &body,
        Some(markup),
        metrics,
    )
    .await
    {
        Ok(message) => message,
        Err(error) => {
            let publication = make_publication(PlanPublicationState::Failed, Vec::new());
            let _ = store.upsert_plan_publication(&publication);
            let _ = store.upsert_workflow_record(
                "plan",
                &key,
                &serde_json::to_value(&publication).unwrap_or(Value::Null),
                now_ms(),
            );
            return Err(error);
        }
    };
    let publication = make_publication(PlanPublicationState::Published, vec![message.message_id]);
    store
        .upsert_plan_publication(&publication)
        .map_err(|error| error.to_string())?;
    store
        .upsert_workflow_record(
            "plan",
            &key,
            &serde_json::to_value(&publication).map_err(|error| error.to_string())?,
            now_ms(),
        )
        .map_err(|error| error.to_string())
}

async fn render_plan_command(
    agent: &AppServerClient,
    store: &SqliteStore,
    bot: &RuntimeBot,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    chat_id: i64,
    session: Option<SessionRecord>,
) -> Result<(), String> {
    let Some(session) = session else {
        return send_text(
            bot,
            &surface_for(bot, config, chat_id, None),
            "请先发送 /new 创建一个 Codex Session。",
            metrics,
        )
        .await;
    };
    let plan = extract_plan_from_thread(agent, &session.thread_id).await?;
    let Some((item_id, turn_id, plan_text)) = plan else {
        return send_text(
            bot,
            &surface_for(bot, config, session.chat_id, session.root_message_id),
            "🧭 Plan\n尚未创建计划。",
            metrics,
        )
        .await;
    };
    let Some(space) = store
        .session_space_for_thread(session.thread_id.as_str())
        .map_err(|error| error.to_string())?
    else {
        return send_text(
            bot,
            &surface_for(bot, config, session.chat_id, session.root_message_id),
            "当前 Session 缺少 durable Telegram space。",
            metrics,
        )
        .await;
    };
    publish_plan_message(
        agent, store, bot, config, metrics, &session, &space, &item_id, &turn_id, &plan_text,
    )
    .await
}

async fn extract_plan_from_thread(
    agent: &AppServerClient,
    thread_id: &ThreadId,
) -> Result<Option<(String, TurnId, String)>, String> {
    let response = agent
        .request(
            "thread/read",
            json!({"threadId": thread_id.as_str(), "includeTurns": true}),
            Duration::from_secs(30),
        )
        .await
        .map_err(|error| error.to_string())?;
    let turns = response
        .get("thread")
        .and_then(|thread| thread.get("turns"))
        .or_else(|| response.get("turns"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for turn in turns.iter().rev() {
        let Some(turn_id) = turn.get("id").and_then(Value::as_str) else {
            continue;
        };
        let Ok(turn_id) = TurnId::new(turn_id) else {
            continue;
        };
        let items = turn
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        for item in items.iter().rev() {
            let kind = item.get("type").and_then(Value::as_str).unwrap_or_default();
            if !matches!(
                kind,
                "plan" | "planUpdate" | "plan_updated" | "plan/updated"
            ) {
                continue;
            }
            let item_id = item
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or("plan")
                .to_owned();
            let text = item
                .get("text")
                .and_then(Value::as_str)
                .or_else(|| item.get("content").and_then(Value::as_str))
                .or_else(|| item.get("plan").and_then(Value::as_str))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            if let Some(text) = text {
                return Ok(Some((item_id, turn_id, text)));
            }
        }
    }
    Ok(None)
}

async fn render_timeline_command(
    agent: &AppServerClient,
    bot: &RuntimeBot,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    chat_id: i64,
    session: Option<SessionRecord>,
) -> Result<(), String> {
    let Some(session) = session else {
        return send_text(
            bot,
            &surface_for(bot, config, chat_id, None),
            "请先发送 /new 创建一个 Codex Session。",
            metrics,
        )
        .await;
    };
    let response = agent
        .request(
            "thread/read",
            json!({"threadId": session.thread_id.as_str(), "includeTurns": true}),
            Duration::from_secs(30),
        )
        .await
        .map_err(|error| error.to_string())?;
    let turns = response
        .get("thread")
        .and_then(|thread| thread.get("turns"))
        .or_else(|| response.get("turns"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut lines = vec![format!("🕒 Timeline · {}", session.thread_id)];
    if turns.is_empty() {
        lines.push("尚无事件。".into());
    }
    for turn in turns.iter().rev().take(20) {
        let id = turn.get("id").and_then(Value::as_str).unwrap_or("-");
        let status = turn
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let item_count = turn
            .get("items")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        lines.push(format!(
            "• {} · {} · items={}",
            short_id_prefix(id),
            status,
            item_count
        ));
    }
    send_text(
        bot,
        &surface_for(bot, config, session.chat_id, session.root_message_id),
        &lines.join("\n"),
        metrics,
    )
    .await
}

async fn handle_attach_command(
    agent: &AppServerClient,
    bot: &RuntimeBot,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    chat_id: i64,
    session: Option<SessionRecord>,
) -> Result<(), String> {
    let Some(session) = session else {
        return send_text(
            bot,
            &surface_for(bot, config, chat_id, None),
            "请先发送 /new 创建一个 Codex Session。",
            metrics,
        )
        .await;
    };
    agent
        .request(
            "thread/resume",
            json!({"threadId": session.thread_id.as_str()}),
            Duration::from_secs(30),
        )
        .await
        .map_err(|error| error.to_string())?;
    send_text(
        bot,
        &surface_for(bot, config, session.chat_id, session.root_message_id),
        &format!(
            "tmux 已就绪：{}；请在本机 Codex 客户端继续交互。",
            session.thread_id
        ),
        metrics,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn handle_answer_command(
    agent: &AppServerClient,
    store: &SqliteStore,
    bot: &RuntimeBot,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    chat_id: i64,
    raw: &str,
    session: Option<SessionRecord>,
) -> Result<(), String> {
    let Some(session) = session else {
        return send_text(
            bot,
            &surface_for(bot, config, chat_id, None),
            "请先发送 /new 创建一个 Codex Session。",
            metrics,
        )
        .await;
    };
    let (left, answer) = raw
        .split_once('|')
        .map(|(left, answer)| (left.trim(), answer.trim()))
        .unwrap_or(("", ""));
    let mut ids = left.split_whitespace();
    let request_key = ids.next().unwrap_or_default();
    let question_id = ids.next().unwrap_or_default();
    if request_key.is_empty() || question_id.is_empty() || answer.is_empty() {
        return send_text(
            bot,
            &surface_for(bot, config, session.chat_id, session.root_message_id),
            "用法：/answer <请求ID> <问题ID> | <回答>",
            metrics,
        )
        .await;
    }
    answer_question(
        agent,
        store,
        bot,
        config,
        metrics,
        &session,
        request_key,
        question_id,
        answer,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn answer_question(
    agent: &AppServerClient,
    store: &SqliteStore,
    bot: &RuntimeBot,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    session: &SessionRecord,
    request_key: &str,
    question_id: &str,
    answer: &str,
    expected_index: Option<usize>,
) -> Result<(), String> {
    let Some(mut question) = store
        .workflow_record("question", request_key)
        .map_err(|error| error.to_string())?
        .and_then(|value| serde_json::from_value::<StoredWorkflowQuestion>(value).ok())
    else {
        return send_text(
            bot,
            &surface_for(bot, config, session.chat_id, session.root_message_id),
            "该问题已过期或不属于当前 Session。",
            metrics,
        )
        .await;
    };
    if question.thread_id != session.thread_id.as_str() || question.status != "pending" {
        return send_text(
            bot,
            &surface_for(bot, config, session.chat_id, session.root_message_id),
            "该问题已过期或不属于当前 Session。",
            metrics,
        )
        .await;
    }
    if question.generation != agent.connection_state().generation {
        return send_text(
            bot,
            &surface_for(bot, config, session.chat_id, session.root_message_id),
            "Codex 连接已经重建，原问题已失效。",
            metrics,
        )
        .await;
    }
    let questions = question.questions.as_array().cloned().unwrap_or_default();
    let Some(question_index) = questions
        .iter()
        .enumerate()
        .find_map(|(index, value)| (question_id_at(value, index) == question_id).then_some(index))
    else {
        return send_text(
            bot,
            &surface_for(bot, config, session.chat_id, session.root_message_id),
            "问题 ID 不匹配。",
            metrics,
        )
        .await;
    };
    if expected_index.is_some_and(|expected| expected != question_index) {
        return send_text(
            bot,
            &surface_for(bot, config, session.chat_id, session.root_message_id),
            "问题按钮已过期，请使用当前题目的按钮。",
            metrics,
        )
        .await;
    }
    if question
        .expires_at_ms
        .is_some_and(|expires_at| expires_at < now_ms())
    {
        question.status = "expired".into();
        store
            .upsert_workflow_record(
                "question",
                request_key,
                &serde_json::to_value(&question).map_err(|error| error.to_string())?,
                now_ms(),
            )
            .map_err(|error| error.to_string())?;
        return send_text(
            bot,
            &surface_for(bot, config, session.chat_id, session.root_message_id),
            "该问题已过期，请回到本机 Codex 客户端继续。",
            metrics,
        )
        .await;
    }
    if answer.trim().is_empty() {
        return send_text(
            bot,
            &surface_for(bot, config, session.chat_id, session.root_message_id),
            "回答不能为空。",
            metrics,
        )
        .await;
    }
    question
        .answers
        .insert(question_id.to_owned(), vec![answer.to_owned()]);
    let known = questions
        .iter()
        .enumerate()
        .map(|(index, value)| question_id_at(value, index))
        .collect::<Vec<_>>();
    let complete = known
        .iter()
        .all(|value| question.answers.contains_key(value));
    if !complete {
        question.current_index = known
            .iter()
            .position(|value| !question.answers.contains_key(value))
            .unwrap_or(question.current_index);
        store
            .upsert_workflow_record(
                "question",
                request_key,
                &serde_json::to_value(&question).map_err(|error| error.to_string())?,
                now_ms(),
            )
            .map_err(|error| error.to_string())?;
        let space = store
            .session_space_for_thread(session.thread_id.as_str())
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "问题所属 Session 缺少 durable Telegram space".to_owned())?;
        render_user_question_prompt(
            agent, store, &question, session, &space, bot, config, metrics,
        )
        .await?;
        return Ok(());
    }
    let answers = question
        .answers
        .iter()
        .map(|(key, values)| (key.clone(), json!({"answers": values})))
        .collect::<serde_json::Map<_, _>>();
    let request_id = question.request_id.clone();
    if let Err(error) = agent.respond(request_id, json!({"answers": answers})).await {
        return send_text(
            bot,
            &surface_for(bot, config, session.chat_id, session.root_message_id),
            &format!("回答暂未送达 Codex：{error}"),
            metrics,
        )
        .await;
    }
    store
        .resolve_question(request_key, &json!({"answers": answers}), now_ms())
        .map_err(|error| error.to_string())?;
    question.status = "resolved".into();
    store
        .upsert_workflow_record(
            "question",
            request_key,
            &serde_json::to_value(&question).map_err(|error| error.to_string())?,
            now_ms(),
        )
        .map_err(|error| error.to_string())?;
    cleanup_question_messages(bot, config, metrics, &question, session).await?;
    send_text(
        bot,
        &surface_for(bot, config, session.chat_id, session.root_message_id),
        "✅ 已将回答提交给 Codex。",
        metrics,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
fn debounce_status_update(
    tasks: &mut HashMap<String, tokio::task::JoinHandle<()>>,
    store: &Arc<SqliteStore>,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    totp: &Arc<TotpManager>,
    space: &RustSessionSpace,
    projection: &ThreadProjection,
) {
    if let Some(previous) = tasks.remove(&space.space_id) {
        previous.abort();
    }
    let store = Arc::clone(store);
    let bots_by_id = bots_by_id.clone();
    let config = config.clone();
    let metrics = metrics.clone();
    let totp = Arc::clone(totp);
    let space = space.clone();
    let projection = projection.clone();
    let space_id = space.space_id.clone();
    let task_space_id = space_id.clone();
    let task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(
            u64::try_from(DASHBOARD_DEBOUNCE_MS.max(0)).unwrap_or(500),
        ))
        .await;
        let Some(current_space) = store.get_session_space(&space_id).ok().flatten() else {
            return;
        };
        if current_space.lifecycle == "closed" || current_space.generation != space.generation {
            return;
        }
        let current_projection = current_space.thread_id.as_deref().and_then(|thread_id| {
            store
                .thread_projections()
                .ok()
                .and_then(|rows| {
                    rows.into_iter()
                        .find(|(candidate, _, _, _)| candidate == thread_id)
                })
                .and_then(|(_, _, payload, _)| {
                    serde_json::from_value::<ThreadProjection>(payload).ok()
                })
        });
        if let Err(error) = update_status_message(
            &store,
            &bots_by_id,
            &config,
            &metrics,
            totp.as_ref(),
            &current_space,
            current_projection.as_ref().or(Some(&projection)),
            None,
            false,
            None,
        )
        .await
        {
            eprintln!("rust bridge debounced status update failed: {error}");
        }
    });
    tasks.insert(task_space_id, task);
}

async fn run_status_heartbeat_worker(
    store: Arc<SqliteStore>,
    bots_by_id: HashMap<String, RuntimeBot>,
    config: RustConfig,
    metrics: MetricsRegistry,
    totp: Arc<TotpManager>,
) {
    let mut animation_frames: HashMap<String, u64> = HashMap::new();
    loop {
        tokio::time::sleep(Duration::from_secs(
            u64::try_from(HEARTBEAT_SECONDS.max(1)).unwrap_or(60),
        ))
        .await;
        let projections = store
            .thread_projections()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|(thread_id, _, payload, _)| {
                serde_json::from_value::<ThreadProjection>(payload)
                    .ok()
                    .map(|projection| (thread_id, projection))
            })
            .collect::<HashMap<_, _>>();
        for space in store.active_session_spaces().unwrap_or_default() {
            let projection = space
                .thread_id
                .as_deref()
                .and_then(|thread_id| projections.get(thread_id));
            // Python space_dashboard advances the moon-phase animation once
            // per heartbeat while a Session is active; idle Sessions keep
            // their current phase and terminal Sessions pin the full moon
            // inside the renderer.
            let animation_frame = {
                let frame = animation_frames.get(&space.space_id).copied().unwrap_or(0);
                if status_is_animated(&space, projection) {
                    animation_frames.insert(space.space_id.clone(), frame.wrapping_add(1));
                }
                Some(frame)
            };
            if let Err(error) = update_status_message_with_edit_timeout(
                &store,
                &bots_by_id,
                &config,
                &metrics,
                totp.as_ref(),
                &space,
                projection,
                None,
                false,
                animation_frame,
                Some(PERF_EDIT_TIMEOUT),
            )
            .await
            {
                eprintln!("rust bridge status heartbeat failed: {error}");
            }
        }
    }
}

const HYDRATION_MAX_ATTEMPTS: u32 = 3;

/// Startup hydration for threads that were created before the projection
/// pipeline existed, mirroring the Python `Bridge.resync()` contract:
/// `thread/resume` → `thread/read` → rebuild + persist the projection →
/// `thread/goal/get` backfill. Failures are retried with exponential backoff
/// and never abort the remaining threads or the daemon startup path.
async fn hydrate_thread_projections(
    agent: &AppServerClient,
    store: &Arc<SqliteStore>,
    projector: &mut EventProjector,
) -> Vec<(RustSessionSpace, ThreadProjection)> {
    let mut hydrated = Vec::new();
    for space in store.active_session_spaces().unwrap_or_default() {
        let Some(thread_id) = space.thread_id.as_deref() else {
            continue;
        };
        let Ok(thread_id) = ThreadId::new(thread_id) else {
            continue;
        };
        let Some(projection) = hydrate_thread_with_retry(agent, &thread_id).await else {
            continue;
        };
        let mut space = space;
        if backfill_space_profile(&mut space, &projection) {
            space.updated_at_ms = now_ms();
            if let Err(error) = store.upsert_session_space(&space) {
                eprintln!(
                    "rust bridge hydration profile backfill failed for {}: {error}",
                    space.space_id
                );
            }
        }
        if let Ok(payload) = serde_json::to_value(&projection) {
            let _ = store.upsert_thread_projection(
                thread_id.as_str(),
                i64::try_from(projection.generation).unwrap_or(i64::MAX),
                &payload,
                now_ms(),
            );
        }
        projector.restore(projection.clone());
        hydrated.push((space, projection));
    }
    hydrated
}

async fn hydrate_thread_with_retry(
    agent: &AppServerClient,
    thread_id: &ThreadId,
) -> Option<ThreadProjection> {
    let mut delay = Duration::from_millis(500);
    for attempt in 1..=HYDRATION_MAX_ATTEMPTS {
        match hydrate_thread_once(agent, thread_id).await {
            Ok(projection) => return Some(projection),
            Err(error) => {
                if attempt == HYDRATION_MAX_ATTEMPTS {
                    eprintln!(
                        "rust bridge hydration failed for {} after {HYDRATION_MAX_ATTEMPTS} attempts: {error}",
                        thread_id.as_str()
                    );
                } else {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(8));
                }
            }
        }
    }
    None
}

async fn hydrate_thread_once(
    agent: &AppServerClient,
    thread_id: &ThreadId,
) -> Result<ThreadProjection, String> {
    agent
        .resume_thread(thread_id)
        .await
        .map_err(|error| error.to_string())?;
    let response = agent
        .request(
            "thread/read",
            json!({"threadId": thread_id.as_str(), "includeTurns": true}),
            Duration::from_secs(30),
        )
        .await
        .map_err(|error| error.to_string())?;
    let mut projection = projection_from_thread_read(thread_id.as_str(), &response);
    // Goal is not part of thread/read; Python resync pulls it separately.
    if let Ok(goal_response) = agent
        .request(
            "thread/goal/get",
            json!({"threadId": thread_id.as_str()}),
            Duration::from_secs(30),
        )
        .await
        && let Some(goal) = goal_response.get("goal").filter(|value| value.is_object())
    {
        projection.goal = Some(goal.clone());
    }
    if projection.updated_at_ms <= 0 {
        projection.updated_at_ms = now_ms();
    }
    Ok(projection)
}

/// Mirrors the Python `_backfill_space_profiles`: a hydrated thread's model
/// profile fills the space-level profile slots that are still empty.
fn backfill_space_profile(space: &mut RustSessionSpace, projection: &ThreadProjection) -> bool {
    if space.normal_model.is_some() && space.normal_effort.is_some() {
        return false;
    }
    let (Some(model), Some(effort)) = (projection.model.as_deref(), projection.effort.as_deref())
    else {
        return false;
    };
    let mut changed = false;
    if space.normal_model.is_none() {
        space.normal_model = Some(model.to_owned());
        changed = true;
    }
    if space.normal_effort.is_none() {
        space.normal_effort = Some(effort.to_owned());
        changed = true;
    }
    if space.plan_model.is_none() {
        space.plan_model = Some(model.to_owned());
        changed = true;
    }
    if space.plan_effort.is_none() {
        space.plan_effort = Some(effort.to_owned());
        changed = true;
    }
    changed
}

/// Persists `thread/settings/updated` into the SessionSpace, mirroring the
/// Python projector's `_sync_space_settings`: the observed TUI mode and the
/// per-mode model profile are durable, so a restart (or a legacy Session
/// without projection data) still renders the mode header.
fn sync_space_settings_from_event(store: &SqliteStore, thread_id: &str, params: &Value) {
    let Some(thread_settings) = params
        .get("threadSettings")
        .or_else(|| params.get("settings"))
    else {
        return;
    };
    let Some(collaboration) = thread_settings
        .get("collaborationMode")
        .or_else(|| thread_settings.get("collaboration_mode"))
    else {
        return;
    };
    let mode = collaboration
        .get("mode")
        .and_then(Value::as_str)
        .or_else(|| collaboration.as_str());
    let Some(mode) = mode.filter(|mode| matches!(*mode, "default" | "plan")) else {
        return;
    };
    let settings = collaboration.get("settings");
    let model = settings
        .and_then(|settings| settings.get("model"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let effort = settings
        .and_then(|settings| {
            settings
                .get("reasoning_effort")
                .or_else(|| settings.get("reasoningEffort"))
        })
        .or_else(|| thread_settings.get("effort"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    for space in store.session_spaces().unwrap_or_default() {
        if space.thread_id.as_deref() != Some(thread_id) || space.lifecycle == "closed" {
            continue;
        }
        let mut space = space;
        space.observed_mode = Some(mode.to_owned());
        if let (Some(model), Some(effort)) = (model, effort) {
            if mode == "plan" {
                space.plan_model = Some(model.to_owned());
                space.plan_effort = Some(effort.to_owned());
            } else {
                space.normal_model = Some(model.to_owned());
                space.normal_effort = Some(effort.to_owned());
            }
        }
        space.updated_at_ms = now_ms();
        if let Err(error) = store.upsert_session_space(&space) {
            eprintln!(
                "rust bridge space settings sync failed for {}: {error}",
                space.space_id
            );
        }
    }
}

const LEGACY_MODEL_REMAP_TARGET: &str = "gpt-5.6-terra";
const LEGACY_MODEL_REMAP_EFFORT: &str = "low";

/// Retired models (notably `gpt-5.6-luna`, but anything missing from the
/// latest `model/list`) are remapped to the current default profile so stored
/// SessionSpaces never point at a model Codex can no longer start.
fn remap_legacy_session_models(space: &mut RustSessionSpace, available: &[ModelChoice]) -> bool {
    let mut changed = false;
    for (model, effort) in [
        (&mut space.normal_model, &mut space.normal_effort),
        (&mut space.plan_model, &mut space.plan_effort),
    ] {
        let Some(stored) = model.as_deref() else {
            continue;
        };
        let known = available.iter().any(|entry| entry.model == stored);
        if stored == "gpt-5.6-luna" || !known {
            *model = Some(LEGACY_MODEL_REMAP_TARGET.to_owned());
            *effort = Some(LEGACY_MODEL_REMAP_EFFORT.to_owned());
            changed = true;
        }
    }
    if changed {
        space.updated_at_ms = now_ms();
    }
    changed
}

/// Disk persistence of projections is rate-limited during streaming turns;
/// terminal transitions always flush immediately. The in-memory projector is
/// the live render source, so status rendering never waits on SQLite.
const PROJECTION_PERSIST_INTERVAL_MS: i64 = 2_000;
/// Terminal threads older than this are evicted from the in-memory projector
/// (the durable row stays and is lazily reloaded on the next event).
const PROJECTION_TERMINAL_RETENTION_MS: i64 = 60 * 60 * 1000;
const PROJECTION_EVICTION_SWEEP_SECONDS: u64 = 60;

#[allow(clippy::too_many_arguments)]
async fn forward_codex_events(
    agent: AppServerClient,
    store: Arc<SqliteStore>,
    sessions: Arc<SessionRegistry>,
    bots_by_id: HashMap<String, RuntimeBot>,
    config: RustConfig,
    metrics: MetricsRegistry,
    totp: Arc<TotpManager>,
    control_runtime: Arc<ControlRuntime>,
) {
    let mut events = agent.subscribe_events();
    let mut projector = EventProjector::default();
    let mut status_tasks: HashMap<String, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut last_persisted_ms: HashMap<String, i64> = HashMap::new();
    let mut last_eviction = Instant::now();
    if let Ok(rows) = store.thread_projections() {
        for (_, _, payload, _) in rows {
            if let Ok(projection) = serde_json::from_value::<ThreadProjection>(payload) {
                projector.restore(projection);
            }
        }
    }
    // Hydrate legacy sessions before live events accumulate: the projector
    // holds the only in-memory copy, so hydration must finish first to keep
    // sparse live updates from clobbering the rebuilt projections.
    if let Ok(models) = list_model_choices(&agent).await {
        for mut space in store.active_session_spaces().unwrap_or_default() {
            if remap_legacy_session_models(&mut space, &models.entries)
                && let Err(error) = store.upsert_session_space(&space)
            {
                eprintln!(
                    "rust bridge legacy model remap failed for {}: {error}",
                    space.space_id
                );
            }
        }
    }
    for (space, projection) in hydrate_thread_projections(&agent, &store, &mut projector).await {
        if let Err(error) = update_status_message(
            &store,
            &bots_by_id,
            &config,
            &metrics,
            totp.as_ref(),
            &space,
            Some(&projection),
            None,
            false,
            None,
        )
        .await
        {
            eprintln!("rust bridge hydration status refresh failed: {error}");
        }
    }
    loop {
        let Some(event) = events.recv().await else {
            return;
        };
        if event.method.ends_with("/delta") {
            continue;
        }
        // Lazily reload an evicted terminal projection from the durable row
        // so a late event lands on the full history instead of an empty shell.
        if let Some(thread_id) = event_thread_id(&event.params)
            && projector.projection(thread_id).is_none()
            && let Ok(Some((_, _, payload, _))) = store.thread_projection(thread_id)
            && let Ok(projection) = serde_json::from_value::<ThreadProjection>(payload)
        {
            projector.restore(projection);
        }
        let effect = projector.apply(&event);
        // Lifecycle and turn transitions change `/sessions` rows; item-level
        // updates do not, so the cached listing is only invalidated here.
        if matches!(
            event.method.as_str(),
            "thread/started"
                | "thread/created"
                | "thread/updated"
                | "thread/status/updated"
                | "thread/status/changed"
                | "turn/started"
                | "turn/created"
                | "turn/completed"
                | "turn/failed"
                | "turn/interrupted"
        ) {
            control_runtime
                .sessions_dirty
                .store(true, Ordering::Release);
        }
        if event.method == "thread/settings/updated"
            && let Some(thread_id) = event_thread_id(&event.params)
        {
            sync_space_settings_from_event(&store, thread_id, &event.params);
        }
        if let Some(thread_id) = event_thread_id(&event.params)
            && let Some(projection) = projector.projection_mut(thread_id)
        {
            let updated_at_ms = now_ms();
            projection.updated_at_ms = updated_at_ms;
            if projection.turn_status.as_deref() == Some("inProgress")
                && projection.started_at_ms.is_none()
            {
                projection.started_at_ms = Some(updated_at_ms);
            }
            let persist_due = last_persisted_ms.get(thread_id).is_none_or(|last| {
                updated_at_ms.saturating_sub(*last) >= PROJECTION_PERSIST_INTERVAL_MS
            });
            let terminal = matches!(
                effect,
                ProjectionEffect::TurnCompleted | ProjectionEffect::Error
            );
            if (persist_due || terminal)
                && let Ok(payload) = serde_json::to_value(&*projection)
                && store
                    .upsert_thread_projection(
                        thread_id,
                        i64::try_from(projection.generation).unwrap_or(i64::MAX),
                        &payload,
                        updated_at_ms,
                    )
                    .is_ok()
            {
                last_persisted_ms.insert(thread_id.to_owned(), updated_at_ms);
            }
        }
        if last_eviction.elapsed() >= Duration::from_secs(PROJECTION_EVICTION_SWEEP_SECONDS) {
            last_eviction = Instant::now();
            let evicted = projector
                .evict_finished_before(now_ms().saturating_sub(PROJECTION_TERMINAL_RETENTION_MS));
            if evicted > 0 {
                eprintln!("rust bridge evicted {evicted} terminal thread projections from memory");
            }
        }
        if effect == ProjectionEffect::None {
            continue;
        }
        let Some(thread_id) = event_thread_id(&event.params) else {
            continue;
        };
        let Some(session) = sessions.by_thread(thread_id) else {
            continue;
        };
        if effect == ProjectionEffect::TurnCompleted {
            let turn = event.params.get("turn").cloned().unwrap_or(Value::Null);
            let turn_id = turn.get("id").and_then(Value::as_str).unwrap_or_default();
            let turn_error = extract_turn_error(&turn);
            let terminal_state = match turn
                .get("status")
                .and_then(Value::as_str)
                .or_else(|| Some(event.method.trim_start_matches("turn/")))
            {
                Some("interrupted") => "interrupted",
                Some("failed") => "failed",
                _ => "completed",
            };
            sessions.set_turn(thread_id, None);
            if let Some(space) = store.session_space_for_thread(thread_id).ok().flatten()
                && let Some(task) = status_tasks.remove(&space.space_id)
            {
                task.abort();
            }
            let finalized_plans = mark_turn_workflows(
                &store,
                thread_id,
                turn_id,
                terminal_state,
                turn_error.as_deref(),
            );
            let answer = match terminal_state {
                "interrupted" => "Codex turn 已中断。".into(),
                "failed" => extract_turn_error(&turn)
                    .map(|message| format!("Codex turn 失败：{message}"))
                    .unwrap_or_else(|| "Codex turn 失败。".into()),
                _ => extract_final_answer(&turn)
                    .or_else(|| extract_review_answer(&turn))
                    .unwrap_or_else(|| "Codex turn 已完成。".into()),
            };
            let Some(bot) = bots_by_id.get(&session.sender_instance_id) else {
                continue;
            };
            for publication in finalized_plans {
                let _ = edit_plan_publication(bot, &session, &config, &metrics, &publication).await;
            }
            let _ = send_text(
                bot,
                &surface_for(bot, &config, session.chat_id, session.root_message_id),
                &format!("{answer}\n\nturn={turn_id}"),
                &metrics,
            )
            .await;
            if let Some(projection) = projector.projection(thread_id)
                && let Some(space) = store.session_space_for_thread(thread_id).ok().flatten()
                && let Err(error) = update_status_message(
                    &store,
                    &bots_by_id,
                    &config,
                    &metrics,
                    totp.as_ref(),
                    &space,
                    Some(projection),
                    Some(&answer),
                    false,
                    None,
                )
                .await
            {
                eprintln!("rust bridge terminal status update failed: {error}");
            }
            if let Some(dispatch_session) = sessions.by_thread(thread_id) {
                let _ = dispatch_next_queued(
                    &store,
                    &agent,
                    &sessions,
                    &dispatch_session,
                    &bots_by_id,
                    &config,
                    &metrics,
                )
                .await;
            }
        } else if effect == ProjectionEffect::Error {
            let message = event
                .params
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("Codex turn failed");
            let turn_id = projector
                .projection(thread_id)
                .and_then(|projection| projection.turn_id.as_deref())
                .or_else(|| session.turn_id.as_ref().map(TurnId::as_str))
                .unwrap_or_default();
            sessions.set_turn(thread_id, None);
            if let Some(space) = store.session_space_for_thread(thread_id).ok().flatten()
                && let Some(task) = status_tasks.remove(&space.space_id)
            {
                task.abort();
            }
            let finalized_plans =
                mark_turn_workflows(&store, thread_id, turn_id, "failed", Some(message));
            if let Some(bot) = bots_by_id.get(&session.sender_instance_id) {
                for publication in finalized_plans {
                    let _ =
                        edit_plan_publication(bot, &session, &config, &metrics, &publication).await;
                }
                let _ = send_text(
                    bot,
                    &surface_for(bot, &config, session.chat_id, session.root_message_id),
                    &format!("Codex 错误：{message}"),
                    &metrics,
                )
                .await;
            }
            if let Some(projection) = projector.projection(thread_id)
                && let Some(space) = store.session_space_for_thread(thread_id).ok().flatten()
            {
                let _ = update_status_message(
                    &store,
                    &bots_by_id,
                    &config,
                    &metrics,
                    totp.as_ref(),
                    &space,
                    Some(projection),
                    Some(message),
                    false,
                    None,
                )
                .await;
            }
            if let Some(dispatch_session) = sessions.by_thread(thread_id) {
                let _ = dispatch_next_queued(
                    &store,
                    &agent,
                    &sessions,
                    &dispatch_session,
                    &bots_by_id,
                    &config,
                    &metrics,
                )
                .await;
            }
        } else if let Some(projection) = projector.projection(thread_id) {
            let Some(bot) = bots_by_id.get(&session.sender_instance_id) else {
                continue;
            };
            let Some(space) = store.session_space_for_thread(thread_id).ok().flatten() else {
                continue;
            };
            if matches!(
                event.method.as_str(),
                "thread/plan/updated" | "plan/updated" | "plan/published"
            ) {
                let plan_value = event
                    .params
                    .get("plan")
                    .or_else(|| event.params.get("item"))
                    .unwrap_or(&event.params);
                if let (Some(plan_text), Some(turn_id)) = (
                    plan_text_from_value(plan_value),
                    event
                        .params
                        .get("turnId")
                        .and_then(Value::as_str)
                        .or(projection.turn_id.as_deref())
                        .and_then(|value| TurnId::new(value).ok()),
                ) {
                    let item_id = event
                        .params
                        .get("itemId")
                        .and_then(Value::as_str)
                        .or_else(|| plan_value.get("id").and_then(Value::as_str))
                        .unwrap_or("plan");
                    if let Err(error) = publish_plan_message(
                        &agent, &store, bot, &config, &metrics, &session, &space, item_id,
                        &turn_id, &plan_text,
                    )
                    .await
                    {
                        eprintln!("rust bridge plan publication failed: {error}");
                    }
                }
            }
            debounce_status_update(
                &mut status_tasks,
                &store,
                &bots_by_id,
                &config,
                &metrics,
                &totp,
                &space,
                projection,
            );
            let _ = bot;
        }
    }
}

fn mark_turn_workflows(
    store: &SqliteStore,
    thread_id: &str,
    turn_id: &str,
    state: &str,
    error: Option<&str>,
) -> Vec<PlanPublication> {
    let mut finalized_plans = Vec::new();
    if let Ok(records) = store.workflow_records("prompt") {
        for (key, mut value) in records {
            let matches_turn = value.get("thread_id").and_then(Value::as_str) == Some(thread_id)
                && (turn_id.is_empty()
                    || value.get("turn_id").and_then(Value::as_str) == Some(turn_id));
            if !matches_turn {
                continue;
            }
            value["state"] = Value::String(state.to_owned());
            if let Some(error) = error {
                value["error"] = Value::String(truncate_text(error));
            }
            let _ = store.upsert_workflow_record("prompt", &key, &value, now_ms());
            if let Some(client_message_id) = value.get("client_message_id").and_then(Value::as_str)
                && let Ok(Some(mut intent)) =
                    store.prompt_intent_by_client_message_id(client_message_id)
            {
                intent.state = match state {
                    "completed" => PromptIntentState::Completed,
                    "failed" => PromptIntentState::Failed,
                    "interrupted" => PromptIntentState::Cancelled,
                    _ => intent.state,
                };
                intent.error = error.map(str::to_owned);
                intent.updated_at_ms = now_ms();
                let _ = store.upsert_prompt_intent(&intent);
            }
        }
    }
    if let Ok(records) = store.workflow_records("queue") {
        for (key, mut value) in records {
            if value.get("thread_id").and_then(Value::as_str) != Some(thread_id)
                || !matches!(
                    value.get("status").and_then(Value::as_str),
                    Some("submitting" | "started")
                )
            {
                continue;
            }
            let entry_turn_matches = value
                .get("turn_id")
                .and_then(Value::as_str)
                .is_some_and(|value| value == turn_id);
            if !(entry_turn_matches
                || turn_id.is_empty()
                || value.get("turn_id").and_then(Value::as_str).is_some())
            {
                continue;
            }
            value["status"] = Value::String(
                match state {
                    "completed" => "completed",
                    "interrupted" => "cancelled",
                    _ => "failed",
                }
                .into(),
            );
            value["finished_at_ms"] = Value::from(now_ms());
            if let Some(error) = error {
                value["error"] = Value::String(truncate_text(error));
            }
            let _ = store.upsert_workflow_record("queue", &key, &value, now_ms());
            if let Some(client_message_id) = value.get("client_message_id").and_then(Value::as_str)
                && let Ok(Some(mut intent)) =
                    store.prompt_intent_by_client_message_id(client_message_id)
            {
                intent.state = match state {
                    "completed" => PromptIntentState::Completed,
                    "interrupted" => PromptIntentState::Cancelled,
                    _ => PromptIntentState::Failed,
                };
                intent.turn_id = TurnId::new(turn_id).ok();
                intent.error = error.map(str::to_owned);
                intent.updated_at_ms = now_ms();
                let _ = store.upsert_prompt_intent(&intent);
            }
        }
    }
    if let Ok(records) = store.workflow_records("plan") {
        for (key, value) in records {
            if value.get("thread_id").and_then(Value::as_str) != Some(thread_id) {
                continue;
            }
            let current_status = value
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let terminal_status = match (state, current_status) {
                ("completed", "executing") => Some(PlanPublicationState::Executed),
                ("failed" | "interrupted", "executing" | "revising") => {
                    Some(PlanPublicationState::Failed)
                }
                _ => None,
            };
            if let Some(status) = terminal_status
                && let Ok(mut publication) = serde_json::from_value::<PlanPublication>(value)
            {
                publication.status = status;
                if publication.decision_turn_id.is_none() {
                    publication.decision_turn_id = TurnId::new(turn_id).ok();
                }
                publication.updated_at_ms = now_ms();
                if store.upsert_plan_publication(&publication).is_ok()
                    && store
                        .upsert_workflow_record(
                            "plan",
                            &key,
                            &serde_json::to_value(&publication).unwrap_or(Value::Null),
                            now_ms(),
                        )
                        .is_ok()
                {
                    finalized_plans.push(publication);
                }
            }
        }
    }
    finalized_plans
}

async fn dispatch_next_queued(
    store: &SqliteStore,
    agent: &AppServerClient,
    sessions: &SessionRegistry,
    session: &SessionRecord,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    if session.turn_id.is_some() {
        return Ok(());
    }
    let Some((key, mut entry)) = store
        .workflow_records("queue")
        .map_err(|error| error.to_string())?
        .into_iter()
        .find(|(_, value)| {
            value.get("thread_id").and_then(Value::as_str) == Some(session.thread_id.as_str())
                && value.get("status").and_then(Value::as_str) == Some("queued")
        })
    else {
        return Ok(());
    };
    let prompt = entry
        .get("prompt")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let client_message_id = format!("queue-{}", key);
    entry["status"] = Value::String("submitting".into());
    store
        .upsert_workflow_record("queue", &key, &entry, now_ms())
        .map_err(|error| error.to_string())?;
    let input = vec![PromptInput::text(prompt).map_err(|error| error.to_string())?];
    let result = agent
        .start_turn(&session.thread_id, input, Some(&client_message_id))
        .await;
    let Some(bot) = bots_by_id.get(&session.sender_instance_id) else {
        return Ok(());
    };
    match result {
        Ok(turn) => {
            sessions.set_turn(session.thread_id.as_str(), Some(turn.id.clone()));
            entry["status"] = Value::String("started".into());
            entry["turn_id"] = Value::String(turn.id.to_string());
            store
                .upsert_workflow_record("queue", &key, &entry, now_ms())
                .map_err(|error| error.to_string())?;
            send_text(
                bot,
                &surface_for(bot, config, session.chat_id, session.root_message_id),
                "▶️ 已从队列提交下一条请求。",
                metrics,
            )
            .await
        }
        Err(error) => {
            entry["status"] = Value::String("uncertain".into());
            entry["error"] = Value::String(error.to_string());
            store
                .upsert_workflow_record("queue", &key, &entry, now_ms())
                .map_err(|error| error.to_string())?;
            send_text(
                bot,
                &surface_for(bot, config, session.chat_id, session.root_message_id),
                "⚠️ 队列请求送达状态待确认；请使用 /queue 检查。",
                metrics,
            )
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_callback(
    callback: codex_telegram_adapter::TelegramCallback,
    inbound_bot: RuntimeBot,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    store: &Arc<SqliteStore>,
    agent: &AppServerClient,
    sessions: &Arc<SessionRegistry>,
    metrics: &MetricsRegistry,
    totp: &Arc<TotpManager>,
    control_runtime: &Arc<ControlRuntime>,
) -> Result<(), String> {
    let root_message_id = sessions
        .by_chat(callback.chat_id)
        .and_then(|session| session.root_message_id);
    let inbound_surface = surface_for(&inbound_bot, config, callback.chat_id, root_message_id);
    if callback.data.starts_with("ctl:") {
        return handle_control_callback(
            callback,
            inbound_bot,
            bots_by_id,
            config,
            store,
            agent,
            sessions,
            metrics,
            totp,
            control_runtime,
        )
        .await;
    }
    if callback.data.starts_with("new:") {
        return handle_new_callback(
            callback,
            inbound_bot,
            bots_by_id,
            config,
            store,
            agent,
            sessions,
            metrics,
        )
        .await;
    }
    if let Some(nonce) = callback.data.strip_prefix("cb:").map(str::to_owned) {
        return handle_status_callback(
            &nonce,
            callback,
            inbound_bot,
            bots_by_id,
            config,
            store,
            agent,
            sessions,
            metrics,
            totp.as_ref(),
        )
        .await;
    }
    if let Some(nonce) = callback.data.strip_prefix("qcancel:") {
        if !workflow_callback_bot_allowed(&inbound_bot)
            || !workflow_callback_owner_authorized(store, &callback)?
        {
            acknowledge_callback(&inbound_bot, &callback, Some("无权操作此队列按钮")).await;
            return Ok(());
        }
        acknowledge_callback(&inbound_bot, &callback, Some("正在取消")).await;
        let Some(preview) = store
            .peek_callback(nonce, now_ms())
            .map_err(|error| error.to_string())?
        else {
            return send_text(
                &inbound_bot,
                &inbound_surface,
                "队列操作已过期或已处理。",
                metrics,
            )
            .await;
        };
        if preview.space_id.is_empty() {
            return send_text(&inbound_bot, &inbound_surface, "队列项无效。", metrics).await;
        }
        let key = preview.action;
        let Some(mut entry) = store
            .workflow_record("queue", &key)
            .map_err(|error| error.to_string())?
        else {
            return send_text(&inbound_bot, &inbound_surface, "队列项不存在。", metrics).await;
        };
        if entry.get("status").and_then(Value::as_str) != Some("queued") {
            return send_text(&inbound_bot, &inbound_surface, "队列项已经处理。", metrics).await;
        }
        if entry.get("chat_id").and_then(Value::as_i64) != Some(callback.chat_id)
            || entry
                .get("space_id")
                .and_then(Value::as_str)
                .is_some_and(|space_id| space_id != preview.space_id)
        {
            acknowledge_callback(&inbound_bot, &callback, Some("队列按钮不属于当前会话")).await;
            return Ok(());
        }
        let Some(_stored) = store
            .take_callback_scoped(
                nonce,
                now_ms(),
                Some(&preview.space_id),
                Some(preview.generation),
            )
            .map_err(|error| error.to_string())?
        else {
            return send_text(
                &inbound_bot,
                &inbound_surface,
                "队列操作已过期或已处理。",
                metrics,
            )
            .await;
        };
        entry["status"] = Value::String("cancelled".into());
        store
            .upsert_workflow_record("queue", &key, &entry, now_ms())
            .map_err(|error| error.to_string())?;
        return send_text(&inbound_bot, &inbound_surface, "已取消队列项。", metrics).await;
    }
    if let Some((nonce, decision)) = parse_plan_callback(&callback.data)
        .map(|(nonce, decision)| (nonce.to_owned(), decision.to_owned()))
    {
        return handle_plan_callback(
            &nonce,
            &decision,
            callback,
            inbound_bot,
            config,
            store,
            agent,
            sessions,
            metrics,
        )
        .await;
    }
    if let Some(nonce) = callback.data.strip_prefix("rq:").map(str::to_owned) {
        return handle_question_callback(
            &nonce,
            callback,
            inbound_bot,
            config,
            store,
            agent,
            sessions,
            metrics,
        )
        .await;
    }
    let Some((nonce, _)) = parse_approval_callback(&callback.data) else {
        acknowledge_callback(&inbound_bot, &callback, Some("已收到操作")).await;
        return send_text(
            &inbound_bot,
            &inbound_surface,
            &format!("已收到操作：{}", callback.data),
            metrics,
        )
        .await;
    };
    if !workflow_callback_bot_allowed(&inbound_bot)
        || !workflow_callback_owner_authorized(store, &callback)?
    {
        acknowledge_callback(&inbound_bot, &callback, Some("无权操作此审批按钮")).await;
        return Ok(());
    }
    let Some(preview) = store
        .peek_callback(nonce, now_ms())
        .map_err(|error| error.to_string())?
    else {
        acknowledge_callback(&inbound_bot, &callback, Some("按钮已过期或已处理")).await;
        return send_text(
            &inbound_bot,
            &inbound_surface,
            "这个审批按钮已过期或已经处理，未执行任何操作。",
            metrics,
        )
        .await;
    };
    let preview_action: StoredApprovalAction = serde_json::from_str(&preview.action)
        .map_err(|_| "审批状态损坏，未执行任何操作".to_owned())?;
    let Some(approval_session) = sessions.by_thread(&preview_action.thread_id) else {
        acknowledge_callback(&inbound_bot, &callback, Some("当前 Session 已不存在")).await;
        return Ok(());
    };
    if preview.space_id.is_empty()
        || preview.space_id
            != store
                .session_space_for_thread(&preview_action.thread_id)
                .map_err(|error| error.to_string())?
                .map(|space| space.space_id)
                .unwrap_or_default()
        || callback.chat_id != approval_session.chat_id
    {
        acknowledge_callback(&inbound_bot, &callback, Some("审批按钮不属于当前会话")).await;
        return Ok(());
    }
    let approval_space_id = preview.space_id.clone();
    let approval_unlocked = if approval_space_id.is_empty() {
        totp.is_unlocked(now_ms())
    } else {
        totp.is_unlocked_for_space(&approval_space_id, now_ms())
    }
    .map_err(|error| error.to_string())?;
    if !approval_unlocked {
        acknowledge_callback(&inbound_bot, &callback, Some("请先完成 TOTP 解锁")).await;
        return send_text(
            &inbound_bot,
            &inbound_surface,
            "审批是写操作，请先发送 /totp <6 位验证码>，再点击审批按钮。",
            metrics,
        )
        .await;
    }
    let Some(stored) = store
        .take_callback_scoped(
            nonce,
            now_ms(),
            Some(&preview.space_id),
            Some(preview.generation),
        )
        .map_err(|error| error.to_string())?
    else {
        acknowledge_callback(&inbound_bot, &callback, Some("按钮已过期或已处理")).await;
        return send_text(
            &inbound_bot,
            &inbound_surface,
            "这个审批按钮已过期或已经处理，未执行任何操作。",
            metrics,
        )
        .await;
    };
    let action: StoredApprovalAction = serde_json::from_str(&stored.action)
        .map_err(|_| "审批状态损坏，未执行任何操作".to_owned())?;
    if action.generation != agent.connection_state().generation {
        acknowledge_callback(&inbound_bot, &callback, Some("Codex 连接已重建")).await;
        return send_text(
            &inbound_bot,
            &inbound_surface,
            "Codex 连接已经重建，原审批请求已失效。",
            metrics,
        )
        .await;
    }
    let approval_id =
        ApprovalId::new(action.approval_id.clone()).map_err(|error| error.to_string())?;
    let mut approval = store
        .get_approval(&approval_id)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "审批记录不存在，未执行任何操作".to_owned())?;
    if approval.decision != ApprovalDecision::Pending {
        acknowledge_callback(&inbound_bot, &callback, Some("审批已由其他按钮处理")).await;
        return send_text(
            &inbound_bot,
            &inbound_surface,
            "这个审批请求已经处理，当前按钮不会再次执行操作。",
            metrics,
        )
        .await;
    }
    let decision = domain_decision(&action.decision)
        .ok_or_else(|| "审批决定无效，未执行任何操作".to_owned())?;
    if let Err(error) = agent
        .respond(action.request_id.clone(), action.response_payload())
        .await
    {
        let _ = store.restore_callback(nonce);
        acknowledge_callback(&inbound_bot, &callback, Some("Codex 未接受响应")).await;
        return send_text(
            &inbound_bot,
            &inbound_surface,
            &format!("审批响应未送达，按钮已恢复，可稍后重试：{error}"),
            metrics,
        )
        .await;
    }
    if let Err(error) = approval.decide(decision, now_ms()) {
        let _ = store.restore_callback(nonce);
        acknowledge_callback(&inbound_bot, &callback, Some("审批状态已变化")).await;
        return send_text(
            &inbound_bot,
            &inbound_surface,
            "审批状态已经变化，当前按钮不会再次执行操作。",
            metrics,
        )
        .await
        .map_err(|send_error| format!("{error}; {send_error}"));
    }
    let event = DomainEvent {
        id: EventId::new(format!("approval-decided-{nonce}")).map_err(|error| error.to_string())?,
        occurred_at_ms: now_ms(),
        kind: DomainEventKind::ApprovalDecided {
            approval: approval.clone(),
        },
    };
    if let Err(error) = store.decide_approval(&approval, &event) {
        let _ = store.restore_callback(nonce);
        return Err(error.to_string());
    }
    acknowledge_callback(&inbound_bot, &callback, Some("审批已提交")).await;
    let confirmation = approval_confirmation(&action.decision, &action.method);
    let sender = sessions
        .by_thread(&action.thread_id)
        .and_then(|session| bots_by_id.get(&session.sender_instance_id))
        .unwrap_or(&inbound_bot);
    send_text(
        sender,
        &surface_for(sender, config, callback.chat_id, root_message_id),
        confirmation,
        metrics,
    )
    .await
}

fn status_callback_markup(
    store: &SqliteStore,
    space: &RustSessionSpace,
    actions: &[(&str, &str)],
) -> Result<Option<Value>, String> {
    status_callback_markup_rows(store, space, &[actions])
}

fn status_callback_markup_rows(
    store: &SqliteStore,
    space: &RustSessionSpace,
    rows: &[&[(&str, &str)]],
) -> Result<Option<Value>, String> {
    status_callback_markup_rows_for_surface(store, space, rows, "status")
}

fn discussion_callback_markup_rows(
    store: &SqliteStore,
    space: &RustSessionSpace,
    rows: &[&[(&str, &str)]],
) -> Result<Option<Value>, String> {
    status_callback_markup_rows_for_surface(store, space, rows, "discussion")
}

fn status_callback_markup_rows_for_surface(
    store: &SqliteStore,
    space: &RustSessionSpace,
    rows: &[&[(&str, &str)]],
    surface: &str,
) -> Result<Option<Value>, String> {
    if store
        .workflow_record("onboarding", "owner")
        .map_err(|error| error.to_string())?
        .and_then(|value| value.get("user_id").and_then(Value::as_i64))
        .is_none()
    {
        return Ok(None);
    }
    let mut keyboard = Vec::with_capacity(rows.len());
    for actions in rows {
        let mut buttons = Vec::with_capacity(actions.len());
        for (label, action) in *actions {
            if !is_status_action(action) {
                return Err(format!("unknown status callback action: {action}"));
            }
            let nonce = format!("status-{}", next_approval_nonce());
            let stored = StoredStatusAction {
                space_id: space.space_id.clone(),
                generation: u64::try_from(space.generation)
                    .map_err(|_| "status generation is negative".to_owned())?,
                thread_id: space.thread_id.clone().unwrap_or_default(),
                action: (*action).to_owned(),
            };
            let callback = StoredCallback {
                nonce: nonce.clone(),
                space_id: space.space_id.clone(),
                generation: space.generation,
                action: serde_json::to_string(&stored).map_err(|error| error.to_string())?,
                expires_at_ms: now_ms() + STATUS_CALLBACK_TTL_MS,
            };
            if surface == "status" {
                store
                    .create_status_callback(&callback)
                    .map_err(|error| error.to_string())?;
            } else {
                store
                    .create_callback(&callback)
                    .map_err(|error| error.to_string())?;
            }
            buttons.push(json!({
                "text": *label,
                "callback_data": format!("cb:{nonce}"),
            }));
        }
        if !buttons.is_empty() {
            keyboard.push(Value::Array(buttons));
        }
    }
    Ok((!keyboard.is_empty()).then(|| json!({"inline_keyboard": keyboard})))
}

fn status_step_value(step: &Value) -> (String, String) {
    let text = step
        .get("step")
        .or_else(|| step.get("title"))
        .or_else(|| step.get("name"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let status = step
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("pending")
        .to_owned();
    (text, status)
}

fn status_steps(plan: Option<&Value>) -> Vec<Value> {
    let Some(plan) = plan else {
        return Vec::new();
    };
    plan.as_array()
        .cloned()
        .or_else(|| plan.get("steps").and_then(Value::as_array).cloned())
        .unwrap_or_default()
}

fn compact_status_event_text(value: &str) -> String {
    const LIMIT: usize = 360;
    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= LIMIT {
        return compact;
    }
    let mut result = compact.chars().take(LIMIT).collect::<String>();
    result.push_str("...");
    result
}

fn status_event_status(item: &Value, default: &str) -> String {
    normalized_status(item.get("status").or_else(|| item.get("state")), default)
}

fn status_event_summary(item: &Value, completed: bool) -> Option<(String, String)> {
    let item_type = item.get("type").and_then(Value::as_str)?;
    let (text, status) = match item_type {
        "agentMessage" => {
            let text = item.get("text").and_then(Value::as_str)?;
            let text = compact_status_event_text(text);
            if text.is_empty() {
                return None;
            }
            (text, "completed".to_owned())
        }
        "plan" => (
            if completed {
                "Plan 已完成".to_owned()
            } else {
                "Plan 正在生成".to_owned()
            },
            if completed {
                "completed".to_owned()
            } else {
                "inProgress".to_owned()
            },
        ),
        "enteredReviewMode" => ("Review 正在执行".to_owned(), "inProgress".to_owned()),
        "exitedReviewMode" => ("Review 已完成".to_owned(), "completed".to_owned()),
        "commandExecution" => {
            let status =
                status_event_status(item, if completed { "completed" } else { "inProgress" });
            let suffix = item
                .get("exitCode")
                .map(|value| format!(" (exit {})", value))
                .unwrap_or_default();
            (format!("命令执行 {status}{suffix}"), status)
        }
        "fileChange" => {
            let count = item
                .get("changes")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            let status = item
                .get("status")
                .map(|value| normalized_status(Some(value), ""))
                .unwrap_or_default();
            let text = format!("文件变更 {count} 项: {status}").trim().to_owned();
            (text, status)
        }
        "mcpToolCall" | "dynamicToolCall" => {
            let name = item
                .get("tool")
                .or_else(|| item.get("server"))
                .and_then(Value::as_str)
                .unwrap_or("tool");
            let status = status_event_status(item, "running");
            (format!("工具 {name}: {status}"), status)
        }
        "collabAgentToolCall" => {
            let tool = item.get("tool").and_then(Value::as_str).unwrap_or_default();
            let status =
                status_event_status(item, if completed { "completed" } else { "inProgress" });
            (
                format!("Agent task {tool}: {status}").trim().to_owned(),
                status,
            )
        }
        "subAgentActivity" => {
            let kind = item.get("kind").and_then(Value::as_str).unwrap_or_default();
            (
                format!("Subagent {kind}").trim().to_owned(),
                kind.to_owned(),
            )
        }
        "imageGeneration" => {
            let status = item
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or_default();
            (
                format!("图像生成: {status}").trim().to_owned(),
                status.to_owned(),
            )
        }
        "contextCompaction" => ("上下文已压缩".to_owned(), "completed".to_owned()),
        "error" | "turnError" => (
            error_message(item.get("error").or(Some(item)))
                .unwrap_or_else(|| "Codex error".to_owned()),
            "failed".to_owned(),
        ),
        _ => {
            let status = if completed { "completed" } else { "inProgress" };
            (format!("{item_type}: {status}"), status.to_owned())
        }
    };
    Some((compact_status_event_text(&text), status))
}

fn status_event_clock(item: &Value) -> Option<String> {
    let seconds = control_epoch_ms(
        [
            "timestamp",
            "createdAt",
            "created_at",
            "updatedAt",
            "updated_at",
        ]
        .iter()
        .find_map(|key| item.get(*key)),
    )?
    .div_euclid(1000)
    .rem_euclid(86_400);
    Some(format!(
        "{:02}:{:02}",
        seconds / 3_600,
        seconds % 3_600 / 60
    ))
}

fn status_event_line(item: &Value, completed: bool) -> Option<String> {
    let (text, status) = status_event_summary(item, completed)?;
    let suffix = if status.is_empty() {
        String::new()
    } else {
        format!(" · {status}")
    };
    Some(match status_event_clock(item) {
        Some(clock) => format!("{clock} {text}{suffix}"),
        None => format!("{text}{suffix}"),
    })
}

fn status_progress_bar(completed: usize, total: usize) -> String {
    const WIDTH: usize = 10;
    if total == 0 {
        return "----------".to_owned();
    }
    let filled = completed.saturating_mul(WIDTH).div_ceil(total).min(WIDTH);
    format!("{}{}", "#".repeat(filled), "-".repeat(WIDTH - filled))
}

/// Python `views.py:ANIMATION_FRAMES` — the moon phase cycles once per
/// heartbeat while a Session is active; terminal Sessions pin the full moon.
const ANIMATION_FRAMES: [&str; 8] = ["🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘"];
const TERMINAL_FRAME_INDEX: u64 = 4;

/// Dual-track status payload: Telegram receives MarkdownV2 first and the
/// plain text on a 400 parse rejection, mirroring the Python
/// `RenderedMessage(markdown, plain)` contract.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct StatusRendered {
    markdown: String,
    plain: String,
}

fn markdown_escape(value: &str) -> String {
    value
        .chars()
        .flat_map(|character| {
            if matches!(
                character,
                '\\' | '_'
                    | '*'
                    | '['
                    | ']'
                    | '('
                    | ')'
                    | '~'
                    | '`'
                    | '>'
                    | '#'
                    | '+'
                    | '-'
                    | '='
                    | '|'
                    | '{'
                    | '}'
                    | '.'
                    | '!'
            ) {
                ['\\', character]
            } else {
                ['\0', character]
            }
        })
        .filter(|character| *character != '\0')
        .collect()
}

fn markdown_code(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('`', "\\`");
    format!("`{escaped}`")
}

fn status_is_animated(space: &RustSessionSpace, projection: Option<&ThreadProjection>) -> bool {
    if space.lifecycle != "active" {
        return false;
    }
    let Some(projection) = projection else {
        return false;
    };
    if projection.turn_status.as_deref() == Some("inProgress")
        || projection.review_status.as_deref() == Some("inProgress")
    {
        return true;
    }
    projection.status.as_deref() == Some("active")
        && !matches!(
            projection.turn_status.as_deref(),
            Some("completed" | "failed" | "interrupted")
        )
}

fn status_animation_frame(
    space: &RustSessionSpace,
    projection: Option<&ThreadProjection>,
    animation_frame: Option<u64>,
) -> Option<&'static str> {
    if status_is_terminal(space, projection) {
        return Some(ANIMATION_FRAMES[TERMINAL_FRAME_INDEX as usize]);
    }
    // Python always renders the current phase in the mode header; an idle
    // Session keeps its last phase (default 🌑).
    let frame = animation_frame.unwrap_or(0);
    Some(ANIMATION_FRAMES[(frame % ANIMATION_FRAMES.len() as u64) as usize])
}

fn format_duration_ms(total_ms: i64) -> String {
    let seconds = total_ms.max(0).div_euclid(1_000);
    if seconds >= 3_600 {
        format!("{}h {:02}m", seconds / 3_600, seconds % 3_600 / 60)
    } else {
        format!("{}m {:02}s", seconds / 60, seconds % 60)
    }
}

/// Cross-turn cumulative execution time (Python `total_duration_ms`):
/// completed turns plus the in-flight turn's elapsed time.
fn status_total_duration(projection: Option<&ThreadProjection>, now: i64) -> String {
    let Some(projection) = projection else {
        return "N/A".to_owned();
    };
    let completed = projection.completed_turns_duration_ms.max(0);
    let current = if projection.turn_status.as_deref() == Some("inProgress") {
        projection
            .started_at_ms
            .filter(|value| *value > 0)
            .map(|started| now.saturating_sub(started).max(0))
            .unwrap_or(0)
    } else {
        0
    };
    let total = completed.saturating_add(current);
    if total <= 0 {
        return "N/A".to_owned();
    }
    format_duration_ms(total)
}

fn status_clock(epoch_ms: i64) -> String {
    let seconds = epoch_ms.div_euclid(1_000).rem_euclid(86_400);
    format!(
        "{:02}:{:02}:{:02}",
        seconds / 3_600,
        seconds % 3_600 / 60,
        seconds % 60
    )
}

fn status_text(
    store: &SqliteStore,
    space: &RustSessionSpace,
    projection: Option<&ThreadProjection>,
    note: Option<&str>,
    totp: &TotpManager,
) -> String {
    status_render(store, space, projection, note, totp, None).plain
}

#[allow(clippy::too_many_lines)]
fn status_render(
    store: &SqliteStore,
    space: &RustSessionSpace,
    projection: Option<&ThreadProjection>,
    note: Option<&str>,
    totp: &TotpManager,
    animation_frame: Option<u64>,
) -> StatusRendered {
    let thread_id = space.thread_id.as_deref().unwrap_or("-");
    let pending_payload = if space.lifecycle == "pending" || space.lifecycle == "repair_required" {
        store
            .workflow_record("pending_space", &space.space_id)
            .ok()
            .flatten()
            .or_else(|| {
                store
                    .workflow_record("space", &space.space_id)
                    .ok()
                    .flatten()
            })
    } else {
        None
    };
    let title = projection
        .and_then(|value| value.title.as_deref())
        .or_else(|| {
            pending_payload
                .as_ref()
                .and_then(|value| value.get("pending_prompt"))
                .and_then(Value::as_str)
        })
        .unwrap_or("Codex Session");
    let lifecycle = space.lifecycle.as_str();
    let raw_status = projection
        .and_then(|value| value.status.as_deref())
        .unwrap_or("unknown");
    let turn_status = projection
        .and_then(|value| value.turn_status.as_deref())
        .unwrap_or("idle");
    let last_error = projection.and_then(|value| value.last_error.as_deref());
    let active_flags = projection
        .map(|value| value.active_flags.as_slice())
        .unwrap_or_default();
    let waiting_on_input = active_flags.iter().any(|flag| {
        matches!(
            flag.as_str(),
            "waitingOnUserInput" | "waiting_on_user_input"
        )
    });
    let waiting_on_approval = active_flags.iter().any(|flag| {
        matches!(
            flag.as_str(),
            "waitingOnApproval"
                | "waiting_on_approval"
                | "waitingForApproval"
                | "waiting_for_approval"
                | "approval"
        )
    });
    let (status_icon, status_label) = if lifecycle == "closed" {
        ("⚫", "已关闭")
    } else if lifecycle == "pending" {
        ("🟡", "待认证")
    } else if lifecycle == "repair_required" {
        ("🟠", "需要修复")
    } else if last_error.is_some() || raw_status == "systemError" || turn_status == "failed" {
        ("🔴", "错误")
    } else if waiting_on_input {
        ("🟡", "等待回答")
    } else if waiting_on_approval {
        ("🟡", "等待审批")
    } else if matches!(turn_status, "completed" | "interrupted") || raw_status == "idle" {
        ("⚪", "空闲")
    } else if raw_status == "active" || turn_status == "inProgress" {
        ("🟢", "执行中")
    } else {
        ("⚫", "未加载")
    };
    let goal = projection.and_then(|value| value.goal.as_ref());
    let goal_status = goal
        .and_then(|value| value.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("none");
    let goal_objective = goal
        .and_then(|value| value.get("objective").or_else(|| value.get("title")))
        .and_then(Value::as_str)
        .unwrap_or("未创建 Goal");
    let steps = status_steps(projection.and_then(|value| value.plan.as_ref()));
    let completed = steps
        .iter()
        .filter(|step| status_step_value(step).1 == "completed")
        .count();
    let plan_total = steps.len();
    let tasks = projection.map_or(0, |value| value.subagents.len());
    let active_tasks = projection.map_or(0, |value| {
        value
            .subagents
            .values()
            .filter(|task| {
                matches!(
                    task.get("status").and_then(Value::as_str),
                    Some("pending" | "pendingInit" | "active" | "running" | "inProgress")
                )
            })
            .count()
    });
    let failed_tasks = projection.map_or(0, |value| {
        value
            .subagents
            .values()
            .filter(|task| {
                matches!(
                    task.get("status").and_then(Value::as_str),
                    Some("failed" | "errored" | "notFound")
                )
            })
            .count()
    });
    let interrupted_tasks = projection.map_or(0, |value| {
        value
            .subagents
            .values()
            .filter(|task| {
                matches!(
                    task.get("status").and_then(Value::as_str),
                    Some("interrupted" | "cancelled" | "canceled")
                )
            })
            .count()
    });
    let queue = store
        .workflow_records("queue")
        .unwrap_or_default()
        .into_iter()
        .filter(|(_, value)| {
            value.get("thread_id").and_then(Value::as_str) == Some(thread_id)
                && value.get("status").and_then(Value::as_str) == Some("queued")
        })
        .count();
    let auth_now = now_ms();
    let auth = match totp.space_unlock_remaining_ms(&space.space_id, auth_now) {
        Ok(remaining_ms) if remaining_ms > 0 => {
            let expiry = totp
                .space_unlock_expires_at_ms(&space.space_id, auth_now)
                .ok()
                .flatten()
                .map(status_clock)
                .unwrap_or_else(|| "--:--:--".to_owned());
            (
                format!(
                    "🔓 TOTP 已认证 · 剩余 {} min · 到期 {}",
                    markdown_code(&((remaining_ms + 59_999) / 60_000).max(1).to_string()),
                    markdown_code(&expiry)
                ),
                format!(
                    "🔓 TOTP 已认证 · 剩余 {} min · 到期 {}",
                    ((remaining_ms + 59_999) / 60_000).max(1),
                    expiry
                ),
            )
        }
        Ok(_)
            if totp
                .space_unlock_expires_at_ms(&space.space_id, auth_now)
                .ok()
                .flatten()
                .is_some_and(|expires_at| expires_at <= auth_now) =>
        {
            ("🔒 TOTP 已过期".to_owned(), "🔒 TOTP 已过期".to_owned())
        }
        _ => ("🔒 TOTP 未认证".to_owned(), "🔒 TOTP 未认证".to_owned()),
    };
    let desired_mode = projection.and_then(|value| value.desired_mode.as_deref());
    let observed_mode = space
        .observed_mode
        .as_deref()
        .filter(|mode| !matches!(*mode, "" | "unknown"))
        .or_else(|| projection.and_then(|value| value.observed_mode.as_deref()))
        .unwrap_or("unknown");
    let review_active =
        projection.and_then(|value| value.review_status.as_deref()) == Some("inProgress");
    let profile_mode = if observed_mode != "unknown" {
        observed_mode
    } else {
        desired_mode.unwrap_or("default")
    };
    let model = if profile_mode == "plan" {
        space.plan_model.as_deref()
    } else {
        space.normal_model.as_deref()
    }
    .or_else(|| projection.and_then(|value| value.model.as_deref()));
    let effort = if profile_mode == "plan" {
        space.plan_effort.as_deref()
    } else {
        space.normal_effort.as_deref()
    }
    .or_else(|| projection.and_then(|value| value.effort.as_deref()));
    let updated_at_ms = projection
        .map(|value| value.updated_at_ms)
        .filter(|value| *value > 0)
        .unwrap_or(space.updated_at_ms);
    let now = now_ms();
    let duration = status_total_duration(projection, now);
    let frame_prefix = status_animation_frame(space, projection, animation_frame)
        .map(|frame| format!("{frame} "))
        .unwrap_or_default();
    let mut lines: Vec<(String, String)> = vec![
        (
            format!("*🤖 Codex · {}*", markdown_escape(&truncate_text(title))),
            format!("🤖 Codex · {}", truncate_text(title)),
        ),
        (
            format!(
                "{} · {} {} · Turn {} · 总执行 {}",
                markdown_code(&truncate_text(thread_id)),
                status_icon,
                markdown_escape(status_label),
                markdown_code(turn_status),
                markdown_code(&duration),
            ),
            format!(
                "{} · {} {} · Turn {} · 总执行 {}",
                truncate_text(thread_id),
                status_icon,
                status_label,
                turn_status,
                duration,
            ),
        ),
        (
            format!(
                "生命周期：{} · Mode：{}",
                markdown_escape(lifecycle),
                markdown_escape(observed_mode)
            ),
            format!("生命周期：{lifecycle} · Mode：{observed_mode}"),
        ),
        (
            format!(
                "*🎯 Goal*  {} · {}",
                markdown_code(goal_status),
                markdown_escape(&truncate_text(goal_objective))
            ),
            format!(
                "🎯 Goal · {} · {}",
                goal_status,
                truncate_text(goal_objective)
            ),
        ),
        (
            format!(
                "*🧭 Plan*  {}  {}",
                markdown_code(&format!("{completed}/{plan_total}")),
                markdown_code(&status_progress_bar(completed, plan_total))
            ),
            format!(
                "🧭 Plan · {completed}/{plan_total} · [{}]",
                status_progress_bar(completed, plan_total)
            ),
        ),
    ];
    if review_active {
        lines.push((
            format!("{frame_prefix}*🔎 Review · 执行中*"),
            format!("{frame_prefix}🔎 Review · 执行中"),
        ));
    } else if observed_mode == "plan" {
        lines.push((
            format!("{frame_prefix}*🧭 TUI Plan mode*"),
            format!("{frame_prefix}🧭 TUI Plan mode"),
        ));
    } else if observed_mode == "default" {
        lines.push((
            format!("{frame_prefix}*⚙️ TUI Normal mode*"),
            format!("{frame_prefix}⚙️ TUI Normal mode"),
        ));
    } else if desired_mode.is_some() {
        lines.push((
            format!("{frame_prefix}*⚪ TUI mode 未确认*"),
            format!("{frame_prefix}⚪ TUI mode 未确认"),
        ));
    }
    if review_active || desired_mode.is_some() || model.is_some() || effort.is_some() {
        lines.push((
            format!(
                "*🧠 Main*  {} · Effort {}",
                markdown_code(model.unwrap_or("N/A")),
                markdown_code(effort.unwrap_or("N/A"))
            ),
            format!(
                "🧠 Main · {} · Effort {}",
                model.unwrap_or("N/A"),
                effort.unwrap_or("N/A")
            ),
        ));
    }
    if waiting_on_input {
        lines.push(("⏳ 等待用户输入".to_owned(), "⏳ 等待用户输入".to_owned()));
    }
    if waiting_on_approval {
        lines.push(("🛂 等待审批".to_owned(), "🛂 等待审批".to_owned()));
    }
    if let Some(cwd) = projection
        .and_then(|value| value.cwd.as_deref())
        .filter(|value| !value.trim().is_empty())
    {
        lines.push((
            format!("📁 项目 · {}", markdown_code(&truncate_text(cwd))),
            format!("📁 项目 · {}", truncate_text(cwd)),
        ));
    }
    if let Some(pending) = pending_payload.as_ref() {
        if let Some(cwd) = pending
            .get("pending_cwd")
            .or_else(|| pending.get("cwd"))
            .and_then(Value::as_str)
        {
            lines.push((
                format!("📁 项目 · {}", markdown_code(&truncate_text(cwd))),
                format!("📁 项目 · {}", truncate_text(cwd)),
            ));
        }
        if let Some(prompt) = pending
            .get("pending_prompt")
            .or_else(|| pending.get("prompt"))
            .and_then(Value::as_str)
        {
            lines.push((
                format!(
                    "📝 首条 prompt · {}",
                    markdown_escape(&truncate_text(prompt))
                ),
                format!("📝 首条 prompt · {}", truncate_text(prompt)),
            ));
        }
        lines.push((
            "🔐 待认证 · 在评论串发送 /totp <验证码>".to_owned(),
            "🔐 待认证 · 在评论串发送 /totp <验证码>".to_owned(),
        ));
    }
    if steps.is_empty() {
        lines.push(("尚未创建计划".to_owned(), "尚未创建计划".to_owned()));
    }
    for (index, step) in steps.iter().take(14).enumerate() {
        let (step_text, step_status) = status_step_value(step);
        let plain = format!(
            "{} {}. {}",
            match step_status.as_str() {
                "completed" => "✅",
                "inProgress" => "▶",
                "blocked" => "⏸",
                "failed" => "❌",
                _ => "○",
            },
            index + 1,
            truncate_text(&step_text)
        );
        let escaped_step = markdown_escape(&truncate_text(&step_text));
        let markdown = match step_status.as_str() {
            "completed" => format!("✅ ~{}\\. {escaped_step}~", index + 1),
            "inProgress" => format!("▶ *{}\\. {escaped_step}*", index + 1),
            "blocked" => format!("⏸ {}\\. {escaped_step}", index + 1),
            "failed" => format!("❌ {}\\. {escaped_step}", index + 1),
            _ => format!("○ {}\\. {escaped_step}", index + 1),
        };
        lines.push((markdown, plain));
    }
    if plan_total > 14 {
        lines.push((
            markdown_escape(&format!("… 另有 {} 项，请使用 /plan 查看", plan_total - 14)),
            format!("… 另有 {} 项，请使用 /plan 查看", plan_total - 14),
        ));
    }
    if goal_status == "complete" && plan_total > 0 && completed != plan_total {
        let warning = format!(
            "Goal 已完成，但 Plan 仍有 {} 项未完成；状态不一致，请先同步 Plan。",
            plan_total - completed
        );
        lines.push((
            format!("⚠️ {}", markdown_escape(&warning)),
            format!("WARNING: {warning}"),
        ));
    }
    if goal_status == "complete" && active_tasks > 0 {
        let warning =
            format!("Goal 已完成，但仍有 {active_tasks} 个 Subagent 运行中；请先等待或结束任务。");
        lines.push((
            format!("⚠️ {}", markdown_escape(&warning)),
            format!("WARNING: {warning}"),
        ));
    }
    lines.push((
        format!(
            "*🧩 Agent Tasks*  {} · Running {} · Failed {} · Interrupted {}",
            markdown_code(&format!("{}/{}", tasks.saturating_sub(active_tasks), tasks)),
            markdown_code(&active_tasks.to_string()),
            markdown_code(&failed_tasks.to_string()),
            markdown_code(&interrupted_tasks.to_string())
        ),
        format!(
            "🧩 Agent Tasks · {}/{} · Running {} · Failed {} · Interrupted {}",
            tasks.saturating_sub(active_tasks),
            tasks,
            active_tasks,
            failed_tasks,
            interrupted_tasks
        ),
    ));
    lines.push((
        format!("*📥 Queue*  {}", markdown_code(&queue.to_string())),
        format!("📥 Queue · {queue}"),
    ));
    let (visible_agents, hidden_agents) = visible_subagents(projection);
    if !visible_agents.is_empty() {
        lines.push(("*🤝 Subagents*".to_owned(), "🤝 Subagents".to_owned()));
        for task in visible_agents {
            lines.push(subagent_task_lines(task, now));
        }
        if hidden_agents > 0 {
            lines.push((
                markdown_escape(&format!("… 另有 {hidden_agents} 个已结束 Agent")),
                format!("… 另有 {hidden_agents} 个已结束 Agent"),
            ));
        }
    }
    if let Some(error) = last_error.filter(|value| !value.trim().is_empty()) {
        lines.push((
            format!("*❌ 错误*  {}", markdown_escape(&truncate_text(error))),
            format!("❌ 错误 · {}", truncate_text(error)),
        ));
    }
    if let Some(projection) = projection {
        let completed = projection.turn_status.as_deref() != Some("inProgress");
        let recent = if projection.item_order.is_empty() {
            projection
                .items
                .values()
                .rev()
                .take(4)
                .filter_map(|item| status_event_line(item, completed))
                .collect::<Vec<_>>()
        } else {
            projection
                .item_order
                .iter()
                .rev()
                .filter_map(|item_id| projection.items.get(item_id))
                .take(4)
                .filter_map(|item| status_event_line(item, completed))
                .collect::<Vec<_>>()
        };
        if !recent.is_empty() {
            lines.push(("*🕘 近期事件*".to_owned(), "🕘 近期事件".to_owned()));
            lines.extend(recent.into_iter().map(|line| {
                let (clock, text) = line.split_once(' ').unwrap_or(("--:--", line.as_str()));
                if clock.chars().all(|c| c.is_ascii_digit() || c == ':') {
                    (
                        format!("{} {}", markdown_code(clock), markdown_escape(text)),
                        line,
                    )
                } else {
                    (markdown_escape(&line), line)
                }
            }));
        }
    }
    for (agent_id, task) in projection
        .map(|value| value.subagents.iter())
        .into_iter()
        .flatten()
        .filter(|(_, task)| {
            matches!(
                task.get("status").and_then(Value::as_str),
                Some("interrupted" | "cancelled" | "canceled")
            )
        })
        .take(4)
    {
        let title = task
            .get("title")
            .or_else(|| task.get("name"))
            .and_then(Value::as_str)
            .unwrap_or(agent_id);
        lines.push((
            format!(
                "↩️ Subagent interrupted · {}",
                markdown_escape(&truncate_text(title))
            ),
            format!("↩️ Subagent interrupted · {}", truncate_text(title)),
        ));
    }
    lines.push(auth);
    lines.push((
        format!(
            "🕒 更新 {} · 心跳 {} · generation {}",
            markdown_code(&status_clock(updated_at_ms.max(0))),
            markdown_code(&format!("≤{HEARTBEAT_SECONDS}s")),
            markdown_code(&projection.map_or(0, |value| value.generation).to_string())
        ),
        format!(
            "🕒 更新 {} · 心跳 ≤{}s · generation {}",
            status_clock(updated_at_ms.max(0)),
            HEARTBEAT_SECONDS,
            projection.map_or(0, |value| value.generation)
        ),
    ));
    let mut markdown = lines
        .iter()
        .map(|(markdown, _)| markdown.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let mut plain = lines
        .iter()
        .map(|(_, plain)| plain.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    if let Some(note) = note.filter(|value| !value.trim().is_empty()) {
        markdown.push_str("\n\n");
        markdown.push_str(&markdown_escape(note));
        plain.push_str("\n\n");
        plain.push_str(note);
    }
    StatusRendered {
        markdown: truncate_text(&markdown),
        plain: truncate_text(&plain),
    }
}

/// Python `views.py:_visible_agents` — active tasks first (oldest start
/// first), then the three most recently finished terminal tasks.
fn visible_subagents(projection: Option<&ThreadProjection>) -> (Vec<&Value>, usize) {
    let Some(projection) = projection else {
        return (Vec::new(), 0);
    };
    let mut active = Vec::new();
    let mut terminal = Vec::new();
    for task in projection.subagents.values() {
        let (_, _, is_active) = subagent_task_status(task);
        if is_active {
            active.push(task);
        } else {
            terminal.push(task);
        }
    }
    active.sort_by_key(|task| {
        task_epoch_ms(task.get("started_at").or_else(|| task.get("updated_at")))
    });
    terminal.sort_by_key(|task| {
        std::cmp::Reverse(task_epoch_ms(
            task.get("finished_at").or_else(|| task.get("updated_at")),
        ))
    });
    let total = projection.subagents.len();
    let mut visible = active;
    visible.extend(terminal.into_iter().take(3));
    let hidden = total.saturating_sub(visible.len());
    (visible, hidden)
}

fn subagent_task_status(task: &Value) -> (&'static str, String, bool) {
    let status = task
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("pending");
    match status {
        "pending" | "pendingInit" => ("🟡", "初始化".to_owned(), true),
        "active" | "running" | "inProgress" => ("🟢", "运行中".to_owned(), true),
        "completed" => ("✅", "已完成".to_owned(), false),
        "shutdown" => ("⚫", "已关闭".to_owned(), false),
        "interrupted" | "cancelled" | "canceled" => ("⏸", "已中断".to_owned(), false),
        "notFound" => ("❓", "未找到".to_owned(), false),
        "errored" | "failed" => ("❌", "失败".to_owned(), false),
        _ => ("⚪", "未知".to_owned(), false),
    }
}

/// Task timestamps come from the Python `TaskState` contract (epoch seconds)
/// while live Rust projections write milliseconds; accept either unit.
fn task_epoch_ms(value: Option<&Value>) -> Option<i64> {
    value
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .map(|value| {
            if value > 10_000_000_000 {
                value
            } else {
                value.saturating_mul(1_000)
            }
        })
}

fn task_clip(task: Option<&Value>, limit: usize) -> String {
    let text = task
        .and_then(Value::as_str)
        .unwrap_or_default()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    text.chars().take(limit).collect()
}

fn subagent_task_lines(task: &Value, now_ms: i64) -> (String, String) {
    let (icon, status_label, _) = subagent_task_status(task);
    let thread_id = task
        .get("agent_thread_id")
        .or_else(|| task.get("task_id"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let path = task
        .get("agent_path")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let nickname = task
        .get("agent_nickname")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let role = task
        .get("agent_role")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let label = {
        let raw = if !nickname.is_empty() {
            nickname
        } else if !path.is_empty() {
            path
        } else if !thread_id.is_empty() {
            short_id_prefix(thread_id)
        } else {
            "agent"
        };
        raw.chars().take(48).collect::<String>()
    };
    let short_id = if !thread_id.is_empty() {
        short_id_prefix(thread_id).to_owned()
    } else if !path.is_empty() {
        path.to_owned()
    } else {
        "unknown".to_owned()
    };
    let model = task
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let effort = task
        .get("reasoning_effort")
        .or_else(|| task.get("reasoningEffort"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let model_effort = [model, effort]
        .into_iter()
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>()
        .join("/");
    let started = task_epoch_ms(
        task.get("started_at")
            .or_else(|| task.get("startedAt"))
            .or_else(|| task.get("updated_at")),
    )
    .unwrap_or(now_ms);
    let finished = task_epoch_ms(task.get("finished_at").or_else(|| task.get("finishedAt")));
    let elapsed = format_duration_ms(finished.unwrap_or(now_ms).saturating_sub(started).max(0));
    let title = task_clip(task.get("title"), 64);
    let title = if title.is_empty() {
        "Agent task".to_owned()
    } else {
        title
    };
    let mut metadata = vec![status_label, short_id];
    if !role.is_empty() {
        metadata.push(role.chars().take(32).collect());
    }
    if !model_effort.is_empty() {
        metadata.push(model_effort.chars().take(64).collect());
    }
    metadata.push(elapsed);
    let markdown = format!(
        "{icon} *{}* · {}\n└ {}",
        markdown_escape(&label),
        metadata
            .iter()
            .map(|value| markdown_code(value))
            .collect::<Vec<_>>()
            .join(" · "),
        markdown_escape(&title)
    );
    let plain = format!("{icon} {label} · {}\n  {title}", metadata.join(" · "));
    (markdown, plain)
}

fn status_bot_for<'a>(
    space: &RustSessionSpace,
    bots_by_id: &'a HashMap<String, RuntimeBot>,
) -> Option<&'a RuntimeBot> {
    space
        .status_bot_instance
        .as_deref()
        .and_then(|id| bots_by_id.get(id))
        .or_else(|| {
            bots_by_id
                .values()
                .find(|bot| bot.role == RuntimeBotRole::Status)
        })
        .or_else(|| {
            bots_by_id
                .values()
                .find(|bot| bot.role == RuntimeBotRole::Discussion)
        })
}

fn preferred_status_bot(bots_by_id: &HashMap<String, RuntimeBot>) -> Option<&RuntimeBot> {
    bots_by_id
        .values()
        .find(|bot| bot.role == RuntimeBotRole::Status)
}

fn status_semantic_fingerprint(
    text: &str,
    lifecycle: &str,
    terminal: bool,
    confirmation: bool,
) -> String {
    let mut digest = Sha256::new();
    digest.update(text.as_bytes());
    digest.update([0]);
    digest.update(lifecycle.as_bytes());
    digest.update([0]);
    let state: &[u8] = if terminal { b"terminal" } else { b"active" };
    digest.update(state);
    digest.update([0]);
    let surface: &[u8] = if confirmation {
        b"confirmation"
    } else {
        b"status"
    };
    digest.update(surface);
    format!("{:x}", digest.finalize())
}

#[allow(clippy::too_many_lines)]
fn channel_status_render(
    store: &SqliteStore,
    space: &RustSessionSpace,
    projection: Option<&ThreadProjection>,
    _totp: &TotpManager,
    animation_frame: Option<u64>,
) -> StatusRendered {
    let thread_id = space.thread_id.as_deref().unwrap_or("Pending");
    let pending_title = store
        .workflow_record("pending_space", &space.space_id)
        .ok()
        .flatten()
        .and_then(|value| {
            value
                .get("pending_prompt")
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
    let title = projection
        .and_then(|value| value.title.as_deref())
        .or(pending_title.as_deref())
        .unwrap_or("Codex session");
    let raw_status = projection
        .and_then(|value| value.status.as_deref())
        .unwrap_or("unknown");
    let turn_status = projection
        .and_then(|value| value.turn_status.as_deref())
        .unwrap_or("idle");
    let flags = projection
        .map(|value| value.active_flags.as_slice())
        .unwrap_or_default();
    let waiting_on_input = flags.iter().any(|flag| {
        matches!(
            flag.as_str(),
            "waitingOnUserInput" | "waiting_on_user_input"
        )
    });
    let waiting_on_approval = flags.iter().any(|flag| {
        matches!(
            flag.as_str(),
            "waitingOnApproval"
                | "waiting_on_approval"
                | "waitingForApproval"
                | "waiting_for_approval"
                | "approval"
        )
    });
    let error = projection.and_then(|value| value.last_error.as_deref());
    let (icon, label) = if space.lifecycle == "pending" {
        ("🟡", "待认证")
    } else if space.lifecycle == "closed" {
        ("⚫", "已关闭")
    } else if space.lifecycle == "repair_required" {
        ("🟠", "需要修复")
    } else if error.is_some() || raw_status == "systemError" || turn_status == "failed" {
        ("🔴", "错误")
    } else if waiting_on_input {
        ("🟡", "等待回答")
    } else if waiting_on_approval {
        ("🟡", "等待审批")
    } else if matches!(turn_status, "completed" | "interrupted") || raw_status == "idle" {
        ("⚪", "空闲")
    } else if raw_status == "active" || turn_status == "inProgress" {
        ("🟢", "执行中")
    } else {
        ("⚫", "未加载")
    };
    let goal = projection.and_then(|value| value.goal.as_ref());
    let goal_status = goal
        .and_then(|value| value.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("none");
    let goal_icon = match goal_status {
        "active" | "inProgress" => "🟢",
        "paused" => "⏸",
        "blocked" | "usageLimited" | "budgetLimited" => "🟠",
        "complete" => "✅",
        _ => "⚪",
    };
    let steps = status_steps(projection.and_then(|value| value.plan.as_ref()));
    let completed = steps
        .iter()
        .filter(|step| status_step_value(step).1 == "completed")
        .count();
    let tasks = projection.map_or(0, |value| value.subagents.len());
    let active_tasks = projection.map_or(0, |value| {
        value
            .subagents
            .values()
            .filter(|task| {
                matches!(
                    task.get("status").and_then(Value::as_str),
                    Some("pending" | "pendingInit" | "active" | "running" | "inProgress")
                )
            })
            .count()
    });
    let failed_tasks = projection.map_or(0, |value| {
        value
            .subagents
            .values()
            .filter(|task| {
                matches!(
                    task.get("status").and_then(Value::as_str),
                    Some("failed" | "errored" | "notFound")
                )
            })
            .count()
    });
    let interrupted_tasks = projection.map_or(0, |value| {
        value
            .subagents
            .values()
            .filter(|task| {
                matches!(
                    task.get("status").and_then(Value::as_str),
                    Some("interrupted" | "cancelled" | "canceled")
                )
            })
            .count()
    });
    let queue = store
        .workflow_records("queue")
        .unwrap_or_default()
        .into_iter()
        .filter(|(_, value)| {
            value.get("thread_id").and_then(Value::as_str) == Some(thread_id)
                && value.get("status").and_then(Value::as_str) == Some("queued")
        })
        .count();
    let desired_mode = projection.and_then(|value| value.desired_mode.as_deref());
    let observed_mode = space
        .observed_mode
        .as_deref()
        .filter(|mode| !matches!(*mode, "" | "unknown"))
        .or_else(|| projection.and_then(|value| value.observed_mode.as_deref()))
        .unwrap_or("unknown");
    let review_active =
        projection.and_then(|value| value.review_status.as_deref()) == Some("inProgress");
    let profile_mode = if observed_mode != "unknown" {
        observed_mode
    } else {
        desired_mode.unwrap_or("default")
    };
    let model = if profile_mode == "plan" {
        space.plan_model.as_deref()
    } else {
        space.normal_model.as_deref()
    }
    .or_else(|| projection.and_then(|value| value.model.as_deref()))
    .unwrap_or("N/A");
    let effort = if profile_mode == "plan" {
        space.plan_effort.as_deref()
    } else {
        space.normal_effort.as_deref()
    }
    .or_else(|| projection.and_then(|value| value.effort.as_deref()))
    .unwrap_or("N/A");
    let frame_prefix = status_animation_frame(space, projection, animation_frame)
        .map(|frame| format!("{frame} "))
        .unwrap_or_default();
    let mut lines: Vec<(String, String)> = Vec::new();
    if review_active {
        lines.push((
            format!("{frame_prefix}*🔎 Review · 执行中*"),
            format!("{frame_prefix}🔎 Review · 执行中"),
        ));
    } else if desired_mode.is_some() || observed_mode != "unknown" {
        let mode_label = match observed_mode {
            "plan" => "🧭 TUI Plan mode",
            "default" => "⚙️ TUI Normal mode",
            _ => "⚪ TUI mode 未确认",
        };
        lines.push((
            format!("{frame_prefix}*{}*", markdown_escape(mode_label)),
            format!("{frame_prefix}{mode_label}"),
        ));
    }
    let frame_prefix = if lines.is_empty() {
        frame_prefix
    } else {
        String::new()
    };
    let duration = status_total_duration(projection, now_ms());
    lines.extend([
        (
            format!(
                "{frame_prefix}*🤖 Codex · {}*",
                markdown_escape(&truncate_text(title))
            ),
            format!("{frame_prefix}🤖 Codex · {}", truncate_text(title)),
        ),
        (
            format!(
                "{} · {} {} · 总执行 {}",
                markdown_code(&truncate_text(thread_id)),
                icon,
                markdown_escape(label),
                markdown_code(&duration),
            ),
            format!(
                "{} · {} {} · 总执行 {}",
                truncate_text(thread_id),
                icon,
                label,
                duration,
            ),
        ),
    ]);
    if review_active
        || desired_mode.is_some()
        || projection.is_some_and(|value| value.model.is_some() || value.effort.is_some())
        || space.normal_model.is_some()
        || space.plan_model.is_some()
    {
        lines.push((
            format!(
                "*🧠 Main*  {} · Effort {}",
                markdown_code(model),
                markdown_code(effort)
            ),
            format!("🧠 Main · {model} · Effort {effort}"),
        ));
    }
    lines.push((
        format!("🎯 Goal {} {}", goal_icon, markdown_code(goal_status)),
        format!("🎯 Goal {goal_icon} {goal_status}"),
    ));
    lines.push((
        format!(
            "🧭 Plan {} {}",
            markdown_code(&format!("{completed}/{}", steps.len())),
            markdown_code(&status_progress_bar(completed, steps.len()))
        ),
        format!(
            "🧭 Plan {}/{} {}",
            completed,
            steps.len(),
            status_progress_bar(completed, steps.len())
        ),
    ));
    lines.push((
        format!(
            "🧩 Tasks {} · Active {} · Failed {} · Interrupted {} · Queue {}",
            markdown_code(&format!("{}/{}", tasks.saturating_sub(active_tasks), tasks)),
            markdown_code(&active_tasks.to_string()),
            markdown_code(&failed_tasks.to_string()),
            markdown_code(&interrupted_tasks.to_string()),
            markdown_code(&queue.to_string())
        ),
        format!(
            "🧩 Tasks {}/{} · Active {} · Failed {} · Interrupted {} · Queue {}",
            tasks.saturating_sub(active_tasks),
            tasks,
            active_tasks,
            failed_tasks,
            interrupted_tasks,
            queue
        ),
    ));
    if let Some(cwd) = projection
        .and_then(|value| value.cwd.as_deref())
        .filter(|value| !value.trim().is_empty())
    {
        lines.push((
            format!("📁 {}", markdown_code(&truncate_text(cwd))),
            format!("📁 {}", truncate_text(cwd)),
        ));
    }
    let updated_at_ms = projection
        .map(|value| value.updated_at_ms)
        .filter(|value| *value > 0)
        .unwrap_or(space.updated_at_ms);
    lines.push((
        format!(
            "🕒 更新 {} · 心跳 {}",
            markdown_code(&status_clock(updated_at_ms.max(0))),
            markdown_code(&format!("≤{HEARTBEAT_SECONDS}s"))
        ),
        format!(
            "🕒 更新 {} · 心跳 ≤{}s",
            status_clock(updated_at_ms.max(0)),
            HEARTBEAT_SECONDS
        ),
    ));
    StatusRendered {
        markdown: truncate_text(
            &lines
                .iter()
                .map(|(markdown, _)| markdown.as_str())
                .collect::<Vec<_>>()
                .join("\n"),
        ),
        plain: truncate_text(
            &lines
                .iter()
                .map(|(_, plain)| plain.as_str())
                .collect::<Vec<_>>()
                .join("\n"),
        ),
    }
}

fn status_is_terminal(space: &RustSessionSpace, projection: Option<&ThreadProjection>) -> bool {
    if space.lifecycle == "closed" {
        return true;
    }
    let Some(projection) = projection else {
        return false;
    };
    if projection
        .goal
        .as_ref()
        .and_then(|value| value.get("status"))
        .and_then(Value::as_str)
        != Some("complete")
    {
        return false;
    }
    // A completed Goal is not enough when the thread-level status is still
    // active. The Python dashboard keeps the status surface live until the
    // current turn reaches a terminal state.
    if projection.status.as_deref() == Some("active")
        && !matches!(
            projection.turn_status.as_deref(),
            Some("completed" | "failed" | "interrupted")
        )
    {
        return false;
    }
    if matches!(projection.turn_status.as_deref(), Some("inProgress"))
        || matches!(projection.review_status.as_deref(), Some("inProgress"))
    {
        return false;
    }
    !projection.subagents.values().any(|task| {
        matches!(
            task.get("status").and_then(Value::as_str),
            Some("pending" | "pendingInit" | "active" | "running" | "inProgress")
        )
    })
}

#[allow(clippy::too_many_arguments)]
async fn ensure_status_message(
    store: &SqliteStore,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    totp: &TotpManager,
    space: &RustSessionSpace,
    projection: Option<&ThreadProjection>,
    animation_frame: Option<u64>,
) -> Result<Option<RustSessionSpace>, String> {
    if space.discussion_chat_id.is_none() || space.discussion_root_message_id.is_none() {
        return Ok(Some(space.clone()));
    }
    let terminal = status_is_terminal(space, projection);
    let rendered = status_render(store, space, projection, None, totp, animation_frame);
    let semantic =
        status_semantic_fingerprint(&rendered.markdown, &space.lifecycle, terminal, false);

    if space.status_message_id.is_some() {
        let Some(preferred) = preferred_status_bot(bots_by_id) else {
            return Ok(Some(space.clone()));
        };
        if space.status_bot_instance.as_deref() == Some(preferred.config.instance_id.as_str()) {
            return Ok(Some(space.clone()));
        }
        let old_bot_instance = space.status_bot_instance.clone();
        let old_message_id = space.status_message_id;
        let preferred = preferred.clone();
        if !terminal {
            store
                .retire_status_callbacks(space.space_id.as_str(), space.generation)
                .map_err(|error| error.to_string())?;
        }
        let markup = if terminal {
            None
        } else {
            status_callback_markup(store, space, &[("取消关注", "space_unwatch")])?
        };
        let message = send_rendered_with_markup(
            &preferred,
            &surface_for(
                &preferred,
                config,
                space.discussion_chat_id.expect("checked above"),
                space.discussion_root_message_id,
            ),
            &rendered,
            markup,
            metrics,
        )
        .await?;
        let mut migrated = space.clone();
        migrated.status_message_id = Some(message.message_id);
        migrated.status_bot_instance = Some(preferred.config.instance_id.clone());
        migrated.updated_at_ms = now_ms();
        store
            .upsert_session_space(&migrated)
            .map_err(|error| error.to_string())?;
        if let (Some(old_bot_instance), Some(old_message_id)) = (old_bot_instance, old_message_id) {
            store
                .schedule_deletion(&ScheduledDeletion {
                    bot_instance_id: old_bot_instance,
                    chat_id: migrated.discussion_chat_id.expect("checked above"),
                    message_id: old_message_id,
                    group_key: format!("status-migration:{}", migrated.space_id),
                    delete_at_ms: now_ms().saturating_add(600_000),
                    attempts: 0,
                    claimed_at_ms: None,
                    last_error_class: None,
                })
                .map_err(|error| error.to_string())?;
        }
        store
            .set_telegram_fingerprint(
                &preferred.config.instance_id,
                migrated.discussion_chat_id.expect("checked above"),
                message.message_id,
                "status",
                &semantic,
                now_ms(),
            )
            .map_err(|error| error.to_string())?;
        return Ok(Some(migrated));
    }

    let Some(bot) = status_bot_for(space, bots_by_id) else {
        return Ok(None);
    };
    if !terminal {
        store
            .retire_status_callbacks(space.space_id.as_str(), space.generation)
            .map_err(|error| error.to_string())?;
    }
    let markup = if terminal {
        None
    } else {
        status_callback_markup(store, space, &[("取消关注", "space_unwatch")])?
    };
    let message = send_rendered_with_markup(
        bot,
        &surface_for(
            bot,
            config,
            space.discussion_chat_id.expect("checked above"),
            space.discussion_root_message_id,
        ),
        &rendered,
        markup,
        metrics,
    )
    .await?;
    let mut updated = space.clone();
    updated.status_message_id = Some(message.message_id);
    updated.status_bot_instance = Some(bot.config.instance_id.clone());
    updated.updated_at_ms = now_ms();
    store
        .upsert_session_space(&updated)
        .map_err(|error| error.to_string())?;
    store
        .set_telegram_fingerprint(
            &bot.config.instance_id,
            updated.discussion_chat_id.expect("checked above"),
            message.message_id,
            "status",
            &semantic,
            now_ms(),
        )
        .map_err(|error| error.to_string())?;
    Ok(Some(updated))
}

#[allow(clippy::too_many_arguments)]
async fn update_status_message(
    store: &SqliteStore,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    totp: &TotpManager,
    space: &RustSessionSpace,
    projection: Option<&ThreadProjection>,
    note: Option<&str>,
    force_refresh: bool,
    animation_frame: Option<u64>,
) -> Result<(), String> {
    update_status_message_with_edit_timeout(
        store,
        bots_by_id,
        config,
        metrics,
        totp,
        space,
        projection,
        note,
        force_refresh,
        animation_frame,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn update_status_message_with_edit_timeout(
    store: &SqliteStore,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    totp: &TotpManager,
    space: &RustSessionSpace,
    projection: Option<&ThreadProjection>,
    note: Option<&str>,
    force_refresh: bool,
    animation_frame: Option<u64>,
    edit_timeout: Option<Duration>,
) -> Result<(), String> {
    let Some(current) = ensure_status_message(
        store,
        bots_by_id,
        config,
        metrics,
        totp,
        space,
        projection,
        animation_frame,
    )
    .await?
    else {
        return Ok(());
    };
    let Some(message_id) = current.status_message_id else {
        return Ok(());
    };
    let Some(bot) = status_bot_for(&current, bots_by_id) else {
        return Ok(());
    };
    let terminal = status_is_terminal(&current, projection);
    let rendered = status_render(store, &current, projection, note, totp, animation_frame);
    let semantic =
        status_semantic_fingerprint(&rendered.markdown, &current.lifecycle, terminal, false);
    let discussion_chat_id = current
        .discussion_chat_id
        .unwrap_or(config.discussion_chat_id);
    let status_changed = force_refresh
        || store
            .telegram_fingerprint(
                &bot.config.instance_id,
                discussion_chat_id,
                message_id,
                "status",
            )
            .map_err(|error| error.to_string())?
            .as_deref()
            != Some(semantic.as_str());
    let dashboard_rendered =
        channel_status_render(store, &current, projection, totp, animation_frame);
    let dashboard_semantic = status_semantic_fingerprint(
        &dashboard_rendered.markdown,
        &current.lifecycle,
        terminal,
        false,
    );
    let control_bot = bots_by_id
        .values()
        .find(|candidate| candidate.role == RuntimeBotRole::Control);
    let dashboard_changed = force_refresh
        || control_bot.is_some_and(|control| {
            store
                .telegram_fingerprint(
                    &control.config.instance_id,
                    current.channel_chat_id,
                    current.channel_post_id,
                    "dashboard",
                )
                .ok()
                .flatten()
                .as_deref()
                != Some(dashboard_semantic.as_str())
        });
    if !status_changed && !dashboard_changed && note.is_none() {
        return Ok(());
    }
    if let Some(control) = control_bot
        && current.channel_post_id > 0
        && dashboard_changed
    {
        let reference = TelegramMessageReference::new(
            current.channel_chat_id.to_string(),
            current.channel_post_id,
        )
        .map_err(|error| error.to_string())?;
        if let Err(error) = edit_rendered_with_markup(
            control,
            &reference,
            &dashboard_rendered,
            None,
            metrics,
            edit_timeout,
        )
        .await
        {
            eprintln!("rust bridge channel dashboard update failed: {error}");
        } else {
            store
                .set_telegram_fingerprint(
                    &control.config.instance_id,
                    current.channel_chat_id,
                    current.channel_post_id,
                    "dashboard",
                    &dashboard_semantic,
                    now_ms(),
                )
                .map_err(|error| error.to_string())?;
        }
    }
    if !status_changed && note.is_none() {
        return Ok(());
    }
    if terminal {
        store
            .retire_status_callbacks(&current.space_id, current.generation)
            .map_err(|error| error.to_string())?;
    }
    let retired_at = if terminal {
        None
    } else {
        Some(
            store
                .retire_status_callbacks_at(&current.space_id, current.generation)
                .map_err(|error| error.to_string())?,
        )
    };
    let markup = if terminal {
        None
    } else {
        status_callback_markup(store, &current, &[("取消关注", "space_unwatch")])?
    };
    let edit_result = edit_rendered_with_markup(
        bot,
        &TelegramMessageReference::new(
            current
                .discussion_chat_id
                .unwrap_or(config.discussion_chat_id)
                .to_string(),
            message_id,
        )
        .map_err(|error| error.to_string())?,
        &rendered,
        markup,
        metrics,
        edit_timeout,
    )
    .await;
    if let Err(error) = edit_result {
        if let Some(retired_at) = retired_at {
            let _ =
                store.restore_status_callbacks(&current.space_id, current.generation, retired_at);
        }
        return Err(error);
    }
    store
        .set_telegram_fingerprint(
            &bot.config.instance_id,
            discussion_chat_id,
            message_id,
            "status",
            &semantic,
            now_ms(),
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_status_callback(
    nonce: &str,
    callback: codex_telegram_adapter::TelegramCallback,
    inbound_bot: RuntimeBot,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    store: &Arc<SqliteStore>,
    agent: &AppServerClient,
    sessions: &Arc<SessionRegistry>,
    metrics: &MetricsRegistry,
    totp: &TotpManager,
) -> Result<(), String> {
    if !matches!(
        inbound_bot.role,
        RuntimeBotRole::Status | RuntimeBotRole::Discussion
    ) {
        acknowledge_callback(&inbound_bot, &callback, Some("该按钮不属于状态 Bot")).await;
        return Ok(());
    }
    let owner_user_id = store
        .workflow_record("onboarding", "owner")
        .map_err(|error| error.to_string())?
        .and_then(|value| value.get("user_id").and_then(Value::as_i64));
    if owner_user_id.is_none() || callback.actor.user_id != owner_user_id {
        acknowledge_callback(&inbound_bot, &callback, Some("无权操作此状态按钮")).await;
        return Ok(());
    }
    let expected_surface = if inbound_bot.role == RuntimeBotRole::Status {
        "status"
    } else {
        "discussion"
    };
    if store
        .callback_surface(nonce, now_ms())
        .map_err(|error| error.to_string())?
        .as_deref()
        != Some(expected_surface)
    {
        acknowledge_callback(&inbound_bot, &callback, Some("按钮不属于状态面板")).await;
        return Ok(());
    }
    let Some(preview) = store
        .peek_callback(nonce, now_ms())
        .map_err(|error| error.to_string())?
    else {
        acknowledge_callback(&inbound_bot, &callback, Some("按钮已过期或已处理")).await;
        return Ok(());
    };
    let action: StoredStatusAction = serde_json::from_str(&preview.action)
        .map_err(|error| format!("status callback payload invalid: {error}"))?;
    let Some(space) = store
        .get_session_space(&action.space_id)
        .map_err(|error| error.to_string())?
    else {
        acknowledge_callback(&inbound_bot, &callback, Some("Session 已不存在")).await;
        return Ok(());
    };
    let expected_status_chat = space
        .discussion_chat_id
        .unwrap_or(config.discussion_chat_id);
    if callback.chat_id != expected_status_chat
        || (inbound_bot.role == RuntimeBotRole::Status
            && space.status_message_id != Some(callback.message_id))
    {
        acknowledge_callback(&inbound_bot, &callback, Some("按钮不属于当前状态消息")).await;
        return Ok(());
    }
    let session = if action.thread_id.trim().is_empty() {
        None
    } else {
        sessions.by_thread(&action.thread_id)
    };
    let expected_chat_id = session
        .as_ref()
        .map(|value| value.chat_id)
        .or(space.discussion_chat_id)
        .unwrap_or(config.discussion_chat_id);
    if preview.space_id != action.space_id
        || callback.chat_id != expected_chat_id
        || !is_status_action(&action.action)
    {
        acknowledge_callback(&inbound_bot, &callback, Some("按钮不属于当前 Session")).await;
        return Ok(());
    }
    if action.action == "status_unwatch_execute"
        && !totp
            .is_unlocked_for_space(&space.space_id, now_ms())
            .map_err(|error| error.to_string())?
    {
        acknowledge_callback(&inbound_bot, &callback, Some("请先完成 TOTP 解锁")).await;
        return send_text(
            &inbound_bot,
            &surface_for(&inbound_bot, config, callback.chat_id, None),
            LOCKED_WRITE_MESSAGE,
            metrics,
        )
        .await;
    }
    let Some(_consumed) = store
        .take_callback_scoped(
            nonce,
            now_ms(),
            Some(&action.space_id),
            Some(space.generation),
        )
        .map_err(|error| error.to_string())?
    else {
        acknowledge_callback(&inbound_bot, &callback, Some("按钮已过期或已处理")).await;
        return Ok(());
    };
    let bot = if inbound_bot.role == RuntimeBotRole::Discussion {
        &inbound_bot
    } else {
        let Some(status_bot) = status_bot_for(&space, bots_by_id).or(Some(&inbound_bot)) else {
            return Ok(());
        };
        status_bot
    };
    match action.action.as_str() {
        "space_refresh" => {
            if let Some(session) = session.as_ref() {
                agent
                    .resume_thread(&session.thread_id)
                    .await
                    .map_err(|error| error.to_string())?;
            }
            acknowledge_callback(&inbound_bot, &callback, Some("已刷新")).await;
            update_status_message(
                store,
                bots_by_id,
                config,
                metrics,
                totp,
                &space,
                None,
                Some("已从 Codex 刷新 Session 状态。"),
                true,
                None,
            )
            .await
        }
        "space_unwatch" => {
            if inbound_bot.role == RuntimeBotRole::Status {
                store
                    .retire_status_callbacks(&space.space_id, space.generation)
                    .map_err(|error| error.to_string())?;
            }
            let rows = &[
                &[("确认取消关注", "status_unwatch_execute")][..],
                &[("返回", "status_unwatch_cancel")][..],
            ];
            let markup = if inbound_bot.role == RuntimeBotRole::Discussion {
                discussion_callback_markup_rows(store, &space, rows)?
            } else {
                status_callback_markup_rows(store, &space, rows)?
            };
            acknowledge_callback(&inbound_bot, &callback, Some("请确认")).await;
            edit_text_with_markup(
                bot,
                &TelegramMessageReference::new(callback.chat_id.to_string(), callback.message_id)
                    .map_err(|error| error.to_string())?,
                UNWATCH_CONFIRM_MESSAGE,
                markup,
                metrics,
            )
            .await
        }
        "status_unwatch_cancel" => {
            acknowledge_callback(&inbound_bot, &callback, Some("已取消")).await;
            if inbound_bot.role == RuntimeBotRole::Discussion {
                return edit_text_with_markup(
                    &inbound_bot,
                    &TelegramMessageReference::new(
                        callback.chat_id.to_string(),
                        callback.message_id,
                    )
                    .map_err(|error| error.to_string())?,
                    UNWATCH_CANCEL_MESSAGE,
                    None,
                    metrics,
                )
                .await;
            }
            update_status_message(
                store,
                bots_by_id,
                config,
                metrics,
                totp,
                &space,
                None,
                Some(UNWATCH_CANCEL_MESSAGE),
                true,
                None,
            )
            .await
        }
        "status_unwatch_execute" => {
            let Some(_closed) = store
                .close_session_space(&space.space_id, space.generation, now_ms())
                .map_err(|error| error.to_string())?
            else {
                acknowledge_callback(&inbound_bot, &callback, Some("Session 状态已变化")).await;
                return Ok(());
            };
            if let Some(thread_id) = space.thread_id.as_deref() {
                sessions.remove(thread_id);
            }
            acknowledge_callback(&inbound_bot, &callback, Some("已取消关注")).await;
            edit_text_with_markup(
                bot,
                &TelegramMessageReference::new(callback.chat_id.to_string(), callback.message_id)
                    .map_err(|error| error.to_string())?,
                UNWATCH_CLOSED_MESSAGE,
                None,
                metrics,
            )
            .await
        }
        _ => {
            acknowledge_callback(&inbound_bot, &callback, Some("未知状态操作")).await;
            Ok(())
        }
    }
}

fn parse_plan_callback(data: &str) -> Option<(&str, &str)> {
    let mut fields = data.split(':');
    if fields.next()? != "rp" {
        return None;
    }
    let nonce = fields.next()?.trim();
    let decision = fields.next()?.trim();
    (!nonce.is_empty() && matches!(decision, "execute" | "revise")).then_some((nonce, decision))
}

fn plan_status_label(status: &PlanPublicationState, turn_id: Option<&TurnId>) -> String {
    let base = match status {
        PlanPublicationState::Published => "状态：等待 Telegram 选择。",
        PlanPublicationState::Executing => "⏳ 状态：已在 Telegram 批准，正在启动执行。",
        PlanPublicationState::Revising => "📝 状态：已选择继续完善计划。",
        PlanPublicationState::RevisionStarted => "📝 状态：已提交继续完善请求。",
        PlanPublicationState::Executed => "✅ 状态：已批准并开始执行。",
        PlanPublicationState::Failed => "❌ 状态：Plan 执行失败。",
        PlanPublicationState::Dismissed => "状态：Plan 已取消。",
        PlanPublicationState::Superseded => "↪ 状态：已被更新版本替代。",
    };
    match turn_id {
        Some(turn_id) => {
            let short_turn_id = turn_id.as_str().chars().take(8).collect::<String>();
            format!("{base} Turn {short_turn_id}")
        }
        None => base.to_owned(),
    }
}

async fn edit_plan_publication(
    bot: &RuntimeBot,
    session: &SessionRecord,
    _config: &RustConfig,
    metrics: &MetricsRegistry,
    publication: &PlanPublication,
) -> Result<(), String> {
    let Some(message_id) = publication
        .message_ids
        .last()
        .copied()
        .or_else(|| publication.action_message_ids.last().copied())
    else {
        return Ok(());
    };
    let text = format!(
        "📋 Codex Plan\n\n{}\n\n{}",
        truncate_text(&publication.plan_text),
        plan_status_label(&publication.status, publication.decision_turn_id.as_ref()),
    );
    edit_text_with_markup(
        bot,
        &TelegramMessageReference::new(session.chat_id.to_string(), message_id)
            .map_err(|error| error.to_string())?,
        &text,
        None,
        metrics,
    )
    .await
}

fn update_plan_publication(
    store: &SqliteStore,
    publication: &mut PlanPublication,
    status: PlanPublicationState,
    decision_turn_id: Option<TurnId>,
) -> Result<(), String> {
    publication.status = status;
    publication.decision_turn_id = decision_turn_id;
    publication.updated_at_ms = now_ms();
    store
        .upsert_plan_publication(publication)
        .map_err(|error| error.to_string())?;
    let key = plan_publication_key(
        &publication.space_id,
        publication.generation,
        &publication.item_id,
        &publication.revision_key,
    );
    store
        .upsert_workflow_record(
            "plan",
            &key,
            &serde_json::to_value(&*publication).map_err(|error| error.to_string())?,
            now_ms(),
        )
        .map_err(|error| error.to_string())
}

#[allow(clippy::too_many_arguments)]
async fn handle_plan_callback(
    nonce: &str,
    decision: &str,
    callback: codex_telegram_adapter::TelegramCallback,
    inbound_bot: RuntimeBot,
    config: &RustConfig,
    store: &Arc<SqliteStore>,
    agent: &AppServerClient,
    sessions: &Arc<SessionRegistry>,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    if !workflow_callback_bot_allowed(&inbound_bot)
        || !workflow_callback_owner_authorized(store, &callback)?
    {
        acknowledge_callback(&inbound_bot, &callback, Some("无权操作此 Plan 按钮")).await;
        return Ok(());
    }
    acknowledge_callback(&inbound_bot, &callback, Some("正在处理 Plan")).await;
    let Some(preview) = store
        .peek_callback(nonce, now_ms())
        .map_err(|error| error.to_string())?
    else {
        return send_text(
            &inbound_bot,
            &surface_for(&inbound_bot, config, callback.chat_id, None),
            "这个 Plan 按钮已过期或已经处理。",
            metrics,
        )
        .await;
    };
    let action: StoredPlanAction =
        serde_json::from_str(&preview.action).map_err(|error| error.to_string())?;
    if action.decision != decision || action.generation != agent.connection_state().generation {
        return send_text(
            &inbound_bot,
            &surface_for(&inbound_bot, config, callback.chat_id, None),
            "Plan 已过期或 Codex 连接已经重建。",
            metrics,
        )
        .await;
    }
    let Some(session) = sessions.by_thread(&action.thread_id) else {
        return send_text(
            &inbound_bot,
            &surface_for(&inbound_bot, config, callback.chat_id, None),
            "当前 Session 已不再由 Rust Bridge 管理。",
            metrics,
        )
        .await;
    };
    if preview.space_id != action.space_id || callback.chat_id != session.chat_id {
        return send_text(
            &inbound_bot,
            &surface_for(&inbound_bot, config, callback.chat_id, None),
            "Plan 按钮不属于当前 Session。",
            metrics,
        )
        .await;
    }
    let Some(_stored) = store
        .take_callback_scoped(
            nonce,
            now_ms(),
            Some(&action.space_id),
            Some(
                i64::try_from(action.generation)
                    .map_err(|_| "Plan generation exceeds SQLite range")?,
            ),
        )
        .map_err(|error| error.to_string())?
    else {
        return send_text(
            &inbound_bot,
            &surface_for(&inbound_bot, config, callback.chat_id, None),
            "这个 Plan 按钮已过期或已经处理。",
            metrics,
        )
        .await;
    };
    let publication_key = plan_publication_key(
        &action.space_id,
        action.generation,
        &action.item_id,
        &action.revision_key,
    );
    let publication = store
        .workflow_record("plan", &publication_key)
        .or_else(|_| {
            store.workflow_record(
                "plan",
                &format!(
                    "{}:{}:{}",
                    action.space_id, action.generation, action.item_id
                ),
            )
        })
        .map_err(|error| error.to_string())?
        .and_then(|value| serde_json::from_value::<PlanPublication>(value).ok());
    let mut publication = publication;
    if publication
        .as_ref()
        .is_none_or(|current| current.status != PlanPublicationState::Published)
    {
        return send_text(
            &inbound_bot,
            &surface_for(
                &inbound_bot,
                config,
                callback.chat_id,
                session.root_message_id,
            ),
            "这个 Plan 已过期或已经处理。",
            metrics,
        )
        .await;
    }
    if decision == "execute" {
        if let Some(current) = publication.as_mut() {
            if let Err(error) =
                update_plan_publication(store, current, PlanPublicationState::Executing, None)
            {
                let _ = store.restore_callback(nonce);
                return Err(error);
            }
            let _ = edit_plan_publication(&inbound_bot, &session, config, metrics, current).await;
        }
        let prompt = "请按照刚才发布的 Plan 开始执行，完成后报告结果。";
        let input = vec![PromptInput::text(prompt).map_err(|error| error.to_string())?];
        let result = if let Some(turn_id) = session.turn_id.as_ref() {
            agent
                .steer_turn(
                    &session.thread_id,
                    turn_id,
                    input,
                    Some(&format!("plan-execute-{}", action.item_id)),
                )
                .await
                .map(|id| AgentTurn {
                    id,
                    thread_id: session.thread_id.clone(),
                    status: "inProgress".into(),
                })
        } else {
            agent
                .start_turn(
                    &session.thread_id,
                    input,
                    Some(&format!("plan-execute-{}", action.item_id)),
                )
                .await
        };
        let turn = match result {
            Ok(turn) => turn,
            Err(error) => {
                let _ = store.restore_callback(nonce);
                if let Some(current) = publication.as_mut() {
                    update_plan_publication(store, current, PlanPublicationState::Published, None)?;
                    let _ = edit_plan_publication(&inbound_bot, &session, config, metrics, current)
                        .await;
                }
                return send_text(
                    &inbound_bot,
                    &surface_for(
                        &inbound_bot,
                        config,
                        callback.chat_id,
                        session.root_message_id,
                    ),
                    &format!("Plan 执行未送达 Codex：{error}"),
                    metrics,
                )
                .await;
            }
        };
        sessions.set_turn(session.thread_id.as_str(), Some(turn.id.clone()));
        if let Some(current) = publication.as_mut() {
            current.decision_turn_id = Some(turn.id.clone());
            if let Err(error) = update_plan_publication(
                store,
                current,
                PlanPublicationState::Executing,
                Some(turn.id.clone()),
            ) {
                let _ = store.restore_callback(nonce);
                return Err(error);
            }
            let _ = edit_plan_publication(&inbound_bot, &session, config, metrics, current).await;
        }
        send_text(
            &inbound_bot,
            &surface_for(
                &inbound_bot,
                config,
                callback.chat_id,
                session.root_message_id,
            ),
            "✅ 已批准 Plan，正在启动执行。",
            metrics,
        )
        .await
    } else {
        if let Some(current) = publication.as_mut() {
            if let Err(error) =
                update_plan_publication(store, current, PlanPublicationState::Revising, None)
            {
                let _ = store.restore_callback(nonce);
                return Err(error);
            }
            let _ = edit_plan_publication(&inbound_bot, &session, config, metrics, current).await;
        }
        let prompt_message = send_text_with_markup_message(
            &inbound_bot,
            &surface_for(
                &inbound_bot,
                config,
                callback.chat_id,
                session.root_message_id,
            ),
            "请回复这条消息，说明需要如何继续完善 Plan。",
            Some(json!({
                "force_reply": true,
                "selective": true,
                "input_field_placeholder": "输入 Plan 修改意见"
            })),
            metrics,
        )
        .await;
        match prompt_message {
            Ok(prompt_message) => {
                if let Some(current) = publication.as_mut() {
                    current.revision_prompt_message_id = Some(prompt_message.message_id);
                    if let Err(error) = update_plan_publication(
                        store,
                        current,
                        PlanPublicationState::Revising,
                        None,
                    ) {
                        let _ = store.restore_callback(nonce);
                        return Err(error);
                    }
                }
                send_text(
                    &inbound_bot,
                    &surface_for(
                        &inbound_bot,
                        config,
                        callback.chat_id,
                        session.root_message_id,
                    ),
                    "📝 已选择继续完善计划，请回复上面的消息发送修改意见。",
                    metrics,
                )
                .await
            }
            Err(error) => {
                let _ = store.restore_callback(nonce);
                if let Some(current) = publication.as_mut() {
                    update_plan_publication(store, current, PlanPublicationState::Published, None)?;
                    let _ = edit_plan_publication(&inbound_bot, &session, config, metrics, current)
                        .await;
                }
                Err(format!("无法创建 Plan 修改请求：{error}"))
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_question_callback(
    nonce: &str,
    callback: codex_telegram_adapter::TelegramCallback,
    inbound_bot: RuntimeBot,
    config: &RustConfig,
    store: &Arc<SqliteStore>,
    agent: &AppServerClient,
    sessions: &Arc<SessionRegistry>,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    if !workflow_callback_bot_allowed(&inbound_bot)
        || !workflow_callback_owner_authorized(store, &callback)?
    {
        acknowledge_callback(&inbound_bot, &callback, Some("无权操作此问题按钮")).await;
        return Ok(());
    }
    acknowledge_callback(&inbound_bot, &callback, Some("已选择")).await;
    let Some(preview) = store
        .peek_callback(nonce, now_ms())
        .map_err(|error| error.to_string())?
    else {
        return send_text(
            &inbound_bot,
            &surface_for(&inbound_bot, config, callback.chat_id, None),
            "问题按钮已过期或已经处理。",
            metrics,
        )
        .await;
    };
    let action: StoredQuestionAction =
        serde_json::from_str(&preview.action).map_err(|error| error.to_string())?;
    if action.generation != agent.connection_state().generation {
        return send_text(
            &inbound_bot,
            &surface_for(&inbound_bot, config, callback.chat_id, None),
            "Codex 连接已经重建，原问题已失效。",
            metrics,
        )
        .await;
    }
    let Some(session) = sessions.by_thread(&action.thread_id) else {
        return send_text(
            &inbound_bot,
            &surface_for(&inbound_bot, config, callback.chat_id, None),
            "当前 Session 已不再由 Rust Bridge 管理。",
            metrics,
        )
        .await;
    };
    if preview.space_id != action.space_id || callback.chat_id != session.chat_id {
        return send_text(
            &inbound_bot,
            &surface_for(&inbound_bot, config, callback.chat_id, None),
            "问题按钮不属于当前 Session。",
            metrics,
        )
        .await;
    }
    let Some(_stored) = store
        .take_callback_scoped(
            nonce,
            now_ms(),
            Some(&action.space_id),
            Some(
                i64::try_from(action.generation)
                    .map_err(|_| "question generation exceeds SQLite range")?,
            ),
        )
        .map_err(|error| error.to_string())?
    else {
        return send_text(
            &inbound_bot,
            &surface_for(&inbound_bot, config, callback.chat_id, None),
            "问题按钮已过期或已经处理。",
            metrics,
        )
        .await;
    };
    answer_question(
        agent,
        store,
        &inbound_bot,
        config,
        metrics,
        &session,
        &action.request_key,
        &action.question_id,
        &action.answer,
        Some(action.question_index),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn handle_server_requests(
    agent: AppServerClient,
    store: Arc<SqliteStore>,
    sessions: Arc<SessionRegistry>,
    bots_by_id: HashMap<String, RuntimeBot>,
    config: RustConfig,
    metrics: MetricsRegistry,
) {
    let mut requests = agent.subscribe_server_requests();
    loop {
        let Some(request) = requests.recv().await else {
            return;
        };
        if let Err(error) = handle_server_request(
            request,
            &agent,
            &store,
            &sessions,
            &bots_by_id,
            &config,
            &metrics,
        )
        .await
        {
            eprintln!("rust bridge server request failed: {error}");
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn present_user_input_request(
    request: AgentServerRequest,
    agent: &AppServerClient,
    store: &Arc<SqliteStore>,
    session: &SessionRecord,
    space: &RustSessionSpace,
    bot: &RuntimeBot,
    config: &RustConfig,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    let questions = request
        .params
        .get("questions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|value| value.is_object())
        .collect::<Vec<_>>();
    if questions.is_empty() {
        agent
            .respond_error(
                request.id,
                -32602,
                "Codex user-input request has no questions",
            )
            .await
            .map_err(|error| error.to_string())?;
        return Ok(());
    }
    if questions.iter().any(|value| {
        value
            .get("isSecret")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }) {
        let _ = send_text(
            bot,
            &surface_for(bot, config, session.chat_id, session.root_message_id),
            "Codex 正在请求敏感输入；为避免泄露，请回到本机 Codex 客户端回答。",
            metrics,
        )
        .await;
        return Ok(());
    }
    let turn_id = request
        .params
        .get("turnId")
        .and_then(Value::as_str)
        .or_else(|| session.turn_id.as_ref().map(TurnId::as_str))
        .ok_or_else(|| "Codex user-input request did not identify turnId".to_owned())?;
    let turn_id = TurnId::new(turn_id).map_err(|error| error.to_string())?;
    let item_id = request
        .params
        .get("itemId")
        .or_else(|| request.params.get("callId"))
        .and_then(Value::as_str)
        .unwrap_or("user-input")
        .to_owned();
    let questions = normalize_question_values(&questions);
    let request_key = stable_question_request_key(&request, session.thread_id.as_str(), &item_id);
    let expires_at_ms = request
        .params
        .get("autoResolutionMs")
        .and_then(Value::as_i64)
        .map(|value| now_ms().saturating_add(value.max(0)))
        .or_else(|| Some(now_ms().saturating_add(APPROVAL_CALLBACK_TTL_MS)));
    let mut stored = StoredWorkflowQuestion {
        request_key: request_key.clone(),
        request_id: request.id.clone(),
        generation: request.generation,
        thread_id: session.thread_id.to_string(),
        turn_id: turn_id.to_string(),
        item_id: item_id.clone(),
        questions: Value::Array(questions.clone()),
        answers: HashMap::new(),
        current_index: 0,
        message_ids: Vec::new(),
        summary_message_id: None,
        status: "pending".into(),
        expires_at_ms,
    };
    let question_request = QuestionRequest {
        request_key: request_key.clone(),
        request_id: request.id.clone(),
        generation: request.generation,
        thread_id: session.thread_id.clone(),
        turn_id: turn_id.clone(),
        item_id: item_id.clone(),
        questions: Value::Array(questions.clone()),
        expires_at_ms,
    };
    store
        .upsert_question(&question_request)
        .map_err(|error| error.to_string())?;
    store
        .upsert_workflow_record(
            "question",
            &request_key,
            &serde_json::to_value(&stored).map_err(|error| error.to_string())?,
            now_ms(),
        )
        .map_err(|error| error.to_string())?;
    let header = send_text_message(
        bot,
        &surface_for(bot, config, session.chat_id, session.root_message_id),
        &format!(
            "❓ Codex 请求输入\nSession {}\n请按顺序回答下面的问题。",
            session.thread_id
        ),
        metrics,
    )
    .await;
    let header = match header {
        Ok(message) => message,
        Err(error) => {
            let _ = store.upsert_workflow_record(
                "question",
                &request_key,
                &json!({
                    "request_key": request_key,
                    "request_id": request.id.clone(),
                    "generation": request.generation,
                    "thread_id": session.thread_id.to_string(),
                    "turn_id": turn_id.to_string(),
                    "item_id": item_id.clone(),
                    "questions": questions,
                    "answers": {},
                    "current_index": 0,
                    "message_ids": [],
                    "summary_message_id": null,
                    "status": "failed",
                    "expires_at_ms": expires_at_ms,
                }),
                now_ms(),
            );
            agent
                .respond_error(request.id, -32001, "Rust Bridge could not deliver the user-input prompt to Telegram")
                .await
                .map_err(|response_error| format!("Telegram question delivery failed: {error}; app-server response failed: {response_error}"))?;
            return Err(error);
        }
    };
    stored.summary_message_id = Some(header.message_id);
    stored.message_ids.push(header.message_id);
    store
        .upsert_workflow_record(
            "question",
            &request_key,
            &serde_json::to_value(&stored).map_err(|error| error.to_string())?,
            now_ms(),
        )
        .map_err(|error| error.to_string())?;
    if let Err(error) =
        render_user_question_prompt(agent, store, &stored, session, space, bot, config, metrics)
            .await
    {
        let _ = agent
            .respond_error(
                request.id,
                -32001,
                "Rust Bridge could not deliver the user-input prompt to Telegram",
            )
            .await;
        return Err(error);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn render_user_question_prompt(
    _agent: &AppServerClient,
    store: &SqliteStore,
    question: &StoredWorkflowQuestion,
    session: &SessionRecord,
    space: &RustSessionSpace,
    bot: &RuntimeBot,
    config: &RustConfig,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    let questions = question
        .questions
        .as_array()
        .ok_or_else(|| "question payload is not an array".to_owned())?;
    let index = question.current_index;
    let value = questions
        .get(index)
        .ok_or_else(|| "question cursor is outside the request".to_owned())?;
    let question_id = question_id_at(value, index);
    let header = value
        .get("header")
        .or_else(|| value.get("title"))
        .and_then(Value::as_str)
        .unwrap_or("Codex 请求输入");
    let prompt = value
        .get("question")
        .or_else(|| value.get("prompt"))
        .and_then(Value::as_str)
        .unwrap_or("请选择一个回答");
    let mut lines = vec![
        format!(
            "❓ 第 {}/{} 题 · {}",
            index + 1,
            questions.len(),
            truncate_text(header)
        ),
        truncate_text(prompt),
    ];
    let mut rows = Vec::new();
    if let Some(options) = value.get("options").and_then(Value::as_array) {
        for option in options {
            let Some(label) = option
                .get("label")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|label| !label.is_empty())
            else {
                continue;
            };
            let nonce = next_approval_nonce();
            let action = StoredQuestionAction {
                request_key: question.request_key.clone(),
                request_id: question.request_id.clone(),
                generation: question.generation,
                thread_id: question.thread_id.clone(),
                space_id: space.space_id.clone(),
                question_id: question_id.clone(),
                question_index: index,
                answer: label.to_owned(),
            };
            store
                .create_callback(&StoredCallback {
                    nonce: nonce.clone(),
                    space_id: space.space_id.clone(),
                    generation: i64::try_from(question.generation)
                        .map_err(|_| "question generation exceeds SQLite range")?,
                    action: serde_json::to_string(&action).map_err(|error| error.to_string())?,
                    expires_at_ms: question
                        .expires_at_ms
                        .unwrap_or(now_ms() + APPROVAL_CALLBACK_TTL_MS),
                })
                .map_err(|error| error.to_string())?;
            let description = option
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if description.is_empty() {
                lines.push(format!("• {}", truncate_text(label)));
            } else {
                lines.push(format!(
                    "• {} — {}",
                    truncate_text(label),
                    truncate_text(description)
                ));
            }
            rows.push(vec![json!({
                "text": truncate_text(label),
                "callback_data": format!("rq:{nonce}")
            })]);
        }
    }
    lines.push(format!(
        "可用命令：/answer {} {} | <回答>",
        question.request_key, question_id
    ));
    let message = send_text_with_markup_message(
        bot,
        &surface_for(bot, config, session.chat_id, session.root_message_id),
        &lines.join("\n"),
        (!rows.is_empty()).then(|| json!({"inline_keyboard": rows})),
        metrics,
    )
    .await?;
    let mut updated = question.clone();
    updated.message_ids.push(message.message_id);
    let updated_key = updated.request_key.clone();
    let updated_value = serde_json::to_value(&updated).map_err(|error| error.to_string())?;
    store
        .upsert_workflow_record("question", &updated_key, &updated_value, now_ms())
        .map_err(|error| error.to_string())
}

async fn cleanup_question_messages(
    bot: &RuntimeBot,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    question: &StoredWorkflowQuestion,
    session: &SessionRecord,
) -> Result<(), String> {
    let chat_id = session.chat_id.to_string();
    let summary = question.summary_message_id;
    for message_id in question.message_ids.iter().copied() {
        if Some(message_id) == summary {
            edit_text_message(
                bot,
                &TelegramMessageReference::new(chat_id.clone(), message_id)
                    .map_err(|error| error.to_string())?,
                "✅ Codex 输入已提交。",
                metrics,
            )
            .await?;
            continue;
        }
        let api = bot.api.clone();
        let token = bot.token.clone();
        let reference = TelegramMessageReference::new(chat_id.clone(), message_id)
            .map_err(|error| error.to_string())?;
        let _ = tokio::task::spawn_blocking(move || api.delete_message(&token, &reference)).await;
    }
    let _ = config;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_server_request(
    request: AgentServerRequest,
    agent: &AppServerClient,
    store: &Arc<SqliteStore>,
    sessions: &Arc<SessionRegistry>,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    let supported = matches!(
        request.method.as_str(),
        "item/commandExecution/requestApproval"
            | "item/fileChange/requestApproval"
            | "item/permissions/requestApproval"
            | "execCommandApproval"
            | "applyPatchApproval"
            | "item/tool/requestUserInput"
    );
    let normalized = normalize_server_request_params(&request.method, &request.params);
    let thread_id = normalized
        .get("threadId")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned);
    if !supported {
        let message = "Rust Bridge does not support this Codex server request";
        agent
            .respond_error(request.id, -32601, message)
            .await
            .map_err(|error| error.to_string())?;
        return Ok(());
    }
    let Some(thread_id) = thread_id else {
        agent
            .respond_error(
                request.id,
                -32600,
                "Interactive request did not identify a managed Codex thread",
            )
            .await
            .map_err(|error| error.to_string())?;
        return Ok(());
    };
    let Some(session) = sessions.by_thread(&thread_id) else {
        agent
            .respond_error(
                request.id,
                -32600,
                "Interactive requests are disabled for unmanaged sessions",
            )
            .await
            .map_err(|error| error.to_string())?;
        return Ok(());
    };
    let Some(bot) = bots_by_id.get(&session.sender_instance_id) else {
        agent
            .respond_error(request.id, -32000, "No Telegram delivery Bot is available")
            .await
            .map_err(|error| error.to_string())?;
        return Ok(());
    };
    let Some(space) = store
        .session_space_for_thread(&thread_id)
        .map_err(|error| error.to_string())?
    else {
        agent
            .respond_error(
                request.id,
                -32600,
                "Managed thread has no durable Telegram session space",
            )
            .await
            .map_err(|error| error.to_string())?;
        return Ok(());
    };
    if request.method == "item/tool/requestUserInput" {
        return present_user_input_request(
            request, agent, store, &session, &space, bot, config, metrics,
        )
        .await;
    }
    let decisions = available_approval_decisions(&request.method, &normalized);
    if decisions.is_empty() {
        agent
            .respond_error(
                request.id,
                -32602,
                "No Telegram-safe approval decision is available",
            )
            .await
            .map_err(|error| error.to_string())?;
        return Ok(());
    }
    let session_id = ensure_approval_session(store, &thread_id, now_ms())?;
    let request_nonce = next_approval_nonce();
    let approval_id =
        ApprovalId::new(format!("approval-{request_nonce}")).map_err(|error| error.to_string())?;
    let approval = ApprovalRequest::pending(
        approval_id.clone(),
        session_id,
        approval_action(&request.method, &normalized),
        now_ms(),
    );
    let requested_event = DomainEvent {
        id: EventId::new(format!("approval-requested-{request_nonce}"))
            .map_err(|error| error.to_string())?,
        occurred_at_ms: now_ms(),
        kind: DomainEventKind::ApprovalRequested {
            approval: approval.clone(),
        },
    };
    store
        .insert_approval(&approval, &requested_event)
        .map_err(|error| error.to_string())?;

    let mut buttons = Vec::with_capacity(decisions.len());
    let mut callback_nonces = Vec::with_capacity(decisions.len());
    for decision in decisions {
        let nonce = next_approval_nonce();
        let response = approval_response_payload(&request.method, &decision, &normalized)?;
        let action = StoredApprovalAction {
            request_id: request.id.clone(),
            generation: request.generation,
            method: request.method.clone(),
            thread_id: thread_id.clone(),
            approval_id: approval_id.to_string(),
            decision: Value::String(decision.clone()),
            response,
        };
        let action_json = serde_json::to_string(&action).map_err(|error| error.to_string())?;
        store
            .create_callback(&StoredCallback {
                nonce: nonce.clone(),
                space_id: space.space_id.clone(),
                generation: i64::try_from(request.generation)
                    .map_err(|_| "app-server generation exceeds SQLite range".to_owned())?,
                action: action_json,
                expires_at_ms: now_ms() + APPROVAL_CALLBACK_TTL_MS,
            })
            .map_err(|error| error.to_string())?;
        buttons.push(json!({
            "text": approval_button_label(&decision),
            "callback_data": format!("ra:{nonce}:{decision}"),
        }));
        callback_nonces.push(nonce);
    }
    let markup = json!({"inline_keyboard": [buttons]});
    let message = approval_message(&request.method, &normalized, &thread_id);
    if let Err(error) = send_text_with_markup(
        bot,
        &surface_for(bot, config, session.chat_id, session.root_message_id),
        &message,
        Some(markup),
        metrics,
    )
    .await
    {
        for nonce in callback_nonces {
            let _ = store.take_callback(&nonce, now_ms());
        }
        reject_undelivered_approval(store, &approval, request_nonce)?;
        agent
            .respond_error(
                request.id,
                -32001,
                "Rust Bridge could not deliver the approval prompt to Telegram",
            )
            .await
            .map_err(|response_error| {
                format!("Telegram approval delivery failed: {error}; app-server response failed: {response_error}")
            })?;
        return Err(format!("Telegram approval delivery failed: {error}"));
    }
    Ok(())
}

fn reject_undelivered_approval(
    store: &SqliteStore,
    approval: &ApprovalRequest,
    request_nonce: String,
) -> Result<(), String> {
    if approval.decision != ApprovalDecision::Pending {
        return Ok(());
    }
    let mut rejected = approval.clone();
    rejected
        .decide(ApprovalDecision::Rejected, now_ms())
        .map_err(|error| error.to_string())?;
    let event = DomainEvent {
        id: EventId::new(format!("approval-delivery-failed-{request_nonce}"))
            .map_err(|error| error.to_string())?,
        occurred_at_ms: now_ms(),
        kind: DomainEventKind::ApprovalDecided {
            approval: rejected.clone(),
        },
    };
    store
        .decide_approval(&rejected, &event)
        .map_err(|error| error.to_string())
}

fn parse_approval_callback(data: &str) -> Option<(&str, &str)> {
    let mut parts = data.splitn(3, ':');
    if parts.next()? != "ra" {
        return None;
    }
    let nonce = parts.next()?.trim();
    let decision = parts.next()?.trim();
    (!nonce.is_empty() && !decision.is_empty()).then_some((nonce, decision))
}

async fn acknowledge_callback(
    bot: &RuntimeBot,
    callback: &codex_telegram_adapter::TelegramCallback,
    text: Option<&str>,
) {
    let api = bot.api.clone();
    let token = bot.token.clone();
    let callback_id = callback.id.clone();
    let text = text.map(str::to_owned);
    let _ = tokio::task::spawn_blocking(move || {
        api.answer_callback_query(&token, &callback_id, text.as_deref())
    })
    .await;
}

async fn send_text_with_markup(
    bot: &RuntimeBot,
    surface: &TelegramSurfaceBinding,
    text: &str,
    markup: Option<Value>,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    send_text_with_markup_message(bot, surface, text, markup, metrics)
        .await
        .map(|_| ())
}

async fn send_text_with_markup_message(
    bot: &RuntimeBot,
    surface: &TelegramSurfaceBinding,
    text: &str,
    markup: Option<Value>,
    metrics: &MetricsRegistry,
) -> Result<SentMessage, String> {
    let text = truncate_text(text);
    let api = bot.api.clone();
    let token = bot.token.clone();
    let surface = surface.clone();
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        api.send_text_with_markup(&token, &surface, &text, markup)
    })
    .await
    .map_err(|error| error.to_string())?;
    match result {
        Ok(message) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                true,
                started.elapsed().as_micros() as u64,
            );
            Ok(message)
        }
        Err(error) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                false,
                started.elapsed().as_micros() as u64,
            );
            Err(error.to_string())
        }
    }
}

async fn edit_text_message(
    bot: &RuntimeBot,
    message: &TelegramMessageReference,
    text: &str,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    let api = bot.api.clone();
    let token = bot.token.clone();
    let reference = message.clone();
    let request = TelegramMessageRequest::new(truncate_text(text));
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        let edited = api.edit_text(&token, &reference, &request)?;
        let _ = api.edit_reply_markup(&token, &reference, None);
        Ok::<SentMessage, codex_telegram_adapter::TelegramError>(edited)
    })
    .await
    .map_err(|error| error.to_string())?;
    match result {
        Ok(_) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                true,
                started.elapsed().as_micros() as u64,
            );
            Ok(())
        }
        Err(error) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                false,
                started.elapsed().as_micros() as u64,
            );
            Err(error.to_string())
        }
    }
}

async fn edit_text_with_markup(
    bot: &RuntimeBot,
    message: &TelegramMessageReference,
    text: &str,
    markup: Option<Value>,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    let api = bot.api.clone();
    let token = bot.token.clone();
    let reference = message.clone();
    let text = truncate_text(text);
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        api.edit_text_with_markup(&token, &reference, &text, markup)
    })
    .await
    .map_err(|error| error.to_string())?;
    match result {
        Ok(_) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                true,
                started.elapsed().as_micros() as u64,
            );
            Ok(())
        }
        Err(error) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                false,
                started.elapsed().as_micros() as u64,
            );
            Err(error.to_string())
        }
    }
}

/// Dual-track variant of `send_text_with_markup_message`: MarkdownV2 first,
/// the plain fallback on a Telegram 400, reusing the adapter's rendered
/// recovery path. The stored JSON keyboard is preserved through the typed
/// markup conversion.
async fn send_rendered_with_markup(
    bot: &RuntimeBot,
    surface: &TelegramSurfaceBinding,
    rendered: &StatusRendered,
    markup: Option<Value>,
    metrics: &MetricsRegistry,
) -> Result<SentMessage, String> {
    let typed_markup = typed_markup_from_json(markup)?;
    let request = TelegramMessageRequest::markdown_v2(
        truncate_text(&rendered.markdown),
        truncate_text(&rendered.plain),
    )
    .with_reply_markup_option(typed_markup);
    let api = bot.api.clone();
    let token = bot.token.clone();
    let surface = surface.clone();
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || api.send_rendered(&token, &surface, &request))
        .await
        .map_err(|error| error.to_string())?;
    match result {
        Ok(message) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                true,
                started.elapsed().as_micros() as u64,
            );
            Ok(message)
        }
        Err(error) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                false,
                started.elapsed().as_micros() as u64,
            );
            Err(error.to_string())
        }
    }
}

async fn edit_rendered_with_markup(
    bot: &RuntimeBot,
    message: &TelegramMessageReference,
    rendered: &StatusRendered,
    markup: Option<Value>,
    metrics: &MetricsRegistry,
    timeout: Option<Duration>,
) -> Result<(), String> {
    let typed_markup = typed_markup_from_json(markup)?;
    let request = TelegramMessageRequest::markdown_v2(
        truncate_text(&rendered.markdown),
        truncate_text(&rendered.plain),
    )
    .with_reply_markup_option(typed_markup);
    let api = bot.api.clone();
    let token = bot.token.clone();
    let reference = message.clone();
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        api.edit_text_with_timeout(&token, &reference, &request, timeout)
    })
    .await
    .map_err(|error| error.to_string())?;
    match result {
        Ok(_) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                true,
                started.elapsed().as_micros() as u64,
            );
            Ok(())
        }
        Err(error) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                false,
                started.elapsed().as_micros() as u64,
            );
            Err(error.to_string())
        }
    }
}

fn normalize_server_request_params(method: &str, params: &Value) -> Value {
    let mut normalized = params.clone();
    let Some(object) = normalized.as_object_mut() else {
        return normalized;
    };
    if method == "execCommandApproval" {
        let thread_id = object
            .get("conversationId")
            .cloned()
            .or_else(|| object.get("threadId").cloned())
            .unwrap_or(Value::String(String::new()));
        let turn_id = object
            .get("turnId")
            .cloned()
            .unwrap_or(Value::String(String::new()));
        let item_id = object
            .get("callId")
            .cloned()
            .or_else(|| object.get("itemId").cloned())
            .unwrap_or(Value::String(String::new()));
        object.insert("threadId".into(), thread_id);
        object.insert("turnId".into(), turn_id);
        object.insert("itemId".into(), item_id);
    } else if method == "applyPatchApproval" {
        let thread_id = object
            .get("conversationId")
            .cloned()
            .or_else(|| object.get("threadId").cloned())
            .unwrap_or(Value::String(String::new()));
        let turn_id = object
            .get("turnId")
            .cloned()
            .unwrap_or(Value::String(String::new()));
        let item_id = object
            .get("callId")
            .cloned()
            .or_else(|| object.get("itemId").cloned())
            .unwrap_or(Value::String(String::new()));
        object.insert("threadId".into(), thread_id);
        object.insert("turnId".into(), turn_id);
        object.insert("itemId".into(), item_id);
    }
    normalized
}

fn available_approval_decisions(method: &str, params: &Value) -> Vec<String> {
    let default: Vec<String> = match method {
        "execCommandApproval" => vec!["accept", "acceptForSession", "decline", "cancel"],
        "applyPatchApproval" => vec!["accept", "acceptForSession", "decline", "cancel"],
        "item/permissions/requestApproval" => vec!["accept", "decline"],
        _ => vec!["accept", "acceptForSession", "decline"],
    }
    .into_iter()
    .map(str::to_owned)
    .collect();
    if method == "item/permissions/requestApproval" {
        return default;
    }
    let Some(values) = params.get("availableDecisions").and_then(Value::as_array) else {
        return default;
    };
    values
        .iter()
        .filter_map(Value::as_str)
        .filter(|value| matches!(*value, "accept" | "acceptForSession" | "decline" | "cancel"))
        .map(str::to_owned)
        .collect()
}

fn approval_response_payload(
    method: &str,
    decision: &str,
    params: &Value,
) -> Result<Value, String> {
    match method {
        "item/commandExecution/requestApproval" | "item/fileChange/requestApproval" => {
            Ok(json!({"decision": decision}))
        }
        "execCommandApproval" | "applyPatchApproval" => {
            let mapped = match decision {
                "accept" => "approved",
                "acceptForSession" => "approved_for_session",
                "decline" => "denied",
                "cancel" => "abort",
                _ => return Err("unsupported legacy approval decision".into()),
            };
            Ok(json!({"decision": mapped}))
        }
        "item/permissions/requestApproval" => {
            if decision == "accept" {
                Ok(json!({
                    "permissions": params
                        .get("permissions")
                        .or_else(|| params.get("requestedPermissions"))
                        .cloned()
                        .unwrap_or_else(|| json!({})),
                    "scope": "turn"
                }))
            } else {
                Ok(json!({"permissions": {}, "scope": "turn"}))
            }
        }
        _ => Err("unsupported approval method".into()),
    }
}

fn approval_action(method: &str, params: &Value) -> ApprovalAction {
    if method == "item/permissions/requestApproval" {
        return ApprovalAction::SendPrompt {
            prompt: params
                .get("permissions")
                .or_else(|| params.get("requestedPermissions"))
                .map(Value::to_string)
                .unwrap_or_else(|| "permissions".into()),
        };
    }
    ApprovalAction::ExecuteCommand {
        command: if method == "item/fileChange/requestApproval" || method == "applyPatchApproval" {
            params
                .get("grantRoot")
                .or_else(|| params.get("cwd"))
                .map(value_text)
                .unwrap_or_else(|| "file change".into())
        } else {
            command_text(params)
        },
    }
}

fn approval_message(method: &str, params: &Value, thread_id: &str) -> String {
    let subject = match method {
        "item/fileChange/requestApproval" | "applyPatchApproval" => "file change",
        "item/permissions/requestApproval" => "permissions",
        _ => "command",
    };
    let detail = if method == "item/permissions/requestApproval" {
        params
            .get("permissions")
            .or_else(|| params.get("requestedPermissions"))
            .map(value_text)
            .unwrap_or_else(|| "unknown permissions".into())
    } else if subject == "file change" {
        params
            .get("grantRoot")
            .or_else(|| params.get("cwd"))
            .map(value_text)
            .unwrap_or_else(|| "current workspace".into())
    } else {
        command_text(params)
    };
    let cwd = params
        .get("cwd")
        .map(value_text)
        .unwrap_or_else(|| "unknown".into());
    let reason = params
        .get("reason")
        .map(value_text)
        .unwrap_or_else(|| "Codex requested an additional decision.".into());
    truncate_text(&format!(
        "[Codex approval]\nthread={}\n{}: {}\ncwd: {}\nreason: {}\nChoose a decision below.",
        truncate_text(thread_id),
        subject,
        detail,
        cwd,
        reason
    ))
}

fn approval_button_label(decision: &str) -> &'static str {
    match decision {
        "accept" => "Allow",
        "acceptForSession" => "Allow session",
        "decline" => "Deny",
        "cancel" => "Cancel",
        _ => "Decide",
    }
}

fn approval_confirmation(decision: &Value, method: &str) -> &'static str {
    let decision = decision.as_str().unwrap_or_default();
    match (method, decision) {
        ("item/permissions/requestApproval", "accept") => "Codex permissions approved.",
        ("item/permissions/requestApproval", _) => "Codex permissions denied.",
        (_, "accept") => "Codex approval accepted.",
        (_, "acceptForSession") => "Codex approval accepted for this session.",
        (_, "decline") => "Codex approval denied.",
        (_, "cancel") => "Codex request cancelled.",
        _ => "Codex approval submitted.",
    }
}

fn domain_decision(value: &Value) -> Option<ApprovalDecision> {
    match value.as_str()? {
        "accept" | "acceptForSession" => Some(ApprovalDecision::Approved),
        "decline" | "cancel" => Some(ApprovalDecision::Rejected),
        _ => None,
    }
}

fn command_text(params: &Value) -> String {
    match params.get("command") {
        Some(Value::Array(values)) => values.iter().map(value_text).collect::<Vec<_>>().join(" "),
        Some(value) => value_text(value),
        None => "unknown command".into(),
    }
}

fn value_text(value: &Value) -> String {
    let text = value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string());
    truncate_text(&text)
}

fn ensure_approval_session(
    store: &SqliteStore,
    thread_id: &str,
    now: i64,
) -> Result<SessionId, String> {
    let session_id =
        SessionId::new(format!("codex-{thread_id}")).map_err(|error| error.to_string())?;
    if store
        .get_session(&session_id)
        .map_err(|error| error.to_string())?
        .is_none()
    {
        let session = Session::new(session_id.clone(), format!("Codex {thread_id}"), now)
            .map_err(|error| error.to_string())?;
        let event = DomainEvent {
            id: EventId::new(format!("session-created-{thread_id}"))
                .map_err(|error| error.to_string())?,
            occurred_at_ms: now,
            kind: DomainEventKind::SessionCreated {
                session: session.clone(),
            },
        };
        store
            .insert_session(&session, &event)
            .map_err(|error| error.to_string())?;
    }
    Ok(session_id)
}

fn next_approval_nonce() -> String {
    format!(
        "{:x}{:x}",
        now_ms().unsigned_abs(),
        NEXT_APPROVAL_NONCE.fetch_add(1, Ordering::Relaxed)
    )
}

fn stable_question_request_key(
    request: &AgentServerRequest,
    thread_id: &str,
    item_id: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(request.generation.to_be_bytes());
    digest.update(thread_id.as_bytes());
    digest.update([0]);
    digest.update(item_id.as_bytes());
    digest.update([0]);
    if let Ok(encoded_id) = serde_json::to_vec(&request.id) {
        digest.update(encoded_id);
    }
    let digest = format!("{:x}", digest.finalize());
    format!("question-{}", &digest[..20])
}

fn normalize_question_values(values: &[Value]) -> Vec<Value> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let mut value = value.clone();
            if let Some(object) = value.as_object_mut() {
                let id = object
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|id| !id.is_empty())
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("question-{}", index + 1));
                object.insert("id".into(), Value::String(id));
            }
            value
        })
        .collect()
}

fn question_id_at(question: &Value, index: usize) -> String {
    question
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("question-{}", index + 1))
}

fn extract_final_answer(turn: &Value) -> Option<String> {
    let items = turn.get("items")?.as_array()?;
    let mut legacy = None;
    for item in items {
        if item.get("type").and_then(Value::as_str) != Some("agentMessage") {
            continue;
        }
        let text = item.get("text").and_then(Value::as_str)?.trim();
        if text.is_empty() {
            continue;
        }
        if item.get("phase").and_then(Value::as_str) == Some("final_answer") {
            return Some(text.to_owned());
        }
        legacy = Some(text.to_owned());
    }
    legacy
}

fn extract_review_answer(turn: &Value) -> Option<String> {
    let items = turn.get("items")?.as_array()?;
    items.iter().rev().find_map(|item| {
        if item.get("type").and_then(Value::as_str) != Some("exitedReviewMode") {
            return None;
        }
        item.get("review")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(str::to_owned)
    })
}

fn extract_turn_error(turn: &Value) -> Option<String> {
    turn.get("error")
        .and_then(|error| error.get("message").or(Some(error)))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|message| !message.is_empty())
        .map(str::to_owned)
}

fn event_thread_id(params: &Value) -> Option<&str> {
    [
        params.get("threadId"),
        params.pointer("/thread/id"),
        params.pointer("/turn/threadId"),
        params.pointer("/item/threadId"),
    ]
    .into_iter()
    .flatten()
    .find_map(Value::as_str)
    .filter(|thread_id| !thread_id.trim().is_empty())
}

#[derive(Clone, Debug)]
struct ModelChoice {
    model: String,
    display_name: String,
    efforts: Vec<String>,
    default_effort: String,
}

#[derive(Clone, Debug)]
struct ModelChoices {
    entries: Vec<ModelChoice>,
    summary: String,
}

async fn list_model_choices(agent: &AppServerClient) -> Result<ModelChoices, String> {
    let mut cursor: Option<String> = None;
    let mut entries = Vec::new();
    for _ in 0..16 {
        let mut params = json!({"limit": 100, "includeHidden": false});
        if let Some(cursor) = cursor.as_deref() {
            params["cursor"] = Value::String(cursor.to_owned());
        }
        let result = agent
            .request("model/list", params, Duration::from_secs(30))
            .await
            .map_err(|error| error.to_string())?;
        let data = result
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| "Codex model/list response did not include data".to_owned())?;
        for item in data {
            let model = item
                .get("model")
                .or_else(|| item.get("id"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "Codex model/list returned a model without an id".to_owned())?;
            let display_name = item
                .get("displayName")
                .and_then(Value::as_str)
                .unwrap_or(model)
                .trim()
                .to_owned();
            let default_effort = item
                .get("defaultReasoningEffort")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or("medium")
                .to_owned();
            let mut efforts = Vec::new();
            if let Some(raw_efforts) = item
                .get("supportedReasoningEfforts")
                .and_then(Value::as_array)
            {
                for effort in raw_efforts {
                    let value = effort
                        .get("reasoningEffort")
                        .or_else(|| effort.get("effort"))
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|value| !value.is_empty());
                    if let Some(value) = value
                        && !efforts.iter().any(|known| known == value)
                    {
                        efforts.push(value.to_owned());
                    }
                }
            }
            if efforts.is_empty() {
                efforts.push(default_effort.clone());
            }
            if !entries
                .iter()
                .any(|known: &ModelChoice| known.model == model)
            {
                entries.push(ModelChoice {
                    model: model.to_owned(),
                    display_name,
                    efforts,
                    default_effort,
                });
            }
        }
        cursor = result
            .get("nextCursor")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    if entries.is_empty() {
        return Err("Codex model/list returned no visible models".into());
    }
    let summary = entries
        .iter()
        .map(|entry| {
            format!(
                "{} ({}) [{}]",
                entry.model,
                entry.display_name,
                entry.efforts.join(", ")
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    Ok(ModelChoices { entries, summary })
}

async fn collaboration_mode_payload(
    agent: &AppServerClient,
    requested_mode: &str,
    explicit_model: Option<&str>,
    explicit_effort: Option<&str>,
) -> Result<Value, String> {
    if explicit_model.is_some() != explicit_effort.is_some() {
        return Err("Collaboration model and effort must be provided together".into());
    }
    if let (Some(model), Some(effort)) = (explicit_model, explicit_effort) {
        let model = model.trim();
        let effort = effort.trim();
        if model.is_empty() || effort.is_empty() {
            return Err("Collaboration model and effort must not be empty".into());
        }
        return Ok(json!({
            "mode": requested_mode,
            "settings": {
                "model": model,
                "reasoning_effort": effort,
                "developer_instructions": null
            }
        }));
    }
    let result = agent
        .request("collaborationMode/list", json!({}), Duration::from_secs(30))
        .await
        .map_err(|error| error.to_string())?;
    let data = result
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| "Codex collaborationMode/list response did not include data".to_owned())?;
    for item in data {
        let name = item
            .get("name")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| "Codex returned a collaboration mode without a name".to_owned())?;
        let mode = item.get("mode").and_then(Value::as_str);
        if let Some(mode) = mode
            && !matches!(mode, "default" | "plan")
        {
            return Err(format!(
                "Codex returned an unknown collaboration mode: {mode:?}"
            ));
        }
        if let Some(model) = item.get("model")
            && !model.is_null()
            && !model.is_string()
        {
            return Err(format!(
                "Codex collaboration mode {name} has an invalid model"
            ));
        }
        if let Some(effort) = item.get("reasoning_effort")
            && !effort.is_null()
            && !effort.is_string()
        {
            return Err(format!(
                "Codex collaboration mode {name} has an invalid effort"
            ));
        }
    }
    let item = data
        .iter()
        .find(|item| item.get("mode").and_then(Value::as_str) == Some(requested_mode))
        .ok_or_else(|| format!("Codex collaboration mode {requested_mode} is unavailable"))?;
    let model = item
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("Codex collaboration mode {requested_mode} has no model"))?;
    let effort = item.get("reasoning_effort").cloned().unwrap_or(Value::Null);
    Ok(json!({
        "mode": requested_mode,
        "settings": {
            "model": model,
            "reasoning_effort": effort,
            "developer_instructions": null
        }
    }))
}

fn parse_review_target(text: &str) -> Result<Value, String> {
    let remainder = text
        .split_once(char::is_whitespace)
        .map(|(_, value)| value.trim())
        .unwrap_or_default();
    if remainder.is_empty() {
        return Ok(json!({"type": "uncommittedChanges"}));
    }
    let (kind, value) = remainder
        .split_once(char::is_whitespace)
        .map(|(kind, value)| (kind, value.trim()))
        .unwrap_or((remainder, ""));
    match kind.to_ascii_lowercase().as_str() {
        "uncommitted" | "changes" => Ok(json!({"type": "uncommittedChanges"})),
        "base" | "branch" if !value.is_empty() => {
            Ok(json!({"type": "baseBranch", "branch": truncate_text(value)}))
        }
        "commit" if !value.is_empty() => {
            Ok(json!({"type": "commit", "sha": truncate_text(value)}))
        }
        "custom" if !value.is_empty() => Ok(json!({
            "type": "custom",
            "instructions": truncate_text(value)
        })),
        _ => Err(
            "用法：/review、/review base <branch>、/review commit <sha> 或 /review custom <instructions>"
                .into(),
        ),
    }
}

fn update_session_plan_mode(
    store: &SqliteStore,
    thread_id: &ThreadId,
    enabled: bool,
) -> Result<(), String> {
    let mut space = store
        .session_space_for_thread(thread_id.as_str())
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "Codex thread has no durable Rust session space".to_owned())?;
    space.plan_mode = enabled;
    space.updated_at_ms = now_ms();
    store
        .upsert_session_space(&space)
        .map_err(|error| error.to_string())
}

struct WorkspaceArtifact {
    relative_path: String,
    file_name: String,
    bytes: Vec<u8>,
    sha256: String,
}

fn read_workspace_artifact(
    workspace_root: &Path,
    requested: &str,
) -> Result<WorkspaceArtifact, String> {
    use std::path::Component;

    let relative = Path::new(requested);
    if relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err("文件路径必须是 workspace 内的相对路径，且不能包含 ..".into());
    }
    let root = fs::canonicalize(workspace_root)
        .map_err(|error| format!("workspace 根目录不可用: {error}"))?;
    let candidate = fs::canonicalize(root.join(relative))
        .map_err(|_| "文件不存在，或无法解析安全路径".to_owned())?;
    if candidate == root || candidate.strip_prefix(&root).is_err() {
        return Err("文件路径必须位于 workspace 根目录内".into());
    }
    let metadata = fs::metadata(&candidate).map_err(|_| "文件元数据不可用".to_owned())?;
    if !metadata.is_file() {
        return Err("只能传输普通文件".into());
    }
    if metadata.len() > MAX_ARTIFACT_BYTES {
        return Err(format!(
            "文件超过 Rust Bridge 的 {} MiB 传输上限",
            MAX_ARTIFACT_BYTES / (1024 * 1024)
        ));
    }
    let bytes = fs::read(&candidate).map_err(|_| "文件读取失败".to_owned())?;
    if bytes.len() as u64 > MAX_ARTIFACT_BYTES {
        return Err("文件在读取期间超过大小上限".into());
    }
    let mut digest = Sha256::new();
    digest.update(&bytes);
    let sha256 = format!("{:x}", digest.finalize());
    let relative_path = candidate
        .strip_prefix(&root)
        .map_err(|_| "文件路径越过 workspace 根目录".to_owned())?
        .to_string_lossy()
        .replace('\\', "/");
    let file_name = candidate
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "文件名不是有效 UTF-8".to_owned())?
        .to_owned();
    Ok(WorkspaceArtifact {
        relative_path,
        file_name,
        bytes,
        sha256,
    })
}

fn safe_attachment_name(name: &str, message_id: i64) -> String {
    let candidate = Path::new(name)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    let mut sanitized = candidate
        .chars()
        .map(|value| {
            if value.is_ascii_alphanumeric() || matches!(value, '.' | '-' | '_') {
                value
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() || sanitized == "." || sanitized == ".." {
        sanitized = format!("telegram-{message_id}.bin");
    }
    sanitized.truncate(128);
    sanitized
}

async fn send_text(
    bot: &RuntimeBot,
    surface: &TelegramSurfaceBinding,
    text: &str,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    let text = truncate_text(text);
    let api = bot.api.clone();
    let token = bot.token.clone();
    let surface = surface.clone();
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || api.send_text(&token, &surface, &text))
        .await
        .map_err(|error| error.to_string())?;
    match result {
        Ok(_) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                true,
                started.elapsed().as_micros() as u64,
            );
            Ok(())
        }
        Err(error) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                false,
                started.elapsed().as_micros() as u64,
            );
            Err(error.to_string())
        }
    }
}

async fn send_text_message(
    bot: &RuntimeBot,
    surface: &TelegramSurfaceBinding,
    text: &str,
    metrics: &MetricsRegistry,
) -> Result<SentMessage, String> {
    let text = truncate_text(text);
    let api = bot.api.clone();
    let token = bot.token.clone();
    let surface = surface.clone();
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || api.send_text(&token, &surface, &text))
        .await
        .map_err(|error| error.to_string())?;
    match result {
        Ok(message) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                true,
                started.elapsed().as_micros() as u64,
            );
            Ok(message)
        }
        Err(error) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                false,
                started.elapsed().as_micros() as u64,
            );
            Err(error.to_string())
        }
    }
}

async fn send_document(
    bot: &RuntimeBot,
    surface: &TelegramSurfaceBinding,
    file_name: &str,
    bytes: Vec<u8>,
    caption: Option<&str>,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    let api = bot.api.clone();
    let token = bot.token.clone();
    let surface = surface.clone();
    let file_name = file_name.to_owned();
    let caption = caption.map(str::to_owned);
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        api.send_document(&token, &surface, &file_name, bytes, caption.as_deref())
    })
    .await
    .map_err(|error| error.to_string())?;
    match result {
        Ok(_) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                true,
                started.elapsed().as_micros() as u64,
            );
            Ok(())
        }
        Err(error) => {
            metrics.observe_delivery_duration_for(
                role_label(bot.role),
                false,
                started.elapsed().as_micros() as u64,
            );
            Err(error.to_string())
        }
    }
}

fn surface_for(
    bot: &RuntimeBot,
    config: &RustConfig,
    chat_id: i64,
    root_message_id: Option<i64>,
) -> TelegramSurfaceBinding {
    let channel = ChannelBinding::new(bot.config.instance_id.clone(), chat_id.to_string())
        .expect("Telegram chat id is non-empty");
    if chat_id == config.discussion_chat_id
        && let Some(root_message_id) = root_message_id
        && let Ok(comment) = NativeCommentBinding::new(
            ChannelBinding::new(
                bot.config.instance_id.clone(),
                config.channel_chat_id.to_string(),
            )
            .expect("channel id is non-empty"),
            config.discussion_chat_id.to_string(),
            root_message_id,
        )
    {
        return TelegramSurfaceBinding::NativeCommentRoot(comment);
    }
    TelegramSurfaceBinding::Channel(channel)
}

fn telegram_message_link(chat_id: i64, message_id: i64) -> String {
    let normalized = chat_id.saturating_abs();
    let internal = if chat_id <= -1_000_000_000_000 {
        normalized.saturating_sub(1_000_000_000_000)
    } else {
        normalized
    };
    format!("https://t.me/c/{internal}/{message_id}")
}

fn runtime_role(capability: BotCapability) -> Option<RuntimeBotRole> {
    match capability {
        BotCapability::Control => Some(RuntimeBotRole::Control),
        BotCapability::Discussion => Some(RuntimeBotRole::Discussion),
        BotCapability::Status => Some(RuntimeBotRole::Status),
        BotCapability::ProductionAlert | BotCapability::CanaryAlert => Some(RuntimeBotRole::Alert),
        BotCapability::Approval | BotCapability::Artifact => None,
    }
}

fn role_label(role: RuntimeBotRole) -> &'static str {
    match role {
        RuntimeBotRole::Control => "control",
        RuntimeBotRole::Status => "status",
        RuntimeBotRole::Discussion => "discussion",
        RuntimeBotRole::Alert => "alert",
    }
}

fn ensure_private_directory(path: &Path) -> Result<(), DaemonError> {
    fs::create_dir_all(path).map_err(|_| DaemonError::StateDirectory)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|_| DaemonError::StateDirectory)?;
    }
    Ok(())
}

fn protected_upload_paths(store: &SqliteStore, root: &Path) -> HashSet<PathBuf> {
    let root = absolute_path(root);
    let mut protected = HashSet::new();
    for path in store.artifact_paths().unwrap_or_default() {
        add_protected_path(&mut protected, &root, &path);
    }
    for kind in [
        "prompt",
        "queue",
        "question",
        "pending_space",
        "space",
        "new",
        "plan",
    ] {
        for (_, value) in store.workflow_records(kind).unwrap_or_default() {
            collect_protected_paths(&value, &root, &mut protected);
        }
    }
    protected
}

fn collect_protected_paths(value: &Value, root: &Path, protected: &mut HashSet<PathBuf>) {
    match value {
        Value::String(path) => add_protected_path(protected, root, Path::new(path)),
        Value::Array(values) => {
            for value in values {
                collect_protected_paths(value, root, protected);
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect_protected_paths(value, root, protected);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn add_protected_path(protected: &mut HashSet<PathBuf>, root: &Path, path: &Path) {
    let path = absolute_path(path);
    if path.starts_with(root) {
        protected.insert(path);
    }
}

fn absolute_path(path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    }
}

fn cleanup_upload_directory(root: &Path, cutoff_ms: i64, protected: &HashSet<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if protected.contains(&absolute_path(&path)) {
            continue;
        }
        if path.is_dir() {
            cleanup_upload_directory(&path, cutoff_ms, protected);
            let _ = fs::remove_dir(&path);
            continue;
        }
        let Ok(metadata) = fs::metadata(&path) else {
            continue;
        };
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        let modified_ms = modified
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or(i64::MAX);
        if modified_ms < cutoff_ms {
            let _ = fs::remove_file(path);
        }
    }
}

fn truncate_text(text: &str) -> String {
    const LIMIT: usize = 3900;
    if text.len() <= LIMIT {
        return text.to_owned();
    }
    let mut end = LIMIT;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let mut result = text[..end].to_owned();
    result.push_str("\n...[truncated]");
    result
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn keyed_dispatcher_parallelizes_chats_and_preserves_same_chat_order() {
        let log = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let handler = {
            let log = log.clone();
            move |(label, delay_ms): (&'static str, u64)| {
                let log = log.clone();
                async move {
                    if delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                    log.lock().expect("log lock poisoned").push(label);
                }
            }
        };
        let mut dispatcher = KeyedDispatcher::new(handler);
        let key = |bot: &str, chat_id: i64| DispatchKey {
            bot_instance_id: bot.to_owned(),
            chat_id,
        };
        // A slow handler occupying one chat must not delay other chats of
        // the same bot or of another bot, while a later update for the same
        // chat still waits its turn.
        dispatcher.dispatch(key("bot-a", 1), ("slow-a1", 300));
        dispatcher.dispatch(key("bot-a", 2), ("fast-a2", 0));
        dispatcher.dispatch(key("bot-b", 1), ("fast-b1", 0));
        dispatcher.dispatch(key("bot-a", 1), ("after-a1", 0));
        tokio::time::sleep(Duration::from_millis(700)).await;
        let log = log.lock().expect("log lock poisoned").clone();
        let position = |label| {
            log.iter()
                .position(|entry| *entry == label)
                .unwrap_or_else(|| panic!("{label} missing from {log:?}"))
        };
        assert!(position("fast-a2") < position("slow-a1"));
        assert!(position("fast-b1") < position("slow-a1"));
        assert!(position("slow-a1") < position("after-a1"));
        assert_eq!(log.len(), 4);
        assert_eq!(dispatcher.worker_count(), 3);
    }

    #[test]
    fn confirmed_update_prefix_stops_at_first_unconfirmed_update() {
        let pending = |entries: &[(i64, bool)]| {
            entries
                .iter()
                .copied()
                .collect::<std::collections::BTreeMap<i64, bool>>()
        };
        assert_eq!(
            confirmed_update_prefix(&pending(&[(1, true), (2, true), (3, false), (4, true)])),
            vec![1, 2]
        );
        assert_eq!(
            confirmed_update_prefix(&pending(&[(5, false), (6, true)])),
            Vec::<i64>::new()
        );
        assert_eq!(
            confirmed_update_prefix(&pending(&[(5, true), (7, true)])),
            vec![5, 7]
        );
        assert_eq!(
            confirmed_update_prefix(&std::collections::BTreeMap::new()),
            Vec::<i64>::new()
        );
    }

    #[test]
    fn sessions_listing_maps_projections_sorted_by_recency() {
        let projection_value = |thread_id: &str, title: Option<&str>, updated_at_ms: i64| {
            serde_json::to_value(ThreadProjection {
                thread_id: thread_id.to_owned(),
                title: title.map(str::to_owned),
                cwd: Some(format!("/tmp/{thread_id}")),
                status: Some("idle".to_owned()),
                turn_status: Some("completed".to_owned()),
                last_error: Some("boom".to_owned()),
                last_error_recoverable: true,
                updated_at_ms,
                ..ThreadProjection::default()
            })
            .unwrap()
        };
        let projections = vec![
            (
                "thread-a".to_owned(),
                1,
                projection_value("thread-a", Some("Alpha"), 20_000),
                0,
            ),
            (
                "thread-b".to_owned(),
                1,
                projection_value("thread-b", None, 40_000),
                0,
            ),
        ];
        let created_at_ms = HashMap::from([("thread-a".to_owned(), 1_700_000_000)]);
        let lifecycle_by_thread =
            HashMap::from([("thread-a".to_owned(), "repair_required".to_owned())]);
        let sessions =
            control_sessions_from_projections(projections, &created_at_ms, &lifecycle_by_thread);
        assert_eq!(sessions.len(), 2);
        // Recency descending, matching the app-server `thread/list` order.
        assert_eq!(sessions[0].thread_id, "thread-b");
        assert_eq!(sessions[0].title, "Codex session");
        assert_eq!(sessions[0].updated_at, Some(40));
        assert_eq!(sessions[0].created_at, None);
        assert_eq!(sessions[0].lifecycle, "");
        assert_eq!(sessions[1].thread_id, "thread-a");
        assert_eq!(sessions[1].title, "Alpha");
        assert_eq!(sessions[1].created_at, Some(1_700_000_000));
        assert_eq!(sessions[1].cwd, "/tmp/thread-a");
        assert_eq!(sessions[1].lifecycle, "repair_required");
        // A recoverable error is not surfaced as a session error row.
        assert!(sessions[1].error.is_empty());
    }

    #[test]
    fn workspace_artifact_path_is_bounded_and_hashed() {
        let root = std::env::temp_dir().join(format!(
            "codex-telegram-bridge-artifact-{}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("report.txt"), b"hello").unwrap();

        let artifact = read_workspace_artifact(&root, "report.txt").unwrap();
        assert_eq!(artifact.relative_path, "report.txt");
        assert_eq!(artifact.file_name, "report.txt");
        assert_eq!(artifact.bytes, b"hello");
        assert_eq!(
            artifact.sha256,
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
        assert!(read_workspace_artifact(&root, "../report.txt").is_err());
        assert!(read_workspace_artifact(&root, "/etc/passwd").is_err());

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn upload_cleanup_preserves_protected_paths() {
        let root = std::env::temp_dir().join(format!(
            "codex-telegram-bridge-upload-cleanup-{}",
            std::process::id()
        ));
        let nested = root.join("chat");
        fs::create_dir_all(&nested).unwrap();
        let protected = nested.join("active.bin");
        let removable = nested.join("stale.bin");
        fs::write(&protected, b"active").unwrap();
        fs::write(&removable, b"stale").unwrap();
        let store = SqliteStore::in_memory().unwrap();
        store
            .upsert_workflow_record("queue", "queue-1", &json!({"path": protected}), 1)
            .unwrap();
        let protected_paths = protected_upload_paths(&store, &root);
        assert!(protected_paths.contains(&absolute_path(&protected)));

        cleanup_upload_directory(&root, i64::MAX, &protected_paths);

        assert!(protected.exists());
        assert!(!removable.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn review_target_defaults_to_uncommitted_changes() {
        assert_eq!(
            parse_review_target("/review").unwrap(),
            json!({"type": "uncommittedChanges"})
        );
        assert_eq!(
            parse_review_target("/review base main").unwrap(),
            json!({"type": "baseBranch", "branch": "main"})
        );
        assert!(parse_review_target("/review unknown value").is_err());
    }

    #[test]
    fn terminal_plan_labels_and_nested_event_thread_ids_are_stable() {
        assert!(plan_status_label(&PlanPublicationState::Executed, None).contains("已批准"));
        assert!(plan_status_label(&PlanPublicationState::Failed, None).contains("失败"));
        assert_eq!(
            event_thread_id(&json!({"turn":{"threadId":"thread-nested"}})),
            Some("thread-nested")
        );
        assert_eq!(
            extract_turn_error(&json!({"error":{"message":"boom"}})).as_deref(),
            Some("boom")
        );
    }

    #[test]
    fn new_arguments_and_message_links_match_python_contract() {
        let parsed = parse_new_arguments(
            "/new gpt-5 | high | planmode | gpt-5-mini | medium | projects/demo | ship it | now",
        )
        .unwrap();
        assert_eq!(parsed.model, "gpt-5");
        assert_eq!(parsed.effort, "high");
        assert_eq!(parsed.mode.as_deref(), Some("planmode"));
        assert_eq!(parsed.plan_model.as_deref(), Some("gpt-5-mini"));
        assert_eq!(parsed.cwd.as_deref(), Some("projects/demo"));
        assert_eq!(parsed.prompt.as_deref(), Some("ship it | now"));
        assert_eq!(
            telegram_message_link(-1004446000549, 81),
            "https://t.me/c/4446000549/81"
        );
        assert!(parse_new_arguments("gpt-5 | high | planmode").is_err());
    }

    #[test]
    fn pending_session_confirmation_matches_python_contract() {
        let (rendered, markup) =
            pending_session_confirmation("https://t.me/c/4446000549/9").unwrap();
        assert_eq!(rendered.operation, RenderOperation::Send);
        assert_eq!(
            rendered.markdown,
            "待认证 Session 帖子已创建。进入评论串并发送 `/totp <验证码>`。"
        );
        assert_eq!(
            rendered.plain.as_deref(),
            Some("待认证 Session 帖子已创建。进入评论串并发送 /totp <验证码>。")
        );
        assert!(rendered.keyboard.is_none());
        assert_eq!(
            markup.rows,
            vec![vec![InlineKeyboardButton::Url {
                text: "打开帖子".to_owned(),
                url: "https://t.me/c/4446000549/9".to_owned(),
            }]]
        );
    }

    #[test]
    fn new_project_resolution_expands_home_and_nested_names() {
        let root = std::env::temp_dir().join(format!(
            "codex-telegram-bridge-project-resolution-{}",
            std::process::id()
        ));
        let nested = root.join("tmp").join("rust_tg_test");
        fs::create_dir_all(&nested).unwrap();
        let root = fs::canonicalize(&root).unwrap();
        assert_eq!(
            expand_user_path("~/PythonProjects"),
            env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."))
                .join("PythonProjects")
        );
        assert_eq!(
            new_existing_projects(&root, "rust_tg_test").unwrap(),
            vec![nested]
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn new_project_creation_requires_a_safe_existing_ancestor() {
        let root = std::env::temp_dir().join(format!(
            "codex-telegram-bridge-project-create-{}",
            std::process::id()
        ));
        fs::create_dir_all(&root).unwrap();
        let root = fs::canonicalize(&root).unwrap();
        assert!(missing_path_has_safe_ancestor(
            &root,
            &root.join("tmp/rust_test")
        ));
        assert!(!missing_path_has_safe_ancestor(
            &root,
            &std::env::temp_dir().join("outside")
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn perf_render_matches_python_metric_sections_and_plain_fallback() {
        let snapshot = crate::perf::PerfSnapshot {
            sampled_at_ms: 1_700_000_000_000,
            uptime_seconds: 90_061,
            load: [1.0, 0.5, 0.25],
            cpu_percent: 12.3,
            memory_used_bytes: 512 * 1024 * 1024,
            memory_total_bytes: 1024 * 1024 * 1024,
            swap_used_bytes: 128 * 1024 * 1024,
            swap_total_bytes: 512 * 1024 * 1024,
            disk_used_bytes: 20 * 1024 * 1024 * 1024,
            disk_total_bytes: 100 * 1024 * 1024 * 1024,
            codex_process_count: 2,
            codex_cpu_percent: 3.4,
            codex_memory_bytes: 64 * 1024 * 1024,
            gpu: Some(crate::perf::GpuSnapshot {
                name: "Test GPU".into(),
                memory_used_mib: Some(512.0),
                memory_total_mib: Some(2048.0),
                utilization_percent: Some(25.0),
                temperature_c: Some(55.0),
                power_w: Some(80.0),
            }),
        };
        let (markdown, plain) = format_perf_snapshot(&snapshot);
        assert!(markdown.contains("*🟠 Ubuntu · WSL*"));
        assert!(markdown.contains("RAM  `50.0%` `#####-----`"));
        assert!(markdown.contains("负载 `1.00 / 0.50 / 0.25`"));
        assert!(markdown.contains("*🟩 NVIDIA · Test GPU*"));
        assert!(plain.contains("Ubuntu · WSL"));
        assert!(!plain.contains('`'));
        assert!(!plain.contains('*'));
    }

    #[test]
    fn new_prompt_drafts_use_a_short_prompt_timeout() {
        let created_before = now_ms();
        let draft = new_draft(42, 7, "prompt", json!({"cwd":"/workspace"}));
        let store = SqliteStore::in_memory().unwrap();
        persist_new_draft(&store, "new:42", &draft).unwrap();
        assert!(
            draft["expires_at_ms"].as_i64().unwrap() - created_before
                <= NEW_INTERACTION_TTL_MS + 1000
        );
        let advanced_before = now_ms();
        let advanced = advance_new_draft(
            &store,
            "new:42",
            &draft,
            "prompt",
            json!({"cwd":"/workspace"}),
            true,
        )
        .unwrap();
        assert!(
            advanced["expires_at_ms"].as_i64().unwrap() - advanced_before
                <= NEW_PROMPT_TTL_MS + 1000
        );
    }

    #[test]
    fn session_activation_workflow_is_finalized_with_the_initial_turn() {
        let store = SqliteStore::in_memory().unwrap();
        let thread_id = ThreadId::new("thread-activation").unwrap();
        let turn_id = TurnId::new("turn-activation").unwrap();
        let intent = PromptIntent {
            intent_id: "intent-activation".into(),
            client_message_id: "telegram-new-space-activation".into(),
            source: "session_activation".into(),
            prompt: "Hello".into(),
            mode: "default".into(),
            thread_id: Some(thread_id.clone()),
            space_id: Some("space-activation".into()),
            generation: 1,
            state: PromptIntentState::Started,
            turn_id: Some(turn_id.clone()),
            queue_id: None,
            error: None,
            created_at_ms: 1,
            updated_at_ms: 2,
        };
        store.upsert_prompt_intent(&intent).unwrap();
        store
            .upsert_workflow_record(
                "prompt",
                &intent.client_message_id,
                &json!({
                    "intent_id": intent.intent_id,
                    "client_message_id": intent.client_message_id,
                    "source": "session_activation",
                    "thread_id": thread_id.as_str(),
                    "turn_id": turn_id.as_str(),
                    "space_id": "space-activation",
                    "generation": 1,
                    "state": "started",
                }),
                2,
            )
            .unwrap();

        assert!(
            mark_turn_workflows(
                &store,
                thread_id.as_str(),
                turn_id.as_str(),
                "completed",
                None,
            )
            .is_empty()
        );
        assert_eq!(
            store
                .prompt_intent_by_client_message_id(&intent.client_message_id)
                .unwrap()
                .unwrap()
                .state,
            PromptIntentState::Completed
        );
        assert_eq!(
            store
                .workflow_record("prompt", &intent.client_message_id)
                .unwrap()
                .unwrap()["state"],
            "completed"
        );
    }

    #[test]
    fn terminal_projection_recovery_only_closes_older_active_intents() {
        let store = SqliteStore::in_memory().unwrap();
        let thread_id = ThreadId::new("thread-recovery").unwrap();
        for (suffix, updated_at_ms) in [("old", 50), ("new", 150)] {
            store
                .upsert_prompt_intent(&PromptIntent {
                    intent_id: format!("intent-recovery-{suffix}"),
                    client_message_id: format!("client-recovery-{suffix}"),
                    source: "session_activation".into(),
                    prompt: "Hello".into(),
                    mode: "default".into(),
                    thread_id: Some(thread_id.clone()),
                    space_id: Some("space-recovery".into()),
                    generation: 1,
                    state: PromptIntentState::Started,
                    turn_id: None,
                    queue_id: None,
                    error: None,
                    created_at_ms: updated_at_ms,
                    updated_at_ms,
                })
                .unwrap();
        }

        assert_eq!(
            reconcile_terminal_prompt_intents(
                &store,
                thread_id.as_str(),
                "completed",
                None,
                Some(100),
            )
            .unwrap(),
            1
        );
        assert_eq!(
            store
                .prompt_intent_by_client_message_id("client-recovery-old")
                .unwrap()
                .unwrap()
                .state,
            PromptIntentState::Completed
        );
        assert_eq!(
            store
                .prompt_intent_by_client_message_id("client-recovery-new")
                .unwrap()
                .unwrap()
                .state,
            PromptIntentState::Started
        );
    }

    #[test]
    fn new_choice_markup_always_keeps_a_cancel_button() {
        let store = SqliteStore::in_memory().unwrap();
        let draft = new_draft(42, 7, "project", json!({"normal_model":"gpt-5"}));
        persist_new_draft(&store, "new:42", &draft).unwrap();
        let mut draft = draft;
        let markup = new_choice_markup(&store, "new:42", &mut draft, &[])
            .unwrap()
            .expect("empty choices still render an exit button");
        assert_eq!(
            markup["inline_keyboard"][0][0]["text"],
            Value::String("退出".to_owned())
        );
        let choices = draft["choices"].as_object().unwrap();
        assert_eq!(choices.len(), 1);
        let cancel_nonce = choices.keys().next().unwrap();
        assert_eq!(choices[cancel_nonce]["event"], "cancel");
        let callback = store
            .consume_control_callback(cancel_nonce, 7, 42, now_ms())
            .unwrap()
            .expect("cancel callback is durable and scoped");
        assert_eq!(callback.action, "cancel");
    }

    #[test]
    fn new_choice_markup_balances_a_trailing_singleton_before_exit() {
        let store = SqliteStore::in_memory().unwrap();
        let draft = new_draft(42, 7, "normal_model", json!({}));
        persist_new_draft(&store, "new:42", &draft).unwrap();
        let mut draft = draft;
        let choices = (0..5)
            .map(|index| {
                (
                    "normal_model".to_owned(),
                    format!("model-{index}"),
                    format!("Model {index}"),
                )
            })
            .collect::<Vec<_>>();
        let markup = new_choice_markup(&store, "new:42", &mut draft, &choices)
            .unwrap()
            .unwrap();
        let rows = markup["inline_keyboard"].as_array().unwrap();
        assert_eq!(
            rows[..3]
                .iter()
                .map(|row| row.as_array().unwrap().len())
                .collect::<Vec<_>>(),
            vec![2, 1, 2]
        );
        assert_eq!(rows[3].as_array().unwrap()[0]["text"], "退出");
    }

    #[test]
    fn new_argument_suggestions_normalize_mode_and_preserve_prompt_tail() {
        let models = ModelChoices {
            entries: vec![
                ModelChoice {
                    model: "gpt-5.6-luna".into(),
                    display_name: "GPT 5.6 Luna".into(),
                    efforts: vec!["low".into(), "high".into(), "max".into()],
                    default_effort: "high".into(),
                },
                ModelChoice {
                    model: "gpt-5.6-sol".into(),
                    display_name: "GPT 5.6 Sol".into(),
                    efforts: vec!["low".into(), "medium".into(), "high".into()],
                    default_effort: "high".into(),
                },
            ],
            summary: String::new(),
        };
        assert_eq!(
            new_argument_suggestion(
                &models,
                "luna | max | nopln | /workspace | keep this | exact prompt"
            ),
            "/new gpt-5.6-luna | max | noplan | /workspace | keep this | exact prompt"
        );
        assert_eq!(
            new_argument_suggestion(
                &models,
                "luna | max | planmode | sol | | /workspace | prompt"
            ),
            "/new gpt-5.6-luna | max | planmode | gpt-5.6-sol | high | /workspace | prompt"
        );
    }

    #[test]
    fn control_session_status_accepts_app_server_objects() {
        let session = control_session_from_value(&json!({
            "id": "thread-1",
            "status": {"type": "idle", "activeFlags": ["waitingOnUserInput"]},
            "turnStatus": {"type": "completed"},
            "title": "Object status",
        }))
        .expect("session should be visible");

        assert_eq!(session.status, "idle");
        assert_eq!(session.turn_status, "completed");
        assert_eq!(session.active_flags, vec!["waitingOnUserInput"]);
    }

    #[test]
    fn session_detail_projection_renders_rich_status_and_live_link() {
        let thread_id = "019fc5f6-2d0c-7f72-9dfb-8041619f4761";
        let response = json!({
            "thread": {
                "id": thread_id,
                "name": "Rich session detail",
                "status": {"type": "active"},
                "collaborationMode": "plan",
                "goal": {"status": "active", "objective": "Ship Rust parity"},
                "updatedAt": 1_700_000_000,
                "turns": [{
                    "id": "turn-1",
                    "status": {"type": "inProgress"},
                    "items": [
                        {"id": "plan-1", "type": "plan", "steps": [
                            {"step": "Inspect", "status": "completed"},
                            {"step": "Deploy", "status": "inProgress"}
                        ]},
                        {"id": "agent-call", "type": "collabAgentToolCall", "agentsStates": {
                            "agent-1": {"status": "running"}
                        }}
                    ]
                }]
            }
        });
        let projection = projection_from_thread_read(thread_id, &response);
        assert_eq!(projection.status.as_deref(), Some("active"));
        assert_eq!(projection.turn_status.as_deref(), Some("inProgress"));
        assert_eq!(projection.item_order, vec!["plan-1", "agent-call"]);
        assert_eq!(projection.cwd, None);
        assert_eq!(
            projection
                .goal
                .as_ref()
                .and_then(|value| value.get("status")),
            Some(&json!("active"))
        );
        assert_eq!(projection.subagents.len(), 1);

        let store = Arc::new(SqliteStore::in_memory().unwrap());
        let totp = TotpManager::new(store.clone(), "/tmp/nonexistent-rust-bridge-totp", 60);
        let space = synthetic_session_space(thread_id, 42);
        let rendered = status_text(&store, &space, Some(&projection), None, &totp);
        assert!(rendered.contains("🤖 Codex · Rich session detail"));
        assert!(rendered.contains("🎯 Goal · active · Ship Rust parity"));
        assert!(rendered.contains("🧭 Plan · 1/2"));
        assert!(rendered.contains("🧩 Agent Tasks · 0/1 · Running 1"));

        let linked_space = RustSessionSpace {
            channel_chat_id: -1004446000549,
            channel_post_id: 81,
            discussion_chat_id: Some(-1004446000549),
            discussion_root_message_id: Some(9),
            status_message_id: Some(82),
            ..space
        };
        let markup = session_status_markup(&linked_space).expect("status link");
        assert_eq!(
            markup["inline_keyboard"][0][0]["url"],
            "https://t.me/c/4446000549/81?comment=82"
        );
    }

    #[test]
    fn recent_event_summary_matches_python_activity_contract() {
        assert_eq!(
            status_event_summary(
                &json!({
                    "id": "item-99",
                    "type": "agentMessage",
                    "memoryCitation": null,
                    "phase": "commentary",
                    "text": "修复91 Bot新建Session文案差异"
                }),
                true,
            ),
            Some((
                "修复91 Bot新建Session文案差异".to_owned(),
                "completed".to_owned()
            ))
        );
        assert_eq!(
            status_event_summary(&json!({"type": "contextCompaction"}), true),
            Some(("上下文已压缩".to_owned(), "completed".to_owned()))
        );
        assert_eq!(
            status_event_summary(
                &json!({
                    "type": "commandExecution",
                    "status": "completed",
                    "exitCode": 0
                }),
                true,
            ),
            Some((
                "命令执行 completed (exit 0)".to_owned(),
                "completed".to_owned()
            ))
        );
    }

    #[test]
    fn status_text_does_not_leak_recent_item_json() {
        let thread_id = "thread-readable-events";
        let response = json!({
            "thread": {
                "id": thread_id,
                "name": "Readable events",
                "status": {"type": "idle"},
                "turns": [{
                    "id": "turn-1",
                    "status": {"type": "completed"},
                    "items": [
                        {
                            "id": "item-99",
                            "type": "agentMessage",
                            "memoryCitation": null,
                            "phase": "commentary",
                            "text": "修复91 Bot新建Session文案差异"
                        },
                        {"id": "item-98", "type": "contextCompaction"}
                    ]
                }]
            }
        });
        let projection = projection_from_thread_read(thread_id, &response);
        let store = Arc::new(SqliteStore::in_memory().unwrap());
        let totp = TotpManager::new(store.clone(), "/tmp/nonexistent-rust-bridge-totp", 60);
        let space = synthetic_session_space(thread_id, 42);
        let rendered = status_text(&store, &space, Some(&projection), None, &totp);

        assert!(rendered.contains("🕘 近期事件"));
        assert!(rendered.contains("修复91 Bot新建Session文案差异 · completed"));
        assert!(rendered.contains("上下文已压缩 · completed"));
        assert!(!rendered.contains("memoryCitation"));
        assert!(!rendered.contains("{\"id\":"));
    }

    #[test]
    fn status_priority_matches_python_terminal_and_waiting_rules() {
        let store = Arc::new(SqliteStore::in_memory().unwrap());
        let totp = TotpManager::new(store.clone(), "/tmp/nonexistent-rust-bridge-totp", 60);
        let space = synthetic_session_space("thread-status-rules", 42);

        let mut projection = ThreadProjection {
            status: Some("active".into()),
            turn_status: Some("completed".into()),
            ..ThreadProjection::default()
        };
        let rendered = status_text(&store, &space, Some(&projection), None, &totp);
        assert!(rendered.contains("⚪ 空闲"));
        assert!(!rendered.contains("🟢 执行中"));

        projection.turn_status = Some("inProgress".into());
        projection.active_flags = vec!["waitingOnUserInput".into()];
        let rendered = status_text(&store, &space, Some(&projection), None, &totp);
        assert!(rendered.contains("🟡 等待回答"));
        assert!(rendered.contains("⏳ 等待用户输入"));

        projection.active_flags = vec!["waitingOnApproval".into()];
        let rendered = status_text(&store, &space, Some(&projection), None, &totp);
        assert!(rendered.contains("🟡 等待审批"));
        assert!(rendered.contains("🛂 等待审批"));

        projection.active_flags.clear();
        projection.desired_mode = Some("plan".into());
        projection.observed_mode = Some("unknown".into());
        let rendered = status_text(&store, &space, Some(&projection), None, &totp);
        assert!(rendered.contains("Mode：unknown"));
        assert!(!rendered.contains("Mode：plan"));
    }

    #[test]
    fn terminal_guard_rejects_stale_active_thread_and_channel_is_compact() {
        let store = Arc::new(SqliteStore::in_memory().unwrap());
        let totp = TotpManager::new(store.clone(), "/tmp/nonexistent-rust-bridge-totp", 60);
        let space = synthetic_session_space("thread-terminal-guard", 42);
        let projection = ThreadProjection {
            thread_id: "thread-terminal-guard".into(),
            status: Some("active".into()),
            turn_status: Some("idle".into()),
            goal: Some(json!({"status":"complete"})),
            ..ThreadProjection::default()
        };
        assert!(!status_is_terminal(&space, Some(&projection)));

        let mut completed = projection.clone();
        completed.turn_status = Some("completed".into());
        assert!(status_is_terminal(&space, Some(&completed)));

        let channel = channel_status_render(&store, &space, Some(&completed), &totp, None).plain;
        assert!(channel.contains("🤖 Codex"));
        assert!(channel.contains("🎯 Goal"));
        assert!(!channel.contains("生命周期："));
        assert!(!channel.contains("🕘 近期事件"));
    }

    #[test]
    fn queue_callbacks_keep_the_record_session_scope() {
        let mut space = synthetic_session_space("thread-queue", 42);
        space.space_id = "space-queue".into();
        space.generation = 7;

        assert_eq!(
            queue_callback_scope(
                &json!({"space_id":"space-queue","generation":7}),
                Some(&space),
            ),
            Some(("space-queue".into(), 7))
        );
        assert_eq!(
            queue_callback_scope(&json!({}), Some(&space)),
            Some(("space-queue".into(), 7))
        );
        assert_eq!(queue_callback_scope(&json!({}), None), None);
    }

    #[test]
    fn moon_frame_appears_while_active_and_pins_full_moon_on_terminal() {
        let store = Arc::new(SqliteStore::in_memory().unwrap());
        let totp = TotpManager::new(store.clone(), "/tmp/nonexistent-rust-bridge-totp", 60);
        let space = synthetic_session_space("thread-moon", 42);
        let mut projection = ThreadProjection {
            thread_id: "thread-moon".into(),
            status: Some("active".into()),
            turn_status: Some("inProgress".into()),
            observed_mode: Some("default".into()),
            started_at_ms: Some(now_ms()),
            ..ThreadProjection::default()
        };
        let rendered = status_render(&store, &space, Some(&projection), None, &totp, Some(3));
        // The frame prefixes the mode header (Python `render_status_comment`),
        // never the title line.
        assert!(rendered.plain.contains("🌔 ⚙️ TUI Normal mode"));
        assert!(rendered.markdown.contains("🌔 *⚙️ TUI Normal mode*"));
        assert!(!rendered.plain.starts_with("🌔"));

        projection.turn_status = Some("completed".into());
        projection.goal = Some(json!({"status":"complete"}));
        let rendered = status_render(&store, &space, Some(&projection), None, &totp, Some(0));
        assert!(rendered.plain.contains("🌕 ⚙️ TUI Normal mode"));
        let channel = channel_status_render(&store, &space, Some(&projection), &totp, Some(0));
        assert!(channel.plain.starts_with("🌕 ") || channel.plain.contains("\n🌕 "));
    }

    #[test]
    fn mode_header_and_main_profile_prefer_space_persisted_values() {
        let store = Arc::new(SqliteStore::in_memory().unwrap());
        let totp = TotpManager::new(store.clone(), "/tmp/nonexistent-rust-bridge-totp", 60);
        let mut space = synthetic_session_space("thread-mode", 42);
        space.observed_mode = Some("default".into());
        space.normal_model = Some("gpt-5.6-terra".into());
        space.normal_effort = Some("low".into());
        // A legacy session without any projection still renders the mode
        // header from the durable space profile.
        let rendered = status_render(&store, &space, None, None, &totp, None);
        assert!(rendered.plain.contains("⚙️ TUI Normal mode"));
        assert!(
            rendered
                .plain
                .contains("🧠 Main · gpt-5.6-terra · Effort low")
        );
        assert!(rendered.markdown.contains("*⚙️ TUI Normal mode*"));

        space.observed_mode = Some("plan".into());
        space.plan_model = Some("gpt-5.6-sol".into());
        space.plan_effort = Some("high".into());
        let rendered = status_render(&store, &space, None, None, &totp, None);
        assert!(rendered.plain.contains("🧭 TUI Plan mode"));
        assert!(
            rendered
                .plain
                .contains("🧠 Main · gpt-5.6-sol · Effort high")
        );
    }

    #[test]
    fn subagents_section_and_cross_turn_duration_match_python_views() {
        let store = Arc::new(SqliteStore::in_memory().unwrap());
        let totp = TotpManager::new(store.clone(), "/tmp/nonexistent-rust-bridge-totp", 60);
        let space = synthetic_session_space("thread-agents", 42);
        let mut subagents = std::collections::BTreeMap::new();
        subagents.insert(
            "agent-1".to_owned(),
            json!({
                "task_id": "agent-1",
                "title": "调研 Rust 重写进度",
                "status": "inProgress",
                "agent_thread_id": "agent-1",
                "agent_path": "worker/parity",
                "model": "gpt-5.6-sol",
                "reasoning_effort": "max",
                "started_at": now_ms().div_euclid(1_000) - 90,
                "finished_at": 0,
                "updated_at": now_ms().div_euclid(1_000)
            }),
        );
        let projection = ThreadProjection {
            thread_id: "thread-agents".into(),
            status: Some("idle".into()),
            turn_status: Some("completed".into()),
            completed_turns_duration_ms: 3_660_000,
            subagents,
            ..ThreadProjection::default()
        };
        let rendered = status_render(&store, &space, Some(&projection), None, &totp, None);
        // Python models.py total_duration_ms: completed turns plus the
        // in-flight turn, so a finished 61-minute session renders 1h 01m.
        assert!(rendered.plain.contains("总执行 1h 01m"));
        assert!(rendered.plain.contains("🤝 Subagents"));
        assert!(rendered.plain.contains("🟢 worker/parity · 运行中"));
        assert!(rendered.plain.contains("调研 Rust 重写进度"));
        assert!(rendered.markdown.contains("*🤝 Subagents*"));
        // No plan steps yet: the Python "尚未创建计划" placeholder shows.
        assert!(rendered.plain.contains("尚未创建计划"));
    }

    #[test]
    fn goal_plan_inconsistency_warning_matches_python_views() {
        let store = Arc::new(SqliteStore::in_memory().unwrap());
        let totp = TotpManager::new(store.clone(), "/tmp/nonexistent-rust-bridge-totp", 60);
        let space = synthetic_session_space("thread-warning", 42);
        let projection = ThreadProjection {
            thread_id: "thread-warning".into(),
            status: Some("idle".into()),
            turn_status: Some("completed".into()),
            goal: Some(json!({"status":"complete","objective":"Ship"})),
            plan: Some(
                json!([{"step":"Inspect","status":"completed"},{"step":"Deploy","status":"pending"}]),
            ),
            ..ThreadProjection::default()
        };
        let rendered = status_render(&store, &space, Some(&projection), None, &totp, None);
        // Python `views.py`: markdown keeps the ⚠️ icon, plain uses WARNING:.
        assert!(rendered.plain.contains(
            "WARNING: Goal 已完成，但 Plan 仍有 1 项未完成；状态不一致，请先同步 Plan。"
        ));
        assert!(
            rendered
                .markdown
                .contains("⚠️ Goal 已完成，但 Plan 仍有 1 项未完成；状态不一致，请先同步 Plan。")
        );
    }

    #[test]
    fn markdown_track_escapes_entities_and_plain_track_stays_clean() {
        let store = Arc::new(SqliteStore::in_memory().unwrap());
        let totp = TotpManager::new(store.clone(), "/tmp/nonexistent-rust-bridge-totp", 60);
        let space = synthetic_session_space("thread-markdown", 42);
        let projection = ThreadProjection {
            thread_id: "thread-markdown".into(),
            title: Some("parity_(v2)".into()),
            status: Some("idle".into()),
            turn_status: Some("completed".into()),
            plan: Some(
                json!([{"step":"Inspect code","status":"completed"},{"step":"Deploy","status":"inProgress"}]),
            ),
            ..ThreadProjection::default()
        };
        let rendered = status_render(&store, &space, Some(&projection), None, &totp, None);
        assert!(rendered.markdown.contains("*🤖 Codex · parity\\_\\(v2\\)*"));
        assert!(rendered.markdown.contains("~1\\. Inspect code~"));
        assert!(rendered.markdown.contains("▶ *2\\. Deploy*"));
        assert!(!rendered.plain.contains('*'));
        assert!(!rendered.plain.contains('\\'));
    }

    #[test]
    fn legacy_luna_and_unknown_models_remap_to_terra_low() {
        let mut space = synthetic_session_space("thread-remap", 42);
        space.normal_model = Some("gpt-5.6-luna".into());
        space.normal_effort = Some("max".into());
        space.plan_model = Some("gpt-5.6-sol".into());
        space.plan_effort = Some("high".into());
        let available = vec![
            ModelChoice {
                model: "gpt-5.6-terra".into(),
                display_name: "GPT 5.6 Terra".into(),
                efforts: vec!["low".into(), "medium".into()],
                default_effort: "medium".into(),
            },
            ModelChoice {
                model: "gpt-5.6-sol".into(),
                display_name: "GPT 5.6 Sol".into(),
                efforts: vec!["high".into()],
                default_effort: "high".into(),
            },
        ];
        assert!(remap_legacy_session_models(&mut space, &available));
        assert_eq!(space.normal_model.as_deref(), Some("gpt-5.6-terra"));
        assert_eq!(space.normal_effort.as_deref(), Some("low"));
        // A model that is still advertised by model/list is left untouched.
        assert_eq!(space.plan_model.as_deref(), Some("gpt-5.6-sol"));
        assert_eq!(space.plan_effort.as_deref(), Some("high"));
        assert!(!remap_legacy_session_models(&mut space, &available));
    }

    #[test]
    fn ask_model_profile_defaults_and_toml_override() {
        let config = RustConfig::default();
        assert_eq!(config.ask_model, "gpt-5.6-terra");
        assert_eq!(config.ask_reasoning_effort, "medium");
        let parsed: RustConfig = toml::from_str("ask_model = \"gpt-5.6-sol\"\n").unwrap();
        assert_eq!(parsed.ask_model, "gpt-5.6-sol");
        assert_eq!(parsed.ask_reasoning_effort, "medium");
    }
}
