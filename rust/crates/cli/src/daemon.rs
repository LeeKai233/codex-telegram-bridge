//! Full Rust runtime orchestration.
//!
//! Telegram's blocking Bot API client is isolated in one polling thread per
//! update-owning Bot. The dispatcher stays on Tokio so Codex app-server calls,
//! notification projection, and shutdown are asynchronous and bounded.

use crate::alerts::AlertWebhookServer;
use crate::config::{BotConfig, RustConfig};
use crate::metrics::{MetricsRegistry, MetricsServer};
use crate::security::TotpManager;
use codex_telegram_adapter::{
    BotCapability, ChannelBinding, IncomingUpdate, LinkedDiscussion, NativeCommentBinding,
    ReqwestTransport, RoutedUpdate, RuntimeBotRole, SentMessage, TelegramBotApi,
    TelegramMessageReference, TelegramMessageRequest, TelegramSurfaceBinding, TokenLeaseRegistry,
    UpdateAuthorization, UpdateRouter, UpdateRoutingPolicy, WorkflowAction, WorkflowCommand,
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
use ctg_storage_sqlite::{NativeCommentRoot, RustSessionSpace, SqliteStore, StoredCallback};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
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
const UPLOAD_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;
static NEXT_APPROVAL_NONCE: AtomicU64 = AtomicU64::new(1);

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
}

struct InboundUpdate {
    bot_instance_id: String,
    update: codex_telegram_adapter::Update,
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
        bots.push(RuntimeBot {
            config: bot.clone(),
            role: role.unwrap_or(RuntimeBotRole::Alert),
            token,
            api: api.clone(),
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
    restore_active_sessions(&sessions, &store, &config, &bots_by_id)?;
    let totp = Arc::new(TotpManager::new(
        store.clone(),
        config.totp_secret_path.clone(),
        config.totp_unlock_seconds,
    ));
    for space in store
        .active_session_spaces()
        .map_err(|error| DaemonError::Store(error.to_string()))?
    {
        if let Err(error) = ensure_status_message(
            &store,
            &bots_by_id,
            &config,
            &metrics,
            totp.as_ref(),
            &space,
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
    let (updates_tx, mut updates_rx) = mpsc::unbounded_channel();
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
    ));
    let request_task = tokio::spawn(handle_server_requests(
        agent.clone(),
        store.clone(),
        sessions.clone(),
        bots_by_id.clone(),
        config.clone(),
        metrics.clone(),
    ));
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
    let dispatch_agent = agent.clone();
    let dispatch_task = tokio::spawn(async move {
        while let Some(inbound) = updates_rx.recv().await {
            let Some(bot) = bots_by_id.get(&inbound.bot_instance_id).cloned() else {
                continue;
            };
            let owner_user_id = store
                .workflow_record("onboarding", "owner")
                .ok()
                .flatten()
                .and_then(|value| value.get("user_id").and_then(Value::as_i64));
            let authorization = UpdateAuthorization {
                owner_user_id,
                bot_username: None,
                enforce_chat_kind: true,
                reject_sender_chat: true,
            };
            let actor_user_id = match IncomingUpdate::from_update(&inbound.update) {
                IncomingUpdate::Message(message) | IncomingUpdate::EditedMessage(message) => {
                    message.actor.user_id
                }
                IncomingUpdate::Callback(callback) => callback.actor.user_id,
                IncomingUpdate::Membership(membership) => membership.actor.user_id,
                IncomingUpdate::Unsupported => None,
            };
            let router =
                match UpdateRouter::new_with_authorization(bot.role, policy.clone(), authorization)
                {
                    Ok(router) => router,
                    Err(error) => {
                        eprintln!("rust bridge routing disabled: {error}");
                        continue;
                    }
                };
            let routed = router.route(&inbound.update);
            if let Err(error) = handle_action(
                routed,
                actor_user_id,
                bot,
                &bots_by_id,
                &config,
                &store,
                &dispatch_agent,
                &sessions,
                &metrics,
                &totp,
            )
            .await
            {
                eprintln!("rust bridge action failed: {error}");
            }
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
        }
        result = &mut dispatch_task => {
            result.map_err(|error| DaemonError::Task(error.to_string()))?;
            shutdown.store(true, Ordering::Release);
            if let Some(task) = &new_expiry_task {
                task.abort();
            }
        }
    }
    let _ = tokio::task::spawn_blocking(move || {
        for poller in pollers {
            let _ = poller.join();
        }
    })
    .await;
    event_task.abort();
    request_task.abort();
    agent.shutdown().await;
    Ok(())
}

fn restore_active_sessions(
    sessions: &SessionRegistry,
    store: &SqliteStore,
    config: &RustConfig,
    bots_by_id: &HashMap<String, RuntimeBot>,
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
        sessions.insert(SessionRecord {
            thread_id,
            turn_id: None,
            chat_id,
            root_message_id: space.discussion_root_message_id,
            sender_instance_id,
        });
        restored += 1;
    }
    eprintln!("rust bridge restored {restored} active session(s)");
    Ok(())
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
    updates_tx: mpsc::UnboundedSender<InboundUpdate>,
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
                    offset = Some(update.update_id.saturating_add(1));
                    match store.record_processed_update(
                        &bot.config.instance_id,
                        update.update_id,
                        now_ms(),
                    ) {
                        Ok(true) => {
                            if updates_tx
                                .send(InboundUpdate {
                                    bot_instance_id: bot.config.instance_id.clone(),
                                    update,
                                })
                                .is_err()
                            {
                                return;
                            }
                        }
                        Ok(false) => {}
                        Err(error) => {
                            eprintln!("rust bridge update state failed: {error}");
                        }
                    }
                }
                metrics.set_event_loop_lag_micros(processing_started.elapsed().as_micros() as u64);
            }
        })
        .expect("poller thread must start")
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
            if !totp
                .is_unlocked_for_space(
                    &root_message_id
                        .and_then(|root| {
                            store
                                .session_space_for_discussion_root(chat_id, root)
                                .ok()
                                .flatten()
                        })
                        .or_else(|| {
                            store
                                .pending_session_space_for_discussion(chat_id)
                                .ok()
                                .flatten()
                        })
                        .map(|space| space.space_id)
                        .unwrap_or_default(),
                    now_ms(),
                )
                .map_err(|error| error.to_string())?
            {
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
                )
                .await
                {
                    eprintln!("rust bridge status message provisioning failed: {error}");
                }
            }
            let sender = bots_by_id
                .values()
                .find(|bot| bot.role == RuntimeBotRole::Discussion)
                .unwrap_or(&inbound_bot);
            send_text(
                sender,
                &TelegramSurfaceBinding::NativeCommentRoot(
                    NativeCommentBinding::new(
                        ChannelBinding::new(
                            sender.config.instance_id.clone(),
                            config.channel_chat_id.to_string(),
                        )
                        .map_err(|error| error.message.to_owned())?,
                        config.discussion_chat_id.to_string(),
                        discussion_root_message_id,
                    )
                    .map_err(|error| error.message.to_owned())?,
                ),
                "Rust Bridge 已绑定这条 Channel 评论入口。",
                metrics,
            )
            .await
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
) -> Result<(), String> {
    let surface = surface_for(&inbound_bot, config, chat_id, None);
    let bound_space = if inbound_bot.role == RuntimeBotRole::Discussion {
        root_message_id
            .and_then(|root| {
                store
                    .session_space_for_discussion_root(chat_id, root)
                    .ok()
                    .flatten()
            })
            .or_else(|| {
                store
                    .pending_session_space_for_discussion(chat_id)
                    .ok()
                    .flatten()
            })
    } else {
        None
    };
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
            let state = agent.connection_state();
            send_text(
                &inbound_bot,
                &surface,
                &format!(
                    "Rust Bridge /perf\napp-server_connected={}\ngeneration={}\nmetrics=127.0.0.1:9465",
                    state.connected, state.generation
                ),
                metrics,
            )
            .await
        }
        WorkflowCommand::Sessions => {
            let query = text
                .split_whitespace()
                .skip(1)
                .collect::<Vec<_>>()
                .join(" ");
            let rendered = render_sessions_list(agent, &query).await?;
            send_text(&inbound_bot, &surface, &rendered, metrics).await
        }
        WorkflowCommand::Topics => {
            let spaces = store
                .active_session_spaces()
                .map_err(|error| error.to_string())?;
            let rendered = render_topics_list(&spaces);
            send_text(&inbound_bot, &surface, &rendered, metrics).await
        }
        WorkflowCommand::Help => {
            let help = match inbound_bot.role {
                RuntimeBotRole::Control => {
                    "/sessions [关键词]  查找 Codex sessions\n/topics  查看 Session 帖子\n/new  创建 Session\n/perf  查看 WSL 与 GPU 性能\n/help  显示帮助"
                }
                RuntimeBotRole::Discussion => {
                    "/status  查看当前 Session 状态\n/totp <code>  认证当前 Session\n/lock  锁定当前 Session\n/planmode on|off  切换 Plan Mode\n/changemodel <model> [effort]  切换模型\n/review [target]  启动 Review\n/cancel  取消当前 turn\n/getfile <relative-path>  发送 workspace 文件\n/help  查看命令\n直接发送文本  提交 Codex Prompt"
                }
                RuntimeBotRole::Status | RuntimeBotRole::Alert => "当前 Bot 没有可用命令。",
            };
            send_text(&inbound_bot, &surface, help, metrics).await
        }
        WorkflowCommand::Totp => {
            let code = text.split_whitespace().nth(1).unwrap_or_default();
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
                "当前 Session 已解锁。"
            } else {
                "验证码无效、已使用，或验证暂时锁定。"
            };
            send_text(&inbound_bot, &surface, message, metrics).await
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
            let markup = status_callback_markup(
                store,
                &space,
                &[
                    ("确认取消关注", "status_unwatch_execute"),
                    ("返回", "status_unwatch_cancel"),
                ],
            )?;
            send_text_with_markup(
                &inbound_bot,
                &surface_for(&inbound_bot, config, chat_id, session.root_message_id),
                "确认取消关注？评论历史会保留，但此评论串将永久只读。",
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
    let mut stored = serde_json::Map::new();
    let mut rows = Vec::new();
    for (event, value, label) in choices {
        let nonce = next_approval_nonce();
        stored.insert(nonce.clone(), json!({"event": event, "value": value}));
        rows.push(vec![json!({
            "text": truncate_text(label),
            "callback_data": format!("new:{nonce}"),
        })]);
    }
    draft["choices"] = Value::Object(stored);
    store
        .upsert_workflow_record("new", key, draft, now_ms())
        .map_err(|error| error.to_string())?;
    Ok((!rows.is_empty()).then(|| json!({"inline_keyboard": rows})))
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
    }
    let args = new_command_arguments(text);
    if args.is_empty() {
        let models = list_model_choices(agent).await?;
        let mut draft = new_draft(
            chat_id,
            user_id,
            "normal_model",
            json!({"channel_post_id": message_id.max(1)}),
        );
        let choices = models
            .entries
            .iter()
            .map(|entry| {
                (
                    "normal_model".to_owned(),
                    entry.model.clone(),
                    format!("{} · {}", entry.display_name, entry.model),
                )
            })
            .collect::<Vec<_>>();
        let markup = new_choice_markup(store, &key, &mut draft, &choices)?;
        return send_text_with_markup(
            bot,
            &surface_for(bot, config, chat_id, None),
            "请选择普通模式模型：",
            markup,
            metrics,
        )
        .await;
    }
    let parsed = parse_new_arguments(text)?;
    let models = list_model_choices(agent).await?;
    let Some(normal) = model_choice(&models, &parsed.model) else {
        return send_text(
            bot,
            &surface_for(bot, config, chat_id, None),
            &format!("模型 {} 不在当前可用列表中。", parsed.model),
            metrics,
        )
        .await;
    };
    if !normal.efforts.iter().any(|value| value == &parsed.effort) {
        return send_text(
            bot,
            &surface_for(bot, config, chat_id, None),
            &format!(
                "effort {} 不适用于 {}；可用：{}",
                parsed.effort,
                normal.model,
                normal.efforts.join(", ")
            ),
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
            return send_text(
                bot,
                &surface_for(bot, config, chat_id, None),
                "Plan model 不可用。",
                metrics,
            )
            .await;
        };
        if !plan.efforts.iter().any(|value| value == plan_effort) {
            return send_text(
                bot,
                &surface_for(bot, config, chat_id, None),
                "Plan effort 不适用。",
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
        store
            .upsert_workflow_record("new", &key, &draft, now_ms())
            .map_err(|error| error.to_string())?;
        return finish_new_project(
            store, agent, sessions, bot, bots_by_id, config, metrics, &key, draft,
        )
        .await;
    }
    if phase == "plan_choice" {
        let choices = vec![
            ("plan_choice".into(), "yes".into(), "进入 Plan Mode".into()),
            ("plan_choice".into(), "no".into(), "不进入 Plan Mode".into()),
        ];
        let markup = new_choice_markup(store, &key, &mut draft, &choices)?;
        send_text_with_markup(
            bot,
            &surface_for(bot, config, chat_id, None),
            "是否进入 Plan Mode？",
            markup,
            metrics,
        )
        .await
    } else {
        store
            .upsert_workflow_record("new", &key, &draft, now_ms())
            .map_err(|error| error.to_string())?;
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
    let Some(choice) = draft
        .get("choices")
        .and_then(Value::as_object)
        .and_then(|choices| choices.get(nonce))
        .cloned()
    else {
        acknowledge_callback(&bot, &callback, Some("选择已处理")).await;
        return Ok(());
    };
    let event = choice
        .get("event")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let value = choice
        .get("value")
        .and_then(Value::as_str)
        .unwrap_or_default();
    acknowledge_callback(&bot, &callback, Some("已收到")).await;
    draft["choices"] = json!({});
    let payload = draft.get("payload").cloned().unwrap_or_else(|| json!({}));
    match event {
        "cancel" => {
            store
                .delete_workflow_record("new", &key)
                .map_err(|error| error.to_string())?;
            edit_text_with_markup(
                &bot,
                &TelegramMessageReference::new(callback.chat_id.to_string(), callback.message_id)
                    .map_err(|error| error.to_string())?,
                "已退出 /new。",
                None,
                metrics,
            )
            .await
        }
        "normal_model" => {
            let models = list_model_choices(agent).await?;
            let Some(model) = model_choice(&models, value) else {
                return send_text(&bot, &surface_for(&bot, config, callback.chat_id, None), "模型已不可用，请重新执行 /new。", metrics).await;
            };
            let next = advance_new_draft(store, &key, &draft, "normal_effort", json!({"normal_model": model.model}), false)?;
            let mut next = next;
            let choices = model.efforts.iter().map(|effort| ("normal_effort".into(), effort.clone(), format!("effort · {effort}"))).collect::<Vec<_>>();
            let markup = new_choice_markup(store, &key, &mut next, &choices)?;
            send_text_with_markup(&bot, &surface_for(&bot, config, callback.chat_id, None), "请选择普通模式 effort：", markup, metrics).await
        }
        "normal_effort" => {
            let mut next_payload = payload;
            next_payload["normal_effort"] = Value::String(value.to_owned());
            let next = advance_new_draft(store, &key, &draft, "plan_choice", next_payload, false)?;
            let mut next = next;
            let choices = vec![("plan_choice".into(), "yes".into(), "进入 Plan Mode".into()), ("plan_choice".into(), "no".into(), "不进入 Plan Mode".into())];
            let markup = new_choice_markup(store, &key, &mut next, &choices)?;
            send_text_with_markup(&bot, &surface_for(&bot, config, callback.chat_id, None), "是否进入 Plan Mode？", markup, metrics).await
        }
        "plan_choice" => {
            if value == "yes" {
                let next = advance_new_draft(store, &key, &draft, "plan_model", payload, false)?;
                let models = list_model_choices(agent).await?;
                let mut next = next;
                let choices = models.entries.iter().map(|entry| ("plan_model".into(), entry.model.clone(), format!("{} · {}", entry.display_name, entry.model))).collect::<Vec<_>>();
                let markup = new_choice_markup(store, &key, &mut next, &choices)?;
                send_text_with_markup(&bot, &surface_for(&bot, config, callback.chat_id, None), "请选择 Plan Mode 模型：", markup, metrics).await
            } else {
                let next = advance_new_draft(store, &key, &draft, "project", payload, false)?;
                send_text(&bot, &surface_for(&bot, config, callback.chat_id, None), "请发送项目地址或项目描述；下一条文本消息会被识别为项目。", metrics).await.map(|_| { let _ = next; })
            }
        }
        "plan_model" => {
            let models = list_model_choices(agent).await?;
            let Some(model) = model_choice(&models, value) else {
                return send_text(&bot, &surface_for(&bot, config, callback.chat_id, None), "Plan model 已不可用，请重新执行 /new。", metrics).await;
            };
            let next = advance_new_draft(store, &key, &draft, "plan_effort", json!({"normal_model": payload.get("normal_model").cloned().unwrap_or(Value::Null), "normal_effort": payload.get("normal_effort").cloned().unwrap_or(Value::Null), "plan_model": model.model}), false)?;
            let mut next = next;
            let choices = model.efforts.iter().map(|effort| ("plan_effort".into(), effort.clone(), format!("effort · {effort}"))).collect::<Vec<_>>();
            let markup = new_choice_markup(store, &key, &mut next, &choices)?;
            send_text_with_markup(&bot, &surface_for(&bot, config, callback.chat_id, None), "请选择 Plan Mode effort：", markup, metrics).await
        }
        "plan_effort" => {
            let mut next_payload = payload;
            next_payload["plan_effort"] = Value::String(value.to_owned());
            let next = advance_new_draft(store, &key, &draft, "project", next_payload, false)?;
            send_text(&bot, &surface_for(&bot, config, callback.chat_id, None), "请发送项目地址或项目描述；下一条文本消息会被识别为项目。", metrics).await.map(|_| { let _ = next; })
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
        "hello" => finish_new_prompt(store, agent, sessions, &bot, bots_by_id, config, metrics, &key, draft, "Hello".into()).await,
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
        "project" | "project_choice" => {
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

fn new_existing_projects(root: &Path, value: &str) -> Result<Vec<PathBuf>, String> {
    let root =
        fs::canonicalize(root).map_err(|error| format!("workspace 根目录不可用: {error}"))?;
    let candidate = Path::new(value);
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
        return Ok(vec![
            fs::canonicalize(direct).map_err(|error| error.to_string())?,
        ]);
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
    let raw = PathBuf::from(value.trim());
    if raw.is_absolute() && !raw.exists() {
        let root = fs::canonicalize(&config.workspace_root).map_err(|error| error.to_string())?;
        if raw.strip_prefix(&root).is_ok() {
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
                "目录不存在，是否创建？",
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
        "没有找到匹配项目。请发送 workspace 内的明确目录路径。",
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
            store, agent, sessions, bot, bots_by_id, config, metrics, key, draft, prompt,
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
        "请发送第一条 prompt。30 秒内未发送时将使用 Hello。",
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
) -> Result<(RustSessionSpace, SentMessage), String> {
    let cwd = payload
        .get("cwd")
        .and_then(Value::as_str)
        .ok_or_else(|| "项目目录未选择".to_owned())?;
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
    let message = send_text_message(
        bot,
        &TelegramSurfaceBinding::Channel(channel),
        &channel_text,
        metrics,
    )
    .await?;
    let now = now_ms();
    let space_id = format!("telegram-pending-{}", next_approval_nonce());
    let space = RustSessionSpace {
        space_id: space_id.clone(),
        thread_id: None,
        lifecycle: "pending".into(),
        generation,
        channel_chat_id: config.channel_chat_id,
        channel_post_id: message.message_id.max(1),
        discussion_chat_id: None,
        discussion_root_message_id: None,
        status_message_id: None,
        status_bot_instance: None,
        owner_chat_id: Some(owner_chat_id),
        plan_mode: plan_model.is_some(),
        created_at_ms: now,
        updated_at_ms: now,
    };
    store
        .upsert_session_space(&space)
        .map_err(|error| error.to_string())?;
    let mut pending = payload.clone();
    pending["space_id"] = Value::String(space_id.clone());
    pending["pending_cwd"] = Value::String(cwd.to_owned());
    pending["pending_prompt"] = Value::String(prompt.to_owned());
    store
        .upsert_workflow_record("pending_space", &space_id, &pending, now)
        .map_err(|error| error.to_string())?;
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
    let client_message_id = format!("telegram-new-{}-{}", repair.space_id, next_approval_nonce());
    let mut intent = PromptIntent {
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
    };
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
        store, bots_by_id, config, metrics, totp, &active, None, None,
    )
    .await?;
    send_text(
        bot,
        &surface_for(
            bot,
            config,
            discussion_chat_id,
            active.discussion_root_message_id,
        ),
        &format!("已创建 Session {}，首条 prompt 已提交。", thread.id),
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
) -> Result<(), String> {
    let chat_id = draft["chat_id"]
        .as_i64()
        .ok_or_else(|| "new draft chat_id missing".to_owned())?;
    if !store
        .delete_workflow_record("new", key)
        .map_err(|error| error.to_string())?
    {
        return Ok(());
    }
    let payload = draft.get("payload").cloned().unwrap_or_else(|| json!({}));
    let (space, channel_post) = match create_pending_session_space(
        store,
        bot,
        config,
        metrics,
        chat_id,
        i64::try_from(agent.connection_state().generation).unwrap_or(0),
        &payload,
        &prompt,
    )
    .await
    {
        Ok(value) => value,
        Err(error) => {
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
    send_text(
        bot,
        &surface_for(bot, config, chat_id, None),
        &format!(
            "待认证 Session 帖子已创建。请进入评论串并发送 /totp <验证码>。\n{}",
            telegram_message_link(space.channel_chat_id, channel_post.message_id)
        ),
        metrics,
    )
    .await
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
                )
                .await
                {
                    eprintln!("rust bridge /new Hello expiry failed: {error}");
                }
            } else if let Err(error) = store.delete_workflow_record("new", &key) {
                eprintln!("rust bridge /new draft cleanup failed: {error}");
            }
        }
    }
}

async fn handle_pair_command(
    text: &str,
    chat_id: i64,
    actor_user_id: Option<i64>,
    bot: &RuntimeBot,
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
    store
        .upsert_workflow_record(
            "queue",
            &id,
            &json!({
                "queue_id": id,
                "thread_id": session.thread_id.as_str(),
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
        store
            .create_callback(&StoredCallback {
                nonce: callback_nonce.clone(),
                space_id: format!("telegram-{}", session.chat_id),
                generation: 0,
                action: key.clone(),
                expires_at_ms: now_ms() + APPROVAL_CALLBACK_TTL_MS,
            })
            .map_err(|error| error.to_string())?;
        rows.push(vec![json!({"text": format!("取消 {visible}"), "callback_data": format!("qcancel:{callback_nonce}")})]);
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
            &id[..id.len().min(8)],
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

async fn render_sessions_list(agent: &AppServerClient, query: &str) -> Result<String, String> {
    const PAGE_SIZE: usize = 5;
    let response = agent
        .list_threads(1000, None)
        .await
        .map_err(|error| error.to_string())?;
    let entries = response
        .get("data")
        .or_else(|| response.get("threads"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|thread| {
            !thread
                .get("ephemeral")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
        .filter(|thread| {
            if query.trim().is_empty() {
                return true;
            }
            let needle = query.to_ascii_lowercase();
            [
                "id",
                "title",
                "name",
                "preview",
                "summary",
                "naturalSummary",
            ]
            .iter()
            .filter_map(|field| thread.get(*field).and_then(Value::as_str))
            .any(|value| value.to_ascii_lowercase().contains(&needle))
        })
        .collect::<Vec<_>>();

    let total_pages = entries.len().div_ceil(PAGE_SIZE).max(1);
    let mut lines = vec![format!("🤖 Codex Sessions · 1/{total_pages}")];
    if !query.trim().is_empty() {
        lines.push(format!("搜索 {}", query.trim()));
    }
    if entries.is_empty() {
        lines.push(String::new());
        lines.push("当前没有 Codex session。".into());
        return Ok(lines.join("\n"));
    }
    for (index, thread) in entries.iter().take(PAGE_SIZE).enumerate() {
        let id = thread
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let summary = ["naturalSummary", "summary", "title", "name", "preview"]
            .iter()
            .find_map(|field| thread.get(*field).and_then(Value::as_str))
            .unwrap_or("Codex session");
        let status = thread
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let cwd = ["cwd", "path"]
            .iter()
            .find_map(|field| thread.get(*field).and_then(Value::as_str))
            .unwrap_or("-");
        lines.push(String::new());
        lines.push(format!(
            "{} {} {}\n📝 {}\n📁 {}\n状态 {}",
            ["①", "②", "③", "④", "⑤"][index],
            status,
            id,
            truncate_text(summary),
            cwd,
            status
        ));
    }
    if entries.len() > PAGE_SIZE {
        lines.push(String::new());
        lines.push(format!(
            "共 {} 个；当前显示前 {} 个。",
            entries.len(),
            PAGE_SIZE
        ));
    }
    Ok(truncate_text(&lines.join("\n")))
}

fn render_topics_list(spaces: &[RustSessionSpace]) -> String {
    if spaces.is_empty() {
        return "当前没有 Session 帖子。".into();
    }
    let mut lines = vec!["🤖 Session 帖子".into()];
    for (index, space) in spaces.iter().take(30).enumerate() {
        lines.push(format!(
            "{}. {} · {} · thread={} · discussion_root={}",
            index + 1,
            space.space_id,
            space.lifecycle,
            space.thread_id.as_deref().unwrap_or("-"),
            space
                .discussion_root_message_id
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".into())
        ));
    }
    truncate_text(&lines.join("\n"))
}

async fn forward_codex_events(
    agent: AppServerClient,
    store: Arc<SqliteStore>,
    sessions: Arc<SessionRegistry>,
    bots_by_id: HashMap<String, RuntimeBot>,
    config: RustConfig,
    metrics: MetricsRegistry,
    totp: Arc<TotpManager>,
) {
    let mut events = agent.subscribe_events();
    let mut projector = EventProjector::default();
    loop {
        let event = match events.recv().await {
            Ok(event) => event,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        };
        if event.method.ends_with("/delta") {
            continue;
        }
        let effect = projector.apply(&event);
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
                )
                .await
            {
                eprintln!("rust bridge terminal status update failed: {error}");
            }
            let _ = dispatch_next_queued(
                &store,
                &agent,
                &sessions,
                &session,
                &bots_by_id,
                &config,
                &metrics,
            )
            .await;
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
                )
                .await;
            }
            let _ = dispatch_next_queued(
                &store,
                &agent,
                &sessions,
                &session,
                &bots_by_id,
                &config,
                &metrics,
            )
            .await;
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
            if let Err(error) = update_status_message(
                &store,
                &bots_by_id,
                &config,
                &metrics,
                totp.as_ref(),
                &space,
                Some(projection),
                None,
            )
            .await
            {
                eprintln!("rust bridge status update failed: {error}");
            }
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
) -> Result<(), String> {
    let root_message_id = sessions
        .by_chat(callback.chat_id)
        .and_then(|session| session.root_message_id);
    let inbound_surface = surface_for(&inbound_bot, config, callback.chat_id, root_message_id);
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
        acknowledge_callback(&inbound_bot, &callback, Some("正在取消")).await;
        let Some(stored) = store
            .take_callback(nonce, now_ms())
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
        if stored.space_id.is_empty() {
            return send_text(&inbound_bot, &inbound_surface, "队列项无效。", metrics).await;
        }
        let key = stored.action;
        let Some(mut entry) = store
            .workflow_record("queue", &key)
            .map_err(|error| error.to_string())?
        else {
            return send_text(&inbound_bot, &inbound_surface, "队列项不存在。", metrics).await;
        };
        if entry.get("status").and_then(Value::as_str) != Some("queued") {
            return send_text(&inbound_bot, &inbound_surface, "队列项已经处理。", metrics).await;
        }
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
    let approval_space_id = store
        .peek_callback(nonce, now_ms())
        .map_err(|error| error.to_string())?
        .map(|stored| stored.space_id)
        .unwrap_or_default();
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
        .take_callback(nonce, now_ms())
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
    if let Err(error) = approval.decide(decision, now_ms()) {
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
    store
        .decide_approval(&approval, &event)
        .map_err(|error| error.to_string())?;
    if let Err(error) = agent
        .respond(action.request_id.clone(), action.response_payload())
        .await
    {
        acknowledge_callback(&inbound_bot, &callback, Some("Codex 未接受响应")).await;
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
    if store
        .workflow_record("onboarding", "owner")
        .map_err(|error| error.to_string())?
        .and_then(|value| value.get("user_id").and_then(Value::as_i64))
        .is_none()
    {
        return Ok(None);
    }
    store
        .retire_status_callbacks(space.space_id.as_str(), space.generation)
        .map_err(|error| error.to_string())?;
    let mut buttons = Vec::with_capacity(actions.len());
    for (label, action) in actions {
        let nonce = format!("status-{}", next_approval_nonce());
        let stored = StoredStatusAction {
            space_id: space.space_id.clone(),
            generation: u64::try_from(space.generation)
                .map_err(|_| "status generation is negative".to_owned())?,
            thread_id: space.thread_id.clone().unwrap_or_default(),
            action: (*action).to_owned(),
        };
        store
            .create_callback(&StoredCallback {
                nonce: nonce.clone(),
                space_id: space.space_id.clone(),
                generation: space.generation,
                action: serde_json::to_string(&stored).map_err(|error| error.to_string())?,
                expires_at_ms: now_ms() + APPROVAL_CALLBACK_TTL_MS,
            })
            .map_err(|error| error.to_string())?;
        buttons.push(json!({
            "text": *label,
            "callback_data": format!("cb:{nonce}"),
        }));
    }
    Ok((!buttons.is_empty()).then(|| json!({"inline_keyboard": [buttons]})))
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

fn status_text(
    store: &SqliteStore,
    space: &RustSessionSpace,
    projection: Option<&ThreadProjection>,
    note: Option<&str>,
    totp: &TotpManager,
) -> String {
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
    let (status_icon, status_label) = if lifecycle == "closed" {
        ("⚫", "已关闭")
    } else if last_error.is_some() || raw_status == "systemError" || turn_status == "failed" {
        ("🔴", "错误")
    } else if turn_status == "inProgress" || raw_status == "active" {
        ("🟢", "执行中")
    } else if turn_status == "completed" || turn_status == "interrupted" || raw_status == "idle" {
        ("⚪", "空闲")
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
    let queue = store
        .workflow_records("queue")
        .unwrap_or_default()
        .into_iter()
        .filter(|(_, value)| {
            value.get("thread_id").and_then(Value::as_str) == Some(thread_id)
                && value.get("status").and_then(Value::as_str) == Some("queued")
        })
        .count();
    let auth = match totp.space_unlock_remaining_ms(&space.space_id, now_ms()) {
        Ok(remaining_ms) if remaining_ms > 0 => format!(
            "🔓 TOTP 已认证 · 剩余 {} min",
            ((remaining_ms + 59_999) / 60_000).max(1)
        ),
        _ => "🔒 TOTP 未认证".to_owned(),
    };
    let mode = projection
        .and_then(|value| value.observed_mode.as_deref())
        .or_else(|| projection.and_then(|value| value.desired_mode.as_deref()))
        .unwrap_or("unknown");
    let mut lines = vec![
        format!("🤖 Codex · {}", truncate_text(title)),
        format!(
            "{} · {} {} · Turn {}",
            truncate_text(thread_id),
            status_icon,
            status_label,
            turn_status
        ),
        format!("生命周期：{} · Mode：{}", lifecycle, mode),
        format!(
            "🎯 Goal · {} · {}",
            goal_status,
            truncate_text(goal_objective)
        ),
        format!("🧭 Plan · {completed}/{plan_total}"),
    ];
    if let Some(pending) = pending_payload.as_ref() {
        if let Some(cwd) = pending
            .get("pending_cwd")
            .or_else(|| pending.get("cwd"))
            .and_then(Value::as_str)
        {
            lines.push(format!("📁 项目 · {}", truncate_text(cwd)));
        }
        if let Some(prompt) = pending
            .get("pending_prompt")
            .or_else(|| pending.get("prompt"))
            .and_then(Value::as_str)
        {
            lines.push(format!("📝 首条 prompt · {}", truncate_text(prompt)));
        }
        lines.push("🔐 待认证 · 在评论串发送 /totp <验证码>".into());
    }
    for (index, step) in steps.iter().take(14).enumerate() {
        let (step_text, step_status) = status_step_value(step);
        let marker = match step_status.as_str() {
            "completed" => "✅",
            "inProgress" => "▶",
            "blocked" => "⏸",
            "failed" => "❌",
            _ => "○",
        };
        lines.push(format!(
            "{marker} {}. {}",
            index + 1,
            truncate_text(&step_text)
        ));
    }
    if plan_total > 14 {
        lines.push(format!("… 另有 {} 项，请使用 /plan 查看", plan_total - 14));
    }
    lines.push(format!(
        "🧩 Agent Tasks · {}/{} · Running {} · Failed {}",
        tasks.saturating_sub(active_tasks),
        tasks,
        active_tasks,
        failed_tasks
    ));
    lines.push(format!("📥 Queue · {queue}"));
    if let Some(error) = last_error.filter(|value| !value.trim().is_empty()) {
        lines.push(format!("❌ 错误 · {}", truncate_text(error)));
    }
    if let Some(projection) = projection {
        let recent = projection
            .items
            .values()
            .rev()
            .take(4)
            .filter_map(|item| {
                let kind = item.get("type").and_then(Value::as_str)?;
                Some(format!("{} · {}", kind, truncate_text(&item.to_string())))
            })
            .collect::<Vec<_>>();
        if !recent.is_empty() {
            lines.push("🕘 近期事件".into());
            lines.extend(recent);
        }
    }
    lines.push(auth);
    lines.push(format!(
        "🕒 更新 · generation {}",
        projection.map_or(0, |value| value.generation)
    ));
    let mut text = lines.join("\n");
    if let Some(note) = note.filter(|value| !value.trim().is_empty()) {
        text.push_str("\n\n");
        text.push_str(note);
    }
    truncate_text(&text)
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

fn channel_status_text(
    store: &SqliteStore,
    space: &RustSessionSpace,
    projection: Option<&ThreadProjection>,
    totp: &TotpManager,
) -> String {
    let full = status_text(store, space, projection, None, totp);
    let mut text = full;
    if text.len() > 950 {
        let mut end = 950;
        while end > 0 && !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push_str("\n…");
    }
    text
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

async fn ensure_status_message(
    store: &SqliteStore,
    bots_by_id: &HashMap<String, RuntimeBot>,
    config: &RustConfig,
    metrics: &MetricsRegistry,
    totp: &TotpManager,
    space: &RustSessionSpace,
    projection: Option<&ThreadProjection>,
) -> Result<Option<RustSessionSpace>, String> {
    if space.status_message_id.is_some()
        || space.discussion_chat_id.is_none()
        || space.discussion_root_message_id.is_none()
    {
        return Ok(Some(space.clone()));
    }
    let Some(bot) = status_bot_for(space, bots_by_id) else {
        return Ok(None);
    };
    let markup = if status_is_terminal(space, projection) {
        None
    } else {
        status_callback_markup(
            store,
            space,
            &[("刷新", "space_refresh"), ("取消关注", "space_unwatch")],
        )?
    };
    let message = send_text_with_markup_message(
        bot,
        &surface_for(
            bot,
            config,
            space.discussion_chat_id.expect("checked above"),
            space.discussion_root_message_id,
        ),
        &status_text(store, space, projection, None, totp),
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
) -> Result<(), String> {
    let Some(current) =
        ensure_status_message(store, bots_by_id, config, metrics, totp, space, projection).await?
    else {
        return Ok(());
    };
    let Some(message_id) = current.status_message_id else {
        return Ok(());
    };
    let Some(bot) = status_bot_for(&current, bots_by_id) else {
        return Ok(());
    };
    if let Some(control) = bots_by_id
        .values()
        .find(|candidate| candidate.role == RuntimeBotRole::Control)
        && current.channel_post_id > 0
    {
        let reference = TelegramMessageReference::new(
            current.channel_chat_id.to_string(),
            current.channel_post_id,
        )
        .map_err(|error| error.to_string())?;
        if let Err(error) = edit_text_message(
            control,
            &reference,
            &channel_status_text(store, &current, projection, totp),
            metrics,
        )
        .await
        {
            eprintln!("rust bridge channel dashboard update failed: {error}");
        }
    }
    let markup = if status_is_terminal(&current, projection) {
        None
    } else {
        status_callback_markup(
            store,
            &current,
            &[("刷新", "space_refresh"), ("取消关注", "space_unwatch")],
        )?
    };
    edit_text_with_markup(
        bot,
        &TelegramMessageReference::new(
            current
                .discussion_chat_id
                .unwrap_or(config.discussion_chat_id)
                .to_string(),
            message_id,
        )
        .map_err(|error| error.to_string())?,
        &status_text(store, &current, projection, note, totp),
        markup,
        metrics,
    )
    .await
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
    let session = if action.thread_id.trim().is_empty() {
        None
    } else {
        sessions.by_thread(&action.thread_id)
    };
    let expected_chat_id = session
        .as_ref()
        .map(|value| value.chat_id)
        .or(space.discussion_chat_id)
        .unwrap_or(callback.chat_id);
    if preview.space_id != action.space_id || callback.chat_id != expected_chat_id {
        acknowledge_callback(&inbound_bot, &callback, Some("按钮不属于当前 Session")).await;
        return Ok(());
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
    let Some(bot) = status_bot_for(&space, bots_by_id).or(Some(&inbound_bot)) else {
        return Ok(());
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
            )
            .await
        }
        "space_unwatch" => {
            let markup = status_callback_markup(
                store,
                &space,
                &[
                    ("确认取消关注", "status_unwatch_execute"),
                    ("返回", "status_unwatch_cancel"),
                ],
            )?;
            acknowledge_callback(&inbound_bot, &callback, Some("请确认")).await;
            edit_text_with_markup(
                bot,
                &TelegramMessageReference::new(callback.chat_id.to_string(), callback.message_id)
                    .map_err(|error| error.to_string())?,
                "确认取消关注？评论历史会保留，但此评论串将永久只读。",
                markup,
                metrics,
            )
            .await
        }
        "status_unwatch_cancel" => {
            acknowledge_callback(&inbound_bot, &callback, Some("已取消")).await;
            update_status_message(
                store,
                bots_by_id,
                config,
                metrics,
                totp,
                &space,
                None,
                Some("已取消操作。"),
            )
            .await
        }
        "status_unwatch_execute" => {
            let mut closed = space.clone();
            closed.lifecycle = "closed".into();
            closed.updated_at_ms = now_ms();
            store
                .upsert_session_space(&closed)
                .map_err(|error| error.to_string())?;
            acknowledge_callback(&inbound_bot, &callback, Some("已取消关注")).await;
            edit_text_with_markup(
                bot,
                &TelegramMessageReference::new(callback.chat_id.to_string(), callback.message_id)
                    .map_err(|error| error.to_string())?,
                "已取消关注。评论历史已保留，此评论串现为只读。",
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
            update_plan_publication(store, current, PlanPublicationState::Executing, None)?;
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
            update_plan_publication(
                store,
                current,
                PlanPublicationState::Executing,
                Some(turn.id.clone()),
            )?;
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
            update_plan_publication(store, current, PlanPublicationState::Revising, None)?;
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
                    update_plan_publication(store, current, PlanPublicationState::Revising, None)?;
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
        let request = match requests.recv().await {
            Ok(request) => request,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
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
    fn new_prompt_drafts_use_a_short_prompt_timeout() {
        let created_before = now_ms();
        let draft = new_draft(42, 7, "prompt", json!({"cwd":"/workspace"}));
        assert!(
            draft["expires_at_ms"].as_i64().unwrap() - created_before <= NEW_INTERACTION_TTL_MS
        );
        let advanced_before = now_ms();
        let advanced = advance_new_draft(
            &SqliteStore::in_memory().unwrap(),
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
}
