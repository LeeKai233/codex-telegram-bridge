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
    BotCapability, ChannelBinding, LinkedDiscussion, NativeCommentBinding, ReqwestTransport,
    RoutedUpdate, RuntimeBotRole, TelegramBotApi, TelegramSurfaceBinding, TokenLeaseRegistry,
    UpdateRouter, UpdateRoutingPolicy, WorkflowAction, WorkflowCommand,
};
use codex_telegram_credentials::BotToken;
use ctg_app_server::{AppServerClient, AppServerConfig};
use ctg_domain::{
    AgentServerRequest, AgentTurn, ApprovalAction, ApprovalDecision, ApprovalId, ApprovalRequest,
    Artifact, ArtifactId, DomainEvent, DomainEventKind, EventId, PromptInput, Session, SessionId,
    ThreadId, TurnId,
};
use ctg_ports::{AgentBackend, ApprovalStore, ArtifactStore, SessionRepository};
use ctg_storage_sqlite::{NativeCommentRoot, RustSessionSpace, SqliteStore, StoredCallback};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::mpsc;

const APP_SERVER_WAIT: Duration = Duration::from_secs(30);
const APPROVAL_CALLBACK_TTL_MS: i64 = 15 * 60 * 1000;
const MAX_ARTIFACT_BYTES: u64 = 10 * 1024 * 1024;
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
        sessions.clone(),
        bots_by_id.clone(),
        config.clone(),
        metrics.clone(),
    ));
    let request_task = tokio::spawn(handle_server_requests(
        agent.clone(),
        store.clone(),
        sessions.clone(),
        bots_by_id.clone(),
        config.clone(),
        metrics.clone(),
    ));
    let dispatch_agent = agent.clone();
    let dispatch_task = tokio::spawn(async move {
        while let Some(inbound) = updates_rx.recv().await {
            let Some(bot) = bots_by_id.get(&inbound.bot_instance_id).cloned() else {
                continue;
            };
            let router = match UpdateRouter::new(bot.role, policy.clone()) {
                Ok(router) => router,
                Err(error) => {
                    eprintln!("rust bridge routing disabled: {error}");
                    continue;
                }
            };
            let routed = router.route(&inbound.update);
            if let Err(error) = handle_action(
                routed,
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
        }
        result = &mut dispatch_task => {
            result.map_err(|error| DaemonError::Task(error.to_string()))?;
            shutdown.store(true, Ordering::Release);
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
            text,
        } => {
            if !command.allowed_for_role(inbound_bot.role) {
                return Ok(());
            }
            handle_command(
                command,
                chat_id,
                message_id,
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
            if !totp
                .is_unlocked(now_ms())
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
            let prompt = PromptInput::text(text).map_err(|error| error.to_string())?;
            let turn = if let Some(turn_id) = session.turn_id {
                agent
                    .steer_turn(
                        &session.thread_id,
                        &turn_id,
                        vec![prompt],
                        Some(&format!("telegram-{message_id}")),
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
                        vec![prompt],
                        Some(&format!("telegram-{message_id}")),
                    )
                    .await
            }
            .map_err(|error| error.to_string())?;
            sessions.set_turn(turn.thread_id.as_str(), Some(turn.id.clone()));
            send_text(
                &inbound_bot,
                &surface_for(&inbound_bot, config, chat_id, root_message_id),
                "Codex 已接收请求，完成后会回传结果。",
                metrics,
            )
            .await
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
    chat_id: i64,
    message_id: i64,
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
    if matches!(
        command,
        WorkflowCommand::PlanMode
            | WorkflowCommand::ChangeModel
            | WorkflowCommand::Review
            | WorkflowCommand::Cancel
            | WorkflowCommand::GetFile
    ) && !totp
        .is_unlocked(now_ms())
        .map_err(|error| error.to_string())?
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
            send_text(
                &inbound_bot,
                &surface,
                "正在连接 Codex app-server…",
                metrics,
            )
            .await?;
            let thread = agent
                .start_thread(
                    config.workspace_root.to_string_lossy().as_ref(),
                    false,
                    false,
                )
                .await
                .map_err(|error| error.to_string())?;
            let record = SessionRecord {
                thread_id: thread.id.clone(),
                turn_id: None,
                chat_id,
                root_message_id: None,
                sender_instance_id: inbound_bot.config.instance_id.clone(),
            };
            sessions.insert(record);
            store
                .upsert_session_space(&RustSessionSpace {
                    space_id: format!("telegram-{chat_id}-{message_id}"),
                    thread_id: Some(thread.id.to_string()),
                    lifecycle: "active".into(),
                    generation: 0,
                    channel_chat_id: config.channel_chat_id,
                    channel_post_id: message_id.max(1),
                    discussion_chat_id: (chat_id == config.discussion_chat_id).then_some(chat_id),
                    discussion_root_message_id: None,
                    status_message_id: None,
                    status_bot_instance: bots_by_id
                        .values()
                        .find(|bot| bot.role == RuntimeBotRole::Status)
                        .map(|bot| bot.config.instance_id.clone()),
                    owner_chat_id: Some(chat_id),
                    plan_mode: false,
                    created_at_ms: now_ms(),
                    updated_at_ms: now_ms(),
                })
                .map_err(|error| error.to_string())?;
            send_text(
                &inbound_bot,
                &surface,
                &format!("Rust Codex Session 已创建：{}", thread.id),
                metrics,
            )
            .await
        }
        WorkflowCommand::Status => {
            let state = agent.connection_state();
            let schema = store.schema_version().map_err(|error| error.to_string())?;
            let unlocked = totp
                .is_unlocked(now_ms())
                .map_err(|error| error.to_string())?;
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
            let verified = totp
                .verify_and_unlock(code, now_ms())
                .map_err(|error| error.to_string())?;
            let message = if verified {
                "TOTP accepted; write operations are unlocked for the configured lease."
            } else {
                "TOTP was not accepted; write operations remain locked."
            };
            send_text(&inbound_bot, &surface, message, metrics).await
        }
        WorkflowCommand::Lock => {
            totp.lock().map_err(|error| error.to_string())?;
            send_text(
                &inbound_bot,
                &surface,
                "Rust Bridge write operations are locked.",
                metrics,
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
            let mode =
                collaboration_mode_payload(agent, if enabled { "plan" } else { "default" }).await?;
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
            "{}. {} · active · thread={} · discussion_root={}",
            index + 1,
            space.space_id,
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
    sessions: Arc<SessionRegistry>,
    bots_by_id: HashMap<String, RuntimeBot>,
    config: RustConfig,
    metrics: MetricsRegistry,
) {
    let mut events = agent.subscribe_events();
    loop {
        let event = match events.recv().await {
            Ok(event) => event,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        };
        if event.method != "turn/completed" && event.method != "error" {
            continue;
        }
        let thread_id = event
            .params
            .get("threadId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(session) = sessions.by_thread(thread_id) else {
            continue;
        };
        if event.method == "turn/completed" {
            let turn = event.params.get("turn").cloned().unwrap_or(Value::Null);
            let turn_id = turn.get("id").and_then(Value::as_str).unwrap_or_default();
            sessions.set_turn(thread_id, None);
            let answer = extract_final_answer(&turn)
                .or_else(|| extract_review_answer(&turn))
                .unwrap_or_else(|| "Codex turn 已完成。".into());
            let Some(bot) = bots_by_id.get(&session.sender_instance_id) else {
                continue;
            };
            let _ = send_text(
                bot,
                &surface_for(bot, &config, session.chat_id, session.root_message_id),
                &format!("{answer}\n\nturn={turn_id}"),
                &metrics,
            )
            .await;
        } else {
            let message = event
                .params
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("Codex turn failed");
            sessions.set_turn(thread_id, None);
            if let Some(bot) = bots_by_id.get(&session.sender_instance_id) {
                let _ = send_text(
                    bot,
                    &surface_for(bot, &config, session.chat_id, session.root_message_id),
                    &format!("Codex 错误：{message}"),
                    &metrics,
                )
                .await;
            }
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
    if !totp
        .is_unlocked(now_ms())
        .map_err(|error| error.to_string())?
    {
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
    );
    let normalized = normalize_server_request_params(&request.method, &request.params);
    let thread_id = normalized
        .get("threadId")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned);
    if !supported {
        let message = if request.method == "item/tool/requestUserInput" {
            "Rust Bridge does not forward interactive user input to Telegram; answer in the local Codex client"
        } else {
            "Rust Bridge does not support this Codex server request"
        };
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
        markup,
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
    markup: Value,
    metrics: &MetricsRegistry,
) -> Result<(), String> {
    let text = truncate_text(text);
    let api = bot.api.clone();
    let token = bot.token.clone();
    let surface = surface.clone();
    let started = Instant::now();
    let result = tokio::task::spawn_blocking(move || {
        api.send_text_with_markup(&token, &surface, &text, Some(markup))
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
) -> Result<Value, String> {
    let result = agent
        .request("collaborationMode/list", json!({}), Duration::from_secs(30))
        .await
        .map_err(|error| error.to_string())?;
    let data = result
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| "Codex collaborationMode/list response did not include data".to_owned())?;
    let item = data
        .iter()
        .find(|item| {
            item.get("mode")
                .and_then(Value::as_str)
                .is_some_and(|mode| mode == requested_mode)
                || item
                    .get("name")
                    .and_then(Value::as_str)
                    .is_some_and(|name| name == requested_mode)
        })
        .ok_or_else(|| format!("Codex collaboration mode {requested_mode} is unavailable"))?;
    let settings = item.get("settings").and_then(Value::as_object);
    let model = settings
        .and_then(|settings| settings.get("model"))
        .or_else(|| item.get("model"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("Codex collaboration mode {requested_mode} has no model"))?;
    let effort = settings
        .and_then(|settings| settings.get("reasoning_effort"))
        .or_else(|| item.get("reasoning_effort"))
        .cloned()
        .unwrap_or(Value::Null);
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
}
