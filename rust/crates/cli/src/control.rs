//! Transport-neutral control-bot contract.
//!
//! The Telegram adapter owns callback nonces and message identifiers.  This
//! module deliberately emits logical effects instead, so the 9527 command
//! surface can be tested without a bot token, scheduler, or SQLite database.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const SESSIONS_DELETE_SECONDS: u64 = 15 * 60;
pub const PERF_LIFETIME_SECONDS: u64 = 30;
pub const PERF_UPDATE_SECONDS: u64 = 5;
pub const NEW_INTERACTION_SECONDS: u64 = 5 * 60;
pub const NEW_PROMPT_SECONDS: u64 = 30;
pub const SESSION_REFRESH_SECONDS: u64 = 5;
const SESSION_PAGE_SIZE: usize = 5;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ControlRequest {
    Help {
        label: String,
        paired: bool,
    },
    Sessions(SessionsRequest),
    Topics {
        topics: Vec<Topic>,
    },
    New {
        draft: NewDraft,
        models: Vec<ModelOption>,
    },
    NewCallback {
        draft: NewDraft,
        event: NewEvent,
        value: String,
        models: Vec<ModelOption>,
    },
    Perf {
        frame: usize,
        markdown_body: String,
        plain_body: String,
    },
    Callback {
        disposition: CallbackDisposition,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SessionsRequest {
    pub query: String,
    pub page: usize,
    pub now: i64,
    pub utc_offset_seconds: i64,
    pub sessions: Vec<Session>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Session {
    pub thread_id: String,
    pub title: String,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub turn_status: String,
    #[serde(default)]
    pub lifecycle: String,
    #[serde(default)]
    pub active_flags: Vec<String>,
    #[serde(default)]
    pub error: String,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub cwd: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Topic {
    pub title: String,
    pub lifecycle: String,
    pub url: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ModelOption {
    pub model: String,
    pub display_name: String,
    pub supported_efforts: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NewDraft {
    pub phase: NewPhase,
    #[serde(default)]
    pub normal_model: Option<String>,
    #[serde(default)]
    pub plan_model: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NewPhase {
    NormalModel,
    NormalEffort,
    PlanChoice,
    PlanModel,
    PlanEffort,
    Project,
    Prompt,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NewEvent {
    Cancel,
    NormalModel,
    NormalEffort,
    PlanChoice,
    PlanModel,
    PlanEffort,
    Hello,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CallbackDisposition {
    Missing,
    Current,
    QueueFull,
    Accepted,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlEffect {
    Render(RenderedEffect),
    CallbackAnswer {
        text: Option<String>,
        show_alert: bool,
    },
    InteractionDeadline {
        phase: NewPhase,
        deadline_seconds: u64,
    },
    DeleteDeadline {
        targets: Vec<DeleteTarget>,
        deadline_seconds: u64,
        group_key: String,
    },
    PerfTicker {
        deadline_seconds: u64,
        update_seconds: u64,
    },
    SessionRefresh {
        after_seconds: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RenderedEffect {
    pub operation: RenderOperation,
    pub markdown: String,
    pub plain: Option<String>,
    pub keyboard: Option<Vec<Vec<ControlButton>>>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RenderOperation {
    Send,
    Edit,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ControlButton {
    pub label: String,
    pub target: ButtonTarget,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ButtonTarget {
    Callback {
        action: String,
        #[serde(default)]
        payload: BTreeMap<String, String>,
    },
    Url {
        url: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeleteTarget {
    Command,
    Reply,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlError {
    NoModels,
    UnknownModel(String),
    UnsupportedNewTransition { phase: NewPhase, event: NewEvent },
}

/// Pure dispatcher for the 9527 control surface.
#[derive(Clone, Copy, Debug, Default)]
pub struct ControlController;

impl ControlController {
    pub fn dispatch(&self, request: ControlRequest) -> Result<Vec<ControlEffect>, ControlError> {
        match request {
            ControlRequest::Help { label, paired } => Ok(vec![render_help(&label, paired)]),
            ControlRequest::Sessions(request) => Ok(render_sessions(request)),
            ControlRequest::Topics { topics } => Ok(vec![render_topics(&topics)]),
            ControlRequest::New { draft, models } => render_new(&draft, &models),
            ControlRequest::NewCallback {
                draft,
                event,
                value,
                models,
            } => render_new_callback(&draft, event, &value, &models),
            ControlRequest::Perf {
                frame,
                markdown_body,
                plain_body,
            } => Ok(render_perf(frame, &markdown_body, &plain_body)),
            ControlRequest::Callback { disposition } => Ok(render_callback(disposition)),
        }
    }
}

pub fn parse_new_arguments(value: &str) -> Option<NewArguments> {
    let mut leading = value.splitn(3, '|');
    let model = leading.next()?.trim();
    let effort = leading.next()?.trim();
    if model.is_empty() || effort.is_empty() {
        return None;
    }
    let remaining = leading.next().map(str::trim).unwrap_or_default();
    if remaining.is_empty() {
        return Some(NewArguments::interactive(model, effort));
    }
    let (mode, tail) = remaining.split_once('|').unwrap_or((remaining, ""));
    match mode.trim().to_ascii_lowercase().as_str() {
        "planmode" => {
            let mut fields = tail.splitn(4, '|').map(str::trim);
            let plan_model = fields.next()?.to_owned();
            let plan_effort = fields.next()?.to_owned();
            if plan_model.is_empty() || plan_effort.is_empty() {
                return None;
            }
            Some(NewArguments {
                model: model.to_owned(),
                effort: effort.to_owned(),
                mode: Some("planmode".to_owned()),
                plan_model: Some(plan_model),
                plan_effort: Some(plan_effort),
                cwd: fields
                    .next()
                    .filter(|item| !item.is_empty())
                    .map(str::to_owned),
                prompt: fields
                    .next()
                    .filter(|item| !item.is_empty())
                    .map(str::to_owned),
            })
        }
        "noplan" => {
            let mut fields = tail.splitn(2, '|').map(str::trim);
            Some(NewArguments {
                model: model.to_owned(),
                effort: effort.to_owned(),
                mode: Some("noplan".to_owned()),
                plan_model: None,
                plan_effort: None,
                cwd: fields
                    .next()
                    .filter(|item| !item.is_empty())
                    .map(str::to_owned),
                prompt: fields
                    .next()
                    .filter(|item| !item.is_empty())
                    .map(str::to_owned),
            })
        }
        _ => Some(NewArguments {
            model: model.to_owned(),
            effort: effort.to_owned(),
            mode: Some(mode.trim().to_ascii_lowercase()),
            plan_model: None,
            plan_effort: None,
            cwd: None,
            prompt: None,
        }),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewArguments {
    pub model: String,
    pub effort: String,
    pub mode: Option<String>,
    pub plan_model: Option<String>,
    pub plan_effort: Option<String>,
    pub cwd: Option<String>,
    pub prompt: Option<String>,
}

impl NewArguments {
    fn interactive(model: &str, effort: &str) -> Self {
        Self {
            model: model.to_owned(),
            effort: effort.to_owned(),
            mode: None,
            plan_model: None,
            plan_effort: None,
            cwd: None,
            prompt: None,
        }
    }
}

fn render_help(label: &str, paired: bool) -> ControlEffect {
    let commands: &[(&str, &str)] = if paired {
        &[
            ("/sessions [关键词]", "查找 Codex sessions"),
            ("/topics", "查看 Session 帖子"),
            ("/new [model | effort | ...]", "交互或参数化创建 Session"),
            ("/perf", "动态查看 30 秒 WSL 性能"),
            ("/help", "显示帮助"),
        ]
    } else {
        &[("/pair", "完成 owner 配对"), ("/help", "显示帮助")]
    };
    let title = format!("🤖 {label}");
    let markdown = std::iter::once(format!("*{}*", escape_markdown(&title)))
        .chain(commands.iter().map(|(command, description)| {
            format!("{}  {}", inline_code(command), escape_markdown(description))
        }))
        .collect::<Vec<_>>()
        .join("\n");
    let plain = std::iter::once(title)
        .chain(
            commands
                .iter()
                .map(|(command, description)| format!("{command}  {description}")),
        )
        .collect::<Vec<_>>()
        .join("\n");
    render(RenderOperation::Send, markdown, Some(plain), None)
}

fn render_sessions(request: SessionsRequest) -> Vec<ControlEffect> {
    let query = request.query.trim();
    let matching: Vec<_> = request
        .sessions
        .iter()
        .filter(|session| {
            query.is_empty()
                || session
                    .thread_id
                    .to_lowercase()
                    .contains(&query.to_lowercase())
                || session.title.to_lowercase().contains(&query.to_lowercase())
        })
        .collect();
    let total_pages = matching.len().div_ceil(SESSION_PAGE_SIZE).max(1);
    let page = request.page.clamp(1, total_pages);
    let selected =
        &matching[(page - 1) * SESSION_PAGE_SIZE..matching.len().min(page * SESSION_PAGE_SIZE)];
    let heading = format!("🤖 Codex Sessions · {page}/{total_pages}");
    let mut markdown_lines = vec![format!("*{}*", escape_markdown(&heading))];
    let mut plain_lines = vec![heading];
    if !query.is_empty() {
        markdown_lines.push(format!("搜索 {}", inline_code(query)));
        plain_lines.push(format!("搜索 {query}"));
    }
    if selected.is_empty() {
        markdown_lines.extend([String::new(), "当前没有 Codex session。".to_owned()]);
        plain_lines.extend([String::new(), "当前没有 Codex session。".to_owned()]);
    }
    let labels = ["①", "②", "③", "④", "⑤"];
    for (index, session) in selected.iter().enumerate() {
        let (icon, _) = session_status(session);
        markdown_lines.extend([
            String::new(),
            format!(
                "{} {icon} {}",
                labels[index],
                inline_code(&session.thread_id)
            ),
            format!("📝 {}", escape_markdown(&session.title)),
            format!(
                "🗓 Created {} · Updated {}",
                inline_code(&clock(session.created_at, request.utc_offset_seconds)),
                inline_code(&relative(session.updated_at, request.now)),
            ),
            format!("📁 {}", inline_code(&session.cwd)),
        ]);
        plain_lines.extend([
            String::new(),
            format!("{} {icon} {}", labels[index], session.thread_id),
            format!("📝 {}", session.title),
            format!(
                "🗓 Created {} · Updated {}",
                clock(session.created_at, request.utc_offset_seconds),
                relative(session.updated_at, request.now),
            ),
            format!("📁 {}", session.cwd),
        ]);
    }

    let mut rows = Vec::new();
    if !selected.is_empty() {
        rows.push(
            selected
                .iter()
                .enumerate()
                .map(|(index, session)| {
                    callback_button(
                        labels[index],
                        "session_detail",
                        [("thread_id", session.thread_id.as_str())],
                    )
                })
                .collect(),
        );
    }
    rows.push(
        pagination(page, total_pages)
            .into_iter()
            .map(|(label, target, current)| {
                let target = target.to_string();
                callback_button(
                    label,
                    if current {
                        "sessions_current"
                    } else {
                        "sessions_page"
                    },
                    [("page", target.as_str()), ("query", query)],
                )
            })
            .collect(),
    );

    vec![
        ControlEffect::DeleteDeadline {
            targets: vec![DeleteTarget::Command, DeleteTarget::Reply],
            deadline_seconds: SESSIONS_DELETE_SECONDS,
            group_key: "sessions".to_owned(),
        },
        render(
            RenderOperation::Send,
            markdown_lines.join("\n"),
            Some(plain_lines.join("\n")),
            Some(rows),
        ),
        ControlEffect::SessionRefresh {
            after_seconds: SESSION_REFRESH_SECONDS,
        },
    ]
}

fn render_topics(topics: &[Topic]) -> ControlEffect {
    if topics.is_empty() {
        return render(
            RenderOperation::Send,
            "当前没有 Session 帖子。".to_owned(),
            None,
            None,
        );
    }
    let mut lines = vec!["*🤖 Session 帖子*".to_owned()];
    let mut buttons = Vec::new();
    for (index, topic) in topics.iter().take(30).enumerate() {
        let number = index + 1;
        lines.push(format!(
            "{number}\\. {} · {}",
            escape_markdown(&topic.title),
            inline_code(&topic.lifecycle),
        ));
        if let Some(url) = &topic.url {
            buttons.push(ControlButton {
                label: format!("打开 {number}"),
                target: ButtonTarget::Url { url: url.clone() },
            });
        }
    }
    render(
        RenderOperation::Send,
        lines.join("\n"),
        None,
        (!buttons.is_empty()).then(|| balanced_rows(buttons, 3)),
    )
}

fn render_new(
    draft: &NewDraft,
    models: &[ModelOption],
) -> Result<Vec<ControlEffect>, ControlError> {
    if draft.phase != NewPhase::NormalModel {
        return Err(ControlError::UnsupportedNewTransition {
            phase: draft.phase,
            event: NewEvent::NormalModel,
        });
    }
    Ok(vec![
        ControlEffect::InteractionDeadline {
            phase: NewPhase::NormalModel,
            deadline_seconds: NEW_INTERACTION_SECONDS,
        },
        model_choices(models, false)?,
    ])
}

fn render_new_callback(
    draft: &NewDraft,
    event: NewEvent,
    value: &str,
    models: &[ModelOption],
) -> Result<Vec<ControlEffect>, ControlError> {
    let mut effects = vec![ControlEffect::CallbackAnswer {
        text: None,
        show_alert: false,
    }];
    match (draft.phase, event) {
        (_, NewEvent::Cancel) => effects.push(render(
            RenderOperation::Edit,
            "已退出 `/new`。".to_owned(),
            Some("已退出 /new。".to_owned()),
            None,
        )),
        (NewPhase::NormalModel, NewEvent::NormalModel) => {
            model(models, value)?;
            effects.push(render(
                RenderOperation::Edit,
                format!("已选择模型 {}。", inline_code(value)),
                Some(format!("已选择模型 {value}。")),
                None,
            ));
            effects.push(ControlEffect::InteractionDeadline {
                phase: NewPhase::NormalEffort,
                deadline_seconds: NEW_INTERACTION_SECONDS,
            });
            effects.push(effort_choices(model(models, value)?, false));
        }
        (NewPhase::NormalEffort, NewEvent::NormalEffort) => {
            let selected = draft
                .normal_model
                .as_deref()
                .ok_or_else(|| ControlError::UnknownModel(value.to_owned()))?;
            let option = model(models, selected)?;
            if !option
                .supported_efforts
                .iter()
                .any(|effort| effort == value)
            {
                return Err(ControlError::UnknownModel(value.to_owned()));
            }
            effects.push(render(
                RenderOperation::Edit,
                format!("已选择 effort {}。", inline_code(value)),
                Some(format!("已选择 effort {value}。")),
                None,
            ));
            effects.push(ControlEffect::InteractionDeadline {
                phase: NewPhase::PlanChoice,
                deadline_seconds: NEW_INTERACTION_SECONDS,
            });
            effects.push(plan_choice());
        }
        (NewPhase::PlanChoice, NewEvent::PlanChoice) if value == "yes" => {
            effects.push(render(
                RenderOperation::Edit,
                "已选择：进入 Plan Mode。".to_owned(),
                Some("已选择：进入 Plan Mode。".to_owned()),
                None,
            ));
            effects.push(ControlEffect::InteractionDeadline {
                phase: NewPhase::PlanModel,
                deadline_seconds: NEW_INTERACTION_SECONDS,
            });
            effects.push(model_choices(models, true)?);
        }
        (NewPhase::PlanChoice, NewEvent::PlanChoice) if value == "no" => {
            effects.push(render(
                RenderOperation::Edit,
                "已选择：不进入 Plan Mode。".to_owned(),
                Some("已选择：不进入 Plan Mode。".to_owned()),
                None,
            ));
            effects.push(ControlEffect::InteractionDeadline {
                phase: NewPhase::Project,
                deadline_seconds: NEW_INTERACTION_SECONDS,
            });
            effects.push(project_prompt());
        }
        (NewPhase::PlanModel, NewEvent::PlanModel) => {
            model(models, value)?;
            effects.push(render(
                RenderOperation::Edit,
                format!("已选择模型 {}。", inline_code(value)),
                Some(format!("已选择模型 {value}。")),
                None,
            ));
            effects.push(ControlEffect::InteractionDeadline {
                phase: NewPhase::PlanEffort,
                deadline_seconds: NEW_INTERACTION_SECONDS,
            });
            effects.push(effort_choices(model(models, value)?, true));
        }
        (NewPhase::PlanEffort, NewEvent::PlanEffort) => {
            let selected = draft
                .plan_model
                .as_deref()
                .ok_or_else(|| ControlError::UnknownModel(value.to_owned()))?;
            let option = model(models, selected)?;
            if !option
                .supported_efforts
                .iter()
                .any(|effort| effort == value)
            {
                return Err(ControlError::UnknownModel(value.to_owned()));
            }
            effects.push(render(
                RenderOperation::Edit,
                format!("已选择 effort {}。", inline_code(value)),
                Some(format!("已选择 effort {value}。")),
                None,
            ));
            effects.push(ControlEffect::InteractionDeadline {
                phase: NewPhase::Project,
                deadline_seconds: NEW_INTERACTION_SECONDS,
            });
            effects.push(project_prompt());
        }
        (NewPhase::Prompt, NewEvent::Hello) => effects.push(ControlEffect::InteractionDeadline {
            phase: NewPhase::Prompt,
            deadline_seconds: NEW_PROMPT_SECONDS,
        }),
        _ => {
            return Err(ControlError::UnsupportedNewTransition {
                phase: draft.phase,
                event,
            });
        }
    }
    Ok(effects)
}

fn render_perf(frame: usize, markdown_body: &str, plain_body: &str) -> Vec<ControlEffect> {
    let frames = ["🕛", "🕒", "🕕", "🕘"];
    let frame = frames[frame % frames.len()];
    vec![
        render(
            RenderOperation::Send,
            format!("*{frame} 动态性能*\n{markdown_body}"),
            Some(format!("{frame} 动态性能\n{plain_body}")),
            None,
        ),
        ControlEffect::DeleteDeadline {
            targets: vec![DeleteTarget::Command, DeleteTarget::Reply],
            deadline_seconds: PERF_LIFETIME_SECONDS,
            group_key: "perf".to_owned(),
        },
        ControlEffect::PerfTicker {
            deadline_seconds: PERF_LIFETIME_SECONDS,
            update_seconds: PERF_UPDATE_SECONDS,
        },
    ]
}

fn render_callback(disposition: CallbackDisposition) -> Vec<ControlEffect> {
    match disposition {
        CallbackDisposition::Current | CallbackDisposition::Accepted => {
            vec![ControlEffect::CallbackAnswer {
                text: None,
                show_alert: false,
            }]
        }
        CallbackDisposition::Missing => vec![ControlEffect::CallbackAnswer {
            text: Some("按钮已使用或过期，请重新执行命令。".to_owned()),
            show_alert: true,
        }],
        CallbackDisposition::QueueFull => vec![ControlEffect::CallbackAnswer {
            text: Some("请求队列已满，请稍后重试。".to_owned()),
            show_alert: true,
        }],
    }
}

fn model_choices(models: &[ModelOption], plan: bool) -> Result<ControlEffect, ControlError> {
    if models.is_empty() {
        return Err(ControlError::NoModels);
    }
    let event = if plan { "plan_model" } else { "normal_model" };
    let buttons = models
        .iter()
        .map(|option| {
            callback_button(
                &option.display_name,
                event,
                [("value", option.model.as_str())],
            )
        })
        .collect();
    let mut rows = balanced_rows(buttons, 2);
    rows.push(vec![callback_button("退出", "cancel", [])]);
    let mode = if plan { "Plan Mode" } else { "当前模式" };
    Ok(render(
        RenderOperation::Send,
        format!("请选择 {mode} 使用的模型："),
        None,
        Some(rows),
    ))
}

fn effort_choices(option: &ModelOption, plan: bool) -> ControlEffect {
    let event = if plan { "plan_effort" } else { "normal_effort" };
    let mut rows = balanced_rows(
        option
            .supported_efforts
            .iter()
            .map(|effort| callback_button(effort, event, [("value", effort.as_str())]))
            .collect(),
        3,
    );
    rows.push(vec![callback_button("退出", "cancel", [])]);
    render(
        RenderOperation::Send,
        format!("模型 {} 支持以下 effort：", inline_code(&option.model)),
        None,
        Some(rows),
    )
}

fn plan_choice() -> ControlEffect {
    render(
        RenderOperation::Send,
        "新 Session 是否先进入 Plan Mode？".to_owned(),
        None,
        Some(vec![
            vec![callback_button("是", "plan_choice", [("value", "yes")])],
            vec![callback_button("否", "plan_choice", [("value", "no")])],
            vec![callback_button("退出", "cancel", [])],
        ]),
    )
}

fn project_prompt() -> ControlEffect {
    render(
        RenderOperation::Send,
        "请发送项目地址或项目描述；下一条文本消息会被识别为项目。".to_owned(),
        None,
        Some(vec![vec![callback_button("退出", "cancel", [])]]),
    )
}

fn render(
    operation: RenderOperation,
    markdown: String,
    plain: Option<String>,
    keyboard: Option<Vec<Vec<ControlButton>>>,
) -> ControlEffect {
    ControlEffect::Render(RenderedEffect {
        operation,
        markdown,
        plain,
        keyboard,
    })
}

fn callback_button<'a>(
    label: impl Into<String>,
    action: impl Into<String>,
    payload: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> ControlButton {
    ControlButton {
        label: label.into(),
        target: ButtonTarget::Callback {
            action: action.into(),
            payload: payload
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
        },
    }
}

fn balanced_rows(buttons: Vec<ControlButton>, columns: usize) -> Vec<Vec<ControlButton>> {
    buttons.chunks(columns).map(|row| row.to_vec()).collect()
}

fn pagination(page: usize, total_pages: usize) -> Vec<(String, usize, bool)> {
    let item = |label: String, target| {
        (
            label.clone(),
            target,
            target == page && label.chars().all(|character| character.is_ascii_digit()),
        )
    };
    match (page, total_pages) {
        (_, 1) => vec![item("1".to_owned(), 1)],
        (1, _) => vec![item("1".to_owned(), 1), item(">>".to_owned(), 2)],
        (current, total) if current == total => vec![
            item("1".to_owned(), 1),
            item("<<".to_owned(), page - 1),
            item(page.to_string(), page),
        ],
        (2, _) => vec![
            item("<<".to_owned(), 1),
            item("2".to_owned(), 2),
            item(">>".to_owned(), 3),
        ],
        (current, total) if current == total - 1 => vec![
            item("1".to_owned(), 1),
            item("<<".to_owned(), page - 1),
            item(page.to_string(), page),
            item(total.to_string(), total),
        ],
        _ => vec![
            item("1".to_owned(), 1),
            item("<<".to_owned(), page - 1),
            item(page.to_string(), page),
            item(">>".to_owned(), page + 1),
        ],
    }
}

fn model<'a>(models: &'a [ModelOption], value: &str) -> Result<&'a ModelOption, ControlError> {
    models
        .iter()
        .find(|option| option.model == value)
        .ok_or_else(|| ControlError::UnknownModel(value.to_owned()))
}

fn session_status(session: &Session) -> (&'static str, &'static str) {
    if session.lifecycle == "pending" {
        return ("🟡", "待认证");
    }
    if session.lifecycle == "closed" {
        return ("⚫", "已关闭");
    }
    if session.lifecycle == "repair_required" {
        return ("🟠", "需要修复");
    }
    if !session.error.is_empty()
        || session.status == "systemError"
        || session.turn_status == "failed"
    {
        return ("🔴", "错误");
    }
    if session
        .active_flags
        .iter()
        .any(|flag| flag == "waitingOnUserInput" || flag == "waitingOnApproval")
    {
        return ("🟡", "等待");
    }
    if session.turn_status == "completed"
        || session.turn_status == "interrupted"
        || session.status == "idle"
    {
        return ("⚪", "空闲");
    }
    if session.status == "active" || session.turn_status == "inProgress" {
        return ("🟢", "执行中");
    }
    ("⚫", "未加载")
}

fn clock(value: Option<i64>, utc_offset_seconds: i64) -> String {
    let Some(epoch) = value else {
        return "N/A".to_owned();
    };
    let local = epoch.saturating_add(utc_offset_seconds);
    let days = local.div_euclid(86_400);
    let seconds = local.rem_euclid(86_400);
    let (_, month, day) = civil_date(days);
    format!(
        "{month:02}-{day:02} {:02}:{:02}",
        seconds / 3_600,
        seconds % 3_600 / 60
    )
}

// Gregorian conversion for days since 1970-01-01, adapted from the public-domain
// civil-date algorithm. The caller supplies the local UTC offset explicitly.
fn civil_date(days_since_epoch: i64) -> (i64, i64, i64) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}

fn relative(value: Option<i64>, now: i64) -> String {
    let Some(value) = value else {
        return "N/A".to_owned();
    };
    let seconds = (now - value).max(0);
    if seconds < 60 {
        "now".to_owned()
    } else if seconds < 3600 {
        format!("{}m ago", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h ago", seconds / 3600)
    } else {
        format!("{}d ago", seconds / 86_400)
    }
}

fn escape_markdown(value: &str) -> String {
    value
        .chars()
        .flat_map(|character| {
            if matches!(
                character,
                '_' | '*'
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

fn inline_code(value: &str) -> String {
    format!("`{}`", value.replace('`', "\\`").replace('\\', "\\\\"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_parameterized_new_without_losing_prompt_pipes() {
        assert_eq!(
            parse_new_arguments(
                "luna | max | planmode | sol | high | /tmp/project | inspect a | b pipeline"
            )
            .unwrap()
            .prompt
            .as_deref(),
            Some("inspect a | b pipeline"),
        );
    }

    #[test]
    fn session_pagination_preserves_current_page_action() {
        assert_eq!(pagination(1, 1), vec![("1".to_owned(), 1, true)]);
    }

    #[test]
    fn formats_created_clock_with_explicit_utc_offset() {
        assert_eq!(clock(Some(1_700_000_000), 28_800), "11-15 06:13");
    }
}
