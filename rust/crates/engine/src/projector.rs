use ctg_domain::{AgentEvent, ThreadId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ThreadProjection {
    pub thread_id: String,
    pub title: Option<String>,
    pub status: Option<String>,
    pub turn_id: Option<String>,
    pub turn_status: Option<String>,
    pub goal: Option<Value>,
    pub plan: Option<Value>,
    pub review_status: Option<String>,
    pub desired_mode: Option<String>,
    pub observed_mode: Option<String>,
    pub items: BTreeMap<String, Value>,
    pub subagents: BTreeMap<String, Value>,
    pub last_error: Option<String>,
    pub generation: u64,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectionEffect {
    None,
    RefreshStatus,
    TurnCompleted,
    Error,
}

#[derive(Default)]
pub struct EventProjector {
    threads: BTreeMap<String, ThreadProjection>,
}

impl EventProjector {
    pub fn projection(&self, thread_id: &str) -> Option<&ThreadProjection> {
        self.threads.get(thread_id)
    }

    pub fn apply(&mut self, event: &AgentEvent) -> ProjectionEffect {
        let Some(thread_id) = thread_id_from_params(&event.params) else {
            return ProjectionEffect::None;
        };
        let projection =
            self.threads
                .entry(thread_id.clone())
                .or_insert_with(|| ThreadProjection {
                    thread_id: thread_id.clone(),
                    generation: event.generation,
                    ..ThreadProjection::default()
                });
        projection.generation = event.generation;
        projection.updated_at_ms = projection.updated_at_ms.saturating_add(1);
        match event.method.as_str() {
            "thread/started" | "thread/created" | "thread/updated" => {
                merge_thread(projection, &event.params);
                ProjectionEffect::RefreshStatus
            }
            "thread/status/updated" | "thread/status/changed" => {
                projection.status = string_at(&event.params, &["status", "thread", "status"]);
                ProjectionEffect::RefreshStatus
            }
            "thread/settings/updated" | "thread/settings/update" => {
                projection.desired_mode = string_at(
                    &event.params,
                    &[
                        "collaborationMode",
                        "collaboration_mode",
                        "settings",
                        "collaborationMode",
                    ],
                );
                projection.observed_mode = string_at(
                    &event.params,
                    &["observedMode", "observed_mode", "settings", "observedMode"],
                );
                ProjectionEffect::RefreshStatus
            }
            "turn/started" | "turn/created" => {
                projection.turn_id = string_at(&event.params, &["turnId", "turn", "id"]);
                projection.turn_status = Some("inProgress".into());
                ProjectionEffect::RefreshStatus
            }
            "turn/updated" => {
                if let Some(turn) = event.params.get("turn") {
                    projection.turn_id = turn.get("id").and_then(Value::as_str).map(str::to_owned);
                    projection.turn_status = turn
                        .get("status")
                        .and_then(Value::as_str)
                        .map(str::to_owned);
                }
                ProjectionEffect::RefreshStatus
            }
            "turn/completed" | "turn/failed" | "turn/interrupted" => {
                projection.turn_status = event
                    .params
                    .get("turn")
                    .and_then(|turn| turn.get("status"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| Some(event.method.trim_start_matches("turn/").to_owned()));
                ProjectionEffect::TurnCompleted
            }
            "item/started" | "item/updated" | "item/completed" | "item/failed" => {
                if let Some(item) = event.params.get("item")
                    && let Some(item_id) = item.get("id").and_then(Value::as_str)
                {
                    projection.items.insert(item_id.to_owned(), item.clone());
                }
                ProjectionEffect::RefreshStatus
            }
            "item/agentMessage/delta" | "item/plan/delta" | "item/reasoning/delta" => {
                ProjectionEffect::RefreshStatus
            }
            "thread/plan/updated" | "plan/updated" | "plan/published" => {
                projection.plan = event
                    .params
                    .get("plan")
                    .cloned()
                    .or_else(|| Some(event.params.clone()));
                ProjectionEffect::RefreshStatus
            }
            "goal/updated" | "thread/goal/updated" => {
                projection.goal = event
                    .params
                    .get("goal")
                    .cloned()
                    .or_else(|| Some(event.params.clone()));
                ProjectionEffect::RefreshStatus
            }
            "subagent/started" | "subagent/updated" | "subagent/completed" => {
                if let Some(task) = event
                    .params
                    .get("task")
                    .or_else(|| event.params.get("subagent"))
                    && let Some(id) = task.get("id").and_then(Value::as_str)
                {
                    projection.subagents.insert(id.to_owned(), task.clone());
                }
                ProjectionEffect::RefreshStatus
            }
            "review/started" | "review/updated" | "review/completed" => {
                projection.review_status = event
                    .params
                    .get("status")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| Some(event.method.trim_start_matches("review/").to_owned()));
                ProjectionEffect::RefreshStatus
            }
            "error" | "turn/error" => {
                projection.last_error = event
                    .params
                    .get("error")
                    .and_then(|error| error.get("message").or(Some(error)))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .or_else(|| Some("Codex error".into()));
                projection.turn_status = Some("failed".into());
                ProjectionEffect::Error
            }
            _ => ProjectionEffect::None,
        }
    }
}

fn thread_id_from_params(params: &Value) -> Option<String> {
    [
        params.get("threadId"),
        params.pointer("/thread/id"),
        params.pointer("/turn/threadId"),
        params.pointer("/item/threadId"),
    ]
    .into_iter()
    .flatten()
    .find_map(Value::as_str)
    .filter(|value| !value.trim().is_empty())
    .map(str::to_owned)
}

fn string_at(value: &Value, paths: &[&str]) -> Option<String> {
    paths.iter().find_map(|path| {
        let value = if path.contains('/') {
            value.pointer(path)
        } else {
            value.get(*path)
        }?;
        value.as_str().map(str::to_owned)
    })
}

fn merge_thread(projection: &mut ThreadProjection, params: &Value) {
    let source = params.get("thread").unwrap_or(params);
    projection.title = ["title", "name", "naturalSummary", "summary"]
        .iter()
        .find_map(|key| source.get(*key).and_then(Value::as_str).map(str::to_owned))
        .or(projection.title.clone());
    projection.status = source
        .get("status")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or(projection.status.clone());
    projection.desired_mode = source
        .get("collaborationMode")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .or(projection.desired_mode.clone());
}

pub fn projection_thread_id(projection: &ThreadProjection) -> Option<ThreadId> {
    ThreadId::new(projection.thread_id.clone()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(method: &str, params: Value) -> AgentEvent {
        AgentEvent {
            method: method.into(),
            params,
            generation: 4,
        }
    }

    #[test]
    fn projects_thread_turn_goal_plan_and_subagent_events() {
        let mut projector = EventProjector::default();
        assert_eq!(
            projector.apply(&event(
                "thread/started",
                serde_json::json!({"threadId":"thread-1","title":"Parity"})
            )),
            ProjectionEffect::RefreshStatus
        );
        projector.apply(&event(
            "turn/started",
            serde_json::json!({"threadId":"thread-1","turnId":"turn-1"}),
        ));
        projector.apply(&event(
            "goal/updated",
            serde_json::json!({"threadId":"thread-1","goal":{"status":"inProgress"}}),
        ));
        projector.apply(&event(
            "plan/published",
            serde_json::json!({"threadId":"thread-1","plan":{"steps":[]}}),
        ));
        projector.apply(&event(
            "subagent/updated",
            serde_json::json!({"threadId":"thread-1","subagent":{"id":"agent-1","status":"running"}}),
        ));
        let state = projector.projection("thread-1").unwrap();
        assert_eq!(state.title.as_deref(), Some("Parity"));
        assert_eq!(state.turn_id.as_deref(), Some("turn-1"));
        assert!(state.goal.is_some());
        assert!(state.plan.is_some());
        assert!(state.subagents.contains_key("agent-1"));
    }

    #[test]
    fn error_event_is_terminal_and_retains_message() {
        let mut projector = EventProjector::default();
        let effect = projector.apply(&event(
            "error",
            serde_json::json!({"threadId":"thread-1","error":{"message":"boom"}}),
        ));
        assert_eq!(effect, ProjectionEffect::Error);
        assert_eq!(
            projector
                .projection("thread-1")
                .unwrap()
                .last_error
                .as_deref(),
            Some("boom")
        );
    }
}
