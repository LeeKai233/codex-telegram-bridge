//! Sanitized NDJSON replay runner. It has no Telegram, filesystem artifact,
//! tmux, or subprocess side effects beyond reading the supplied fixture.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::Instant;
use thiserror::Error;

#[derive(Clone, Debug, Deserialize)]
struct ReplayEvent {
    schema_version: u8,
    offset_ms: u64,
    kind: String,
    subject: BTreeMap<String, Value>,
    result: Option<String>,
    attempt: Option<u64>,
    queue_depth: Option<u64>,
    latency_ms: Option<u64>,
    error_class: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BenchmarkReport {
    pub schema_version: u8,
    pub scenario: String,
    pub implementation: String,
    pub repetitions: u64,
    pub events_processed: u64,
    pub elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p50_event_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p95_event_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peak_rss_bytes: Option<u64>,
    pub outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<String>,
}

pub fn run_fixture(
    fixture: impl AsRef<Path>,
    scenario: impl Into<String>,
    implementation: impl Into<String>,
    repetitions: u64,
    warmup_repetitions: u64,
) -> Result<BenchmarkReport, ReplayError> {
    if repetitions == 0 || repetitions > 1_000_000 || warmup_repetitions > 1_000_000 {
        return Err(ReplayError::Schema);
    }
    let text = fs::read_to_string(fixture.as_ref()).map_err(|_| ReplayError::Runtime)?;
    let events = parse_events(&text)?;
    let mut elapsed_samples = Vec::with_capacity(repetitions as usize);
    let mut peak_rss_bytes = current_rss_bytes();
    for _ in 0..warmup_repetitions {
        validate_events(&events)?;
    }
    let started = Instant::now();
    for _ in 0..repetitions {
        let event_started = Instant::now();
        validate_events(&events)?;
        elapsed_samples.push(event_started.elapsed().as_micros() as u64);
    }
    let elapsed_ms = started.elapsed().as_millis() as u64;
    peak_rss_bytes = match (peak_rss_bytes, current_rss_bytes()) {
        (Some(peak), Some(current)) => Some(peak.max(current)),
        (peak, current) => peak.or(current),
    };
    elapsed_samples.sort_unstable();
    Ok(BenchmarkReport {
        schema_version: 1,
        scenario: scenario.into(),
        implementation: implementation.into(),
        repetitions,
        events_processed: events.len() as u64 * repetitions,
        elapsed_ms,
        p50_event_us: percentile(&elapsed_samples, 50),
        p95_event_us: percentile(&elapsed_samples, 95),
        peak_rss_bytes,
        outcome: "success".into(),
        failure_class: None,
    })
}

/// Resident set of this process at sample time, reported as the report's
/// `peak_rss_bytes` upper bound. sysinfo is used instead of parsing
/// `/proc/self` so the metric stays platform-neutral; the run is short, so
/// the start/end samples bound the peak closely enough for a benchmark
/// comparison signal.
fn current_rss_bytes() -> Option<u64> {
    let pid = sysinfo::get_current_pid().ok()?;
    let mut system = sysinfo::System::new();
    system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
    system.process(pid).map(|process| process.memory())
}

fn parse_events(text: &str) -> Result<Vec<ReplayEvent>, ReplayError> {
    let mut events = Vec::new();
    for line in text.lines().filter(|line| !line.trim().is_empty()) {
        let value: Value = serde_json::from_str(line).map_err(|_| ReplayError::Schema)?;
        validate_allowed_fields(&value)?;
        let event: ReplayEvent = serde_json::from_value(value).map_err(|_| ReplayError::Schema)?;
        if event.schema_version != 1 || event.offset_ms > 86_400_000 {
            return Err(ReplayError::Schema);
        }
        validate_subject(&event.subject)?;
        validate_event_shape(&event)?;
        events.push(event);
    }
    if events.is_empty() {
        return Err(ReplayError::Schema);
    }
    Ok(events)
}

fn validate_allowed_fields(value: &Value) -> Result<(), ReplayError> {
    let allowed: BTreeSet<&str> = [
        "schema_version",
        "offset_ms",
        "kind",
        "subject",
        "result",
        "attempt",
        "queue_depth",
        "latency_ms",
        "error_class",
    ]
    .into_iter()
    .collect();
    let object = value.as_object().ok_or(ReplayError::Schema)?;
    if object.keys().any(|key| !allowed.contains(key.as_str())) {
        return Err(ReplayError::Schema);
    }
    Ok(())
}

fn validate_subject(subject: &BTreeMap<String, Value>) -> Result<(), ReplayError> {
    let allowed: BTreeSet<&str> = [
        "session",
        "thread",
        "turn",
        "item",
        "component",
        "bot_role",
        "queue",
    ]
    .into_iter()
    .collect();
    if subject.keys().any(|key| !allowed.contains(key.as_str())) {
        return Err(ReplayError::Schema);
    }
    for (key, value) in subject {
        if matches!(key.as_str(), "session" | "thread" | "turn" | "item") {
            let value = value.as_str().ok_or(ReplayError::Schema)?;
            if !value.starts_with("replay-") || value.len() > 70 {
                return Err(ReplayError::Schema);
            }
        }
    }
    Ok(())
}

fn validate_event_shape(event: &ReplayEvent) -> Result<(), ReplayError> {
    let allowed_kinds = [
        "thread_started",
        "turn_started",
        "turn_completed",
        "plan_published",
        "plan_approved",
        "prompt_enqueued",
        "delivery_attempted",
        "delivery_completed",
        "poll_completed",
        "component_health_changed",
        "supervisor_restarted",
        "shutdown_requested",
        "shutdown_completed",
    ];
    if !allowed_kinds.contains(&event.kind.as_str()) {
        return Err(ReplayError::Schema);
    }
    if let Some(result) = &event.result
        && !["success", "failed", "cancelled", "rejected"].contains(&result.as_str())
    {
        return Err(ReplayError::Schema);
    }
    if let Some(attempt) = event.attempt
        && !(1..=1000).contains(&attempt)
    {
        return Err(ReplayError::Schema);
    }
    if event.queue_depth.is_some_and(|depth| depth > 1_000_000)
        || event.latency_ms.is_some_and(|latency| latency > 3_600_000)
    {
        return Err(ReplayError::Schema);
    }
    if event.error_class.as_deref().is_some_and(|error| {
        ![
            "timeout",
            "transport",
            "rate_limited",
            "remote_rejected",
            "internal",
        ]
        .contains(&error)
    }) {
        return Err(ReplayError::Schema);
    }
    Ok(())
}

fn validate_events(events: &[ReplayEvent]) -> Result<(), ReplayError> {
    let mut previous_offset = 0;
    let mut deliveries = BTreeSet::new();
    let mut completed = BTreeSet::new();
    let mut poll_state: BTreeMap<String, bool> = BTreeMap::new();
    let mut recovery_required: BTreeSet<String> = BTreeSet::new();
    for (index, event) in events.iter().enumerate() {
        if index > 0 && event.offset_ms < previous_offset {
            return Err(ReplayError::Invariant);
        }
        previous_offset = event.offset_ms;
        if event.kind == "delivery_attempted" {
            let attempt = event.attempt.ok_or(ReplayError::Invariant)?;
            deliveries.insert((subject_id(&event.subject, "thread")?, attempt));
        }
        if event.kind == "delivery_completed" {
            let attempt = event.attempt.ok_or(ReplayError::Invariant)?;
            let key = (subject_id(&event.subject, "thread")?, attempt);
            if !deliveries.contains(&key) || !completed.insert(key) {
                return Err(ReplayError::Invariant);
            }
        }
        if event.kind == "poll_completed" {
            let role = subject_id(&event.subject, "bot_role")?;
            let success = event.result.as_deref() == Some("success");
            poll_state.insert(role.clone(), success);
            if !success {
                recovery_required.insert(role);
            }
        }
        if event.kind == "component_health_changed" && event.result.as_deref() == Some("success") {
            let role = event
                .subject
                .get("bot_role")
                .and_then(Value::as_str)
                .unwrap_or("discussion")
                .to_owned();
            if recovery_required.contains(&role) && !poll_state.get(&role).copied().unwrap_or(false)
            {
                return Err(ReplayError::Invariant);
            }
            recovery_required.remove(&role);
        }
    }
    if deliveries != completed {
        return Err(ReplayError::Invariant);
    }
    Ok(())
}

fn subject_id(subject: &BTreeMap<String, Value>, key: &str) -> Result<String, ReplayError> {
    subject
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or(ReplayError::Invariant)
}

fn percentile(values: &[u64], percentile: usize) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let index = ((values.len() - 1) * percentile / 100).min(values.len() - 1);
    Some(values[index])
}

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("schema")]
    Schema,
    #[error("invariant")]
    Invariant,
    #[error("runtime")]
    Runtime,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn supplied_fixture_replays_without_side_effects() {
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../../replay/fixtures/steady_delivery.ndjson");
        let report = run_fixture(fixture, "steady_delivery", "rust-vnext", 2, 1).unwrap();
        assert_eq!(report.events_processed, 12);
        assert_eq!(report.outcome, "success");
        let peak_rss = report
            .peak_rss_bytes
            .expect("the runner samples its own RSS");
        assert!(peak_rss > 0);
        let payload = serde_json::to_value(&report).unwrap();
        assert_eq!(payload["peak_rss_bytes"], peak_rss);
    }
}
