# Rust vNext Observability Contract

This contract gives the Rust vNext daemon and an adapter for the existing Python Bridge one
Prometheus vocabulary. It is intentionally about externally observable behaviour, not either
implementation's internal types. Version one is additive only: producers may add metrics and label
values, but must not rename, remove, or reinterpret a metric below without a new contract version.

## Endpoint and labels

Each implementation exposes Prometheus text exposition at `GET /metrics` on a loopback-only
listener. The deployment configuration reserves `127.0.0.1:9464` for the existing Python Bridge and
`127.0.0.1:9465` for Rust vNext. `/metrics` must return `200`, `text/plain; version=0.0.4` or a
compatible OpenMetrics content type, and must not require a token. Health and administrative
endpoints are separate from this contract and must not be exposed through the metrics listener.

Every metric emitted by a producer has these bounded labels where applicable:

| Label | Allowed values | Meaning |
| --- | --- | --- |
| `implementation` | `python`, `rust-vnext` | Serving implementation. Prometheus supplies this label from the scrape target. |
| `bot_role` | `control`, `discussion`, `status`, `alert` | Telegram Bot role. `alert` is send-only and never emits polling samples. |
| `component` | `bridge`, `telegram_polling`, `app_server`, `delivery` | Independently health-checked component. |
| `queue` | `prompt`, `delivery`, `workload` | Bounded queue name. |
| `result` | `success`, `failed`, `cancelled`, `rejected` | Terminal operation result. |

Never use a session ID, thread ID, turn ID, chat ID, Telegram message ID, username, path, prompt,
exception text, URL, token, project name, or unbounded error string as a metric label. These values
belong in access-controlled logs or the sanitized replay contract, not Prometheus.

## Required metrics

| Metric | Type | Labels | Semantics |
| --- | --- | --- | --- |
| `codex_telegram_bridge_build_info` | gauge | `version`, `revision` | Constant `1` for the running build. `revision` is a short public source revision, never a filesystem path. |
| `codex_telegram_bridge_component_healthy` | gauge | `component` | `1` only when the component can perform its required work; `0` otherwise. |
| `codex_telegram_bridge_process_start_time_seconds` | gauge | none | Unix start time of this process. |
| `codex_telegram_bridge_telegram_poll_last_success_unixtime` | gauge | `bot_role` | Unix time of the most recent successful Telegram poll. It must not advance on a failed request. |
| `codex_telegram_bridge_telegram_poll_requests_total` | counter | `bot_role`, `result` | Completed poll requests by terminal result. |
| `codex_telegram_bridge_telegram_delivery_attempts_total` | counter | `bot_role`, `result` | Telegram send/edit/delete attempts by terminal result. |
| `codex_telegram_bridge_telegram_delivery_duration_seconds` | histogram | `bot_role`, `result` | End-to-end duration of a completed delivery attempt. |
| `codex_telegram_bridge_queue_depth` | gauge | `queue` | Current queued work count, sampled at scrape time. |
| `codex_telegram_bridge_event_loop_lag_seconds` | gauge | none | Most recent scheduler/event-loop lag. A value greater than zero is not an error by itself. |
| `codex_telegram_bridge_supervisor_restarts_total` | counter | `component` | Restarts initiated by the Bridge's own supervision logic. |

Counters use the Prometheus `_total` suffix and never reset except when their process restarts.
Histograms use conventional cumulative `_bucket`, `_sum`, and `_count` samples. Missing optional
components are omitted; a configured component that is known unhealthy must emit `0` rather than
disappearing.

## Producer mapping

Rust vNext is the reference producer and exports these metrics directly. The existing Bridge needs a
small in-process or sidecar adapter that maps its existing `PollingHealth`, delivery scheduler,
`MetricsSampler`, and app-server supervisor state to the contract. That adapter is intentionally
outside this configuration-only lane; it must not infer health from a stale SQLite snapshot.

The Prometheus scrape `up` metric answers whether the exporter responded. The
`component_healthy` metrics answer whether the Bridge is operational. Both are required for alerts:
an exporter can be reachable while Telegram polling is stalled.

## Alerting and dashboard

`monitoring/prometheus/alerts/bridge.yml` evaluates target reachability, component health, polling
staleness, delivery failure ratio, queue backlog, and event-loop lag. The alert rules assume all
required metrics above are present for every configured implementation. Remove an absent target
from `monitoring/prometheus/prometheus.yml`; do not suppress a target-down alert with a fake metric.

The Alertmanager default receiver discards notifications. Critical Bridge alerts use the Rust
daemon's loopback-only webhook at `127.0.0.1:18091/alerts`; the daemon forwards them through the
separate send-only monitoring Bot. The endpoint carries no secret and is intentionally bound to
loopback, so it is not an Internet-facing webhook. Keep the monitoring Bot limited to the intended
alert chat.
