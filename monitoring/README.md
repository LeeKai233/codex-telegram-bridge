# Local Monitoring Stack

This directory provisions a local Prometheus, Alertmanager, and Grafana stack for both the
existing Python Bridge and Rust vNext. It intentionally contains no compose file, service unit,
credential, public listener, or remote receiver. Operators own process supervision and bind all
three applications to loopback unless they deliberately provide equivalent network protection.

## Layout

- `prometheus/prometheus.yml` scrapes the Python adapter at `127.0.0.1:9464` and Rust vNext at
  `127.0.0.1:9465`.
- `prometheus/alerts/bridge.yml` evaluates the shared metric contract in
  `../docs/rust-vnext/observability.md`.
- `alertmanager/alertmanager.yml` discards alerts by default. Only critical Bridge alerts are sent
  to the optional local webhook at `127.0.0.1:18091/alerts`.
- `grafana/` provides the Prometheus data source and one immutable dashboard.

Copy or mount the directories at the locations selected by the local Prometheus, Alertmanager, and
Grafana installations. For Grafana, mount the dashboard JSON at
`/etc/grafana/provisioning/dashboards/codex-telegram-bridge.json`; the provider config deliberately
uses that same directory.

The supplied target addresses are contracts, not an instruction to expose a port. A target is only
enabled after the relevant Bridge implementation serves the documented `/metrics` endpoint on that
loopback address. Delete a target stanza for an implementation that is not installed, otherwise the
target-down alert is expected.

## Validation

With the native tools installed, validate before deployment:

```bash
promtool check config monitoring/prometheus/prometheus.yml
promtool check rules monitoring/prometheus/alerts/bridge.yml
amtool check-config monitoring/alertmanager/alertmanager.yml
jq empty monitoring/grafana/dashboards/codex-telegram-bridge.json
```

`promtool` and `amtool` are intentionally not bundled or installed by this repository.
