# Local Monitoring Stack

This directory provisions a local Prometheus, Alertmanager, and Grafana stack for both the
existing Python Bridge and Rust vNext. It intentionally contains no compose file, credential,
public listener, or remote receiver. The repository supplies a rootless installer and user units;
all three applications bind to loopback and run as the bridge user.

## Layout

- `prometheus/prometheus.yml` scrapes the Python adapter at `127.0.0.1:9464` and Rust vNext at
  `127.0.0.1:9465`.
- `prometheus/alerts/bridge.yml` evaluates the shared metric contract in
  `../docs/rust-vnext/observability.md`.
- `alertmanager/alertmanager.yml` discards alerts by default. Only critical Bridge alerts are sent
  to the Rust daemon's loopback webhook at `127.0.0.1:18091/alerts`, which forwards them through
  the separate monitoring Bot.
- `grafana/` provides the Prometheus data source and one immutable dashboard.

The supported local installation uses pinned official Linux binaries and user-owned paths:

```bash
bash scripts/install-monitoring.sh install
```

The installer verifies SHA-256 checksums, renders the Grafana paths for the current user, validates
Prometheus/Alertmanager/Grafana configuration, installs systemd user units, and starts the stack.
Use `bash scripts/install-monitoring.sh status` to inspect it and `... stop` to stop it. The
Prometheus and Alertmanager data directories are under
`~/.local/state/codex-telegram-bridge/monitoring/`; Grafana data is under
`~/.local/share/codex-telegram-bridge/monitoring/`.

The intended rootless launch flags keep the stack local and bound the retained series to the
planned budget:

```bash
prometheus \
  --config.file=monitoring/prometheus/prometheus.yml \
  --storage.tsdb.path="$XDG_STATE_HOME/codex-telegram-bridge/prometheus" \
  --storage.tsdb.retention.time=30d \
  --storage.tsdb.retention.size=2GB \
  --web.listen-address=127.0.0.1:9090

alertmanager \
  --config.file=monitoring/alertmanager/alertmanager.yml \
  --storage.path="$XDG_STATE_HOME/codex-telegram-bridge/alertmanager" \
  --web.listen-address=127.0.0.1:9093
```

Grafana should use `monitoring/grafana/grafana.ini`, which disables anonymous access and binds its
HTTP listener to `127.0.0.1:3000`. These commands are operator-owned and are not executed by the
bridge or by tests.

The supplied target addresses are contracts, not an instruction to expose a port. A target is only
healthy after the relevant Bridge implementation serves the documented `/metrics` endpoint on that
loopback address. The Python Bridge is currently stopped during the Rust live test, so its `9464`
target-down alert is expected until Python is started or that target is intentionally removed from
the local Prometheus configuration. The Rust daemon owns the webhook receiver while it is running.

## Validation

With the native tools installed, validate before deployment:

```bash
promtool check config monitoring/prometheus/prometheus.yml
promtool check rules monitoring/prometheus/alerts/bridge.yml
amtool check-config monitoring/alertmanager/alertmanager.yml
jq empty monitoring/grafana/dashboards/codex-telegram-bridge.json
```

The installer keeps `promtool` and `amtool` beside their pinned server binaries and runs both checks
before starting the services.
