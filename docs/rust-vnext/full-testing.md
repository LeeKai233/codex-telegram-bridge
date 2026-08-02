# Rust full-runtime live test

This deployment is an explicit, reversible test boundary for the Rust Bridge.
It uses the four `rust_*` credentials from `~/.tgrc`, an independent SQLite
database and polling leases, and the existing Codex app-server Unix socket.
It never migrates or writes the Python Bridge database.

## Topology

- Channel: `Rust Cotton Field Channel` (`-1004446000549`)
- Linked discussion: `Rust Cotton Field Forum` (`-1004290500369`)
- Control chat: the existing owner private chat (`5174236639`)
- Rust state: `~/.local/state/codex-telegram-bridge/rust-vnext-full/`
- Config: `~/.config/codex-telegram-bridge/rust-vnext.toml` (mode `0600`)
- Metrics: `127.0.0.1:9465/metrics`
- Alertmanager webhook: `127.0.0.1:18091/alerts`
- Unit: `codex-telegram-rust-full.service`

The control Bot owns the private control chat. The discussion Bot owns the
linked forum and binds automatic channel forwards to native comment roots.
The status Bot polls its own update stream for callbacks; the monitoring Bot is
send-only and never acquires a polling lease.

## Install and probes

From this checkout, as the normal user:

```bash
bash scripts/rust-vnext-full.sh install
cargo run --release --manifest-path rust/Cargo.toml -p codex-telegram-cli -- probe
curl --fail --silent http://127.0.0.1:9465/metrics | head
```

`probe` calls `getMe`, `getChat`, and `getChatMember` for the configured
surfaces. It prints bot usernames and membership states, never token values.
Do not start the Rust unit until the probe and app-server socket are healthy.

## Cutover and smoke test

```bash
bash scripts/rust-vnext-full.sh cutover
journalctl --user -u codex-telegram-rust-full.service -n 80 --no-pager
```

In the owner private chat, send `/help`, `/status`, `/perf`, `/new`, then a
small prompt. Write prompts, Plan/model/review/cancel commands, file uploads,
and approval callbacks require a TOTP lease; send `/totp <6-digit-code>` before
testing them and `/lock` after the test. `/planmode on|off`, `/changemodel
<model> [effort]`, `/review [base <branch>|commit <sha>|custom <text>]`,
`/cancel`, and `/getfile <workspace-relative-path>` now call the corresponding
Codex/app-server or Telegram adapter and retain a bounded artifact record in
Rust SQLite. In the linked discussion, publish a channel post and reply to the
automatic native comment. Verify the Rust reply is attached to the native
comment root. Modern and legacy command/file/permission approval callbacks are
wired to SQLite and the Codex JSON-RPC response; sensitive
`requestUserInput` remains deliberately refused.

Verify the loopback metrics endpoint and the independent state file after the
smoke test. Prometheus already has a `127.0.0.1:9465` target in
`monitoring/prometheus/prometheus.yml`; Grafana and Alertmanager remain local
operator processes and must not be exposed publicly.

Active Rust session spaces are restored from the independent SQLite database at
daemon startup, so a service restart does not require `/new` merely to rebuild
the in-memory chat-to-thread registry. Invalid or ownerless records are skipped
without exposing their identifiers in logs.

## Rollback

```bash
bash scripts/rust-vnext-full.sh rollback
```

Rollback stops Rust before starting Python. The Python unit, credentials, and
production SQLite state are not edited. `restore-files` is only for undoing the
unit/config installation itself; it also leaves both services stopped.
