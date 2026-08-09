# Parallel Task Contract

- Goal slug: v027-bridge-ux-state
- Baseline SHA: bfa7a50c69c9d0514c619a2de9ae4aea413e7500
- Integration branch: current dirty worktree (no commit requested)
- Root coordinator: `/root`, bridge root coordinator, `gpt-5.6-sol/xhigh`
- Effective permission mode: Full Access; children remain behaviorally read-only
- In-scope behavior: Telegram topics/buttons/Subagent presentation, shared moon-phase rendering, terminal Tasks counters, parent Agent state refresh, recoverable reconnect cleanup, permission inheritance after model changes, and focused/full local verification.
- Out-of-scope behavior: reply-to routing or edit behavior, Forum Topics migration, credentials, remote Git state, releases, version/tag metadata, package installation, service restart, unrelated user files, and the existing untracked `assets/` directory.

| Lane | Agent role | Model | Effort | Selection reason | Worktree | Branch | Owned files | Inputs/contracts | Required tests | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| telegram-root | root coordinator | gpt-5.6-sol | xhigh | Owns architecture, HIGH/CRITICAL paths, all writes, integration, and local verification because the dirty worktree overlaps the implementation surface. | current worktree | current branch | All in-scope source/tests except `assets/` | Existing dirty implementation is user-owned input; preserve and extend it in place. | Focused pytest, full pytest, Ruff, GitNexus detect_changes | 1 |
| codex-audit | bridge-explorer | gpt-5.6-terra | high | Read-only audit of Codex WebSocket/RPC scheduling, reconnect, resync, and subagent hydration contracts. | none (read-only) | none | none | Return symbol-level findings and focused test cases; no file writes or service actions. | none (read-only) | advisory before gates 2-3 |
| persistence-audit | bridge-explorer | gpt-5.6-terra | high | Read-only audit of schema/event retention, maintenance isolation, startup/polling recovery, and status observability. | none (read-only) | none | none | Return schema migration risks, operational ordering, and focused test cases; no file writes or service actions. | none (read-only) | advisory before gates 3-5 |
| integration-review | bridge-reviewer | gpt-5.6-luna | max | Independent read-only final regression and concurrency review after integration. | none (read-only) | none | none | Review the final diff and verification evidence; no file writes or service actions. | none (read-only) | final |

## Shared Rules

- One writer per file per phase.
- The coordinator selects and records every lane's role, model, effort, and routing reason before spawn; implicit inheritance is not a routing decision.
- Default routing is explorer=`gpt-5.6-terra/xhigh`, worker=`gpt-5.6-terra/xhigh`, reviewer=`gpt-5.6-sol/max`, modernizer=`gpt-5.6-terra/max`, root=`gpt-5.6-sol/max`.
- Existing symbols require GitNexus impact before edits.
- HIGH or CRITICAL impact must be reported before editing.
- No child may modify services, credentials, installed packages, other worktrees, user assets, releases, or remotes.
- Full Access inheritance is assumed; read-only roles are enforced by instructions and verified with Git diff.
- Every writer returns a commit hash, focused test results, Ruff results, detect_changes scope, and residual risks.
- The coordinator alone integrates, runs full regression, deploys, and reports completion.

## Active Goal Addendum: bridge-interaction-performance-v11

- Goal ID: `019f88d2-78ed-7df3-a2c9-ee0e2066dba5`
- Baseline SHA: `8915a3182dcef65905025999b87bd9d2ddc224e0`
- Codex reference: `rust-v0.145.0` at `25af12f7e61572b0bc18ddb1008be543b91519b0`
- Integration branch: `main` in the root worktree; preserve all pre-existing dirty files.
- Root coordinator: `/root`, `gpt-5.6-sol/xhigh`; owns architecture, shared contracts,
  integration, conflicts, final verification, service state, and user communication.
- Effective permission mode: Full Access; children must stay inside their assigned
  worktrees and must not control services, install packages, edit credentials, push, publish,
  or touch `assets/`.
- In scope: event projection isolation, thread turn gate, prompt intent reconciliation, schema v11,
  Telegram traffic classes, SWR snapshots, dashboard fingerprints, approval lifecycle, Plan/TUI
  race handling, tmux dead-pane detection, migrations, observability, and tests.
- Out of scope: upstream Codex changes, fabricated late-attach TUI collaboration state, releases,
  remote Git state, credentials, package installation, and the untracked `assets/` directory.

| Lane | Agent role | Model | Effort | Selection reason | Worktree | Branch | Owned files | Required tests | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| core-state | bridge-worker | gpt-5.6-sol | high | Implements persisted state and Codex protocol semantics as one coherent owner. | `/tmp/codex-bridge-v11-core` | `agent/bridge-v11/core-state` | `src/codex_telegram_bridge/{codex,bridge,projector,models,store,space_coordinator}.py`, `tests/test_{codex,bridge,projector,core_store,space_coordinator}.py` | owned focused tests, Ruff, GitNexus detect_changes | 1 |
| telegram-transport | bridge-worker | gpt-5.6-sol | high | Isolates latency-sensitive outbound traffic and runtime scheduling without core-state file overlap. | `/tmp/codex-bridge-v11-transport` | `agent/bridge-v11/telegram-transport` | `src/codex_telegram_bridge/{outbound,telegram_common,workloads,main}.py`, `tests/test_{outbound,telegram_common,workloads,main}.py` | owned focused tests, Ruff, GitNexus detect_changes | 2 |
| ux-maintenance | bridge-worker | gpt-5.6-sol | high | Owns Telegram workflows, dashboard behavior, approval presentation, metrics, tmux safety, and mode presentation. | `/tmp/codex-bridge-v11-ux` | `agent/bridge-v11/ux-maintenance` | `src/codex_telegram_bridge/{discussion_bot,control_bot,space_dashboard,metrics,tmux,approval,views}.py`, `tests/test_{discussion_bot,discussion_commands,control_workflows,telegram_spaces,dashboard,metrics,tmux,views}.py` | owned focused tests, Ruff, GitNexus detect_changes | 3 |
| integration-review | bridge-reviewer | gpt-5.6-luna | max | Performs independent read-only correctness, concurrency, migration, and regression review after integration. | none | none | none | review final diff and all verification evidence | final |

### Shared Contracts

- `PromptIntent` is the durable idempotency record for every initial prompt, queued prompt, upload,
  Plan prompt, and steer attempt. Its state machine is `received -> awaiting_choice/queued/submitting
  -> started/steered -> completed`, with terminal `failed`, `uncertain`, and `cancelled` states.
- Every submission carries `client_message_id`; reconciliation uses persisted Codex
  `userMessage.clientId` before any retry can create another turn.
- `ThreadLiveSnapshot` separates desired settings from current-generation observed settings.
  Collaboration mode observation may be `unknown`; `thread/resume` alone never proves it.
- Queue ownership is thread-scoped. Matching `turn/completed` releases the gate; idle only starts a
  one-second reconciliation fallback and never immediately dequeues the next prompt.
- Codex notification projection remains ordered and side-effect-free on its hot path. Telegram,
  tmux, and file effects are dispatched asynchronously after state has been committed.
- Telegram outbound traffic classes are `callback_ack`, `interactive`, `media`, and `maintenance`.
  PTB remains the authority for API rate limits; interactive work preserves per-chat FIFO.
- Approval state is `pending -> claimed -> awaiting_resolved -> resolved`; Telegram cleanup is
  background work after the Codex response succeeds.
- Dashboard persistence stores semantic fingerprints. Restart emits zero edits for unchanged
  spaces, and animation is limited to active/in-progress spaces.

### Integration Rules

- Workers may add new files only when the root explicitly assigns them; cross-lane API changes are
  reported to the root before implementation.
- Each worker must run GitNexus upstream impact before editing existing symbols and must report
  HIGH or CRITICAL risk before proceeding.
- The root integrates one committed lane at a time, then resolves shared imports and behavioral
  contracts centrally before running repository-wide verification.

## Active Goal Addendum: collaboration-mode-observation-root-fix

- Goal thread ID: `019f88d2-78ed-7df3-a2c9-ee0e2066dba5`
- Baseline SHA: `8767201a96b4229d85a78a6fca3b4551f4620e47`
- Integration branch: `main` in the root worktree; preserve all pre-existing dirty files.
- Root coordinator/writer: `/root`, `gpt-5.6-sol/xhigh`; selected because the projector,
  reconnect, persistence, and Telegram guard changes form one small state contract.
- Independent reviewer: `/root/integration_review`, `bridge-reviewer`, `gpt-5.6-luna/max`;
  selected for a read-only final correctness and regression review after integration.
- In scope: repeatable `thread/settings/updated` projection, compact mode evidence, reconnect
  observation invalidation, locked `/status`, focused/full verification, bridge-only restart, and
  a controlled Plan-to-Default live round trip.
- Out of scope: Codex app-server/TUI restarts, credentials, releases, remote Git state, package
  installation, unrelated user files, and the untracked `assets/` directory.
- Prior GitNexus impact results: `EventProjector.project`, `Bridge._on_codex_connection`,
  `Store._compact_event_payload`, and `DiscussionBotController._guard` are LOW risk; no HIGH or
  CRITICAL warning was returned.

| Lane | Agent role | Model | Effort | Selection reason | Worktree | Branch | Owned files | Required tests | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| root-fix | root coordinator | gpt-5.6-sol | xhigh | Owns the cross-file observation contract and live deployment. | current worktree | `main` | approved source and focused test files only | focused/full pytest, Ruff, shell/systemd, diff, GitNexus | 1 |
| integration-review | bridge-reviewer | gpt-5.6-luna | max | Independent read-only review of deduplication, reconnect truthfulness, security guard, and retained evidence. | none | none | none | review final diff and verification evidence | final |

## Active Goal Addendum: three-bot-routing-status-coalescing

- Goal thread ID: `019f88d2-78ed-7df3-a2c9-ee0e2066dba5`
- Baseline SHA: `8767201a96b4229d85a78a6fca3b4551f4620e47`
- Integration branch: `main` in the root worktree; preserve all pre-existing dirty files.
- Root coordinator/writer: `/root`, `bridge-worker`, `gpt-5.6-sol/xhigh`; owns architecture,
  dirty-file integration, credentials, service state, migration, and final verification.
- In scope: canonical 9527/426/69 token contract, status Bot runtime and callbacks,
  active status-message ownership migration, lane-aware dashboard delivery, outbound latest-wins
  coalescing, installer/docs/tests, and live deployment/acceptance.
- Out of scope: upstream Codex changes, releases, remote Git state, package publication, and
  the untracked `assets/` directory.

| Lane | Agent role | Model | Effort | Selection reason | Worktree | Branch | Owned files | Required tests | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| token-contract | bridge-worker | gpt-5.6-sol | high | Keeps credential paths, atomic three-token configuration, installer, and public docs coherent. | `/tmp/codex-bridge-three-token` | `agent/bridge-three-bot/token-contract` | `src/codex_telegram_bridge/config.py`, `src/codex_telegram_bridge/cli.py`, `src/codex_telegram_bridge/files.py`, `install.sh`, `README.md`, owned config/CLI/installer tests | focused pytest, Ruff, `bash -n install.sh`, `git diff --check`, GitNexus detect_changes | 1 |
| outbound-coalescing | bridge-worker | gpt-5.6-sol | high | Isolates latency-sensitive queue coalescing and dashboard lane contracts with no overlap with runtime/state files. | `/tmp/codex-bridge-three-outbound` | `agent/bridge-three-bot/outbound-coalescing` | `src/codex_telegram_bridge/outbound.py`, `src/codex_telegram_bridge/delivery.py`, `src/codex_telegram_bridge/telegram_common.py`, `src/codex_telegram_bridge/space_dashboard.py`, owned outbound/delivery/dashboard tests | focused pytest, Ruff, `git diff --check`, GitNexus detect_changes | 2 |
| integration-review | bridge-reviewer | gpt-5.6-luna | max | Independent read-only review after root integration for migration, callback scope, concurrency, and regressions. | none | none | none | final diff and full verification evidence | final |

### Shared Contracts

- Bot roles are `control`, `discussion`, and `status`; status message ownership is persisted in
  Space state as `status_bot_role`, with legacy bound messages interpreted as `discussion`.
- `DeliveryIntent` carries an explicit lane; outbound idempotent edits may coalesce by
  `(bot_role, chat_id, message_id)`, while non-idempotent operations never coalesce.
- The root owns `main.py`, `store.py`, `space_coordinator.py`, `discussion_bot.py`, the new
  status controller, integration tests, credential changes on disk, systemd, and deployment.

### Integration Rules

- Each writer must run upstream impact before editing existing symbols and report HIGH/CRITICAL
  results before proceeding; no child may control systemd, credentials, packages, remotes, or assets.
- The root integrates one committed lane at a time, runs the affected tests after each integration,
  then closes all children before any local install, credential rename, or service restart.

## Active Goal Addendum: app-server-supervision-auto-recovery

- Goal thread: current user request to implement the approved first-class Codex app-server supervision plan.
- Baseline SHA: `04707b8ffe33783055f65a377cc2b7380d7bc227`
- Integration branch: `main` in the current dirty worktree; preserve all pre-existing user edits and `assets/`.
- Root coordinator: `/root`, architecture, dirty-file integration, GitNexus gates, full verification, live deployment, and user communication.
- Scope: Codex connection transition telemetry, app-server supervisor/recovery state machine, durable notification latches, app-server mode contract, diagnostics, installer watchdog units, focused tests, and isolated recovery drill.
- Out of scope: credentials, releases, remote Git state, unrelated user files, and production app-server fault injection.

| Lane | Agent role | Model | Effort | Selection reason | Worktree | Branch | Owned files | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| supervisor-core | bridge-worker | gpt-5.6-luna | max | New supervisor and protocol probe can be implemented independently before root integration. | `/tmp/codex-bridge-app-supervisor` | `agent/app-server-supervision/supervisor-core` | `src/codex_telegram_bridge/app_server.py`, `tests/test_app_server.py` | 1 |
| mode-diagnostics-installer | bridge-worker | gpt-5.6-luna | max | Config mode validation, CLI diagnostics, and installer units are disjoint from dirty Codex/Bridge runtime files. | `/tmp/codex-bridge-app-installer` | `agent/app-server-supervision/mode-diagnostics-installer` | `src/codex_telegram_bridge/config.py`, `src/codex_telegram_bridge/cli.py`, `install.sh`, `tests/test_cli.py`, `tests/test_installer.py` | 2 |
| integration-review | bridge-reviewer | gpt-5.6-luna | max | Independent read-only review after root integration for recovery races, notification deduplication, and deployment safety. | none | none | none | final |

### Shared Contracts

- `AppServerMode` values are `installer-service`, `managed-daemon`, and `external`; the missing environment default is `external`.
- `AppServerSupervisor` receives a `CodexClient`, mode, socket/binary paths, and injectable command/probe functions; `snapshot()` is JSON-safe and exposes state/action/error counters.
- The root integrates the supervisor with existing `CodexClient`, `Bridge`, `run_service`, `Store` metadata, and health snapshot contracts without reverting dirty user changes.
- Workers may create temporary commits in their isolated branches for integration; the root must not create a final repository commit unless separately requested.

### Execution Note

- The configured `gpt-5.6-luna` route was unavailable in this environment, so the two writer lanes executed with the available `gpt-5.6-terra/high` fallback; root integration remained with the root coordinator.

### Integration Rules

- Before editing an existing symbol, run GitNexus upstream impact and report HIGH/CRITICAL risk; new files still require focused tests.
- Workers must run focused tests, Ruff, and scoped `detect_changes`; they must not control systemd, credentials, packages, remotes, or production services.
- Root integrates one lane at a time, runs affected tests, closes all children, then performs local install/restart and the isolated `CODEX_HOME` recovery drill.

## Active Goal Addendum: app-server-start-limit-pid-recovery

- Goal thread: `019f9928-d3f7-7e61-84f7-f0a20ab7cb25`
- Baseline SHA: `51c078483a0e12b75e1f5ddf0f487f318f312d84`
- Integration branch: `main` in the current dirty worktree; preserve all pre-existing user edits and the untracked `assets/` directory.
- Root coordinator/writer: `/root`, `gpt-5.6-sol/xhigh`; selected because the affected installer, app-server recovery, systemd contract, and tests overlap existing dirty files and require one coherent integration.
- In scope: immutable v0.3.1 README checksum, bounded PID-record reads, lock-held identity-checked stale PID unlink, mode-specific start limits, marker-driven managed-daemon bridge restart after app-server recovery, focused tests, and local static/unit/impact verification.
- Out of scope: credentials, releases, remote Git state, package installation, unrelated user files, production fault injection, and `assets/`.

| Lane | Agent role | Model | Effort | Selection reason | Worktree | Branch | Owned files | Required tests | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| root-implementation | root coordinator | gpt-5.6-sol | xhigh | Owns all writes because every implementation file is already dirty and the recovery contract crosses Python, shell, unit, and tests. | current worktree | `main` | approved README, install.sh, app_server.py, cli.py, systemd, and focused tests | focused/full pytest, Ruff, shell/systemd checks, GitNexus detect_changes | 1 |
| pid-recovery-audit | bridge-explorer | gpt-5.6-luna | max | Read-only concurrency audit of PID record parsing, sibling locking, replacement races, and safe deletion invariants. | none | none | none | none; return symbol-level findings and test cases | advisory before implementation |
| systemd-recovery-audit | bridge-explorer | gpt-5.6-luna | max | Read-only audit of managed-daemon watchdog, start-limit behavior, marker lifecycle, and intentionally inactive bridge semantics. | none | none | none | none; return unit/state-machine findings and test cases | advisory before implementation |
| integration-review | bridge-reviewer | gpt-5.6-luna | max | Independent final read-only review of concurrency, recovery convergence, installer mode contracts, and regression gaps. | none | none | none | inspect final diff and verification evidence | final |

### Shared Contracts

- PID cleanup reads at most `MAX_PID_RECORD_BYTES + 1`, rejects oversized/malformed/non-regular/symlink records, and skips deletion on contention or uncertain identity.
- PID cleanup holds the sibling daemon lock through a re-read, identity validation, and unlink; replacement after classification must not be deleted.
- Managed-daemon bridge throttling remains `5/300`; installer-service and external units remain `30/900`.
- A private atomic restart marker is written when app-server recovery becomes fatal. A healthy watchdog pass resets/starts only a failed bridge, retains the marker on failed restart, clears it after a healthy successor, and never starts an intentionally inactive bridge.
- No public CLI command is added; `app-server-watchdog --recover` owns the recovery extension.
- Root performs all integration, service actions, and final validation after read-only agents are closed.

### Integration Rules

- Existing symbols require GitNexus upstream impact before edits; report HIGH/CRITICAL results before proceeding.
- Read-only agents must not write files, control systemd, install packages, edit credentials, touch other worktrees, push, or publish.
- Root must run `detect_changes(compare main)` before Goal completion and close every child before any live service action.

## Active Goal Addendum: rust-live-test-daemon

- Goal thread: current request to deploy the Rust vNext Bridge into the dedicated Telegram test environment.
- Baseline SHA: `69f567bee5aa8eb73c729a731dd93bc40f549cf1`.
- Integration branch: `main` in the current dirty root worktree; preserve every pre-existing user edit.
- Root coordinator: `/root`, `gpt-5.6-sol/xhigh`; owns architecture, integration, credentials,
  Telegram discovery, local installation, systemd, live validation, rollback, and user communication.
- In scope: a bounded Rust Telegram canary daemon, explicit per-Bot polling opt-in, cross-process
  ownership locks, command replies, metrics, dedicated test configuration, rootless systemd user
  deployment, rollback documentation, tests, and live verification against the Rust test Bots.
- Out of scope: the Python production service, 9527/426/69 credentials, production polling transfer,
  automatic conversion of Telegram groups into Forums, releases/remotes, package installation,
  unrelated user files, and `assets/`.

| Lane | Agent role | Model | Effort | Selection reason | Worktree | Branch | Owned files | Required tests | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| runtime-daemon | bridge-worker | gpt-5.6-luna | max | Implements the safety-critical polling and reply loop behind explicit test-only configuration. | `/tmp/codex-bridge-rust-live-runtime` | `agent/rust-live/runtime-daemon` | `rust/crates/cli/src/{config,daemon,main}.rs`, CLI crate tests only | cargo fmt/test/clippy, GitNexus detect_changes | 1 |
| deployment | bridge-worker | gpt-5.6-luna | max | Keeps rootless service/config/install/runbook artifacts separate from runtime code. | `/tmp/codex-bridge-rust-live-deploy` | `agent/rust-live/deployment` | new Rust test systemd unit, deployment script, config template, live-testing docs | shell/static checks, git diff check, GitNexus detect_changes | 2 |
| integration-review | bridge-reviewer | gpt-5.6-luna | max | Independently reviews ownership, backlog handling, secret safety, shutdown, deployment isolation, and test gaps after integration. | none | none | none | final diff and verification evidence | final |

Execution note: the configured `gpt-5.6-luna` routes are unavailable in this environment. Writer
lanes use the available `worker / gpt-5.6-terra / high` fallback; the final review uses the
available read-only reviewer route if the configured reviewer route is likewise unavailable.

### Shared Contracts

- The daemon polls only Bot entries with an explicit opt-in; no implicit polling of every enabled Bot.
- It must acquire `TokenLeaseRegistry::acquire_with_lock` before the first `getUpdates` call.
- The dedicated Rust test service uses only `rust_91_bot_key`, `rust_818_bot_key`,
  `rust_411_bot_key`, and optionally `rust_826_bot_key`; it never reads production Bot files except
  the existing `.tgrc` registry selected by configuration.
- First-start backlog policy is explicit and bounded. Message text, token values, chat titles,
  usernames, paths, and exception bodies are not metric labels or routine logs.
- Supported live commands are bounded (`/ping`, `/status`, `/help`); arbitrary messages are not
  forwarded or echoed until a Codex app-server transport is implemented.
- The test daemon never claims to be the full Codex Bridge: Codex app-server RPC, durable projection,
  approvals, artifacts, and migration remain future adapters.
- Deployment is a separate user service with its own config, binary, state/lock directory, metrics
  port, and rollback path. It must not restart or modify the Python production service.

### Integration Rules

- Each writer must run GitNexus upstream impact before editing existing symbols and report any HIGH
  or CRITICAL result before proceeding.
- Workers must commit only owned files and must not touch services, credentials, packages, remotes,
  the root worktree, other worktrees, or `assets/`.
- Root integrates commits sequentially, closes all children, runs full Rust gates and a read-only
  reviewer, then alone installs/starts/stops the real test service and performs Telegram acceptance.

## Active Goal Addendum: rust-full-runtime-cutover

- Goal thread: `019fb8c2-8c42-78a3-b34f-e0e74206ae80`.
- Baseline SHA: `69f567bee5aa8eb73c729a731dd93bc40f549cf1`.
- Integration branch: `main` in the current dirty root worktree; preserve every pre-existing user edit.
- Root coordinator: `/root`, `gpt-5.6-sol/xhigh`; owns architecture, cross-lane contracts, integration,
  credentials, service state, live Telegram validation, rollback, and user communication.
- Effective permission mode: Full Access. Children must remain in isolated worktrees, must not control
  systemd, edit credentials, install packages, touch other worktrees or `assets/`, push, or publish.
- In scope: a complete Rust replacement behind the existing Python behavior contract; Codex Unix
  app-server JSON-RPC transport; pluggable AgentBackend/engine/ports boundaries; independent SQLite
  state; three business Bot roles plus the separate monitoring Bot; native channel linked-discussion
  comments; commands, callbacks, approvals/TOTP, files, Plan/review/status flows; low-cardinality
  Prometheus/OpenMetrics; rootless Prometheus/Grafana OSS/Alertmanager assets; isolated install,
  controlled cutover, live acceptance, and rollback.
- Out of scope: upstream Codex changes, Python database migration, production credential edits,
  releases/remotes, automatic Forum conversion, and user-visible Claude/team-collaboration features
  beyond the future backend adapter boundary.

| Lane | Agent role | Model | Effort | Selection reason | Worktree | Branch | Owned files | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| core-runtime | bridge-worker | gpt-5.6-terra | high | Owns the transport-neutral domain/engine contracts and Codex Unix app-server client; this is the core compatibility boundary and needs a single coherent writer. | `/tmp/codex-bridge-rust-full-core` | `agent/rust-full/core-runtime` | `rust/crates/domain`, `rust/crates/ports`, `rust/crates/engine`, new `rust/crates/app-server`, owned tests | 1 |
| telegram-state | bridge-worker | gpt-5.6-terra | high | Owns Telegram polling/role routing, native linked-comment behavior, fresh SQLite state, and workflow controllers without overlapping the transport core. | `/tmp/codex-bridge-rust-full-telegram` | `agent/rust-full/telegram-state` | `rust/crates/telegram`, `rust/crates/storage-sqlite`, new controller modules/tests | 2 |
| observability-deploy | bridge-worker | gpt-5.6-terra | high | Owns metrics/exporter, monitoring configuration, service templates, config wiring, and live-test runbook; kept separate from business behavior. | `/tmp/codex-bridge-rust-full-deploy` | `agent/rust-full/observability-deploy` | `rust/crates/cli`, monitoring/deploy docs and units | 3 |
| integration-review | bridge-reviewer | gpt-5.6-terra | high | Independent read-only review after integration for protocol correctness, Telegram ownership, persistence, secret safety, and rollback gaps. | none | none | none | final |

### Shared Contracts

- Bot roles are `control` (91), `status` (818), `discussion` (411), and `monitoring` (826). Only
  control handles `/perf`; monitoring sends Alertmanager firing/resolved notifications.
- The Telegram topology is channel `-1004446000549` plus linked discussion `-1004290500369`; it is
  not a Topics forum, so native `Leave a comment`/automatic-forward semantics remain authoritative.
- The Rust process is the sole Telegram poller during cutover and acquires a cross-process lease before
  `getUpdates`. Its state directory and SQLite database are independent of Python's.
- The Codex client performs `initialize`/`initialized`, bounded pending-RPC and notification queues,
  server-request handling, thread/turn calls, reconnect, and idempotent request correlation over the
  existing Unix app-server socket.
- No metric label may contain message text, token values, chat titles, usernames, paths, exception
  bodies, or correlation IDs. Correlation IDs belong only in redacted structured logs.
- The root integrates one lane at a time, runs the affected Rust tests after each integration, closes
  every child, then performs local install/service actions and the live acceptance/rollback sequence.

## Active Goal Addendum: rust-python-business-parity

- Goal thread: `019fc25c-d4fe-74e3-b7ee-48e0b9cb2d61`.
- Baseline SHA: `e93f6935f58b48a0f025808d865b5c6471748201`.
- Integration branch: `main` in the current dirty root worktree; preserve `AGENTS.md`,
  `CLAUDE.md`, and the untracked `assets/` directory.
- Root coordinator: `/root`, `gpt-5.6-sol/xhigh`; owns domain/ports contracts, the HIGH
  `handle_command` path, CRITICAL SQLite migration/import, daemon integration, cutover, and all
  user communication.
- No push, tag, release, package installation, or child service/credential control.
- In scope: Python golden parity for all three business Bot surfaces, Telegram adapter effects,
  Codex projection/server requests/approvals, native Rust persistence/recovery, read-only Python
  SQLite import, complete `codex-tg` CLI parity, and controlled cutover validation.
- Out of scope: Rust-only monitoring experiments as business substitutes, upstream Codex changes,
  unrelated user files, and the existing `assets/` directory.

| Lane | Agent role | Model | Effort | Selection reason | Worktree | Branch | Owned files | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| telegram-parity | bridge-worker | gpt-5.6-terra | max | Fallback because configured luna is unavailable; owns Telegram transport types, role routing, controllers, renderers, callbacks, and golden Telegram effects without touching core state or migration. | `/tmp/codex-bridge-rust-python-telegram` | `agent/rust-python-parity/telegram` | `rust/crates/telegram/src`, Telegram-focused tests/fixtures | 1 |
| codex-parity | bridge-worker | gpt-5.6-terra | max | Fallback because configured luna is unavailable; owns Codex event projection, server-request handling, queue/plan/question/approval workflows, and recovery tests behind the frozen domain/ports interfaces. | `/tmp/codex-bridge-rust-python-codex` | `agent/rust-python-parity/codex` | `rust/crates/engine/src`, `rust/crates/app-server/src`, engine/app-server tests/fixtures | 2 |
| cli-parity | bridge-worker | gpt-5.6-terra | max | Fallback because configured luna is unavailable; owns local Rust CLI command/flag/output parity and offline doctor/status/watchdog behavior without touching migration or daemon composition. | `/tmp/codex-bridge-rust-python-cli` | `agent/rust-python-parity/cli` | `rust/crates/cli/src/config.rs`, `main.rs`, `replay.rs`, `security.rs`, CLI tests | 3 |
| integration-review | bridge-reviewer | gpt-5.6-terra | max | Fallback because configured luna is unavailable; independent read-only review after integration for contract drift, migration safety, poller uniqueness, recovery races, and missing tests. | none | none | none | final |

### Shared Contracts

- Python is the behavior oracle. Fixtures compare exact text, parse mode, reply/edit/delete order,
  keyboard row matrix, callback data, command menus, Codex calls, and logical state transitions;
  only declared volatile IDs/timestamps may be normalized.
- Root freezes the `domain`/`ports` interfaces before worker edits. Every existing symbol requires
  upstream impact; HIGH/CRITICAL results must be reported before editing.
- `SqliteStore::migrate` remains root-owned. The import command creates a new native target only,
  opens Python SQLite read-only, emits count/hash reconciliation, and refuses unresolved
  connection-bound work or ambiguous dispatched prompts.
- Workers run focused Rust tests, Clippy, and scoped `detect_changes`; the root integrates one
  commit at a time, reruns affected tests, closes all children, and only then performs service or
  cutover actions.

## Active Goal Addendum: rust-91-control-parity

- Goal thread: `019fc5f6-2d0c-7f72-9dfb-8041619f4761`.
- Baseline SHA: `d72cde43311d01e02324f4ddd25a2655e33f54ec`.
- Integration branch: `main` in the current dirty root worktree; preserve `AGENTS.md`,
  `CLAUDE.md`, the existing task-contract edits, and the untracked `assets/` directory.
- Root coordinator: `/root`, `gpt-5.6-sol/xhigh`; owns architecture, HIGH/CRITICAL symbols,
  daemon integration, SQLite v5 migration, performance sampling, deployment, live Telegram
  acceptance, rollback, and user communication.
- In scope: Rust 91 Control Bot behavior parity with Python 9527 for command menus, pairing guard,
  help, sessions, topics, new, perf, callbacks, MarkdownV2 fallback, durable control state,
  schema v5, and Rust-only upgrade/rollback.
- Out of scope: Python 9527 behavior/database/service changes, credentials, remote Git state,
  releases, package installation, unrelated user files, and `assets/`.

| Lane | Agent role | Model | Effort | Selection reason | Worktree | Branch | Owned files | Required tests | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| telegram-surface | bridge-worker | gpt-5.6-terra | high | Configured luna is unavailable here; terra/high fallback isolates Bot API command scopes, MarkdownV2/plain effects, and transport-level callback/menu contracts from root-owned daemon and persistence code. | `/tmp/codex-bridge-91-telegram` | `agent/rust-91-control-parity/telegram-surface` | `rust/crates/telegram/src/lib.rs`, `rust/crates/telegram/src/controllers.rs`, Telegram unit tests and fixtures only | focused cargo test, fmt, Clippy, scoped `detect_changes` | 1 |
| control-contract | bridge-worker | gpt-5.6-terra | high | Configured luna is unavailable here; terra/high fallback builds the controller and Python/Rust golden contract in new files, keeping workflow semantics independently testable before daemon integration. | `/tmp/codex-bridge-91-control` | `agent/rust-91-control-parity/control-contract` | new `rust/crates/cli/src/control.rs`, new Rust/Python parity fixtures and focused tests only | focused cargo/Python tests, fmt, Clippy, scoped `detect_changes` | 2 |
| integration-review | bridge-reviewer | gpt-5.6-terra | high | Configured luna is unavailable here; terra/high fallback independently reviews authorization, callback races, migration safety, effect ordering, and live rollback gaps. | none | none | none | final diff and verification evidence | final |

### Shared Contracts

- Control menu scopes are default empty, all-private `/pair` and `/help`, and owner-chat full
  commands. Owner is created only by a real `/pair`; existing Rust Session rows remain untouched.
- Python 9527 is the behavior oracle. Golden fixtures compare exact text, parse mode, plain
  fallback, keyboard labels/rows, callback effects, edit/delete order, deadlines, and app-server
  calls; only declared volatile IDs, timestamps, nonces, and temporary paths are normalized.
- Schema v5 adds native control interactions, callbacks, and scheduled deletions. Revision/claim,
  one-time callback consumption, fixed deadlines, restart recovery, and v4 data preservation are
  transactional contracts. `SqliteStore::migrate` is root-only after CRITICAL impact review.
- Children must not edit services or credentials, install packages, touch other worktrees, push,
  publish, or operate on `assets/`. Root integrates one lane at a time and closes every child before
  any service action.

## Active Goal Addendum: rust-818-python-69-business-parity-v2

- Goal thread ID: `019fca52-43af-7583-92fd-df005fdbf263`.
- Baseline SHA: `b2f72d582236a63658c9760a8f6f8ffe381ec8a5`.
- Integration branch: `main` in the current dirty root worktree; preserve all pre-existing user
  edits, especially `rust/crates/cli/src/daemon.rs`, task-contract/AGENTS/CLAUDE/docs/systemd
  changes, and the untracked `assets/`, `.playwright-cli/`, and `target/` paths.
- Root coordinator: `/root`, `gpt-5.6-sol/xhigh`; owns the CRITICAL status/callback path, SQLite
  migration, daemon integration, service state, credentials, deployment, live Telegram validation,
  rollback, and user communication.
- In scope: exact Python 69 status/dashboard contract on Rust 818, 411 confirmation handoff and
  close transaction, generation/callback invalidation, scoped queued-prompt cancellation, status
  message migration and delayed cleanup, persisted thread projections, richer SessionSpace state,
  status callback records, semantic Telegram fingerprints, SQLite v6 migration/recovery, golden
  fixtures, focused/full verification, and controlled Rust upgrade/live acceptance.
- Out of scope: Python service/database mutation, upstream Codex changes, releases/remotes, package
  publication, automatic Forum conversion, unrelated user files, and the existing untracked
  `assets/` directory.

| Lane | Agent role | Model | Effort | Selection reason | Worktree | Branch | Owned files | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| python-status-audit | bridge-explorer | gpt-5.6-luna | max | Read-only oracle audit of Python 69 status text, keyboard, callback expiry, debounce, and close/error effects before Rust edits. | none | none | none | advisory before implementation |
| rust-recovery-audit | bridge-explorer | gpt-5.6-terra | high | Configured luna unavailable; fallback read-only audit of Rust SQLite schema, thread/session persistence, callback storage, delivery ownership, restart recovery, and migration hazards. | none | none | none | advisory before implementation |
| integration-review | bridge-reviewer | gpt-5.6-luna | max | Read-only final review of callback authorization, generation races, atomic close, migration/recovery, dashboard coalescing, and live rollback evidence. | none | none | none | final |

Execution note: configured `gpt-5.6-luna/max` was unavailable in this environment (the spawn
registry exposes only `gpt-5.6-sol` and `gpt-5.6-terra`). Both audit lanes therefore use the
available `bridge-explorer / gpt-5.6-terra / high` fallback; root architecture/integration remains
with the coordinator.

### Shared Contracts

- Python 69 is the behavior oracle. Status cards normally expose only `取消关注`; terminal cards
  expose no buttons. Locked writes use `写操作已锁定，请先发送 /totp <验证码>。认证后可再次点击原按钮。`.
- 818 `取消关注` hands off to 411 confirmation text `确认取消关注？评论历史会保留，但此评论串将永久只读。`
  with `确认取消关注` and `返回`; successful close edits that confirmation to
  `已取消关注。评论历史已保留，此评论串现为只读。`.
- Close atomically increments generation, marks lifecycle `closed`, invalidates old callbacks,
  cancels queued prompts, unsubscribes unused threads, updates queue state, and emits closed state.
- Status migration sends through 818, retires old callbacks, and schedules deletion of the old
  discussion-owned message after 600 seconds. Dashboard debounce is `0.5s`, heartbeat `60s`,
  status callback expiry `300s`.
- SQLite v6 must preserve v5 data and persist richer SessionSpace/ThreadProjection state,
  status-specific callbacks, and semantic Telegram fingerprints sufficient for restart deduplication.

### Integration Rules

- Every existing symbol edit requires the recorded GitNexus upstream impact; HIGH/CRITICAL results
  are warnings already reported before implementation. New pure-contract files still require tests.
- Read-only lanes must not edit files, services, credentials, packages, remotes, other worktrees, or
  `assets/`; they only return evidence and testable findings.
- Root integrates one writer change at a time, reruns focused tests after each integration, closes
  every child before `rust-vnext-full.sh upgrade` or any service operation, and runs
  `detect_changes(compare main)` before Goal completion.

## Active Goal Addendum: rust-runtime-ownership-parity-repair

- Goal thread: `019fda1d-1809-74e3-a92c-3f940e28343f`.
- Runtime baseline SHA: `2f499f4cd134f52418db7c2038945762d70f77f1`; this is the revision reported
  by the live Rust metrics endpoint and already contains the per-bot pipeline and `follow_space`.
- Integration branch/worktree: `agent/rust-parity-repair/integration` in
  `/home/linuxie26/PythonProjects/codex-telegram-bridge-worktrees/rust-parity-repair-integration`.
- Root coordinator: `/root`, `gpt-5.6-sol/max`; owns architecture, sequential integration,
  HIGH/CRITICAL risk decisions, Goal/Plan state, systemd, state backups, deployment, rollback,
  live Telegram acceptance, and all user communication.
- Preserve the current dirty `main` worktree verbatim, including agent configuration, existing
  candidate patches, `.playwright-cli/`, `target/`, and the untracked `assets/` directory.
- In scope: canonical Rust service ownership and exclusive runtime switching; active-space handoff;
  reliable TUI mode observation; recoverable error lifecycle; Python-equivalent discussion
  `/planmode`; full Rust/Python visible-business audit, remediation, validation, and live cutover.
- Out of scope: credentials, package installation, remote Git state, releases/tags, upstream Codex
  changes, destructive cleanup of previous worktrees, and unrelated user files.

| Lane | Agent role | Model | Effort | Selection reason | Worktree | Branch | Owned paths | Required gates | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| service-ownership | bridge-worker | gpt-5.6-terra | xhigh | Isolates systemd naming, lifetime lock, transactional switching/handoff, installer/watchdog migration, and rollback from Rust daemon business paths. | `/home/linuxie26/PythonProjects/codex-telegram-bridge-worktrees/rust-parity-repair-service` | `agent/rust-parity-repair/service` | `systemd/**`, `scripts/bridge-service.sh`, new service/handoff helpers under `scripts/`, `scripts/rust-vnext-full.sh`, service-related `install.sh` and Python `cli.py` edits, service/installer tests, related docs | upstream impact for edited symbols, focused pytest, Ruff for Python edits, shell/static/systemd checks, scoped detect_changes, commit | 1 |
| runtime-business | bridge-worker | gpt-5.6-terra | xhigh | Keeps projector, snapshot hydration, discussion interaction, app-server turn payload, persistence, renderers, Python oracle, and parity fixtures under one coherent owner because they converge in `daemon.rs`. | `/home/linuxie26/PythonProjects/codex-telegram-bridge-worktrees/rust-parity-repair-runtime` | `agent/rust-parity-repair/runtime` | Rust engine/app-server/storage/CLI/Telegram/component business sources and tests; Python projector/bridge/discussion/store/views oracle sources and tests; parity contracts/fixtures | upstream impact with HIGH/CRITICAL report, focused Rust/Python tests, fmt, Clippy, Ruff, scoped detect_changes, commit | 2 |
| full-parity-audit | bridge-explorer | gpt-5.6-terra | xhigh | Independent read-only comparison of all four bot roles, exact Telegram effects, Codex calls, state transitions, persistence, recovery, and service-visible behavior after the four named fixes are mapped. | none (read-only) | none | none | evidence-backed finding IDs and missing-test matrix; zero diff | advisory before remediation |
| integration-review | bridge-reviewer | gpt-5.6-sol | max | Independent final read-only review for correctness, concurrency, migration safety, runtime exclusivity, security, parity regressions, and test gaps. | none (read-only) | none | none | review integrated diff and verification/deployment evidence; zero diff | final |

### Shared Contracts

- `codex-telegram-bridge.service` is canonical Rust and boot-enabled;
  `codex-telegram-bridge-python.service` is the disabled Python fallback; the old Rust unit name is
  only an alias. Direct competing starts fail on one process-lifetime `flock`; `--force` performs a
  verified switch and restores the exact prior owner/enabled state on failure.
- TUI observed mode is evidence-based: explicit observed/settings notification, successful
  collaboration turn, or the latest exact-thread rollout `turn_context`; desired snapshot state is
  never promoted unconditionally.
- New error retryability comes only from `willRetry`; legacy missing provenance may clear a textual
  reconnect error only after explicit healthy evidence. Renderers consume normalized projection.
- `/planmode` matches Python text, syntax, keyboard rows, 5-minute selection timeout, 30-second
  first-prompt timeout, guards, durable revision/claim recovery, and one `turn/start` carrying
  `collaborationMode`.
- Python is the visible-business oracle except where this Goal's newer direct-mode and canonical-Rust
  contracts explicitly supersede it. Exact text, parse mode, keyboard geometry, callback scope,
  ordering, app-server requests, persistence, recovery, and error semantics are acceptance criteria.

### Integration Rules

- Before any existing symbol edit, the owning worker runs GitNexus upstream impact and immediately
  reports HIGH or CRITICAL results to root. Root decides whether to continue before the edit.
- Writers edit only owned paths, commit only those paths, and never operate services, credentials,
  packages, remotes, other worktrees, or `assets/`. Root cherry-picks one lane at a time and resolves
  cross-lane interfaces centrally.
- All children must be completed/closed before root touches installed units or live state. Goal
  completion requires Plan `8/8`, no active children, full validation, canonical Rust as the sole
  owner, retained rollback artifacts, and reported local integration/deployment SHAs.
