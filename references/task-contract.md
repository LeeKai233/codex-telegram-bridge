# Parallel Task Contract

- Goal slug: `telegram-plan-terminal-sync`
- Baseline SHA: `283d86aaa45d0050e29dd4b2b96a3530b08fdcab`
- Integration branch: `main`
- Root coordinator: current root Codex agent, architecture and deployment owner
- Effective permission mode: full workspace access
- Execution policy: two read-only explorers audit independent lanes; the root owns every edit, integration decision, migration, test run, live database backup, and service restart.

## Scope

- Make control-bot `/new` model and effort keyboards use the same unpadded row construction as discussion `/changemodel` and `/planmode`.
- Reconcile real TUI Plan approval from live `item/started` notifications rather than empty `turn/started` snapshots.
- Update the original Telegram Plan article and remove its actions after Telegram or TUI approval, Telegram revision choice, or supersession.
- Delete every Telegram chunk of a Plan when the observed TUI `Implement this plan?` prompt disappears without an approval event; No and Escape intentionally share this behavior.
- Repair stale Plan actions and messages after restart while preserving existing dirty changes and the user-owned untracked `assets/` directory.

## Routing

| Lane | Agent role | Model | Effort | Selection reason | Workspace | Write ownership |
| --- | --- | --- | --- | --- | --- | --- |
| keyboard-audit | `bridge-explorer` | `gpt-5.6-terra` | high | Read-only comparison of control `/new` and discussion profile keyboard construction plus focused tests | isolated read-only agent workspace | none |
| plan-lifecycle-audit | `bridge-explorer` | `gpt-5.6-terra` | high | Read-only audit of live event ordering, Store migration, tmux prompt observation, races, and startup repair | isolated read-only agent workspace | none |
| integration-review | `bridge-reviewer` | `gpt-5.6-luna` | max | Independent final regression and migration review after root integration | isolated read-only agent workspace | none |
| root-integration | root coordinator | platform-managed frontier model | current | The dirty shared state, schema migration, Telegram message edits, monitor lifecycle, and deployment require central ownership | main checkout | all in-scope source, tests, config, and this contract |

## Invariants

- Explorers and reviewer do not edit files, control systemd, install packages, publish, or touch other workspaces.
- `/new` keeps model rows at two columns, effort rows at three columns, and Exit as a single final row; callback payloads and interaction guards remain unchanged.
- TUI approval requires a `userMessage` item with `clientId=null` and exact text `Implement the plan.`; unrelated TUI and Telegram turns cannot consume Plan actions.
- TUI No/Escape deletion begins only after the bridge has observed the matching tmux prompt; missing windows and capture errors fail open and keep Telegram actions.
- Telegram Plan updates preserve all rendered Plan content, add one terminal status to the final chunk, and clear the keyboard in the same edit.
- Multi-chunk TUI No/Escape removes every Plan chunk. New Plan revisions retire and visually close older revision actions.
- Schema migration is backup-first, keeps file permissions private, and supports restart repair of existing v7 data.
- All monitor tasks are generation- and revision-scoped, race-safe against TUI approval, and cancelled during controller shutdown.
- Final verification includes focused and full pytest, Ruff, `git diff --check`, `uv build`, archive inspection, GitNexus `detect_changes`, independent review, live SQLite backup/integrity check, and service health checks.

## v0.2.5 Release Addendum

- Release goal: publish the integrated Telegram workflow repair as immutable public `v0.2.5`.
- Release baseline: `283d86aaa45d0050e29dd4b2b96a3530b08fdcab` on `main`.
- Remote baseline: `origin/main` at public `v0.2.0`; the two local commits after it and the reviewed working-tree changes are intentionally included.
- Root coordinator owns version edits, commits, push, CI verification, tag creation, GitHub Release publication, and public smoke testing.

| Lane | Agent role | Model | Effort | Selection reason | Write ownership |
| --- | --- | --- | --- | --- | --- |
| release-audit | `bridge-explorer` | `gpt-5.6-terra` | high | Read-only audit of version surfaces, installer/docs contract, archive membership, and public smoke-test commands | none |
| release-review | `bridge-reviewer` | `gpt-5.6-luna` | max | Independent findings-first review of the final staged release diff before push/tag | none |
| release-integration | root coordinator | platform-managed frontier model | current | The exact-SHA commit, CI, tag, immutable release, and public state changes must remain serial and centrally owned | all in-scope release files and remote operations |

Release invariants:

- Keep untracked `assets/`, ignored `.release/` history, pytest basetemp output, credentials, live state, and generated GitNexus stats out of commits and release archives.
- Build wheel and sdist from the final release commit; both must include `approval.py` and exclude deleted `bot.py`.
- Split the integrated functionality and `v0.2.5` metadata into reviewable commits, then pin push, CI, tag, Release, assets, checksums, and smoke tests to one exact final SHA.
- Do not publish the GitHub Release until local gates and GitHub Actions pass for that exact SHA.

# Parallel Task Contract — rust-alignment-20260809

- Goal slug: `rust-alignment-20260809`
- Baseline SHA: `cfef051041392ba983d648395944bc42ecdaa2eb`
- Integration branch: `main`
- Root coordinator: current root agent (Kimi Code CLI), architecture and deployment owner
- Effective permission mode: full workspace access
- Execution policy: at most two concurrent writers in Wave 1 (disjoint file ownership), one writer in Wave 2 after Wave-1 integration; root owns integration, deployment, live config/state edits, and service control.

## Scope

- W1: hydrate running/legacy Codex threads into Rust projections at startup (resync equivalent), extend projector event coverage (turn/plan/updated, subagent item derivation, goal/get), fix python->rust threads migration, and align status/channel message rendering with the frozen Python oracle (moon-phase frames, subagents detail, cumulative duration, MarkdownV2 dual-track, space-level mode/profile persistence).
- W2 (Wave 2): dispatcher concurrency, /perf ticker absolute scheduling + fault tolerance, /sessions cache + resilient refresh, app-server request deadline, deletion worker backoff, projection memory truncation, upgrade script speedups.
- W3: /ask uses configured gpt-5.6-terra/medium in Rust; all remaining gpt-5.6-luna defaults/mocks/docs move to gpt-5.6-terra (low where a default effort is needed); CI paths filtering plus a Rust CI job.

## Routing

| Lane | Agent role | Model | Effort | Selection reason | Workspace | Write ownership |
| --- | --- | --- | --- | --- | --- | --- |
| parity | `bridge-worker` | kimi primary (platform-managed) | default | Largest write lane: hydration, projector, rendering parity, /ask wiring | worktree `agent/rust-alignment-20260809/parity` | rust/crates/engine/**, rust/crates/storage-sqlite/**, rust/crates/app-server/**, rust/crates/cli/src/{daemon.rs,config.rs,migration.rs} |
| models-ci | `bridge-worker` | kimi primary (platform-managed) | default | Disjoint docs/fixture/CI lane parallel to parity | worktree `agent/rust-alignment-20260809/models-ci` | .github/**, README.md, fixtures/control_contract/9527.json, tests/**, docs/rust-vnext/full-testing.config.example.toml |
| performance | `bridge-worker` | kimi primary (platform-managed) | default | Wave 2 writer on daemon.rs hot paths after parity merges; avoids same-file contention | worktree `agent/rust-alignment-20260809/performance` | rust/crates/cli/src/{daemon.rs,perf.rs,control.rs}, rust/crates/telegram/**, rust/crates/app-server/**, scripts/rust-vnext-full.sh, rust/Cargo.toml, systemd/** |
| integration-review | `bridge-reviewer` | kimi primary (platform-managed) | default | Independent read-only review of the integrated diff before deploy | isolated read-only | none |
| root-integration | root coordinator | kimi primary (platform-managed) | default | Baseline, task contract, integration, live config/state, deployment, service control | main checkout | all integration surfaces, references/task-contract.md, live config.toml |

## Invariants

- Workers never touch systemd, credentials, live state, other worktrees, or remote Git; root alone deploys.
- `fixtures/status_contract/818.json` and the button/debounce/heartbeat contract in `fixtures/control_contract/9527.json` stay byte-stable; only model catalog names inside 9527.json may be renamed luna->terra with both pytest and cargo fixture consumers kept green.
- Python `src/` stays frozen; the frozen Python suite remains the rendering oracle (`tests/test_views.py` semantics) and is not edited.
- Live `~/.config/codex-telegram-bridge/config.toml` is edited only by root at deploy time (ask_model -> gpt-5.6-terra, effort medium).
- `gpt-5.6-terra` effort support for low/medium must be verified against live `model/list` before final rollout; a mismatch is reported to the user instead of silently substituting.
- Final verification: cargo fmt/clippy/test, scoped pytest, contract fixtures green, GitNexus detect_changes, independent review, live SQLite backup, timed upgrade, service health checks.
