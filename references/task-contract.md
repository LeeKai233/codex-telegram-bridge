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
