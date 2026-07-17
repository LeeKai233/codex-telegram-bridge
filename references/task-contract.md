# Parallel Task Contract

- Goal slug: `telegram-bridge-followups`
- Baseline SHA: `dc4607a2c7e8ae2053fcc62d55503e9cf20f8cb6`
- Integration branch: `main`
- Root coordinator: root agent, `gpt-5.6-luna/max`
- Effective permission mode: workspace write with read-only `.git`
- Execution policy: subagent distribution is disabled because the primary model is `gpt-5.6-luna/max`; the root agent owns discovery, implementation, review, deployment, and reporting.
- In-scope behavior: 300-second `/ask` timeout, Telegram command approval in non-full-access requests, balanced three-column model/effort keyboards, current Plan approval callback reliability and diagnostics, tests, docs, deployment, and live verification.
- Out-of-scope behavior: `assets/`, credentials, remote pushes/releases, unrelated refactors, and changes to native Telegram comment transport.

| Lane | Agent role | Model | Effort | Selection reason | Worktree | Branch | Owned files | Required tests | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| root-serial | root coordinator | gpt-5.6-luna | max | Primary model rule forbids subagent distribution; root retains one coherent state-machine implementation | main checkout | `main` | `AGENTS.md`, `references/task-contract.md`, `src/codex_telegram_bridge/codex.py`, `src/codex_telegram_bridge/bridge.py`, `src/codex_telegram_bridge/store.py`, `src/codex_telegram_bridge/telegram_common.py`, `src/codex_telegram_bridge/control_bot.py`, `src/codex_telegram_bridge/discussion_bot.py`, affected tests, `README.md` | Focused tests, full `uv run pytest -q`, Ruff, `git diff --check`, GitNexus `detect_changes` | 1 |

## Root-Owned Shared Contract

- `CodexClient.ask_fork_question` defaults to `300.0` seconds; `/ask` timeout text reports 300 seconds.
- Command approvals reuse the durable `pending_inputs` and `question_messages` envelope with a request-kind marker; no separate approval table or schema version is introduced.
- Modern command approval decisions are `accept`, `acceptForSession`, and `decline`; legacy `execCommandApproval` decisions are mapped separately if received.
- Approval buttons are owner-bound, Session/generation-bound, TOTP-protected, one-time, and recoverable after service restart.
- Shared model/effort keyboard rows contain at most three buttons; a remainder of one is balanced into the final two rows (`4 -> 2+2`, `7 -> 3+2+2`).
- Plan callback handling uses the callback message chat as the authoritative chat ID, logs arrival/rejection/failure reasons, and preserves latest-publication and exactly-once execution checks.
- Existing dirty files and untracked `assets/` remain user-owned and must be preserved.

## Shared Rules

- Run GitNexus upstream impact before every existing symbol edit and report HIGH or CRITICAL blast radius before proceeding.
- Run focused tests and Ruff after each logical implementation slice; run `detect_changes` before any commit.
- Do not control systemd, install packages, edit credentials, touch other worktrees, push, publish releases, or operate on `assets/` outside the requested live verification.
- Root alone performs integration, SQLite backup/integrity checks, service restart, live Telegram verification, and final reporting.
