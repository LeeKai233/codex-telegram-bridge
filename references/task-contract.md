# Parallel Task Contract

- Goal slug: `telegram-mode-upgrade`
- Baseline SHA: `8266e3642244670abf4620c30f9e8f241be2152c`
- Integration branch: `main`
- Root coordinator: root agent, `gpt-5.6-sol/xhigh`
- Effective permission mode: Full Access; child restrictions remain behavioral contracts
- In-scope behavior: dynamic `/perf`; interactive and parameterized `/new`; `/planmode`; `/changemodel`; explicit normal/plan profiles; Plan approval recovery; project-directory confirmation; status animation with main/subagent model and effort; tests, migration, deployment, and live recovery
- Out-of-scope behavior: `assets/`, credentials, remote pushes/releases, unrelated refactors, and changes to existing channel native-comment behavior

| Lane | Agent role | Model | Effort | Selection reason | Worktree | Branch | Owned files | Inputs/contracts | Required tests | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| control | bridge-worker | gpt-5.6-luna | max | User override: materially better writer quality at nearly the same cost as terra/high | `/tmp/codex-telegram-bridge-agent-control` | `agent/telegram-mode-upgrade/control` | `src/codex_telegram_bridge/control_bot.py`, `src/codex_telegram_bridge/metrics.py`, `tests/test_metrics.py`, new `tests/test_control_workflows.py` only | Root-owned APIs: model catalog, interaction drafts, profile-aware pending creation, safe directory preparation/creation | Focused owned tests, Ruff on owned files, `detect_changes` | 2 |
| discussion | bridge-worker | gpt-5.6-luna | max | User override: state-machine and callback work benefits from luna/max at similar cost | `/tmp/codex-telegram-bridge-agent-discussion` | `agent/telegram-mode-upgrade/discussion` | `src/codex_telegram_bridge/discussion_bot.py`, `src/codex_telegram_bridge/space_dashboard.py`, `tests/test_discussion_bot.py`, `tests/test_dashboard.py`, new `tests/test_discussion_commands.py` only | Root-owned APIs: profile-aware collaboration starts/settings updates, durable intents, recoverable Plan action state | Focused owned tests, Ruff on owned files, `detect_changes` | 3 |
| final-review | bridge-reviewer | gpt-5.6-luna | max | Cross-lane state-machine and live-recovery review merits upgraded review depth | read-only integrated checkout | none | No writes | Integrated diff, tests, GitNexus change report, live recovery procedure | Read-only findings report | 5 |

## Root-Owned Shared Contract

- `CodexClient` exposes validated paginated `model/list`, effective `thread/resume` settings, `thread/settings/update`, and explicit collaboration-mode payload construction.
- `SessionSpace` persists normal/plan profiles and current mode without discarding them after activation.
- Durable interaction drafts are scoped by bot role, chat, user, optional space/generation, flow ID, revision, phase, payload, and expiry; text/button/timeout completion uses an atomic claim.
- Plan execution validates and builds its profile before action claim; post-claim RPC errors reconcile by deterministic client message ID before retry is allowed.
- Existing `executing` Plan rows are reconciled on startup: delivered stays claimed, definitely absent returns to `published` and receives fresh buttons, unknown remains blocked.
- Existing dirty changes in `AGENTS.md`, `bridge.py`, `projector.py`, `views.py`, `tests/test_projector.py`, and `tests/test_views.py` are user-owned and must be preserved.

## Shared Rules

- One writer per file per phase; children must not edit root-owned or other-lane files.
- Every writer runs GitNexus upstream impact before editing each existing symbol. HIGH or CRITICAL results stop the lane and are reported to root before edits.
- Children must not spawn descendants, control services, install packages, edit credentials, touch other worktrees, push, publish releases, or operate on `assets/`.
- Each writer returns a commit hash, focused test results, Ruff results, `detect_changes` scope, and residual risks.
- Root alone integrates, resolves contracts, runs full regression, performs SQLite backup/integrity checks, deploys, repairs live Plan state, and reports completion.
