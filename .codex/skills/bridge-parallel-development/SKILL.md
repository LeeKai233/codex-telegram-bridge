---
name: bridge-parallel-development
description: Coordinate parallel Codex development in this repository with isolated worktrees, exclusive file ownership, GitNexus gates, supervised integration, and Full Access guardrails.
---

# Bridge Parallel Development

Use this workflow when a task has at least two independent implementation or audit lanes.

1. Freeze the baseline SHA and complete the task contract in `references/task-contract.md`.
2. Route every lane before spawning it. Record the selected agent role, model, effort, and a short selection reason in the task contract.
3. Use `bridge-explorer` (`gpt-5.6-terra/high`) for bounded read-heavy work, `bridge-worker` (`gpt-5.6-sol/high`) for scoped implementation, and `bridge-reviewer` (`gpt-5.6-luna/max`) for independent final review. Keep architecture, CRITICAL paths, and final integration with the root coordinator (`gpt-5.6-sol/xhigh`) unless the user overrides the routing policy.
4. The root agent owns user communication, architecture decisions, model/effort routing, integration, deployment, and remote Git state.
5. Create one `agent/<goal>/<lane>` branch and isolated worktree per writer. Never run two writers on the same file in the same phase.
6. Spawn no more than three direct subagents. Keep `agents.max_depth = 1`.
7. Assign exploration and review as no-write work. Full Access is inherited, so this is a behavioral rule rather than a sandbox boundary.
8. Before editing an existing function, class, or method, run GitNexus upstream impact. Report HIGH or CRITICAL results before editing.
9. Children must not control systemd, install packages, edit credentials, touch other worktrees, push, publish releases, or operate on unrelated user files.
10. Each writer runs focused tests, Ruff, and `detect_changes` for its worktree before committing.
11. The root agent integrates one lane at a time, resolves contracts centrally, reruns affected tests, and uses a separate reviewer after integration.
12. Close all children before the root agent performs local installation, service restart, or release work.

Prefer read-heavy parallelism first. Parallel writes are allowed only when the task contract proves disjoint file ownership.
