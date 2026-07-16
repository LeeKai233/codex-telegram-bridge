---
name: bridge-parallel-development
description: Coordinate parallel Codex development in this repository with isolated worktrees, exclusive file ownership, GitNexus gates, supervised integration, and Full Access guardrails.
---

# Bridge Parallel Development

Use this workflow when a task has at least two independent implementation or audit lanes.

1. Freeze the baseline SHA and complete the task contract in `references/task-contract.md`.
2. The root agent owns user communication, architecture decisions, integration, deployment, and remote Git state.
3. Create one `agent/<goal>/<lane>` branch and isolated worktree per writer. Never run two writers on the same file in the same phase.
4. Spawn no more than three direct subagents. Keep `agents.max_depth = 1`.
5. Assign exploration and review as no-write work. Full Access is inherited, so this is a behavioral rule rather than a sandbox boundary.
6. Before editing an existing function, class, or method, run GitNexus upstream impact. Report HIGH or CRITICAL results before editing.
7. Children must not control systemd, install packages, edit credentials, touch other worktrees, push, publish releases, or operate on unrelated user files.
8. Each writer runs focused tests, Ruff, and `detect_changes` for its worktree before committing.
9. The root agent integrates one lane at a time, resolves contracts centrally, reruns affected tests, and uses a separate reviewer after integration.
10. Close all children before the root agent performs local installation, service restart, or release work.

Prefer read-heavy parallelism first. Parallel writes are allowed only when the task contract proves disjoint file ownership.

