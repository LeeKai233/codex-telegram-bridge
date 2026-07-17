<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **codex-telegram-bridge** (3433 symbols, 7764 relationships, 265 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> Index stale? Run `node .gitnexus/run.cjs analyze` from the project root — it auto-selects an available runner. No `.gitnexus/run.cjs` yet? `npx gitnexus analyze` (npm 11 crash → `npm i -g gitnexus`; #1939).

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows. For regression review, compare against the default branch: `detect_changes({scope: "compare", base_ref: "main"})`.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `query({search_query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `context({name: "symbolName"})`.
- For security review, `explain({target: "fileOrSymbol"})` lists taint findings (source→sink flows; needs `analyze --pdg`).

## Never Do

- NEVER edit a function, class, or method without first running `impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `rename` which understands the call graph.
- NEVER commit changes without running `detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/codex-telegram-bridge/context` | Codebase overview, check index freshness |
| `gitnexus://repo/codex-telegram-bridge/clusters` | All functional areas |
| `gitnexus://repo/codex-telegram-bridge/processes` | All execution flows |
| `gitnexus://repo/codex-telegram-bridge/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->

# Multi-Agent Development

- For tasks with independent lanes, use the project `bridge-parallel-development` skill and keep the root agent as the only coordinator.
- The root may run at most three direct subagents. Subagents must not spawn descendants.
- Freeze a baseline SHA, use one isolated worktree and `agent/<goal>/<lane>` branch per writer, and assign exclusive file ownership before edits.
- The user communicates with the root agent. Subagents are inspected or redirected through the root instead of being managed independently.
- The active permission mode is inherited by subagents. Under Full Access, treat every child as Full Access even when its role is described as read-only.
- Read-only exploration and review are behavioral contracts verified by Git diff; they are not security boundaries.
- Subagents must not control systemd, install packages, edit credentials, touch other worktrees, push, publish releases, or modify unrelated user files.
- The root agent alone integrates commits, resolves cross-lane contracts, runs final review, performs deployment, and reports results.
- If the primary model is `gpt-5.6-luna` with `max` effort, do not spawn or distribute any subagent; the root agent handles all lanes directly.
- Before completing a Goal, the root must mark every Plan item `completed` and verify the visible counter is `N/N`. A complete Goal with pending or in-progress Plan items is an invalid final state.
- Before completing a Goal, the root must confirm every subagent is completed, interrupted, or closed and that no child remains active.
- Before spawning any lane, the root must select and record its agent role, model, effort, and routing reason. Do not treat implicit parent inheritance as a routing decision.
- Default routing is `bridge-explorer`=`gpt-5.6-terra/high`, `bridge-worker`=`gpt-5.6-sol/high`, and `bridge-reviewer`=`gpt-5.6-luna/max`; keep architecture, CRITICAL paths, and final integration with the root at `gpt-5.6-sol/xhigh` unless the user overrides it.
- Keep the existing untracked `assets/` directory outside agent scope unless the user explicitly assigns it.
