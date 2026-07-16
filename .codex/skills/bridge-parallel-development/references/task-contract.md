# Parallel Task Contract

- Goal slug:
- Baseline SHA:
- Integration branch:
- Root coordinator:
- Effective permission mode:
- In-scope behavior:
- Out-of-scope behavior:

| Lane | Agent role | Model | Effort | Selection reason | Worktree | Branch | Owned files | Inputs/contracts | Required tests | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | | | | | | | |

## Shared Rules

- One writer per file per phase.
- The coordinator selects and records every lane's role, model, effort, and routing reason before spawn; implicit inheritance is not a routing decision.
- Default routing is explorer=`gpt-5.6-terra/high`, worker=`gpt-5.6-sol/high`, reviewer=`gpt-5.6-luna/max`, root=`gpt-5.6-sol/xhigh`.
- Existing symbols require GitNexus impact before edits.
- HIGH or CRITICAL impact must be reported before editing.
- No child may modify services, credentials, installed packages, other worktrees, user assets, releases, or remotes.
- Full Access inheritance is assumed; read-only roles are enforced by instructions and verified with Git diff.
- Every writer returns a commit hash, focused test results, Ruff results, detect_changes scope, and residual risks.
- The coordinator alone integrates, runs full regression, deploys, and reports completion.
