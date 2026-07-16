# Parallel Task Contract

- Goal slug:
- Baseline SHA:
- Integration branch:
- Root coordinator:
- Effective permission mode:
- In-scope behavior:
- Out-of-scope behavior:

| Lane | Agent role | Worktree | Branch | Owned files | Inputs/contracts | Required tests | Integration order |
| --- | --- | --- | --- | --- | --- | --- | --- |
| | | | | | | | |

## Shared Rules

- One writer per file per phase.
- Existing symbols require GitNexus impact before edits.
- HIGH or CRITICAL impact must be reported before editing.
- No child may modify services, credentials, installed packages, other worktrees, user assets, releases, or remotes.
- Full Access inheritance is assumed; read-only roles are enforced by instructions and verified with Git diff.
- Every writer returns a commit hash, focused test results, Ruff results, detect_changes scope, and residual risks.
- The coordinator alone integrates, runs full regression, deploys, and reports completion.

