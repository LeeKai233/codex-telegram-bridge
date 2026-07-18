# Codex Telegram Bridge

Codex Telegram Bridge connects Codex sessions running in WSL2 to a private Telegram channel and
its linked discussion group. It uses two Telegram Bots and outbound long polling, so it does not
open a public webhook or network port.

```text
Control Bot private chat
  -> session post in your private channel
  -> Telegram's Leave a comment link
  -> fixed thread in the linked private discussion group
  -> Discussion Bot forwards messages to the matching Codex session
```

The linked discussion group must have Forum Topics disabled. The comments under each channel post,
not Telegram Forum Topics, are the stable session boundary.

## What it provides

- Browse, search, follow, and create Codex sessions from the Control Bot.
- Maintain one channel post and one live status comment per followed session without message spam.
- Show goals, plans, tasks, queue depth, elapsed time, and recent activity.
- Send prompts, steer a running turn, queue work, and transfer approved files from a session thread.
- Open a tmux window attached to the same Codex session.
- Gate Telegram write actions with TOTP and recovery codes.
- Validate the owner, private channel, linked discussion group, Bot identities, and Bot permissions.

## Supported host

The v0.2.5 installer intentionally has a narrow host contract:

- Ubuntu or Debian under WSL2.
- systemd enabled in WSL and available as PID 1.
- A normal non-root user with `sudo` access.
- A real interactive terminal.
- tmux. The installer adds it with apt when it is missing.
- Two different Telegram Bots, one private channel, and one linked private supergroup.

WSL1, Linux without systemd, Docker, macOS, native Windows, and system-wide/root installs are not
supported by this installer.

## Telegram preparation

Before installing:

1. Create two Bots with [BotFather](https://t.me/BotFather) and keep both tokens ready.
2. Create a private channel and a private supergroup.
3. Link the supergroup as the channel's discussion group.
4. Disable Forum Topics in the discussion group.
5. Add the Control Bot to the channel with only the permissions needed to post and edit messages.
6. Add the Discussion Bot to the discussion group with at least permission to delete messages.
7. Disable anonymous-admin mode for the Telegram account that will own the Bridge.

The `/bind` flow checks the linkage, Bot roles, and required permissions. It warns when a Bot has
unnecessary administrator privileges.

## One-line installation

Run the version-pinned installer from a WSL terminal:

```bash
bash -c 'set -Eeuo pipefail; url="https://github.com/LeeKai233/codex-telegram-bridge/releases/download/v0.2.5/install.sh"; sha256="319752820e12dadfd36eb59d898057731b718330eee6c21fb9e55df8009e3eb2"; installer="$(mktemp)"; cleanup() { rm -f -- "$installer"; }; trap cleanup EXIT; curl --proto "=https" --tlsv1.2 -fsSL --retry 3 --retry-all-errors -o "$installer" "$url"; printf "%s  %s\n" "$sha256" "$installer" | sha256sum -c -; bash "$installer"'
```

The installer:

1. Checks WSL2, systemd, the user manager, current-boot swap I/O errors, Windows swap-disk free
   space, and network access.
2. Installs missing `ca-certificates`, `curl`, `git`, and `tmux` packages.
3. Installs pinned uv 0.11.28 without editing shell profiles, then installs uv-managed Python 3.14.
4. Installs Bridge v0.2.5 into `~/.local/bin` from the matching immutable Git tag.
5. Installs the latest official standalone Codex release into `~/.local/bin` and verifies the
   required `app-server --listen unix://` and `--remote unix://` capabilities.
6. Pauses while you configure and authenticate Codex in another WSL terminal.
7. Reads both Bot tokens with hidden terminal prompts, validates them with Telegram `getMe`, and
   stores them in private `0600` files. Tokens are never command-line arguments or installer logs.
8. Lets you choose user-facing names for the two Bots, defaulting to their Telegram usernames.
9. Installs and starts two systemd user services, then guides TOTP, `/pair`, and `/bind` onboarding.

The installer enables user lingering so both services return after WSL/systemd restarts. It does
not copy another machine's Codex configuration, authentication, skills, MCP servers, hooks, Bridge
database, or Telegram credentials.

The bootstrap downloads the immutable `v0.2.5` release asset to a temporary file, verifies the
SHA-256 shown above, and only then executes it with terminal input still attached. The installer
also pins and verifies the official uv and Codex installer-script contents. Codex itself is
intentionally selected as the latest official standalone release at installation time; its
installer verifies the selected release artifact, and this installer rejects a CLI that lacks the
required Unix-socket capabilities. A future OpenAI installer-script change fails closed until a new
Bridge installer records and tests its checksum.

### Installer options

```text
--version                    print the installer version
--check-only                 run read-only host checks and exit
--skip-onboarding            install/start services and defer TOTP, /pair, and /bind
--reuse-existing-app-server  reuse an existing private, reachable Codex Unix socket
```

Use `--reuse-existing-app-server` only when the socket at
`~/.codex/app-server-control/app-server-control.sock` is already intentionally managed. Without
that flag, an existing socket or unowned service name is treated as a collision and the installer
stops. The Bridge service is never allowed to overwrite an unowned service with the same name.

Re-running the installer updates the installed tools and installer-owned units, but refuses to
downgrade units written by a newer installer and preserves
existing `config.toml`, Bot tokens, TOTP secret, SQLite state, proxy file, custom Bot names, and
Telegram bindings. A partial credential state with only one token file is rejected for manual
review instead of overwriting the remaining credential. If both credentials exist but
`config.toml` does not, the recovered configuration prompts with the generic `Control Bot` and
`Discussion Bot` label defaults because it does not replace the credentials just to rediscover
their usernames.

The v0.2 installer owns the standard paths under `~/.config/codex-telegram-bridge`,
`~/.local/state/codex-telegram-bridge`, `~/.local/bin`, and `~/.codex`. An existing `config.toml`
may customize labels and behavior, but overrides for `config_dir`, `state_dir`, `codex_home`,
`codex_socket`, or `codex_binary` must match those standard paths. Unsupported path overrides are
rejected before Bot tokens are requested.

To rotate either Bot token after pairing, revoke the old Telegram authority first:

```bash
codex-tg owner-reset
codex-tg configure-tokens --prompt --force
systemctl --user restart codex-telegram-bridge.service
codex-tg onboard
```

`owner-reset` also closes old Bot-owned SessionSpaces and cancels queued authority, preventing a new
Bot identity from inheriting stale channel bindings.

## WSL swap preflight

The installer fails before making installation changes when the current boot contains swap-device
I/O errors. It resolves either the default C: location or the local Windows drive configured by
`swapFile`, verifies that drive is mounted into WSL, and checks its actual free space. Less than
10 GiB free is an error and 10-20 GiB is a warning. The installer reports the mounted Windows drive
with the most free space, but never edits Windows configuration or shuts WSL down.

To relocate swap manually, edit `C:\Users\<Windows-user>\.wslconfig` in Windows, for example:

```ini
[wsl2]
swap=2GB
swapFile=G:\\wsl-swap.vhdx
```

Then run this in Windows PowerShell, not inside WSL:

```powershell
wsl --shutdown
```

Only remove an old `swap.vhdx` after every WSL distribution is stopped and the new swap file is
confirmed active after restart.

## Onboarding

The normal installer starts both services before running:

```bash
codex-tg onboard --timeout 600
```

The command is resumable. It skips completed stages and guides you through:

1. Enrolling TOTP and saving one-time recovery codes.
2. Sending `/pair <code>` to the Control Bot in a private chat.
3. Sending `/bind <code>` to the Discussion Bot in the linked discussion group.
4. Running the online diagnostic checks.

If onboarding was skipped or interrupted, run that command again. Each Telegram wait stage has a
10-minute timeout by default.

## Services and diagnostics

```bash
systemctl --user status codex-telegram-app-server.service
systemctl --user status codex-telegram-bridge.service
journalctl --user -u codex-telegram-bridge.service -f
codex-tg status
codex-tg doctor
codex-tg doctor --offline
```

The App Server unit fixes `CODEX_HOME` to the installer-managed path and runs the official command:

```bash
CODEX_HOME="$HOME/.codex" codex app-server --listen unix://
```

The socket and Bridge credentials are private to the current WSL user. The Bridge unit uses
`KillMode=process` so restarting it does not kill a tmux server that now owns attached Codex panes.

If Telegram or Codex needs an HTTP proxy, create this optional file with mode `0600`:

```text
~/.config/codex-telegram-bridge/proxy.env
```

Both installer-managed services load it. Use it for proxy variables only; both service commands
force `CODEX_HOME=$HOME/.codex` so a manager or proxy-file override cannot move the expected socket.
Keep secrets out of the unit files themselves.

## Commands

Control Bot private chat:

```text
/pair <code>                 pair the single owner
/sessions [search]           browse recent Codex sessions
/topics                      show followed session posts
/new                         choose normal/Plan profiles, project, and first prompt interactively
/new <model> | <effort> [ | noplan | <cwd> [ | <prompt> ] ]
/new <model> | <effort> | planmode | <plan-model> | <plan-effort> [ | <cwd> [ | <prompt> ] ]
/perf                        update WSL/Codex resource usage every ~1s for 30s
/help                        show context-appropriate help
```

Discussion Bot session comments:

```text
/bind <code>                 bind the linked channel and discussion group
/status                      refresh session status
/totp <code>                 unlock this thread for 30 minutes
/lock                        revoke the current unlock
/prompt <text>               send a prompt to the fixed session
/queue [text]                view or append to the FIFO prompt queue
/ask <question>              ask through an isolated temporary fork
/planmode                    choose a Plan profile and prompt interactively
/planmode <model> | <effort> [ | <prompt> ]
/changemodel                 choose the current mode's model and effort interactively
/changemodel <model> | <effort>
/plan                        show the complete plan
/timeline                    show recent structured events
/attach                      create or find the matching tmux window
/getfile <description>       resolve and confirm a local outbound file
/unwatch                     permanently freeze this session thread after confirmation
/help                        show context-appropriate help
```

Session commands accept no UUID argument. The owner, Bot role, channel comment root, callback nonce,
and persisted SessionSpace generation determine the route.

## Configuration

The installer creates this file only when it does not already exist:

```text
~/.config/codex-telegram-bridge/config.toml
```

Example:

```toml
[bridge]
allowed_root = "/home/your-user"
tmux_session = "CodexBot"
control_bot_label = "@my_control_bot"
discussion_bot_label = "@my_discussion_bot"
ask_model = "gpt-5.6-luna"
ask_reasoning_effort = "medium"
dashboard_debounce_seconds = 0.5
heartbeat_seconds = 60
totp_unlock_seconds = 1800
disconnect_threshold_seconds = 30
```

`ask_model` and `ask_reasoning_effort` select the read-only utility profile used by both `/ask`
and `/getfile`. They default to `gpt-5.6-luna` and `medium`; neither command changes the parent
Session model or effort.

Dashboard Braille frames advance with each real status refresh. The bridge does not issue
animation-only Telegram edits, so active work remains visually dynamic without consuming the
channel/group flood-control budget while a session is idle.

State and inbox files live under `~/.local/state/codex-telegram-bridge/`. Private directories use
mode `0700`; tokens, TOTP, and SQLite files use mode `0600`. Database schema upgrades create an
online SQLite backup first.

Credential writes use a private transaction lock, descriptor-relative atomic commits, inode/content
verification, and rollback. Two concurrent first-time configurations cannot mix or overwrite each
other's Bot tokens.

`allowed_root` limits Bridge file and working-directory operations. It is not a confidentiality
sandbox for Codex itself.

## Security model

- Both Bots use outbound Telegram HTTPS long polling; there is no webhook listener.
- The Control Bot accepts one private-chat owner. The Discussion Bot accepts that owner only in the
  validated private discussion group and mapped channel comment threads.
- TOTP leases are per session and process-local; a restart locks all session writes again.
- Sensitive Codex questions are not forwarded to Telegram. Command execution approvals for
  non-full-access workspace turns are forwarded to the owner in the Session discussion and can
  be approved once, approved for the Session, or declined with one-use TOTP-protected buttons.
- New and queued workspace turns use `on-request` approval policy; read-only `/ask` forks keep
  approvals disabled. Steering inherits the active local turn's permissions, so use Queue when
  that inheritance is undesirable.
- Token values are hidden during entry, stored in private files, and redacted from Bridge logs.
- Callback actions bind Bot role, chat, owner, session generation, and a short-lived one-use nonce.

Enable Telegram two-step verification and grant each Bot only the permissions reported by `/bind`.

## Development install

For contributors working from a checkout:

```bash
uv sync --dev
uv run ruff check .
uv run pytest
bash -n install.sh
uvx --from shellcheck-py shellcheck install.sh
git diff --check
```

Run the checkout without installing the public tool:

```bash
uv run codex-tg configure-tokens --prompt
uv run codex-telegram-bridge
```

The project requires Python 3.14 and locks runtime dependencies in `uv.lock`.

## License

[MIT](LICENSE), copyright 2026 LeeKai233.
