from __future__ import annotations

import json
import os
import sqlite3
import stat
from concurrent.futures import ThreadPoolExecutor
from io import StringIO
from pathlib import Path
from types import SimpleNamespace

import pytest

import codex_telegram_bridge.cli as cli
from codex_telegram_bridge.cli import (
    CliError,
    _app_server_watchdog,
    _assignment_value,
    _install_token_files,
    _read_bashrc_token,
    _status,
    _validate_telegram_token,
    build_parser,
    configure_prompt_tokens,
    migrate_bashrc_token,
    migrate_bashrc_tokens,
    onboard,
    run,
)
from codex_telegram_bridge.config import AppServerMode, Config
from codex_telegram_bridge.models import Owner
from codex_telegram_bridge.security import Enrollment, SecurityManager
from codex_telegram_bridge.store import SCHEMA_VERSION, Store

TOKEN = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcd1234"
FORUM_TOKEN = "987654321:ZYXWVUTSRQPONMLKJIHGFEDCBA_1234567"
STATUS_TOKEN = "696969696:ABCDEFGHIJKLMNOPQRSTUVWXYZ_status_bot_"
NEW_TOKEN = "333333333:ABCDEFGHIJKLMNOPQRSTUVWXYZ_new_control"
NEW_FORUM_TOKEN = "444444444:ABCDEFGHIJKLMNOPQRSTUVWXYZ_new_forum__"
NEW_STATUS_TOKEN = "555555555:ABCDEFGHIJKLMNOPQRSTUVWXYZ_new_status_"


def token_identity(value: str) -> str:
    return {
        TOKEN: "control_bot",
        FORUM_TOKEN: "forum_bot",
        STATUS_TOKEN: "status_bot",
    }.get(value, f"bot_{value.partition(':')[0]}")


def make_config(root: Path) -> Config:
    config_dir = root / "config"
    state_dir = root / "state"
    codex_home = root / ".codex"
    return Config(
        config_dir=config_dir,
        state_dir=state_dir,
        codex_home=codex_home,
        codex_socket=codex_home / "control.sock",
        codex_binary=root / "codex",
        allowed_root=root,
    )


@pytest.mark.parametrize(
    "line",
    [
        f"export TELEGRAM_GPT_BOT_TOKEN={TOKEN}\n",
        f" TELEGRAM_GPT_BOT_TOKEN = '{TOKEN}' # bot\n",
        f'export TELEGRAM_GPT_BOT_TOKEN="{TOKEN}";\n',
    ],
)
def test_assignment_value_parses_direct_shell_assignments(line: str) -> None:
    assert _assignment_value(line) == TOKEN


def test_assignment_value_ignores_comments_and_rejects_expansion() -> None:
    assert _assignment_value(f"# export TELEGRAM_GPT_BOT_TOKEN={TOKEN}") is None
    with pytest.raises(CliError, match="不能包含变量展开或命令"):
        _assignment_value("export TELEGRAM_GPT_BOT_TOKEN=$BOT_TOKEN")


def test_read_bashrc_uses_last_assignment_and_removes_all(tmp_path: Path) -> None:
    bashrc = tmp_path / ".bashrc"
    second = "987654321:ZYXWVUTSRQPONMLKJIHGFEDCBA_1234567"
    bashrc.write_text(
        f"export TELEGRAM_GPT_BOT_TOKEN='{TOKEN}'\n"
        'export PATH="$HOME/bin:$PATH"\n'
        f"TELEGRAM_GPT_BOT_TOKEN={second}\n",
        encoding="utf-8",
    )

    token, original, cleaned, mode = _read_bashrc_token(bashrc)

    assert token == second
    assert original.endswith(f"TELEGRAM_GPT_BOT_TOKEN={second}\n")
    assert cleaned == 'export PATH="$HOME/bin:$PATH"\n'
    assert mode == stat.S_IMODE(bashrc.stat().st_mode)


@pytest.mark.asyncio
async def test_migration_validates_then_writes_private_file_and_cleans_bashrc(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    bashrc = tmp_path / ".bashrc"
    original = f"# 中文配置\nexport TELEGRAM_GPT_BOT_TOKEN='{TOKEN}'\nexport EDITOR=vim\n"
    bashrc.write_text(original, encoding="utf-8")
    bashrc.chmod(0o640)
    validated: list[str] = []

    async def validator(value: str) -> str:
        validated.append(value)
        assert not config.bot_token_path.exists()
        assert bashrc.read_text(encoding="utf-8") == original
        return "test_bot"

    identity = await migrate_bashrc_token(config, bashrc, validator=validator)

    assert identity == "test_bot"
    assert validated == [TOKEN]
    assert config.bot_token_path.read_text(encoding="utf-8") == TOKEN + "\n"
    assert stat.S_IMODE(config.bot_token_path.stat().st_mode) == 0o600
    assert bashrc.read_text(encoding="utf-8") == "# 中文配置\nexport EDITOR=vim\n"
    assert stat.S_IMODE(bashrc.stat().st_mode) == 0o640


@pytest.mark.asyncio
async def test_failed_validation_leaves_bashrc_and_destination_untouched(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    bashrc = tmp_path / ".bashrc"
    original = f"export TELEGRAM_GPT_BOT_TOKEN={TOKEN}\n"
    bashrc.write_text(original, encoding="utf-8")

    async def validator(_value: str) -> str:
        raise CliError("validation failed")

    with pytest.raises(CliError, match="validation failed"):
        await migrate_bashrc_token(config, bashrc, validator=validator)

    assert bashrc.read_text(encoding="utf-8") == original
    assert not config.bot_token_path.exists()


@pytest.mark.asyncio
async def test_validation_error_never_includes_token(monkeypatch: pytest.MonkeyPatch) -> None:
    class LeakyBot:
        def __init__(self, *, token: str) -> None:
            self.token = token

        async def __aenter__(self) -> LeakyBot:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def get_me(self) -> object:
            raise RuntimeError(f"https://api.telegram.invalid/bot{self.token}/getMe")

    monkeypatch.setattr("codex_telegram_bridge.cli.Bot", LeakyBot)

    with pytest.raises(CliError) as captured:
        await _validate_telegram_token(TOKEN)

    assert TOKEN not in str(captured.value)


@pytest.mark.asyncio
async def test_concurrent_bashrc_edit_is_not_overwritten_and_token_write_rolls_back(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    bashrc = tmp_path / ".bashrc"
    original = f"export TELEGRAM_GPT_BOT_TOKEN={TOKEN}\n"
    changed = original + "export EDITOR=nvim\n"
    bashrc.write_text(original, encoding="utf-8")

    async def validator(_value: str) -> str:
        bashrc.write_text(changed, encoding="utf-8")
        return "test_bot"

    with pytest.raises(CliError, match="文件发生变化"):
        await migrate_bashrc_token(config, bashrc, validator=validator)

    assert bashrc.read_text(encoding="utf-8") == changed
    assert not config.bot_token_path.exists()


@pytest.mark.asyncio
async def test_bashrc_mode_is_final_before_atomic_replace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(tmp_path)
    bashrc = tmp_path / ".bashrc"
    original = f"export TELEGRAM_GPT_BOT_TOKEN={TOKEN}\n"
    bashrc.write_text(original, encoding="utf-8")
    bashrc.chmod(0o640)
    real_replace = os.replace
    observed_mode: int | None = None

    def inspect_bashrc_replace(
        source: str | Path,
        destination: str | Path,
        *args: object,
        **kwargs: object,
    ) -> None:
        nonlocal observed_mode
        if not kwargs and Path(destination) == bashrc:
            observed_mode = stat.S_IMODE(Path(source).stat().st_mode)
        real_replace(source, destination, *args, **kwargs)

    monkeypatch.setattr("codex_telegram_bridge.cli.os.replace", inspect_bashrc_replace)

    async def validator(_value: str) -> str:
        return "test_bot"

    await migrate_bashrc_token(config, bashrc, validator=validator)

    assert observed_mode == 0o640
    assert stat.S_IMODE(bashrc.stat().st_mode) == 0o640


@pytest.mark.asyncio
async def test_bashrc_directory_fsync_failure_keeps_committed_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(tmp_path)
    bashrc = tmp_path / ".bashrc"
    original = (
        f"export TELEGRAM_GPT_BOT_TOKEN={TOKEN}\n"
        f"export TELEGRAM_426_BOT_TOKEN={FORUM_TOKEN}\n"
        f"export TELEGRAM_69_BOT_TOKEN={STATUS_TOKEN}\n"
        "export EDITOR=vim\n"
    )
    bashrc.write_text(original, encoding="utf-8")
    bashrc.chmod(0o640)

    def fail_directory_fsync(_path: Path) -> None:
        raise OSError("simulated directory fsync failure")

    monkeypatch.setattr(
        "codex_telegram_bridge.cli._fsync_directory", fail_directory_fsync
    )

    async def validator(value: str) -> str:
        return token_identity(value)

    with pytest.raises(CliError, match="凭据已保留") as captured:
        await migrate_bashrc_tokens(config, bashrc, validator=validator)

    assert "检查磁盘/WSL 状态" in str(captured.value)
    assert "codex-tg doctor --offline" in str(captured.value)
    assert config.read_token("control") == TOKEN
    assert config.read_token("forum") == FORUM_TOKEN
    assert config.read_token("status") == STATUS_TOKEN
    assert bashrc.read_text(encoding="utf-8") == "export EDITOR=vim\n"
    assert stat.S_IMODE(bashrc.stat().st_mode) == 0o640
    assert TOKEN not in str(captured.value)
    assert FORUM_TOKEN not in str(captured.value)


@pytest.mark.asyncio
async def test_configure_tokens_reuses_private_control_and_migrates_forum(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    config.bot_token_path.write_text(TOKEN + "\n", encoding="utf-8")
    config.bot_token_path.chmod(0o600)
    bashrc = tmp_path / ".bashrc"
    bashrc.write_text(
        f"export TELEGRAM_426_BOT_TOKEN='{FORUM_TOKEN}'\n"
        f"export TELEGRAM_69_BOT_TOKEN='{STATUS_TOKEN}'\n"
        "export EDITOR=vim\n",
        encoding="utf-8",
    )
    validated: list[str] = []

    async def validator(value: str) -> str:
        validated.append(value)
        return token_identity(value)

    identities = await migrate_bashrc_tokens(config, bashrc, validator=validator)

    assert set(validated) == {TOKEN, FORUM_TOKEN, STATUS_TOKEN}
    assert identities == {
        "control": "control_bot",
        "forum": "forum_bot",
        "status": "status_bot",
    }
    assert config.read_token("control") == TOKEN
    assert config.read_token("forum") == FORUM_TOKEN
    assert config.read_token("status") == STATUS_TOKEN
    assert bashrc.read_text(encoding="utf-8") == "export EDITOR=vim\n"


@pytest.mark.asyncio
async def test_configure_tokens_rejects_one_token_for_both_bots(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    bashrc = tmp_path / ".bashrc"
    bashrc.write_text(
        f"TELEGRAM_GPT_BOT_TOKEN={TOKEN}\n"
        f"TELEGRAM_426_BOT_TOKEN={TOKEN}\n"
        f"TELEGRAM_69_BOT_TOKEN={STATUS_TOKEN}\n",
        encoding="utf-8",
    )
    with pytest.raises(CliError, match="不同的 token"):
        await migrate_bashrc_tokens(config, bashrc)
    assert not config.bot_token_path.exists()
    assert not config.forum_bot_token_path.exists()


@pytest.mark.asyncio
async def test_bashrc_migration_cannot_bypass_token_rotation_reset(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    config.bot_token_path.write_text(TOKEN + "\n", encoding="utf-8")
    config.forum_bot_token_path.write_text(FORUM_TOKEN + "\n", encoding="utf-8")
    config.status_bot_token_path.write_text(STATUS_TOKEN + "\n", encoding="utf-8")
    config.bot_token_path.chmod(0o600)
    config.forum_bot_token_path.chmod(0o600)
    config.status_bot_token_path.chmod(0o600)
    store = Store(config.database_path)
    store.set_owner(Owner(7, 8, "owner"))
    store.close()
    bashrc = tmp_path / ".bashrc"
    original = (
        f"TELEGRAM_GPT_BOT_TOKEN={NEW_TOKEN}\n"
        f"TELEGRAM_426_BOT_TOKEN={NEW_FORUM_TOKEN}\n"
        f"TELEGRAM_69_BOT_TOKEN={NEW_STATUS_TOKEN}\n"
    )
    bashrc.write_text(original, encoding="utf-8")

    async def validator(value: str) -> str:
        return f"bot_{value.partition(':')[0]}"

    with pytest.raises(CliError, match="owner-reset"):
        await migrate_bashrc_tokens(config, bashrc, validator=validator)

    assert bashrc.read_text(encoding="utf-8") == original
    assert config.read_token("control") == TOKEN
    assert config.read_token("forum") == FORUM_TOKEN
    assert config.read_token("status") == STATUS_TOKEN


def test_parser_exposes_dual_token_and_binding_commands() -> None:
    parser = build_parser()
    assert parser.parse_args(["configure-tokens"]).command == "configure-tokens"
    assert parser.parse_args(["bind-code"]).command == "bind-code"


def test_status_reports_dual_credentials_and_binding_without_secrets(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    config.bot_token_path.write_text(TOKEN + "\n", encoding="utf-8")
    config.forum_bot_token_path.write_text(FORUM_TOKEN + "\n", encoding="utf-8")
    config.status_bot_token_path.write_text(STATUS_TOKEN + "\n", encoding="utf-8")
    config.bot_token_path.chmod(0o600)
    config.forum_bot_token_path.chmod(0o600)
    config.status_bot_token_path.chmod(0o600)
    store = Store(config.database_path)
    store.set_telegram_binding(
        {
            "channel_chat_id": -1001,
            "discussion_chat_id": -1002,
            "control_bot_id": 10,
            "forum_bot_id": 20,
            "is_forum": False,
        }
    )
    store.close()
    output = StringIO()

    assert _status(config, output) == 0
    rendered = output.getvalue()
    assert "control token: configured/private" in rendered
    assert "forum token: configured/private" in rendered
    assert "status token: configured/private" in rendered
    assert "channel binding: ready" in rendered
    assert "app-server mode: external" in rendered
    assert TOKEN not in rendered
    assert FORUM_TOKEN not in rendered
    assert STATUS_TOKEN not in rendered


def test_status_json_reports_persisted_health_without_secrets(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    store = Store(config.database_path)
    store.write_health_snapshot(
        {
            "service_state": "running",
            "polling": [{"role": "control", "seconds_since_success": 1.5}],
            "outbound": {"control": {"queues": {"interactive": 0}}},
            "delivery": {"pending": 0},
            "bridge": {
                "codex": {"generation": 3, "notification_queued": 0},
                "resync": {"failures": 0},
            },
        }
    )
    store.close()
    output = StringIO()

    assert _status(config, output, as_json=True) == 0

    payload = json.loads(output.getvalue())
    assert payload["database"]["schema_version"] == SCHEMA_VERSION
    assert payload["database"]["bytes"] > 0
    assert payload["runtime"]["health_state"] == "fresh"
    assert payload["runtime"]["health"]["polling"][0]["role"] == "control"
    assert payload["runtime"]["health"]["bridge"]["codex"]["generation"] == 3
    assert payload["credentials"]["status_token"] == "missing/insecure"
    assert payload["app_server"] == {"mode": "external", "socket": "unavailable/insecure"}
    assert TOKEN not in output.getvalue()
    assert FORUM_TOKEN not in output.getvalue()


def test_status_does_not_create_or_migrate_database(tmp_path: Path) -> None:
    missing_config = make_config(tmp_path / "missing")
    assert _status(missing_config, StringIO(), as_json=True) == 0
    assert not missing_config.database_path.exists()

    config = make_config(tmp_path / "legacy")
    config.state_dir.mkdir(parents=True)
    connection = sqlite3.connect(config.database_path)
    connection.execute("PRAGMA user_version=9")
    connection.commit()
    connection.close()

    output = StringIO()
    assert _status(config, output, as_json=True) == 0
    connection = sqlite3.connect(config.database_path)
    try:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 9
        assert connection.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'"
        ).fetchone()[0] == 0
    finally:
        connection.close()


def test_app_server_mode_accepts_only_supported_values(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(tmp_path)
    assert config.app_server_mode is AppServerMode.EXTERNAL
    managed = Config(
        config_dir=config.config_dir,
        state_dir=config.state_dir,
        codex_home=config.codex_home,
        codex_socket=config.codex_socket,
        codex_binary=config.codex_binary,
        allowed_root=config.allowed_root,
        app_server_mode="managed-daemon",
    )
    assert managed.app_server_mode is AppServerMode.MANAGED_DAEMON
    with pytest.raises(ValueError, match="app_server_mode"):
        Config(
            config_dir=config.config_dir,
            state_dir=config.state_dir,
            codex_home=config.codex_home,
            codex_socket=config.codex_socket,
            codex_binary=config.codex_binary,
            allowed_root=config.allowed_root,
            app_server_mode="unsafe",
        )
    monkeypatch.setenv("CODEX_APP_SERVER_MODE", "installer-service")
    assert Config.default().app_server_mode is AppServerMode.INSTALLER_SERVICE


def test_app_server_watchdog_avoids_telegram_and_database_side_effects(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    output = StringIO()
    assert _app_server_watchdog(config, output) == 0
    assert "external ownership" in output.getvalue()
    assert not config.database_path.exists()


def test_watchdog_command_routes_without_telegram_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(tmp_path)
    monkeypatch.setattr(cli, "_config_from_args", lambda _args: config)
    output = StringIO()
    assert run(build_parser().parse_args(["app-server-watchdog"]), output=output) == 0
    assert not config.database_path.exists()


def test_managed_daemon_watchdog_restarts_and_rechecks_protocol(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = Config(
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        codex_home=tmp_path / ".codex",
        codex_socket=tmp_path / ".codex" / "control.sock",
        codex_binary=tmp_path / "codex",
        allowed_root=tmp_path,
        app_server_mode=AppServerMode.MANAGED_DAEMON,
    )
    protocol_ready = False
    commands: list[tuple[str, ...]] = []

    monkeypatch.setattr(cli, "_socket_is_private", lambda _path: True)
    monkeypatch.setattr(cli, "_managed_daemon_version_available", lambda _config: True)

    async def protocol_probe(_path: Path) -> bool:
        if not protocol_ready:
            raise RuntimeError("protocol unavailable")
        return protocol_ready

    def run(command: list[str], **_kwargs: object) -> SimpleNamespace:
        nonlocal protocol_ready
        commands.append(tuple(command))
        if command[-1] == "restart":
            protocol_ready = True
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(cli, "_probe_app_server_protocol", protocol_probe)
    monkeypatch.setattr(cli.subprocess, "run", run)
    output = StringIO()

    assert _app_server_watchdog(config, output, recover=True) == 0
    assert (str(config.codex_binary), "app-server", "daemon", "restart") in commands
    assert "daemon recovered and protocol verified" in output.getvalue()


@pytest.mark.asyncio
async def test_prompt_configures_distinct_tokens_with_private_permissions(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    entered = iter([TOKEN, FORUM_TOKEN, STATUS_TOKEN])
    prompts: list[str] = []

    def token_reader(prompt: str) -> str:
        prompts.append(prompt)
        return next(entered)

    async def validator(value: str) -> str:
        return token_identity(value)

    identities = await configure_prompt_tokens(
        config,
        token_reader=token_reader,
        validator=validator,
    )

    assert identities == {
        "control": "control_bot",
        "forum": "forum_bot",
        "status": "status_bot",
    }
    assert len(prompts) == 3
    assert all("不会显示" in prompt for prompt in prompts)
    assert config.read_token("control") == TOKEN
    assert config.read_token("forum") == FORUM_TOKEN
    assert config.read_token("status") == STATUS_TOKEN
    assert stat.S_IMODE(config.bot_token_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(config.forum_bot_token_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(config.status_bot_token_path.stat().st_mode) == 0o600


@pytest.mark.asyncio
async def test_fill_missing_migrates_legacy_control_and_adds_status(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    config.legacy_bot_token_path.write_text(TOKEN + "\n", encoding="utf-8")
    config.forum_bot_token_path.write_text(FORUM_TOKEN + "\n", encoding="utf-8")
    config.legacy_bot_token_path.chmod(0o600)
    config.forum_bot_token_path.chmod(0o600)
    prompts: list[str] = []

    def token_reader(prompt: str) -> str:
        prompts.append(prompt)
        return STATUS_TOKEN

    async def validator(value: str) -> str:
        return token_identity(value)

    identities = await configure_prompt_tokens(
        config,
        fill_missing=True,
        token_reader=token_reader,
        validator=validator,
    )

    assert identities == {
        "control": "control_bot",
        "forum": "forum_bot",
        "status": "status_bot",
    }
    assert len(prompts) == 1 and "Status Bot" in prompts[0]
    assert not config.legacy_bot_token_path.exists()
    assert config.read_token("control") == TOKEN
    assert config.read_token("forum") == FORUM_TOKEN
    assert config.read_token("status") == STATUS_TOKEN


@pytest.mark.asyncio
async def test_fill_missing_rejects_conflicting_legacy_control_without_prompt(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    config.bot_token_path.write_text(TOKEN + "\n", encoding="utf-8")
    config.legacy_bot_token_path.write_text(NEW_TOKEN + "\n", encoding="utf-8")
    config.bot_token_path.chmod(0o600)
    config.legacy_bot_token_path.chmod(0o600)
    prompts = 0

    def token_reader(_prompt: str) -> str:
        nonlocal prompts
        prompts += 1
        return STATUS_TOKEN

    with pytest.raises(CliError, match="内容冲突") as captured:
        await configure_prompt_tokens(config, fill_missing=True, token_reader=token_reader)

    assert prompts == 0
    assert config.bot_token_path.read_text(encoding="utf-8") == TOKEN + "\n"
    assert config.legacy_bot_token_path.read_text(encoding="utf-8") == NEW_TOKEN + "\n"
    assert TOKEN not in str(captured.value)
    assert NEW_TOKEN not in str(captured.value)


@pytest.mark.asyncio
async def test_fill_missing_validation_failure_keeps_legacy_and_missing_targets(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    config.legacy_bot_token_path.write_text(TOKEN + "\n", encoding="utf-8")
    config.forum_bot_token_path.write_text(FORUM_TOKEN + "\n", encoding="utf-8")
    config.legacy_bot_token_path.chmod(0o600)
    config.forum_bot_token_path.chmod(0o600)

    async def validator(value: str) -> str:
        if value == STATUS_TOKEN:
            raise CliError("validation failed")
        return token_identity(value)

    with pytest.raises(CliError, match="validation failed"):
        await configure_prompt_tokens(
            config,
            fill_missing=True,
            token_reader=lambda _prompt: STATUS_TOKEN,
            validator=validator,
        )

    assert config.legacy_bot_token_path.read_text(encoding="utf-8") == TOKEN + "\n"
    assert not config.bot_token_path.exists()
    assert not config.status_bot_token_path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("symlink_parent", [False, True])
async def test_prompt_rejects_symlinked_credential_paths_before_reading_tokens(
    tmp_path: Path, symlink_parent: bool
) -> None:
    real = tmp_path / "real"
    real.mkdir()
    lexical_root = tmp_path / "lexical"
    if symlink_parent:
        lexical_root.symlink_to(real, target_is_directory=True)
        config = make_config(lexical_root)
    else:
        lexical_root.mkdir()
        config = make_config(lexical_root)
        config.config_dir.symlink_to(real, target_is_directory=True)
    prompts = 0

    def token_reader(_prompt: str) -> str:
        nonlocal prompts
        prompts += 1
        return TOKEN

    with pytest.raises(CliError, match="symlink or non-directory"):
        await configure_prompt_tokens(config, token_reader=token_reader)

    assert prompts == 0
    assert list(real.iterdir()) == []


@pytest.mark.asyncio
async def test_prompt_requires_force_and_rejects_same_bot_identity(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    config.bot_token_path.write_text(TOKEN + "\n", encoding="utf-8")
    config.forum_bot_token_path.write_text(FORUM_TOKEN + "\n", encoding="utf-8")
    config.status_bot_token_path.write_text(STATUS_TOKEN + "\n", encoding="utf-8")
    config.bot_token_path.chmod(0o600)
    config.forum_bot_token_path.chmod(0o600)
    config.status_bot_token_path.chmod(0o600)

    async def distinct_validator(value: str) -> str:
        return "new_control" if value == FORUM_TOKEN else "new_forum"

    entered = iter([FORUM_TOKEN, TOKEN])
    with pytest.raises(CliError, match="--force"):
        await configure_prompt_tokens(
            config,
            token_reader=lambda _prompt: next(entered),
            validator=distinct_validator,
        )
    assert config.read_token("control") == TOKEN
    assert config.read_token("forum") == FORUM_TOKEN

    async def same_identity_validator(_value: str) -> str:
        return "same_bot"

    entered = iter([TOKEN, FORUM_TOKEN, STATUS_TOKEN])
    with pytest.raises(CliError, match="同一个 Bot") as captured:
        await configure_prompt_tokens(
            make_config(tmp_path / "other"),
            token_reader=lambda _prompt: next(entered),
            validator=same_identity_validator,
        )
    assert TOKEN not in str(captured.value)
    assert FORUM_TOKEN not in str(captured.value)


@pytest.mark.asyncio
async def test_prompt_rolls_back_both_credentials_when_commit_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    old_control = "111111111:ABCDEFGHIJKLMNOPQRSTUVWXYZ_old_control"
    old_forum = "222222222:ABCDEFGHIJKLMNOPQRSTUVWXYZ_old_forum__"
    old_status = "666666666:ABCDEFGHIJKLMNOPQRSTUVWXYZ_old_status_"
    config.bot_token_path.write_text(old_control + "\n", encoding="utf-8")
    config.forum_bot_token_path.write_text(old_forum + "\n", encoding="utf-8")
    config.status_bot_token_path.write_text(old_status + "\n", encoding="utf-8")
    config.bot_token_path.chmod(0o600)
    config.forum_bot_token_path.chmod(0o600)
    config.status_bot_token_path.chmod(0o600)
    real_replace = os.replace
    replacement_count = 0

    def fail_second_replace(
        source: str | Path,
        destination: str | Path,
        *args: object,
        **kwargs: object,
    ) -> None:
        nonlocal replacement_count
        replacement_count += 1
        if replacement_count == 2:
            raise OSError("simulated failure")
        real_replace(source, destination, *args, **kwargs)

    monkeypatch.setattr("codex_telegram_bridge.cli.os.replace", fail_second_replace)

    async def validator(value: str) -> str:
        return token_identity(value)

    entered = iter([TOKEN, FORUM_TOKEN, STATUS_TOKEN])
    with pytest.raises(CliError, match="原有凭据已恢复") as captured:
        await configure_prompt_tokens(
            config,
            force=True,
            token_reader=lambda _prompt: next(entered),
            validator=validator,
        )

    assert config.read_token("control") == old_control
    assert config.read_token("forum") == old_forum
    assert config.read_token("status") == old_status
    assert TOKEN not in str(captured.value)
    assert FORUM_TOKEN not in str(captured.value)


@pytest.mark.asyncio
async def test_prompt_interrupt_after_first_commit_restores_both_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    old_control = "111111111:ABCDEFGHIJKLMNOPQRSTUVWXYZ_old_control"
    old_forum = "222222222:ABCDEFGHIJKLMNOPQRSTUVWXYZ_old_forum__"
    old_status = "666666666:ABCDEFGHIJKLMNOPQRSTUVWXYZ_old_status_"
    config.bot_token_path.write_text(old_control + "\n", encoding="utf-8")
    config.forum_bot_token_path.write_text(old_forum + "\n", encoding="utf-8")
    config.status_bot_token_path.write_text(old_status + "\n", encoding="utf-8")
    config.bot_token_path.chmod(0o600)
    config.forum_bot_token_path.chmod(0o600)
    config.status_bot_token_path.chmod(0o600)
    real_fsync = os.fsync
    fsync_count = 0

    def interrupt_after_first_replace(descriptor: int) -> None:
        nonlocal fsync_count
        fsync_count += 1
        if fsync_count == 3:
            raise KeyboardInterrupt
        real_fsync(descriptor)

    monkeypatch.setattr("codex_telegram_bridge.cli.os.fsync", interrupt_after_first_replace)

    async def validator(value: str) -> str:
        return token_identity(value)

    entered = iter([TOKEN, FORUM_TOKEN, STATUS_TOKEN])
    with pytest.raises(KeyboardInterrupt):
        await configure_prompt_tokens(
            config,
            force=True,
            token_reader=lambda _prompt: next(entered),
            validator=validator,
        )

    assert config.read_token("control") == old_control
    assert config.read_token("forum") == old_forum
    assert config.read_token("status") == old_status


@pytest.mark.asyncio
async def test_prompt_never_chmods_or_reads_substituted_symlink_target(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(tmp_path)
    victim = tmp_path / "victim"
    victim.write_text("do-not-touch\n", encoding="utf-8")
    victim.chmod(0o640)
    real_link = os.link
    replaced = False

    def link_then_substitute_symlink(
        source: str | Path,
        destination: str | Path,
        *args: object,
        **kwargs: object,
    ) -> None:
        nonlocal replaced
        real_link(source, destination, *args, **kwargs)
        if not replaced:
            replaced = True
            destination_dir_fd = kwargs.get("dst_dir_fd")
            assert isinstance(destination_dir_fd, int)
            os.unlink(destination, dir_fd=destination_dir_fd)
            os.symlink(victim, destination, dir_fd=destination_dir_fd)

    monkeypatch.setattr("codex_telegram_bridge.cli.os.link", link_then_substitute_symlink)

    async def validator(value: str) -> str:
        return token_identity(value)

    entered = iter([TOKEN, FORUM_TOKEN, STATUS_TOKEN])
    with pytest.raises(CliError, match="回滚未完整完成") as captured:
        await configure_prompt_tokens(
            config,
            token_reader=lambda _prompt: next(entered),
            validator=validator,
        )

    assert victim.read_text(encoding="utf-8") == "do-not-touch\n"
    assert stat.S_IMODE(victim.stat().st_mode) == 0o640
    assert TOKEN not in str(captured.value)
    assert FORUM_TOKEN not in str(captured.value)


@pytest.mark.asyncio
async def test_prompt_detects_parent_directory_swap_and_does_not_write_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    moved = tmp_path / "moved-config"
    real_link = os.link
    swapped = False

    def link_then_swap_parent(
        source: str | Path,
        destination: str | Path,
        *args: object,
        **kwargs: object,
    ) -> None:
        nonlocal swapped
        real_link(source, destination, *args, **kwargs)
        if not swapped:
            swapped = True
            config.config_dir.rename(moved)
            config.config_dir.mkdir(mode=0o700)

    monkeypatch.setattr("codex_telegram_bridge.cli.os.link", link_then_swap_parent)

    async def validator(value: str) -> str:
        return token_identity(value)

    entered = iter([TOKEN, FORUM_TOKEN, STATUS_TOKEN])
    with pytest.raises(CliError, match="回滚未完整完成") as captured:
        await configure_prompt_tokens(
            config,
            token_reader=lambda _prompt: next(entered),
            validator=validator,
        )

    assert list(config.config_dir.iterdir()) == []
    assert [path.name for path in moved.iterdir()] == [".telegram-credentials.lock"]
    assert TOKEN not in str(captured.value)
    assert FORUM_TOKEN not in str(captured.value)


@pytest.mark.asyncio
async def test_changed_tokens_require_owner_reset_before_rotation(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    config.bot_token_path.write_text(TOKEN + "\n", encoding="utf-8")
    config.forum_bot_token_path.write_text(FORUM_TOKEN + "\n", encoding="utf-8")
    config.status_bot_token_path.write_text(STATUS_TOKEN + "\n", encoding="utf-8")
    config.bot_token_path.chmod(0o600)
    config.forum_bot_token_path.chmod(0o600)
    config.status_bot_token_path.chmod(0o600)
    store = Store(config.database_path)
    store.set_owner(Owner(7, 8, "owner"))
    store.close()

    async def validator(value: str) -> str:
        return f"bot_{value.partition(':')[0]}"

    entered = iter([NEW_TOKEN, NEW_FORUM_TOKEN, NEW_STATUS_TOKEN])
    with pytest.raises(CliError, match="owner-reset") as captured:
        await configure_prompt_tokens(
            config,
            force=True,
            token_reader=lambda _prompt: next(entered),
            validator=validator,
        )

    assert config.read_token("control") == TOKEN
    assert config.read_token("forum") == FORUM_TOKEN
    assert "systemctl --user restart" in str(captured.value)
    assert "codex-tg onboard" in str(captured.value)
    assert NEW_TOKEN not in str(captured.value)
    assert NEW_FORUM_TOKEN not in str(captured.value)


def test_non_force_commit_never_overwrites_concurrently_created_credential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(tmp_path)
    real_link = os.link
    concurrent = "555555555:CONCURRENT_CONTROL_TOKEN_VALUE\n"
    created = False

    def create_destination_before_link(
        source: str | Path,
        destination: str | Path,
        *args: object,
        **kwargs: object,
    ) -> None:
        nonlocal created
        if not created:
            created = True
            destination_dir_fd = kwargs.get("dst_dir_fd")
            assert isinstance(destination_dir_fd, int)
            descriptor = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=destination_dir_fd,
            )
            os.write(descriptor, concurrent.encode())
            os.close(descriptor)
        real_link(source, destination, *args, **kwargs)

    monkeypatch.setattr("codex_telegram_bridge.cli.os.link", create_destination_before_link)

    with pytest.raises(CliError, match="另一个配置进程"):
        _install_token_files(
            {
                "control": (config.bot_token_path, TOKEN),
                "forum": (config.forum_bot_token_path, FORUM_TOKEN),
            },
            force=False,
        )

    assert config.bot_token_path.read_text(encoding="utf-8") == concurrent
    assert not config.forum_bot_token_path.exists()


def test_two_concurrent_first_time_writers_cannot_mix_or_overwrite_tokens(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    pairs = [(TOKEN, FORUM_TOKEN), (NEW_TOKEN, NEW_FORUM_TOKEN)]

    def install(pair: tuple[str, str]) -> str:
        try:
            _install_token_files(
                {
                    "control": (config.bot_token_path, pair[0]),
                    "forum": (config.forum_bot_token_path, pair[1]),
                },
                force=False,
            )
        except CliError:
            return "rejected"
        return "installed"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(install, pairs))

    assert sorted(results) == ["installed", "rejected"]
    stored = (config.read_token("control"), config.read_token("forum"))
    assert stored in pairs


@pytest.mark.asyncio
async def test_rotation_succeeds_after_owner_reset_and_same_token_force_is_nondestructive(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    config.bot_token_path.write_text(TOKEN + "\n", encoding="utf-8")
    config.forum_bot_token_path.write_text(FORUM_TOKEN + "\n", encoding="utf-8")
    config.status_bot_token_path.write_text(STATUS_TOKEN + "\n", encoding="utf-8")
    config.bot_token_path.chmod(0o600)
    config.forum_bot_token_path.chmod(0o600)
    config.status_bot_token_path.chmod(0o600)
    store = Store(config.database_path)
    store.set_owner(Owner(7, 8, "owner"))
    store.set_telegram_binding(
        {
            "channel_chat_id": -1001,
            "discussion_chat_id": -1002,
            "control_bot_id": 10,
            "forum_bot_id": 20,
            "is_forum": False,
        }
    )
    store.close()

    async def validator(value: str) -> str:
        return f"bot_{value.partition(':')[0]}"

    same = iter([TOKEN, FORUM_TOKEN, STATUS_TOKEN])
    await configure_prompt_tokens(
        config,
        force=True,
        token_reader=lambda _prompt: next(same),
        validator=validator,
    )
    store = Store(config.database_path)
    assert store.get_owner() is not None
    assert store.get_telegram_binding() is not None
    store.reset_owner()
    store.close()

    changed = iter([NEW_TOKEN, NEW_FORUM_TOKEN, NEW_STATUS_TOKEN])
    await configure_prompt_tokens(
        config,
        force=True,
        token_reader=lambda _prompt: next(changed),
        validator=validator,
    )
    assert config.read_token("control") == NEW_TOKEN
    assert config.read_token("forum") == NEW_FORUM_TOKEN
    assert config.read_token("status") == NEW_STATUS_TOKEN
    store = Store(config.database_path)
    try:
        assert store.get_owner() is None
        assert store.get_telegram_binding() is None
    finally:
        store.close()


def test_configure_tokens_json_outputs_identities_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(tmp_path)

    async def fake_prompt(
        _config: Config, *, force: bool, fill_missing: bool
    ) -> dict[str, str]:
        assert _config is config
        assert force is True
        assert fill_missing is False
        return {"control": "control_bot", "forum": "讨论_bot", "status": "status_bot"}

    monkeypatch.setattr("codex_telegram_bridge.cli._config_from_args", lambda _args: config)
    monkeypatch.setattr("codex_telegram_bridge.cli.configure_prompt_tokens", fake_prompt)
    args = build_parser().parse_args(["configure-tokens", "--prompt", "--json", "--force"])
    output = StringIO()

    assert run(args, output=output) == 0
    assert json.loads(output.getvalue()) == {
        "control": "control_bot",
        "forum": "讨论_bot",
        "status": "status_bot",
    }
    assert set(json.loads(output.getvalue())) == {"control", "forum", "status"}
    assert TOKEN not in output.getvalue()
    assert FORUM_TOKEN not in output.getvalue()


def test_parser_exposes_prompt_json_force_and_onboard_timeout() -> None:
    parser = build_parser()
    configured = parser.parse_args(["configure-tokens", "--prompt", "--json", "--force"])
    onboard_args = parser.parse_args(["onboard", "--timeout", "42"])

    assert configured.prompt is True
    assert configured.json is True
    assert configured.force is True
    assert configured.fill_missing is False
    fill_missing = parser.parse_args(["configure-tokens", "--prompt", "--fill-missing"])
    assert fill_missing.fill_missing is True
    assert onboard_args.command == "onboard"
    assert onboard_args.timeout == 42


@pytest.mark.asyncio
async def test_onboard_resumes_stages_and_runs_online_doctor(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    config.totp_secret_path.write_text("ALREADY-CONFIGURED\n", encoding="utf-8")
    config.totp_secret_path.chmod(0o600)
    now = 0.0
    sleep_count = 0
    doctor_calls: list[bool] = []

    def clock() -> float:
        return now

    async def sleeper(delay: float) -> None:
        nonlocal now, sleep_count
        now += delay
        sleep_count += 1
        external = Store(config.database_path)
        try:
            if sleep_count == 1:
                external.set_owner(Owner(7, 8, "owner"))
            elif sleep_count == 2:
                external.set_telegram_binding(
                    {
                        "channel_chat_id": -1001,
                        "discussion_chat_id": -1002,
                        "control_bot_id": 10,
                        "forum_bot_id": 20,
                        "is_forum": False,
                    }
                )
        finally:
            external.close()

    async def doctor(_config: Config, *, offline: bool, output: StringIO) -> int:
        assert _config is config
        assert output is rendered
        doctor_calls.append(offline)
        return 0

    rendered = StringIO()
    assert (
        await onboard(
            config,
            timeout=5,
            output=rendered,
            clock=clock,
            sleeper=sleeper,
            doctor=doctor,
        )
        == 0
    )
    assert sleep_count == 2
    assert doctor_calls == [False]
    assert "TOTP 已配置，跳过注册" in rendered.getvalue()
    assert "Telegram owner 配对完成" in rendered.getvalue()
    assert "频道与讨论组绑定完成" in rendered.getvalue()
    store = Store(config.database_path)
    try:
        assert store.get_meta("pair_code_digest") == ""
        assert store.get_meta("bind_code_digest") == ""
    finally:
        store.close()


@pytest.mark.asyncio
async def test_onboard_skips_completed_stages_without_waiting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    config.totp_secret_path.write_text("ALREADY-CONFIGURED\n", encoding="utf-8")
    config.totp_secret_path.chmod(0o600)
    store = Store(config.database_path)
    store.set_owner(Owner(7, 8, "owner"))
    store.set_telegram_binding(
        {
            "channel_chat_id": -1001,
            "discussion_chat_id": -1002,
            "control_bot_id": 10,
            "forum_bot_id": 20,
            "is_forum": False,
        }
    )
    store.close()

    async def sleeper(_delay: float) -> None:
        pytest.fail("completed onboarding must not sleep")

    async def doctor(_config: Config, *, offline: bool, output: StringIO) -> int:
        assert offline is False
        return 0

    monkeypatch.setattr("codex_telegram_bridge.cli._enroll", lambda *_args, **_kwargs: pytest.fail())
    output = StringIO()
    assert await onboard(config, output=output, sleeper=sleeper, doctor=doctor) == 0
    assert "跳过配对" in output.getvalue()
    assert "跳过绑定" in output.getvalue()


@pytest.mark.asyncio
async def test_onboard_enrolls_totp_when_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(tmp_path)
    store = Store(config.database_path)
    store.set_owner(Owner(7, 8, "owner"))
    store.set_telegram_binding(
        {
            "channel_chat_id": -1001,
            "discussion_chat_id": -1002,
            "control_bot_id": 10,
            "forum_bot_id": 20,
            "is_forum": False,
        }
    )
    store.close()
    enrollments = 0

    def enroll(
        security: SecurityManager, _enrollment: Enrollment, **_kwargs: object
    ) -> None:
        nonlocal enrollments
        enrollments += 1
        secret_path = security.secret_path
        secret_path.parent.mkdir(parents=True, exist_ok=True)
        secret_path.write_text("NEW-TOTP-SECRET\n", encoding="utf-8")
        secret_path.chmod(0o600)

    async def doctor(_config: Config, *, offline: bool, output: StringIO) -> int:
        assert offline is False
        return 0

    monkeypatch.setattr("codex_telegram_bridge.cli._enroll", enroll)
    output = StringIO()
    assert await onboard(config, output=output, doctor=doctor) == 0
    assert enrollments == 1
    assert config.totp_secret_path.read_text(encoding="utf-8") == "NEW-TOTP-SECRET\n"
    assert "开始注册 TOTP" in output.getvalue()


@pytest.mark.asyncio
async def test_onboard_timeout_clears_pair_code_and_does_not_run_doctor(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    config.totp_secret_path.write_text("ALREADY-CONFIGURED\n", encoding="utf-8")
    config.totp_secret_path.chmod(0o600)
    now = 0.0

    def clock() -> float:
        return now

    async def sleeper(delay: float) -> None:
        nonlocal now
        now += delay

    async def doctor(_config: Config, *, offline: bool, output: StringIO) -> int:
        pytest.fail("timeout must not run doctor")

    with pytest.raises(CliError, match="超时"):
        await onboard(
            config,
            timeout=2,
            output=StringIO(),
            clock=clock,
            sleeper=sleeper,
            doctor=doctor,
        )

    store = Store(config.database_path)
    try:
        assert store.get_meta("pair_code_digest") == ""
        assert store.get_meta("pair_code_expires") == 0
    finally:
        store.close()
