from __future__ import annotations

import os
from pathlib import Path

import pytest

from codex_telegram_bridge.config import Config, ensure_private_directory


def make_config(tmp_path: Path, **overrides: object) -> Config:
    values: dict[str, object] = {
        "config_dir": tmp_path / "config",
        "state_dir": tmp_path / "state",
        "codex_home": tmp_path / ".codex",
        "codex_socket": tmp_path / "codex.sock",
        "codex_binary": tmp_path / "codex",
        "allowed_root": tmp_path,
    }
    values.update(overrides)
    return Config(**values)  # type: ignore[arg-type]


def test_bot_token_requires_private_regular_file(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    token = config.config_dir / "telegram_bot_token"
    token.write_text("123456:secret\n", encoding="utf-8")
    token.chmod(0o600)
    assert config.read_bot_token() == "123456:secret"

    token.chmod(0o644)
    with pytest.raises(RuntimeError, match="group or others"):
        config.read_bot_token()


def test_bot_token_refuses_symlink(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    target = tmp_path / "token"
    target.write_text("123456:secret", encoding="utf-8")
    target.chmod(0o600)
    config.bot_token_path.symlink_to(target)
    with pytest.raises(RuntimeError, match="securely open"):
        config.read_bot_token()


def test_two_bot_tokens_are_private_and_role_addressable(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir()
    config.bot_token_path.write_text("123456:control-secret-value\n", encoding="utf-8")
    config.forum_bot_token_path.write_text("654321:forum-secret-value\n", encoding="utf-8")
    config.bot_token_path.chmod(0o600)
    config.forum_bot_token_path.chmod(0o600)

    assert config.read_token("9527") == "123456:control-secret-value"
    assert config.read_forum_bot_token() == "654321:forum-secret-value"
    assert config.token_path("426") == config.forum_bot_token_path
    with pytest.raises(ValueError, match="Unknown Telegram bot role"):
        config.token_path("unknown")


def test_ensure_private_directory_repairs_mode_and_refuses_symlink_components(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "private"
    directory.mkdir(mode=0o755)
    ensure_private_directory(directory)
    assert directory.stat().st_mode & 0o777 == 0o700

    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(RuntimeError, match="symlink or non-directory"):
        ensure_private_directory(link)

    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(target, target_is_directory=True)
    with pytest.raises(RuntimeError, match="symlink or non-directory"):
        ensure_private_directory(linked_parent / "private")
    assert not (target / "private").exists()


def test_ensure_private_directory_fails_when_mode_cannot_be_tightened(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "private"
    directory.mkdir(mode=0o755)
    monkeypatch.setattr(os, "fchmod", lambda _descriptor, _mode: None)

    with pytest.raises(RuntimeError, match="permissions could not be secured"):
        ensure_private_directory(directory)


def test_config_load_refuses_symlinked_file_and_parent(tmp_path: Path) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    target = tmp_path / "attacker.toml"
    target.write_text('[bridge]\nconfig_dir = "/tmp/attacker"\n', encoding="utf-8")
    target.chmod(0o600)
    linked_file = private / "config.toml"
    linked_file.symlink_to(target)

    with pytest.raises(ValueError, match="must not be a symbolic link"):
        Config.load(linked_file)

    linked_file.unlink()
    target.replace(private / "config.toml")
    linked_parent = tmp_path / "linked-private"
    linked_parent.symlink_to(private, target_is_directory=True)
    with pytest.raises(ValueError, match="Cannot securely access"):
        Config.load(linked_parent / "config.toml")


def test_config_rejects_incoherent_limits(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="download_limit"):
        make_config(tmp_path, telegram_download_limit=101, inbox_quota_bytes=100)
    with pytest.raises(ValueError, match="minimum_free_bytes"):
        make_config(tmp_path, minimum_free_bytes=-1)
    with pytest.raises(ValueError, match="tmux_session"):
        make_config(tmp_path, tmux_session="bad:name")


def test_bot_labels_default_to_generic_names_and_load_trimmed(tmp_path: Path) -> None:
    default = make_config(tmp_path)
    assert default.control_bot_label == "Control Bot"
    assert default.discussion_bot_label == "Discussion Bot"

    config_path = tmp_path / "bridge.toml"
    config_path.write_text(
        '[bridge]\ncontrol_bot_label = "  控制_[Bot]  "\n'
        'discussion_bot_label = "  Session 助手  "\n',
        encoding="utf-8",
    )
    loaded = Config.load(config_path)
    assert loaded.control_bot_label == "控制_[Bot]"
    assert loaded.discussion_bot_label == "Session 助手"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("control_bot_label", ""),
        ("discussion_bot_label", "   "),
        ("control_bot_label", "bad\nlabel"),
        ("discussion_bot_label", "bad\tlabel"),
        ("control_bot_label", "x" * 41),
        ("discussion_bot_label", 426),
    ],
)
def test_bot_labels_reject_invalid_values(tmp_path: Path, field: str, value: object) -> None:
    with pytest.raises(ValueError, match=field):
        make_config(tmp_path, **{field: value})


def test_ask_model_settings_default_to_inherited_and_load_trimmed(tmp_path: Path) -> None:
    default = make_config(tmp_path)
    assert default.ask_model is None
    assert default.ask_reasoning_effort is None

    config_path = tmp_path / "bridge.toml"
    config_path.write_text(
        '[bridge]\nask_model = "  gpt-5.6-luna  "\nask_reasoning_effort = "  max  "\n',
        encoding="utf-8",
    )

    loaded = Config.load(config_path)
    assert loaded.ask_model == "gpt-5.6-luna"
    assert loaded.ask_reasoning_effort == "max"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("ask_model", ""),
        ("ask_reasoning_effort", "  "),
        ("ask_model", "bad\nmodel"),
        ("ask_reasoning_effort", "bad\teffort"),
        ("ask_model", "x" * 129),
        ("ask_reasoning_effort", 5),
    ],
)
def test_ask_model_settings_reject_invalid_values(
    tmp_path: Path, field: str, value: object
) -> None:
    with pytest.raises(ValueError, match=field):
        make_config(tmp_path, **{field: value})


def test_loaded_paths_are_expanded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    config_path = tmp_path / "bridge.toml"
    config_path.write_text('[bridge]\nallowed_root = "~/workspace"\n', encoding="utf-8")
    config = Config.load(config_path)
    assert config.allowed_root == (tmp_path / "workspace").resolve()
    assert os.path.isabs(config.allowed_root)
