from __future__ import annotations

import contextlib
import os
import stat
import tomllib
import unicodedata
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any


def _expand_path(value: str | Path) -> Path:
    # Keep the lexical path so later O_NOFOLLOW checks can still detect symlinks.
    return Path(os.path.abspath(os.fspath(Path(value).expanduser())))


_DIRECTORY_OPEN_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)


def _open_directory_components(path: Path, *, create: bool) -> int:
    absolute = _expand_path(path)
    descriptor = os.open(absolute.anchor, _DIRECTORY_OPEN_FLAGS)
    try:
        for component in absolute.parts[1:]:
            try:
                child = os.open(component, _DIRECTORY_OPEN_FLAGS, dir_fd=descriptor)
            except FileNotFoundError:
                if not create:
                    raise
                with contextlib.suppress(FileExistsError):
                    os.mkdir(component, mode=0o700, dir_fd=descriptor)
                child = os.open(component, _DIRECTORY_OPEN_FLAGS, dir_fd=descriptor)
            except OSError as exc:
                raise RuntimeError(
                    f"Private path contains a symlink or non-directory component: {absolute}"
                ) from exc
            os.close(descriptor)
            descriptor = child
        metadata = os.fstat(descriptor)
        if not stat.S_ISDIR(metadata.st_mode):
            raise RuntimeError(f"Private state path is not a real directory: {absolute}")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def open_private_directory(path: Path, *, create: bool = True) -> int:
    """Open a user-owned private directory without following any symlink component."""
    descriptor = _open_directory_components(path, create=create)
    try:
        metadata = os.fstat(descriptor)
        if metadata.st_uid != os.getuid():
            raise RuntimeError(f"Private state path has an unexpected owner: {_expand_path(path)}")
        os.fchmod(descriptor, 0o700)
        tightened = os.fstat(descriptor)
        if stat.S_IMODE(tightened.st_mode) != 0o700:
            raise RuntimeError(f"Private state path permissions could not be secured: {_expand_path(path)}")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _load_config_document(path: Path) -> dict[str, Any] | None:
    try:
        parent_descriptor = _open_directory_components(path.parent, create=False)
    except FileNotFoundError:
        return None
    except RuntimeError as exc:
        raise ValueError(f"Cannot securely access bridge configuration {path}: {exc}") from exc
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        try:
            descriptor = os.open(path.name, flags, dir_fd=parent_descriptor)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise ValueError(f"Bridge configuration must not be a symbolic link: {path}") from exc
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError(f"Bridge configuration is not a regular file: {path}")
            if metadata.st_uid not in {os.getuid(), 0}:
                raise ValueError(f"Bridge configuration has an unexpected owner: {path}")
            if stat.S_IMODE(metadata.st_mode) & 0o022:
                raise ValueError(f"Bridge configuration is writable by group or others: {path}")
            with os.fdopen(descriptor, "rb") as handle:
                descriptor = -1
                document = tomllib.load(handle)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
    finally:
        os.close(parent_descriptor)
    if not isinstance(document, dict):
        raise ValueError(f"Invalid bridge configuration in {path}")
    return document


def _normalize_bot_label(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    if any(
        unicodedata.category(character) == "Cc" or character in "\u2028\u2029"
        for character in value
    ):
        raise ValueError(f"{name} must not contain control characters or newlines")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    if len(normalized) > 40:
        raise ValueError(f"{name} must not exceed 40 characters")
    return normalized


def _normalize_optional_codex_setting(name: str, value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string or omitted")
    if any(
        unicodedata.category(character) == "Cc" or character in "\u2028\u2029"
        for character in value
    ):
        raise ValueError(f"{name} must not contain control characters or newlines")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty when configured")
    if len(normalized) > 128:
        raise ValueError(f"{name} must not exceed 128 characters")
    return normalized


def _read_private_text(path: Path, *, max_bytes: int = 4096) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        if isinstance(exc, FileNotFoundError):
            raise
        raise RuntimeError(f"Cannot securely open credential file {path}: {exc}") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise RuntimeError(f"Credential path is not a regular file: {path}")
        if metadata.st_uid not in {os.getuid(), 0}:
            raise RuntimeError(f"Credential file has an unexpected owner: {path}")
        if metadata.st_mode & 0o077:
            raise RuntimeError(f"Credential file must not be accessible by group or others: {path}")
        data = os.read(descriptor, max_bytes + 1)
        if len(data) > max_bytes:
            raise RuntimeError(f"Credential file is unexpectedly large: {path}")
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RuntimeError(f"Credential file is not valid UTF-8: {path}") from exc
    finally:
        os.close(descriptor)


@dataclass(frozen=True, slots=True)
class Config:
    config_dir: Path
    state_dir: Path
    codex_home: Path
    codex_socket: Path
    codex_binary: Path
    allowed_root: Path
    tmux_session: str = "CodexBot"
    dashboard_debounce_seconds: float = 2.0
    heartbeat_seconds: int = 60
    totp_unlock_seconds: int = 1800
    pair_code_seconds: int = 600
    callback_seconds: int = 300
    disconnect_threshold_seconds: int = 30
    upload_retention_days: int = 7
    telegram_download_limit: int = 20_000_000
    telegram_upload_limit: int = 50_000_000
    inbox_quota_bytes: int = 1_000_000_000
    minimum_free_bytes: int = 256_000_000
    control_bot_label: str = "Control Bot"
    discussion_bot_label: str = "Discussion Bot"
    ask_model: str | None = None
    ask_reasoning_effort: str | None = None

    def __post_init__(self) -> None:
        for name in ("control_bot_label", "discussion_bot_label"):
            object.__setattr__(self, name, _normalize_bot_label(name, getattr(self, name)))
        for name in ("ask_model", "ask_reasoning_effort"):
            object.__setattr__(
                self,
                name,
                _normalize_optional_codex_setting(name, getattr(self, name)),
            )
        positive = {
            "dashboard_debounce_seconds": self.dashboard_debounce_seconds,
            "heartbeat_seconds": self.heartbeat_seconds,
            "totp_unlock_seconds": self.totp_unlock_seconds,
            "pair_code_seconds": self.pair_code_seconds,
            "callback_seconds": self.callback_seconds,
            "disconnect_threshold_seconds": self.disconnect_threshold_seconds,
            "upload_retention_days": self.upload_retention_days,
            "telegram_download_limit": self.telegram_download_limit,
            "telegram_upload_limit": self.telegram_upload_limit,
            "inbox_quota_bytes": self.inbox_quota_bytes,
        }
        invalid = [
            name
            for name, value in positive.items()
            if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0
        ]
        if invalid:
            raise ValueError(f"Configuration values must be positive: {', '.join(invalid)}")
        if not isinstance(self.minimum_free_bytes, int) or self.minimum_free_bytes < 0:
            raise ValueError("minimum_free_bytes must be a non-negative integer")
        if self.telegram_download_limit > self.inbox_quota_bytes:
            raise ValueError("telegram_download_limit must not exceed inbox_quota_bytes")
        if not self.tmux_session.strip() or any(character in self.tmux_session for character in ":.\\/"):
            raise ValueError("tmux_session contains unsupported characters")

    @classmethod
    def default(cls) -> Config:
        home = Path.home()
        codex_home = _expand_path(os.environ.get("CODEX_HOME", home / ".codex"))
        return cls(
            config_dir=home / ".config" / "codex-telegram-bridge",
            state_dir=home / ".local" / "state" / "codex-telegram-bridge",
            codex_home=codex_home,
            codex_socket=codex_home / "app-server-control" / "app-server-control.sock",
            codex_binary=home / ".local" / "bin" / "codex",
            allowed_root=home,
        )

    @classmethod
    def load(cls, path: Path | None = None) -> Config:
        default = cls.default()
        config_path = _expand_path(path or default.config_dir / "config.toml")
        values: dict[str, Any] = {field.name: getattr(default, field.name) for field in fields(cls)}
        document = _load_config_document(config_path)
        if document is not None:
            source = document.get("bridge", document)
            if not isinstance(source, dict):
                raise ValueError(f"Invalid bridge configuration in {config_path}")
            for key, value in source.items():
                if key not in values:
                    raise ValueError(f"Unknown configuration key: {key}")
                values[key] = value
        for key in ("config_dir", "state_dir", "codex_home", "codex_socket", "codex_binary", "allowed_root"):
            values[key] = _expand_path(values[key])
        return cls(**values)

    @property
    def database_path(self) -> Path:
        return self.state_dir / "state.sqlite3"

    @property
    def inbox_dir(self) -> Path:
        return self.state_dir / "inbox"

    @property
    def bot_token_path(self) -> Path:
        credentials = os.environ.get("CREDENTIALS_DIRECTORY")
        if credentials:
            candidate = Path(credentials) / "telegram_bot_token"
            if candidate.is_file():
                return candidate
        return self.config_dir / "telegram_bot_token"

    @property
    def forum_bot_token_path(self) -> Path:
        credentials = os.environ.get("CREDENTIALS_DIRECTORY")
        if credentials:
            candidate = Path(credentials) / "telegram_426_bot_token"
            if candidate.is_file():
                return candidate
        return self.config_dir / "telegram_426_bot_token"

    @property
    def discussion_bot_token_path(self) -> Path:
        return self.forum_bot_token_path

    def token_path(self, bot_role: str = "control") -> Path:
        normalized = bot_role.strip().casefold()
        if normalized in {"control", "controller", "primary", "9527"}:
            return self.bot_token_path
        if normalized in {"forum", "discussion", "comment", "426"}:
            return self.forum_bot_token_path
        raise ValueError(f"Unknown Telegram bot role: {bot_role}")

    @property
    def totp_secret_path(self) -> Path:
        credentials = os.environ.get("CREDENTIALS_DIRECTORY")
        if credentials:
            candidate = Path(credentials) / "totp_secret"
            if candidate.is_file():
                return candidate
        return self.config_dir / "totp_secret"

    def read_bot_token(self) -> str:
        try:
            token = _read_private_text(self.bot_token_path).strip()
        except FileNotFoundError as exc:
            raise RuntimeError(
                "Telegram bot token is not configured; run "
                f"`codex-tg configure-token` ({self.bot_token_path})"
            ) from exc
        if not token or ":" not in token:
            raise RuntimeError(f"Invalid Telegram bot token in {self.bot_token_path}")
        return token

    def read_forum_bot_token(self) -> str:
        return self.read_token("forum")

    def read_token(self, bot_role: str = "control") -> str:
        path = self.token_path(bot_role)
        try:
            token = _read_private_text(path).strip()
        except FileNotFoundError as exc:
            raise RuntimeError(
                "Telegram bot token is not configured; run "
                f"`codex-tg configure-tokens` ({path})"
            ) from exc
        if not token or ":" not in token:
            raise RuntimeError(f"Invalid Telegram bot token in {path}")
        return token


def ensure_private_directory(path: Path) -> None:
    descriptor = open_private_directory(path)
    os.close(descriptor)
