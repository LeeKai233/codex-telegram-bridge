from __future__ import annotations

import argparse
import asyncio
import contextlib
import fcntl
import getpass
import hmac
import json
import os
import re
import secrets
import shlex
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

import segno
from telegram import Bot
from websockets.asyncio.client import unix_connect

from .app_server import RecoveryLock
from .config import AppServerMode, Config, ensure_private_directory, open_private_directory
from .security import Enrollment, SecurityManager
from .store import Store

CONTROL_TOKEN_VARIABLE = "TELEGRAM_9527_BOT_TOKEN"
TOKEN_VARIABLE = "TELEGRAM_GPT_BOT_TOKEN"
FORUM_TOKEN_VARIABLE = "TELEGRAM_426_BOT_TOKEN"
STATUS_TOKEN_VARIABLE = "TELEGRAM_69_BOT_TOKEN"
TOKEN_VARIABLES = (
    CONTROL_TOKEN_VARIABLE,
    TOKEN_VARIABLE,
    FORUM_TOKEN_VARIABLE,
    STATUS_TOKEN_VARIABLE,
)
_ASSIGNMENT = re.compile(rf"^[ \t]*(?:export[ \t]+)?{TOKEN_VARIABLE}[ \t]*=[ \t]*(?P<value>.*)$")
_TOKEN_SHAPE = re.compile(r"^[0-9]{6,16}:[A-Za-z0-9_-]{20,}$")


class CliError(RuntimeError):
    pass


class _AtomicWriteCommittedError(CliError):
    pass


@dataclass(frozen=True, slots=True)
class _CredentialSnapshot:
    path: Path
    existed: bool
    content: str = ""
    mode: int = 0o600
    device: int = 0
    inode: int = 0


@dataclass(slots=True)
class _CredentialTarget:
    path: Path
    token: str
    parent_descriptor: int
    snapshot: _CredentialSnapshot
    staged_name: str | None = None
    staged_descriptor: int = -1
    staged_identity: tuple[int, int] | None = None
    committed_identity: tuple[int, int] | None = None


def _assignment_value_for(line: str, variable: str) -> str | None:
    assignment = re.compile(
        rf"^[ \t]*(?:export[ \t]+)?{re.escape(variable)}[ \t]*=[ \t]*(?P<value>.*)$"
    )
    match = assignment.match(line.rstrip("\r\n"))
    if not match:
        return None
    lexer = shlex.shlex(match.group("value"), posix=True, punctuation_chars=";")
    lexer.whitespace_split = True
    lexer.commenters = "#"
    try:
        parts = list(lexer)
    except ValueError as exc:
        raise CliError(f"无法静态解析 {variable} 赋值") from exc
    if parts[-1:] == [";"]:
        parts.pop()
    if len(parts) != 1 or not _TOKEN_SHAPE.fullmatch(parts[0]):
        raise CliError(f"{variable} 必须是直接的 Bot token 赋值，不能包含变量展开或命令")
    return parts[0]


def _read_bashrc_tokens(
    path: Path,
    variables: tuple[str, ...] = TOKEN_VARIABLES,
    *,
    require_all: bool = True,
) -> tuple[dict[str, str], str, str, int]:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise CliError(f"未找到 shell 配置文件：{path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise CliError(f"拒绝从符号链接或非普通文件迁移凭据：{path}")
    original = path.read_text(encoding="utf-8")
    kept: list[str] = []
    candidates: dict[str, list[str]] = {variable: [] for variable in variables}
    for number, line in enumerate(original.splitlines(keepends=True), start=1):
        matched = False
        for variable in variables:
            try:
                value = _assignment_value_for(line, variable)
            except CliError as exc:
                raise CliError(f"{path}:{number}: {exc}") from None
            if value is not None:
                candidates[variable].append(value)
                matched = True
                break
        if not matched:
            kept.append(line)
    missing = [variable for variable, values in candidates.items() if not values]
    if missing and require_all:
        raise CliError(f"{path} 中没有找到以下直接赋值：{', '.join(missing)}")
    tokens = {variable: values[-1] for variable, values in candidates.items() if values}
    return tokens, original, "".join(kept), stat.S_IMODE(metadata.st_mode)


def _assignment_value(line: str) -> str | None:
    """Return a statically parsed token assignment, without evaluating shell code."""
    match = _ASSIGNMENT.match(line.rstrip("\r\n"))
    if not match:
        return None
    lexer = shlex.shlex(match.group("value"), posix=True, punctuation_chars=";")
    lexer.whitespace_split = True
    lexer.commenters = "#"
    try:
        parts = list(lexer)
    except ValueError as exc:
        raise CliError(f"无法静态解析 {TOKEN_VARIABLE} 赋值") from exc
    if parts[-1:] == [";"]:
        parts.pop()
    if len(parts) != 1 or not _TOKEN_SHAPE.fullmatch(parts[0]):
        raise CliError(f"{TOKEN_VARIABLE} 必须是直接的 Bot token 赋值，不能包含变量展开或命令")
    return parts[0]


def _read_bashrc_token(path: Path) -> tuple[str, str, str, int]:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise CliError(f"未找到 shell 配置文件：{path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise CliError(f"拒绝从符号链接或非普通文件迁移凭据：{path}")

    original = path.read_text(encoding="utf-8")
    kept: list[str] = []
    candidates: list[str] = []
    for number, line in enumerate(original.splitlines(keepends=True), start=1):
        try:
            value = _assignment_value(line)
        except CliError as exc:
            raise CliError(f"{path}:{number}: {exc}") from None
        if value is None:
            kept.append(line)
        else:
            candidates.append(value)
    if not candidates:
        raise CliError(f"{path} 中没有找到 {TOKEN_VARIABLE} 的直接赋值")
    return candidates[-1], original, "".join(kept), stat.S_IMODE(metadata.st_mode)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write_text(path: Path, content: str, mode: int, *, expected: str | None = None) -> None:
    if path.is_symlink():
        raise CliError(f"拒绝替换符号链接：{path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    replaced = False
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(
            descriptor,
            "w",
            encoding="utf-8",
            newline="",
            closefd=False,
        ) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        if stat.S_IMODE(os.fstat(descriptor).st_mode) != mode:
            raise CliError(f"临时文件权限校验失败，未替换：{path}")
        if expected is not None:
            try:
                current = path.read_text(encoding="utf-8")
            except FileNotFoundError as exc:
                raise CliError(f"写入期间文件消失：{path}") from exc
            if not hmac.compare_digest(current.encode("utf-8"), expected.encode("utf-8")):
                raise CliError(f"写入期间文件发生变化，未替换：{path}")
        os.replace(temporary, path)
        replaced = True
        _fsync_directory(path.parent)
    except BaseException:
        if replaced:
            raise _AtomicWriteCommittedError(
                f"文件已替换，但无法确认目录写入已持久化：{path}"
            ) from None
        raise
    finally:
        with contextlib.suppress(OSError):
            os.close(descriptor)
        if not replaced:
            temporary.unlink(missing_ok=True)


def _credential_snapshot_at(path: Path, parent_descriptor: int) -> _CredentialSnapshot:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path.name, flags, dir_fd=parent_descriptor)
    except FileNotFoundError:
        return _CredentialSnapshot(path=path, existed=False)
    except OSError:
        raise CliError(f"无法安全打开凭据文件：{path}") from None
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise CliError(f"拒绝替换非普通凭据文件：{path}")
        if metadata.st_uid != os.getuid():
            raise CliError(f"凭据文件所有者不是当前用户：{path}")
        if stat.S_IMODE(metadata.st_mode) & 0o077:
            raise CliError(f"凭据文件权限过宽：{path}")
        payload = os.read(descriptor, 4097)
        if len(payload) > 4096:
            raise CliError(f"凭据文件大小异常：{path}")
        try:
            content = payload.decode("utf-8")
        except UnicodeDecodeError:
            raise CliError(f"凭据文件不是有效的 UTF-8：{path}") from None
        return _CredentialSnapshot(
            path=path,
            existed=True,
            content=content,
            mode=stat.S_IMODE(metadata.st_mode),
            device=metadata.st_dev,
            inode=metadata.st_ino,
        )
    finally:
        os.close(descriptor)


def _credential_snapshot(path: Path) -> _CredentialSnapshot:
    parent_descriptor = open_private_directory(path.parent)
    try:
        return _credential_snapshot_at(path, parent_descriptor)
    finally:
        os.close(parent_descriptor)


def _lock_credential_directory(parent_descriptor: int, path: Path) -> int:
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(
            ".telegram-credentials.lock",
            flags,
            0o600,
            dir_fd=parent_descriptor,
        )
    except OSError:
        raise CliError(f"无法安全打开凭据事务锁：{path}") from None
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
            raise CliError(f"凭据事务锁类型或所有者无效：{path}")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _snapshot_matches_current(target: _CredentialTarget) -> bool:
    current = _credential_snapshot_at(target.path, target.parent_descriptor)
    snapshot = target.snapshot
    if current.existed != snapshot.existed:
        return False
    if not current.existed:
        return True
    return (
        (current.device, current.inode) == (snapshot.device, snapshot.inode)
        and current.mode == snapshot.mode
        and hmac.compare_digest(current.content.encode(), snapshot.content.encode())
    )


def _stage_private_text_at(
    path: Path,
    parent_descriptor: int,
    content: str,
    mode: int = 0o600,
) -> tuple[str, int, tuple[int, int]]:
    flags = (
        os.O_RDWR
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = -1
    temporary_name = ""
    try:
        for _attempt in range(128):
            temporary_name = f".{path.name}.{secrets.token_hex(12)}"
            try:
                descriptor = os.open(
                    temporary_name,
                    flags,
                    mode,
                    dir_fd=parent_descriptor,
                )
            except FileExistsError:
                continue
            break
        else:  # pragma: no cover - cryptographic names make exhaustion impractical
            raise CliError(f"无法为凭据创建临时文件：{path}")

        os.fchmod(descriptor, mode)
        payload = content.encode("utf-8")
        remaining = memoryview(payload)
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:  # pragma: no cover - regular-file writes either progress or raise
                raise OSError("credential write made no progress")
            remaining = remaining[written:]
        os.fsync(descriptor)
        metadata = os.fstat(descriptor)
        return temporary_name, descriptor, (metadata.st_dev, metadata.st_ino)
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary_name:
            with contextlib.suppress(OSError):
                os.unlink(temporary_name, dir_fd=parent_descriptor)
        raise


def _directory_path_matches_descriptor(path: Path, descriptor: int) -> bool:
    try:
        current_descriptor = open_private_directory(path, create=False)
    except (OSError, RuntimeError):
        return False
    try:
        current = os.fstat(current_descriptor)
        held = os.fstat(descriptor)
        return (current.st_dev, current.st_ino) == (held.st_dev, held.st_ino)
    finally:
        os.close(current_descriptor)


def _verify_descriptor_at(
    target: _CredentialTarget,
    *,
    descriptor: int,
    identity: tuple[int, int],
    expected: str,
    mode: int,
) -> None:
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
        raise CliError(f"凭据文件类型或所有者校验失败：{target.path}")
    if stat.S_IMODE(metadata.st_mode) != mode:
        raise CliError(f"凭据文件权限校验失败：{target.path}")
    if (metadata.st_dev, metadata.st_ino) != identity:
        raise CliError(f"凭据文件描述符在写入期间发生变化：{target.path}")

    encoded = expected.encode("utf-8")
    os.lseek(descriptor, 0, os.SEEK_SET)
    stored = os.read(descriptor, len(encoded) + 1)
    if not hmac.compare_digest(stored, encoded):
        raise CliError("凭据文件写入校验失败")

    try:
        destination = os.stat(
            target.path.name,
            dir_fd=target.parent_descriptor,
            follow_symlinks=False,
        )
    except OSError:
        raise CliError(f"凭据目标在写入期间被替换：{target.path}") from None
    if not stat.S_ISREG(destination.st_mode) or (
        destination.st_dev,
        destination.st_ino,
    ) != identity:
        raise CliError(f"凭据目标在写入期间被替换：{target.path}")
    if not _directory_path_matches_descriptor(target.path.parent, target.parent_descriptor):
        raise CliError(f"凭据目录在写入期间被替换：{target.path.parent}")


def _destination_matches_committed(target: _CredentialTarget) -> bool:
    if target.committed_identity is None:
        return False
    try:
        metadata = os.stat(
            target.path.name,
            dir_fd=target.parent_descriptor,
            follow_symlinks=False,
        )
    except OSError:
        return False
    return (metadata.st_dev, metadata.st_ino) == target.committed_identity


def _restore_credentials(targets: Sequence[_CredentialTarget]) -> None:
    for target in reversed(targets):
        if target.committed_identity is None:
            continue
        if not _destination_matches_committed(target):
            raise CliError(f"回滚时凭据目标已被其他操作替换：{target.path}")
        snapshot = target.snapshot
        if snapshot.existed:
            name, descriptor, identity = _stage_private_text_at(
                target.path,
                target.parent_descriptor,
                snapshot.content,
                snapshot.mode,
            )
            try:
                os.replace(
                    name,
                    target.path.name,
                    src_dir_fd=target.parent_descriptor,
                    dst_dir_fd=target.parent_descriptor,
                )
                name = ""
                os.fsync(target.parent_descriptor)
                _verify_descriptor_at(
                    target,
                    descriptor=descriptor,
                    identity=identity,
                    expected=snapshot.content,
                    mode=snapshot.mode,
                )
            finally:
                os.close(descriptor)
                if name:
                    with contextlib.suppress(OSError):
                        os.unlink(name, dir_fd=target.parent_descriptor)
        else:
            os.unlink(target.path.name, dir_fd=target.parent_descriptor)
            os.fsync(target.parent_descriptor)
        if not _directory_path_matches_descriptor(target.path.parent, target.parent_descriptor):
            raise CliError(f"回滚时凭据目录已被替换：{target.path.parent}")


def _install_token_files(
    destinations: dict[str, tuple[Path, str]],
    *,
    force: bool,
    finalize: Callable[[], None] | None = None,
) -> None:
    paths = [path for path, _token in destinations.values()]
    if len(set(paths)) != len(paths):
        raise CliError("多个 Bot token 不能写入同一个凭据文件")
    targets: list[_CredentialTarget] = []
    parent_descriptors: dict[Path, int] = {}
    lock_descriptors: list[int] = []
    try:
        for parent in sorted({path.parent for path in paths}, key=os.fspath):
            parent_descriptor = open_private_directory(parent)
            parent_descriptors[parent] = parent_descriptor
            lock_descriptors.append(_lock_credential_directory(parent_descriptor, parent))
        for path, token in destinations.values():
            parent_descriptor = parent_descriptors[path.parent]
            snapshot = _credential_snapshot_at(path, parent_descriptor)
            targets.append(
                _CredentialTarget(
                    path=path,
                    token=token,
                    parent_descriptor=parent_descriptor,
                    snapshot=snapshot,
                )
            )
        if not force and any(target.snapshot.existed for target in targets):
            raise CliError("Bot token 凭据已存在；确认替换时请添加 --force")

        for target in targets:
            (
                target.staged_name,
                target.staged_descriptor,
                target.staged_identity,
            ) = _stage_private_text_at(
                target.path,
                target.parent_descriptor,
                target.token + "\n",
            )
        for target in targets:
            if target.staged_name is None or target.staged_identity is None:
                raise CliError("凭据文件暂存状态无效")
            if not _snapshot_matches_current(target):
                raise CliError(f"凭据文件在配置期间发生变化，未覆盖并发更新：{target.path}")
            if force:
                os.replace(
                    target.staged_name,
                    target.path.name,
                    src_dir_fd=target.parent_descriptor,
                    dst_dir_fd=target.parent_descriptor,
                )
            else:
                try:
                    os.link(
                        target.staged_name,
                        target.path.name,
                        src_dir_fd=target.parent_descriptor,
                        dst_dir_fd=target.parent_descriptor,
                        follow_symlinks=False,
                    )
                except FileExistsError:
                    raise CliError(
                        f"凭据文件由另一个配置进程创建，未覆盖：{target.path}"
                    ) from None
                os.unlink(target.staged_name, dir_fd=target.parent_descriptor)
            target.staged_name = None
            target.committed_identity = target.staged_identity
            os.fsync(target.parent_descriptor)
            _verify_descriptor_at(
                target,
                descriptor=target.staged_descriptor,
                identity=target.staged_identity,
                expected=target.token + "\n",
                mode=0o600,
            )
        if finalize is not None:
            finalize()
    except BaseException as exc:
        if isinstance(exc, _AtomicWriteCommittedError):
            raise CliError(
                f"{exc}；Bot token 私有凭据已保留。请检查磁盘/WSL 状态与旧凭据源，"
                "并运行 `codex-tg doctor --offline` 检查凭据"
            ) from None
        if any(target.committed_identity is not None for target in targets):
            try:
                _restore_credentials(targets)
            except Exception:
                raise CliError("凭据更新失败且回滚未完整完成；请立即检查凭据文件") from None
        if isinstance(exc, (KeyboardInterrupt, SystemExit)):
            raise
        if isinstance(exc, CliError):
            raise
        raise CliError("凭据文件更新失败；原有凭据已恢复") from None
    finally:
        for target in targets:
            if target.staged_descriptor >= 0:
                os.close(target.staged_descriptor)
            if target.staged_name:
                with contextlib.suppress(OSError):
                    os.unlink(target.staged_name, dir_fd=target.parent_descriptor)
        for descriptor in reversed(lock_descriptors):
            with contextlib.suppress(OSError):
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)
        for descriptor in parent_descriptors.values():
            os.close(descriptor)


async def _validate_telegram_token(token: str) -> str:
    try:
        async with Bot(token=token) as bot:
            identity = await bot.get_me()
    except Exception:
        # PTB exceptions can contain the request URL. Never interpolate them here because
        # Bot API URLs contain the credential.
        raise CliError("Telegram getMe 验证失败；凭据未迁移，.bashrc 保持不变") from None
    return identity.username or str(identity.id)


async def _validate_distinct_tokens(
    control: str,
    forum: str,
    status: str | None = None,
    *,
    validator: Callable[[str], Awaitable[str]],
) -> dict[str, str]:
    tokens = {"control": control, "forum": forum}
    if status is not None:
        tokens["status"] = status
    if any(not _TOKEN_SHAPE.fullmatch(token) for token in tokens.values()):
        raise CliError("Bot token 格式无效；未写入任何凭据")
    token_values = list(tokens.values())
    if any(
        hmac.compare_digest(token, other)
        for index, token in enumerate(token_values)
        for other in token_values[index + 1 :]
    ):
        raise CliError("所有 Bot 必须使用不同的 token；未写入任何凭据")
    identity_values = await asyncio.gather(*(validator(token) for token in token_values))
    normalized_identities = [identity.casefold() for identity in identity_values]
    if any(
        hmac.compare_digest(identity, other)
        for index, identity in enumerate(normalized_identities)
        for other in normalized_identities[index + 1 :]
    ):
        raise CliError("多个 token 指向同一个 Bot；未写入任何凭据")
    return dict(zip(tokens, identity_values, strict=True))


def _rotation_requires_owner_reset(config: Config) -> bool:
    store = Store(config.database_path)
    try:
        if store.get_owner() is not None or store.get_telegram_binding() is not None:
            return True
        return any(space.get("lifecycle") != "closed" for space in store.list_spaces())
    finally:
        store.close()


def _token_paths(config: Config) -> dict[str, Path]:
    return {
        "control": config.bot_token_path,
        "forum": config.forum_bot_token_path,
        "status": config.status_bot_token_path,
    }


def _existing_token_state(
    config: Config,
) -> tuple[dict[str, str], dict[str, _CredentialSnapshot], _CredentialSnapshot]:
    snapshots = {role: _credential_snapshot(path) for role, path in _token_paths(config).items()}
    legacy = _credential_snapshot(config.legacy_bot_token_path)
    tokens = {
        role: snapshot.content.strip()
        for role, snapshot in snapshots.items()
        if snapshot.existed
    }
    if legacy.existed:
        legacy_token = legacy.content.strip()
        canonical = tokens.get("control")
        if canonical is not None and not hmac.compare_digest(canonical, legacy_token):
            raise CliError(
                "旧 telegram_bot_token 与 canonical telegram_9527_bot_token 内容冲突；"
                "未修改任何凭据，请人工确认正确的 9527 Bot token"
            )
        tokens.setdefault("control", legacy_token)
    return tokens, snapshots, legacy


def _finalize_token_sources(
    *,
    legacy: _CredentialSnapshot | None = None,
    bashrc: tuple[Path, str, str, int] | None = None,
) -> None:
    if legacy is None or not legacy.existed:
        if bashrc is not None:
            path, original, cleaned, mode = bashrc
            _atomic_write_text(path, cleaned, mode, expected=original)
        return

    parent_descriptor = open_private_directory(legacy.path.parent)
    backup_name = f".{legacy.path.name}.migrating-{secrets.token_hex(8)}"
    moved = False
    try:
        current = _credential_snapshot_at(legacy.path, parent_descriptor)
        if current != legacy:
            raise CliError(f"旧凭据文件在配置期间发生变化，未删除：{legacy.path}")
        os.rename(
            legacy.path.name,
            backup_name,
            src_dir_fd=parent_descriptor,
            dst_dir_fd=parent_descriptor,
        )
        moved = True
        try:
            os.fsync(parent_descriptor)
        except OSError:
            os.rename(
                backup_name,
                legacy.path.name,
                src_dir_fd=parent_descriptor,
                dst_dir_fd=parent_descriptor,
            )
            moved = False
            with contextlib.suppress(OSError):
                os.fsync(parent_descriptor)
            raise CliError(f"无法持久化旧凭据迁移：{legacy.path}") from None

        try:
            if bashrc is not None:
                path, original, cleaned, mode = bashrc
                _atomic_write_text(path, cleaned, mode, expected=original)
        except BaseException:
            try:
                os.rename(
                    backup_name,
                    legacy.path.name,
                    src_dir_fd=parent_descriptor,
                    dst_dir_fd=parent_descriptor,
                )
                moved = False
                os.fsync(parent_descriptor)
            except OSError:
                raise CliError("旧凭据迁移失败且源文件恢复失败；请立即检查凭据目录") from None
            raise

        try:
            os.unlink(backup_name, dir_fd=parent_descriptor)
            moved = False
        except OSError:
            try:
                os.rename(
                    backup_name,
                    legacy.path.name,
                    src_dir_fd=parent_descriptor,
                    dst_dir_fd=parent_descriptor,
                )
                moved = False
                os.fsync(parent_descriptor)
            except OSError:
                raise CliError("旧凭据迁移失败且源文件恢复失败；请立即检查凭据目录") from None
            raise CliError(f"无法删除已迁移的旧凭据文件：{legacy.path}") from None
        try:
            os.fsync(parent_descriptor)
        except OSError:
            raise _AtomicWriteCommittedError(
                f"旧凭据已迁移，但无法 fsync 目录 {legacy.path.parent}"
            ) from None
    finally:
        if moved:
            with contextlib.suppress(OSError):
                os.rename(
                    backup_name,
                    legacy.path.name,
                    src_dir_fd=parent_descriptor,
                    dst_dir_fd=parent_descriptor,
                )
                os.fsync(parent_descriptor)
        os.close(parent_descriptor)


async def migrate_bashrc_token(
    config: Config,
    bashrc: Path,
    *,
    validator: Callable[[str], Awaitable[str]] = _validate_telegram_token,
) -> str:
    token, original, cleaned, bashrc_mode = _read_bashrc_token(bashrc)
    identity = await validator(token)
    existing, _snapshots, legacy = _existing_token_state(config)
    current = existing.get("control")
    changed = current is None or not hmac.compare_digest(current, token)
    if changed and _rotation_requires_owner_reset(config):
        raise CliError(
            "检测到 Bot token 变更，但仍存在 Telegram 授权状态；"
            "请先运行 `codex-tg owner-reset` 再迁移凭据"
        )
    _install_token_files(
        {"control": (config.bot_token_path, token)},
        force=True,
        finalize=lambda: _finalize_token_sources(
            legacy=legacy,
            bashrc=(bashrc, original, cleaned, bashrc_mode),
        ),
    )
    return identity


async def migrate_bashrc_tokens(
    config: Config,
    bashrc: Path,
    *,
    validator: Callable[[str], Awaitable[str]] = _validate_telegram_token,
) -> dict[str, str]:
    tokens, original, cleaned, bashrc_mode = _read_bashrc_tokens(bashrc, require_all=False)
    shell_control = tokens.get(CONTROL_TOKEN_VARIABLE)
    legacy_shell_control = tokens.get(TOKEN_VARIABLE)
    if (
        shell_control is not None
        and legacy_shell_control is not None
        and not hmac.compare_digest(shell_control, legacy_shell_control)
    ):
        raise CliError(
            f"{CONTROL_TOKEN_VARIABLE} 与旧 {TOKEN_VARIABLE} 内容冲突；未修改任何凭据"
        )
    existing, _snapshots, legacy = _existing_token_state(config)
    values = {
        "control": shell_control or legacy_shell_control or existing.get("control"),
        "forum": tokens.get(FORUM_TOKEN_VARIABLE) or existing.get("forum"),
        "status": tokens.get(STATUS_TOKEN_VARIABLE) or existing.get("status"),
    }
    missing = [role for role, value in values.items() if value is None]
    if missing:
        raise CliError(
            "三个 Bot token 必须分别存在于 .bashrc 直接赋值或私有凭据文件中；"
            f"缺少角色：{', '.join(missing)}"
        )
    control = str(values["control"])
    forum = str(values["forum"])
    status = str(values["status"])
    identities = await _validate_distinct_tokens(control, forum, status, validator=validator)

    destinations = {
        "control": (config.bot_token_path, control),
        "forum": (config.forum_bot_token_path, forum),
        "status": (config.status_bot_token_path, status),
    }
    changed = any(
        role not in existing or not hmac.compare_digest(existing[role], token)
        for role, token in (("control", control), ("forum", forum), ("status", status))
    )
    if changed and _rotation_requires_owner_reset(config):
        raise CliError(
            "检测到 Bot token 变更，但仍存在 Telegram 授权状态；"
            "请先运行 `codex-tg owner-reset` 再迁移凭据"
        )
    _install_token_files(
        destinations,
        force=True,
        finalize=lambda: _finalize_token_sources(
            legacy=legacy,
            bashrc=(bashrc, original, cleaned, bashrc_mode),
        ),
    )
    return identities


async def configure_prompt_tokens(
    config: Config,
    *,
    force: bool = False,
    fill_missing: bool = False,
    token_reader: Callable[[str], str] = getpass.getpass,
    validator: Callable[[str], Awaitable[str]] = _validate_telegram_token,
) -> dict[str, str]:
    if force and fill_missing:
        raise CliError("--force 与 --fill-missing 不能同时使用")
    try:
        ensure_private_directory(config.config_dir)
        existing, snapshots, legacy = _existing_token_state(config)
    except (CliError, RuntimeError) as exc:
        raise CliError(f"无法安全准备 Telegram 凭据目录：{exc}") from None
    if not force and not fill_missing and (existing or legacy.existed):
        raise CliError("Bot token 凭据已存在；补齐升级请添加 --fill-missing，确认替换请添加 --force")

    values = dict(existing) if fill_missing else {}
    labels = {
        "control": config.control_bot_label,
        "forum": config.discussion_bot_label,
        "status": "Status Bot",
    }
    try:
        for role in ("control", "forum", "status"):
            if role not in values:
                values[role] = token_reader(
                    f"输入 {labels[role]} token（输入内容不会显示）："
                ).strip()
    except (EOFError, OSError):
        raise CliError("无法从终端安全读取 Bot token") from None

    control = values["control"]
    forum = values["forum"]
    status = values["status"]
    identities = await _validate_distinct_tokens(control, forum, status, validator=validator)
    changed = any(
        role not in existing or not hmac.compare_digest(existing[role], token)
        for role, token in (("control", control), ("forum", forum), ("status", status))
    )
    if force and changed and _rotation_requires_owner_reset(config):
        raise CliError(
            "检测到 Bot token 变更，但仍存在 owner、频道绑定或未关闭 SessionSpace；"
            "请先运行 `codex-tg owner-reset`，再重新执行 token 配置，随后运行 "
            "`systemctl --user restart codex-telegram-bridge` 和 `codex-tg onboard`"
        )
    paths = _token_paths(config)
    destinations = (
        {
            role: (paths[role], values[role])
            for role, snapshot in snapshots.items()
            if not snapshot.existed
        }
        if fill_missing
        else {role: (paths[role], values[role]) for role in paths}
    )
    if legacy.existed:
        destinations["control"] = (paths["control"], control)
    if destinations:
        _install_token_files(
            destinations,
            force=force or legacy.existed,
            finalize=(
                (lambda: _finalize_token_sources(legacy=legacy)) if legacy.existed else None
            ),
        )
    return identities


def _config_from_args(args: argparse.Namespace) -> Config:
    return Config.load(args.config.expanduser() if args.config else None)


def _with_store(config: Config) -> Store:
    return Store(config.database_path)


def _enroll(
    security: SecurityManager,
    enrollment: Enrollment,
    *,
    output: TextIO,
    code_reader: Callable[[str], str] = getpass.getpass,
) -> None:
    output.write("请用认证器扫描二维码（该二维码包含 TOTP 密钥）：\n")
    segno.make(enrollment.provisioning_uri, error="M").terminal(out=output, compact=True)
    output.write(f"\n无法扫码时可手动输入：{enrollment.secret}\n")
    code = code_reader("输入认证器生成的 6 位验证码：").strip()
    if not security.commit_enrollment(enrollment, code):
        raise CliError("验证码无效；未保存新的 TOTP 密钥")
    output.write("TOTP 已启用。以下恢复码仅显示一次，请离线保存：\n")
    output.write("\n".join(enrollment.recovery_codes) + "\n")


def _confirm(prompt: str, expected: str, *, reader: Callable[[str], str] = input) -> None:
    if reader(prompt).strip() != expected:
        raise CliError("操作已取消")


def _mode_is_private(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return (
        stat.S_ISREG(metadata.st_mode)
        and not stat.S_ISLNK(metadata.st_mode)
        and not (stat.S_IMODE(metadata.st_mode) & 0o077)
    )


def _socket_is_private(path: Path) -> bool:
    try:
        metadata = path.stat()
    except FileNotFoundError:
        return False
    return stat.S_ISSOCK(metadata.st_mode) and not (stat.S_IMODE(metadata.st_mode) & 0o077)


def _bashrc_contains_token(path: Path) -> bool:
    if not path.is_file():
        return False
    return any(_ASSIGNMENT.match(line) for line in path.read_text(encoding="utf-8").splitlines())


def _bashrc_token_variables(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    found: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        for variable in TOKEN_VARIABLES:
            assignment = re.compile(
                rf"^[ \t]*(?:export[ \t]+)?{re.escape(variable)}[ \t]*="
            )
            if assignment.match(line):
                found.add(variable)
    return found


def _binding_issues(binding: dict[str, object] | None) -> list[str]:
    if binding is None:
        return ["尚未绑定频道与讨论组"]
    issues: list[str] = []
    for field in ("channel_chat_id", "discussion_chat_id"):
        value = binding.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value == 0:
            issues.append(f"{field} 缺失或无效")
    control_id = binding.get("control_bot_id", binding.get("controller_bot_id"))
    forum_id = binding.get("forum_bot_id", binding.get("discussion_bot_id"))
    if isinstance(control_id, bool) or not isinstance(control_id, int) or control_id <= 0:
        issues.append("control_bot_id 缺失或无效")
    if isinstance(forum_id, bool) or not isinstance(forum_id, int) or forum_id <= 0:
        issues.append("forum_bot_id 缺失或无效")
    if binding.get("is_forum") is True:
        issues.append("讨论组启用了 Forum Topics")
    if binding.get("channel_chat_id") == binding.get("discussion_chat_id"):
        issues.append("频道与讨论组不能是同一个 chat")
    return issues


async def _wait_for_store_state(
    ready: Callable[[], bool],
    *,
    timeout: int,
    stage: str,
    clock: Callable[[], float],
    sleeper: Callable[[float], Awaitable[None]],
) -> None:
    deadline = clock() + timeout
    while not ready():
        remaining = deadline - clock()
        if remaining <= 0:
            raise CliError(f"等待 {stage} 超时；请重新运行 `codex-tg onboard`")
        await sleeper(min(1.0, remaining))


def _clear_one_time_code(store: Store, prefix: str) -> None:
    store.set_meta_many(
        {
            f"{prefix}_code_salt": "",
            f"{prefix}_code_digest": "",
            f"{prefix}_code_expires": 0,
            f"{prefix}_code_failures": 0,
        }
    )


async def _doctor(config: Config, *, offline: bool, output: TextIO) -> int:
    failures = 0

    def report(level: str, text: str) -> None:
        nonlocal failures
        if level == "FAIL":
            failures += 1
        output.write(f"[{level}] {text}\n")

    report("OK", f"Python {sys.version_info.major}.{sys.version_info.minor}")

    identities: dict[str, str] = {}
    for role, path in _token_paths(config).items():
        if _mode_is_private(path):
            report("OK", f"Telegram {role} token 文件存在且权限为私有")
            if not offline:
                try:
                    identities[role] = await _validate_telegram_token(config.read_token(role))
                except Exception:
                    report("FAIL", f"Telegram {role} getMe 验证失败")
                else:
                    report("OK", f"Telegram {role} getMe 验证成功")
        else:
            report("FAIL", f"Telegram {role} token 缺失、不是普通文件或权限过宽：{path}")
    if not offline and len(identities) == 3:
        normalized = {identity.casefold() for identity in identities.values()}
        if len(normalized) != 3:
            report("FAIL", "control、forum 与 status token 必须指向三个不同的 Bot")

    if os.path.lexists(config.legacy_bot_token_path):
        report(
            "FAIL",
            "仍存在旧 telegram_bot_token；请运行 "
            "`codex-tg configure-tokens --prompt --fill-missing` 完成一次性迁移",
        )
    else:
        report("OK", "旧 telegram_bot_token 已迁移或不存在")

    bashrc_variables = _bashrc_token_variables(Path.home() / ".bashrc")
    if bashrc_variables:
        report("FAIL", f"~/.bashrc 仍包含 Bot token 明文赋值：{', '.join(sorted(bashrc_variables))}")
    else:
        report("OK", "~/.bashrc 中没有 Bot token 的明文赋值")

    environment_tokens = [
        variable for variable in TOKEN_VARIABLES if os.environ.get(variable)
    ]
    if environment_tokens:
        report(
            "WARN",
            "当前进程环境仍含 Bot token；请新开 shell，并清理 systemd user manager 环境："
            + ", ".join(environment_tokens),
        )
    residue_candidates = [
        Path.home() / ".bashrc~",
        Path.home() / ".bashrc.bak",
        Path.home() / ".bashrc.backup",
        Path.home() / ".bash_history",
    ]
    residue = [path.name for path in residue_candidates if _bashrc_token_variables(path)]
    if residue:
        report("WARN", f"以下 shell 历史/备份仍有 token 赋值：{', '.join(residue)}")

    if _mode_is_private(config.totp_secret_path):
        report("OK", "TOTP 密钥存在且权限为私有")
    else:
        report("WARN", "TOTP 尚未配置，或密钥权限过宽")

    report("OK", f"Codex app-server mode: {config.app_server_mode.value}")
    if _socket_is_private(config.codex_socket):
        report("OK", "Codex app-server Unix socket 可用且权限为私有")
        if not offline:
            try:
                await _probe_app_server_protocol(config.codex_socket)
            except Exception as exc:
                report("FAIL", f"Codex app-server initialize 握手失败：{type(exc).__name__}")
            else:
                report("OK", "Codex app-server initialize 握手成功")
            if config.app_server_mode is AppServerMode.MANAGED_DAEMON:
                daemon_version_ok = _managed_daemon_version_available(config)
                report(
                    "OK" if daemon_version_ok else "FAIL",
                    "Codex managed daemon version 查询成功"
                    if daemon_version_ok
                    else "Codex managed daemon version 查询失败",
                )
    else:
        report("FAIL", f"Codex app-server socket 不可用或权限过宽：{config.codex_socket}")

    codex = config.codex_binary
    if codex.is_file() and os.access(codex, os.X_OK):
        report("OK", f"Codex CLI 可执行文件：{codex}")
    else:
        report("FAIL", f"Codex CLI 不可执行：{codex}")
    tmux = shutil.which("tmux")
    report("OK" if tmux else "FAIL", "tmux 可用" if tmux else "未找到 tmux")

    try:
        store = _with_store(config)
    except Exception:
        report("FAIL", "状态数据库无法打开")
    else:
        try:
            owner = store.get_owner()
            report("OK" if owner else "WARN", "Telegram owner 已配对" if owner else "尚未配对 owner")
            binding_issues = _binding_issues(store.get_telegram_binding())
            if binding_issues:
                report("WARN", "Telegram 频道绑定未就绪：" + "；".join(binding_issues))
            else:
                report("OK", "Telegram 频道与讨论组绑定结构有效")
            report("OK", f"状态数据库 schema v{store.schema_version}")
        finally:
            store.close()
    return 1 if failures else 0


async def onboard(
    config: Config,
    *,
    timeout: int = 600,
    output: TextIO,
    code_reader: Callable[[str], str] = getpass.getpass,
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    doctor: Callable[..., Awaitable[int]] = _doctor,
    store_factory: Callable[[Config], Store] = _with_store,
) -> int:
    if timeout <= 0:
        raise CliError("--timeout 必须是正整数秒数")

    store = store_factory(config)
    try:
        security = SecurityManager(store, config.totp_secret_path, config.totp_unlock_seconds)
        if security.configured:
            output.write("TOTP 已配置，跳过注册。\n")
        else:
            output.write("开始注册 TOTP。\n")
            _enroll(
                security,
                security.begin_enrollment(),
                output=output,
                code_reader=code_reader,
            )

        if store.get_owner() is not None:
            output.write("Telegram owner 已配对，跳过配对。\n")
        else:
            code = security.create_pair_code(max(config.pair_code_seconds, timeout))
            output.write(f"配对码：{code}\n")
            output.write(
                f"请在 {config.control_bot_label} 私聊发送 /pair <配对码>，正在等待配对完成。\n"
            )
            try:
                await _wait_for_store_state(
                    lambda: store.get_owner() is not None,
                    timeout=timeout,
                    stage="owner 配对",
                    clock=clock,
                    sleeper=sleeper,
                )
            finally:
                _clear_one_time_code(store, "pair")
            output.write("Telegram owner 配对完成。\n")

        if not _binding_issues(store.get_telegram_binding()):
            output.write("频道与讨论组绑定有效，跳过绑定。\n")
        else:
            code = security.create_bind_code(max(config.pair_code_seconds, timeout))
            output.write(f"绑定码：{code}\n")
            output.write(
                f"请在已关联的讨论组向 {config.discussion_bot_label} 发送 /bind <绑定码>，"
                "正在等待绑定完成。\n"
            )
            try:
                await _wait_for_store_state(
                    lambda: not _binding_issues(store.get_telegram_binding()),
                    timeout=timeout,
                    stage="频道绑定",
                    clock=clock,
                    sleeper=sleeper,
                )
            finally:
                _clear_one_time_code(store, "bind")
            output.write("频道与讨论组绑定完成。\n")
    finally:
        store.close()

    output.write("引导阶段完成，开始运行最终检查。\n")
    return await doctor(config, offline=False, output=output)


def _read_status_database(path: Path) -> dict[str, object]:
    sizes = {
        "database_bytes": path.stat().st_size if path.is_file() else 0,
        "wal_bytes": Path(f"{path}-wal").stat().st_size if Path(f"{path}-wal").is_file() else 0,
        "shm_bytes": Path(f"{path}-shm").stat().st_size if Path(f"{path}-shm").is_file() else 0,
    }
    empty: dict[str, object] = {
        **sizes,
        "schema_version": 0,
        "owner_paired": False,
        "subscriptions": 0,
        "binding": None,
        "spaces": [],
        "auth_epoch": 0,
        "force_locked": True,
        "pending_disconnect": False,
        "runtime_active": False,
        "health": None,
    }
    if not path.is_file():
        return empty
    uri = f"{path.resolve().as_uri()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=1.0)
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        tables = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }

        def meta(name: str, default: object) -> object:
            if "metadata" not in tables:
                return default
            row = connection.execute("SELECT value FROM metadata WHERE key=?", (name,)).fetchone()
            return json.loads(str(row[0])) if row is not None else default

        owner_paired = bool(
            connection.execute("SELECT 1 FROM owner WHERE singleton=1").fetchone()
        ) if "owner" in tables else False
        subscriptions = int(
            connection.execute("SELECT COUNT(*) FROM subscriptions WHERE active=1").fetchone()[0]
        ) if "subscriptions" in tables else 0
        binding: dict[str, object] | None = None
        if "telegram_binding" in tables:
            row = connection.execute(
                "SELECT binding_json FROM telegram_binding WHERE singleton=1"
            ).fetchone()
            if row is not None:
                value = json.loads(str(row[0]))
                binding = value if isinstance(value, dict) else None
        spaces: list[dict[str, object]] = []
        if "session_spaces" in tables:
            spaces = [
                {"lifecycle": str(row[0]), "count": int(row[1])}
                for row in connection.execute(
                    "SELECT lifecycle, COUNT(*) FROM session_spaces GROUP BY lifecycle"
                )
            ]
        health: dict[str, object] | None = None
        if "health_snapshots" in tables:
            row = connection.execute(
                "SELECT snapshot_json, updated_at FROM health_snapshots WHERE singleton=1"
            ).fetchone()
            if row is not None:
                value = json.loads(str(row[0]))
                health = value if isinstance(value, dict) else {}
                health["updated_at"] = int(row[1])
        return {
            **sizes,
            "schema_version": int(connection.execute("PRAGMA user_version").fetchone()[0]),
            "owner_paired": owner_paired,
            "subscriptions": subscriptions,
            "binding": binding,
            "spaces": spaces,
            "auth_epoch": int(meta("totp_auth_epoch", 0)),
            "force_locked": bool(meta("totp_force_locked", True)),
            "pending_disconnect": bool(meta("telegram_disconnect_pending", False)),
            "runtime_active": bool(meta("telegram_runtime_active", False)),
            "health": health,
        }
    finally:
        connection.close()


def _status(config: Config, output: TextIO, *, as_json: bool = False) -> int:
    database = _read_status_database(config.database_path)
    owner_paired = bool(database["owner_paired"])
    binding = database["binding"]
    spaces = {
        str(row["lifecycle"]): int(row["count"])
        for row in database["spaces"]  # type: ignore[union-attr]
    }
    health = database["health"]
    health_age = (
        max(0, int(time.time()) - int(health.get("updated_at") or 0))
        if isinstance(health, dict)
        else None
    )
    health_state = "missing" if health_age is None else ("fresh" if health_age <= 60 else "stale")
    control_token_status = (
        "configured/private" if _mode_is_private(config.bot_token_path) else "missing/insecure"
    )
    forum_token_status = (
        "configured/private" if _mode_is_private(config.forum_bot_token_path) else "missing/insecure"
    )
    status_token_status = (
        "configured/private" if _mode_is_private(config.status_bot_token_path) else "missing/insecure"
    )
    totp_status = "configured" if _mode_is_private(config.totp_secret_path) else "missing/insecure"
    socket_status = "ready" if _socket_is_private(config.codex_socket) else "unavailable/insecure"
    payload = {
        "app_server": {
            "mode": config.app_server_mode.value,
            "socket": socket_status,
        },
        "credentials": {
            "control_token": control_token_status,
            "forum_token": forum_token_status,
            "status_token": status_token_status,
            "totp": totp_status,
        },
        "owner": {"paired": owner_paired},
        "channel_binding": {"ready": not _binding_issues(binding)},
        "write_access": {"globally_locked": bool(database["force_locked"])},
        "watched_sessions": int(database["subscriptions"]),
        "session_spaces": {
            name: spaces.get(name, 0)
            for name in ("pending", "active", "closed", "repair_required")
        },
        "auth_epoch": int(database["auth_epoch"]),
        "database": {
            "schema_version": int(database["schema_version"]),
            "bytes": int(database["database_bytes"]),
            "wal_bytes": int(database["wal_bytes"]),
            "shm_bytes": int(database["shm_bytes"]),
        },
        "runtime": {
            "active_marker": bool(database["runtime_active"]),
            "pending_disconnect": bool(database["pending_disconnect"]),
            "health_state": health_state,
            "health_age_seconds": health_age,
            "health": health,
        },
        "codex_socket": socket_status,
    }
    if as_json:
        output.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
        return 0
    output.write(f"control token: {control_token_status}\n")
    output.write(f"forum token: {forum_token_status}\n")
    output.write(f"status token: {status_token_status}\n")
    output.write(f"owner: {'paired' if owner_paired else 'unpaired'}\n")
    output.write(f"channel binding: {'ready' if not _binding_issues(binding) else 'missing/invalid'}\n")
    output.write(f"totp: {totp_status}\n")
    output.write(
        "write access gate: "
        f"{'globally locked' if database['force_locked'] else 'space leases are process-local'}\n"
    )
    output.write(f"watched sessions: {database['subscriptions']}\n")
    output.write(
        "session spaces: "
        + " · ".join(
            f"{name}={spaces.get(name, 0)}"
            for name in ("pending", "active", "closed", "repair_required")
        )
        + "\n"
    )
    output.write(f"auth epoch: {database['auth_epoch']}\n")
    output.write(f"database schema: v{database['schema_version']}\n")
    output.write(
        f"database bytes: {database['database_bytes']} + wal {database['wal_bytes']}\n"
    )
    output.write(
        "last runtime marker: "
        f"{'active/unclean' if database['runtime_active'] else 'cleanly stopped'}\n"
    )
    output.write(
        "deferred disconnect emoji: "
        f"{'pending' if database['pending_disconnect'] else 'none'}\n"
    )
    output.write(f"health snapshot: {health_state}\n")
    output.write(f"app-server mode: {config.app_server_mode.value}\n")
    output.write(f"codex socket: {socket_status}\n")
    return 0


async def _probe_app_server_protocol(socket_path: Path) -> None:
    """Complete the harmless initialization handshake used by the bridge client."""
    websocket = await unix_connect(
        path=str(socket_path),
        uri="ws://localhost/",
        compression=None,
        user_agent_header=None,
        open_timeout=5,
        ping_interval=None,
        close_timeout=2,
    )
    try:
        request_id = 1
        await websocket.send(
            json.dumps(
                {
                    "id": request_id,
                    "method": "initialize",
                    "params": {
                        "clientInfo": {
                            "name": "codex_telegram_bridge_watchdog",
                            "title": "Codex Telegram Bridge Watchdog",
                            "version": "1",
                        },
                        "capabilities": {"experimentalApi": True},
                    },
                },
                separators=(",", ":"),
            )
        )
        while True:
            raw_message = await asyncio.wait_for(websocket.recv(), timeout=5)
            message = json.loads(raw_message)
            if message.get("id") != request_id:
                continue
            if not isinstance(message.get("result"), dict):
                raise RuntimeError("app-server initialize did not return a result")
            await websocket.send(json.dumps({"method": "initialized", "params": {}}))
            return
    finally:
        await websocket.close(code=1000, reason="watchdog probe complete")


def _managed_daemon_version_available(config: Config) -> bool:
    environment = os.environ.copy()
    environment["CODEX_HOME"] = str(config.codex_home)
    try:
        completed = subprocess.run(
            [str(config.codex_binary), "app-server", "daemon", "version"],
            check=False,
            capture_output=True,
            env=environment,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return completed.returncode == 0


def _app_server_watchdog(config: Config, output: TextIO, *, recover: bool = False) -> int:
    """Check only local app-server readiness; never opens Telegram state or credentials."""
    if config.app_server_mode is AppServerMode.EXTERNAL:
        output.write("app-server watchdog: external ownership; no action\n")
        return 0
    def healthy() -> bool:
        if not _socket_is_private(config.codex_socket):
            return False
        if (
            config.app_server_mode is AppServerMode.MANAGED_DAEMON
            and not _managed_daemon_version_available(config)
        ):
            return False
        try:
            asyncio.run(_probe_app_server_protocol(config.codex_socket))
        except Exception:
            return False
        return True

    if healthy():
        output.write(f"app-server watchdog: {config.app_server_mode.value} protocol ready\n")
        return 0
    if not recover or config.app_server_mode is not AppServerMode.MANAGED_DAEMON:
        output.write(
            f"app-server watchdog: {config.app_server_mode.value} protocol unavailable: "
            f"{config.codex_socket}\n"
        )
        return 1
    with RecoveryLock(config.state_dir / "app-server-recovery.lock") as acquired:
        if not acquired:
            output.write("app-server watchdog: recovery already in progress\n")
            return 0
        environment = os.environ.copy()
        environment["CODEX_HOME"] = str(config.codex_home)
        for command in (
            [str(config.codex_binary), "app-server", "daemon", "restart"],
            [str(config.codex_binary), "app-server", "daemon", "bootstrap"],
        ):
            try:
                completed = subprocess.run(
                    command,
                    check=False,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    env=environment,
                    timeout=30,
                )
            except (OSError, subprocess.TimeoutExpired):
                continue
            if completed.returncode == 0 and healthy():
                output.write("app-server watchdog: daemon recovered and protocol verified\n")
                return 0
    output.write("app-server watchdog: recovery failed\n")
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="codex-tg", description="Codex Telegram Bridge 本机管理工具")
    parser.add_argument("--config", type=Path, help="config.toml 路径")
    commands = parser.add_subparsers(dest="command", required=True)

    configure = commands.add_parser(
        "configure-token",
        aliases=["migrate-token"],
        help="验证并从 .bashrc 安全迁移 Bot token",
    )
    configure.add_argument("--bashrc", type=Path, default=Path.home() / ".bashrc")

    configure_all = commands.add_parser(
        "configure-tokens",
        help="交互录入或从 .bashrc 安全迁移三个 Bot token",
    )
    configure_all.add_argument("--bashrc", type=Path, default=Path.home() / ".bashrc")
    configure_all.add_argument("--prompt", action="store_true", help="从隐藏终端提示录入 token")
    configure_all.add_argument("--json", action="store_true", help="只输出验证后的 Bot 身份 JSON")
    replace_mode = configure_all.add_mutually_exclusive_group()
    replace_mode.add_argument("--force", action="store_true", help="允许 --prompt 替换已有凭据")
    replace_mode.add_argument(
        "--fill-missing",
        action="store_true",
        help="验证全部身份，只提示并写入缺失凭据，同时迁移旧 control 文件",
    )

    commands.add_parser("pair-code", help="生成一次性 owner 配对码")
    commands.add_parser("bind-code", help="生成一次性频道/讨论组绑定码")
    onboard_parser = commands.add_parser("onboard", help="完成 TOTP、owner 配对、频道绑定和检查")
    onboard_parser.add_argument("--timeout", type=int, default=600, help="每个等待阶段的超时秒数")
    commands.add_parser("totp-enroll", help="首次注册 TOTP")
    reset_totp = commands.add_parser("totp-reset", help="更换 TOTP 密钥并废弃旧恢复码")
    reset_totp.add_argument("--force", action="store_true", help="本机强制重置（需要再次确认）")
    reset_totp.add_argument("--yes", action="store_true", help="与 --force 一起跳过交互确认")

    reset_owner = commands.add_parser("owner-reset", help="删除已配对 owner")
    reset_owner.add_argument("--yes", action="store_true", help="跳过 RESET 确认")
    commands.add_parser("lock", help="立即撤销 Telegram 写操作解锁状态")

    doctor = commands.add_parser("doctor", help="检查凭据、权限、Codex socket 与依赖")
    doctor.add_argument("--offline", action="store_true", help="不调用 Telegram getMe")
    status = commands.add_parser("status", help="显示本机桥接状态（不显示秘密）")
    status.add_argument("--json", action="store_true", help="输出机器可读 JSON")
    watchdog = commands.add_parser(
        "app-server-watchdog", help="检查并按 ownership 恢复 Codex app-server；不访问 Telegram"
    )
    watchdog.add_argument("--recover", action="store_true", help="允许 managed-daemon 自动恢复")
    return parser


def run(args: argparse.Namespace, *, output: TextIO = sys.stdout) -> int:
    config = _config_from_args(args)
    if args.command in {"configure-token", "migrate-token"}:
        identity = asyncio.run(migrate_bashrc_token(config, args.bashrc.expanduser()))
        output.write(f"Bot @{identity} 验证成功；token 已迁移到私有凭据文件并从 .bashrc 删除。\n")
        return 0
    if args.command == "configure-tokens":
        if args.fill_missing and not args.prompt:
            raise CliError("--fill-missing 必须与 --prompt 一起使用")
        if args.prompt:
            identities = asyncio.run(
                configure_prompt_tokens(
                    config,
                    force=args.force,
                    fill_missing=args.fill_missing,
                )
            )
        else:
            identities = asyncio.run(migrate_bashrc_tokens(config, args.bashrc.expanduser()))
        if args.json:
            output.write(json.dumps(identities, ensure_ascii=False, separators=(",", ":")) + "\n")
        elif args.prompt:
            output.write(
                f"{config.control_bot_label} @{identities['control']} 与 "
                f"{config.discussion_bot_label} @{identities['forum']}、"
                f"Status Bot @{identities['status']} 验证成功；"
                "三个 token 已保存到私有凭据文件。\n"
            )
            if args.force:
                output.write(
                    "如 Bridge 服务正在运行，请执行 `systemctl --user restart "
                    "codex-telegram-bridge`，然后运行 `codex-tg onboard`。\n"
                )
        else:
            output.write(
                f"{config.control_bot_label} @{identities['control']} 与 "
                f"{config.discussion_bot_label} @{identities['forum']}、"
                f"Status Bot @{identities['status']} 验证成功；"
                "三个 token 已迁移到私有凭据文件并从 .bashrc 删除。\n"
            )
        return 0
    if args.command == "onboard":
        return asyncio.run(onboard(config, timeout=args.timeout, output=output))
    if args.command == "doctor":
        return asyncio.run(_doctor(config, offline=args.offline, output=output))
    if args.command == "status":
        return _status(config, output, as_json=args.json)
    if args.command == "app-server-watchdog":
        return _app_server_watchdog(config, output, recover=args.recover)

    store = _with_store(config)
    try:
        if args.command == "pair-code":
            if store.get_owner() is not None:
                raise CliError("owner 已配对；如需更换，请先运行 `codex-tg owner-reset`")
            security = SecurityManager(store, config.totp_secret_path, config.totp_unlock_seconds)
            code = security.create_pair_code(config.pair_code_seconds)
            output.write(f"配对码：{code}\n")
            minutes = config.pair_code_seconds // 60
            output.write(f"有效期：{minutes} 分钟；请在 Bot 私聊发送 /pair <配对码>。\n")
        elif args.command == "bind-code":
            if store.get_owner() is None:
                raise CliError(f"尚未配对 owner；请先完成 {config.control_bot_label} 私聊配对")
            security = SecurityManager(store, config.totp_secret_path, config.totp_unlock_seconds)
            code = security.create_bind_code(config.pair_code_seconds)
            output.write(f"绑定码：{code}\n")
            minutes = config.pair_code_seconds // 60
            output.write(f"有效期：{minutes} 分钟；请在已关联的讨论组发送 /bind <绑定码>。\n")
        elif args.command == "totp-enroll":
            security = SecurityManager(store, config.totp_secret_path, config.totp_unlock_seconds)
            if security.configured:
                raise CliError("TOTP 已配置；请使用 `codex-tg totp-reset`")
            _enroll(security, security.begin_enrollment(), output=output)
        elif args.command == "totp-reset":
            security = SecurityManager(store, config.totp_secret_path, config.totp_unlock_seconds)
            if security.configured and not args.force:
                current = getpass.getpass("输入当前 TOTP 或恢复码：").strip()
                if not security.verify(current):
                    raise CliError("当前 TOTP/恢复码验证失败")
            elif args.force and not args.yes:
                _confirm("这会废弃当前 TOTP 和全部恢复码。输入 RESET-TOTP 继续：", "RESET-TOTP")
            _enroll(security, security.begin_enrollment(), output=output)
        elif args.command == "owner-reset":
            if not args.yes:
                _confirm("这会删除唯一 owner。输入 RESET 继续：", "RESET")
            store.reset_owner()
            store.set_meta("totp_force_locked", True)
            store.set_meta("totp_unlocked_until", 0)
            output.write("owner 已删除；Telegram 写操作已锁定。\n")
        elif args.command == "lock":
            security = SecurityManager(store, config.totp_secret_path, config.totp_unlock_seconds)
            security.lock_all()
            output.write("Telegram 写操作已锁定。\n")
        else:  # pragma: no cover - argparse owns command validation
            raise CliError(f"未知命令：{args.command}")
    finally:
        store.close()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return run(args)
    except (CliError, ValueError) as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("操作已取消", file=sys.stderr)
        return 130
