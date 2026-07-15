from __future__ import annotations

import fcntl
import hashlib
import os
import re
import shutil
import stat
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from .config import ensure_private_directory

SENSITIVE_PARTS = {
    ".cargo",
    ".direnv",
    ".aws",
    ".azure",
    ".codex",
    ".docker",
    ".git",
    ".gnupg",
    ".kube",
    ".oci",
    ".password-store",
    ".ssh",
    ".terraform.d",
}
SENSITIVE_PATHS = {
    (".config", "1password"),
    (".config", "codex"),
    (".config", "codex-telegram-bridge"),
    (".config", "containers"),
    (".config", "doctl"),
    (".config", "fish"),
    (".config", "gcloud"),
    (".config", "gh"),
    (".config", "google-cloud-sdk"),
    (".config", "heroku"),
    (".config", "openai"),
    (".config", "op"),
    (".config", "pip"),
    (".config", "pypoetry"),
    (".config", "rclone"),
    (".local", "share", "fish"),
    (".local", "share", "keyrings"),
    (".local", "state", "codex-telegram-bridge"),
}
SENSITIVE_NAMES = {
    ".curlrc",
    ".git-credentials",
    ".gitconfig",
    ".netrc",
    ".npmrc",
    ".pypirc",
    ".wgetrc",
    ".yarnrc",
    ".yarnrc.yml",
    "application_default_credentials.json",
    "auth.toml",
    "auth.json",
    "credentials",
    "credentials.json",
    "credentials.toml",
    "credentials.tfrc.json",
    "pip.conf",
    "rclone.conf",
    "runtime.env",
    "secrets.json",
    "telegram_bot_token",
    "token.json",
    "totp_secret",
}
SENSITIVE_SHELL_NAMES = {
    ".bash_history",
    ".bash_login",
    ".bash_profile",
    ".bashrc",
    ".fish_history",
    ".mysql_history",
    ".node_repl_history",
    ".profile",
    ".psql_history",
    ".python_history",
    ".zlogin",
    ".zprofile",
    ".zsh_history",
    ".zshenv",
    ".zshrc",
}
SENSITIVE_SUFFIXES = (".key", ".p12", ".pem", ".pfx")
_PART_SUFFIX = ".part"
_NAME_MAX_BYTES = 255
_SAFE_FILENAME_MAX_BYTES = _NAME_MAX_BYTES - len(_PART_SUFFIX.encode("ascii"))


class PathPolicyError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class FileCandidate:
    path: Path
    size: int
    modified_at: int
    device: int = 0
    inode: int = 0
    modified_ns: int = 0


@dataclass(slots=True)
class InboxDestination:
    inbox: Path
    final_path: Path
    part_path: Path
    expected_size: int | None
    max_size: int
    quota_bytes: int
    minimum_free_bytes: int
    _descriptor: int = field(repr=False)
    _renamed: bool = field(default=False, init=False, repr=False)
    _committed: bool = field(default=False, init=False, repr=False)

    def _open_metadata(self) -> os.stat_result:
        if self._descriptor < 0:
            raise PathPolicyError("下载临时文件已经关闭")
        metadata = os.fstat(self._descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise PathPolicyError("下载临时路径不是普通文件")
        return metadata

    def _validate_size(self, metadata: os.stat_result) -> None:
        if metadata.st_size > self.max_size:
            raise PathPolicyError(f"下载文件超过 {self.max_size} 字节限制")
        if self.expected_size is not None and metadata.st_size != self.expected_size:
            raise PathPolicyError(f"下载文件大小不完整: 预期 {self.expected_size}，实际 {metadata.st_size}")

    def _close_descriptor(self) -> None:
        if self._descriptor < 0:
            return
        descriptor, self._descriptor = self._descriptor, -1
        os.close(descriptor)

    def write_from(self, source: BinaryIO) -> int:
        """Copy a downloaded payload into the already-open no-follow temporary file."""
        try:
            self._open_metadata()
            os.ftruncate(self._descriptor, 0)
            os.lseek(self._descriptor, 0, os.SEEK_SET)
            total = 0
            while True:
                chunk = source.read(1024 * 1024)
                if not chunk:
                    break
                if not isinstance(chunk, (bytes, bytearray, memoryview)):
                    raise TypeError("Downloaded file stream must be binary")
                if total + len(chunk) > self.max_size:
                    raise PathPolicyError(f"下载文件超过 {self.max_size} 字节限制")
                view = memoryview(chunk)
                while view:
                    written = os.write(self._descriptor, view)
                    if written <= 0:
                        raise OSError("无法写入下载临时文件")
                    view = view[written:]
                total += len(chunk)
            metadata = self._open_metadata()
            self._validate_size(metadata)
            _check_inbox_capacity(
                self.inbox,
                additional_bytes=0,
                quota_bytes=self.quota_bytes,
                minimum_free_bytes=self.minimum_free_bytes,
            )
            os.fsync(self._descriptor)
            return total
        except BaseException:
            self.abort()
            raise

    def commit(self) -> Path:
        try:
            metadata = self._open_metadata()
            self._validate_size(metadata)
            _check_inbox_capacity(
                self.inbox,
                additional_bytes=0,
                quota_bytes=self.quota_bytes,
                minimum_free_bytes=self.minimum_free_bytes,
            )
            if self.final_path.exists() or self.final_path.is_symlink():
                raise PathPolicyError("下载目标已存在")
            part_metadata = os.stat(self.part_path, follow_symlinks=False)
            if (
                not stat.S_ISREG(part_metadata.st_mode)
                or part_metadata.st_dev != metadata.st_dev
                or part_metadata.st_ino != metadata.st_ino
            ):
                raise PathPolicyError("下载临时文件路径在下载期间发生变化")
            os.fchmod(self._descriptor, 0o600)
            os.fsync(self._descriptor)
            os.replace(self.part_path, self.final_path)
            self._renamed = True
            final_metadata = os.stat(self.final_path, follow_symlinks=False)
            if (
                not stat.S_ISREG(final_metadata.st_mode)
                or final_metadata.st_dev != metadata.st_dev
                or final_metadata.st_ino != metadata.st_ino
            ):
                raise PathPolicyError("下载文件在原子落盘时发生变化")
            directory_descriptor = os.open(
                self.final_path.parent,
                os.O_RDONLY
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
            final_metadata = os.stat(self.final_path, follow_symlinks=False)
            if (
                not stat.S_ISREG(final_metadata.st_mode)
                or final_metadata.st_dev != metadata.st_dev
                or final_metadata.st_ino != metadata.st_ino
            ):
                raise PathPolicyError("下载文件在落盘确认时发生变化")
        except BaseException:
            self.abort()
            raise
        self._committed = True
        self._close_descriptor()
        return self.final_path

    def abort(self) -> None:
        if self._committed:
            return
        with suppress(OSError):
            self._close_descriptor()
        with suppress(OSError):
            self.part_path.unlink(missing_ok=True)
        if self._renamed:
            with suppress(OSError):
                self.final_path.unlink(missing_ok=True)
        with suppress(OSError):
            self.final_path.parent.rmdir()


def _truncate_utf8(value: str, max_bytes: int) -> str:
    return value.encode("utf-8")[:max_bytes].decode("utf-8", errors="ignore")


def safe_filename(value: str, *, max_bytes: int = _SAFE_FILENAME_MAX_BYTES) -> str:
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    name = Path(value or "upload.bin").name
    name = re.sub(r"[^\w.()\-\u4e00-\u9fff]+", "_", name, flags=re.UNICODE).strip("._")
    name = name or "upload.bin"
    if len(name.encode("utf-8")) <= max_bytes:
        return name
    suffix = Path(name).suffix
    suffix_bytes = len(suffix.encode("utf-8"))
    if suffix and suffix_bytes < max_bytes:
        stem = _truncate_utf8(name[: -len(suffix)], max_bytes - suffix_bytes)
        if stem:
            return stem + suffix
    return _truncate_utf8(name, max_bytes) or "upload.bin"


def _safe_scope(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_-]+", "_", value).strip("_")[:80]
    digest = hashlib.sha256(value.encode("utf-8", errors="replace")).hexdigest()[:12]
    return f"{normalized or 'session'}-{digest}"


class PathPolicy:
    def __init__(self, root: Path, upload_limit: int) -> None:
        try:
            resolved_root = root.expanduser().resolve(strict=True)
        except OSError as exc:
            raise PathPolicyError(f"允许的根目录不存在: {root}") from exc
        if not resolved_root.is_dir():
            raise PathPolicyError(f"允许的根路径不是目录: {root}")
        if upload_limit <= 0:
            raise ValueError("upload_limit must be positive")
        self.root = resolved_root
        self.upload_limit = upload_limit
        metadata = os.stat(self.root, follow_symlinks=False)
        self._root_identity = (metadata.st_dev, metadata.st_ino)

    def _inside_root(self, path: Path) -> bool:
        return path == self.root or self.root in path.parents

    def _sensitive(self, path: Path) -> bool:
        relative = path.relative_to(self.root)
        parts = tuple(part.casefold() for part in relative.parts)
        if any(part in SENSITIVE_PARTS or part.startswith(".env") for part in parts):
            return True
        if any(
            part == name or part == f"{name}~" or part == f".#{name}" or part.startswith(f"{name}.")
            for part in parts
            for name in SENSITIVE_SHELL_NAMES
        ):
            return True
        if any(
            parts[index : index + len(prefix)] == prefix
            for prefix in SENSITIVE_PATHS
            for index in range(len(parts))
        ):
            return True
        if parts and parts[-1] in SENSITIVE_NAMES:
            return True
        return bool(parts and parts[-1].endswith(SENSITIVE_SUFFIXES))

    def _resolve_inside_root(self, value: str | Path) -> Path:
        lexical = Path(value).expanduser()
        if not lexical.is_absolute():
            lexical = Path.cwd() / lexical
        lexical = Path(os.path.abspath(lexical))
        if not self._inside_root(lexical):
            raise PathPolicyError(f"路径必须位于 {self.root} 下")
        try:
            candidate = lexical.resolve(strict=True)
        except OSError as exc:
            raise PathPolicyError(f"路径不存在或无法解析: {value}") from exc
        if not self._inside_root(candidate):
            raise PathPolicyError(f"路径必须位于 {self.root} 下")
        if self._sensitive(lexical) or self._sensitive(candidate):
            raise PathPolicyError("拒绝访问敏感路径")
        return candidate

    def validate_directory(self, value: str | Path) -> Path:
        candidate = self._resolve_inside_root(value)
        try:
            metadata = os.stat(candidate, follow_symlinks=False)
        except OSError as exc:
            raise PathPolicyError(f"无法读取目录: {candidate}") from exc
        if not stat.S_ISDIR(metadata.st_mode):
            raise PathPolicyError("目标不是目录")
        return candidate

    def validate_file(self, value: str | Path, *, check_size: bool = True) -> FileCandidate:
        path = self._resolve_inside_root(value)
        try:
            metadata = os.stat(path, follow_symlinks=False)
        except OSError as exc:
            raise PathPolicyError(f"无法读取文件: {path}") from exc
        if not stat.S_ISREG(metadata.st_mode):
            raise PathPolicyError(f"文件必须是 {self.root} 下的普通文件")
        if check_size and metadata.st_size > self.upload_limit:
            raise PathPolicyError(f"文件大小 {metadata.st_size} 超过 Telegram 限制 {self.upload_limit}")
        return FileCandidate(
            path=path,
            size=metadata.st_size,
            modified_at=int(metadata.st_mtime),
            device=metadata.st_dev,
            inode=metadata.st_ino,
            modified_ns=metadata.st_mtime_ns,
        )

    @contextmanager
    def open_outbound(self, value: str | Path | FileCandidate) -> Iterator[BinaryIO]:
        expected = value if isinstance(value, FileCandidate) else None
        current = self.validate_file(expected.path if expected else value)
        baseline = expected or current
        descriptor = self._open_anchored(current.path)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise PathPolicyError("拒绝发送非普通文件")
            if metadata.st_size > self.upload_limit:
                raise PathPolicyError(f"文件大小 {metadata.st_size} 超过 Telegram 限制 {self.upload_limit}")
            if baseline.device and (
                metadata.st_dev != baseline.device
                or metadata.st_ino != baseline.inode
                or metadata.st_size != baseline.size
                or (baseline.modified_ns and metadata.st_mtime_ns != baseline.modified_ns)
            ):
                raise PathPolicyError("文件在确认后发生变化，请重新选择")
            status_flags = fcntl.fcntl(descriptor, fcntl.F_GETFL)
            fcntl.fcntl(descriptor, fcntl.F_SETFL, status_flags & ~os.O_NONBLOCK)
            with os.fdopen(descriptor, "rb") as handle:
                descriptor = -1
                yield handle
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def _open_anchored(self, path: Path) -> int:
        relative = path.relative_to(self.root)
        if not relative.parts:
            raise PathPolicyError("根目录不是可发送文件")
        directory_flags = (
            getattr(os, "O_PATH", os.O_RDONLY)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            root_descriptor = os.open(self.root, directory_flags)
        except OSError as exc:
            raise PathPolicyError(f"无法安全打开根目录: {exc}") from exc
        descriptors = [root_descriptor]
        try:
            root_metadata = os.fstat(root_descriptor)
            if (root_metadata.st_dev, root_metadata.st_ino) != self._root_identity:
                raise PathPolicyError("允许的根目录已被替换")
            current = root_descriptor
            for part in relative.parts[:-1]:
                current = os.open(part, directory_flags, dir_fd=current)
                descriptors.append(current)
            file_flags = (
                os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            )
            return os.open(relative.parts[-1], file_flags, dir_fd=current)
        except OSError as exc:
            raise PathPolicyError(f"无法安全打开文件: {exc}") from exc
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)


def sha256_file(path: Path, *, policy: PathPolicy | None = None) -> str:
    digest = hashlib.sha256()
    manager = policy.open_outbound(path) if policy is not None else _open_nofollow(path)
    with manager as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@contextmanager
def _open_nofollow(path: Path) -> Iterator[BinaryIO]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise PathPolicyError("目标不是普通文件")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            yield handle
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def inbox_usage(inbox: Path) -> int:
    if not inbox.exists():
        return 0
    total = 0
    for root, directories, files in os.walk(inbox, followlinks=False):
        directories[:] = [name for name in directories if not (Path(root) / name).is_symlink()]
        for name in files:
            try:
                metadata = os.stat(Path(root) / name, follow_symlinks=False)
            except OSError:
                continue
            if stat.S_ISREG(metadata.st_mode):
                total += metadata.st_size
    return total


def _check_inbox_capacity(
    inbox: Path,
    *,
    additional_bytes: int,
    quota_bytes: int,
    minimum_free_bytes: int,
) -> None:
    if additional_bytes < 0 or quota_bytes <= 0 or minimum_free_bytes < 0:
        raise ValueError("Invalid inbox capacity limits")
    used = inbox_usage(inbox)
    if used + additional_bytes > quota_bytes:
        raise PathPolicyError("Telegram 收件箱配额不足")
    free = shutil.disk_usage(inbox).free
    if free - additional_bytes < minimum_free_bytes:
        raise PathPolicyError("磁盘剩余空间不足，拒绝下载")


@contextmanager
def _inbox_lock(inbox: Path) -> Iterator[None]:
    lock_path = inbox / ".inbox.lock"
    descriptor = os.open(
        lock_path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def cleanup_inbox(
    inbox: Path,
    retention_days: int,
    *,
    protected_paths: Iterable[Path] = (),
) -> int:
    if retention_days <= 0:
        raise ValueError("retention_days must be positive")
    if not inbox.exists():
        return 0
    ensure_private_directory(inbox)
    protected = {Path(os.path.abspath(path.expanduser())) for path in protected_paths}
    cutoff = time.time() - retention_days * 86400
    removed = 0
    with _inbox_lock(inbox):
        for root, directories, files in os.walk(inbox, topdown=False, followlinks=False):
            root_path = Path(root)
            for name in files:
                path = root_path / name
                if path.name == ".inbox.lock" or Path(os.path.abspath(path)) in protected:
                    continue
                try:
                    metadata = os.stat(path, follow_symlinks=False)
                    if metadata.st_mtime < cutoff and (
                        stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode)
                    ):
                        path.unlink()
                        removed += 1
                except OSError:
                    continue
            for name in directories:
                path = root_path / name
                try:
                    metadata = os.stat(path, follow_symlinks=False)
                    if stat.S_ISLNK(metadata.st_mode):
                        if metadata.st_mtime < cutoff:
                            path.unlink()
                            removed += 1
                    elif stat.S_ISDIR(metadata.st_mode) and not any(path.iterdir()):
                        path.rmdir()
                except OSError:
                    continue
    return removed


def prepare_inbox_path(
    inbox: Path,
    thread_id: str,
    file_name: str,
    *,
    expected_size: int | None = None,
    download_limit: int = 20_000_000,
    quota_bytes: int = 1_000_000_000,
    minimum_free_bytes: int = 256_000_000,
) -> InboxDestination:
    if expected_size is not None and expected_size < 0:
        raise PathPolicyError("Telegram 文件大小无效")
    reservation = download_limit if expected_size is None else expected_size
    if download_limit <= 0 or reservation > download_limit:
        raise PathPolicyError(f"Telegram 文件超过 {download_limit} 字节限制")
    ensure_private_directory(inbox)
    with _inbox_lock(inbox):
        _check_inbox_capacity(
            inbox,
            additional_bytes=reservation,
            quota_bytes=quota_bytes,
            minimum_free_bytes=minimum_free_bytes,
        )
        directory = inbox / _safe_scope(thread_id) / f"{time.time_ns()}-{os.getpid()}"
        directory.mkdir(parents=True, exist_ok=False, mode=0o700)
        os.chmod(directory.parent, 0o700, follow_symlinks=False)
        final_path = directory / safe_filename(file_name)
        part_path = final_path.with_name(f"{final_path.name}{_PART_SUFFIX}")
        descriptor = os.open(
            part_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
    return InboxDestination(
        inbox=inbox,
        final_path=final_path,
        part_path=part_path,
        expected_size=expected_size,
        max_size=download_limit,
        quota_bytes=quota_bytes,
        minimum_free_bytes=minimum_free_bytes,
        _descriptor=descriptor,
    )
