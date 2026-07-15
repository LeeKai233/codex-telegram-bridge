from __future__ import annotations

import base64
import datetime as dt
import hashlib
import hmac
import os
import secrets
import stat
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import pyotp

from .config import ensure_private_directory
from .store import Store


@dataclass(frozen=True, slots=True)
class Enrollment:
    secret: str
    provisioning_uri: str
    recovery_codes: list[str]


def _recovery_digest(code: str, salt: bytes) -> str:
    digest = hashlib.scrypt(code.encode("utf-8"), salt=salt, n=2**14, r=8, p=1, dklen=32)
    return base64.b64encode(digest).decode("ascii")


def _matching_totp_timecode(secret: str, code: str, *, valid_window: int = 1) -> int | None:
    totp = pyotp.TOTP(secret)
    current = dt.datetime.now(dt.UTC)
    for offset in range(-valid_window, valid_window + 1):
        candidate_time = current + dt.timedelta(seconds=offset * totp.interval)
        if totp.verify(code, for_time=candidate_time, valid_window=0):
            return totp.timecode(candidate_time)
    return None


class SecurityManager:
    _LEGACY_SPACE = "__legacy__"

    def __init__(self, store: Store, secret_path: Path, unlock_seconds: int = 1800) -> None:
        if unlock_seconds <= 0:
            raise ValueError("unlock_seconds must be positive")
        self.store = store
        self.secret_path = secret_path
        self.unlock_seconds = unlock_seconds
        self._leases: dict[str, tuple[float, int]] = {}
        self._verify_lock = threading.RLock()

    def begin_enrollment(self, account: str = "owner") -> Enrollment:
        secret = pyotp.random_base32()
        uri = pyotp.TOTP(secret, name=account, issuer="Codex Telegram Bridge").provisioning_uri()
        codes = [f"{secrets.token_hex(3).upper()}-{secrets.token_hex(3).upper()}" for _ in range(10)]
        return Enrollment(secret=secret, provisioning_uri=uri, recovery_codes=codes)

    def commit_enrollment(self, enrollment: Enrollment, code: str) -> bool:
        try:
            timecode = _matching_totp_timecode(enrollment.secret, code.strip(), valid_window=1)
        except TypeError, ValueError:
            return False
        if timecode is None:
            return False
        ensure_private_directory(self.secret_path.parent)
        temporary = self.secret_path.with_name(
            f".{self.secret_path.name}.{os.getpid()}.{secrets.token_hex(6)}.tmp"
        )
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        descriptor = os.open(temporary, flags, 0o600)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(enrollment.secret + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.secret_path)
            directory_descriptor = os.open(
                self.secret_path.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
            )
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        entries: list[tuple[str, str]] = []
        for recovery in enrollment.recovery_codes:
            salt = secrets.token_bytes(16)
            entries.append((_recovery_digest(recovery, salt), base64.b64encode(salt).decode("ascii")))
        self.store.replace_recovery_codes(entries)
        self._leases.clear()
        epoch = self.store.increment_auth_epoch()
        self.store.set_meta_many(
            {
                "totp_last_timecode": int(timecode),
                "totp_unlocked_until": 0,
                "totp_force_locked": True,
                "totp_failures": 0,
                "totp_locked_until": 0,
                "totp_auth_epoch": epoch,
            }
        )
        return True

    @property
    def configured(self) -> bool:
        try:
            metadata = self.secret_path.lstat()
        except FileNotFoundError:
            return False
        return stat.S_ISREG(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode)

    def is_unlocked(self) -> bool:
        return self.is_space_unlocked(self._LEGACY_SPACE)

    @property
    def auth_epoch(self) -> int:
        return self.store.auth_epoch()

    def is_space_unlocked(self, space_id: str) -> bool:
        return self.space_unlock_remaining(space_id) > 0

    def space_unlock_remaining(self, space_id: str) -> int:
        if not space_id:
            return 0
        if bool(self.store.get_meta("totp_force_locked", True)):
            return 0
        lease = self._leases.get(space_id)
        if lease is None or lease[1] != self.auth_epoch:
            return 0
        return max(0, int(lease[0] - time.monotonic()))

    def lock(self) -> None:
        self.lock_all()

    def lock_space(self, space_id: str) -> None:
        with self._verify_lock:
            self._leases.pop(space_id, None)

    def lock_all(self) -> int:
        with self._verify_lock:
            self._leases.clear()
            return self.store.increment_auth_epoch()

    def reset_enrollment(self) -> None:
        with self._verify_lock:
            try:
                metadata = self.secret_path.lstat()
            except FileNotFoundError:
                metadata = None
            if metadata is not None:
                if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
                    raise RuntimeError(f"Refusing to remove unsafe TOTP path: {self.secret_path}")
                self.secret_path.unlink()
            self.store.replace_recovery_codes([])
            self._leases.clear()
            epoch = self.store.increment_auth_epoch()
            self.store.set_meta_many(
                {
                    "totp_last_timecode": -1,
                    "totp_unlocked_until": 0,
                    "totp_force_locked": True,
                    "totp_failures": 0,
                    "totp_locked_until": 0,
                    "totp_auth_epoch": epoch,
                }
            )

    def verify(self, value: str) -> bool:
        return self.verify_for_space(self._LEGACY_SPACE, value)

    def verify_for_space(self, space_id: str, value: str) -> bool:
        if not space_id:
            return False
        with self._verify_lock:
            return self._verify_locked(value, space_id)

    def _verify_locked(self, value: str, space_id: str) -> bool:
        now = int(time.time())
        if int(self.store.get_meta("totp_locked_until", 0)) > now:
            return False
        if self._verify_totp(value):
            self._record_success(now, space_id)
            return True
        if self._verify_recovery(value):
            self._record_success(now, space_id)
            return True
        self.store.record_totp_failure(now=now)
        return False

    def _record_success(self, now: int, space_id: str) -> None:
        epoch = self.auth_epoch
        self._leases[space_id] = (time.monotonic() + self.unlock_seconds, epoch)
        self.store.record_totp_success(now=now, unlock_seconds=self.unlock_seconds)

    def _verify_totp(self, value: str) -> bool:
        if not self.configured or not value.isdigit() or len(value) != 6:
            return False
        try:
            secret = self._read_secret()
            timecode = _matching_totp_timecode(secret, value, valid_window=1)
        except OSError, RuntimeError, TypeError, ValueError:
            return False
        if timecode is None:
            return False
        return self.store.accept_totp_timecode(int(timecode))

    def _read_secret(self) -> str:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self.secret_path, flags)
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise RuntimeError("TOTP secret is not a regular file")
            if metadata.st_uid not in {os.getuid(), 0} or metadata.st_mode & 0o077:
                raise RuntimeError("TOTP secret permissions are unsafe")
            payload = os.read(descriptor, 257)
            if len(payload) > 256:
                raise RuntimeError("TOTP secret file is unexpectedly large")
            return payload.decode("ascii").strip()
        finally:
            os.close(descriptor)

    def _verify_recovery(self, value: str) -> bool:
        normalized = value.strip().upper()
        if len(normalized) != 13 or normalized[6] != "-":
            return False
        if not all(character in "0123456789ABCDEF" for character in normalized.replace("-", "")):
            return False
        for expected, encoded_salt in self.store.unused_recovery_codes():
            try:
                salt = base64.b64decode(encoded_salt, validate=True)
            except TypeError, ValueError:
                continue
            candidate = _recovery_digest(normalized, salt)
            if hmac.compare_digest(candidate, expected):
                return self.store.consume_recovery_code(expected)
        return False

    def create_pair_code(self, ttl_seconds: int) -> str:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        code = secrets.token_hex(5).upper()
        salt = secrets.token_bytes(16)
        digest = hashlib.sha256(salt + code.encode("ascii")).hexdigest()
        self.store.set_meta_many(
            {
                "pair_code_salt": base64.b64encode(salt).decode("ascii"),
                "pair_code_digest": digest,
                "pair_code_expires": int(time.time()) + ttl_seconds,
                "pair_code_failures": 0,
            }
        )
        return code

    def consume_pair_code(self, code: str) -> bool:
        with self._verify_lock:
            return self.store.consume_pair_code(code)

    def create_bind_code(self, ttl_seconds: int) -> str:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        code = secrets.token_hex(5).upper()
        salt = secrets.token_bytes(16)
        digest = hashlib.sha256(salt + code.encode("ascii")).hexdigest()
        self.store.set_meta_many(
            {
                "bind_code_salt": base64.b64encode(salt).decode("ascii"),
                "bind_code_digest": digest,
                "bind_code_expires": int(time.time()) + ttl_seconds,
                "bind_code_failures": 0,
            }
        )
        return code

    def consume_bind_code(self, code: str) -> bool:
        with self._verify_lock:
            return self.store.consume_bind_code(code)
