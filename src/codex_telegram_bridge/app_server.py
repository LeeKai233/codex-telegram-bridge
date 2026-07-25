from __future__ import annotations

import asyncio
import contextlib
import fcntl
import json
import logging
import os
import stat
import subprocess
import time
from collections.abc import Awaitable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

from websockets.asyncio.client import unix_connect

from .config import open_private_directory

LOGGER = logging.getLogger(__name__)

type AppServerMode = Literal["installer-service", "managed-daemon", "external"]
type AppServerState = Literal[
    "starting",
    "healthy",
    "disconnected",
    "recovering_start",
    "recovering_restart",
    "verifying",
    "degraded_external",
    "fatal",
]
type Command = tuple[str, ...]
type CommandResult = int | bool | None
type PidRecordVerdict = Literal["live", "dead", "recycled", "unknown"]

APP_SERVER_COMMAND_TIMEOUT = 30.0
APP_SERVER_TERMINATE_TIMEOUT = 5.0
DAEMON_PID_DIRECTORY = "app-server-daemon"
DAEMON_PID_RECORD_NAMES = ("app-server.pid", "app-server-updater.pid")
PID_START_TIME_TIMEOUT = 5.0
MAX_PID_RECORD_BYTES = 4096


class RecoveryLock:
    """Non-blocking cross-process lock shared by bridge and systemd watchdog."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._descriptor: int | None = None

    def __enter__(self) -> bool:
        try:
            parent_descriptor = open_private_directory(self.path.parent)
        except (OSError, RuntimeError):
            return False
        descriptor = -1
        try:
            flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(self.path.name, flags, 0o600, dir_fd=parent_descriptor)
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
                return False
            os.fchmod(descriptor, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._descriptor = descriptor
            descriptor = -1
        except BlockingIOError:
            return False
        except OSError:
            return False
        finally:
            os.close(parent_descriptor)
            if descriptor >= 0:
                os.close(descriptor)
        return True

    def __exit__(self, *_exc: object) -> None:
        descriptor = self._descriptor
        self._descriptor = None
        if descriptor is None:
            return
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


@dataclass(frozen=True, slots=True)
class ReclaimedPidRecord:
    """One pid record removed because its recorded process no longer exists."""

    path: Path
    pid: int | None
    verdict: PidRecordVerdict
    recorded_start_time: str | None


@dataclass(frozen=True, slots=True)
class _PidRecordSnapshot:
    device: int
    inode: int
    content: bytes
    pid: int
    recorded_start_time: str


def _normalize_start_time(value: str) -> str:
    """Collapse ``ps -o lstart=`` padding so single-digit days compare equal."""
    return " ".join(value.split())


def _parse_pid_record(raw: bytes) -> tuple[int, str] | None:
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    pid = payload.get("pid")
    start_time = payload.get("processStartTime")
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return None
    if not isinstance(start_time, str) or not start_time.strip():
        return None
    return pid, start_time


def _read_pid_record_snapshot(path: Path) -> _PidRecordSnapshot | None:
    """Read and identify a bounded, regular PID record without following symlinks."""
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    parent_descriptor = -1
    descriptor = -1
    try:
        parent_descriptor = open_private_directory(path.parent, create=False)
        descriptor = os.open(path.name, flags, dir_fd=parent_descriptor)
        status = os.fstat(descriptor)
        if not stat.S_ISREG(status.st_mode) or status.st_uid != os.getuid():
            return None
        raw = os.read(descriptor, MAX_PID_RECORD_BYTES + 1)
        if len(raw) > MAX_PID_RECORD_BYTES:
            return None
        record = _parse_pid_record(raw)
        if record is None:
            return None
        pid, recorded_start_time = record
        return _PidRecordSnapshot(
            device=status.st_dev,
            inode=status.st_ino,
            content=raw,
            pid=pid,
            recorded_start_time=recorded_start_time,
        )
    except (OSError, RuntimeError, UnicodeError, ValueError):
        return None
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if parent_descriptor >= 0:
            os.close(parent_descriptor)


def _read_pid_record(path: Path) -> tuple[int, str] | None:
    """Parse ``{"pid": int, "processStartTime": str}``; return None when unusable."""
    snapshot = _read_pid_record_snapshot(path)
    if snapshot is None:
        return None
    return snapshot.pid, snapshot.recorded_start_time


def _read_process_start_time(pid: int) -> tuple[int, str] | None:
    """Ask ``ps`` for a pid's start time, matching the Codex daemon's own oracle."""
    try:
        completed = subprocess.run(
            ["ps", "-p", str(pid), "-o", "lstart="],
            check=False,
            capture_output=True,
            stdin=subprocess.DEVNULL,
            text=True,
            timeout=PID_START_TIME_TIMEOUT,
        )
    except (OSError, subprocess.SubprocessError, UnicodeDecodeError):
        return None
    return completed.returncode, completed.stdout


def _process_command_name(pid: int) -> str | None:
    """Best-effort process identity used to reject false recycled verdicts."""
    with contextlib.suppress(OSError):
        return Path(f"/proc/{pid}/comm").read_text().strip()
    try:
        completed = subprocess.run(
            ["ps", "-p", str(pid), "-o", "comm="],
            check=False,
            capture_output=True,
            stdin=subprocess.DEVNULL,
            text=True,
            timeout=PID_START_TIME_TIMEOUT,
        )
    except (OSError, subprocess.SubprocessError, UnicodeDecodeError):
        return None
    return completed.stdout.strip() if completed.returncode == 0 else None


def _classify_pid_record(pid: int, recorded_start_time: str) -> PidRecordVerdict:
    """Decide whether a recorded pid is still the process the daemon started."""
    reading = _read_process_start_time(pid)
    if reading is None:
        return "unknown"
    returncode, stdout = reading
    if returncode != 0 or not stdout.strip():
        return "dead"
    if _normalize_start_time(stdout) == _normalize_start_time(recorded_start_time):
        return "live"
    # A start-time mismatch alone is not proof of recycling: a locale or procps
    # formatting difference would look identical.  Codex tolerates a recycled pid
    # without hard-failing, so only reclaim when the holder is not Codex at all.
    command_name = _process_command_name(pid)
    if command_name is None or "codex" in command_name.lower():
        return "live"
    return "recycled"


@contextlib.contextmanager
def _pid_record_lock(path: Path) -> Iterator[bool]:
    """Hold the Codex sibling lock through PID classification and unlink."""
    lock_path = path.with_name(f"{path.name}.lock")
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    parent_descriptor = -1
    descriptor = -1
    acquired = False
    try:
        try:
            parent_descriptor = open_private_directory(path.parent, create=False)
            descriptor = os.open(lock_path.name, flags, 0o600, dir_fd=parent_descriptor)
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
                yield False
                return
            os.fchmod(descriptor, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, RuntimeError):
            yield False
            return
        acquired = True
        yield True
    finally:
        if descriptor >= 0:
            if acquired:
                with contextlib.suppress(OSError):
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)
        if parent_descriptor >= 0:
            os.close(parent_descriptor)


def _unlink_pid_record_if_unchanged(path: Path, expected: _PidRecordSnapshot) -> bool:
    """Unlink only the same regular record observed before classification."""
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    parent_descriptor = -1
    descriptor = -1
    try:
        parent_descriptor = open_private_directory(path.parent, create=False)
        descriptor = os.open(path.name, flags, dir_fd=parent_descriptor)
        status = os.fstat(descriptor)
        if (
            not stat.S_ISREG(status.st_mode)
            or status.st_uid != os.getuid()
            or (status.st_dev, status.st_ino) != (expected.device, expected.inode)
        ):
            return False
        raw = os.read(descriptor, MAX_PID_RECORD_BYTES + 1)
        if len(raw) > MAX_PID_RECORD_BYTES or raw != expected.content:
            return False
        os.unlink(path.name, dir_fd=parent_descriptor)
        return True
    except (OSError, RuntimeError):
        return False
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if parent_descriptor >= 0:
            os.close(parent_descriptor)


def _reclaim_one_pid_record(path: Path) -> ReclaimedPidRecord | None:
    with _pid_record_lock(path) as acquired:
        if not acquired:
            LOGGER.info("event=app_server_pid_record_locked file=%s", path.name)
            return None
        snapshot = _read_pid_record_snapshot(path)
        if snapshot is None:
            # Leave unparsable, oversized, symlink, and non-regular records alone:
            # recovery must never delete a record it cannot identify safely.
            if path.exists():
                LOGGER.warning("event=app_server_pid_record_unreadable file=%s", path.name)
            return None
        verdict = _classify_pid_record(snapshot.pid, snapshot.recorded_start_time)
        if verdict not in {"dead", "recycled"}:
            return None
        current = _read_pid_record_snapshot(path)
        if current is None or (
            current.device,
            current.inode,
            current.content,
        ) != (snapshot.device, snapshot.inode, snapshot.content):
            LOGGER.info("event=app_server_pid_record_replaced file=%s", path.name)
            return None
        if not _unlink_pid_record_if_unchanged(path, snapshot):
            return None
        pid = snapshot.pid
        recorded_start_time = snapshot.recorded_start_time
    LOGGER.warning(
        "event=app_server_pid_record_reclaimed file=%s pid=%s verdict=%s recorded_start_time=%s",
        path.name,
        pid,
        verdict,
        recorded_start_time,
    )
    return ReclaimedPidRecord(
        path=path, pid=pid, verdict=verdict, recorded_start_time=recorded_start_time
    )


def reclaim_stale_daemon_pid_records(codex_home: Path | str) -> tuple[ReclaimedPidRecord, ...]:
    """Delete pid records whose process is gone so ``daemon bootstrap`` starts clean.

    Every WSL VM restart yields a fresh PID namespace, so the pid recorded in
    ``$CODEX_HOME/app-server-daemon/*.pid`` outlives its process.  The Codex
    binary then hard-fails with ``failed to read start time for pid-managed app
    server N`` and both ``daemon restart`` and ``daemon bootstrap`` crash-loop.
    Never removes a record whose process is still alive, and never raises.
    """
    directory = Path(codex_home) / DAEMON_PID_DIRECTORY
    reclaimed: list[ReclaimedPidRecord] = []
    for name in DAEMON_PID_RECORD_NAMES:
        try:
            record = _reclaim_one_pid_record(directory / name)
        except Exception as exc:  # pragma: no cover - defensive: never break recovery
            LOGGER.warning(
                "event=app_server_pid_record_scan_failed file=%s error=%s",
                name,
                type(exc).__name__,
            )
            continue
        if record is not None:
            reclaimed.append(record)
    return tuple(reclaimed)


class CommandRunner(Protocol):
    def __call__(self, command: Command) -> Awaitable[CommandResult]: ...


class ProtocolProbe(Protocol):
    def __call__(self, command: Command) -> Awaitable[bool]: ...


class AsyncAction(Protocol):
    def __call__(self) -> Awaitable[CommandResult]: ...


class StateChangeHandler(Protocol):
    def __call__(self, state: AppServerState, snapshot: dict[str, Any]) -> Awaitable[None] | None: ...


async def protocol_probe(command: Command) -> bool:
    """Run a minimal JSON-RPC initialize handshake against a Unix socket target."""
    if len(command) != 1 or not command[0].startswith("unix://"):
        return False
    try:
        async with unix_connect(
            path=command[0].removeprefix("unix://"),
            uri="ws://localhost/",
            compression=None,
            user_agent_header=None,
            open_timeout=5,
            close_timeout=2,
        ) as connection:
            await connection.send(
                json.dumps(
                    {
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "clientInfo": {
                                "name": "codex_telegram_bridge_supervisor",
                                "title": "Codex Telegram Bridge Supervisor",
                                "version": "0.1.0",
                            },
                            "capabilities": {"experimentalApi": True},
                        },
                    },
                    separators=(",", ":"),
                )
            )
            response = json.loads(await asyncio.wait_for(connection.recv(), timeout=5))
            if not isinstance(response, dict) or response.get("id") != 1 or "error" in response:
                return False
            await connection.send('{"method":"initialized","params":{}}')
            return isinstance(response.get("result"), dict)
    except Exception:
        return False


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    """Stop and reap a control child after timeout or task cancellation."""
    if process.returncode is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        process.terminate()
    try:
        await asyncio.wait_for(process.wait(), timeout=APP_SERVER_TERMINATE_TIMEOUT)
        return
    except TimeoutError:
        pass
    with contextlib.suppress(ProcessLookupError):
        process.kill()
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(process.wait(), timeout=APP_SERVER_TERMINATE_TIMEOUT)


async def command_runner(command: Command) -> int:
    """Run one bounded app-server daemon control command."""
    process = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        return await asyncio.wait_for(process.wait(), timeout=APP_SERVER_COMMAND_TIMEOUT)
    except TimeoutError:
        await _terminate_process(process)
        raise
    except asyncio.CancelledError:
        await _terminate_process(process)
        raise


class AppServerSupervisor:
    """Bounded recovery controller for a Codex app-server daemon.

    The bridge client is intentionally duck typed: only its ``connected``
    property and optional ``health_snapshot`` method are inspected.  A caller
    that does not own a bridge client can instead inject a protocol probe.
    """

    def __init__(
        self,
        client: Any,
        mode: AppServerMode,
        socket_path: Path,
        codex_binary: Path | str,
        state_dir: Path,
        *,
        codex_home: Path | str | None = None,
        command_runner: CommandRunner | None = None,
        installer_restart: AsyncAction | None = None,
        protocol_probe: ProtocolProbe | None = None,
        on_state_change: StateChangeHandler | None = None,
        reconnect_grace: float = 10.0,
        verify_timeout: float = 30.0,
        max_recovery_cycles: int = 3,
    ) -> None:
        if mode not in {"installer-service", "managed-daemon", "external"}:
            raise ValueError(f"Unsupported app-server mode: {mode}")
        if reconnect_grace < 0 or verify_timeout <= 0 or max_recovery_cycles <= 0:
            raise ValueError("App-server recovery limits must be positive")
        self.client = client
        self.mode = mode
        self.socket_path = Path(socket_path)
        self.codex_binary = str(codex_binary)
        self.state_dir = Path(state_dir)
        self._codex_home = Path(codex_home) if codex_home is not None else None
        self._command_runner = command_runner or globals()["command_runner"]
        self._installer_restart = installer_restart
        self._protocol_probe = protocol_probe or globals()["protocol_probe"]
        self._on_state_change = on_state_change
        self.reconnect_grace = reconnect_grace
        self.verify_timeout = verify_timeout
        self.max_recovery_cycles = max_recovery_cycles
        self._state: AppServerState = "starting"
        self._last_error: str | None = None
        self._last_connected_at: float | None = None
        self._last_connected_monotonic: float | None = None
        self._monitor_started_at: float | None = None
        self._recovery_cycles = 0
        self._start_attempts = 0
        self._restart_attempts = 0
        self._wake_event = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._monitor_task: asyncio.Task[None] | None = None
        self._recovery_lock_path = self.state_dir / "app-server-recovery.lock"

    @property
    def state(self) -> AppServerState:
        return self._state

    @property
    def fatal_error(self) -> str | None:
        return self._last_error if self._state == "fatal" else None

    def snapshot(self) -> dict[str, Any]:
        client_health = getattr(self.client, "health_snapshot", None)
        health = client_health() if callable(client_health) else None
        return {
            "mode": self.mode,
            "state": self._state,
            "socket_path": str(self.socket_path),
            "state_dir": str(self.state_dir),
            "recovery_cycles": self._recovery_cycles,
            "max_recovery_cycles": self.max_recovery_cycles,
            "start_attempts": self._start_attempts,
            "restart_attempts": self._restart_attempts,
            "last_error": self._last_error,
            "last_connected_at": self._last_connected_at,
            "client": health if isinstance(health, dict) else None,
        }

    async def wake(self) -> None:
        """Request an immediate monitor pass after a bridge disconnect."""
        self._wake_event.set()

    async def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        task = self._monitor_task
        if task is not None and task is not asyncio.current_task() and not task.done():
            await task

    async def monitor(self, stop_event: asyncio.Event, interval: float = 1.0) -> None:
        if interval <= 0:
            raise ValueError("App-server monitor interval must be positive")
        if self._monitor_task is not None and self._monitor_task is not asyncio.current_task():
            raise RuntimeError("App-server supervisor is already being monitored")
        self._monitor_task = asyncio.current_task()
        self._monitor_started_at = time.monotonic()
        try:
            while not stop_event.is_set() and not self._stop_event.is_set():
                await self.check_once()
                if self._state == "fatal":
                    return
                await self._wait_for_wake_or_stop(stop_event, interval)
        finally:
            if self._monitor_task is asyncio.current_task():
                self._monitor_task = None

    async def check_once(self) -> None:
        if self._state == "fatal" or self._stop_event.is_set():
            return
        if await self._is_healthy():
            self._last_connected_at = time.time()
            self._last_connected_monotonic = time.monotonic()
            self._recovery_cycles = 0
            self._last_error = None
            await self._set_state("healthy")
            return
        await self._set_state("disconnected")
        if self.mode == "external":
            await self._set_state("degraded_external")
            return
        if self._within_reconnect_grace():
            return
        with RecoveryLock(self._recovery_lock_path) as acquired:
            if not acquired:
                self._last_error = "recovery already in progress in another process"
                return
            if self._recovery_cycles >= self.max_recovery_cycles:
                await self._mark_fatal("app-server recovery limit exhausted")
                return
            self._recovery_cycles += 1
            if self.mode == "installer-service":
                await self._recover_installer_service()
            else:
                await self._recover_managed_daemon()

    async def _recover_installer_service(self) -> None:
        if self._installer_restart is None:
            await self._mark_fatal("installer-service mode requires installer_restart")
            return
        await self._set_state("recovering_restart")
        self._restart_attempts += 1
        if not await self._run_action(self._installer_restart, "installer restart"):
            return
        await self._verify_or_exhaust("installer restart")

    async def _reclaim_stale_pid_records(self) -> None:
        """Clear stale pid records before control commands that would hard-fail on them."""
        if self._codex_home is None:
            return
        with contextlib.suppress(Exception):
            await asyncio.to_thread(reclaim_stale_daemon_pid_records, self._codex_home)

    async def _recover_managed_daemon(self) -> None:
        await self._reclaim_stale_pid_records()
        await self._set_state("recovering_start")
        self._start_attempts += 1
        if await self._run_command(
            (self.codex_binary, "app-server", "daemon", "start"), "daemon start"
        ) and await self._verify("daemon start"):
            return
        await self._set_state("recovering_restart")
        self._restart_attempts += 1
        if not await self._run_command(
            (self.codex_binary, "app-server", "daemon", "restart"), "daemon restart"
        ):
            return
        await self._verify_or_exhaust("daemon restart")

    async def _verify_or_exhaust(self, action: str) -> None:
        if await self._verify(action):
            return
        if self._recovery_cycles >= self.max_recovery_cycles:
            await self._mark_fatal(f"{action} did not restore the app-server")

    async def _verify(self, action: str) -> bool:
        await self._set_state("verifying")
        deadline = time.monotonic() + self.verify_timeout
        while not self._stop_event.is_set() and time.monotonic() < deadline:
            if await self._is_healthy():
                self._last_connected_at = time.time()
                self._last_connected_monotonic = time.monotonic()
                self._recovery_cycles = 0
                self._last_error = None
                await self._set_state("healthy")
                return True
            await asyncio.sleep(min(0.25, max(0.01, deadline - time.monotonic())))
        self._last_error = f"{action} verification timed out"
        return False

    async def _is_healthy(self) -> bool:
        if bool(getattr(self.client, "connected", False)):
            return True
        if self._protocol_probe is None:
            return False
        try:
            return bool(await self._protocol_probe(self._status_command()))
        except Exception as exc:
            self._last_error = f"protocol probe failed: {type(exc).__name__}"
            return False

    async def _run_command(self, command: Command, action: str) -> bool:
        try:
            return self._command_succeeded(await self._command_runner(command))
        except Exception as exc:
            self._last_error = f"{action} failed: {type(exc).__name__}"
            return False

    async def _run_action(self, action: AsyncAction, name: str) -> bool:
        try:
            return self._command_succeeded(await action())
        except Exception as exc:
            self._last_error = f"{name} failed: {type(exc).__name__}"
            return False

    async def _wait_for_wake_or_stop(self, stop_event: asyncio.Event, interval: float) -> None:
        wake_task = asyncio.create_task(self._wake_event.wait())
        stop_task = asyncio.create_task(stop_event.wait())
        local_stop_task = asyncio.create_task(self._stop_event.wait())
        done, pending = await asyncio.wait(
            (wake_task, stop_task, local_stop_task),
            timeout=interval,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if wake_task in done:
            self._wake_event.clear()

    async def _set_state(self, state: AppServerState) -> None:
        if state == self._state:
            return
        self._state = state
        if self._on_state_change is None:
            return
        result = self._on_state_change(state, self.snapshot())
        if hasattr(result, "__await__"):
            await result

    async def _mark_fatal(self, message: str) -> None:
        self._last_error = message
        await self._set_state("fatal")

    def _within_reconnect_grace(self) -> bool:
        reference = self._last_connected_monotonic or self._monitor_started_at
        return (
            reference is not None
            and time.monotonic() - reference < self.reconnect_grace
        )

    def _status_command(self) -> Command:
        return (f"unix://{self.socket_path}",)

    @staticmethod
    def _command_succeeded(result: CommandResult) -> bool:
        if result is None:
            return True
        if isinstance(result, bool):
            return result
        return result == 0
