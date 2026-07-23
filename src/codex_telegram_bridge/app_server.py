from __future__ import annotations

import asyncio
import contextlib
import fcntl
import json
import os
import time
from collections.abc import Awaitable
from pathlib import Path
from typing import Any, Literal, Protocol

from websockets.asyncio.client import unix_connect

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

APP_SERVER_COMMAND_TIMEOUT = 30.0
APP_SERVER_TERMINATE_TIMEOUT = 5.0


class RecoveryLock:
    """Non-blocking cross-process lock shared by bridge and systemd watchdog."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._descriptor: int | None = None

    def __enter__(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(descriptor)
            return False
        self._descriptor = descriptor
        return True

    def __exit__(self, *_exc: object) -> None:
        descriptor = self._descriptor
        self._descriptor = None
        if descriptor is None:
            return
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


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

    async def _recover_managed_daemon(self) -> None:
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
