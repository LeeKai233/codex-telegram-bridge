from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from codex_telegram_bridge import app_server as app_server_module
from codex_telegram_bridge.app_server import AppServerSupervisor, RecoveryLock


class FakeClient:
    def __init__(self, connected: bool = False) -> None:
        self.connected = connected

    def health_snapshot(self) -> dict[str, Any]:
        return {"connected": self.connected}


def build_supervisor(
    *,
    client: FakeClient | None = None,
    mode: str = "managed-daemon",
    runner: Any = None,
    restart: Any = None,
    probe: Any = None,
    **kwargs: Any,
) -> AppServerSupervisor:
    return AppServerSupervisor(
        client or FakeClient(),
        mode,  # type: ignore[arg-type]
        Path("/tmp/codex.sock"),
        Path("/tmp/codex"),
        Path("/tmp/state"),
        command_runner=runner,
        installer_restart=restart,
        protocol_probe=probe,
        reconnect_grace=0,
        verify_timeout=0.02,
        **kwargs,
    )


@pytest.mark.asyncio
async def test_managed_daemon_starts_then_verifies() -> None:
    client = FakeClient()
    commands: list[tuple[str, ...]] = []

    async def runner(command: tuple[str, ...]) -> int:
        commands.append(command)
        return 0

    async def probe(_command: tuple[str, ...]) -> bool:
        return commands[-1][-1] == "start" if commands else False

    supervisor = build_supervisor(client=client, runner=runner, probe=probe)

    await supervisor.check_once()

    assert commands == [("/tmp/codex", "app-server", "daemon", "start")]
    assert supervisor.state == "healthy"
    assert supervisor.snapshot()["start_attempts"] == 1
    assert supervisor.fatal_error is None


@pytest.mark.asyncio
async def test_managed_daemon_escalates_from_start_to_restart() -> None:
    commands: list[tuple[str, ...]] = []

    async def runner(command: tuple[str, ...]) -> int:
        commands.append(command)
        return 0

    async def probe(_command: tuple[str, ...]) -> bool:
        return bool(commands and commands[-1][-1] == "restart")

    supervisor = build_supervisor(runner=runner, probe=probe)

    await supervisor.check_once()

    assert [command[-1] for command in commands] == ["start", "restart"]
    assert supervisor.state == "healthy"
    assert supervisor.snapshot()["restart_attempts"] == 1


@pytest.mark.asyncio
async def test_installer_service_uses_injected_restart_and_probe() -> None:
    restart_calls = 0

    async def restart() -> None:
        nonlocal restart_calls
        restart_calls += 1

    async def probe(_command: tuple[str, ...]) -> bool:
        return restart_calls == 1

    supervisor = build_supervisor(mode="installer-service", restart=restart, probe=probe)

    await supervisor.check_once()

    assert restart_calls == 1
    assert supervisor.state == "healthy"
    assert supervisor.snapshot()["restart_attempts"] == 1


@pytest.mark.asyncio
async def test_external_mode_never_starts_or_restarts() -> None:
    calls: list[tuple[str, ...]] = []

    async def runner(command: tuple[str, ...]) -> int:
        calls.append(command)
        return 0

    async def probe(_command: tuple[str, ...]) -> bool:
        return False

    supervisor = build_supervisor(mode="external", runner=runner, probe=probe)

    await supervisor.check_once()

    assert calls == []
    assert supervisor.state == "degraded_external"
    assert supervisor.fatal_error is None


@pytest.mark.asyncio
async def test_recovery_limit_becomes_fatal_with_json_snapshot() -> None:
    async def runner(_command: tuple[str, ...]) -> int:
        return 1

    supervisor = build_supervisor(runner=runner, max_recovery_cycles=1)

    await supervisor.check_once()
    await supervisor.check_once()

    assert supervisor.state == "fatal"
    assert supervisor.fatal_error == "app-server recovery limit exhausted"
    assert supervisor.snapshot() == {
        "mode": "managed-daemon",
        "state": "fatal",
        "socket_path": "/tmp/codex.sock",
        "state_dir": "/tmp/state",
        "recovery_cycles": 1,
        "max_recovery_cycles": 1,
        "start_attempts": 1,
        "restart_attempts": 1,
        "last_error": "app-server recovery limit exhausted",
        "last_connected_at": None,
        "client": {"connected": False},
    }


@pytest.mark.asyncio
async def test_monitor_wake_runs_an_immediate_second_check() -> None:
    client = FakeClient(connected=True)
    supervisor = build_supervisor(client=client, mode="external")
    stop_event = asyncio.Event()
    monitor = asyncio.create_task(supervisor.monitor(stop_event, interval=30))

    await asyncio.sleep(0)
    client.connected = False
    await supervisor.wake()
    await asyncio.sleep(0.02)
    await supervisor.stop()
    await monitor

    assert supervisor.state == "degraded_external"


@pytest.mark.asyncio
async def test_initial_reconnect_grace_defers_recovery_commands(tmp_path: Path) -> None:
    commands: list[tuple[str, ...]] = []

    async def runner(command: tuple[str, ...]) -> int:
        commands.append(command)
        return 0

    async def probe(_command: tuple[str, ...]) -> bool:
        return False

    supervisor = AppServerSupervisor(
        FakeClient(),
        "managed-daemon",
        tmp_path / "codex.sock",
        tmp_path / "codex",
        tmp_path / "state",
        command_runner=runner,
        protocol_probe=probe,
        reconnect_grace=0.1,
        verify_timeout=0.01,
    )
    stop_event = asyncio.Event()
    monitor = asyncio.create_task(supervisor.monitor(stop_event, interval=0.01))
    try:
        await asyncio.sleep(0.03)
        assert commands == []
        assert supervisor.state == "disconnected"
    finally:
        stop_event.set()
        await supervisor.stop()
        await monitor


@pytest.mark.asyncio
async def test_recovery_lock_contention_does_not_consume_budget(tmp_path: Path) -> None:
    async def probe(_command: tuple[str, ...]) -> bool:
        return False

    supervisor = AppServerSupervisor(
        FakeClient(),
        "managed-daemon",
        tmp_path / "codex.sock",
        tmp_path / "codex",
        tmp_path / "state",
        protocol_probe=probe,
        reconnect_grace=0,
        verify_timeout=0.01,
        max_recovery_cycles=1,
    )
    with RecoveryLock(supervisor._recovery_lock_path) as acquired:
        assert acquired is True
        await supervisor.check_once()

    assert supervisor.state == "disconnected"
    assert supervisor.snapshot()["recovery_cycles"] == 0
    assert supervisor.fatal_error is None


class BlockingProcess:
    def __init__(self) -> None:
        self.returncode: int | None = None
        self.terminated = 0
        self.killed = 0
        self._finished = asyncio.Event()

    async def wait(self) -> int:
        await self._finished.wait()
        return self.returncode or 0

    def terminate(self) -> None:
        self.terminated += 1
        self.returncode = -15
        self._finished.set()

    def kill(self) -> None:
        self.killed += 1
        self.returncode = -9
        self._finished.set()


@pytest.mark.asyncio
async def test_command_runner_timeout_terminates_control_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = BlockingProcess()

    async def create_process(*_command: str, **_kwargs: Any) -> BlockingProcess:
        return process

    monkeypatch.setattr(app_server_module.asyncio, "create_subprocess_exec", create_process)
    monkeypatch.setattr(app_server_module, "APP_SERVER_COMMAND_TIMEOUT", 0.01)

    with pytest.raises(asyncio.TimeoutError):
        await app_server_module.command_runner(("codex", "app-server", "daemon", "start"))

    assert process.terminated == 1
    assert process.killed == 0


@pytest.mark.asyncio
async def test_command_runner_cancellation_terminates_control_child(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process = BlockingProcess()

    async def create_process(*_command: str, **_kwargs: Any) -> BlockingProcess:
        return process

    monkeypatch.setattr(app_server_module.asyncio, "create_subprocess_exec", create_process)
    task = asyncio.create_task(app_server_module.command_runner(("codex", "app-server", "daemon", "restart")))
    await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert process.terminated == 1
    assert process.killed == 0


def test_recovery_lock_is_non_blocking_and_process_shared(tmp_path: Path) -> None:
    path = tmp_path / "app-server-recovery.lock"
    with RecoveryLock(path) as acquired:
        assert acquired is True
        with RecoveryLock(path) as contended:
            assert contended is False
