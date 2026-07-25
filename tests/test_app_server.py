from __future__ import annotations

import asyncio
import fcntl
import json
import os
from pathlib import Path
from typing import Any

import pytest

from codex_telegram_bridge import app_server as app_server_module
from codex_telegram_bridge.app_server import (
    AppServerSupervisor,
    RecoveryLock,
    reclaim_stale_daemon_pid_records,
)


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


def test_recovery_lock_rejects_symlinked_parent(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(real, target_is_directory=True)

    with RecoveryLock(linked / "app-server-recovery.lock") as acquired:
        assert acquired is False

    assert not (real / "app-server-recovery.lock").exists()


RECORDED_START_TIME = "Sat Jul 25 15:46:16 2026"


def write_pid_record(
    codex_home: Path,
    name: str = "app-server.pid",
    *,
    pid: int = 416,
    start_time: str = RECORDED_START_TIME,
    raw: str | None = None,
) -> Path:
    directory = codex_home / "app-server-daemon"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    payload = raw if raw is not None else json.dumps({"pid": pid, "processStartTime": start_time})
    path.write_text(payload, encoding="utf-8")
    return path


def stub_start_time(
    monkeypatch: pytest.MonkeyPatch,
    result: tuple[int, str] | None,
    *,
    command_name: str | None = "codex",
) -> None:
    monkeypatch.setattr(app_server_module, "_read_process_start_time", lambda _pid: result)
    monkeypatch.setattr(app_server_module, "_process_command_name", lambda _pid: command_name)


def test_live_pid_record_is_left_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_pid_record(tmp_path)
    stub_start_time(monkeypatch, (0, f"{RECORDED_START_TIME}\n"))

    assert reclaim_stale_daemon_pid_records(tmp_path) == ()
    assert path.exists()


def test_dead_pid_record_is_reclaimed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = write_pid_record(tmp_path)
    stub_start_time(monkeypatch, (1, ""))

    reclaimed = reclaim_stale_daemon_pid_records(tmp_path)

    assert not path.exists()
    assert [(record.pid, record.verdict) for record in reclaimed] == [(416, "dead")]
    assert reclaimed[0].recorded_start_time == RECORDED_START_TIME


def test_recycled_pid_record_is_reclaimed_when_holder_is_not_codex(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_pid_record(tmp_path)
    stub_start_time(monkeypatch, (0, "Sat Jul 25 09:00:00 2026"), command_name="nginx")

    reclaimed = reclaim_stale_daemon_pid_records(tmp_path)

    assert not path.exists()
    assert [record.verdict for record in reclaimed] == ["recycled"]


def test_mismatched_start_time_on_a_codex_process_is_left_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_pid_record(tmp_path)
    stub_start_time(monkeypatch, (0, "Sat Jul 25 09:00:00 2026"), command_name="codex")

    assert reclaim_stale_daemon_pid_records(tmp_path) == ()
    assert path.exists()


def test_start_time_padding_variants_compare_equal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_pid_record(tmp_path, start_time="Sat Jul  5 09:00:00 2026")
    stub_start_time(monkeypatch, (0, "  Sat Jul 5 09:00:00 2026\n"))

    assert reclaim_stale_daemon_pid_records(tmp_path) == ()
    assert path.exists()


@pytest.mark.parametrize(
    "raw",
    ["", "not json", '{"pid":"x","processStartTime":"y"}', '{"pid":416}', '{"pid":0,"processStartTime":"y"}'],
)
def test_malformed_records_are_ignored_without_raising(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, raw: str
) -> None:
    path = write_pid_record(tmp_path, raw=raw)
    stub_start_time(monkeypatch, (1, ""))

    assert reclaim_stale_daemon_pid_records(tmp_path) == ()
    assert path.exists()


def test_oversized_pid_records_are_rejected_before_classification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_pid_record(tmp_path, raw="{" + "x" * 5000)

    def classify(_pid: int, _start_time: str) -> str:
        pytest.fail("oversized records must not reach process classification")

    monkeypatch.setattr(app_server_module, "_classify_pid_record", classify)

    assert reclaim_stale_daemon_pid_records(tmp_path) == ()
    assert path.exists()


def test_pid_lock_errors_defer_reclamation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_pid_record(tmp_path)

    def fail_lock(*_args: object) -> None:
        raise OSError("lock unavailable")

    monkeypatch.setattr(app_server_module.fcntl, "flock", fail_lock)

    assert reclaim_stale_daemon_pid_records(tmp_path) == ()
    assert path.exists()


def test_pid_record_replaced_after_classification_is_not_deleted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_pid_record(tmp_path, pid=416)
    replacement = json.dumps({"pid": 999, "processStartTime": RECORDED_START_TIME})

    def classify(_pid: int, _start_time: str) -> str:
        path.write_text(replacement, encoding="utf-8")
        return "dead"

    monkeypatch.setattr(app_server_module, "_classify_pid_record", classify)

    assert reclaim_stale_daemon_pid_records(tmp_path) == ()
    assert json.loads(path.read_text(encoding="utf-8"))["pid"] == 999


def test_missing_directory_is_ignored(tmp_path: Path) -> None:
    assert reclaim_stale_daemon_pid_records(tmp_path / "absent") == ()


def test_symlinked_pid_record_parent_is_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    codex_home = tmp_path / "codex-home"
    codex_home.mkdir()
    target = tmp_path / "target-daemon"
    target.mkdir()
    linked = codex_home / "app-server-daemon"
    linked.symlink_to(target, target_is_directory=True)
    record = target / "app-server.pid"
    record.write_text(
        json.dumps({"pid": 416, "processStartTime": RECORDED_START_TIME}), encoding="utf-8"
    )
    stub_start_time(monkeypatch, (1, ""))

    assert reclaim_stale_daemon_pid_records(codex_home) == ()
    assert record.exists()


def test_unavailable_ps_leaves_records_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_pid_record(tmp_path)
    stub_start_time(monkeypatch, None)

    assert reclaim_stale_daemon_pid_records(tmp_path) == ()
    assert path.exists()


def test_both_daemon_records_are_scanned_independently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dead = write_pid_record(tmp_path, "app-server.pid", pid=416)
    live = write_pid_record(tmp_path, "app-server-updater.pid", pid=421)

    def reader(pid: int) -> tuple[int, str]:
        return (0, RECORDED_START_TIME) if pid == 421 else (1, "")

    monkeypatch.setattr(app_server_module, "_read_process_start_time", reader)

    reclaimed = reclaim_stale_daemon_pid_records(tmp_path)

    assert not dead.exists()
    assert live.exists()
    assert [record.pid for record in reclaimed] == [416]


def test_reclaim_never_touches_lock_or_log_siblings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_pid_record(tmp_path)
    siblings = [
        path.with_name("app-server.pid.lock"),
        path.with_name("daemon.lock"),
        path.with_name("settings.json"),
        path.with_name("app-server.stderr.log"),
    ]
    for sibling in siblings:
        sibling.touch()
    stub_start_time(monkeypatch, (1, ""))

    reclaim_stale_daemon_pid_records(tmp_path)

    assert not path.exists()
    assert all(sibling.exists() for sibling in siblings)


def test_held_pid_lock_defers_reclamation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_pid_record(tmp_path)
    lock_path = path.with_name("app-server.pid.lock")
    lock_path.touch()
    stub_start_time(monkeypatch, (1, ""))
    descriptor = os.open(lock_path, os.O_RDWR)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert reclaim_stale_daemon_pid_records(tmp_path) == ()
        assert path.exists()
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


@pytest.mark.asyncio
async def test_managed_daemon_recovery_reclaims_stale_record_before_daemon_start(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_pid_record(tmp_path)
    stub_start_time(monkeypatch, (1, ""))
    existed_at_command: list[bool] = []

    async def runner(command: tuple[str, ...]) -> int:
        existed_at_command.append(path.exists())
        return 0

    async def probe(_command: tuple[str, ...]) -> bool:
        return True

    supervisor = build_supervisor(runner=runner, probe=probe, codex_home=tmp_path)
    await supervisor._recover_managed_daemon()

    assert not path.exists()
    assert existed_at_command and existed_at_command[0] is False


@pytest.mark.asyncio
async def test_installer_service_recovery_does_not_touch_pid_records(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_pid_record(tmp_path)
    stub_start_time(monkeypatch, (1, ""))

    async def restart() -> int:
        return 0

    async def probe(_command: tuple[str, ...]) -> bool:
        return True

    supervisor = build_supervisor(
        mode="installer-service", restart=restart, probe=probe, codex_home=tmp_path
    )
    await supervisor._recover_installer_service()

    assert path.exists()


@pytest.mark.asyncio
async def test_supervisor_without_codex_home_skips_reclamation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = write_pid_record(tmp_path)
    stub_start_time(monkeypatch, (1, ""))

    async def runner(_command: tuple[str, ...]) -> int:
        return 0

    async def probe(_command: tuple[str, ...]) -> bool:
        return True

    supervisor = build_supervisor(runner=runner, probe=probe)
    await supervisor._recover_managed_daemon()

    assert path.exists()
