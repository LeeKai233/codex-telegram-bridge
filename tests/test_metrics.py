from __future__ import annotations

import asyncio

import pytest

from codex_telegram_bridge import metrics
from codex_telegram_bridge.metrics import (
    GpuSnapshot,
    MetricsSnapshot,
    ascii_bar,
    collect_gpu_metrics,
    parse_nvidia_smi_csv,
    render_metrics,
    resolve_nvidia_smi,
)


def snapshot(*, gpus: tuple[GpuSnapshot, ...] = ()) -> MetricsSnapshot:
    gib = 1024**3
    return MetricsSnapshot(
        sampled_at=1_700_000_000,
        uptime_seconds=90_061,
        load=(0.25, 0.5, 0.75),
        cpu_percent=23.6,
        memory_total=8 * gib,
        memory_available=3 * gib,
        memory_percent=62.5,
        swap_total=2 * gib,
        swap_used=512 * 1024**2,
        swap_percent=25.0,
        disk_total=100 * gib,
        disk_free=76 * gib,
        disk_percent=24.0,
        codex_processes=3,
        codex_rss=768 * 1024**2,
        codex_cpu=12.2,
        gpus=gpus,
    )


def test_ascii_bar_clamps_and_uses_ten_cells() -> None:
    assert ascii_bar(23.6) == "##--------"
    assert ascii_bar(58.7) == "######----"
    assert ascii_bar(-5) == "----------"
    assert ascii_bar(110) == "##########"
    with pytest.raises(ValueError):
        ascii_bar(10, cells=0)


def test_resolve_nvidia_smi_prefers_path_then_wsl(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(metrics.shutil, "which", lambda _name: "/usr/bin/nvidia-smi")
    assert resolve_nvidia_smi() == "/usr/bin/nvidia-smi"

    monkeypatch.setattr(metrics.shutil, "which", lambda _name: None)
    monkeypatch.setattr(metrics.os.path, "isfile", lambda path: path == metrics._NVIDIA_SMI_WSL)
    monkeypatch.setattr(metrics.os, "access", lambda path, mode: path == metrics._NVIDIA_SMI_WSL)
    assert resolve_nvidia_smi() == "/usr/lib/wsl/lib/nvidia-smi"


def test_parse_nvidia_smi_csv_handles_multiple_gpus_and_na() -> None:
    parsed = parse_nvidia_smi_csv(
        "0, NVIDIA GeForce RTX 4060, 2048, 8192, 31, 54, 37.2\n"
        "1, NVIDIA A100, 1024, 40960, N/A, [N/A], 110.5\n"
        "malformed,row\n"
    )
    assert len(parsed) == 2
    assert parsed[0] == GpuSnapshot(0, "NVIDIA GeForce RTX 4060", 2048.0, 8192.0, 31.0, 54.0, 37.2)
    assert parsed[1].index == 1
    assert parsed[1].utilization_percent is None
    assert parsed[1].temperature_c is None


@pytest.mark.asyncio
async def test_collect_gpu_metrics_passes_structured_query(monkeypatch: pytest.MonkeyPatch) -> None:
    class Process:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"0, NVIDIA RTX, 100, 1000, 20, 50, 30\n", b""

    arguments: tuple[object, ...] = ()

    async def create(*args: object, **kwargs: object) -> Process:
        nonlocal arguments
        arguments = args
        assert kwargs["stdout"] == asyncio.subprocess.PIPE
        return Process()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    result = await collect_gpu_metrics(executable="/test/nvidia-smi")
    assert result[0].name == "NVIDIA RTX"
    assert arguments[0] == "/test/nvidia-smi"
    assert arguments[1] == (
        "--query-gpu=index,name,memory.used,memory.total,utilization.gpu,temperature.gpu,power.draw"
    )
    assert arguments[2] == "--format=csv,noheader,nounits"


@pytest.mark.asyncio
async def test_collect_gpu_metrics_kills_and_reaps_timed_out_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Process:
        returncode = None

        def __init__(self) -> None:
            self.communicate_calls = 0
            self.killed = False

        async def communicate(self) -> tuple[bytes, bytes]:
            self.communicate_calls += 1
            if self.communicate_calls == 1:
                await asyncio.Event().wait()
            return b"", b""

        def kill(self) -> None:
            self.killed = True

        async def wait(self) -> int:
            return -9

    process = Process()

    async def create(*_args: object, **_kwargs: object) -> Process:
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", create)
    assert await collect_gpu_metrics(executable="/test/nvidia-smi", timeout=0.001) == ()
    assert process.killed is True
    assert process.communicate_calls == 2


def test_render_metrics_uses_markdown_ascii_bars_and_all_gpus() -> None:
    rendered = render_metrics(
        snapshot(
            gpus=(
                GpuSnapshot(0, "NVIDIA RTX 4060", 2048, 8192, 31, 54, 37.2),
                GpuSnapshot(1, "NVIDIA A100", 1024, 40960, 5, None, 110.5),
            )
        )
    )
    assert rendered.startswith("*🟠 Ubuntu · WSL*")
    assert "`23.6%` `##--------`" in rendered
    assert "Swap `25.0%` `###-------`" in rendered
    assert rendered.count("*🟩 NVIDIA ·") == 2
    assert "NVIDIA RTX 4060" in rendered
    assert "VRAM `2.0 / 8.0 GiB` `###-------`" in rendered
    assert "温度 `N/A`" in rendered


def test_render_metrics_reports_gpu_na() -> None:
    assert "*🟩 NVIDIA*\nGPU  `N/A`" in render_metrics(snapshot())
