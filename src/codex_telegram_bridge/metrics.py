from __future__ import annotations

import asyncio
import contextlib
import csv
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

import psutil

from .markdown import escape, inline_code

_NVIDIA_SMI_WSL = "/usr/lib/wsl/lib/nvidia-smi"
_NVIDIA_QUERY = (
    "index,name,memory.used,memory.total,utilization.gpu,temperature.gpu,power.draw"
)


@dataclass(frozen=True, slots=True)
class GpuSnapshot:
    index: int
    name: str
    memory_used_mib: float | None
    memory_total_mib: float | None
    utilization_percent: float | None
    temperature_c: float | None
    power_w: float | None


@dataclass(slots=True)
class MetricsSnapshot:
    sampled_at: int
    uptime_seconds: int
    load: tuple[float, float, float]
    cpu_percent: float
    memory_total: int
    memory_available: int
    memory_percent: float
    swap_total: int
    swap_used: int
    swap_percent: float
    disk_total: int
    disk_free: int
    disk_percent: float
    codex_processes: int
    codex_rss: int
    codex_cpu: float
    gpus: tuple[GpuSnapshot, ...] = ()
    # Kept for snapshots created by older callers during the schema transition.
    gpu: str | None = None


def _human_bytes(value: int) -> str:
    number = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if number < 1024 or unit == "TiB":
            return f"{number:.1f} {unit}"
        number /= 1024
    return f"{number:.1f} TiB"


def ascii_bar(percent: float | None, cells: int = 10) -> str:
    if cells < 1:
        raise ValueError("cells must be positive")
    value = min(100.0, max(0.0, float(percent or 0.0)))
    filled = min(cells, int(value * cells / 100.0 + 0.5))
    return "#" * filled + "-" * (cells - filled)


def resolve_nvidia_smi() -> str | None:
    resolved = shutil.which("nvidia-smi")
    if resolved:
        return resolved
    if os.path.isfile(_NVIDIA_SMI_WSL) and os.access(_NVIDIA_SMI_WSL, os.X_OK):
        return _NVIDIA_SMI_WSL
    return None


def _optional_float(value: str) -> float | None:
    normalized = value.strip()
    if not normalized or normalized.casefold() in {"n/a", "[n/a]", "not supported"}:
        return None
    try:
        return float(normalized)
    except ValueError:
        return None


def parse_nvidia_smi_csv(output: str) -> tuple[GpuSnapshot, ...]:
    snapshots: list[GpuSnapshot] = []
    for row_number, row in enumerate(csv.reader(output.splitlines(), skipinitialspace=True)):
        if len(row) != 7:
            continue
        try:
            index = int(row[0].strip())
        except ValueError:
            index = row_number
        name = row[1].strip()
        if not name:
            continue
        snapshots.append(
            GpuSnapshot(
                index=index,
                name=name,
                memory_used_mib=_optional_float(row[2]),
                memory_total_mib=_optional_float(row[3]),
                utilization_percent=_optional_float(row[4]),
                temperature_c=_optional_float(row[5]),
                power_w=_optional_float(row[6]),
            )
        )
    return tuple(snapshots)


async def _terminate_process(process: asyncio.subprocess.Process) -> None:
    with contextlib.suppress(ProcessLookupError):
        process.kill()
    with contextlib.suppress(Exception):
        await process.communicate()
        return
    with contextlib.suppress(Exception):
        await process.wait()


async def collect_gpu_metrics(
    *, timeout: float = 3.0, executable: str | None = None
) -> tuple[GpuSnapshot, ...]:
    command = executable or resolve_nvidia_smi()
    if not command:
        return ()
    try:
        process = await asyncio.create_subprocess_exec(
            command,
            f"--query-gpu={_NVIDIA_QUERY}",
            "--format=csv,noheader,nounits",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
    except OSError:
        return ()
    try:
        stdout, _ = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except TimeoutError:
        await _terminate_process(process)
        return ()
    if process.returncode:
        return ()
    return parse_nvidia_smi_csv(stdout.decode("utf-8", errors="replace"))


class MetricsSampler:
    def __init__(self, disk_path: Path, interval: float = 5.0) -> None:
        self.disk_path = disk_path
        self.interval = interval
        self.snapshot: MetricsSnapshot | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    def start(self) -> None:
        if self._task and not self._task.done():
            return
        psutil.cpu_percent(interval=None)
        for process in psutil.process_iter(["name"]):
            with contextlib.suppress(psutil.Error):
                process.cpu_percent(interval=None)
        self._stop.clear()
        self._task = asyncio.create_task(self._run(), name="metrics-sampler")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _run(self) -> None:
        while not self._stop.is_set():
            self.snapshot = await self.sample()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), self.interval)

    async def sample(self) -> MetricsSnapshot:
        return await asyncio.to_thread(self._sample_sync)

    def _sample_sync(self) -> MetricsSnapshot:
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()
        disk = psutil.disk_usage(self.disk_path)
        codex_processes = 0
        codex_rss = 0
        codex_cpu = 0.0
        for process in psutil.process_iter(["name", "cmdline", "memory_info"]):
            try:
                name = (process.info.get("name") or "").casefold()
                command = " ".join(process.info.get("cmdline") or []).casefold()
                if "codex" not in name and "codex" not in command:
                    continue
                codex_processes += 1
                memory_info = process.info.get("memory_info")
                codex_rss += int(memory_info.rss) if memory_info else 0
                codex_cpu += process.cpu_percent(interval=None)
            except psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess:
                continue
        return MetricsSnapshot(
            sampled_at=int(time.time()),
            uptime_seconds=max(0, int(time.time() - psutil.boot_time())),
            load=tuple(float(value) for value in os.getloadavg()),
            cpu_percent=psutil.cpu_percent(interval=None),
            memory_total=int(memory.total),
            memory_available=int(memory.available),
            memory_percent=float(memory.percent),
            swap_total=int(swap.total),
            swap_used=int(swap.used),
            swap_percent=float(swap.percent),
            disk_total=int(disk.total),
            disk_free=int(disk.free),
            disk_percent=float(disk.percent),
            codex_processes=codex_processes,
            codex_rss=codex_rss,
            codex_cpu=codex_cpu,
        )

    async def with_gpu(self) -> MetricsSnapshot:
        # Interactive metrics must not reuse the background sampler's older host sample.
        snapshot = await self.sample()
        snapshot.gpus = await _gpu_status()
        return snapshot


async def _gpu_status() -> tuple[GpuSnapshot, ...]:
    return await collect_gpu_metrics()


def _legacy_gpu(snapshot: MetricsSnapshot) -> tuple[GpuSnapshot, ...]:
    if not snapshot.gpu:
        return ()
    fields = [part.strip() for part in snapshot.gpu.split(",", 5)]
    if len(fields) != 6:
        return ()
    return (
        GpuSnapshot(
            index=0,
            name=fields[0],
            memory_used_mib=_optional_float(fields[1]),
            memory_total_mib=_optional_float(fields[2]),
            utilization_percent=_optional_float(fields[3]),
            temperature_c=_optional_float(fields[4]),
            power_w=_optional_float(fields[5]),
        ),
    )


def _format_number(value: float | None, unit: str = "") -> str:
    return "N/A" if value is None else f"{value:.1f}{unit}"


def render_metrics(snapshot: MetricsSnapshot) -> str:
    hours, remainder = divmod(snapshot.uptime_seconds, 3600)
    days, hours = divmod(hours, 24)
    minutes = remainder // 60
    memory_used = snapshot.memory_total - snapshot.memory_available
    disk_used = snapshot.disk_total - snapshot.disk_free
    memory_percent = snapshot.memory_percent
    disk_percent = snapshot.disk_percent
    lines = [
        "*🟠 Ubuntu · WSL*",
        f"CPU  {inline_code(f'{snapshot.cpu_percent:.1f}%')} {inline_code(ascii_bar(snapshot.cpu_percent))}",
        f"RAM  {inline_code(f'{memory_percent:.1f}%')} {inline_code(ascii_bar(memory_percent))}",
        f"Swap {inline_code(f'{snapshot.swap_percent:.1f}%')} "
        f"{inline_code(ascii_bar(snapshot.swap_percent))}",
        f"Disk {inline_code(f'{disk_percent:.1f}%')} {inline_code(ascii_bar(disk_percent))}",
        f"内存 {inline_code(f'{_human_bytes(memory_used)} / {_human_bytes(snapshot.memory_total)}')}",
        f"交换 {inline_code(f'{_human_bytes(snapshot.swap_used)} / {_human_bytes(snapshot.swap_total)}')}",
        f"磁盘 {inline_code(f'{_human_bytes(disk_used)} / {_human_bytes(snapshot.disk_total)}')}",
        f"负载 {inline_code(' / '.join(f'{value:.2f}' for value in snapshot.load))}",
        f"运行 {inline_code(f'{days}d {hours}h {minutes}m')}",
        "",
        "*⚙️ Codex*",
        f"进程 {inline_code(snapshot.codex_processes)} · RSS {inline_code(_human_bytes(snapshot.codex_rss))}",
        f"CPU  {inline_code(f'{snapshot.codex_cpu:.1f}%')} {inline_code(ascii_bar(snapshot.codex_cpu))}",
    ]
    gpus = snapshot.gpus or _legacy_gpu(snapshot)
    if not gpus:
        lines.extend(["", "*🟩 NVIDIA*", "GPU  `N/A`"])
    for gpu in gpus:
        lines.extend(["", f"*🟩 NVIDIA · {escape(gpu.name)}*"])
        lines.append(
            f"GPU  {inline_code(_format_number(gpu.utilization_percent, '%'))} "
            f"{inline_code(ascii_bar(gpu.utilization_percent))}"
        )
        if gpu.memory_used_mib is None or gpu.memory_total_mib is None:
            lines.append("VRAM `N/A`")
        else:
            memory_ratio = (
                100.0 * gpu.memory_used_mib / gpu.memory_total_mib if gpu.memory_total_mib else 0.0
            )
            if gpu.memory_total_mib >= 1024:
                memory = f"{gpu.memory_used_mib / 1024:.1f} / {gpu.memory_total_mib / 1024:.1f} GiB"
            else:
                memory = f"{gpu.memory_used_mib:.0f} / {gpu.memory_total_mib:.0f} MiB"
            lines.append(f"VRAM {inline_code(memory)} {inline_code(ascii_bar(memory_ratio))}")
        temperature = inline_code(_format_number(gpu.temperature_c, "°C"))
        power = inline_code(_format_number(gpu.power_w, " W"))
        lines.append(f"温度 {temperature} · 功耗 {power}")
    lines.append("")
    lines.append(f"采样 {inline_code(time.strftime('%H:%M:%S', time.localtime(snapshot.sampled_at)))}")
    return "\n".join(lines)


def render_metrics_plain(snapshot: MetricsSnapshot) -> str:
    markdown = render_metrics(snapshot)
    return markdown.replace("*", "").replace("`", "").replace("\\", "")
