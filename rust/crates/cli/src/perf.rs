//! Bounded local performance sampling for the Control Bot `/perf` panel.

use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sysinfo::{Disks, ProcessesToUpdate, System};

const GPU_COMMAND_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Debug, PartialEq)]
pub struct GpuSnapshot {
    pub name: String,
    pub memory_used_mib: Option<f32>,
    pub memory_total_mib: Option<f32>,
    pub utilization_percent: Option<f32>,
    pub temperature_c: Option<f32>,
    pub power_w: Option<f32>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PerfSnapshot {
    pub sampled_at_ms: i64,
    pub uptime_seconds: u64,
    pub load: [f32; 3],
    pub cpu_percent: f32,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub swap_used_bytes: u64,
    pub swap_total_bytes: u64,
    pub disk_used_bytes: u64,
    pub disk_total_bytes: u64,
    pub codex_process_count: usize,
    pub codex_cpu_percent: f32,
    pub codex_memory_bytes: u64,
    pub gpu: Option<GpuSnapshot>,
}

pub struct PerfSampler {
    system: Mutex<System>,
    disks: Mutex<Disks>,
    last_cpu_refresh: Mutex<Option<Instant>>,
}

impl Default for PerfSampler {
    fn default() -> Self {
        Self::new()
    }
}

impl PerfSampler {
    pub fn new() -> Self {
        Self {
            system: Mutex::new(System::new_all()),
            disks: Mutex::new(Disks::new_with_refreshed_list()),
            last_cpu_refresh: Mutex::new(None),
        }
    }

    pub fn sample(&self, include_gpu: bool) -> PerfSnapshot {
        let (
            uptime_seconds,
            load,
            cpu_percent,
            memory_used_bytes,
            memory_total_bytes,
            swap_used_bytes,
            swap_total_bytes,
            codex_process_count,
            codex_cpu_percent,
            codex_memory_bytes,
        ) = {
            let mut system = self.system.lock().expect("perf system lock poisoned");
            let should_refresh_cpu = self
                .last_cpu_refresh
                .lock()
                .expect("perf cpu timestamp lock poisoned")
                .is_none_or(|last| last.elapsed() >= sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
            if should_refresh_cpu {
                system.refresh_cpu_usage();
                *self
                    .last_cpu_refresh
                    .lock()
                    .expect("perf cpu timestamp lock poisoned") = Some(Instant::now());
            }
            system.refresh_memory();
            system.refresh_processes(ProcessesToUpdate::All, false);
            let memory_total_bytes = system.total_memory();
            let memory_used_bytes = memory_total_bytes.saturating_sub(system.available_memory());
            let mut codex_process_count = 0;
            let mut codex_cpu_percent = 0.0;
            let mut codex_memory_bytes: u64 = 0;
            for process in system.processes().values() {
                let name = process.name().to_string_lossy();
                let exe_name = process
                    .exe()
                    .and_then(|exe| exe.file_name())
                    .map(|exe| exe.to_string_lossy().into_owned());
                if codex_process_matches(&name, exe_name.as_deref()) {
                    codex_process_count += 1;
                    codex_cpu_percent += process.cpu_usage();
                    codex_memory_bytes = codex_memory_bytes.saturating_add(process.memory());
                }
            }
            (
                System::uptime(),
                {
                    let load = System::load_average();
                    [load.one as f32, load.five as f32, load.fifteen as f32]
                },
                system.global_cpu_usage(),
                memory_used_bytes,
                memory_total_bytes,
                system.used_swap(),
                system.total_swap(),
                codex_process_count,
                codex_cpu_percent,
                codex_memory_bytes,
            )
        };
        let (disk_used_bytes, disk_total_bytes) = {
            let mut disks = self.disks.lock().expect("perf disk lock poisoned");
            disks.refresh(false);
            let disk = disks
                .iter()
                .find(|disk| disk.mount_point() == Path::new("/"))
                .or_else(|| disks.iter().next());
            disk.map_or((0, 0), |disk| {
                let total = disk.total_space();
                (total.saturating_sub(disk.available_space()), total)
            })
        };
        PerfSnapshot {
            sampled_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(i64::MAX as u128) as i64,
            uptime_seconds,
            load,
            cpu_percent,
            memory_used_bytes,
            memory_total_bytes,
            swap_used_bytes,
            swap_total_bytes,
            disk_used_bytes,
            disk_total_bytes,
            codex_process_count,
            codex_cpu_percent,
            codex_memory_bytes,
            gpu: include_gpu.then(sample_gpu).flatten(),
        }
    }
}

/// Counts bridge and Codex app-server processes for the `/perf` Codex
/// section. The process name (`comm`) is matched first; the executable
/// basename is a fallback because runtimes may rename `comm` (for example to
/// `MainThread`) and the app-server binary may be installed as
/// `codex-app-server` or plain `app-server`, in which case a `comm`-only
/// match reports zero processes, CPU, and RSS.
fn codex_process_matches(name: &str, exe_name: Option<&str>) -> bool {
    fn matches(value: &str) -> bool {
        let value = value.to_ascii_lowercase();
        value.contains("codex") || value.contains("app-server") || value.contains("app_server")
    }
    matches(name) || exe_name.is_some_and(matches)
}

fn sample_gpu() -> Option<GpuSnapshot> {
    let mut child = ["nvidia-smi", "/usr/lib/wsl/lib/nvidia-smi"]
        .into_iter()
        .find_map(|program| {
            Command::new(program)
                .args([
                    "--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw",
                    "--format=csv,noheader,nounits",
                ])
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .ok()
        })?;
    let deadline = Instant::now() + GPU_COMMAND_TIMEOUT;
    loop {
        match child.try_wait().ok()? {
            Some(status) if status.success() => {
                let output = child.wait_with_output().ok()?;
                let output_text = String::from_utf8_lossy(&output.stdout);
                let line = output_text
                    .lines()
                    .next()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())?;
                let fields = line.split(',').map(str::trim).collect::<Vec<_>>();
                if fields.len() < 6 || fields[0].is_empty() {
                    return None;
                }
                let parse = |value: &str| {
                    let value = value.trim();
                    if value.is_empty() || value.eq_ignore_ascii_case("n/a") {
                        None
                    } else {
                        value.parse::<f32>().ok()
                    }
                };
                return Some(GpuSnapshot {
                    name: fields[0].to_owned(),
                    utilization_percent: parse(fields[1]),
                    memory_used_mib: parse(fields[2]),
                    memory_total_mib: parse(fields[3]),
                    temperature_c: parse(fields[4]),
                    power_w: parse(fields[5]),
                });
            }
            Some(_) => return None,
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
            None => thread::sleep(Duration::from_millis(25)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_process_matching_covers_comm_exe_and_app_server_names() {
        assert!(codex_process_matches("codex", None));
        // `comm` is truncated to 15 bytes for the installed bridge binary.
        assert!(codex_process_matches("codex-telegram-", None));
        assert!(codex_process_matches("codex-app-server", None));
        assert!(codex_process_matches("app-server", None));
        // A runtime-renamed `comm` still matches via the executable basename.
        assert!(codex_process_matches("MainThread", Some("codex")));
        assert!(codex_process_matches("node", Some("app-server")));
        assert!(!codex_process_matches("grafana", None));
        assert!(!codex_process_matches("prometheus", Some("prometheus")));
    }

    #[test]
    fn sampler_returns_bounded_snapshot_without_gpu_requirement() {
        let sampler = PerfSampler::new();
        let snapshot = sampler.sample(false);
        assert!(snapshot.memory_total_bytes >= snapshot.memory_used_bytes);
        assert!(snapshot.swap_total_bytes >= snapshot.swap_used_bytes);
        assert!(snapshot.disk_total_bytes >= snapshot.disk_used_bytes);
        assert!(snapshot.gpu.is_none());
    }

    #[test]
    fn cpu_refresh_timestamp_prevents_subinterval_refresh_contract_breakage() {
        let sampler = PerfSampler::new();
        let first = sampler.sample(false);
        let second = sampler.sample(false);
        assert!(second.sampled_at_ms >= first.sampled_at_ms);
        assert!(sampler.last_cpu_refresh.lock().unwrap().is_some());
    }
}
