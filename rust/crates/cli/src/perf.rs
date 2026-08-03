//! Bounded local performance sampling for the Control Bot `/perf` panel.

use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sysinfo::{Disks, ProcessesToUpdate, System};

const GPU_COMMAND_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Debug, PartialEq)]
pub struct PerfSnapshot {
    pub sampled_at_ms: i64,
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
    pub gpu: Option<String>,
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
            let mut codex_process_count = 0;
            let mut codex_cpu_percent = 0.0;
            let mut codex_memory_bytes: u64 = 0;
            for process in system.processes().values() {
                let name = process.name().to_string_lossy().to_ascii_lowercase();
                if name.contains("codex") {
                    codex_process_count += 1;
                    codex_cpu_percent += process.cpu_usage();
                    codex_memory_bytes = codex_memory_bytes.saturating_add(process.memory());
                }
            }
            (
                system.global_cpu_usage(),
                system.used_memory(),
                system.total_memory(),
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
            disks.iter().fold((0_u64, 0_u64), |(used, total), disk| {
                let disk_total = disk.total_space();
                let disk_used = disk_total.saturating_sub(disk.available_space());
                (
                    used.saturating_add(disk_used),
                    total.saturating_add(disk_total),
                )
            })
        };
        PerfSnapshot {
            sampled_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(i64::MAX as u128) as i64,
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

fn sample_gpu() -> Option<String> {
    let mut child = Command::new("nvidia-smi")
        .args([
            "--query-gpu=name,utilization.gpu,memory.used,memory.total,temperature.gpu",
            "--format=csv,noheader,nounits",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
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
                return Some(line.to_owned());
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
