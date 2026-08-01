//! Loopback-only Prometheus text exporter for the Rust vNext process.

use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Clone)]
pub struct MetricsRegistry {
    start_time: u64,
    component_healthy: Arc<AtomicBool>,
    poll_last_success: Arc<AtomicU64>,
    poll_success: Arc<AtomicU64>,
    poll_failed: Arc<AtomicU64>,
    delivery_success: Arc<AtomicU64>,
    delivery_failed: Arc<AtomicU64>,
    delivery_success_duration_micros: Arc<AtomicU64>,
    delivery_failed_duration_micros: Arc<AtomicU64>,
    queue_depth: Arc<AtomicU64>,
    event_loop_lag_micros: Arc<AtomicU64>,
    supervisor_restarts: Arc<AtomicU64>,
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self {
            start_time: unix_seconds(),
            component_healthy: Arc::new(AtomicBool::new(true)),
            poll_last_success: Arc::new(AtomicU64::new(0)),
            poll_success: Arc::new(AtomicU64::new(0)),
            poll_failed: Arc::new(AtomicU64::new(0)),
            delivery_success: Arc::new(AtomicU64::new(0)),
            delivery_failed: Arc::new(AtomicU64::new(0)),
            delivery_success_duration_micros: Arc::new(AtomicU64::new(0)),
            delivery_failed_duration_micros: Arc::new(AtomicU64::new(0)),
            queue_depth: Arc::new(AtomicU64::new(0)),
            event_loop_lag_micros: Arc::new(AtomicU64::new(0)),
            supervisor_restarts: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl MetricsRegistry {
    pub fn set_component_healthy(&self, healthy: bool) {
        self.component_healthy.store(healthy, Ordering::Relaxed);
    }

    pub fn observe_poll(&self, success: bool) {
        if success {
            self.poll_success.fetch_add(1, Ordering::Relaxed);
            self.poll_last_success
                .store(unix_seconds(), Ordering::Relaxed);
        } else {
            self.poll_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn observe_delivery(&self, success: bool) {
        self.observe_delivery_duration(success, 0);
    }

    pub fn observe_delivery_duration(&self, success: bool, duration_micros: u64) {
        if success {
            self.delivery_success.fetch_add(1, Ordering::Relaxed);
            self.delivery_success_duration_micros
                .fetch_add(duration_micros, Ordering::Relaxed);
        } else {
            self.delivery_failed.fetch_add(1, Ordering::Relaxed);
            self.delivery_failed_duration_micros
                .fetch_add(duration_micros, Ordering::Relaxed);
        }
    }

    pub fn set_queue_depth(&self, depth: u64) {
        self.queue_depth.store(depth, Ordering::Relaxed);
    }

    pub fn set_event_loop_lag_micros(&self, lag: u64) {
        self.event_loop_lag_micros.store(lag, Ordering::Relaxed);
    }

    pub fn observe_supervisor_restart(&self) {
        self.supervisor_restarts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn render(&self) -> String {
        let healthy = u8::from(self.component_healthy.load(Ordering::Relaxed));
        let poll_last = self.poll_last_success.load(Ordering::Relaxed);
        let poll_success = self.poll_success.load(Ordering::Relaxed);
        let poll_failed = self.poll_failed.load(Ordering::Relaxed);
        let delivery_success = self.delivery_success.load(Ordering::Relaxed);
        let delivery_failed = self.delivery_failed.load(Ordering::Relaxed);
        let delivery_success_sum = self
            .delivery_success_duration_micros
            .load(Ordering::Relaxed) as f64
            / 1_000_000.0;
        let delivery_failed_sum =
            self.delivery_failed_duration_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let queue_depth = self.queue_depth.load(Ordering::Relaxed);
        let lag = self.event_loop_lag_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let restarts = self.supervisor_restarts.load(Ordering::Relaxed);
        format!(
            "# TYPE codex_telegram_bridge_build_info gauge\n\
codex_telegram_bridge_build_info{{version=\"0.1.0\",revision=\"local\"}} 1\n\
# TYPE codex_telegram_bridge_component_healthy gauge\n\
codex_telegram_bridge_component_healthy{{component=\"bridge\"}} {healthy}\n\
codex_telegram_bridge_component_healthy{{component=\"telegram_polling\"}} {healthy}\n\
# TYPE codex_telegram_bridge_process_start_time_seconds gauge\n\
codex_telegram_bridge_process_start_time_seconds {}\n\
# TYPE codex_telegram_bridge_telegram_poll_last_success_unixtime gauge\n\
codex_telegram_bridge_telegram_poll_last_success_unixtime{{bot_role=\"discussion\"}} {poll_last}\n\
# TYPE codex_telegram_bridge_telegram_poll_requests_total counter\n\
codex_telegram_bridge_telegram_poll_requests_total{{bot_role=\"discussion\",result=\"success\"}} {poll_success}\n\
codex_telegram_bridge_telegram_poll_requests_total{{bot_role=\"discussion\",result=\"failed\"}} {poll_failed}\n\
# TYPE codex_telegram_bridge_telegram_delivery_attempts_total counter\n\
codex_telegram_bridge_telegram_delivery_attempts_total{{bot_role=\"discussion\",result=\"success\"}} {delivery_success}\n\
codex_telegram_bridge_telegram_delivery_attempts_total{{bot_role=\"discussion\",result=\"failed\"}} {delivery_failed}\n\
# TYPE codex_telegram_bridge_telegram_delivery_duration_seconds histogram\n\
codex_telegram_bridge_telegram_delivery_duration_seconds_bucket{{bot_role=\"discussion\",result=\"success\",le=\"0.5\"}} 0\n\
codex_telegram_bridge_telegram_delivery_duration_seconds_bucket{{bot_role=\"discussion\",result=\"success\",le=\"1\"}} 0\n\
codex_telegram_bridge_telegram_delivery_duration_seconds_bucket{{bot_role=\"discussion\",result=\"success\",le=\"+Inf\"}} {delivery_success}\n\
codex_telegram_bridge_telegram_delivery_duration_seconds_sum{{bot_role=\"discussion\",result=\"success\"}} {delivery_success_sum:.6}\n\
codex_telegram_bridge_telegram_delivery_duration_seconds_count{{bot_role=\"discussion\",result=\"success\"}} {delivery_success}\n\
codex_telegram_bridge_telegram_delivery_duration_seconds_bucket{{bot_role=\"discussion\",result=\"failed\",le=\"0.5\"}} 0\n\
codex_telegram_bridge_telegram_delivery_duration_seconds_bucket{{bot_role=\"discussion\",result=\"failed\",le=\"1\"}} 0\n\
codex_telegram_bridge_telegram_delivery_duration_seconds_bucket{{bot_role=\"discussion\",result=\"failed\",le=\"+Inf\"}} {delivery_failed}\n\
codex_telegram_bridge_telegram_delivery_duration_seconds_sum{{bot_role=\"discussion\",result=\"failed\"}} {delivery_failed_sum:.6}\n\
codex_telegram_bridge_telegram_delivery_duration_seconds_count{{bot_role=\"discussion\",result=\"failed\"}} {delivery_failed}\n\
# TYPE codex_telegram_bridge_queue_depth gauge\n\
codex_telegram_bridge_queue_depth{{queue=\"workload\"}} {queue_depth}\n\
# TYPE codex_telegram_bridge_event_loop_lag_seconds gauge\n\
codex_telegram_bridge_event_loop_lag_seconds {lag:.6}\n\
# TYPE codex_telegram_bridge_supervisor_restarts_total counter\n\
codex_telegram_bridge_supervisor_restarts_total{{component=\"telegram_polling\"}} {restarts}\n",
            self.start_time
        )
    }
}

pub struct MetricsServer {
    shutdown: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl MetricsServer {
    pub fn start(bind: &str, registry: MetricsRegistry) -> Result<Self, MetricsError> {
        let address: SocketAddr = bind.parse().map_err(|_| MetricsError::InvalidAddress)?;
        if !is_loopback(address.ip()) {
            return Err(MetricsError::NotLoopback);
        }
        let listener = TcpListener::bind(address).map_err(|_| MetricsError::BindFailed)?;
        listener
            .set_nonblocking(true)
            .map_err(|_| MetricsError::BindFailed)?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let flag = shutdown.clone();
        let join = thread::Builder::new()
            .name("codex-metrics".into())
            .spawn(move || {
                while !flag.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => serve_connection(stream, &registry),
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(std::time::Duration::from_millis(25));
                        }
                        Err(_) => break,
                    }
                }
            })
            .map_err(|_| MetricsError::ThreadStartFailed)?;
        Ok(Self {
            shutdown,
            join: Some(join),
        })
    }
}

impl Drop for MetricsServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn serve_connection(mut stream: TcpStream, registry: &MetricsRegistry) {
    let mut request = [0_u8; 2048];
    let size = stream.read(&mut request).unwrap_or(0);
    let request = String::from_utf8_lossy(&request[..size]);
    let path = request.split_whitespace().nth(1).unwrap_or("/");
    let (status, content_type, body) = if path == "/metrics" {
        ("200 OK", "text/plain; version=0.0.4", registry.render())
    } else {
        (
            "404 Not Found",
            "text/plain; version=0.0.4",
            "not found\n".into(),
        )
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
}

fn is_loopback(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(value) => value.is_loopback(),
        IpAddr::V6(value) => value.is_loopback(),
    }
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("metrics bind address is invalid")]
    InvalidAddress,
    #[error("metrics listener must bind to loopback")]
    NotLoopback,
    #[error("metrics listener could not bind")]
    BindFailed,
    #[error("metrics thread could not start")]
    ThreadStartFailed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_has_contract_names_and_no_unbounded_labels() {
        let metrics = MetricsRegistry::default();
        let body = metrics.render();
        assert!(body.contains("codex_telegram_bridge_component_healthy"));
        assert!(body.contains("bot_role=\"discussion\""));
        assert!(!body.contains("token"));
    }

    #[test]
    fn non_loopback_listener_is_rejected() {
        assert!(matches!(
            MetricsServer::start("0.0.0.0:9465", MetricsRegistry::default()),
            Err(MetricsError::NotLoopback)
        ));
    }
}
