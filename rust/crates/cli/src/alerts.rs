//! Loopback-only Alertmanager webhook delivery for the send-only alert Bot.

use crate::metrics::MetricsRegistry;
use codex_telegram_adapter::{ReqwestTransport, TelegramBotApi, TelegramSurfaceBinding};
use codex_telegram_credentials::BotToken;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use thiserror::Error;

const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_ALERTS: usize = 20;
const MAX_MESSAGE_BYTES: usize = 3900;

#[derive(Debug, Error)]
pub enum AlertWebhookError {
    #[error("Alertmanager webhook address is invalid")]
    InvalidAddress,
    #[error("Alertmanager webhook must bind to loopback")]
    NotLoopback,
    #[error("Alertmanager webhook could not bind")]
    BindFailed,
    #[error("Alertmanager webhook thread could not start")]
    ThreadStartFailed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct AlertmanagerPayload {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub alerts: Vec<AlertmanagerAlert>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct AlertmanagerAlert {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
}

pub struct AlertWebhookServer {
    shutdown: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl AlertWebhookServer {
    pub fn start(
        bind: &str,
        api: Arc<TelegramBotApi<ReqwestTransport>>,
        token: BotToken,
        surface: TelegramSurfaceBinding,
        metrics: MetricsRegistry,
    ) -> Result<Self, AlertWebhookError> {
        let address: SocketAddr = bind
            .parse()
            .map_err(|_| AlertWebhookError::InvalidAddress)?;
        if !is_loopback(address.ip()) {
            return Err(AlertWebhookError::NotLoopback);
        }
        let listener = TcpListener::bind(address).map_err(|_| AlertWebhookError::BindFailed)?;
        listener
            .set_nonblocking(true)
            .map_err(|_| AlertWebhookError::BindFailed)?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let flag = shutdown.clone();
        let join = thread::Builder::new()
            .name("codex-alert-webhook".into())
            .spawn(move || {
                while !flag.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            handle_connection(&stream, &api, &token, &surface, &metrics);
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(25));
                        }
                        Err(_) => break,
                    }
                }
            })
            .map_err(|_| AlertWebhookError::ThreadStartFailed)?;
        Ok(Self {
            shutdown,
            join: Some(join),
        })
    }
}

impl Drop for AlertWebhookServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

fn handle_connection(
    stream: &TcpStream,
    api: &TelegramBotApi<ReqwestTransport>,
    token: &BotToken,
    surface: &TelegramSurfaceBinding,
    metrics: &MetricsRegistry,
) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let result = read_request(stream).and_then(|request| {
        if request.method != "POST" {
            return Err(HttpFailure::MethodNotAllowed);
        }
        if request.path != "/alerts" {
            return Err(HttpFailure::NotFound);
        }
        let payload: AlertmanagerPayload =
            serde_json::from_slice(&request.body).map_err(|_| HttpFailure::BadRequest)?;
        let message = format_alertmanager_payload(&payload);
        let started = Instant::now();
        let delivery = api.send_text(token, surface, &message);
        metrics.observe_delivery_duration_for(
            "alert",
            delivery.is_ok(),
            started.elapsed().as_micros() as u64,
        );
        delivery.map_err(|_| HttpFailure::DeliveryFailed)?;
        Ok(())
    });
    let (status, body) = match result {
        Ok(()) => ("200 OK", "accepted\n"),
        Err(HttpFailure::BadRequest) => ("400 Bad Request", "invalid alert payload\n"),
        Err(HttpFailure::MethodNotAllowed) => ("405 Method Not Allowed", "method not allowed\n"),
        Err(HttpFailure::NotFound) => ("404 Not Found", "not found\n"),
        Err(HttpFailure::PayloadTooLarge) => ("413 Payload Too Large", "payload too large\n"),
        Err(HttpFailure::ReadFailed) => ("400 Bad Request", "invalid request\n"),
        Err(HttpFailure::DeliveryFailed) => ("503 Service Unavailable", "delivery failed\n"),
    };
    write_response(stream, status, body);
}

fn format_alertmanager_payload(payload: &AlertmanagerPayload) -> String {
    let status = if payload.status.trim().is_empty() {
        "unknown"
    } else {
        payload.status.as_str()
    };
    let mut message = format!(
        "[{}] Codex Telegram Bridge alert(s): {}\n",
        status,
        payload.alerts.len()
    );
    for alert in payload.alerts.iter().take(MAX_ALERTS) {
        let alert_name = alert
            .labels
            .get("alertname")
            .map(String::as_str)
            .unwrap_or("unnamed");
        let alert_status = if alert.status.trim().is_empty() {
            status
        } else {
            alert.status.as_str()
        };
        message.push_str(&format!("- {} ({})", alert_name, alert_status));
        for (key, value) in &alert.labels {
            if key == "alertname" {
                continue;
            }
            message.push_str(&format!(" {}={}", key, value));
        }
        if let Some(summary) = alert.annotations.get("summary") {
            message.push_str(&format!("\n  {}", summary));
        }
        if let Some(description) = alert.annotations.get("description") {
            message.push_str(&format!("\n  {}", description));
        }
        message.push('\n');
    }
    if payload.alerts.len() > MAX_ALERTS {
        message.push_str(&format!(
            "... {} more alert(s) omitted\n",
            payload.alerts.len() - MAX_ALERTS
        ));
    }
    truncate_message(&message)
}

fn truncate_message(message: &str) -> String {
    if message.len() <= MAX_MESSAGE_BYTES {
        return message.to_owned();
    }
    let mut end = MAX_MESSAGE_BYTES;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    let mut result = message[..end].to_owned();
    result.push_str("\n...[truncated]");
    result
}

struct HttpRequest {
    method: String,
    path: String,
    body: Vec<u8>,
}

#[derive(Clone, Copy)]
enum HttpFailure {
    BadRequest,
    MethodNotAllowed,
    NotFound,
    PayloadTooLarge,
    ReadFailed,
    DeliveryFailed,
}

fn read_request(stream: &TcpStream) -> Result<HttpRequest, HttpFailure> {
    let mut bytes = Vec::with_capacity(8192);
    let mut header_end = None;
    let mut content_length = None;
    let mut chunk = [0_u8; 8192];
    loop {
        if bytes.len() >= MAX_REQUEST_BYTES {
            return Err(HttpFailure::PayloadTooLarge);
        }
        let read = (&*stream)
            .read(&mut chunk)
            .map_err(|_| HttpFailure::ReadFailed)?;
        if read == 0 {
            return Err(HttpFailure::ReadFailed);
        }
        bytes.extend_from_slice(&chunk[..read]);
        if header_end.is_none() {
            header_end = find_header_end(&bytes);
            if let Some(end) = header_end {
                let header =
                    std::str::from_utf8(&bytes[..end]).map_err(|_| HttpFailure::BadRequest)?;
                let mut lines = header.split("\r\n");
                let request_line = lines.next().ok_or(HttpFailure::BadRequest)?;
                let mut parts = request_line.split_whitespace();
                let method = parts.next().ok_or(HttpFailure::BadRequest)?.to_owned();
                let path = parts.next().ok_or(HttpFailure::BadRequest)?.to_owned();
                if parts.next().is_none() {
                    return Err(HttpFailure::BadRequest);
                }
                for line in lines {
                    if let Some(value) = line.strip_prefix("Content-Length:") {
                        content_length = Some(
                            value
                                .trim()
                                .parse::<usize>()
                                .map_err(|_| HttpFailure::BadRequest)?,
                        );
                    }
                }
                let length = content_length.ok_or(HttpFailure::BadRequest)?;
                if length > MAX_REQUEST_BYTES {
                    return Err(HttpFailure::PayloadTooLarge);
                }
                let body_start = end + 4;
                let total = body_start
                    .checked_add(length)
                    .ok_or(HttpFailure::PayloadTooLarge)?;
                if total > MAX_REQUEST_BYTES {
                    return Err(HttpFailure::PayloadTooLarge);
                }
                if bytes.len() >= total {
                    return Ok(HttpRequest {
                        method,
                        path,
                        body: bytes[body_start..total].to_vec(),
                    });
                }
            }
        } else if let (Some(end), Some(length)) = (header_end, content_length) {
            let total = end + 4 + length;
            if bytes.len() >= total {
                let header =
                    std::str::from_utf8(&bytes[..end]).map_err(|_| HttpFailure::BadRequest)?;
                let mut parts = header
                    .split("\r\n")
                    .next()
                    .ok_or(HttpFailure::BadRequest)?
                    .split_whitespace();
                let method = parts.next().ok_or(HttpFailure::BadRequest)?.to_owned();
                let path = parts.next().ok_or(HttpFailure::BadRequest)?.to_owned();
                return Ok(HttpRequest {
                    method,
                    path,
                    body: bytes[end + 4..total].to_vec(),
                });
            }
        }
    }
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes.windows(4).position(|window| window == b"\r\n\r\n")
}

fn write_response(mut stream: &TcpStream, status: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_alerts_without_unbounded_output() {
        let payload = AlertmanagerPayload {
            status: "firing".into(),
            alerts: vec![AlertmanagerAlert {
                status: "firing".into(),
                labels: BTreeMap::from([
                    ("alertname".into(), "BridgeDown".into()),
                    ("severity".into(), "critical".into()),
                ]),
                annotations: BTreeMap::from([("summary".into(), "bridge unavailable".into())]),
            }],
        };
        let message = format_alertmanager_payload(&payload);
        assert!(message.contains("BridgeDown"));
        assert!(message.contains("severity=critical"));
        assert!(message.contains("bridge unavailable"));
        assert!(message.len() <= MAX_MESSAGE_BYTES + 15);
    }

    #[test]
    fn rejects_non_loopback_listener() {
        let result = AlertWebhookServer::start(
            "0.0.0.0:18091",
            Arc::new(TelegramBotApi::new(
                ReqwestTransport::new(Duration::from_secs(1)).unwrap(),
            )),
            BotToken::parse("123456:token").unwrap(),
            TelegramSurfaceBinding::Channel(
                codex_telegram_adapter::ChannelBinding::new("monitoring", "1").unwrap(),
            ),
            MetricsRegistry::default(),
        );
        assert!(matches!(result, Err(AlertWebhookError::NotLoopback)));
    }

    #[test]
    fn locates_http_header_end() {
        assert_eq!(
            find_header_end(b"POST /alerts HTTP/1.1\r\n\r\nbody"),
            Some(21)
        );
    }
}
