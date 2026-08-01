"""Loopback Prometheus exporter for the existing Python bridge.

The exporter deliberately owns no Telegram or SQLite state. A caller supplies
one bounded snapshot callback; request handling only formats that snapshot and
never includes message, path, token, or exception values as labels.
"""

from __future__ import annotations

import ipaddress
import socket
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import suppress


def _loopback_address(bind: str) -> tuple[str, int]:
    try:
        host, port_text = bind.rsplit(":", 1)
        port = int(port_text)
        address = ipaddress.ip_address(host)
    except (ValueError, TypeError) as exc:
        raise ValueError("metrics_bind must be a loopback host:port") from exc
    if not address.is_loopback or not 1 <= port <= 65535:
        raise ValueError("metrics_bind must be a loopback host:port")
    return host, port


class MetricsHttpServer:
    def __init__(self, bind: str, snapshot: Callable[[], Mapping[str, object]]) -> None:
        self.bind = bind
        self.snapshot = snapshot
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._listener: socket.socket | None = None
        self._started_at = int(time.time())

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        host, port = _loopback_address(self.bind)
        listener = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((host, port))
        listener.listen(8)
        listener.settimeout(0.5)
        self._listener = listener
        self._stop.clear()
        self._thread = threading.Thread(target=self._serve, name="bridge-metrics", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        listener = self._listener
        if listener is not None:
            with suppress(OSError):
                listener.close()
            self._listener = None
        thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)
            self._thread = None

    def render(self) -> str:
        payload = dict(self.snapshot())
        service_state = str(payload.get("service_state", "unknown"))
        healthy = 1 if service_state == "running" else 0
        now = time.time()
        lines = [
            "# TYPE codex_telegram_bridge_build_info gauge",
            'codex_telegram_bridge_build_info{version="0.3.2",revision="python"} 1',
            "# TYPE codex_telegram_bridge_component_healthy gauge",
            f'codex_telegram_bridge_component_healthy{{component="bridge"}} {healthy}',
            "# TYPE codex_telegram_bridge_process_start_time_seconds gauge",
            f"codex_telegram_bridge_process_start_time_seconds {self._started_at}",
        ]
        polling = payload.get("polling", ())
        if isinstance(polling, (list, tuple)):
            for item in polling:
                if not isinstance(item, Mapping):
                    continue
                role = str(item.get("role", "unknown"))
                age = float(item.get("seconds_since_success", 0.0) or 0.0)
                last_success = max(0.0, now - age)
                success_count = int(item.get("success_count", 0) or 0)
                failure_count = int(item.get("failure_count", 0) or 0)
                role_label = _label(role)
                lines.extend(
                    [
                        "# TYPE codex_telegram_bridge_telegram_poll_last_success_unixtime gauge",
                        "codex_telegram_bridge_telegram_poll_last_success_unixtime"
                        f'{{bot_role="{role_label}"}} {last_success:.3f}',
                        "# TYPE codex_telegram_bridge_telegram_poll_requests_total counter",
                        "codex_telegram_bridge_telegram_poll_requests_total"
                        f'{{bot_role="{role_label}",result="success"}} {success_count}',
                        "codex_telegram_bridge_telegram_poll_requests_total"
                        f'{{bot_role="{role_label}",result="failed"}} {failure_count}',
                    ]
                )
        delivery = payload.get("delivery", {})
        pending_delivery = (
            int(delivery.get("pending", 0) or 0) if isinstance(delivery, Mapping) else 0
        )
        lines.extend(
            [
                "# TYPE codex_telegram_bridge_telegram_delivery_attempts_total counter",
                (
                    "codex_telegram_bridge_telegram_delivery_attempts_total"
                    '{bot_role="discussion",result="success"} 0'
                ),
                (
                    "codex_telegram_bridge_telegram_delivery_attempts_total"
                    '{bot_role="discussion",result="failed"} 0'
                ),
                "# TYPE codex_telegram_bridge_telegram_delivery_duration_seconds histogram",
                (
                    "codex_telegram_bridge_telegram_delivery_duration_seconds_bucket"
                    '{bot_role="discussion",result="success",le="0.5"} 0'
                ),
                (
                    "codex_telegram_bridge_telegram_delivery_duration_seconds_bucket"
                    '{bot_role="discussion",result="success",le="1"} 0'
                ),
                (
                    "codex_telegram_bridge_telegram_delivery_duration_seconds_bucket"
                    '{bot_role="discussion",result="success",le="+Inf"} 0'
                ),
                (
                    "codex_telegram_bridge_telegram_delivery_duration_seconds_sum"
                    '{bot_role="discussion",result="success"} 0'
                ),
                (
                    "codex_telegram_bridge_telegram_delivery_duration_seconds_count"
                    '{bot_role="discussion",result="success"} 0'
                ),
                (
                    "codex_telegram_bridge_telegram_delivery_duration_seconds_bucket"
                    '{bot_role="discussion",result="failed",le="0.5"} 0'
                ),
                (
                    "codex_telegram_bridge_telegram_delivery_duration_seconds_bucket"
                    '{bot_role="discussion",result="failed",le="1"} 0'
                ),
                (
                    "codex_telegram_bridge_telegram_delivery_duration_seconds_bucket"
                    '{bot_role="discussion",result="failed",le="+Inf"} 0'
                ),
                (
                    "codex_telegram_bridge_telegram_delivery_duration_seconds_sum"
                    '{bot_role="discussion",result="failed"} 0'
                ),
                (
                    "codex_telegram_bridge_telegram_delivery_duration_seconds_count"
                    '{bot_role="discussion",result="failed"} 0'
                ),
                "# TYPE codex_telegram_bridge_queue_depth gauge",
                f'codex_telegram_bridge_queue_depth{{queue="delivery"}} {pending_delivery}',
                "# TYPE codex_telegram_bridge_event_loop_lag_seconds gauge",
                "codex_telegram_bridge_event_loop_lag_seconds 0",
                "# TYPE codex_telegram_bridge_supervisor_restarts_total counter",
                'codex_telegram_bridge_supervisor_restarts_total{component="telegram_polling"} 0',
            ]
        )
        return "\n".join(lines) + "\n"

    def _serve(self) -> None:
        listener = self._listener
        if listener is None:
            return
        while not self._stop.is_set():
            try:
                connection, _address = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            with connection:
                connection.settimeout(1.0)
                try:
                    request = connection.recv(2048).decode("ascii", errors="ignore")
                except OSError:
                    continue
                path = request.split()[1] if len(request.split()) > 1 else "/"
                if path == "/metrics":
                    body = self.render().encode("utf-8")
                    status = b"200 OK"
                else:
                    body = b"not found\n"
                    status = b"404 Not Found"
                response = (
                    b"HTTP/1.1 "
                    + status
                    + b"\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: "
                    + str(len(body)).encode("ascii")
                    + b"\r\nConnection: close\r\n\r\n"
                    + body
                )
                with suppress(OSError):
                    connection.sendall(response)


def _label(value: str) -> str:
    return "".join(character if character.isalnum() or character in "_-" else "_" for character in value)
