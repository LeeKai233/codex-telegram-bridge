from __future__ import annotations

from codex_telegram_bridge.metrics_http import MetricsHttpServer


def test_metrics_http_server_is_loopback_only_and_contract_shaped() -> None:
    server = MetricsHttpServer(
        "127.0.0.1:9464",
        lambda: {
            "service_state": "running",
            "polling": [
                {
                    "role": "discussion",
                    "seconds_since_success": 2,
                    "success_count": 3,
                    "failure_count": 1,
                }
            ],
        },
    )
    assert "codex_telegram_bridge_component_healthy" in server.render()
    assert 'bot_role="discussion"' in server.render()
    assert "token" not in server.render().casefold()
