from __future__ import annotations

import asyncio
import logging
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from telegram import Bot
from telegram.error import InvalidToken

from codex_telegram_bridge.config import Config
from codex_telegram_bridge.main import (
    DISCONNECT_EMOJI,
    HANDSHAKE_EMOJI,
    AlreadyRunning,
    ConnectionPresence,
    PollingRecoveryError,
    PollingSupervisor,
    TelegramRuntimeServices,
    _build_runtime,
    _health_snapshot_monitor,
    _install_token_redaction,
    _runtime_health_payload,
    instance_lock,
    main,
    run_service,
)
from codex_telegram_bridge.models import Owner
from codex_telegram_bridge.store import Store


class FakeBot:
    def __init__(self) -> None:
        self.messages: list[tuple[int, str]] = []
        self.fail = False

    async def get_me(self) -> object:
        if self.fail:
            raise RuntimeError("offline")
        return object()

    async def send_message(self, *, chat_id: int, text: str, **_kwargs: Any) -> object:
        if self.fail:
            raise RuntimeError("offline")
        self.messages.append((chat_id, text))
        return object()


def make_store(tmp_path: Path) -> Store:
    store = Store(tmp_path / "state" / "state.sqlite3")
    store.set_owner(Owner(user_id=10, chat_id=20, username="owner"))
    return store


def make_config(tmp_path: Path) -> Config:
    return Config(
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        codex_home=tmp_path / ".codex",
        codex_socket=tmp_path / ".codex" / "control.sock",
        codex_binary=tmp_path / "codex",
        allowed_root=tmp_path,
    )


def test_instance_lock_rejects_a_second_process_lock(tmp_path: Path) -> None:
    lock = tmp_path / "state" / "bridge.lock"
    with instance_lock(lock), pytest.raises(AlreadyRunning), instance_lock(lock):
        pass
    with instance_lock(lock):
        assert lock.read_text(encoding="ascii").strip().isdigit()


def test_main_quiets_transport_loggers_before_bot_construction_and_never_logs_request_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "123456789:SECRET_TOKEN"
    request_url = f"https://api.telegram.invalid/bot{secret}/getMe"
    frame_secret = "PRIVATE_CODEX_PROMPT_FRAME"
    config = make_config(tmp_path)
    observed: dict[str, int] = {}

    async def fake_run_service(_config: Config) -> None:
        for logger_name in ("telegram", "httpx", "httpcore", "websockets"):
            observed[logger_name] = logging.getLogger(logger_name).getEffectiveLevel()
        Bot(secret)
        logging.getLogger("websockets.client").debug("< TEXT %s", frame_secret)
        raise RuntimeError(request_url)

    monkeypatch.setattr("codex_telegram_bridge.main.Config.load", lambda _path=None: config)
    monkeypatch.setattr("codex_telegram_bridge.main.run_service", fake_run_service)
    transport_loggers = [logging.getLogger(name) for name in ("telegram", "httpx", "httpcore", "websockets")]
    previous_levels = [logger.level for logger in transport_loggers]
    for logger in transport_loggers:
        logger.setLevel(logging.DEBUG)
    try:
        with caplog.at_level(logging.DEBUG):
            assert main(["--log-level", "DEBUG"]) == 1
    finally:
        for logger, level in zip(transport_loggers, previous_levels, strict=True):
            logger.setLevel(level)

    assert observed == {
        "telegram": logging.WARNING,
        "httpx": logging.WARNING,
        "httpcore": logging.WARNING,
        "websockets": logging.WARNING,
    }
    assert secret not in caplog.text
    assert request_url not in caplog.text
    assert frame_secret not in caplog.text


def test_root_formatter_redacts_invalid_token_traceback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    token = "123456789:SUPER-SECRET"
    logger = logging.getLogger("telegram.token-redaction-test")

    with caplog.at_level(logging.ERROR, logger=logger.name):
        installed = _install_token_redaction(token)
        try:
            try:
                raise InvalidToken(token)
            except InvalidToken:
                logger.exception("PTB rejected the configured token")
        finally:
            for handler, formatter in installed:
                handler.setFormatter(formatter)

    assert token not in caplog.text
    assert "[REDACTED_TELEGRAM_BOT_TOKEN]" in caplog.text
    assert "InvalidToken" in caplog.text
    assert all(token not in record.getMessage() for record in caplog.records)
    assert all(not record.exc_text or token not in record.exc_text for record in caplog.records)


def test_build_runtime_installs_redaction_before_application_builder(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    token = "123456789:BUILD-SECRET"
    discussion_token = "987654321:DISCUSSION-SECRET"
    config = make_config(tmp_path)
    config.config_dir.mkdir(mode=0o700)
    config.bot_token_path.write_text(token + "\n", encoding="utf-8")
    config.bot_token_path.chmod(0o600)
    config.forum_bot_token_path.write_text(discussion_token + "\n", encoding="utf-8")
    config.forum_bot_token_path.chmod(0o600)
    logger = logging.getLogger("telegram.builder-redaction-test")

    def fail_builder(value: str, _polling_health: Any = None) -> None:
        try:
            raise InvalidToken(value)
        except InvalidToken:
            logger.exception("Application builder rejected token")
        raise RuntimeError("builder stopped")

    monkeypatch.setattr("codex_telegram_bridge.telegram_common.build_application", fail_builder)
    root_handlers = list(logging.getLogger().handlers)
    original_formatters = [handler.formatter for handler in root_handlers]
    with caplog.at_level(logging.ERROR, logger=logger.name), pytest.raises(RuntimeError):
        try:
            _build_runtime(config)
        finally:
            for handler, formatter in zip(root_handlers, original_formatters, strict=True):
                handler.setFormatter(formatter)

    assert token not in caplog.text
    assert "[REDACTED_TELEGRAM_BOT_TOKEN]" in caplog.text
    assert "InvalidToken" in caplog.text


def test_build_runtime_wires_distinct_control_and_discussion_applications(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    config.config_dir.mkdir(mode=0o700)
    config.bot_token_path.write_text(
        "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi\n", encoding="utf-8"
    )
    config.forum_bot_token_path.write_text(
        "987654321:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi\n", encoding="utf-8"
    )
    config.bot_token_path.chmod(0o600)
    config.forum_bot_token_path.chmod(0o600)

    runtime = _build_runtime(config)
    try:
        assert len(runtime.applications) == 2
        assert runtime.application is not runtime.discussion_application
        assert runtime.telegram_runtime is not None
        assert len(runtime.telegram_runtime.controllers) == 2
        assert len(runtime.presence.probe_bots) == 2
        assert runtime.bridge.on_state_change == runtime.telegram_runtime.dashboards.on_thread_change
    finally:
        runtime.store.close()


def test_build_runtime_rejects_duplicate_tokens_with_generic_error(tmp_path: Path) -> None:
    token = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"
    config = make_config(tmp_path)
    config.config_dir.mkdir(mode=0o700)
    config.bot_token_path.write_text(token + "\n", encoding="utf-8")
    config.forum_bot_token_path.write_text(token + "\n", encoding="utf-8")
    config.bot_token_path.chmod(0o600)
    config.forum_bot_token_path.chmod(0o600)
    root_handlers = list(logging.getLogger().handlers)
    original_formatters = [handler.formatter for handler in root_handlers]

    try:
        with pytest.raises(RuntimeError) as captured:
            _build_runtime(config)
    finally:
        for handler, formatter in zip(root_handlers, original_formatters, strict=True):
            handler.setFormatter(formatter)

    assert str(captured.value) == "两个 Telegram Bot 必须使用不同 token"
    assert token not in str(captured.value)


@pytest.mark.asyncio
async def test_presence_sends_handshake_and_graceful_disconnect(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    bot = FakeBot()
    presence = ConnectionPresence(bot, store, disconnect_threshold_seconds=1)

    await presence.runtime_started()
    await presence.graceful_disconnect()

    assert bot.messages == [(20, HANDSHAKE_EMOJI), (20, DISCONNECT_EMOJI)]
    assert store.get_meta("telegram_runtime_active") is False
    assert store.get_meta("telegram_disconnect_pending") is False
    store.close()


@pytest.mark.asyncio
async def test_unclean_runtime_marker_is_reported_before_reconnect(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    store.set_meta("telegram_runtime_active", True)
    store.set_meta("telegram_runtime_chat_id", 20)
    bot = FakeBot()
    presence = ConnectionPresence(bot, store, disconnect_threshold_seconds=1)

    await presence.runtime_started()

    assert bot.messages == [(20, DISCONNECT_EMOJI), (20, HANDSHAKE_EMOJI)]
    assert store.get_meta("telegram_disconnect_pending") is False
    store.close()


@pytest.mark.asyncio
async def test_successful_disconnect_notice_is_not_replayed_when_handshake_fails(
    tmp_path: Path,
) -> None:
    class HandshakeFailingBot(FakeBot):
        async def send_message(self, *, chat_id: int, text: str, **_kwargs: Any) -> object:
            if text == HANDSHAKE_EMOJI:
                raise RuntimeError("handshake unavailable")
            return await super().send_message(chat_id=chat_id, text=text)

    store = make_store(tmp_path)
    store.set_meta("telegram_runtime_active", True)
    store.set_meta("telegram_runtime_chat_id", 20)
    bot = HandshakeFailingBot()
    presence = ConnectionPresence(bot, store, disconnect_threshold_seconds=1)

    await presence.runtime_started()
    await presence.probe()

    assert bot.messages == [(20, DISCONNECT_EMOJI)]
    lifecycle = store.runtime_lifecycle()
    assert lifecycle is not None
    assert lifecycle["startup_disconnect_state"] == "delivered"
    assert lifecycle["handshake_state"] == "failed"
    store.close()


@pytest.mark.asyncio
async def test_pair_handler_handshake_is_not_duplicated_by_health_probe(tmp_path: Path) -> None:
    store = Store(tmp_path / "state" / "state.sqlite3")
    bot = FakeBot()
    presence = ConnectionPresence(bot, store, disconnect_threshold_seconds=1)
    await presence.runtime_started()
    store.set_owner(Owner(user_id=10, chat_id=20, username="owner"))

    await presence.probe()

    assert bot.messages == [(20, HANDSHAKE_EMOJI)]
    assert store.get_meta("telegram_runtime_chat_id") == 20
    store.close()


@pytest.mark.asyncio
async def test_network_recovery_does_not_replay_presence_emojis(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    bot = FakeBot()
    presence = ConnectionPresence(bot, store, disconnect_threshold_seconds=1)
    await presence.runtime_started()
    bot.messages.clear()

    bot.fail = True
    presence._failure_started = time.monotonic() - 2
    await presence.probe()
    assert store.get_meta("telegram_disconnect_pending") is False
    assert bot.messages == []

    bot.fail = False
    await presence.probe()
    assert bot.messages == []
    assert store.get_meta("telegram_disconnect_pending") is False
    store.close()


@pytest.mark.asyncio
async def test_telegram_runtime_starts_and_stops_coordinator_with_dependencies() -> None:
    events: list[str] = []

    class Messenger:
        def __init__(self, name: str) -> None:
            self.name = name

        def start(self) -> None:
            events.append(f"{self.name}.start")

        async def stop(self) -> None:
            events.append(f"{self.name}.stop")

    class Service:
        def __init__(self, name: str) -> None:
            self.name = name

        async def start(self) -> None:
            events.append(f"{self.name}.start")

        async def stop(self) -> None:
            events.append(f"{self.name}.stop")

    class Controller:
        async def set_commands(self) -> None:
            events.append("commands")

        async def stop(self) -> None:
            events.append("controller.stop")

    class DeliveryService(Service):
        def start(self) -> None:
            events.append("delivery.start")

    runtime = TelegramRuntimeServices(
        Messenger("control"),
        Messenger("discussion"),
        Service("deletions"),
        Service("dashboards"),
        (Controller(),),
        coordinator=Service("coordinator"),
        delivery=DeliveryService("delivery"),
    )

    await runtime.start()
    startup_tasks = tuple(runtime._maintenance_tasks.values())
    await asyncio.gather(*startup_tasks)
    await runtime.quiesce()
    await runtime.stop()

    assert events == [
        "control.start",
        "discussion.start",
        "delivery.start",
        "deletions.start",
        "commands",
        "dashboards.start",
        "coordinator.start",
        "controller.stop",
        "coordinator.stop",
        "dashboards.stop",
        "deletions.stop",
        "delivery.stop",
        "discussion.stop",
        "control.stop",
    ]


@pytest.mark.asyncio
async def test_telegram_runtime_start_does_not_wait_for_slow_maintenance() -> None:
    entered = asyncio.Event()

    class Messenger:
        def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

    class Service:
        async def start(self) -> None:
            entered.set()
            await asyncio.Event().wait()

        async def stop(self) -> None:
            pass

    class Controller:
        async def set_commands(self) -> None:
            entered.set()
            await asyncio.Event().wait()

        async def stop(self) -> None:
            pass

    runtime = TelegramRuntimeServices(
        Messenger(),
        Messenger(),
        Service(),
        Service(),
        (Controller(),),
        coordinator=Service(),
    )

    await asyncio.wait_for(runtime.start(), timeout=0.1)
    await asyncio.wait_for(entered.wait(), timeout=0.1)

    assert runtime._started
    assert runtime._maintenance_tasks
    await runtime.stop()
    assert runtime._maintenance_tasks == {}


@pytest.mark.asyncio
async def test_manual_lifecycle_order_and_explicit_allowed_updates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[Any] = []

    class Updater:
        async def start_polling(self, **kwargs: Any) -> None:
            events.append(("polling.start", kwargs))

        async def stop(self) -> None:
            events.append("polling.stop")

    class Application:
        updater = Updater()

        async def initialize(self) -> None:
            events.append("app.initialize")

        async def start(self) -> None:
            events.append("app.start")

        async def stop(self) -> None:
            events.append("app.stop")

        async def shutdown(self) -> None:
            events.append("app.shutdown")

        async def post_init(self, _application: Any) -> None:
            events.append("post_init")

        async def post_stop(self, _application: Any) -> None:
            events.append("post_stop")

        async def post_shutdown(self, _application: Any) -> None:
            events.append("post_shutdown")

    class Bridge:
        async def start(self) -> None:
            events.append("bridge.start")

        async def stop(self) -> None:
            events.append("bridge.stop")

    class Presence:
        async def runtime_started(self) -> None:
            events.append("presence.start")

        async def graceful_disconnect(self) -> None:
            events.append("presence.stop")

    class TelegramRuntime:
        async def start(self) -> None:
            events.append("telegram.start")

        async def quiesce(self) -> None:
            events.append("telegram.quiesce")

        async def stop(self) -> None:
            events.append("telegram.stop")

    store = make_store(tmp_path)
    runtime = type(
        "FakeRuntime",
        (),
        {
            "application": Application(),
            "bridge": Bridge(),
            "presence": Presence(),
            "store": store,
            "allowed_updates": ["message", "callback_query"],
            "polling_health": (),
            "telegram_runtime": TelegramRuntime(),
        },
    )()
    monkeypatch.setattr("codex_telegram_bridge.main._build_runtime", lambda _config: runtime)
    stop_event = asyncio.Event()
    stop_event.set()

    await run_service(make_config(tmp_path), stop_event)

    assert events == [
        "app.initialize",
        "post_init",
        (
            "polling.start",
            {"allowed_updates": ["message", "callback_query"], "drop_pending_updates": False},
        ),
        "telegram.start",
        "bridge.start",
        "app.start",
        "presence.start",
        "polling.stop",
        "telegram.quiesce",
        "app.stop",
        "presence.stop",
        "bridge.stop",
        "telegram.stop",
        "post_stop",
        "app.shutdown",
        "post_shutdown",
    ]
    reopened = Store(store.path)
    try:
        snapshot = reopened.health_snapshot()
        assert snapshot is not None
        assert snapshot["service_state"] == "stopped"
    finally:
        reopened.close()


def test_runtime_health_payload_aggregates_polling_delivery_and_codex_queues(
    tmp_path: Path,
) -> None:
    class SnapshotComponent:
        def __init__(self, value: dict[str, Any]) -> None:
            self.value = value

        def snapshot(self) -> dict[str, Any]:
            return self.value

    class ControlController:
        _workloads = SnapshotComponent({"queued": 3, "active": 1})

    class Bridge:
        @staticmethod
        def health_snapshot() -> dict[str, Any]:
            return {"codex": {"rpc_queued": 2, "notification_queued": 4}}

    store = make_store(tmp_path)
    runtime = type(
        "FakeRuntime",
        (),
        {
            "bridge": Bridge(),
            "store": store,
            "polling_health": (SnapshotComponent({"role": "control", "failures": 0}),),
            "telegram_runtime": type(
                "FakeTelegramRuntime",
                (),
                {
                    "control_messenger": SnapshotComponent({"queues": {"interactive": 1}}),
                    "discussion_messenger": SnapshotComponent({"queues": {"interactive": 0}}),
                    "delivery": SnapshotComponent({"pending": 2, "workers": 2}),
                    "controllers": (ControlController(),),
                },
            )(),
        },
    )()
    supervisor = PollingSupervisor((), (), [])
    try:
        payload = _runtime_health_payload(runtime, supervisor, service_state="running")

        assert payload["polling"] == [{"role": "control", "failures": 0}]
        assert payload["outbound"]["control"]["queues"]["interactive"] == 1
        assert payload["delivery"] == {"pending": 2, "workers": 2}
        assert payload["workloads"]["control"] == {"queued": 3, "active": 1}
        assert payload["bridge"]["codex"] == {
            "rpc_queued": 2,
            "notification_queued": 4,
        }
    finally:
        store.close()


def test_runtime_health_payload_exposes_outbound_traffic_class_metrics(tmp_path: Path) -> None:
    class SnapshotComponent:
        def __init__(self, value: dict[str, Any]) -> None:
            self.value = value

        def snapshot(self) -> dict[str, Any]:
            return self.value

    traffic_classes = {
        "callback_ack": {"queued": 1, "active": 2, "concurrency": 4},
        "interactive": {"queued": 3, "active": 2, "concurrency": 2},
        "media": {"queued": 0, "active": 1, "concurrency": 1},
        "maintenance": {"queued": 2, "active": 0, "concurrency": 1},
    }
    store = make_store(tmp_path)
    runtime = type(
        "FakeRuntime",
        (),
        {
            "bridge": object(),
            "store": store,
            "polling_health": (),
            "telegram_runtime": type(
                "FakeTelegramRuntime",
                (),
                {
                    "control_messenger": SnapshotComponent(
                        {"traffic_classes": traffic_classes}
                    ),
                    "discussion_messenger": SnapshotComponent(
                        {"traffic_classes": traffic_classes}
                    ),
                    "delivery": None,
                    "controllers": (),
                },
            )(),
        },
    )()
    try:
        payload = _runtime_health_payload(
            runtime,
            PollingSupervisor((), (), []),
            service_state="running",
        )

        assert payload["outbound"]["control"]["traffic_classes"] == traffic_classes
        assert payload["outbound"]["discussion"]["traffic_classes"] == traffic_classes
    finally:
        store.close()


@pytest.mark.asyncio
async def test_health_snapshot_write_failure_is_isolated_from_polling(
    tmp_path: Path,
) -> None:
    stop_event = asyncio.Event()
    loop_thread_id = threading.get_ident()

    class FailingStore:
        path = tmp_path / "state.sqlite3"
        writes = 0
        writer_thread_id: int | None = None
        written = threading.Event()

        def write_health_snapshot(self, _snapshot: dict[str, Any]) -> None:
            self.writes += 1
            self.writer_thread_id = threading.get_ident()
            self.written.set()
            raise RuntimeError("snapshot unavailable")

    store = FailingStore()
    runtime = type(
        "FakeRuntime",
        (),
        {
            "bridge": object(),
            "store": store,
            "polling_health": (),
            "telegram_runtime": None,
        },
    )()
    supervisor = PollingSupervisor((), (), [])

    task = asyncio.create_task(
        _health_snapshot_monitor(runtime, supervisor, stop_event, interval=0.001)
    )
    assert await asyncio.to_thread(store.written.wait, 1.0)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1.0)

    assert store.writes == 1
    assert store.writer_thread_id != loop_thread_id
    assert supervisor.fatal_error is None


@pytest.mark.asyncio
async def test_run_service_drains_health_write_before_final_stopped_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first_write_started = threading.Event()
    release_first_write = threading.Event()
    states: list[str] = []

    class Store:
        path = tmp_path / "state.sqlite3"

        def write_health_snapshot(self, snapshot: dict[str, Any]) -> None:
            state = str(snapshot["service_state"])
            if state == "running":
                first_write_started.set()
                release_first_write.wait(timeout=2.0)
            states.append(state)

        def close(self) -> None:
            pass

    class Updater:
        async def start_polling(self, **_kwargs: Any) -> None:
            pass

        async def stop(self) -> None:
            pass

    class Application:
        updater = Updater()
        post_init = None
        post_stop = None
        post_shutdown = None

        async def initialize(self) -> None:
            pass

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def shutdown(self) -> None:
            pass

    class Bridge:
        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

    class Presence:
        async def runtime_started(self) -> None:
            pass

        async def graceful_disconnect(self) -> None:
            pass

    runtime = type(
        "FakeRuntime",
        (),
        {
            "application": Application(),
            "bridge": Bridge(),
            "presence": Presence(),
            "store": Store(),
            "allowed_updates": ["message"],
            "polling_health": (),
            "telegram_runtime": None,
        },
    )()
    monkeypatch.setattr("codex_telegram_bridge.main._build_runtime", lambda _config: runtime)
    stop_event = asyncio.Event()
    service = asyncio.create_task(run_service(make_config(tmp_path), stop_event))
    assert await asyncio.to_thread(first_write_started.wait, 1.0)

    stop_event.set()
    await asyncio.sleep(0.05)
    assert not service.done()
    release_first_write.set()
    await asyncio.wait_for(service, timeout=2.0)

    assert states == ["running", "stopped"]


@pytest.mark.asyncio
async def test_dual_applications_drain_before_runtime_dependencies_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []

    class TelegramRuntime:
        stopped = False

        async def start(self) -> None:
            events.append("telegram.start")

        async def quiesce(self) -> None:
            events.append("telegram.quiesce")

        async def stop(self) -> None:
            self.stopped = True
            events.append("telegram.stop")

    telegram_runtime = TelegramRuntime()

    class Updater:
        def __init__(self, name: str) -> None:
            self.name = name

        async def start_polling(self, **_kwargs: Any) -> None:
            events.append(f"{self.name}.polling.start")

        async def stop(self) -> None:
            events.append(f"{self.name}.polling.stop")

    class Application:
        post_init = None
        post_stop = None
        post_shutdown = None

        def __init__(self, name: str) -> None:
            self.name = name
            self.updater = Updater(name)

        async def initialize(self) -> None:
            events.append(f"{self.name}.initialize")

        async def start(self) -> None:
            events.append(f"{self.name}.start")

        async def stop(self) -> None:
            assert telegram_runtime.stopped is False
            events.append(f"{self.name}.stop")
            await asyncio.sleep(0)

        async def shutdown(self) -> None:
            events.append(f"{self.name}.shutdown")

    class Bridge:
        stopped = False

        async def start(self) -> None:
            events.append("bridge.start")

        async def stop(self) -> None:
            self.stopped = True
            events.append("bridge.stop")

    class Presence:
        async def runtime_started(self) -> None:
            events.append("presence.start")

        async def graceful_disconnect(self) -> None:
            events.append("presence.stop")

    control = Application("control")
    discussion = Application("discussion")
    store = make_store(tmp_path)
    runtime = type(
        "FakeRuntime",
        (),
        {
            "application": control,
            "applications": (control, discussion),
            "bridge": Bridge(),
            "presence": Presence(),
            "store": store,
            "allowed_updates": ["message"],
            "telegram_runtime": telegram_runtime,
        },
    )()
    monkeypatch.setattr("codex_telegram_bridge.main._build_runtime", lambda _config: runtime)
    stop_event = asyncio.Event()
    stop_event.set()

    await run_service(make_config(tmp_path), stop_event)

    shutdown = events[events.index("discussion.polling.stop") :]
    assert shutdown == [
        "discussion.polling.stop",
        "control.polling.stop",
        "telegram.quiesce",
        "discussion.stop",
        "control.stop",
        "presence.stop",
        "bridge.stop",
        "telegram.stop",
        "discussion.shutdown",
        "control.shutdown",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("supervisor_mode", ("return", "crash"))
async def test_polling_supervisor_termination_cleans_up_before_service_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    supervisor_mode: str,
) -> None:
    events: list[str] = []

    class Updater:
        async def start_polling(self, **_kwargs: Any) -> None:
            events.append("polling.start")

        async def stop(self) -> None:
            events.append("polling.stop")

    class Application:
        updater = Updater()
        post_init = None
        post_stop = None
        post_shutdown = None

        async def initialize(self) -> None:
            events.append("app.initialize")

        async def start(self) -> None:
            events.append("app.start")

        async def stop(self) -> None:
            events.append("app.stop")

        async def shutdown(self) -> None:
            events.append("app.shutdown")

    class Bridge:
        async def start(self) -> None:
            events.append("bridge.start")

        async def stop(self) -> None:
            events.append("bridge.stop")

    class Presence:
        async def runtime_started(self) -> None:
            events.append("presence.start")

        async def probe(self) -> None:
            events.append("presence.probe")

        async def graceful_disconnect(self) -> None:
            events.append("presence.stop")

    class TerminatingSupervisor:
        def __init__(
            self,
            _applications: Any,
            _health: Any,
            _allowed_updates: Any,
        ) -> None:
            pass

        async def monitor(self, _stop_event: asyncio.Event, *, interval: float) -> None:
            assert interval > 0
            events.append(f"polling.{supervisor_mode}")
            if supervisor_mode == "return":
                return
            raise RuntimeError("supervisor crashed")

    store = make_store(tmp_path)
    runtime = type(
        "FakeRuntime",
        (),
        {
            "application": Application(),
            "bridge": Bridge(),
            "presence": Presence(),
            "store": store,
            "allowed_updates": ["message"],
            "telegram_runtime": None,
        },
    )()
    monkeypatch.setattr("codex_telegram_bridge.main._build_runtime", lambda _config: runtime)
    monkeypatch.setattr("codex_telegram_bridge.main.PollingSupervisor", TerminatingSupervisor)

    expected_error = (
        "polling supervisor stopped unexpectedly"
        if supervisor_mode == "return"
        else "polling supervisor crashed: RuntimeError"
    )
    with pytest.raises(PollingRecoveryError, match=expected_error):
        await run_service(make_config(tmp_path), asyncio.Event())

    assert events == [
        "app.initialize",
        "polling.start",
        "bridge.start",
        "app.start",
        "presence.start",
        f"polling.{supervisor_mode}",
        "polling.stop",
        "app.stop",
        "presence.stop",
        "bridge.stop",
        "app.shutdown",
    ]
