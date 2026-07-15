from __future__ import annotations

import argparse
import asyncio
import contextlib
import fcntl
import hmac
import logging
import os
import signal
import time
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from telegram import Bot

from .config import Config, ensure_private_directory
from .store import Store

LOGGER = logging.getLogger(__name__)
HANDSHAKE_EMOJI = "🤝"
DISCONNECT_EMOJI = "👋"
REDACTED_BOT_TOKEN = "[REDACTED_TELEGRAM_BOT_TOKEN]"


def _redact_log_value(value: Any, token: str) -> Any:
    if isinstance(value, str):
        return value.replace(token, REDACTED_BOT_TOKEN)
    if isinstance(value, tuple):
        return tuple(_redact_log_value(item, token) for item in value)
    if isinstance(value, dict):
        return {key: _redact_log_value(item, token) for key, item in value.items()}
    return value


class _ExactTokenRedactingFormatter(logging.Formatter):
    def __init__(self, formatter: logging.Formatter, token: str) -> None:
        super().__init__()
        self.formatter = formatter
        self.token = token

    def format(self, record: logging.LogRecord) -> str:
        record.msg = _redact_log_value(record.msg, self.token)
        record.args = _redact_log_value(record.args, self.token)
        if record.stack_info:
            record.stack_info = record.stack_info.replace(self.token, REDACTED_BOT_TOKEN)
        if record.exc_text:
            record.exc_text = record.exc_text.replace(self.token, REDACTED_BOT_TOKEN)
        rendered = self.formatter.format(record)
        if record.exc_text:
            record.exc_text = record.exc_text.replace(self.token, REDACTED_BOT_TOKEN)
        return rendered.replace(self.token, REDACTED_BOT_TOKEN)


def _install_token_redaction(
    token: str,
) -> list[tuple[logging.Handler, logging.Formatter | None]]:
    if not token:
        raise ValueError("Telegram Bot token cannot be empty")
    installed: list[tuple[logging.Handler, logging.Formatter | None]] = []
    for handler in logging.getLogger().handlers:
        formatter = handler.formatter
        if isinstance(formatter, _ExactTokenRedactingFormatter) and formatter.token == token:
            continue
        installed.append((handler, formatter))
        handler.setFormatter(_ExactTokenRedactingFormatter(formatter or logging.Formatter(), token))
    return installed


class AlreadyRunning(RuntimeError):
    pass


@contextmanager
def instance_lock(path: Path) -> Any:
    ensure_private_directory(path.parent)
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise AlreadyRunning(f"已有 bridge 实例持有锁：{path}") from None
        os.ftruncate(descriptor, 0)
        os.write(descriptor, f"{os.getpid()}\n".encode("ascii"))
        os.fsync(descriptor)
        yield descriptor
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


class ConnectionPresence:
    """Persist Telegram connectivity so unobservable disconnects can be reported later."""

    def __init__(
        self,
        bot: Bot,
        store: Store,
        disconnect_threshold_seconds: int,
        *,
        probe_bots: Sequence[Bot] | None = None,
    ) -> None:
        self.bot = bot
        self.probe_bots = tuple(probe_bots or (bot,))
        self.store = store
        self.disconnect_threshold_seconds = max(1, disconnect_threshold_seconds)
        self._healthy = False
        self._failure_started: float | None = None
        self._announced_chat_id: int | None = None
        self._deferred_disconnect = bool(store.get_meta("telegram_disconnect_pending", False))
        self._previous_runtime_active = bool(store.get_meta("telegram_runtime_active", False))
        previous_chat_id = int(store.get_meta("telegram_runtime_chat_id", 0))
        self._previous_runtime_chat_id = previous_chat_id or None

    async def runtime_started(self) -> None:
        self.store.set_meta("telegram_runtime_active", True)
        owner = self.store.get_owner()
        self.store.set_meta("telegram_runtime_chat_id", owner.chat_id if owner else 0)
        await self._mark_healthy()

    async def probe(self) -> None:
        try:
            await asyncio.gather(*(bot.get_me() for bot in self.probe_bots))
        except Exception:
            self._mark_failure()
            return
        await self._mark_healthy()

    def _mark_failure(self) -> None:
        now = time.monotonic()
        if self._failure_started is None:
            self._failure_started = now
            return
        if self._healthy and now - self._failure_started >= self.disconnect_threshold_seconds:
            self._healthy = False
            self._deferred_disconnect = True
            self.store.set_meta("telegram_disconnect_pending", True)

    async def _mark_healthy(self) -> None:
        was_healthy = self._healthy
        self._healthy = True
        self._failure_started = None
        owner = self.store.get_owner()
        if owner is None:
            return
        owner_changed = owner.chat_id != self._announced_chat_id
        reconnected = not was_healthy and self._announced_chat_id is not None
        unclean_for_owner = self._previous_runtime_active and self._previous_runtime_chat_id == owner.chat_id
        deferred = self._deferred_disconnect or unclean_for_owner
        if was_healthy and owner_changed and not deferred:
            # The /pair handler sends its own handshake. Remember a newly paired owner
            # without duplicating that message on the next health probe.
            self._announced_chat_id = owner.chat_id
            self.store.set_meta("telegram_runtime_chat_id", owner.chat_id)
            return
        if not owner_changed and not reconnected and not deferred:
            return
        try:
            if deferred or reconnected:
                await self.bot.send_message(chat_id=owner.chat_id, text=DISCONNECT_EMOJI)
            await self.bot.send_message(chat_id=owner.chat_id, text=HANDSHAKE_EMOJI)
        except Exception:
            self._healthy = False
            self._deferred_disconnect = True
            self.store.set_meta("telegram_disconnect_pending", True)
            return
        self._announced_chat_id = owner.chat_id
        self._deferred_disconnect = False
        self._previous_runtime_active = False
        self.store.set_meta("telegram_disconnect_pending", False)
        self.store.set_meta("telegram_runtime_chat_id", owner.chat_id)

    async def graceful_disconnect(self) -> None:
        owner = self.store.get_owner()
        delivered = False
        if owner is not None and self._healthy:
            try:
                await self.bot.send_message(chat_id=owner.chat_id, text=DISCONNECT_EMOJI)
                delivered = True
            except Exception:
                LOGGER.warning("Could not deliver Telegram disconnect emoji; deferring it")
        self.store.set_meta("telegram_runtime_active", False)
        self.store.set_meta("telegram_disconnect_pending", bool(owner) and not delivered)


async def _health_monitor(
    presence: ConnectionPresence,
    stop_event: asyncio.Event,
    interval: float,
) -> None:
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except TimeoutError:
            await presence.probe()


@dataclass(slots=True)
class Runtime:
    application: Any
    bridge: Any
    store: Store
    presence: ConnectionPresence
    allowed_updates: list[str]
    discussion_application: Any | None = None
    telegram_runtime: Any | None = None

    @property
    def applications(self) -> tuple[Any, ...]:
        if self.discussion_application is None:
            return (self.application,)
        return (self.application, self.discussion_application)


@dataclass(slots=True)
class TelegramRuntimeServices:
    control_messenger: Any
    discussion_messenger: Any
    deletions: Any
    dashboards: Any
    controllers: tuple[Any, ...]
    coordinator: Any | None = None
    _started: bool = False
    _quiesced: bool = False

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._quiesced = False
        self.control_messenger.start()
        self.discussion_messenger.start()
        try:
            await self.deletions.start()
            for controller in self.controllers:
                await controller.set_commands()
            await self.dashboards.start()
            if self.coordinator is not None:
                await self.coordinator.start()
        except Exception:
            await self.stop()
            raise

    async def quiesce(self) -> None:
        if not self._started or self._quiesced:
            return
        self._quiesced = True
        for controller in reversed(self.controllers):
            stop = getattr(controller, "stop", None)
            if stop is not None:
                with contextlib.suppress(Exception):
                    await stop()

    async def stop(self) -> None:
        if not self._started:
            return
        await self.quiesce()
        self._started = False
        if self.coordinator is not None:
            with contextlib.suppress(Exception):
                await self.coordinator.stop()
        with contextlib.suppress(Exception):
            await self.dashboards.stop()
        with contextlib.suppress(Exception):
            await self.deletions.stop()
        with contextlib.suppress(Exception):
            await self.discussion_messenger.stop()
        with contextlib.suppress(Exception):
            await self.control_messenger.stop()


def _build_runtime(config: Config) -> Runtime:
    # Imported lazily so local management commands and unit tests don't require the
    # Telegram handler graph to be imported or initialized.
    from .bridge import Bridge
    from .control_bot import ControlBotController
    from .deletions import MessageDeletionManager
    from .discussion_bot import DiscussionBotController
    from .outbound import OutboundMessenger
    from .security import SecurityManager
    from .space_coordinator import SessionSpaceCoordinator
    from .space_dashboard import SpaceDashboardManager
    from .telegram_common import (
        ALLOWED_UPDATES,
        CONTROL_ROLE,
        DISCUSSION_ROLE,
        TelegramEndpoint,
        build_application,
    )

    control_token = config.read_token(CONTROL_ROLE)
    discussion_token = config.read_token(DISCUSSION_ROLE)
    _install_token_redaction(control_token)
    _install_token_redaction(discussion_token)
    if hmac.compare_digest(control_token, discussion_token):
        raise RuntimeError("两个 Telegram Bot 必须使用不同 token")
    store = Store(config.database_path)
    try:
        control_application = build_application(control_token)
        discussion_application = build_application(discussion_token)
        control_messenger = OutboundMessenger()
        discussion_messenger = OutboundMessenger()
        control_endpoint = TelegramEndpoint(
            CONTROL_ROLE, control_application.bot, control_messenger
        )
        discussion_endpoint = TelegramEndpoint(
            DISCUSSION_ROLE, discussion_application.bot, discussion_messenger
        )
        bridge = Bridge(config, store, control_application.bot, control_messenger)
        security = SecurityManager(store, config.totp_secret_path, config.totp_unlock_seconds)
        dashboards = SpaceDashboardManager(
            config,
            store,
            security,
            control_endpoint,
            discussion_endpoint,
        )
        coordinator = SessionSpaceCoordinator(
            store,
            bridge,
            control_endpoint,
            discussion_endpoint,
            dashboards,
        )
        deletions = MessageDeletionManager(
            store,
            {
                CONTROL_ROLE: control_endpoint,
                DISCUSSION_ROLE: discussion_endpoint,
            },
        )
        control_controller = ControlBotController(
            config,
            store,
            security,
            bridge,
            control_endpoint,
            coordinator,
            deletions,
        )
        discussion_controller = DiscussionBotController(
            config,
            store,
            security,
            bridge,
            control_endpoint,
            discussion_endpoint,
            coordinator,
            dashboards,
            deletions,
        )
        control_controller.install(control_application)
        discussion_controller.install(discussion_application)
        bridge.on_state_change = dashboards.on_thread_change
        telegram_runtime = TelegramRuntimeServices(
            control_messenger,
            discussion_messenger,
            deletions,
            dashboards,
            (control_controller, discussion_controller),
            coordinator=coordinator,
        )
        presence = ConnectionPresence(
            control_application.bot,
            store,
            config.disconnect_threshold_seconds,
            probe_bots=(control_application.bot, discussion_application.bot),
        )
        return Runtime(
            control_application,
            bridge,
            store,
            presence,
            list(ALLOWED_UPDATES),
            discussion_application=discussion_application,
            telegram_runtime=telegram_runtime,
        )
    except Exception:
        store.close()
        raise


async def _call_hook(hook: Any, application: Any) -> None:
    if hook is not None:
        await hook(application)


async def run_service(config: Config, stop_event: asyncio.Event | None = None) -> None:
    runtime = _build_runtime(config)
    applications = tuple(getattr(runtime, "applications", (runtime.application,)))
    if any(application.updater is None for application in applications):
        runtime.store.close()
        raise RuntimeError("Telegram Application 没有 Updater，无法启动 long polling")

    stop_event = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals: list[signal.Signals] = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop_event.set)
            registered_signals.append(signum)
        except NotImplementedError, RuntimeError:  # pragma: no cover - Unix service path
            pass

    initialized: list[Any] = []
    polling: list[Any] = []
    application_started: list[Any] = []
    bridge_attempted = False
    telegram_attempted = False
    presence_started = False
    health_task: asyncio.Task[None] | None = None
    try:
        for application in applications:
            await application.initialize()
            initialized.append(application)
            await _call_hook(application.post_init, application)
        for application in applications:
            await application.updater.start_polling(
                allowed_updates=runtime.allowed_updates,
                drop_pending_updates=False,
            )
            polling.append(application)
        telegram_runtime = getattr(runtime, "telegram_runtime", None)
        if telegram_runtime is not None:
            telegram_attempted = True
            await telegram_runtime.start()
        bridge_attempted = True
        await runtime.bridge.start()
        for application in applications:
            await application.start()
            application_started.append(application)
        await runtime.presence.runtime_started()
        presence_started = True
        interval = max(5.0, min(15.0, config.disconnect_threshold_seconds / 2))
        health_task = asyncio.create_task(
            _health_monitor(runtime.presence, stop_event, interval),
            name="telegram-connection-health",
        )
        LOGGER.info("Codex Telegram Bridge is running")
        await stop_event.wait()
    finally:
        if health_task is not None:
            health_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await health_task
        for application in reversed(polling):
            with contextlib.suppress(Exception):
                await application.updater.stop()
        if telegram_attempted:
            with contextlib.suppress(Exception):
                await telegram_runtime.quiesce()
        for application in reversed(application_started):
            with contextlib.suppress(Exception):
                await application.stop()
        if presence_started:
            with contextlib.suppress(Exception):
                await runtime.presence.graceful_disconnect()
        if bridge_attempted:
            with contextlib.suppress(Exception):
                await runtime.bridge.stop()
        if telegram_attempted:
            with contextlib.suppress(Exception):
                await telegram_runtime.stop()
        for application in reversed(application_started):
            with contextlib.suppress(Exception):
                await _call_hook(application.post_stop, application)
        for application in reversed(initialized):
            with contextlib.suppress(Exception):
                await application.shutdown()
            with contextlib.suppress(Exception):
                await _call_hook(application.post_shutdown, application)
        for signum in registered_signals:
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.remove_signal_handler(signum)
        runtime.store.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="codex-telegram-bridge")
    parser.add_argument("--config", type=Path, help="config.toml 路径")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Bot API request URLs contain the token. Keep transport loggers above INFO/DEBUG
    # even when bridge diagnostics are explicitly enabled.
    for logger_name in ("telegram", "httpx", "httpcore", "websockets"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    try:
        config = Config.load(args.config.expanduser() if args.config else None)
        lock_path = config.state_dir / "bridge.lock"
        with instance_lock(lock_path):
            asyncio.run(run_service(config))
    except AlreadyRunning as exc:
        LOGGER.error("%s", exc)
        return 73
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        # Telegram transport exceptions may embed a Bot API URL, which itself contains
        # the token. Log only the exception type at this boundary.
        LOGGER.error("Bridge stopped because of a fatal %s", type(exc).__name__)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
