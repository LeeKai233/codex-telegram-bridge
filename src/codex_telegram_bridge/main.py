from __future__ import annotations

import argparse
import asyncio
import contextlib
import fcntl
import logging
import os
import signal
import time
from collections.abc import Awaitable, Callable, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .app_server import AppServerSupervisor
from .config import Config, ensure_private_directory
from .outbound import TelegramOutcomeUncertain
from .store import Store

LOGGER = logging.getLogger(__name__)
HANDSHAKE_EMOJI = "🤝"
DISCONNECT_EMOJI = "👋"
REDACTED_BOT_TOKEN = "[REDACTED_TELEGRAM_BOT_TOKEN]"
POLLING_HEALTH_CHECK_SECONDS = 15.0
POLLING_STALE_SECONDS = 90.0
POLLING_RESTART_COOLDOWN_SECONDS = 30.0
POLLING_RECOVERY_VERIFY_SECONDS = 45.0


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


class PollingRecoveryError(RuntimeError):
    pass


class AppServerRecoveryError(RuntimeError):
    pass


async def _disabled_protocol_probe(_command: tuple[str, ...]) -> bool:
    """Keep the in-process supervisor authoritative for fresh client generations."""
    return False


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
    """Emit each runtime lifecycle notice at most once across crashes and reconnects."""

    def __init__(
        self,
        endpoint: Any,
        store: Store,
        disconnect_threshold_seconds: int,
        *,
        probe_bots: Sequence[Any] | None = None,
    ) -> None:
        self.endpoint = endpoint
        self.probe_bots = tuple(probe_bots or (endpoint,))
        self.store = store
        self.disconnect_threshold_seconds = max(1, disconnect_threshold_seconds)
        self._healthy = False
        self._failure_started: float | None = None
        self._runtime_id: str | None = None

    async def runtime_started(self) -> None:
        owner = self.store.get_owner()
        lifecycle = self.store.begin_runtime_lifecycle(owner.chat_id if owner else None)
        self._runtime_id = str(lifecycle["runtime_id"])
        await self._mark_healthy()
        self.store.mark_runtime_ready(self._runtime_id)

    async def probe(self) -> None:
        try:
            await asyncio.gather(*(self._get_me(endpoint) for endpoint in self.probe_bots))
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

    async def _mark_healthy(self) -> None:
        self._healthy = True
        self._failure_started = None
        if self._runtime_id is None:
            return
        owner = self.store.get_owner()
        if owner is None:
            return
        self.store.bind_runtime_owner(self._runtime_id, owner.chat_id)
        lifecycle = self.store.runtime_lifecycle()
        if lifecycle is None or lifecycle.get("runtime_id") != self._runtime_id:
            return
        if lifecycle.get("startup_disconnect_state") == "pending":
            await self._deliver_notice("startup_disconnect", owner.chat_id, DISCONNECT_EMOJI)
        if lifecycle.get("handshake_state") == "pending":
            await self._deliver_notice("handshake", owner.chat_id, HANDSHAKE_EMOJI)

    async def graceful_disconnect(self) -> None:
        runtime_id = self._runtime_id
        if runtime_id is None:
            return
        owner = self.store.get_owner()
        if owner is not None:
            await self._deliver_notice("shutdown_disconnect", owner.chat_id, DISCONNECT_EMOJI)
        self.store.finish_runtime_lifecycle(runtime_id)

    async def _deliver_notice(self, notice: str, chat_id: int, emoji: str) -> None:
        runtime_id = self._runtime_id
        if runtime_id is None or not self.store.claim_runtime_notice(runtime_id, notice):
            return
        outcome = "delivered"
        try:
            await self._send_text(chat_id, emoji)
        except TelegramOutcomeUncertain:
            outcome = "uncertain"
            LOGGER.warning("event=telegram_presence_uncertain notice=%s", notice)
        except Exception as exc:
            outcome = "failed"
            LOGGER.warning(
                "event=telegram_presence_failed notice=%s error_type=%s",
                notice,
                type(exc).__name__,
            )
        self.store.complete_runtime_notice(runtime_id, notice, outcome)

    async def _send_text(self, chat_id: int, text: str) -> Any:
        if hasattr(self.endpoint, "send_text"):
            return await self.endpoint.send_text(
                chat_id,
                text,
                plain=text,
                priority=0,
                lane="urgent",
                semantics="non_idempotent",
            )
        return await self.endpoint.send_message(chat_id=chat_id, text=text)

    @staticmethod
    async def _get_me(endpoint: Any) -> Any:
        if hasattr(endpoint, "get_me"):
            try:
                return await endpoint.get_me(lane="maintenance")
            except TypeError:
                return await endpoint.get_me()
        raise RuntimeError("Telegram presence endpoint cannot probe getMe")


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


class PollingSupervisor:
    """Recover each stale polling transport without stopping healthy bridge services."""

    def __init__(
        self,
        applications: Sequence[Any],
        polling_health: Sequence[Any],
        allowed_updates: Sequence[str],
        *,
        fatal_event: asyncio.Event | None = None,
        stale_after: float = POLLING_STALE_SECONDS,
        restart_cooldown: float = POLLING_RESTART_COOLDOWN_SECONDS,
        recovery_verify_after: float = POLLING_RECOVERY_VERIFY_SECONDS,
    ) -> None:
        if polling_health and len(applications) != len(polling_health):
            raise ValueError("applications and polling_health must have the same length")
        self.targets = tuple(zip(applications, polling_health, strict=True)) if polling_health else ()
        self.allowed_updates = list(allowed_updates)
        self.stale_after = stale_after
        self.restart_cooldown = restart_cooldown
        self.recovery_verify_after = recovery_verify_after
        self.fatal_event = fatal_event or asyncio.Event()
        self.fatal_error: PollingRecoveryError | None = None
        self._cooldown_until: dict[str, float] = {}
        self._recovery_pending: dict[str, tuple[int, float]] = {}
        self.recovery_failures: dict[str, int] = {}
        self.last_recovery_error: dict[str, str] = {}

    async def monitor(self, stop_event: asyncio.Event, *, interval: float) -> None:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except TimeoutError:
                await self.check_once()

    def snapshot(self) -> dict[str, Any]:
        return {
            "recoveries_pending": len(self._recovery_pending),
            "recovery_failures": dict(self.recovery_failures),
            "last_recovery_error": dict(self.last_recovery_error),
        }

    async def check_once(self) -> None:
        now = time.monotonic()
        for application, health in self.targets:
            recovery = self._recovery_pending.get(health.role)
            if recovery is not None:
                success_count, deadline = recovery
                if health.success_count > success_count:
                    self._recovery_pending.pop(health.role, None)
                    self._cooldown_until.pop(health.role, None)
                    LOGGER.info(
                        "event=telegram_polling_recovery_confirmed bot_role=%s",
                        health.role,
                    )
                elif now >= deadline:
                    self._schedule_retry(
                        health,
                        reason="verification_timeout",
                        error_type="PollingRecoveryVerificationTimeout",
                        now=now,
                    )
                continue
            if not health.stale_for(self.stale_after, now=now):
                continue
            if now < self._cooldown_until.get(health.role, 0.0):
                continue
            self._cooldown_until[health.role] = now + self.restart_cooldown
            await self._restart(application, health)

    async def _restart(self, application: Any, health: Any) -> None:
        updater = getattr(application, "updater", None)
        if updater is None:
            self._schedule_retry(
                health,
                reason="no_updater",
                error_type="MissingUpdater",
            )
            return
        stale_since = health.last_success_at
        LOGGER.warning(
            "event=telegram_polling_stale bot_role=%s age_seconds=%.1f failures=%s "
            "last_error=%s action=restart",
            health.role,
            max(0.0, time.monotonic() - health.last_success_at),
            health.consecutive_failures,
            health.last_error_type or "none",
        )
        try:
            if getattr(updater, "running", True):
                await updater.stop()
            polling_request = health.polling_request
            if polling_request is None:
                raise RuntimeError("polling request is unavailable")
            await polling_request.shutdown()
            await polling_request.initialize()
            LOGGER.info(
                "event=telegram_polling_transport_recycled bot_role=%s",
                health.role,
            )
            success_count = health.success_count
            await updater.start_polling(
                allowed_updates=self.allowed_updates,
                drop_pending_updates=False,
            )
        except Exception as exc:
            health.last_success_at = stale_since
            LOGGER.exception(
                "event=telegram_polling_restart_failed bot_role=%s error_type=%s action=retry",
                health.role,
                type(exc).__name__,
            )
            self._schedule_retry(
                health,
                reason="restart_failed",
                error_type=type(exc).__name__,
            )
            return
        health.mark_started()
        self._cooldown_until.pop(health.role, None)
        self._recovery_pending[health.role] = (
            success_count,
            time.monotonic() + self.recovery_verify_after,
        )
        LOGGER.info(
            "event=telegram_polling_restarted bot_role=%s verification_seconds=%.1f",
            health.role,
            self.recovery_verify_after,
        )

    def _schedule_retry(
        self,
        health: Any,
        *,
        reason: str,
        error_type: str = "none",
        now: float | None = None,
    ) -> None:
        current = time.monotonic() if now is None else now
        self._recovery_pending.pop(health.role, None)
        self._cooldown_until[health.role] = current + self.restart_cooldown
        health.last_success_at = min(health.last_success_at, current - self.stale_after)
        health.mark_failure(error_type)
        self.recovery_failures[health.role] = self.recovery_failures.get(health.role, 0) + 1
        self.last_recovery_error[health.role] = f"{reason}:{error_type}"
        LOGGER.error(
            "event=telegram_polling_unrecoverable bot_role=%s reason=%s "
            "error_type=%s action=retry retry_seconds=%.1f",
            health.role,
            reason,
            error_type,
            self.restart_cooldown,
        )


@dataclass(slots=True)
class Runtime:
    application: Any
    bridge: Any
    store: Store
    presence: ConnectionPresence
    allowed_updates: list[str]
    polling_health: tuple[Any, ...] = ()
    discussion_application: Any | None = None
    status_application: Any | None = None
    telegram_runtime: Any | None = None

    @property
    def applications(self) -> tuple[Any, ...]:
        if self.discussion_application is None:
            return (self.application,)
        if self.status_application is None:
            return (self.application, self.discussion_application)
        return (self.application, self.discussion_application, self.status_application)


def _runtime_health_payload(
    runtime: Runtime,
    polling_supervisor: PollingSupervisor,
    *,
    service_state: str,
) -> dict[str, Any]:
    telegram_runtime = getattr(runtime, "telegram_runtime", None)
    outbound: dict[str, Any] = {}
    workloads: dict[str, Any] = {}
    delivery: dict[str, Any] = {}
    if telegram_runtime is not None:
        for name in ("control_messenger", "discussion_messenger", "status_messenger"):
            component = getattr(telegram_runtime, name, None)
            snapshot = getattr(component, "snapshot", None)
            if callable(snapshot):
                outbound[name.removesuffix("_messenger")] = snapshot()
        component = getattr(telegram_runtime, "delivery", None)
        snapshot = getattr(component, "snapshot", None)
        if callable(snapshot):
            delivery = snapshot()
        for controller in getattr(telegram_runtime, "controllers", ()):
            scheduler = getattr(controller, "_workloads", None)
            snapshot = getattr(scheduler, "snapshot", None)
            if callable(snapshot):
                workloads[type(controller).__name__.removesuffix("Controller").casefold()] = (
                    snapshot()
                )
    bridge_snapshot = getattr(runtime.bridge, "health_snapshot", None)
    database_path = runtime.store.path
    sizes = {
        "database_bytes": database_path.stat().st_size if database_path.is_file() else 0,
        "wal_bytes": Path(f"{database_path}-wal").stat().st_size
        if Path(f"{database_path}-wal").is_file()
        else 0,
        "shm_bytes": Path(f"{database_path}-shm").stat().st_size
        if Path(f"{database_path}-shm").is_file()
        else 0,
    }
    return {
        "sampled_at": int(time.time()),
        "service_state": service_state,
        "database": sizes,
        "polling": [health.snapshot() for health in runtime.polling_health],
        "polling_supervisor": polling_supervisor.snapshot(),
        "outbound": outbound,
        "delivery": delivery,
        "workloads": workloads,
        "bridge": bridge_snapshot() if callable(bridge_snapshot) else {},
    }


async def _health_snapshot_monitor(
    runtime: Runtime,
    polling_supervisor: PollingSupervisor,
    stop_event: asyncio.Event,
    *,
    interval: float = 15.0,
) -> None:
    while not stop_event.is_set():
        try:
            await asyncio.to_thread(
                runtime.store.write_health_snapshot,
                _runtime_health_payload(runtime, polling_supervisor, service_state="running"),
            )
        except Exception as exc:
            LOGGER.warning(
                "event=health_snapshot_failed error_type=%s",
                type(exc).__name__,
            )
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop_event.wait(), timeout=interval)


@dataclass(slots=True)
class TelegramRuntimeServices:
    control_messenger: Any
    discussion_messenger: Any
    deletions: Any
    dashboards: Any
    controllers: tuple[Any, ...]
    coordinator: Any | None = None
    delivery: Any | None = None
    status_messenger: Any | None = None
    _started: bool = False
    _quiesced: bool = False
    _maintenance_tasks: dict[str, asyncio.Task[None]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._quiesced = False
        self.control_messenger.start()
        self.discussion_messenger.start()
        if self.status_messenger is not None:
            self.status_messenger.start()
        try:
            if self.delivery is not None:
                self.delivery.start()
            self._schedule_maintenance("deletions", self.deletions.start)
            for controller in self.controllers:
                name = type(controller).__name__.removesuffix("Controller").casefold()
                self._schedule_maintenance(f"commands-{name}", controller.set_commands)
            self._schedule_maintenance("dashboards", self.dashboards.start)
            if self.coordinator is not None:
                self._schedule_maintenance("coordinator", self.coordinator.start)
        except Exception:
            await self.stop()
            raise

    def _schedule_maintenance(
        self,
        name: str,
        operation: Callable[[], Awaitable[None]],
    ) -> None:
        current = self._maintenance_tasks.get(name)
        if current is not None and not current.done():
            return
        task = asyncio.create_task(
            self._run_maintenance(name, operation),
            name=f"telegram-startup-{name}",
        )
        self._maintenance_tasks[name] = task
        task.add_done_callback(
            lambda completed, task_name=name: (
                self._maintenance_tasks.pop(task_name, None)
                if self._maintenance_tasks.get(task_name) is completed
                else None
            )
        )

    async def _run_maintenance(
        self,
        name: str,
        operation: Callable[[], Awaitable[None]],
    ) -> None:
        delays = (1.0, 5.0, 30.0, 120.0, 300.0)
        attempt = 0
        while self._started and not self._quiesced:
            try:
                await operation()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                delay = delays[min(attempt, len(delays) - 1)]
                attempt += 1
                LOGGER.warning(
                    "event=telegram_startup_maintenance_failed task=%s attempt=%s "
                    "error_type=%s retry_seconds=%.1f",
                    name,
                    attempt,
                    type(exc).__name__,
                    delay,
                )
                await asyncio.sleep(delay)

    async def quiesce(self) -> None:
        if not self._started or self._quiesced:
            return
        self._quiesced = True
        tasks = list(self._maintenance_tasks.values())
        self._maintenance_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
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
        if self.delivery is not None:
            with contextlib.suppress(Exception):
                await self.delivery.stop()
        with contextlib.suppress(Exception):
            await self.discussion_messenger.stop()
        if self.status_messenger is not None:
            with contextlib.suppress(Exception):
                await self.status_messenger.stop()
        with contextlib.suppress(Exception):
            await self.control_messenger.stop()


def _build_runtime(config: Config) -> Runtime:
    # Imported lazily so local management commands and unit tests don't require the
    # Telegram handler graph to be imported or initialized.
    from .bridge import Bridge
    from .control_bot import ControlBotController
    from .deletions import MessageDeletionManager
    from .delivery import TelegramDeliveryEngine
    from .discussion_bot import DiscussionBotController
    from .outbound import OutboundMessenger
    from .security import SecurityManager
    from .space_coordinator import SessionSpaceCoordinator
    from .space_dashboard import SpaceDashboardManager
    from .status_bot import StatusBotController
    from .telegram_common import (
        ALLOWED_UPDATES,
        CONTROL_ROLE,
        DISCUSSION_ROLE,
        STATUS_ROLE,
        PollingHealth,
        TelegramEndpoint,
        build_application,
    )

    control_token = config.read_token(CONTROL_ROLE)
    discussion_token = config.read_token(DISCUSSION_ROLE)
    status_token = config.read_token(STATUS_ROLE)
    _install_token_redaction(control_token)
    _install_token_redaction(discussion_token)
    _install_token_redaction(status_token)
    if len({control_token, discussion_token, status_token}) != 3:
        raise RuntimeError("三个 Telegram Bot 必须使用不同 token")
    store = Store(config.database_path)
    try:
        recovered_intents = store.recover_outbound_intents()
        if recovered_intents:
            LOGGER.warning(
                "event=telegram_outbound_intents_recovered count=%s status=uncertain",
                recovered_intents,
            )
        control_polling_health = PollingHealth(CONTROL_ROLE)
        discussion_polling_health = PollingHealth(DISCUSSION_ROLE)
        status_polling_health = PollingHealth(STATUS_ROLE)
        control_application = build_application(control_token, control_polling_health)
        discussion_application = build_application(discussion_token, discussion_polling_health)
        status_application = build_application(status_token, status_polling_health)
        async def recycle_control_transport() -> None:
            await control_application.bot.request.shutdown()
            await control_application.bot.request.initialize()

        async def recycle_discussion_transport() -> None:
            await discussion_application.bot.request.shutdown()
            await discussion_application.bot.request.initialize()

        async def recycle_status_transport() -> None:
            await status_application.bot.request.shutdown()
            await status_application.bot.request.initialize()

        control_messenger = OutboundMessenger(
            bot_role=CONTROL_ROLE,
            journal=store,
            recycle_transport=recycle_control_transport,
        )
        discussion_messenger = OutboundMessenger(
            bot_role=DISCUSSION_ROLE,
            journal=store,
            recycle_transport=recycle_discussion_transport,
        )
        status_messenger = OutboundMessenger(
            bot_role=STATUS_ROLE,
            journal=store,
            recycle_transport=recycle_status_transport,
        )
        control_endpoint = TelegramEndpoint(
            CONTROL_ROLE, control_application.bot, control_messenger
        )
        discussion_endpoint = TelegramEndpoint(
            DISCUSSION_ROLE, discussion_application.bot, discussion_messenger
        )
        status_endpoint = TelegramEndpoint(STATUS_ROLE, status_application.bot, status_messenger)
        delivery = TelegramDeliveryEngine(
            {
                CONTROL_ROLE: control_endpoint,
                DISCUSSION_ROLE: discussion_endpoint,
                STATUS_ROLE: status_endpoint,
            }
        )
        bridge = Bridge(
            config,
            store,
            control_application.bot,
            control_messenger,
            control_endpoint=control_endpoint,
            delivery=delivery,
            manage_messenger=False,
        )
        bridge.app_server_supervisor = AppServerSupervisor(
            bridge.client,
            config.app_server_mode,
            config.codex_socket,
            config.codex_binary,
            config.state_dir,
            installer_restart=_restart_installer_app_server,
            protocol_probe=_disabled_protocol_probe,
        )
        security = SecurityManager(store, config.totp_secret_path, config.totp_unlock_seconds)
        dashboards = SpaceDashboardManager(
            config,
            store,
            security,
            control_endpoint,
            discussion_endpoint,
            delivery,
            status=status_endpoint,
        )
        deletions = MessageDeletionManager(
            store,
            {
                CONTROL_ROLE: control_endpoint,
                DISCUSSION_ROLE: discussion_endpoint,
                STATUS_ROLE: status_endpoint,
            },
        )
        coordinator = SessionSpaceCoordinator(
            store,
            bridge,
            control_endpoint,
            discussion_endpoint,
            dashboards,
            status=status_endpoint,
            deletions=deletions,
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
        status_controller = StatusBotController(store, discussion_controller, status_endpoint)
        status_controller.install(status_application)
        bridge.on_state_change = dashboards.on_thread_change
        telegram_runtime = TelegramRuntimeServices(
            control_messenger,
            discussion_messenger,
            deletions,
            dashboards,
            (control_controller, discussion_controller, status_controller),
            coordinator=coordinator,
            delivery=delivery,
            status_messenger=status_messenger,
        )
        presence = ConnectionPresence(
            control_endpoint,
            store,
            config.disconnect_threshold_seconds,
            probe_bots=(control_endpoint, discussion_endpoint, status_endpoint),
        )
        return Runtime(
            control_application,
            bridge,
            store,
            presence,
            list(ALLOWED_UPDATES),
            polling_health=(control_polling_health, discussion_polling_health, status_polling_health),
            discussion_application=discussion_application,
            status_application=status_application,
            telegram_runtime=telegram_runtime,
        )
    except Exception:
        store.close()
        raise


async def _call_hook(hook: Any, application: Any) -> None:
    if hook is not None:
        await hook(application)


async def _restart_installer_app_server() -> int:
    process = await asyncio.create_subprocess_exec(
        "systemctl",
        "--user",
        "restart",
        "codex-telegram-app-server.service",
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    return await process.wait()


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
    polling_health_task: asyncio.Task[None] | None = None
    health_snapshot_task: asyncio.Task[None] | None = None
    app_server_health_task: asyncio.Task[None] | None = None
    stop_wait_task: asyncio.Task[bool] | None = None
    polling_supervisor = PollingSupervisor(
        applications,
        tuple(getattr(runtime, "polling_health", ())),
        runtime.allowed_updates,
    )
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
        for health in getattr(runtime, "polling_health", ()):
            health.mark_started()
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
        polling_health_task = asyncio.create_task(
            polling_supervisor.monitor(stop_event, interval=POLLING_HEALTH_CHECK_SECONDS),
            name="telegram-polling-health",
        )
        health_snapshot_task = asyncio.create_task(
            _health_snapshot_monitor(runtime, polling_supervisor, stop_event),
            name="bridge-health-snapshot",
        )
        app_server_supervisor = getattr(runtime.bridge, "app_server_supervisor", None)
        if app_server_supervisor is not None:
            app_server_health_task = asyncio.create_task(
                app_server_supervisor.monitor(stop_event, interval=1.0),
                name="codex-app-server-health",
            )
        LOGGER.info("Codex Telegram Bridge is running")
        stop_wait_task = asyncio.create_task(stop_event.wait(), name="bridge-stop-wait")
        completed, _pending = await asyncio.wait(
            tuple(
                task
                for task in (stop_wait_task, polling_health_task, app_server_health_task)
                if task is not None
            ),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_event.is_set():
            pass
        elif polling_health_task in completed:
            supervisor_error = polling_health_task.exception()
            if supervisor_error is None:
                raise PollingRecoveryError("Telegram polling supervisor stopped unexpectedly")
            raise PollingRecoveryError(
                f"Telegram polling supervisor crashed: {type(supervisor_error).__name__}"
            ) from supervisor_error
        elif app_server_health_task is not None and app_server_health_task in completed:
            supervisor_error = app_server_health_task.exception()
            app_server = getattr(runtime.bridge, "app_server_supervisor", None)
            if app_server is not None and getattr(app_server, "fatal_error", None):
                raise AppServerRecoveryError(str(app_server.fatal_error))
            if supervisor_error is None:
                raise AppServerRecoveryError("Codex app-server supervisor stopped unexpectedly")
            raise AppServerRecoveryError(
                f"Codex app-server supervisor crashed: {type(supervisor_error).__name__}"
            ) from supervisor_error
    finally:
        stop_event.set()
        wait_tasks = tuple(task for task in (stop_wait_task,) if task is not None)
        for task in wait_tasks:
            task.cancel()
        for task in wait_tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if polling_health_task is not None:
            polling_health_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await polling_health_task
        if app_server_health_task is not None:
            app_server_health_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await app_server_health_task
        app_server_supervisor = getattr(runtime.bridge, "app_server_supervisor", None)
        if app_server_supervisor is not None:
            with contextlib.suppress(Exception):
                await app_server_supervisor.stop()
        if health_snapshot_task is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await health_snapshot_task
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
        with contextlib.suppress(Exception):
            runtime.store.write_health_snapshot(
                _runtime_health_payload(runtime, polling_supervisor, service_state="stopped")
            )
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
