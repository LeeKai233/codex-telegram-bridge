from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from telegram.error import BadRequest, TelegramError
from telegram.ext import ApplicationHandlerStop

from codex_telegram_bridge.bot import ALLOWED_UPDATES, BotController, build_application
from codex_telegram_bridge.config import Config
from codex_telegram_bridge.files import PathPolicy
from codex_telegram_bridge.models import Owner, ThreadState
from codex_telegram_bridge.store import Store


class FakeMessenger:
    async def call(self, operation: Any, *, priority: int = 10) -> Any:
        del priority
        return await operation()


class FakeBot:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.documents: list[dict[str, Any]] = []
        self.deleted: list[tuple[int, int]] = []
        self.fail_markdown_once = False

    async def send_message(self, **kwargs: Any) -> Any:
        self.sent.append(kwargs)
        if self.fail_markdown_once and kwargs.get("parse_mode"):
            self.fail_markdown_once = False
            raise BadRequest("can't parse entities")
        return SimpleNamespace(message_id=len(self.sent))

    async def delete_message(self, *, chat_id: int, message_id: int) -> None:
        self.deleted.append((chat_id, message_id))

    async def send_document(self, **kwargs: Any) -> Any:
        self.documents.append(kwargs)
        return SimpleNamespace(message_id=1)


class FakeSecurity:
    def __init__(self, *, unlocked: bool = True, pair_valid: bool = True) -> None:
        self.configured = True
        self.unlocked = unlocked
        self.pair_valid = pair_valid
        self.locked = False

    def is_unlocked(self) -> bool:
        return self.unlocked

    def consume_pair_code(self, code: str) -> bool:
        return self.pair_valid and code == "PAIR-CODE"

    def verify(self, value: str) -> bool:
        if value == "123456":
            self.unlocked = True
            return True
        return False

    def lock(self) -> None:
        self.unlocked = False
        self.locked = True


class FakePolicy:
    def validate_file(self, path: str) -> Any:
        value = Path(path)
        stat = value.stat()
        return SimpleNamespace(path=value, size=stat.st_size, modified_at=int(stat.st_mtime))

    @contextmanager
    def open_outbound(self, candidate: Any) -> Any:
        with candidate.path.open("rb") as handle:
            yield handle


class FakeMetrics:
    async def with_gpu(self) -> Any:
        raise AssertionError("not used")


class FakeBridge:
    def __init__(self) -> None:
        self.bot = FakeBot()
        self.messenger = FakeMessenger()
        self.path_policy = FakePolicy()
        self.metrics = FakeMetrics()
        self.on_question: Any = None
        self.on_notice: Any = None
        self.sessions = [
            ThreadState(
                thread_id="abcdef0123456789",
                title="A [session]",
                cwd="/home/example",
                status="idle",
            )
        ]
        self.prompt_result = "started"
        self.prompt_calls: list[dict[str, Any]] = []
        self.watch_calls: list[str] = []
        self.answers: list[tuple[str, dict[str, list[str]]]] = []
        self.file_candidates: list[Any] = []
        self.upload_calls: list[dict[str, Any]] = []

    async def list_sessions(self) -> list[ThreadState]:
        return self.sessions

    async def resolve_thread(self, selector: str) -> ThreadState:
        for state in self.sessions:
            if state.thread_id.startswith(selector):
                return state
        raise ValueError("not found")

    async def refresh(self, thread_id: str) -> ThreadState:
        return await self.resolve_thread(thread_id)

    async def watch(self, thread_id: str) -> ThreadState:
        self.watch_calls.append(thread_id)
        return await self.resolve_thread(thread_id)

    async def unwatch(self, thread_id: str) -> None:
        self.watch_calls.append(f"unwatch:{thread_id}")

    async def send_prompt(self, thread_id: str, prompt: str, **kwargs: Any) -> str:
        self.prompt_calls.append({"thread_id": thread_id, "prompt": prompt, **kwargs})
        return self.prompt_result

    async def answer_question(self, request_key: str, answers: dict[str, list[str]]) -> None:
        self.answers.append((request_key, answers))

    async def resolve_files(self, thread_id: str, description: str) -> list[Any]:
        del thread_id, description
        return self.file_candidates

    async def send_upload(self, thread_id: str, path: Path, caption: str, **kwargs: Any) -> str:
        self.upload_calls.append({"thread_id": thread_id, "path": path, "caption": caption, **kwargs})
        return "started"


@pytest.fixture
def setup(tmp_path: Path) -> Iterator[tuple[BotController, Store, FakeBridge, FakeSecurity]]:
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        allowed_root=tmp_path,
        callback_seconds=60,
    )
    store = Store(tmp_path / "state" / "state.sqlite3")
    bridge = FakeBridge()
    security = FakeSecurity()
    controller = BotController(config, store, security, bridge)  # type: ignore[arg-type]
    try:
        yield controller, store, bridge, security
    finally:
        store.close()


def make_update(
    text: str = "",
    *,
    update_id: int = 1,
    user_id: int = 100,
    chat_id: int = 100,
    chat_type: str = "private",
    callback_query: Any = None,
) -> Any:
    message = SimpleNamespace(text=text, caption=None, message_id=42, photo=[], document=None)
    return SimpleNamespace(
        update_id=update_id,
        effective_chat=SimpleNamespace(id=chat_id, type=chat_type),
        effective_user=SimpleNamespace(id=user_id, username="owner"),
        effective_message=message,
        callback_query=callback_query,
    )


@pytest.mark.asyncio
async def test_guard_allows_only_pair_before_owner_and_deduplicates(setup: Any) -> None:
    controller, store, bridge, _ = setup
    context = SimpleNamespace()

    await controller._guard(make_update("/pair CODE", update_id=10), context)
    assert not store.telegram_update_seen(10)

    with pytest.raises(ApplicationHandlerStop):
        await controller._guard(make_update("/sessions", update_id=11), context)
    assert bridge.bot.sent[-1]["text"].startswith("Bot 尚未配对")

    await controller._guard(make_update("/pair CODE", update_id=10), context)

    store.set_owner(Owner(100, 100, "owner"))
    await controller._guard(make_update("/sessions", update_id=12), context)
    assert store.telegram_update_seen(12)
    with pytest.raises(ApplicationHandlerStop):
        await controller._guard(make_update("/sessions", update_id=12), context)


@pytest.mark.asyncio
async def test_guard_rejects_groups_and_non_owner_silently(setup: Any) -> None:
    controller, store, bridge, _ = setup
    store.set_owner(Owner(100, 100, "owner"))

    with pytest.raises(ApplicationHandlerStop):
        await controller._guard(make_update("/status", chat_type="group"), SimpleNamespace())
    with pytest.raises(ApplicationHandlerStop):
        await controller._guard(
            make_update("/status", update_id=2, user_id=200, chat_id=200), SimpleNamespace()
        )
    assert not store.telegram_update_seen(1)
    assert not store.telegram_update_seen(2)
    assert bridge.bot.sent == []


@pytest.mark.asyncio
async def test_pair_persists_owner_and_totp_deletes_code_message(setup: Any) -> None:
    controller, store, bridge, security = setup
    update = make_update("/pair PAIR-CODE")
    await controller.pair(update, SimpleNamespace(args=["PAIR-CODE"]))
    assert store.get_owner() == Owner(100, 100, "owner")
    assert store.get_meta("telegram_runtime_chat_id") == 100
    assert "配对成功" in bridge.bot.sent[-1]["text"]

    await controller.totp(make_update("/totp 123456", update_id=2), SimpleNamespace())
    assert security.unlocked
    assert bridge.bot.deleted == [(100, 42)]
    assert "已解锁" in bridge.bot.sent[-1]["text"]


@pytest.mark.asyncio
async def test_active_prompt_uses_single_use_nonce_without_exposing_prompt(setup: Any) -> None:
    controller, store, bridge, _ = setup
    store.set_owner(Owner(100, 100, "owner"))
    bridge.prompt_result = "choose"

    await controller.prompt(make_update("/prompt abcdef01 do a secret-ish task"), SimpleNamespace(args=[]))

    markup = bridge.bot.sent[-1]["reply_markup"]
    buttons = markup.inline_keyboard[0]
    assert len(buttons) == 2
    assert all(button.callback_data.startswith("cb:") for button in buttons)
    assert all("secret-ish" not in button.callback_data for button in buttons)
    action, payload = store.consume_callback(buttons[0].callback_data[3:], 100) or (None, {})
    assert action == "prompt"
    assert payload["mode"] == "steer"
    assert payload["prompt"] == "do a secret-ish task"
    assert store.consume_callback(buttons[0].callback_data[3:], 100) is None


@pytest.mark.asyncio
async def test_callback_nonce_is_owner_bound_and_one_time(setup: Any) -> None:
    controller, store, bridge, _ = setup
    store.set_owner(Owner(100, 100, "owner"))
    button = controller._button("Watch", "watch", {"thread_id": "abcdef0123456789"})

    query = SimpleNamespace(data=button.callback_data, answers=[])

    async def answer(**kwargs: Any) -> None:
        query.answers.append(kwargs)

    query.answer = answer
    update = make_update(callback_query=query)
    await controller.callback(update, SimpleNamespace())
    assert bridge.watch_calls == ["abcdef0123456789"]

    await controller.callback(update, SimpleNamespace())
    assert query.answers[-1] == {
        "text": "按钮已使用或过期，请重新执行命令。",
        "show_alert": True,
    }


@pytest.mark.asyncio
async def test_markdown_failure_retries_as_plain_text(setup: Any) -> None:
    controller, _, bridge, _ = setup
    bridge.bot.fail_markdown_once = True

    await controller._send_text(100, "*broken [markdown]*", plain="plain fallback")

    assert len(bridge.bot.sent) == 2
    assert bridge.bot.sent[0]["parse_mode"] == "MarkdownV2"
    assert bridge.bot.sent[1]["text"] == "plain fallback"
    assert "parse_mode" not in bridge.bot.sent[1]


@pytest.mark.asyncio
async def test_question_options_are_forwarded_and_completed(setup: Any) -> None:
    controller, store, bridge, _ = setup
    store.set_owner(Owner(100, 100, "owner"))
    questions = [
        {
            "id": "mode",
            "header": "投递方式",
            "question": "如何投递？",
            "options": [{"label": "Queue", "description": "稍后执行"}],
        }
    ]
    store.put_pending_input("request-1", "1", 1, "abcdef0123456789", "turn", "item", questions, None)

    await controller.forward_question("request-1", {"threadId": "abcdef0123456789", "questions": questions})
    markup = bridge.bot.sent[-1]["reply_markup"]
    callback_data = markup.inline_keyboard[0][0].callback_data
    action, payload = store.consume_callback(callback_data[3:], 100) or (None, {})
    assert action == "question"

    await controller._record_question_answer(100, payload)
    assert bridge.answers == [("request-1", {"mode": ["Queue"]})]
    assert "回答已提交" in bridge.bot.sent[-1]["text"]


@pytest.mark.asyncio
async def test_getfile_confirmation_persists_file_identity(setup: Any, tmp_path: Path) -> None:
    controller, store, bridge, _ = setup
    store.set_owner(Owner(100, 100, "owner"))
    path = tmp_path / "result.txt"
    path.write_text("result", encoding="utf-8")
    policy = PathPolicy(tmp_path, 1_000_000)
    bridge.path_policy = policy
    bridge.file_candidates = [policy.validate_file(path)]

    await controller.getfile(make_update("/getfile abcdef01 final result"), SimpleNamespace(args=[]))

    button = bridge.bot.sent[-1]["reply_markup"].inline_keyboard[0][0]
    action, payload = store.consume_callback(button.callback_data[3:], 100) or (None, {})
    assert action == "file"
    assert payload["device"] > 0
    assert payload["inode"] > 0
    assert payload["modified_ns"] > 0


@pytest.mark.asyncio
async def test_upload_commits_part_before_creating_confirmation(setup: Any) -> None:
    controller, store, bridge, _ = setup
    store.set_owner(Owner(100, 100, "owner"))
    content = b"telegram upload"

    class RemoteFile:
        async def download_to_memory(self, *, out: Any) -> None:
            out.write(content)

        async def download_to_drive(self, *, custom_path: Path) -> None:
            raise AssertionError(f"unsafe path-based download attempted: {custom_path}")

    class TelegramDocument:
        file_size = len(content)
        file_name = "notes.txt"
        file_unique_id = "unique-file"

        async def get_file(self) -> RemoteFile:
            return RemoteFile()

    update = make_update(update_id=99)
    update.effective_message.caption = "abcdef01 summarize it"
    update.effective_message.document = TelegramDocument()

    await controller.upload(update, SimpleNamespace())

    button = bridge.bot.sent[-1]["reply_markup"].inline_keyboard[0][0]
    action, payload = store.consume_callback(button.callback_data[3:], 100) or (None, {})
    final_path = Path(payload["path"])
    assert action == "upload"
    assert final_path.read_bytes() == content
    assert not final_path.with_name(f"{final_path.name}.part").exists()
    assert payload["client_message_id"] == "telegram-upload-99-unique-file"


def test_build_application_is_sequential_and_controller_install_binds_bridge(setup: Any) -> None:
    controller, _, bridge, _ = setup
    application = build_application("123456:TESTTOKEN")
    controller.install(application)

    assert application.update_processor.max_concurrent_updates == 1
    assert ALLOWED_UPDATES == ["message", "callback_query"]
    assert -100 in application.handlers
    assert bridge.on_question == controller.forward_question
    assert bridge.on_notice == controller.forward_notice


@pytest.mark.asyncio
async def test_callback_and_handler_errors_log_type_without_token_or_traceback(
    setup: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    controller, _, _, _ = setup
    secret = "123456789:SECRET_TOKEN"
    request_url = f"https://api.telegram.invalid/bot{secret}/answerCallbackQuery"

    async def answer(**_kwargs: Any) -> None:
        raise TelegramError(request_url)

    query = SimpleNamespace(answer=answer)
    with caplog.at_level(logging.DEBUG, logger="codex_telegram_bridge.bot"):
        await controller._answer_callback(query)
        await controller.error(object(), SimpleNamespace(error=RuntimeError(request_url)))

    assert "TelegramError" in caplog.text
    assert "RuntimeError" in caplog.text
    assert secret not in caplog.text
    assert request_url not in caplog.text
    assert all(record.exc_info is None for record in caplog.records)
