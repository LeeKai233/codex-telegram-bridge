from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from codex_telegram_bridge import control_bot
from codex_telegram_bridge.control_bot import ControlBotController
from codex_telegram_bridge.metrics import MetricsSnapshot


@dataclass
class Draft:
    scope_key: str
    flow_id: str
    revision: int
    kind: str
    phase: str
    payload: dict[str, Any]
    user_id: int
    bot_role: str
    chat_id: int
    expires_at: int
    claimed_at: int | None = None


class FakeStore:
    def __init__(self) -> None:
        self.drafts: dict[str, Draft] = {}
        self.callbacks: dict[str, tuple[str, dict[str, Any]]] = {}
        self.sequence = 0
        self.owner = SimpleNamespace(user_id=7, chat_id=70, username="owner")

    def get_owner(self) -> Any:
        return self.owner

    def put_callback(
        self,
        nonce: str,
        action: str,
        payload: dict[str, Any],
        *_args: Any,
        **_kwargs: Any,
    ) -> None:
        self.callbacks[nonce] = (action, payload)

    def replace_interaction(self, scope_key: str, **kwargs: Any) -> Draft:
        self.sequence += 1
        draft = Draft(
            scope_key=scope_key,
            flow_id=f"flow-{self.sequence}",
            revision=0,
            kind=str(kwargs["kind"]),
            phase=str(kwargs["phase"]),
            payload=dict(kwargs["payload"]),
            user_id=int(kwargs["user_id"]),
            bot_role=str(kwargs["bot_role"]),
            chat_id=int(kwargs["chat_id"]),
            expires_at=int(kwargs["expires_at"]),
        )
        self.drafts[scope_key] = draft
        return draft

    def get_interaction(self, scope_key: str) -> Draft | None:
        return self.drafts.get(scope_key)

    def advance_interaction(
        self,
        scope_key: str,
        flow_id: str,
        revision: int,
        **kwargs: Any,
    ) -> Draft | None:
        draft = self.drafts.get(scope_key)
        if (
            draft is None
            or draft.flow_id != flow_id
            or draft.revision != revision
            or draft.claimed_at is not None
        ):
            return None
        draft.revision += 1
        draft.phase = str(kwargs["phase"])
        draft.payload = dict(kwargs["payload"])
        draft.expires_at = int(kwargs["expires_at"])
        return draft

    def claim_interaction(
        self, scope_key: str, flow_id: str, revision: int
    ) -> Draft | None:
        draft = self.drafts.get(scope_key)
        if (
            draft is None
            or draft.flow_id != flow_id
            or draft.revision != revision
            or draft.claimed_at is not None
        ):
            return None
        draft.claimed_at = int(time.time())
        return draft

    def claim_live_interaction(
        self, scope_key: str, flow_id: str, revision: int
    ) -> Draft | None:
        draft = self.drafts.get(scope_key)
        if draft is None or draft.expires_at <= int(time.time()):
            return None
        return self.claim_interaction(scope_key, flow_id, revision)

    def claim_expired_interaction(
        self, scope_key: str, flow_id: str, revision: int
    ) -> Draft | None:
        draft = self.drafts.get(scope_key)
        if draft is None or draft.expires_at > int(time.time()):
            return None
        return self.claim_interaction(scope_key, flow_id, revision)

    def delete_interaction(self, scope_key: str) -> None:
        self.drafts.pop(scope_key, None)

    def list_interactions(self, *, kind: str | None = None) -> list[Draft]:
        return [draft for draft in self.drafts.values() if kind is None or draft.kind == kind]


class FakeEndpoint:
    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []
        self.edited: list[dict[str, Any]] = []
        self.next_message_id = 1000
        self.bot = SimpleNamespace()

    async def send_text(self, chat_id: int, markdown: str, **kwargs: Any) -> Any:
        self.sent.append({"chat_id": chat_id, "markdown": markdown, **kwargs})
        result = SimpleNamespace(message_id=self.next_message_id)
        self.next_message_id += 1
        return result

    async def edit_text(
        self, chat_id: int, message_id: int, markdown: str, **kwargs: Any
    ) -> bool:
        self.edited.append(
            {"chat_id": chat_id, "message_id": message_id, "markdown": markdown, **kwargs}
        )
        return True


class FakeDeletions:
    def __init__(self) -> None:
        self.scheduled: list[dict[str, Any]] = []
        self.deleted_now: list[dict[str, Any]] = []

    def schedule(
        self,
        bot_role: str,
        chat_id: int,
        message_ids: list[int] | tuple[int, ...],
        **kwargs: Any,
    ) -> None:
        self.scheduled.append(
            {
                "bot_role": bot_role,
                "chat_id": chat_id,
                "message_ids": tuple(message_ids),
                **kwargs,
            }
        )

    async def delete_now(
        self,
        bot_role: str,
        chat_id: int,
        message_ids: list[int] | tuple[int, ...],
        **kwargs: Any,
    ) -> None:
        self.deleted_now.append(
            {
                "bot_role": bot_role,
                "chat_id": chat_id,
                "message_ids": tuple(message_ids),
                **kwargs,
            }
        )


class FakeMetrics:
    def __init__(self) -> None:
        self.calls = 0

    async def with_gpu(self) -> MetricsSnapshot:
        self.calls += 1
        gib = 1024**3
        return MetricsSnapshot(
            sampled_at=1_700_000_000 + self.calls,
            uptime_seconds=3600,
            load=(0.1, 0.2, 0.3),
            cpu_percent=float(self.calls),
            memory_total=8 * gib,
            memory_available=4 * gib,
            memory_percent=50.0,
            swap_total=2 * gib,
            swap_used=0,
            swap_percent=0.0,
            disk_total=100 * gib,
            disk_free=75 * gib,
            disk_percent=25.0,
            codex_processes=2,
            codex_rss=gib,
            codex_cpu=10.0,
        )


class FakeBridge:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.metrics = FakeMetrics()
        self.options = [
            SimpleNamespace(
                model="gpt-5.6-luna",
                display_name="GPT 5.6 Luna",
                supported_efforts=("low", "max"),
                default_effort="max",
                is_default=True,
            ),
            SimpleNamespace(
                model="gpt-5.6-sol",
                display_name="GPT 5.6 Sol",
                supported_efforts=("high", "xhigh"),
                default_effort="high",
                is_default=False,
            ),
        ]

    async def list_model_options(self) -> list[Any]:
        return self.options

    async def resolve_model_profile(self, model: str, effort: str) -> Any:
        matches = [
            option
            for option in self.options
            if option.model == model or option.model.endswith(f"-{model}")
        ]
        if len(matches) != 1 or effort not in matches[0].supported_efforts:
            raise ValueError("invalid profile")
        return SimpleNamespace(model=matches[0].model, effort=effort)

    async def resolve_directory(self, description: str) -> list[Path]:
        if description in {"project", str(self.root)}:
            return [self.root]
        return []

    async def prepare_directory_creation(self, value: str) -> Path | None:
        candidate = Path(value).expanduser()
        return candidate if candidate.is_absolute() else None

    async def create_project_directory(self, target: Path) -> Path:
        target.mkdir(parents=True, exist_ok=True)
        return target


class FakeCoordinator:
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []

    async def create_pending(self, cwd: Path, prompt: str, **kwargs: Any) -> dict[str, Any]:
        self.created.append({"cwd": cwd, "prompt": prompt, **kwargs})
        return {"channel_chat_id": -1001234567890, "channel_post_id": 42}


def make_update(text: str, *, update_id: int = 1, message_id: int = 10) -> Any:
    return SimpleNamespace(
        update_id=update_id,
        effective_chat=SimpleNamespace(id=70, type="private"),
        effective_user=SimpleNamespace(id=7, username="owner"),
        effective_message=SimpleNamespace(message_id=message_id, text=text, caption=None),
    )


def build_controller(tmp_path: Path) -> tuple[Any, ...]:
    store = FakeStore()
    endpoint = FakeEndpoint()
    bridge = FakeBridge(tmp_path)
    coordinator = FakeCoordinator()
    deletions = FakeDeletions()
    controller = ControlBotController(
        SimpleNamespace(callback_seconds=300),
        store,  # type: ignore[arg-type]
        SimpleNamespace(),  # type: ignore[arg-type]
        bridge,  # type: ignore[arg-type]
        endpoint,  # type: ignore[arg-type]
        coordinator,  # type: ignore[arg-type]
        deletions,  # type: ignore[arg-type]
    )
    return controller, store, endpoint, bridge, coordinator, deletions


async def click_last_button(
    controller: ControlBotController,
    store: FakeStore,
    endpoint: FakeEndpoint,
    *,
    label: str,
) -> None:
    markup = endpoint.sent[-1]["reply_markup"]
    button = next(
        button
        for row in markup.inline_keyboard
        for button in row
        if button.text == label
    )
    action, payload = store.callbacks[str(button.callback_data)[3:]]
    assert action == "new_flow"
    await controller._handle_new_callback(70, payload)


@pytest.mark.asyncio
async def test_new_interactive_flow_captures_project_and_prompt(tmp_path: Path) -> None:
    controller, store, endpoint, _bridge, coordinator, _deletions = build_controller(tmp_path)
    try:
        await controller.new(make_update("/new"), SimpleNamespace())
        await click_last_button(controller, store, endpoint, label="GPT 5.6 Luna")
        await click_last_button(controller, store, endpoint, label="max")
        await click_last_button(controller, store, endpoint, label="否")

        await controller.observe_message(make_update("project"), SimpleNamespace())
        await controller.observe_message(make_update("Build the feature"), SimpleNamespace())

        assert coordinator.created == [
            {
                "cwd": tmp_path,
                "prompt": "Build the feature",
                "normal_model": "gpt-5.6-luna",
                "normal_effort": "max",
                "plan_model": None,
                "plan_effort": None,
                "current_mode": "default",
            }
        ]
        assert store.drafts == {}
    finally:
        await controller.stop()


@pytest.mark.asyncio
async def test_model_and_effort_choices_use_explicit_column_rows(tmp_path: Path) -> None:
    controller, store, endpoint, bridge, _coordinator, _deletions = build_controller(tmp_path)
    bridge.options = [
        SimpleNamespace(
            model=f"model-{index}",
            display_name=f"Model {index}",
            supported_efforts=("low", "medium", "high", "max"),
            default_effort="high",
            is_default=index == 0,
        )
        for index in range(4)
    ]
    draft = store.replace_interaction(
        "control:70:7:new",
        kind="new",
        phase="normal_model",
        payload={},
        user_id=7,
        bot_role="control",
        chat_id=70,
        expires_at=int(time.time()) + 300,
    )

    await controller._show_model_choices(70, draft, plan=False)
    assert endpoint.sent[-1]["markdown"] == "请选择 当前模式 使用的模型："
    model_markup = endpoint.sent[-1]["reply_markup"]
    assert [len(row) for row in model_markup.inline_keyboard] == [2, 2, 1]
    assert [button.text for row in model_markup.inline_keyboard for button in row] == [
        "Model 0",
        "Model 1",
        "Model 2",
        "Model 3",
        "退出",
    ]

    draft.payload["normal_model"] = "model-0"
    await controller._show_effort_choices(70, draft, plan=False)
    assert endpoint.sent[-1]["markdown"] == "模型 `model-0` 支持以下 effort："
    effort_markup = endpoint.sent[-1]["reply_markup"]
    assert [len(row) for row in effort_markup.inline_keyboard] == [2, 2, 1]
    assert [button.text for row in effort_markup.inline_keyboard for button in row] == [
        "low",
        "medium",
        "high",
        "max",
        "退出",
    ]

    await controller._show_plan_choice(70, draft)
    plan_markup = endpoint.sent[-1]["reply_markup"]
    assert [len(row) for row in plan_markup.inline_keyboard] == [1, 1, 1]
    assert [row[0].text for row in plan_markup.inline_keyboard] == ["是", "否", "退出"]


@pytest.mark.asyncio
async def test_new_parameterized_plan_preserves_pipe_in_prompt(tmp_path: Path) -> None:
    controller, _store, _endpoint, _bridge, coordinator, _deletions = build_controller(tmp_path)
    try:
        command = (
            "/new gpt-5.6-luna | max | planmode | luna | low | "
            f"{tmp_path} | inspect a | b pipeline"
        )
        await controller.new(make_update(command), SimpleNamespace())

        assert coordinator.created[0]["prompt"] == "inspect a | b pipeline"
        assert coordinator.created[0]["normal_model"] == "gpt-5.6-luna"
        assert coordinator.created[0]["plan_model"] == "gpt-5.6-luna"
        assert coordinator.created[0]["plan_effort"] == "low"
        assert coordinator.created[0]["current_mode"] == "plan"
    finally:
        await controller.stop()


@pytest.mark.asyncio
async def test_new_two_argument_form_continues_at_plan_choice(tmp_path: Path) -> None:
    controller, store, endpoint, _bridge, coordinator, _deletions = build_controller(tmp_path)
    try:
        await controller.new(make_update("/new luna | max"), SimpleNamespace())

        draft = next(iter(store.drafts.values()))
        assert draft.phase == "plan_choice"
        assert draft.payload == {
            "normal_model": "gpt-5.6-luna",
            "normal_effort": "max",
        }
        assert "是否先进入 Plan Mode" in endpoint.sent[-1]["markdown"]
        assert coordinator.created == []
    finally:
        await controller.stop()


@pytest.mark.asyncio
async def test_new_invalid_profile_suggests_canonical_command(tmp_path: Path) -> None:
    controller, _store, endpoint, _bridge, _coordinator, _deletions = build_controller(tmp_path)
    try:
        await controller.new(
            make_update("/new luna | max | nopln"), SimpleNamespace()
        )

        assert "你可能想发送" in endpoint.sent[-1]["markdown"]
        assert "/new gpt-5.6-luna | max | noplan" in endpoint.sent[-1]["markdown"]
    finally:
        await controller.stop()


@pytest.mark.asyncio
async def test_new_invalid_mode_suggestion_preserves_directory_and_prompt(
    tmp_path: Path,
) -> None:
    controller, _store, endpoint, _bridge, _coordinator, _deletions = build_controller(
        tmp_path
    )
    target = tmp_path / "new-project"
    try:
        await controller.new(
            make_update(
                f"/new luna | max | nopln | {target} | keep this | exact prompt"
            ),
            SimpleNamespace(),
        )

        suggestion = endpoint.sent[-1]["markdown"]
        assert f"/new gpt-5.6-luna | max | noplan | {target}" in suggestion
        assert "keep this | exact prompt" in suggestion
    finally:
        await controller.stop()


@pytest.mark.asyncio
async def test_new_incomplete_arguments_suggest_a_complete_command(tmp_path: Path) -> None:
    controller, _store, endpoint, _bridge, _coordinator, _deletions = build_controller(tmp_path)
    try:
        await controller.new(make_update("/new lunaa"), SimpleNamespace())

        assert "你可能想发送" in endpoint.sent[-1]["markdown"]
        assert "/new gpt-5.6-luna | max" in endpoint.sent[-1]["markdown"]
    finally:
        await controller.stop()


@pytest.mark.asyncio
async def test_new_missing_plan_effort_suggestion_preserves_tail(tmp_path: Path) -> None:
    controller, _store, endpoint, _bridge, _coordinator, _deletions = build_controller(
        tmp_path
    )
    target = tmp_path / "project"
    try:
        await controller.new(
            make_update(
                f"/new luna | max | planmode | sol | | {target} | first prompt"
            ),
            SimpleNamespace(),
        )

        suggestion = endpoint.sent[-1]["markdown"]
        assert "planmode | gpt-5.6-sol | high" in suggestion
        assert f"{target} | first prompt" in suggestion
    finally:
        await controller.stop()


@pytest.mark.asyncio
async def test_new_prompt_timeout_claims_hello_once(tmp_path: Path) -> None:
    controller, store, endpoint, _bridge, coordinator, _deletions = build_controller(tmp_path)
    try:
        await controller.new(make_update("/new luna | max | noplan | project"), SimpleNamespace())
        draft = next(iter(store.drafts.values()))
        draft.expires_at = int(time.time()) - 1
        await controller._run_new_timeout(draft.scope_key, draft.flow_id, draft.revision)
        await controller._run_new_timeout(draft.scope_key, draft.flow_id, draft.revision)

        assert [item["prompt"] for item in coordinator.created] == ["Hello"]
        assert "30 秒" in endpoint.sent[-2]["markdown"]
    finally:
        await controller.stop()


@pytest.mark.asyncio
async def test_new_late_prompt_cannot_replace_timeout_hello(tmp_path: Path) -> None:
    controller, store, _endpoint, _bridge, coordinator, _deletions = build_controller(tmp_path)
    try:
        await controller.new(make_update("/new luna | max | noplan | project"), SimpleNamespace())
        draft = next(iter(store.drafts.values()))
        draft.expires_at = int(time.time()) - 1

        await controller.observe_message(make_update("Late prompt"), SimpleNamespace())

        assert [item["prompt"] for item in coordinator.created] == ["Hello"]
    finally:
        await controller.stop()


@pytest.mark.asyncio
async def test_confirmed_project_is_claimed_before_directory_creation(
    tmp_path: Path,
) -> None:
    controller, store, _endpoint, bridge, _coordinator, _deletions = build_controller(
        tmp_path
    )
    target = tmp_path / "confirmed-project"
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked_create(value: Path) -> Path:
        assert value == target
        entered.set()
        await release.wait()
        value.mkdir(parents=True)
        return value

    bridge.create_project_directory = blocked_create
    try:
        await controller.new(
            make_update(f"/new luna | max | noplan | {target}"),
            SimpleNamespace(),
        )
        confirming = next(iter(store.drafts.values()))
        assert confirming.phase == "project_confirmation"
        confirmed_revision = confirming.revision
        callback = {
            "scope_key": confirming.scope_key,
            "flow_id": confirming.flow_id,
            "revision": confirming.revision,
            "event": "create_project",
            "value": str(target),
        }
        task = asyncio.create_task(
            controller._handle_new_callback(confirming.chat_id, callback)
        )
        await entered.wait()

        applying = store.get_interaction(confirming.scope_key)
        assert applying is not None
        assert applying.phase == "creating_project"
        assert (
                store.claim_interaction(
                    confirming.scope_key, confirming.flow_id, confirmed_revision
                )
            is None
        )

        release.set()
        await task
        waiting = store.get_interaction(confirming.scope_key)
        assert waiting is not None and waiting.phase == "prompt"
    finally:
        await controller.stop()


@pytest.mark.asyncio
async def test_perf_refreshes_frames_and_uses_one_fixed_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, _store, endpoint, bridge, _coordinator, deletions = build_controller(tmp_path)
    monkeypatch.setattr(control_bot, "_PERF_LIFETIME_SECONDS", 0.09)
    monkeypatch.setattr(control_bot, "_PERF_UPDATE_SECONDS", 0.02)
    monkeypatch.setattr(control_bot.time, "time", lambda: 10_000.0)
    try:
        await controller.perf(make_update("/perf", update_id=90, message_id=30), SimpleNamespace())
        await asyncio.sleep(0.12)

        assert bridge.metrics.calls >= 2
        assert endpoint.sent[-1]["markdown"].startswith("*🕛 动态性能*")
        assert endpoint.edited[0]["markdown"].startswith("*🕒 动态性能*")
        assert all(item["priority"] == 50 for item in endpoint.edited)
        assert [item["message_ids"] for item in deletions.scheduled] == [(30, 1000)]
        assert {item["delete_at"] for item in deletions.scheduled} == {10_001}
        assert {item["group_key"] for item in deletions.scheduled} == {"perf:90"}
    finally:
        await controller.stop()


@pytest.mark.asyncio
async def test_second_perf_cancels_and_deletes_previous_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, _store, _endpoint, _bridge, _coordinator, deletions = build_controller(tmp_path)
    monkeypatch.setattr(control_bot, "_PERF_LIFETIME_SECONDS", 1.0)
    monkeypatch.setattr(control_bot, "_PERF_UPDATE_SECONDS", 0.2)
    try:
        await controller.perf(make_update("/perf", update_id=1, message_id=11), SimpleNamespace())
        await controller.perf(make_update("/perf", update_id=2, message_id=12), SimpleNamespace())

        assert deletions.deleted_now[0]["message_ids"] == (11, 1000)
        assert len(controller._perf_runs) == 1
    finally:
        await controller.stop()
