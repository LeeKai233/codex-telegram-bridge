from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from codex_telegram_bridge.config import Config
from codex_telegram_bridge.discussion_bot import DiscussionBotController
from codex_telegram_bridge.models import Owner, ThreadState
from codex_telegram_bridge.telegram_common import DISCUSSION_ROLE

SPACE = {
    "space_id": "space-command",
    "generation": 3,
    "lifecycle": "active",
    "thread_id": "thread-command",
    "discussion_chat_id": -100426,
    "discussion_root_id": 42,
    "current_mode": "normal",
    "normal_model": "gpt-5.6-sol",
    "normal_effort": "xhigh",
    "plan_model": "gpt-5.6-luna",
    "plan_effort": "max",
}


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
    space_id: str | None
    generation: int
    expires_at: int
    created_at: int
    updated_at: int
    claimed_at: int | None = None


class MemoryStore:
    def __init__(self) -> None:
        self.owner = Owner(7, 7, "owner")
        self.drafts: dict[str, Draft] = {}
        self.callbacks: list[tuple[str, dict[str, Any]]] = []
        self.spaces = {str(SPACE["space_id"]): dict(SPACE)}
        self.queue: list[dict[str, Any]] = []
        self.publications: list[dict[str, Any]] = []
        self.released: list[tuple[str, int, str, str]] = []
        self.completed: list[tuple[str, int, str, str, str]] = []

    def get_owner(self) -> Owner:
        return self.owner

    def get_space(self, space_id: str) -> dict[str, Any] | None:
        return self.spaces.get(space_id)

    def space_queue_entries(self, _space_id: str, _generation: int) -> list[dict[str, Any]]:
        return self.queue

    def put_callback(
        self,
        _nonce: str,
        action: str,
        payload: dict[str, Any],
        *_args: Any,
        **_kwargs: Any,
    ) -> None:
        self.callbacks.append((action, payload))

    def replace_interaction(
        self,
        scope_key: str,
        *,
        kind: str,
        phase: str,
        payload: dict[str, Any],
        user_id: int,
        bot_role: str,
        chat_id: int,
        expires_at: int,
        space_id: str | None = None,
        generation: int = 0,
    ) -> Draft:
        now = int(time.time())
        draft = Draft(
            scope_key,
            f"flow-{len(self.drafts) + 1}",
            1,
            kind,
            phase,
            dict(payload),
            user_id,
            bot_role,
            chat_id,
            space_id,
            generation,
            expires_at,
            now,
            now,
        )
        self.drafts[scope_key] = draft
        return draft

    def get_interaction(self, scope_key: str) -> Draft | None:
        draft = self.drafts.get(scope_key)
        return draft if draft and draft.claimed_at is None else None

    def advance_interaction(
        self,
        scope_key: str,
        flow_id: str,
        revision: int,
        *,
        phase: str,
        payload: dict[str, Any],
        expires_at: int,
    ) -> Draft | None:
        current = self.get_interaction(scope_key)
        if current is None or (current.flow_id, current.revision) != (flow_id, revision):
            return None
        advanced = replace(
            current,
            revision=revision + 1,
            phase=phase,
            payload=dict(payload),
            expires_at=expires_at,
            updated_at=int(time.time()),
        )
        self.drafts[scope_key] = advanced
        return advanced

    def claim_interaction(
        self, scope_key: str, flow_id: str, revision: int
    ) -> Draft | None:
        current = self.get_interaction(scope_key)
        if current is None or (current.flow_id, current.revision) != (flow_id, revision):
            return None
        claimed = replace(current, claimed_at=int(time.time()))
        self.drafts[scope_key] = claimed
        return claimed

    def delete_interaction(self, scope_key: str) -> None:
        self.drafts.pop(scope_key, None)

    def list_interactions(self, kind: str | None = None) -> list[Draft]:
        return [
            draft
            for draft in self.drafts.values()
            if draft.claimed_at is None and (kind is None or draft.kind == kind)
        ]

    def executing_plan_publications(self) -> list[dict[str, Any]]:
        return list(self.publications)

    def recoverable_plan_publications(self) -> list[dict[str, Any]]:
        return [
            {**publication, "status": str(publication.get("status") or "executing")}
            for publication in self.publications
        ]

    def complete_plan_action(
        self,
        space_id: str,
        generation: int,
        item_id: str,
        *,
        expected_status: str,
        status: str,
    ) -> bool:
        self.completed.append(
            (space_id, generation, item_id, expected_status, status)
        )
        return True

    def release_plan_action(
        self,
        space_id: str,
        generation: int,
        item_id: str,
        *,
        expected_status: str = "executing",
    ) -> bool:
        self.released.append((space_id, generation, item_id, expected_status))
        return True


class FakeBridge:
    def __init__(self, store: MemoryStore) -> None:
        self.store = store
        self.options = [
            SimpleNamespace(
                model="gpt-5.6-luna",
                display_name="GPT-5.6 Luna",
                supported_efforts=("high", "max"),
                default_effort="max",
                is_default=False,
            ),
            SimpleNamespace(
                model="gpt-5.6-sol",
                display_name="GPT-5.6 Sol",
                supported_efforts=("high", "xhigh"),
                default_effort="xhigh",
                is_default=True,
            ),
        ]
        self.profile_sets: list[tuple[str, str, str, str]] = []
        self.model_changes: list[tuple[str, str, str]] = []
        self.turns: list[dict[str, Any]] = []
        self.reconcile_status = "absent"

    async def list_model_options(self) -> list[object]:
        return self.options

    async def resolve_model_profile(self, model: str, effort: str) -> object:
        aliases = {"luna": "gpt-5.6-luna", "sol": "gpt-5.6-sol"}
        selected = aliases.get(model, model)
        option = next((item for item in self.options if item.model == selected), None)
        if option is None or effort not in option.supported_efforts:
            raise ValueError("invalid model profile")
        return SimpleNamespace(model=selected, effort=effort)

    async def set_space_profile(
        self, space_id: str, mode: str, model: str, effort: str
    ) -> dict[str, Any]:
        self.profile_sets.append((space_id, mode, model, effort))
        return self.store.spaces[space_id]

    async def change_space_model(
        self, space_id: str, model: str, effort: str
    ) -> dict[str, Any]:
        self.model_changes.append((space_id, model, effort))
        return self.store.spaces[space_id]

    async def start_space_collaboration_turn(
        self,
        space_id: str,
        prompt: str,
        *,
        mode: str,
        client_message_id: str,
        profile: object,
    ) -> dict[str, str]:
        self.turns.append(
            {
                "space_id": space_id,
                "prompt": prompt,
                "mode": mode,
                "client_message_id": client_message_id,
                "profile": profile,
            }
        )
        return {"id": "turn-planmode"}

    async def reconcile_plan_execution(
        self,
        _space_id: str,
        _generation: int,
        _item_id: str,
        _client_message_id: str,
    ) -> str:
        return self.reconcile_status


class FakeDashboards:
    def __init__(self) -> None:
        self.calls: list[tuple[str, bool]] = []

    async def schedule_space(self, space_id: str, *, immediate: bool = False) -> None:
        self.calls.append((space_id, immediate))


def update(text: str) -> SimpleNamespace:
    return SimpleNamespace(effective_message=SimpleNamespace(text=text, caption=None))


@pytest.fixture
def command_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    store = MemoryStore()
    bridge = FakeBridge(store)
    dashboards = FakeDashboards()
    config = replace(
        Config.default(),
        config_dir=tmp_path / "config",
        state_dir=tmp_path / "state",
        allowed_root=tmp_path,
    )
    controller = DiscussionBotController(
        config,
        store,  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        bridge,  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        dashboards,  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
    )
    sent: list[dict[str, Any]] = []

    async def send_space(
        _space: dict[str, Any],
        markdown: str,
        **kwargs: Any,
    ) -> SimpleNamespace:
        sent.append({"markdown": markdown, **kwargs})
        return SimpleNamespace(message_id=len(sent))

    async def state(_space: dict[str, Any]) -> ThreadState:
        return ThreadState(thread_id="thread-command", status="idle")

    monkeypatch.setattr(controller, "_send_space", send_space)
    monkeypatch.setattr(controller, "_state", state)
    monkeypatch.setattr(controller, "_require_active_unlocked", lambda _update: dict(SPACE))
    monkeypatch.setattr(controller, "_schedule_interaction_timeout", lambda _draft: None)
    monkeypatch.setattr(controller, "_cancel_interaction_timeout", lambda _scope: None)
    return SimpleNamespace(
        controller=controller,
        store=store,
        bridge=bridge,
        dashboards=dashboards,
        sent=sent,
    )


@pytest.mark.asyncio
async def test_planmode_full_command_preserves_pipe_text_and_uses_selected_profile(
    command_runtime: SimpleNamespace,
) -> None:
    runtime = command_runtime

    await runtime.controller.planmode(
        update("/planmode luna | max | inspect A | then B"), SimpleNamespace()
    )

    assert runtime.bridge.profile_sets == [
        ("space-command", "plan", "gpt-5.6-luna", "max")
    ]
    [turn] = runtime.bridge.turns
    assert turn["prompt"] == "inspect A | then B"
    assert turn["mode"] == "plan"
    assert (turn["profile"].model, turn["profile"].effort) == (
        "gpt-5.6-luna",
        "max",
    )


@pytest.mark.asyncio
async def test_invalid_profile_returns_a_normalized_command_suggestion(
    command_runtime: SimpleNamespace,
) -> None:
    runtime = command_runtime

    await runtime.controller.changemodel(
        update("/changemodel lunaa | mx"), SimpleNamespace()
    )

    assert runtime.bridge.model_changes == []
    assert (
        "/changemodel gpt-5.6-luna | max" in runtime.sent[-1]["markdown"]
    )


@pytest.mark.asyncio
async def test_invalid_planmode_profile_suggestion_preserves_prompt(
    command_runtime: SimpleNamespace,
) -> None:
    runtime = command_runtime

    await runtime.controller.planmode(
        update("/planmode lunaa | mx | keep this | exact prompt"),
        SimpleNamespace(),
    )

    assert (
        "/planmode gpt-5.6-luna | max | keep this | exact prompt"
        in runtime.sent[-1]["markdown"]
    )


@pytest.mark.asyncio
async def test_planmode_interaction_advances_revision_then_claims_first_prompt(
    command_runtime: SimpleNamespace,
) -> None:
    runtime = command_runtime
    controller = runtime.controller

    await controller._begin_profile_interaction(dict(SPACE), "planmode")
    action, model_payload = runtime.store.callbacks[0]
    assert action == "profile_model"
    await controller._profile_model_selected(dict(SPACE), model_payload)

    draft = next(iter(runtime.store.drafts.values()))
    assert (draft.revision, draft.phase) == (2, "select_effort")
    with pytest.raises(RuntimeError, match="替换或已经过期"):
        controller._current_draft(model_payload, phase="select_model")

    action, effort_payload = runtime.store.callbacks[-1]
    assert action == "profile_effort"
    await controller._profile_effort_selected(dict(SPACE), effort_payload)
    waiting = next(iter(runtime.store.drafts.values()))
    assert (waiting.revision, waiting.phase) == (3, "await_prompt")

    consumed = await controller._consume_plan_prompt(dict(SPACE), waiting, "Draft a plan")
    assert consumed is True
    assert runtime.bridge.turns[-1]["prompt"] == "Draft a plan"
    assert runtime.store.get_interaction(waiting.scope_key) is None


@pytest.mark.asyncio
async def test_profile_effort_claims_revision_before_profile_side_effect(
    command_runtime: SimpleNamespace,
) -> None:
    runtime = command_runtime
    controller = runtime.controller
    await controller._begin_profile_interaction(dict(SPACE), "planmode")
    await controller._profile_model_selected(
        dict(SPACE), runtime.store.callbacks[0][1]
    )
    effort_payload = runtime.store.callbacks[-1][1]
    selecting = next(iter(runtime.store.drafts.values()))
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocked_set_profile(
        space_id: str, mode: str, model: str, effort: str
    ) -> dict[str, Any]:
        entered.set()
        await release.wait()
        runtime.bridge.profile_sets.append((space_id, mode, model, effort))
        return runtime.store.spaces[space_id]

    runtime.bridge.set_space_profile = blocked_set_profile
    task = asyncio.create_task(
        controller._profile_effort_selected(dict(SPACE), effort_payload)
    )
    await entered.wait()

    waiting = runtime.store.get_interaction(selecting.scope_key)
    assert waiting is not None
    assert (waiting.revision, waiting.phase) == (3, "await_prompt")
    assert (
        runtime.store.claim_interaction(
            selecting.scope_key, selecting.flow_id, selecting.revision
        )
        is None
    )

    release.set()
    await task
    assert runtime.bridge.profile_sets[-1] == (
        "space-command",
        "plan",
        "gpt-5.6-luna",
        "max",
    )


@pytest.mark.asyncio
async def test_changemodel_is_allowed_while_turn_runs_and_only_updates_future_turns(
    command_runtime: SimpleNamespace,
) -> None:
    runtime = command_runtime

    async def active_state(_space: dict[str, Any]) -> ThreadState:
        return ThreadState(
            thread_id="thread-command",
            status="active",
            turn_status="inProgress",
        )

    runtime.controller._state = active_state
    await runtime.controller.changemodel(
        update("/changemodel sol | xhigh"), SimpleNamespace()
    )

    assert runtime.bridge.model_changes == [
        ("space-command", "gpt-5.6-sol", "xhigh")
    ]
    assert "当前 turn 不变，后续 turn 使用新配置" in runtime.sent[-1]["markdown"]


@pytest.mark.asyncio
async def test_planmode_gate_rejects_pending_input_and_nonempty_queue(
    command_runtime: SimpleNamespace,
) -> None:
    runtime = command_runtime

    async def waiting_state(_space: dict[str, Any]) -> ThreadState:
        return ThreadState(
            thread_id="thread-command",
            status="idle",
            active_flags=["waitingOnUserInput"],
        )

    runtime.controller._state = waiting_state
    with pytest.raises(RuntimeError, match="等待审批或用户输入"):
        await runtime.controller._ensure_plan_ready(dict(SPACE))

    async def idle_state(_space: dict[str, Any]) -> ThreadState:
        return ThreadState(thread_id="thread-command", status="idle")

    runtime.controller._state = idle_state
    runtime.store.queue.append({"queue_id": 1})
    with pytest.raises(RuntimeError, match="队列非空"):
        await runtime.controller._ensure_plan_ready(dict(SPACE))


@pytest.mark.asyncio
async def test_expired_prompt_and_absent_plan_execution_are_recovered(
    command_runtime: SimpleNamespace,
) -> None:
    runtime = command_runtime
    draft = runtime.store.replace_interaction(
        "discussion:-100426:space-command:3:7",
        kind="planmode",
        phase="await_prompt",
        payload={"model": "gpt-5.6-luna", "effort": "max"},
        user_id=7,
        bot_role=DISCUSSION_ROLE,
        chat_id=-100426,
        expires_at=int(time.time()) - 1,
        space_id="space-command",
        generation=3,
    )
    await runtime.controller._expire_interaction(
        draft.scope_key, draft.flow_id, draft.revision, draft.expires_at
    )
    assert "Plan Mode 切换已取消" in runtime.sent[-1]["markdown"]

    runtime.store.publications = [
        {
            "space_id": "space-command",
            "generation": 3,
            "item_id": "item-plan",
            "thread_id": "thread-command",
            "turn_id": "turn-plan",
        }
    ]
    await runtime.controller._recover_plan_executions()

    assert runtime.store.released == [
        ("space-command", 3, "item-plan", "executing")
    ]
    assert "没有送达 Codex" in runtime.sent[-1]["markdown"]
    assert runtime.sent[-1]["reply_markup"] is not None


@pytest.mark.asyncio
async def test_startup_plan_recovery_waits_for_codex_connection(
    command_runtime: SimpleNamespace,
) -> None:
    runtime = command_runtime
    connected = asyncio.Event()

    class Client:
        connected = False

        async def wait_connected(self, timeout: float) -> None:
            assert timeout == 120
            await connected.wait()

    runtime.bridge.client = Client()
    runtime.store.publications = [
        {
            "space_id": "space-command",
            "generation": 3,
            "item_id": "item-startup",
            "thread_id": "thread-command",
            "turn_id": "turn-plan",
        }
    ]

    await runtime.controller._ensure_plan_recovery()

    task = runtime.controller._plan_recovery_task
    assert task is not None
    assert runtime.store.released == []
    connected.set()
    await task
    assert runtime.controller._plan_recovery_done
    assert runtime.store.released == [
        ("space-command", 3, "item-startup", "executing")
    ]


@pytest.mark.asyncio
async def test_startup_releases_revision_that_was_not_delivered(
    command_runtime: SimpleNamespace,
) -> None:
    runtime = command_runtime
    runtime.store.publications = [
        {
            "space_id": "space-command",
            "generation": 3,
            "item_id": "item-revision",
            "thread_id": "thread-command",
            "turn_id": "turn-plan",
            "status": "revising",
        }
    ]

    await runtime.controller._recover_plan_executions()

    assert runtime.store.released == [
        ("space-command", 3, "item-revision", "revising")
    ]
    assert runtime.sent[-1]["reply_markup"] is not None
