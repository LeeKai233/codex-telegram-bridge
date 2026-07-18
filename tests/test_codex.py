from __future__ import annotations

import asyncio
import inspect
import json
from pathlib import Path
from typing import Any

import pytest

import codex_telegram_bridge.codex as codex_module
from codex_telegram_bridge.codex import CodexClient, CodexRpcError


@pytest.mark.asyncio
async def test_resume_thread_merges_latest_turn_metadata() -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))
    calls: list[tuple[str, dict[str, Any], float]] = []

    async def request(method: str, params: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
        calls.append((method, params, timeout))
        return {
            "thread": {"id": "thread-1", "turns": []},
            "model": "gpt-5.6-luna",
            "reasoningEffort": "max",
            "cwd": "/tmp/project",
            "initialTurnsPage": {
                "data": [{"id": "turn-2", "status": "inProgress", "items": [], "itemsView": "notLoaded"}]
            },
        }

    client.request = request  # type: ignore[method-assign]

    thread = await client.resume_thread("thread-1")

    assert thread["turns"][0]["id"] == "turn-2"
    assert thread["model"] == "gpt-5.6-luna"
    assert thread["reasoningEffort"] == "max"
    assert thread["cwd"] == "/tmp/project"
    assert calls == [
        (
            "thread/resume",
            {
                "threadId": "thread-1",
                "excludeTurns": True,
                "initialTurnsPage": {"limit": 1, "sortDirection": "desc", "itemsView": "notLoaded"},
            },
            60,
        )
    ]


@pytest.mark.asyncio
async def test_resume_thread_tolerates_missing_initial_page() -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))

    async def request(method: str, params: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
        del method, params, timeout
        return {"thread": {"id": "thread-1", "turns": []}}

    client.request = request  # type: ignore[method-assign]

    assert await client.resume_thread("thread-1") == {"id": "thread-1", "turns": []}


@pytest.mark.asyncio
async def test_start_thread_defaults_to_workspace_write_with_approval_prompts(tmp_path: Path) -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))
    calls: list[tuple[str, dict[str, Any], float]] = []

    async def request(method: str, params: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
        calls.append((method, params, timeout))
        return {"thread": {"id": "thread-1"}}

    client.request = request  # type: ignore[method-assign]

    await client.start_thread(tmp_path)

    assert calls[0][1]["sandbox"] == "workspace-write"
    assert calls[0][1]["approvalPolicy"] == "on-request"


@pytest.mark.asyncio
async def test_start_thread_keeps_read_only_fork_without_approvals(tmp_path: Path) -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))
    calls: list[tuple[str, dict[str, Any], float]] = []

    async def request(method: str, params: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
        calls.append((method, params, timeout))
        return {"thread": {"id": "thread-read-only"}}

    client.request = request  # type: ignore[method-assign]

    await client.start_thread(tmp_path, read_only=True)

    assert calls[0][1]["sandbox"] == "read-only"
    assert calls[0][1]["approvalPolicy"] == "never"


def test_ask_fork_question_defaults_to_five_minutes() -> None:
    timeout = inspect.signature(CodexClient.ask_fork_question).parameters["timeout"].default

    assert timeout == 300.0


@pytest.mark.asyncio
async def test_start_turn_passes_security_overrides(tmp_path: Path) -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))
    calls: list[tuple[str, dict[str, Any], float]] = []

    async def request(method: str, params: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
        calls.append((method, params, timeout))
        return {"turn": {"id": "turn-1"}}

    client.request = request  # type: ignore[method-assign]
    policy = {"type": "workspaceWrite", "networkAccess": False}

    await client.start_turn(
        "thread-1",
        [{"type": "text", "text": "hello"}],
        cwd=tmp_path,
        sandbox_policy=policy,
        approval_policy="never",
        model="gpt-5.6-luna",
        effort="max",
    )

    params = calls[0][1]
    assert params["cwd"] == str(tmp_path)
    assert params["sandboxPolicy"] == policy
    assert params["approvalPolicy"] == "never"
    assert params["model"] == "gpt-5.6-luna"
    assert params["effort"] == "max"


@pytest.mark.asyncio
async def test_start_turn_passes_resolved_collaboration_mode() -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))
    calls: list[tuple[str, dict[str, Any], float]] = []

    async def request(method: str, params: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
        calls.append((method, params, timeout))
        return {"turn": {"id": "turn-plan"}}

    client.request = request  # type: ignore[method-assign]
    collaboration_mode = {
        "mode": "plan",
        "settings": {
            "model": "gpt-5.6-sol",
            "reasoning_effort": "xhigh",
            "developer_instructions": None,
        },
    }

    await client.start_turn(
        "thread-1",
        [{"type": "text", "text": "plan"}],
        collaboration_mode=collaboration_mode,
    )

    assert calls[0][1]["collaborationMode"] == collaboration_mode
    with pytest.raises(ValueError, match="cannot be combined"):
        await client.start_turn(
            "thread-1",
            [{"type": "text", "text": "invalid"}],
            model="gpt-5.6-sol",
            collaboration_mode=collaboration_mode,
        )


@pytest.mark.asyncio
async def test_collaboration_modes_are_validated_and_resolved() -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))
    calls: list[tuple[str, dict[str, Any]]] = []

    async def request(
        method: str, params: dict[str, Any], timeout: float = 30.0
    ) -> dict[str, Any]:
        del timeout
        calls.append((method, params))
        return {
            "data": [
                {
                    "name": "Plan",
                    "mode": "plan",
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "xhigh",
                },
                {
                    "name": "Default",
                    "mode": "default",
                    "model": "gpt-5.6-sol",
                    "reasoning_effort": "high",
                },
            ]
        }

    client.request = request  # type: ignore[method-assign]

    resolved = await client.resolve_collaboration_mode("plan")

    assert calls == [("collaborationMode/list", {})]
    assert resolved == {
        "mode": "plan",
        "settings": {
            "model": "gpt-5.6-sol",
            "reasoning_effort": "xhigh",
            "developer_instructions": None,
        },
    }

    explicit = await client.resolve_collaboration_mode(
        "default",
        model=" gpt-5.6-luna ",
        effort=" max ",
    )
    assert explicit == {
        "mode": "default",
        "settings": {
            "model": "gpt-5.6-luna",
            "reasoning_effort": "max",
            "developer_instructions": None,
        },
    }
    assert calls == [("collaborationMode/list", {})]


@pytest.mark.asyncio
async def test_collaboration_modes_fail_closed_on_missing_or_invalid_capability() -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))

    async def unavailable(
        method: str, params: dict[str, Any], timeout: float = 30.0
    ) -> dict[str, Any]:
        del method, params, timeout
        return {"data": [{"name": "Default", "mode": "default", "model": None}]}

    client.request = unavailable  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="has no model"):
        await client.resolve_collaboration_mode("default")
    with pytest.raises(RuntimeError, match="unavailable"):
        await client.resolve_collaboration_mode("plan")
    with pytest.raises(ValueError, match="Unsupported"):
        await client.resolve_collaboration_mode("review")
    with pytest.raises(ValueError, match="provided together"):
        await client.resolve_collaboration_mode("plan", model="gpt-5.6-luna")


@pytest.mark.asyncio
async def test_model_options_follow_pagination_and_validate_efforts() -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))
    calls: list[tuple[str, dict[str, Any]]] = []

    async def request(
        method: str, params: dict[str, Any], timeout: float = 30.0
    ) -> dict[str, Any]:
        del timeout
        calls.append((method, params))
        if "cursor" not in params:
            return {
                "data": [
                    {
                        "model": "gpt-5.6-luna",
                        "displayName": "GPT-5.6 Luna",
                        "defaultReasoningEffort": "high",
                        "isDefault": True,
                        "supportedReasoningEfforts": [
                            {"reasoningEffort": "high", "description": ""},
                            {"reasoningEffort": "max", "description": ""},
                        ],
                    }
                ],
                "nextCursor": "page-2",
            }
        return {
            "data": [
                {
                    "model": "gpt-5.6-sol",
                    "displayName": "GPT-5.6 Sol",
                    "defaultReasoningEffort": "xhigh",
                    "isDefault": False,
                    "supportedReasoningEfforts": [
                        {"reasoningEffort": "xhigh", "description": ""}
                    ],
                }
            ],
            "nextCursor": None,
        }

    client.request = request  # type: ignore[method-assign]

    options = await client.list_model_options(page_size=1)

    assert [option.model for option in options] == ["gpt-5.6-luna", "gpt-5.6-sol"]
    assert options[0].supported_efforts == ("high", "max")
    assert options[0].is_default
    assert calls == [
        ("model/list", {"limit": 1}),
        ("model/list", {"limit": 1, "cursor": "page-2"}),
    ]


@pytest.mark.asyncio
async def test_update_thread_settings_sends_explicit_collaboration_profile() -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))
    calls: list[tuple[str, dict[str, Any], float]] = []

    async def request(method: str, params: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
        calls.append((method, params, timeout))
        return {}

    client.request = request  # type: ignore[method-assign]
    collaboration_mode = await client.resolve_collaboration_mode(
        "plan",
        model="gpt-5.6-luna",
        effort="low",
    )

    await client.update_thread_settings(
        "thread-1",
        collaboration_mode=collaboration_mode,
    )

    assert calls == [
        (
            "thread/settings/update",
            {"threadId": "thread-1", "collaborationMode": collaboration_mode},
            30,
        )
    ]


@pytest.mark.asyncio
async def test_list_thread_page_passes_search_and_cursor() -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))
    calls: list[tuple[str, dict[str, Any], float]] = []

    async def request(method: str, params: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
        calls.append((method, params, timeout))
        return {
            "data": [{"id": "thread-2"}],
            "nextCursor": "next-page",
            "backwardsCursor": "previous-page",
        }

    client.request = request  # type: ignore[method-assign]

    page = await client.list_thread_page(limit=5, cursor="current-page", search_term="  fitting  ")

    assert [thread["id"] for thread in page.data] == ["thread-2"]
    assert page.next_cursor == "next-page"
    assert page.backwards_cursor == "previous-page"
    assert calls[0][1] == {
        "limit": 5,
        "sortKey": "recency_at",
        "sortDirection": "desc",
        "useStateDbOnly": True,
        "cursor": "current-page",
        "searchTerm": "fitting",
    }


@pytest.mark.asyncio
async def test_list_threads_follows_cursors_until_exact_limit() -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))
    calls: list[dict[str, Any]] = []

    async def request(method: str, params: dict[str, Any], timeout: float = 30.0) -> dict[str, Any]:
        del method, timeout
        calls.append(params)
        if "cursor" not in params:
            return {"data": [{"id": "one"}, {"id": "two"}], "nextCursor": "older"}
        return {"data": [{"id": "three"}, {"id": "four"}], "nextCursor": "oldest"}

    client.request = request  # type: ignore[method-assign]

    threads = await client.list_threads(limit=3, search_term="session")

    assert [thread["id"] for thread in threads] == ["one", "two", "three"]
    assert calls == [
        {
            "limit": 3,
            "sortKey": "recency_at",
            "sortDirection": "desc",
            "useStateDbOnly": True,
            "searchTerm": "session",
        },
        {
            "limit": 1,
            "sortKey": "recency_at",
            "sortDirection": "desc",
            "useStateDbOnly": True,
            "cursor": "older",
            "searchTerm": "session",
        },
    ]


@pytest.mark.asyncio
async def test_isolated_fork_questions_are_correlated_and_read_only(tmp_path: Path) -> None:
    forwarded: list[tuple[str, dict[str, Any]]] = []

    async def on_notification(method: str, params: dict[str, Any]) -> None:
        forwarded.append((method, params))

    client = CodexClient(Path("/tmp/not-used.sock"), on_notification=on_notification)
    calls: list[tuple[str, dict[str, Any]]] = []

    async def request(
        method: str, params: dict[str, Any], timeout: float = 30.0
    ) -> dict[str, Any]:
        del timeout
        calls.append((method, params))
        if method == "thread/fork":
            fork_id = f"fork-{params['threadId']}"
            return {"thread": {"id": fork_id, "ephemeral": True}}
        if method == "turn/start":
            fork_id = str(params["threadId"])
            turn_id = f"turn-{fork_id}"

            async def complete() -> None:
                await asyncio.sleep(0)
                await client._dispatch_notification(
                    "item/completed",
                    {
                        "threadId": fork_id,
                        "turnId": turn_id,
                        "item": {
                            "id": f"commentary-{fork_id}",
                            "type": "agentMessage",
                            "phase": "commentary",
                            "text": "interim",
                        },
                    },
                )
                await client._dispatch_notification(
                    "turn/completed",
                    {
                        "threadId": fork_id,
                        "turn": {
                            "id": turn_id,
                            "status": "completed",
                            "items": [
                                {
                                    "id": f"answer-{fork_id}",
                                    "type": "agentMessage",
                                    "phase": "final_answer",
                                    "text": f"answer for {fork_id}",
                                }
                            ],
                        },
                    },
                )

            asyncio.create_task(complete())
            return {"turn": {"id": turn_id, "status": "inProgress", "items": []}}
        return {}

    client.request = request  # type: ignore[method-assign]

    first, second = await asyncio.gather(
        client.ask_fork_question(
            "one", tmp_path, "first?", client_message_id="telegram-one"
        ),
        client.ask_fork_question(
            "two", tmp_path, "second?", client_message_id="telegram-two"
        ),
    )

    assert (first, second) == ("answer for fork-one", "answer for fork-two")
    assert forwarded == []
    forks = [params for method, params in calls if method == "thread/fork"]
    assert all(params["ephemeral"] is True for params in forks)
    assert all(params["sandbox"] == "read-only" for params in forks)
    assert all(params["approvalPolicy"] == "never" for params in forks)
    turns = [params for method, params in calls if method == "turn/start"]
    assert {params["clientUserMessageId"] for params in turns} == {
        "telegram-one",
        "telegram-two",
    }
    assert all(
        params["sandboxPolicy"] == {"type": "readOnly", "networkAccess": False}
        for params in turns
    )
    assert all(params["approvalPolicy"] == "never" for params in turns)
    assert {params["threadId"] for method, params in calls if method == "thread/delete"} == {
        "fork-one",
        "fork-two",
    }
    assert not any(method == "thread/read" for method, _ in calls)
    assert client._ephemeral_thread_ids == set()


@pytest.mark.asyncio
async def test_ephemeral_turn_without_base_thread_uses_notifications_instead_of_thread_read(
    tmp_path: Path,
) -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))
    calls: list[tuple[str, dict[str, Any]]] = []

    async def request(
        method: str, params: dict[str, Any], timeout: float = 30.0
    ) -> dict[str, Any]:
        del timeout
        calls.append((method, params))
        if method == "thread/start":
            return {"thread": {"id": "resolver-thread", "ephemeral": True}}
        if method == "turn/start":
            await client._dispatch_notification(
                "turn/completed",
                {
                    "threadId": "resolver-thread",
                    "turn": {
                        "id": "resolver-turn",
                        "status": "completed",
                        "items": [
                            {
                                "id": "resolver-answer",
                                "type": "agentMessage",
                                "text": json.dumps({"paths": ["report.pdf"]}),
                            }
                        ],
                    },
                },
            )
            return {"turn": {"id": "resolver-turn", "status": "inProgress", "items": []}}
        return {}

    client.request = request  # type: ignore[method-assign]

    answer = await client.run_ephemeral_turn(
        tmp_path,
        "Find the resume PDF",
        output_schema={"type": "object"},
    )

    assert answer == json.dumps({"paths": ["report.pdf"]})
    assert not any(method == "thread/read" for method, _ in calls)
    assert any(
        method == "thread/start"
        and params["ephemeral"] is True
        and params["sandbox"] == "read-only"
        for method, params in calls
    )
    assert client._ephemeral_thread_ids == set()


@pytest.mark.asyncio
async def test_isolated_question_timeout_interrupts_exact_turn_and_deletes_fork(
    tmp_path: Path,
) -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))
    calls: list[tuple[str, dict[str, Any]]] = []

    async def request(
        method: str, params: dict[str, Any], timeout: float = 30.0
    ) -> dict[str, Any]:
        del timeout
        calls.append((method, params))
        if method == "thread/fork":
            return {"thread": {"id": "fork-timeout", "ephemeral": True}}
        if method == "turn/start":
            return {"turn": {"id": "turn-timeout", "status": "inProgress", "items": []}}
        return {}

    client.request = request  # type: ignore[method-assign]

    with pytest.raises(TimeoutError, match="timed out"):
        await client.ask_fork_question(
            "primary", tmp_path, "question?", client_message_id="telegram-timeout", timeout=0.01
        )

    assert ("turn/interrupt", {"threadId": "fork-timeout", "turnId": "turn-timeout"}) in calls
    assert ("thread/delete", {"threadId": "fork-timeout"}) in calls
    assert client._isolated_questions == {}
    assert client._ephemeral_thread_ids == set()


@pytest.mark.asyncio
async def test_isolated_question_model_override_failure_is_scoped_and_clear(
    tmp_path: Path,
) -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))
    calls: list[tuple[str, dict[str, Any]]] = []

    async def request(
        method: str, params: dict[str, Any], timeout: float = 30.0
    ) -> dict[str, Any]:
        del timeout
        calls.append((method, params))
        if method == "thread/fork":
            return {"thread": {"id": "fork-unsupported", "ephemeral": True}}
        if method == "turn/start":
            raise CodexRpcError(method, {"message": "unknown model gpt-5.6-luna"})
        if method == "thread/list":
            return {"data": [{"id": "primary"}]}
        return {}

    client.request = request  # type: ignore[method-assign]

    with pytest.raises(
        RuntimeError,
        match=r"Configured utility model or effort was rejected.*gpt-5\.6-luna, max",
    ):
        await client.ask_fork_question(
            "primary",
            tmp_path,
            "question?",
            client_message_id="telegram-model",
            model="gpt-5.6-luna",
            effort="max",
        )

    turn_params = next(params for method, params in calls if method == "turn/start")
    assert turn_params["model"] == "gpt-5.6-luna"
    assert turn_params["effort"] == "max"
    assert ("thread/delete", {"threadId": "fork-unsupported"}) in calls
    assert await client.list_threads(limit=1) == [{"id": "primary"}]
    assert client._ephemeral_thread_ids == set()


@pytest.mark.asyncio
async def test_isolated_question_binds_exact_turn_when_completion_precedes_start_response(
    tmp_path: Path,
) -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))

    async def request(
        method: str, params: dict[str, Any], timeout: float = 30.0
    ) -> dict[str, Any]:
        del timeout
        if method == "thread/fork":
            return {"thread": {"id": "fork-early", "ephemeral": True}}
        if method == "turn/start":
            await client._dispatch_notification(
                "turn/completed",
                {
                    "threadId": "fork-early",
                    "turn": {
                        "id": "unrelated-turn",
                        "status": "completed",
                        "items": [
                            {"id": "wrong", "type": "agentMessage", "text": "wrong answer"}
                        ],
                    },
                },
            )
            await client._dispatch_notification(
                "turn/completed",
                {
                    "threadId": "fork-early",
                    "turn": {
                        "id": "expected-turn",
                        "status": "completed",
                        "items": [
                            {"id": "right", "type": "agentMessage", "text": "right answer"}
                        ],
                    },
                },
            )
            return {"turn": {"id": "expected-turn", "status": "inProgress", "items": []}}
        return {}

    client.request = request  # type: ignore[method-assign]

    answer = await client.ask_fork_question(
        "primary", tmp_path, "question?", client_message_id="telegram-early"
    )

    assert answer == "right answer"


@pytest.mark.asyncio
async def test_failed_isolated_question_deletes_fork_without_interrupting_terminal_turn(
    tmp_path: Path,
) -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))
    calls: list[tuple[str, dict[str, Any]]] = []

    async def request(
        method: str, params: dict[str, Any], timeout: float = 30.0
    ) -> dict[str, Any]:
        del timeout
        calls.append((method, params))
        if method == "thread/fork":
            return {"thread": {"id": "fork-failed", "ephemeral": True}}
        if method == "turn/start":
            asyncio.create_task(
                client._dispatch_notification(
                    "turn/completed",
                    {
                        "threadId": "fork-failed",
                        "turn": {
                            "id": "turn-failed",
                            "status": "failed",
                            "error": {"message": "model failed"},
                            "items": [],
                        },
                    },
                )
            )
            return {"turn": {"id": "turn-failed", "status": "inProgress", "items": []}}
        return {}

    client.request = request  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="model failed"):
        await client.ask_fork_question(
            "primary", tmp_path, "question?", client_message_id="telegram-failed"
        )

    assert not any(method == "turn/interrupt" for method, _ in calls)
    assert ("thread/delete", {"threadId": "fork-failed"}) in calls
    assert client._ephemeral_thread_ids == set()


@pytest.mark.asyncio
async def test_isolated_question_discards_ephemeral_id_when_delete_fails(
    tmp_path: Path,
) -> None:
    client = CodexClient(Path("/tmp/not-used.sock"))

    async def request(
        method: str, params: dict[str, Any], timeout: float = 30.0
    ) -> dict[str, Any]:
        del timeout
        if method == "thread/fork":
            return {"thread": {"id": "fork-delete-fails", "ephemeral": True}}
        if method == "turn/start":
            await client._dispatch_notification(
                "turn/completed",
                {
                    "threadId": "fork-delete-fails",
                    "turn": {
                        "id": "turn-delete-fails",
                        "status": "completed",
                        "items": [
                            {
                                "id": "answer-delete-fails",
                                "type": "agentMessage",
                                "phase": "final_answer",
                                "text": "answer",
                            }
                        ],
                    },
                },
            )
            return {"turn": {"id": "turn-delete-fails", "status": "inProgress"}}
        if method == "thread/delete":
            raise RuntimeError("delete failed")
        return {}

    client.request = request  # type: ignore[method-assign]

    answer = await client.ask_fork_question(
        "primary", tmp_path, "question?", client_message_id="telegram-delete-fails"
    )

    assert answer == "answer"
    assert client._ephemeral_thread_ids == set()


@pytest.mark.asyncio
async def test_stop_waits_for_reader_server_request_task_cancellation() -> None:
    handler_started = asyncio.Event()
    handler_cancelled = asyncio.Event()

    async def on_server_request(
        request_id: int | str, method: str, params: dict[str, Any], generation: int
    ) -> None:
        del request_id, method, params, generation
        handler_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await asyncio.sleep(0)
            handler_cancelled.set()
            raise

    class FakeWebSocket:
        def __init__(self) -> None:
            self.sent_request = False
            self.closed = asyncio.Event()

        def __aiter__(self) -> FakeWebSocket:
            return self

        async def __anext__(self) -> str:
            if not self.sent_request:
                self.sent_request = True
                return '{"id": 42, "method": "item/tool/requestUserInput", "params": {}}'
            await self.closed.wait()
            raise StopAsyncIteration

        async def close(self, *, code: int, reason: str) -> None:
            del code, reason
            self.closed.set()

    client = CodexClient(
        Path("/tmp/not-used.sock"), on_server_request=on_server_request
    )
    websocket = FakeWebSocket()
    client._websocket = websocket  # type: ignore[assignment]
    client._runner = asyncio.create_task(client._reader(websocket))  # type: ignore[arg-type]

    await asyncio.wait_for(handler_started.wait(), timeout=1)
    assert len(client._server_request_tasks) == 1

    await asyncio.wait_for(client.stop(), timeout=1)

    assert handler_cancelled.is_set()
    assert client._server_request_tasks == set()


@pytest.mark.asyncio
async def test_disconnect_drains_server_requests_before_connection_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler_started = asyncio.Event()
    handler_cancelled = asyncio.Event()
    disconnected_after_cleanup = False

    async def on_server_request(
        request_id: int | str, method: str, params: dict[str, Any], generation: int
    ) -> None:
        del request_id, method, params, generation
        handler_started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await asyncio.sleep(0)
            handler_cancelled.set()
            raise

    async def on_connection(connected: bool, generation: int, reason: str | None) -> None:
        nonlocal disconnected_after_cleanup
        del generation, reason
        if not connected:
            disconnected_after_cleanup = handler_cancelled.is_set()
            client._stopping.set()

    class DisconnectingWebSocket:
        def __init__(self) -> None:
            self.sent_request = False

        def __aiter__(self) -> DisconnectingWebSocket:
            return self

        async def __anext__(self) -> str:
            if not self.sent_request:
                self.sent_request = True
                return '{"id": 43, "method": "item/tool/requestUserInput", "params": {}}'
            await handler_started.wait()
            raise StopAsyncIteration

    websocket = DisconnectingWebSocket()

    async def connect(**kwargs: Any) -> DisconnectingWebSocket:
        del kwargs
        return websocket

    client = CodexClient(
        Path("/tmp/not-used.sock"),
        on_server_request=on_server_request,
        on_connection=on_connection,
    )

    async def initialize() -> None:
        return None

    client._initialize = initialize  # type: ignore[method-assign]
    monkeypatch.setattr(codex_module, "unix_connect", connect)

    await asyncio.wait_for(client._run(), timeout=1)

    assert disconnected_after_cleanup is True
    assert client._server_request_tasks == set()


@pytest.mark.asyncio
async def test_ephemeral_thread_started_before_rpc_response_is_never_forwarded() -> None:
    notifications: list[str] = []
    server_requests: list[str] = []
    rejected: list[tuple[int | str, int, str]] = []

    async def on_notification(method: str, params: dict[str, Any]) -> None:
        del params
        notifications.append(method)

    async def on_server_request(
        request_id: int | str, method: str, params: dict[str, Any], generation: int
    ) -> None:
        del request_id, params, generation
        server_requests.append(method)

    client = CodexClient(
        Path("/tmp/not-used.sock"),
        on_notification=on_notification,
        on_server_request=on_server_request,
    )

    async def respond_error(request_id: int | str, code: int, message: str) -> None:
        rejected.append((request_id, code, message))

    client.respond_error = respond_error  # type: ignore[method-assign]
    await client._dispatch_notification(
        "thread/started",
        {
            "thread": {
                "id": "early-fork",
                "ephemeral": True,
                "forkedFromId": "primary",
            }
        },
    )
    await client._dispatch_notification(
        "item/completed",
        {
            "threadId": "early-fork",
            "turnId": "side-turn",
            "item": {"id": "answer", "type": "agentMessage", "text": "hidden"},
        },
    )
    await client._dispatch_notification(
        "turn/completed",
        {
            "threadId": "early-fork",
            "turn": {"id": "side-turn", "status": "completed", "items": []},
        },
    )
    await client._dispatch_server_request(
        41,
        "item/tool/requestUserInput",
        {"threadId": "early-fork", "turnId": "side-turn", "questions": []},
        3,
    )

    assert notifications == []
    assert server_requests == []
    assert rejected == [
        (41, -32600, "Interactive requests are disabled for isolated side questions")
    ]
