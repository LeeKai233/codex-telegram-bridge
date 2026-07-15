from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Awaitable, Callable
from typing import Any

from .models import LifecycleActivity, PlanStep, TaskState, ThreadState
from .store import Store

ChangeHandler = Callable[[ThreadState, str], Awaitable[None]]


async def _noop_change(state: ThreadState, reason: str) -> None:
    del state, reason


def _canonical_hash(method: str, params: dict[str, Any]) -> str:
    raw = json.dumps([method, params], sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


_REPEATABLE_NOTIFICATIONS = {
    "thread/status/changed",
    "error",
    "warning",
    "guardianWarning",
    "deprecationNotice",
    "configWarning",
}


class EventProjector:
    def __init__(self, store: Store, on_change: ChangeHandler = _noop_change) -> None:
        self.store = store
        self.on_change = on_change
        self._last_repeatable_event: tuple[str, str] | None = None

    def apply_thread(self, payload: dict[str, Any]) -> ThreadState:
        thread_id = str(payload.get("id") or "")
        state = self.store.get_thread(thread_id) or ThreadState(thread_id=thread_id)
        state.title = str(payload.get("name") or payload.get("preview") or state.title)
        state.cwd = str(payload.get("cwd") or state.cwd)
        state.created_at = int(payload.get("createdAt") or state.created_at)
        state.updated_at = int(payload.get("updatedAt") or state.updated_at)
        status = payload.get("status") or {}
        if isinstance(status, dict):
            state.status = str(status.get("type") or state.status)
            state.active_flags = [str(value) for value in status.get("activeFlags") or []]
        turns = [value for value in payload.get("turns") or [] if isinstance(value, dict)]
        recent_activity = state.recent_activity
        for turn in turns:
            turn_id = str(turn.get("id") or "")
            duration_ms = turn.get("durationMs")
            if turn_id and isinstance(duration_ms, int) and str(turn.get("status")) != "inProgress":
                state.completed_turn_durations_ms[turn_id] = max(0, duration_ms)
        if turns:
            turn = turns[-1]
            state.turn_id = str(turn.get("id") or state.turn_id or "") or None
            state.turn_status = str(turn.get("status") or state.turn_status or "") or None
            state.turn_started_at = turn.get("startedAt") or state.turn_started_at
            duration_ms = turn.get("durationMs")
            state.turn_duration_ms = max(0, duration_ms) if isinstance(duration_ms, int) else 0
            for item in turn.get("items") or []:
                if isinstance(item, dict):
                    self._apply_item(state, item, completed=state.turn_status != "inProgress")
            state.recent_activity = recent_activity
        state.queue_count = self.store.queue_count(thread_id)
        self.store.save_thread(state)
        return state

    async def ingest(self, method: str, params: dict[str, Any]) -> None:
        thread_id = self._thread_id(params)
        if method not in {"thread/tokenUsage/updated"}:
            digest = _canonical_hash(method, params)
            if method in _REPEATABLE_NOTIFICATIONS:
                marker = (method, digest)
                if marker == self._last_repeatable_event:
                    return
                self._last_repeatable_event = marker
                event_key = f"{digest}:{time.time_ns()}"
            else:
                self._last_repeatable_event = None
                event_key = digest
            if not self.store.record_event(event_key, thread_id, method, params):
                return
        if not thread_id:
            return
        if method == "thread/started":
            state = self.apply_thread(dict(params.get("thread") or {}))
            await self.on_change(state, method)
            return
        state = self.store.get_thread(thread_id) or ThreadState(thread_id=thread_id)
        changed = self._apply(state, method, params)
        if not changed:
            return
        state.queue_count = self.store.queue_count(thread_id)
        state.updated_at = int(time.time())
        self.store.save_thread(state)
        await self.on_change(state, method)

    @staticmethod
    def _thread_id(params: dict[str, Any]) -> str | None:
        direct = params.get("threadId")
        if direct:
            return str(direct)
        thread = params.get("thread")
        if isinstance(thread, dict) and thread.get("id"):
            return str(thread["id"])
        return None

    def _apply(self, state: ThreadState, method: str, params: dict[str, Any]) -> bool:
        if method == "thread/name/updated":
            state.title = str(params.get("threadName") or state.title)
            self._record_activity(state, method, f"Session renamed: {state.title}")
            return True
        if method == "thread/status/changed":
            status = params.get("status") or {}
            state.status = str(status.get("type") or state.status)
            state.active_flags = [str(value) for value in status.get("activeFlags") or []]
            state.latest_activity = f"Session {state.status}"
            self._record_activity(state, method, state.latest_activity, state.status)
            return True
        if method in {"thread/closed", "thread/deleted", "thread/archived"}:
            state.status = "notLoaded"
            state.latest_activity = method.split("/")[-1]
            self._record_activity(state, method, state.latest_activity, state.status)
            return True
        if method == "thread/goal/updated":
            state.goal = dict(params.get("goal") or {})
            state.latest_activity = f"Goal: {state.goal.get('status', 'updated')}"
            self._record_activity(
                state, method, state.latest_activity, str(state.goal.get("status") or "updated")
            )
            return True
        if method == "thread/goal/cleared":
            state.goal = None
            state.latest_activity = "Goal 已清除"
            self._record_activity(state, method, state.latest_activity, "cleared")
            return True
        if method == "turn/started":
            turn = params.get("turn") or {}
            state.turn_id = str(turn.get("id") or "") or state.turn_id
            state.turn_status = str(turn.get("status") or "inProgress")
            state.turn_started_at = int(turn.get("startedAt") or time.time())
            state.turn_duration_ms = max(0, int(turn.get("durationMs") or 0))
            state.status = "active"
            state.latest_activity = "Turn 已开始"
            state.last_error = ""
            self._record_activity(state, method, state.latest_activity, state.turn_status)
            return True
        if method == "turn/completed":
            turn = params.get("turn") or {}
            state.turn_id = str(turn.get("id") or state.turn_id or "") or None
            state.turn_status = str(turn.get("status") or "completed")
            duration_ms = turn.get("durationMs")
            if state.turn_id and isinstance(duration_ms, int):
                state.completed_turn_durations_ms[state.turn_id] = max(0, duration_ms)
                state.turn_duration_ms = max(0, duration_ms)
            error = turn.get("error")
            if isinstance(error, dict):
                state.last_error = str(error.get("message") or "Turn failed")
            turn_activity = f"Turn {state.turn_status}"
            state.latest_activity = turn_activity
            for item in turn.get("items") or []:
                if isinstance(item, dict):
                    self._apply_item(state, item, completed=True)
            self._record_activity(state, method, turn_activity, state.turn_status)
            return True
        if method == "turn/plan/updated":
            new_plan = [PlanStep.from_value(value) for value in params.get("plan") or []]
            before = [(step.step, step.status) for step in state.plan]
            after = [(step.step, step.status) for step in new_plan]
            if before != after:
                if [step.step for step in state.plan] != [step.step for step in new_plan]:
                    state.plan_revision += 1
                elif state.plan_revision == 0:
                    state.plan_revision = 1
                state.plan = new_plan
                state.latest_activity = str(params.get("explanation") or "计划已更新")
                status = f"{state.completed_steps}/{len(state.plan)}"
                self._record_activity(state, method, state.latest_activity, status)
                return True
            return False
        if method in {"item/started", "item/completed"}:
            item = params.get("item") or {}
            if isinstance(item, dict):
                self._apply_item(state, item, completed=method == "item/completed")
                return True
        if method == "error":
            error = params.get("error") or {}
            state.last_error = str(error.get("message") or "Codex error")
            state.latest_activity = "正在重试" if params.get("willRetry") else "执行失败"
            status = "retrying" if params.get("willRetry") else "failed"
            self._record_activity(state, method, state.last_error, status)
            return True
        if method in {"warning", "guardianWarning", "deprecationNotice", "configWarning"}:
            state.latest_activity = str(params.get("message") or method)
            self._record_activity(state, method, state.latest_activity, "warning")
            return True
        return False

    def _apply_item(self, state: ThreadState, item: dict[str, Any], *, completed: bool) -> None:
        item_type = str(item.get("type") or "item")
        if item_type == "agentMessage":
            text = " ".join(str(item.get("text") or "").split())
            if text:
                state.last_agent_message = text[:2000]
                state.latest_activity = text[:360]
                self._record_activity(state, item_type, state.latest_activity, "completed")
            return
        if item_type == "commandExecution":
            status = str(item.get("status") or ("completed" if completed else "inProgress"))
            suffix = f" (exit {item.get('exitCode')})" if item.get("exitCode") is not None else ""
            state.latest_activity = f"命令执行 {status}{suffix}"
            self._record_activity(state, item_type, state.latest_activity, status)
            return
        if item_type == "fileChange":
            changes = item.get("changes") or []
            state.latest_activity = f"文件变更 {len(changes)} 项: {item.get('status', '')}".strip()
            self._record_activity(state, item_type, state.latest_activity, str(item.get("status") or ""))
            return
        if item_type in {"mcpToolCall", "dynamicToolCall"}:
            name = item.get("tool") or item.get("server") or "tool"
            state.latest_activity = f"工具 {name}: {item.get('status', 'running')}"
            status = str(item.get("status") or "running")
            self._record_activity(state, item_type, state.latest_activity, status)
            return
        if item_type == "collabAgentToolCall":
            states = item.get("agentsStates") or {}
            tasks = {task.task_id: task for task in state.tasks}
            now = int(time.time())
            prompt = " ".join(str(item.get("prompt") or "").split())[:240]
            for agent_thread_id, value in states.items():
                if not isinstance(value, dict):
                    continue
                task_id = str(agent_thread_id)
                current = tasks.get(task_id)
                raw_status = str(value.get("status") or "pendingInit")
                task_status = self._task_status(raw_status)
                tasks[task_id] = TaskState(
                    task_id=task_id,
                    title=prompt or (current.title if current else f"Agent {task_id[:8]}"),
                    status=task_status,
                    agent_thread_id=task_id,
                    message=str(value.get("message") or (current.message if current else "")),
                    updated_at=now,
                )
            state.tasks = sorted(tasks.values(), key=lambda task: (task.updated_at, task.task_id))[-50:]
            state.agents_active = sum(task.status in {"pending", "inProgress"} for task in state.tasks)
            state.agents_completed = sum(task.status == "completed" for task in state.tasks)
            state.agents_failed = sum(task.status == "failed" for task in state.tasks)
            state.latest_activity = f"Agent task {item.get('tool', '')}: {item.get('status', '')}".strip()
            self._record_activity(
                state, item_type, state.latest_activity, str(item.get("status") or "inProgress")
            )
            return
        if item_type == "subAgentActivity":
            state.latest_activity = f"Subagent {item.get('kind', '')}".strip()
            self._record_activity(state, item_type, state.latest_activity, str(item.get("kind") or ""))
            return
        if item_type == "imageGeneration":
            state.latest_activity = f"图像生成: {item.get('status', '')}".strip()
            self._record_activity(state, item_type, state.latest_activity, str(item.get("status") or ""))
            return
        if item_type == "contextCompaction":
            state.latest_activity = "上下文已压缩"
            self._record_activity(state, item_type, state.latest_activity, "completed")
            return
        state.latest_activity = f"{item_type}: {'completed' if completed else 'started'}"
        self._record_activity(
            state, item_type, state.latest_activity, "completed" if completed else "inProgress"
        )

    @staticmethod
    def _task_status(status: str) -> str:
        if status in {"completed", "shutdown"}:
            return "completed"
        if status in {"errored", "interrupted", "notFound"}:
            return "failed"
        if status == "running":
            return "inProgress"
        return "pending"

    @staticmethod
    def _record_activity(
        state: ThreadState,
        kind: str,
        text: str,
        status: str = "",
        *,
        timestamp: int | None = None,
    ) -> None:
        entry = LifecycleActivity(
            kind=kind,
            text=" ".join(text.split())[:360],
            status=status,
            timestamp=timestamp or int(time.time()),
        )
        if state.recent_activity and state.recent_activity[-1] == entry:
            return
        state.recent_activity = [*state.recent_activity[-19:], entry]
