from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Awaitable, Callable
from typing import Any

from .models import LifecycleActivity, PlanStep, TaskState, ThreadState
from .store import Store

ChangeHandler = Callable[[ThreadState, str], Awaitable[None]]
ManagedEventPredicate = Callable[[str, dict[str, Any]], bool]


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

_ACTIVE_AGENT_TASK_STATUSES = {"pending", "pendingInit", "active", "running", "inProgress"}
_TERMINAL_AGENT_TASK_STATUSES = {
    "completed",
    "shutdown",
    "failed",
    "errored",
    "interrupted",
    "notFound",
}
_CHILD_THREAD_TASK_STATUS = {
    "active": "inProgress",
    "idle": "completed",
    "notLoaded": "shutdown",
    "systemError": "failed",
}


class EventProjector:
    def __init__(
        self,
        store: Store,
        on_change: ChangeHandler = _noop_change,
        is_managed: ManagedEventPredicate | None = None,
    ) -> None:
        self.store = store
        self.on_change = on_change
        self.is_managed = is_managed or (
            lambda _method, params: store.is_managed_thread(self._thread_id(params))
        )
        self._last_repeatable_event: tuple[str, str] | None = None
        self._parent_changes: dict[str, ThreadState] = {}

    def take_parent_changes(self) -> list[ThreadState]:
        changes = list(self._parent_changes.values())
        self._parent_changes.clear()
        return changes

    def apply_thread(self, payload: dict[str, Any]) -> ThreadState:
        thread_id = str(payload.get("id") or "")
        persisted = self.store.get_thread(thread_id)
        state = persisted or ThreadState(thread_id=thread_id)
        incoming_updated_at = int(payload.get("updatedAt") or 0)
        stale_payload = bool(
            persisted is not None
            and incoming_updated_at
            and persisted.updated_at
            and incoming_updated_at < persisted.updated_at
        )
        state.title = str(payload.get("name") or payload.get("preview") or state.title)
        state.cwd = str(payload.get("cwd") or state.cwd)
        state.session_id = str(payload.get("sessionId") or state.session_id)
        state.model = str(payload.get("model") or state.model)
        state.reasoning_effort = str(
            payload.get("reasoningEffort")
            or payload.get("reasoning_effort")
            or state.reasoning_effort
        )
        if "parentThreadId" in payload:
            state.parent_thread_id = str(payload.get("parentThreadId") or "") or None
        state.agent_nickname = str(payload.get("agentNickname") or state.agent_nickname)
        state.agent_role = str(payload.get("agentRole") or state.agent_role)
        if "ephemeral" in payload:
            state.ephemeral = bool(payload.get("ephemeral"))
        self._apply_subagent_source(state, payload.get("source"))
        state.created_at = int(payload.get("createdAt") or state.created_at)
        if not stale_payload:
            state.updated_at = incoming_updated_at or state.updated_at
            self._apply_security_settings(state, payload)
            status = payload.get("status") or {}
            if isinstance(status, dict):
                state.status = str(status.get("type") or state.status)
                state.active_flags = [str(value) for value in status.get("activeFlags") or []]
                if state.status in {"active", "idle"} and self._clear_recoverable_error(state):
                    state.latest_activity = "连接已恢复"
                    self._record_activity(
                        state,
                        "thread/resynced",
                        state.latest_activity,
                        "recovered",
                        timestamp=incoming_updated_at or int(time.time()),
                    )
        turns = (
            []
            if stale_payload
            else [value for value in payload.get("turns") or [] if isinstance(value, dict)]
        )
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
                    self._apply_item(
                        state,
                        item,
                        completed=state.turn_status != "inProgress",
                        historical=True,
                    )
            state.recent_activity = recent_activity
        self._reconcile_tasks_from_children(state)
        state.queue_count = self.store.queue_count(thread_id)
        self.store.save_thread(state)
        self._sync_parent_agent_metadata(state)
        return state

    def apply_goal(self, state: ThreadState, goal: object) -> ThreadState:
        incoming = dict(goal) if isinstance(goal, dict) else None
        if incoming is not None and self._goal_update_is_stale(state.goal, incoming):
            return state
        state.goal = incoming
        if state.goal and str(state.goal.get("status") or "") == "complete":
            self._finalize_active_tasks(state)
        self.store.save_thread(state)
        self._sync_parent_agent_metadata(state)
        return state

    def _apply_goal_notification(
        self,
        state: ThreadState,
        params: dict[str, Any],
    ) -> tuple[bool, bool]:
        incoming = dict(params.get("goal") or {})
        if self._goal_update_is_stale(state.goal, incoming):
            return False, False
        current = dict(state.goal or {})
        before_visible = self._goal_visible_key(current)
        before_tasks = [(task.task_id, task.status) for task in state.tasks]
        state.goal = incoming
        if str(incoming.get("status") or "") == "complete":
            self._finalize_active_tasks(state)
        after_tasks = [(task.task_id, task.status) for task in state.tasks]
        persisted = current != incoming or before_tasks != after_tasks
        visible = before_visible != self._goal_visible_key(incoming) or before_tasks != after_tasks
        if visible:
            state.latest_activity = f"Goal: {incoming.get('status', 'updated')}"
            self._record_activity(
                state,
                "thread/goal/updated",
                state.latest_activity,
                str(incoming.get("status") or "updated"),
            )
        return persisted, visible

    @staticmethod
    def _goal_visible_key(goal: dict[str, Any]) -> tuple[str, str]:
        return (
            str(goal.get("status") or ""),
            str(goal.get("objective") or ""),
        )

    @staticmethod
    def _goal_update_is_stale(current: dict[str, Any] | None, incoming: dict[str, Any]) -> bool:
        if not current:
            return False
        current_updated = int(current.get("updatedAt") or 0)
        incoming_updated = int(incoming.get("updatedAt") or 0)
        if current_updated and incoming_updated and incoming_updated < current_updated:
            return True
        return bool(
            current_updated
            and incoming_updated == current_updated
            and str(current.get("status") or "") == "complete"
            and str(incoming.get("status") or "") != "complete"
        )

    @staticmethod
    def _apply_subagent_source(state: ThreadState, source: object) -> None:
        if not isinstance(source, dict):
            return
        subagent = source.get("subAgent") or source.get("subagent")
        if not isinstance(subagent, dict):
            return
        spawned = subagent.get("threadSpawn") or subagent.get("thread_spawn")
        if not isinstance(spawned, dict):
            return
        state.parent_thread_id = str(
            spawned.get("parentThreadId")
            or spawned.get("parent_thread_id")
            or state.parent_thread_id
            or ""
        ) or None
        state.agent_path = str(
            spawned.get("agentPath") or spawned.get("agent_path") or state.agent_path
        )
        state.agent_nickname = str(
            spawned.get("agentNickname")
            or spawned.get("agent_nickname")
            or state.agent_nickname
        )
        state.agent_role = str(
            spawned.get("agentRole") or spawned.get("agent_role") or state.agent_role
        )

    @staticmethod
    def _apply_security_settings(state: ThreadState, values: dict[str, Any]) -> None:
        if "activePermissionProfile" in values:
            profile = values.get("activePermissionProfile")
            profile_id = profile.get("id") if isinstance(profile, dict) else None
            if profile_id:
                state.permissions = str(profile_id).strip()
            elif "permissions" in values:
                permissions = values.get("permissions")
                state.permissions = str(permissions).strip() if permissions else None
            else:
                state.permissions = None
        elif "permissions" in values:
            permissions = values.get("permissions")
            state.permissions = str(permissions).strip() if permissions else None

        if "approvalPolicy" in values:
            policy = values.get("approvalPolicy")
            if policy is None or isinstance(policy, (str, dict)):
                state.approval_policy = policy
        if "approvalsReviewer" in values:
            reviewer = values.get("approvalsReviewer")
            state.approvals_reviewer = str(reviewer).strip() if reviewer else None
        if "sandboxPolicy" in values:
            policy = values.get("sandboxPolicy")
            if isinstance(policy, dict):
                state.sandbox_policy = dict(policy)
            elif "sandbox" not in values:
                state.sandbox_policy = None
        if "sandbox" in values and not isinstance(values.get("sandboxPolicy"), dict):
            policy = values.get("sandbox")
            if isinstance(policy, dict):
                state.sandbox_policy = dict(policy)

    def _sync_parent_agent_metadata(self, state: ThreadState) -> ThreadState | None:
        if not state.parent_thread_id:
            return None
        parent = self.store.get_thread(state.parent_thread_id)
        if parent is None:
            return None
        changed = False
        for task in parent.tasks:
            if task.agent_thread_id != state.thread_id and task.task_id != state.thread_id:
                continue
            before = (
                task.agent_path,
                task.agent_nickname,
                task.agent_role,
                task.model,
                task.reasoning_effort,
                task.status,
                task.started_at,
                task.finished_at,
                task.updated_at,
            )
            task.agent_path = state.agent_path or task.agent_path
            task.agent_nickname = state.agent_nickname or task.agent_nickname
            task.agent_role = state.agent_role or task.agent_role
            task.model = state.model or task.model
            task.reasoning_effort = state.reasoning_effort or task.reasoning_effort
            self._reconcile_task_from_child(task, state)
            changed = before != (
                task.agent_path,
                task.agent_nickname,
                task.agent_role,
                task.model,
                task.reasoning_effort,
                task.status,
                task.started_at,
                task.finished_at,
                task.updated_at,
            )
            break
        if changed:
            self._refresh_agent_counts(parent)
            self.store.save_thread(parent)
            self._parent_changes[parent.thread_id] = parent
            return parent
        return None

    def _reconcile_tasks_from_children(self, state: ThreadState) -> bool:
        changed = False
        for task in state.tasks:
            child = self.store.get_thread(task.agent_thread_id or task.task_id)
            if child is not None:
                changed = self._reconcile_task_from_child(task, child) or changed
        if changed:
            self._refresh_agent_counts(state)
        return changed

    @staticmethod
    def _reconcile_task_from_child(task: TaskState, child: ThreadState) -> bool:
        projected = _CHILD_THREAD_TASK_STATUS.get(child.status)
        if projected is None:
            return False
        if (
            projected in _ACTIVE_AGENT_TASK_STATUSES
            and task.status in _TERMINAL_AGENT_TASK_STATUSES
            and task.finished_at
            and child.updated_at
            and child.updated_at <= task.finished_at
        ):
            return False
        if projected in {"completed", "shutdown"} and task.status in {
            "completed",
            "failed",
            "interrupted",
            "notFound",
        }:
            projected = task.status
        now = int(time.time())
        before = (
            task.model,
            task.reasoning_effort,
            task.status,
            task.started_at,
            task.finished_at,
            task.updated_at,
        )
        task.model = child.model or task.model
        task.reasoning_effort = child.reasoning_effort or task.reasoning_effort
        if child.created_at and (not task.started_at or child.created_at < task.started_at):
            task.started_at = child.created_at
        task.status = projected
        if projected in _ACTIVE_AGENT_TASK_STATUSES:
            task.finished_at = 0
        else:
            task.finished_at = task.finished_at or child.updated_at or now
        after = (
            task.model,
            task.reasoning_effort,
            task.status,
            task.started_at,
            task.finished_at,
            task.updated_at,
        )
        if before != after:
            task.updated_at = max(task.updated_at, child.updated_at or now)
        return before != (
            task.model,
            task.reasoning_effort,
            task.status,
            task.started_at,
            task.finished_at,
            task.updated_at,
        )

    @staticmethod
    def _refresh_agent_counts(state: ThreadState) -> None:
        state.agents_active = sum(
            task.status in _ACTIVE_AGENT_TASK_STATUSES for task in state.tasks
        )
        state.agents_completed = sum(
            task.status in {"completed", "shutdown"} for task in state.tasks
        )
        state.agents_failed = sum(
            task.status in {"failed", "errored", "interrupted", "notFound"}
            for task in state.tasks
        )

    def _finalize_active_tasks(self, state: ThreadState) -> None:
        now = int(time.time())
        for task in state.tasks:
            if task.status not in _ACTIVE_AGENT_TASK_STATUSES:
                continue
            child = self.store.get_thread(task.agent_thread_id or task.task_id)
            if child is not None:
                self._reconcile_task_from_child(task, child)
                if child.status == "active" or task.status not in _ACTIVE_AGENT_TASK_STATUSES:
                    continue
            task.status = "shutdown"
            task.finished_at = task.finished_at or now
            task.updated_at = max(task.updated_at, now)
        self._refresh_agent_counts(state)

    async def ingest(self, method: str, params: dict[str, Any]) -> None:
        thread_id = self._thread_id(params)
        if not thread_id or not self.is_managed(method, params):
            return
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
            if not self.store.record_event(event_key, thread_id, method, params, managed=True):
                return
        if method == "thread/started":
            state = self.apply_thread(dict(params.get("thread") or {}))
            await self.on_change(state, method)
            await self._notify_parent_changes("subagent/updated")
            return
        if method == "thread/settings/updated":
            self._sync_space_settings(thread_id, params)
        state = self.store.get_thread(thread_id) or ThreadState(thread_id=thread_id)
        notify = True
        if method == "thread/goal/updated":
            changed, notify = self._apply_goal_notification(state, params)
        else:
            changed = self._apply(state, method, params)
        if not changed:
            return
        state.queue_count = self.store.queue_count(thread_id)
        state.updated_at = int(time.time())
        self.store.save_thread(state)
        self._sync_parent_agent_metadata(state)
        if notify:
            await self.on_change(state, method)
        await self._notify_parent_changes("subagent/updated")

    async def _notify_parent_changes(self, reason: str) -> None:
        for parent in self.take_parent_changes():
            await self.on_change(parent, reason)

    def _sync_space_settings(self, thread_id: str, params: dict[str, Any]) -> None:
        thread_settings = params.get("threadSettings")
        if not isinstance(thread_settings, dict):
            return
        collaboration = thread_settings.get("collaborationMode")
        if not isinstance(collaboration, dict):
            return
        mode = str(collaboration.get("mode") or "")
        settings = collaboration.get("settings")
        if mode not in {"default", "plan"} or not isinstance(settings, dict):
            return
        model = str(settings.get("model") or "").strip()
        effort = str(
            settings.get("reasoning_effort") or thread_settings.get("effort") or ""
        ).strip()
        for raw in self.store.list_spaces():
            if (
                str(raw.get("thread_id") or "") != thread_id
                or raw.get("lifecycle") == "closed"
            ):
                continue
            space = self.store.get_session_space(str(raw["space_id"]))
            if space is None:
                continue
            space.current_mode = mode
            if model and effort:
                if mode == "plan":
                    space.plan_model = model
                    space.plan_effort = effort
                else:
                    space.normal_model = model
                    space.normal_effort = effort
            self.store.save_session_space(space)

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
            recovered = state.status in {"active", "idle"} and self._clear_recoverable_error(state)
            state.latest_activity = "连接已恢复" if recovered else f"Session {state.status}"
            self._record_activity(
                state,
                method,
                state.latest_activity,
                "recovered" if recovered else state.status,
            )
            return True
        if method == "thread/settings/updated":
            settings = params.get("threadSettings")
            if isinstance(settings, dict):
                self._apply_security_settings(state, settings)
            state.latest_activity = "Session settings updated"
            self._record_activity(state, method, state.latest_activity, "updated")
            return True
        if method in {"thread/closed", "thread/deleted", "thread/archived"}:
            state.status = "notLoaded"
            state.latest_activity = method.split("/")[-1]
            self._record_activity(state, method, state.latest_activity, state.status)
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
            self._clear_recoverable_error(state)
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
                self._finish_recoverable_error(state)
                state.last_error = str(error.get("message") or "Turn failed")
            elif state.turn_status == "completed":
                self._clear_recoverable_error(state)
                state.last_error = ""
                state.last_error_recoverable = False
            elif state.turn_status in {"failed", "interrupted"}:
                self._finish_recoverable_error(state)
            turn_activity = f"Turn {state.turn_status}"
            state.latest_activity = turn_activity
            for item in turn.get("items") or []:
                if isinstance(item, dict):
                    self._apply_item(state, item, completed=True, historical=True)
            self._reconcile_tasks_from_children(state)
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
            state.last_error_recoverable = bool(params.get("willRetry"))
            state.latest_activity = "正在重试" if state.last_error_recoverable else "执行失败"
            status = "retrying" if state.last_error_recoverable else "failed"
            self._record_activity(state, method, state.last_error, status)
            return True
        if method in {"warning", "guardianWarning", "deprecationNotice", "configWarning"}:
            state.latest_activity = str(params.get("message") or method)
            self._record_activity(state, method, state.latest_activity, "warning")
            return True
        return False

    @staticmethod
    def _clear_recoverable_error(state: ThreadState) -> bool:
        latest_error = next(
            (
                activity
                for activity in reversed(state.recent_activity)
                if activity.kind == "error"
            ),
            None,
        )
        has_retrying_activity = latest_error is not None and latest_error.status == "retrying"
        terminal_error_supersedes = latest_error is not None and latest_error.status not in {
            "",
            "retrying",
        }
        recoverable = (
            not terminal_error_supersedes
            and (
                state.last_error_recoverable
                or "reconnecting" in state.last_error.casefold()
                or has_retrying_activity
            )
        )
        if not recoverable:
            return False
        state.last_error = ""
        state.last_error_recoverable = False
        state.recent_activity = [
            activity
            for activity in state.recent_activity
            if not (activity.kind == "error" and activity.status == "retrying")
        ]
        return True

    @staticmethod
    def _finish_recoverable_error(state: ThreadState) -> bool:
        has_retrying_activity = any(
            activity.kind == "error" and activity.status == "retrying"
            for activity in state.recent_activity
        )
        if not state.last_error_recoverable and not has_retrying_activity:
            return False
        state.last_error_recoverable = False
        state.recent_activity = [
            activity
            for activity in state.recent_activity
            if not (activity.kind == "error" and activity.status == "retrying")
        ]
        return True

    def _apply_item(
        self,
        state: ThreadState,
        item: dict[str, Any],
        *,
        completed: bool,
        historical: bool = False,
    ) -> None:
        item_type = str(item.get("type") or "item")
        if item_type == "agentMessage":
            text = " ".join(str(item.get("text") or "").split())
            if text:
                state.last_agent_message = text[:2000]
                state.latest_activity = text[:360]
                self._record_activity(state, item_type, state.latest_activity, "completed")
            return
        if item_type == "plan":
            state.latest_activity = "Plan 已完成" if completed else "Plan 正在生成"
            self._record_activity(
                state,
                item_type,
                state.latest_activity,
                "completed" if completed else "inProgress",
            )
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
            prompt = " ".join(str(item.get("prompt") or "").split())[:160]
            tool = str(item.get("tool") or "")
            receivers = {
                str(value) for value in (item.get("receiverThreadIds") or []) if value
            }
            spawned_model = str(item.get("model") or "")
            spawned_effort = str(item.get("reasoningEffort") or "")
            for agent_thread_id, value in states.items():
                if not isinstance(value, dict):
                    continue
                task_id = str(agent_thread_id)
                current = tasks.get(task_id)
                child_state = self.store.get_thread(task_id)
                raw_status = str(value.get("status") or "pendingInit")
                task_status = self._task_status(raw_status)
                if (
                    historical
                    and current is not None
                    and current.status in _TERMINAL_AGENT_TASK_STATUSES
                    and task_status in _ACTIVE_AGENT_TASK_STATUSES
                ):
                    task_status = current.status
                started_at = current.started_at if current else 0
                if not started_at and task_status in {"pending", "inProgress"}:
                    started_at = now
                finished_at = current.finished_at if current else 0
                if task_status in {"completed", "failed", "interrupted", "shutdown", "notFound"}:
                    finished_at = finished_at or now
                elif task_status in {"pending", "inProgress"}:
                    finished_at = 0
                use_prompt = prompt and (tool in {"spawnAgent", "spawn_agent"} or current is None)
                is_spawn_receiver = (
                    tool in {"spawnAgent", "spawn_agent"} and task_id in receivers
                )
                tasks[task_id] = TaskState(
                    task_id=task_id,
                    title=prompt if use_prompt else (current.title if current else f"Agent {task_id[:8]}"),
                    status=task_status,
                    agent_thread_id=task_id,
                    agent_path=(
                        (child_state.agent_path if child_state else "")
                        or (current.agent_path if current else "")
                    ),
                    agent_nickname=(
                        (child_state.agent_nickname if child_state else "")
                        or (current.agent_nickname if current else "")
                    ),
                    agent_role=(
                        (child_state.agent_role if child_state else "")
                        or (current.agent_role if current else "")
                    ),
                    model=(
                        (child_state.model if child_state else "")
                        or (spawned_model if is_spawn_receiver else "")
                        or (current.model if current else "")
                    ),
                    reasoning_effort=(
                        (child_state.reasoning_effort if child_state else "")
                        or (spawned_effort if is_spawn_receiver else "")
                        or (current.reasoning_effort if current else "")
                    ),
                    message=str(value.get("message") or (current.message if current else "")),
                    started_at=started_at,
                    finished_at=finished_at,
                    updated_at=now,
                )
            state.tasks = sorted(tasks.values(), key=lambda task: (task.updated_at, task.task_id))[-50:]
            self._refresh_agent_counts(state)
            state.latest_activity = f"Agent task {item.get('tool', '')}: {item.get('status', '')}".strip()
            self._record_activity(
                state, item_type, state.latest_activity, str(item.get("status") or "inProgress")
            )
            return
        if item_type == "subAgentActivity":
            agent_thread_id = str(item.get("agentThreadId") or "")
            agent_path = " ".join(str(item.get("agentPath") or "").split())[:120]
            kind = str(item.get("kind") or "")
            now = int(time.time())
            tasks = {task.task_id: task for task in state.tasks}
            current = tasks.get(agent_thread_id)
            if agent_thread_id:
                if kind == "interrupted":
                    task_status = "interrupted"
                elif kind in {"started", "interacted"}:
                    task_status = (
                        current.status
                        if historical
                        and current is not None
                        and current.status in _TERMINAL_AGENT_TASK_STATUSES
                        else "inProgress"
                    )
                else:
                    task_status = current.status if current else "pending"
                finished_at = current.finished_at if current else 0
                finished_at = (
                    finished_at or now
                    if task_status in _TERMINAL_AGENT_TASK_STATUSES
                    else 0
                )
                tasks[agent_thread_id] = TaskState(
                    task_id=agent_thread_id,
                    title=current.title if current else f"Agent {agent_path or agent_thread_id[:8]}",
                    status=task_status,
                    agent_thread_id=agent_thread_id,
                    agent_path=agent_path or (current.agent_path if current else ""),
                    agent_nickname=current.agent_nickname if current else "",
                    agent_role=current.agent_role if current else "",
                    model=current.model if current else "",
                    reasoning_effort=current.reasoning_effort if current else "",
                    message=current.message if current else "",
                    started_at=(current.started_at if current else 0) or now,
                    finished_at=finished_at,
                    updated_at=now,
                )
                state.tasks = sorted(
                    tasks.values(), key=lambda task: (task.updated_at, task.task_id)
                )[-50:]
                self._refresh_agent_counts(state)
            state.latest_activity = f"Subagent {kind}".strip()
            self._record_activity(state, item_type, state.latest_activity, kind)
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
        if status == "completed":
            return "completed"
        if status == "shutdown":
            return "shutdown"
        if status == "interrupted":
            return "interrupted"
        if status == "notFound":
            return "notFound"
        if status == "errored":
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
