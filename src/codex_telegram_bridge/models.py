from __future__ import annotations

import hashlib
import time
from dataclasses import asdict, dataclass, field
from typing import Any


def plan_revision_key(turn_id: str, text: str) -> str:
    payload = f"{turn_id.strip()}\0{text.strip()}".encode()
    return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class ModelOption:
    model: str
    display_name: str
    supported_efforts: tuple[str, ...]
    default_effort: str
    is_default: bool = False


@dataclass(frozen=True, slots=True)
class ModelProfile:
    model: str
    effort: str


@dataclass(slots=True)
class InteractionDraft:
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
    claimed_at: int | None
    created_at: int
    updated_at: int


@dataclass(slots=True)
class PlanStep:
    step: str
    status: str = "pending"

    @classmethod
    def from_value(cls, value: dict[str, Any]) -> PlanStep:
        return cls(step=str(value.get("step") or ""), status=str(value.get("status") or "pending"))


@dataclass(slots=True)
class TaskState:
    task_id: str
    title: str
    status: str = "pending"
    agent_thread_id: str | None = None
    agent_path: str = ""
    agent_nickname: str = ""
    agent_role: str = ""
    model: str = ""
    reasoning_effort: str = ""
    message: str = ""
    started_at: int = 0
    finished_at: int = 0
    updated_at: int = 0

    @classmethod
    def from_value(cls, value: dict[str, Any]) -> TaskState:
        return cls(
            task_id=str(value.get("task_id") or ""),
            title=str(value.get("title") or ""),
            status=str(value.get("status") or "pending"),
            agent_thread_id=(
                str(value.get("agent_thread_id") or value.get("agentThreadId"))
                if value.get("agent_thread_id") or value.get("agentThreadId")
                else None
            ),
            agent_path=str(value.get("agent_path") or value.get("agentPath") or ""),
            agent_nickname=str(value.get("agent_nickname") or value.get("agentNickname") or ""),
            agent_role=str(value.get("agent_role") or value.get("agentRole") or ""),
            model=str(value.get("model") or ""),
            reasoning_effort=str(value.get("reasoning_effort") or value.get("reasoningEffort") or ""),
            message=str(value.get("message") or ""),
            started_at=int(value.get("started_at") or value.get("startedAt") or 0),
            finished_at=int(value.get("finished_at") or value.get("finishedAt") or 0),
            updated_at=int(value.get("updated_at") or value.get("updatedAt") or 0),
        )


@dataclass(slots=True)
class LifecycleActivity:
    kind: str
    text: str
    status: str = ""
    timestamp: int = 0

    @classmethod
    def from_value(cls, value: dict[str, Any]) -> LifecycleActivity:
        return cls(
            kind=str(value.get("kind") or "activity"),
            text=str(value.get("text") or ""),
            status=str(value.get("status") or ""),
            timestamp=int(value.get("timestamp") or 0),
        )


@dataclass(slots=True)
class ThreadState:
    thread_id: str
    title: str = "Codex session"
    cwd: str = ""
    session_id: str = ""
    model: str = ""
    reasoning_effort: str = ""
    permissions: str | None = None
    approval_policy: str | dict[str, Any] | None = None
    approvals_reviewer: str | None = None
    sandbox_policy: dict[str, Any] | None = None
    parent_thread_id: str | None = None
    agent_nickname: str = ""
    agent_role: str = ""
    agent_path: str = ""
    ephemeral: bool = False
    status: str = "notLoaded"
    active_flags: list[str] = field(default_factory=list)
    turn_id: str | None = None
    turn_status: str | None = None
    turn_started_at: int | None = None
    goal: dict[str, Any] | None = None
    plan: list[PlanStep] = field(default_factory=list)
    plan_revision: int = 0
    latest_activity: str = ""
    last_agent_message: str = ""
    agents_active: int = 0
    agents_completed: int = 0
    agents_failed: int = 0
    tasks: list[TaskState] = field(default_factory=list)
    recent_activity: list[LifecycleActivity] = field(default_factory=list)
    queue_count: int = 0
    created_at: int = 0
    updated_at: int = field(default_factory=lambda: int(time.time()))
    completed_turn_durations_ms: dict[str, int] = field(default_factory=dict)
    turn_duration_ms: int = 0
    dashboard_message_id: int | None = None
    subscribed: bool = False
    last_error: str = ""
    last_error_recoverable: bool = False

    @property
    def short_id(self) -> str:
        return self.thread_id[:8]

    @property
    def is_subagent(self) -> bool:
        return self.parent_thread_id is not None

    @property
    def completed_steps(self) -> int:
        return sum(step.status == "completed" for step in self.plan)

    @property
    def in_progress_step(self) -> PlanStep | None:
        return next((step for step in self.plan if step.status == "inProgress"), None)

    @property
    def completed_duration_ms(self) -> int:
        return sum(max(0, value) for value in self.completed_turn_durations_ms.values())

    @property
    def current_duration_ms(self) -> int:
        if self.turn_status != "inProgress" or not self.turn_started_at:
            return 0
        elapsed = max(0, int(time.time()) - self.turn_started_at) * 1000
        return max(self.turn_duration_ms, elapsed)

    @property
    def total_duration_ms(self) -> int:
        return self.completed_duration_ms + self.current_duration_ms

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ThreadState:
        data = dict(value)
        data["plan"] = [PlanStep.from_value(item) for item in data.get("plan") or []]
        data["tasks"] = [TaskState.from_value(item) for item in data.get("tasks") or []]
        data["recent_activity"] = [
            LifecycleActivity.from_value(item) for item in data.get("recent_activity") or []
        ]
        return cls(**data)


@dataclass(slots=True)
class SessionSpace:
    space_id: str
    generation: int = 1
    space_type: str = "existing"
    lifecycle: str = "pending"
    thread_id: str | None = None
    channel_chat_id: int | None = None
    channel_post_id: int | None = None
    discussion_chat_id: int | None = None
    discussion_root_id: int | None = None
    status_message_id: int | None = None
    pending_cwd: str = ""
    pending_prompt: str = ""
    normal_model: str = ""
    normal_effort: str = ""
    plan_model: str = ""
    plan_effort: str = ""
    current_mode: str = "default"
    desired_mode: str = ""
    observed_mode: str = "unknown"
    created_at: int = field(default_factory=lambda: int(time.time()))
    updated_at: int = field(default_factory=lambda: int(time.time()))
    last_error: str = ""
    provision_stage: str = ""
    provision_attempts: int = 0
    provision_retry_at: float = 0.0

    @property
    def active(self) -> bool:
        return self.lifecycle == "active" and bool(self.thread_id)

    def __post_init__(self) -> None:
        self.desired_mode = self.desired_mode or self.current_mode
        self.current_mode = self.desired_mode

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> SessionSpace:
        data = dict(value)
        data.setdefault("desired_mode", str(data.get("current_mode") or "default"))
        data.setdefault("current_mode", str(data["desired_mode"] or "default"))
        data.setdefault("observed_mode", "unknown")
        return cls(**data)


@dataclass(frozen=True, slots=True)
class Owner:
    user_id: int
    chat_id: int
    username: str | None = None


@dataclass(frozen=True, slots=True)
class QueuedPrompt:
    queue_id: int
    thread_id: str
    prompt: str
    inputs: list[dict[str, Any]]
    client_message_id: str
    created_at: int
    prompt_intent_id: str | None = None


@dataclass(frozen=True, slots=True)
class PromptIntent:
    intent_id: str
    client_message_id: str
    source: str
    prompt: str
    mode: str
    thread_id: str | None
    space_id: str | None
    generation: int
    state: str
    turn_id: str | None
    queue_id: int | None
    error: str | None
    receipt_key: str | None
    created_at: int
    updated_at: int
