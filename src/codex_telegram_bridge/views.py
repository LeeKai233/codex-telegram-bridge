from __future__ import annotations

import math
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from .markdown import clip, compact_path, escape, inline_code
from .metrics import ascii_bar

TELEGRAM_MESSAGE_LIMIT = 4096
SESSIONS_MESSAGE_BUDGET = 3900
CHANNEL_POST_BUDGET = 1000
STATUS_COMMENT_BUDGET = 3900
SHORT_MESSAGE_BUDGET = 2000
SESSION_PAGE_SIZE = 5

_DETAIL_LABELS = ("①", "②", "③", "④", "⑤")


@dataclass(frozen=True, slots=True)
class RenderedMessage:
    markdown: str
    plain: str


@dataclass(frozen=True, slots=True)
class PageButton:
    label: str
    page: int
    current: bool = False


@dataclass(frozen=True, slots=True)
class SessionDetail:
    label: str
    thread_id: str


@dataclass(frozen=True, slots=True)
class SessionsPageView:
    message: RenderedMessage
    page: int
    total_pages: int
    details: tuple[SessionDetail, ...]
    navigation: tuple[PageButton, ...]


def _value(source: object | None, *names: str, default: Any = None) -> Any:
    if source is None:
        return default
    for name in names:
        if isinstance(source, Mapping) and name in source:
            return source[name]
        if hasattr(source, name):
            return getattr(source, name)
    return default


def _bounded_message(
    markdown: str,
    plain: str,
    *,
    budget: int,
    fallback_markdown: str,
    fallback_plain: str,
) -> RenderedMessage:
    limit = min(TELEGRAM_MESSAGE_LIMIT, max(1, budget))
    if len(markdown) > limit:
        markdown = fallback_markdown
    if len(markdown) > limit:
        markdown = escape("内容过长，请打开状态详情查看。")
    if len(plain) > limit:
        plain = clip(plain, limit)
    if len(plain) > limit:
        plain = plain[:limit]
    return RenderedMessage(markdown=markdown, plain=plain)


def _epoch(value: object | None) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return int(value.timestamp())
    if isinstance(value, int | float):
        numeric = float(value)
        epoch = int(numeric / 1000 if numeric > 10_000_000_000 else numeric)
        return epoch if epoch > 0 else None
    text = str(value).strip()
    try:
        numeric = float(text)
    except ValueError:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return int(parsed.timestamp())
    epoch = int(numeric / 1000 if numeric > 10_000_000_000 else numeric)
    return epoch if epoch > 0 else None


def _clock(value: object | None, *, fallback: str = "N/A") -> str:
    epoch = _epoch(value)
    return fallback if epoch is None else time.strftime("%m-%d %H:%M", time.localtime(epoch))


def _relative(value: object | None, now: int) -> str:
    epoch = _epoch(value)
    if epoch is None:
        return "N/A"
    seconds = max(0, now - epoch)
    if seconds < 60:
        return "now"
    if seconds < 3600:
        return f"{seconds // 60}m ago"
    if seconds < 86_400:
        return f"{seconds // 3600}h ago"
    return f"{seconds // 86_400}d ago"


def _duration(seconds: object | None) -> str:
    try:
        total = max(0, int(float(seconds or 0)))
    except TypeError, ValueError:
        total = 0
    hours, remainder = divmod(total, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _total_duration(state: object, now: int, explicit: int | float | None) -> int:
    if explicit is not None:
        return max(0, int(explicit))
    seconds = _value(state, "total_duration_seconds", "duration_seconds")
    if seconds is not None:
        return max(0, int(float(seconds)))
    milliseconds = _value(state, "total_duration_ms", "duration_ms", "durationMs")
    if milliseconds is not None:
        return max(0, int(float(milliseconds) / 1000))
    started = _epoch(_value(state, "turn_started_at", "started_at", "startedAt"))
    return max(0, now - started) if started is not None else 0


def _thread_id(state: object) -> str:
    return str(_value(state, "thread_id", "threadId", "id", default="") or "")


def _title(state: object) -> str:
    return str(_value(state, "title", "name", "summary", default="Codex session") or "Codex session")


def _cwd(state: object) -> str:
    return compact_path(str(_value(state, "cwd", "directory", default="") or "N/A"))


def _status(state: object, lifecycle: str | None = None) -> tuple[str, str]:
    lifecycle = lifecycle or str(_value(state, "lifecycle", default="") or "")
    raw = str(_value(state, "status", default="notLoaded") or "notLoaded")
    turn = str(_value(state, "turn_status", "turnStatus", default="") or "")
    flags = set(_value(state, "active_flags", "activeFlags", default=()) or ())
    error = str(_value(state, "last_error", "error", default="") or "")
    if lifecycle == "pending":
        return "🟡", "待认证"
    if lifecycle == "closed":
        return "⚫", "已关闭"
    if lifecycle == "repair_required":
        return "🟠", "需要修复"
    if error or raw == "systemError" or turn == "failed":
        return "🔴", "错误"
    if "waitingOnUserInput" in flags:
        return "🟡", "等待回答"
    if "waitingOnApproval" in flags:
        return "🟡", "等待审批"
    if raw == "active" or turn == "inProgress":
        return "🟢", "执行中"
    if raw == "idle" or turn in {"completed", "interrupted"}:
        return "⚪", "空闲"
    return "⚫", "未加载"


def _goal(state: object) -> tuple[str, str, str]:
    goal = _value(state, "goal") or {}
    status = str(_value(goal, "status", default="none") or "none")
    icons = {
        "active": "🟢",
        "paused": "⏸",
        "blocked": "🟠",
        "usageLimited": "🟠",
        "budgetLimited": "🟠",
        "complete": "✅",
    }
    objective = str(_value(goal, "objective", "title", default="未创建 Goal") or "未创建 Goal")
    return icons.get(status, "⚪"), status, objective


def _steps(state: object) -> list[object]:
    plan = _value(state, "plan", default=()) or ()
    return list(plan) if isinstance(plan, Sequence) and not isinstance(plan, str | bytes) else []


def _step_value(step: object) -> tuple[str, str]:
    return (
        str(_value(step, "step", "title", "name", default="") or ""),
        str(_value(step, "status", default="pending") or "pending"),
    )


def _plan_counts(state: object) -> tuple[int, int]:
    steps = _steps(state)
    return sum(_step_value(step)[1] == "completed" for step in steps), len(steps)


def _task_counts(state: object) -> tuple[int, int, int, int]:
    tasks = _value(state, "tasks")
    if tasks and isinstance(tasks, Sequence) and not isinstance(tasks, str | bytes):
        statuses = [str(_value(task, "status", default="pending") or "pending") for task in tasks]
        completed = sum(status in {"completed", "shutdown"} for status in statuses)
        active = sum(status in {"active", "running", "inProgress", "pendingInit"} for status in statuses)
        failed = sum(status in {"failed", "errored", "interrupted", "notFound"} for status in statuses)
        return completed, len(statuses), active, failed
    completed = int(_value(state, "agents_completed", default=0) or 0)
    active = int(_value(state, "agents_active", default=0) or 0)
    failed = int(_value(state, "agents_failed", default=0) or 0)
    return completed, completed + active + failed, active, failed


def _agent_task_counts(state: object) -> tuple[int, int, int, int, int]:
    tasks = _value(state, "tasks")
    if tasks and isinstance(tasks, Sequence) and not isinstance(tasks, str | bytes):
        statuses = [str(_value(task, "status", default="pending") or "pending") for task in tasks]
        active_statuses = {"pending", "pendingInit", "active", "running", "inProgress"}
        failed_statuses = {"failed", "errored", "notFound"}
        interrupted_statuses = {"interrupted"}
        terminal_statuses = {"completed", "shutdown"} | failed_statuses | interrupted_statuses
        return (
            sum(status in terminal_statuses for status in statuses),
            len(statuses),
            sum(status in active_statuses for status in statuses),
            sum(status in failed_statuses for status in statuses),
            sum(status in interrupted_statuses for status in statuses),
        )
    completed = int(_value(state, "agents_completed", default=0) or 0)
    active = int(_value(state, "agents_active", default=0) or 0)
    failed = int(_value(state, "agents_failed", default=0) or 0)
    return completed + failed, completed + active + failed, active, failed, 0


def _queue_count(state: object, explicit: int | None = None) -> int:
    return max(0, int(explicit if explicit is not None else (_value(state, "queue_count", default=0) or 0)))


def pagination_layout(page: int, total_pages: int) -> tuple[PageButton, ...]:
    if total_pages < 1:
        total_pages = 1
    if page < 1 or page > total_pages:
        raise ValueError("page is outside the available range")

    def button(label: str, target: int) -> PageButton:
        return PageButton(label=label, page=target, current=target == page and label.isdecimal())

    if total_pages == 1:
        return (button("1", 1),)
    if page == 1:
        return (button("1", 1), button(">>", 2))
    if page == total_pages:
        return (button("1", 1), button("<<", page - 1), button(str(page), page))
    if page == 2:
        return (button("<<", 1), button("2", 2), button(">>", 3))
    if page == total_pages - 1:
        return (
            button("1", 1),
            button("<<", page - 1),
            button(str(page), page),
            button(str(total_pages), total_pages),
        )
    return (
        button("1", 1),
        button("<<", page - 1),
        button(str(page), page),
        button(">>", page + 1),
    )


def render_sessions_page(
    states: Sequence[object],
    *,
    page: int = 1,
    page_size: int = SESSION_PAGE_SIZE,
    now: int | None = None,
    query: str = "",
) -> SessionsPageView:
    if page_size != SESSION_PAGE_SIZE:
        raise ValueError("sessions pages contain exactly five items")
    now = int(time.time()) if now is None else now
    total_pages = max(1, math.ceil(len(states) / page_size))
    page = min(max(1, page), total_pages)
    start = (page - 1) * page_size
    selected = states[start : start + page_size]
    heading = f"🤖 Codex Sessions · {page}/{total_pages}"
    markdown_lines = [f"*{escape(heading)}*"]
    plain_lines = [heading]
    if query:
        markdown_lines.append(f"搜索 {inline_code(clip(query, 80))}")
        plain_lines.append(f"搜索 {clip(query, 80)}")
    details: list[SessionDetail] = []
    if not selected:
        markdown_lines.extend(["", "当前没有 Codex session。"])
        plain_lines.extend(["", "当前没有 Codex session。"])
    for offset, state in enumerate(selected):
        label = _DETAIL_LABELS[offset]
        thread_id = _thread_id(state)
        icon, _ = _status(state)
        summary = str(
            _value(state, "natural_summary", "summary", "title", "name", default="Codex session")
            or "Codex session"
        )
        created = _value(state, "created_at", "createdAt")
        updated = _value(state, "updated_at", "updatedAt")
        cwd = _cwd(state)
        markdown_lines.extend(
            [
                "",
                f"{label} {icon} {inline_code(thread_id, 80)}",
                f"📝 {escape(clip(summary, 180))}",
                f"🗓 Created {inline_code(_clock(created))} · Updated {inline_code(_relative(updated, now))}",
                f"📁 {inline_code(cwd, 140)}",
            ]
        )
        plain_lines.extend(
            [
                "",
                f"{label} {icon} {thread_id}",
                f"📝 {clip(summary, 180)}",
                f"🗓 Created {_clock(created)} · Updated {_relative(updated, now)}",
                f"📁 {cwd}",
            ]
        )
        details.append(SessionDetail(label=label, thread_id=thread_id))
    fallback_plain = f"{heading}\n内容过长，请缩小搜索范围。"
    message = _bounded_message(
        "\n".join(markdown_lines),
        "\n".join(plain_lines),
        budget=SESSIONS_MESSAGE_BUDGET,
        fallback_markdown=f"*{escape(heading)}*\n内容过长，请缩小搜索范围。",
        fallback_plain=fallback_plain,
    )
    return SessionsPageView(
        message=message,
        page=page,
        total_pages=total_pages,
        details=tuple(details),
        navigation=pagination_layout(page, total_pages),
    )


def render_channel_post(
    state: object,
    *,
    now: int | None = None,
    lifecycle: str | None = None,
    total_duration_seconds: int | float | None = None,
    queue_count: int | None = None,
    heartbeat_seconds: int = 60,
) -> RenderedMessage:
    now = int(time.time()) if now is None else now
    icon, status = _status(state, lifecycle)
    goal_icon, goal_status, _ = _goal(state)
    completed, plan_total = _plan_counts(state)
    tasks_completed, tasks_total, tasks_active, _ = _task_counts(state)
    duration = _duration(_total_duration(state, now, total_duration_seconds))
    queue = _queue_count(state, queue_count)
    title = clip(_title(state), 80)
    thread_id = _thread_id(state) or "Pending"
    cwd = _cwd(state)
    progress = 100.0 * completed / plan_total if plan_total else 0.0
    markdown_lines = [
        f"*🤖 Codex · {escape(title)}*",
        f"{inline_code(thread_id, 80)} · {icon} {escape(status)} · 总执行 {inline_code(duration)}",
        f"🎯 Goal {goal_icon} {inline_code(goal_status)}",
        f"🧭 Plan {inline_code(f'{completed}/{plan_total}')} {inline_code(ascii_bar(progress))}",
        f"🧩 Tasks {inline_code(f'{tasks_completed}/{tasks_total}')} · Active "
        f"{inline_code(tasks_active)} · Queue {inline_code(queue)}",
        f"🕒 更新 {inline_code(time.strftime('%H:%M', time.localtime(now)))} · "
        f"心跳 {inline_code(f'≤{heartbeat_seconds}s')}",
        f"📁 {inline_code(cwd, 140)}",
    ]
    plain_lines = [
        f"🤖 Codex · {title}",
        f"{thread_id} · {icon} {status} · 总执行 {duration}",
        f"🎯 Goal {goal_icon} {goal_status}",
        f"🧭 Plan {completed}/{plan_total} {ascii_bar(progress)}",
        f"🧩 Tasks {tasks_completed}/{tasks_total} · Active {tasks_active} · Queue {queue}",
        f"🕒 更新 {time.strftime('%H:%M', time.localtime(now))} · 心跳 ≤{heartbeat_seconds}s",
        f"📁 {cwd}",
    ]
    fallback_plain = (
        f"Codex · {title}\n{thread_id} · {status}\n"
        f"更新 {time.strftime('%H:%M', time.localtime(now))}"
    )
    return _bounded_message(
        "\n".join(markdown_lines),
        "\n".join(plain_lines),
        budget=CHANNEL_POST_BUDGET,
        fallback_markdown=(
            f"*Codex · {escape(clip(title, 80))}*\n{inline_code(thread_id, 80)} · {escape(status)}\n"
            f"更新 {inline_code(time.strftime('%H:%M', time.localtime(now)))}"
        ),
        fallback_plain=fallback_plain,
    )


def _visible_steps(steps: Sequence[object]) -> tuple[list[tuple[int, object]], int]:
    if len(steps) <= 14:
        return list(enumerate(steps, 1)), 0
    statuses = [_step_value(step)[1] for step in steps]
    priority = [
        index
        for index, status in enumerate(statuses)
        if status in {"inProgress", "failed", "blocked"}
    ][:14]
    selected = set(priority)
    completed = [index for index, status in enumerate(statuses) if status == "completed"][-5:]
    for index in completed:
        if len(selected) >= 14:
            break
        selected.add(index)
    for index, status in enumerate(statuses):
        if len(selected) >= 14:
            break
        if status == "pending":
            selected.add(index)
    ordered_indexes = sorted(selected)
    return [(index + 1, steps[index]) for index in ordered_indexes], len(steps) - len(ordered_indexes)


def _step_lines(index: int, step: object) -> tuple[str, str]:
    text, status = _step_value(step)
    text = clip(text, 180)
    if status == "completed":
        return f"~{index}\\. {escape(text)}~ ✅", f"[x] {index}. {text}"
    if status == "inProgress":
        return f"▶ *{index}\\. {escape(text)}*", f"> {index}. {text}"
    if status == "blocked":
        return f"⏸ {index}\\. {escape(text)}", f"[!] {index}. {text}"
    if status == "failed":
        return f"❌ {index}\\. {escape(text)}", f"[!] {index}. {text}"
    return f"○ {index}\\. {escape(text)}", f"[ ] {index}. {text}"


def _auth_line(source: object, auth_expires_at: object | None, now: int) -> tuple[str, str]:
    expires = _epoch(
        auth_expires_at
        if auth_expires_at is not None
        else _value(source, "auth_expires_at", "totp_expires_at", "unlocked_until")
    )
    if expires is None:
        return "🔒 TOTP 未认证", "TOTP 未认证"
    remaining = expires - now
    if remaining <= 0:
        return "🔒 TOTP 已过期", "TOTP 已过期"
    minutes = max(1, math.ceil(remaining / 60))
    expires_at = time.strftime("%H:%M:%S", time.localtime(expires))
    return (
        f"🔓 TOTP 已认证 · 剩余 {inline_code(f'{minutes} min')} · "
        f"到期 {inline_code(expires_at)}",
        f"TOTP 已认证 · 剩余 {minutes} min · 到期 {expires_at}",
    )


def _agent_status(task: object) -> tuple[str, str, bool]:
    status = str(_value(task, "status", default="pending") or "pending")
    statuses = {
        "pending": ("🟡", "初始化", True),
        "pendingInit": ("🟡", "初始化", True),
        "active": ("🟢", "运行中", True),
        "running": ("🟢", "运行中", True),
        "inProgress": ("🟢", "运行中", True),
        "completed": ("✅", "已完成", False),
        "shutdown": ("⚫", "已关闭", False),
        "interrupted": ("⏸", "已中断", False),
        "notFound": ("❓", "未找到", False),
        "errored": ("❌", "失败", False),
        "failed": ("❌", "失败", False),
    }
    return statuses.get(status, ("⚪", "未知", False))


def _visible_agents(state: object) -> tuple[list[object], int]:
    tasks = _value(state, "tasks", default=()) or ()
    if not isinstance(tasks, Sequence) or isinstance(tasks, str | bytes):
        return [], 0
    active: list[object] = []
    terminal: list[object] = []
    for task in tasks:
        _, _, is_active = _agent_status(task)
        (active if is_active else terminal).append(task)
    active.sort(
        key=lambda task: int(_value(task, "started_at", "updated_at", default=0) or 0)
    )
    terminal.sort(
        key=lambda task: int(
            _value(task, "finished_at", "updated_at", default=0) or 0
        ),
        reverse=True,
    )
    visible = active + terminal[:3]
    return visible, max(0, len(tasks) - len(visible))


def _agent_lines(task: object, now: int) -> tuple[str, str]:
    icon, status, _ = _agent_status(task)
    thread_id = str(_value(task, "agent_thread_id") or _value(task, "task_id") or "")
    path = str(_value(task, "agent_path", default="") or "")
    nickname = str(_value(task, "agent_nickname", default="") or "")
    role = str(_value(task, "agent_role", default="") or "")
    label = clip(nickname or path or thread_id[:8] or "agent", 48)
    short_id = thread_id[:8] or path or "unknown"
    model = str(_value(task, "model", default="") or "")
    effort = str(_value(task, "reasoning_effort", default="") or "")
    model_effort = "/".join(value for value in (model, effort) if value)
    started = _epoch(_value(task, "started_at", "updated_at")) or now
    finished = _epoch(_value(task, "finished_at"))
    elapsed = _duration(max(0, (finished or now) - started))
    title = clip(str(_value(task, "title", default="Agent task") or "Agent task"), 100)
    metadata = [status, short_id]
    if role:
        metadata.append(clip(role, 32))
    if model_effort:
        metadata.append(clip(model_effort, 64))
    metadata.append(elapsed)
    markdown = (
        f"{icon} *{escape(label)}* · "
        + " · ".join(inline_code(value) for value in metadata)
        + f"\n└ {escape(title)}"
    )
    plain = f"{icon} {label} · " + " · ".join(metadata) + f"\n  {title}"
    return markdown, plain


def _main_profile(space: object | None, state: object) -> tuple[str, str, str]:
    mode = str(_value(space, "current_mode", default="") or "")
    if mode not in {"default", "plan"}:
        mode = ""
    if mode == "plan":
        model = str(_value(space, "plan_model", default="") or "")
        effort = str(_value(space, "plan_effort", default="") or "")
    else:
        model = str(_value(space, "normal_model", default="") or "")
        effort = str(_value(space, "normal_effort", default="") or "")
    model = model or str(_value(state, "model", default="") or "")
    effort = effort or str(
        _value(state, "reasoning_effort", "reasoningEffort", default="") or ""
    )
    return mode, model, effort


def render_status_comment(
    state: object,
    *,
    space: object | None = None,
    now: int | None = None,
    lifecycle: str | None = None,
    total_duration_seconds: int | float | None = None,
    queue_count: int | None = None,
    auth_expires_at: object | None = None,
    heartbeat_seconds: int = 60,
    animation_frame: int | None = None,
) -> RenderedMessage:
    now = int(time.time()) if now is None else now
    lifecycle = lifecycle or str(_value(space, "lifecycle", default="") or "") or None
    icon, status = _status(state, lifecycle)
    goal_icon, goal_status, objective = _goal(state)
    steps = _steps(state)
    completed, plan_total = _plan_counts(state)
    tasks_finished, tasks_total, tasks_active, tasks_failed, tasks_interrupted = (
        _agent_task_counts(state)
    )
    queue = _queue_count(state, queue_count)
    duration = _duration(_total_duration(state, now, total_duration_seconds))
    title = clip(_title(state), 80)
    thread_id = _thread_id(state) or str(_value(space, "thread_id", default="Pending") or "Pending")
    current_mode, main_model, main_effort = _main_profile(space, state)
    progress = 100.0 * completed / plan_total if plan_total else 0.0
    markdown_lines: list[str] = []
    plain_lines: list[str] = []
    if current_mode:
        mode_icon, mode_label = ("🧭", "Plan mode") if current_mode == "plan" else ("⚙️", "Normal mode")
        frame = ""
        if animation_frame is not None:
            frame = ("🕛", "🕒", "🕕", "🕘")[animation_frame % 4]
        mode_header = f"{mode_icon} {mode_label}" + (f" · {frame}" if frame else "")
        markdown_lines.extend([f"*{mode_header}*", ""])
        plain_lines.extend([mode_header, ""])
    markdown_lines.extend(
        [
        f"*🤖 Codex · {escape(title)}*",
        f"{inline_code(thread_id, 80)} · {icon} {escape(status)} · 总执行 {inline_code(duration)}",
        *(
            [
                f"*🧠 Main*  {inline_code(main_model or 'N/A', 80)} · "
                f"Effort {inline_code(main_effort or 'N/A', 32)}"
            ]
            if current_mode or main_model or main_effort
            else []
        ),
        "",
        f"*🎯 Goal*  {goal_icon} {inline_code(goal_status)}",
        escape(clip(objective, 320)),
        "",
        f"*🧭 Plan*  {inline_code(f'{completed}/{plan_total}')}  {inline_code(ascii_bar(progress))}",
        ]
    )
    plain_lines.extend(
        [
        f"🤖 Codex · {title}",
        f"{thread_id} · {icon} {status} · 总执行 {duration}",
        *(
            [f"🧠 Main · {main_model or 'N/A'} · Effort {main_effort or 'N/A'}"]
            if current_mode or main_model or main_effort
            else []
        ),
        "",
        f"🎯 Goal · {goal_icon} {goal_status}",
        clip(objective, 320),
        "",
        f"🧭 Plan · {completed}/{plan_total} {ascii_bar(progress)}",
        ]
    )
    visible_steps, hidden = _visible_steps(steps)
    if not visible_steps:
        markdown_lines.append("尚未创建计划")
        plain_lines.append("尚未创建计划")
    for index, step in visible_steps:
        markdown, plain = _step_lines(index, step)
        markdown_lines.append(markdown)
        plain_lines.append(plain)
    if hidden:
        markdown_lines.append(escape(f"… 另有 {hidden} 项，使用 /plan 查看"))
        plain_lines.append(f"... 另有 {hidden} 项，使用 /plan 查看")
    if goal_status == "complete" and plan_total and completed != plan_total:
        remaining = plan_total - completed
        warning = f"Goal 已完成，但 Plan 仍有 {remaining} 项未完成；状态不一致，请先同步 Plan。"
        markdown_lines.append(f"⚠️ {escape(warning)}")
        plain_lines.append(f"WARNING: {warning}")
    if goal_status == "complete" and tasks_active:
        warning = f"Goal 已完成，但仍有 {tasks_active} 个 Subagent 运行中；请先等待或结束任务。"
        markdown_lines.append(f"⚠️ {escape(warning)}")
        plain_lines.append(f"WARNING: {warning}")
    markdown_lines.extend(
        [
            "",
            f"*🧩 Agent Tasks*  {inline_code(f'{tasks_finished}/{tasks_total}')} · Running "
            f"{inline_code(tasks_active)} · Failed {inline_code(tasks_failed)} · Interrupted "
            f"{inline_code(tasks_interrupted)}",
            f"*📥 Queue*  {inline_code(queue)}",
        ]
    )
    plain_lines.extend(
        [
            "",
            f"🧩 Agent Tasks · {tasks_finished}/{tasks_total} · Running {tasks_active} · "
            f"Failed {tasks_failed} · Interrupted {tasks_interrupted}",
            f"📥 Queue · {queue}",
        ]
    )
    visible_agents, hidden_agents = _visible_agents(state)
    if visible_agents:
        markdown_lines.extend(["", "*🤝 Subagents*"])
        plain_lines.extend(["", "🤝 Subagents"])
        for task in visible_agents:
            markdown, plain = _agent_lines(task, now)
            markdown_lines.append(markdown)
            plain_lines.append(plain)
        if hidden_agents:
            markdown_lines.append(escape(f"… 另有 {hidden_agents} 个已结束 Agent"))
            plain_lines.append(f"... 另有 {hidden_agents} 个已结束 Agent")
    latest = str(_value(state, "latest_activity", "activity", default="") or "")
    if latest:
        markdown_lines.append(f"*⚡ 最新*  {escape(clip(latest, 360))}")
        plain_lines.append(f"⚡ 最新 · {clip(latest, 360)}")
    error = str(_value(state, "last_error", "error", default="") or "")
    if error:
        markdown_lines.append(f"*❌ 错误*  {escape(clip(error, 360))}")
        plain_lines.append(f"❌ 错误 · {clip(error, 360)}")
    recent = _value(state, "recent_activity", "timeline", default=()) or ()
    if isinstance(recent, Sequence) and not isinstance(recent, str | bytes):
        visible_recent = list(recent)[-4:]
        if visible_recent:
            markdown_lines.extend(["", "*🕘 近期事件*"])
            plain_lines.extend(["", "🕘 近期事件"])
        for activity in visible_recent:
            activity_text = clip(
                str(_value(activity, "text", "message", "kind", default="activity") or "activity"),
                180,
            )
            activity_status = str(_value(activity, "status", default="") or "")
            timestamp = _epoch(_value(activity, "timestamp", "created_at", "createdAt"))
            clock = time.strftime("%H:%M", time.localtime(timestamp)) if timestamp else "--:--"
            suffix = f" · {activity_status}" if activity_status else ""
            markdown_lines.append(
                f"{inline_code(clock)} {escape(activity_text)}{escape(suffix)}"
            )
            plain_lines.append(f"{clock} {activity_text}{suffix}")
    auth_markdown, auth_plain = _auth_line(space or state, auth_expires_at, now)
    updated = _epoch(_value(state, "updated_at", "updatedAt")) or now
    markdown_lines.extend(
        [
            "",
            auth_markdown,
            f"🕒 更新 {inline_code(time.strftime('%H:%M:%S', time.localtime(updated)))} · "
            f"心跳 {inline_code(f'≤{heartbeat_seconds}s')}",
        ]
    )
    plain_lines.extend(
        [
            "",
            auth_plain,
            f"🕒 更新 {time.strftime('%H:%M:%S', time.localtime(updated))} · 心跳 ≤{heartbeat_seconds}s",
        ]
    )
    fallback_plain = f"Codex · {title}\n{thread_id} · {status}\nPlan {completed}/{plan_total}\n{auth_plain}"
    return _bounded_message(
        "\n".join(markdown_lines),
        "\n".join(plain_lines),
        budget=STATUS_COMMENT_BUDGET,
        fallback_markdown=(
            f"*Codex · {escape(title)}*\n{inline_code(thread_id, 80)} · {escape(status)}\n"
            f"Plan {inline_code(f'{completed}/{plan_total}')}\n{auth_markdown}"
        ),
        fallback_plain=fallback_plain,
    )


def render_pending_space(space: object, *, now: int | None = None) -> RenderedMessage:
    now = int(time.time()) if now is None else now
    title = clip(
        str(_value(space, "title", "session_title", default="New Codex session") or "New Codex session"),
        80,
    )
    cwd = compact_path(str(_value(space, "pending_cwd", "cwd", default="N/A") or "N/A"))
    prompt = clip(
        str(_value(space, "prompt", "pending_prompt", default="等待首个 prompt") or "等待首个 prompt"),
        320,
    )
    markdown = "\n".join(
        [
            f"*🤖 Codex · {escape(title)}*",
            "🟡 Pending · 等待 TOTP 认证",
            f"📁 {inline_code(cwd, 140)}",
            f"📝 {escape(prompt)}",
            "",
            f"在本评论串发送 {inline_code('/totp 123456')} 以创建 session。",
            f"🕒 创建 {inline_code(time.strftime('%H:%M', time.localtime(now)))}",
        ]
    )
    plain = "\n".join(
        [
            f"🤖 Codex · {title}",
            "🟡 Pending · 等待 TOTP 认证",
            f"📁 {cwd}",
            f"📝 {prompt}",
            "",
            "在本评论串发送 /totp 123456 以创建 session。",
            f"🕒 创建 {time.strftime('%H:%M', time.localtime(now))}",
        ]
    )
    return _bounded_message(
        markdown,
        plain,
        budget=SHORT_MESSAGE_BUDGET,
        fallback_markdown="*Codex · Pending*\n等待 TOTP 认证。",
        fallback_plain="Codex · Pending\n等待 TOTP 认证。",
    )


def render_closed_space(
    state: object,
    *,
    closed_at: object | None = None,
    now: int | None = None,
) -> RenderedMessage:
    now = int(time.time()) if now is None else now
    title = clip(_title(state), 80)
    thread_id = _thread_id(state) or "Unknown"
    closed = _epoch(closed_at or _value(state, "closed_at")) or now
    cwd = _cwd(state)
    markdown = "\n".join(
        [
            f"*🤖 Codex · {escape(title)}*",
            f"{inline_code(thread_id, 80)} · ⚫ 已关闭",
            f"📁 {inline_code(cwd, 140)}",
            "",
            "🔒 已取消关注，仅保留历史记录。",
            f"🕒 关闭 {inline_code(time.strftime('%m-%d %H:%M', time.localtime(closed)))}",
        ]
    )
    plain = "\n".join(
        [
            f"🤖 Codex · {title}",
            f"{thread_id} · ⚫ 已关闭",
            f"📁 {cwd}",
            "",
            "🔒 已取消关注，仅保留历史记录。",
            f"🕒 关闭 {time.strftime('%m-%d %H:%M', time.localtime(closed))}",
        ]
    )
    return _bounded_message(
        markdown,
        plain,
        budget=SHORT_MESSAGE_BUDGET,
        fallback_markdown=f"*Codex · {escape(title)}*\n⚫ 已关闭",
        fallback_plain=f"Codex · {title}\n已关闭",
    )


def _escaped_clipped(value: object, *, plain_limit: int, escaped_limit: int) -> tuple[str, str]:
    plain = clip(value, plain_limit)
    markdown = escape(plain)
    if len(markdown) <= escaped_limit:
        return plain, markdown
    upper = len(plain)
    lower = 0
    while lower < upper:
        middle = (lower + upper + 1) // 2
        candidate = plain[:middle].rstrip() + "…"
        if len(escape(candidate)) <= escaped_limit:
            lower = middle
        else:
            upper = middle - 1
    clipped = plain[:lower].rstrip() + "…"
    return clipped, escape(clipped)


def render_ask_waiting(
    question: object,
    ask_id: str,
    *,
    clarification: bool = False,
) -> RenderedMessage:
    question_plain, question_markdown = _escaped_clipped(
        question,
        plain_limit=1800,
        escaped_limit=2200,
    )
    label = "反问 Codex" if clarification else "Ask Codex"
    markdown = "\n".join(
        [
            f"*❓ {escape(label)}* · {inline_code(ask_id, 16)}",
            question_markdown,
            "",
            "⏳ 正在独立回答，不会写入当前 Session…",
        ]
    )
    plain = "\n".join(
        [
            f"❓ {label} · {ask_id}",
            question_plain,
            "",
            "⏳ 正在独立回答，不会写入当前 Session...",
        ]
    )
    return _bounded_message(
        markdown,
        plain,
        budget=STATUS_COMMENT_BUDGET,
        fallback_markdown=f"*❓ {escape(label)}* · {inline_code(ask_id, 16)}\n⏳ 正在独立回答…",
        fallback_plain=f"❓ {label} · {ask_id}\n正在独立回答...",
    )


def render_ask_answer(question: object, answer: object, ask_id: str) -> RenderedMessage:
    question_plain, question_markdown = _escaped_clipped(
        question,
        plain_limit=500,
        escaped_limit=600,
    )
    answer_plain, answer_markdown = _escaped_clipped(
        answer,
        plain_limit=3600,
        escaped_limit=3150,
    )
    markdown = "\n".join(
        [
            f"*💬 Codex 回答* · {inline_code(ask_id, 16)}",
            f"❓ {question_markdown}",
            "",
            answer_markdown,
        ]
    )
    plain = "\n".join(
        [
            f"💬 Codex 回答 · {ask_id}",
            f"❓ {question_plain}",
            "",
            answer_plain,
        ]
    )
    return _bounded_message(
        markdown,
        plain,
        budget=STATUS_COMMENT_BUDGET,
        fallback_markdown=f"*💬 Codex 回答* · {inline_code(ask_id, 16)}\n{answer_markdown}",
        fallback_plain=f"💬 Codex 回答 · {ask_id}\n{answer_plain}",
    )


def render_ask_error(ask_id: str, error: object) -> RenderedMessage:
    error_plain, error_markdown = _escaped_clipped(
        error,
        plain_limit=800,
        escaped_limit=1200,
    )
    markdown = f"*⚠️ Ask 失败* · {inline_code(ask_id, 16)}\n{error_markdown}"
    plain = f"⚠ Ask 失败 · {ask_id}\n{error_plain}"
    return _bounded_message(
        markdown,
        plain,
        budget=SHORT_MESSAGE_BUDGET,
        fallback_markdown=f"*⚠️ Ask 失败* · {inline_code(ask_id, 16)}",
        fallback_plain=f"Ask 失败 · {ask_id}",
    )


def render_help(
    role: Literal["9527", "426", "controller", "session"],
    *,
    label: str | None = None,
    paired: bool = True,
    bound: bool = True,
    in_session_thread: bool = True,
) -> RenderedMessage:
    controller = role in {"9527", "controller"}
    bot_label = label if label is not None else ("Control Bot" if controller else "Discussion Bot")
    if controller and not paired:
        commands = [("/pair", "完成 owner 配对"), ("/help", "显示帮助")]
        title = f"🤖 {bot_label}"
    elif controller:
        commands = [
            ("/sessions [关键词]", "查找 Codex sessions"),
            ("/topics", "查看 Session 帖子"),
            ("/new [model | effort | ...]", "交互或参数化创建 Session"),
            ("/perf", "动态查看 30 秒 WSL 性能"),
            ("/help", "显示帮助"),
        ]
        title = f"🤖 {bot_label}"
    elif not bound:
        commands = [("/bind <code>", "绑定讨论组"), ("/help", "显示帮助")]
        title = f"🤖 {bot_label}"
    elif not in_session_thread:
        commands = [("/help", "显示帮助")]
        title = f"🤖 {bot_label} · 评论串命令"
    else:
        commands = [
            ("/status", "查看实时状态"),
            ("/totp <code>", "认证当前 session"),
            ("/lock", "锁定当前 session"),
            ("/prompt <text>", "发送 prompt"),
            ("/ask <question>", "独立提问，不影响当前任务"),
            ("/queue [text]", "查看或加入队列"),
            ("/planmode [model | effort | prompt]", "切入 Plan Mode"),
            ("/changemodel [model | effort]", "切换当前模式配置"),
            ("/plan", "查看完整计划"),
            ("/timeline", "查看近期事件"),
            ("/attach", "查看上传文件"),
            ("/getfile <描述>", "获取本机文件"),
            ("/unwatch", "取消关注"),
            ("/help", "显示帮助"),
        ]
        title = f"🤖 {bot_label} · Session"
    markdown_lines = [f"*{escape(title)}*"]
    plain_lines = [title]
    for command, description in commands:
        markdown_lines.append(f"{inline_code(command)}  {escape(description)}")
        plain_lines.append(f"{command}  {description}")
    return _bounded_message(
        "\n".join(markdown_lines),
        "\n".join(plain_lines),
        budget=SHORT_MESSAGE_BUDGET,
        fallback_markdown=f"*{escape(title)}*\n{inline_code('/help')}  显示帮助",
        fallback_plain=f"{title}\n/help  显示帮助",
    )
