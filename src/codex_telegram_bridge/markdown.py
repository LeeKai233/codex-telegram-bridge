from __future__ import annotations

import time
from pathlib import Path

from telegram.helpers import escape_markdown

from .models import PlanStep, ThreadState

MAX_MESSAGE_LENGTH = 4096


def escape(value: object) -> str:
    return escape_markdown(str(value), version=2)


def inline_code(value: object, limit: int | None = None) -> str:
    text = str(value)
    if limit and len(text) > limit:
        text = text[: max(1, limit - 1)] + "…"
    return f"`{escape_markdown(text, version=2, entity_type='code')}`"


def clip(value: object, limit: int) -> str:
    text = " ".join(str(value or "").split())
    return text if len(text) <= limit else text[: max(1, limit - 1)].rstrip() + "…"


def compact_path(value: str) -> str:
    home = str(Path.home())
    return "~" + value[len(home) :] if value.startswith(home) else value


def _duration(state: ThreadState, now: int) -> str:
    if not state.turn_started_at:
        return "00:00:00"
    seconds = max(0, now - state.turn_started_at)
    hours, rest = divmod(seconds, 3600)
    minutes, seconds = divmod(rest, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _thread_status(state: ThreadState) -> tuple[str, str]:
    if state.last_error or state.status == "systemError":
        return "🔴", "错误"
    if "waitingOnUserInput" in state.active_flags:
        return "🟡", "等待回答"
    if "waitingOnApproval" in state.active_flags:
        return "🟡", "等待审批"
    if state.status == "active" or state.turn_status == "inProgress":
        return "🟢", "执行中"
    if state.status == "idle":
        return "⚪", "空闲"
    return "⚫", "未加载"


def _goal_status(goal: dict[str, object] | None) -> tuple[str, str]:
    status = str((goal or {}).get("status") or "none")
    icons = {
        "active": "🟢",
        "paused": "⏸",
        "blocked": "🟠",
        "usageLimited": "🟠",
        "budgetLimited": "🟠",
        "complete": "✅",
    }
    return icons.get(status, "⚪"), status


def _step_line(index: int, step: PlanStep) -> str:
    label = escape(clip(step.step, 180))
    if step.status == "completed":
        return f"~{index}\\. {label}~ ✅"
    if step.status == "inProgress":
        return f"▶ *{index}\\. {label}*"
    if step.status == "blocked":
        return f"⏸ {index}\\. {label}"
    if step.status == "failed":
        return f"❌ {index}\\. {label}"
    return f"○ {index}\\. {label}"


def _visible_steps(plan: list[PlanStep]) -> tuple[list[tuple[int, PlanStep]], int]:
    if len(plan) <= 12:
        return list(enumerate(plan, 1)), 0
    active = next((index for index, step in enumerate(plan) if step.status == "inProgress"), None)
    completed = [index for index, step in enumerate(plan) if step.status == "completed"][-6:]
    pending = [index for index, step in enumerate(plan) if step.status != "completed"][:5]
    selected = set(completed + pending)
    if active is not None:
        selected.add(active)
    ordered = [(index + 1, plan[index]) for index in sorted(selected)]
    return ordered, len(plan) - len(ordered)


def render_dashboard(state: ThreadState, now: int | None = None) -> str:
    now = now or int(time.time())
    icon, status = _thread_status(state)
    goal_icon, goal_status = _goal_status(state.goal)
    title = escape(clip(state.title, 80))
    lines = [
        f"*Codex · {title}*",
        f"{inline_code(state.short_id)} · {icon} {escape(status)} · {inline_code(_duration(state, now))}",
        inline_code(compact_path(state.cwd), 110),
        "",
        f"*Goal*  {goal_icon} {escape(goal_status)}",
    ]
    objective = clip((state.goal or {}).get("objective") or "未创建 Goal", 320)
    lines.append(escape(objective))
    if state.plan:
        completed = state.completed_steps
        total = len(state.plan)
        filled = round((completed / total) * 10) if total else 0
        bar = "█" * filled + "░" * (10 - filled)
        lines.extend(
            ["", f"*Plan r{state.plan_revision}*  {inline_code(f'{completed}/{total}')}  {inline_code(bar)}"]
        )
        visible, hidden = _visible_steps(state.plan)
        lines.extend(_step_line(index, step) for index, step in visible)
        if hidden:
            lines.append(escape(f"… 另有 {hidden} 项，使用 /plan 查看"))
        if completed == total and goal_status != "complete":
            lines.append("⏳ 计划项已完成，等待遗漏检查与 Goal 收口")
    else:
        lines.extend(["", "*Plan*  尚未创建"])
    agents_total = state.agents_completed + state.agents_active + state.agents_failed
    lines.extend(
        [
            "",
            f"*Tasks*  {inline_code(f'{state.agents_completed}/{agents_total}')}"
            f" · Agents {inline_code(f'{state.agents_active} active')}",
            f"*Queue*  {inline_code(state.queue_count)}",
        ]
    )
    if state.latest_activity:
        lines.append(f"*最新*  {escape(clip(state.latest_activity, 360))}")
    if state.last_error:
        lines.append(f"*错误*  {escape(clip(state.last_error, 360))}")
    lines.append(f"*更新*  {inline_code(time.strftime('%H:%M:%S', time.localtime(now)))} · 心跳 ≤60s")
    message = "\n".join(lines)
    if len(message) <= MAX_MESSAGE_LENGTH:
        return message
    # All dynamic text is clipped above; this protects against unexpected escaping expansion.
    fallback_lines = lines[:5] + ["", "内容过长，使用 /status、/plan 或 /timeline 查看详情"]
    while fallback_lines and len("\n".join(fallback_lines)) > MAX_MESSAGE_LENGTH:
        fallback_lines.pop(-2 if len(fallback_lines) > 2 else -1)
    return "\n".join(fallback_lines)


def render_dashboard_plain(state: ThreadState, now: int | None = None) -> str:
    now = now or int(time.time())
    _, status = _thread_status(state)
    _, goal_status = _goal_status(state.goal)
    lines = [
        f"Codex · {clip(state.title, 80)}",
        f"{state.short_id} · {status} · {_duration(state, now)}",
        compact_path(state.cwd),
        "",
        f"Goal · {goal_status}",
        clip((state.goal or {}).get("objective") or "未创建 Goal", 320),
        "",
        f"Plan · {state.completed_steps}/{len(state.plan)}",
    ]
    visible, hidden = _visible_steps(state.plan)
    for index, step in visible:
        marker = {"completed": "[x]", "inProgress": ">", "pending": "[ ]"}.get(step.status, "[!]")
        lines.append(f"{marker} {index}. {clip(step.step, 180)}")
    if hidden:
        lines.append(f"... 另有 {hidden} 项，使用 /plan 查看")
    agents_total = state.agents_completed + state.agents_active + state.agents_failed
    lines.extend(
        [
            "",
            f"Tasks · {state.agents_completed}/{agents_total}",
            f"Queue · {state.queue_count}",
            f"最新 · {clip(state.latest_activity, 360)}" if state.latest_activity else "",
            f"更新 · {time.strftime('%H:%M:%S', time.localtime(now))} · 心跳 <=60s",
        ]
    )
    while lines and len("\n".join(lines)) > MAX_MESSAGE_LENGTH:
        lines.pop(-2 if len(lines) > 2 else -1)
    return "\n".join(line for line in lines if line != "")
