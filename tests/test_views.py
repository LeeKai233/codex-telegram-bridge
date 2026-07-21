from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from codex_telegram_bridge.models import (
    LifecycleActivity,
    PlanStep,
    SessionSpace,
    TaskState,
    ThreadState,
)
from codex_telegram_bridge.views import (
    ANIMATION_FRAMES,
    CHANNEL_POST_BUDGET,
    SESSIONS_MESSAGE_BUDGET,
    STATUS_COMMENT_BUDGET,
    pagination_layout,
    render_ask_answer,
    render_ask_waiting,
    render_channel_post,
    render_closed_space,
    render_help,
    render_pending_space,
    render_sessions_page,
    render_status_comment,
)


def test_animation_frames_use_the_eight_moon_phases() -> None:
    assert ANIMATION_FRAMES == ("🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘")


@dataclass
class State:
    thread_id: str = "019f615b-8a0f-7ab2-aeb0-5dc7464605cd"
    title: str = "Improve bot & status"
    natural_summary: str = "优化 Telegram Bot 的交互和状态展示"
    cwd: str = str(Path.home() / "projects" / "codex-telegram-bridge")
    status: str = "active"
    turn_status: str = "inProgress"
    turn_started_at: int = 1_700_000_000
    created_at: int = 1_699_990_000
    updated_at: int = 1_700_000_000
    goal: dict[str, str] = field(
        default_factory=lambda: {"status": "active", "objective": "Ship channel comments"}
    )
    plan: list[PlanStep] = field(
        default_factory=lambda: [
            PlanStep("Inspect architecture", "completed"),
            PlanStep("Implement routing", "inProgress"),
            PlanStep("Run verification", "pending"),
        ]
    )
    agents_completed: int = 2
    agents_active: int = 1
    agents_failed: int = 0
    queue_count: int = 2
    latest_activity: str = "Rendering dashboard"
    last_error: str = ""


def labels(page: int, total: int) -> list[str]:
    return [button.label for button in pagination_layout(page, total)]


def test_pagination_layout_matches_every_approved_position() -> None:
    assert labels(1, 8) == ["1", ">>"]
    assert labels(2, 8) == ["<<", "2", ">>"]
    assert labels(3, 8) == ["1", "<<", "3", ">>"]
    assert labels(7, 8) == ["1", "<<", "7", "8"]
    assert labels(8, 8) == ["1", "<<", "8"]
    assert labels(1, 1) == ["1"]
    with pytest.raises(ValueError):
        pagination_layout(9, 8)


def test_sessions_page_contains_five_four_line_items_and_detail_slots() -> None:
    states = [
        State(
            thread_id=f"019f615b-8a0f-7ab2-aeb0-{index:012d}",
            title=f"Session {index}",
            natural_summary=f"Natural summary {index}",
        )
        for index in range(8)
    ]
    rendered = render_sessions_page(states, page=1, now=1_700_000_120)
    assert rendered.page == 1
    assert rendered.total_pages == 2
    assert [detail.label for detail in rendered.details] == ["①", "②", "③", "④", "⑤"]
    assert "🤖 Codex Sessions · 1/2" in rendered.message.markdown
    assert "① 🟢 `019f615b-8a0f-7ab2-aeb0-000000000000`" in rendered.message.markdown
    assert "📝 Natural summary 0" in rendered.message.markdown
    created = time.strftime("%m-%d %H:%M", time.localtime(1_699_990_000))
    assert f"🗓 Created `{created}` · Updated `2m ago`" in rendered.message.markdown
    assert "📁 `~/projects/codex-telegram-bridge`" in rendered.message.markdown
    assert len(rendered.message.markdown) <= SESSIONS_MESSAGE_BUDGET


def test_sessions_search_and_page_clamping() -> None:
    rendered = render_sessions_page([State()], page=99, query="bot.*")
    assert rendered.page == 1
    assert "搜索 `bot.*`" in rendered.message.markdown
    assert rendered.navigation[0].current is True

    unset_created = render_sessions_page([ThreadState(thread_id="thread")])
    assert "Created `N/A`" in unset_created.message.markdown


def test_channel_post_contains_compact_live_summary() -> None:
    rendered = render_channel_post(State(), now=1_700_000_120, total_duration_seconds=1122)
    assert "*🤖 Codex · Improve bot & status*" in rendered.markdown
    assert "🟢 执行中 · 总执行 `00:18:42`" in rendered.markdown
    assert "🎯 Goal 🟢 `active`" in rendered.markdown
    assert "🧭 Plan `1/3` `###-------`" in rendered.markdown
    assert "🧩 Tasks `2/3` · Active `1` · Failed `0` · Interrupted `0` · Queue `2`" in rendered.markdown
    assert "心跳 `≤60s`" in rendered.markdown
    assert len(rendered.markdown) <= CHANNEL_POST_BUDGET
    assert "*" not in rendered.plain


def test_full_status_comment_has_plan_tasks_auth_update_and_plain_fallback() -> None:
    state = State()
    state.recent_activity = [
        LifecycleActivity(kind="plan", text="Plan created", status="completed", timestamp=1_700_000_000)
    ]
    rendered = render_status_comment(
        state,
        now=1_700_000_120,
        total_duration_seconds=1122,
        auth_expires_at=1_700_001_900,
    )
    assert "✅ ~1\\. Inspect architecture~" in rendered.markdown
    assert "▶ *2\\. Implement routing*" in rendered.markdown
    assert "*🧩 Agent Tasks*  `2/3` · Running `1` · Failed `0` · Interrupted `0`" in rendered.markdown
    assert "*📥 Queue*  `2`" in rendered.markdown
    assert "🔓 TOTP 已认证 · 剩余 `30 min`" in rendered.markdown
    expires = time.strftime("%H:%M:%S", time.localtime(1_700_001_900))
    assert f"到期 `{expires}`" in rendered.markdown
    assert "*⚡ 最新*" not in rendered.markdown
    assert "⚡ 最新" not in rendered.plain
    assert "*🕘 近期事件*" in rendered.markdown
    assert "Plan created · completed" in rendered.markdown
    updated = time.strftime("%H:%M:%S", time.localtime(1_700_000_000))
    assert f"🕒 更新 `{updated}` · 心跳 `≤60s`" in rendered.markdown
    assert "[x] 1. Inspect architecture" in rendered.plain
    assert len(rendered.markdown) <= STATUS_COMMENT_BUDGET
    assert len(rendered.plain) <= STATUS_COMMENT_BUDGET


def test_status_comment_lists_active_agents_before_bounded_terminal_history() -> None:
    state = ThreadState(thread_id="parent", title="Parallel task", status="active")
    state.tasks = [
        TaskState(
            task_id="done-old",
            title="Old completion",
            status="completed",
            finished_at=1_700_000_010,
            updated_at=1_700_000_010,
        ),
        TaskState(
            task_id="active-agent-id",
            title="Implement status rendering",
            status="inProgress",
            agent_thread_id="active-agent-id",
            agent_path="/root/security_status",
            agent_nickname="Ada_*[]",
            agent_role="reviewer",
            model="gpt-5.6-luna",
            reasoning_effort="max",
            message="PRIVATE REASONING AND TOOL ARGUMENTS",
            started_at=1_700_000_000,
            updated_at=1_700_000_100,
        ),
        TaskState(
            task_id="done-new",
            title="Recent completion",
            status="completed",
            finished_at=1_700_000_090,
            updated_at=1_700_000_090,
        ),
        TaskState(
            task_id="failed-agent",
            title="Failed review",
            status="failed",
            finished_at=1_700_000_080,
            updated_at=1_700_000_080,
        ),
        TaskState(
            task_id="closed-agent",
            title="Closed worker",
            status="shutdown",
            finished_at=1_700_000_070,
            updated_at=1_700_000_070,
        ),
    ]

    rendered = render_status_comment(state, now=1_700_000_120)

    assert "*🤝 Subagents*" in rendered.markdown
    assert "Ada\\_\\*\\[\\]" in rendered.markdown
    assert "`reviewer`" in rendered.markdown
    assert "`gpt-5.6-luna/max`" in rendered.markdown
    assert "`00:02:00`" in rendered.markdown
    assert rendered.plain.index("active-a") < rendered.plain.index("done-new")
    assert "Recent completion" in rendered.plain
    assert "Old completion" not in rendered.plain
    assert "另有 1 个已结束 Agent" in rendered.plain
    assert "PRIVATE REASONING" not in rendered.markdown
    assert "PRIVATE REASONING" not in rendered.plain


def test_status_comment_shows_animated_mode_and_main_and_subagent_profiles() -> None:
    state = ThreadState(thread_id="parent", title="Profile status", status="active")
    state.tasks = [
        TaskState(
            task_id="worker",
            title="Implement command",
            status="inProgress",
            model="gpt-5.6-luna",
            reasoning_effort="max",
        )
    ]
    space = SessionSpace(
        space_id="space-1",
        lifecycle="active",
        thread_id="parent",
        normal_model="gpt-5.6-luna",
        normal_effort="max",
        plan_model="gpt-5.6-sol",
        plan_effort="xhigh",
        current_mode="plan",
    )

    rendered = render_status_comment(state, space=space, animation_frame=1)

    assert rendered.markdown.startswith("🌒 *🧭 Plan mode*")
    assert "*🧠 Main*  `gpt-5.6-sol` · Effort `xhigh`" in rendered.markdown
    assert "`gpt-5.6-luna/max`" in rendered.markdown
    assert rendered.plain.startswith("🌒 🧭 Plan mode")

    channel = render_channel_post(state, space=space, animation_frame=1)
    assert channel.markdown.startswith("🌒 *🧭 Plan mode*")
    assert "*🧠 Main*  `gpt-5.6-sol` · Effort `xhigh`" in channel.markdown

    space.current_mode = "default"
    normal = render_channel_post(state, space=space, animation_frame=2)
    assert normal.markdown.startswith("🌓 *⚙️ Normal mode*")
    assert "*🧠 Main*  `gpt-5.6-luna` · Effort `max`" in normal.markdown


def test_status_comment_separates_interrupted_tasks_and_warns_on_goal_plan_mismatch() -> None:
    state = ThreadState(
        thread_id="parent",
        status="idle",
        goal={"status": "complete", "objective": "Ship safely"},
        plan=[PlanStep("Implement", "completed"), PlanStep("Deploy", "inProgress")],
        tasks=[
            TaskState(
                task_id="stopped-agent",
                title="Stopped audit",
                status="interrupted",
                started_at=1_700_000_000,
                finished_at=1_700_000_030,
                updated_at=1_700_000_030,
            )
        ],
    )

    rendered = render_status_comment(state, now=1_700_000_120)

    assert "Goal 已完成，但 Plan 仍有 1 项未完成" in rendered.markdown
    assert "*🧩 Agent Tasks*  `1/1` · Running `0` · Failed `0` · Interrupted `1`" in rendered.markdown
    assert "WARNING: Goal 已完成，但 Plan 仍有 1 项未完成" in rendered.plain


def test_status_comment_warns_when_complete_goal_still_has_running_subagent() -> None:
    state = ThreadState(
        thread_id="parent",
        status="idle",
        goal={"status": "complete", "objective": "Premature"},
        plan=[PlanStep("Verify", "completed")],
        tasks=[TaskState(task_id="live", title="Still running", status="inProgress")],
    )

    rendered = render_status_comment(state, now=1_700_000_120)

    assert "Goal 已完成，但仍有 1 个 Subagent 运行中" in rendered.markdown
    assert "WARNING: Goal 已完成，但仍有 1 个 Subagent 运行中" in rendered.plain


def test_status_comment_expires_auth_and_stays_bounded_with_hostile_text() -> None:
    state = State(
        title="*_[]()~`>#+-=|{}.!" * 100,
        latest_activity="[]()_*~`>#+-=|{}.!" * 1000,
        last_error="failure" * 1000,
    )
    state.goal = {"status": "blocked", "objective": "x" * 10_000}
    state.plan = [PlanStep("step" * 200, "completed") for _ in range(100)]
    rendered = render_status_comment(state, now=1_700_000_120, auth_expires_at=1_700_000_000)
    assert "TOTP 已过期" in rendered.markdown
    assert len(rendered.markdown) <= STATUS_COMMENT_BUDGET
    assert len(rendered.plain) <= STATUS_COMMENT_BUDGET


def test_pending_closed_and_contextual_help_templates() -> None:
    pending = render_pending_space(
        {"title": "New model", "pending_cwd": "/tmp/project", "pending_prompt": "Run tests"},
        now=1_700_000_000,
    )
    assert "🟡 Pending · 等待 TOTP 认证" in pending.markdown
    assert "`/totp 123456`" in pending.markdown
    assert "`/tmp/project`" in pending.markdown

    closed = render_closed_space(State(), now=1_700_000_000)
    assert "⚫ 已关闭" in closed.markdown
    assert "仅保留历史记录" in closed.markdown

    unpaired = render_help("9527", paired=False)
    assert "`/pair`" in unpaired.markdown
    assert "`/sessions [关键词]`" not in unpaired.markdown

    session = render_help("426")
    assert "`/prompt <text>`" in session.markdown
    assert "`/ask <question>`" in session.markdown
    assert "`/unwatch`" in session.markdown

    outside = render_help("426", in_session_thread=False)
    assert "`/help`" in outside.markdown
    assert "`/prompt <text>`" not in outside.markdown

    custom_control = render_help("controller", label="控制_*[Bot]")
    assert custom_control.plain.startswith("🤖 控制_*[Bot]")
    assert "控制\\_\\*\\[Bot\\]" in custom_control.markdown
    assert "9527" not in custom_control.markdown

    custom_discussion = render_help("session", label="讨论_(Bot)")
    assert custom_discussion.plain.startswith("🤖 讨论_(Bot) · Session")
    assert "讨论\\_\\(Bot\\)" in custom_discussion.markdown
    assert "426" not in custom_discussion.markdown


def test_isolated_ask_templates_correlate_and_escape_hostile_markdown() -> None:
    waiting = render_ask_waiting("What does *this* do?", "deadbeef", clarification=True)
    assert "反问 Codex" in waiting.markdown
    assert "`deadbeef`" in waiting.markdown
    assert "\\*this\\*" in waiting.markdown
    assert "不会写入当前 Session" in waiting.markdown

    answer = render_ask_answer(
        "What does *this* do?",
        "_*[]()~`>#+-=|{}.!" * 1000,
        "deadbeef",
    )
    assert "`deadbeef`" in answer.markdown
    assert "❓ What does \\*this\\* do?" in answer.markdown
    assert len(answer.markdown) <= STATUS_COMMENT_BUDGET
    assert len(answer.plain) <= STATUS_COMMENT_BUDGET


def test_empty_structured_tasks_fall_back_to_agent_counts() -> None:
    state = ThreadState(
        thread_id="thread",
        status="active",
        agents_completed=3,
        agents_active=2,
        agents_failed=1,
    )
    rendered = render_channel_post(state, now=1_700_000_000)
    assert "🧩 Tasks `4/6` · Active `2` · Failed `1` · Interrupted `0` · Queue `0`" in rendered.markdown


def test_channel_task_counter_includes_all_terminal_statuses() -> None:
    state = ThreadState(
        thread_id="thread-mixed-tasks",
        status="active",
        tasks=[
            TaskState(task_id="completed", title="Completed", status="completed"),
            TaskState(task_id="failed", title="Failed", status="failed"),
            TaskState(task_id="interrupted", title="Interrupted", status="interrupted"),
            TaskState(task_id="active", title="Active", status="inProgress"),
            TaskState(task_id="shutdown", title="Shutdown", status="shutdown"),
            TaskState(task_id="missing", title="Missing", status="notFound"),
        ],
    )

    rendered = render_channel_post(state, now=1_700_000_000)

    assert "🧩 Tasks `5/6` · Active `1` · Failed `2` · Interrupted `1` · Queue `0`" in rendered.markdown
