from __future__ import annotations

import json
from pathlib import Path

FIXTURE = Path(__file__).parents[1] / "fixtures" / "status_contract" / "818.json"


def test_818_status_fixture_matches_python_69_contract() -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert fixture["bot_role"] == "status"
    assert fixture["locked_write"] == "写操作已锁定，请先发送 /totp <验证码>。认证后可再次点击原按钮。"
    assert fixture["unwatch_confirm"] == "确认取消关注？评论历史会保留，但此评论串将永久只读。"
    assert fixture["unwatch_closed"] == "已取消关注。评论历史已保留，此评论串现为只读。"
    assert fixture["active_buttons"] == [[{"label": "取消关注", "action": "space_unwatch"}]]
    assert fixture["terminal_buttons"] == []
    assert fixture["confirmation_buttons"] == [
        [{"label": "确认取消关注", "action": "status_unwatch_execute"}],
        [{"label": "返回", "action": "status_unwatch_cancel"}],
    ]
    assert fixture["debounce_ms"] == 500
    assert fixture["heartbeat_seconds"] == 60
    assert fixture["callback_expiry_seconds"] == 300
