//! Pure Telegram 818/69 status-surface business contract.
//!
//! The daemon owns transport and persistence, while this module keeps the
//! user-visible labels, callback actions, expiry, and close messages stable
//! enough to test against the Python 69 oracle without a Telegram client.

use serde::{Deserialize, Serialize};

pub const DASHBOARD_DEBOUNCE_MS: i64 = 500;
pub const HEARTBEAT_SECONDS: i64 = 60;
pub const STATUS_CALLBACK_EXPIRY_SECONDS: i64 = 300;
pub const STATUS_CALLBACK_TTL_MS: i64 = STATUS_CALLBACK_EXPIRY_SECONDS * 1_000;

pub const LOCKED_WRITE_MESSAGE: &str =
    "写操作已锁定，请先发送 /totp <验证码>。认证后可再次点击原按钮。";
pub const UNWATCH_CONFIRM_MESSAGE: &str = "确认取消关注？评论历史会保留，但此评论串将永久只读。";
pub const UNWATCH_CANCEL_MESSAGE: &str = "已取消操作。";
pub const UNWATCH_CLOSED_MESSAGE: &str = "已取消关注。评论历史已保留，此评论串现为只读。";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StatusButton {
    pub label: String,
    pub action: String,
}

pub fn status_buttons(lifecycle: &str, terminal: bool) -> Vec<StatusButton> {
    if terminal || lifecycle == "closed" {
        return Vec::new();
    }
    if matches!(lifecycle, "pending" | "active" | "repair_required") {
        return vec![StatusButton {
            label: "取消关注".to_owned(),
            action: "space_unwatch".to_owned(),
        }];
    }
    Vec::new()
}

pub fn unwatch_confirmation_buttons() -> Vec<StatusButton> {
    vec![
        StatusButton {
            label: "确认取消关注".to_owned(),
            action: "status_unwatch_execute".to_owned(),
        },
        StatusButton {
            label: "返回".to_owned(),
            action: "status_unwatch_cancel".to_owned(),
        },
    ]
}

pub fn is_status_action(action: &str) -> bool {
    matches!(
        action,
        "space_refresh" | "space_unwatch" | "status_unwatch_execute" | "status_unwatch_cancel"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn python_69_fixture_keeps_status_surface_contract() {
        let fixture: Value = serde_json::from_str(include_str!(
            "../../../../fixtures/status_contract/818.json"
        ))
        .expect("status fixture is valid JSON");
        assert_eq!(fixture["locked_write"], LOCKED_WRITE_MESSAGE);
        assert_eq!(fixture["unwatch_confirm"], UNWATCH_CONFIRM_MESSAGE);
        assert_eq!(fixture["unwatch_cancel"], UNWATCH_CANCEL_MESSAGE);
        assert_eq!(fixture["unwatch_closed"], UNWATCH_CLOSED_MESSAGE);
        assert_eq!(fixture["debounce_ms"], DASHBOARD_DEBOUNCE_MS);
        assert_eq!(fixture["heartbeat_seconds"], HEARTBEAT_SECONDS);
        assert_eq!(
            fixture["callback_expiry_seconds"],
            STATUS_CALLBACK_EXPIRY_SECONDS
        );

        let active = status_buttons("active", false);
        assert_eq!(
            active,
            vec![StatusButton {
                label: "取消关注".into(),
                action: "space_unwatch".into(),
            }]
        );
        assert!(status_buttons("closed", false).is_empty());
        assert_eq!(
            unwatch_confirmation_buttons(),
            vec![
                StatusButton {
                    label: "确认取消关注".into(),
                    action: "status_unwatch_execute".into(),
                },
                StatusButton {
                    label: "返回".into(),
                    action: "status_unwatch_cancel".into(),
                },
            ]
        );
    }
}
