from __future__ import annotations

from typing import Any

ApprovalDecision = str | dict[str, Any]

_MODERN_METHOD = "item/commandExecution/requestApproval"
_LEGACY_METHOD = "execCommandApproval"
_MODERN_FILE_METHOD = "item/fileChange/requestApproval"
_MODERN_PERMISSIONS_METHOD = "item/permissions/requestApproval"
_LEGACY_PATCH_METHOD = "applyPatchApproval"
_SIMPLE_DECISIONS = {"accept", "acceptForSession", "decline", "cancel"}
_DEFAULT_MODERN_DECISIONS: tuple[ApprovalDecision, ...] = (
    "accept",
    "acceptForSession",
    "decline",
)
_DEFAULT_LEGACY_DECISIONS: tuple[ApprovalDecision, ...] = (
    "accept",
    "acceptForSession",
    "decline",
    "cancel",
)


def approval_decision_kind(decision: object) -> str | None:
    if isinstance(decision, str):
        return decision if decision in _SIMPLE_DECISIONS else None
    if not isinstance(decision, dict) or len(decision) != 1:
        return None
    if "acceptWithExecpolicyAmendment" in decision:
        detail = decision["acceptWithExecpolicyAmendment"]
        if not isinstance(detail, dict):
            return None
        amendment = detail.get("execpolicy_amendment")
        if not isinstance(amendment, list) or not all(isinstance(value, str) for value in amendment):
            return None
        return "acceptWithExecpolicyAmendment"
    if "applyNetworkPolicyAmendment" in decision:
        detail = decision["applyNetworkPolicyAmendment"]
        if not isinstance(detail, dict):
            return None
        amendment = detail.get("network_policy_amendment")
        if not isinstance(amendment, dict):
            return None
        if amendment.get("action") not in {"allow", "deny"}:
            return None
        if not isinstance(amendment.get("host"), str) or not amendment["host"].strip():
            return None
        return "applyNetworkPolicyAmendment"
    return None


def command_approval_decisions(method: str, params: dict[str, Any]) -> list[ApprovalDecision]:
    if method == _MODERN_METHOD:
        raw = params.get("availableDecisions")
        if raw is None:
            return list(_DEFAULT_MODERN_DECISIONS)
        if not isinstance(raw, list):
            return []
        return [decision for decision in raw if approval_decision_kind(decision) is not None]
    if method == _LEGACY_METHOD:
        return list(_DEFAULT_LEGACY_DECISIONS)
    return []


def approval_decision_is_available(
    decision: object,
    available: list[ApprovalDecision],
) -> bool:
    return approval_decision_kind(decision) is not None and any(
        decision == candidate for candidate in available
    )


def normalize_command_approval_params(method: str, params: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(params)
    if method == _LEGACY_METHOD:
        normalized["threadId"] = params.get("conversationId") or params.get("threadId") or ""
        normalized["turnId"] = params.get("turnId") or ""
        normalized["itemId"] = params.get("callId") or params.get("itemId") or ""
    return normalized


def interactive_approval_decisions(method: str, params: dict[str, Any]) -> list[ApprovalDecision]:
    """Return Telegram-safe choices for every approval protocol we can answer."""
    if method in {_MODERN_METHOD, _LEGACY_METHOD}:
        return command_approval_decisions(method, params)
    if method in {_MODERN_FILE_METHOD, _LEGACY_PATCH_METHOD}:
        raw = params.get("availableDecisions")
        if raw is None:
            return list(_DEFAULT_LEGACY_DECISIONS)
        if not isinstance(raw, list):
            return []
        return [decision for decision in raw if approval_decision_kind(decision) in _SIMPLE_DECISIONS]
    if method == _MODERN_PERMISSIONS_METHOD:
        permissions = params.get("permissions") or params.get("requestedPermissions")
        if not isinstance(permissions, dict):
            return []
        turn_grant: ApprovalDecision = {"permissions": permissions, "scope": "turn"}
        if not permissions:
            return [turn_grant]
        return [
            turn_grant,
            {"permissions": permissions, "scope": "session"},
            {"permissions": {}, "scope": "turn"},
        ]
    return []


def interactive_approval_is_available(
    method: str,
    decision: object,
    available: list[ApprovalDecision],
) -> bool:
    if method == _MODERN_PERMISSIONS_METHOD:
        return isinstance(decision, dict) and any(decision == candidate for candidate in available)
    return approval_decision_is_available(decision, available)


def approval_response_payload(method: str, decision: ApprovalDecision) -> dict[str, Any]:
    """Build the JSON-RPC result envelope for modern and legacy approvals."""
    if method == _MODERN_PERMISSIONS_METHOD:
        if not isinstance(decision, dict):
            raise ValueError("权限审批响应无效")
        permissions = decision.get("permissions")
        scope = decision.get("scope")
        strict = decision.get("strictAutoReview")
        if not isinstance(permissions, dict) or scope not in {"turn", "session"}:
            raise ValueError("权限审批响应无效")
        if strict is not None and not isinstance(strict, bool):
            raise ValueError("strictAutoReview 必须是布尔值")
        if scope == "session" and strict is True:
            raise ValueError("Session 权限不能启用 strictAutoReview")
        return dict(decision)
    if approval_decision_kind(decision) is None:
        raise ValueError("命令审批决定无效")
    if method in {_MODERN_METHOD, _MODERN_FILE_METHOD}:
        return {"decision": decision}
    if method in {_LEGACY_METHOD, _LEGACY_PATCH_METHOD}:
        if not isinstance(decision, str):
            raise ValueError("旧版审批只接受简单决定")
        mapped = {
            "accept": "approved",
            "acceptForSession": "approved_for_session",
            "decline": "denied",
            "cancel": "abort",
        }.get(decision)
        if mapped is None:
            raise ValueError("旧版审批决定无效")
        return {"decision": mapped}
    raise ValueError("未知的审批协议")


def normalize_interactive_approval_params(method: str, params: dict[str, Any]) -> dict[str, Any]:
    """Normalize legacy command and patch requests to the current thread/item keys."""
    if method == _LEGACY_METHOD:
        return normalize_command_approval_params(method, params)
    normalized = dict(params)
    if method == _LEGACY_PATCH_METHOD:
        normalized["threadId"] = params.get("conversationId") or params.get("threadId") or ""
        normalized["turnId"] = params.get("turnId") or ""
        normalized["itemId"] = params.get("callId") or params.get("itemId") or ""
    return normalized
