from __future__ import annotations

from typing import Any

ApprovalDecision = str | dict[str, Any]

_MODERN_METHOD = "item/commandExecution/requestApproval"
_LEGACY_METHOD = "execCommandApproval"
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
