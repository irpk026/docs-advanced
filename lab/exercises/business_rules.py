#!/usr/bin/env python3
"""
Exercise 3 — business rules on top of schema validation.

Schema validation answers "is this well-formed?". It cannot answer "does this
make sense?". A response can be perfectly schema-valid and still say the
situation is CRITICAL while recommending no reorder.

Drop this file next to response_validator.py, then chain it in
consume_velocity_alerts_with_agent.py:

    from response_validator import validate_agent_response
    from business_rules import enforce_business_rules

    payload = enforce_business_rules(validate_agent_response(raw))

Order matters: schema first (guarantees the fields exist and have the right
types), business rules second (reason about the values).

A raised ValueError propagates to the main loop, so the Kafka offset is not
committed and the alert is reprocessed — the same at-least-once behaviour the
lab already relies on for schema failures.
"""

from __future__ import annotations

from typing import Any, Dict, List

# Actions that actually place or move stock.
REORDER_ACTIONS = {"RUSH_REORDER", "STANDARD_REORDER", "STOCK_TRANSFER"}


def enforce_business_rules(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Validate decision coherence. Raises ValueError listing every violation."""
    violations: List[str] = []

    decision = payload.get("agentDecision", {})
    urgency = decision.get("urgencyLevel")
    score = decision.get("urgencyScore")
    actions = decision.get("recommendedActions", []) or []
    reorder = payload.get("reorderRecommendation", {})

    # Rule 1 — the exercise's requirement.
    # CRITICAL means "act now", so it must come with an actual reorder.
    if urgency == "CRITICAL" and not reorder.get("shouldReorder"):
        violations.append(
            "urgencyLevel is CRITICAL but reorderRecommendation.shouldReorder is false"
        )

    # Rule 2 — a reorder must specify how many units.
    if reorder.get("shouldReorder") and not (reorder.get("reorderQuantity") or 0) > 0:
        violations.append(
            "shouldReorder is true but reorderQuantity is missing or not positive"
        )

    # Rule 3 — urgencyScore and urgencyLevel must agree.
    expected = {"CRITICAL": (8, 10), "HIGH": (6, 9), "MEDIUM": (3, 7), "LOW": (0, 4)}
    if urgency in expected and isinstance(score, int):
        low, high = expected[urgency]
        if not low <= score <= high:
            violations.append(
                f"urgencyLevel {urgency} expects urgencyScore {low}-{high}, got {score}"
            )

    # Rule 4 — CRITICAL/HIGH must recommend something that moves stock.
    if urgency in {"CRITICAL", "HIGH"} and not (set(actions) & REORDER_ACTIONS):
        violations.append(
            f"urgencyLevel {urgency} recommends no stock-moving action: {actions}"
        )

    # Rule 5 — reasoning must be substantive, not a single empty string.
    reasoning = decision.get("reasoning", []) or []
    if not any(str(r).strip() for r in reasoning):
        violations.append("agentDecision.reasoning contains no substantive entries")

    if violations:
        raise ValueError(
            "Agent response failed business rules: " + "; ".join(violations)
        )

    return payload
