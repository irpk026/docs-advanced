#!/usr/bin/env python3
"""
Exercise 3 — tests for business_rules.py.

Run from the folder holding both files:

    uv run python -m pytest test_business_rules.py -v
    # or, with no pytest installed:
    uv run python test_business_rules.py
"""

from business_rules import enforce_business_rules


def _payload(**decision_overrides):
    """A coherent CRITICAL response; override fields to make it incoherent."""
    decision = {
        "urgencyLevel": "CRITICAL",
        "urgencyScore": 9,
        "reasoning": ["Selling 12x baseline", "3 hours to stockout"],
        "recommendedActions": ["RUSH_REORDER", "SURGE_PRICING"],
    }
    decision.update(decision_overrides)
    return {
        "alertId": "ALERT-1",
        "agentDecision": decision,
        "reorderRecommendation": {"shouldReorder": True, "reorderQuantity": 200},
    }


def _expect_failure(payload, fragment):
    try:
        enforce_business_rules(payload)
    except ValueError as err:
        assert fragment in str(err), f"expected {fragment!r} in: {err}"
        return
    raise AssertionError(f"expected a ValueError mentioning {fragment!r}")


def test_coherent_response_passes():
    assert enforce_business_rules(_payload())["alertId"] == "ALERT-1"


def test_critical_without_reorder_fails():
    p = _payload()
    p["reorderRecommendation"]["shouldReorder"] = False
    _expect_failure(p, "CRITICAL but reorderRecommendation.shouldReorder is false")


def test_reorder_without_quantity_fails():
    p = _payload()
    p["reorderRecommendation"]["reorderQuantity"] = 0
    _expect_failure(p, "reorderQuantity is missing or not positive")


def test_score_inconsistent_with_level_fails():
    _expect_failure(_payload(urgencyScore=2), "expects urgencyScore 8-10")


def test_critical_without_stock_action_fails():
    _expect_failure(_payload(recommendedActions=["MONITOR"]), "no stock-moving action")


def test_empty_reasoning_fails():
    _expect_failure(_payload(reasoning=["", "  "]), "no substantive entries")


if __name__ == "__main__":
    passed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn(); print(f"  ok  {name}"); passed += 1
    print(f"\n{passed} tests passed")
