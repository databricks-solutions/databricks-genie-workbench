"""Unit tests for flag_reason categorization logic.

The categorizeFlagReason function is implemented in InsightTabs.tsx
on the frontend. This module tests the equivalent Python logic that
mirrors the frontend categorization, ensuring consistent behaviour.
"""

from __future__ import annotations

import pytest


def categorize_flag_reason(reason: str) -> str:
    """Python mirror of the frontend ``categorizeFlagReason`` function.

    Kept in sync with InsightTabs.tsx for regression testing.
    """
    if not reason:
        return "other"
    r = reason.lower()
    if "persistent" in r or "additive_levers_exhausted" in r or "consecutive" in r:
        return "persistence"
    if "tvf removal" in r or "remove_tvf" in r or "remove tvf" in r:
        return "tvf_removal"
    if "strategist" in r or "flag_for_review" in r:
        return "strategist"
    return "other"


class TestCategorizeFlagReason:
    def test_persistent_failed_evals(self):
        assert categorize_flag_reason(
            "PERSISTENT: failed 3/5 evals, 3 consecutive"
        ) == "persistence"

    def test_additive_levers_exhausted(self):
        assert categorize_flag_reason(
            "ADDITIVE_LEVERS_EXHAUSTED: failed 4/5 evals"
        ) == "persistence"

    def test_consecutive_keyword(self):
        assert categorize_flag_reason(
            "Failed 3 consecutive iterations"
        ) == "persistence"

    def test_low_confidence_tvf_removal(self):
        assert categorize_flag_reason(
            "Low-confidence TVF removal recommended: get_amenity_imp"
        ) == "tvf_removal"

    def test_medium_confidence_tvf_removal(self):
        assert categorize_flag_reason(
            "Medium-confidence TVF removal applied: some_tvf"
        ) == "tvf_removal"

    def test_remove_tvf_keyword(self):
        assert categorize_flag_reason(
            "remove_tvf escalation applied"
        ) == "tvf_removal"

    def test_strategist_flagged(self):
        assert categorize_flag_reason(
            "Strategist flagged for review"
        ) == "strategist"

    def test_flag_for_review(self):
        assert categorize_flag_reason(
            "flag_for_review: requires human attention"
        ) == "strategist"

    def test_empty_string(self):
        assert categorize_flag_reason("") == "other"

    def test_unknown_format(self):
        assert categorize_flag_reason("Some totally new reason") == "other"

    def test_case_insensitive_persistent(self):
        assert categorize_flag_reason("persistent failure pattern") == "persistence"

    def test_case_insensitive_tvf(self):
        assert categorize_flag_reason("TVF REMOVAL recommended") == "tvf_removal"
