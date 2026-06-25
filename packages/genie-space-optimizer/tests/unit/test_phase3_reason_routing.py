"""Phase 3 (D2) — judge re-architecture.

Covers the official Benchmark API ``assessment_reasons`` → ``RcaKind`` → lever
routing (replacing the retired 9 scored judges), the derived asset-type
annotation feeding Lever 5, and the API-accuracy acceptance gate (no per-judge
thresholds remain).
"""

from __future__ import annotations

from databricks.sdk.service.dashboards import ScoreReason

from genie_space_optimizer.common.config import DEFAULT_THRESHOLDS
from genie_space_optimizer.optimization.evaluation import all_thresholds_met
from genie_space_optimizer.optimization.rca import (
    _ASSESSMENT_REASON_TO_RCA_KIND,
    _NON_ACTIONABLE_ASSESSMENT_REASONS,
    _RCA_KIND_TO_LEVERS,
    RcaKind,
    extract_rca_findings_from_row,
    levers_for_assessment_reasons,
    rca_kind_for_assessment_reason,
    recommended_levers_for_rca_kind,
)
from genie_space_optimizer.optimization.scorers import (
    EXPECTED_JUDGE_SET,
    RETIRED_JUDGES,
)

# The 25 official ScoreReason values, sourced from the installed SDK so the test
# fails loudly if Databricks adds a value GSO does not yet handle.
OFFICIAL_REASONS = sorted(m.value for m in ScoreReason)

# The 6 LLM_JUDGE_* reasons that were NOT mirrored before Phase 3 (the plan's
# "11 of 17 mirrored today; cover the missing 6").
PREVIOUSLY_UNMIRRORED = {
    "LLM_JUDGE_MISSING_JOIN",
    "LLM_JUDGE_SEMANTIC_ERROR",
    "LLM_JUDGE_SYNTAX_ERROR",
    "LLM_JUDGE_WRONG_AGGREGATION",
    "LLM_JUDGE_WRONG_COLUMNS",
    "LLM_JUDGE_WRONG_FILTER",
}


# ── Reason → RcaKind coverage ────────────────────────────────────────────────
def test_all_25_official_reasons_are_explicitly_mapped() -> None:
    assert len(OFFICIAL_REASONS) == 25
    unmapped = [r for r in OFFICIAL_REASONS if r not in _ASSESSMENT_REASON_TO_RCA_KIND]
    assert unmapped == [], f"official ScoreReason values not handled: {unmapped}"


def test_previously_unmirrored_six_are_now_handled() -> None:
    # All six resolve to a concrete (non-fallback) RcaKind and recommend levers.
    for reason in PREVIOUSLY_UNMIRRORED:
        assert reason in _ASSESSMENT_REASON_TO_RCA_KIND
        kind = rca_kind_for_assessment_reason(reason)
        assert isinstance(kind, RcaKind)
        assert recommended_levers_for_rca_kind(kind)


def test_mapping_targets_are_valid_rca_kinds_with_known_levers() -> None:
    for reason, kind in _ASSESSMENT_REASON_TO_RCA_KIND.items():
        assert isinstance(kind, RcaKind), reason
        # Every target kind is present in the established lever map (no new map).
        assert kind in _RCA_KIND_TO_LEVERS, reason


def test_unknown_reason_falls_back_to_unknown_kind() -> None:
    assert rca_kind_for_assessment_reason("SOMETHING_NEW") is RcaKind.UNKNOWN
    assert rca_kind_for_assessment_reason("") is RcaKind.UNKNOWN
    assert rca_kind_for_assessment_reason(None) is RcaKind.UNKNOWN


def test_reason_to_lever_reuses_established_map() -> None:
    # Joins route to Lever 4; filters route through 5/6; columns to Lever 1.
    assert 4 in levers_for_assessment_reasons(["LLM_JUDGE_MISSING_JOIN"])
    assert set(levers_for_assessment_reasons(["LLM_JUDGE_WRONG_FILTER"])) >= {5, 6}
    assert levers_for_assessment_reasons(["LLM_JUDGE_WRONG_COLUMNS"]) == (1,)


def test_levers_union_in_first_seen_order_and_dedupe() -> None:
    levers = levers_for_assessment_reasons(
        ["RESULT_EXTRA_ROWS", "RESULT_EXTRA_ROWS", "LLM_JUDGE_MISSING_JOIN"]
    )
    # FILTER_LOGIC_MISMATCH -> (2,5,6) then JOIN -> (4,5); 5 dedup'd, order stable.
    assert levers == (2, 5, 6, 4)


# ── Non-actionable reasons (ground-truth defects) ────────────────────────────
def test_empty_good_sql_is_non_actionable() -> None:
    assert "EMPTY_GOOD_SQL" in _NON_ACTIONABLE_ASSESSMENT_REASONS
    assert levers_for_assessment_reasons(["EMPTY_GOOD_SQL"]) == ()


def test_empty_good_sql_produces_no_finding() -> None:
    row = {
        "question_id": "q1",
        "assessment": "BAD",
        "assessment_reasons": ["EMPTY_GOOD_SQL"],
    }
    assert extract_rca_findings_from_row(row) == []


# ── Finding extraction from official rows ────────────────────────────────────
def test_official_row_reasons_drive_findings() -> None:
    row = {
        "question_id": "q1",
        "assessment": "BAD",
        "assessment_reasons": ["LLM_JUDGE_MISSING_JOIN"],
    }
    findings = extract_rca_findings_from_row(row)
    kinds = {f.rca_kind for f in findings}
    assert RcaKind.JOIN_SPEC_MISSING_OR_WRONG in kinds
    join_finding = next(f for f in findings if f.rca_kind is RcaKind.JOIN_SPEC_MISSING_OR_WRONG)
    assert 4 in join_finding.recommended_levers
    assert join_finding.target_qids == ("q1",)


def test_asset_type_mismatch_annotation_adds_routing_finding() -> None:
    row = {
        "question_id": "q1",
        "assessment": "BAD",
        "assessment_reasons": [],
        "asset_type_mismatch": True,
        "expected_asset_type": "MV",
        "actual_asset_type": "TABLE",
    }
    findings = extract_rca_findings_from_row(row)
    kinds = {f.rca_kind for f in findings}
    assert RcaKind.ASSET_TYPE_ROUTING_MISMATCH in kinds
    asset_finding = next(
        f for f in findings if f.rca_kind is RcaKind.ASSET_TYPE_ROUTING_MISMATCH
    )
    # Asset-type routing is a Lever 5 (instructions/example-SQL) nudge.
    assert asset_finding.recommended_levers == (5,)


def test_legacy_row_without_reasons_is_unaffected() -> None:
    # No top-level ``assessment_reasons`` key ⇒ no reason-derived findings (the
    # in-process / mocked path is untouched by Phase 3 routing).
    assert extract_rca_findings_from_row({"question_id": "q9"}) == []


def test_reason_finding_confidence_below_sql_shape() -> None:
    # Coarse reason findings must rank below the deterministic SQL-shape findings
    # (0.8–0.9) so the fine sub-router wins on merge.
    row = {"question_id": "q1", "assessment": "BAD", "assessment_reasons": ["LLM_JUDGE_OTHER"]}
    findings = extract_rca_findings_from_row(row)
    assert findings
    assert all(f.confidence <= 0.6 for f in findings)


# ── Acceptance: API-accuracy gating, no per-judge thresholds ─────────────────
def test_default_thresholds_has_no_per_judge_entries() -> None:
    # Only the API-accuracy gate remains; none of the retired judges (other than
    # the ``result_correctness`` accuracy carrier) may be a threshold.
    assert set(DEFAULT_THRESHOLDS) == {"result_correctness"}
    retired_minus_carrier = set(RETIRED_JUDGES) - {"result_correctness"}
    assert retired_minus_carrier.isdisjoint(DEFAULT_THRESHOLDS)


def test_all_thresholds_met_gates_on_accuracy_only() -> None:
    assert all_thresholds_met({"result_correctness": 90.0}) is True
    assert all_thresholds_met({"result_correctness": 80.0}) is False
    # Retired per-judge scores are ignored entirely.
    assert all_thresholds_met({"result_correctness": 90.0, "syntax_validity": 0.0}) is True


def test_all_thresholds_met_accepts_overall_accuracy_alias() -> None:
    assert all_thresholds_met({"overall_accuracy": 90.0}) is True
    assert all_thresholds_met({}) is False


def test_retired_judges_constant_matches_legacy_set() -> None:
    assert RETIRED_JUDGES == EXPECTED_JUDGE_SET
    assert len(RETIRED_JUDGES) == 9
