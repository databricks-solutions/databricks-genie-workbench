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


# ── Active cluster / routing path (cross-review regressions) ──────────────────
# The reviewer flagged that the ACTIVE clustering + lever-assignment path
# (cluster_failures → _map_to_lever in the harness; recommended_levers_for_cluster
# / stamp_recommended_levers_on_clusters in the strategist path) still routed
# official Benchmark rows via the legacy judge/root-cause maps. These regressions
# pin the official-reason path through those active functions.
from genie_space_optimizer.optimization.optimizer import (  # noqa: E402
    _map_to_lever,
    cluster_failures,
)
from genie_space_optimizer.optimization.stages.action_groups import (  # noqa: E402
    recommended_levers_for_cluster,
    stamp_recommended_levers_on_clusters,
)


def _official_bad_row(qid: str, reasons, *, expected_sql="", asset_mismatch=False):
    """An official-runner-shaped BAD eval row (verdict ``no`` + reasons)."""
    return {
        "question_id": qid,
        "result_correctness/value": "no",
        "feedback/result_correctness/value": "no",
        "arbiter/value": "skipped",
        "assessment": "BAD",
        "assessment_reasons": list(reasons),
        "asset_type_mismatch": asset_mismatch,
        "request": {"question": f"q {qid}", "expected_sql": expected_sql},
        "response": {"response": "SELECT 1", "comparison": {}},
    }


def test_map_to_lever_prefers_official_reasons_over_legacy_judge_map() -> None:
    # Legacy: root_cause='wrong_column' + judge='result_correctness' → Lever 1.
    assert _map_to_lever("wrong_column", judge="result_correctness") == 1
    # Official MISSING_JOIN reason must override → Lever 4 (Join Specs).
    assert (
        _map_to_lever(
            "wrong_column",
            judge="result_correctness",
            assessment_reasons=["LLM_JUDGE_MISSING_JOIN"],
        )
        == 4
    )


def test_map_to_lever_falls_back_to_legacy_when_reasons_non_actionable() -> None:
    # Non-actionable EMPTY_GOOD_SQL contributes no lever ⇒ legacy path wins.
    assert (
        _map_to_lever(
            "wrong_column",
            judge="result_correctness",
            assessment_reasons=["EMPTY_GOOD_SQL"],
        )
        == 1
    )
    # No reasons at all ⇒ unchanged legacy routing.
    assert _map_to_lever("wrong_join") == 4


def test_cluster_failures_stamps_official_reasons_on_cluster() -> None:
    row = _official_bad_row("q1", ["LLM_JUDGE_MISSING_JOIN"])
    clusters = cluster_failures({"rows": [row]}, {}, verbose=False)
    assert len(clusters) == 1
    assert clusters[0]["assessment_reasons"] == ["LLM_JUDGE_MISSING_JOIN"]


def test_active_mapped_lever_comes_from_assessment_reasons_not_legacy() -> None:
    # End-to-end: an official BAD row whose SQL-shape root_cause would route one
    # way under the legacy judge map, but whose official reason routes elsewhere.
    row = _official_bad_row("q1", ["LLM_JUDGE_MISSING_JOIN"])
    cluster = cluster_failures({"rows": [row]}, {}, verbose=False)[0]

    # The legacy mapping (no reasons) and the official mapping disagree here.
    legacy_lever = _map_to_lever(
        cluster["root_cause"], judge=cluster.get("affected_judge")
    )
    # This mirrors the harness _mapped_lever call (harness.py ~11142).
    active_lever = _map_to_lever(
        cluster["root_cause"],
        asi_failure_type=cluster.get("asi_failure_type"),
        blame_set=cluster.get("asi_blame_set"),
        judge=cluster.get("affected_judge"),
        assessment_reasons=cluster.get("assessment_reasons"),
    )
    assert active_lever == 4  # Join Specs, from LLM_JUDGE_MISSING_JOIN
    assert active_lever != legacy_lever


def test_asset_mismatch_cluster_routes_to_lever_5_through_active_path() -> None:
    row = _official_bad_row("q1", [], asset_mismatch=True)
    cluster = cluster_failures({"rows": [row]}, {}, verbose=False)[0]
    assert cluster.get("asset_type_mismatch") is True
    assert "ASSET_TYPE_MISMATCH" in cluster["assessment_reasons"]
    active_lever = _map_to_lever(
        cluster["root_cause"],
        judge=cluster.get("affected_judge"),
        assessment_reasons=cluster.get("assessment_reasons"),
    )
    assert active_lever == 5


# ── Strategist recommended_levers (blocking issue 2) ─────────────────────────
def test_recommended_levers_for_cluster_official_overrides_shape_default() -> None:
    # A single-question 'plural_top_n_collapse' cluster would get the per-question
    # shape default (3, 5); the official MISSING_JOIN reason overrides → (4, 5).
    cluster = {
        "cluster_id": "H001",
        "question_ids": ["q1"],
        "q_count": 1,
        "root_cause": "plural_top_n_collapse",
        "assessment_reasons": ["LLM_JUDGE_MISSING_JOIN"],
    }
    assert recommended_levers_for_cluster(cluster) == (4, 5)


def test_stamp_recommended_levers_on_official_cluster_uses_reason_mapping() -> None:
    clusters = [
        {
            "cluster_id": "H001",
            "question_ids": ["q1"],
            "q_count": 1,
            "root_cause": "plural_top_n_collapse",  # legacy default would be (3,5,6)/(3,5)
            "assessment_reasons": ["LLM_JUDGE_WRONG_FILTER"],
        }
    ]
    stamped = stamp_recommended_levers_on_clusters(clusters)
    # WRONG_FILTER → FILTER_LOGIC_MISMATCH → (2, 5, 6); NOT the root-cause default.
    assert stamped[0]["recommended_levers"] == [2, 5, 6]


def test_stamp_recommended_levers_preserves_existing_when_no_official() -> None:
    clusters = [
        {
            "cluster_id": "H001",
            "question_ids": ["q1"],
            "q_count": 1,
            "root_cause": "plural_top_n_collapse",
            "recommended_levers": [6],  # explicit upstream recommendation
        }
    ]
    stamped = stamp_recommended_levers_on_clusters(clusters)
    # No official reasons ⇒ the explicit value is preserved, not clobbered by
    # the root-cause shape default.
    assert stamped[0]["recommended_levers"] == [6]


def test_stamp_recommended_levers_official_beats_existing_explicit() -> None:
    clusters = [
        {
            "cluster_id": "H001",
            "question_ids": ["q1"],
            "q_count": 1,
            "root_cause": "plural_top_n_collapse",
            "recommended_levers": [6],
            "assessment_reasons": ["LLM_JUDGE_MISSING_JOIN"],
        }
    ]
    stamped = stamp_recommended_levers_on_clusters(clusters)
    # Official reason-derived levers win over a stale explicit value.
    assert stamped[0]["recommended_levers"] == [4, 5]
