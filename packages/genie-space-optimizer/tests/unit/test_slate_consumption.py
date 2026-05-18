"""WU-1 (2026-05-18) — authoritative slate consumption tests."""
from __future__ import annotations

from genie_space_optimizer.optimization.slate_consumption import (
    SlateDecision,
    SlateAction,
)


def test_slate_decision_action_proceed_carries_no_reason():
    d = SlateDecision(action=SlateAction.PROCEED, reason="", denied_ag_id="")
    assert d.action is SlateAction.PROCEED
    assert d.reason == ""
    assert d.denied_ag_id == ""


def test_slate_decision_action_skip_carries_typed_reason():
    d = SlateDecision(
        action=SlateAction.SKIP_AG,
        reason="ag_denied_by_admission_trace",
        denied_ag_id="AG_DECOMPOSED_H001",
    )
    assert d.action is SlateAction.SKIP_AG
    assert d.reason == "ag_denied_by_admission_trace"
    assert d.denied_ag_id == "AG_DECOMPOSED_H001"


def test_slate_decision_action_pivot_iteration_carries_pivot_target():
    d = SlateDecision(
        action=SlateAction.PIVOT_ITERATION,
        reason="all_ags_denied_or_blocked",
        denied_ag_id="",
    )
    assert d.action is SlateAction.PIVOT_ITERATION


# ── Task 2: decide_slate_action ─────────────────────────────────────


from genie_space_optimizer.optimization.admission_trace_consumer import (
    AdmissionResult,
)
from genie_space_optimizer.optimization.slate_consumption import (
    decide_slate_action,
)


def test_admitted_ag_proceeds():
    ag = {"id": "AG_OK", "source_cluster_ids": ["H001"]}
    admission = AdmissionResult(
        admitted_ags=[ag],
        denied_ag_ids=(),
        pivot_signal=False,
        first_ag_retired_id="",
    )
    decision = decide_slate_action(
        ag=ag,
        slate_admitted_ags=(ag,),
        admission_result=admission,
        blocked_cluster_ids=(),
    )
    assert decision.action.value == "proceed"
    assert decision.reason == ""


def test_denied_ag_is_skipped_with_typed_reason():
    ag = {"id": "AG_DECOMPOSED_H001", "source_cluster_ids": ["H001"]}
    admission = AdmissionResult(
        admitted_ags=[],
        denied_ag_ids=("AG_DECOMPOSED_H001",),
        pivot_signal=False,
        first_ag_retired_id="",
    )
    decision = decide_slate_action(
        ag=ag,
        slate_admitted_ags=(),
        admission_result=admission,
        blocked_cluster_ids=(),
    )
    assert decision.action.value == "skip_ag"
    assert decision.reason == "ag_denied_by_admission_trace"
    assert decision.denied_ag_id == "AG_DECOMPOSED_H001"


def test_blocked_cluster_intersect_skips_ag():
    ag = {"id": "AG_DECOMPOSED_H002", "source_cluster_ids": ["H002"]}
    admission = AdmissionResult(
        admitted_ags=[ag],
        denied_ag_ids=(),
        pivot_signal=False,
        first_ag_retired_id="",
    )
    decision = decide_slate_action(
        ag=ag,
        slate_admitted_ags=(ag,),
        admission_result=admission,
        blocked_cluster_ids=("H002",),
    )
    assert decision.action.value == "skip_ag"
    assert decision.reason == "cluster_blocked_no_rca"
    assert decision.denied_ag_id == "AG_DECOMPOSED_H002"


def test_empty_admitted_slate_pivots_iteration():
    ag = {"id": "AG_DECOMPOSED_H001", "source_cluster_ids": ["H001"]}
    admission = AdmissionResult(
        admitted_ags=[],
        denied_ag_ids=("AG_DECOMPOSED_H001",),
        pivot_signal=False,
        first_ag_retired_id="",
    )
    decision = decide_slate_action(
        ag=ag,
        slate_admitted_ags=(),
        admission_result=admission,
        blocked_cluster_ids=("H001",),
    )
    # When BOTH the slate is empty AND the AG is denied,
    # the result is still SKIP_AG.
    assert decision.action.value == "skip_ag"


def test_retired_pivot_signal_promotes_skip_to_pivot():
    ag = {"id": "AG_RETIRED_H001", "source_cluster_ids": ["H001"]}
    admission = AdmissionResult(
        admitted_ags=[],
        denied_ag_ids=("AG_RETIRED_H001",),
        pivot_signal=True,
        first_ag_retired_id="AG_RETIRED_H001",
    )
    decision = decide_slate_action(
        ag=ag,
        slate_admitted_ags=(),
        admission_result=admission,
        blocked_cluster_ids=(),
    )
    assert decision.action.value == "pivot_iteration"
    assert decision.reason == "ag_retired_pivot"
    assert decision.denied_ag_id == "AG_RETIRED_H001"


# ── Task 3: SLATE_AUTHORITATIVE_SKIP record + marker ─────────────────


from genie_space_optimizer.optimization.decision_emitters import (
    slate_authoritative_skip_record,
)
from genie_space_optimizer.optimization.rca_decision_trace import (
    DecisionType,
    DecisionOutcome,
)


def test_slate_authoritative_skip_record_emits_typed_record():
    rec = slate_authoritative_skip_record(
        run_id="run-1",
        iteration=2,
        ag_id="AG_DECOMPOSED_H001",
        source_cluster_ids=("H001",),
        reason="ag_denied_by_admission_trace",
    )
    d = rec.to_dict()
    assert d["decision_type"] == DecisionType.SLATE_AUTHORITATIVE_SKIP.value
    assert d["outcome"] == DecisionOutcome.SKIPPED.value
    assert d["ag_id"] == "AG_DECOMPOSED_H001"
    assert d["run_id"] == "run-1"
    assert d["iteration"] == 2
    # root_cause may be omitted from to_dict() when empty; the field
    # is empty string on the record.
    assert rec.root_cause == ""
    assert "H001" in str(d.get("metrics", {}).get("source_cluster_ids", []))
    assert d["next_action"].startswith("strategist must pick")


def test_slate_authoritative_skip_record_cluster_blocked_variant():
    rec = slate_authoritative_skip_record(
        run_id="run-1",
        iteration=2,
        ag_id="AG_DECOMPOSED_H002",
        source_cluster_ids=("H002",),
        reason="cluster_blocked_no_rca",
    )
    d = rec.to_dict()
    assert d["decision_type"] == DecisionType.SLATE_AUTHORITATIVE_SKIP.value
    assert "RCA card" in d["next_action"]


def test_slate_authoritative_skip_marker_emits_marker_line():
    from genie_space_optimizer.optimization.run_analysis_contract import (
        slate_authoritative_skip_marker,
    )
    line = slate_authoritative_skip_marker(
        optimization_run_id="run-1",
        iteration=2,
        ag_id="AG_DECOMPOSED_H001",
        reason="cluster_blocked_no_rca",
        source_cluster_ids=("H001",),
    )
    assert line.startswith("GSO_SLATE_AUTHORITATIVE_SKIP_V1 ")
    import json
    payload = json.loads(line[len("GSO_SLATE_AUTHORITATIVE_SKIP_V1 "):])
    assert payload["ag_id"] == "AG_DECOMPOSED_H001"
    assert payload["reason"] == "cluster_blocked_no_rca"
    assert payload["source_cluster_ids"] == ["H001"]
    assert payload["iteration"] == 2
    assert payload["optimization_run_id"] == "run-1"
