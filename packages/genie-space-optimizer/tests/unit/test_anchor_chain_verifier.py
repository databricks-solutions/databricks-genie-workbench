"""WU-A — anchor-chain verifier unit tests."""
from __future__ import annotations

from pathlib import Path

import pytest


def test_data_contract_exports() -> None:
    """Public API is importable from the package root."""
    from genie_space_optimizer.verification import (
        AnchorChainVerifier,
        AnchorVerdict,
        LifecyclePath,
        VerifierResult,
        verify_runid_dir,
    )
    assert AnchorChainVerifier is not None
    assert AnchorVerdict is not None
    assert LifecyclePath is not None
    assert VerifierResult is not None
    assert callable(verify_runid_dir)


def test_lifecycle_path_enum_values() -> None:
    """LifecyclePath enum carries the four allowed values + UNKNOWN."""
    from genie_space_optimizer.verification import LifecyclePath
    assert LifecyclePath.GROUNDED_WITH_CANDIDATE.value == "A"
    assert LifecyclePath.GROUNDED_WITH_TYPED_DECLINE.value == "B"
    assert LifecyclePath.PREFLIGHT_SKIP.value == "C"
    assert LifecyclePath.UNKNOWN.value == "UNKNOWN"


def test_anchor_verdict_serializable() -> None:
    """AnchorVerdict is a frozen dataclass and supports asdict()."""
    from dataclasses import asdict
    from genie_space_optimizer.verification import (
        AnchorVerdict,
        LifecyclePath,
    )
    v = AnchorVerdict(
        qid_suffix="gs_013",
        cluster_id="H001",
        iteration=1,
        lifecycle_path=LifecyclePath.UNKNOWN,
        passed=False,
        reasons=("no card grounded; not preflight-skipped",),
    )
    d = asdict(v)
    assert d["qid_suffix"] == "gs_013"
    assert d["lifecycle_path"] == LifecyclePath.UNKNOWN  # enum preserved
    assert d["passed"] is False


def test_verifier_result_aggregates_anchor_verdicts() -> None:
    """VerifierResult.passed is True iff every per-anchor verdict
    passed AND every global invariant passed."""
    from genie_space_optimizer.verification import (
        AnchorVerdict,
        LifecyclePath,
        VerifierResult,
    )
    v_pass = AnchorVerdict(
        qid_suffix="gs_013",
        cluster_id="H001",
        iteration=1,
        lifecycle_path=LifecyclePath.GROUNDED_WITH_CANDIDATE,
        passed=True,
        reasons=(),
    )
    v_fail = AnchorVerdict(
        qid_suffix="gs_026",
        cluster_id="H002",
        iteration=3,
        lifecycle_path=LifecyclePath.UNKNOWN,
        passed=False,
        reasons=("missing_rca_card",),
    )
    result_pass = VerifierResult(
        anchor_verdicts=(v_pass,),
        global_failures=(),
        best_of_n_structural_fire_count=2,
    )
    result_fail = VerifierResult(
        anchor_verdicts=(v_pass, v_fail),
        global_failures=(),
        best_of_n_structural_fire_count=1,
    )
    result_global_fail = VerifierResult(
        anchor_verdicts=(v_pass,),
        global_failures=("best_of_n_structural_never_fired",),
        best_of_n_structural_fire_count=0,
    )
    assert result_pass.passed is True
    assert result_fail.passed is False
    assert result_global_fail.passed is False


def test_postmortem_iteration_records_extracts_relevant_fields() -> None:
    """parse_iteration_records turns iteration_summary into typed
    IterationRecord namedtuples with the fields the classifier
    needs."""
    from genie_space_optimizer.verification.anchor_chain import (
        parse_iteration_records,
    )
    pm = {
        "iteration_summary": [
            {
                "iteration": 1,
                "ag_id": "AG_DECOMPOSED_H001",
                "cluster_ids": ["H001"],
                "target_qids": ["7now_delivery_analytics_space_gs_013"],
                "directive_outcome": {"6": "proposal_emitted"},
                "no_structural_candidate": {
                    "skipped_reason": "missing_rca_card",
                    "attempted_archetypes": [],
                },
                "terminal_reason": "full_eval_regression",
            }
        ]
    }
    records = parse_iteration_records(pm)
    assert len(records) == 1
    r = records[0]
    assert r.iteration == 1
    assert r.ag_id == "AG_DECOMPOSED_H001"
    assert r.cluster_ids == ("H001",)
    assert r.target_qids == ("7now_delivery_analytics_space_gs_013",)
    assert r.directive_outcome == {"6": "proposal_emitted"}
    assert r.no_structural_candidate == {
        "skipped_reason": "missing_rca_card",
        "attempted_archetypes": [],
    }


def test_postmortem_parse_handles_missing_iteration_summary() -> None:
    """Empty / absent iteration_summary returns empty tuple, not
    raises."""
    from genie_space_optimizer.verification.anchor_chain import (
        parse_iteration_records,
    )
    assert parse_iteration_records({}) == ()
    assert parse_iteration_records({"iteration_summary": []}) == ()
    assert parse_iteration_records({"iteration_summary": None}) == ()


def test_postmortem_anchor_qid_suffix_match() -> None:
    """qid_suffix_for_match strips space prefix → "gs_013"."""
    from genie_space_optimizer.verification.anchor_chain import (
        qid_suffix_for_match,
    )
    assert qid_suffix_for_match("7now_delivery_analytics_space_gs_013") == "gs_013"
    assert qid_suffix_for_match("airline_space_gs_009") == "gs_009"
    assert qid_suffix_for_match("gs_026") == "gs_026"
    assert qid_suffix_for_match("") == ""
