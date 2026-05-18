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


def test_marker_parser_extracts_no_structural_candidate_lines() -> None:
    """Lines like 'GSO_NO_STRUCTURAL_CANDIDATE_V1 {json}' are
    extracted with the JSON payload parsed into a dict."""
    from genie_space_optimizer.verification.anchor_chain import (
        parse_transcript_markers,
    )
    transcript = (
        'unrelated log line\n'
        'GSO_NO_STRUCTURAL_CANDIDATE_V1 {"ag_id":"AG_X","attempted_archetypes":[],"iteration":1,"skipped_reason":"missing_rca_card"}\n'
        'more log noise\n'
        'GSO_BEST_OF_N_RANKED_V1 {"intended_patch_shape":"structural","samples_emitted":3,"iteration":2}\n'
    )
    markers = parse_transcript_markers(transcript)
    by_name: dict[str, list[dict]] = {}
    for m in markers:
        by_name.setdefault(m.name, []).append(dict(m.payload))
    assert "GSO_NO_STRUCTURAL_CANDIDATE_V1" in by_name
    assert by_name["GSO_NO_STRUCTURAL_CANDIDATE_V1"][0]["skipped_reason"] == "missing_rca_card"
    assert "GSO_BEST_OF_N_RANKED_V1" in by_name
    assert by_name["GSO_BEST_OF_N_RANKED_V1"][0]["intended_patch_shape"] == "structural"


def test_marker_parser_ignores_unparseable_lines() -> None:
    """A marker line with a malformed JSON payload is silently
    dropped (defensive — transcripts contain truncated lines)."""
    from genie_space_optimizer.verification.anchor_chain import (
        parse_transcript_markers,
    )
    transcript = (
        'GSO_NO_STRUCTURAL_CANDIDATE_V1 {malformed\n'
        'GSO_BEST_OF_N_RANKED_V1 {"intended_patch_shape":"structural"}\n'
    )
    markers = parse_transcript_markers(transcript)
    names = [m.name for m in markers]
    assert names == ["GSO_BEST_OF_N_RANKED_V1"]


def test_marker_parser_empty_transcript() -> None:
    from genie_space_optimizer.verification.anchor_chain import (
        parse_transcript_markers,
    )
    assert parse_transcript_markers("") == ()


def test_count_best_of_n_structural_fires() -> None:
    """count_best_of_n_structural_fires(markers) returns the number
    of GSO_BEST_OF_N_RANKED_V1 markers whose intended_patch_shape
    is exactly 'structural'."""
    from genie_space_optimizer.verification.anchor_chain import (
        MarkerLine,
        count_best_of_n_structural_fires,
    )
    markers = (
        MarkerLine("GSO_BEST_OF_N_RANKED_V1", {"intended_patch_shape": "structural"}),
        MarkerLine("GSO_BEST_OF_N_RANKED_V1", {"intended_patch_shape": "instructional"}),
        MarkerLine("GSO_BEST_OF_N_RANKED_V1", {"intended_patch_shape": "structural"}),
        MarkerLine("GSO_OTHER", {"intended_patch_shape": "structural"}),
    )
    assert count_best_of_n_structural_fires(markers) == 2


def test_count_admitted_with_empty_intent() -> None:
    """count_admitted_with_empty_intent counts
    GSO_STRUCTURAL_REPAIR_DECISION_V1 entries whose gate_verdict
    is 'admitted' AND both intended_patch_shape and rca_root_cause
    are empty — the canonical pre-WU-3.5 + pre-WU-5 bug signature."""
    from genie_space_optimizer.verification.anchor_chain import (
        MarkerLine,
        count_admitted_with_empty_intent,
    )
    markers = (
        MarkerLine("GSO_STRUCTURAL_REPAIR_DECISION_V1", {
            "gate_verdict": "admitted",
            "intended_patch_shape": "",
            "rca_root_cause": "",
        }),
        MarkerLine("GSO_STRUCTURAL_REPAIR_DECISION_V1", {
            "gate_verdict": "admitted",
            "intended_patch_shape": "structural",
            "rca_root_cause": "non-empty",
        }),
        MarkerLine("GSO_STRUCTURAL_REPAIR_DECISION_V1", {
            "gate_verdict": "rejected",
            "intended_patch_shape": "",
            "rca_root_cause": "",
        }),
    )
    assert count_admitted_with_empty_intent(markers) == 1


def _pre_fix_postmortem() -> dict:
    """The exact shape the canonical pre-fix 7now run produces for
    its iter-1 H001/gs_013 anchor (extracted from
    docs/runid_analysis/ab65fefe-9bb5-411c-9818-f62633ec9cfd/postmortem.json)."""
    return {
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
                "next_step": "skip_productive",
            }
        ]
    }


def test_classifier_pre_fix_postmortem_marks_anchor_failed_with_missing_rca_card() -> None:
    """The canonical pre-WU-3.5 shape for gs_013 must FAIL with
    'missing_rca_card' as the reason."""
    from genie_space_optimizer.verification.anchor_chain import (
        AnchorChainVerifier,
        LifecyclePath,
    )
    v = AnchorChainVerifier(postmortem=_pre_fix_postmortem())
    result = v.run()
    assert not result.passed
    gs013 = next(
        (av for av in result.anchor_verdicts if av.qid_suffix == "gs_013"),
        None,
    )
    assert gs013 is not None
    assert gs013.passed is False
    assert gs013.lifecycle_path == LifecyclePath.UNKNOWN
    assert any("missing_rca_card" in r for r in gs013.reasons)


def test_classifier_grounded_with_candidate_path_a() -> None:
    """directive_outcome shows proposal_emitted AND
    no_structural_candidate is empty → Path A passes."""
    from genie_space_optimizer.verification.anchor_chain import (
        AnchorChainVerifier,
        LifecyclePath,
    )
    pm = {
        "iteration_summary": [
            {
                "iteration": 1,
                "ag_id": "AG_X",
                "cluster_ids": ["H001"],
                "target_qids": ["x_gs_013"],
                "directive_outcome": {"6": "proposal_emitted"},
                "no_structural_candidate": {},
                "terminal_reason": "",
                "next_step": "continue",
            }
        ]
    }
    result = AnchorChainVerifier(
        postmortem=pm,
        transcript_text='GSO_BEST_OF_N_RANKED_V1 {"intended_patch_shape":"structural"}\n',
    ).run()
    gs013 = next(av for av in result.anchor_verdicts if av.qid_suffix == "gs_013")
    assert gs013.lifecycle_path == LifecyclePath.GROUNDED_WITH_CANDIDATE
    assert gs013.passed is True


def test_classifier_grounded_with_typed_decline_path_b() -> None:
    """no_structural_candidate carries a typed archetype-decline
    reason AND non-empty attempted_archetypes → Path B passes."""
    from genie_space_optimizer.verification.anchor_chain import (
        AnchorChainVerifier,
        LifecyclePath,
    )
    pm = {
        "iteration_summary": [
            {
                "iteration": 3,
                "ag_id": "AG_X",
                "cluster_ids": ["H002"],
                "target_qids": ["x_gs_026"],
                "directive_outcome": {"5": "no_structural_candidate"},
                "no_structural_candidate": {
                    "skipped_reason": "no_top_n_archetype",
                    "attempted_archetypes": ["top_n", "filter_removal"],
                },
                "terminal_reason": "proposal_generation_empty",
                "next_step": "retry_strategy_switch",
            }
        ]
    }
    result = AnchorChainVerifier(
        postmortem=pm,
        transcript_text='GSO_BEST_OF_N_RANKED_V1 {"intended_patch_shape":"structural"}\n',
    ).run()
    gs026 = next(av for av in result.anchor_verdicts if av.qid_suffix == "gs_026")
    assert gs026.lifecycle_path == LifecyclePath.GROUNDED_WITH_TYPED_DECLINE
    assert gs026.passed is True


def test_classifier_preflight_skip_path_c() -> None:
    """If the postmortem records the WU-3 SKIP_AG signature
    (terminal_reason=='early_preflight_cluster_blocked_no_rca' OR a
    matching marker), Path C passes."""
    from genie_space_optimizer.verification.anchor_chain import (
        AnchorChainVerifier,
        LifecyclePath,
    )
    pm = {
        "iteration_summary": [
            {
                "iteration": 1,
                "ag_id": "AG_X",
                "cluster_ids": ["H001"],
                "target_qids": ["x_gs_013"],
                "directive_outcome": {},
                "no_structural_candidate": {},
                "terminal_reason": "early_preflight_cluster_blocked_no_rca",
                "next_step": "skip_ag",
            }
        ]
    }
    result = AnchorChainVerifier(
        postmortem=pm,
        transcript_text='GSO_BEST_OF_N_RANKED_V1 {"intended_patch_shape":"structural"}\n',
    ).run()
    gs013 = next(av for av in result.anchor_verdicts if av.qid_suffix == "gs_013")
    assert gs013.lifecycle_path == LifecyclePath.PREFLIGHT_SKIP
    assert gs013.passed is True


def test_classifier_global_failure_best_of_n_never_fired() -> None:
    """Even if every anchor passes individually, the global
    invariant 'best_of_n fires at least once for structural intent'
    must hold. A run with zero such markers FAILs overall."""
    from genie_space_optimizer.verification.anchor_chain import (
        AnchorChainVerifier,
    )
    pm = {
        "iteration_summary": [
            {
                "iteration": 1,
                "ag_id": "AG_X",
                "cluster_ids": ["H001"],
                "target_qids": ["x_gs_013"],
                "directive_outcome": {"6": "proposal_emitted"},
                "no_structural_candidate": {},
                "terminal_reason": "",
                "next_step": "continue",
            }
        ]
    }
    result = AnchorChainVerifier(
        postmortem=pm,
        transcript_text="",
    ).run()
    assert result.passed is False
    assert any(
        "best_of_n_structural_never_fired" in f
        for f in result.global_failures
    )


def test_classifier_global_failure_admitted_with_empty_intent() -> None:
    """The 7now iter-1 attempt-11 bug signature: at least one
    GSO_STRUCTURAL_REPAIR_DECISION_V1 with gate_verdict=admitted
    AND both intent fields empty. This is a hard FAIL even if
    individual anchors pass."""
    from genie_space_optimizer.verification.anchor_chain import (
        AnchorChainVerifier,
    )
    pm = {
        "iteration_summary": [
            {
                "iteration": 1,
                "ag_id": "AG_X",
                "cluster_ids": ["H001"],
                "target_qids": ["x_gs_013"],
                "directive_outcome": {"6": "proposal_emitted"},
                "no_structural_candidate": {},
                "terminal_reason": "",
                "next_step": "continue",
            }
        ]
    }
    transcript = (
        'GSO_BEST_OF_N_RANKED_V1 {"intended_patch_shape":"structural"}\n'
        'GSO_STRUCTURAL_REPAIR_DECISION_V1 '
        '{"gate_verdict":"admitted","intended_patch_shape":"","rca_root_cause":""}\n'
    )
    result = AnchorChainVerifier(
        postmortem=pm, transcript_text=transcript
    ).run()
    assert result.passed is False
    assert any(
        "admitted_with_empty_intent" in f
        for f in result.global_failures
    )


def test_classifier_anchor_not_present_in_run_is_silent() -> None:
    """Not every run will exercise every anchor. If a canonical
    anchor (e.g., gs_009) is not present in any iteration's target
    qids, the verifier emits no verdict for it and does not FAIL —
    it just verifies what's there."""
    from genie_space_optimizer.verification.anchor_chain import (
        AnchorChainVerifier,
    )
    pm = {
        "iteration_summary": [
            {
                "iteration": 1,
                "ag_id": "AG_X",
                "cluster_ids": ["H001"],
                "target_qids": ["x_gs_013"],
                "directive_outcome": {"6": "proposal_emitted"},
                "no_structural_candidate": {},
                "terminal_reason": "",
                "next_step": "continue",
            }
        ]
    }
    result = AnchorChainVerifier(
        postmortem=pm,
        transcript_text='GSO_BEST_OF_N_RANKED_V1 {"intended_patch_shape":"structural"}\n',
    ).run()
    qids = {av.qid_suffix for av in result.anchor_verdicts}
    assert "gs_013" in qids
    assert "gs_009" not in qids
