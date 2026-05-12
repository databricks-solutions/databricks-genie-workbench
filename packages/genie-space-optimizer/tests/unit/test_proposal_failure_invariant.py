"""Plan P-F — operator-transcript projection + iteration invariant tests."""

from __future__ import annotations


def test_proposal_generation_stage_projects_failure_decided_type() -> None:
    """Stage 6 (proposal_generation) carries both PROPOSAL_GENERATED and
    PROPOSAL_FAILURE_DECIDED so the operator transcript renders the
    failure record + the typed next-action label adjacent."""
    from genie_space_optimizer.optimization.operator_process_transcript import (
        _STAGE_DECISION_TYPE_MAP,
    )
    from genie_space_optimizer.optimization.rca_decision_trace import (
        DecisionType,
    )

    types = _STAGE_DECISION_TYPE_MAP.get("proposal_generation", ())
    assert DecisionType.PROPOSAL_GENERATED in types
    assert DecisionType.PROPOSAL_FAILURE_DECIDED in types


def test_emit_proposal_failure_decided_helper_appends_record_and_marker(
    monkeypatch,
) -> None:
    """The internal helper emits exactly one record + one marker into
    iter_inputs when the flag is on."""
    monkeypatch.setenv("GSO_PROPOSAL_FAILURE_DECIDED", "1")

    from genie_space_optimizer.optimization.harness import (
        _emit_proposal_failure_decided,
    )

    iter_inputs: dict = {}
    _emit_proposal_failure_decided(
        run_id="run_x",
        iteration=1,
        ag_id="AG_1",
        cluster_id="C1",
        cluster_signature="sig:abc",
        rca_id="rca_1",
        root_cause="missing_filter",
        failure_mode="proposal_generation_empty",
        lever_set=(1, 5, 6),
        tried_lever_families=(1,),
        ag_source_cluster_count=1,
        rca_card_grounded=True,
        prior_failure_count=0,
        target_qids=("q1",),
        iter_inputs=iter_inputs,
    )

    records = iter_inputs.get("decision_records") or []
    assert len(records) == 1
    rec = records[0]
    assert rec["decision_type"] == "proposal_failure_decided"
    assert rec["reason_code"] == "rotate_lever_family"

    markers = iter_inputs.get("markers") or []
    assert any(m.startswith("GSO_PROPOSAL_FAILURE_DECIDED_V1 ") for m in markers)


def test_emit_proposal_failure_decided_helper_noop_when_flag_off(
    monkeypatch,
) -> None:
    """Flag-off path appends nothing — replay byte-stability."""
    monkeypatch.delenv("GSO_PROPOSAL_FAILURE_DECIDED", raising=False)

    from genie_space_optimizer.optimization.harness import (
        _emit_proposal_failure_decided,
    )

    iter_inputs: dict = {}
    _emit_proposal_failure_decided(
        run_id="run_x",
        iteration=1,
        ag_id="AG_1",
        cluster_id="C1",
        cluster_signature="sig:abc",
        rca_id="rca_1",
        root_cause="missing_filter",
        failure_mode="proposal_generation_empty",
        lever_set=(1, 5, 6),
        tried_lever_families=(1,),
        ag_source_cluster_count=1,
        rca_card_grounded=True,
        prior_failure_count=0,
        target_qids=("q1",),
        iter_inputs=iter_inputs,
    )

    assert "decision_records" not in iter_inputs
    assert "markers" not in iter_inputs


def test_emit_force_l6_outcome_fires_taxonomy_record_on_declined(
    monkeypatch,
) -> None:
    """When _emit_force_l6_outcome is called with outcome='declined' AND
    the flag is on, a PROPOSAL_FAILURE_DECIDED record is emitted."""
    monkeypatch.setenv("GSO_PROPOSAL_FAILURE_DECIDED", "1")
    monkeypatch.setenv("GSO_LEVER6_FORCE_TYPED_OUTCOMES", "1")

    from genie_space_optimizer.optimization.harness import (
        _emit_force_l6_outcome,
    )

    iter_inputs: dict = {}
    _emit_force_l6_outcome(
        outcome="declined",
        run_id="run_y",
        iteration=4,
        ag_id="AG_FORCE",
        cluster_id="C9",
        root_cause="sql_shape",
        target_qids=("q5",),
        exception_repr="",
        iter_inputs=iter_inputs,
    )

    records = iter_inputs.get("decision_records") or []
    failure_decided = [
        r for r in records
        if r.get("decision_type") == "proposal_failure_decided"
    ]
    assert len(failure_decided) == 1
    assert failure_decided[0]["metrics"]["failure_mode"] == (
        "lever6_force_llm_declined"
    )


def test_no_causal_applyable_patch_path_constants_resolve() -> None:
    """Sanity check that the constants referenced by the wiring exist."""
    from genie_space_optimizer.optimization.harness import (
        _emit_proposal_failure_decided,
    )
    from genie_space_optimizer.common.config import (
        no_causal_applyable_halt_enabled,
    )
    from genie_space_optimizer.optimization.proposal_failure_policy import (
        ProposalFailureNextAction,
    )

    assert callable(_emit_proposal_failure_decided)
    assert callable(no_causal_applyable_halt_enabled)
    assert "narrow_ag_scope" in {a.value for a in ProposalFailureNextAction}


def test_emit_helper_accepts_all_selected_dropped_failure_mode(monkeypatch) -> None:
    """Verify the helper accepts the two new failure modes and the
    correct next-action emerges on a multi-cluster AG."""
    monkeypatch.setenv("GSO_PROPOSAL_FAILURE_DECIDED", "1")

    from genie_space_optimizer.optimization.harness import (
        _emit_proposal_failure_decided,
    )

    iter_inputs: dict = {}
    _emit_proposal_failure_decided(
        run_id="run_z",
        iteration=5,
        ag_id="AG_MULTI",
        cluster_id="C10",
        cluster_signature="sig:multi",
        rca_id="rca_z",
        root_cause="anything",
        failure_mode="all_selected_patches_dropped_by_applier",
        lever_set=(1, 5),
        tried_lever_families=(1,),
        ag_source_cluster_count=3,
        rca_card_grounded=True,
        prior_failure_count=0,
        target_qids=("q1",),
        iter_inputs=iter_inputs,
    )

    records = iter_inputs["decision_records"]
    assert len(records) == 1
    assert records[0]["reason_code"] == "narrow_ag_scope"

    iter_inputs2: dict = {}
    _emit_proposal_failure_decided(
        run_id="run_z",
        iteration=5,
        ag_id="AG_NO_APPLIED",
        cluster_id="C11",
        cluster_signature="sig:noapp",
        rca_id="rca_z2",
        root_cause="anything",
        failure_mode="no_applied_patches",
        lever_set=(1, 5, 6),
        tried_lever_families=(1,),
        ag_source_cluster_count=1,
        rca_card_grounded=True,
        prior_failure_count=0,
        target_qids=(),
        iter_inputs=iter_inputs2,
    )
    assert iter_inputs2["decision_records"][0]["reason_code"] == (
        "rotate_lever_family"
    )


def test_invariant_passes_when_iteration_has_failure_decided_record() -> None:
    """Iteration with zero applied patches and one failure_decided record
    satisfies the invariant."""
    from genie_space_optimizer.optimization.invariants import (
        check_proposal_failure_decided_coverage,
    )

    iter_inputs = {
        "applied_patches_total": 0,
        "exit_path": "proposals_empty",
        "decision_records": [
            {
                "decision_type": "proposal_failure_decided",
                "reason_code": "rotate_lever_family",
            }
        ],
    }
    result = check_proposal_failure_decided_coverage(iter_inputs)
    assert result.violated is False


def test_invariant_fails_when_iteration_has_zero_applied_and_no_failure_decided() -> None:
    """Iteration with zero applied patches and zero failure_decided records
    violates the invariant."""
    from genie_space_optimizer.optimization.invariants import (
        check_proposal_failure_decided_coverage,
    )

    iter_inputs = {
        "applied_patches_total": 0,
        "exit_path": "skipped_no_applied_patches",
        "decision_records": [
            {"decision_type": "acceptance_decided"},
        ],
    }
    result = check_proposal_failure_decided_coverage(iter_inputs)
    assert result.violated is True
    assert "proposal_failure_decided" in result.message


def test_invariant_skips_when_iteration_has_applied_patches() -> None:
    """Iterations with applied patches are out of scope for the invariant."""
    from genie_space_optimizer.optimization.invariants import (
        check_proposal_failure_decided_coverage,
    )

    iter_inputs = {
        "applied_patches_total": 2,
        "exit_path": "accepted",
        "decision_records": [],
    }
    result = check_proposal_failure_decided_coverage(iter_inputs)
    assert result.violated is False


def test_harness_emits_invariant_marker_when_coverage_violated(
    monkeypatch,
) -> None:
    """When the coverage invariant fails and the flag is on, the harness
    emits a GSO_INVARIANT_VIOLATION_V1 marker with the typed
    invariant_name."""
    monkeypatch.setenv("GSO_PROPOSAL_FAILURE_DECIDED", "1")

    from genie_space_optimizer.optimization.harness import (
        _check_and_emit_proposal_failure_coverage,
    )

    iter_inputs = {
        "applied_patches_total": 0,
        "exit_path": "proposals_empty",
        "decision_records": [],
    }
    markers: list[str] = []
    iter_inputs["markers"] = markers

    _check_and_emit_proposal_failure_coverage(
        run_id="run_q",
        iteration=2,
        iter_inputs=iter_inputs,
    )

    assert any(
        m.startswith("GSO_INVARIANT_VIOLATION_V1 ")
        and "proposal_failure_decided_coverage" in m
        for m in markers
    )


def test_harness_skips_invariant_marker_when_flag_off(monkeypatch) -> None:
    """Flag-off path emits nothing — replay byte-stability."""
    monkeypatch.delenv("GSO_PROPOSAL_FAILURE_DECIDED", raising=False)

    from genie_space_optimizer.optimization.harness import (
        _check_and_emit_proposal_failure_coverage,
    )

    iter_inputs = {
        "applied_patches_total": 0,
        "exit_path": "proposals_empty",
        "decision_records": [],
        "markers": [],
    }
    _check_and_emit_proposal_failure_coverage(
        run_id="run_q",
        iteration=2,
        iter_inputs=iter_inputs,
    )

    assert iter_inputs["markers"] == []
